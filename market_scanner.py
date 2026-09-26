import asyncio
import time
import logging
import pandas as pd

from config import Config
from strategy import analyse, score

def _passes_filters(delta, adx, slope, vrt, freshness) -> bool:
    return (
        delta <= Config.DELTA_MAX
        and Config.ADX_MIN <= adx <= Config.ADX_MAX
        and abs(slope) >= Config.SLOPE_MIN
        and Config.VARIATION_MIN <= abs(vrt) <= Config.VARIATION_MAX
        and freshness is not None
        and freshness >= Config.TREND_FRESHNESS_MIN_RATIO
    )

async def _scan_markets(exchange) -> dict:
    """
    Lance analyse() UNE SEULE FOIS par marché éligible (spot + margin + actif).
    Retourne un dict {symbole: résultat_analyse} avec TOUS les résultats bruts
    (filtrés ou non).

    Ce dict est ensuite partagé par _filter_and_rank() (nouvelles opportunités)
    ET par WaitlistManager.W_selection() (rafraîchissement de la waitlist
    existante), pour ne jamais rappeler analyse() deux fois sur le même
    symbole dans un même cycle -- et donc minimiser les appels API.
    """
    markets = await exchange.load_markets()
    marches = [
        symbol for symbol, m in markets.items()
        if m.get('swap')
        and m.get('active')
        and m.get('quote') == 'USDT'
        and m.get('base') not in Config.TRADFI_EXCLUDED_BASES
    ]
    sem = asyncio.Semaphore(Config.ANALYSIS_SEMAPHORE)

    async def safe_analyse(coin):
        async with sem:
            return await analyse(coin, exchange)

    results = await asyncio.gather(*(safe_analyse(s) for s in marches), return_exceptions=True)
    logging.info(f"{len(marches)} marchés analysés.")

    scan = {}
    for coin, res in zip(marches, results):
        if isinstance(res, Exception) or not res:
            continue
        scan[coin] = res
    return scan


def _filter_and_rank(scan_results: dict) -> list:
    """
    Logique pure (aucun appel réseau) : applique les seuils de Config
    (delta, ADX, pente, variation), trie par score, garde le top
    MAX_SELECTION.

    Utilisée à la fois par selection() (usage autonome) et par
    WaitlistManager.W_selection(), qui lui passe le même scan déjà en main
    plutôt que d'en refaire un.
    """
    now = int(time.time())
    candidats = []
    for coin, res in scan_results.items():
        delt, pente, adx, vrt, fresh, scr, startP, _ = res
        if _passes_filters(delt, adx, pente, vrt, fresh):
            side = "LONG" if pente > 0 else "SHORT"
            candidats.append((
                coin, scr, adx, pente, delt, side,
                now * 1000, startP, vrt
            ))
    candidats.sort(key=lambda x: x[1], reverse=True)
    return candidats[:Config.MAX_SELECTION]


async def _advanced_score(cand, exchange):
    """
    Filtre coûteux en réseau, appliqué uniquement sur les candidats déjà
    pré-filtrés par _filter_and_rank : rejette un spread trop large, puis
    calcule le score() réel (retourné pour être réutilisé comme score de
    classement final -- pas de second appel nécessaire côté appelant).
    Retourne None si le candidat est rejeté.
    """
    coin, _cheap_score, _adx, _slope, _delta, side, open_time, start_price, _vrt = cand
    try:
        # Ticker et funding rate en parallèle -- un seul aller-retour réseau en plus
        # du spread/liquidité, pas de latence séquentielle supplémentaire.
        ticker, funding = await asyncio.gather(
            exchange.fetch_ticker(coin),
            exchange.fetch_funding_rate(coin),
        )
        ask, bid = ticker.get("ask"), ticker.get("bid")
        if ask is None or bid is None or bid <= 0:
            return None
        if (ask - bid) / bid > Config.SPREAD_MAX:
            return None

        # Filtre de liquidité : rejette les symboles trop peu échangés sur 24h.
        quote_volume = ticker.get("quoteVolume")
        if quote_volume is None or quote_volume < Config.MIN_QUOTE_VOLUME_24H:
            return None

        # Filtre funding rate : rejette si le funding est défavorable au sens de
        # la position au-delà du seuil accepté (coût de portage trop élevé).
        # LONG paie quand funding > 0, SHORT paie quand funding < 0.
        funding_rate = funding.get("fundingRate")
        if funding_rate is None:
            return None  # donnée manquante -> on écarte par prudence plutôt que de laisser passer
        if side == "LONG" and funding_rate > Config.FUNDING_RATE_MAX:
            return None
        if side == "SHORT" and funding_rate < -Config.FUNDING_RATE_MAX:
            return None

        scr = await score((coin, open_time, side, start_price), exchange)

        if scr is None or scr <= Config.SCORE_SCAN_MIN:
            return None
        return scr
    except Exception as e:
        logging.error(f"Erreur filtre avancé sur {coin}: {e}")
        return None


async def _filter_advanced(candidats: list, exchange) -> list:
    """Applique _advanced_score en parallèle (sémaphore) sur une liste de
    candidats déjà filtrés bon marché. Retourne [(cand, score_reel), ...]."""
    sem = asyncio.Semaphore(Config.SCAN_ADVANCED_SEMAPHORE)

    async def guarded(cand):
        async with sem:
            return cand, await _advanced_score(cand, exchange)

    results = await asyncio.gather(*(guarded(c) for c in candidats), return_exceptions=True)
    return [
        (cand, scr) for cand, scr in
        (r for r in results if not isinstance(r, Exception))
        if scr is not None
    ]


async def selection(exchange) -> list:
    """Usage autonome : renvoie désormais [(candidat, score_reel), ...] triés
    par score décroissant (changement de contrat vs avant -- à adapter si
    du code externe consomme cette fonction)."""
    scan = await _scan_markets(exchange)
    candidats = _filter_and_rank(scan)
    prepared = await _filter_advanced(candidats, exchange)
    prepared.sort(key=lambda x: x[1], reverse=True)
    return prepared