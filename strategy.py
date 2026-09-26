import asyncio
import time
import logging
from math import log10, exp, tanh
import asyncio

import numpy as np
import pandas as pd
import pandas_ta_classic as ta  # noqa: F401  (nécessaire pour l'accessor df.ta)

from config import Config


def _bounded(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    """Clamp générique — garantit qu'un sous-score reste dans son intervalle attendu,
    même si une formule dérive légèrement hors bornes (garde-fou de cohérence)."""
    return max(lo, min(hi, x))


def pente_delta(series):
    """Régression linéaire normalisée. Retourne (pente, delta), delta = écart
    (percentile 95) à la tendance. Réutilisée par analyse() ET score()."""
    y = np.asarray(series)
    denom = y.max() - y.min()
    y = (y - y.min()) / denom if denom != 0 else np.zeros_like(y)
    x = np.linspace(0, 1, len(y))
    slope, intercept = np.polyfit(x, y, 1)
    trend = slope * x + intercept
    delta = np.percentile(np.abs(y - trend), 95)
    return round(float(slope), 3), round(float(delta), 3)

def _trend_freshness_ratio(closes, recent_hours: int, baseline_hours: int):
    """
    Compare la variation de prix sur la fenêtre récente (recent_hours) à la
    variation sur la fenêtre de référence plus longue (baseline_hours), les
    deux mesurées depuis le prix courant vers le passé.

    Retourne un ratio proche de 1 si la quasi-totalité du mouvement observé
    sur baseline_hours s'est produite dans les recent_hours les plus récentes
    (tendance fraîche). Un ratio faible, nul ou négatif signale une tendance
    déjà largement amorcée avant la fenêtre récente (ou un renversement en
    cours) -- les signes s'annulent naturellement si le mouvement récent va
    dans le même sens que le mouvement de référence, pas besoin de connaître
    la direction (LONG/SHORT) en paramètre.
    Retourne None si l'historique disponible est insuffisant.
    """
    if len(closes) < baseline_hours + 1:
        return None
    arr = np.asarray(closes, dtype=float)
    p_now = arr[-1]
    p_recent0 = arr[-(recent_hours + 1)]
    p_baseline0 = arr[-(baseline_hours + 1)]
    var_baseline = p_now - p_baseline0
    if var_baseline == 0:
        return None
    var_recent = p_now - p_recent0
    return round(float(var_recent / var_baseline), 3)

def fReg(v):
    """Combinaison sigmoïdes + partie rationnelle : transforme une pente en
    composante de score signée."""
    sigmoide_droite = 1 / (1 + exp(-25 * (v + 0.2)))
    sigmoide_gauche = 1 / (1 + exp(25 * (v + 0.2)))
    partie_rationnelle = (0.5 * v * (1 - v)) / (v**2 - 1.6 * v + 0.735)
    return (sigmoide_droite * partie_rationnelle) - sigmoide_gauche


async def score(position, exchange,live_pnl=None):
    """
    Quantifie à quel point le symbole continue de suivre sa tendance (LONG ou SHORT).

    Principe : chaque indicateur produit un sous-score borné dans [-1, 1]
    (positif = confirme la tendance, négatif = signale un retournement) :
      - reg_score    : pente de la régression linéaire 1h (poids dominant)
      - adx_score    : force de la tendance (ADX)
      - jump_score   : mouvement brutal récent (5m), normalisé par la volatilité horaire
      - volume_score : confirmation/essoufflement par le volume récent

    'delta' (dispersion autour de la régression) n'est PAS un score directionnel :
    c'est un facteur de CONFIANCE qui atténue l'ensemble si le mouvement est bruité.

    'pnl' n'est PAS un sous-score en compétition avec les autres : c'est une
    TOLÉRANCE asymétrique qui n'amortit qu'un trend_score négatif, et seulement
    si la position est gagnante (jamais l'inverse, jamais sur un signal déjà positif).

    position : (symbole, startTime, side, startPrice)
    """
    try:
        symb, start, side, startP = position

        # 2 appels réseau indépendants, lancés en parallèle (minimum possible :
        # aucun appel supplémentaire vs la version précédente).
        ohlcv, ohlcv_5m = await asyncio.gather(
            exchange.fetch_ohlcv(symb, '1h', limit=Config.SCORE_LIMIT_OHLCV),
            exchange.fetch_ohlcv(symb, '5m', limit=Config.SCORE_JUMP_WINDOW + 1),
        )
        if not ohlcv:
            return None

        df = pd.DataFrame(ohlcv, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['Date'] = pd.to_datetime(df['Date'], unit='ms')
        df.set_index('Date', inplace=True)
        df = df.astype(float)
        df.ta.adx(length=14, append=True)

        duration = (time.time() * 1000 - start) / 3600000  # float : décroissance lisse du bonus
        currP = df['Close'].iloc[-1]

        if live_pnl is not None:
            pnl = live_pnl
        else:
            pnl = (currP - startP) / startP
            pnl = pnl if side == "LONG" else -pnl

        sgn = 1 if side == "LONG" else -1

        # --- régression 1h ---
        c_arr = df["Close"].tolist()
        sample1H = c_arr[-Config.SCORE_SAMPLE_LEN:]
        pente1H, delta1H = pente_delta(sample1H)
        reg_score = _bounded((1 / exp(min(((abs(pente1H) - 0.7) / 0.5) ** 4, 700))))

        # --- ADX : force de la tendance, direction donnée par la pente ---
        adx_val = round(df['ADX_14'].iloc[-2]) if ('ADX_14' in df.columns and not pd.isna(df['ADX_14'].iloc[-2])) else 20  # bougie clôturée
        adx_strength = 1 / exp(min(((adx_val - 32.5) / 20) ** 4, 700))
        adx_score = _bounded(sgn * ((pente1H > 0) * 2 - 1) * adx_strength)

        # --- volatilité horaire (réutilisée pour le jump, aucun appel réseau) ---
        hourly_returns = df['Close'].pct_change().dropna()
        vol_1h = hourly_returns.std()
        if not vol_1h or vol_1h != vol_1h:  # NaN guard (coin trop récent / plat)
            vol_1h = 1e-6

        # --- jump : mouvement brutal récent (5m), normalisé par la volatilité horaire ---
        recent_return = 0.0
        if ohlcv_5m and len(ohlcv_5m) >= Config.SCORE_JUMP_WINDOW + 1:
            closes_5m = [c[4] for c in ohlcv_5m]
            recent_price = closes_5m[-2]                                 # dernière bougie 5m CLÔTURÉE
            ref_price_5m = closes_5m[-(Config.SCORE_JUMP_WINDOW + 1)]    # N bougies plus tôt, clôturée
            if ref_price_5m:
                recent_return = (recent_price - ref_price_5m) / ref_price_5m
        jump_score = sgn * tanh((recent_return / vol_1h) / Config.SCORE_JUMP_SENSITIVITY)

        # --- volume : la tendance est-elle confirmée ou s'essouffle-t-elle ? ---
        vol_recent = df['Volume'].tail(Config.SCORE_VOLUME_WINDOW).mean()
        vol_avg = df['Volume'].mean()
        vol_ratio = (vol_recent / vol_avg) if vol_avg else 1.0
        volume_score = sgn * tanh((vol_ratio - 1) / Config.SCORE_VOLUME_SENSITIVITY)

        # --- confiance : delta élevé (bruit) atténue TOUT le trend_score ---
        confidence = _bounded(1 - 1.7 / (1 + exp(-10 * (delta1H - 0.5))), 0.0, 1.0)

        trend_score = confidence * (
            Config.SCORE_W_REG * reg_score
            + Config.SCORE_W_ADX * adx_score
            + Config.SCORE_W_JUMP * jump_score
            + Config.SCORE_W_VOLUME * volume_score
        )

        # --- tolérance pnl : atténue UNIQUEMENT un signal négatif, et seulement
        #     si la position est gagnante ; ne modifie jamais un signal positif ---
        if trend_score < 0:
            tolerance = 1 / (1 + Config.SCORE_PNL_TOLERANCE_K * max(pnl, 0))
            trend_score *= tolerance

        raw_bonus = -log10(0.2 * duration + 1) + 0.7
        bonus_duration = raw_bonus if raw_bonus > 0 else 0

        headroom = 1.0 - abs(trend_score)          # marge disponible avant saturation, ∈ [0,1]
        total_score = trend_score + sgn * bonus_duration * headroom

        return round(total_score, 2)

    except Exception as e:
        logging.error(f"Erreur de score sur {symb}: {e}")
        return None

def _analyse_compute(ohlcv, coin):
    """
    Logique de calcul PURE d'analyse() -- aucun appel réseau. Isolée pour être
    réutilisable telle quelle par le backtest de détection (mêmes seuils/formules
    que le bot en production, aucune divergence possible).
    """
    cols = ["open", "high", "low", "close", "volume"]
    if not ohlcv or len(ohlcv) < Config.ANALYSIS_MIN_CANDLES:
        return None

    df = pd.DataFrame(ohlcv, columns=["time"] + cols)
    df[cols] = df[cols].astype(float)
    df.ta.adx(length=14, append=True)

    if "ADX_14" not in df.columns or pd.isna(df["ADX_14"].iloc[-2]):
        return None

    currPrice = df["close"].iloc[-1]
    y_raw = df["close"].tail(Config.ANALYSIS_TAIL_LEN).values

    if y_raw.max() == y_raw.min():
        return None

    variation_pct = (y_raw[-1] - y_raw[0]) / y_raw[0]
    slope, delta = pente_delta(y_raw)
    adx = round(df["ADX_14"].iloc[-2])

    freshness = _trend_freshness_ratio(
        df["close"].values, Config.TREND_FRESH_WINDOW_H, Config.TREND_BASELINE_WINDOW_H
    )

    delta = abs(round(delta, 2))
    slope = round(slope, 2)
    vrt = round(variation_pct, 2)

    safe_slope = max(3.5 * abs(slope) + 1, 1e-10)
    score_slope = min(log10(safe_slope) + 0.5, 1.0)
    score_delta = 1 / (3 * delta + 0.7)
    score_vrt = 1 / exp(min(((abs(vrt) - 0.1) / 0.1) ** 4, 700))
    score_adx = 1 / exp(min(((adx - 32.5) / 20) ** 4, 700))
    scoreTot = (5 * score_delta + 4 * score_slope + 3 * score_vrt + score_adx) / 13

    return (
        delta,
        slope,
        adx,
        vrt,
        freshness,
        round(scoreTot, 3),
        currPrice,
        coin,
    )


async def analyse(coin, exchange):
    """
    Analyse un marché pour détecter une occasion LONG ou SHORT.
    Retourne (delta, slope, adx, variation, score, prix_courant, symbole) ou None.
    """
    try:
        ohlcv = await exchange.fetch_ohlcv(coin, "1h", limit=Config.ANALYSIS_LIMIT_OHLCV)
        return _analyse_compute(ohlcv, coin)
    except Exception as e:
        logging.error(f"Erreur d'analyse sur {coin}: {e}")
        return None
