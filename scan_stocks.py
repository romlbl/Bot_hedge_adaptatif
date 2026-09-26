"""
Scanner autonome (hors bot) : détecte les meilleures opportunités LONG/SHORT
sur l'univers du S&P 500 via yfinance, en réutilisant SANS DUPLICATION la même
logique de détection que le bot crypto (_analyse_compute + _filter_and_rank,
importées telles quelles depuis strategy.py / market_scanner.py).

Aucune dépendance à ccxt/aiosqlite/db.py/bot.py -- fichier 100% indépendant,
exécutable seul : python stock_scanner.py
"""
import asyncio
import logging

import pandas as pd
import yfinance as yf

from config import Config
from strategy import _analyse_compute
from market_scanner import _filter_and_rank

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Filet de sécurité UNIQUEMENT : utilisé si le scraping Wikipedia échoue
# (page indisponible, changement de structure, pas de réseau...). Volontairement
# minimaliste -- le vrai univers vient de get_sp500_tickers() ci-dessous.
FALLBACK_UNIVERSE = [
    # Les 19 initiaux
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "ORCL",
    "COST", "NFLX", "AMD", "ADBE", "CRM", "PEP", "KO", "MCD", "NKE", "DIS",
    # Complément pour atteindre 100 grandes capitalisations
    "BRK.B", "JNJ", "V", "WMT", "JPM", "MA", "UNH", "PG", "HD", "BAC",
    "XOM", "ABBV", "CVX", "LLY", "MRK", "PFE", "TMO", "ABT", "CSCO", "ACN",
    "LIN", "WFC", "INTU", "TXN", "QCOM", "MS", "AMAT", "IBM", "GE", "CAT",
    "PM", "NOW", "GS", "ISRG", "UNP", "NEE", "HON", "CMCSA", "SPGI", "BKNG",
    "RTX", "T", "LOW", "AMGN", "BLK", "VZ", "SYK", "TJX", "DHR", "MDLZ",
    "PGR", "LMT", "SCHW", "ELV", "C", "BA", "DE", "ADP", "MU", "LRCX",
    "PLTR", "FI", "SBUX", "INTC", "COP", "PANW", "REGN", "GILD", "BMY", "CI",
    "MDT", "KLAC", "SNPS", "CDNS", "CEG", "AMT", "CB", "MMC", "MO", "SHW", "KMB"
]

TOP_N = 5  # indépendant de Config.MAX_SELECTION (15), propre à ce scanner

# yfinance limite l'intraday '1h' à 730 jours d'historique max -- 3 mois est
# largement suffisant pour couvrir TREND_BASELINE_WINDOW_H (120h) et
# ANALYSIS_LIMIT_OHLCV (150 bougies).
YF_PERIOD = "3mo"
YF_INTERVAL = "1h"

# yf.download batché en un seul appel réseau plante ou devient très lent au-delà
# de quelques centaines de tickers -- on découpe le S&P500 (~500 tickers) en
# lots, séquentiels par lot (chaque lot reste threadé en interne par yfinance).
DOWNLOAD_CHUNK_SIZE = 100


def _fetch_sp500_tickers_sync() -> list:
    """
    Scrape la table des composants du S&P 500 sur Wikipedia (colonne 'Symbol').
    Appel réseau SYNCHRONE -- exécuté via asyncio.to_thread par l'appelant,
    même pattern que _download_all().

    Conversion '.' -> '-' (ex. BRK.B -> BRK-B) : Wikipedia utilise la notation
    boursière standard, yfinance attend la notation Yahoo Finance. Sans ce
    correctif, ces tickers échoueraient silencieusement au téléchargement.
    """
    tables = pd.read_html(SP500_WIKI_URL)
    df = tables[0]
    tickers = df["Symbol"].astype(str).str.strip().str.replace(".", "-", regex=False)
    return sorted(tickers.unique().tolist())


async def get_sp500_tickers() -> list:
    """Équivalent stock de exchange.load_markets() : univers récupéré
    dynamiquement plutôt que codé en dur. Fallback sur une petite liste statique
    si le scraping échoue, pour ne jamais bloquer le scanner."""
    try:
        tickers = await asyncio.to_thread(_fetch_sp500_tickers_sync)
        if not tickers:
            raise ValueError("liste vide")
        logging.info(f"{len(tickers)} tickers S&P 500 récupérés depuis Wikipedia.")
        return tickers
    except Exception as e:
        logging.error(
            f"get_sp500_tickers: échec récupération S&P 500 ({e}), "
            f"repli sur FALLBACK_UNIVERSE ({len(FALLBACK_UNIVERSE)} tickers)."
        )
        return FALLBACK_UNIVERSE


def _yf_to_ohlcv(df) -> list:
    """
    Convertit un DataFrame yfinance (index=Datetime) au format ccxt-like
    [ts_ms, open, high, low, close, volume] attendu par _analyse_compute --
    même format que le chemin crypto, aucune divergence.
    """
    df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    out = [
        [int(ts.timestamp() * 1000), float(row["Open"]), float(row["High"]),
         float(row["Low"]), float(row["Close"]), float(row["Volume"])]
        for ts, row in df.iterrows()
    ]
    # Aligné sur analyse() : ne garde que les ANALYSIS_LIMIT_OHLCV bougies les
    # plus récentes, même si l'historique téléchargé en contient plus.
    return out[-Config.ANALYSIS_LIMIT_OHLCV:]


def _download_chunk(tickers: list) -> dict:
    """Un seul appel yf.download batché pour un LOT de tickers (voir
    DOWNLOAD_CHUNK_SIZE) -- limite la charge par requête tout en évitant un
    appel réseau individuel par symbole, comme _download_all() le faisait
    déjà pour un univers plus petit."""
    data = yf.download(
        tickers=tickers, period=YF_PERIOD, interval=YF_INTERVAL,
        group_by="ticker", auto_adjust=False, threads=True, progress=False,
    )
    result = {}
    for ticker in tickers:
        try:
            df = data[ticker] if len(tickers) > 1 else data
        except (KeyError, TypeError):
            continue
        if df is not None and not df.empty:
            result[ticker] = df
    return result


def _download_all(tickers: list) -> dict:
    """Découpe l'univers en lots de DOWNLOAD_CHUNK_SIZE et agrège les
    résultats -- nécessaire pour ~500 tickers (S&P500), là où un seul
    yf.download() géant devient peu fiable."""
    result = {}
    for i in range(0, len(tickers), DOWNLOAD_CHUNK_SIZE):
        chunk = tickers[i:i + DOWNLOAD_CHUNK_SIZE]
        try:
            result.update(_download_chunk(chunk))
        except Exception as e:
            logging.error(f"_download_all: échec sur le lot {i}-{i + len(chunk)}: {e}")
    return result


async def _scan_stocks(tickers: list) -> dict:
    """
    Équivalent stock de _scan_markets() : téléchargement réseau (par lots,
    dans un thread séparé pour ne pas geler la boucle asyncio), puis
    _analyse_compute() -- pure, sans réseau -- appliquée à chaque symbole.
    """
    raw = await asyncio.to_thread(_download_all, tickers)
    scan = {}
    for ticker, df in raw.items():
        ohlcv = _yf_to_ohlcv(df)
        res = _analyse_compute(ohlcv, ticker)
        if res:
            scan[ticker] = res
    logging.info(f"{len(tickers)} actions analysées, {len(scan)} résultats exploitables.")
    return scan


async def top_opportunities(tickers: list = None, top_n: int = TOP_N) -> list:
    tickers = tickers if tickers is not None else await get_sp500_tickers()
    scan = await _scan_stocks(tickers)
    candidats = _filter_and_rank(scan)  # seuils Config + tri, réutilisés tels quels
    return candidats[:top_n]


def _print_results(candidats: list) -> None:
    if not candidats:
        print("Aucune opportunité détectée sur l'univers scanné.")
        return
    print(f"\nTop {len(candidats)} opportunités actions :\n")
    for coin, scr, adx, pente, delt, side, open_time, startP, vrt in candidats:
        print(
            f"  {coin:<8} {side:<5}  score={scr:.3f}  adx={adx}  "
            f"pente={pente:+.3f}  delta={delt:.3f}  variation={vrt:+.2%}  prix={startP:.2f}"
        )


if __name__ == "__main__":
    results = asyncio.run(top_opportunities())
    _print_results(results)