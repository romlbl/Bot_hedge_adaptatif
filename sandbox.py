"""
sandbox.py — consultation ponctuelle du top des opportunités actuelles.

Utilise directement market_scanner.selection(), qui est 100% read-only :
aucun appel DB, aucune écriture de waitlist, aucun ordre passé.
Sûr à lancer en parallèle du bot en prod.
"""

import asyncio

import ccxt.pro as ccxtpro

from market_scanner import selection


async def main(top_n: int = 5):
    exchange = ccxtpro.bybit({
        "enableRateLimit": True,
        "timeout": 10000,
    })
    try:
        prepared = await selection(exchange)  # déjà trié par score réel décroissant
        top = prepared[:top_n]

        if not top:
            print("Aucun candidat ne passe les filtres actuellement.")
            return

        print(f"\n=== Top {len(top)} opportunités actuelles ===\n")
        for cand, real_score in top:
            symbol, _cheap_score, adx, slope, delta, side, _open_time, start_price, vrt = cand
            print(
                f"{symbol:15s} {side:5s}  score={real_score:.3f}  "
                f"adx={adx:<4}  slope={slope:+.2f}  delta={delta:.2f}  "
                f"variation={vrt:+.2%}  prix={start_price}"
            )
    finally:
        await exchange.close()


if __name__ == "__main__":
    asyncio.run(main(top_n=5))