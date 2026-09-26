import asyncio
import time
import logging

from config import Config
from db import Database
from position_manager import PositionManager
from market_scanner import _passes_filters, _scan_markets, _filter_and_rank, _filter_advanced


class WaitlistManager:
    def __init__(self, exchange, db: Database, position_manager: PositionManager):
        self.exchange = exchange
        self.db = db
        self.position_manager = position_manager

    async def W_selection(self) -> None:
        # 0. Pas de scan si déjà au max de positions actives (point 3).
        active_positions = await self.db.get_active_positions()
        if len(active_positions) >= Config.MAX_ACTIVE_POSITIONS:
            logging.info("W_selection: positions au maximum, scan ignoré ce cycle.")
            return

        now_ms = int(time.time() * 1000)

        # 1. UN SEUL scan de tous les marchés éligibles pour tout le cycle.
        scan = await _scan_markets(self.exchange)
        candidats = _filter_and_rank(scan)

        # 2. Rafraîchit les candidats déjà en waitlist depuis ce même scan.
        waitlist_db = await self.db.get_waitlist()
        deja_presents = {c[0] for c in candidats}
        for symbol, start_time, side in waitlist_db:
            if symbol in deja_presents:
                continue
            res = scan.get(symbol)
            if not res:
                continue
            delta, slope, adx_val, vrt, fresh, scr, startP, _ = res
            if (abs(now_ms - start_time) <= Config.POSITION_DURATION_WAIT_MS) and _passes_filters(delta, adx_val, slope, vrt, fresh):
                candidats.append((symbol, scr, adx_val, slope, delta, side, start_time, startP, vrt))
                deja_presents.add(symbol)

        if not candidats:
            return

        # 3. Filtre avancé (réseau) : spread < SPREAD_MAX et score() > SCORE_SCAN_MIN.
        #    Le score() calculé ici sert AUSSI de score de classement final.
        prepared = await _filter_advanced(candidats, self.exchange)
        if not prepared:
            return

        prepared.sort(key=lambda x: x[1], reverse=True)
        prepared = prepared[:Config.MAX_ACTIVE_POSITIONS]

        # 4. Persiste la nouvelle waitlist.
        rows = [
            (cand[0], cand[5], cand[6], cand[4], cand[8], cand[3], cand_score, cand[2])
            for cand, cand_score in prepared
        ]
        await self.db.replace_waitlist(rows)

        # 5. Ouvre des positions sur les places libres.
        open_symbols = {symbol for _, symbol, _ in active_positions}
        libre = Config.MAX_ACTIVE_POSITIONS - len(active_positions)
        if libre <= 0:
            return

        cands_to_open = [c for c in prepared if c[0][0] not in open_symbols][:libre]
        if not cands_to_open:
            return

        for cand, cand_score in cands_to_open:
            symbol, side = cand[0], cand[5]
            id_pos, succes = await self.position_manager.W_openPos(symbol, side, cand_score)
            if succes:
                self.position_manager.spawn_gestion(id_pos)
            else:
                logging.info(f"Erreur ouverture sur la position {id_pos} ({symbol}).")
            await asyncio.sleep(1)