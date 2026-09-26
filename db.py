import asyncio
import time
import aiosqlite

from config import Config


class Database:
    """
    Accès asynchrone unique à la base SQLite.

    - Un seul worker consomme une write_queue pour sérialiser les écritures
      (évite les locks SQLite en cas d'écritures concurrentes).
    - Les lectures sont directes (sûres en concurrence grâce au mode WAL).
    - Ne crée AUCUNE table : le schéma est initialisé une seule fois, en amont,
      par database.py.
    """

    def __init__(self, db_path: str = Config.DB_PATH):
        self.db_path = db_path
        self.write_queue = asyncio.Queue()
        self.worker_task = None
        self.db = None

    # ------------------------------------------------------------------ #
    # Cycle de vie
    # ------------------------------------------------------------------ #

    async def start(self):
        """Connexion + PRAGMA + démarrage du worker d'écriture."""
        self.db = await aiosqlite.connect(self.db_path)
        await self.db.execute("PRAGMA journal_mode=WAL;")
        await self.db.execute(f"PRAGMA busy_timeout={Config.DB_TIMEOUT};")
        await self.db.commit()
        self.worker_task = asyncio.create_task(self._write_worker())

    async def stop(self):
        """Attend que toutes les écritures en attente soient traitées, puis ferme la connexion."""
        await self.write_queue.join()
        if self.worker_task:
            self.worker_task.cancel()
        if self.db:
            await self.db.close()

    async def _write_worker(self):
        """Worker unique : dépile et exécute les écritures une par une."""
        while True:
            sql, params, future = await self.write_queue.get()
            try:
                async with self.db.execute(sql, params) as cursor:
                    await self.db.commit()
                    result = {"rowcount": cursor.rowcount, "lastrowid": cursor.lastrowid}
                    if not future.done():
                        future.set_result(result)
            except Exception as exc:
                if not future.done():
                    future.set_exception(exc)
            finally:
                self.write_queue.task_done()

    # ------------------------------------------------------------------ #
    # Primitives génériques
    # ------------------------------------------------------------------ #

    async def execute_write(self, sql: str, params: tuple = ()) -> dict:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self.write_queue.put((sql, params, future))
        return await future

    async def execute_read(self, sql: str, params: tuple = ()) -> list:
        async with self.db.execute(sql, params) as cursor:
            return await cursor.fetchall()

    # ------------------------------------------------------------------ #
    # Méthodes métier — évitent de disperser du SQL dans les autres modules
    # ------------------------------------------------------------------ #

    async def get_active_positions(self) -> list:
        """Utilisé au démarrage (recover_active_positions) et pour compter les places libres."""
        return await self.execute_read(
            "SELECT id, symbol, side FROM positions WHERE status IN ('active', 'hedged')"
        )

    async def get_waitlist(self) -> list:
        """Utilisé par W_selection pour rafraîchir les candidats déjà suivis."""
        return await self.execute_read(
            "SELECT symbol, openTime, side FROM waitlist"
        )

    async def replace_waitlist(self, rows: list[tuple]) -> None:
        """
        Remplace entièrement la waitlist.
        rows : liste de tuples (symbol, side, openTime, delta, variation, slope, score, adx)
        """
        await self.execute_write("DELETE FROM waitlist;")
        if not rows:
            return
        placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?)"] * len(rows))
        sql = (
            "INSERT INTO waitlist (symbol, side, openTime, delta, variation, slope, score, adx) "
            f"VALUES {placeholders};"
        )
        params = tuple(val for row in rows for val in row)
        await self.execute_write(sql, params)

    async def insert_pending_position(self, **fields) -> int:
        """
        Crée une ligne 'pending' avant l'ouverture réelle.
        Générique : passer uniquement les colonnes utiles, ex.
        insert_pending_position(symbol=..., side=..., status="pending", ...)
        """
        columns = ", ".join(fields.keys())
        placeholders = ", ".join(["?"] * len(fields))
        sql = f"INSERT INTO positions ({columns}) VALUES ({placeholders})"
        result = await self.execute_write(sql, tuple(fields.values()))
        return result["lastrowid"]

    async def update_position(self, id_pos: int, **fields) -> None:
        """Mise à jour ciblée pendant la gestion active (bid/ask, drawdown, pnl...)."""
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        sql = f"UPDATE positions SET {set_clause} WHERE id = ?"
        await self.execute_write(sql, tuple(fields.values()) + (id_pos,))

    async def close_position(self, id_pos: int, **fields) -> None:
        """Bascule une position en 'closed' (close, closeTime, gain, pnl, total_fees...)."""
        fields.setdefault("status", "closed")
        await self.update_position(id_pos, **fields)

    async def insert_hedge(self, **fields) -> int:
        """Optionnel — à utiliser si W_openHedgePos est implémentée."""
        columns = ", ".join(fields.keys())
        placeholders = ", ".join(["?"] * len(fields))
        sql = f"INSERT INTO hedges ({columns}) VALUES ({placeholders})"
        result = await self.execute_write(sql, tuple(fields.values()))
        return result["lastrowid"]

    async def insert_pending_order(self, **fields) -> int:
        """Crée une ligne 'filling' pour un ordre en cours de remplissage (VWAP progressif)."""
        columns = ", ".join(fields.keys())
        placeholders = ", ".join(["?"] * len(fields))
        sql = f"INSERT INTO pending_orders ({columns}) VALUES ({placeholders})"
        result = await self.execute_write(sql, tuple(fields.values()))
        return result["lastrowid"]

    async def update_pending_order(self, id_order: int, **fields) -> None:
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        sql = f"UPDATE pending_orders SET {set_clause} WHERE id = ?"
        await self.execute_write(sql, tuple(fields.values()) + (id_order,))

    async def get_pending_order(self, id_order: int) -> dict | None:
        async with self.db.execute("SELECT * FROM pending_orders WHERE id = ?", (id_order,)) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            columns = [d[0] for d in cursor.description]
            return dict(zip(columns, row))

    async def get_open_pending_orders(self) -> list[dict]:
        """Utilisé au démarrage pour détecter les remplissages interrompus par un crash/restart."""
        rows = await self.execute_read(
            "SELECT * FROM pending_orders WHERE status = 'filling'"
        )
        columns = ["id", "id_position", "id_hedge", "kind", "side", "target_size",
                   "filled_size", "vwap_price", "ref_price", "status", "started_at", "updated_at"]
        return [dict(zip(columns, row)) for row in rows]

    # db.py — à ajouter dans la classe Database
    async def get_position(self, id_pos: int) -> dict | None:
        """Retourne la ligne complète de la position id_pos sous forme de dict."""
        async with self.db.execute("SELECT * FROM positions WHERE id = ?", (id_pos,)) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            columns = [d[0] for d in cursor.description]
            return dict(zip(columns, row))

    async def get_hedge(self, id_hedge: int) -> dict | None:
        """Retourne la ligne complète du hedge id_hedge sous forme de dict."""
        async with self.db.execute("SELECT * FROM hedges WHERE id = ?", (id_hedge,)) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            columns = [d[0] for d in cursor.description]
            return dict(zip(columns, row))

    async def update_hedge(self, id_hedge: int, **fields) -> None:
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        sql = f"UPDATE hedges SET {set_clause} WHERE id = ?"
        await self.execute_write(sql, tuple(fields.values()) + (id_hedge,))

    async def log(self, level: str, message: str) -> None:
        """Appelée par DBLogHandler pour persister les logs dans la table 'logs'."""
        now_ms = int(time.time() * 1000)
        await self.execute_write(
            "INSERT INTO logs (log, Time) VALUES (?, ?)",
            (f"[{level}] {message}", now_ms),
        )

    async def purge_old_logs(self, max_age_days: int) -> None:
            """Supprime les entrées de 'logs' plus vieilles que max_age_days."""
            cutoff_ms = int(time.time() * 1000) - max_age_days * 86_400_000
            await self.execute_write("DELETE FROM logs WHERE Time < ?", (cutoff_ms,))

    async def purge_old_aborted_positions(self, max_age_days: int) -> None:
        """Supprime les positions 'aborted' plus vieilles que max_age_days.
        Ne touche jamais 'closed' (historique de trading réel, à conserver)."""
        cutoff_ms = int(time.time() * 1000) - max_age_days * 86_400_000
        await self.execute_write(
            "DELETE FROM positions WHERE status = 'aborted' AND closeTime < ?",
            (cutoff_ms,),
        )