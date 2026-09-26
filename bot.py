import asyncio
import logging
import time

import ccxt.pro as ccxtpro

from config import Config
from db import Database
from logger import DBLogHandler
from position_manager import PositionManager
from waitlist_manager import WaitlistManager


class Bot:
    """
    Orchestrateur principal : instancie l'exchange, la base de données et les
    deux managers (PositionManager, WaitlistManager), gère la boucle horaire
    et l'arrêt propre de toutes les ressources.
    """

    def __init__(self):
        self.exchange = ccxtpro.bybit({
            'enableRateLimit': True,
            'timeout': 10000,
        })
        self.db = Database(Config.DB_PATH)
        self.position_manager = PositionManager(self.exchange, self.db)
        self.waitlist_manager = WaitlistManager(self.exchange, self.db, self.position_manager)
        self.log_handler: DBLogHandler | None = None
        self._last_log_purge = 0

    async def run(self):
        await self.db.start()

        # Tous les logging.info/error(...) du reste du code partent désormais
        # aussi vers la table 'logs', en plus de la console (cf. main.py).
        self.log_handler = DBLogHandler(self.db)
        logging.getLogger().addHandler(self.log_handler)
        self.log_handler.start()

        logging.info("Bot démarré et connecté à la base de données.")

        # Reprise après redémarrage/crash, dans cet ordre précis :
        # 1) nettoie les remplissages orphelins interrompus en plein vol
        # 2) retente les positions jamais confirmées 'active'
        # 3) relance la gestion des positions déjà actives/hedgées
        await self.position_manager.recover_stale_pending_orders()
        await self.position_manager.recover_pending_positions()
        await self.position_manager.recover_active_positions()

        try:
            while True:
                await self.waitlist_manager.W_selection()
                await self._maybe_purge_logs()
                await asyncio.sleep(Config.MANAGER_CALL_SLEEP)
        finally:
            await self.shutdown()

    async def _maybe_purge_logs(self):
        now_ms = int(time.time() * 1000)
        if now_ms - self._last_log_purge >= 86_400_000:  # 1x/jour
            await self.db.purge_old_logs(Config.LOG_RETENTION_DAYS)
            await self.db.purge_old_aborted_positions(Config.ABORTED_POSITION_RETENTION_DAYS)
            self._last_log_purge = now_ms
            logging.info(
                f"Logs de plus de {Config.LOG_RETENTION_DAYS} jours et positions 'aborted' de plus "
                f"de {Config.ABORTED_POSITION_RETENTION_DAYS} jours purgés."
            )

    async def shutdown(self):
        logging.info("Arrêt du bot : nettoyage des connexions...")

        tasks = list(self.position_manager.tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if self.log_handler:
            await self.log_handler.stop()

        await self.db.stop()
        await self.exchange.close()
