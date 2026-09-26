import asyncio
import logging

from db import Database


class DBLogHandler(logging.Handler):
    """
    Handler de logging qui persiste chaque log dans la table 'logs' via Database.

    - emit() est appelée de façon SYNCHRONE par le module logging (potentiellement
      depuis n'importe quel contexte). Elle ne fait jamais d'I/O elle-même : elle
      formate le message et le pousse dans une asyncio.Queue de façon thread-safe.
    - _consume() est une tâche de fond qui vide cette queue et appelle db.log(),
      seul endroit où l'écriture réelle a lieu.

    Ça permet de garder tous les logging.info(...) / logging.error(...) déjà
    présents dans le reste du code SANS RIEN CHANGER à leurs appels : il suffit
    d'attacher ce handler au logger racine une fois au démarrage.
    """

    def __init__(self, db: Database, loop: asyncio.AbstractEventLoop | None = None):
        super().__init__()
        self.db = db
        self.loop = loop or asyncio.get_event_loop()
        self.queue: asyncio.Queue = asyncio.Queue()
        self._consumer_task: asyncio.Task | None = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            self.loop.call_soon_threadsafe(
                self.queue.put_nowait, (record.levelname, message)
            )
        except Exception:
            self.handleError(record)

    def start(self) -> asyncio.Task:
        """Démarre la tâche de fond qui vide la queue vers la base. À appeler une seule fois,
        depuis la boucle asyncio en cours (typiquement dans Bot.run(), juste après db.start())."""
        self._consumer_task = asyncio.create_task(self._consume())
        return self._consumer_task

    async def stop(self) -> None:
        """Arrête proprement la tâche de fond. À appeler avant db.stop()."""
        if self._consumer_task:
            self._consumer_task.cancel()

    async def _consume(self) -> None:
        while True:
            level, message = await self.queue.get()
            try:
                await self.db.log(level, message)
            except Exception:
                pass  # un handler de logging ne doit jamais lever d'exception
            finally:
                self.queue.task_done()