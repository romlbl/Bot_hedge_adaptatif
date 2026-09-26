import asyncio
import logging

from bot import Bot

# Handler console : reste actif en plus du DBLogHandler branché dans Bot.run().
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == "__main__":
    try:
        bot = Bot()
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logging.info("Arrêt du programme par l'utilisateur.")