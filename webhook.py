import asyncio
import logging
import os
from datetime import datetime
import pytz
from telegram_bot import BotState, Scanner, main as bot_main
from fastapi import FastAPI
import uvicorn

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)

app = FastAPI()

RESTART_TIMES = ["21:00"]

async def schedule_restart():
    kyiv_tz = pytz.timezone('Europe/Kyiv')
    while True:
        try:
            now = datetime.now(kyiv_tz)
            current_time = now.strftime("%H:%M")
            logger.debug(f"Checking restart schedule: current_time={current_time}, RESTART_TIMES={RESTART_TIMES}")
            if current_time in RESTART_TIMES:
                logger.info(f"Scheduled restart triggered at {current_time}")
                os._exit(0)
            else:
                logger.debug(f"No restart needed at {current_time}")
        except Exception as e:
            logger.error(f"Error in schedule_restart: {str(e)}")
        await asyncio.sleep(60)

@app.on_event("startup")
async def startup_event():
    logger.info("Starting up FastAPI application")
    scanner = Scanner()
    await scanner.init_session()
    global state
    state = BotState(scanner)
    asyncio.create_task(schedule_restart())
    asyncio.create_task(bot_main())

@app.get("/")
async def root():
    return {"message": "Manta Gas Bot is running"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)