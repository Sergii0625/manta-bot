import asyncio
import logging
from web3 import AsyncWeb3, AsyncHTTPProvider
from decimal import Decimal

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)

class Scanner:
    def __init__(self):
        # Используем AsyncWeb3 вместо Web3 для асинхронной работы
        self.w3 = AsyncWeb3(AsyncHTTPProvider('https://pacific-rpc.manta.network/http'))
        logger.info("Scanner initialized with AsyncWeb3")

    async def close(self):
        # AsyncWeb3 автоматически управляет сессией, закрывать не нужно
        logger.info("Scanner closed")

    async def get_current_gas(self):
        try:
            gas_price = await self.w3.eth.gas_price
            gas_price_gwei = self.w3.from_wei(gas_price, 'gwei')
            return Decimal(str(gas_price_gwei))
        except Exception as e:
            logger.error(f"Unexpected error while fetching gas: {str(e)}")
            return None

    async def get_manta_spot_volume(self):
        # Заглушка для получения спотового объема
        return None

    async def get_manta_futures_volume(self):
        # Заглушка для получения фьючерсного объема
        return None

    async def monitor_gas(self, interval, callback):
        while True:
            logger.info("Starting gas value check...")
            gas_value = await self.get_current_gas()
            if gas_value is not None:
                await callback(gas_value)
            else:
                logger.warning("Gas value is None, skipping callback")
            await asyncio.sleep(interval)