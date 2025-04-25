import asyncio
import logging
from web3 import AsyncWeb3, AsyncHTTPProvider
from decimal import Decimal
import aiohttp
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)

# Константы
RPC_URL = "https://pacific-rpc.manta.network/http"
BINANCE_API_URL = "https://api.binance.com/api/v3/ticker/24hr?symbol=MANTAUSDT"
BINANCE_FUTURES_API_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=MANTAUSDT"
COINGECKO_API_URL_30D = "https://api.coingecko.com/api/v3/coins/manta-network/market_chart?vs_currency=usd&days=30&interval=daily"
COINGECKO_API_URL_ALL = "https://api.coingecko.com/api/v3/coins/manta-network?localization=false&tickers=false&market_data=true"

class Scanner:
    def __init__(self):
        self.web3 = AsyncWeb3(AsyncHTTPProvider(RPC_URL))
        self.last_price_data = None
        self.last_price_time = None
        self.price_cooldown = 10  # Секунд между запросами цены
        logger.info("Scanner initialized with AsyncWeb3")

    async def get_current_gas(self):
        """Получение текущего значения газа через fee_history"""
        try:
            if not await self.web3.is_connected():
                logger.error("Не удалось подключиться к Manta Pacific")
                return None
            block_count = 1
            newest_block = "latest"
            reward_percentiles = [25, 50, 75]
            fee_history = await self.web3.eth.fee_history(block_count, newest_block, reward_percentiles)
            base_fee_per_gas = fee_history["baseFeePerGas"][-1]
            base_fee_gwei = Decimal(base_fee_per_gas) / Decimal('1000000000')  # Преобразуем wei в Gwei
            priority_fee_gwei = [Decimal(fee) / Decimal('1000000000') for fee in fee_history["reward"][0]]  # Преобразуем приоритетные комиссии в Gwei
            max_fee_slow = base_fee_gwei + priority_fee_gwei[0]  # Используем 25-й перцентиль для "медленной" транзакции
            logger.info(f"Current gas price: {max_fee_slow:.6f} Gwei (base: {base_fee_gwei:.6f}, priority: {priority_fee_gwei[0]:.6f})")
            return max_fee_slow
        except Exception as e:
            logger.error(f"Ошибка при получении газа: {str(e)}")
            return None

    async def get_manta_price_and_changes(self):
        """Получение текущей цены MANTA/USDT и изменений"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(BINANCE_API_URL) as binance_resp:
                    if binance_resp.status != 200:
                        logger.error(f"Binance API вернул ошибку: {binance_resp.status}")
                        return None, None, None, None, None, None, None
                    binance_data = await binance_resp.json()
                    price = Decimal(binance_data['lastPrice'])
                    price_change_24h = Decimal(binance_data['priceChangePercent'])

                async with session.get(COINGECKO_API_URL_30D) as coingecko_30d_resp:
                    if coingecko_30d_resp.status == 429:
                        logger.error("Превышен лимит запросов CoinGecko (30 дней)")
                        return None, None, None, None, None, None, None
                    if coingecko_30d_resp.status != 200:
                        logger.error(f"CoinGecko 30d API вернул ошибку: {coingecko_30d_resp.status}")
                        return price, price_change_24h, None, None, None, None, None
                    coingecko_30d_data = await coingecko_30d_resp.json()
                    prices_30d = coingecko_30d_data['prices']
                    current_price = Decimal(str(prices_30d[-1][1]))
                    price_7d_ago = Decimal(str(prices_30d[-8][1])) if len(prices_30d) >= 8 else current_price
                    price_30d_ago = Decimal(str(prices_30d[0][1])) if prices_30d else current_price
                    price_change_7d = ((current_price - price_7d_ago) / price_7d_ago * 100) if price_7d_ago != 0 else Decimal('0')
                    price_change_30d = ((current_price - price_30d_ago) / price_30d_ago * 100) if price_30d_ago != 0 else Decimal('0')

                async with session.get(COINGECKO_API_URL_ALL) as coingecko_all_resp:
                    if coingecko_all_resp.status == 429:
                        logger.error("Превышен лимит запросов CoinGecko (все время)")
                        return None, None, None, None, None, None, None
                    if coingecko_all_resp.status != 200:
                        logger.error(f"CoinGecko all time API вернул ошибку: {coingecko_all_resp.status}")
                        return price, price_change_24h, price_change_7d, price_change_30d, None, None, None
                    coingecko_all_data = await coingecko_all_resp.json()
                    ath_price = Decimal(str(coingecko_all_data['market_data']['ath']['usd']))
                    ath_date = datetime.strptime(coingecko_all_data['market_data']['ath_date']['usd'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y")
                    atl_price = Decimal(str(coingecko_all_data['market_data']['atl']['usd']))
                    atl_date = datetime.strptime(coingecko_all_data['market_data']['atl_date']['usd'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y")
                    price_change_all = ((current_price - ath_price) / ath_price * 100) if ath_price != 0 else Decimal('0')

                    logger.info(f"Цена MANTA/USDT: {price}, 24ч: {price_change_24h}%, 7д: {price_change_7d}%, 30д: {price_change_30d}%, Все время: {price_change_all}%, ATH: {ath_price} ({ath_date}), ATL: {atl_price} ({atl_date})")
                    return price, price_change_24h, price_change_7d, price_change_30d, price_change_all, (ath_price, ath_date), (atl_price, atl_date)

        except Exception as e:
            logger.error(f"Ошибка при получении цены Manta: {str(e)}")
            return None, None, None, None, None, None, None

    async def get_price(self, ticker):
        """Получение текущей цены токена по тикеру через Binance API"""
        try:
            url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={ticker}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        logger.error(f"Binance API вернул ошибку для {ticker}: {resp.status}")
                        return None
                    data = await resp.json()
                    price = Decimal(data['lastPrice'])
                    logger.info(f"Цена {ticker}: {price}")
                    return price
        except Exception as e:
            logger.error(f"Ошибка при получении цены {ticker}: {str(e)}")
            return None

    async def get_price_and_changes(self, ticker):
        """Получение цены и изменений для любого токена"""
        try:
            # Маппинг тикеров на ID CoinGecko
            coingecko_ids = {
                "MANTAUSDT": "manta-network",
                "OPUSDT": "optimism",
                "ARBUSDT": "arbitrum",
                "STRKUSDT": "starknet",
                "ZKUSDT": "zksync",
                "SCRUSDT": "scroll"
            }
            coingecko_id = coingecko_ids.get(ticker, "")

            async with aiohttp.ClientSession() as session:
                # Binance для текущей цены и 24ч
                url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={ticker}"
                async with session.get(url) as binance_resp:
                    if binance_resp.status != 200:
                        logger.error(f"Binance API вернул ошибку для {ticker}: {binance_resp.status}")
                        return None, None, None, None, None
                    binance_data = await binance_resp.json()
                    price = Decimal(binance_data['lastPrice'])
                    price_change_24h = Decimal(binance_data['priceChangePercent'])

                # CoinGecko для 7д, 30д и все время
                coingecko_30d_url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart?vs_currency=usd&days=30&interval=daily"
                async with session.get(coingecko_30d_url) as coingecko_30d_resp:
                    if coingecko_30d_resp.status == 429:
                        logger.error(f"Превышен лимит запросов CoinGecko (30 дней) для {ticker}")
                        return price, price_change_24h, None, None, None
                    if coingecko_30d_resp.status != 200:
                        logger.error(f"CoinGecko 30d API вернул ошибку для {ticker}: {coingecko_30d_resp.status}")
                        return price, price_change_24h, None, None, None
                    coingecko_30d_data = await coingecko_30d_resp.json()
                    prices_30d = coingecko_30d_data['prices']
                    current_price = Decimal(str(prices_30d[-1][1]))
                    price_7d_ago = Decimal(str(prices_30d[-8][1])) if len(prices_30d) >= 8 else current_price
                    price_30d_ago = Decimal(str(prices_30d[0][1])) if prices_30d else current_price
                    price_change_7d = ((current_price - price_7d_ago) / price_7d_ago * 100) if price_7d_ago != 0 else Decimal('0')
                    price_change_30d = ((current_price - price_30d_ago) / price_30d_ago * 100) if price_30d_ago != 0 else Decimal('0')

                coingecko_all_url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}?localization=false&tickers=false&market_data=true"
                async with session.get(coingecko_all_url) as coingecko_all_resp:
                    if coingecko_all_resp.status == 429:
                        logger.error(f"Превышен лимит запросов CoinGecko (все время) для {ticker}")
                        return price, price_change_24h, price_change_7d, price_change_30d, None
                    if coingecko_all_resp.status != 200:
                        logger.error(f"CoinGecko all time API вернул ошибку для {ticker}: {coingecko_all_resp.status}")
                        return price, price_change_24h, price_change_7d, price_change_30d, None
                    coingecko_all_data = await coingecko_all_resp.json()
                    ath_price = Decimal(str(coingecko_all_data['market_data']['ath']['usd']))
                    price_change_all = ((current_price - ath_price) / ath_price * 100) if ath_price != 0 else Decimal('0')

                    logger.info(f"Цена {ticker}: {price}, 24ч: {price_change_24h}%, 7д: {price_change_7d}%, 30д: {price_change_30d}%, Все время: {price_change_all}%")
                    return price, price_change_24h, price_change_7d, price_change_30d, price_change_all

        except Exception as e:
            logger.error(f"Ошибка при получении данных для {ticker}: {str(e)}")
            return None, None, None, None, None

    async def get_manta_spot_volume(self):
        """Получение 24-часового объема торгов MANTA/USDT на споте"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(BINANCE_API_URL) as resp:
                    if resp.status != 200:
                        logger.error(f"Binance Spot API вернул ошибку: {resp.status}")
                        return None
                    data = await resp.json()
                    volume = Decimal(data['quoteVolume'])
                    logger.info(f"24-часовой объем торгов MANTA/USDT на споте: {volume:.2f} USDT")
                    return volume
        except Exception as e:
            logger.error(f"Ошибка при получении объема спота MANTA: {str(e)}")
            return None

    async def get_manta_futures_volume(self):
        """Получение 24-часового объема торгов MANTA/USDT на фьючерсах"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(BINANCE_FUTURES_API_URL) as resp:
                    if resp.status != 200:
                        logger.error(f"Binance Futures API вернул ошибку: {resp.status}")
                        return None
                    data = await resp.json()
                    volume = Decimal(data['quoteVolume'])
                    logger.info(f"24-часовой объем торгов MANTA/USDT на фьючерсах: {volume:.2f} USDT")
                    return volume
        except Exception as e:
            logger.error(f"Ошибка при получении объема фьючерсов MANTA: {str(e)}")
            return None

    async def monitor_gas(self, interval, callback):
        """Мониторинг газа с вызовом callback для передачи данных"""
        while True:
            logger.info("Starting gas value check...")
            gas_value = await self.get_current_gas()
            if gas_value is not None:
                await callback(gas_value)
            await asyncio.sleep(interval)

    async def close(self):
        """Закрытие соединений"""
        try:
            await self.web3.provider.session.close()
            logger.info("Web3 session closed")
        except Exception as e:
            logger.error(f"Error closing Web3 session: {str(e)}")