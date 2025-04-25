import asyncio
import logging
from web3 import Web3
from web3.providers import HTTPProvider
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
        # Создаём aiohttp сессию с таймаутом
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        # Инициализируем Web3 с кастомной сессией
        self.web3 = Web3(HTTPProvider(RPC_URL, session=self.session))
        self.last_price_data = None
        self.last_price_time = None
        self.price_cooldown = 10  # Секунд между запросами цены
        logger.info("Scanner initialized with custom HTTP session (timeout=10s)")

    async def close(self):
        """Закрытие сессии при завершении работы"""
        try:
            await self.session.close()
            logger.info("HTTP session closed")
        except Exception as e:
            logger.error(f"Error closing HTTP session: {str(e)}")

    async def get_current_gas(self):
        """Получение текущего значения газа"""
        try:
            # Проверяем подключение к Manta Pacific
            if not self.web3.is_connected():
                logger.error("Не удалось подключиться к Manta Pacific")
                return None

            block_count = 1
            newest_block = "latest"
            reward_percentiles = [25, 50, 75]
            logger.debug("Fetching fee history from Manta Pacific")
            fee_history = self.web3.eth.fee_history(block_count, newest_block, reward_percentiles)
            base_fee_per_gas = fee_history["baseFeePerGas"][-1]
            base_fee_gwei = Decimal(self.web3.from_wei(base_fee_per_gas, 'gwei'))
            priority_fee_gwei = [Decimal(self.web3.from_wei(fee, 'gwei')) for fee in fee_history["reward"][0]]
            max_fee_slow = base_fee_gwei + priority_fee_gwei[0]
            logger.debug(
                f"Gas fetched: base_fee={base_fee_gwei:.6f} Gwei, priority_fee={priority_fee_gwei[0]:.6f} Gwei, max_fee_slow={max_fee_slow:.6f} Gwei")
            return max_fee_slow

        except aiohttp.ClientError as e:
            logger.error(f"Network error while fetching gas: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching gas: {str(e)}")
            return None

    async def get_manta_price_and_changes(self):
        """Получение текущей цены MANTA/USDT и изменений"""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                logger.debug("Fetching price from Binance API")
                async with session.get(BINANCE_API_URL) as binance_resp:
                    if binance_resp.status != 200:
                        logger.error(f"Binance API вернул ошибку: {binance_resp.status}")
                        return None, None, None, None, None, None, None
                    binance_data = await binance_resp.json()
                    price = Decimal(binance_data['lastPrice'])
                    price_change_24h = Decimal(binance_data['priceChangePercent'])

                logger.debug("Fetching 30-day price data from CoinGecko API")
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
                    price_change_7d = (
                                (current_price - price_7d_ago) / price_7d_ago * 100) if price_7d_ago != 0 else Decimal(
                        '0')
                    price_change_30d = ((
                                                    current_price - price_30d_ago) / price_30d_ago * 100) if price_30d_ago != 0 else Decimal(
                        '0')

                logger.debug("Fetching all-time price data from CoinGecko API")
                async with session.get(COINGECKO_API_URL_ALL) as coingecko_all_resp:
                    if coingecko_all_resp.status == 429:
                        logger.error("Превышен лимит запросов CoinGecko (все время)")
                        return None, None, None, None, None, None, None
                    if coingecko_all_resp.status != 200:
                        logger.error(f"CoinGecko all time API вернул ошибку: {coingecko_all_resp.status}")
                        return price, price_change_24h, price_change_7d, price_change_30d, None, None, None
                    coingecko_all_data = await coingecko_all_resp.json()
                    ath_price = Decimal(str(coingecko_all_data['market_data']['ath']['usd']))
                    ath_date = datetime.strptime(coingecko_all_data['market_data']['ath_date']['usd'],
                                                 "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y")
                    atl_price = Decimal(str(coingecko_all_data['market_data']['atl']['usd']))
                    atl_date = datetime.strptime(coingecko_all_data['market_data']['atl_date']['usd'],
                                                 "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y")
                    price_change_all = ((current_price - ath_price) / ath_price * 100) if ath_price != 0 else Decimal(
                        '0')

                    logger.info(
                        f"Цена MANTA/USDT: {price}, 24ч: {price_change_24h}%, 7д: {price_change_7d}%, 30д: {price_change_30d}%, Все время: {price_change_all}%, ATH: {ath_price} ({ath_date}), ATL: {atl_price} ({atl_date})")
                    return price, price_change_24h, price_change_7d, price_change_30d, price_change_all, (
                    ath_price, ath_date), (atl_price, atl_date)

        except aiohttp.ClientError as e:
            logger.error(f"Network error while fetching Manta price: {str(e)}")
            return None, None, None, None, None, None, None
        except Exception as e:
            logger.error(f"Unexpected error while fetching Manta price: {str(e)}")
            return None, None, None, None, None, None, None

    async def get_price(self, ticker):
        """Получение текущей цены токена по тикеру через Binance API"""
        try:
            url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={ticker}"
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                logger.debug(f"Fetching price for {ticker} from Binance API")
                async with session.get(url) as resp:
                    if resp.status != 200:
                        logger.error(f"Binance API вернул ошибку для {ticker}: {resp.status}")
                        return None
                    data = await resp.json()
                    price = Decimal(data['lastPrice'])
                    logger.info(f"Цена {ticker}: {price}")
                    return price
        except aiohttp.ClientError as e:
            logger.error(f"Network error while fetching price for {ticker}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching price for {ticker}: {str(e)}")
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

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                # Binance для текущей цены и 24ч
                logger.debug(f"Fetching price for {ticker} from Binance API")
                url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={ticker}"
                async with session.get(url) as binance_resp:
                    if binance_resp.status != 200:
                        logger.error(f"Binance API вернул ошибку для {ticker}: {binance_resp.status}")
                        return None, None, None, None, None
                    binance_data = await binance_resp.json()
                    price = Decimal(binance_data['lastPrice'])
                    price_change_24h = Decimal(binance_data['priceChangePercent'])

                # CoinGecko для 7д, 30д и все время
                logger.debug(f"Fetching 30-day price data for {ticker} from CoinGecko API")
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
                    price_change_7d = (
                                (current_price - price_7d_ago) / price_7d_ago * 100) if price_7d_ago != 0 else Decimal(
                        '0')
                    price_change_30d = ((
                                                    current_price - price_30d_ago) / price_30d_ago * 100) if price_30d_ago != 0 else Decimal(
                        '0')

                logger.debug(f"Fetching all-time price data for {ticker} from CoinGecko API")
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
                    price_change_all = ((current_price - ath_price) / ath_price * 100) if ath_price != 0 else Decimal(
                        '0')

                    logger.info(
                        f"Цена {ticker}: {price}, 24ч: {price_change_24h}%, 7д: {price_change_7d}%, 30д: {price_change_30d}%, Все время: {price_change_all}%")
                    return price, price_change_24h, price_change_7d, price_change_30d, price_change_all

        except aiohttp.ClientError as e:
            logger.error(f"Network error while fetching price data for {ticker}: {str(e)}")
            return None, None, None, None, None
        except Exception as e:
            logger.error(f"Unexpected error while fetching price data for {ticker}: {str(e)}")
            return None, None, None, None, None

    async def get_manta_spot_volume(self):
        """Получение 24-часового объема торгов MANTA/USDT на споте"""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                logger.debug("Fetching spot volume from Binance API")
                async with session.get(BINANCE_API_URL) as resp:
                    if resp.status != 200:
                        logger.error(f"Binance Spot API вернул ошибку: {resp.status}")
                        return None
                    data = await resp.json()
                    volume = Decimal(data['quoteVolume'])
                    logger.info(f"24-часовой объем торгов MANTA/USDT на споте: {volume:.2f} USDT")
                    return volume
        except aiohttp.ClientError as e:
            logger.error(f"Network error while fetching Manta spot volume: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching Manta spot volume: {str(e)}")
            return None

    async def get_manta_futures_volume(self):
        """Получение 24-часового объема торгов MANTA/USDT на фьючерсах"""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                logger.debug("Fetching futures volume from Binance Futures API")
                async with session.get(BINANCE_FUTURES_API_URL) as resp:
                    if resp.status != 200:
                        logger.error(f"Binance Futures API вернул ошибку: {resp.status}")
                        return None
                    data = await resp.json()
                    volume = Decimal(data['quoteVolume'])
                    logger.info(f"24-часовой объем торгов MANTA/USDT на фьючерсах: {volume:.2f} USDT")
                    return volume
        except aiohttp.ClientError as e:
            logger.error(f"Network error while fetching Manta futures volume: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching Manta futures volume: {str(e)}")
            return None

    async def monitor_gas(self, interval, callback):
        """Мониторинг газа с вызовом callback для передачи данных"""
        while True:
            try:
                logger.info("Starting gas value check...")
                gas_value = await self.get_current_gas()
                if gas_value is not None:
                    await callback(gas_value)
                else:
                    logger.warning("Gas value is None, skipping callback")
            except Exception as e:
                logger.error(f"Error in monitor_gas loop: {str(e)}")
            await asyncio.sleep(interval)