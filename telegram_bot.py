import asyncio
import logging
import os
import json
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from decimal import Decimal
from datetime import datetime
import aiohttp
from aiohttp import web
from monitoring_scanner import Scanner
import asyncpg

# Настройка логирования с большей детализацией
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler('bot.log'),  # Сохраняем логи в файл
        logging.StreamHandler()          # Выводим в консоль
    ]
)
logger = logging.getLogger(__name__)

# Константы
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CMC_API_KEY = os.getenv("CMC_API_KEY")
ALLOWED_USERS = [
    (501156257, "Сергей"),
    (7009557842, "Мой лайф")
]
ADMIN_ID = 501156257
INTERVAL = 60
CONFIRMATION_INTERVAL = 20
CONFIRMATION_COUNT = 3

# Функции для работы с PostgreSQL
async def init_db():
    try:
        logger.debug("Инициализация подключения к базе данных")
        conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_levels (
                user_id BIGINT PRIMARY KEY,
                levels TEXT
            );
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id BIGINT PRIMARY KEY,
                stats TEXT
            );
        """)
        logger.info("База данных успешно инициализирована")
        return conn
    except Exception as e:
        logger.error(f"Ошибка инициализации базы данных: {str(e)}")
        raise

async def save_levels(user_id, levels):
    try:
        conn = await init_db()
        levels_str = json.dumps([str(level) for level in levels])
        await conn.execute(
            """
            INSERT INTO user_levels (user_id, levels)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET levels = $2
            """,
            user_id, levels_str
        )
        await conn.close()
        logger.debug(f"Уровни сохранены в БД для user_id={user_id}: {levels}")
    except Exception as e:
        logger.error(f"Ошибка сохранения уровней для user_id={user_id}: {str(e)}")
        raise

async def load_levels(user_id):
    try:
        conn = await init_db()
        result = await conn.fetchrow(
            "SELECT levels FROM user_levels WHERE user_id = $1",
            user_id
        )
        await conn.close()
        if result and result['levels']:
            levels = json.loads(result['levels'])
            if not levels:
                logger.debug(f"Пустой список уровней в БД для user_id={user_id}")
                return None
            loaded_levels = [Decimal(level) for level in levels]
            logger.debug(f"Уровни загружены из БД для user_id={user_id}: {loaded_levels}")
            return loaded_levels
        logger.debug(f"Уровни не найдены в БД для user_id={user_id}")
        return None
    except Exception as e:
        logger.error(f"Ошибка загрузки уровней для user_id={user_id}: {str(e)}")
        raise

async def save_stats(user_id, stats):
    try:
        conn = await init_db()
        stats_str = json.dumps(stats)
        await conn.execute(
            """
            INSERT INTO user_stats (user_id, stats)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET stats = $2
            """,
            user_id, stats_str
        )
        await conn.close()
        logger.debug(f"Статистика сохранена для user_id={user_id}")
    except Exception as e:
        logger.error(f"Ошибка сохранения статистики для user_id={user_id}: {str(e)}")
        raise

async def load_stats(user_id):
    try:
        conn = await init_db()
        result = await conn.fetchrow(
            "SELECT stats FROM user_stats WHERE user_id = $1",
            user_id
        )
        await conn.close()
        if result:
            logger.debug(f"Статистика загружена для user_id={user_id}")
            return json.loads(result['stats'])
        logger.debug(f"Статистика не найдена для user_id={user_id}")
        return None
    except Exception as e:
        logger.error(f"Ошибка загрузки статистики для user_id={user_id}: {str(e)}")
        raise

class BotState:
    def __init__(self, scanner):
        self.bot = Bot(token=TELEGRAM_TOKEN)
        self.dp = Dispatcher()
        self.scanner = scanner
        self.user_states = {}
        self.pending_commands = {}
        self.message_ids = {}
        self.l2_data_cache = None
        self.l2_data_time = None
        self.l2_data_cooldown = 300
        self.fear_greed_cache = None
        self.fear_greed_time = None
        self.fear_greed_cooldown = 300
        self.user_stats = {}
        logger.info("BotState успешно инициализирован")

    async def init_user_state(self, user_id):
        if user_id not in self.user_states:
            self.user_states[user_id] = {
                'prev_level': None,
                'last_measured_gas': None,
                'current_levels': [],
                'active_level': None,
                'confirmation_state': {'count': 0, 'values': [], 'target_level': None, 'direction': None},
                'notified_levels': set()
            }
            await self.load_or_set_default_levels(user_id)
            logger.debug(f"Состояние пользователя инициализировано для user_id={user_id}, current_levels={self.user_states[user_id]['current_levels']}")

    def init_user_stats(self, user_id):
        if user_id not in self.user_stats:
            self.user_stats[user_id] = {}
        today = datetime.now().date().isoformat()
        default_stats = {
            "Проверить газ": 0, "Manta Price": 0, "Сравнение L2": 0,
            "Задать уровни": 0, "Уведомления": 0, "Админ": 0, "Страх и Жадность": 0
        }
        if today not in self.user_stats[user_id]:
            self.user_stats[user_id][today] = default_stats.copy()
        else:
            for key in default_stats:
                if key not in self.user_stats[user_id][today]:
                    self.user_stats[user_id][today][key] = 0
        asyncio.create_task(self.save_user_stats(user_id))
        logger.debug(f"Статистика пользователя инициализирована для user_id={user_id}")

    async def check_access(self, message: types.Message):
        chat_id = message.chat.id
        logger.debug(f"Проверка доступа для chat_id={chat_id}")
        if chat_id not in [user[0] for user in ALLOWED_USERS]:
            try:
                await self.bot.send_message(chat_id, "⛽ У вас нет доступа к этому боту.")
                logger.warning(f"Доступ запрещён для chat_id={chat_id}")
            except Exception as e:
                logger.warning(f"Не удалось уведомить chat_id={chat_id}: {str(e)}")
            return False
        await self.init_user_state(chat_id)
        self.init_user_stats(chat_id)
        logger.debug(f"Доступ разрешён для chat_id={chat_id}")
        return True

    async def set_menu_button(self):
        try:
            await self.bot.set_chat_menu_button(menu_button=types.MenuButtonCommands())
            logger.info("Кнопка меню установлена на 'commands'")
        except Exception as e:
            logger.error(f"Ошибка установки кнопки меню: {str(e)}")

    async def load_or_set_default_levels(self, user_id):
        try:
            levels = await load_levels(user_id)
            if levels is None:
                levels = [
                    Decimal('0.010000'), Decimal('0.009500'), Decimal('0.009000'), Decimal('0.008500'),
                    Decimal('0.008000'), Decimal('0.007500'), Decimal('0.007000'), Decimal('0.006500'),
                    Decimal('0.006000'), Decimal('0.005500'), Decimal('0.005000'), Decimal('0.004500'),
                    Decimal('0.004000'), Decimal('0.003500'), Decimal('0.003000'), Decimal('0.002500'),
                    Decimal('0.002000'), Decimal('0.001500'), Decimal('0.001000'), Decimal('0.000900'),
                    Decimal('0.000800'), Decimal('0.000700'), Decimal('0.000600'), Decimal('0.000500'),
                    Decimal('0.000400'), Decimal('0.000300'), Decimal('0.000200'), Decimal('0.000100'),
                    Decimal('0.000050')
                ]
                await save_levels(user_id, levels)
                logger.info(f"Установлены уровни по умолчанию для user_id={user_id}: {levels}")
            self.user_states[user_id]['current_levels'] = levels
            self.user_states[user_id]['current_levels'].sort(reverse=True)
            logger.info(f"Уровни загружены для user_id={user_id}: {self.user_states[user_id]['current_levels']}")
        except Exception as e:
            logger.error(f"Ошибка загрузки уровней для user_id={user_id}: {str(e)}, установка уровней по умолчанию")
            levels = [
                Decimal('0.010000'), Decimal('0.009500'), Decimal('0.009000'), Decimal('0.008500'),
                Decimal('0.008000'), Decimal('0.007500'), Decimal('0.007000'), Decimal('0.006500'),
                Decimal('0.006000'), Decimal('0.005500'), Decimal('0.005000'), Decimal('0.004500'),
                Decimal('0.004000'), Decimal('0.003500'), Decimal('0.003000'), Decimal('0.002500'),
                Decimal('0.002000'), Decimal('0.001500'), Decimal('0.001000'), Decimal('0.000900'),
                Decimal('0.000800'), Decimal('0.000700'), Decimal('0.000600'), Decimal('0.000500'),
                Decimal('0.000400'), Decimal('0.000300'), Decimal('0.000200'), Decimal('0.000100'),
                Decimal('0.000050')
            ]
            self.user_states[user_id]['current_levels'] = levels
            await save_levels(user_id, self.user_states[user_id]['current_levels'])
            logger.info(f"Установлены уровни по умолчанию из-за ошибки для user_id={user_id}: {self.user_states[user_id]['current_levels']}")

    async def save_levels(self, user_id, levels):
        levels.sort(reverse=True)
        try:
            await save_levels(user_id, levels)
            self.user_states[user_id]['current_levels'] = levels
            logger.debug(f"Уровни сохранены для user_id={user_id}: {levels}")
        except Exception as e:
            logger.error(f"Ошибка сохранения уровней для user_id={user_id}: {str(e)}")
            raise

    async def load_user_stats(self, user_id):
        try:
            stats = await load_stats(user_id)
            if stats is None:
                stats = {}
            self.user_stats[user_id] = stats
            self.init_user_stats(user_id)
            logger.debug(f"Статистика пользователя загружена для user_id={user_id}")
        except Exception as e:
            logger.error(f"Ошибка загрузки статистики для user_id={user_id}: {str(e)}")
            self.init_user_stats(user_id)

    async def save_user_stats(self, user_id):
        try:
            await save_stats(user_id, self.user_stats[user_id])
            logger.debug(f"Статистика сохранена для user_id={user_id}")
        except Exception as e:
            logger.error(f"Ошибка сохранения статистики для user_id={user_id}: {str(e)}")

    async def update_message(self, chat_id, text, reply_markup=None):
        try:
            msg = await self.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=reply_markup)
            self.message_ids[chat_id] = msg.message_id
            logger.debug(f"Отправлено новое сообщение message_id={msg.message_id} для chat_id={chat_id}")
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения для chat_id={chat_id}: {str(e)}")
            raise

    async def confirm_level_crossing(self, chat_id, initial_value, direction, target_level):
        state = self.user_states[chat_id]['confirmation_state']
        state['count'] = 1
        state['values'] = [initial_value]
        state['target_level'] = target_level
        state['direction'] = direction
        logger.info(f"Начало подтверждения для chat_id={chat_id}: {initial_value:.6f} Gwei, направление: {direction}, цель: {target_level:.6f}")

        for i in range(CONFIRMATION_COUNT - 1):
            await asyncio.sleep(CONFIRMATION_INTERVAL)
            current_slow = await self.scanner.get_current_gas()
            if current_slow is None:
                logger.error(f"Не удалось получить газ на попытке {i + 2} для chat_id={chat_id}")
                state['count'] = 0
                state['values'] = []
                return
            state['count'] += 1
            state['values'].append(current_slow)
            logger.debug(f"Попытка {i + 2} для chat_id={chat_id}: {current_slow:.6f} Gwei")

        values = state['values']
        is_confirmed = False
        if direction == 'down' and all(v <= target_level for v in values):
            is_confirmed = True
        elif direction == 'up' and all(v >= target_level for v in values):
            is_confirmed = True

        if is_confirmed and target_level not in self.user_states[chat_id]['notified_levels']:
            last_measured = self.user_states[chat_id]['last_measured_gas']
            notification_message = (
                f"<pre>{'🟩' if direction == 'down' else '🟥'} ◆ ГАЗ {'УМЕНЬШИЛСЯ' if direction == 'down' else 'УВЕЛИЧИЛСЯ'} до: {values[-1]:.6f} Gwei\n"
                f"Уровень: {target_level:.6f} Gwei подтверждён</pre>"
            )
            await self.update_message(chat_id, notification_message, create_main_keyboard(chat_id))
            self.user_states[chat_id]['notified_levels'].add(target_level)
            self.user_states[chat_id]['active_level'] = target_level
            self.user_states[chat_id]['prev_level'] = last_measured
            logger.info(f"Уровень {target_level:.6f} подтверждён для chat_id={chat_id}, уведомление отправлено")

        state['count'] = 0
        state['values'] = []
        state['target_level'] = None
        state['direction'] = None

    async def get_manta_gas(self, chat_id, force_base_message=False):
        try:
            logger.debug(f"Получение данных о газе для chat_id={chat_id}")
            current_slow = await self.scanner.get_current_gas()
            if current_slow is None:
                await self.update_message(chat_id, "<b>⚠️ Не удалось подключиться к Manta Pacific</b>", create_main_keyboard(chat_id))
                logger.error(f"Не удалось получить данные о газе для chat_id={chat_id}")
                return

            logger.info(f"Газ для chat_id={chat_id}: Slow={current_slow:.6f}")
            gas_str = f"{current_slow:.6f}"
            decimal_part = gas_str.split('.')[1] if '.' in gas_str else ''
            leading_zeros = 0
            for char in decimal_part:
                if char == '0':
                    leading_zeros += 1
                else:
                    break
            zeros_text = f"({leading_zeros})"
            base_message = f"<pre>⛽️ Manta Pacific Gas\n◆ <b>ТЕКУЩИЙ ГАЗ</b>:   {current_slow:.6f} Gwei  {zeros_text}</pre>"

            self.user_states[chat_id]['last_measured_gas'] = current_slow
            prev_level = self.user_states[chat_id]['prev_level']
            levels = self.user_states[chat_id]['current_levels']
            confirmation_state = self.user_states[chat_id]['confirmation_state']

            if not levels:
                await self.load_or_set_default_levels(chat_id)
                levels = self.user_states[chat_id]['current_levels']

            if not levels:
                if force_base_message:
                    await self.update_message(chat_id, base_message + "\n\nУровни не заданы. Используйте 'Задать уровни'.", create_main_keyboard(chat_id))
                else:
                    logger.info(f"Уровни не заданы для chat_id={chat_id}, пропуск проверки уведомлений")
                self.user_states[chat_id]['prev_level'] = current_slow
                return

            if prev_level is None or force_base_message:
                await self.update_message(chat_id, base_message, create_main_keyboard(chat_id))
                self.user_states[chat_id]['prev_level'] = current_slow
            elif confirmation_state['count'] == 0:
                sorted_levels = sorted(levels)
                for level in sorted_levels:
                    if prev_level < level <= current_slow:
                        logger.info(f"Обнаружено пересечение вверх для chat_id={chat_id}: {level:.6f}")
                        asyncio.create_task(self.confirm_level_crossing(chat_id, current_slow, 'up', level))
                    elif prev_level > level >= current_slow:
                        logger.info(f"Обнаружено пересечение вниз для chat_id={chat_id}: {level:.6f}")
                        asyncio.create_task(self.confirm_level_crossing(chat_id, current_slow, 'down', level))

            self.user_states[chat_id]['active_level'] = min(levels, key=lambda x: abs(x - current_slow))

        except Exception as e:
            logger.error(f"Ошибка получения газа для chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

    async def fetch_l2_data(self):
        logger.debug("Получение данных L2 токенов")
        l2_tokens = {
            "MANTA": "manta-network",
            "Optimism": "optimism",
            "Arbitrum": "arbitrum",
            "Starknet": "starknet",
            "ZKsync": "zksync",
            "Scroll": "scroll",
            "Mantle": "mantle",
            "Taiko": "taiko"
        }
        token_data = {}

        current_time = datetime.now()
        if self.l2_data_time and (current_time - self.l2_data_time).total_seconds() < self.l2_data_cooldown and self.l2_data_cache:
            logger.debug("Использование кэшированных данных L2")
            return self.l2_data_cache

        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": ",".join(l2_tokens.values()),
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "24h,7d,30d"
        }

        async with aiohttp.ClientSession() as session:
            try:
                logger.debug(f"Отправка запроса к CoinGecko: {url}")
                async with session.get(url, params=params) as response:
                    logger.debug(f"Статус ответа CoinGecko: {response.status}")
                    if response.status != 200:
                        logger.error(f"Ошибка API CoinGecko: {response.status}")
                        return None
                    data = await response.json()
                    logger.debug("Данные CoinGecko успешно получены")

                    token_map = {coin["id"]: coin for coin in data}
                    for name, token_id in l2_tokens.items():
                        coin = token_map.get(token_id)
                        if coin:
                            price = coin.get("current_price", "Н/Д")
                            price_change_24h = coin.get("price_change_percentage_24h_in_currency", "Н/Д")
                            price_change_7d = coin.get("price_change_percentage_7d_in_currency", "Н/Д")
                            price_change_30d = coin.get("price_change_percentage_30d_in_currency", "Н/Д")
                            price_change_all = ((price - coin.get("ath", price)) / coin.get("ath", price) * 100) if price != "Н/Д" and coin.get("ath") else "Н/Д"
                            ath_price = coin.get("ath", "Н/Д")
                            ath_date = coin.get("ath_date", "Н/Д")
                            if ath_date != "Н/Д":
                                ath_date = datetime.strptime(ath_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y")
                            atl_price = coin.get("atl", "Н/Д")
                            atl_date = coin.get("atl_date", "Н/Д")
                            if atl_date != "Н/Д":
                                atl_date = datetime.strptime(atl_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y")
                            token_data[name] = {
                                "price": price,
                                "24h": price_change_24h if price_change_24h is not None else "Н/Д",
                                "7d": price_change_7d if price_change_7d is not None else "Н/Д",
                                "30d": price_change_30d if price_change_30d is not None else "Н/Д",
                                "all": price_change_all,
                                "ath_price": ath_price,
                                "ath_date": ath_date,
                                "atl_price": atl_price,
                                "atl_date": atl_date
                            }
                        else:
                            token_data[name] = {
                                "price": "Н/Д", "24h": "Н/Д", "7d": "Н/Д", "30d": "Н/Д", "all": "Н/Д",
                                "ath_price": "Н/Д", "ath_date": "Н/Д", "atl_price": "Н/Д", "atl_date": "Н/Д"
                            }
            except Exception as e:
                logger.error(f"Ошибка получения данных L2: {str(e)}")
                return None

        self.l2_data_cache = token_data
        self.l2_data_time = current_time
        logger.debug("Данные L2 сохранены в кэш")
        return token_data

    async def fetch_fear_greed(self):
        logger.debug("Получение данных Fear & Greed")
        current_time = datetime.now()
        if self.fear_greed_time and (current_time - self.fear_greed_time).total_seconds() < self.fear_greed_cooldown and self.fear_greed_cache:
            logger.debug("Использование кэшированных данных Fear & Greed")
            return self.fear_greed_cache

        url = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical"
        headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
        params = {"limit": 30}

        async with aiohttp.ClientSession() as session:
            try:
                logger.debug(f"Отправка запроса к CMC Fear & Greed: {url}")
                async with session.get(url, headers=headers, params=params) as response:
                    logger.debug(f"Статус ответа CMC Fear & Greed: {response.status}")
                    if response.status != 200:
                        logger.error(f"Ошибка API CMC Fear & Greed: {response.status}")
                        return None
                    data = await response.json()
                    if "data" not in data or not data["data"]:
                        logger.error("Данные Fear & Greed не возвращены API")
                        return None

                    fg_data = data["data"]
                    current = fg_data[0]
                    current_value = int(current["value"])
                    current_category = current["value_classification"]

                    yesterday = fg_data[1]
                    yesterday_value = int(yesterday["value"])
                    yesterday_category = yesterday["value_classification"]

                    week_ago = fg_data[7] if len(fg_data) > 7 else fg_data[-1]
                    week_ago_value = int(week_ago["value"])
                    week_ago_category = week_ago["value_classification"]

                    month_ago = fg_data[-1]
                    month_ago_value = int(month_ago["value"])
                    month_ago_category = month_ago["value_classification"]

                    year_data = fg_data[:365] if len(fg_data) > 365 else fg_data
                    year_values = [(int(d["value"]), d["timestamp"], d["value_classification"]) for d in year_data]
                    max_year = max(year_values, key=lambda x: x[0])
                    min_year = min(year_values, key=lambda x: x[0])
                    max_year_value, max_year_date, max_year_category = max_year
                    min_year_value, min_year_date, min_year_category = min_year

                    def parse_timestamp(ts):
                        try:
                            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y")
                        except ValueError:
                            return datetime.fromtimestamp(int(ts)).strftime("%d.%m.%Y")

                    max_year_date = parse_timestamp(max_year_date)
                    min_year_date = parse_timestamp(min_year_date)

                    fear_greed_data = {
                        "current": {"value": current_value, "category": current_category},
                        "yesterday": {"value": yesterday_value, "category": yesterday_category},
                        "week_ago": {"value": week_ago_value, "category": week_ago_category},
                        "month_ago": {"value": month_ago_value, "category": month_ago_category},
                        "year_max": {"value": max_year_value, "date": max_year_date, "category": max_year_category},
                        "year_min": {"value": min_year_value, "date": min_year_date, "category": min_year_category}
                    }

                    self.fear_greed_cache = fear_greed_data
                    self.fear_greed_time = current_time
                    logger.debug("Данные Fear & Greed сохранены в кэш")
                    return fear_greed_data
            except Exception as e:
                logger.error(f"Ошибка получения данных Fear & Greed: {str(e)}")
                return None

    async def get_manta_price(self, chat_id):
        try:
            logger.debug(f"Получение цены MANTA для chat_id={chat_id}")
            token_data = await self.fetch_l2_data()
            if not token_data:
                await self.update_message(chat_id, "⚠️ Не удалось получить данные от CoinGecko.", create_main_keyboard(chat_id))
                logger.error(f"Не удалось получить данные L2 для chat_id={chat_id}")
                return

            manta_data = token_data["MANTA"]
            price = manta_data["price"]
            price_change_24h = manta_data["24h"]
            price_change_7d = manta_data["7d"]
            price_change_30d = manta_data["30d"]
            price_change_all = manta_data["all"]
            ath_price = manta_data["ath_price"]
            ath_date = manta_data["ath_date"]
            atl_price = manta_data["atl_price"]
            atl_date = manta_data["atl_date"]

            spot_volume = await self.scanner.get_manta_spot_volume()
            futures_volume = await self.scanner.get_manta_futures_volume()
            spot_volume_m = round(spot_volume / Decimal('1000000')) if spot_volume else 0
            futures_volume_m = round(futures_volume / Decimal('1000000')) if futures_volume else 0
            spot_volume_str = f"{spot_volume_m}M$" if spot_volume else "Н/Д"
            futures_volume_str = f"{futures_volume_m}M$" if futures_volume else "Н/Д"

            message = (
                f"<pre>"
                f"🦎 Данные с CoinGecko:\n"
                f"◆ MANTA/USDT: ${float(price):.3f}\n\n"
                f"◆ ИЗМЕНЕНИЕ:\n"
                f"◆ 24 ЧАСА:     {float(price_change_24h):>6.2f}%\n"
                f"◆ 7 ДНЕЙ:      {float(price_change_7d):>6.2f}%\n"
                f"◆ МЕСЯЦ:       {float(price_change_30d):>6.2f}%\n"
                f"◆ ВСЕ ВРЕМЯ:   {float(price_change_all):>6.2f}%\n\n"
                f"◆ Binance Volume Trade 24ч:\n"
                f"◆ (Futures):   {futures_volume_str}\n"
                f"◆ (Spot):      {spot_volume_str}\n\n"
                f"◆ ${float(ath_price):.2f} ({ath_date})\n"
                f"◆ ${float(atl_price):.2f} ({atl_date})"
                f"</pre>"
            )
            await self.update_message(chat_id, message, create_main_keyboard(chat_id))
            logger.debug(f"Цена MANTA отправлена для chat_id={chat_id}")

        except Exception as e:
            logger.error(f"Ошибка получения цены для chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

    async def get_l2_comparison(self, chat_id):
        try:
            logger.debug(f"Получение сравнения L2 для chat_id={chat_id}")
            token_data = await self.fetch_l2_data()
            if not token_data:
                await self.update_message(chat_id, "⚠️ Не удалось получить данные от CoinGecko.", create_main_keyboard(chat_id))
                logger.error(f"Не удалось получить данные L2 для chat_id={chat_id}")
                return

            message = (
                f"<pre>"
                f"🦎 Данные с CoinGecko:\n"
                f"◆ Сравнение L2 токенов (24 часа):\n"
            )
            sorted_by_24h = sorted(
                token_data.items(),
                key=lambda x: float(x[1]["24h"]) if x[1]["24h"] not in ("Н/Д", None) else float('-inf'),
                reverse=True
            )
            for name, data in sorted_by_24h:
                price_str = f"${float(data['price']):.3f}" if data['price'] not in ("Н/Д", None) else "Н/Д"
                change_str = f"{float(data['24h']):>6.2f}%" if data['24h'] not in ("Н/Д", None) else "Н/Д"
                message += f"◆ {name:<9}: {price_str} | {change_str}\n"

            message += "\n◆ Сравнение L2 токенов (7 дней):\n"
            sorted_by_7d = sorted(
                token_data.items(),
                key=lambda x: float(x[1]["7d"]) if x[1]["7d"] not in ("Н/Д", None) else float('-inf'),
                reverse=True
            )
            for name, data in sorted_by_7d:
                price_str = f"${float(data['price']):.3f}" if data['price'] not in ("Н/Д", None) else "Н/Д"
                change_str = f"{float(data['7d']):>6.2f}%" if data['7d'] not in ("Н/Д", None) else "Н/Д"
                message += f"◆ {name:<9}: {price_str} | {change_str}\n"

            message += "\n◆ Сравнение L2 токенов (месяц):\n"
            sorted_by_30d = sorted(
                token_data.items(),
                key=lambda x: float(x[1]["30d"]) if x[1]["30d"] not in ("Н/Д", None) else float('-inf'),
                reverse=True
            )
            for name, data in sorted_by_30d:
                price_str = f"${float(data['price']):.3f}" if data['price'] not in ("Н/Д", None) else "Н/Д"
                change_str = f"{float(data['30d']):>6.2f}%" if data['30d'] not in ("Н/Д", None) else "Н/Д"
                message += f"◆ {name:<9}: {price_str} | {change_str}\n"

            message += "\n◆ Сравнение L2 токенов (все время):\n"
            sorted_by_all = sorted(
                token_data.items(),
                key=lambda x: float(x[1]["all"]) if x[1]["all"] not in ("Н/Д", None) else float('-inf'),
                reverse=True
            )
            for name, data in sorted_by_all:
                price_str = f"${float(data['price']):.3f}" if data['price'] not in ("Н/Д", None) else "Н/Д"
                change_str = f"{float(data['all']):>6.2f}%" if data['all'] not in ("Н/Д", None) else "Н/Д"
                message += f"◆ {name:<9}: {price_str} | {change_str}\n"
            message += "</pre>"

            await self.update_message(chat_id, message, create_main_keyboard(chat_id))
            logger.debug(f"Сравнение L2 отправлено для chat_id={chat_id}")

        except Exception as e:
            logger.error(f"Ошибка получения сравнения L2 для chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

    async def get_fear_greed(self, chat_id):
        try:
            logger.debug(f"Получение Fear & Greed для chat_id={chat_id}")
            fg_data = await self.fetch_fear_greed()
            if not fg_data:
                await self.update_message(chat_id, "⚠️ Не удалось получить данные Fear & Greed от CoinMarketCap.", create_main_keyboard(chat_id))
                logger.error(f"Не удалось получить данные Fear & Greed для chat_id={chat_id}")
                return

            current_value = fg_data["current"]["value"]
            yesterday_value = fg_data["yesterday"]["value"]
            week_ago_value = fg_data["week_ago"]["value"]
            month_ago_value = fg_data["month_ago"]["value"]
            max_year_value = fg_data["year_max"]["value"]
            max_year_date = fg_data["year_max"]["date"]
            min_year_value = fg_data["year_min"]["value"]
            min_year_date = fg_data["year_min"]["date"]

            bar_length = 20
            filled = int(current_value / 100 * bar_length)
            progress_bar = f"🔴 {'█' * filled}{'▁' * (bar_length - filled)} 🟢"

            message = (
                f"<pre>"
                f"◆ Индекс страха и жадности: {current_value}\n"
                f"\n"
                f"{progress_bar}\n"
                f"\n"
                f"История:\n"
                f"🕒 Вчера: {yesterday_value}\n"
                f"🕒 Прошлая неделя: {week_ago_value}\n"
                f"🕒 Прошлый месяц: {month_ago_value}\n"
                f"\n"
                f"Годовые экстремумы:\n"
                f"📈 Макс: {max_year_value} ({max_year_date})\n"
                f"📉 Мин: {min_year_value} ({min_year_date})"
                f"</pre>"
            )

            await self.update_message(chat_id, message, create_main_keyboard(chat_id))
            logger.debug(f"Данные Fear & Greed отправлены для chat_id={chat_id}")

        except Exception as e:
            logger.error(f"Ошибка получения Fear & Greed для chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

    async def get_admin_stats(self, chat_id):
        if chat_id != ADMIN_ID:
            return
        logger.debug(f"Получение статистики админа для chat_id={chat_id}")
        today = datetime.now().date().isoformat()
        message = "<b>Статистика использования бота за сегодня:</b>\n\n<pre>"
        has_activity = False
        for user_id, user_name in ALLOWED_USERS:
            if user_id == ADMIN_ID:
                continue
            await self.load_user_stats(user_id)
            stats = self.user_stats[user_id].get(today, {})
            if any(stats.values()):
                message += f"{user_id} {user_name}\n"
                for action, count in stats.items():
                    if count > 0:
                        message += f"{action} - {count}\n"
                message += "\n"
                has_activity = True
        message += "</pre>"
        if not has_activity:
            message = "<b>Статистика использования бота за сегодня:</b>\n\nСегодня никто из пользователей (кроме админа) не использовал бота."
        await self.update_message(chat_id, message, create_main_keyboard(chat_id))
        logger.debug(f"Статистика админа отправлена для chat_id={chat_id}")

def create_main_keyboard(chat_id):
    keyboard = [
        [types.KeyboardButton(text="Проверить газ"), types.KeyboardButton(text="Страх и Жадность")],
        [types.KeyboardButton(text="Manta Price"), types.KeyboardButton(text="Сравнение L2")],
        [types.KeyboardButton(text="Задать уровни"), types.KeyboardButton(text="Уведомления")]
    ]
    if chat_id == ADMIN_ID:
        keyboard.append([types.KeyboardButton(text="Админ")])
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def create_levels_menu_keyboard():
    keyboard = [
        [types.KeyboardButton(text="0.00001–0.01")],
        [types.KeyboardButton(text="Удалить уровни")],
        [types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")]
    ]
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def create_level_input_keyboard():
    keyboard = [
        [types.KeyboardButton(text="Добавить еще уровень")],
        [types.KeyboardButton(text="Завершить")],
        [types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")]
    ]
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def create_delete_levels_keyboard(levels):
    keyboard = [[types.KeyboardButton(text=f"Удалить {level:.5f} Gwei")] for level in levels]
    keyboard.append([types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")])
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

scanner = Scanner()
state = BotState(scanner)

@state.dp.message(Command("start"))
async def start_command(message: types.Message):
    if not await state.check_access(message):
        return
    chat_id = message.chat.id
    logger.info(f"Получена команда /start от chat_id={chat_id}")
    await state.update_message(chat_id, "<b>Бот для Manta Pacific запущен.</b>\nВыберите действие:", create_main_keyboard(chat_id))
    try:
        await message.delete()
        logger.debug(f"Сообщение /start удалено, message_id={message.message_id}")
    except Exception as e:
        logger.error(f"Ошибка удаления сообщения /start message_id={message.message_id}: {str(e)}")

@state.dp.message(lambda message: message.text in ["Проверить газ", "Manta Price", "Сравнение L2", "Страх и Жадность", "Задать уровни", "Уведомления", "Админ"])
async def handle_main_button(message: types.Message):
    if not await state.check_access(message):
        return
    chat_id = message.chat.id
    text = message.text
    logger.debug(f"Нажата кнопка: {text} для chat_id={chat_id}")

    today = datetime.now().date().isoformat()
    state.user_stats[chat_id][today][text] += 1
    await state.save_user_stats(chat_id)

    if chat_id in state.pending_commands and text != "Задать уровни":
        del state.pending_commands[chat_id]
        logger.debug(f"Очищена команда в ожидании для chat_id={chat_id}")

    if text == "Проверить газ":
        await state.get_manta_gas(chat_id, force_base_message=True)
    elif text == "Manta Price":
        await state.get_manta_price(chat_id)
    elif text == "Сравнение L2":
        await state.get_l2_comparison(chat_id)
    elif text == "Страх и Жадность":
        await state.get_fear_greed(chat_id)
    elif text == "Задать уровни":
        state.pending_commands[chat_id] = {'step': 'range_selection'}
        await state.update_message(chat_id, "Выберите действие для уровней уведомлений:", create_levels_menu_keyboard())
    elif text == "Уведомления":
        current_levels = state.user_states[chat_id]['current_levels']
        logger.debug(f"Уровни уведомлений для chat_id={chat_id}: {current_levels}")
        if current_levels:
            levels_text = "\n".join([f"◆ {level:.6f} Gwei" for level in current_levels])
            formatted_message = f"<b><pre>ТЕКУЩИЕ УВЕДОМЛЕНИЯ:\n\n{levels_text}</pre></b>"
            await state.update_message(chat_id, formatted_message, create_main_keyboard(chat_id))
        else:
            await state.update_message(chat_id, "Уровни не установлены.", create_main_keyboard(chat_id))
    elif text == "Админ" and chat_id == ADMIN_ID:
        await state.get_admin_stats(chat_id)

    try:
        await message.delete()
        logger.debug(f"Сообщение пользователя удалено, message_id={message.message_id}")
    except Exception as e:
        logger.error(f"Ошибка удаления сообщения message_id={message.message_id}: {str(e)}")

@state.dp.message()
async def process_value(message: types.Message):
    if not await state.check_access(message):
        return
    chat_id = message.chat.id
    text = message.text.strip()
    logger.debug(f"Получено сообщение: {text} от chat_id={chat_id}")

    if chat_id not in state.pending_commands:
        await state.update_message(chat_id, "Выберите действие с помощью кнопок.", create_main_keyboard(chat_id))
    else:
        state_data = state.pending_commands[chat_id]

        if state_data['step'] == 'range_selection':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Действие отменено.", create_main_keyboard(chat_id))
                logger.debug(f"Действие отменено для chat_id={chat_id}")
            elif text == "Назад":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Возврат в главное меню.", create_main_keyboard(chat_id))
                logger.debug(f"Возврат в главное меню для chat_id={chat_id}")
            elif text == "0.00001–0.01":
                min_val, max_val = 0.00001, 0.01
                state_data['range'] = (min_val, max_val)
                state_data['levels'] = state.user_states[chat_id]['current_levels'].copy()
                state_data['step'] = 'level_input'
                await state.update_message(chat_id, "Введите уровень от 0,00001 до 0,01:", create_level_input_keyboard())
                logger.debug(f"Выбран диапазон 0.00001–0.01 для chat_id={chat_id}")
            elif text == "Удалить уровни":
                if not state.user_states[chat_id]['current_levels']:
                    await state.update_message(chat_id, "Уровни не установлены.", create_main_keyboard(chat_id))
                    del state.pending_commands[chat_id]
                    logger.debug(f"Уровни не установлены, отмена для chat_id={chat_id}")
                else:
                    state_data['step'] = 'delete_level_selection'
                    await state.update_message(chat_id, "Выберите уровень для удаления:", create_delete_levels_keyboard(state.user_states[chat_id]['current_levels']))
                    logger.debug(f"Переход к удалению уровней для chat_id={chat_id}")
            else:
                await state.update_message(chat_id, "Выберите действие из предложенных.", create_levels_menu_keyboard())
                logger.debug(f"Некорректное действие для chat_id={chat_id}")

        elif state_data['step'] == 'level_input':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Действие отменено.", create_main_keyboard(chat_id))
                logger.debug(f"Действие отменено для chat_id={chat_id}")
            elif text == "Назад":
 Interpolated Response:
                state_data['step'] = 'range_selection'
                await state.update_message(chat_id, "Возврат к выбору диапазона.", create_levels_menu_keyboard())
                logger.debug(f"Возврат к выбору диапазона для chat_id={chat_id}")
            elif text == "Добавить еще уровень" or text == "Завершить":
                await state.update_message(chat_id, "Сначала введите уровень.", create_level_input_keyboard())
                logger.debug(f"Попытка завершить или добавить без уровня для chat_id={chat_id}")
            else:
                try:
                    text_normalized = text.replace(',', '.')
                    level = Decimal(text_normalized)
                    min_val, max_val = state_data['range']
                    if not (min_val <= float(level) <= max_val):
                        await state.update_message(chat_id, f"Ошибка: введите значение в диапазоне {min_val}–{max_val}", create_level_input_keyboard())
                        logger.error(f"Уровень вне диапазона для chat_id={chat_id}: {text}")
                        return

                    if level not in state_data['levels']:
                        state_data['levels'].append(level)
                        await state.save_levels(chat_id, state_data['levels'])
                    if len(state_data['levels']) >= 100:
                        del state.pending_commands[chat_id]
                        await state.update_message(chat_id, "Достигнут лимит в 100 уровней. Уровни сохранены.", create_main_keyboard(chat_id))
                        logger.info(f"Достигнут лимит уровней для chat_id={chat_id}")
                    else:
                        state_data['step'] = 'level_choice'
                        await state.update_message(chat_id, f"Уровень {level:.6f} добавлен. Что дальше?", create_level_input_keyboard())
                        logger.debug(f"Уровень {level:.6f} добавлен для chat_id={chat_id}")
                except ValueError:
                    await state.update_message(chat_id, "Ошибка: введите корректное число (используйте точку или запятую)", create_level_input_keyboard())
                    logger.error(f"Некорректный ввод уровня для chat_id={chat_id}: {text}")

        elif state_data['step'] == 'level_choice':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Действие отменено.", create_main_keyboard(chat_id))
                logger.debug(f"Действие отменено для chat_id={chat_id}")
            elif text == "Назад":
                state_data['step'] = 'level_input'
                min_val, max_val = state_data['range']
                await state.update_message(chat_id, f"Введите уровень от {min_val} до {max_val}:", create_level_input_keyboard())
                logger.debug(f"Возврат к вводу уровня для chat_id={chat_id}")
            elif text == "Добавить еще уровень":
                state_data['step'] = 'level_input'
                min_val, max_val = state_data['range']
                await state.update_message(chat_id, f"Введите следующий уровень (в пределах {min_val}–{max_val}):", create_level_input_keyboard())
                logger.debug(f"Добавление нового уровня для chat_id={chat_id}")
            elif text == "Завершить":
                await state.save_levels(chat_id, state_data['levels'])
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Уровни сохранены.", create_main_keyboard(chat_id))
                logger.info(f"Уровни сохранены и действие завершено для chat_id={chat_id}")
            else:
                try:
                    text_normalized = text.replace(',', '.')
                    level = Decimal(text_normalized)
                    min_val, max_val = state_data['range']
                    if not (min_val <= float(level) <= max_val):
                        await state.update_message(chat_id, f"Ошибка: введите значение в диапазоне {min_val}–{max_val}", create_level_input_keyboard())
                        logger.error(f"Уровень вне диапазона для chat_id={chat_id}: {text}")
                        return

                    if level not in state_data['levels']:
                        state_data['levels'].append(level)
                        await state.save_levels(chat_id, state_data['levels'])
                    if len(state_data['levels']) >= 100:
                        del state.pending_commands[chat_id]
                        await state.update_message(chat_id, "Достигнут лимит в 100 уровней. Уровни сохранены.", create_main_keyboard(chat_id))
                        logger.info(f"Достигнут лимит уровней для chat_id={chat_id}")
                    else:
                        state_data['step'] = 'level_choice'
                        await state.update_message(chat_id, f"Уровень {level:.6f} добавлен. Что дальше?", create_level_input_keyboard())
                        logger.debug(f"Уровень {level:.6f} добавлен для chat_id={chat_id}")
                except ValueError:
                    await state.update_message(chat_id, "Ошибка: введите корректное число (используйте точку или запятую)", create_level_input_keyboard())
                    logger.error(f"Некорректный ввод уровня для chat_id={chat_id}: {text}")

        elif state_data['step'] == 'delete_level_selection':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Удаление уровней отменено.", create_main_keyboard(chat_id))
                logger.debug(f"Удаление уровней отменено для chat_id={chat_id}")
            elif text == "Назад":
                state_data['step'] = 'range_selection'
                await state.update_message(chat_id, "Возврат к выбору действия.", create_levels_menu_keyboard())
                logger.debug(f"Возврат к выбору действия для chat_id={chat_id}")
            elif text.startswith("Удалить "):
                level_str = text.replace("Удалить ", "").replace(" Gwei", "")
                try:
                    level_to_delete = Decimal(level_str)
                    if level_to_delete in state.user_states[chat_id]['current_levels']:
                        state.user_states[chat_id]['current_levels'].remove(level_to_delete)
                        await state.save_levels(chat_id, state.user_states[chat_id]['current_levels'])
                        del state.pending_commands[chat_id]
                        await state.update_message(chat_id, f"Уровень {level_to_delete:.6f} Gwei удалён.", create_main_keyboard(chat_id))
                        logger.info(f"Уровень {level_to_delete:.6f} удалён для chat_id={chat_id}")
                    else:
                        await state.update_message(chat_id, "Уровень не найден.", create_main_keyboard(chat_id))
                        del state.pending_commands[chat_id]
                        logger.error(f"Уровень не найден для удаления для chat_id={chat_id}: {level_str}")
                except ValueError:
                    await state.update_message(chat_id, "Ошибка при удалении уровня.", create_delete_levels_keyboard(state.user_states[chat_id]['current_levels']))
                    logger.error(f"Ошибка удаления уровня для chat_id={chat_id}: {level_str}")
            else:
                await state.update_message(chat_id, "Выберите уровень для удаления.", create_delete_levels_keyboard(state.user_states[chat_id]['current_levels']))
                logger.debug(f"Некорректный выбор уровня для удаления для chat_id={chat_id}")

    try:
        await message.delete()
        logger.debug(f"Сообщение пользователя удалено, message_id={message.message_id}")
    except Exception as e:
        logger.error(f"Ошибка удаления сообщения message_id={message.message_id}: {str(e)}")

async def monitor_gas_callback(gas_value):
    logger.debug(f"Получено значение газа: {gas_value:.6f} Gwei")
    for user_id, _ in ALLOWED_USERS:
        await state.init_user_state(user_id)
        state.init_user_stats(user_id)
        try:
            await asyncio.sleep(1)
            await state.get_manta_gas(user_id)
            logger.debug(f"Обработано значение газа для user_id={user_id}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка для user_id={user_id}: {str(e)}")

async def health_check_task():
    """Периодическая проверка состояния бота"""
    while True:
        logger.info("Бот активен, проверка состояния выполнена")
        await asyncio.sleep(300)  # Проверка каждые 5 минут

async def main():
    logger.info("Начало инициализации бота")
    try:
        await state.set_menu_button()
        for user_id, _ in ALLOWED_USERS:
            await state.init_user_state(user_id)
            state.init_user_stats(user_id)
            await state.load_user_stats(user_id)
            logger.debug(f"Пользователь user_id={user_id} инициализирован")

        # Добавляем HTTP-сервер для Render
        app = web.Application()
        async def health_check(request):
            logger.debug("Получен запрос проверки состояния HTTP")
            return web.Response(text="OK")
        app.router.add_get('/', health_check)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8000)
        await site.start()
        logger.info("HTTP-сервер запущен на порту 8000")

        # Задержка для завершения старых сессий
        await asyncio.sleep(5)
        logger.debug("Задержка для завершения старых сессий завершена")

        # Запускаем задачу проверки состояния
        asyncio.create_task(health_check_task())
        logger.info("Задача проверки состояния бота запущена")

        try:
            await asyncio.gather(
                state.dp.start_polling(state.bot),
                scanner.monitor_gas(INTERVAL, monitor_gas_callback),
                return_exceptions=True
            )
        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {str(e)}")
            raise
        finally:
            logger.info("Основной цикл завершён, начало очистки")
            await runner.cleanup()
            logger.info("HTTP-сервер остановлен")
            await state.bot.session.close()
            logger.info("Сессия Telegram бота закрыта")
            await scanner.close()
            logger.info("Сканнер закрыт")

    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {str(e)}")
        raise
    finally:
        logger.info("Бот полностью остановлен")

if __name__ == "__main__":
    try:
        logger.info("Запуск основного цикла бота")
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"Критическая ошибка при выполнении: {str(e)}")
    finally:
        logger.info("Программа завершена")