# -*- coding: utf-8 -*-
import time, logging, ccxt, json, os, sqlite3
from contextlib import closing
from typing import Optional, Dict, List, Any
from threading import Thread, Lock, Event
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters, CallbackQueryHandler

# =======================
#   ЛОГИРОВАНИЕ
# =======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PositionBot")
for n in ('ccxt', 'urllib3', 'telegram', 'httpx'):
    logging.getLogger(n).setLevel(logging.WARNING)

# =======================
#   TELEGRAM
# =======================
TG_TOKEN = "8371689586:AAHmkpP-x_Z4eeuqeKV0-yCShu1nKhncW3s"
ADMIN_ID = 6024660648

# Главное меню кнопки
BTN_COINS = "📊 Монеты"
BTN_ADD_COIN = "➕ Добавить монету"
BTN_REMOVE_COIN = "❌ Удалить монету"
BTN_GLOBAL_STATUS = "🌍 Глобальный статус"

# Кнопки монеты
BTN_PRESETS = "🛠️ Пресеты"
BTN_BACK = "⬅ Назад"
BTN_APPLY = "✅ Применить"
BTN_COIN = "🪙 Монета"
BTN_STEP = "📐 Шаг, %"
BTN_FIRST = "🎯 Первичный вход, USDT"
BTN_ORDER = "📦 Размер ордера, USDT"
BTN_LEV = "🪜 Установить плечо"
BTN_INITDEP = "💰 Начальный депозит"
BTN_PARAMS = "📊 Показать параметры"
BTN_TRIGGER = "🎯 Триггерная цена"
BTN_COIN_START = "⚪ Старт монеты"
BTN_COIN_STOP = "⚪ Стоп монеты"
BTN_COIN_REPORT = "📈 Отчёт монеты"

# Кнопка режима (отдельная «большая» сверху)
MODE_LONG_LABEL = "🟩 Long 🟩"
MODE_SHORT_LABEL = "🟥 Short 🟥"

# Кнопка «Адаптивность»
ADAPT_ON_LABEL = "🟩 Адаптивность"
ADAPT_OFF_LABEL = "Адаптивность"

# =======================
#   МУЛЬТИ-МОНЕТА КОНФИГ
# =======================
GLOBAL_LOCK = Lock()
COINS_CONFIG = {}  # {coin_symbol: CoinConfig}
ACTIVE_COINS = {}  # {coin_symbol: CoinTrader}
MAX_COINS = 5
PENDING = {}  # {user_id: {"action": str, "coin": str, "param": str}}
PRESET_WAIT = {}  # {user_id: {"coin": str, "preset": str}}
CURRENT_COIN_MENU = {}  # {user_id: coin_symbol} - текущая монета в меню пользователя

# Глобальные настройки
GLOBAL_INITIAL_DEPOSIT = 0.0  # Глобальный начальный депозит для всех монет


class CoinConfig:
    def __init__(self, coin_symbol: str):
        self.coin = coin_symbol
        self.leverage = 10
        self.step_percentage = 0.3
        self.first_volume_usdt = 80.0
        self.order_volume_usdt = 80.0
        self.trade_mode = "LONG"  # LONG | SHORT
        self.adaptive_enabled = False
        self.adaptive_volume_increment_pct = 2.0
        self.adaptive_step_increment_per3_pct = 0.1
        self.trigger_price = 0.0
        self.trigger_waiting = False
        self.trigger_last_notification = 0.0
        self.is_running = False

    def to_dict(self) -> dict:
        return {
            "coin": self.coin,
            "leverage": self.leverage,
            "step_percentage": self.step_percentage,
            "first_volume_usdt": self.first_volume_usdt,
            "order_volume_usdt": self.order_volume_usdt,
            "trade_mode": self.trade_mode,
            "adaptive_enabled": self.adaptive_enabled,
            "adaptive_volume_increment_pct": self.adaptive_volume_increment_pct,
            "adaptive_step_increment_per3_pct": self.adaptive_step_increment_per3_pct,
            "trigger_price": self.trigger_price,
            "trigger_waiting": self.trigger_waiting,
            "trigger_last_notification": self.trigger_last_notification,
            "is_running": self.is_running
        }

    def from_dict(self, data: dict):
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)


# Пресеты: ПЛЕЧО 10 для всех, проценты от (депозит × плечо), есть поле desc (заполняете вручную)
PRESETS = {
    "① Гипер стабильность": {"lev": 10, "step": 0.60, "first_pct": 0.02, "order_pct": 0.02, "desc": ""},
    "② Стабильность": {"lev": 10, "step": 0.60, "first_pct": 0.03, "order_pct": 0.03, "desc": ""},
    "③ Нормал": {"lev": 10, "step": 0.50, "first_pct": 0.04, "order_pct": 0.04, "desc": ""},
    "④ Умеренно рисковый": {"lev": 10, "step": 0.50, "first_pct": 0.05, "order_pct": 0.05, "desc": ""},
    "⑤ Рисковый": {"lev": 10, "step": 0.50, "first_pct": 0.06, "order_pct": 0.06, "desc": ""},
    "⑥ Разгон депо": {"lev": 10, "step": 0.40, "first_pct": 0.07, "order_pct": 0.07, "desc": ""},
}

# ---------- Индикаторы (пресеты + динамическая клавиатура) ----------
DECORATED_TO_KEY = {}
BOT_MANAGER = None


class BotManager:
    """Менеджер для управления несколькими торговыми ботами"""

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.coins_traders = {}  # {coin_symbol: CoinTrader}
        self.lock = Lock()

        # Глобальная просадка портфеля
        self.global_max_drawdown_percent = 0.0
        try:
            saved_drawdown = STORE.get_config_one("global_max_drawdown_percent")
            if saved_drawdown:
                self.global_max_drawdown_percent = float(saved_drawdown)
        except:
            self.global_max_drawdown_percent = 0.0

    def add_coin(self, coin_symbol: str) -> bool:
        """Добавить новую монету для торговли"""
        with self.lock:
            if len(self.coins_traders) >= MAX_COINS:
                return False
            if coin_symbol in self.coins_traders:
                return False

            # Создаем конфиг для новой монеты
            config = CoinConfig(coin_symbol)
            COINS_CONFIG[coin_symbol] = config

            # Создаем торговца для монеты
            trader = CoinTrader(self.api_key, self.api_secret, coin_symbol, config)
            self.coins_traders[coin_symbol] = trader
            ACTIVE_COINS[coin_symbol] = trader

            # Сохраняем конфиг
            self._save_coin_config(coin_symbol)

            logger.info(f"✅ Добавлена монета {coin_symbol}. Всего активных: {len(self.coins_traders)}")
            return True

    def remove_coin(self, coin_symbol: str) -> bool:
        """Удалить монету из торговли"""
        with self.lock:
            if coin_symbol not in self.coins_traders:
                return False

            # Останавливаем торговца
            trader = self.coins_traders[coin_symbol]
            trader.pause()

            # Удаляем из всех структур
            del self.coins_traders[coin_symbol]
            del ACTIVE_COINS[coin_symbol]
            if coin_symbol in COINS_CONFIG:
                del COINS_CONFIG[coin_symbol]

            # Удаляем конфиг из БД
            self._delete_coin_config(coin_symbol)

            logger.info(f"❌ Удалена монета {coin_symbol}. Осталось активных: {len(self.coins_traders)}")
            return True

    def get_coin_trader(self, coin_symbol: str) -> Optional['CoinTrader']:
        """Получить торговца для монеты"""
        return self.coins_traders.get(coin_symbol)

    def get_active_coins(self) -> List[str]:
        """Получить список активных монет"""
        return list(self.coins_traders.keys())

    def start_coin(self, coin_symbol: str) -> bool:
        """Запустить торговлю для конкретной монеты"""
        trader = self.get_coin_trader(coin_symbol)
        if trader:
            trader.resume()
            return True
        return False

    def stop_coin(self, coin_symbol: str) -> bool:
        """Остановить торговлю для конкретной монеты"""
        trader = self.get_coin_trader(coin_symbol)
        if trader:
            trader.pause()
            return True
        return False

    def is_coin_running(self, coin_symbol: str) -> bool:
        """Проверить запущена ли торговля для монеты"""
        trader = self.get_coin_trader(coin_symbol)
        if trader:
            try:
                return trader._run_event.is_set()
            except:
                return False
        return False

    def get_global_status(self) -> str:
        """Получить глобальный статус всех монет"""
        if not self.coins_traders:
            return "🔍 Нет активных монет\n\nДобавьте монету через '➕ Добавить монету'"

        status_lines = ["🌍 Глобальный статус:\n"]
        for coin_symbol, trader in self.coins_traders.items():
            running = self.is_coin_running(coin_symbol)
            status = "🟢 Работает" if running else "🔴 Остановлен"

            try:
                price = trader.get_current_price() or 0.0
                pos_size = trader.position_size

                # Показываем направление только если есть открытая позиция
                direction = ""
                if abs(pos_size) > 0:
                    config = COINS_CONFIG.get(coin_symbol)
                    if config:
                        direction = " | 📈 Long" if config.trade_mode == "LONG" else " | 📉 Short"

                pos_info = f"Поз: {pos_size:.4f}" if abs(pos_size) > 0 else "Flat"
                status_lines.append(f"• {coin_symbol}: {status}{direction} | {price:.2f} | {pos_info}")
            except:
                status_lines.append(f"• {coin_symbol}: {status} | Цена: —")

        return "\n".join(status_lines)

    def _save_coin_config(self, coin_symbol: str):
        """Сохранить конфиг монеты в БД"""
        if coin_symbol in COINS_CONFIG:
            try:
                config_data = COINS_CONFIG[coin_symbol].to_dict()
                STORE.set_config_one(f"coin_{coin_symbol}", config_data)
            except Exception as e:
                logger.warning(f"Ошибка сохранения конфига для {coin_symbol}: {e}")

    def _delete_coin_config(self, coin_symbol: str):
        """Удалить конфиг монеты из БД"""
        try:
            # Удаляем все ключи связанные с монетой
            keys_to_delete = [f"coin_{coin_symbol}"]
            with DB_LOCK, sqlite3.connect(STORE.path) as cn:
                cn.execute("DELETE FROM config WHERE k = ?", (f"coin_{coin_symbol}",))
                cn.commit()
        except Exception as e:
            logger.warning(f"Ошибка удаления конфига для {coin_symbol}: {e}")

    def load_coins_from_db(self):
        """Загрузить монеты из БД"""
        try:
            all_configs = STORE.get_config_all()
            for key, config_data in all_configs.items():
                if key.startswith("coin_"):
                    coin_symbol = key[5:]  # убираем префикс "coin_"
                    if isinstance(config_data, dict) and coin_symbol:
                        config = CoinConfig(coin_symbol)
                        config.from_dict(config_data)
                        COINS_CONFIG[coin_symbol] = config

                        # Создаем торговца
                        trader = CoinTrader(self.api_key, self.api_secret, coin_symbol, config)
                        self.coins_traders[coin_symbol] = trader
                        ACTIVE_COINS[coin_symbol] = trader

                        logger.info(f"📂 Загружена монета {coin_symbol} из БД")
        except Exception as e:
            logger.warning(f"Ошибка загрузки монет из БД: {e}")

    def format_portfolio_report(self) -> str:
        """Создать общий отчет портфеля"""
        if not self.coins_traders:
            return "🔍 Портфель пуст\n\nДобавьте монету через '➕ Добавить монету'"

        # Получаем общую информацию о балансе
        try:
            # Берем первого трейдера для получения общего баланса
            first_trader = next(iter(self.coins_traders.values()))
            balance_info = first_trader._fetch_equity_snapshot()
            equity = balance_info.get('equity', 0.0)
            available = balance_info.get('available', 0.0)
            used_pct = balance_info.get('used_pct', 0.0)
        except:
            equity = 0.0
            available = 0.0
            used_pct = 0.0

        # Подсчитываем статистику только по запущенным монетам
        active_traders = {}
        for coin, trader in self.coins_traders.items():
            if self.is_coin_running(coin):
                active_traders[coin] = trader

        coin_lines = []
        long_count = 0
        short_count = 0
        total_unrealized_pnl = 0.0

        for coin_symbol, trader in active_traders.items():
            try:
                config = COINS_CONFIG.get(coin_symbol)
                if not config:
                    continue

                running = self.is_coin_running(coin_symbol)
                status_icon = "🟢" if running else "🔴"

                # Получаем данные позиции
                price = trader.get_current_price() or 0.0
                pos_size = trader.position_size
                avg_price = trader.average_price

                # Получаем PnL
                try:
                    fp = trader.fetch_position()
                    unrealized_pnl = fp.get('unrealized_pnl', 0.0) if isinstance(fp, dict) else 0.0
                except:
                    unrealized_pnl = 0.0

                # Получаем ордера
                try:
                    buy_orders, sell_orders = trader.get_active_orders()
                    buy_count = len(buy_orders)
                    sell_count = len(sell_orders)
                except:
                    buy_count = sell_count = 0

                # Проверяем умный хвост
                tail_icon = " | 🪢 хвост" if getattr(trader, 'tail_active', False) else ""

                # Проверяем триггер
                trigger_price = config.trigger_price if config.trigger_price > 0 else None
                trigger_icon = f" | 🎯 {trigger_price:.2f}" if trigger_price else ""

                # Показываем только монеты с позициями
                if abs(pos_size) > 0:
                    # Определяем режим торговли для счетчика
                    is_long = config.trade_mode == "LONG"
                    if is_long:
                        long_count += 1
                        mode_icon = "🟩"
                        mode_text = "LONG"
                    else:
                        short_count += 1
                        mode_icon = "🟥"
                        mode_text = "SHORT"

                    # Добавляем к общему PnL
                    total_unrealized_pnl += unrealized_pnl

                    # Формируем строку монеты
                    coin_line = f"• {coin_symbol} {status_icon} {mode_icon} {mode_text} | шаг {config.step_percentage}% · плечо {config.leverage}x{tail_icon}{trigger_icon}"

                    pnl_sign = "+" if unrealized_pnl >= 0 else ""
                    base_currency = trader.market.get('base', coin_symbol) if hasattr(trader,
                                                                                      'market') and trader.market else coin_symbol
                    pos_text = f"{mode_text.title()} {abs(pos_size):.6f} {base_currency} @ {avg_price:.2f} | Mark {price:.2f}"
                    coin_line += f"\n📊 {pos_text}"
                    coin_line += f"\nНереализованный uPnL: {pnl_sign}{unrealized_pnl:.2f}"

                    coin_line += f"\n🧷 Ордеры: BUY {buy_count} / SELL {sell_count}"
                    coin_lines.append(coin_line)

            except Exception as e:
                logger.warning(f"Ошибка при формировании отчета для {coin_symbol}: {e}")
                coin_lines.append(f"• {coin_symbol}: Ошибка получения данных")

        # Рассчитываем PnL с начала (используем глобальный депозит как базу)
        global GLOBAL_INITIAL_DEPOSIT
        initial_deposit = float(GLOBAL_INITIAL_DEPOSIT or 1000.0)
        pnl_from_start = equity - initial_deposit if equity > 0 else 0.0
        pnl_percent = (pnl_from_start / initial_deposit * 100.0) if initial_deposit > 0 else 0.0

        # Рассчитываем просадки по той же логике что и в отчете по монете
        current_drawdown = 0.0
        if equity > 0 and total_unrealized_pnl < 0:
            current_drawdown = abs(total_unrealized_pnl) / equity * 100.0

        # Обновляем и получаем максимальную просадку портфеля
        self._update_global_drawdown(current_drawdown)
        max_drawdown = self.global_max_drawdown_percent

        # Рассчитываем реализованный и нереализованный PnL
        realized_pnl = pnl_from_start - total_unrealized_pnl  # Реализованный = общий - нереализованный

        # Формируем отчет
        pnl_sign = "+" if pnl_from_start >= 0 else ""
        realized_sign = "+" if realized_pnl >= 0 else ""
        unrealized_sign = "+" if total_unrealized_pnl >= 0 else "−"
        unrealized_abs = abs(total_unrealized_pnl)

        report = f"""🧭 Портфель — 5мин сводка

💰 Стартовый депозит: {initial_deposit:.2f} USDT
📊 Equity сейчас:     {equity:.2f} USDT
➕ PnL с начала:       {pnl_sign}{pnl_from_start:.2f} USDT ({pnl_sign}{pnl_percent:.2f}%)
├ Реализованный: {realized_sign}{realized_pnl:.2f} USDT 
└ Нереализованный: {unrealized_sign}{unrealized_abs:.2f}
📉 Просадка (текущая): {current_drawdown:.1f}%
📉 Просадка (максимальная): {max_drawdown:.1f}%
📦 Маржа использовано: {used_pct:.0f}%
📦 Маржа свободно: {available:.2f} USDT

🪙Монет: {len(coin_lines)}
🟩 Long: {long_count}
🟥 Short: {short_count}

Монеты
{chr(10).join(coin_lines)}

🧾 Исполнения
📆 Сегодня TP: {sum(STORE.get_tp_counter(coin, "today") for coin, trader in active_traders.items() if abs(trader.position_size) > 0)}
📆 За неделю TP: —
📆 За месяц TP: —

По монетам (сегодня, только TP):
{chr(10).join([f"• {coin}: TP {STORE.get_tp_counter(coin, 'today')}" for coin, trader in active_traders.items() if abs(trader.position_size) > 0])}"""

        return report

    def send_portfolio_report(self):
        """Отправить отчет портфеля в Telegram"""
        try:
            report = self.format_portfolio_report()
            # Отправляем через первого доступного трейдера
            if self.coins_traders:
                first_trader = next(iter(self.coins_traders.values()))
                first_trader.tg_notify(report)
        except Exception as e:
            logger.warning(f"Ошибка отправки портфельного отчета: {e}")

    def _update_global_drawdown(self, current_drawdown: float):
        """Обновить глобальную максимальную просадку портфеля"""
        if current_drawdown > self.global_max_drawdown_percent:
            self.global_max_drawdown_percent = current_drawdown
            # Сохраняем в базу данных
            try:
                STORE.set_config_one("global_max_drawdown_percent", str(self.global_max_drawdown_percent))
            except Exception as e:
                logger.debug(f"Ошибка сохранения глобальной просадки: {e}")

    def start_portfolio_reporter(self):
        """Запустить поток для периодической отправки портфельных отчетов"""
        import threading
        import time

        def report_loop():
            while True:
                try:
                    time.sleep(300)  # Каждые 300 секунд (5 минут)
                    if self.coins_traders:  # Если есть активные монеты
                        self.send_portfolio_report()
                        logger.info("📊 Автоматический портфельный отчет отправлен")
                except Exception as e:
                    logger.warning(f"Ошибка в цикле портфельных отчетов: {e}")

        reporter_thread = threading.Thread(target=report_loop, daemon=True)
        reporter_thread.start()
        logger.info("🚀 Запущен поток автоматических портфельных отчетов (каждые 300 сек)")


def get_current_coin(user_id: int) -> Optional[str]:
    """Получить текущую монету пользователя"""
    return CURRENT_COIN_MENU.get(user_id)


def set_current_coin(user_id: int, coin_symbol: str):
    """Установить текущую монету пользователя"""
    CURRENT_COIN_MENU[user_id] = coin_symbol


def is_coin_running(coin_symbol: str) -> bool:
    """Проверить запущена ли торговля для монеты"""
    if BOT_MANAGER:
        return BOT_MANAGER.is_coin_running(coin_symbol)
    return False


def is_short_mode(coin_symbol: str) -> bool:
    """Проверить режим SHORT для монеты"""
    if coin_symbol in COINS_CONFIG:
        return str(COINS_CONFIG[coin_symbol].trade_mode).upper() == "SHORT"
    return False


def mode_btn_label(coin_symbol: str) -> str:
    """Получить лейбл кнопки режима для монеты"""
    return MODE_SHORT_LABEL if is_short_mode(coin_symbol) else MODE_LONG_LABEL


def adaptive_btn_label(coin_symbol: str) -> str:
    """Получить лейбл кнопки адаптивности для монеты"""
    if coin_symbol in COINS_CONFIG:
        enabled = COINS_CONFIG[coin_symbol].adaptive_enabled
        return ADAPT_ON_LABEL if enabled else ADAPT_OFF_LABEL
    return ADAPT_OFF_LABEL


def trigger_btn_label(coin_symbol: str) -> str:
    """Получить лейбл кнопки триггера для монеты"""
    if coin_symbol in COINS_CONFIG:
        trigger_price = float(COINS_CONFIG[coin_symbol].trigger_price or 0.0)
        if trigger_price > 0:
            return "❌ Удалить триггерную цену"
        else:
            return BTN_TRIGGER


def main_kb() -> ReplyKeyboardMarkup:
    """Главное меню"""
    keyboard = [
        [KeyboardButton(BTN_COINS)],
        [KeyboardButton(BTN_ADD_COIN), KeyboardButton(BTN_REMOVE_COIN)],
        [KeyboardButton(BTN_INITDEP)],
        [KeyboardButton(BTN_GLOBAL_STATUS)],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def coins_list_kb() -> InlineKeyboardMarkup:
    """Клавиатура со списком монет"""
    if not BOT_MANAGER:
        return InlineKeyboardMarkup([])

    coins = BOT_MANAGER.get_active_coins()
    if not coins:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Нет активных монет", callback_data="no_coins")]
        ])

    buttons = []
    for coin in coins:
        running = is_coin_running(coin)
        status = "🟢" if running else "🔴"
        buttons.append([InlineKeyboardButton(f"{status} {coin}", callback_data=f"coin_{coin}")])

    return InlineKeyboardMarkup(buttons)


def coin_menu_kb(coin_symbol: str) -> ReplyKeyboardMarkup:
    """Меню настроек конкретной монеты"""
    if coin_symbol not in COINS_CONFIG:
        return main_kb()

    config = COINS_CONFIG[coin_symbol]
    running = is_coin_running(coin_symbol)

    # Логика: показываем цвет последнего нажатого действия
    # При запуске программы running=False, значит последнее действие = стоп
    start_label = "🟢 Старт монеты" if running else "⚪ Старт монеты"
    stop_label = "⚪ Стоп монеты" if running else "🔴 Стоп монеты"

    keyboard = [
        [KeyboardButton(f"💎 {coin_symbol}")],
        [KeyboardButton(mode_btn_label(coin_symbol))],
        [KeyboardButton(start_label), KeyboardButton(stop_label)],
        [KeyboardButton(adaptive_btn_label(coin_symbol))],
        [KeyboardButton(trigger_btn_label(coin_symbol))],
        [KeyboardButton(BTN_PRESETS)],
        [KeyboardButton(BTN_STEP), KeyboardButton(BTN_LEV)],
        [KeyboardButton(BTN_FIRST), KeyboardButton(BTN_ORDER)],
        [KeyboardButton(BTN_PARAMS)],
        [KeyboardButton(BTN_COIN_REPORT)],
        [KeyboardButton(BTN_BACK)],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def _dot_by_index(i: int) -> str:
    return "🟢" if i < 2 else ("🟡" if i < 4 else "🔴")


def preset_menu_kb():
    global DECORATED_TO_KEY
    DECORATED_TO_KEY = {}
    rows = []
    for i, k in enumerate(PRESETS.keys()):
        label = f"{k} {_dot_by_index(i)}"
        DECORATED_TO_KEY[label] = k
        rows.append([KeyboardButton(label)])
    rows.append([KeyboardButton(BTN_BACK)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def preset_confirm_kb():
    return ReplyKeyboardMarkup([[KeyboardButton(BTN_APPLY), KeyboardButton(BTN_BACK)]], resize_keyboard=True)


def _total_base(dep, lev): return dep * lev


def preset_preview_text(preset_key: str, coin_symbol: str) -> str:
    """Предпросмотр пресета для конкретной монеты"""
    p = PRESETS[preset_key]

    global GLOBAL_INITIAL_DEPOSIT
    dep = float(GLOBAL_INITIAL_DEPOSIT or 0.0)

    lev = int(p.get("lev", 10))
    base_total = dep  # Общая база = начальный депозит (без умножения на плечо)
    fv = round(base_total * p["first_pct"], 2)
    ov = round(base_total * p["order_pct"], 2)
    desc = p.get("desc", "").strip() or "(добавьте описание вручную)"
    note = "" if dep > 0 else "\n\n⚠️ Начальный депозит сейчас 0 — суммы будут 0. Задай «💰 Начальный депозит»."
    return (
        f"Монета: {coin_symbol}\n"
        f"Пресет: {preset_key}\n"
        f"Плечо: {lev}x\n"
        f"Шаг: {p['step']} %\n"
        f"Общая база: {base_total:.2f} USDT\n"
        f"Первичный вход: {fv} USDT ({int(p['first_pct'] * 100)}% от базы)\n"
        f"Размер ордера: {ov} USDT ({int(p['order_pct'] * 100)}% от базы)\n"
        f"Описание: {desc}{note}"
    )


# Старые функции удалены - используется конфиг монеты


def apply_preset(preset_key: str, coin_symbol: str) -> str:
    """Применить пресет для конкретной монеты"""
    if coin_symbol not in COINS_CONFIG:
        return "❌ Монета не найдена"

    p = PRESETS[preset_key]
    config = COINS_CONFIG[coin_symbol]

    # Обновляем конфиг монеты
    config.step_percentage = float(p["step"])
    config.leverage = int(p["lev"])

    # Пересчитываем объемы от глобального депозита
    global GLOBAL_INITIAL_DEPOSIT
    dep = float(GLOBAL_INITIAL_DEPOSIT or 0.0)
    if dep > 0:
        config.first_volume_usdt = round(dep * float(p["first_pct"]), 2)
        config.order_volume_usdt = round(dep * float(p["order_pct"]), 2)

    # Сохраняем конфиг
    if BOT_MANAGER:
        BOT_MANAGER._save_coin_config(coin_symbol)

    lev_msg = "Плечо сохранено (10x), применится при flat (позиция=0)."
    try:
        trader = BOT_MANAGER.get_coin_trader(coin_symbol) if BOT_MANAGER else None
        if trader and abs(getattr(trader, "position_size", 0.0)) <= 0:
            trader.exchange.set_leverage(int(p["lev"]), trader.symbol)
            trader.leverage = int(p["lev"])
            lev_msg = "Плечо 10x применено сразу."
    except Exception as e:
        lev_msg = f"Не удалось применить плечо сейчас: {e}"

    return "✅ Пресет применён.\n" + fmt_coin_cfg(coin_symbol) + f"\n\n{lev_msg}"


# =======================
#   SQLite KV
# =======================
DB_PATH, DB_LOCK = "bot_state.db", Lock()


class SQLiteKV:
    def __init__(self, path: str):
        self.path = path
        with DB_LOCK, sqlite3.connect(self.path) as cn:
            cn.executescript("""
                CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);
                CREATE TABLE IF NOT EXISTS processed_trades (coin TEXT, trade_id TEXT, ts REAL, PRIMARY KEY (coin, trade_id));
                CREATE TABLE IF NOT EXISTS processed_buy_orders (coin TEXT, id TEXT, ts REAL, PRIMARY KEY (coin, id));
                CREATE TABLE IF NOT EXISTS config (k TEXT PRIMARY KEY, v TEXT);
                CREATE TABLE IF NOT EXISTS equity_history (coin TEXT, ts REAL, equity REAL, unrealized_pnl REAL, PRIMARY KEY (coin, ts));
                CREATE TABLE IF NOT EXISTS coin_state (coin TEXT, k TEXT, v TEXT, PRIMARY KEY (coin, k));
            """);
            cn.commit()

    def get(self, k: str, default: Optional[str] = None) -> Optional[str]:
        with DB_LOCK, sqlite3.connect(self.path) as cn, closing(cn.cursor()) as cur:
            cur.execute("SELECT v FROM kv WHERE k=?", (k,));
            row = cur.fetchone()
            return row[0] if row else default

    def set(self, k: str, v: str):
        with DB_LOCK, sqlite3.connect(self.path) as cn:
            cn.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v));
            cn.commit()

    def delete_kv_keys(self, keys):
        if not keys: return
        placeholders = ",".join("?" * len(keys))
        with DB_LOCK, sqlite3.connect(self.path) as cn:
            cn.execute(f"DELETE FROM kv WHERE k IN ({placeholders})", keys);
            cn.commit()

    def get_config_all(self) -> dict:
        with DB_LOCK, sqlite3.connect(self.path) as cn, closing(cn.cursor()) as cur:
            cur.execute("SELECT k, v FROM config")
            return {k: json.loads(v) for k, v in cur.fetchall()}

    def get_config_one(self, k: str, default=None):
        with DB_LOCK, sqlite3.connect(self.path) as cn, closing(cn.cursor()) as cur:
            cur.execute("SELECT v FROM config WHERE k=?", (k,))
            row = cur.fetchone()
            return json.loads(row[0]) if row else default

    def is_trade_processed(self, coin_symbol: str, trade_id: str) -> bool:
        """Проверяет, была ли сделка уже обработана"""
        with DB_LOCK, sqlite3.connect(self.path) as cn, closing(cn.cursor()) as cur:
            cur.execute("SELECT 1 FROM processed_trades WHERE coin=? AND trade_id=?", (coin_symbol, trade_id))
            return cur.fetchone() is not None

    def mark_trade_processed(self, coin_symbol: str, trade_id: str):
        """Отмечает сделку как обработанную"""
        with DB_LOCK, sqlite3.connect(self.path) as cn:
            cn.execute("INSERT OR IGNORE INTO processed_trades (coin, trade_id, ts) VALUES (?, ?, ?)",
                       (coin_symbol, trade_id, time.time()))

    def set_config_many(self, cfg: dict):
        with DB_LOCK, sqlite3.connect(self.path) as cn:
            for k, v in cfg.items():
                cn.execute("INSERT INTO config(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                           (k, json.dumps(v, ensure_ascii=False)))
            cn.commit()

    def set_config_one(self, k: str, v):
        self.set_config_many({k: v})

    def is_buy_order_processed(self, coin: str, oid: str) -> bool:
        with DB_LOCK, sqlite3.connect(self.path) as cn, closing(cn.cursor()) as cur:
            cur.execute("SELECT 1 FROM processed_buy_orders WHERE coin=? AND id=?", (coin, oid));
            return cur.fetchone() is not None

    def add_processed_buy_order(self, coin: str, oid: str, ts: float):
        with DB_LOCK, sqlite3.connect(self.path) as cn:
            cn.execute("INSERT OR IGNORE INTO processed_buy_orders(coin, id, ts) VALUES(?,?,?)", (coin, oid, ts));
            cn.commit()

    def clear_processed(self, coin: str = None):
        with DB_LOCK, sqlite3.connect(self.path) as cn:
            if coin:
                cn.execute("DELETE FROM processed_trades WHERE coin=?", (coin,));
                cn.execute("DELETE FROM processed_buy_orders WHERE coin=?", (coin,));
            else:
                cn.executescript("DELETE FROM processed_trades; DELETE FROM processed_buy_orders;");
            cn.commit()

    # Методы для работы с состоянием монет
    def get_coin_state(self, coin: str, k: str, default: Optional[str] = None) -> Optional[str]:
        with DB_LOCK, sqlite3.connect(self.path) as cn, closing(cn.cursor()) as cur:
            cur.execute("SELECT v FROM coin_state WHERE coin=? AND k=?", (coin, k));
            row = cur.fetchone()
            return row[0] if row else default

    def set_coin_state(self, coin: str, k: str, v: str):
        with DB_LOCK, sqlite3.connect(self.path) as cn:
            cn.execute("INSERT INTO coin_state(coin,k,v) VALUES(?,?,?) ON CONFLICT(coin,k) DO UPDATE SET v=excluded.v",
                       (coin, k, v));
            cn.commit()

    def delete_coin_state_keys(self, coin: str, keys: List[str]):
        if not keys: return
        placeholders = ",".join("?" * len(keys))
        with DB_LOCK, sqlite3.connect(self.path) as cn:
            cn.execute(f"DELETE FROM coin_state WHERE coin=? AND k IN ({placeholders})", [coin] + keys);
            cn.commit()

    def clear_coin_state(self, coin: str):
        with DB_LOCK, sqlite3.connect(self.path) as cn:
            cn.execute("DELETE FROM coin_state WHERE coin=?", (coin,));
            cn.commit()

    def increment_tp_counter(self, coin_symbol: str, period: str = "total"):
        """Увеличивает счетчик TP для монеты"""
        key = f"{coin_symbol}_tp_{period}"
        current = self.get_config_one(key, 0)
        self.set_config_one(key, current + 1)

    def get_tp_counter(self, coin_symbol: str, period: str = "total") -> int:
        """Получает счетчик TP для монеты"""
        key = f"{coin_symbol}_tp_{period}"
        return self.get_config_one(key, 0)

    def vacuum_processed_limits(self, keep_last: int = 5000):
        with DB_LOCK, sqlite3.connect(self.path) as cn:
            for tbl in ("processed_trades", "processed_buy_orders"):
                cn.execute(f"""
                    DELETE FROM {tbl}
                    WHERE id NOT IN (SELECT id FROM {tbl} ORDER BY ts DESC LIMIT ?)
                """, (keep_last,))
            cn.commit()


STORE = SQLiteKV(DB_PATH)


# =======================
#   ЗАГРУЗКА/СОХРАНЕНИЕ КОНФИГА
# =======================
# Старые функции удалены - теперь используется BotManager


def fmt_coin_cfg(coin_symbol: str) -> str:
    """Форматировать конфиг конкретной монеты"""
    if coin_symbol not in COINS_CONFIG:
        return "❌ Монета не найдена"

    c = COINS_CONFIG[coin_symbol]
    lines = [
        f"Монета: {c.coin}",
        f"Шаг: {c.step_percentage} %",
        f"Первичный вход: {c.first_volume_usdt} USDT",
        f"Размер ордера: {c.order_volume_usdt} USDT",
        f"Плечо: {c.leverage}x",
        f"Режим: {c.trade_mode}",
        f"Адаптивность: {'ON' if c.adaptive_enabled else 'OFF'}",
        f"Статус: {'🟢 Работает' if is_coin_running(coin_symbol) else '🔴 Остановлен'}",
    ]

    trigger_price = float(c.trigger_price or 0.0)
    if trigger_price > 0:
        status = "🟡 Ожидание" if c.trigger_waiting else "✅ Установлена"
        lines.append(f"Триггерная цена: {trigger_price} ({status})")

    return "\n".join(lines)


def fmt_cfg() -> str:
    """Старая функция для совместимости - показывает первую монету или общий статус"""
    if not COINS_CONFIG:
        return "🔍 Нет активных монет"

    # Показываем первую монету
    first_coin = list(COINS_CONFIG.keys())[0]
    return fmt_coin_cfg(first_coin)


# =======================
#   TELEGRAM ХЕНДЛЕРЫ
# =======================
async def ensure_admin(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    if uid != ADMIN_ID:
        await update.effective_message.reply_text("Доступ запрещён.", reply_markup=main_kb());
        return False
    return True


async def tg_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_admin(update): return

    # Инициализируем BotManager если его нет
    global BOT_MANAGER
    if not BOT_MANAGER:
        API_KEY = "mI3GM3UxKtlLvdPJ95xnbU1iHp6jETE5bi0l0IhuGgWApkkC1Ge6OvWNS7gRnVXC"
        API_SECRET = "bcEvVu8CduJM7Mf5NrLWDUPTGB8UYLBGyNwQcvVmCzy5dZcNwNUdzDbspwDfQlvS"
        BOT_MANAGER = BotManager(API_KEY, API_SECRET)
        BOT_MANAGER.load_coins_from_db()

    await update.message.reply_text(
        "🚀 Мульти-монетный бот готов!\n\n"
        "📊 Управляйте до 5 монет одновременно\n"
        "➕ Добавляйте новые монеты\n"
        "⚙️ Настраивайте каждую отдельно\n\n"
        "Используйте кнопки ниже:",
        reply_markup=main_kb()
    )


async def tg_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_admin(update): return
    if BOT_MANAGER:
        status = BOT_MANAGER.get_global_status()
    else:
        status = "🔍 Бот-менеджер не инициализирован"
    await update.message.reply_text(status, reply_markup=main_kb())


async def handle_coin_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора монеты из inline кнопок"""
    if not await ensure_admin(update): return

    query = update.callback_query
    await query.answer()

    if query.data.startswith("coin_"):
        coin_symbol = query.data[5:]  # убираем префикс "coin_"
        user_id = update.effective_user.id
        set_current_coin(user_id, coin_symbol)

        await query.edit_message_text(
            f"🪙 Настройки {coin_symbol}:\n\n" + fmt_coin_cfg(coin_symbol),
            reply_markup=None
        )
        await ctx.bot.send_message(
            chat_id=user_id,
            text=f"Выбрана монета {coin_symbol}. Используйте кнопки для настройки:",
            reply_markup=coin_menu_kb(coin_symbol)
        )
    elif query.data == "no_coins":
        await query.edit_message_text(
            "🔍 Нет активных монет\n\nДобавьте монету через главное меню",
            reply_markup=None
        )


def apply_setting(coin_symbol: str, key: str, value_str: str) -> str:
    """Применить настройку для конкретной монеты"""
    try:
        if coin_symbol not in COINS_CONFIG:
            return "❌ Монета не найдена"

        config = COINS_CONFIG[coin_symbol]

        if key == "step":
            v = float(value_str)
            if not (0.01 <= v <= 5.0): return "❌ Шаг должен быть в пределах 0.01..5%."
            config.step_percentage = v
            logger.info(f"⚙️ {coin_symbol}: Шаг, % → {v}")

        elif key in ("first", "order"):
            v = float(value_str)
            if v <= 0: return f"❌ {'Первичный вход' if key == 'first' else 'Размер ордера'} должен быть > 0."
            if key == "first":
                config.first_volume_usdt = v
            else:
                config.order_volume_usdt = v
            logger.info(f"⚙️ {coin_symbol}: {('Первичный вход' if key == 'first' else 'Размер ордера')}, USDT → {v}")

        elif key == "lev":
            v = int(float(value_str))
            if not (1 <= v <= 50): return "❌ Плечо должно быть в пределах 1..50."
            config.leverage = v
            logger.info(f"⚙️ {coin_symbol}: Плечо → {v}x")

        elif key == "trigger":
            v = float(value_str)
            if v <= 0: return "❌ Триггерная цена должна быть > 0."
            config.trigger_price = v
            config.trigger_waiting = False  # Пока не запущен цикл
            config.trigger_last_notification = 0.0
            logger.info(f"⚙️ {coin_symbol}: Триггерная цена → {v}")
        else:
            return "❌ Неизвестный параметр."

        # Сохраняем конфиг монеты
        if BOT_MANAGER:
            BOT_MANAGER._save_coin_config(coin_symbol)

        return "✅ Обновлено."
    except Exception as e:
        return f"❌ Ошибка: {e}"


BTN_MAP = {
    BTN_STEP: ("step", "Введи шаг в % (0.01 .. 5):"),
    BTN_FIRST: ("first", "Введи сумму в USDT (> 0):"),
    BTN_ORDER: ("order", "Введи сумму в USDT (> 0):"),
    BTN_LEV: ("lev", "Введи плечо (1..50):"),
    BTN_TRIGGER: ("trigger", "Введи триггерную цену (например, 3500.50):"),
}


async def tg_buttons(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await ensure_admin(update): return
    global GLOBAL_INITIAL_DEPOSIT
    text = (update.message.text or "").strip()
    uid = update.effective_user.id

    # Главное меню
    if text == BTN_COINS:
        await update.message.reply_text("Выберите монету:", reply_markup=coins_list_kb())
        return

    if text == BTN_ADD_COIN:
        if not BOT_MANAGER:
            await update.message.reply_text("❌ Бот-менеджер не инициализирован", reply_markup=main_kb())
            return

        if len(BOT_MANAGER.get_active_coins()) >= MAX_COINS:
            await update.message.reply_text(f"❌ Максимум {MAX_COINS} монет", reply_markup=main_kb())
            return

        PENDING[uid] = {"action": "add_coin"}
        await update.message.reply_text("Введите символ монеты (например, BTC, ETH, SOL):", reply_markup=main_kb())
        return

    if text == BTN_REMOVE_COIN:
        if not BOT_MANAGER or not BOT_MANAGER.get_active_coins():
            await update.message.reply_text("🔍 Нет активных монет для удаления", reply_markup=main_kb())
            return

        PENDING[uid] = {"action": "remove_coin"}
        await update.message.reply_text("Введите символ монеты для удаления:", reply_markup=main_kb())
        return

    if text == BTN_GLOBAL_STATUS:
        if BOT_MANAGER:
            status = BOT_MANAGER.get_global_status()
            await update.message.reply_text(status, reply_markup=main_kb())
        else:
            await update.message.reply_text("🔍 Бот-менеджер не инициализирован", reply_markup=main_kb())
        return

    if text == BTN_INITDEP:
        PENDING[uid] = {"action": "global_deposit"}
        await update.message.reply_text(
            f"💰 Текущий глобальный депозит: {GLOBAL_INITIAL_DEPOSIT} USDT\n\nВведи новый начальный депозит в USDT (например, 1000):",
            reply_markup=main_kb())
        return

    # Меню монеты
    current_coin = get_current_coin(uid)
    if current_coin:
        # Обработка нажатия на кнопку с названием монеты
        if text == f"💎 {current_coin}":
            await update.message.reply_text(
                "ℹ️ Используйте другие кнопки для управления, сменить монету можно в предыдущем меню",
                reply_markup=coin_menu_kb(current_coin)
            )
            return

        # Кнопки режима для текущей монеты
        if text in (MODE_LONG_LABEL, MODE_SHORT_LABEL):
            if current_coin in COINS_CONFIG:
                # Проверяем, есть ли открытая позиция
                trader = BOT_MANAGER.get_coin_trader(current_coin) if BOT_MANAGER else None
                if trader and abs(trader.position_size) > 0:
                    await update.message.reply_text(
                        f"❌ Нельзя переключить режим {current_coin} - есть открытая позиция: {trader.position_size:.4f}\n\n"
                        f"Закройте позицию перед сменой направления торговли.",
                        reply_markup=coin_menu_kb(current_coin)
                    )
                    return

                config = COINS_CONFIG[current_coin]
                current_mode = str(config.trade_mode).upper()
                config.trade_mode = "SHORT" if current_mode == "LONG" else "LONG"

                # Сброс умного хвоста при смене режима
                trader = BOT_MANAGER.get_coin_trader(current_coin) if BOT_MANAGER else None
                if trader and trader.tail_active:
                    trader.tail_active = False
                    trader._persist_state(tail_active=trader.tail_active)
                    logger.info("🎯 Умный хвост сброшен: смена режима торговли")

                if BOT_MANAGER:
                    BOT_MANAGER._save_coin_config(current_coin)
                logger.info(f"⚙️ {current_coin}: Режим → {config.trade_mode}")
                await update.message.reply_text(f"✅ Режим {current_coin} переключён: {config.trade_mode}",
                                                reply_markup=coin_menu_kb(current_coin))
            return

        # Адаптивность для текущей монеты
        if text in (ADAPT_ON_LABEL, ADAPT_OFF_LABEL):
            if current_coin in COINS_CONFIG:
                config = COINS_CONFIG[current_coin]
                config.adaptive_enabled = not config.adaptive_enabled
                if BOT_MANAGER:
                    BOT_MANAGER._save_coin_config(current_coin)
                state = "ON" if config.adaptive_enabled else "OFF"
                logger.info(f"⚙️ {current_coin}: Адаптивность → {state}")
                await update.message.reply_text(f"Адаптивность {current_coin}: {state}",
                                                reply_markup=coin_menu_kb(current_coin))
            return

        # Управление конкретной монетой
        if "Старт монеты" in text or "Монета работает" in text:
            if BOT_MANAGER:
                BOT_MANAGER.start_coin(current_coin)
                await update.message.reply_text(f"🟢 {current_coin} запущен",
                                                reply_markup=coin_menu_kb(current_coin))
            return

        if "Стоп монеты" in text:
            if BOT_MANAGER:
                BOT_MANAGER.stop_coin(current_coin)
                await update.message.reply_text(f"🔴 {current_coin} остановлен",
                                                reply_markup=coin_menu_kb(current_coin))
            return

        # Триггерная цена для монеты
        if text == BTN_TRIGGER:
            PENDING[uid] = {"action": "trigger", "coin": current_coin}
            await update.message.reply_text(f"Введи триггерную цену для {current_coin} (например, 3500.50):",
                                            reply_markup=coin_menu_kb(current_coin))
            return

        if text == "❌ Удалить триггерную цену":
            if current_coin in COINS_CONFIG:
                config = COINS_CONFIG[current_coin]
                config.trigger_price = 0.0
                config.trigger_waiting = False
                config.trigger_last_notification = 0.0
                if BOT_MANAGER:
                    BOT_MANAGER._save_coin_config(current_coin)
                logger.info(f"⚙️ {current_coin}: Триггерная цена удалена")
                await update.message.reply_text(f"✅ Триггерная цена {current_coin} удалена.",
                                                reply_markup=coin_menu_kb(current_coin))
            return

        # Пресеты для монеты
        if text == BTN_PRESETS:
            PRESET_WAIT[uid] = {"coin": current_coin}
            await update.message.reply_text(f"Выбери пресет для {current_coin}:", reply_markup=preset_menu_kb())
            return

        # Отчет монеты
        if text == BTN_COIN_REPORT:
            trader = BOT_MANAGER.get_coin_trader(current_coin) if BOT_MANAGER else None
            if trader:
                report = trader._format_30m_report()
                await update.message.reply_text(report, reply_markup=coin_menu_kb(current_coin))
            else:
                await update.message.reply_text(f"❌ Трейдер для {current_coin} не найден",
                                                reply_markup=coin_menu_kb(current_coin))
            return

        # Параметры монеты
        if text == BTN_PARAMS:
            await update.message.reply_text(f"Параметры {current_coin}:\n\n" + fmt_coin_cfg(current_coin),
                                            reply_markup=coin_menu_kb(current_coin))
            return

        # Настройки монеты
        if text in BTN_MAP:
            key, prompt = BTN_MAP[text]
            PENDING[uid] = {"action": key, "coin": current_coin}
            await update.message.reply_text(f"{current_coin}: {prompt}", reply_markup=coin_menu_kb(current_coin))
            return

    # Пресеты
    if text in PRESETS or text in DECORATED_TO_KEY:
        key = text if text in PRESETS else DECORATED_TO_KEY[text]
        preset_coin = PRESET_WAIT.get(uid, {}).get("coin")
        if preset_coin:
            PRESET_WAIT[uid] = {"coin": preset_coin, "preset": key}
            await update.message.reply_text(preset_preview_text(key, preset_coin), reply_markup=preset_confirm_kb())
        return

    if text == BTN_APPLY:
        preset_data = PRESET_WAIT.get(uid, {})
        preset_coin = preset_data.get("coin")
        preset_key = preset_data.get("preset")
        if preset_coin and preset_key:
            PRESET_WAIT.pop(uid, None)
            result = apply_preset(preset_key, preset_coin)
            await update.message.reply_text(result, reply_markup=coin_menu_kb(preset_coin))
        else:
            await update.message.reply_text("Сначала выбери пресет.", reply_markup=preset_menu_kb())
        return

    # Назад
    if text == BTN_BACK:
        if uid in PRESET_WAIT:
            PRESET_WAIT.pop(uid, None)
            preset_coin = PRESET_WAIT.get(uid, {}).get("coin", current_coin)
            if preset_coin:
                await update.message.reply_text(f"Настройки {preset_coin}:", reply_markup=coin_menu_kb(preset_coin))
            else:
                await update.message.reply_text("Главное меню.", reply_markup=main_kb())
        elif current_coin:
            CURRENT_COIN_MENU.pop(uid, None)
            await update.message.reply_text("Главное меню.", reply_markup=main_kb())
        else:
            await update.message.reply_text("Главное меню.", reply_markup=main_kb())
        return

    # Обработка ввода
    if uid in PENDING:
        pending_data = PENDING.pop(uid)

        if isinstance(pending_data, dict):
            action = pending_data.get("action")
            coin = pending_data.get("coin")

            if action == "add_coin":
                coin_symbol = text.strip().upper()
                if not coin_symbol.isalpha():
                    await update.message.reply_text("❌ Символ монеты должен содержать только буквы",
                                                    reply_markup=main_kb())
                    return

                if BOT_MANAGER and BOT_MANAGER.add_coin(coin_symbol):
                    # Запускаем трейдер в отдельном потоке
                    trader = BOT_MANAGER.get_coin_trader(coin_symbol)
                    if trader:
                        trader.setup()
                        Thread(target=trader.run, daemon=True).start()

                    await update.message.reply_text(f"✅ Монета {coin_symbol} добавлена", reply_markup=main_kb())
                else:
                    await update.message.reply_text(f"❌ Не удалось добавить {coin_symbol}", reply_markup=main_kb())
                return

            elif action == "remove_coin":
                coin_symbol = text.strip().upper()
                if BOT_MANAGER and BOT_MANAGER.remove_coin(coin_symbol):
                    await update.message.reply_text(f"✅ Монета {coin_symbol} удалена", reply_markup=main_kb())
                else:
                    await update.message.reply_text(f"❌ Монета {coin_symbol} не найдена", reply_markup=main_kb())
                return

            elif action == "global_deposit":
                try:
                    val = float(text.strip())
                    if val < 0:
                        await update.message.reply_text("❌ Депозит не может быть отрицательным", reply_markup=main_kb())
                        return
                    GLOBAL_INITIAL_DEPOSIT = val
                    # Сохраняем в БД
                    STORE.set_config_one("global_initial_deposit", val)
                    await update.message.reply_text(
                        f"✅ Глобальный депозит установлен: {val} USDT\n\nЭтот депозит будет использоваться для всех монет в отчетах и расчетах.",
                        reply_markup=main_kb())
                except ValueError:
                    await update.message.reply_text("❌ Введи корректное число", reply_markup=main_kb())
                return

            elif coin and action in [v[0] for v in BTN_MAP.values()]:
                val = text.strip()
                res = apply_setting(coin, action, val)

                if action == "trigger" and "✅ Обновлено." in res:
                    trigger_price = float(val)
                    trigger_msg = (
                            f"🎯 Триггерная цена {coin} установлена: {trigger_price}\n\n"
                            f"⚠️ Бот перейдет в режим ожидания после запуска.\n"
                            f"Торговля начнется только при достижении триггерной цены.\n\n"
                            + fmt_coin_cfg(coin)
                    )
                    await update.message.reply_text(trigger_msg, reply_markup=coin_menu_kb(coin))
                else:
                    await update.message.reply_text(res + "\n\n" + fmt_coin_cfg(coin), reply_markup=coin_menu_kb(coin))
                return

    await update.message.reply_text("Не понял. Используйте кнопки меню.", reply_markup=main_kb())


def build_telegram_app():
    app = ApplicationBuilder().token(TG_TOKEN).build()
    app.add_handler(CommandHandler("start", tg_start))
    app.add_handler(CommandHandler("status", tg_status))
    app.add_handler(CallbackQueryHandler(handle_coin_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, tg_buttons))
    return app


# =======================
#   БОТ ТОРГОВЛИ ДЛЯ МОНЕТ
# =======================
class CoinTrader:
    """Торговый бот для одной монеты"""

    def __init__(self, api_key: str, api_secret: str, coin_symbol: str, config: CoinConfig):
        self.exchange = ccxt.binance({
            'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True,
            'options': {'defaultType': 'future', 'adjustForTimeDifference': True, 'recvWindow': 20000,
                        'fetchCurrencies': False}
        })

        self.coin_symbol = coin_symbol
        self.symbol = f"{coin_symbol}/USDT:USDT"
        self.market = None
        self.config = config

        # Параметры торговли из конфига
        self.leverage = config.leverage
        self.step_percentage = config.step_percentage
        self.first_volume_usdt = config.first_volume_usdt
        self.order_volume_usdt = config.order_volume_usdt

        self.position_size = self.average_price = self.last_price = 0.0

        # Умный хвост
        self.tail_active = False
        self.tail_threshold_mult = 1.5
        self.processed_trades, self.processed_buy_orders = set(), set()
        self.bot_start_time = self.min_event_ts = time.time()
        self.last_market_order_time, self.last_market_order_id = 0.0, ""
        self.last_status_time = self.last_position_check = 0.0

        self._applied_leverage, self._applied_step = self.leverage, self.step_percentage
        self._applied_first, self._applied_order = self.first_volume_usdt, self.order_volume_usdt
        self._applied_symbol = self.symbol
        self._applied_mode = "LONG"

        self._orders_cache, self._orders_stale = {"buys": [], "sells": [], "ts": 0.0}, False
        self._last_buy_place_ts = 0.0
        self._run_event = Event();
        self._run_event.clear()
        self._last_tick_ts, self.TICK_INTERVAL = 0.0, 15.0
        self._last_flat_reset_ts, self._banner_pending, self._has_traded = 0.0, 0, False

        self.awaiting_primary_confirm, self.PRIMARY_COOLDOWN = False, 300.0
        self._flat_confirm_counter, self._flat_confirm_required = 0, 2
        self._last_valid_pos_ts = self._last_valid_price_ts = 0.0

        # Трекинг просадок (максимальная просадка в % за все время)
        self.max_drawdown_percent = 0.0  # Максимальная просадка в % за все время
        self.last_drawdown_update = 0.0

        try:
            # Загружаем состояние для конкретной монеты
            self.position_size = float(json.loads(STORE.get_coin_state(coin_symbol, "position_size", "0") or "0"))
            self.average_price = float(json.loads(STORE.get_coin_state(coin_symbol, "average_price", "0") or "0"))
            self.last_price = float(json.loads(STORE.get_coin_state(coin_symbol, "last_price", "0") or "0"))
            self.tail_active = bool(json.loads(STORE.get_coin_state(coin_symbol, "tail_active", "false") or "false"))

            # Синхронизируем с реальной позицией на бирже при старте
            try:
                balance = self.exchange.fetch_balance()
                positions = balance.get('info', {}).get('positions', [])
                real_size = 0.0
                real_entry_price = 0.0

                for pos in positions:
                    if pos.get('symbol') == self.symbol.replace(':', ''):
                        real_size = float(pos.get('positionAmt', 0) or 0)
                        real_entry_price = float(pos.get('entryPrice', 0) or 0)
                        break

                if abs(real_size) != abs(self.position_size):
                    logger.info(f"🔄 {coin_symbol}: Синхронизация позиции БД({self.position_size}) → Биржа({real_size})")
                    self.position_size = real_size
                    if real_size != 0:
                        self.average_price = real_entry_price
                    self._persist_state(position_size=self.position_size, average_price=self.average_price)
            except Exception as sync_err:
                logger.warning(f"⚠️ {coin_symbol}: Не удалось синхронизировать позицию: {sync_err}")
            self.last_market_order_time = float(
                json.loads(STORE.get_coin_state(coin_symbol, "last_market_order_time", "0") or "0"))
            self.last_market_order_id = str(
                json.loads(STORE.get_coin_state(coin_symbol, "last_market_order_id", '""') or ""))
            self.last_status_time = float(json.loads(STORE.get_coin_state(coin_symbol, "last_status_time", "0") or "0"))
            self.last_position_check = float(
                json.loads(STORE.get_coin_state(coin_symbol, "last_position_check", "0") or "0"))
            self._last_buy_place_ts = float(
                json.loads(STORE.get_coin_state(coin_symbol, "_last_buy_place_ts", "0") or "0"))
            self._last_tick_ts = float(json.loads(STORE.get_coin_state(coin_symbol, "_last_tick_ts", "0") or "0"))

            bs = STORE.get_coin_state(coin_symbol, "bot_start_time")
            self.bot_start_time = float(json.loads(bs)) if bs is not None else self.bot_start_time
            if bs is None: STORE.set_coin_state(coin_symbol, "bot_start_time", json.dumps(self.bot_start_time))

            me = STORE.get_coin_state(coin_symbol, "min_event_ts")
            self.min_event_ts = float(json.loads(me)) if me is not None else self.min_event_ts
            if me is None: STORE.set_coin_state(coin_symbol, "min_event_ts", json.dumps(self.min_event_ts))

            bp = STORE.get_coin_state(coin_symbol, "startup_banner_pending");
            self._banner_pending = int(json.loads(bp)) if bp is not None else 0

            ht = STORE.get_coin_state(coin_symbol, "has_traded_flag");
            self._has_traded = bool(int(json.loads(ht))) if ht is not None else False

            # Восстановление данных просадок
            self.max_drawdown_percent = float(
                json.loads(STORE.get_coin_state(coin_symbol, "max_drawdown_percent", "0") or "0"))
        except Exception as e:
            logger.warning(f"Восстановление состояния {coin_symbol} из SQLite: {e}")

        try:
            STORE.set_coin_state(coin_symbol, "run_event_flag", json.dumps(0))
        except Exception:
            pass
        self._run_event.clear()

        if abs(self.position_size) <= 0: self._reset_state_db_when_flat("init_flat")

    # --------- Утилиты для режима LONG/SHORT и адаптивности ---------
    def is_short(self) -> bool:
        return str(self.config.trade_mode).upper() == "SHORT"

    def is_flat(self) -> bool:
        try:
            return abs(float(self.position_size)) <= 1e-12
        except Exception:
            return False

    def check_trigger_condition(self, current_price: float) -> bool:
        """Проверяет условие триггера и переводит в режим торговли если условие выполнено"""
        trigger_price = float(self.config.trigger_price or 0.0)
        trigger_waiting = self.config.trigger_waiting

        if not trigger_waiting or trigger_price <= 0:
            return True  # Нет триггера - торговать можно

        # Проверяем достижение триггерной цены
        trigger_reached = False
        if self.is_short():
            # В шорте ждем когда цена поднимется ДО или ВЫШЕ триггера
            trigger_reached = current_price >= trigger_price
        else:
            # В лонге ждем когда цена опустится ДО или НИЖЕ триггера
            trigger_reached = current_price <= trigger_price

        if trigger_reached:
            # Обнуляем триггер после срабатывания
            self.config.trigger_waiting = False
            self.config.trigger_price = 0.0
            self.config.trigger_last_notification = 0.0

            # Сохраняем конфиг монеты
            if BOT_MANAGER:
                BOT_MANAGER._save_coin_config(self.coin_symbol)

            mode = "SHORT" if self.is_short() else "LONG"
            self.tg_notify_with_keyboard(
                f"🎯 ТРИГГЕР СРАБОТАЛ! ({self.coin_symbol})\n"
                f"Цена достигла {trigger_price}\n"
                f"Текущая цена: {current_price}\n"
                f"Режим: {mode}\n\n"
                f"🚀 Начинаю торговлю!\n\n"
                f"✅ Триггерная цена очищена."
            )
            logger.info(
                f"🎯 {self.coin_symbol}: Триггер сработал! Цена {current_price} достигла триггера {trigger_price}. Триггер очищен.")
            return True

        return False  # Триггер не сработал - торговать нельзя

    def send_trigger_waiting_notification(self, current_price: float):
        """Отправляет уведомление о режиме ожидания триггера каждые 5 минут"""
        trigger_price = float(self.config.trigger_price or 0.0)
        trigger_waiting = self.config.trigger_waiting
        last_notification = float(self.config.trigger_last_notification or 0.0)

        if not trigger_waiting or trigger_price <= 0:
            return

        now = time.time()
        if now - last_notification < 300:  # 5 минут = 300 секунд
            return

        self.config.trigger_last_notification = now

        # Сохраняем конфиг монеты
        if BOT_MANAGER:
            BOT_MANAGER._save_coin_config(self.coin_symbol)

        mode = "SHORT" if self.is_short() else "LONG"
        direction = "выше" if self.is_short() else "ниже"

        self.tg_notify(
            f"🟡 Ожидание триггера ({self.coin_symbol})\n"
            f"Режим: {mode}\n"
            f"Триггерная цена: {trigger_price}\n"
            f"Текущая цена: {current_price}\n"
            f"Ожидаю цену {direction} {trigger_price}"
        )

    def entry_side(self) -> str:
        return 'sell' if self.is_short() else 'buy'

    def tp_side(self) -> str:
        return 'buy' if self.is_short() else 'sell'

    def _count_tp_orders(self) -> int:
        buys, sells = self.get_active_orders()
        if self._orders_stale:
            buys, sells = self._orders_cache["buys"], self._orders_cache["sells"]
        return len(buys) if self.is_short() else len(sells)

    def _adaptive_params_for_k(self, k: int):
        enabled = bool(self.config.adaptive_enabled)
        vol_inc_per = float(self.config.adaptive_volume_increment_pct)
        step_inc_per3 = float(self.config.adaptive_step_increment_per3_pct)

        if not enabled: return 0.0, 0.0
        vol_plus = k * vol_inc_per
        step_plus = (k // 3) * step_inc_per3
        return vol_plus, step_plus

    def _adaptive_suffix_for_next_entry(self, pred_k: int) -> str:
        if not self.config.adaptive_enabled:
            return ""
        vol_plus, step_plus = self._adaptive_params_for_k(pred_k)
        return f"\nАдаптивность: объём +{vol_plus:.0f}% · шаг +{step_plus:.1f}%"

    def _desired_entry_price_and_amount(self, current_price: float):
        k = self._count_tp_orders()
        vol_plus, step_plus = self._adaptive_params_for_k(k)
        order_usdt = self.order_volume_usdt * (1.0 + vol_plus / 100.0)
        step_use = self.step_percentage + step_plus
        price = current_price * (1 - step_use / 100.0) if not self.is_short() else current_price * (
                1 + step_use / 100.0)
        amt = self.calculate_amount_for_usdt(price, order_usdt)
        return price, amt

    def _desired_tp_price_and_amount(self, current_price: float):
        price = current_price * (1 + self.step_percentage / 100.0) if not self.is_short() else current_price * (
                1 - self.step_percentage / 100.0)
        amt = self.calculate_amount_for_usdt(price, self.order_volume_usdt)
        return price, amt

    def place_entry_limit(self, price: float, amount: float):
        try:
            price, amount = self.price_to_precision(price), self.amount_to_precision(amount)
            if amount <= 0: return None
            if self.is_short():
                return self.exchange.create_limit_sell_order(self.symbol, amount, price)
            else:
                return self.exchange.create_limit_buy_order(self.symbol, amount, price)
        except Exception as e:
            logger.error(f"Ошибка размещения entry-лимита: {e}");
            return None

    def place_tp_reduce(self, price: float, amount: float):
        try:
            price, amount = self.price_to_precision(price), self.amount_to_precision(amount)
            if amount <= 0: return None
            params = {'reduceOnly': True}
            if self.is_short():
                return self.exchange.create_limit_buy_order(self.symbol, amount, price, params)  # TP для шорта
            else:
                return self.exchange.create_limit_sell_order(self.symbol, amount, price, params)  # TP для лонга
        except Exception as e:
            logger.error(f"Ошибка постановки TP (reduceOnly): {e}");
            return None

    def tp_capacity(self) -> float:
        open_tp_amt = self.get_open_side_amount(self.tp_side())
        return self.normalize_amount(max(0.0, abs(self.position_size) - open_tp_amt))

    # --------- Telegram уведомления ---------
    def tg_notify(self, text: str):
        try:
            import asyncio
            from telegram import Bot
            async def _send():
                await Bot(TG_TOKEN).send_message(chat_id=ADMIN_ID, text=text)

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            (loop.create_task(_send()) if loop and loop.is_running() else asyncio.run(_send()))
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление в Telegram: {e}")

    def tg_notify_with_keyboard(self, text: str):
        """Отправляет уведомление с обновленной клавиатурой"""
        try:
            import asyncio
            from telegram import Bot
            async def _send():
                bot = Bot(TG_TOKEN)
                await bot.send_message(chat_id=ADMIN_ID, text=text, reply_markup=main_kb())

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            (loop.create_task(_send()) if loop and loop.is_running() else asyncio.run(_send()))
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление с клавиатурой: {e}")

    def send_start_banner(self):
        try:
            price = self.get_current_price() or 0.0
        except Exception:
            price = 0.0
        base = (self.market or {}).get('base') or self.symbol.split('/')[0]
        global GLOBAL_INITIAL_DEPOSIT
        init_dep = float(GLOBAL_INITIAL_DEPOSIT or 0.0)
        mode = str(self.config.trade_mode).upper()
        adapt = "ON" if self.config.adaptive_enabled else "OFF"
        self.tg_notify(
            f"🚀 {self.coin_symbol}: Бот запущен\n"
            f"Инструмент: {self.symbol}\nБаза: {base}\n"
            f"Режим: {mode} | Адаптивность: {adapt}\n"
            f"Плечо: {self.leverage}x · Шаг: {self.step_percentage}%\n"
            f"Первичный вход: {self.first_volume_usdt} USDT · Ордер: {self.order_volume_usdt} USDT\n"
            f"Начальный депозит: {init_dep:.2f} USDT\n" + (f"Текущая цена: {price:.2f}" if price else "Текущая цена: —")
        )

    def send_started_text(self):
        self.tg_notify(f"{self.coin_symbol}: Бот запущен. Основной цикл работает.")

    # --------- Persist helpers ---------
    def _persist_state(self, **kwargs):
        try:
            for k, v in kwargs.items():
                STORE.set_coin_state(self.coin_symbol, k, json.dumps(v))
        except Exception as e:
            logger.debug(f"{self.coin_symbol}: persist_state предупреждение: {e}")

    def _reset_state_db_when_flat(self, reason: str = ""):
        if abs(self.position_size) > 0: return
        now = time.time()
        if now - self._last_flat_reset_ts < 1.0: return
        try:
            STORE.delete_coin_state_keys(self.coin_symbol,
                                         ["position_size", "average_price", "last_price", "last_market_order_time",
                                          "last_position_check", "_last_buy_place_ts", "_last_tick_ts",
                                          "last_market_order_id", "tail_active"])
            # Сброс умного хвоста при flat
            self.tail_active = False
            STORE.clear_processed(self.coin_symbol)
            self.bot_start_time = self.min_event_ts = now
            STORE.set_coin_state(self.coin_symbol, "bot_start_time", json.dumps(self.bot_start_time))
            STORE.set_coin_state(self.coin_symbol, "min_event_ts", json.dumps(self.min_event_ts))
            self._last_flat_reset_ts = now
            logger.info(f"🧼 {self.coin_symbol}: Очистка состояния в БД (позиция = 0). Конфиг сохранён.")
        except Exception as e:
            logger.warning(f"{self.coin_symbol}: Не удалось очистить состояние при flat: {e}")

    # --------- Управление циклом ---------
    def pause(self):
        try:
            self._run_event.clear()
        except Exception:
            from threading import Event as _Event;
            self._run_event = _Event();
            self._run_event.clear()
        logger.info("⏸️ Основной цикл ОСТАНОВЛЕН по команде Telegram.")
        self._persist_state(run_event_flag=0)

    def resume(self):
        try:
            running = self._run_event.is_set()
        except Exception:
            from threading import Event as _Event;
            self._run_event = _Event();
            running = False

        if self.is_flat():
            self.min_event_ts = self.bot_start_time = time.time()
            # Сброс умного хвоста при запуске/возобновлении
            if self.tail_active:
                self.tail_active = False
                logger.info("🎯 Умный хвост сброшен: запуск/возобновление торговли")
            self._persist_state(min_event_ts=self.min_event_ts, bot_start_time=self.bot_start_time,
                                tail_active=self.tail_active)

        # Проверяем есть ли триггерная цена и включаем режим ожидания
        trigger_price = float(self.config.trigger_price or 0.0)
        if trigger_price > 0:
            self.config.trigger_waiting = True
            self.config.trigger_last_notification = 0.0
            # Сохраняем конфиг монеты
            if BOT_MANAGER:
                BOT_MANAGER._save_coin_config(self.coin_symbol)

        if not running:
            self.last_status_time = time.time()
            self._run_event.set()
            logger.info("▶️ Основной цикл ЗАПУЩЕН по команде Telegram.")

            # Уведомление зависит от наличия триггера
            if trigger_price > 0:
                mode = "SHORT" if self.is_short() else "LONG"
                direction = "выше" if self.is_short() else "ниже"
                self.tg_notify(
                    f"🎯 {self.coin_symbol}: Бот запущен в режиме ожидания триггера\n"
                    f"Режим: {mode}\n"
                    f"Триггерная цена: {trigger_price}\n"
                    f"Ожидаю цену {direction} {trigger_price}\n\n"
                    f"🟡 Торговля начнется при достижении триггерной цены"
                )
            else:
                self.send_started_text();
                self.send_start_banner()

            self._banner_pending = 0;
            STORE.set_coin_state(self.coin_symbol, "startup_banner_pending", json.dumps(0))
        else:
            logger.info("▶️ Команда «Старт» получена, но цикл уже запущен.")
        self._persist_state(run_event_flag=1)

    # --------- Инициализация биржи ---------
    def setup(self):
        logger.info("Загружаем рынки...")
        self.exchange.load_markets(reload=True)
        self.market = self.exchange.market(self.symbol)
        logger.info(f"Рынки загружены. Символ: {self.symbol}")
        try:
            self.exchange.set_leverage(self.leverage, self.symbol)
            self._applied_leverage = self.leverage
            logger.info(f"Плечо установлено: {self.leverage}x")
        except Exception as e:
            logger.warning(f"Не удалось установить плечо автоматически: {e}")

    def price_to_precision(self, price: float) -> float:
        return float(self.exchange.price_to_precision(self.symbol, price))

    def amount_to_precision(self, amount: float) -> float:
        return float(self.exchange.amount_to_precision(self.symbol, amount))

    def normalize_amount(self, amount: float) -> float:
        try:
            amt = self.amount_to_precision(max(0.0, float(amount)))
        except Exception:
            amt = max(0.0, float(amount))
        min_amt = 0.0
        try:
            min_amt = float((self.market.get('limits', {}).get('amount') or {}).get('min') or 0.0)
        except Exception:
            pass
        if not min_amt:
            try:
                for f in self.market['info'].get('filters', []):
                    if f.get('filterType') in ('LOT_SIZE', 'LOT_SIZE_FILTER'): min_amt = float(
                        f.get('minQty') or 0.0); break
            except Exception:
                pass
        return 0.0 if (min_amt and amt < min_amt) else amt

    # --------- Биржевые запросы ---------
    def get_current_price(self):
        try:
            t = self.exchange.fetch_ticker(self.symbol)
            self._last_valid_price_ts = time.time()
            return float(t['last'])
        except Exception as e:
            logger.error(f"Ошибка получения цены: {e}");
            return None

    def fetch_position(self):
        try:
            positions = self.exchange.fetch_positions([self.symbol])
        except Exception as e1:
            logger.error(f"Ошибка fetch_positions[by symbol]: {e1}")
            try:
                positions = self.exchange.fetch_positions()
            except Exception as e2:
                logger.error(f"Ошибка fetch_positions(all): {e2}");
                return None
        if not positions:
            return {'size': 0.0, 'entry_price': 0.0, 'unrealized_pnl': 0.0, 'percentage': 0.0, 'mark_price': 0.0}
        for pos in positions:
            if isinstance(pos, dict) and pos.get('symbol') == self.symbol:
                size = next((float(pos[k]) for k in ('size', 'contracts', 'amount') if pos.get(k) is not None), 0.0)
                entry = next((float(pos[k]) for k in ('entryPrice', 'markPrice', 'price') if pos.get(k)), 0.0)
                return {'size': size, 'entry_price': entry, 'unrealized_pnl': float(pos.get('unrealizedPnl') or 0.0),
                        'percentage': float(pos.get('percentage') or 0.0),
                        'mark_price': float(pos.get('markPrice') or 0.0)}
        return {'size': 0.0, 'entry_price': 0.0, 'unrealized_pnl': 0.0, 'percentage': 0.0, 'mark_price': 0.0}

    def get_active_orders(self):
        try:
            orders = self.exchange.fetch_open_orders(self.symbol)
            buys, sells = [o for o in orders if o.get('side') == 'buy'], [o for o in orders if o.get('side') == 'sell']
            self._orders_cache, self._orders_stale = {"buys": buys, "sells": sells, "ts": time.time()}, False
            return buys, sells
        except Exception as e:
            logger.error(f"Ошибка получения открытых ордеров: {e}")
            self._orders_stale = True;
            return self._orders_cache["buys"], self._orders_cache["sells"]

    def cancel_all_orders(self, side=None):
        try:
            for o in self.exchange.fetch_open_orders(self.symbol):
                if side is None or o.get('side') == side:
                    try:
                        self.exchange.cancel_order(o['id'], self.symbol)
                    except Exception as ce:
                        logger.warning(f"Не удалось снять ордер {o.get('id')}: {ce}")
        except Exception as e:
            logger.error(f"Ошибка отмены ордеров: {e}")

    # --------- Расчёты и постановка ---------
    def calculate_amount_for_usdt(self, price: float, usdt_value: float) -> float:
        if price <= 0: return 0.0
        amt = usdt_value / price

        # Получаем минимальный объем
        min_amt = 0.0
        try:
            min_amt = float((self.market['limits']['amount'] or {}).get('min') or 0.0)
        except Exception:
            pass

        # Если расчетный объем меньше минимального, используем минимальный
        if amt < min_amt:
            amt = min_amt

        # Применяем точность биржи
        amt = self.amount_to_precision(amt)

        # Еще раз проверяем минимум после округления
        if amt < min_amt:
            amt = min_amt

        return amt

    def place_limit_buy(self, price: float, amount: float):
        try:
            price, amount = self.price_to_precision(price), self.amount_to_precision(amount)
            if amount <= 0: return None
            order = self.exchange.create_limit_buy_order(self.symbol, amount, price)
            self._last_buy_place_ts = time.time();
            self._persist_state(_last_buy_place_ts=self._last_buy_place_ts)
            return order
        except Exception as e:
            logger.error(f"Ошибка размещения лимит BUY: {e}");
            return None

    def place_limit_sell_reduce(self, price: float, amount: float):
        try:
            price, amount = self.price_to_precision(price), self.amount_to_precision(amount)
            if amount <= 0: return None
            return self.exchange.create_limit_sell_order(self.symbol, amount, price, {'reduceOnly': True})
        except Exception as e:
            logger.error(f"Ошибка размещения лимит SELL (reduceOnly): {e}");
            return None

    def place_market_buy_usdt(self, usdt_value: float):
        price = self.get_current_price()
        if not price: return None
        amount = self.calculate_amount_for_usdt(price, usdt_value)
        if amount <= 0: return None
        try:
            order = self.exchange.create_market_buy_order(self.symbol, amount)
            self.last_market_order_time, self.last_market_order_id, self._has_traded = time.time(), str(
                order.get('id') or ""), True
            self._persist_state(last_market_order_time=self.last_market_order_time,
                                last_market_order_id=self.last_market_order_id, has_traded_flag=1)
            base = (self.market or {}).get('base') or self.symbol.split('/')[0]
            self.tg_notify(f"🟢 Маркет BUY: ~{amount:.6f} {base} @ ≈{price:.2f} ({self.symbol})")
            logger.info(f"🟢 Маркет покупка: ~{usdt_value} USDT ({amount} {self.market['base']}) при цене ~{price}")
            return order
        except Exception as e:
            logger.error(f"Ошибка маркет покупки: {e}");
            return None

    def place_market_sell_usdt(self, usdt_value: float):
        price = self.get_current_price()
        if not price: return None
        amount = self.calculate_amount_for_usdt(price, usdt_value)
        if amount <= 0: return None
        try:
            order = self.exchange.create_market_sell_order(self.symbol, amount)
            self.last_market_order_time, self.last_market_order_id, self._has_traded = time.time(), str(
                order.get('id') or ""), True
            self._persist_state(last_market_order_time=self.last_market_order_time,
                                last_market_order_id=self.last_market_order_id, has_traded_flag=1)
            base = (self.market or {}).get('base') or self.symbol.split('/')[0]
            self.tg_notify(f"🔴 Маркет SELL (шорт): ~{amount:.6f} {base} @ ≈{price:.2f} ({self.symbol})")
            logger.info(
                f"🔴 Маркет продажа (шорт): ~{usdt_value} USDT ({amount} {self.market['base']}) при цене ~{price}")
            return order
        except Exception as e:
            logger.error(f"Ошибка маркет продажи: {e}");
            return None

    def get_open_side_amount(self, side='sell') -> float:
        buys, sells = self.get_active_orders();
        arr = sells if side == 'sell' else buys
        total = 0.0
        for o in arr:
            try:
                total += float(o.get('amount') or 0.0)
            except Exception:
                pass
        return total

    def reconcile_tp_with_position(self):
        try:
            side = self.tp_side()
            excess = self.get_open_side_amount(side) - abs(self.position_size)
            if excess <= 1e-12: return
            buys, sells = self.get_active_orders()
            if self._orders_stale: return
            arr = buys if side == 'buy' else sells
            keyfn = (lambda x: float(x['price']))
            reverse = (side == 'sell')
            for o in sorted(arr, key=keyfn, reverse=reverse):
                if excess <= 1e-12: break
                try:
                    self.exchange.cancel_order(o['id'], self.symbol);
                    excess -= float(o.get('amount') or 0.0)
                except Exception as e:
                    logger.warning(f"Не удалось снять TP {o.get('id')}: {e}")
            logger.info("🧹 Сверка: привели сумму TP-лимитов к размеру позиции")
        except Exception as e:
            logger.error(f"Ошибка сверки TP-лимитов: {e}")

    # --------- Поддержка позиции / логика ---------
    def update_position_info(self, force_wait=False):
        p = None
        if force_wait:
            for _ in range(5):
                p = self.fetch_position()
                if p is not None: break
                time.sleep(1.2)
        else:
            p = self.fetch_position()
        if p is None:
            logger.warning("Позиция неизвестна (сбой API). Сохраняю предыдущее значение.");
            return

        old = self.position_size
        self.position_size, self.average_price = float(p['size']), float(p.get('entry_price') or 0.0)
        self._persist_state(position_size=self.position_size, average_price=self.average_price)
        self._last_valid_pos_ts = time.time()
        self._flat_confirm_counter = self._flat_confirm_counter + 1 if self.is_flat() else 0
        if not self.is_flat() and self.awaiting_primary_confirm: self.awaiting_primary_confirm = False

        if old == 0 and self.position_size > 0:
            logger.info(f"📊 Позиция открыта: {self.position_size} {self.market['base']}, средняя {self.average_price}")
        elif abs(self.position_size - old) > 1e-6:
            logger.info(
                f"📊 Позиция обновлена: {self.position_size} {self.market['base']}, средняя {self.average_price}")
        if self.is_flat() and old != 0: self._reset_state_db_when_flat("update_position_info_flat")

    def ensure_orders_when_position(self, current_price: float):
        if self.is_flat(): return
        buy_orders, sell_orders = self.get_active_orders()
        if self._orders_stale:
            logger.info("⏸️ Пропуск постановки лимитов: список открытых ордеров устарел.");
            return

        entry_orders = sell_orders if self.is_short() else buy_orders
        tp_orders = buy_orders if self.is_short() else sell_orders

        desired_entry_price, desired_entry_amt = self._desired_entry_price_and_amount(current_price)

        if len(entry_orders) > 1:
            try:
                keep = min(entry_orders, key=lambda o: abs(float(o['price']) - desired_entry_price))
                for o in entry_orders:
                    if o['id'] != keep['id']:
                        try:
                            self.exchange.cancel_order(o['id'], self.symbol)
                        except Exception as e:
                            logger.warning(f"Не удалось снять лишний ENTRY {o.get('id')}: {e}")
                logger.info("🧽 Очистка: оставили один ENTRY, близкий к целевой цене.")
            except Exception:
                pass

        if not entry_orders:
            if time.time() - self._last_buy_place_ts < 3.0:
                logger.info("⏸️ Кулдаун после постановки ENTRY — пропускаем.")
            else:
                buys2, sells2 = self.get_active_orders()
                arr = sells2 if self.is_short() else buys2
                if self._orders_stale:
                    logger.info("⏸️ Пропуск ENTRY: список ордеров устарел.")
                elif arr:
                    logger.info("⏸️ Пропуск ENTRY: ENTRY уже есть (single protection).")
                elif desired_entry_amt > 0 and self.place_entry_limit(desired_entry_price, desired_entry_amt):
                    logger.info(f"🔵 ENTRY лимит: {desired_entry_amt} по {desired_entry_price}")

        cap = self.tp_capacity()

        # Проверяем умный хвост: если позиция мала, отменяем существующие TP и активируем хвост
        # Сравниваем в USDT: позиция * цена vs order_volume
        position_usdt = abs(self.position_size) * current_price
        threshold_usdt = self.order_volume_usdt * self.tail_threshold_mult
        if position_usdt < threshold_usdt:
            if tp_orders:
                # Отменяем существующие TP ордера для активации умного хвоста
                for tp_order in tp_orders:
                    try:
                        self.exchange.cancel_order(tp_order['id'], self.symbol)
                        logger.info(f"❌ Отменён TP для умного хвоста: {tp_order['amount']} по {tp_order['price']}")
                    except Exception as e:
                        logger.warning(f"Не удалось отменить TP {tp_order.get('id')}: {e}")

            if not self.tail_active:
                self.tail_active = True
                self._persist_state(tail_active=self.tail_active)
                logger.info(f"🎯 Умный хвост активирован: позиция {position_usdt:.2f} USDT < {threshold_usdt:.2f} USDT")
        elif not tp_orders and cap > 0:
            # Обычная логика постановки TP для больших позиций
            tp_price, want = self._desired_tp_price_and_amount(current_price)
            tp_amt = self.normalize_amount(min(cap, want))
            if tp_amt > 0 and self.place_tp_reduce(tp_price, tp_amt):
                logger.info(f"🟠 TP (reduceOnly): {tp_amt} по {tp_price}")

    def after_buy_trade(self, trade_amount_base: float, current_price: float):
        self._has_traded = True;
        self._persist_state(has_traded_flag=1)
        self.update_position_info(force_wait=True)

        # Сброс умного хвоста при исполнении добавки или большой позиции
        # Сравниваем в USDT: позиция * цена vs order_volume
        position_usdt = abs(self.position_size) * current_price
        threshold_usdt = self.order_volume_usdt * self.tail_threshold_mult
        if self.tail_active and (trade_amount_base > 0 or position_usdt >= threshold_usdt):
            self.tail_active = False
            self._persist_state(tail_active=self.tail_active)
            logger.info("🎯 Умный хвост сброшен: исполнена добавка или позиция >= 1.5×order")
        if self.is_short():
            self.cancel_all_orders(side=self.entry_side())
            entry_price, entry_amt = self._desired_entry_price_and_amount(current_price)
            if entry_amt > 0: self.place_entry_limit(entry_price, entry_amt); logger.info(
                f"🔵 Новый ENTRY: {entry_amt} по {entry_price}")
            cap = self.tp_capacity()
            # Проверяем умный хвост для SHORT: если позиция мала, не ставим TP
            # Сравниваем в USDT: позиция * цена vs order_volume
            position_usdt = abs(self.position_size) * current_price
            threshold_usdt = self.order_volume_usdt * self.tail_threshold_mult
            if position_usdt < threshold_usdt:
                self.tail_active = True
                self._persist_state(tail_active=self.tail_active)
                logger.info(
                    f"🎯 Умный хвост активирован (SHORT): позиция {position_usdt:.2f} USDT < {threshold_usdt:.2f} USDT")
            else:
                tp_price, want = self._desired_tp_price_and_amount(current_price)
                tp_amt = self.normalize_amount(min(cap, want))
                if tp_amt > 0: self.place_tp_reduce(tp_price, tp_amt); logger.info(
                    f"🟠 Новый TP: {tp_amt} по {tp_price}")
        else:
            self.cancel_all_orders(side='buy')
            buys_chk, _ = self.get_active_orders()
            if self._orders_stale:
                logger.info("⏸️ Пропуск BUY после buy: список ордеров устарел.")
            elif buys_chk:
                logger.info("⏸️ Пропуск BUY после buy: BUY ещё висит.")
            else:
                bp, ba = self._desired_entry_price_and_amount(current_price)
                if ba > 0: self.place_entry_limit(bp, ba); logger.info(f"🔵 Новый BUY: {ba} по {bp}")

            cap = self.tp_capacity()
            # Проверяем умный хвост для LONG: если позиция мала, не ставим TP
            # Сравниваем в USDT: позиция * цена vs order_volume
            position_usdt = abs(self.position_size) * current_price
            threshold_usdt = self.order_volume_usdt * self.tail_threshold_mult
            if position_usdt < threshold_usdt:
                self.tail_active = True
                self._persist_state(tail_active=self.tail_active)
                logger.info(
                    f"🎯 Умный хвост активирован (LONG): позиция {position_usdt:.2f} USDT < {threshold_usdt:.2f} USDT")
            else:
                tp_price, want = self._desired_tp_price_and_amount(current_price)
                sa = self.normalize_amount(min(cap, want))
                if sa > 0:
                    self.place_tp_reduce(tp_price, sa);
                    logger.info(f"🟠 Новый SELL (reduceOnly): {sa} по {tp_price}")
                else:
                    logger.info("⛔ Пропускаем SELL после buy: нет ёмкости или объём ниже минимума")

    def after_sell_trade(self, was_highest: bool, current_price: float):
        self.update_position_info(force_wait=True)
        if self.is_flat():
            buys, sells = self.get_active_orders()
            tp_orders = buys if self.is_short() else sells
            for o in tp_orders:
                try:
                    self.exchange.cancel_order(o['id'], self.symbol)
                except Exception as e:
                    logger.warning(f"Не удалось снять TP {o.get('id')}: {e}")
            logger.info("💡 Позиция = 0 — сняли все TP-лимиты; ждём новый первичный вход.")
            self._reset_state_db_when_flat("after_tp_trade_flat");
            return

        self.cancel_all_orders(side=self.entry_side())
        buys_chk, sells_chk = self.get_active_orders()
        if self._orders_stale:
            logger.info("⏸️ Пропуск ENTRY после сделки: список ордеров устарел.")
        else:
            ep, ea = self._desired_entry_price_and_amount(current_price)
            if ea > 0: self.place_entry_limit(ep, ea); logger.info(f"🔵 Новый ENTRY: {ea} по {ep}")

        place_tp = True
        if not self.is_short():
            place_tp = bool(was_highest)

        if place_tp:
            cap = self.tp_capacity()
            if cap > 0:
                # Проверяем умный хвост: если позиция мала, не ставим TP
                # Сравниваем в USDT: позиция * цена vs order_volume
                position_usdt = abs(self.position_size) * current_price
                threshold_usdt = self.order_volume_usdt * self.tail_threshold_mult
                if position_usdt < threshold_usdt:
                    self.tail_active = True
                    self._persist_state(tail_active=self.tail_active)
                    logger.info(
                        f"🎯 Умный хвост активирован: позиция {position_usdt:.2f} USDT < {threshold_usdt:.2f} USDT")
                else:
                    sp, want = self._desired_tp_price_and_amount(current_price)
                    sa = self.normalize_amount(min(cap, want))
                    if sa > 0:
                        self.place_tp_reduce(sp, sa);
                        logger.info(f"🟠 Новый TP: {sa} по {sp}")
                    else:
                        logger.info("⛔ Пропускаем TP: объём ниже минимума")
            else:
                logger.info("⛔ Нет ёмкости для TP после сделки")
        else:
            logger.info("⏸️ Пропускаем TP: сработал промежуточный лимит (не экстремум)")

    # --------- Умный хвост ---------
    def check_executed_orders(self):
        """Проверяет исполненные ордера и обновляет статистику"""
        try:
            # Получаем недавние сделки
            trades = self.exchange.fetch_my_trades(self.symbol, limit=50)

            # Проверяем новые сделки
            for trade in trades:
                trade_id = str(trade['id'])

                # Пропускаем уже обработанные сделки
                if STORE.is_trade_processed(self.coin_symbol, trade_id):
                    continue

                # Отмечаем сделку как обработанную
                STORE.mark_trade_processed(self.coin_symbol, trade_id)

                # Определяем тип сделки
                side = trade['side']  # 'buy' или 'sell'
                amount = float(trade['amount'])
                price = float(trade['price'])

                # Проверяем если это TP (Take Profit)
                if trade.get('order', {}).get('reduceOnly', False) or (
                        side == 'sell' and self.config.trade_mode == 'LONG'
                ) or (
                        side == 'buy' and self.config.trade_mode == 'SHORT'
                ):
                    # Это TP - обновляем счетчик
                    logger.info(f"✅ TP исполнен: {side.upper()} {amount} по {price}")

                    # Увеличиваем счетчик TP
                    STORE.increment_tp_counter(self.coin_symbol, "total")
                    STORE.increment_tp_counter(self.coin_symbol, "today")
                    logger.info(
                        f"📊 TP засчитан: {self.coin_symbol} (всего: {STORE.get_tp_counter(self.coin_symbol, 'total')})")

                logger.info(f"📋 Обработана сделка: {side.upper()} {amount} по {price}")

        except Exception as e:
            logger.warning(f"Ошибка проверки исполненных ордеров: {e}")

    def handle_smart_tail(self, current_price: float):
        """Подтягивает лимит добавки при активном умном хвосте"""
        if not self.tail_active:
            return

        # Проверяем есть ли TP ордера - если есть, то хвост не активен
        buy_orders, sell_orders = self.get_active_orders()
        tp_orders = buy_orders if self.is_short() else sell_orders
        if tp_orders:
            # Есть TP - сбрасываем хвост
            self.tail_active = False
            self._persist_state(tail_active=self.tail_active)
            logger.info("🎯 Умный хвост сброшен: появился TP ордер")
            return

        # Находим текущий лимит добавки (ENTRY)
        entry_orders = sell_orders if self.is_short() else buy_orders
        if not entry_orders:
            return

        entry_order = entry_orders[0]  # Берем первый лимит
        entry_price = float(entry_order['price'])

        # Вычисляем отклонение от текущей цены
        step_size = current_price * self.step_percentage / 100.0

        if self.is_short():
            # SHORT: лимит должен быть выше цены, проверяем не ушла ли цена слишком низко
            deviation = entry_price - current_price
            if deviation >= 2 * step_size:
                # Переставляем лимит на 1 шаг выше текущей цены
                new_entry_price = current_price + step_size
                try:
                    # Отменяем старый лимит
                    self.exchange.cancel_order(entry_order['id'], self.symbol)
                    # Ставим новый
                    new_amount = float(entry_order['amount'])
                    self.place_entry_limit(new_entry_price, new_amount)
                    logger.info(
                        f"🎯 Хвост: переставили ENTRY лимит {new_amount:.4f} с {entry_price:.2f} на {new_entry_price:.2f}")
                except Exception as e:
                    logger.warning(f"Ошибка перестановки ENTRY лимита: {e}")
        else:
            # LONG: лимит должен быть ниже цены, проверяем не ушла ли цена слишком высоко
            deviation = current_price - entry_price
            if deviation >= 2 * step_size:
                # Переставляем лимит на 1 шаг ниже текущей цены
                new_entry_price = current_price - step_size
                try:
                    # Отменяем старый лимит
                    self.exchange.cancel_order(entry_order['id'], self.symbol)
                    # Ставим новый
                    new_amount = float(entry_order['amount'])
                    self.place_entry_limit(new_entry_price, new_amount)
                    logger.info(
                        f"🎯 Хвост: переставили ENTRY лимит {new_amount:.4f} с {entry_price:.2f} на {new_entry_price:.2f}")
                except Exception as e:
                    logger.warning(f"Ошибка перестановки ENTRY лимита: {e}")

    # --------- Безопасный первичный вход ---------
    def check_position_volume_and_buy(self):
        try:
            now = time.time()
            if now - self._last_valid_price_ts > 30 or now - self._last_valid_pos_ts > 30: return
            if self.is_flat() and self._flat_confirm_counter >= self._flat_confirm_required:
                if self.awaiting_primary_confirm or now - self.last_market_order_time < self.PRIMARY_COOLDOWN: return
                self.cancel_all_orders();
                time.sleep(0.5)
                order = self.place_market_sell_usdt(
                    self.first_volume_usdt) if self.is_short() else self.place_market_buy_usdt(self.first_volume_usdt)
                if order:
                    self.awaiting_primary_confirm, self._flat_confirm_counter = True, 0
                    self.update_position_info(force_wait=True)
        except Exception as e:
            logger.error(f"Ошибка первичного входа: {e}")

    # --------- Группировка недавних трейдов ---------
    def _group_recent(self, trades, side, now, exclude_market=False):
        groups = {}
        for tr in trades:
            if tr.get('side') != side: continue
            tsec = float(tr['timestamp']) / 1000.0
            if tsec <= self.min_event_ts or now - tsec > 120: continue
            amt, price = float(tr.get('amount') or 0.0), float(tr.get('price') or 0.0)
            if amt <= 0 or price <= 0: continue
            oid = str(tr.get('order') or tr.get('orderId') or tr.get('info', {}).get('orderId') or f"noid_{tr['id']}")
            if exclude_market and (self.last_market_order_id and oid == self.last_market_order_id): continue
            if exclude_market and (
                    self.last_market_order_time and abs(tsec - self.last_market_order_time) <= 7.5): continue
            g = groups.setdefault(oid, {'amount': 0.0, 'value': 0.0, 'last_ts': 0.0, 'last_price': 0.0})
            g['amount'] += amt;
            g['value'] += amt * price;
            g['last_ts'] = max(g['last_ts'], tsec);
            g['last_price'] = price
        return groups

    # --------- Проверка исполненных сделок ---------
    def check_executed_orders(self):
        try:
            trades = self.exchange.fetch_my_trades(self.symbol, limit=50)
            if not trades: return
            now = time.time()
            recent = [tr for tr in trades if tr and 'id' in tr and 'timestamp' in tr]
            if not recent: return

            base_ccy = (self.market or {}).get('base') or self.symbol.split('/')[0]
            buy_groups = self._group_recent(recent, 'buy', now, exclude_market=True)
            sell_groups = self._group_recent(recent, 'sell', now, exclude_market=True)

            current_price = self.get_current_price()

            if current_price:
                if not self.is_short():
                    for oid, g in buy_groups.items():
                        if STORE.is_buy_order_processed(self.coin_symbol, oid) or (
                                oid in self.processed_buy_orders): continue
                        self.processed_buy_orders.add(oid);
                        STORE.add_processed_buy_order(self.coin_symbol, oid, g['last_ts'] or now)

                        _, sells_now = self.get_active_orders()
                        k_pred = (len(sells_now) if isinstance(sells_now, list) else 0) + 1
                        suffix = self._adaptive_suffix_for_next_entry(k_pred)

                        total_amt = float(g['amount'])
                        avg_price = (g['value'] / g['amount']) if g['amount'] > 0 else current_price
                        self.tg_notify(
                            f"🟢 Лимитный BUY исполнен {total_amt:.6f} {base_ccy} @ ≈{avg_price:.2f} ({self.symbol}){suffix}")
                        self.after_buy_trade(trade_amount_base=total_amt, current_price=current_price)
                        self.awaiting_primary_confirm = False
                        if g['last_ts']: self.min_event_ts = max(self.min_event_ts, g['last_ts']); STORE.set_coin_state(
                            self.coin_symbol, "min_event_ts", json.dumps(self.min_event_ts))
                else:
                    for oid, g in sell_groups.items():
                        sid = f"SE_{oid}"
                        if STORE.is_trade_processed(self.coin_symbol, sid) or (sid in self.processed_trades): continue
                        self.processed_trades.add(sid);
                        STORE.mark_trade_processed(self.coin_symbol, sid)

                        buys_now, _ = self.get_active_orders()
                        k_pred = (len(buys_now) if isinstance(buys_now, list) else 0) + 1
                        suffix = self._adaptive_suffix_for_next_entry(k_pred)

                        total_amt = float(g['amount'])
                        sell_avg_price = (g['value'] / g['amount']) if g['amount'] > 0 else g['last_price']
                        self.tg_notify(
                            f"🔴 Лимитный SELL (entry) исполнен {total_amt:.6f} {base_ccy} @ ≈{sell_avg_price:.2f} ({self.symbol}){suffix}")
                        self.after_sell_trade(was_highest=False, current_price=current_price)
                        if g['last_ts']: self.min_event_ts = max(self.min_event_ts, g['last_ts']); STORE.set_coin_state(
                            self.coin_symbol, "min_event_ts", json.dumps(self.min_event_ts))

            # TP обработка - используем exclude_market=False только для противоположной стороны от entry
            if current_price:
                if not self.is_short():
                    # В LONG режиме TP = SELL, exclude_market=False для sell_groups
                    sell_groups_tp = self._group_recent(recent, 'sell', now, exclude_market=False)
                    for oid, g in sell_groups_tp.items():
                        sid = f"SO_{oid}"
                        if STORE.is_trade_processed(self.coin_symbol, sid) or (sid in self.processed_trades): continue
                        self.processed_trades.add(sid);
                        STORE.mark_trade_processed(self.coin_symbol, sid)

                        _, sells_before = self.get_active_orders()
                        k_before = len(sells_before) if isinstance(sells_before, list) else 0
                        k_pred = max(0, k_before - 1)
                        suffix = self._adaptive_suffix_for_next_entry(k_pred)

                        sell_avg_price = (g['value'] / g['amount']) if g['amount'] > 0 else g['last_price']
                        is_highest = True
                        if sells_before:
                            try:
                                is_highest = max(float(o['price']) for o in sells_before) <= sell_avg_price + 1e-9
                            except Exception:
                                pass

                        self.tg_notify(
                            f"🔴 Лимитный SELL исполнен {float(g['amount']):.6f} {base_ccy} @ ≈{sell_avg_price:.2f} ({self.symbol}){suffix}")
                        self.after_sell_trade(was_highest=is_highest, current_price=current_price)
                        if g['last_ts']: self.min_event_ts = max(self.min_event_ts, g['last_ts']); STORE.set_coin_state(
                            self.coin_symbol, "min_event_ts", json.dumps(self.min_event_ts))
                else:
                    # В SHORT режиме TP = BUY, exclude_market=False для buy_groups
                    buy_groups_tp = self._group_recent(recent, 'buy', now, exclude_market=False)
                    for oid, g in buy_groups_tp.items():
                        if STORE.is_buy_order_processed(self.coin_symbol, oid) or (
                                oid in self.processed_buy_orders): continue
                        self.processed_buy_orders.add(oid);
                        STORE.add_processed_buy_order(self.coin_symbol, oid, g['last_ts'] or now)

                        buys_before, _ = self.get_active_orders()
                        k_before = len(buys_before) if isinstance(buys_before, list) else 0
                        k_pred = max(0, k_before - 1)
                        suffix = self._adaptive_suffix_for_next_entry(k_pred)

                        buy_avg_price = (g['value'] / g['amount']) if g['amount'] > 0 else g['last_price']
                        is_lowest = True
                        if buys_before:
                            try:
                                is_lowest = min(float(o['price']) for o in buys_before) >= buy_avg_price - 1e-9
                            except Exception:
                                pass

                        self.tg_notify(
                            f"🟢 Лимитный BUY (TP) исполнен {float(g['amount']):.6f} {base_ccy} @ ≈{buy_avg_price:.2f} ({self.symbol}){suffix}")
                        self.after_sell_trade(was_highest=is_lowest, current_price=current_price)
                        if g['last_ts']: self.min_event_ts = max(self.min_event_ts, g['last_ts']); STORE.set_coin_state(
                            self.coin_symbol, "min_event_ts", json.dumps(self.min_event_ts))

        except Exception as e:
            logger.error(f"Ошибка проверки сделок: {e}")

        if len(self.processed_trades) > 1000: self.processed_trades.clear()
        if len(self.processed_buy_orders) > 1000: self.processed_buy_orders.clear()
        try:
            STORE.vacuum_processed_limits(keep_last=5000)
        except Exception:
            pass

    # --------- Управление ордерами ---------
    def manage_orders(self, current_price: float):
        if self.is_flat():
            buys, sells = self.get_active_orders()
            tp_orders = buys if self.is_short() else sells
            if tp_orders:
                logger.info("Позиция = 0 → снимаем все TP лимиты")
                if not self._orders_stale:
                    for o in tp_orders:
                        try:
                            self.exchange.cancel_order(o['id'], self.symbol)
                        except Exception as e:
                            logger.warning(f"Не удалось снять TP {o.get('id')}: {e}")
            return
        self.ensure_orders_when_position(current_price);
        self.reconcile_tp_with_position()

    # --------- Снапшот маржи / отчёт ---------
    def _fetch_equity_snapshot(self):
        try:
            info = (self.exchange.fetch_balance().get('info') or {})
            total_wallet = float(info.get('totalWalletBalance') or 0.0)
            total_unreal = float(info.get('totalUnrealizedProfit') or 0.0)
            total_margin_bal = float(info.get('totalMarginBalance') or (total_wallet + total_unreal))
            avail = float(info.get('availableBalance') or 0.0)
            init_margin = float(info.get('totalInitialMargin') or 0.0)
            maint_margin = float(info.get('totalMaintMargin') or 0.0)
            used_pct = (init_margin / total_margin_bal * 100.0) if total_margin_bal > 0 else 0.0
            return {"equity": total_margin_bal, "available": avail, "initial_margin": init_margin,
                    "maint_margin": maint_margin, "used_pct": used_pct}
        except Exception as e:
            logger.warning(f"Не удалось получить баланс/маржу: {e}")
        return {"equity": 0.0, "available": 0.0, "initial_margin": 0.0, "maint_margin": 0.0, "used_pct": 0.0}

    def _update_drawdown_tracking(self, equity: float, unrealized_pnl: float):
        """Обновление трекинга максимальной просадки в % каждые 5 минут"""
        # Рассчитываем текущую просадку: |нереализованный| / equity * 100%
        if equity > 0 and unrealized_pnl < 0:
            current_drawdown = abs(unrealized_pnl) / equity * 100.0

            # Обновляем максимальную просадку если текущая больше
            if current_drawdown > self.max_drawdown_percent:
                self.max_drawdown_percent = current_drawdown

        # Сохраняем обновленное значение
        try:
            self._persist_state(max_drawdown_percent=self.max_drawdown_percent)
        except Exception as e:
            logger.debug(f"Ошибка сохранения максимальной просадки: {e}")

    def _calculate_drawdown(self, equity: float, unrealized_pnl: float):
        """Расчет просадки: |нереализованный| / equity * 100%"""
        now = time.time()

        # Текущая просадка = |нереализованный PnL| / equity * 100%
        current_drawdown = 0.0
        if equity > 0 and unrealized_pnl < 0:
            current_drawdown = abs(unrealized_pnl) / equity * 100.0

        # Сохраняем текущий equity в историю только при отчете
        try:
            with DB_LOCK, sqlite3.connect(STORE.path) as cn:
                cn.execute(
                    "INSERT OR REPLACE INTO equity_history (coin, ts, equity, unrealized_pnl) VALUES (?, ?, ?, ?)",
                    (self.coin_symbol, now, equity, unrealized_pnl))
                cn.commit()
        except Exception as e:
            logger.warning(f"Ошибка сохранения истории equity: {e}")

        return {
            "current_drawdown": current_drawdown,
            "max_drawdown": self.max_drawdown_percent
        }

    def _format_30m_report(self) -> str:
        global GLOBAL_INITIAL_DEPOSIT
        init_dep = float(GLOBAL_INITIAL_DEPOSIT or 0.0)
        mode = str(self.config.trade_mode).upper()
        adapt = "ON" if self.config.adaptive_enabled else "OFF"
        snap = self._fetch_equity_snapshot()
        equity, avail, used_pct = float(snap["equity"]), float(snap["available"]), float(snap["used_pct"])
        pos = self.fetch_position()
        u_pnl = float(pos.get('unrealized_pnl') or 0.0) if isinstance(pos, dict) else 0.0
        delta = equity - init_dep
        delta_pct_str = f"{(delta / init_dep * 100.0):+.2f}%" if init_dep > 0 else "—"
        r_pnl = delta - u_pnl

        # Рассчитываем просадку
        drawdown_data = self._calculate_drawdown(equity, u_pnl)

        base = (self.market or {}).get('base') or self.symbol.split('/')[0]
        size = float(pos.get('size') or 0.0) if isinstance(pos, dict) else 0.0
        entry = float(pos.get('entry_price') or 0.0) if isinstance(pos, dict) else 0.0
        mark = float(pos.get('mark_price') or 0.0) if isinstance(pos, dict) else (self.last_price or 0.0)
        side_label = "Short" if self.is_short() else "Long"
        pos_line = f"• {side_label} {abs(size):.6f} {base} @ {entry:.2f} | Mark {mark:.2f} | uPnL {u_pnl:+.2f} USDT" if abs(
            size) > 0 else "• Нет открытых позиций"

        buys, sells = self.get_active_orders()
        return (
            f"⏱️ 4х часовой отчёт ({self.coin_symbol})\n"
            f"Монета: {self.symbol} | Режим: {mode} | Адаптивность: {adapt} | Плечо: {self.leverage}x | Шаг: {self.step_percentage}%\n\n"
            f"💰 Стартовый депозит: {init_dep:.2f} USDT\n"
            f"📊 Equity сейчас:     {equity:.2f} USDT\n"
            f"➕ PnL с начала:       {delta:+.2f} USDT ({delta_pct_str})\n"
            f"   ├─ Реализованный:   {r_pnl:+.2f} USDT\n"
            f"   └─ Нереализованный: {u_pnl:+.2f} USDT\n\n"
            f"📉 Просадка: текущая {drawdown_data['current_drawdown']:.1f}% | максимальная {drawdown_data['max_drawdown']:.1f}%\n\n"
            f"🧩 Позиции:\n{pos_line}\n\n"
            f"📦 Маржа: использовано {used_pct:.0f}% | Свободно {avail:.2f} USDT\n"
            f"🧷 Ордеры: BUY {len(buys)} / SELL {len(sells)}"
        )

    # --------- Применение рантайм-конфига ---------
    def _apply_runtime_config(self):
        old_lev, old_step, old_first, old_order, old_mode = self._applied_leverage, self._applied_step, self._applied_first, self._applied_order, self._applied_mode

        # Обновляем параметры из конфига монеты
        cfg_leverage = int(self.config.leverage)
        cfg_step = float(self.config.step_percentage)
        cfg_first = float(self.config.first_volume_usdt)
        cfg_order = float(self.config.order_volume_usdt)
        cfg_mode = str(self.config.trade_mode).upper()

        self.step_percentage, self.first_volume_usdt, self.order_volume_usdt = cfg_step, cfg_first, cfg_order
        self.leverage = cfg_leverage

        if cfg_step != old_step:  self._applied_step = cfg_step;  logger.info(
            f"✅ Применён Шаг, %: {old_step} → {cfg_step}")
        if cfg_first != old_first: self._applied_first = cfg_first; logger.info(
            f"✅ Применён Первичный вход, USDT: {old_first} → {cfg_first}")
        if cfg_order != old_order: self._applied_order = cfg_order; logger.info(
            f"✅ Применён Размер ордера, USDT: {old_order} → {cfg_order}")
        if cfg_mode != old_mode:  self._applied_mode = cfg_mode;  logger.info(f"✅ Применён режим: {cfg_mode}")

        if cfg_leverage != old_lev:
            try:
                self.exchange.set_leverage(cfg_leverage, self.symbol)
                self.leverage = self._applied_leverage = cfg_leverage
                logger.info(f"✅ {self.coin_symbol}: Применено новое плечо: {old_lev}x → {cfg_leverage}x")
            except Exception as e:
                logger.warning(f"{self.coin_symbol}: Не удалось обновить плечо: {e}")

    # --------- Главный цикл ---------
    def run(self):
        logger.info(f"{self.coin_symbol}: Запуск торгового бота (режим ожидания до команды «Старт»)")
        self.last_status_time, first_run = time.time(), True
        while True:
            self._run_event.wait()
            try:
                now = time.time()
                if now - self._last_tick_ts < self.TICK_INTERVAL: time.sleep(0.5); continue
                self._last_tick_ts = now

                self._apply_runtime_config()

                price = self.get_current_price()
                if not price: continue
                self.last_price = price
                self._persist_state(last_price=self.last_price, last_status_time=self.last_status_time,
                                    last_position_check=self.last_position_check, _last_tick_ts=self._last_tick_ts)

                # Проверка триггерной цены
                if not self.check_trigger_condition(price):
                    # Триггер не сработал - отправляем периодические уведомления и пропускаем торговлю
                    self.send_trigger_waiting_notification(price)
                    continue

                self.update_position_info()
                if self.position_size == 0: self._reset_state_db_when_flat("run_loop_flat")

                # Логика умного хвоста: подтягиваем лимит добавки
                self.handle_smart_tail(price)

                self.check_position_volume_and_buy()

                # Обновление максимальной просадки каждые 5 минут
                if now - self.last_drawdown_update >= 300:  # 5 минут
                    try:
                        snap = self._fetch_equity_snapshot()
                        equity = float(snap.get("equity", 0.0))
                        pos = self.fetch_position()
                        unrealized_pnl = float(pos.get('unrealized_pnl') or 0.0) if isinstance(pos, dict) else 0.0
                        global GLOBAL_INITIAL_DEPOSIT
                        init_dep = float(GLOBAL_INITIAL_DEPOSIT or 0.0)
                        total_pnl = equity - init_dep

                        self._update_drawdown_tracking(equity, unrealized_pnl)
                        logger.debug(
                            f"📉 Обновлена просадка: текущая={abs(unrealized_pnl) / equity * 100 if equity > 0 and unrealized_pnl < 0 else 0:.1f}%, макс={self.max_drawdown_percent:.1f}%")
                    except Exception as e:
                        logger.warning(f"Ошибка обновления просадки: {e}")
                    self.last_drawdown_update = now

                if now - self.last_position_check >= 300:
                    try:
                        active = []
                        for p in self.exchange.fetch_positions() or []:
                            if isinstance(p, dict):
                                size = p.get('size') or p.get('contracts') or p.get('amount') or 0
                                if abs(float(size)) > 1e-6: active.append(f"{p.get('symbol')}: {size}")
                        logger.info(
                            f"📊 Активные позиции: {', '.join(active[:5])}" if active else "📊 Активных позиций не найдено")
                    except Exception as e:
                        logger.warning(f"Ошибка принудительной проверки позиций: {e}")
                    self.last_position_check = now;
                    self._persist_state(last_position_check=self.last_position_check)

                if first_run:
                    buys, sells = self.get_active_orders()
                    logger.info(f"Старт: BUY-лимитов {len(buys)}, SELL-лимитов {len(sells)}");
                    first_run = False

                self.check_executed_orders()
                self.manage_orders(price)

                # Отправляем статус каждые 60 секунд
                if now - self.last_status_time >= 60:
                    pnl = 0.0;
                    fp = self.fetch_position()
                    if isinstance(fp, dict): pnl = fp.get('unrealized_pnl', 0.0)
                    logger.info(
                        f"📈 Статус: Цена {price}, Позиция {self.position_size}, Средняя {self.average_price}, PnL {pnl}")

                    self.last_status_time = now;
                    self._persist_state(last_status_time=self.last_status_time)

            except KeyboardInterrupt:
                logger.info("Остановка бота пользователем...");
                break
            except Exception as e:
                logger.error(f"Ошибка в основном цикле: {e}");
                time.sleep(5)


# =======================
#        ЗАПУСК
# =======================
if __name__ == "__main__":
    API_KEY = "mI3GM3UxKtlLvdPJ95xnbU1iHp6jETE5bi0l0IhuGgWApkkC1Ge6OvWNS7gRnVXC"
    API_SECRET = "bcEvVu8CduJM7Mf5NrLWDUPTGB8UYLBGyNwQcvVmCzy5dZcNwNUdzDbspwDfQlvS"

    # Загружаем глобальные настройки из БД
    try:
        global_deposit = STORE.get_config_one("global_initial_deposit")
        if global_deposit is not None:
            GLOBAL_INITIAL_DEPOSIT = float(global_deposit)
            logger.info(f"📂 Загружен глобальный депозит: {GLOBAL_INITIAL_DEPOSIT} USDT")
    except Exception as e:
        logger.warning(f"Ошибка загрузки глобального депозита: {e}")

    # Инициализируем BotManager
    BOT_MANAGER = BotManager(API_KEY, API_SECRET)

    # Загружаем монеты из БД
    BOT_MANAGER.load_coins_from_db()

    # Запускаем поток автоматических портфельных отчетов
    BOT_MANAGER.start_portfolio_reporter()

    # Запускаем всех загруженных трейдеров
    for coin_symbol, trader in BOT_MANAGER.coins_traders.items():
        try:
            trader.setup()
            Thread(target=trader.run, daemon=True).start()
            logger.info(f"🚀 {coin_symbol}: Трейдер запущен в отдельном потоке")
        except Exception as e:
            logger.error(f"❌ {coin_symbol}: Ошибка запуска трейдера: {e}")

    if TG_TOKEN:
        app = build_telegram_app()
        logger.info("🤖 Мульти-монетный Телеграм-бот запущен!")
        logger.info(f"📊 Активных монет: {len(BOT_MANAGER.get_active_coins())}")
        logger.info("💬 Отправьте /start в Telegram для начала работы")
        app.run_polling()
    else:
        logger.warning("TG_TOKEN пуст — Телеграм-бот не запущен.")
        while True: time.sleep(3600)


