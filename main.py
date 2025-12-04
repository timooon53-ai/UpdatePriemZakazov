import os
import sqlite3
import logging
import requests
import random
from datetime import datetime
from functools import wraps
from cfg import *

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    filters, ContextTypes, CallbackQueryHandler, ConversationHandler
)

TOKEN = TOKEN
ADMIN_IDS = ADMIN_IDS
SCREENSHOTS_DIR = SCREENSHOTS_DIR
DB_DIR = DB_DIR

DB_PATH = r"bd"
USERS_DB = ORDERS_DB = BANNED_DB = DB_PATH

TRANSFER_DETAILS = (os.getenv("TRANSFER_DETAILS") or locals().get("TRANSFER_DETAILS") or "2200248021994636").strip()
SBP_DETAILS = (os.getenv("SBP_DETAILS") or locals().get("SBP_DETAILS") or "+79088006072").strip()
SBP_BANK_INFO = (os.getenv("SBP_BANK_INFO") or locals().get("SBP_BANK_INFO") or "🔵 Банк ВТБ").strip()
LTC_WALLET = (
    os.getenv("LTC_WALLET")
    or locals().get("LTC_WALLET")
    or "ltc1q9l65njf0cx2jypuvhtmkkjzrf9aq7ylgfku786"
).strip()
USDT_TRC20_WALLET = (
    os.getenv("USDT_TRC20_WALLET")
    or locals().get("USDT_TRC20_WALLET")
    or "TJRe5tyJXMDp7PkUhKN97SQjpV2PR5VRR2"
).strip()
USDT_TRX_WALLET = (
    os.getenv("USDT_TRX_WALLET")
    or locals().get("USDT_TRX_WALLET")
    or "TJRe5tyJXMDp7PkUhKN97SQjpV2PR5VRR2"
).strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)

# Некоторые эндпоинты требуют именно POST-запросы, даже если параметры
# передаются через строку запроса.
FORCE_POST_ENDPOINTS = {
    "https://tc.taxi.yandex.net/3.0/launch",
    "https://tc.mobile.yandex.net/4.0/persuggest/v1/suggest?mobcf=russia%25go_ru_by_geo_hosts_2%25default&mobpr=go_ru_by_geo_hosts_2_TAXI_V4_0",
    "https://tc.mobile.yandex.net/3.0/routestats?mobcf=russia%25go_ru_by_geo_hosts_2%25default&mobpr=go_ru_by_geo_hosts_2_TAXI_0",
}


def perform_request(url: str, *, params=None, json=None, timeout: int = 10):
    """Отправить запрос с учетом требований к методу.

    Для некоторых URL метод принудительно меняется на POST, чтобы корректно
    обрабатывались запросы к API.
    """

    method = "POST" if url in FORCE_POST_ENDPOINTS else "GET"
    response = requests.request(
        method,
        url,
        params=params,
        json=json,
        timeout=timeout,
    )
    response.raise_for_status()
    return response


def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ==========================
# Инициализация БД
# ==========================
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS banned (
                tg_id INTEGER PRIMARY KEY
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                tg_id INTEGER PRIMARY KEY,
                username TEXT,
                balance REAL DEFAULT 0.00,
                orders_count INTEGER DEFAULT 0,
                coefficient REAL DEFAULT 0.55,
                city TEXT
            )
        """)

        existing_columns = {row[1] for row in c.execute("PRAGMA table_info(users)").fetchall()}
        if "city" not in existing_columns:
            c.execute("ALTER TABLE users ADD COLUMN city TEXT")

        c.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER,
                type TEXT,
                screenshot_path TEXT,
                city TEXT,
                address_from TEXT,
                address_to TEXT,
                address_extra TEXT,
                tariff TEXT,
                child_seat TEXT,
                child_seat_type TEXT,
                wishes TEXT,
                comment TEXT,
                status TEXT DEFAULT 'pending',
                amount REAL,
                base_amount REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)

        existing_columns = {row[1] for row in c.execute("PRAGMA table_info(orders)").fetchall()}
        new_columns = {
            "address_extra": "TEXT",
            "tariff": "TEXT",
            "child_seat": "TEXT",
            "child_seat_type": "TEXT",
            "wishes": "TEXT",
            "base_amount": "REAL",
        }
        for column, definition in new_columns.items():
            if column not in existing_columns:
                c.execute(f"ALTER TABLE orders ADD COLUMN {column} {definition}")

        c.execute("""
            CREATE TABLE IF NOT EXISTS orders_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER,
                tg_id INTEGER,
                order_number TEXT,
                token2 TEXT,
                card_x TEXT,
                external_id TEXT,
                link TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        info_columns = {row[1] for row in c.execute("PRAGMA table_info(orders_info)").fetchall()}
        needed_info_columns = {
            "order_number": "TEXT",
            "token2": "TEXT",
            "card_x": "TEXT",
            "external_id": "TEXT",
            "link": "TEXT",
            "is_active": "INTEGER DEFAULT 1",
            "tg_id": "INTEGER",
        }
        for column, definition in needed_info_columns.items():
            if column not in info_columns:
                c.execute(f"ALTER TABLE orders_info ADD COLUMN {column} {definition}")

        c.execute("""
            CREATE TABLE IF NOT EXISTS favorite_addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER,
                address TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        c.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('ordering_enabled', '1')"
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER,
                order_id INTEGER,
                type TEXT,
                method TEXT,
                amount REAL,
                currency TEXT,
                comment_code TEXT,
                requisites TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )

        payment_columns = {row[1] for row in c.execute("PRAGMA table_info(payments)").fetchall()}
        extra_payment_columns = {
            "original_amount": "REAL",
            "original_currency": "TEXT",
        }
        for column, definition in extra_payment_columns.items():
            if column not in payment_columns:
                c.execute(f"ALTER TABLE payments ADD COLUMN {column} {definition}")

        conn.commit()


def get_setting(key, default=None):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = c.fetchone()
        return row[0] if row else default


def set_setting(key, value):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()


def is_ordering_enabled():
    return get_setting("ordering_enabled", "1") == "1"

# ==========================
# Работа с пользователями
# ==========================
def add_user(tg_id, username):
    with sqlite3.connect(USERS_DB) as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO users (tg_id, username)
            VALUES (?, ?)
            ON CONFLICT(tg_id) DO UPDATE SET username = COALESCE(excluded.username, users.username)
            """,
            (tg_id, username),
        )
        conn.commit()

def get_user(tg_id):
    with sqlite3.connect(USERS_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,))
        row = c.fetchone()
        return dict(row) if row else None

def update_balance(tg_id, amount):
    with sqlite3.connect(USERS_DB) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET balance = balance + ? WHERE tg_id=?", (amount, tg_id))
        conn.commit()

def set_balance(tg_id, value):
    with sqlite3.connect(USERS_DB) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET balance = ? WHERE tg_id=?", (value, tg_id))
        conn.commit()

def increment_orders_count(tg_id):
    with sqlite3.connect(USERS_DB) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET orders_count = orders_count + 1 WHERE tg_id=?", (tg_id,))
        conn.commit()


def update_user_city(tg_id, city):
    with sqlite3.connect(USERS_DB) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET city=? WHERE tg_id=?", (city, tg_id))
        conn.commit()


def get_favorite_addresses(tg_id):
    with sqlite3.connect(ORDERS_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM favorite_addresses WHERE tg_id=? ORDER BY id", (tg_id,))
        return c.fetchall()


def add_favorite_address(tg_id, address):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("INSERT INTO favorite_addresses (tg_id, address) VALUES (?, ?)", (tg_id, address))
        conn.commit()
        return c.lastrowid


def update_favorite_address(fav_id, tg_id, address):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("UPDATE favorite_addresses SET address=? WHERE id=? AND tg_id=?", (address, fav_id, tg_id))
        conn.commit()


def delete_favorite_address(fav_id, tg_id):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("DELETE FROM favorite_addresses WHERE id=? AND tg_id=?", (fav_id, tg_id))
        conn.commit()


def get_all_user_ids():
    with sqlite3.connect(USERS_DB) as conn:
        c = conn.cursor()
        c.execute("SELECT tg_id FROM users")
        return [row[0] for row in c.fetchall()]


def create_payment(
    tg_id,
    method,
    amount,
    type_="topup",
    order_id=None,
    currency="RUB",
    comment_code=None,
    requisites=None,
    original_amount=None,
    original_currency="RUB",
):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO payments (
                tg_id, order_id, type, method, amount, currency, comment_code,
                requisites, original_amount, original_currency
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tg_id,
                order_id,
                type_,
                method,
                amount,
                currency,
                comment_code,
                requisites,
                original_amount if original_amount is not None else amount,
                original_currency,
            ),
        )
        conn.commit()
        return c.lastrowid


def update_payment(payment_id, **fields):
    if not fields:
        return
    fields["updated_at"] = current_timestamp()
    placeholders = ", ".join([f"{key}=?" for key in fields.keys()])
    values = list(fields.values()) + [payment_id]
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute(f"UPDATE payments SET {placeholders} WHERE id=?", values)
        conn.commit()


def get_payment(payment_id):
    with sqlite3.connect(ORDERS_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM payments WHERE id=?", (payment_id,))
        row = c.fetchone()
        return dict(row) if row else None


def get_user_orders(tg_id, limit=5):
    with sqlite3.connect(ORDERS_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT id, status, amount, base_amount, created_at FROM orders WHERE tg_id=? ORDER BY id DESC LIMIT ?",
            (tg_id, limit),
        )
        return c.fetchall()


def get_latest_user_order(tg_id):
    with sqlite3.connect(ORDERS_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT * FROM orders WHERE tg_id=? ORDER BY created_at DESC, id DESC LIMIT 1",
            (tg_id,),
        )
        row = c.fetchone()
        return dict(row) if row else None

# ==========================
# Работа с заказами
# ==========================
def create_order(
    tg_id,
    type_,
    screenshot_path=None,
    city=None,
    address_from=None,
    address_to=None,
    address_extra=None,
    tariff=None,
    child_seat=None,
    child_seat_type=None,
    wishes=None,
    comment=None,
):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO orders (
                tg_id, type, screenshot_path, city, address_from, address_to, address_extra,
                tariff, child_seat, child_seat_type, wishes, comment
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tg_id,
                type_,
                screenshot_path,
                city,
                address_from,
                address_to,
                address_extra,
                tariff,
                child_seat,
                child_seat_type,
                wishes,
                comment,
            ),
        )
        order_id = c.lastrowid
        conn.commit()
        return order_id


def get_order(order_id):
    with sqlite3.connect(ORDERS_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM orders WHERE id=?", (order_id,))
        row = c.fetchone()
        return dict(row) if row else None

def update_order_status(order_id, status):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute(
            "UPDATE orders SET status=?, updated_at=? WHERE id=?",
            (status, current_timestamp(), order_id),
        )
        conn.commit()

def update_order_amount(order_id, amount):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute(
            "UPDATE orders SET amount=?, updated_at=? WHERE id=?",
            (amount, current_timestamp(), order_id),
        )
        conn.commit()


def update_order_fields(order_id, **fields):
    if not fields:
        return
    placeholders = ", ".join([f"{key}=?" for key in fields.keys()])
    values = list(fields.values()) + [current_timestamp(), order_id]
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute(
            f"UPDATE orders SET {placeholders}, updated_at=? WHERE id=?",
            values,
        )
        conn.commit()


def create_order_info(order_id):
    order = get_order(order_id)
    tg_id = order.get("tg_id") if order else None
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO orders_info (order_id, tg_id, is_active) VALUES (?, ?, 1)",
            (order_id, tg_id),
        )
        conn.commit()
        return c.lastrowid


def get_order_info(info_id):
    with sqlite3.connect(ORDERS_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM orders_info WHERE id=?", (info_id,))
        row = c.fetchone()
        return dict(row) if row else None


def list_active_order_infos():
    with sqlite3.connect(ORDERS_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT * FROM orders_info WHERE is_active=1 ORDER BY created_at DESC, id DESC",
        )
        return [dict(row) for row in c.fetchall()]


def update_order_info_field(info_id, **fields):
    if not fields:
        return
    placeholders = ", ".join([f"{key}=?" for key in fields.keys()])
    values = list(fields.values()) + [info_id]
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute(f"UPDATE orders_info SET {placeholders} WHERE id=?", values)
        conn.commit()


def deactivate_order_info(info_id):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("UPDATE orders_info SET is_active=0 WHERE id=?", (info_id,))
        conn.commit()

# ==========================
# Декоратор проверки админа
# ==========================
def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("❌ У вас нет прав администратора")
            return
        return await func(update, context)
    return wrapper

# ==========================
# Клавиатуры
# ==========================
def main_menu_keyboard(user_id=None):
    buttons = [
        [KeyboardButton("Профиль 👤")],
        [KeyboardButton("Заказать такси 🚖")],
        [KeyboardButton("Помощь ❓")],
    ]
    if user_id in ADMIN_IDS:
        buttons.append([KeyboardButton("Админка ⚙️")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def back_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("Назад ◀️")]], resize_keyboard=True)


def profile_keyboard(has_city: bool, has_favorites: bool):
    buttons = []
    city_buttons = [InlineKeyboardButton("🏙️ Указать город" if not has_city else "✏️ Изменить город", callback_data="profile_city_set")]
    if has_city:
        city_buttons.append(InlineKeyboardButton("🗑️ Удалить город", callback_data="profile_city_clear"))
    buttons.append(city_buttons)

    fav_row = [InlineKeyboardButton("⭐ Любимые адреса", callback_data="profile_fav_manage")]
    buttons.append(fav_row)
    buttons.append([InlineKeyboardButton("💳 Пополнить баланс", callback_data="profile_topup")])
    buttons.append([InlineKeyboardButton("🔙 В главное меню", callback_data="profile_back")])
    return InlineKeyboardMarkup(buttons)


def favorites_manage_keyboard(favorites):
    buttons = []
    for fav in favorites:
        buttons.append([
            InlineKeyboardButton(f"✏️ {fav['address']}", callback_data=f"profile_fav_edit_{fav['id']}"),
            InlineKeyboardButton("🗑️", callback_data=f"profile_fav_delete_{fav['id']}")
        ])
    if len(favorites) < 3:
        buttons.append([InlineKeyboardButton("➕ Добавить адрес", callback_data="profile_fav_add")])
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="profile_fav_back")])
    return InlineKeyboardMarkup(buttons)


def favorites_select_keyboard(favorites, stage):
    buttons = []
    for fav in favorites:
        buttons.append([InlineKeyboardButton(fav['address'], callback_data=f"fav_{stage}_{fav['id']}")])
    buttons.append([InlineKeyboardButton("📝 Ввести новый", callback_data=f"fav_{stage}_manual")])
    return InlineKeyboardMarkup(buttons)

def order_type_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Отправить скриншотом 🖼️", callback_data="order_screenshot")],
        [InlineKeyboardButton("Отправить текстом 📝", callback_data="order_text")],
        [InlineKeyboardButton("Назад ◀️", callback_data="order_back")]
    ])


def set_active_token2(token2: str, tg_id=None):
    token2 = (token2 or "").strip()
    if not token2:
        return

    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("UPDATE orders_info SET is_active=0 WHERE is_active=1")
        c.execute(
            """
            INSERT INTO orders_info (order_id, tg_id, order_number, token2, is_active)
            VALUES (NULL, ?, 'USER', ?, 1)
            """,
            (tg_id, token2),
        )
        conn.commit()


def yes_no_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Добавить", callback_data="address_yes"),
            InlineKeyboardButton("🚫 Пропустить", callback_data="address_no"),
        ]
    ])


def tariff_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Эконом 💸", callback_data="tariff_Эконом"), InlineKeyboardButton("Комфорт 😊", callback_data="tariff_Комфорт")],
        [InlineKeyboardButton("Комфорт+ ✨", callback_data="tariff_Комфорт+"), InlineKeyboardButton("Бизнес 💼", callback_data="tariff_Бизнес")],
        [InlineKeyboardButton("Премьер 👑", callback_data="tariff_Премьер"), InlineKeyboardButton("Элит 🏆", callback_data="tariff_Элит")],
    ])


def child_seat_type_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛄 Свое", callback_data="seat_type_Свое")],
        [InlineKeyboardButton("👶 9м - 4л", callback_data="seat_type_9м-4л")],
        [InlineKeyboardButton("🧒 3-7л", callback_data="seat_type_3-7л")],
        [InlineKeyboardButton("👦 6-12л", callback_data="seat_type_6-12л")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="seat_type_exit")],
    ])


def additional_options_keyboard(order_data):
    selected_wishes = set(order_data.get("wishes", []))
    child_seat = order_data.get("child_seat")
    child_seat_type = order_data.get("child_seat_type")

    def mark(text, active):
        return f"{'✅' if active else '⬜️'} {text}"

    child_selected = child_seat is not None and child_seat != "Не требуется"
    child_label = "Детское кресло"
    if child_selected:
        detail = child_seat_type or child_seat
        child_label = f"{child_label} ({detail})"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(mark(child_label, child_selected), callback_data="additional_child")],
        [InlineKeyboardButton(mark("Перевозка животных 🐾", "Перевозка животных" in selected_wishes), callback_data="additional_animals")],
        [InlineKeyboardButton(mark("Буду с инвалидным креслом ♿", "Буду с инвалидным креслом" in selected_wishes), callback_data="additional_wheelchair")],
        [InlineKeyboardButton("✅ Готово", callback_data="additional_done"), InlineKeyboardButton("⏭️ Пропустить", callback_data="additional_skip")],
    ])


def replacement_fields_keyboard(info):
    def mark(value, label):
        return f"{'✅' if value else '➕'} {label}"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(mark(info.get("order_number"), "OrderID"), callback_data=f"replacement_field_orderid_{info['id']}")],
        [InlineKeyboardButton(mark(info.get("card_x"), "card-x"), callback_data=f"replacement_field_cardx_{info['id']}")],
        [InlineKeyboardButton(mark(info.get("external_id"), "ID"), callback_data=f"replacement_field_extid_{info['id']}")],
        [InlineKeyboardButton(mark(info.get("token2"), "token2"), callback_data=f"replacement_field_token2_{info['id']}")],
        [InlineKeyboardButton(mark(info.get("link"), "Ссылка"), callback_data=f"replacement_field_link_{info['id']}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="replacement_back")],
    ])


def replacement_list_keyboard(infos):
    buttons = []
    for info in infos:
        label = f"{info.get('created_at', '')}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"replacement_view_{info['id']}")])
    buttons.append([InlineKeyboardButton("⬅️ В админку", callback_data="replacement_back")])
    return InlineKeyboardMarkup(buttons)


def format_mono(text: str) -> str:
    return f"<code>{text}</code>"


def payment_methods_keyboard(prefix: str, order_id: int | None = None):
    base = prefix
    if order_id is not None:
        base = f"{prefix}{order_id}_"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("💳 Карта", callback_data=f"{base}transfer")],
            [InlineKeyboardButton("💸 СБП", callback_data=f"{base}sbp")],
            [InlineKeyboardButton("🪙 Litecoin", callback_data=f"{base}ltc")],
            [InlineKeyboardButton("💵 USDT (TRC20)", callback_data=f"{base}usdt_trc20")],
            [InlineKeyboardButton("💵 USDT (TRX)", callback_data=f"{base}usdt_trx")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="profile_back")],
        ]
    )

def admin_order_buttons(order_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Взял в работу ✅", callback_data=f"take_{order_id}"),
         InlineKeyboardButton("Отклонить ❌", callback_data=f"reject_{order_id}")]
    ])

def admin_in_progress_buttons(order_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Поиск такси 🔍", callback_data=f"search_{order_id}"),
         InlineKeyboardButton("Отменить заказ ❎", callback_data=f"cancel_{order_id}")]
    ])

def admin_search_buttons(order_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Связаться с заказчиком 💬", callback_data=f"chat_{order_id}")],
        [InlineKeyboardButton("Нашлась машина 🚘", callback_data=f"found_{order_id}"),
         InlineKeyboardButton("Отменить поиск ⏹", callback_data=f"cancelsearch_{order_id}")]
    ])


def payment_choice_keyboard(order_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Карта", callback_data=f"pay_card_{order_id}")],
        [InlineKeyboardButton("💰 Баланс", callback_data=f"pay_balance_{order_id}")],
    ])


def admin_panel_keyboard():
    ordering_enabled = is_ordering_enabled()
    ordering_label = "⏹️ Остановить приём заказов" if ordering_enabled else "▶️ Включить приём заказов"
    status_text = "✅ Заказы включены" if ordering_enabled else "🚧 Заказы выключены"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Баланс пользователя", callback_data="admin_balance")],
        [InlineKeyboardButton("📦 Заказы пользователя", callback_data="admin_orders")],
        [InlineKeyboardButton("🔁 Обновить информацию", callback_data="admin_refresh")],
        [InlineKeyboardButton("📢 Рассылка по всем", callback_data="admin_broadcast")],
        [InlineKeyboardButton("🔄 Заказы для подмены", callback_data="admin_replacements")],
        [InlineKeyboardButton(ordering_label, callback_data="admin_toggle")],
        [InlineKeyboardButton(status_text, callback_data="admin_status")],
    ])


async def admin_show_panel(target):
    await target.reply_text("⚙️ Админ-панель", reply_markup=admin_panel_keyboard())

# ==========================
# Обработчики команд
# ==========================
def not_banned(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        with sqlite3.connect(BANNED_DB) as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM banned WHERE tg_id=?", (tg_id,))
            if c.fetchone():
                await update.message.reply_text("❌ Вы заблокированы и не можете использовать бота.")
                return
        return await func(update, context)
    return wrapper


@not_banned
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    add_user(user.id, user.username)
    await update.message.reply_text(
        f"Привет, @{user.username or 'не указан'}! Добро пожаловать в сервис заказа такси 🚖",
        reply_markup=main_menu_keyboard(user.id)
    )


async def start_over(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбросить любой сценарий и показать стартовое меню."""
    context.user_data.clear()
    return await start(update, context) or ConversationHandler.END

async def send_profile_info(target, user_id, context):
    user = get_user(user_id)
    if not user:
        await target.reply_text("Пользователь не найден.")
        return False
    context.user_data.pop("awaiting_city", None)
    context.user_data.pop("awaiting_fav_action", None)
    context.user_data.pop("fav_edit_id", None)

    username = user["username"]
    balance = user["balance"]
    orders_count = user["orders_count"]
    coefficient = user["coefficient"]
    city = user["city"]

    favorites = get_favorite_addresses(user_id)
    favorites_text = "\n".join([f"{idx + 1}. {fav['address']}" for idx, fav in enumerate(favorites)]) or "—"

    text = (
        f"👤 Профиль\n"
        f"Username: @{username or 'не указан'}\n"
        f"Telegram ID: {user_id}\n"
        f"Баланс: {balance:.2f} ₽\n"
        f"Заказано поездок: {orders_count}\n"
        f"Коэффициент: {coefficient:.2f}\n"
        f"Город: {city or 'не указан'}\n"
        f"Любимые адреса:\n{favorites_text}"
    )
    await target.reply_text(
        text,
        reply_markup=profile_keyboard(bool(city), bool(favorites)),
    )
    return True


async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await send_profile_info(update.message, user_id, context)

async def help_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🆘 Помощь по сервису\n"
        "1. Для заказа такси нажмите «Заказать такси 🚖».\n"
        "2. Вы можете отправить заказ скриншотом или текстом.\n"
        "3. Статус заказа отслеживается через уведомления.\n"
        "4. Баланс пополняется через администратора.\n"
        "5. При проблемах — пишите @MikeWazovsk1y"
    )
    await update.message.reply_text(text, reply_markup=back_keyboard())


def generate_comment():
    return str(random.randint(10**7, 10**10 - 1))


def fetch_crypto_rate(method: str):
    mapping = {
        "ltc": "litecoin",
        "usdt_trc20": "tether",
        "usdt_trx": "tron",
    }
    coin_id = mapping.get(method)
    if not coin_id:
        return None
    try:
        resp = perform_request(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": coin_id, "vs_currencies": "rub"},
            timeout=10,
        )
        data = resp.json()
        return float(data.get(coin_id, {}).get("rub") or 0) or None
    except Exception as e:
        logger.error(f"Не удалось получить курс {method}: {e}")
        return None


def payment_requisites(method: str):
    mapping = {
        "transfer": TRANSFER_DETAILS,
        "sbp": SBP_DETAILS,
        "ltc": LTC_WALLET,
        "usdt_trc20": USDT_TRC20_WALLET,
        "usdt_trx": USDT_TRX_WALLET,
    }
    return mapping.get(method, "Реквизиты уточните у оператора")


async def build_and_send_payment(user_id: int, method: str, amount: float | None, context: ContextTypes.DEFAULT_TYPE, target, type_="topup", order_id=None):
    comment_code = None if method in {"ltc", "usdt_trc20", "usdt_trx"} else generate_comment()
    raw_requisites = payment_requisites(method)
    display_requisites = raw_requisites
    if method == "sbp":
        display_requisites = f"{raw_requisites}\n{SBP_BANK_INFO}"
    currency = "LTC" if method == "ltc" else ("USDT" if method.startswith("usdt") else "RUB")
    original_amount = amount
    original_currency = "RUB"

    rate_text = None
    if method in {"ltc", "usdt_trc20", "usdt_trx"}:
        rate = fetch_crypto_rate(method)
        if rate:
            rate_text = f"📈 Курс: 1 {currency} = {rate:.2f} ₽"
            if amount is not None:
                converted = round(amount / rate, 4)
                amount = converted
        else:
            rate_text = "⚠️ Курс недоступен, используйте рублёвый эквивалент"

    payment_id = create_payment(
        tg_id=user_id,
        method=method,
        amount=amount,
        type_=type_,
        order_id=order_id,
        currency=currency,
        comment_code=comment_code,
        requisites=display_requisites,
        original_amount=original_amount,
        original_currency=original_currency,
    )

    method_titles = {
        "transfer": "💳 Карта",
        "sbp": "💸 СБП",
        "ltc": "🪙 Litecoin",
        "usdt_trc20": "💵 USDT (TRC20)",
        "usdt_trx": "💵 USDT (TRX)",
    }
    parts = [
        "💰 Детали оплаты:",
        f"Метод: {method_titles.get(method, method)}",
    ]
    if amount is None:
        parts.append(f"Сумма: укажите при переводе в {currency}")
    else:
        parts.append(
            f"Сумма: {amount:.4f} {currency}" if currency != "RUB" else f"Сумма: {amount:.2f} {currency}"
        )
    requisites_text = format_mono(raw_requisites)
    if method == "sbp":
        requisites_text = f"{requisites_text}\n{SBP_BANK_INFO}"

    parts.append(f"Реквизиты: {requisites_text}")
    if currency != "RUB" and original_amount is not None:
        parts.append(f"💵 Эквивалент: {original_amount:.2f} {original_currency}")
    if rate_text:
        parts.append(rate_text)
    if comment_code:
        parts.append(f"Комментарий к переводу: {format_mono(comment_code)}")
    if type_ == "topup":
        parts.append("После оплаты нажмите кнопку ниже.")
    else:
        parts.append("После оплаты сообщите об оплате ниже, мы проверим и подтвердим заказ.")

    buttons = [[InlineKeyboardButton("✅ Оплатил", callback_data=f"payment_paid_{payment_id}")]]
    if type_ != "topup":
        buttons.append([InlineKeyboardButton("🔍 Проверить оплату", callback_data=f"payment_check_{payment_id}")])
    buttons.append([InlineKeyboardButton("❌ Отменить", callback_data=f"payment_cancel_{payment_id}")])
    keyboard = InlineKeyboardMarkup(buttons)
    await target.reply_text("\n".join(parts), reply_markup=keyboard, parse_mode="HTML")
    return payment_id


async def profile_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    if data == "profile_city_set":
        context.user_data["awaiting_city"] = True
        await query.message.reply_text("🏙️ Введите ваш город:")
    elif data == "profile_city_clear":
        update_user_city(user_id, None)
        await query.message.reply_text("🗑️ Город удалён")
        await send_profile_info(query.message, user_id, context)
    elif data == "profile_back":
        await query.message.reply_text(
            "Возврат в главное меню", reply_markup=main_menu_keyboard(user_id)
        )
    elif data == "profile_fav_manage":
        favorites = get_favorite_addresses(user_id)
        await query.message.reply_text(
            "⭐ Любимые адреса",
            reply_markup=favorites_manage_keyboard(favorites),
        )
    elif data == "profile_fav_add":
        favorites = get_favorite_addresses(user_id)
        if len(favorites) >= 3:
            await query.answer("Можно сохранить не более 3 адресов", show_alert=True)
            return
        context.user_data["awaiting_fav_action"] = "add"
        await query.message.reply_text("➕ Пришлите адрес, который хотите добавить в избранное")
    elif data.startswith("profile_fav_edit_"):
        fav_id = int(data.rsplit("_", 1)[1])
        context.user_data["awaiting_fav_action"] = "edit"
        context.user_data["fav_edit_id"] = fav_id
        await query.message.reply_text("✏️ Пришлите новый вариант адреса")
    elif data.startswith("profile_fav_delete_"):
        fav_id = int(data.rsplit("_", 1)[1])
        delete_favorite_address(fav_id, user_id)
        await query.message.reply_text("🗑️ Адрес удалён")
        favorites = get_favorite_addresses(user_id)
        await query.message.reply_text(
            "⭐ Любимые адреса",
            reply_markup=favorites_manage_keyboard(favorites),
        )
    elif data == "profile_fav_back":
        await send_profile_info(query.message, user_id, context)
    elif data == "profile_topup":
        await query.message.reply_text(
            "💳 Выберите способ пополнения", reply_markup=payment_methods_keyboard("topup_")
        )


async def topup_method_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    method = data.split("_", 1)[1]
    user = get_user(query.from_user.id)
    if not user:
        await query.message.reply_text("Пользователь не найден")
        return ConversationHandler.END

    balance = user.get("balance", 0)
    if balance < 0:
        amount = abs(balance)
        await query.message.reply_text(
            f"Ваш баланс отрицательный ({balance:.2f} ₽). Сумма к оплате: {amount:.2f} ₽"
        )
        await build_and_send_payment(query.from_user.id, method, amount, context, query.message)
        return ConversationHandler.END

    if method in {"ltc", "usdt_trc20", "usdt_trx"}:
        await build_and_send_payment(query.from_user.id, method, None, context, query.message)
        return ConversationHandler.END

    context.user_data["topup_method"] = method
    await query.message.reply_text("Введите сумму пополнения (от 100 ₽):")
    return WAIT_TOPUP_AMOUNT


async def order_payment_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, order_id, method = query.data.split("_", 2)
    order_id = int(order_id)
    order = get_order(order_id)
    if not order:
        await query.message.reply_text("Заказ не найден")
        return ConversationHandler.END
    amount = order.get("amount") or order.get("base_amount") or 0
    if not amount:
        await query.message.reply_text("Сумма заказа не указана, обратитесь к оператору")
        return ConversationHandler.END
    await query.message.reply_text(
        f"К оплате за заказ №{order_id}: {amount:.2f} ₽"
    )
    await build_and_send_payment(query.from_user.id, method, amount, context, query.message, type_="order", order_id=order_id)
    return ConversationHandler.END


# ==========================
# Conversation States
# ==========================
(
    WAIT_SCREENSHOT,
    WAIT_CITY,
    WAIT_ADDRESS_FROM,
    WAIT_ADDRESS_TO,
    WAIT_ADDRESS_THIRD_DECISION,
    WAIT_ADDRESS_THIRD,
    WAIT_TARIFF,
    WAIT_ADDITIONAL,
    WAIT_CHILD_SEAT_TYPE,
    WAIT_COMMENT,
    WAIT_CONFIRMATION,
    WAIT_REPLACEMENT_FIELD,
    WAIT_ADMIN_MESSAGE,
    WAIT_ADMIN_SUM,
    WAIT_ADMIN_BALANCE,
    WAIT_ADMIN_BALANCE_UPDATE,
    WAIT_ADMIN_ORDERS,
    WAIT_ADMIN_BROADCAST,
    WAIT_TOPUP_AMOUNT,
    WAIT_PAYMENT_PROOF,
    WAIT_ADMIN_TOPUP_AMOUNT,
) = range(21)

# ==========================
# Пользовательский сценарий заказа
# ==========================
async def order_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_ordering_enabled():
        await update.message.reply_text(
            "⚙️ Заказ такси временно недоступен. Бот на технических работах, попробуйте позже.",
            reply_markup=main_menu_keyboard(update.effective_user.id),
        )
        return
    await update.message.reply_text("Выберите способ заказа:", reply_markup=order_type_keyboard())
async def order_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_ordering_enabled():
        await query.message.reply_text(
            "⚙️ Заказ такси временно недоступен. Бот на технических работах, попробуйте позже.",
            reply_markup=main_menu_keyboard(query.from_user.id),
        )
        return ConversationHandler.END
    data = query.data
    context.user_data.clear()
    context.user_data['order_data'] = {}
    if data == "order_screenshot":
        await query.message.reply_text("Пришлите скриншот маршрута 📎")
        return WAIT_SCREENSHOT
    elif data == "order_text":
        context.user_data['order_type'] = "text"
        saved_user = get_user(query.from_user.id)
        if saved_user and saved_user.get("city"):
            context.user_data.setdefault('order_data', {})['city'] = saved_user.get("city")
            await ask_address_from(query, context)
            return WAIT_ADDRESS_FROM
        await query.message.reply_text(
            "Введите город 🏙️",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="order_back")]]),
        )
        return WAIT_CITY
    elif data == "order_back":
        await query.message.reply_text(
            "Возврат в главное меню",
            reply_markup=main_menu_keyboard(query.from_user.id),
        )
        return ConversationHandler.END

# ---- Клавиатура "Пропустить" ----
def skip_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("Пропустить ➡️")]], resize_keyboard=True)

# ---- Скриншотный заказ (приём фото) ----
async def screenshot_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    photo = update.message.photo[-1]
    file = await photo.get_file()
    tg_id = update.effective_user.id

    order_id = create_order(tg_id, type_="screenshot")
    path = os.path.join(SCREENSHOTS_DIR, f"{order_id}.jpg")
    await file.download_to_drive(path)

    update_order_fields(order_id, screenshot_path=path)
    context.user_data['order_id'] = order_id
    context.user_data['order_type'] = "screenshot"
    context.user_data['order_data'] = {}

    await update.message.reply_text("Выберите тариф 🚕", reply_markup=tariff_keyboard())
    return WAIT_TARIFF

# ---- Текстовый заказ: последовательность шагов ----
async def ask_address_from(update_or_query, context):
    user_id = update_or_query.effective_user.id if isinstance(update_or_query, Update) else update_or_query.from_user.id
    favorites = get_favorite_addresses(user_id)
    target = update_or_query.message
    if favorites:
        await target.reply_text("Адрес откуда 📍", reply_markup=favorites_select_keyboard(favorites, "from"))
    else:
        await target.reply_text("Адрес откуда 📍")


async def ask_address_to(update_or_query, context):
    user_id = update_or_query.effective_user.id if isinstance(update_or_query, Update) else update_or_query.from_user.id
    favorites = get_favorite_addresses(user_id)
    target = update_or_query.message
    if favorites:
        await target.reply_text("Адрес куда 📍", reply_markup=favorites_select_keyboard(favorites, "to"))
    else:
        await target.reply_text("Адрес куда 📍")


async def ask_address_third(update_or_query, context):
    user_id = update_or_query.effective_user.id if isinstance(update_or_query, Update) else update_or_query.from_user.id
    favorites = get_favorite_addresses(user_id)
    target = update_or_query.message
    if favorites:
        await target.reply_text("Введите третий адрес 🧭", reply_markup=favorites_select_keyboard(favorites, "third"))
    else:
        await target.reply_text("Введите третий адрес 🧭")


async def text_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    city = update.message.text
    context.user_data.setdefault('order_data', {})['city'] = city
    update_user_city(update.effective_user.id, city)
    await ask_address_from(update, context)
    return WAIT_ADDRESS_FROM


async def topup_amount_entered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.replace(",", "."))
    except ValueError:
        await update.message.reply_text("Введите число в рублях")
        return WAIT_TOPUP_AMOUNT
    if amount < 100:
        await update.message.reply_text("Минимальная сумма 100 ₽")
        return WAIT_TOPUP_AMOUNT
    method = context.user_data.get("topup_method")
    if not method:
        await update.message.reply_text("Способ пополнения не выбран")
        return ConversationHandler.END
    await build_and_send_payment(update.effective_user.id, method, amount, context, update.message)
    context.user_data.pop("topup_method", None)
    return ConversationHandler.END

async def text_address_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault('order_data', {})['address_from'] = update.message.text
    await ask_address_to(update, context)
    return WAIT_ADDRESS_TO

async def text_address_to(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault('order_data', {})['address_to'] = update.message.text
    await update.message.reply_text("Хотите добавить ещё один адрес?", reply_markup=yes_no_keyboard())
    return WAIT_ADDRESS_THIRD_DECISION


async def text_address_third(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault('order_data', {})['address_extra'] = update.message.text
    await ask_tariff(update, context)
    return WAIT_TARIFF


async def address_third_decision(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "address_yes":
        await ask_address_third(query, context)
        return WAIT_ADDRESS_THIRD
    await ask_tariff(query, context)
    return WAIT_TARIFF


async def ask_tariff(update_or_query, context):
    if isinstance(update_or_query, Update):
        target = update_or_query.message
    else:
        target = update_or_query.message
    await target.reply_text("Выберите тариф 🚕", reply_markup=tariff_keyboard())


async def favorite_address_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, stage, payload = query.data.split("_", 2)
    user_id = query.from_user.id
    favorites = {fav['id']: fav for fav in get_favorite_addresses(user_id)}

    if payload == "manual":
        prompt = {
            "from": "Введите адрес откуда 📍",
            "to": "Введите адрес куда 📍",
            "third": "Введите третий адрес 🧭",
        }.get(stage, "Введите адрес")
        await query.message.reply_text(prompt)
        return {
            "from": WAIT_ADDRESS_FROM,
            "to": WAIT_ADDRESS_TO,
            "third": WAIT_ADDRESS_THIRD,
        }.get(stage, ConversationHandler.END)

    fav_id = int(payload)
    fav = favorites.get(fav_id)
    if not fav:
        await query.answer("Адрес не найден", show_alert=True)
        return ConversationHandler.END

    data = context.user_data.setdefault('order_data', {})
    if stage == "from":
        data['address_from'] = fav['address']
        await query.message.reply_text(f"Адрес откуда выбран: {fav['address']}")
        await ask_address_to(query, context)
        return WAIT_ADDRESS_TO
    if stage == "to":
        data['address_to'] = fav['address']
        await query.message.reply_text(f"Адрес куда выбран: {fav['address']}")
        await query.message.reply_text("Хотите добавить ещё один адрес?", reply_markup=yes_no_keyboard())
        return WAIT_ADDRESS_THIRD_DECISION
    if stage == "third":
        data['address_extra'] = fav['address']
        await ask_tariff(query, context)
        return WAIT_TARIFF
    return ConversationHandler.END

async def text_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comment = update.message.text
    if comment and comment.lower() == "пропустить ➡️":
        comment = None

    order_type = context.user_data.get('order_type')
    data = context.user_data.get('order_data', {})
    data['comment'] = comment

    if order_type == "text":
        city = data.get('city')
        addr_from = data.get('address_from')
        addr_to = data.get('address_to')

        if not (city and addr_from and addr_to):
            await update.message.reply_text(
                "Ошибка: не все поля введены. Попробуйте снова.",
                reply_markup=main_menu_keyboard(update.effective_user.id),
            )
            context.user_data.clear()
            return ConversationHandler.END


        order_id = create_order(
            tg_id=update.effective_user.id,
            type_="text",
            city=city,
            address_from=addr_from,
            address_to=addr_to,
            address_extra=data.get('address_extra'),
            tariff=data.get('tariff'),
            child_seat=data.get('child_seat'),
            child_seat_type=data.get('child_seat_type'),
            wishes=", ".join(data.get('wishes', [])) if data.get('wishes') else None,
            comment=comment,
        )

    else:
        order_id = context.user_data.get('order_id')
        if not order_id:
            await update.message.reply_text("Произошла ошибка: заказ не найден.")
            return ConversationHandler.END
        update_order_fields(
            order_id,
            tariff=data.get('tariff'),
            child_seat=data.get('child_seat'),
            child_seat_type=data.get('child_seat_type'),
            wishes=", ".join(data.get('wishes', [])) if data.get('wishes') else None,
            comment=comment,
        )

    context.user_data['order_id'] = order_id
    await send_order_preview(update.message, order_id)

    return WAIT_CONFIRMATION


async def send_order_preview(target, order_id: int):
    order = get_order(order_id)
    if not order:
        await target.reply_text("❌ Не удалось сформировать предварительный просмотр заказа.")
        return ConversationHandler.END

    cost_source = order.get("amount") or order.get("base_amount")
    cost_text = f"{cost_source:.2f} ₽" if cost_source else "Будет рассчитана оператором"

    parts = [
        f"Предварительный просмотр заказа №{order_id}",
        f"Тариф: {order.get('tariff') or 'не выбран'}",
        f"Стоимость: {cost_text}",
    ]

    if order.get("city"):
        parts.append(f"Город: {order.get('city')}")
    if order.get("address_from"):
        parts.append(f"Откуда: {order.get('address_from')}")
    if order.get("address_to"):
        parts.append(f"Куда: {order.get('address_to')}")
    if order.get("address_extra"):
        parts.append(f"Доп. адрес: {order.get('address_extra')}")
    if order.get("child_seat"):
        parts.append(f"Детское кресло: {order.get('child_seat')}")
    if order.get("child_seat_type"):
        parts.append(f"Тип кресла: {order.get('child_seat_type')}")
    if order.get("wishes"):
        parts.append(f"Пожелания: {order.get('wishes')}")
    if order.get("comment"):
        parts.append(f"Комментарий: {order.get('comment')}")

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("❌ Отменить", callback_data=f"order_cancel_{order_id}"),
                InlineKeyboardButton("✅ Подтвердить заказ", callback_data=f"order_confirm_{order_id}"),
            ]
        ]
    )

    await target.reply_text("\n".join(parts), reply_markup=keyboard)


async def tariff_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tariff = query.data.split("_", 1)[1]
    context.user_data.setdefault('order_data', {})['tariff'] = tariff
    await query.message.reply_text(
        "Выберите доп. опции по необходимости",
        reply_markup=additional_options_keyboard(context.user_data.get('order_data', {})),
    )
    return WAIT_ADDITIONAL


async def order_preview_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    _, action, order_id_raw = query.data.split("_", 2)
    order_id = int(order_id_raw)
    order = get_order(order_id)

    if not order or order.get("tg_id") != query.from_user.id:
        await query.answer("Заказ не найден", show_alert=True)
        return ConversationHandler.END

    if action == "cancel":
        update_order_status(order_id, "canceled")
        await query.edit_message_text(f"🚫 Заказ №{order_id} отменён.")
        await query.message.reply_text(
            "Возвращаем вас в главное меню.",
            reply_markup=main_menu_keyboard(query.from_user.id),
        )
        context.user_data.clear()
        return ConversationHandler.END

    if order.get("status") == "canceled":
        await query.answer("Заказ уже отменён", show_alert=True)
        return ConversationHandler.END

    increment_orders_count(query.from_user.id)
    await query.edit_message_text(
        f"✅ Ваш заказ №{order_id} отправлен операторам.",
    )
    await query.message.reply_text(
        "Мы уведомили операторов. Ожидайте, пожалуйста!",
        reply_markup=main_menu_keyboard(query.from_user.id),
    )
    await notify_admins(context, order_id)
    context.user_data.clear()
    return ConversationHandler.END


async def child_seat_type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "seat_type_exit":
        await query.edit_message_text(
            "Выберите доп. опции по необходимости",
            reply_markup=additional_options_keyboard(context.user_data.get('order_data', {})),
        )
        return WAIT_ADDITIONAL

    seat_type = data.split("_", 2)[2]
    order_data = context.user_data.setdefault('order_data', {})
    order_data['child_seat'] = "Нужно"
    order_data['child_seat_type'] = seat_type
    await query.edit_message_text(
        "Выберите доп. опции по необходимости",
        reply_markup=additional_options_keyboard(order_data),
    )
    return WAIT_ADDITIONAL

async def additional_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    order_data = context.user_data.setdefault('order_data', {})
    current_wishes = set(order_data.get('wishes', []))

    if data == "additional_child":
        await query.edit_message_text(
            "Выберите тип детского кресла",
            reply_markup=child_seat_type_keyboard(),
        )
        return WAIT_CHILD_SEAT_TYPE

    if data in {"additional_animals", "additional_wheelchair"}:
        label = "Перевозка животных" if data == "additional_animals" else "Буду с инвалидным креслом"
        if label in current_wishes:
            current_wishes.remove(label)
        else:
            current_wishes.add(label)
        order_data['wishes'] = list(current_wishes)
        await query.edit_message_reply_markup(reply_markup=additional_options_keyboard(order_data))
        return WAIT_ADDITIONAL

    if data in {"additional_done", "additional_skip"}:
        await query.message.reply_text(
            "Добавьте комментарий для админа или нажмите «Пропустить ➡️»",
            reply_markup=skip_keyboard(),
        )
        return WAIT_COMMENT

    return WAIT_ADDITIONAL


# ==========================
# Админ уведомление
# ==========================
async def notify_admins(context, order_id):
    order = get_order(order_id)

    if not order:
        return

    tg_id = order.get("tg_id")
    type_ = order.get("type")
    user_info = get_user(tg_id)
    username = user_info.get("username") if user_info else None
    username_label = f"@{username}" if username else "не указан"

    parts = [
        f"НОВЫЙ ЗАКАЗ №{order_id}",
        f"Тип: {type_}",
        f"Пользователь: {username_label} (ID: {tg_id})",
    ]
    if order.get("city"):
        parts.append(f"Город: {order.get('city')}")
    if order.get("address_from"):
        parts.append(f"Откуда: {order.get('address_from')}")
    if order.get("address_to"):
        parts.append(f"Куда: {order.get('address_to')}")
    if order.get("address_extra"):
        parts.append(f"Доп. адрес: {order.get('address_extra')}")
    if order.get("tariff"):
        parts.append(f"Тариф: {order.get('tariff')}")
    if order.get("child_seat"):
        parts.append(f"Детское кресло: {order.get('child_seat')}")
    if order.get("child_seat_type"):
        parts.append(f"Тип кресла: {order.get('child_seat_type')}")
    if order.get("wishes"):
        parts.append(f"Пожелания: {order.get('wishes')}")
    if order.get("comment"):
        parts.append(f"Комментарий: {order.get('comment')}")

    text = "\n".join(parts)

    for admin_id in ADMIN_IDS:
        try:
            if order.get("screenshot_path"):
                with open(order.get("screenshot_path"), "rb") as photo:
                    await context.bot.send_photo(admin_id, photo=photo, caption=text, reply_markup=admin_order_buttons(order_id))
            else:
                await context.bot.send_message(admin_id, text, reply_markup=admin_order_buttons(order_id))
        except Exception as e:
            logger.error(f"Ошибка уведомления админа {admin_id}: {e}")


def replacement_info_text(info):
    user = get_user(info.get("tg_id")) if info.get("tg_id") else None
    username = user.get("username") if user else None
    parts = [
        f"🧩 Заказ для подмены #{info['id']}",
        f"Создан: {info.get('created_at') or '—'}",
        f"Заказчик: @{username or 'не указан'} (ID: {info.get('tg_id') or '—'})",
        f"OrderID: {info.get('order_number') or '—'}",
        f"card-x: {info.get('card_x') or '—'}",
        f"ID: {info.get('external_id') or '—'}",
        f"token2: {info.get('token2') or '—'}",
        f"Ссылка: {info.get('link') or '—'}",
        f"Связан с заказом №{info.get('order_id') or '—'}",
    ]
    return "\n".join(parts)


async def notify_replacement_done(info, context):
    tg_id = info.get("tg_id")
    if not tg_id:
        return
    text = (
        "✨ Поездка успешно завершена!\n\n"
        "Спасибо, что выбрали нас.\n"
        "📢 Канал: @FreeEatTaxi\n"
        "🧑‍💼 Админ: @MikeWazovsk1y\n\n"
        "Нажмите /start, чтобы вернуться в главное меню.\n"
        "Поделитесь, пожалуйста, отзывом в чате — нам важно ваше мнение! 💬"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Оставить отзыв", url="https://t.me/+kE869Hcdm_w1OWVh")]
    ])
    try:
        await context.bot.send_message(tg_id, text, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Не удалось уведомить пользователя {tg_id} о завершении: {e}")


async def notify_admins_payment(context: ContextTypes.DEFAULT_TYPE, payment_id: int):
    payment = get_payment(payment_id)
    if not payment:
        return
    user = get_user(payment.get("tg_id")) or {}
    method = payment.get("method")
    method_titles = {
        "transfer": "💳 Карта",
        "sbp": "💸 СБП",
        "ltc": "🪙 Litecoin",
        "usdt_trc20": "💵 USDT (TRC20)",
        "usdt_trx": "💵 USDT (TRX)",
    }
    original_amount = payment.get("original_amount")
    original_currency = payment.get("original_currency") or "RUB"
    display_currency = payment.get("currency") or "RUB"
    amount_value = payment.get("amount")
    amount_text = (
        (f"{amount_value:.4f}" if display_currency != "RUB" else f"{amount_value:.2f}")
        if amount_value is not None
        else "не указана"
    )
    parts = [
        "📥 Новая оплата",
        f"Пользователь: @{user.get('username') or 'не указан'} (ID: {payment.get('tg_id')})",
        f"Тип: {'Пополнение баланса' if payment.get('type') == 'topup' else 'Оплата заказа'}",
        f"Метод: {method_titles.get(method, method)}",
        f"Сумма: {amount_text} {display_currency if amount_value is not None else ''}",
        f"Реквизиты: {payment.get('requisites')}",
    ]
    if original_amount and display_currency != original_currency:
        parts.append(f"💵 Эквивалент: {original_amount:.2f} {original_currency}")
    if payment.get("comment_code"):
        parts.append(f"Комментарий: {payment.get('comment_code')}")
    if payment.get("order_id"):
        parts.append(f"Заказ: #{payment.get('order_id')}")

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Зачислить ✅", callback_data=f"payapprove_{payment_id}")],
            [InlineKeyboardButton("Нет 🚫", callback_data=f"paydecline_{payment_id}")],
        ]
    )
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(admin_id, "\n".join(parts), reply_markup=keyboard)
        except Exception as e:
            logger.error(f"Не удалось уведомить админа {admin_id}: {e}")


# ==========================
# CallbackQuery обработка (админ)
# ==========================
async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Нет доступа", show_alert=True)
        return ConversationHandler.END
    # Взял в работу
    if data.startswith("take_"):
        order_id = int(data.split("_")[1])
        order = get_order(order_id)

        if order.get("status") != "pending":
            await query.answer("❌ Этот заказ уже в работе или отменён", show_alert=True)
            return

        update_order_status(order_id, "in_progress")
        await query.edit_message_reply_markup(reply_markup=admin_in_progress_buttons(order_id))

        user_id = order.get("tg_id")
        await context.bot.send_message(user_id, f"Ваш заказ №{order_id} взят в работу! 🚖")

        # удаляем сообщение у других админов
        for admin_id in ADMIN_IDS:
            if admin_id != query.from_user.id:
                try:
                    await context.bot.delete_message(chat_id=admin_id, message_id=query.message.message_id)
                except:
                    pass

    # Отклонить
    elif data.startswith("reject_"):
        order_id = int(data.split("_")[1])
        update_order_status(order_id, "cancelled")
        await query.edit_message_text("Заказ отклонён ❌")
        order = get_order(order_id)
        user_id = order.get("tg_id")
        await context.bot.send_message(user_id, f"Ваш заказ №{order_id} отклонён 😔")
    # Поиск
    elif data.startswith("search_"):
        order_id = int(data.split("_")[1])
        update_order_status(order_id, "search")
        await query.edit_message_reply_markup(reply_markup=admin_search_buttons(order_id))
        order = get_order(order_id)
        user_id = order.get("tg_id")
        await context.bot.send_message(user_id, f"Начинаем поиск такси для вашего заказа №{order_id}! ⏳")
    # Отмена поиска / заказ
    elif data.startswith("cancel_") or data.startswith("cancelsearch_"):
        order_id = int(data.split("_")[1])
        update_order_status(order_id, "cancelled")
        await query.edit_message_text("Заказ отменён ❌")
        order = get_order(order_id)
        user_id = order.get("tg_id")
        await context.bot.send_message(user_id, f"Ваш заказ №{order_id} отменён 😔")
    # Нашлась машина
    elif data.startswith("found_"):
        order_id = int(data.split("_")[1])
        context.user_data['order_id'] = order_id
        order = get_order(order_id)
        tg_id = order.get("tg_id")
        await context.bot.send_message(tg_id,
                                       f"🚘 Ваш заказ №{order_id} нашёл машину! Пожалуйста, ожидайте инструкций от администратора.")
        await query.message.reply_text("Введите сообщение пользователю:")
        return WAIT_ADMIN_MESSAGE

    elif data.startswith("chat_"):
        order_id = int(data.split("_")[1])
        context.user_data['order_id'] = order_id
        await query.message.reply_text("Введите сообщение пользователю:")
        return WAIT_ADMIN_MESSAGE
    elif data.startswith("pay_card_"):
        order_id = int(data.split("_")[2])
        order = get_order(order_id)
        if not order:
            await query.answer("Заказ не найден", show_alert=True)
            return ConversationHandler.END
        base_amount = order.get("base_amount") or order.get("amount") or 0
        tg_id = order.get("tg_id")
        total = order.get("amount") or base_amount
        message = (
            "🧾 Оплата поездки\n"
            f"🚗 Заказ №{order_id}\n"
            f"💵 Стоимость: {base_amount:.2f} ₽\n"
            f"К оплате: {total:.2f} ₽\n\n"
            "Выберите удобный способ оплаты:"
        )
        await context.bot.send_message(
            tg_id,
            message,
            reply_markup=payment_methods_keyboard("orderpay_", order_id),
        )
        await query.message.reply_text("Меню оплаты отправлено клиенту")
    elif data.startswith("pay_balance_"):
        order_id = int(data.split("_")[2])
        order = get_order(order_id)
        if not order:
            await query.answer("Заказ не найден", show_alert=True)
            return ConversationHandler.END
        total = order.get("amount") or 0
        tg_id = order.get("tg_id")
        update_balance(tg_id, total)
        await context.bot.send_message(tg_id, f"💰 На ваш баланс начислено {total:.2f} ₽ за заказ №{order_id}")
        await query.message.reply_text("Баланс пользователя пополнен ✅")
    elif data.startswith("replacement_offer_add_"):
        order_id = int(data.rsplit("_", 1)[1])
        info_id = create_order_info(order_id)
        info = get_order_info(info_id)
        await query.message.reply_text(
            "Заполните данные заказа для подмены:",
            reply_markup=replacement_fields_keyboard(info),
        )
    elif data.startswith("replacement_offer_skip_"):
        await query.message.reply_text("Добавление заказа для подмены пропущено.")
    elif data == "admin_replacements":
        infos = list_active_order_infos()
        if not infos:
            await query.message.reply_text(
                "Список заказов для подмены пуст", reply_markup=admin_panel_keyboard()
            )
            return ConversationHandler.END
        await query.message.reply_text(
            "Активные заказы для подмены:", reply_markup=replacement_list_keyboard(infos)
        )
    elif data.startswith("replacement_view_"):
        info_id = int(data.rsplit("_", 1)[1])
        info = get_order_info(info_id)
        if not info:
            await query.answer("Запись не найдена", show_alert=True)
            return ConversationHandler.END
        await query.message.reply_text(
            replacement_info_text(info),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="replacement_back")],
                [InlineKeyboardButton("Завершить заказ ✅", callback_data=f"replacement_finish_{info_id}")],
            ]),
        )
    elif data.startswith("replacement_field_"):
        parts = data.split("_")
        field_key = parts[2]
        info_id = int(parts[3])
        context.user_data['replacement_field'] = field_key
        context.user_data['replacement_info_id'] = info_id
        prompts = {
            "orderid": "Пришлите OrderID",
            "cardx": "Пришлите card-x",
            "extid": "Пришлите ID",
            "token2": "Добавьте token2 для этой подмены",
            "link": "Пришлите ссылку",
        }
        await query.message.reply_text(prompts.get(field_key, "Пришлите значение"))
        return WAIT_REPLACEMENT_FIELD
    elif data == "replacement_back":
        await admin_show_panel(query.message)
        return ConversationHandler.END
    elif data.startswith("replacement_finish_"):
        info_id = int(data.rsplit("_", 1)[1])
        info = get_order_info(info_id)
        if not info:
            await query.answer("Запись не найдена", show_alert=True)
            return ConversationHandler.END
        deactivate_order_info(info_id)
        await notify_replacement_done(info, context)
        await query.message.reply_text(
            "Заказ для подмены завершён и убран из списка.", reply_markup=admin_panel_keyboard()
        )
    elif data == "admin_balance":
        await query.message.reply_text("Введите Telegram ID пользователя для просмотра баланса:")
        return WAIT_ADMIN_BALANCE
    elif data == "admin_orders":
        await query.message.reply_text("Введите Telegram ID пользователя для просмотра его заказов:")
        return WAIT_ADMIN_ORDERS
    elif data == "admin_refresh":
        await refresh_all_users(query.message, context)
        return ConversationHandler.END
    elif data == "admin_broadcast":
        await query.message.reply_text("Введите текст рассылки для всех пользователей:")
        return WAIT_ADMIN_BROADCAST
    elif data == "admin_toggle":
        new_value = "0" if is_ordering_enabled() else "1"
        set_setting("ordering_enabled", new_value)
        status = "🚧 Приём заказов остановлен" if new_value == "0" else "✅ Приём заказов возобновлён"
        await query.message.reply_text(status, reply_markup=admin_panel_keyboard())
    elif data == "admin_status":
        status = "✅ Приём заказов включён" if is_ordering_enabled() else "🚧 Приём заказов выключен"
        await query.message.reply_text(status, reply_markup=admin_panel_keyboard())
    elif data.startswith("payapprove_"):
        payment_id = int(data.rsplit("_", 1)[1])
        payment = get_payment(payment_id)
        if not payment:
            await query.message.reply_text("Платёж не найден")
            return ConversationHandler.END
        user_id = payment.get("tg_id")
        method = payment.get("method")
        if payment.get("type") == "order":
            order_id = payment.get("order_id")
            if order_id:
                update_order_status(order_id, "paid")
            update_payment(payment_id, status="success")
            await context.bot.send_message(user_id, "✅ Оплата за поездку подтверждена! Спасибо!")
            await query.message.reply_text("Оплата отмечена как успешная")
            return ConversationHandler.END

        if method in {"ltc", "usdt_trc20", "usdt_trx"}:
            context.user_data['admin_topup_payment'] = payment_id
            await query.message.reply_text("Введите сумму для зачисления на баланс:")
            return WAIT_ADMIN_TOPUP_AMOUNT

        amount = payment.get("amount") or 0
        update_balance(user_id, amount)
        update_payment(payment_id, status="success")
        await context.bot.send_message(user_id, f"💰 Баланс пополнен на {amount:.2f} ₽")
        await query.message.reply_text("Баланс пополнен")
    elif data.startswith("paydecline_"):
        payment_id = int(data.rsplit("_", 1)[1])
        payment = get_payment(payment_id)
        update_payment(payment_id, status="declined")
        await query.message.reply_text("Отмечено как не найдено")
        if payment:
            method = payment.get("method")
            crypto_methods = {"ltc", "usdt_trc20", "usdt_trx"}
            is_crypto = method in crypto_methods
            request_text = (
                "⚠️ Оплата не найдена. Пришлите, пожалуйста, ссылку на транзакцию или свяжитесь с оператором."
                if is_crypto
                else "⚠️ Оплата не найдена. Пришлите, пожалуйста, чек в ответном сообщении или свяжитесь с оператором."
            )
            button_label = "🔗 Ссылка" if is_crypto else "📄 Чек"
            await context.bot.send_message(
                payment.get("tg_id"),
                request_text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(button_label, callback_data=f"payment_receipt_{payment_id}")]]),
            )


# ==========================
# Отправка сообщений пользователю
# ==========================
async def admin_send_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    order_id = context.user_data.get('order_id')
    order = get_order(order_id)
    tg_id = order.get("tg_id")
    await context.bot.send_message(tg_id, f"💬 Сообщение от администратора:\n{text}")
    await update.message.reply_text("Сообщение отправлено. Теперь введите сумму заказа (₽):")
    return WAIT_ADMIN_SUM


async def admin_replacement_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    info_id = context.user_data.get('replacement_info_id')
    field_key = context.user_data.get('replacement_field')
    if not info_id or not field_key:
        await update.message.reply_text("Запись не выбрана", reply_markup=admin_panel_keyboard())
        return ConversationHandler.END

    value = update.message.text.strip()
    mapping = {
        "orderid": "order_number",
        "cardx": "card_x",
        "extid": "external_id",
        "token2": "token2",
        "link": "link",
    }
    column = mapping.get(field_key)
    if not column:
        await update.message.reply_text("Неизвестное поле", reply_markup=admin_panel_keyboard())
        return ConversationHandler.END

    update_order_info_field(info_id, **{column: value})
    context.user_data.pop('replacement_info_id', None)
    context.user_data.pop('replacement_field', None)

    info = get_order_info(info_id)
    await update.message.reply_text(
        "Сохранено", reply_markup=replacement_fields_keyboard(info)
    )
    return ConversationHandler.END


async def admin_balance_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Введите числовой Telegram ID")
        return WAIT_ADMIN_BALANCE

    user = get_user(target_id)
    if not user:
        await update.message.reply_text("Пользователь не найден", reply_markup=admin_panel_keyboard())
        return ConversationHandler.END

    text = (
        f"👤 Пользователь: @{user.get('username') or 'не указан'}\n"
        f"ID: {target_id}\n"
        f"Баланс: {user.get('balance', 0):.2f} ₽\n"
        f"Коэффициент: {user.get('coefficient', 1):.2f}"
    )
    context.user_data['balance_edit_id'] = target_id
    await update.message.reply_text(
        text + "\n\nВведите новый баланс (можно использовать дробный формат).",
        reply_markup=admin_panel_keyboard(),
    )
    return WAIT_ADMIN_BALANCE_UPDATE


async def admin_balance_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_id = context.user_data.get('balance_edit_id')
    if not target_id:
        await update.message.reply_text("❌ Пользователь не выбран", reply_markup=admin_panel_keyboard())
        return ConversationHandler.END

    text = update.message.text.replace(" ", "").replace(",", ".")
    try:
        new_balance = float(text)
    except ValueError:
        await update.message.reply_text("❌ Некорректное число, введите баланс заново")
        return WAIT_ADMIN_BALANCE_UPDATE

    set_balance(target_id, new_balance)
    user = get_user(target_id)
    context.user_data.pop('balance_edit_id', None)

    await update.message.reply_text(
        f"✅ Баланс пользователя @{user.get('username') or 'не указан'} обновлён: {new_balance:.2f} ₽",
        reply_markup=admin_panel_keyboard(),
    )
    return ConversationHandler.END


async def admin_topup_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    payment_id = context.user_data.get('admin_topup_payment')
    if not payment_id:
        await update.message.reply_text("Платёж не выбран")
        return ConversationHandler.END
    try:
        amount = float(update.message.text.replace(",", "."))
    except ValueError:
        await update.message.reply_text("Введите сумму числом")
        return WAIT_ADMIN_TOPUP_AMOUNT
    payment = get_payment(payment_id)
    if not payment:
        await update.message.reply_text("Платёж не найден")
        return ConversationHandler.END
    update_balance(payment.get("tg_id"), amount)
    update_payment(payment_id, status="success", amount=amount)
    await update.message.reply_text("Баланс пополнен по крипто-оплате")
    await context.bot.send_message(payment.get("tg_id"), f"💰 Баланс пополнен на {amount:.2f}")
    context.user_data.pop('admin_topup_payment', None)
    return ConversationHandler.END


async def payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("payment_paid_"):
        payment_id = int(data.rsplit("_", 1)[1])
        update_payment(payment_id, status="waiting_admin")
        await query.message.reply_text("✅ Заявка на оплату отправлена администратору")
        await notify_admins_payment(context, payment_id)
    elif data.startswith("payment_check_"):
        payment_id = int(data.rsplit("_", 1)[1])
        update_payment(payment_id, status="waiting_admin")
        await query.message.reply_text("🔍 Отправили данные администратору, ждём подтверждения")
        await notify_admins_payment(context, payment_id)
    elif data.startswith("payment_cancel_"):
        payment_id = int(data.rsplit("_", 1)[1])
        update_payment(payment_id, status="cancelled")
        await query.message.reply_text(
            "Отмена операции", reply_markup=main_menu_keyboard(query.from_user.id)
        )
    elif data.startswith("payment_receipt_"):
        payment_id = int(data.rsplit("_", 1)[1])
        payment = get_payment(payment_id)
        context.user_data['waiting_receipt'] = payment_id
        receipt_kind = "link" if payment and payment.get("method") in {"ltc", "usdt_trc20", "usdt_trx"} else "proof"
        context.user_data['waiting_receipt_kind'] = receipt_kind
        prompt = "Пришлите ссылку на транзакцию" if receipt_kind == "link" else "Пришлите чек (фото или файл)"
        await query.message.reply_text(prompt)
        return WAIT_PAYMENT_PROOF
    return ConversationHandler.END


async def payment_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    payment_id = context.user_data.get('waiting_receipt')
    if not payment_id:
        await update.message.reply_text("Чек не ожидается")
        return ConversationHandler.END
    payment = get_payment(payment_id)
    if not payment:
        await update.message.reply_text("Платёж не найден")
        return ConversationHandler.END
    receipt_kind = context.user_data.pop('waiting_receipt_kind', 'proof')
    caption_lines = [
        f"Данные по оплате #{payment_id}",
        f"Метод: {payment.get('method')}",
    ]
    amount_value = payment.get('amount')
    if amount_value is not None:
        caption_lines.append(
            f"Сумма: {amount_value:.4f} {payment.get('currency') or 'RUB'}"
            if (payment.get("currency") or "RUB") != "RUB"
            else f"Сумма: {amount_value:.2f} {payment.get('currency') or 'RUB'}"
        )
    if payment.get("original_amount") and (payment.get("currency") or "RUB") != (payment.get("original_currency") or "RUB"):
        caption_lines.append(
            f"Эквивалент: {payment.get('original_amount', 0):.2f} {payment.get('original_currency') or 'RUB'}"
        )
    if payment.get("requisites"):
        caption_lines.append(f"Реквизиты: {payment.get('requisites')}")
    caption = "\n".join(caption_lines)
    admin_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Зачислить ✅", callback_data=f"payapprove_{payment_id}")],
        [InlineKeyboardButton("Нет 🚫", callback_data=f"paydecline_{payment_id}")],
    ])
    forwarded = False
    for admin_id in ADMIN_IDS:
        try:
            if receipt_kind == "link" and update.message.text:
                await context.bot.send_message(
                    admin_id,
                    caption + f"\n🔗 Ссылка на транзакцию: {update.message.text}",
                    reply_markup=admin_keyboard,
                )
                forwarded = True
            elif update.message.photo:
                photo = update.message.photo[-1]
                await context.bot.send_photo(admin_id, photo=photo.file_id, caption=caption, reply_markup=admin_keyboard)
                forwarded = True
            elif update.message.document:
                doc = update.message.document
                await context.bot.send_document(admin_id, document=doc.file_id, caption=caption, reply_markup=admin_keyboard)
                forwarded = True
            elif update.message.text:
                await context.bot.send_message(admin_id, caption + f"\n📝 Комментарий: {update.message.text}", reply_markup=admin_keyboard)
                forwarded = True
        except Exception as e:
            logger.error(f"Не удалось переслать чек админу {admin_id}: {e}")
    if forwarded:
        await update.message.reply_text("Данные отправлены администратору")
    else:
        await update.message.reply_text("Не удалось отправить данные администратору, попробуйте ещё раз")
    context.user_data.pop('waiting_receipt', None)
    context.user_data.pop('waiting_receipt_kind', None)
    return ConversationHandler.END


async def admin_orders_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Введите числовой Telegram ID")
        return WAIT_ADMIN_ORDERS

    orders = get_user_orders(target_id, limit=5)
    if not orders:
        await update.message.reply_text("Заказы не найдены", reply_markup=admin_panel_keyboard())
        return ConversationHandler.END

    lines = ["📦 Последние заказы:"]
    for order in orders:
        lines.append(
            f"№{order['id']} — {order['status']} — {order['amount'] or 0:.2f} ₽ (база {order['base_amount'] or 0:.2f} ₽) — {order['created_at']}"
        )
    await update.message.reply_text("\n".join(lines), reply_markup=admin_panel_keyboard())
    return ConversationHandler.END


async def refresh_all_users(target, context: ContextTypes.DEFAULT_TYPE):
    user_ids = get_all_user_ids()
    if not user_ids:
        await target.reply_text("ℹ️ В базе пока нет пользователей", reply_markup=admin_panel_keyboard())
        return

    updated = 0
    checked = 0
    failed: list[int] = []

    for uid in user_ids:
        try:
            chat = await context.bot.get_chat(uid)
            db_user = get_user(uid)
            old_username = db_user.get("username") if db_user else None
            add_user(uid, chat.username)
            if chat.username and chat.username != old_username:
                updated += 1
            checked += 1
        except Exception as e:
            failed.append(uid)
            logger.error(f"Не удалось обновить пользователя {uid}: {e}")

    lines = [
        "🔁 Проверка завершена:",
        f"👥 Проверено пользователей: {checked}",
        f"✏️ Обновлено username: {updated}",
    ]
    if failed:
        lines.append("⚠️ Не удалось обновить: " + ", ".join(map(str, failed)))

    await target.reply_text("\n".join(lines), reply_markup=admin_panel_keyboard())


async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_ids = get_all_user_ids()
    sent = 0
    failed = 0
    for uid in user_ids:
        try:
            await context.bot.send_message(uid, f"📢 Рассылка:\n{text}")
            sent += 1
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение {uid}: {e}")
            failed += 1
    await update.message.reply_text(
        f"Рассылка завершена. ✅ {sent} отправлено, ❌ {failed} не доставлено.",
        reply_markup=admin_panel_keyboard(),
    )
    return ConversationHandler.END



# Подтверждение суммы и списание баланса
async def admin_sum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.replace(" ", "").replace(",", ".")
    order_id = context.user_data.get('order_id')
    if not order_id:
        await update.message.reply_text("Ошибка: заказ не найден")
        return ConversationHandler.END
    try:
        amount = float(text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Некорректная сумма, введите число >0")
        return WAIT_ADMIN_SUM

    order = get_order(order_id)
    tg_id = order.get("tg_id")
    user = get_user(tg_id)
    coefficient = user["coefficient"] if user else 1
    total = round(amount * coefficient, 2)

    update_order_fields(order_id, status="car_found", amount=total, base_amount=amount)

    await update.message.reply_text(
        f"✅ Сумма заказа сохранена. Итог для клиента: {total:.2f} ₽",
        reply_markup=payment_choice_keyboard(order_id),
    )

    await update.message.reply_text(
        "Добавить заказ для подмены?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Добавить", callback_data=f"replacement_offer_add_{order_id}")],
            [InlineKeyboardButton("Нет", callback_data=f"replacement_offer_skip_{order_id}")],
        ]),
    )

    return ConversationHandler.END



@admin_only
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from datetime import timedelta
    now = datetime.now()
    day_ago = now - timedelta(days=1)
    day_ago_str = day_ago.strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        # Заказы за сутки
        c.execute("SELECT SUM(amount), COUNT(*) FROM orders WHERE created_at >= ?", (day_ago_str,))
        day_sum, day_count = c.fetchone()
        # Заказы за всё время
        c.execute("SELECT SUM(amount), COUNT(*) FROM orders")
        total_sum, total_count = c.fetchone()

    day_sum = day_sum or 0
    total_sum = total_sum or 0
    day_count = day_count or 0
    total_count = total_count or 0

    text = (
        f"📊 <b>Статистика заказов</b>\n\n"
        f"🗓️ Кол-во заказов за сутки: {day_count}\n"
        f"📅 Кол-во заказов за всё время: {total_count}\n\n"
        f"💰 Сумма заказов за сутки: {day_sum:.2f} ₽\n"
        f"💵 Сумма заказов за всё время: {total_sum:.2f} ₽\n\n"
        f"🤑 Заработок за сутки: {day_sum:.2f} ₽\n"
        f"💸 Заработок за всё время: {total_sum:.2f} ₽"
    )
    await update.message.reply_text(text, parse_mode="HTML")

@admin_only
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /ban <tg_id>")
        return
    try:
        tg_id = int(context.args[0])
        with sqlite3.connect(BANNED_DB) as conn:
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO banned (tg_id) VALUES (?)", (tg_id,))
            conn.commit()
        await update.message.reply_text(f"✅ Пользователь {tg_id} заблокирован")
    except ValueError:
        await update.message.reply_text("❌ Некорректный tg_id")


# ==========================
# Основной запуск
# ==========================
def main():
    init_db()
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("ban", ban_user))

    # ConversationHandler для заказов и админа
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(order_type_callback, pattern="^order_")],
        states={
            WAIT_SCREENSHOT: [MessageHandler(filters.PHOTO, screenshot_receive)],
            WAIT_CITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, text_city)],
            WAIT_ADDRESS_FROM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, text_address_from),
                CallbackQueryHandler(favorite_address_callback, pattern="^fav_from_"),
            ],
            WAIT_ADDRESS_TO: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, text_address_to),
                CallbackQueryHandler(favorite_address_callback, pattern="^fav_to_"),
            ],
            WAIT_ADDRESS_THIRD_DECISION: [CallbackQueryHandler(address_third_decision, pattern="^address_")],
            WAIT_ADDRESS_THIRD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, text_address_third),
                CallbackQueryHandler(favorite_address_callback, pattern="^fav_third_"),
            ],
            WAIT_TARIFF: [CallbackQueryHandler(tariff_selected, pattern="^tariff_")],
            WAIT_ADDITIONAL: [CallbackQueryHandler(additional_selected, pattern="^additional_")],
            WAIT_CHILD_SEAT_TYPE: [CallbackQueryHandler(child_seat_type_selected, pattern="^seat_type_")],
            WAIT_COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, text_comment)],
            WAIT_CONFIRMATION: [CallbackQueryHandler(order_preview_action, pattern="^order_(confirm|cancel)_")],
        },
        fallbacks=[CommandHandler("start", start_over)],
        per_user=True,
        per_message=False,
    )

    admin_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_callback, pattern="^(chat_|found_|admin_balance|admin_orders|admin_refresh|admin_broadcast|admin_toggle|admin_status|admin_replacements|replacement_|take_|reject_|search_|cancelsearch_|cancel_|payapprove_|paydecline_)")],
        states={
            WAIT_ADMIN_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_send_message)],
            WAIT_ADMIN_SUM: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_sum)],
            WAIT_ADMIN_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_balance_lookup)],
            WAIT_ADMIN_BALANCE_UPDATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_balance_update)],
            WAIT_ADMIN_ORDERS: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_orders_lookup)],
            WAIT_ADMIN_BROADCAST: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast)],
            WAIT_REPLACEMENT_FIELD: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_replacement_save)],
            WAIT_ADMIN_TOPUP_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_topup_amount)],
        },
        fallbacks=[CommandHandler("start", start_over)],
        per_user=True,
        per_message=False,
    )

    payment_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(topup_method_selected, pattern="^topup_"),
            CallbackQueryHandler(order_payment_method, pattern="^orderpay_"),
            CallbackQueryHandler(payment_callback, pattern="^payment_")
        ],
        states={
            WAIT_TOPUP_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, topup_amount_entered)],
            WAIT_PAYMENT_PROOF: [MessageHandler((filters.TEXT | filters.PHOTO | filters.Document.ALL) & ~filters.COMMAND, payment_receipt)],
        },
        fallbacks=[CommandHandler("start", start_over)],
        per_user=True,
        per_message=False,
    )

    app.add_handler(conv_handler)
    app.add_handler(admin_conv_handler)
    app.add_handler(payment_conv)
    app.add_handler(CallbackQueryHandler(profile_callback, pattern="^profile_"))
    app.add_handler(CallbackQueryHandler(favorite_address_callback, pattern="^fav_(from|to|third)_"))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^(take_|reject_|search_|cancel_|cancelsearch_|pay_card_|pay_balance_|replacement_|admin_replacements|admin_refresh|payapprove_|paydecline_)"))

    # Меню пользователя
    async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text
        user_id = update.effective_user.id

        if context.user_data.get("awaiting_city"):
            city = text.strip()
            update_user_city(user_id, city)
            context.user_data.pop("awaiting_city", None)
            await update.message.reply_text(f"🏙️ Город сохранён: {city}")
            await send_profile_info(update.message, user_id, context)
            return

        if user_id in ADMIN_IDS and text == "Админка ⚙️":
            await admin_show_panel(update.message)
            return

        if context.user_data.get("awaiting_fav_action"):
            action = context.user_data.get("awaiting_fav_action")
            if action == "add":
                favorites = get_favorite_addresses(user_id)
                if len(favorites) >= 3:
                    await update.message.reply_text("Можно сохранить не более 3 адресов")
                else:
                    add_favorite_address(user_id, text.strip())
                    await update.message.reply_text("⭐ Адрес добавлен")
                context.user_data.pop("awaiting_fav_action", None)
                await send_profile_info(update.message, user_id, context)
                return
            elif action == "edit":
                fav_id = context.user_data.get("fav_edit_id")
                if fav_id:
                    update_favorite_address(fav_id, user_id, text.strip())
                    await update.message.reply_text("✏️ Адрес обновлён")
                context.user_data.pop("awaiting_fav_action", None)
                context.user_data.pop("fav_edit_id", None)
                await send_profile_info(update.message, user_id, context)
                return

        if text == "Профиль 👤":
            await profile(update, context)
        elif text == "Помощь ❓":
            await help_menu(update, context)
        elif text == "Заказать такси 🚖":
            await order_menu(update, context)
        elif text == "Назад ◀️":
            await update.message.reply_text(
                "Возврат в главное меню",
                reply_markup=main_menu_keyboard(update.effective_user.id),
            )

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    # CallbackQueryHandler для админа
    #app.add_handler(CallbackQueryHandler(admin_callback, pattern="^(take_|reject_|search_|cancel_|cancelsearch_|found_|chat_)"))

    logger.info("Бот запущен")
    app.run_polling()


if __name__ == "__main__":
    main()