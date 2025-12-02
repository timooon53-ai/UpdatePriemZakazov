from cfg import *
import os
import sqlite3
import logging
import requests
from datetime import datetime
from functools import wraps

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

DB_PATH = os.path.join(DB_DIR, "DB.db")
USERS_DB = ORDERS_DB = BANNED_DB = DB_PATH

logging.basicConfig(
    filename="bot.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)


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
        c.execute("INSERT OR IGNORE INTO users (tg_id, username) VALUES (?, ?)", (tg_id, username))
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


def get_user_orders(tg_id, limit=5):
    with sqlite3.connect(ORDERS_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT id, status, amount, base_amount, created_at FROM orders WHERE tg_id=? ORDER BY id DESC LIMIT ?",
            (tg_id, limit),
        )
        return c.fetchall()

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


def yes_no_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👍 Да", callback_data="address_yes"),
            InlineKeyboardButton("👎 Нет", callback_data="address_no"),
        ]
    ])


def tariff_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Эконом 💸", callback_data="tariff_Эконом"), InlineKeyboardButton("Комфорт 😊", callback_data="tariff_Комфорт")],
        [InlineKeyboardButton("Комфорт+ ✨", callback_data="tariff_Комфорт+"), InlineKeyboardButton("Бизнес 💼", callback_data="tariff_Бизнес")],
        [InlineKeyboardButton("Премьер 👑", callback_data="tariff_Премьер"), InlineKeyboardButton("Элит 🏆", callback_data="tariff_Элит")],
    ])


def child_seat_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👶 Кресло", callback_data="seat_need")],
        [InlineKeyboardButton("📝 Пожелания", callback_data="seat_wish")],
        [InlineKeyboardButton("⏭️ Пропустить", callback_data="seat_skip")],
    ])


def child_seat_type_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛄 Свое", callback_data="seat_type_Свое")],
        [InlineKeyboardButton("👶 9м - 4л", callback_data="seat_type_9м-4л")],
        [InlineKeyboardButton("🧒 3-7л", callback_data="seat_type_3-7л")],
        [InlineKeyboardButton("👦 6-12л", callback_data="seat_type_6-12л")],
        [InlineKeyboardButton("🚪 Выйти", callback_data="seat_type_exit")],
    ])


def wishes_keyboard(selected=None):
    selected = selected or []
    def label(option, text):
        return f"{'✅' if option in selected else '⬜️'} {text}"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(label("animals", "Перевозка животных 🐾"), callback_data="wish_animals")],
        [InlineKeyboardButton(label("wheelchair", "Буду с инвалидным креслом ♿"), callback_data="wish_wheelchair")],
        [InlineKeyboardButton("✅ Готово", callback_data="wish_done"), InlineKeyboardButton("⏭️ Пропустить", callback_data="wish_skip")],
    ])

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
        [InlineKeyboardButton("📢 Рассылка по всем", callback_data="admin_broadcast")],
        [InlineKeyboardButton(ordering_label, callback_data="admin_toggle")],
        [InlineKeyboardButton(status_text, callback_data="admin_status")],
    ])


async def admin_show_panel(target):
    await target.reply_text("⚙️ Админ-панель", reply_markup=admin_panel_keyboard())

# ==========================
# Геокодирование
# ==========================
def geocode(address):
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": address, "format": "json"}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data:
            return None
        return f"{data[0]['lat']},{data[0]['lon']}"
    except Exception as e:
        logger.error(f"Geocode error: {e}")
        return None

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
    WAIT_CHILD_SEAT,
    WAIT_CHILD_SEAT_TYPE,
    WAIT_WISHES,
    WAIT_COMMENT,
    WAIT_ADMIN_MESSAGE,
    WAIT_ADMIN_SUM,
    WAIT_ADMIN_BALANCE,
    WAIT_ADMIN_ORDERS,
    WAIT_ADMIN_BROADCAST,
) = range(16)

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
        await query.message.reply_text("Введите город 🏙️")
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

async def text_address_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # сохраняем в user_data (геокодим если нужно)
    context.user_data.setdefault('order_data', {})['address_from'] = geocode(update.message.text) or update.message.text
    await ask_address_to(update, context)
    return WAIT_ADDRESS_TO

async def text_address_to(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault('order_data', {})['address_to'] = geocode(update.message.text) or update.message.text
    await update.message.reply_text("Хотите добавить ещё один адрес?", reply_markup=yes_no_keyboard())
    return WAIT_ADDRESS_THIRD_DECISION


async def text_address_third(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault('order_data', {})['address_extra'] = geocode(update.message.text) or update.message.text
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

    increment_orders_count(update.effective_user.id)
    await update.message.reply_text(
        f"✅ Ваш заказ №{order_id} создан",
        reply_markup=main_menu_keyboard(update.effective_user.id),
    )
    await notify_admins(context, order_id)

    context.user_data.clear()
    return ConversationHandler.END


async def tariff_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tariff = query.data.split("_", 1)[1]
    context.user_data.setdefault('order_data', {})['tariff'] = tariff
    await query.message.reply_text("Нужен ли детский кресло?", reply_markup=child_seat_keyboard())
    return WAIT_CHILD_SEAT


async def child_seat_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    choice = query.data
    if choice == "seat_need":
        context.user_data.setdefault('order_data', {})['child_seat'] = "Нужно"
        await query.message.reply_text("Выберите тип кресла", reply_markup=child_seat_type_keyboard())
        return WAIT_CHILD_SEAT_TYPE
    elif choice == "seat_wish":
        context.user_data.setdefault('order_data', {})['child_seat'] = "Пожелания"
    else:
        context.user_data.setdefault('order_data', {})['child_seat'] = "Не требуется"
    await query.message.reply_text("Выберите пожелания", reply_markup=wishes_keyboard(context.user_data.get('order_data', {}).get('wishes')))
    return WAIT_WISHES


async def child_seat_type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "seat_type_exit":
        await query.message.reply_text("Выберите пожелания", reply_markup=wishes_keyboard(context.user_data.get('order_data', {}).get('wishes')))
        return WAIT_WISHES

    seat_type = data.split("_", 2)[2]
    context.user_data.setdefault('order_data', {})['child_seat_type'] = seat_type
    await query.message.reply_text("Выберите пожелания", reply_markup=wishes_keyboard(context.user_data.get('order_data', {}).get('wishes')))
    return WAIT_WISHES


async def wishes_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    order_data = context.user_data.setdefault('order_data', {})
    current_wishes = set(order_data.get('wishes', []))

    if data == "wish_done" or data == "wish_skip":
        await query.message.reply_text("Добавьте комментарий для админа или нажмите «Пропустить ➡️»", reply_markup=skip_keyboard())
        return WAIT_COMMENT

    option = "animals" if data == "wish_animals" else "wheelchair"
    label = "Перевозка животных" if option == "animals" else "Буду с инвалидным креслом"
    if label in current_wishes:
        current_wishes.remove(label)
    else:
        current_wishes.add(label)
    order_data['wishes'] = list(current_wishes)
    await query.edit_message_reply_markup(reply_markup=wishes_keyboard(order_data['wishes']))
    return WAIT_WISHES


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

    parts = [
        f"НОВЫЙ ЗАКАЗ №{order_id}",
        f"Тип: {type_}",
        f"Пользователь: @{username} (ID: {tg_id})",
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
        total = order.get("amount") or 0
        tg_id = order.get("tg_id")
        message = (
            "🚖 Ваше такси уже едет к вам! Ссылка на отслеживание выше!\n"
            f"💵 Стоимость поездки: {base_amount:.2f} ₽\n"
            f"💰 Ваша оплата нам: {total:.2f} ₽\n\n"
            "Оплатить необходимо: ВТБ банк по номеру телефона +79088006072"
        )
        await context.bot.send_message(tg_id, message)
        await query.message.reply_text("💳 Инструкция по оплате отправлена пользователю")
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
    elif data == "admin_balance":
        await query.message.reply_text("Введите Telegram ID пользователя для просмотра баланса:")
        return WAIT_ADMIN_BALANCE
    elif data == "admin_orders":
        await query.message.reply_text("Введите Telegram ID пользователя для просмотра его заказов:")
        return WAIT_ADMIN_ORDERS
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
    await update.message.reply_text(text, reply_markup=admin_panel_keyboard())
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
            WAIT_CHILD_SEAT: [CallbackQueryHandler(child_seat_selected, pattern="^seat_")],
            WAIT_CHILD_SEAT_TYPE: [CallbackQueryHandler(child_seat_type_selected, pattern="^seat_type_")],
            WAIT_WISHES: [CallbackQueryHandler(wishes_selected, pattern="^wish_")],
            WAIT_COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, text_comment)],
        },
        fallbacks=[],
        per_user=True,
        per_message=False,
    )

    admin_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_callback, pattern="^(chat_|found_|admin_balance|admin_orders|admin_broadcast|admin_toggle|admin_status)")],
        states={
            WAIT_ADMIN_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_send_message)],
            WAIT_ADMIN_SUM: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_sum)],
            WAIT_ADMIN_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_balance_lookup)],
            WAIT_ADMIN_ORDERS: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_orders_lookup)],
            WAIT_ADMIN_BROADCAST: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast)],
        },
        fallbacks=[],
        per_user=True,
        per_message=False,
    )

    app.add_handler(conv_handler)
    app.add_handler(admin_conv_handler)
    app.add_handler(CallbackQueryHandler(profile_callback, pattern="^profile_"))
    app.add_handler(CallbackQueryHandler(favorite_address_callback, pattern="^fav_(from|to|third)_"))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^(take_|reject_|search_|cancel_|cancelsearch_)"))

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
