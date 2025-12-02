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
USERS_DB = USERS_DB
ORDERS_DB = ORDERS_DB
BANNED_DB = BANNED_DB

logging.basicConfig(
    filename="bot.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)

# ==========================
# Инициализация БД
# ==========================
def init_db():
    with sqlite3.connect("banned.db") as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS banned (
                tg_id INTEGER PRIMARY KEY
            )
        """)
        conn.commit()

    with sqlite3.connect(USERS_DB) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                tg_id INTEGER PRIMARY KEY,
                username TEXT,
                balance REAL DEFAULT 0.00,
                orders_count INTEGER DEFAULT 0,
                coefficient REAL DEFAULT 0.55
            )
        """)
        conn.commit()
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER,
                type TEXT,
                screenshot_path TEXT,
                city TEXT,
                address_from TEXT,
                address_to TEXT,
                comment TEXT,
                status TEXT DEFAULT 'pending',
                amount REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        conn.commit()

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
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,))
        return c.fetchone()

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

# ==========================
# Работа с заказами
# ==========================
def create_order(tg_id, type_, screenshot_path=None, city=None, address_from=None, address_to=None, comment=None):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO orders (tg_id, type, screenshot_path, city, address_from, address_to, comment)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (tg_id, type_, screenshot_path, city, address_from, address_to, comment))
        order_id = c.lastrowid
        conn.commit()
        return order_id

def get_order(order_id):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM orders WHERE id=?", (order_id,))
        return c.fetchone()

def update_order_status(order_id, status):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("UPDATE orders SET status=?, updated_at=? WHERE id=?", (status, datetime.now(), order_id))
        conn.commit()

def update_order_amount(order_id, amount):
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("UPDATE orders SET amount=?, updated_at=? WHERE id=?", (amount, datetime.now(), order_id))
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
def main_menu_keyboard():
    buttons = [
        [KeyboardButton("Профиль 👤")],
        [KeyboardButton("Заказать такси 🚖")],
        [KeyboardButton("Помощь ❓")]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def back_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("Назад ◀️")]], resize_keyboard=True)

def order_type_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Отправить скриншотом 🖼️", callback_data="order_screenshot")],
        [InlineKeyboardButton("Отправить текстом 📝", callback_data="order_text")],
        [InlineKeyboardButton("Назад ◀️", callback_data="order_back")]
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
        with sqlite3.connect("banned.db") as conn:
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
        reply_markup=main_menu_keyboard()
    )

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = get_user(user_id)
    if not user:
        await update.message.reply_text("Пользователь не найден.")
        return
    tg_id, username, balance, orders_count, coefficient = user
    text = (
        f"👤 Профиль\n"
        f"Username: @{username or 'не указан'}\n"
        f"Telegram ID: {tg_id}\n"
        f"Баланс: {balance:.2f} ₽\n"
        f"Заказано поездок: {orders_count}\n"
        f"Коэффициент: {coefficient:.2f}"
    )
    await update.message.reply_text(text, reply_markup=back_keyboard())

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


# ==========================
# Conversation States
# ==========================
(
    WAIT_SCREENSHOT, WAIT_SCREENSHOT_COMMENT,
    WAIT_CITY, WAIT_ADDRESS_FROM, WAIT_ADDRESS_TO, WAIT_COMMENT,
    WAIT_ADMIN_MESSAGE, WAIT_ADMIN_SUM
) = range(8)

# ==========================
# Пользовательский сценарий заказа
# ==========================
async def order_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Выберите способ заказа:", reply_markup=order_type_keyboard())

async def order_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "order_screenshot":
        await query.message.reply_text("Пришлите скриншот маршрута 📎")
        return WAIT_SCREENSHOT
    elif data == "order_text":
        await query.message.reply_text("Введите город 🏙️")
        return WAIT_CITY
    elif data == "order_back":
        await query.message.reply_text("Возврат в главное меню", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

# ---- Клавиатура "Пропустить" ----
def skip_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("Пропустить ➡️")]], resize_keyboard=True)

# ---- Скриншотный заказ (приём фото) ----
async def screenshot_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    photo = update.message.photo[-1]
    file = await photo.get_file()
    tg_id = update.effective_user.id

    # создаём запись заказа сразу (без полей маршрута)
    order_id = create_order(tg_id, type_="screenshot")
    path = os.path.join(SCREENSHOTS_DIR, f"{order_id}.jpg")
    await file.download_to_drive(path)

    # сохраняем путь в БД
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("UPDATE orders SET screenshot_path=? WHERE id=?", (path, order_id))
        conn.commit()

    context.user_data['order_id'] = order_id
    await update.message.reply_text("Комментарий 💬 или «Пропустить ➡️»", reply_markup=skip_keyboard())
    return WAIT_SCREENSHOT_COMMENT

async def screenshot_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comment = update.message.text
    if comment and comment.lower() == "пропустить ➡️":
        comment = None

    order_id = context.user_data.get('order_id')
    if not order_id:
        await update.message.reply_text("Произошла ошибка: заказ не найден.")
        return ConversationHandler.END

    # обновляем комментарий
    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        c.execute("UPDATE orders SET comment=? WHERE id=?", (comment, order_id))
        conn.commit()

    increment_orders_count(update.effective_user.id)
    await update.message.reply_text(f"✅ Ваш заказ №{order_id} создан", reply_markup=main_menu_keyboard())
    await notify_admins(context, order_id)

    # очистка состояния пользователя
    context.user_data.clear()
    return ConversationHandler.END

# ---- Текстовый заказ: последовательность шагов ----
async def text_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['city'] = update.message.text
    await update.message.reply_text("Адрес откуда 📍")
    return WAIT_ADDRESS_FROM

async def text_address_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # сохраняем в user_data (геокодим если нужно)
    context.user_data['address_from'] = geocode(update.message.text) or update.message.text
    await update.message.reply_text("Адрес куда 📍")
    return WAIT_ADDRESS_TO

async def text_address_to(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['address_to'] = geocode(update.message.text) or update.message.text
    await update.message.reply_text("Комментарий 💬 или «Пропустить ➡️»", reply_markup=skip_keyboard())
    return WAIT_COMMENT

async def text_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # финальный шаг: только здесь создаём запись заказа
    comment = update.message.text
    if comment and comment.lower() == "пропустить ➡️":
        comment = None

    data = context.user_data
    # проверяем, что необходимые поля есть
    city = data.get('city')
    addr_from = data.get('address_from')
    addr_to = data.get('address_to')

    if not (city and addr_from and addr_to):
        await update.message.reply_text("Ошибка: не все поля введены. Попробуйте снова.", reply_markup=main_menu_keyboard())
        context.user_data.clear()
        return ConversationHandler.END

    order_id = create_order(
        tg_id=update.effective_user.id,
        type_="text",
        city=city,
        address_from=addr_from,
        address_to=addr_to,
        comment=comment
    )

    increment_orders_count(update.effective_user.id)
    await update.message.reply_text(f"✅ Ваш заказ №{order_id} создан", reply_markup=main_menu_keyboard())
    await notify_admins(context, order_id)

    context.user_data.clear()
    return ConversationHandler.END


# ==========================
# Админ уведомление
# ==========================
async def notify_admins(context, order_id):
    order = get_order(order_id)

    tg_id = order[1]
    type_ = order[2]
    username = get_user(tg_id)[1]

    city = order[4]
    address_from = order[5]
    address_to = order[6]
    comment = order[7]

    text = (
        f"НОВЫЙ ЗАКАЗ №{order_id}\n"
        f"Тип: {type_}\n"
        f"Пользователь: @{username} (ID: {tg_id})\n"
        f"Город: {city}\n"
        f"Откуда: {address_from}\n"
        f"Куда: {address_to}\n"
        f"Комментарий: {comment}"
    )

    for admin_id in ADMIN_IDS:
        try:
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
    # Взял в работу
    if data.startswith("take_"):
        order_id = int(data.split("_")[1])
        order = get_order(order_id)

        if order[8] != "pending":  # индекс 8 = status
            await query.answer("❌ Этот заказ уже в работе или отменён", show_alert=True)
            return

        update_order_status(order_id, "in_progress")
        await query.edit_message_reply_markup(reply_markup=admin_in_progress_buttons(order_id))

        user_id = order[1]
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
        user_id = get_order(order_id)[1]
        await context.bot.send_message(user_id, f"Ваш заказ №{order_id} отклонён 😔")
    # Поиск
    elif data.startswith("search_"):
        order_id = int(data.split("_")[1])
        update_order_status(order_id, "search")
        await query.edit_message_reply_markup(reply_markup=admin_search_buttons(order_id))
        user_id = get_order(order_id)[1]
        await context.bot.send_message(user_id, f"Начинаем поиск такси для вашего заказа №{order_id}! ⏳")
    # Отмена поиска / заказ
    elif data.startswith("cancel_") or data.startswith("cancelsearch_"):
        order_id = int(data.split("_")[1])
        update_order_status(order_id, "cancelled")
        await query.edit_message_text("Заказ отменён ❌")
        user_id = get_order(order_id)[1]
        await context.bot.send_message(user_id, f"Ваш заказ №{order_id} отменён 😔")
    # Нашлась машина
    elif data.startswith("found_"):
        order_id = int(data.split("_")[1])
        context.user_data['order_id'] = order_id
        tg_id = get_order(order_id)[1]
        await context.bot.send_message(tg_id,
                                       f"🚘 Ваш заказ №{order_id} нашёл машину! Пожалуйста, ожидайте инструкций от администратора.")
        await query.message.reply_text("Введите сообщение пользователю:")
        return WAIT_ADMIN_MESSAGE

    elif data.startswith("chat_"):
        order_id = int(data.split("_")[1])
        context.user_data['order_id'] = order_id
        await query.message.reply_text("Введите сообщение пользователю:")
        return WAIT_ADMIN_MESSAGE


# ==========================
# Отправка сообщений пользователю
# ==========================
async def admin_send_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    order_id = context.user_data.get('order_id')
    tg_id = get_order(order_id)[1]
    await context.bot.send_message(tg_id, f"💬 Сообщение от администратора:\n{text}")
    await update.message.reply_text("Сообщение отправлено. Теперь введите сумму заказа (₽):")
    return WAIT_ADMIN_SUM



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

    tg_id = get_order(order_id)[1]
    user = get_user(tg_id)
    coefficient = user[4]
    total = round(amount * coefficient, 2)

    # просто списываем, даже если будет минус
    update_balance(tg_id, -total)
    update_order_status(order_id, "car_found")
    update_order_amount(order_id, total)

    await update.message.reply_text(f"✅ Заказ №{order_id} завершён, списано {total} ₽")
    await context.bot.send_message(tg_id, f"Машина найдена! 🚘\nСумма к оплате: {total} ₽")

    return ConversationHandler.END



@admin_only
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from datetime import timedelta
    now = datetime.now()
    day_ago = now - timedelta(days=1)

    with sqlite3.connect(ORDERS_DB) as conn:
        c = conn.cursor()
        # Заказы за сутки
        c.execute("SELECT SUM(amount), COUNT(*) FROM orders WHERE created_at >= ?", (day_ago,))
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
        with sqlite3.connect("banned.db") as conn:
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
            WAIT_SCREENSHOT_COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, screenshot_comment)],
            WAIT_CITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, text_city)],
            WAIT_ADDRESS_FROM: [MessageHandler(filters.TEXT & ~filters.COMMAND, text_address_from)],
            WAIT_ADDRESS_TO: [MessageHandler(filters.TEXT & ~filters.COMMAND, text_address_to)],
            WAIT_COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, text_comment)],
        },
        fallbacks=[],
        per_user=True,
    )

    admin_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_callback, pattern="^(chat_|found_)")],
        states={
            WAIT_ADMIN_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_send_message)],
            WAIT_ADMIN_SUM: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_sum)],
        },
        fallbacks=[],
        per_user=True
    )

    app.add_handler(conv_handler)
    app.add_handler(admin_conv_handler)
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^(take_|reject_|search_|cancel_|cancelsearch_)"))

    # Меню пользователя
    async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text
        if text == "Профиль 👤":
            await profile(update, context)
        elif text == "Помощь ❓":
            await help_menu(update, context)
        elif text == "Заказать такси 🚖":
            await order_menu(update, context)
        elif text == "Назад ◀️":
            await update.message.reply_text("Возврат в главное меню", reply_markup=main_menu_keyboard())

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    # CallbackQueryHandler для админа
    #app.add_handler(CallbackQueryHandler(admin_callback, pattern="^(take_|reject_|search_|cancel_|cancelsearch_|found_|chat_)"))

    logger.info("Бот запущен")
    app.run_polling()


if __name__ == "__main__":
    main()
