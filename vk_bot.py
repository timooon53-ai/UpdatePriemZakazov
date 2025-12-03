import logging
import os
from typing import Dict

import vk_api
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll
from vk_api.keyboard import VkKeyboard, VkKeyboardColor

from cfg import *  # noqa: F401,F403
from main import add_user, create_order, ensure_default_order_info, get_user, init_db

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[logging.StreamHandler(), logging.FileHandler("vk_bot.log")],
)
logger = logging.getLogger(__name__)

VK_TOKEN = os.getenv("VK_TOKEN", locals().get("VK_TOKEN", "")).strip()
GROUP_ID = int(os.getenv("VK_GROUP_ID", locals().get("VK_GROUP_ID", 0)) or 0)


class OrderState:
    def __init__(self):
        self.stage = "city"
        self.city = None
        self.address_from = None
        self.address_to = None
        self.comment = None


user_states: Dict[int, OrderState] = {}


def keyboard_main():
    kb = VkKeyboard(one_time=False)
    kb.add_button("Создать заказ", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Помощь", VkKeyboardColor.SECONDARY)
    return kb


def keyboard_cancel():
    kb = VkKeyboard(one_time=False)
    kb.add_button("Отмена", VkKeyboardColor.NEGATIVE)
    return kb


def send_message(vk, user_id: int, text: str, keyboard=None):
    vk.messages.send(
        user_id=user_id,
        random_id=vk_api.utils.get_random_id(),
        message=text,
        keyboard=keyboard.get_keyboard() if keyboard else None,
    )


def reset_state(user_id: int):
    if user_id in user_states:
        del user_states[user_id]


def handle_start(vk, user_id: int):
    reset_state(user_id)
    add_user(user_id, username=None)
    send_message(
        vk,
        user_id,
        "Привет! Я бот для приема заказов. Нажми \"Создать заказ\" чтобы начать.",
        keyboard_main(),
    )
    logger.info("Пользователь %s открыл главное меню", user_id)


def handle_help(vk, user_id: int):
    send_message(
        vk,
        user_id,
        "Я помогу оформить заказ. Нажми \"Создать заказ\" и следуй подсказкам. Для отмены отправь \"Отмена\".",
        keyboard_main(),
    )


def start_order(vk, user_id: int):
    user_states[user_id] = OrderState()
    send_message(vk, user_id, "Укажи город, где требуется заказ", keyboard_cancel())
    logger.info("Пользователь %s начал оформление заказа", user_id)


def process_order_message(vk, user_id: int, text: str):
    state = user_states.get(user_id)
    if not state:
        return False

    if text.lower() == "отмена":
        reset_state(user_id)
        send_message(vk, user_id, "Заказ отменен", keyboard_main())
        logger.info("Пользователь %s отменил оформление", user_id)
        return True

    if state.stage == "city":
        state.city = text
        state.stage = "from"
        send_message(vk, user_id, "Введите адрес отправления")
        return True

    if state.stage == "from":
        state.address_from = text
        state.stage = "to"
        send_message(vk, user_id, "Введите адрес назначения")
        return True

    if state.stage == "to":
        state.address_to = text
        state.stage = "comment"
        send_message(vk, user_id, "Добавьте комментарий или отправьте '-'")
        return True

    if state.stage == "comment":
        state.comment = None if text.strip() == "-" else text
        order_id = create_order(
            tg_id=user_id,
            type_="vk",
            city=state.city,
            address_from=state.address_from,
            address_to=state.address_to,
            comment=state.comment,
        )
        reset_state(user_id)
        send_message(
            vk,
            user_id,
            f"Заказ №{order_id} создан. Мы свяжемся с вами после обработки.",
            keyboard_main(),
        )
        logger.info("Создан заказ %s пользователем %s", order_id, user_id)
        return True

    return False


def main():
    init_db()
    ensure_default_order_info()

    if not VK_TOKEN or not GROUP_ID:
        raise RuntimeError("VK_TOKEN и VK_GROUP_ID должны быть заданы в cfg.py или переменных окружения")

    vk_session = vk_api.VkApi(token=VK_TOKEN)
    vk = vk_session.get_api()
    longpoll = VkBotLongPoll(vk_session, GROUP_ID)

    logger.info("VK бот запущен и ожидает события группы %s", GROUP_ID)

    for event in longpoll.listen():
        if event.type == VkBotEventType.MESSAGE_NEW:
            message = event.message
            user_id = message["from_id"]
            text = (message.get("text") or "").strip()

            logger.info("Получено сообщение от %s: %s", user_id, text)

            lowered = text.lower()
            if lowered in {"/start", "start", "начать"}:
                handle_start(vk, user_id)
                continue

            if lowered == "помощь":
                handle_help(vk, user_id)
                continue

            if lowered == "создать заказ":
                start_order(vk, user_id)
                continue

            if process_order_message(vk, user_id, text):
                continue

            if get_user(user_id):
                send_message(
                    vk,
                    user_id,
                    "Не понял запрос. Нажми \"Создать заказ\" чтобы оформить заказ или \"Помощь\" для справки.",
                    keyboard_main(),
                )
            else:
                handle_start(vk, user_id)


if __name__ == "__main__":
    main()
