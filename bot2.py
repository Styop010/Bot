import os
import sqlite3
import logging
from datetime import datetime
from enum import Enum
from typing import Optional

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
OPERATORS_CHAT_ID = -5159543096
DB_PATH = os.getenv("DB_PATH", "/var/data/support_bot.db")

print("DEBUG BOT_TOKEN EXISTS:", BOT_TOKEN is not None)
print("DEBUG BOT_TOKEN REPR:", repr(BOT_TOKEN))
print("DEBUG BOT_TOKEN LEN:", len(BOT_TOKEN) if BOT_TOKEN else 0)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class Step(str, Enum):
    CHOOSE_LANGUAGE = "choose_language"
    CHOOSE_TOPIC = "choose_topic"
    WAIT_ISSUE = "wait_issue"
    WAIT_CLID = "wait_clid"
    WAIT_SUPPORT_ID = "wait_support_id"
    DONE = "done"


LANG_TEXTS = {
    "hy": {
        "lang_name": "Հայերեն",
        "start": "Ընտրեք լեզուն",
        "choose_topic": "Խնդրում ենք ընտրել դիմումի թեման։",
        "ask_issue": "Ուղարկեք խնդրի նկարագրությունը մեկ հաղորդագրությամբ կամ ուղարկեք նկար + caption։",
        "ask_clid": "Խնդրում ենք մուտքագրել տաքսոպարկի CLID-ը։",
        "ask_support_id": "Խնդրում ենք մուտքագրել support դիմումի ID-ն։",
        "request_sent": "Ձեր դիմումն ուղարկվել է օպերատորին։",
        "reply_prefix": "💬 Օպերատորի պատասխան",
        "restart_hint": "Սկսեք /start հրամանով։",
        "invalid_clid": "CLID-ը չպետք է դատարկ լինի։ Կրկին ուղարկեք։",
        "invalid_support_id": "Support ID-ը չպետք է դատարկ լինի։ Կրկին ուղարկեք։",
        "issue_required": "Ուղարկեք խնդրի տեքստ կամ նկար։",
        "closed_to_user": "✅ Ձեր դիմումը փակվել է։",
        "topics": {
            "drivers": "Վարորդների հետ խնդիրներ",
            "dispatch": "Դիսպետչերական ծրագրի խնդիրներ",
            "payouts": "Վճարումների խնդիրներ",
            "other": "Այլ",
        },
    },
    "ru": {
        "lang_name": "Русский",
        "start": "Выберите язык",
        "choose_topic": "Пожалуйста, выберите тему обращения.",
        "ask_issue": "Отправьте описание проблемы одним сообщением или отправьте фото с подписью.",
        "ask_clid": "Пожалуйста, введите CLID таксопарка.",
        "ask_support_id": "Пожалуйста, введите ID обращения в поддержку.",
        "request_sent": "Ваше обращение отправлено оператору.",
        "reply_prefix": "💬 Ответ оператора",
        "restart_hint": "Начните с команды /start.",
        "invalid_clid": "CLID не должен быть пустым. Отправьте ещё раз.",
        "invalid_support_id": "ID обращения не должен быть пустым. Отправьте ещё раз.",
        "issue_required": "Отправьте текст проблемы или фото.",
        "closed_to_user": "✅ Ваше обращение было закрыто.",
        "topics": {
            "drivers": "Проблемы с водителями",
            "dispatch": "Проблемы с диспетчерской программой",
            "payouts": "Проблемы с выплатами",
            "other": "Прочее",
        },
    },
    "en": {
        "lang_name": "English",
        "start": "Choose language",
        "choose_topic": "Please choose the request topic.",
        "ask_issue": "Send the issue as one text message or send a photo with caption.",
        "ask_clid": "Please enter the fleet CLID.",
        "ask_support_id": "Please enter the support request ID.",
        "request_sent": "Your request has been sent to the operator.",
        "reply_prefix": "💬 Operator reply",
        "restart_hint": "Start with /start.",
        "invalid_clid": "CLID must not be empty. Please send it again.",
        "invalid_support_id": "Support request ID must not be empty. Please send it again.",
        "issue_required": "Please send issue text or a photo.",
        "closed_to_user": "✅ Your request has been closed.",
        "topics": {
            "drivers": "Driver issues",
            "dispatch": "Dispatch software issues",
            "payouts": "Payout issues",
            "other": "Other",
        },
    },
}

user_sessions = {}


def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def init_db():
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_number TEXT UNIQUE,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            language TEXT NOT NULL,
            topic_key TEXT NOT NULL,
            topic_label TEXT NOT NULL,

            issue_kind TEXT NOT NULL DEFAULT 'text',
            issue_text TEXT,
            issue_photo_file_id TEXT,
            issue_photo_caption TEXT,

            clid TEXT NOT NULL,
            support_request_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',

            operator_group_message_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS request_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER NOT NULL,
            sender_type TEXT NOT NULL,
            telegram_user_id INTEGER,
            message_kind TEXT NOT NULL DEFAULT 'text',
            message_text TEXT,
            photo_file_id TEXT,
            photo_caption TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(request_id) REFERENCES requests(id)
        )
    """)

    conn.commit()
    conn.close()


def generate_request_number(request_id: int) -> str:
    return f"REQ-{request_id:06d}"


def create_request(
    user_id: int,
    username: Optional[str],
    full_name: str,
    language: str,
    topic_key: str,
    topic_label: str,
    issue_kind: str,
    issue_text: Optional[str],
    issue_photo_file_id: Optional[str],
    issue_photo_caption: Optional[str],
    clid: str,
    support_request_id: str,
) -> int:
    conn = db_connect()
    cur = conn.cursor()
    ts = now_iso()

    cur.execute("""
        INSERT INTO requests (
            request_number, user_id, username, full_name, language,
            topic_key, topic_label, issue_kind, issue_text,
            issue_photo_file_id, issue_photo_caption, clid,
            support_request_id, status, operator_group_message_id,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "",
        user_id,
        username,
        full_name,
        language,
        topic_key,
        topic_label,
        issue_kind,
        issue_text,
        issue_photo_file_id,
        issue_photo_caption,
        clid,
        support_request_id,
        "new",
        None,
        ts,
        ts,
    ))

    request_id = cur.lastrowid
    request_number = generate_request_number(request_id)

    cur.execute("""
        UPDATE requests
        SET request_number = ?, updated_at = ?
        WHERE id = ?
    """, (request_number, ts, request_id))

    cur.execute("""
        INSERT INTO request_messages (
            request_id, sender_type, telegram_user_id,
            message_kind, message_text, photo_file_id, photo_caption, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        request_id,
        "user",
        user_id,
        issue_kind,
        issue_text,
        issue_photo_file_id,
        issue_photo_caption,
        ts,
    ))

    conn.commit()
    conn.close()
    return request_id


def set_operator_group_message_id(request_id: int, message_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        UPDATE requests
        SET operator_group_message_id = ?, updated_at = ?
        WHERE id = ?
    """, (message_id, now_iso(), request_id))
    conn.commit()
    conn.close()


def get_request_by_group_message_id(group_message_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM requests WHERE operator_group_message_id = ?
    """, (group_message_id,))
    row = cur.fetchone()
    conn.close()
    return row


def get_request_by_number(request_number: str):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM requests WHERE request_number = ?
    """, (request_number,))
    row = cur.fetchone()
    conn.close()
    return row


def update_request_status(request_id: int, status: str):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        UPDATE requests
        SET status = ?, updated_at = ?
        WHERE id = ?
    """, (status, now_iso(), request_id))
    conn.commit()
    conn.close()


def add_request_message(
    request_id: int,
    sender_type: str,
    telegram_user_id: Optional[int],
    message_kind: str,
    message_text: Optional[str] = None,
    photo_file_id: Optional[str] = None,
    photo_caption: Optional[str] = None,
):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO request_messages (
            request_id, sender_type, telegram_user_id,
            message_kind, message_text, photo_file_id, photo_caption, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        request_id,
        sender_type,
        telegram_user_id,
        message_kind,
        message_text,
        photo_file_id,
        photo_caption,
        now_iso(),
    ))
    conn.commit()
    conn.close()


def get_requests_by_status(status: str, limit: int = 20):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM requests
        WHERE status = ?
        ORDER BY id DESC
        LIMIT ?
    """, (status, limit))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_stats():
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM requests")
    total = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) AS cnt FROM requests WHERE status = 'new'")
    new_cnt = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) AS cnt FROM requests WHERE status = 'in_progress'")
    in_progress_cnt = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) AS cnt FROM requests WHERE status = 'closed'")
    closed_cnt = cur.fetchone()["cnt"]

    conn.close()
    return total, new_cnt, in_progress_cnt, closed_cnt


def get_user_lang(user_id: int) -> str:
    session = user_sessions.get(user_id, {})
    return session.get("lang", "ru")


def build_language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Հայերեն", callback_data="lang:hy")],
        [InlineKeyboardButton("Русский", callback_data="lang:ru")],
        [InlineKeyboardButton("English", callback_data="lang:en")],
    ])


def build_topic_keyboard(lang: str) -> InlineKeyboardMarkup:
    topics = LANG_TEXTS[lang]["topics"]
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(topics["drivers"], callback_data="topic:drivers")],
        [InlineKeyboardButton(topics["dispatch"], callback_data="topic:dispatch")],
        [InlineKeyboardButton(topics["payouts"], callback_data="topic:payouts")],
        [InlineKeyboardButton(topics["other"], callback_data="topic:other")],
    ])


def build_operator_keyboard(request_number: str, status: str) -> InlineKeyboardMarkup:
    row1 = []
    if status == "new":
        row1.append(InlineKeyboardButton("🛠 Взять в работу", callback_data=f"req:start:{request_number}"))
        row1.append(InlineKeyboardButton("✅ Закрыть", callback_data=f"req:close:{request_number}"))
    elif status == "in_progress":
        row1.append(InlineKeyboardButton("✅ Закрыть", callback_data=f"req:close:{request_number}"))
        row1.append(InlineKeyboardButton("🔄 Переоткрыть", callback_data=f"req:reopen:{request_number}"))
    else:
        row1.append(InlineKeyboardButton("🔄 Переоткрыть", callback_data=f"req:reopen:{request_number}"))

    row2 = [InlineKeyboardButton("📋 Показать карточку", callback_data=f"req:card:{request_number}")]
    return InlineKeyboardMarkup([row1, row2])


def request_card_text(row) -> str:
    issue_block = row["issue_text"] or row["issue_photo_caption"] or "—"

    return (
        f"📩 Новое обращение от таксопарка\n\n"
        f"🧾 Номер заявки: {row['request_number']}\n"
        f"📌 Статус: {row['status']}\n"
        f"🌐 Язык: {LANG_TEXTS[row['language']]['lang_name']}\n"
        f"📂 Тема: {row['topic_label']}\n"
        f"🆔 CLID: {row['clid']}\n"
        f"🎫 ID обращения в поддержку: {row['support_request_id']}\n\n"
        f"👤 Имя: {row['full_name']}\n"
        f"👤 Telegram User ID: {row['user_id']}\n"
        f"🔗 Username: @{row['username'] if row['username'] else 'нет'}\n\n"
        f"💬 Описание:\n{issue_block}\n\n"
        f"🕒 Создано: {row['created_at']}"
    )


def short_request_line(row) -> str:
    return (
        f"{row['request_number']} | {row['status']} | "
        f"CLID: {row['clid']} | Support ID: {row['support_request_id']} | "
        f"{row['topic_label']}"
    )


async def send_request_to_operator_chat(context: ContextTypes.DEFAULT_TYPE, row) -> int:
    text = request_card_text(row)
    keyboard = build_operator_keyboard(row["request_number"], row["status"])

    if row["issue_kind"] == "photo" and row["issue_photo_file_id"]:
        sent = await context.bot.send_photo(
            chat_id=OPERATORS_CHAT_ID,
            photo=row["issue_photo_file_id"],
            caption=text,
            reply_markup=keyboard,
        )
    else:
        sent = await context.bot.send_message(
            chat_id=OPERATORS_CHAT_ID,
            text=text,
            reply_markup=keyboard,
        )

    return sent.message_id


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not update.message:
        return

    user_sessions[user.id] = {
        "step": Step.CHOOSE_LANGUAGE,
        "lang": "ru",
    }

    await update.message.reply_text(
        "Выберите язык / Choose language / Ընտրեք լեզուն",
        reply_markup=build_language_keyboard(),
    )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user

    if not query or not user:
        return

    await query.answer()

    data = query.data or ""

    if data.startswith("lang:"):
        lang = data.split(":", 1)[1]
        user_sessions[user.id] = {
            "step": Step.CHOOSE_TOPIC,
            "lang": lang,
        }
        await query.message.reply_text(
            LANG_TEXTS[lang]["choose_topic"],
            reply_markup=build_topic_keyboard(lang),
        )
        return

    if data.startswith("topic:"):
        topic_key = data.split(":", 1)[1]
        session = user_sessions.get(user.id)

        if not session:
            await query.message.reply_text(LANG_TEXTS["ru"]["restart_hint"])
            return

        session["topic"] = topic_key
        session["step"] = Step.WAIT_ISSUE
        lang = session["lang"]
        await query.message.reply_text(LANG_TEXTS[lang]["ask_issue"])
        return

    if data.startswith("req:"):
        parts = data.split(":")
        if len(parts) != 3:
            return

        action = parts[1]
        request_number = parts[2]

        row = get_request_by_number(request_number)
        if not row:
            await query.message.reply_text("Заявка не найдена.")
            return

        if query.message.chat_id != OPERATORS_CHAT_ID:
            return

        if action == "start":
            update_request_status(row["id"], "in_progress")
            row = get_request_by_number(request_number)
            await query.edit_message_reply_markup(
                reply_markup=build_operator_keyboard(request_number, row["status"])
            )
            await query.message.reply_text(
                f"🛠 Заявка {request_number} переведена в статус in_progress."
            )
            return

        if action == "close":
            update_request_status(row["id"], "closed")
            row = get_request_by_number(request_number)
            await query.edit_message_reply_markup(
                reply_markup=build_operator_keyboard(request_number, row["status"])
            )
            await query.message.reply_text(
                f"✅ Заявка {request_number} переведена в статус closed."
            )

            try:
                lang = row["language"]
                await context.bot.send_message(
                    chat_id=row["user_id"],
                    text=f"{LANG_TEXTS[lang]['closed_to_user']}\n\n🧾 Request number: {request_number}",
                )
            except Exception as e:
                logger.warning("Не удалось уведомить пользователя о закрытии: %s", e)
            return

        if action == "reopen":
            update_request_status(row["id"], "in_progress")
            row = get_request_by_number(request_number)
            await query.edit_message_reply_markup(
                reply_markup=build_operator_keyboard(request_number, row["status"])
            )
            await query.message.reply_text(
                f"🔄 Заявка {request_number} переоткрыта."
            )
            return

        if action == "card":
            row = get_request_by_number(request_number)
            if row["issue_kind"] == "photo" and row["issue_photo_file_id"]:
                await query.message.reply_photo(
                    photo=row["issue_photo_file_id"],
                    caption=request_card_text(row),
                )
            else:
                await query.message.reply_text(request_card_text(row))
            return


async def handle_private_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    user = update.effective_user
    chat = update.effective_chat

    if not message or not user or not chat or chat.type != "private":
        return

    session = user_sessions.get(user.id)
    if not session:
        await message.reply_text(LANG_TEXTS["ru"]["restart_hint"])
        return

    current_step = session.get("step")
    text = (message.text or "").strip()
    lang = session.get("lang", "ru")

    if current_step == Step.WAIT_ISSUE:
        if not text:
            await message.reply_text(LANG_TEXTS[lang]["issue_required"])
            return

        session["issue_kind"] = "text"
        session["issue_text"] = text
        session["issue_photo_file_id"] = None
        session["issue_photo_caption"] = None
        session["step"] = Step.WAIT_CLID
        await message.reply_text(LANG_TEXTS[lang]["ask_clid"])
        return

    if current_step == Step.WAIT_CLID:
        if not text:
            await message.reply_text(LANG_TEXTS[lang]["invalid_clid"])
            return

        session["clid"] = text
        session["step"] = Step.WAIT_SUPPORT_ID
        await message.reply_text(LANG_TEXTS[lang]["ask_support_id"])
        return

    if current_step == Step.WAIT_SUPPORT_ID:
        if not text:
            await message.reply_text(LANG_TEXTS[lang]["invalid_support_id"])
            return

        session["support_request_id"] = text
        session["step"] = Step.DONE

        topic_key = session["topic"]
        topic_label = LANG_TEXTS[lang]["topics"][topic_key]

        request_id = create_request(
            user_id=user.id,
            username=user.username,
            full_name=user.full_name,
            language=lang,
            topic_key=topic_key,
            topic_label=topic_label,
            issue_kind=session.get("issue_kind", "text"),
            issue_text=session.get("issue_text"),
            issue_photo_file_id=session.get("issue_photo_file_id"),
            issue_photo_caption=session.get("issue_photo_caption"),
            clid=session["clid"],
            support_request_id=session["support_request_id"],
        )

        request_number = generate_request_number(request_id)
        row = get_request_by_number(request_number)

        operator_message_id = await send_request_to_operator_chat(context, row)
        set_operator_group_message_id(request_id, operator_message_id)

        await message.reply_text(
            f"{LANG_TEXTS[lang]['request_sent']}\n\n🧾 Request number: {request_number}"
        )

        user_sessions[user.id] = {
            "step": Step.CHOOSE_LANGUAGE,
            "lang": lang,
        }
        return

    await message.reply_text(LANG_TEXTS[lang]["restart_hint"])


async def handle_private_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    user = update.effective_user
    chat = update.effective_chat

    if not message or not user or not chat or chat.type != "private":
        return

    session = user_sessions.get(user.id)
    if not session:
        await message.reply_text(LANG_TEXTS["ru"]["restart_hint"])
        return

    lang = session.get("lang", "ru")
    current_step = session.get("step")

    if current_step != Step.WAIT_ISSUE:
        await message.reply_text(LANG_TEXTS[lang]["restart_hint"])
        return

    if not message.photo:
        await message.reply_text(LANG_TEXTS[lang]["issue_required"])
        return

    largest_photo = message.photo[-1]
    session["issue_kind"] = "photo"
    session["issue_text"] = message.caption or ""
    session["issue_photo_file_id"] = largest_photo.file_id
    session["issue_photo_caption"] = message.caption or ""
    session["step"] = Step.WAIT_CLID

    await message.reply_text(LANG_TEXTS[lang]["ask_clid"])


async def handle_operator_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    chat = update.effective_chat
    operator = update.effective_user

    if not message or not chat or chat.id != OPERATORS_CHAT_ID:
        return

    if message.text and message.text.startswith("/"):
        return

    if not message.reply_to_message:
        await message.reply_text("Чтобы ответить пользователю, сделайте reply на сообщение заявки.")
        return

    replied_message_id = message.reply_to_message.message_id
    row = get_request_by_group_message_id(replied_message_id)

    if not row:
        await message.reply_text("Не найдена заявка для этого reply.")
        return

    lang = row["language"]
    request_number = row["request_number"]

    if row["status"] == "new":
        update_request_status(row["id"], "in_progress")

    if message.photo:
        largest_photo = message.photo[-1]
        caption = message.caption or ""

        await context.bot.send_photo(
            chat_id=row["user_id"],
            photo=largest_photo.file_id,
            caption=f"{LANG_TEXTS[lang]['reply_prefix']}\n\n{caption}\n\n🧾 Request number: {request_number}".strip(),
        )

        add_request_message(
            request_id=row["id"],
            sender_type="operator",
            telegram_user_id=operator.id if operator else None,
            message_kind="photo",
            photo_file_id=largest_photo.file_id,
            photo_caption=caption,
        )

        await message.reply_text(f"Фото-ответ отправлен пользователю.\n🧾 {request_number}")
        return

    text = message.text or message.caption or ""
    if not text:
        await message.reply_text("Поддерживаются текст и фото.")
        return

    await context.bot.send_message(
        chat_id=row["user_id"],
        text=f"{LANG_TEXTS[lang]['reply_prefix']}\n\n{text}\n\n🧾 Request number: {request_number}",
    )

    add_request_message(
        request_id=row["id"],
        sender_type="operator",
        telegram_user_id=operator.id if operator else None,
        message_kind="text",
        message_text=text,
    )

    await message.reply_text(f"Ответ отправлен пользователю.\n🧾 {request_number}")


async def cmd_new_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.id != OPERATORS_CHAT_ID:
        return

    rows = get_requests_by_status("new")
    if not rows:
        await update.message.reply_text("Новых заявок нет.")
        return

    await update.message.reply_text(
        "🆕 Новые заявки:\n\n" + "\n".join(short_request_line(r) for r in rows)
    )


async def cmd_in_progress(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.id != OPERATORS_CHAT_ID:
        return

    rows = get_requests_by_status("in_progress")
    if not rows:
        await update.message.reply_text("Заявок в работе нет.")
        return

    await update.message.reply_text(
        "🛠 Заявки в работе:\n\n" + "\n".join(short_request_line(r) for r in rows)
    )


async def cmd_closed_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.id != OPERATORS_CHAT_ID:
        return

    rows = get_requests_by_status("closed")
    if not rows:
        await update.message.reply_text("Закрытых заявок нет.")
        return

    await update.message.reply_text(
        "✅ Закрытые заявки:\n\n" + "\n".join(short_request_line(r) for r in rows)
    )


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.id != OPERATORS_CHAT_ID:
        return

    total, new_cnt, in_progress_cnt, closed_cnt = get_stats()
    await update.message.reply_text(
        "📊 Статистика заявок\n\n"
        f"Всего: {total}\n"
        f"New: {new_cnt}\n"
        f"In progress: {in_progress_cnt}\n"
        f"Closed: {closed_cnt}"
    )


def main():
    logger.info("BOT STARTING")
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("new_requests", cmd_new_requests))
    app.add_handler(CommandHandler("in_progress", cmd_in_progress))
    app.add_handler(CommandHandler("closed_requests", cmd_closed_requests))
    app.add_handler(CommandHandler("stats", cmd_stats))

    app.add_handler(CallbackQueryHandler(handle_callback))

    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.PHOTO, handle_private_photo),
        group=1,
    )
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, handle_private_text),
        group=2,
    )
    app.add_handler(
        MessageHandler(filters.Chat(OPERATORS_CHAT_ID) & (filters.TEXT | filters.PHOTO) & ~filters.COMMAND, handle_operator_reply),
        group=3,
    )

    logger.info("RUN POLLING")
    app.run_polling()


if __name__ == "__main__":
    main()
