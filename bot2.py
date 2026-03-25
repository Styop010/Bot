import os
import sqlite3
import logging
import random
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
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
OPERATORS_CHAT_ID = -1003855882419
DB_PATH = os.getenv("DB_PATH", "/var/data/support_bot.db")

# SLA в минутах — можешь поменять под себя
SLA_TAKE_MINUTES = 10
SLA_FIRST_REPLY_MINUTES = 20
SLA_CLOSE_HOURS = 24

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
        "restart_hint": "Սկսեք /start հրամանով։",
        "invalid_clid": "CLID-ը պետք է լինի միայն թվերից, սկսվի 40000-ով։",
        "invalid_support_id": "Support ID-ը չպետք է դատարկ լինի և պետք է լինի մինչև 50 սիմվոլ։",
        "issue_required": "Ուղարկեք խնդրի տեքստ կամ նկար։",
        "closed_to_user": "✅ Ձեր դիմումը փակվել է։",
        "wait_operator": "⏳ Խնդրում ենք սպասել օպերատորի պատասխանին ընթացիկ դիմումի շրջանակում։",
        "one_message_now": "✅ Օպերատորը պատասխանել է։ Դուք կարող եք ուղարկել մեկ հաջորդ հաղորդագրություն այս դիմումի շրջանակում։",
        "ticket_taken": "👤 Ձեր դիմումը վերցրել է օպերատորը։ Հանրային անունը՝ {alias}",
        "ticket_closed_create_new": "✅ Հին դիմումը փակված է։ Հիմա կարող եք ստեղծել նոր դիմում /start հրամանով։",
        "reply_prefix": "💬 {alias}-ի պատասխանը",
        "topics": {
            "drivers": "Վարորդների հետ խնդիրներ",
            "dispatch": "Դիսպետչերական ծրագրի խնդիրներ",
            "payouts": "Վճարումների խնդիրներ",
            "other": "Այլ",
        },
        "wait_button": "⏳ Սպասում եմ օպերատորի պատասխանին",
    },
    "ru": {
        "lang_name": "Русский",
        "start": "Выберите язык",
        "choose_topic": "Пожалуйста, выберите тему обращения.",
        "ask_issue": "Отправьте описание проблемы одним сообщением или отправьте фото с подписью.",
        "ask_clid": "Пожалуйста, введите CLID таксопарка.",
        "ask_support_id": "Пожалуйста, введите ID обращения в поддержку.",
        "request_sent": "Ваше обращение отправлено оператору.",
        "restart_hint": "Начните с команды /start.",
        "invalid_clid": "CLID должен содержать только цифры и начинаться с 40000.",
        "invalid_support_id": "ID обращения не должен быть пустым и должен быть не длиннее 50 символов.",
        "issue_required": "Отправьте текст проблемы или фото.",
        "closed_to_user": "✅ Ваше обращение было закрыто.",
        "wait_operator": "⏳ Дождитесь ответа оператора по текущему обращению.",
        "one_message_now": "✅ Оператор ответил. Вы можете отправить одно следующее сообщение в рамках текущего обращения.",
        "ticket_taken": "👤 Ваше обращение взял в работу оператор. Публичное имя: {alias}",
        "ticket_closed_create_new": "✅ Предыдущее обращение закрыто. Теперь вы можете открыть новое через /start.",
        "reply_prefix": "💬 Ответ оператора {alias}",
        "topics": {
            "drivers": "Проблемы с водителями",
            "dispatch": "Проблемы с диспетчерской программой",
            "payouts": "Проблемы с выплатами",
            "other": "Прочее",
        },
        "wait_button": "⏳ Жду ответа оператора",
    },
    "en": {
        "lang_name": "English",
        "start": "Choose language",
        "choose_topic": "Please choose the request topic.",
        "ask_issue": "Send the issue as one text message or send a photo with caption.",
        "ask_clid": "Please enter the fleet CLID.",
        "ask_support_id": "Please enter the support request ID.",
        "request_sent": "Your request has been sent to the operator.",
        "restart_hint": "Start with /start.",
        "invalid_clid": "CLID must contain digits only and start with 40000.",
        "invalid_support_id": "Support request ID must not be empty and must be no longer than 50 characters.",
        "issue_required": "Please send issue text or a photo.",
        "closed_to_user": "✅ Your request has been closed.",
        "wait_operator": "⏳ Please wait for the operator’s reply on your current ticket.",
        "one_message_now": "✅ The operator has replied. You may send one next message in this ticket.",
        "ticket_taken": "👤 Your request has been taken by an operator. Public name: {alias}",
        "ticket_closed_create_new": "✅ Your previous request is closed. You may now open a new one with /start.",
        "reply_prefix": "💬 Reply from {alias}",
        "topics": {
            "drivers": "Driver issues",
            "dispatch": "Dispatch software issues",
            "payouts": "Payout issues",
            "other": "Other",
        },
        "wait_button": "⏳ Waiting for operator reply",
    },
}

# Публичные псевдонимы операторов по языкам
PUBLIC_OPERATOR_ALIASES = {
    "ru": ["Анна", "Максим", "Елена", "Мария", "Даниил", "София"],
    "hy": ["Անի", "Դավիթ", "Մարիամ", "Նարե", "Արման", "Լիլիթ"],
    "en": ["Anna", "David", "Emma", "Daniel", "Sophia", "Leo"],
}

user_sessions = {}


# =========================
# DB
# =========================

def db_connect():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def column_exists(conn, table_name: str, column_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    cols = [row[1] for row in cur.fetchall()]
    return column_name in cols


def ensure_column(conn, table_name: str, column_name: str, column_def: str):
    if not column_exists(conn, table_name, column_name):
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
        conn.commit()


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def plus_minutes_iso(minutes: int) -> str:
    return (datetime.utcnow() + timedelta(minutes=minutes)).isoformat(timespec="seconds")


def plus_hours_iso(hours: int) -> str:
    return (datetime.utcnow() + timedelta(hours=hours)).isoformat(timespec="seconds")


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
            dialog_state TEXT NOT NULL DEFAULT 'waiting_operator',

            initial_group_message_id INTEGER,
            topic_thread_id INTEGER,

            assigned_operator_id INTEGER,
            assigned_operator_name TEXT,
            assigned_operator_alias TEXT,

            created_at TEXT NOT NULL,
            taken_at TEXT,
            first_reply_at TEXT,
            closed_at TEXT,
            updated_at TEXT NOT NULL,

            sla_take_deadline TEXT,
            sla_first_reply_deadline TEXT,
            sla_close_deadline TEXT
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

    # Миграция на случай старой базы
    ensure_column(conn, "requests", "dialog_state", "TEXT NOT NULL DEFAULT 'waiting_operator'")
    ensure_column(conn, "requests", "initial_group_message_id", "INTEGER")
    ensure_column(conn, "requests", "topic_thread_id", "INTEGER")
    ensure_column(conn, "requests", "assigned_operator_id", "INTEGER")
    ensure_column(conn, "requests", "assigned_operator_name", "TEXT")
    ensure_column(conn, "requests", "assigned_operator_alias", "TEXT")
    ensure_column(conn, "requests", "taken_at", "TEXT")
    ensure_column(conn, "requests", "first_reply_at", "TEXT")
    ensure_column(conn, "requests", "closed_at", "TEXT")
    ensure_column(conn, "requests", "sla_take_deadline", "TEXT")
    ensure_column(conn, "requests", "sla_first_reply_deadline", "TEXT")
    ensure_column(conn, "requests", "sla_close_deadline", "TEXT")

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
            support_request_id, status, dialog_state,
            initial_group_message_id, topic_thread_id,
            assigned_operator_id, assigned_operator_name, assigned_operator_alias,
            created_at, taken_at, first_reply_at, closed_at, updated_at,
            sla_take_deadline, sla_first_reply_deadline, sla_close_deadline
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        "waiting_operator",
        None,
        None,
        None,
        None,
        None,
        ts,
        None,
        None,
        None,
        ts,
        plus_minutes_iso(SLA_TAKE_MINUTES),
        plus_minutes_iso(SLA_FIRST_REPLY_MINUTES),
        plus_hours_iso(SLA_CLOSE_HOURS),
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


def get_request_by_id(request_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM requests WHERE id = ?", (request_id,))
    row = cur.fetchone()
    conn.close()
    return row


def get_request_by_number(request_number: str):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM requests WHERE request_number = ?", (request_number,))
    row = cur.fetchone()
    conn.close()
    return row


def get_request_by_initial_message_id(message_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM requests WHERE initial_group_message_id = ?", (message_id,))
    row = cur.fetchone()
    conn.close()
    return row


def get_request_by_thread_id(thread_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM requests WHERE topic_thread_id = ?", (thread_id,))
    row = cur.fetchone()
    conn.close()
    return row


def get_active_request_for_user(user_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM requests
        WHERE user_id = ? AND status != 'closed'
        ORDER BY id DESC
        LIMIT 1
    """, (user_id,))
    row = cur.fetchone()
    conn.close()
    return row


def set_initial_group_message_id(request_id: int, message_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        UPDATE requests
        SET initial_group_message_id = ?, updated_at = ?
        WHERE id = ?
    """, (message_id, now_iso(), request_id))
    conn.commit()
    conn.close()


def assign_request_to_operator(
    request_id: int,
    operator_id: int,
    operator_name: str,
    operator_alias: str,
    topic_thread_id: int,
):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        UPDATE requests
        SET status = 'in_progress',
            assigned_operator_id = ?,
            assigned_operator_name = ?,
            assigned_operator_alias = ?,
            topic_thread_id = ?,
            taken_at = ?,
            updated_at = ?
        WHERE id = ?
    """, (
        operator_id,
        operator_name,
        operator_alias,
        topic_thread_id,
        now_iso(),
        now_iso(),
        request_id,
    ))
    conn.commit()
    conn.close()


def update_request_status(request_id: int, status: str):
    conn = db_connect()
    cur = conn.cursor()

    if status == "closed":
        cur.execute("""
            UPDATE requests
            SET status = ?, dialog_state = 'closed', closed_at = ?, updated_at = ?
            WHERE id = ?
        """, (status, now_iso(), now_iso(), request_id))
    else:
        cur.execute("""
            UPDATE requests
            SET status = ?, updated_at = ?
            WHERE id = ?
        """, (status, now_iso(), request_id))

    conn.commit()
    conn.close()


def update_dialog_state(request_id: int, dialog_state: str):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        UPDATE requests
        SET dialog_state = ?, updated_at = ?
        WHERE id = ?
    """, (dialog_state, now_iso(), request_id))
    conn.commit()
    conn.close()


def mark_first_reply_if_needed(request_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT first_reply_at FROM requests WHERE id = ?", (request_id,))
    row = cur.fetchone()
    if row and not row["first_reply_at"]:
        cur.execute("""
            UPDATE requests
            SET first_reply_at = ?, updated_at = ?
            WHERE id = ?
        """, (now_iso(), now_iso(), request_id))
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


def get_requests_by_clid(clid: str, limit: int = 20):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM requests
        WHERE clid = ?
        ORDER BY id DESC
        LIMIT ?
    """, (clid, limit))
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


# =========================
# HELPERS
# =========================

def get_user_lang(user_id: int) -> str:
    session = user_sessions.get(user_id, {})
    return session.get("lang", "ru")


def pick_operator_alias(lang: str) -> str:
    aliases = PUBLIC_OPERATOR_ALIASES.get(lang, PUBLIC_OPERATOR_ALIASES["ru"])
    return random.choice(aliases)


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


def build_wait_keyboard(lang: str):
    return ReplyKeyboardMarkup(
        [[LANG_TEXTS[lang]["wait_button"]]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def build_general_ticket_keyboard(request_number: str, status: str):
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
    taken_line = ""
    if row["assigned_operator_name"]:
        taken_line = (
            f"\n👤 Взял в работу: {row['assigned_operator_name']}"
            f"\n🎭 Публичный псевдоним: {row['assigned_operator_alias'] or '—'}"
        )

    thread_line = ""
    if row["topic_thread_id"]:
        thread_line = f"\n🧵 Topic ID: {row['topic_thread_id']}"

    return (
        f"📩 Новое обращение от таксопарка\n\n"
        f"🧾 Номер заявки: {row['request_number']}\n"
        f"📌 Статус: {row['status']}\n"
        f"🔁 Диалог: {row['dialog_state']}\n"
        f"🌐 Язык: {LANG_TEXTS[row['language']]['lang_name']}\n"
        f"📂 Тема: {row['topic_label']}\n"
        f"🆔 CLID: {row['clid']}\n"
        f"🎫 ID обращения в поддержку: {row['support_request_id']}\n"
        f"{taken_line}"
        f"{thread_line}\n\n"
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


def validate_clid(clid: str) -> bool:
    return clid.isdigit() and clid.startswith("40000") and len(clid) >= 10


def validate_support_request_id(value: str) -> bool:
    return bool(value) and len(value) <= 50


async def send_wait_notice(context: ContextTypes.DEFAULT_TYPE, user_id: int, lang: str):
    await context.bot.send_message(
        chat_id=user_id,
        text=LANG_TEXTS[lang]["wait_operator"],
        reply_markup=build_wait_keyboard(lang),
    )


async def send_user_message_to_thread(context: ContextTypes.DEFAULT_TYPE, row, text=None, photo_file_id=None, caption=None):
    prefix = f"👤 Пользователь ответил по заявке {row['request_number']}\n\n"

    if photo_file_id:
        await context.bot.send_photo(
            chat_id=OPERATORS_CHAT_ID,
            message_thread_id=row["topic_thread_id"],
            photo=photo_file_id,
            caption=prefix + (caption or ""),
        )
    else:
        await context.bot.send_message(
            chat_id=OPERATORS_CHAT_ID,
            message_thread_id=row["topic_thread_id"],
            text=prefix + (text or ""),
        )


async def send_operator_reply_to_user(context: ContextTypes.DEFAULT_TYPE, row, text=None, photo_file_id=None, caption=None):
    alias = row["assigned_operator_alias"] or "Support"

    if photo_file_id:
        await context.bot.send_photo(
            chat_id=row["user_id"],
            photo=photo_file_id,
            caption=f"{LANG_TEXTS[row['language']]['reply_prefix'].format(alias=alias)}\n\n{caption or ''}\n\n🧾 Request number: {row['request_number']}",
            reply_markup=ReplyKeyboardRemove(),
        )
    else:
        await context.bot.send_message(
            chat_id=row["user_id"],
            text=f"{LANG_TEXTS[row['language']]['reply_prefix'].format(alias=alias)}\n\n{text or ''}\n\n🧾 Request number: {row['request_number']}",
            reply_markup=ReplyKeyboardRemove(),
        )


# =========================
# START / CALLBACKS
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    if not user or not message:
        return

    active = get_active_request_for_user(user.id)
    if active:
        lang = active["language"]
        if active["dialog_state"] == "waiting_operator":
            await message.reply_text(
                LANG_TEXTS[lang]["wait_operator"],
                reply_markup=build_wait_keyboard(lang),
            )
        elif active["dialog_state"] == "waiting_user":
            await message.reply_text(
                LANG_TEXTS[lang]["one_message_now"],
                reply_markup=ReplyKeyboardRemove(),
            )
        else:
            await message.reply_text(LANG_TEXTS[lang]["ticket_closed_create_new"])
        return

    user_sessions[user.id] = {
        "step": Step.CHOOSE_LANGUAGE,
        "lang": "ru",
    }

    await message.reply_text(
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

    # Выбор языка
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

    # Выбор темы
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

    # Операторские действия
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

        operator_name = user.full_name or str(user.id)

        # Взять в работу
        if action == "start":
            if row["topic_thread_id"]:
                await query.message.reply_text("Заявка уже взята в работу и переведена в отдельную тему.")
                return

            alias = pick_operator_alias(row["language"])

            forum_topic = await context.bot.create_forum_topic(
                chat_id=OPERATORS_CHAT_ID,
                name=f"{request_number} | CLID {row['clid']}",
            )

            thread_id = forum_topic.message_thread_id

            assign_request_to_operator(
                request_id=row["id"],
                operator_id=user.id,
                operator_name=operator_name,
                operator_alias=alias,
                topic_thread_id=thread_id,
            )

            row = get_request_by_number(request_number)

            # Обновить кнопки на старой карточке
            await query.edit_message_reply_markup(
                reply_markup=build_general_ticket_keyboard(request_number, row["status"])
            )

            # Сообщение в новую тему
            await context.bot.send_message(
                chat_id=OPERATORS_CHAT_ID,
                message_thread_id=thread_id,
                text=(
                    f"🧾 Тикет {request_number} взят в работу\n"
                    f"👤 Оператор: {operator_name}\n"
                    f"🎭 Публичный псевдоним для пользователя: {alias}\n"
                    f"📌 Статус: {row['status']}\n"
                    f"🔁 Диалог: {row['dialog_state']}"
                ),
            )

            # Карточка в тему
            if row["issue_kind"] == "photo" and row["issue_photo_file_id"]:
                await context.bot.send_photo(
                    chat_id=OPERATORS_CHAT_ID,
                    message_thread_id=thread_id,
                    photo=row["issue_photo_file_id"],
                    caption=request_card_text(row),
                )
            else:
                await context.bot.send_message(
                    chat_id=OPERATORS_CHAT_ID,
                    message_thread_id=thread_id,
                    text=request_card_text(row),
                )

            # Уведомить пользователя
            await context.bot.send_message(
                chat_id=row["user_id"],
                text=LANG_TEXTS[row["language"]]["ticket_taken"].format(alias=alias),
                reply_markup=build_wait_keyboard(row["language"]),
            )

            await query.message.reply_text(
                f"🛠 Заявка {request_number} взята в работу и перенесена в отдельную тему."
            )
            return

        # Закрыть
        if action == "close":
            update_request_status(row["id"], "closed")
            row = get_request_by_number(request_number)

            await query.edit_message_reply_markup(
                reply_markup=build_general_ticket_keyboard(request_number, row["status"])
            )

            await query.message.reply_text(f"✅ Заявка {request_number} переведена в статус closed.")

            try:
                await context.bot.send_message(
                    chat_id=row["user_id"],
                    text=f"{LANG_TEXTS[row['language']]['closed_to_user']}\n\n🧾 Request number: {request_number}\n\n{LANG_TEXTS[row['language']]['ticket_closed_create_new']}",
                    reply_markup=ReplyKeyboardRemove(),
                )
            except Exception as e:
                logger.warning("Не удалось уведомить пользователя о закрытии: %s", e)
            return

        # Переоткрыть
        if action == "reopen":
            update_request_status(row["id"], "in_progress")
            update_dialog_state(row["id"], "waiting_operator")
            row = get_request_by_number(request_number)

            await query.edit_message_reply_markup(
                reply_markup=build_general_ticket_keyboard(request_number, row["status"])
            )
            await query.message.reply_text(f"🔄 Заявка {request_number} переоткрыта.")
            return

        # Показать карточку
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


# =========================
# PRIVATE USER FLOW
# =========================

async def handle_private_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    user = update.effective_user
    chat = update.effective_chat

    if not message or not user or not chat or chat.type != "private":
        return

    # Игнорируем сообщения от самого wait-кнопки как "нормальное" сообщение
    incoming_text = (message.text or "").strip()
    has_photo = bool(message.photo)
    caption = message.caption or ""
    lang_session = get_user_lang(user.id)

    active = get_active_request_for_user(user.id)

    # Если есть активный тикет — не создаём новый
    if active:
        lang = active["language"]

        # Пока ждём оператора — блокируем повторные сообщения
        if active["dialog_state"] == "waiting_operator":
            await message.reply_text(
                LANG_TEXTS[lang]["wait_operator"],
                reply_markup=build_wait_keyboard(lang),
            )
            return

        # Пользователю разрешено ровно одно следующее сообщение
        if active["dialog_state"] == "waiting_user":
            if not active["topic_thread_id"]:
                await message.reply_text(
                    LANG_TEXTS[lang]["wait_operator"],
                    reply_markup=build_wait_keyboard(lang),
                )
                return

            if not incoming_text and not has_photo:
                await message.reply_text(
                    LANG_TEXTS[lang]["issue_required"],
                    reply_markup=ReplyKeyboardRemove(),
                )
                return

            if has_photo:
                largest_photo = message.photo[-1]
                await send_user_message_to_thread(
                    context=context,
                    row=active,
                    photo_file_id=largest_photo.file_id,
                    caption=caption,
                )
                add_request_message(
                    request_id=active["id"],
                    sender_type="user",
                    telegram_user_id=user.id,
                    message_kind="photo",
                    photo_file_id=largest_photo.file_id,
                    photo_caption=caption,
                )
            else:
                await send_user_message_to_thread(
                    context=context,
                    row=active,
                    text=incoming_text,
                )
                add_request_message(
                    request_id=active["id"],
                    sender_type="user",
                    telegram_user_id=user.id,
                    message_kind="text",
                    message_text=incoming_text,
                )

            update_dialog_state(active["id"], "waiting_operator")

            await message.reply_text(
                LANG_TEXTS[lang]["wait_operator"],
                reply_markup=build_wait_keyboard(lang),
            )
            return

    # Если активного тикета нет — обычный flow создания тикета
    session = user_sessions.get(user.id)
    if not session:
        await message.reply_text(LANG_TEXTS["ru"]["restart_hint"])
        return

    current_step = session.get("step")
    lang = session.get("lang", "ru")

    # Шаг: описание проблемы
    if current_step == Step.WAIT_ISSUE:
        if not incoming_text and not has_photo:
            await message.reply_text(LANG_TEXTS[lang]["issue_required"])
            return

        if has_photo:
            largest_photo = message.photo[-1]
            session["issue_kind"] = "photo"
            session["issue_text"] = caption
            session["issue_photo_file_id"] = largest_photo.file_id
            session["issue_photo_caption"] = caption
        else:
            session["issue_kind"] = "text"
            session["issue_text"] = incoming_text
            session["issue_photo_file_id"] = None
            session["issue_photo_caption"] = None

        session["step"] = Step.WAIT_CLID
        await message.reply_text(LANG_TEXTS[lang]["ask_clid"])
        return

    # Шаг: CLID
    if current_step == Step.WAIT_CLID:
        if not validate_clid(incoming_text):
            await message.reply_text(LANG_TEXTS[lang]["invalid_clid"])
            return

        session["clid"] = incoming_text
        session["step"] = Step.WAIT_SUPPORT_ID
        await message.reply_text(LANG_TEXTS[lang]["ask_support_id"])
        return

    # Шаг: support ID
    if current_step == Step.WAIT_SUPPORT_ID:
        if not validate_support_request_id(incoming_text):
            await message.reply_text(LANG_TEXTS[lang]["invalid_support_id"])
            return

        session["support_request_id"] = incoming_text
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

        keyboard = build_general_ticket_keyboard(request_number, row["status"])

        if row["issue_kind"] == "photo" and row["issue_photo_file_id"]:
            sent = await context.bot.send_photo(
                chat_id=OPERATORS_CHAT_ID,
                photo=row["issue_photo_file_id"],
                caption=request_card_text(row),
                reply_markup=keyboard,
            )
        else:
            sent = await context.bot.send_message(
                chat_id=OPERATORS_CHAT_ID,
                text=request_card_text(row),
                reply_markup=keyboard,
            )

        set_initial_group_message_id(request_id, sent.message_id)

        await message.reply_text(
            f"{LANG_TEXTS[lang]['request_sent']}\n\n🧾 Request number: {request_number}",
            reply_markup=build_wait_keyboard(lang),
        )

        # После создания тикета пользователь ждёт ответа оператора
        user_sessions[user.id] = {
            "step": Step.CHOOSE_LANGUAGE,
            "lang": lang,
        }
        return

    await message.reply_text(LANG_TEXTS[lang]["restart_hint"])


# =========================
# OPERATOR MESSAGES
# =========================

async def handle_operator_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    chat = update.effective_chat
    operator = update.effective_user

    if not message or not chat or chat.id != OPERATORS_CHAT_ID:
        return

    if not operator or operator.is_bot:
        return

    if message.text and message.text.startswith("/"):
        return

    # 1) Если сообщение пришло в отдельной теме тикета — это рабочий ответ оператора
    if message.message_thread_id:
        row = get_request_by_thread_id(message.message_thread_id)
        if row and row["status"] != "closed":
            if message.photo:
                largest_photo = message.photo[-1]
                caption = message.caption or ""

                await send_operator_reply_to_user(
                    context=context,
                    row=row,
                    photo_file_id=largest_photo.file_id,
                    caption=caption,
                )

                add_request_message(
                    request_id=row["id"],
                    sender_type="operator",
                    telegram_user_id=operator.id,
                    message_kind="photo",
                    photo_file_id=largest_photo.file_id,
                    photo_caption=caption,
                )
            else:
                text = message.text or message.caption or ""
                if not text:
                    return

                await send_operator_reply_to_user(
                    context=context,
                    row=row,
                    text=text,
                )

                add_request_message(
                    request_id=row["id"],
                    sender_type="operator",
                    telegram_user_id=operator.id,
                    message_kind="text",
                    message_text=text,
                )

            mark_first_reply_if_needed(row["id"])
            update_dialog_state(row["id"], "waiting_user")

            await context.bot.send_message(
                chat_id=row["user_id"],
                text=LANG_TEXTS[row["language"]]["one_message_now"],
                reply_markup=ReplyKeyboardRemove(),
            )

            await message.reply_text(
                f"Ответ отправлен пользователю.\n🧾 {row['request_number']}\n🔁 Диалог: waiting_user"
            )
            return

    # 2) Если оператор пишет reply на первичную карточку до взятия — подсказываем сначала взять
    if message.reply_to_message:
        row = get_request_by_initial_message_id(message.reply_to_message.message_id)
        if row and not row["topic_thread_id"]:
            await message.reply_text(
                f"Сначала нажмите «Взять в работу» у заявки {row['request_number']}. "
                f"После этого бот создаст отдельную тему для переписки."
            )
            return


# =========================
# COMMANDS FOR OPERATORS
# =========================

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

async def cmd_force_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.id != OPERATORS_CHAT_ID:
        return

    if not context.args:
        await update.message.reply_text("Использование: /force_close REQ-000123")
        return

    request_number = context.args[0].strip()

    row = get_request_by_number(request_number)
    if not row:
        await update.message.reply_text(f"Тикет {request_number} не найден.")
        return

    if row["status"] == "closed":
        await update.message.reply_text(f"Тикет {request_number} уже закрыт.")
        return

    update_request_status(row["id"], "closed")

    try:
        await context.bot.send_message(
            chat_id=row["user_id"],
            text=(
                f"✅ Ваше обращение {request_number} было закрыто оператором.\n\n"
                f"Теперь вы можете создать новое через /start."
            ),
            reply_markup=ReplyKeyboardRemove(),
        )
    except Exception as e:
        logger.warning("Не удалось уведомить пользователя: %s", e)

    await update.message.reply_text(f"Тикет {request_number} закрыт.")

async def cmd_force_close_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.id != OPERATORS_CHAT_ID:
        return

    conn = db_connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, request_number, user_id, language
        FROM requests
        WHERE status != 'closed'
    """)
    rows = cur.fetchall()

    if not rows:
        conn.close()
        await update.message.reply_text("Активных тикетов нет.")
        return

    cur.execute("""
        UPDATE requests
        SET status = 'closed',
            dialog_state = 'closed',
            closed_at = ?,
            updated_at = ?
        WHERE status != 'closed'
    """, (now_iso(), now_iso()))

    conn.commit()
    conn.close()

    closed_count = 0

    for row in rows:
        try:
            await context.bot.send_message(
                chat_id=row["user_id"],
                text=(
                    f"✅ Ваше обращение {row['request_number']} было принудительно закрыто.\n\n"
                    f"Теперь вы можете открыть новое через /start."
                ),
                reply_markup=ReplyKeyboardRemove(),
            )
        except Exception as e:
            logger.warning("Не удалось уведомить пользователя %s: %s", row["user_id"], e)

        closed_count += 1

    await update.message.reply_text(f"Принудительно закрыто тикетов: {closed_count}")


async def cmd_clid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.id != OPERATORS_CHAT_ID:
        return

    if not context.args:
        await update.message.reply_text("Использование: /clid 400004151762")
        return

    clid = context.args[0].strip()
    rows = get_requests_by_clid(clid)

    if not rows:
        await update.message.reply_text(f"По CLID {clid} заявки не найдены.")
        return

    await update.message.reply_text(
        f"🔎 Заявки по CLID {clid}:\n\n" + "\n".join(short_request_line(r) for r in rows)
    )


# =========================
# MAIN
# =========================

def main():
    logger.info("BOT STARTING")
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("new_requests", cmd_new_requests))
    app.add_handler(CommandHandler("in_progress", cmd_in_progress))
    app.add_handler(CommandHandler("closed_requests", cmd_closed_requests))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("clid", cmd_clid))
    app.add_handler(CommandHandler("force_close", cmd_force_close))
    app.add_handler(CommandHandler("force_close_all", cmd_force_close_all))

    app.add_handler(CallbackQueryHandler(handle_callback))

    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & (filters.TEXT | filters.PHOTO), handle_private_message),
        group=1,
    )

    app.add_handler(
        MessageHandler(filters.Chat(OPERATORS_CHAT_ID) & (filters.TEXT | filters.PHOTO) & ~filters.COMMAND, handle_operator_message),
        group=2,
    )

    logger.info("RUN POLLING")
    app.run_polling()


if __name__ == "__main__":
    main()
