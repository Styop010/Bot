import os
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters

BOT_TOKEN = os.getenv("BOT_TOKEN")
OPERATORS_CHAT_ID = -5159543096

print("DEBUG BOT_TOKEN EXISTS:", BOT_TOKEN is not None)
print("DEBUG BOT_TOKEN REPR:", repr(BOT_TOKEN))
print("DEBUG BOT_TOKEN LEN:", len(BOT_TOKEN) if BOT_TOKEN else 0)

message_map = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("COMMAND /start RECEIVED")
    await update.message.reply_text("Напишите ваш вопрос, оператор скоро ответит.")

async def handle_private_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("PRIVATE MESSAGE RECEIVED")
    message = update.message
    chat = update.effective_chat
    user = update.effective_user

    if not message or not chat or not user:
        return

    sent = await context.bot.send_message(
        chat_id=OPERATORS_CHAT_ID,
        text=(
            f"📩 Новое обращение\n\n"
            f"👤 Имя: {user.full_name}\n"
            f"🆔 User ID: {user.id}\n"
            f"🔗 Username: @{user.username if user.username else 'нет'}\n\n"
            f"💬 Сообщение:\n{message.text}"
        )
    )

    message_map[sent.message_id] = user.id
    print("SENT TO GROUP:", sent.message_id)

    await message.reply_text("Ваше обращение отправлено оператору.")

async def handle_operator_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("GROUP REPLY RECEIVED")
    message = update.message
    chat = update.effective_chat

    if not message or not chat:
        return

    if not message.reply_to_message:
        print("NOT A REPLY")
        return

    replied_message_id = message.reply_to_message.message_id
    target_user_id = message_map.get(replied_message_id)

    print("REPLY TO:", replied_message_id)
    print("TARGET USER:", target_user_id)

    if not target_user_id:
        await message.reply_text("Не найден пользователь для этого reply.")
        return

    await context.bot.send_message(
        chat_id=target_user_id,
        text=f"💬 Ответ оператора:\n\n{message.text}"
    )

    await message.reply_text("Ответ отправлен пользователю.")

def main():
    print("BOT STARTING")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    print("APP BUILT")

    app.add_handler(CommandHandler("start", start), group=0)
    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
            handle_private_message,
        ),
        group=1,
    )
    app.add_handler(
        MessageHandler(
            filters.Chat(OPERATORS_CHAT_ID) & filters.REPLY & filters.TEXT & ~filters.COMMAND,
            handle_operator_reply,
        ),
        group=2,
    )

    print("RUN POLLING")
    app.run_polling()

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
