"""
Telegram-бот: выгрузка (.xls/.xlsx) → готовые xlsx (split_by_helpers).

Локально (polling): положите токен в файл .env рядом со скриптом:
    TELEGRAM_BOT_TOKEN=...
  или в PowerShell: $env:TELEGRAM_BOT_TOKEN="..."
  Запуск: python tg_bot.py

Вебхук (Railway или ngrok) — позже:
  На Railway сработает автоматически (есть RAILWAY_PROJECT_ID и публичный URL).
  Локально с вебхуком: USE_WEBHOOK=1 и PUBLIC_URL=https://...
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from telegram import BotCommand, Update
from telegram.error import Conflict
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram.request import HTTPXRequest

from split_by_helpers import process_export

_BOT_DIR = Path(__file__).resolve().parent
if load_dotenv is not None:
    load_dotenv(_BOT_DIR / ".env")

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

ALLOWED_DOC_EXTENSIONS = {".xls", ".xlsx"}
WEBHOOK_PATH = "/webhook"

# Дефолтные таймауты PTB (5 с) малы для выгрузки 2–3 МБ; расширяем скачивание/отправку файлов.
def _bot_http_request() -> HTTPXRequest:
    return HTTPXRequest(
        read_timeout=120.0,
        write_timeout=120.0,
        connect_timeout=30.0,
        media_write_timeout=120.0,
    )


def _configure_event_loop_policy() -> None:
    # На Windows без Selector policy polling python-telegram-bot часто «молчит» (нет ответов).
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _telegram_api_get(token: str, method: str, timeout: float = 30.0) -> dict:
    url = f"https://api.telegram.org/bot{token}/{method}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _diagnose_and_clear_webhook_for_polling(token: str) -> None:
    """
    Если у бота висит вебхук (Railway, старый деплой), getUpdates/polling не получает сообщения —
    в чате «тишина». Снимаем вебхук через HTTP до старта polling.
    """
    try:
        me = _telegram_api_get(token, "getMe")
        if not me.get("ok"):
            raise SystemExit(f"Токен не подходит: getMe → {me}")
        u = me["result"].get("username", "?")
        name = me["result"].get("first_name", "")
        wh = _telegram_api_get(token, "getWebhookInfo")
        res = wh.get("result") or {}
        url = (res.get("url") or "").strip()
        pending = res.get("pending_update_count", 0)
        print("\n========== TELEGRAM ==========")
        print(f"Бот: @{u} ({name})")
        print("Пишите в Telegram именно этому боту — иначе ответов не будет.")
        print(f"Вебхук до сброса: {url or '(нет)'}")
        print(f"Апдейтов в очереди у сервера: {pending}")
        if url:
            print("→ Снимаю вебхук (иначе локальный polling не видит сообщения).")
        del_url = (
            f"https://api.telegram.org/bot{token}/deleteWebhook"
            f"?drop_pending_updates=true"
        )
        req = urllib.request.Request(del_url, method="GET")
        with urllib.request.urlopen(req, timeout=30.0) as resp:
            del_j = json.loads(resp.read().decode("utf-8"))
        if not del_j.get("ok"):
            print(f"ВНИМАНИЕ: deleteWebhook: {del_j}")
        wh2 = _telegram_api_get(token, "getWebhookInfo")
        url2 = ((wh2.get("result") or {}).get("url") or "").strip()
        if url2:
            print(f"ОШИБКА: вебхук всё ещё: {url2}")
        else:
            print("Вебхук снят — polling может получать сообщения.")
        print("==============================\n")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        raise SystemExit(f"Ошибка HTTP к api.telegram.org: {e} {body}") from e
    except OSError as e:
        raise SystemExit(f"Нет сети или Telegram недоступен: {e}") from e


def _public_base_url() -> str | None:
    u = os.environ.get("PUBLIC_URL", "").strip().rstrip("/")
    if u:
        return u
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip().rstrip("/")
    if domain:
        if not domain.startswith("http"):
            return f"https://{domain}"
        return domain
    for key in ("RAILWAY_STATIC_URL", "WEBHOOK_BASE_URL"):
        u = os.environ.get(key, "").strip().rstrip("/")
        if u:
            return u
    return None


def _webhook_secret(token: str) -> str:
    explicit = os.environ.get("WEBHOOK_SECRET", "").strip()
    if explicit:
        return explicit[:256]
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:32]


def _document_suffix(doc) -> tuple[bool, str]:
    """(разрешён ли файл, суффикс с точкой для сохранения)."""
    name = (doc.file_name or "").strip()
    name_lower = name.lower()
    suffix = Path(name_lower).suffix
    mime = (doc.mime_type or "").lower()

    if suffix in ALLOWED_DOC_EXTENSIONS:
        return True, suffix
    if "spreadsheetml" in mime:
        return True, ".xlsx"
    if "ms-excel" in mime or mime == "application/vnd.ms-excel":
        return True, ".xls"
    if "excel" in mime or "officedocument.spreadsheet" in mime:
        return True, ".xlsx" if "spreadsheetml" in mime else ".xls"
    if suffix == "" and name_lower.endswith(".xls"):
        return True, ".xls"
    if suffix == "" and name_lower.endswith(".xlsx"):
        return True, ".xlsx"
    return False, suffix


def register_handlers(application: Application) -> None:
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(
        MessageHandler(filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.VOICE, handle_not_a_file)
    )
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_not_excel))


async def _post_init(application: Application) -> None:
    me = await application.bot.get_me()
    logger.info("Подключено к Telegram: @%s (%s)", me.username, me.first_name)
    await application.bot.set_my_commands(
        [BotCommand("start", "Справка"), BotCommand("help", "Справка")]
    )


async def _on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, Conflict):
        logger.critical(
            "КОНФЛИКТ Telegram: второй процесс с этим же токеном или снова включён вебхук. "
            "Закройте все другие окна python с ботом, остановите Railway/вебхук, перезапустите."
        )
    logger.error("Исключение в обработчике: %s", err, exc_info=err)


def build_application(token: str) -> Application:
    application = (
        Application.builder()
        .token(token)
        .request(_bot_http_request())
        .concurrent_updates(True)
        .post_init(_post_init)
        .build()
    )
    application.add_error_handler(_on_error)
    register_handlers(application)
    return application


HELP_TEXT = (
    "Ничего вводить не нужно.\n\n"
    "📎 Нажмите скрепку → «Файл» → выберите выгрузку с расширением .xls или .xlsx.\n\n"
    "Бот сам разобьёт её и пришлёт готовые Excel обратно в этот чат "
    "(до пяти файлов по группам + «остальные», если в таблице есть колонка группы).\n\n"
    "Команда /start не обязательна — можно сразу кидать файл."
)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message:
        return
    await update.effective_message.reply_text(HELP_TEXT)


async def handle_text_not_excel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg or not msg.text:
        return
    await msg.reply_text(
        "❌ Текст не нужен.\n\n"
        "Отправьте файл выгрузки: скрепка → «Файл» → .xls или .xlsx (не фото, не голый текст)."
    )


async def handle_not_a_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    await msg.reply_text(
        "❌ Нужен файл Excel.\n"
        "Скрепка → «Файл» → документ .xls / .xlsx. Фото/видео сюда не подходят."
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg or not msg.document:
        return

    doc = msg.document
    logger.info(
        "Документ: name=%s size=%s mime=%s chat=%s",
        doc.file_name,
        getattr(doc, "file_size", None),
        doc.mime_type,
        msg.chat_id,
    )
    ok, suffix = _document_suffix(doc)
    if not ok:
        await msg.reply_text(
            "❌ Нужен именно Excel: .xls или .xlsx как файл (не картинка)."
        )
        return

    status = await msg.reply_text("⏳ Скачиваю и режу файл…")

    try:
        tg_file = await context.bot.get_file(doc.file_id)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            in_name = doc.file_name or f"export{suffix}"
            in_path = tmp_path / in_name
            await tg_file.download_to_drive(custom_path=str(in_path))

            out_dir = tmp_path / "out"
            out_dir.mkdir(parents=True, exist_ok=True)

            created, notes = await asyncio.to_thread(process_export, in_path, out_dir)

            if not created:
                await status.edit_text(
                    "❌ Не удалось сформировать файлы: нет данных или не подошёл формат выгрузки."
                )
                return

            note_block = ""
            if notes:
                note_block = "\n\n" + "\n".join(notes)

            await status.edit_text(
                f"✅ Обработка завершена! Создано файлов: {len(created)}{note_block}\n\n"
                "Отправляю файлы…"
            )

            for p in sorted(created, key=lambda x: x.name):
                with open(p, "rb") as f:
                    await msg.reply_document(document=f, filename=p.name)

            await msg.reply_text(
                "✅ Готово. Если файлов меньше пяти — в выгрузке нет колонки «Группа помощника»; "
                "тогда всё попадает в один «УР_Остальные»."
            )
            try:
                await status.delete()
            except Exception:
                pass
    except Exception as e:
        logger.exception("handle_document failed")
        try:
            await status.edit_text(f"❌ Ошибка: {e}")
        except Exception:
            await msg.reply_text(f"❌ Ошибка: {e}")


def run_webhook_server() -> None:
    """
    Railway / облако: встроенный HTTP-сервер PTB (aiohttp), без Starlette/uvicorn.
    Так корректно обрабатываются вебхук и secret_token; раньше свой ASGI-обработчик
    мог не получать или отбрасывать запросы Telegram.
    """
    token = os.environ["TELEGRAM_BOT_TOKEN"].strip()
    port = int(os.environ.get("PORT", "8080"))
    public = _public_base_url()
    if not public:
        raise SystemExit(
            "Railway: нет публичного URL. Включите Public Networking или задайте PUBLIC_URL (https://...)."
        )
    webhook_url = f"{public.rstrip('/')}{WEBHOOK_PATH}"
    url_path = WEBHOOK_PATH.strip("/") or "webhook"
    secret = _webhook_secret(token)

    application = build_application(token)
    logger.info("Вебхук PTB: url=%s path=/%s port=%s", webhook_url, url_path, port)

    application.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path=url_path,
        webhook_url=webhook_url,
        secret_token=secret,
        allowed_updates=None,
        drop_pending_updates=True,
        bootstrap_retries=5,
        close_loop=True,
    )


def run_polling() -> None:
    token = os.environ["TELEGRAM_BOT_TOKEN"].strip()
    _diagnose_and_clear_webhook_for_polling(token)
    application = build_application(token)
    logger.info("Режим polling. Остановка: Ctrl+C.")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        bootstrap_retries=5,
    )


def main() -> None:
    _configure_event_loop_policy()
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit(
            "Задайте переменную окружения TELEGRAM_BOT_TOKEN (токен от @BotFather в Telegram)."
        )

    public = _public_base_url()
    on_railway = bool(os.environ.get("RAILWAY_PROJECT_ID", "").strip())
    want_webhook = os.environ.get("USE_WEBHOOK", "").strip().lower() in ("1", "true", "yes", "on")

    # Railway: только вебхук + HTTP на PORT (настроите позже).
    if on_railway:
        if not public:
            raise SystemExit(
                "Railway: нет публичного URL. Включите Public Networking "
                "или задайте PUBLIC_URL (https://...)."
            )
        run_webhook_server()
        return

    # Локально: всегда polling, пока явно не включите вебхук и не укажете PUBLIC_URL.
    if want_webhook and public:
        run_webhook_server()
        return

    run_polling()


if __name__ == "__main__":
    main()
