# Telegram-бот: разбивка выгрузки часов по помощникам

Бот принимает файл выгрузки Excel (`.xls` / `.xlsx`), обрабатывает его скриптом `split_by_helpers.py` и присылает готовые `.xlsx` в чат.

Репозиторий: [github.com/gle61k/split-by-helpers-tg-bot](https://github.com/gle61k/split-by-helpers-tg-bot)

## Локально (Windows)

1. Скопируйте `.env.example` в `.env`, вставьте токен от [@BotFather](https://t.me/BotFather).
2. Запустите `run_bot.bat` или: `pip install -r requirements.txt` → `python tg_bot.py`.
3. В Telegram откройте своего бота и отправьте файл выгрузки (**скрепка → Файл**). Команда `/start` не обязательна.

Пока окно с ботом закрыто или компьютер выключен, бот не работает.

## Облако (Railway) — с телефона 24/7

Пошаговая инструкция: **[RAILWAY.md](./RAILWAY.md)**.

## Файлы

| Файл | Назначение |
|------|------------|
| `tg_bot.py` | Telegram-бот (polling локально, webhook на Railway) |
| `split_by_helpers.py` | Логика разбивки выгрузки |
| `requirements.txt` | Зависимости Python |

**Не коммитьте** файл `.env` (там токен). В GitHub попадает только `.env.example`.
