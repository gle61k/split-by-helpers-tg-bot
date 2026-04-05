@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo Установка зависимостей (если нужно)...
python -m pip install -r requirements.txt -q
echo Запуск бота...
echo В телефоне: просто отправьте файл боту. С ПК это окно должно быть открыто.
echo Чтобы не держать компьютер включенным — выложите бота на хостинг ^(Railway и т.п.^).
python tg_bot.py
pause
