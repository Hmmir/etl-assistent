@echo off
cd /d "%~dp0backend"

echo ============================================
echo   ETL Assistant Backend
echo ============================================
echo.

echo [1/4] Активация виртуального окружения...
call venv\Scripts\activate.bat

echo [2/4] Установка зависимостей...
pip install -q uvicorn fastapi python-multipart pandas chardet lxml

echo [3/4] Настройка окружения...
set OPENROUTER_API_KEY=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

echo [4/4] Запуск backend...
echo.
echo ============================================
echo   Backend доступен на:
echo   - API: http://localhost:8000
echo   - Docs: http://localhost:8000/docs
echo   - Health: http://localhost:8000/health
echo ============================================
echo.
echo Нажмите Ctrl+C для остановки
echo.

python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

pause
