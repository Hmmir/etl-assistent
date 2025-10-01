# Скрипт запуска Backend с детальным логированием

Write-Host "=== Запуск ETL Assistant Backend ===" -ForegroundColor Cyan
Write-Host ""

# Проверка Python
$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    Write-Host "ERROR: Python не найден!" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Python версия: " -NoNewline
python --version

# Переход в директорию backend
Set-Location -Path "backend"

# Проверка виртуального окружения
if (-not (Test-Path "venv")) {
    Write-Host ""
    Write-Host "Создаем виртуальное окружение..." -ForegroundColor Yellow
    python -m venv venv
}

# Активация виртуального окружения
Write-Host "✓ Активация виртуального окружения..."
.\venv\Scripts\Activate.ps1

# Установка зависимостей
Write-Host "✓ Проверка зависимостей..."
pip install -q -r requirements.txt

# Установка API ключа
$env:OPENROUTER_API_KEY = "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"

Write-Host ""
Write-Host "=== Backend запускается ===" -ForegroundColor Green
Write-Host ""
Write-Host "API будет доступен на:" -ForegroundColor Cyan
Write-Host "  → http://localhost:8000" -ForegroundColor White
Write-Host "  → http://localhost:8000/docs (Swagger)" -ForegroundColor White
Write-Host ""
Write-Host "Логи сохраняются в:" -ForegroundColor Cyan
Write-Host "  → logs/app_*.log (все логи)" -ForegroundColor White
Write-Host "  → logs/requests_*.log (HTTP запросы)" -ForegroundColor White
Write-Host "  → logs/errors_*.log (ошибки)" -ForegroundColor White
Write-Host ""
Write-Host "Нажмите Ctrl+C для остановки" -ForegroundColor Yellow
Write-Host ""

# Запуск uvicorn
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
