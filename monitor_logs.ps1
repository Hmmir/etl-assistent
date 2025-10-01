# Скрипт для мониторинга логов в реальном времени

param(
    [string]$LogType = "requests"  # app, requests, errors
)

$logsDir = "logs"

Write-Host "=== Мониторинг логов ETL Assistant ===" -ForegroundColor Cyan
Write-Host ""

# Находим последний лог файл
$today = Get-Date -Format "yyyyMMdd"
$logFile = Join-Path $logsDir "${LogType}_${today}.log"

if (-not (Test-Path $logFile)) {
    Write-Host "Лог файл не найден: $logFile" -ForegroundColor Yellow
    Write-Host "Ожидание создания файла..." -ForegroundColor Yellow
    
    # Ждем создания файла
    while (-not (Test-Path $logFile)) {
        Start-Sleep -Seconds 1
    }
}

Write-Host "Мониторинг: $logFile" -ForegroundColor Green
Write-Host "Нажмите Ctrl+C для остановки" -ForegroundColor Yellow
Write-Host ""
Write-Host "─────────────────────────────────────────────────────────" -ForegroundColor DarkGray
Write-Host ""

# Мониторинг в реальном времени
Get-Content $logFile -Wait -Tail 20 | ForEach-Object {
    $line = $_
    
    # Цветовое выделение
    if ($line -match "ERROR") {
        Write-Host $line -ForegroundColor Red
    }
    elseif ($line -match "WARNING") {
        Write-Host $line -ForegroundColor Yellow
    }
    elseif ($line -match "USER_ACTION") {
        Write-Host $line -ForegroundColor Cyan
    }
    elseif ($line -match "POST|GET|PUT|DELETE") {
        Write-Host $line -ForegroundColor Green
    }
    else {
        Write-Host $line
    }
}
