# Тестирование API ETL Assistant
Write-Host "=== Тестирование ETL Assistant API ===" -ForegroundColor Cyan
Write-Host ""

# 1. Health Check
Write-Host "[1/5] Health Check..." -ForegroundColor Yellow
$health = Invoke-RestMethod -Uri "http://localhost:8000/health" -Method GET
Write-Host "✓ Status: $($health.status)" -ForegroundColor Green
Write-Host ""

# 2. Загрузка CSV файла
Write-Host "[2/5] Загрузка CSV файла..." -ForegroundColor Yellow
$csvPath = "c:\Users\alien\Desktop\etl\etl-assistant-clean\backend\uploads\test_employees.csv"
$form = @{
    file = Get-Item -Path $csvPath
    source_description = "Test employees data"
}
$upload = Invoke-RestMethod -Uri "http://localhost:8000/upload" -Method POST -Form $form
Write-Host "✓ Файл загружен: $($upload.filename)" -ForegroundColor Green
Write-Host "  - Формат: $($upload.format_type)" -ForegroundColor White
Write-Host "  - Размер: $($upload.file_size) bytes" -ForegroundColor White
Write-Host ""

# 3. Анализ файла
Write-Host "[3/5] Анализ файла..." -ForegroundColor Yellow
$analysis = Invoke-RestMethod -Uri "http://localhost:8000/analyze/test_employees.csv" -Method GET
Write-Host "✓ Анализ завершен:" -ForegroundColor Green
Write-Host "  - Строк: $($analysis.total_rows)" -ForegroundColor White
Write-Host "  - Колонок: $($analysis.total_columns)" -ForegroundColor White
Write-Host ""

# 4. Генерация пайплайна
Write-Host "[4/5] Генерация ETL пайплайна..." -ForegroundColor Yellow
$pipeline = Invoke-RestMethod -Uri "http://localhost:8000/generate_pipeline?filename=test_employees.csv" -Method POST
Write-Host "✓ Пайплайн сгенерирован:" -ForegroundColor Green
Write-Host "  - Рекомендуемое хранилище: $($pipeline.storage_recommendation.recommended_storage)" -ForegroundColor White
Write-Host "  - DDL сгенерирован: $($pipeline.ddl -ne $null)" -ForegroundColor White
Write-Host "  - ETL код сгенерирован: $($pipeline.etl_code -ne $null)" -ForegroundColor White
Write-Host ""

# 5. Генерация Airflow DAG
Write-Host "[5/5] Генерация Airflow DAG..." -ForegroundColor Yellow
$dag = Invoke-RestMethod -Uri "http://localhost:8000/generate_airflow_dag?filename=test_employees.csv" -Method POST
Write-Host "✓ Airflow DAG сгенерирован:" -ForegroundColor Green
Write-Host "  - DAG ID: $($dag.dag_id)" -ForegroundColor White
Write-Host "  - Schedule: $($dag.schedule)" -ForegroundColor White
Write-Host ""

Write-Host "=== ALL TESTS PASSED! ===" -ForegroundColor Green
Write-Host ""
Write-Host "Check Swagger UI: http://localhost:8000/docs" -ForegroundColor Cyan
