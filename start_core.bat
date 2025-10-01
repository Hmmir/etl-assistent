@echo off
echo 🚀 Запуск основных сервисов ETL Assistant...
docker-compose --profile core up -d
echo ✅ Основные сервисы запущены!
echo.
echo 📊 Доступные сервисы:
echo   - Backend API: http://localhost:8000
echo   - Frontend: http://localhost:3000
echo   - API Docs: http://localhost:8000/docs
echo   - PostgreSQL: localhost:5432
echo   - ClickHouse: localhost:9000, http://localhost:8123/play
echo   - Redis: localhost:6379
echo.
pause
