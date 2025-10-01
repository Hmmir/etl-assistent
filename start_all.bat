@echo off
echo 🚀 Запуск ВСЕХ сервисов ETL Assistant...
docker-compose --profile all up -d
echo ✅ Все сервисы запущены!
echo.
echo 📊 Доступные сервисы:
echo   - Backend API: http://localhost:8000
echo   - Frontend: http://localhost:3000
echo   - API Docs: http://localhost:8000/docs
echo   - Airflow: http://localhost:8081 (admin/admin)
echo   - Hadoop NameNode: http://localhost:9870
echo   - Kafka UI: http://localhost:8080
echo   - ClickHouse: http://localhost:8123/play
echo   - PostgreSQL: localhost:5432
echo   - Redis: localhost:6379
echo   - Kafka: localhost:9092
echo.
pause
