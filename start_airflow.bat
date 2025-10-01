@echo off
echo 🚀 Запуск Airflow сервисов...
docker-compose --profile airflow up -d
echo ✅ Airflow запущен!
echo.
echo 📊 Airflow доступен по адресу: http://localhost:8081
echo    Логин: admin
echo    Пароль: admin
echo.
pause
