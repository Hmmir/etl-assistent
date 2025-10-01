@echo off
echo 🚀 Запуск Hadoop сервисов...
docker-compose --profile hadoop up -d
echo ✅ Hadoop запущен!
echo.
echo 📊 Hadoop доступен по адресу: http://localhost:9870
echo.
pause
