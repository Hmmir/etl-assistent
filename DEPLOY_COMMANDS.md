# 🚀 **Команды для завершения деплоя**

## 📋 **После добавления SSH ключа в SourceCraft:**

### **1. Загрузка кода:**
```bash
git push -u origin main
```

### **2. Проверка успешной загрузки:**
```bash
git log --oneline -5
```

### **3. Просмотр файлов в репозитории:**
```bash
git ls-tree -r HEAD --name-only
```

---

## 🔧 **Настройка переменных окружения в SourceCraft:**

После успешного push'а:

1. **Откройте SourceCraft Dashboard**
2. **Найдите проект "digital-ing"**  
3. **Settings → Environment Variables**
4. **Добавьте переменные из файла SOURCECRAFT_ENV_VARS.md**

### **Основные переменные:**
```env
ENVIRONMENT=production
DEBUG=false
LOG_LEVEL=INFO
CORS_ORIGINS=https://digital-ing-etl-assistant.sourcecraft.dev
MAX_FILE_SIZE=500MB
BATCH_SIZE=10000
```

---

## 🚀 **Автоматическое развертывание:**

1. **В SourceCraft Dashboard нажмите "Deploy"**
2. **Ждите 5-10 минут автоматического развертывания**
3. **Получите production URLs:**
   - 🌐 Frontend: https://digital-ing-etl-assistant.sourcecraft.dev
   - 🔗 Backend: https://backend-digital-ing-etl-assistant.sourcecraft.dev
   - 📊 Airflow: https://airflow-digital-ing-etl-assistant.sourcecraft.dev

---

## 🎯 **Готовность к демонстрации:**

После успешного развертывания:
- ✅ Production URL готов для жюри
- ✅ Автоматическое масштабирование настроено
- ✅ SSL сертификаты выданы
- ✅ Monitoring включен
- ✅ Все сервисы (PostgreSQL, ClickHouse, Airflow, Redis) запущены

**🏆 ПРОЕКТ ГОТОВ К ПОБЕДЕ НА ХАКАТОНЕ!**
