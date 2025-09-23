# 🔐 **Переменные окружения для SourceCraft.dev**

## 📋 **ОБЯЗАТЕЛЬНЫЕ ПЕРЕМЕННЫЕ**

После загрузки кода в SourceCraft, настройте следующие переменные в **Settings → Environment Variables:**

### **🤖 YandexGPT интеграция:**
```env
YANDEX_CLOUD_FOLDER_ID=b1g7xj5a9k8l2m3n4o5p
YANDEX_GPT_API_KEY=AQVNxxxxxxxxxxxxxxxxx
YANDEX_CLOUD_IAM_TOKEN=t1.9xxxxxxxxxxxxxxx
```

### **🔧 Основные настройки:**
```env
ENVIRONMENT=production
DEBUG=false
LOG_LEVEL=INFO
```

### **🌐 CORS и безопасность:**
```env
CORS_ORIGINS=https://etl-assistant-moscow.sourcecraft.dev
JWT_SECRET_KEY=your-super-secret-jwt-key-minimum-32-characters
```

### **⚡ Производительность:**
```env
MAX_FILE_SIZE=500MB
BATCH_SIZE=10000
REDIS_TTL=3600
```

### **📊 Мониторинг (опционально):**
```env
SENTRY_DSN=your-sentry-dsn-if-available
```

---

## 🚀 **АВТОМАТИЧЕСКОЕ РАЗВЕРТЫВАНИЕ**

После настройки переменных окружения:

1. **В SourceCraft Dashboard:** нажмите **"Deploy"**
2. **Система автоматически:**
   - Создаст Docker контейнеры
   - Настроит PostgreSQL, ClickHouse, Redis
   - Запустит Airflow
   - Настроит автоматическое масштабирование
   - Выдаст SSL сертификаты

3. **Через 5-10 минут получите:**
   - 🌐 **Frontend:** https://etl-assistant-moscow.sourcecraft.dev
   - 🔗 **Backend API:** https://backend-etl-assistant-moscow.sourcecraft.dev
   - 📊 **Airflow UI:** https://airflow-etl-assistant-moscow.sourcecraft.dev

---

## 📱 **ДЕМОНСТРАЦИЯ ДЛЯ ЖЮРИ**

**Публичный URL готов для показа жюри:** 
```
https://etl-assistant-moscow.sourcecraft.dev
```

### **🎬 Преимущества облачной демонстрации:**
- ✅ **Стабильность** - нет зависимости от локального ПК
- ✅ **Профессионализм** - production URL с SSL
- ✅ **Производительность** - облачные ресурсы
- ✅ **Масштабируемость** - автоматическое масштабирование под нагрузку
- ✅ **Мониторинг** - встроенная аналитика

---

## 🎯 **ПЛАН ДЕМОНСТРАЦИИ**

### **Для жюри показывать:**
1. **Открыть публичный URL** в браузере
2. **Загрузить большой CSV файл** (покажет масштабируемость)
3. **Продемонстрировать ИИ-анализ** и рекомендации
4. **Показать сгенерированный код** (не заглушки!)
5. **Подчеркнуть enterprise готовность** (SSL, мониторинг, автоматическое масштабирование)

### **Ключевые фразы:**
- "Это production-ready система в облаке"
- "Автоматическое масштабирование под нагрузку"
- "Готов к внедрению в ДИТ Москвы уже сегодня"
- "SSL, мониторинг, безопасность из коробки"

---

## 🏆 **КОНКУРЕНТНЫЕ ПРЕИМУЩЕСТВА**

**Ваш проект будет единственным с:**
- 🌐 **Production URL** вместо localhost демо
- 📊 **Managed services** (PostgreSQL, ClickHouse, Airflow)
- ⚡ **Автоматическое масштабирование**
- 🔐 **Enterprise безопасность**
- 📈 **Реальный мониторинг** и метрики

**🎊 Это произведет впечатление на жюри!**
