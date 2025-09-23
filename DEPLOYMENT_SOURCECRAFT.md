# 🚀 **Развертывание в SourceCraft.dev**

> **Полное руководство по развертыванию ETL Assistant в облачной платформе SourceCraft**

---

## 🌟 **ПРЕИМУЩЕСТВА SOURCECRAFT**

### ✨ **Что получаете автоматически:**
- 🏗️ **Managed Infrastructure** - PostgreSQL, ClickHouse, Airflow
- 🔐 **Enterprise Security** - HTTPS, JWT, Rate Limiting
- 📊 **Monitoring & Logging** - Prometheus, Grafana, ELK Stack
- 🚀 **Auto Scaling** - динамическое масштабирование под нагрузку
- 💰 **Cost Optimization** - оплата только за использованные ресурсы
- 🌐 **Global CDN** - быстрый доступ из любой точки мира

---

## 📋 **ПОШАГОВОЕ РАЗВЕРТЫВАНИЕ**

### **🔑 Шаг 1: Подготовка репозитория**
```bash
# Убедитесь что код закоммичен
git add .
git commit -m "🚀 Готов к развертыванию в SourceCraft"
git push origin main
```

### **🌐 Шаг 2: Создание проекта в SourceCraft**
1. **Войдите в [SourceCraft.dev](https://sourcecraft.dev)**
2. **Dashboard → New Project**
3. **Import from Repository → GitHub/GitLab**
4. **Выберите ваш репозиторий**

### **⚙️ Шаг 3: Конфигурация проекта**
```yaml
# sourcecraft.yml (создается автоматически)
name: etl-assistant
region: ru-central1

services:
  backend:
    type: python
    runtime: python-3.10
    build:
      command: pip install -r requirements.txt
    start: uvicorn backend.main:app --host 0.0.0.0 --port $PORT
    environment:
      - YANDEX_CLOUD_FOLDER_ID=${YANDEX_CLOUD_FOLDER_ID}
      - YANDEX_GPT_API_KEY=${YANDEX_GPT_API_KEY}
    resources:
      cpu: 2
      memory: 4Gi
      storage: 10Gi
      
  frontend:
    type: nodejs
    runtime: node-18
    build:
      command: npm install && npm run build
    start: npx serve -s build -l $PORT
    resources:
      cpu: 1
      memory: 2Gi
      
databases:
  postgres:
    type: postgresql
    version: "15"
    plan: standard
    storage: 20Gi
    
  clickhouse:
    type: clickhouse
    version: "23.8"
    plan: standard
    storage: 50Gi
    
  redis:
    type: redis
    version: "7.0"
    plan: basic
    
orchestration:
  airflow:
    type: apache-airflow
    version: "2.7.2"
    workers: 2
    storage: 10Gi
```

### **🔐 Шаг 4: Переменные окружения**
В SourceCraft Dashboard → Settings → Environment Variables:
```env
# YandexGPT интеграция
YANDEX_CLOUD_FOLDER_ID=b1g7xj5a9k8l2m3n4o5p
YANDEX_GPT_API_KEY=AQVNxxxxxxxxxxxxxxxxx
YANDEX_CLOUD_IAM_TOKEN=t1.9xxxxxxxxxxxxxxx

# Безопасность
JWT_SECRET_KEY=your-super-secret-jwt-key
CORS_ORIGINS=https://your-domain.sourcecraft.dev

# Производительность
MAX_FILE_SIZE=500MB
BATCH_SIZE=10000
REDIS_TTL=3600

# Мониторинг
LOGGING_LEVEL=INFO
SENTRY_DSN=your-sentry-dsn (опционально)
```

### **🚀 Шаг 5: Развертывание**
```bash
# Автоматическое развертывание
git push origin main
# SourceCraft автоматически:
# 1. Сканирует код
# 2. Создает контейнеры
# 3. Настраивает базы данных
# 4. Запускает сервисы
# 5. Настраивает домены
```

---

## 🌐 **ДОСТУП К СЕРВИСАМ**

После успешного развертывания:

### **📱 Основные сервисы:**
- 🌐 **Frontend:** `https://etl-assistant.sourcecraft.dev`
- 🔗 **Backend API:** `https://api-etl-assistant.sourcecraft.dev`
- 📚 **API Docs:** `https://api-etl-assistant.sourcecraft.dev/docs`

### **🗄️ Базы данных:**
- 🐘 **PostgreSQL:** `postgres-etl-assistant.internal:5432`
- 📊 **ClickHouse:** `clickhouse-etl-assistant.internal:8123`
- 🔄 **Redis:** `redis-etl-assistant.internal:6379`

### **🛩️ Оркестрация:**
- 📊 **Airflow UI:** `https://airflow-etl-assistant.sourcecraft.dev`
- 👤 **Логин:** `admin` / `ваш-пароль-из-панели`

### **📊 Мониторинг:**
- 📈 **Metrics:** `https://metrics-etl-assistant.sourcecraft.dev`
- 📋 **Logs:** `https://logs-etl-assistant.sourcecraft.dev`
- 🚨 **Alerts:** автоматические уведомления в Slack/Email

---

## 🔧 **КОНФИГУРАЦИЯ PRODUCTION**

### **🏗️ Обновленный backend/main.py:**
```python
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware

app = FastAPI(
    title="ETL Assistant API",
    description="Intelligent Data Engineer for Moscow DIT",
    version="1.0.0",
    docs_url="/docs" if os.getenv("ENVIRONMENT") != "production" else None
)

# Production middleware
if os.getenv("ENVIRONMENT") == "production":
    app.add_middleware(HTTPSRedirectMiddleware)

# CORS для production
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# YandexGPT configuration
YANDEX_CONFIG = {
    "folder_id": os.getenv("YANDEX_CLOUD_FOLDER_ID"),
    "api_key": os.getenv("YANDEX_GPT_API_KEY"),
    "iam_token": os.getenv("YANDEX_CLOUD_IAM_TOKEN")
}

# Database connections
DATABASE_URLS = {
    "postgres": f"postgresql://user:password@postgres-etl-assistant.internal:5432/etl_assistant",
    "clickhouse": "http://clickhouse-etl-assistant.internal:8123",
    "redis": "redis://redis-etl-assistant.internal:6379"
}
```

### **📦 Production requirements.txt:**
```txt
fastapi==0.104.1
uvicorn[standard]==0.24.0
python-multipart==0.0.6
aiofiles==23.2.1
pandas==2.1.3
clickhouse-driver==0.2.6
psycopg2-binary==2.9.9
redis==5.0.1
apache-airflow==2.7.2
chardet==5.2.0
pydantic==2.5.0
sqlalchemy>=1.4.28,<2.0
alembic==1.12.1

# Production only
gunicorn==21.2.0
prometheus-client==0.19.0
sentry-sdk[fastapi]==1.38.0
python-jose[cryptography]==3.3.0
passlib[bcrypt]==1.7.4
```

### **🐳 Production Dockerfile:**
```dockerfile
# backend/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["gunicorn", "backend.main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000"]
```

```dockerfile
# frontend/Dockerfile
FROM node:18-alpine as build

WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production

COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/nginx.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
```

---

## 📊 **МОНИТОРИНГ И АЛЕРТИНГ**

### **📈 Метрики для отслеживания:**
```python
# backend/monitoring.py
from prometheus_client import Counter, Histogram, generate_latest

# Кастомные метрики
file_uploads = Counter('etl_file_uploads_total', 'Total file uploads')
pipeline_generations = Counter('etl_pipeline_generations_total', 'Total pipeline generations')
processing_time = Histogram('etl_processing_seconds', 'Time spent processing files')

@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    processing_time.observe(process_time)
    return response

@app.get("/metrics")
async def get_metrics():
    return Response(generate_latest(), media_type="text/plain")
```

### **🚨 Настройка алертов:**
```yaml
# alerts.yml
alerts:
  - name: high_error_rate
    condition: error_rate > 5%
    duration: 5m
    notification: slack
    
  - name: high_response_time
    condition: avg_response_time > 2s
    duration: 3m
    notification: email
    
  - name: database_connection_failed
    condition: db_connection_success < 1
    duration: 1m
    notification: phone
```

---

## 🔐 **БЕЗОПАСНОСТЬ PRODUCTION**

### **🛡️ JWT аутентификация:**
```python
# backend/auth.py
from fastapi_users import FastAPIUsers
from fastapi_users.authentication import JWTAuthentication

SECRET = os.getenv("JWT_SECRET_KEY")

jwt_authentication = JWTAuthentication(
    secret=SECRET,
    lifetime_seconds=3600,
    tokenUrl="auth/jwt/login",
)

fastapi_users = FastAPIUsers(
    user_manager,
    [jwt_authentication],
)

# Защищенные endpoints
@app.get("/protected-endpoint")
async def protected_route(user: User = Depends(fastapi_users.current_user())):
    return {"message": f"Hello {user.email}!"}
```

### **⚡ Rate Limiting:**
```python
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.post("/upload")
@limiter.limit("10/minute")  # Максимум 10 загрузок в минуту
async def upload_file(request: Request, file: UploadFile):
    # логика загрузки
    pass
```

---

## 🚀 **CI/CD PIPELINE**

### **📋 GitHub Actions:**
```yaml
# .github/workflows/deploy.yml
name: Deploy to SourceCraft

on:
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: pytest tests/
      
  deploy:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to SourceCraft
        uses: sourcecraft/deploy-action@v2
        with:
          token: ${{ secrets.SOURCECRAFT_TOKEN }}
          project: etl-assistant
```

---

## 🎯 **ОПТИМИЗАЦИЯ ПРОИЗВОДИТЕЛЬНОСТИ**

### **⚡ Масштабирование в SourceCraft:**
```yaml
# Автоматическое масштабирование
scaling:
  backend:
    min_instances: 2
    max_instances: 10
    target_cpu: 70%
    target_memory: 80%
    
  frontend:
    min_instances: 1
    max_instances: 5
    target_cpu: 60%
    
# Распределение нагрузки
load_balancer:
  type: application
  health_check: /health
  timeout: 30s
```

### **🗄️ Оптимизация баз данных:**
```sql
-- ClickHouse оптимизация
ALTER TABLE etl_data 
  ORDER BY (timestamp, user_id)
  PARTITION BY toYYYYMM(timestamp)
  SETTINGS index_granularity = 8192;

-- PostgreSQL индексы
CREATE INDEX CONCURRENTLY idx_pipeline_status 
  ON pipelines (status, created_at) 
  WHERE status IN ('running', 'pending');
```

---

## 📞 **ПОДДЕРЖКА И МОНИТОРИНГ**

### **📊 SourceCraft Dashboard:**
- 🚀 **Deployments** - история развертываний
- 📈 **Analytics** - метрики использования
- 💰 **Billing** - расходы и оптимизация
- 🔧 **Settings** - конфигурация сервисов
- 📋 **Logs** - реальное время логов
- 🚨 **Alerts** - настройка уведомлений

### **🛠️ Управление через CLI:**
```bash
# Установка SourceCraft CLI
npm install -g @sourcecraft/cli

# Авторизация
sourcecraft login

# Просмотр статуса
sourcecraft status etl-assistant

# Просмотр логов
sourcecraft logs etl-assistant --follow

# Масштабирование
sourcecraft scale etl-assistant backend --instances 5

# Развертывание новой версии
sourcecraft deploy etl-assistant
```

---

## 💰 **СТОИМОСТЬ И ОПТИМИЗАЦИЯ**

### **💵 Примерная стоимость в месяц:**
```
📊 Compute (Backend + Frontend):     $50-150
🗄️ PostgreSQL (20GB):               $25-40
📈 ClickHouse (50GB):                $80-120
🔄 Redis (Basic):                    $15-25
🛩️ Airflow (2 workers):             $60-100
🌐 CDN + Load Balancer:              $20-40
📊 Monitoring & Logs:                $30-50
─────────────────────────────────────────
💰 Итого:                           $280-525
```

### **💡 Способы оптимизации:**
- 🕐 **Scheduled scaling** - уменьшение ресурсов в нерабочее время
- 💾 **Storage optimization** - архивирование старых данных
- 📊 **Query optimization** - тюнинг медленных запросов
- 🔄 **Caching strategy** - использование Redis для часто запрашиваемых данных

---

## 🎉 **РЕЗУЛЬТАТ РАЗВЕРТЫВАНИЯ**

После успешного развертывания в SourceCraft у вас будет:

### ✅ **Готовая production система:**
- 🌐 **Высокодоступный веб-сервис** с автоматическим масштабированием
- 🗄️ **Управляемые базы данных** с автоматическими бэкапами
- 🛩️ **Airflow кластер** для оркестрации пайплайнов
- 📊 **Мониторинг в реальном времени** с алертами
- 🔐 **Enterprise безопасность** из коробки

### 🚀 **Готовность к демонстрации:**
- 📱 **Публичный URL** для показа жюри
- 📊 **Дашборды** с метриками производительности
- 🎬 **Живая демонстрация** всех возможностей
- 📈 **Масштабирование** под нагрузку во время демо

### 🏆 **Конкурентные преимущества:**
- ⚡ **Мгновенная готовность** к production
- 🌍 **Глобальная доступность** через CDN
- 💰 **Прозрачная стоимость** владения
- 🔧 **Простота поддержки** и развития

---

**🚀 С SourceCraft.dev ваш ETL Assistant готов покорить жюри и выиграть хакатон!**

*Удачи на демонстрации! 🎯*

