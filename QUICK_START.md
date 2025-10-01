# 🚀 Быстрый старт ETL Assistant

## Предварительные требования

- Python 3.10+
- Node.js 16+
- Git

## 1️⃣ Клонирование репозитория

```bash
git clone https://github.com/Hmmir/etl-assistent.git
cd etl-assistent
```

## 2️⃣ Настройка Backend

### Windows PowerShell:

```powershell
cd backend

# Создать виртуальное окружение
python -m venv venv
.\venv\Scripts\activate

# Установить зависимости
pip install -r requirements.txt

# Создать .env файл
Copy-Item .env.example .env

# Отредактировать .env и добавить OPENROUTER_API_KEY
notepad .env
```

### Linux/Mac:

```bash
cd backend

# Создать виртуальное окружение
python3 -m venv venv
source venv/bin/activate

# Установить зависимости
pip install -r requirements.txt

# Создать .env файл
cp .env.example .env

# Отредактировать .env
nano .env
```

## 3️⃣ Запуск Backend

```bash
cd backend

# Windows
$env:OPENROUTER_API_KEY = "your-key-here"
python -m uvicorn api.main:app --reload

# Linux/Mac
export OPENROUTER_API_KEY="your-key-here"
python -m uvicorn api.main:app --reload
```

Backend запустится на `http://localhost:8000`

API Docs: `http://localhost:8000/docs`

## 4️⃣ Настройка Frontend

```bash
cd frontend

# Установить зависимости
npm install

# Запустить dev сервер
npm start
```

Frontend запустится на `http://localhost:3000`

## 5️⃣ Тестирование

```bash
cd backend

# Запустить все тесты
pytest tests/ -v

# С покрытием кода
pytest tests/ --cov=. --cov-report=html

# Открыть отчет
start htmlcov/index.html  # Windows
open htmlcov/index.html   # Mac
```

## 6️⃣ Docker (опционально)

### Запуск всего стека:

```bash
# Запустить все сервисы
docker-compose up -d

# Проверить статус
docker-compose ps

# Просмотр логов
docker-compose logs -f

# Остановка
docker-compose down
```

Сервисы:
- Backend API: `http://localhost:8000`
- Frontend: `http://localhost:3000`
- PostgreSQL: `localhost:5432`
- ClickHouse: `localhost:9000`
- Kafka: `localhost:9092`

## 7️⃣ Использование API

### Загрузка файла:

```bash
curl -X POST "http://localhost:8000/upload" \
  -F "file=@data.csv" \
  -F "source_description=Test data"
```

### Генерация пайплайна:

```bash
curl -X POST "http://localhost:8000/generate_pipeline?filename=data.csv"
```

### Генерация Airflow DAG:

```bash
curl -X POST "http://localhost:8000/generate_airflow_dag?filename=data.csv"
```

## 🎯 Первый запуск (полная инструкция)

### 1. Установка всех зависимостей

```powershell
# Backend
cd backend
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt

# Frontend (в новом терминале)
cd frontend
npm install
```

### 2. Настройка переменных окружения

Создайте файл `backend\.env`:

```env
OPENROUTER_API_KEY=sk-or-v1-your-key-here
API_HOST=0.0.0.0
API_PORT=8000
```

### 3. Запуск в разных терминалах

**Терминал 1 - Backend:**
```powershell
cd backend
.\venv\Scripts\activate
$env:OPENROUTER_API_KEY = "sk-or-v1-your-key-here"
python -m uvicorn api.main:app --reload
```

**Терминал 2 - Frontend:**
```powershell
cd frontend
npm start
```

### 4. Тестирование

Откройте браузер:
- Frontend: http://localhost:3000
- API Docs: http://localhost:8000/docs

## 📝 Примеры тестовых данных

В папке `backend/uploads/` создайте файл `test.csv`:

```csv
id,name,email,age,salary
1,John Doe,john@example.com,30,50000
2,Jane Smith,jane@example.com,25,60000
3,Bob Johnson,bob@example.com,35,70000
```

Загрузите через API:

```bash
curl -X POST "http://localhost:8000/upload" \
  -F "file=@backend/uploads/test.csv"
```

Сгенерируйте пайплайн:

```bash
curl -X POST "http://localhost:8000/generate_pipeline?filename=test.csv"
```

## ❓ Troubleshooting

### Ошибка: "OpenRouter API key not found"

```powershell
$env:OPENROUTER_API_KEY = "your-key-here"
```

### Ошибка: "Module not found"

```bash
pip install -r requirements.txt
```

### Ошибка: "Port already in use"

Измените порт в команде запуска:

```bash
uvicorn api.main:app --reload --port 8001
```

### Тесты не проходят

```bash
# Убедитесь что API key установлен
$env:OPENROUTER_API_KEY = "your-key"

# Запустите тесты
pytest tests/ -v
```

## 🎉 Готово!

Теперь вы можете:
- Загружать файлы данных
- Генерировать ETL пайплайны
- Создавать Airflow DAG
- Получать рекомендации по хранилищам

Для подробностей см. [README.md](README.md)
