# 🚀 ETL Assistant - AI-Powered ETL Pipeline Generator

**ETL Assistant** - интеллектуальная система для автоматической генерации ETL пайплайнов на основе анализа данных с использованием AI.

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-18+-blue.svg)](https://reactjs.org/)
[![Tests](https://img.shields.io/badge/tests-50%25-yellow.svg)](./backend/tests/)

---

## 📋 Возможности

### 🔍 Автоматический анализ данных
- ✅ Поддержка CSV, JSON, XML форматов
- ✅ Автоопределение кодировки и разделителей
- ✅ Анализ структуры и типов данных
- ✅ Профилирование данных с статистикой

### 🏗️ Интеллектуальная генерация
- ✅ Рекомендация оптимального хранилища (PostgreSQL, ClickHouse, HDFS)
- ✅ Генерация DDL для 5+ типов баз данных
- ✅ Создание Python ETL скриптов
- ✅ Генерация Airflow DAG
- ✅ Kafka producer/consumer для стриминга

### 🤖 AI-интеграция
- ✅ Использование LLM для генерации кода
- ✅ Интеллектуальные рекомендации
- ✅ Оптимизация запросов

---

## 🛠️ Технологический стек

### Backend
- **FastAPI** - современный веб-фреймворк
- **Pandas** - обработка данных
- **OpenRouter API** - AI генерация кода
- **pytest** - тестирование

### Frontend
- **React** - UI библиотека
- **Axios** - HTTP клиент
- **Material-UI** - компоненты

### Поддерживаемые хранилища
- PostgreSQL
- ClickHouse
- MySQL
- SQLite
- HDFS/Hive
- Kafka (streaming)

---

## 🚀 Быстрый старт

### Требования
- Python 3.10+
- Node.js 16+
- Docker (опционально)

### Установка

#### 1. Клонирование репозитория
```bash
git clone https://github.com/Hmmir/etl-assistent.git
cd etl-assistent
```

#### 2. Backend

```bash
cd backend

# Создать виртуальное окружение
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Установить зависимости
pip install -r requirements.txt

# Настроить переменные окружения
cp .env.example .env
# Отредактировать .env и добавить OPENROUTER_API_KEY

# Запустить сервер
python -m uvicorn main:app --reload
```

Backend будет доступен на `http://localhost:8000`

API документация: `http://localhost:8000/docs`

#### 3. Frontend

```bash
cd frontend

# Установить зависимости
npm install

# Запустить dev сервер
npm start
```

Frontend будет доступен на `http://localhost:3000`

---

## 📖 Использование

### 1. Загрузка файла данных

```bash
curl -X POST "http://localhost:8000/upload" \
  -F "file=@data.csv" \
  -F "source_description=Sales data"
```

### 2. Генерация пайплайна

```bash
curl -X POST "http://localhost:8000/generate_pipeline?filename=data.csv"
```

Ответ:
```json
{
  "storage_recommendation": "PostgreSQL",
  "ddl": "CREATE TABLE data (...)",
  "etl_code": "#!/usr/bin/env python3...",
  "etl_steps": [...],
  "optimization_suggestions": [...]
}
```

### 3. Генерация Airflow DAG

```bash
curl -X POST "http://localhost:8000/generate_airflow_dag?filename=data.csv"
```

---

## 🏗️ Архитектура

```
etl-assistant-clean/
├── backend/                    # Backend приложение
│   ├── api/                   # API endpoints
│   │   ├── routes/           # HTTP routes
│   │   │   ├── upload.py
│   │   │   ├── pipeline.py
│   │   │   └── code.py
│   │   └── main.py           # FastAPI app
│   ├── services/             # Business logic
│   │   ├── data_service.py
│   │   ├── generator_service.py
│   │   └── streaming_service.py
│   ├── ml/                   # ML/AI компоненты
│   │   ├── code_generator.py
│   │   ├── data_profiler.py
│   │   └── storage_selector.py
│   ├── core/                 # Core utilities
│   │   ├── config.py
│   │   └── exceptions.py
│   └── tests/                # Тесты
│
├── frontend/                  # React приложение
│   ├── src/
│   │   ├── components/      # React компоненты
│   │   └── App.js
│   └── public/
│
└── docs/                     # Документация
```

---

## 🧪 Тестирование

### Запуск всех тестов
```bash
cd backend
pytest tests/ -v
```

### Запуск с покрытием
```bash
pytest tests/ --cov=. --cov-report=html
```

### Текущее покрытие
- **DataAnalyzer**: 90% ✅
- **ETLGenerator**: 59% ⚠️
- **AirflowGenerator**: 93% ✅
- **API Endpoints**: 60% ⚠️

**Общее покрытие**: 50% (71/142 теста)

---

## 📊 Примеры использования

### Анализ CSV файла

```python
from backend.services.data_service import DataService

service = DataService()
analysis = service.analyze_file("data.csv")

print(f"Rows: {analysis['total_rows']}")
print(f"Columns: {analysis['total_columns']}")
print(f"Format: {analysis['format_type']}")
```

### Генерация DDL

```python
from backend.services.generator_service import GeneratorService

service = GeneratorService()
ddl = service.generate_ddl(
    analysis=analysis,
    storage_type="PostgreSQL",
    table_name="my_table"
)
print(ddl)
```

---

## 🤝 Вклад в проект

Мы приветствуем вклад в проект! Пожалуйста:

1. Форкните репозиторий
2. Создайте ветку для фичи (`git checkout -b feature/AmazingFeature`)
3. Закоммитьте изменения (`git commit -m 'Add AmazingFeature'`)
4. Запушьте в ветку (`git push origin feature/AmazingFeature`)
5. Откройте Pull Request

---

## 📝 Лицензия

Этот проект распространяется под лицензией MIT. См. файл `LICENSE` для деталей.

---

## 🔗 Полезные ссылки

- [Документация API](./docs/API.md)
- [Архитектура](./docs/ARCHITECTURE.md)
- [Руководство разработчика](./docs/DEVELOPMENT.md)
- [Changelog](./CHANGELOG.md)

---

## 📧 Контакты

- **GitHub**: [@Hmmir](https://github.com/Hmmir)
- **Email**: your.email@example.com

---

## 🙏 Благодарности

- [FastAPI](https://fastapi.tiangolo.com/) - за отличный фреймворк
- [OpenRouter](https://openrouter.ai/) - за доступ к LLM
- [Apache Airflow](https://airflow.apache.org/) - за вдохновение

---

**⭐ Поставьте звезду, если проект вам понравился!**
