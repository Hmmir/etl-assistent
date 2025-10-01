"""
FastAPI Main Application для ETL Assistant
"""

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import logging

# Импорты из нашей структуры
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import settings
from core.exceptions import ETLException, FileNotFoundException
from core.logging_config import setup_logging, log_request, log_user_action, log_error
from services.data_service import DataAnalyzer
from services.generator_service import PythonETLGenerator
from services.airflow_service import AirflowDAGGenerator
from services.infrastructure_service import InfrastructureManager
from services.streaming_service import KafkaStreamingGenerator
from ml.storage_selector import StorageSelector
from ml.data_profiler import DataProfiler

# Настройка детального логирования
setup_logging(settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

# Создание FastAPI приложения
app = FastAPI(
    title="ETL Assistant API",
    description="AI-Powered ETL Pipeline Generator",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Инициализация сервисов
data_analyzer = DataAnalyzer()
etl_generator = PythonETLGenerator()
airflow_generator = AirflowDAGGenerator()
kafka_generator = KafkaStreamingGenerator()
storage_selector = StorageSelector()
data_profiler = DataProfiler()
infrastructure_manager = InfrastructureManager()

# Создание директории для загрузок
UPLOAD_DIR = settings.UPLOAD_DIR
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "message": "ETL Assistant API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs"
    }


@app.get("/health")
async def health_check():
    """Проверка здоровья API"""
    return {
        "status": "healthy",
        "version": "1.0.0"
    }


@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    source_description: str = ""
):
    """
    Загрузка файла данных для анализа
    """
    log_user_action("UPLOAD_FILE", {"filename": file.filename, "description": source_description})
    try:
        # Проверка типа файла
        allowed_extensions = {'.csv', '.json', '.xml'}
        file_extension = Path(file.filename).suffix.lower()
        
        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"Неподдерживаемый формат файла. Разрешены: {', '.join(allowed_extensions)}"
            )
        
        # Сохранение файла
        file_path = UPLOAD_DIR / file.filename
        
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Определение формата файла
        format_type = file_extension[1:].upper()
        
        # Базовая информация о файле
        file_info = {
            "filename": file.filename,
            "file_size": len(content),
            "file_type": file_extension,
            "format_type": format_type,
            "source_description": source_description,
            "upload_path": str(file_path),
            "status": "uploaded"
        }
        
        logger.info(f"Файл {file.filename} ({format_type}) загружен успешно ({len(content)} байт)")
        
        return file_info
        
    except Exception as e:
        logger.error(f"Ошибка загрузки файла: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки файла: {str(e)}")


@app.post("/generate_pipeline")
async def generate_pipeline(filename: str):
    """
    Генерация ETL пайплайна на основе анализа данных
    """
    log_user_action("GENERATE_PIPELINE", {"filename": filename})
    try:
        file_path = UPLOAD_DIR / filename
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Файл не найден")
        
        logger.info(f"Начинаем анализ файла {filename}")
        
        # Шаг 1: Анализ структуры данных
        data_analysis = data_analyzer.analyze_file(str(file_path))
        logger.info(f"Анализ данных завершен: {data_analysis.get('total_rows', 'неизвестно')} строк")
        
        # Шаг 2: Рекомендация хранилища
        # Подготовка данных для StorageSelector
        row_count = data_analysis.get('total_rows', data_analysis.get('total_records', 0))
        file_size_mb = data_analysis.get('file_size_mb', 0)
        
        # Создаем упрощенный профиль
        profile = {
            'row_count': row_count,
            'column_count': data_analysis.get('total_columns', data_analysis.get('total_fields', 0)),
            'data_types': {},
            'estimated_volume': data_analysis.get('estimated_data_volume', 'medium')
        }
        
        # Создаем sample данных
        df_sample = {
            'preview': data_analysis.get('data_preview', [])[:5],
            'columns': data_analysis.get('columns', data_analysis.get('fields', []))
        }
        
        try:
            storage_recommendation = storage_selector.recommend_storage(
                df_sample=df_sample,
                profile=profile,
                row_count=row_count,
                file_size_mb=file_size_mb
            )
        except Exception as e:
            # Fallback к простой rule-based рекомендации
            logger.warning(f"Ошибка LLM рекомендации: {e}, используем rule-based")
            storage_recommendation = _simple_storage_recommendation(data_analysis)
        
        # Нормализация ключей для совместимости
        recommended_storage = storage_recommendation.get('storage') or storage_recommendation.get('recommended_storage', 'PostgreSQL')
        reason = storage_recommendation.get('reason') or storage_recommendation.get('reasoning', 'Автоматическая рекомендация')
        optimization_tips = storage_recommendation.get('optimization_tips') or storage_recommendation.get('optimization_suggestions', [])
        
        logger.info(f"Рекомендовано хранилище: {recommended_storage}")
        
        # Шаг 3: Генерация DDL
        ddl = storage_recommendation.get('ddl_script') or etl_generator.generate_ddl(
            data_analysis, 
            recommended_storage
        )
        
        # Шаг 4: Генерация ETL кода
        etl_code = etl_generator.generate_etl_script(data_analysis, storage_recommendation)
        
        # Доп. метрики для UI
        confidence = storage_recommendation.get('confidence', 0.8)
        schedule = airflow_generator._determine_schedule(data_analysis)
        # Формирование итогового ответа с устойчивыми ключами для фронтенда
        pipeline_result = {
            # Рекомендации по хранилищу (все ключи-синонимы, чтобы UI точно не был пуст)
            "storage": recommended_storage,
            "recommended_storage": recommended_storage,
            "storage_recommendation": recommended_storage,
            "reason": reason,
            "ai_reasoning": reason,
            "ai_reasoning_details": reason,
            # Сгенерированные артефакты
            "ddl": ddl,
            "etl_code": etl_code,
            "optimization_suggestions": optimization_tips,
            # Краткий анализ для UI (+ ключи, которые ожидает фронтенд)
            "data_analysis": {
                "total_rows": data_analysis.get('total_rows', data_analysis.get('total_records', 0)),
                "total_columns": data_analysis.get('total_columns', data_analysis.get('total_fields', 0)),
                "file_size_mb": data_analysis.get('file_size_mb', 0),
                "format_type": data_analysis.get('format_type'),
                "estimated_data_volume": data_analysis.get('estimated_data_volume'),
                # дополнительно для UI
                "data_type": data_analysis.get('format_type') or "Unknown",
                "recommended_storage": recommended_storage,
                "reasoning": reason,
                "confidence": confidence,
                "update_schedule": schedule
            },
            "data_type": data_analysis.get('format_type') or "Unknown",
            "pipeline_generated": True
        }
        
        logger.info(f"Пайплайн успешно сгенерирован для файла {filename}")
        
        return pipeline_result
        
    except Exception as e:
        logger.error(f"Ошибка генерации пайплайна: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка генерации пайплайна: {str(e)}")


@app.post("/generate_airflow_dag")
async def generate_airflow_dag(filename: str):
    """
    Генерация Airflow DAG для ETL процесса
    """
    try:
        file_path = UPLOAD_DIR / filename
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Файл не найден")
        
        # Анализ данных
        data_analysis = data_analyzer.analyze_file(str(file_path))
        storage_recommendation = storage_selector.recommend_storage(data_analysis)
        
        # Генерация DAG
        dag_code = airflow_generator.generate_dag(
            data_analysis,
            storage_recommendation
        )
        
        return {
            "dag_code": dag_code,
            "dag_id": airflow_generator._generate_dag_id(filename),
            "schedule": airflow_generator._determine_schedule(data_analysis),
            "target_storage": storage_recommendation['storage']
        }
        
    except Exception as e:
        logger.error(f"Ошибка генерации Airflow DAG: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analyze/{filename}")
async def analyze_file(filename: str):
    """
    Анализ файла без генерации пайплайна
    """
    try:
        file_path = UPLOAD_DIR / filename
        
        if not file_path.exists():
            raise FileNotFoundException(filename)
        
        analysis = data_analyzer.analyze_file(str(file_path))
        
        return {
            "filename": filename,
            "analysis": analysis,
            "status": "completed"
        }
        
    except FileNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Ошибка анализа файла: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/execute_pipeline/{filename}")
async def execute_pipeline(filename: str, method: str = "airflow"):
    """
    Выполнение ETL пайплайна
    
    Args:
        filename: Имя файла для обработки
        method: Метод выполнения ('airflow' или 'direct')
    """
    log_user_action("EXECUTE_PIPELINE", {"filename": filename, "method": method})
    
    try:
        file_path = UPLOAD_DIR / filename
        
        if not file_path.exists():
            raise FileNotFoundException(filename)
        
        logger.info(f"Запуск выполнения пайплайна для {filename} методом {method}")
        
        if method == "airflow":
            # Выполнение через Airflow DAG
            return await _execute_via_airflow(filename, file_path)
        else:
            # Прямое выполнение ETL скрипта
            return await _execute_direct(filename, file_path)
            
    except FileNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Ошибка выполнения пайплайна: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка выполнения: {str(e)}")


async def _execute_via_airflow(filename: str, file_path: Path) -> dict:
    """Выполнение пайплайна через Airflow"""
    import requests
    import time
    from pathlib import Path as PathLib
    
    try:
        # 1. Анализ данных
        logger.info("Шаг 1: Анализ данных")
        data_analysis = data_analyzer.analyze_file(str(file_path))
        
        # 2. Получение рекомендаций
        logger.info("Шаг 2: Получение рекомендаций по хранилищу")
        row_count = data_analysis.get('total_rows', data_analysis.get('total_records', 0))
        file_size_mb = data_analysis.get('file_size_mb', 0)
        
        # Создаем profile и df_sample
        profile = {
            'null_percentage': sum(col.get('null_count', 0) for col in data_analysis.get('columns', [])) / max(row_count, 1) * 100,
            'numeric_columns': sum(1 for col in data_analysis.get('columns', []) if 'int' in col.get('data_type', '').lower() or 'float' in col.get('data_type', '').lower()),
            'datetime_columns': sum(1 for col in data_analysis.get('columns', []) if 'date' in col.get('data_type', '').lower()),
            'categorical_columns': sum(1 for col in data_analysis.get('columns', []) if col.get('data_type', '') == 'object')
        }
        
        df_sample = {col['name']: [str(val) for val in col.get('sample_values', [])[:5]] 
                     for col in data_analysis.get('columns', [])}
        
        storage_recommendation = storage_selector.recommend_storage(
            df_sample, profile, row_count, file_size_mb
        )
        
        # 3. Генерация DAG
        logger.info("Шаг 3: Генерация Airflow DAG")
        dag_code = airflow_generator.generate_dag(
            data_analysis,
            storage_recommendation,
            etl_steps=["extract", "transform", "load"]
        )
        
        # 4. Сохранение DAG в папку Airflow
        dag_id = airflow_generator._generate_dag_id(filename)
        dag_filename = f"{dag_id}.py"
        
        # Папка airflow/dags в корне проекта (смонтирована в контейнер Airflow)
        project_root = PathLib(__file__).resolve().parents[2]
        dags_folder = project_root / "airflow" / "dags"
        dags_folder.mkdir(parents=True, exist_ok=True)
        
        dag_path = dags_folder / dag_filename
        
        with open(dag_path, 'w', encoding='utf-8') as f:
            f.write(dag_code)
        
        logger.info(f"DAG сохранен: {dag_path}")
        
        # 5. Триггер DAG через Airflow REST API
        logger.info("Шаг 4: Триггер DAG в Airflow")
        # Используем порт 8080, как в демо-инфраструктуре
        airflow_url = "http://localhost:8080"
        
        # Быстрая проверка Airflow (максимум 10 секунд)
        auth = ("admin", "admin")  # Дефолтные креды Airflow
        dag_registered = False
        for _ in range(2):  # Только 2 попытки по 5 сек
            try:
                check = requests.get(f"{airflow_url}/api/v1/dags/{dag_id}", auth=auth, timeout=5)
                if check.status_code == 200:
                    dag_registered = True
                    break
            except requests.exceptions.RequestException:
                pass
            time.sleep(5)
        
        # Триггерим DAG
        trigger_url = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        headers = {
            "Content-Type": "application/json",
        }
        # auth объявлен выше
        
        trigger_data = {
            "conf": {
                "filename": filename,
                "source_file": str(file_path)
            }
        }
        
        try:
            response = requests.post(
                trigger_url, 
                json=trigger_data,
                headers=headers,
                auth=auth,
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                dag_run = response.json()
                dag_run_id = dag_run.get('dag_run_id', 'unknown')
                
                logger.info(f"DAG триггернут успешно: {dag_run_id}")
                
                return {
                    "status": "success",
                    "message": f"✅ Пайплайн запущен в Airflow (API): {dag_run_id}",
                    "execution_method": "airflow_api",
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "dag_url": f"{airflow_url}/dags/{dag_id}/grid"
                }
            else:
                # Попытка fallback-триггера через docker exec, если авторизация/доступ не прошли
                logger.warning(f"Не удалось триггернуть DAG через API: {response.status_code}. Пробуем docker exec...")
                try:
                    import subprocess, json
                    trigger_conf = json.dumps({
                        "filename": filename,
                        "source_file": str(file_path)
                    })
                    # Определяем контейнер Airflow автоматически
                    ps = subprocess.run(['docker', 'ps', '--format', '{{.Names}}'], capture_output=True, text=True, encoding='utf-8')
                    container_candidates = [
                        name for name in ps.stdout.splitlines()
                        if 'airflow' in name.lower() and ('webserver' in name.lower() or 'scheduler' in name.lower())
                    ]
                    # Добавим наш дефолт как fallback в конец
                    if 'etl_airflow_webserver' not in container_candidates:
                        container_candidates.append('etl_airflow_webserver')

                    for container in container_candidates:
                        exec_result = subprocess.run(
                            [
                                'docker', 'exec', container,
                                'airflow', 'dags', 'trigger', dag_id,
                                '--conf', trigger_conf
                            ],
                            capture_output=True, text=True, encoding='utf-8'
                        )
                        if exec_result.returncode == 0:
                            logger.info(f"DAG успешно триггернут через docker exec ({container})")
                            return {
                                "status": "started",
                                "message": "✅ Пайплайн запущен в Airflow (docker exec)",
                                "execution_method": "airflow",
                                "dag_id": dag_id,
                                "dag_file": str(dag_path),
                                "airflow_ui": f"{airflow_url}/dags/{dag_id}/grid"
                            }
                        else:
                            logger.warning(f"Не удалось выполнить docker exec в контейнере {container}: {exec_result.stderr}")
                except Exception as e:
                    logger.warning(f"Fallback docker exec не удался: {e}")

                # Если и docker exec не помог — возвращаем понятное сообщение
                return {
                    "status": "success",
                    "message": f"✅ DAG создан: {dag_id}. Откройте Airflow UI для запуска.",
                    "execution_method": "manual",
                    "dag_id": dag_id,
                    "dag_url": f"{airflow_url}/dags/{dag_id}/grid"
                }
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Airflow API недоступен: {str(e)}")
            
            return {
                "status": "dag_created",
                "message": "⚠️ DAG создан, но Airflow API недоступен. Запустите вручную.",
                "execution_method": "airflow",
                "dag_id": dag_id,
                "dag_file": str(dag_path),
                "airflow_ui": f"{airflow_url}/dags/{dag_id}/grid",
                "manual_steps": [
                    f"1. Убедитесь что Airflow запущен: {airflow_url}",
                    f"2. DAG автоматически появится в списке через 30-60 секунд",
                    f"3. Найдите DAG '{dag_id}' и нажмите 'Trigger DAG'",
                    "4. Отслеживайте выполнение в Grid View"
                ]
            }
            
    except Exception as e:
        logger.error(f"Ошибка выполнения через Airflow: {str(e)}")
        raise


async def _execute_direct(filename: str, file_path: Path) -> dict:
    """Прямое выполнение ETL скрипта"""
    import subprocess
    import tempfile
    
    try:
        # 1. Анализ данных
        logger.info("Прямое выполнение: анализ данных")
        data_analysis = data_analyzer.analyze_file(str(file_path))
        
        # 2. Генерация ETL скрипта
        logger.info("Генерация ETL скрипта")
        row_count = data_analysis.get('total_rows', data_analysis.get('total_records', 0))
        file_size_mb = data_analysis.get('file_size_mb', 0)
        
        profile = {
            'null_percentage': 0,
            'numeric_columns': 0,
            'datetime_columns': 0,
        }
        df_sample = {}
        
        storage_recommendation = storage_selector.recommend_storage(
            df_sample, profile, row_count, file_size_mb
        )
        
        etl_code = etl_generator.generate_etl_script(
            data_analysis,
            storage_recommendation.get('storage', storage_recommendation.get('recommended_storage', 'PostgreSQL'))
        )
        
        # 3. Сохранение и запуск скрипта
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
            f.write(etl_code)
            script_path = f.name
        
        logger.info(f"ETL скрипт сохранен: {script_path}")
        
        # Запуск в фоновом режиме
        process = subprocess.Popen(
            ['python', script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        return {
            "status": "started",
            "message": "✅ ETL процесс запущен напрямую",
            "execution_method": "direct",
            "script_path": script_path,
            "process_id": process.pid,
            "note": "Процесс выполняется в фоновом режиме. Проверьте логи для отслеживания прогресса."
        }
        
    except Exception as e:
        logger.error(f"Ошибка прямого выполнения: {str(e)}")
        raise


@app.get("/pipeline_status/{dag_id}")
async def get_pipeline_status(dag_id: str):
    """
    Проверка статуса выполнения пайплайна в Airflow
    """
    import requests
    
    try:
        airflow_url = "http://localhost:8080"
        status_url = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        auth = ("admin", "admin")
        
        response = requests.get(status_url, auth=auth, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            dag_runs = data.get('dag_runs', [])
            
            if dag_runs:
                latest_run = dag_runs[0]
                return {
                    "dag_id": dag_id,
                    "status": latest_run.get('state'),
                    "start_date": latest_run.get('start_date'),
                    "end_date": latest_run.get('end_date'),
                    "execution_date": latest_run.get('execution_date'),
                    "airflow_ui": f"{airflow_url}/dags/{dag_id}/grid"
                }
            else:
                return {
                    "dag_id": dag_id,
                    "status": "not_started",
                    "message": "DAG еще не запускался"
                }
        else:
            raise HTTPException(status_code=503, detail="Airflow API недоступен")
            
    except requests.exceptions.RequestException:
        raise HTTPException(status_code=503, detail="Не удалось подключиться к Airflow")


@app.get("/infrastructure/status")
async def infrastructure_status():
    """
    Статус инфраструктуры (Docker Compose: ClickHouse, PostgreSQL, Redis, Airflow)
    """
    try:
        status = infrastructure_manager.get_infrastructure_status()
        return status
    except Exception as e:
        logger.error(f"Ошибка получения статуса инфраструктуры: {str(e)}")
        raise HTTPException(status_code=500, detail="Не удалось получить статус инфраструктуры")


@app.post("/infrastructure/deploy")
async def infrastructure_deploy():
    """
    Развертывание демо-инфраструктуры (Docker Compose: ClickHouse, PostgreSQL, Redis, Airflow)
    """
    try:
        result = infrastructure_manager.deploy_demo_infrastructure()
        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error", "Ошибка развёртывания"))
        return result
    except Exception as e:
        logger.error(f"Ошибка развёртывания инфраструктуры: {str(e)}")
        raise HTTPException(status_code=500, detail="Не удалось развернуть инфраструктуру")


@app.post("/generate_kafka_streaming")
async def generate_kafka_streaming(filename: str, topic_name: str = None):
    """
    Генерация Kafka Producer и Consumer для стриминга данных
    
    Args:
        filename: Имя загруженного файла
        topic_name: Имя Kafka топика (опционально, генерируется автоматически)
    """
    try:
        file_path = UPLOAD_DIR / filename
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Файл не найден")
        
        logger.info(f"Генерация Kafka streaming для {filename}")
        
        # Анализ данных
        data_analysis = data_analyzer.analyze_file(str(file_path))
        
        # Рекомендация хранилища для consumer
        row_count = data_analysis.get('total_rows', 0)
        file_size_mb = data_analysis.get('file_size_mb', 0)
        
        profile = {
            'row_count': row_count,
            'column_count': data_analysis.get('total_columns', 0)
        }
        df_sample = {}
        
        storage_recommendation = storage_selector.recommend_storage(
            df_sample, profile, row_count, file_size_mb
        )
        
        target_storage = storage_recommendation.get('storage', 'PostgreSQL')
        
        # Генерация topic name если не указан
        if not topic_name:
            topic_name = f"etl_{Path(filename).stem.replace('-', '_').replace('.', '_')}"
        
        # Генерация Kafka Producer
        producer_code = kafka_generator.generate_kafka_producer(
            topic_name=topic_name,
            source_config={'file_path': str(file_path), 'filename': filename},
            analysis=data_analysis
        )
        
        # Генерация Kafka Consumer
        consumer_code = kafka_generator.generate_kafka_consumer(
            topic_name=topic_name,
            target_storage=target_storage,
            analysis=data_analysis
        )
        
        # Генерация Docker Compose для Kafka
        docker_compose = kafka_generator.generate_docker_compose(
            topic_name=topic_name,
            analysis=data_analysis
        )
        
        logger.info(f"✅ Kafka streaming сгенерирован: топик={topic_name}, storage={target_storage}")
        
        return {
            "topic_name": topic_name,
            "target_storage": target_storage,
            "producer_code": producer_code,
            "consumer_code": consumer_code,
            "docker_compose": docker_compose,
            "kafka_bootstrap_servers": "localhost:9092",
            "message": f"✅ Kafka Producer и Consumer готовы для топика {topic_name}"
        }
        
    except Exception as e:
        logger.error(f"Ошибка генерации Kafka streaming: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка генерации: {str(e)}")


def _simple_storage_recommendation(analysis: dict) -> dict:
    """Простая rule-based рекомендация хранилища как fallback"""
    row_count = analysis.get('total_rows', analysis.get('total_records', 0))
    file_size_mb = analysis.get('file_size_mb', 0)
    data_volume = analysis.get('estimated_data_volume', 'medium')
    
    if data_volume == 'large' or row_count > 1000000 or file_size_mb > 100:
        storage = 'ClickHouse'
        reason = 'Большой объем данных - оптимально для аналитики'
    elif data_volume == 'small' or row_count < 10000:
        storage = 'PostgreSQL'
        reason = 'Малый объем данных - универсальное реляционное хранилище'
    else:
        storage = 'PostgreSQL'
        reason = 'Средний объем данных - стандартное реляционное хранилище'
    
    # Генерация DDL
    ddl = etl_generator.generate_ddl(analysis, storage)
    
    return {
        'storage': storage,
        'recommended_storage': storage,
        'reason': reason,
        'reasoning': reason,
        'confidence': 0.8,
        'rule_based_storage': storage,
        'llm_storage': None,
        'agreement': True,
        'optimization_tips': [
            'Добавьте индексы на часто используемые колонки',
            'Рассмотрите партицирование для больших таблиц'
        ],
        'optimization_suggestions': [
            'Добавьте индексы на часто используемые колонки',
            'Рассмотрите партицирование для больших таблиц'
        ],
        'ddl_script': ddl
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.API_RELOAD
    )
