"""
Модуль генерации Airflow DAG для ETL Assistant

Модуль 6: Автоматическая генерация DAG файлов для оркестрации ETL процессов
"""

import os
import textwrap
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path


class AirflowDAGGenerator:
    """
    Генератор Apache Airflow DAG для ETL процессов
    """
    
    def __init__(self):
        self.default_args = {
            'owner': 'etl_assistant',
            'depends_on_past': False,
            'start_date': '2025-01-01',
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': 'timedelta(minutes=5)'
        }
    
    def generate_dag(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any], 
                    etl_steps: List[str]) -> str:
        """
        Генерация Airflow DAG на основе анализа данных и ETL шагов
        
        Args:
            analysis: Результат анализа данных
            storage_recommendation: Рекомендации по хранилищу
            etl_steps: Шаги ETL процесса
            
        Returns:
            str: Готовый Python код DAG файла
        """
        filename = analysis.get('filename', 'data.csv')
        storage = storage_recommendation['recommended_storage']
        total_rows = analysis.get('total_rows', analysis.get('total_records', 0))
        dag_id = self._generate_dag_id(filename)
        
        # Определяем расписание на основе объема данных
        schedule = self._determine_schedule(analysis)
        
        # Генерируем компоненты DAG
        imports = self._generate_imports(storage)
        dag_definition = self._generate_dag_definition(dag_id, schedule, analysis)
        task_definitions = self._generate_task_definitions(analysis, storage_recommendation, etl_steps)
        task_dependencies = self._generate_task_dependencies(etl_steps)
        
        # Собираем полный DAG
        dag_code = f'''#!/usr/bin/env python3
"""
Airflow DAG для ETL обработки {filename}

Автоматически сгенерирован ETL Assistant
Дата создания: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Источник: {analysis.get('format_type')} файл ({analysis.get('file_size_mb', 0)} MB)
Целевое хранилище: {storage}
Строк данных: {total_rows:,}
Расписание: {schedule}
"""

{imports}

# Настройки по умолчанию для всех задач
default_args = {{
    'owner': '{self.default_args["owner"]}',
    'depends_on_past': {self.default_args["depends_on_past"]},
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': {self.default_args["email_on_failure"]},
    'email_on_retry': {self.default_args["email_on_retry"]},
    'retries': {self.default_args["retries"]},
    'retry_delay': {self.default_args["retry_delay"]}
}}

{dag_definition}

{task_definitions}

{task_dependencies}
'''
        
        return dag_code
    
    def _generate_imports(self, storage: str) -> str:
        """Генерация блока импортов"""
        imports = [
            "from datetime import datetime, timedelta",
            "from airflow import DAG",
            "from airflow.operators.python import PythonOperator",
            "from airflow.operators.bash import BashOperator", 
            "from airflow.operators.email import EmailOperator",
            "import pandas as pd",
            "import logging",
            "import os"
        ]
        
        # Добавляем специфичные для хранилища импорты
        if storage == 'ClickHouse':
            imports.extend([
                "from airflow.providers.http.operators.http import SimpleHttpOperator",
                "from clickhouse_driver import Client"
            ])
        elif storage == 'PostgreSQL':
            imports.extend([
                "from airflow.providers.postgres.operators.postgres import PostgresOperator",
                "from airflow.providers.postgres.hooks.postgres import PostgresHook"
            ])
        elif storage == 'HDFS':
            imports.extend([
                "from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor",
                "import hdfs3"
            ])
        
        return "\n".join(imports)
    
    def _generate_dag_definition(self, dag_id: str, schedule: str, analysis: Dict[str, Any]) -> str:
        """Генерация определения DAG"""
        description = f"ETL процесс для {analysis.get('filename', 'файла данных')}"
        
        return f'''
# Определение DAG
dag = DAG(
    dag_id='{dag_id}',
    description='{description}',
    default_args=default_args,
    schedule_interval='{schedule}',
    catchup=False,
    tags=['etl', 'auto-generated', '{analysis.get("format_type", "data").lower()}'],
    max_active_runs=1
)
'''
    
    def _generate_task_definitions(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any], 
                                  etl_steps: List[str]) -> str:
        """Генерация определений задач"""
        filename = analysis.get('filename', 'data.csv')
        storage = storage_recommendation['recommended_storage']
        
        tasks = []
        
        # ВАЖНО: FileSensor НЕ ИСПОЛЬЗУЕТСЯ, так как файл находится на хост-системе
        # Для production: настройте volume mapping в docker-compose:
        #   volumes:
        #     - C:/Users/alien/Desktop/etl/backend/uploads:/data
        
        # 1. Задача начального старта (заменяет FileSensor)
        tasks.append(f'''
def start_etl_task(**context):
    """
    Инициализация ETL процесса (вместо FileSensor)
    """
    logger = logging.getLogger(__name__)
    logger.info("🚀 Запуск ETL процесса для {filename}")
    
    # Получаем параметры из конфигурации DAG run
    conf = context.get('dag_run').conf or {{}}
    source_file = conf.get('source_file', '/data/{filename}')
    
    logger.info(f"Исходный файл: {{source_file}}")
    
    return {{
        "source_file": source_file,
        "etl_started": True,
        "start_time": str(datetime.now())
    }}

start_etl = PythonOperator(
    task_id='start_etl',
    python_callable=start_etl_task,
    dag=dag
)''')
        
        # 2. Задача анализа данных
        tasks.append(f'''
def analyze_data_task(**context):
    """
    Задача анализа структуры данных
    """
    logger = logging.getLogger(__name__)
    ti = context['ti']
    start_result = ti.xcom_pull(task_ids='start_etl')
    file_path = start_result.get('source_file', '/data/{filename}')
    
    logger.info(f"Анализ данных файла: {{file_path}}")
    
    # Проверяем существование файла
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        logger.info(f"✅ Файл найден, размер: {{file_size:,}} байт")
    else:
        # Если файл не найден - используем API для получения данных
        logger.warning(f"⚠️ Файл {{file_path}} не найден локально, используем API")
        file_size = 0
    
    # Возвращаем результат для следующих задач
    return {{
        "file_path": file_path,
        "file_size": file_size,
        "analysis_completed": True
    }}

analyze_data = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data_task,
    dag=dag
)''')
        
        # 3. Задача извлечения данных (Extract)
        extract_task = self._generate_extract_task(analysis, storage)
        tasks.append(extract_task)
        
        # 4. Задача трансформации данных (Transform)  
        transform_task = self._generate_transform_task(analysis)
        tasks.append(transform_task)
        
        # 5. Задача загрузки данных (Load)
        load_task = self._generate_load_task(analysis, storage_recommendation)
        tasks.append(load_task)
        
        # 6. Задача проверки качества данных
        quality_task = self._generate_quality_check_task(analysis, storage)
        tasks.append(quality_task)
        
        # 7. Задача уведомления об успешном завершении
        tasks.append('''
# Задача уведомления об успешном завершении
success_notification = EmailOperator(
    task_id='success_notification',
    to=['admin@company.com'],
    subject='ETL процесс завершен успешно: {{ ds }}',
    html_content="""
    <h3>ETL процесс успешно завершен</h3>
    <p><strong>Дата:</strong> {{ ds }}</p>
    <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
    <p><strong>Статус:</strong> SUCCESS</p>
    """,
    dag=dag
)''')
        
        return "\n".join(tasks)
    
    def _generate_extract_task(self, analysis: Dict[str, Any], storage: str) -> str:
        """Генерация задачи извлечения данных"""
        format_type = analysis.get('format_type', 'CSV')
        filename = analysis.get('filename', 'data.csv')
        
        if format_type == 'CSV':
            encoding = analysis.get('encoding', 'utf-8')
            separator = analysis.get('separator', ',')
            
            return f'''
def extract_csv_data(**context):
    """
    Извлечение данных из CSV файла
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем извлечение данных из CSV")
    
    file_path = "/data/{filename}"
    
    try:
        # Читаем CSV с определенными параметрами
        df = pd.read_csv(
            file_path,
            sep='{separator}',
            encoding='{encoding}',
            low_memory=False
        )
        
        logger.info(f"Извлечено {{len(df):,}} строк, {{len(df.columns)}} колонок")
        
        # Сохраняем временно для следующей задачи
        temp_path = "/tmp/extracted_data.parquet"
        df.to_parquet(temp_path)
        
        return {{
            "temp_file": temp_path,
            "rows_extracted": len(df),
            "columns_count": len(df.columns)
        }}
        
    except Exception as e:
        logger.error(f"Ошибка извлечения данных: {{str(e)}}")
        raise

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_csv_data,
    dag=dag
)'''
        
        elif format_type == 'JSON':
            return '''
def extract_json_data(**context):
    """
    Извлечение данных из JSON файла
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем извлечение данных из JSON")
    
    file_path = "/data/''' + filename + '''"
    
    try:
        df = pd.read_json(file_path)
        logger.info(f"Извлечено {len(df):,} записей")
        
        # Сохраняем временно
        temp_path = "/tmp/extracted_data.parquet"
        df.to_parquet(temp_path)
        
        return {
            "temp_file": temp_path,
            "rows_extracted": len(df)
        }
        
    except Exception as e:
        logger.error(f"Ошибка извлечения JSON данных: {str(e)}")
        raise

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_json_data,
    dag=dag
)'''
        
        else:  # XML или другие форматы
            return '''
def extract_data_generic(**context):
    """
    Извлечение данных из файла
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем извлечение данных")
    
    # Здесь будет логика для конкретного формата
    # В реальной реализации вызов ETL скрипта
    
    return {"extraction_completed": True}

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_generic,
    dag=dag
)'''
    
    def _generate_transform_task(self, analysis: Dict[str, Any]) -> str:
        """Генерация задачи трансформации данных"""
        columns = analysis.get('columns', analysis.get('fields', []))
        has_nulls = any(col.get('null_count', 0) > 0 for col in columns)
        has_datetime = any(col.get('data_type') == 'datetime' for col in columns)
        
        cleaning_logic = []
        if has_nulls:
            cleaning_logic.append("        # Обработка пропущенных значений")
            cleaning_logic.append("        df = df.dropna(how='all')  # Удаляем полностью пустые строки")
            
        if has_datetime:
            cleaning_logic.append("        # Нормализация дат")
            cleaning_logic.append("        datetime_cols = [col for col in df.columns if 'date' in col.lower()]")
            cleaning_logic.append("        for col in datetime_cols:")
            cleaning_logic.append("            df[col] = pd.to_datetime(df[col], errors='coerce')")
        
        cleaning_code = "\\n".join(cleaning_logic) if cleaning_logic else "        # Данные не требуют специальной очистки"
        
        return f'''
def transform_data_task(**context):
    """
    Трансформация и очистка данных
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем трансформацию данных")
    
    # Получаем путь к временному файлу из предыдущей задачи
    ti = context['ti']
    extract_result = ti.xcom_pull(task_ids='extract_data')
    temp_file = extract_result.get('temp_file')
    
    try:
        # Читаем данные
        df = pd.read_parquet(temp_file)
        original_rows = len(df)
        
{cleaning_code}
        
        # Добавляем служебные поля
        df['etl_processed_at'] = pd.Timestamp.now()
        df['etl_batch_id'] = context['run_id']
        
        # Сохраняем трансформированные данные
        transformed_path = "/tmp/transformed_data.parquet"
        df.to_parquet(transformed_path)
        
        logger.info(f"Трансформация завершена: {{original_rows}} → {{len(df)}} строк")
        
        return {{
            "transformed_file": transformed_path,
            "rows_processed": len(df),
            "transformation_completed": True
        }}
        
    except Exception as e:
        logger.error(f"Ошибка трансформации данных: {{str(e)}}")
        raise

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_task,
    dag=dag
)'''
    
    def _generate_load_task(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any]) -> str:
        """Генерация задачи загрузки данных"""
        storage = storage_recommendation['recommended_storage']
        table_name = self._get_table_name(analysis)
        
        if storage == 'ClickHouse':
            return f'''
def load_to_clickhouse_task(**context):
    """
    Загрузка данных в ClickHouse
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем загрузку данных в ClickHouse")
    
    # Получаем трансформированные данные
    ti = context['ti']
    transform_result = ti.xcom_pull(task_ids='transform_data')
    transformed_file = transform_result.get('transformed_file')
    
    try:
        # Подключение к ClickHouse
        client = Client(
            host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
            port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
            database=os.getenv('CLICKHOUSE_DB', 'default')
        )
        
        # Читаем данные
        df = pd.read_parquet(transformed_file)
        
        # Проверяем существование таблицы
        table_exists = client.execute(
            "SELECT count() FROM system.tables WHERE name = %(table)s",
            {{'table': '{table_name}'}}
        )[0][0] > 0
        
        if not table_exists:
            logger.info("Таблица не существует - создаем")
            # В реальной реализации здесь будет DDL из рекомендаций
            
        # Загружаем данные
        data_tuples = [tuple(row) for row in df.values]
        placeholders = "(" + ",".join(["%s"] * len(df.columns)) + ")"
        
        insert_query = f"INSERT INTO {table_name} VALUES {{placeholders}}"
        client.execute(insert_query, data_tuples)
        
        logger.info(f"Успешно загружено {{len(df):,}} записей в ClickHouse")
        
        return {{
            "rows_loaded": len(df),
            "target_table": "{table_name}",
            "load_completed": True
        }}
        
    except Exception as e:
        logger.error(f"Ошибка загрузки в ClickHouse: {{str(e)}}")
        raise

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_to_clickhouse_task,
    dag=dag
)'''
        
        elif storage == 'PostgreSQL':
            return f'''
def load_to_postgresql_task(**context):
    """
    Загрузка данных в PostgreSQL
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем загрузку данных в PostgreSQL")
    
    # Получаем трансформированные данные
    ti = context['ti']
    transform_result = ti.xcom_pull(task_ids='transform_data')
    transformed_file = transform_result.get('transformed_file')
    
    try:
        # Используем Airflow PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Читаем данные
        df = pd.read_parquet(transformed_file)
        
        # Загружаем в PostgreSQL
        df.to_sql(
            name='{table_name}',
            con=postgres_hook.get_sqlalchemy_engine(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        logger.info(f"Успешно загружено {{len(df):,}} записей в PostgreSQL")
        
        return {{
            "rows_loaded": len(df),
            "target_table": "{table_name}",
            "load_completed": True
        }}
        
    except Exception as e:
        logger.error(f"Ошибка загрузки в PostgreSQL: {{str(e)}}")
        raise

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_to_postgresql_task,
    dag=dag
)'''
        
        else:  # HDFS или другие
            return '''
def load_to_storage_task(**context):
    """
    Загрузка данных в целевое хранилище
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем загрузку данных")
    
    # Получаем трансформированные данные
    ti = context['ti']
    transform_result = ti.xcom_pull(task_ids='transform_data')
    
    # Здесь будет логика загрузки в конкретное хранилище
    logger.info("Загрузка завершена")
    
    return {"load_completed": True}

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_to_storage_task,
    dag=dag
)'''
    
    def _generate_quality_check_task(self, analysis: Dict[str, Any], storage: str) -> str:
        """Генерация задачи проверки качества данных"""
        return '''
def quality_check_task(**context):
    """
    Проверка качества загруженных данных
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем проверку качества данных")
    
    # Получаем результаты загрузки
    ti = context['ti']
    load_result = ti.xcom_pull(task_ids='load_data')
    rows_loaded = load_result.get('rows_loaded', 0)
    
    # Базовые проверки
    if rows_loaded == 0:
        raise ValueError("Не загружено ни одной записи")
    
    # Дополнительные проверки качества можно добавить здесь
    logger.info(f"Проверка качества пройдена: {rows_loaded:,} записей")
    
    return {
        "quality_check_passed": True,
        "rows_validated": rows_loaded
    }

quality_check = PythonOperator(
    task_id='quality_check',
    python_callable=quality_check_task,
    dag=dag
)'''
    
    def _generate_task_dependencies(self, etl_steps: List[str]) -> str:
        """Генерация зависимостей между задачами"""
        # FileSensor УБРАН - используется start_etl вместо него
        return '''
# Определение зависимостей между задачами
# FileSensor заменен на start_etl для корректной работы без file sensor
start_etl >> analyze_data >> extract_data >> transform_data >> load_data >> quality_check >> success_notification
'''
    
    def _generate_dag_id(self, filename: str) -> str:
        """Генерация ID для DAG"""
        # Получаем имя файла и расширение
        file_path = Path(filename)
        name_without_ext = file_path.stem.lower()
        extension = file_path.suffix.lower().replace('.', '')  # .csv -> csv
        
        # Очищаем имя от специальных символов
        dag_id = name_without_ext.replace('-', '_').replace(' ', '_')
        dag_id = ''.join(c for c in dag_id if c.isalnum() or c == '_')
        
        # Добавляем расширение в конце
        if extension:
            dag_id = f"{dag_id}_{extension}"
        
        return dag_id if dag_id else "etl_data_processing"
    
    def _get_table_name(self, analysis: Dict[str, Any]) -> str:
        """Получение имени таблицы"""
        filename = analysis.get('filename', 'data.csv')
        table_name = Path(filename).stem.lower()
        table_name = table_name.replace('-', '_').replace(' ', '_')
        table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
        return table_name or 'etl_data'
    
    def _determine_schedule(self, analysis: Dict[str, Any]) -> str:
        """Определение расписания на основе объема данных"""
        # Проверяем, передан ли уже schedule явно
        if 'schedule' in analysis:
            return analysis['schedule']
        
        data_volume = analysis.get('estimated_data_volume', 'medium')
        file_size_mb = analysis.get('file_size_mb', 0)
        total_rows = analysis.get('total_rows', analysis.get('total_records', 0))
        
        # Логика определения частоты запуска
        if data_volume == 'small' or file_size_mb < 10 or total_rows < 10000:
            return '@daily'  # Ежедневно для небольших данных
        elif data_volume == 'medium' or file_size_mb < 100:
            return '@hourly'   # Каждый час для средних данных (требуют частого обновления)
        elif data_volume == 'large' or file_size_mb < 1000:
            return '0 2 * * *'  # В 2:00 каждый день для больших данных
        else:
            return '0 1 * * 0'  # Еженедельно в воскресенье в 1:00 для очень больших данных


class AirflowConfigGenerator:
    """
    Генератор конфигурационных файлов для Airflow
    """
    
    def generate_airflow_configs(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any]) -> Dict[str, str]:
        """
        Генерация конфигурационных файлов для Airflow
        
        Returns:
            dict: Конфигурационные файлы
        """
        configs = {
            'airflow_cfg': self._generate_airflow_cfg(),
            'docker_compose_airflow': self._generate_airflow_docker_compose(storage_recommendation),
            'connections_script': self._generate_connections_script(storage_recommendation),
            'requirements_airflow': self._generate_airflow_requirements(storage_recommendation)
        }
        
        return configs
    
    def _generate_airflow_cfg(self) -> str:
        """Генерация базовой конфигурации Airflow"""
        return '''[core]
# Основные настройки Airflow для ETL Assistant
dags_folder = /opt/airflow/dags
base_log_folder = /opt/airflow/logs
logging_level = INFO
executor = LocalExecutor
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow
load_examples = False
max_active_runs_per_dag = 1

[webserver]
base_url = http://localhost:8080
web_server_port = 8080
authenticate = True
auth_backend = airflow.contrib.auth.backends.password_auth

[scheduler]
dag_dir_list_interval = 300
catchup_by_default = False
max_threads = 2

[email]
email_backend = airflow.utils.email.send_email_smtp
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_port = 587'''
    
    def _generate_airflow_docker_compose(self, storage_recommendation: Dict[str, Any]) -> str:
        """Генерация docker-compose для Airflow"""
        storage = storage_recommendation['recommended_storage']
        
        compose = f'''version: '3.8'

# Airflow + ETL Services
# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

x-airflow-common: &airflow-common
  image: apache/airflow:2.5.0
  environment:
    - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
    - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
    - AIRFLOW__CORE__LOAD_EXAMPLES=false
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./data:/data
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    volumes:
      - postgres_db_volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    ports:
      - "5432:5432"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "${{HOSTNAME}}"']
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always

  airflow-init:
    <<: *airflow-common
    command: version
    environment:
      - _AIRFLOW_DB_UPGRADE=true
      - _AIRFLOW_WWW_USER_CREATE=true
      - _AIRFLOW_WWW_USER_USERNAME=admin
      - _AIRFLOW_WWW_USER_PASSWORD=admin
'''
        
        # Добавляем сервисы в зависимости от хранилища
        if storage == 'ClickHouse':
            compose += '''
  clickhouse:
    image: yandex/clickhouse-server:latest
    ports:
      - "9000:9000"
      - "8123:8123"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
'''
        
        elif storage == 'PostgreSQL':
            compose += '''
  postgres-data:
    image: postgres:13
    environment:
      - POSTGRES_USER=etl_user
      - POSTGRES_PASSWORD=etl_password
      - POSTGRES_DB=etl_data
    ports:
      - "5433:5432"
    volumes:
      - postgres_etl_data:/var/lib/postgresql/data
'''
        
        compose += '''
volumes:
  postgres_db_volume:'''
        
        if storage == 'ClickHouse':
            compose += '\n  clickhouse_data:'
        elif storage == 'PostgreSQL':
            compose += '\n  postgres_etl_data:'
        
        return compose
    
    def _generate_connections_script(self, storage_recommendation: Dict[str, Any]) -> str:
        """Генерация скрипта настройки подключений Airflow"""
        storage = storage_recommendation['recommended_storage']
        
        script = '''#!/bin/bash
# Скрипт настройки подключений Airflow для ETL Assistant

# Ожидание запуска Airflow
sleep 30

echo "Настраиваем подключения Airflow..."
'''
        
        if storage == 'ClickHouse':
            script += '''
# ClickHouse подключение
airflow connections add 'clickhouse_default' \\
    --conn-type 'http' \\
    --conn-host 'clickhouse' \\
    --conn-port 8123 \\
    --conn-extra '{"protocol": "http"}'
'''
        
        elif storage == 'PostgreSQL':
            script += '''
# PostgreSQL подключение для данных
airflow connections add 'postgres_data' \\
    --conn-type 'postgres' \\
    --conn-host 'postgres-data' \\
    --conn-port 5432 \\
    --conn-login 'etl_user' \\
    --conn-password 'etl_password' \\
    --conn-schema 'etl_data'
'''
        
        script += '''
# Файловая система подключение
airflow connections add 'fs_default' \\
    --conn-type 'fs' \\
    --conn-extra '{"path": "/data"}'

echo "Подключения настроены успешно!"
'''
        
        return script
    
    def _generate_airflow_requirements(self, storage_recommendation: Dict[str, Any]) -> str:
        """Генерация requirements.txt для Airflow"""
        storage = storage_recommendation['recommended_storage']
        
        requirements = [
            "# Airflow ETL Requirements",
            f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "apache-airflow==2.5.0",
            "pandas>=1.5.0",
            "numpy>=1.21.0"
        ]
        
        if storage == 'ClickHouse':
            requirements.extend([
                "clickhouse-driver>=0.2.0",
                "apache-airflow-providers-http>=4.0.0"
            ])
        elif storage == 'PostgreSQL':
            requirements.extend([
                "psycopg2-binary>=2.9.0",
                "apache-airflow-providers-postgres>=5.0.0"
            ])
        elif storage == 'HDFS':
            requirements.extend([
                "hdfs3>=0.3.0",
                "pyarrow>=8.0.0",
                "apache-airflow-providers-apache-hdfs>=3.0.0"
            ])
        
        return "\n".join(requirements)
