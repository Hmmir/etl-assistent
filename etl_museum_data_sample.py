#!/usr/bin/env python3
"""
Airflow DAG для ETL обработки museum_data_sample.csv

Автоматически сгенерирован ETL Assistant
Дата создания: 2025-09-20 15:11:25

Источник: CSV файл (332.11 MB)
Целевое хранилище: ClickHouse
Строк данных: 478,615
Расписание: @daily
"""

from datetime import datetime, timedelta\nfrom airflow import DAG\nfrom airflow.operators.python import PythonOperator\nfrom airflow.operators.bash import BashOperator\nfrom airflow.operators.email import EmailOperator\nfrom airflow.sensors.filesystem import FileSensor\nimport pandas as pd\nimport logging\nimport os\nfrom airflow.providers.http.operators.http import SimpleHttpOperator\nfrom clickhouse_driver import Client

# Настройки по умолчанию для всех задач
default_args = {
    'owner': 'etl_assistant',
    'depends_on_past': False,
    'start_date': datetime(2025, 01, 01),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# Определение DAG
dag = DAG(
    dag_id='etl_museum_data_sample',
    description='ETL процесс для museum_data_sample.csv',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'auto-generated', 'csv'],
    max_active_runs=1
)



# Задача 1: Проверка наличия исходного файла
file_sensor = FileSensor(
    task_id='check_source_file',
    filepath='/data/museum_data_sample.csv',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag
)\n
def analyze_data_task(**context):
    """
    Задача анализа структуры данных
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем анализ данных файла museum_data_sample.csv")
    
    # Здесь будет логика анализа данных
    # В реальной реализации подключается к ETL Assistant API
    
    file_path = "/data/museum_data_sample.csv"
    
    # Базовая проверка файла
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл {file_path} не найден")
    
    file_size = os.path.getsize(file_path)
    logger.info(f"Размер файла: {file_size} байт")
    
    # Возвращаем результат для следующих задач
    return {
        "file_path": file_path,
        "file_size": file_size,
        "analysis_completed": True
    }

analyze_data = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data_task,
    dag=dag
)\n
def extract_csv_data(**context):
    """
    Извлечение данных из CSV файла
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем извлечение данных из CSV")
    
    file_path = "/data/museum_data_sample.csv"
    
    try:
        # Читаем CSV с определенными параметрами
        df = pd.read_csv(
            file_path,
            sep=';',
            encoding='utf-8',
            low_memory=False
        )
        
        logger.info(f"Извлечено {len(df):,} строк, {len(df.columns)} колонок")
        
        # Сохраняем временно для следующей задачи
        temp_path = "/tmp/extracted_data.parquet"
        df.to_parquet(temp_path)
        
        return {
            "temp_file": temp_path,
            "rows_extracted": len(df),
            "columns_count": len(df.columns)
        }
        
    except Exception as e:
        logger.error(f"Ошибка извлечения данных: {str(e)}")
        raise

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_csv_data,
    dag=dag
)\n
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
        
        # Обработка пропущенных значений\n        df = df.dropna(how='all')  # Удаляем полностью пустые строки\n        # Нормализация дат\n        datetime_cols = [col for col in df.columns if 'date' in col.lower()]\n        for col in datetime_cols:\n            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Добавляем служебные поля
        df['etl_processed_at'] = pd.Timestamp.now()
        df['etl_batch_id'] = context['run_id']
        
        # Сохраняем трансформированные данные
        transformed_path = "/tmp/transformed_data.parquet"
        df.to_parquet(transformed_path)
        
        logger.info(f"Трансформация завершена: {original_rows} → {len(df)} строк")
        
        return {
            "transformed_file": transformed_path,
            "rows_processed": len(df),
            "transformation_completed": True
        }
        
    except Exception as e:
        logger.error(f"Ошибка трансформации данных: {str(e)}")
        raise

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_task,
    dag=dag
)\n
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
            {'table': 'museum_data_sample'}
        )[0][0] > 0
        
        if not table_exists:
            logger.info("Таблица не существует - создаем")
            # В реальной реализации здесь будет DDL из рекомендаций
            
        # Загружаем данные
        data_tuples = [tuple(row) for row in df.values]
        placeholders = "(" + ",".join(["%s"] * len(df.columns)) + ")"
        
        insert_query = f"INSERT INTO museum_data_sample VALUES {placeholders}"
        client.execute(insert_query, data_tuples)
        
        logger.info(f"Успешно загружено {len(df):,} записей в ClickHouse")
        
        return {
            "rows_loaded": len(df),
            "target_table": "museum_data_sample",
            "load_completed": True
        }
        
    except Exception as e:
        logger.error(f"Ошибка загрузки в ClickHouse: {str(e)}")
        raise

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_to_clickhouse_task,
    dag=dag
)\n
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
)\n
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
)


# Определение зависимостей между задачами
file_sensor >> analyze_data >> extract_data >> transform_data >> load_data >> quality_check >> success_notification

