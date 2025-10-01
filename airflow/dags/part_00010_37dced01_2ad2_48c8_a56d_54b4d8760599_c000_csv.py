#!/usr/bin/env python3
"""
Airflow DAG для ETL обработки part-00010-37dced01-2ad2-48c8-a56d-54b4d8760599-c000.csv

Автоматически сгенерирован ETL Assistant
Дата создания: 2025-10-01 02:43:36

Источник: CSV файл (332.02 MB)
Целевое хранилище: PostgreSQL
Строк данных: 478,619
Расписание: @hourly
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd
import logging
import os
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Настройки по умолчанию для всех задач
default_args = {
    'owner': 'etl_assistant',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# Определение DAG
dag = DAG(
    dag_id='part_00010_37dced01_2ad2_48c8_a56d_54b4d8760599_c000_csv',
    description='ETL процесс для part-00010-37dced01-2ad2-48c8-a56d-54b4d8760599-c000.csv',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    tags=['etl', 'auto-generated', 'csv'],
    max_active_runs=1
)



# Задача 1: Проверка наличия исходного файла
file_sensor = FileSensor(
    task_id='check_source_file',
    filepath='/data/part-00010-37dced01-2ad2-48c8-a56d-54b4d8760599-c000.csv',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag
)

def analyze_data_task(**context):
    """
    Задача анализа структуры данных
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем анализ данных файла part-00010-37dced01-2ad2-48c8-a56d-54b4d8760599-c000.csv")
    
    # Здесь будет логика анализа данных
    # В реальной реализации подключается к ETL Assistant API
    
    file_path = "/data/part-00010-37dced01-2ad2-48c8-a56d-54b4d8760599-c000.csv"
    
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
)

def extract_csv_data(**context):
    """
    Извлечение данных из CSV файла
    """
    logger = logging.getLogger(__name__)
    logger.info("Начинаем извлечение данных из CSV")
    
    file_path = "/data/part-00010-37dced01-2ad2-48c8-a56d-54b4d8760599-c000.csv"
    
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
)

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
)

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
            name='part_00010_37dced01_2ad2_48c8_a56d_54b4d8760599_c000',
            con=postgres_hook.get_sqlalchemy_engine(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        logger.info(f"Успешно загружено {len(df):,} записей в PostgreSQL")
        
        return {
            "rows_loaded": len(df),
            "target_table": "part_00010_37dced01_2ad2_48c8_a56d_54b4d8760599_c000",
            "load_completed": True
        }
        
    except Exception as e:
        logger.error(f"Ошибка загрузки в PostgreSQL: {str(e)}")
        raise

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_to_postgresql_task,
    dag=dag
)

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
)

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

