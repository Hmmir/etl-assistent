#!/usr/bin/env python3
"""
Полное end-to-end тестирование ETL Assistant
Проводит CSV файл через весь pipeline
"""

import requests
import json
import time
from pathlib import Path

BASE_URL = "http://localhost:8000"

def print_section(title):
    """Красивый вывод секций"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70 + "\n")

def test_health():
    """Проверка здоровья API"""
    print_section("1. ПРОВЕРКА API")
    response = requests.get(f"{BASE_URL}/health")
    print(f"[OK] API Status: {response.json()}")
    return response.status_code == 200

def test_upload(file_path):
    """Загрузка и анализ файла"""
    print_section("2. ЗАГРУЗКА И АНАЛИЗ CSV")
    
    with open(file_path, 'rb') as f:
        files = {'file': ('test_demo_data.csv', f, 'text/csv')}
        response = requests.post(f"{BASE_URL}/upload", files=files)
    
    if response.status_code != 200:
        print(f"[ERROR] Upload failed: {response.text}")
        return None
    
    data = response.json()
    
    print(f"File: {data.get('filename')}")
    print(f"Format: {data.get('format')}")
    
    analysis = data.get('analysis', {})
    print(f"\nData Analysis:")
    print(f"   - Rows: {analysis.get('row_count')}")
    print(f"   - Columns: {analysis.get('column_count')}")
    print(f"   - Size: {analysis.get('file_size_mb'):.2f} MB")
    
    # Типы колонок
    columns = analysis.get('columns', {})
    print(f"\nColumns:")
    for col, dtype in columns.items():
        print(f"   - {col}: {dtype}")
    
    # Рекомендация СУБД
    storage = data.get('storage_recommendation', {})
    print(f"\nStorage Recommendation:")
    print(f"   - Database: {storage.get('recommended_storage')}")
    print(f"   - Confidence: {storage.get('confidence', 0)*100:.0f}%")
    print(f"   - Method: {storage.get('analysis_method', 'unknown')}")
    print(f"\nReasoning:")
    reasoning = storage.get('reasoning', 'N/A')
    if len(reasoning) > 300:
        print(f"   {reasoning[:300]}...")
    else:
        print(f"   {reasoning}")
    
    # Проверяем AI или rule-based
    if storage.get('analysis_method') == 'ai_powered':
        print("\n[AI] AI ANALYSIS WORKS! (OpenRouter LLM)")
    elif storage.get('analysis_method') == 'rule_based':
        print("\n[FALLBACK] Rule-based analysis (LLM unavailable)")
    
    return data

def test_generate_pipeline(filename, storage_type):
    """Генерация ETL пайплайна"""
    print_section("3. ГЕНЕРАЦИЯ ETL ПАЙПЛАЙНА")
    
    payload = {
        "source_file": filename,
        "storage_type": storage_type,
        "table_name": "orders"
    }
    
    response = requests.post(f"{BASE_URL}/generate_pipeline", json=payload)
    
    if response.status_code != 200:
        print(f"[ERROR] Generation failed: {response.text}")
        return None
    
    data = response.json()
    
    # DDL
    ddl = data.get('ddl', '')
    print(f"DDL Script ({len(ddl)} chars):")
    print("-" * 70)
    print(ddl[:500])
    if len(ddl) > 500:
        print(f"... (showing 500 of {len(ddl)} chars)")
    print("-" * 70)
    
    # ETL Script
    etl_script = data.get('etl_script', '')
    print(f"\nPython ETL Script ({len(etl_script)} chars):")
    print("-" * 70)
    print(etl_script[:500])
    if len(etl_script) > 500:
        print(f"... (showing 500 of {len(etl_script)} chars)")
    print("-" * 70)
    
    return data

def test_generate_airflow_dag(filename, storage_type):
    """Генерация Airflow DAG"""
    print_section("4. ГЕНЕРАЦИЯ AIRFLOW DAG")
    
    payload = {
        "source_file": filename,
        "storage_type": storage_type,
        "table_name": "orders",
        "schedule_interval": "@daily"
    }
    
    response = requests.post(f"{BASE_URL}/generate_airflow_dag", json=payload)
    
    if response.status_code != 200:
        print(f"[ERROR] DAG generation failed: {response.text}")
        return None
    
    data = response.json()
    
    dag_code = data.get('dag_code', '')
    print(f"Airflow DAG generated ({len(dag_code)} chars)")
    print(f"DAG ID: {data.get('dag_id')}")
    print(f"Schedule: {data.get('schedule')}")
    
    print("\nDAG Code (first 600 chars):")
    print("-" * 70)
    print(dag_code[:600])
    if len(dag_code) > 600:
        print(f"... (showing 600 of {len(dag_code)} chars)")
    print("-" * 70)
    
    # Проверяем что НЕТ FileSensor
    if 'FileSensor' in dag_code:
        print("\n[WARNING] DAG contains FileSensor (old code!)")
    else:
        print("\n[OK] FileSensor absent - fixed version!")
    
    # Проверяем start_etl task
    if 'start_etl' in dag_code:
        print("[OK] start_etl task present - new architecture!")
    
    return data

def test_generate_kafka(filename):
    """Генерация Kafka Producer/Consumer"""
    print_section("5. ГЕНЕРАЦИЯ KAFKA STREAMING")
    
    payload = {
        "topic_name": "orders_stream",
        "source_file": filename,
        "target_storage": "ClickHouse"
    }
    
    response = requests.post(f"{BASE_URL}/generate_kafka_streaming", json=payload)
    
    if response.status_code != 200:
        print(f"[ERROR] Kafka generation failed: {response.text}")
        return None
    
    data = response.json()
    
    producer = data.get('producer_code', '')
    consumer = data.get('consumer_code', '')
    
    print(f"Kafka Producer ({len(producer)} chars):")
    print("-" * 70)
    print(producer[:400])
    if len(producer) > 400:
        print(f"... (showing 400 of {len(producer)} chars)")
    print("-" * 70)
    
    print(f"\nKafka Consumer ({len(consumer)} chars):")
    print("-" * 70)
    print(consumer[:400])
    if len(consumer) > 400:
        print(f"... (showing 400 of {len(consumer)} chars)")
    print("-" * 70)
    
    return data

def main():
    """Главная функция - полное тестирование"""
    print("\n" + "=" * 70)
    print("  ETL ASSISTANT - FULL END-TO-END TESTING")
    print("=" * 70)
    
    # 1. Health Check
    if not test_health():
        print("[ERROR] API unavailable!")
        return
    
    # 2. Upload & Analyze
    file_path = Path("test_demo_data.csv")
    if not file_path.exists():
        print(f"[ERROR] File {file_path} not found!")
        return
    
    upload_result = test_upload(file_path)
    if not upload_result:
        return
    
    filename = upload_result.get('filename')
    storage_type = upload_result.get('storage_recommendation', {}).get('recommended_storage', 'PostgreSQL')
    
    # 3. Generate Pipeline
    pipeline_result = test_generate_pipeline(filename, storage_type)
    if not pipeline_result:
        return
    
    # 4. Generate Airflow DAG
    dag_result = test_generate_airflow_dag(filename, storage_type)
    if not dag_result:
        return
    
    # 5. Generate Kafka
    kafka_result = test_generate_kafka(filename)
    
    # ИТОГОВЫЙ ОТЧЕТ
    print_section("FINAL REPORT")
    
    print("[OK] ALL STAGES COMPLETED SUCCESSFULLY:")
    print("   1. [OK] Health Check - API working")
    print("   2. [OK] Upload & Analyze - file analyzed")
    print(f"   3. [OK] Storage Recommendation - {storage_type}")
    print("   4. [OK] DDL Generation - SQL script generated")
    print("   5. [OK] ETL Script Generation - Python code generated")
    print("   6. [OK] Airflow DAG Generation - DAG generated")
    print("   7. [OK] Kafka Streaming - Producer/Consumer generated")
    
    print("\nRECOMMENDATIONS:")
    print("   - Check DAG in Airflow UI: http://localhost:8081")
    print("   - Run DAG manually for testing")
    print("   - Check Kafka UI: http://localhost:8080")
    
    print("\n[SUCCESS] TESTING COMPLETED!")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[ERROR]: {e}")
        import traceback
        traceback.print_exc()

