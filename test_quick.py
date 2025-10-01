#!/usr/bin/env python3
"""Quick test of ETL Assistant"""
import requests
import json
from pathlib import Path

BASE_URL = "http://localhost:8000"

print("=" * 70)
print("  QUICK ETL ASSISTANT TEST")
print("=" * 70)

# Test 1: Health
print("\n[1/4] Health Check...")
r = requests.get(f"{BASE_URL}/health")
print(f"Result: {r.json()}")

# Test 2: Upload
print("\n[2/4] Upload CSV file...")
file_path = Path("test_demo_data.csv")
if not file_path.exists():
    print(f"ERROR: {file_path} not found!")
    exit(1)

with open(file_path, 'rb') as f:
    files = {'file': ('test_demo_data.csv', f, 'text/csv')}
    r = requests.post(f"{BASE_URL}/upload", files=files)

if r.status_code != 200:
    print(f"ERROR: {r.status_code} - {r.text}")
    exit(1)

data = r.json()
print(f"File: {data.get('filename')}")
print(f"Rows: {data.get('analysis', {}).get('row_count', 'N/A')}")

storage = data.get('storage_recommendation', {})
print(f"Recommended DB: {storage.get('recommended_storage', 'N/A')}")
print(f"Method: {storage.get('analysis_method', 'N/A')}")

if storage.get('analysis_method') == 'ai_powered':
    print("\n*** AI WORKS! ***")
elif storage.get('analysis_method') == 'rule_based':
    print("\n*** FALLBACK (rule-based) ***")

# Test 3: Generate Pipeline
print("\n[3/4] Generate ETL Pipeline...")
payload = {
    "source_file": data.get('filename'),
    "storage_type": storage.get('recommended_storage', 'PostgreSQL'),
    "table_name": "orders"
}
r = requests.post(f"{BASE_URL}/generate_pipeline", json=payload)
if r.status_code == 200:
    result = r.json()
    print(f"DDL: {len(result.get('ddl', ''))} chars")
    print(f"ETL Script: {len(result.get('etl_script', ''))} chars")
else:
    print(f"ERROR: {r.status_code}")

# Test 4: Generate Airflow DAG
print("\n[4/4] Generate Airflow DAG...")
payload = {
    "source_file": data.get('filename'),
    "storage_type": storage.get('recommended_storage', 'PostgreSQL'),
    "table_name": "orders"
}
r = requests.post(f"{BASE_URL}/generate_airflow_dag", json=payload)
if r.status_code == 200:
    result = r.json()
    dag_code = result.get('dag_code', '')
    print(f"DAG Code: {len(dag_code)} chars")
    print(f"DAG ID: {result.get('dag_id', 'N/A')}")
    
    if 'FileSensor' in dag_code:
        print("[WARNING] FileSensor found!")
    else:
        print("[OK] FileSensor fixed!")
    
    if 'start_etl' in dag_code:
        print("[OK] start_etl present!")
else:
    print(f"ERROR: {r.status_code}")

print("\n" + "=" * 70)
print("  TEST COMPLETED!")
print("=" * 70)

