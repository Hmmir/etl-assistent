#!/usr/bin/env python3
"""
Infrastructure Manager для автоматизации развертывания ETL системы

Автоматически сгенерирован ETL Assistant
Дата создания: 2025-09-22

Поддерживаемые режимы:
- DEMO: Локальный Docker Compose (ClickHouse + PostgreSQL + Airflow)
- PRODUCTION: Yandex Cloud Managed Services
"""

import subprocess
import os
import time
import requests
import logging
from pathlib import Path
from typing import Dict, List, Tuple
import yaml
import json

logger = logging.getLogger(__name__)

class InfrastructureManager:
    def __init__(self):
        self.project_root = Path(".")
        self.demo_mode = True  # По умолчанию демо режим
        
    def check_docker_available(self) -> bool:
        """Проверка доступности Docker"""
        try:
            result = subprocess.run(['docker', '--version'], 
                                 capture_output=True, text=True)
            return result.returncode == 0
        except:
            return False
    
    def check_yc_cli_available(self) -> bool:
        """Проверка доступности Yandex Cloud CLI"""
        try:
            result = subprocess.run(['yc', '--version'], 
                                 capture_output=True, text=True)
            return result.returncode == 0
        except:
            return False
    
    def create_demo_docker_compose(self) -> str:
        """Создание Docker Compose файла для демо режима"""
        compose_content = """# ETL Assistant Demo Infrastructure
# Автоматически сгенерирован: 2025-09-22

name: etl-assistant-demo

services:
  # ClickHouse - основное аналитическое хранилище
  clickhouse:
    image: yandex/clickhouse-server:latest
    container_name: etl_clickhouse
    ports:
      - "9000:9000"   # Native protocol
      - "8123:8123"   # HTTP interface
    environment:
      - CLICKHOUSE_DB=etl_data
      - CLICKHOUSE_USER=default
      - CLICKHOUSE_PASSWORD=demo123
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./init-clickhouse.sql:/docker-entrypoint-initdb.d/init.sql
    networks:
      - etl_network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8123/ping"]
      interval: 10s
      timeout: 5s
      retries: 3

  # PostgreSQL - транзакционное хранилище  
  postgres:
    image: postgres:15-alpine
    container_name: etl_postgres
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_DB=etl_metadata
      - POSTGRES_USER=etl_user
      - POSTGRES_PASSWORD=demo123
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init-postgres.sql:/docker-entrypoint-initdb.d/init.sql
    networks:
      - etl_network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U etl_user -d etl_metadata"]
      interval: 10s
      timeout: 5s
      retries: 3

  # Redis - кэш и очереди
  redis:
    image: redis:7-alpine
    container_name: etl_redis
    ports:
      - "6379:6379"
    networks:
      - etl_network
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 3

  # Apache Airflow - оркестрация
  airflow-init:
    image: apache/airflow:2.7.2
    container_name: etl_airflow_init
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://etl_user:demo123@postgres:5432/etl_metadata
      - AIRFLOW__CORE__FERNET_KEY=YlCImzjge_TeZc7jGvnKrYjNBUeTeSEXZr-oB3nVJJ8=
      - AIRFLOW__WEBSERVER__SECRET_KEY=demo_secret_key_for_airflow
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - etl_network
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
    command: >
      bash -c "
        airflow db init &&
        airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
      "

  airflow-webserver:
    image: apache/airflow:2.7.2
    container_name: etl_airflow_webserver
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://etl_user:demo123@postgres:5432/etl_metadata
      - AIRFLOW__CORE__FERNET_KEY=YlCImzjge_TeZc7jGvnKrYjNBUeTeSEXZr-oB3nVJJ8=
      - AIRFLOW__WEBSERVER__SECRET_KEY=demo_secret_key_for_airflow
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
    ports:
      - "8080:8080"
    depends_on:
      - airflow-init
      - postgres
      - redis
    networks:
      - etl_network
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
    command: airflow webserver
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  airflow-scheduler:
    image: apache/airflow:2.7.2
    container_name: etl_airflow_scheduler
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://etl_user:demo123@postgres:5432/etl_metadata
      - AIRFLOW__CORE__FERNET_KEY=YlCImzjge_TeZc7jGvnKrYjNBUeTeSEXZr-oB3nVJJ8=
      - AIRFLOW__WEBSERVER__SECRET_KEY=demo_secret_key_for_airflow
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
    depends_on:
      - airflow-init
      - postgres
      - redis
    networks:
      - etl_network
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
    command: airflow scheduler

volumes:
  clickhouse_data:
  postgres_data:

networks:
  etl_network:
    driver: bridge
"""
        
        with open("docker-compose-demo.yml", "w", encoding="utf-8") as f:
            f.write(compose_content)
        
        return "docker-compose-demo.yml"
    
    def create_init_scripts(self):
        """Создание SQL скриптов инициализации"""
        
        # ClickHouse init script
        clickhouse_init = """
-- Создание базы данных для ETL
CREATE DATABASE IF NOT EXISTS etl_data;

-- Создание пользователя для ETL
CREATE USER IF NOT EXISTS etl_user IDENTIFIED BY 'demo123';
GRANT ALL ON etl_data.* TO etl_user;

-- Пример таблицы для демонстрации
CREATE TABLE IF NOT EXISTS etl_data.demo_table (
    id UInt64,
    name String,
    created_at DateTime,
    data String
) ENGINE = MergeTree()
ORDER BY id
PARTITION BY toYYYYMM(created_at);

-- Вставка тестовых данных
INSERT INTO etl_data.demo_table VALUES 
(1, 'Demo Record 1', now(), 'Sample data for demonstration'),
(2, 'Demo Record 2', now(), 'Another sample record');
"""
        
        with open("init-clickhouse.sql", "w", encoding="utf-8") as f:
            f.write(clickhouse_init)
        
        # PostgreSQL init script
        postgres_init = """
-- Создание схем для метаданных ETL
CREATE SCHEMA IF NOT EXISTS airflow;
CREATE SCHEMA IF NOT EXISTS etl_metadata;

-- Таблица для метаданных пайплайнов
CREATE TABLE IF NOT EXISTS etl_metadata.pipelines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    source_type VARCHAR(100),
    target_type VARCHAR(100),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Вставка тестовых метаданных
INSERT INTO etl_metadata.pipelines (name, source_type, target_type, status) VALUES
('demo_csv_pipeline', 'CSV', 'ClickHouse', 'active'),
('demo_json_pipeline', 'JSON', 'PostgreSQL', 'inactive');
"""
        
        with open("init-postgres.sql", "w", encoding="utf-8") as f:
            f.write(postgres_init)
    
    def create_airflow_directories(self):
        """Создание директорий для Airflow"""
        directories = [
            "airflow/dags",
            "airflow/logs", 
            "airflow/plugins"
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def deploy_demo_infrastructure(self) -> Dict:
        """Развертывание демо инфраструктуры"""
        try:
            logger.info("🚀 Начинаем развертывание демо инфраструктуры...")
            
            # Проверяем Docker
            if not self.check_docker_available():
                return {
                    "success": False,
                    "error": "Docker не установлен или недоступен"
                }
            
            # Создаем файлы конфигурации
            compose_file = self.create_demo_docker_compose()
            self.create_init_scripts()
            self.create_airflow_directories()
            
            logger.info("📁 Файлы конфигурации созданы")
            
            # Останавливаем существующие контейнеры
            subprocess.run(['docker', 'compose', '-f', compose_file, '-p', 'etl-assistant-demo', 'down'], 
                         capture_output=True)
            
            # Запускаем инфраструктуру
            logger.info("🐳 Запускаем Docker Compose...")
            result = subprocess.run(['docker', 'compose', '-f', compose_file, '-p', 'etl-assistant-demo', 'up', '-d'], 
                                  capture_output=True, text=True, encoding='utf-8')
            
            if result.returncode != 0:
                return {
                    "success": False,
                    "error": f"Ошибка запуска Docker Compose: {result.stderr}"
                }
            
            # Ждем готовности сервисов
            logger.info("⏳ Ожидаем готовности сервисов...")
            services_status = self.wait_for_services()
            
            return {
                "success": True,
                "message": "✅ Демо инфраструктура развернута успешно!",
                "services": services_status,
                "endpoints": {
                    "clickhouse_http": "http://localhost:8123",
                    "postgres": "localhost:5432",
                    "airflow_ui": "http://localhost:8080",
                    "redis": "localhost:6379"
                },
                "credentials": {
                    "clickhouse": {"user": "etl_user", "password": "demo123"},
                    "postgres": {"user": "etl_user", "password": "demo123", "db": "etl_metadata"},
                    "airflow": {"user": "admin", "password": "admin"}
                }
            }
            
        except Exception as e:
            logger.error(f"Ошибка развертывания: {str(e)}")
            return {
                "success": False,
                "error": f"Ошибка развертывания: {str(e)}"
            }
    
    def wait_for_services(self, timeout: int = 120) -> Dict:
        """Ожидание готовности сервисов"""
        services = {
            "clickhouse": {"url": "http://localhost:8123/ping", "status": "unknown"},
            "postgres": {"port": 5432, "status": "unknown"},
            "airflow": {"url": "http://localhost:8080/health", "status": "unknown"},
            "redis": {"port": 6379, "status": "unknown"}
        }
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            all_ready = True
            
            # Проверка ClickHouse
            try:
                response = requests.get("http://localhost:8123/ping", timeout=5)
                services["clickhouse"]["status"] = "ready" if response.status_code == 200 else "starting"
            except:
                services["clickhouse"]["status"] = "starting"
                all_ready = False
            
            # Проверка Airflow
            try:
                response = requests.get("http://localhost:8080/health", timeout=5)
                services["airflow"]["status"] = "ready" if response.status_code == 200 else "starting"
            except:
                services["airflow"]["status"] = "starting"
                all_ready = False
            
            # Проверка PostgreSQL и Redis через docker ps
            try:
                result = subprocess.run(['docker', 'ps', '--format', 'table {{.Names}}\\t{{.Status}}'], 
                                      capture_output=True, text=True)
                
                if "etl_postgres" in result.stdout and "Up" in result.stdout:
                    services["postgres"]["status"] = "ready"
                else:
                    services["postgres"]["status"] = "starting"
                    all_ready = False
                    
                if "etl_redis" in result.stdout and "Up" in result.stdout:
                    services["redis"]["status"] = "ready"
                else:
                    services["redis"]["status"] = "starting"
                    all_ready = False
            except:
                services["postgres"]["status"] = "unknown"
                services["redis"]["status"] = "unknown"
                all_ready = False
            
            if all_ready:
                break
                
            time.sleep(5)
        
        return services
    
    def get_infrastructure_status(self) -> Dict:
        """Получение статуса инфраструктуры"""
        try:
            # Проверяем запущенные контейнеры проекта
            result = subprocess.run(['docker', 'compose', '-p', 'etl-assistant-demo', 'ps', '--format', 'json'], 
                                  capture_output=True, text=True, encoding='utf-8')
            
            containers = []
            
            if result.returncode == 0 and result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        try:
                            container = json.loads(line)
                            containers.append({
                                "name": container.get('Name', ''),
                                "status": container.get('Status', ''),
                                "state": container.get('State', ''),
                                "ports": container.get('Publishers', [])
                            })
                        except:
                            pass
            
            # Fallback: проверяем через docker ps
            if not containers:
                result = subprocess.run(['docker', 'ps', '--format', 'json'], 
                                      capture_output=True, text=True, encoding='utf-8')
                
                if result.returncode == 0:
                    for line in result.stdout.strip().split('\n'):
                        if line.strip():
                            try:
                                container = json.loads(line)
                                if any(name in container.get('Names', '') for name in ['etl_', 'airflow']):
                                    containers.append({
                                        "name": container.get('Names', ''),
                                        "status": container.get('Status', ''),
                                        "ports": container.get('Ports', '')
                                    })
                            except:
                                pass
            
            # Проверяем доступность сервисов
            services_status = self.wait_for_services(timeout=10)
            
            return {
                "success": True,
                "containers": containers,
                "services": services_status,
                "demo_mode": self.demo_mode
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def stop_infrastructure(self) -> Dict:
        """Остановка инфраструктуры"""
        try:
            compose_file = "docker-compose-demo.yml"
            
            if not Path(compose_file).exists():
                return {"success": False, "error": "Docker Compose файл не найден"}
            
            result = subprocess.run(['docker', 'compose', '-f', compose_file, '-p', 'etl-assistant-demo', 'down'], 
                                  capture_output=True, text=True, encoding='utf-8')
            
            if result.returncode == 0:
                return {"success": True, "message": "🛑 Инфраструктура остановлена"}
            else:
                return {"success": False, "error": f"Ошибка остановки: {result.stderr}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}

    def deploy_dag_to_airflow(self, dag_content: str, dag_name: str) -> Dict:
        """Развертывание DAG в Airflow"""
        try:
            dag_file_path = Path(f"airflow/dags/{dag_name}")
            
            with open(dag_file_path, 'w', encoding='utf-8') as f:
                f.write(dag_content)
            
            # Ждем пока Airflow подхватит новый DAG (обычно 30-60 секунд)
            return {
                "success": True,
                "message": f"✅ DAG {dag_name} развернут в Airflow",
                "dag_file": str(dag_file_path),
                "airflow_ui": "http://localhost:8080"
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
