#!/usr/bin/env python3
"""
Полное тестирование ETL Assistant
Проверка всех компонентов и интеграций

Автоматически сгенерирован ETL Assistant
Дата: 2025-09-25

Тестирует:
- Backend API endpoints
- YandexGPT интеграцию
- Анализ данных
- Генерацию ETL кода
- Airflow DAG
- Kafka streaming
- Infrastructure management
"""

import asyncio
import aiohttp
import json
import os
import time
from pathlib import Path
import tempfile
import csv
from typing import Dict, List, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLAssistantTester:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = None
        self.test_results = []
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            
    async def log_test(self, test_name: str, success: bool, details: str = ""):
        """Логирование результатов тестов"""
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"{status} {test_name}: {details}")
        
        self.test_results.append({
            "test": test_name,
            "success": success,
            "details": details,
            "timestamp": time.time()
        })
        
    async def test_health_check(self):
        """Тест health check endpoint"""
        try:
            async with self.session.get(f"{self.base_url}/health") as response:
                if response.status == 200:
                    data = await response.json()
                    yandexgpt_status = data.get("yandexgpt_configured", False)
                    await self.log_test(
                        "Health Check",
                        True,
                        f"Service healthy, YandexGPT configured: {yandexgpt_status}"
                    )
                    return True
                else:
                    await self.log_test("Health Check", False, f"HTTP {response.status}")
                    return False
        except Exception as e:
            await self.log_test("Health Check", False, str(e))
            return False
            
    async def test_yandexgpt_status(self):
        """Тест статуса YandexGPT"""
        try:
            async with self.session.get(f"{self.base_url}/yandexgpt/status") as response:
                if response.status == 200:
                    data = await response.json()
                    yandexgpt = data.get("yandexgpt", {})
                    configured = yandexgpt.get("configured", False)
                    
                    await self.log_test(
                        "YandexGPT Status",
                        True,
                        f"Configured: {configured}, Folder ID: {yandexgpt.get('folder_id_set', False)}"
                    )
                    return configured
                else:
                    await self.log_test("YandexGPT Status", False, f"HTTP {response.status}")
                    return False
        except Exception as e:
            await self.log_test("YandexGPT Status", False, str(e))
            return False
            
    async def test_yandexgpt_api(self):
        """Тест YandexGPT API"""
        try:
            test_data = {
                "prompt": "Что такое ETL? Ответь кратко."
            }
            
            async with self.session.post(
                f"{self.base_url}/yandexgpt/test",
                json=test_data
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("status") == "success":
                        response_text = data.get("response", "")
                        await self.log_test(
                            "YandexGPT API Test",
                            True,
                            f"Response length: {len(response_text)} chars"
                        )
                        return True
                    else:
                        await self.log_test(
                            "YandexGPT API Test",
                            False,
                            data.get("error", "Unknown error")
                        )
                        return False
                else:
                    await self.log_test("YandexGPT API Test", False, f"HTTP {response.status}")
                    return False
        except Exception as e:
            await self.log_test("YandexGPT API Test", False, str(e))
            return False
            
    async def create_test_csv(self) -> str:
        """Создание тестового CSV файла"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'email', 'date', 'amount', 'category'])
            
            # Генерируем тестовые данные
            for i in range(1000):
                writer.writerow([
                    i + 1,
                    f"User {i+1}",
                    f"user{i+1}@example.com",
                    f"2023-0{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                    round((i + 1) * 10.50, 2),
                    ["A", "B", "C", "D"][i % 4]
                ])
            
            return f.name
            
    async def test_file_upload_and_analysis(self):
        """Тест загрузки файла и анализа данных"""
        try:
            # Создаем тестовый файл
            csv_path = await self.create_test_csv()
            
            # Загружаем файл
            with open(csv_path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('file', f, filename='test_data.csv')
                
                async with self.session.post(
                    f"{self.base_url}/upload",
                    data=data
                ) as response:
                    if response.status == 200:
                        upload_result = await response.json()
                        filename = upload_result.get('filename')
                        
                        await self.log_test(
                            "File Upload",
                            True,
                            f"Uploaded as: {filename}"
                        )
                        
                        # Анализируем загруженный файл
                        return await self.test_data_analysis(filename)
                    else:
                        await self.log_test("File Upload", False, f"HTTP {response.status}")
                        return False
                        
        except Exception as e:
            await self.log_test("File Upload", False, str(e))
            return False
        finally:
            # Удаляем временный файл
            if 'csv_path' in locals():
                os.unlink(csv_path)
                
    async def test_data_analysis(self, filename: str):
        """Тест анализа данных"""
        try:
            async with self.session.get(f"{self.base_url}/analyze/{filename}") as response:
                if response.status == 200:
                    analysis = await response.json()
                    
                    # Проверяем ключевые поля анализа
                    data_info = analysis.get('data_info', {})
                    storage_rec = analysis.get('storage_recommendation', {})
                    
                    rows = data_info.get('rows', 0)
                    columns = data_info.get('columns', 0)
                    recommended_storage = storage_rec.get('recommended_storage', 'unknown')
                    
                    await self.log_test(
                        "Data Analysis",
                        rows > 0 and columns > 0,
                        f"Rows: {rows}, Columns: {columns}, Storage: {recommended_storage}"
                    )
                    
                    return filename
                else:
                    await self.log_test("Data Analysis", False, f"HTTP {response.status}")
                    return None
        except Exception as e:
            await self.log_test("Data Analysis", False, str(e))
            return None
            
    async def test_etl_generation(self, filename: str):
        """Тест генерации ETL кода"""
        try:
            async with self.session.post(f"{self.base_url}/generate_etl_script/{filename}") as response:
                if response.status == 200:
                    etl_result = await response.json()
                    
                    etl_code = etl_result.get('etl_script', '')
                    sql_scripts = etl_result.get('sql_scripts', {})
                    
                    await self.log_test(
                        "ETL Code Generation",
                        len(etl_code) > 100,
                        f"ETL script: {len(etl_code)} chars, SQL scripts: {len(sql_scripts)} types"
                    )
                    return True
                else:
                    await self.log_test("ETL Code Generation", False, f"HTTP {response.status}")
                    return False
        except Exception as e:
            await self.log_test("ETL Code Generation", False, str(e))
            return False
            
    async def test_airflow_dag_generation(self, filename: str):
        """Тест генерации Airflow DAG"""
        try:
            async with self.session.post(f"{self.base_url}/generate_airflow_dag/{filename}") as response:
                if response.status == 200:
                    dag_result = await response.json()
                    
                    dag_code = dag_result.get('dag_code', '')
                    dag_config = dag_result.get('dag_config', {})
                    
                    await self.log_test(
                        "Airflow DAG Generation",
                        len(dag_code) > 200,
                        f"DAG code: {len(dag_code)} chars, Config keys: {len(dag_config)}"
                    )
                    return True
                else:
                    await self.log_test("Airflow DAG Generation", False, f"HTTP {response.status}")
                    return False
        except Exception as e:
            await self.log_test("Airflow DAG Generation", False, str(e))
            return False
            
    async def test_pipeline_execution(self, filename: str):
        """Тест выполнения пайплайна"""
        try:
            async with self.session.post(f"{self.base_url}/execute_pipeline/{filename}") as response:
                if response.status == 200:
                    execution_result = await response.json()
                    
                    status = execution_result.get('status', '')
                    processed_rows = execution_result.get('processed_rows', 0)
                    
                    await self.log_test(
                        "Pipeline Execution",
                        status == 'success',
                        f"Status: {status}, Processed rows: {processed_rows}"
                    )
                    return True
                else:
                    await self.log_test("Pipeline Execution", False, f"HTTP {response.status}")
                    return False
        except Exception as e:
            await self.log_test("Pipeline Execution", False, str(e))
            return False
            
    async def test_kafka_streaming(self):
        """Тест Kafka streaming"""
        try:
            config = {
                "source_type": "kafka",
                "target_type": "clickhouse",
                "topic_name": "test_stream"
            }
            
            async with self.session.post(
                f"{self.base_url}/generate_streaming_pipeline",
                json=config
            ) as response:
                if response.status == 200:
                    stream_result = await response.json()
                    
                    generated_files = stream_result.get('generated_files', [])
                    features = stream_result.get('features', [])
                    
                    await self.log_test(
                        "Kafka Streaming",
                        len(generated_files) >= 3,
                        f"Files: {len(generated_files)}, Features: {len(features)}"
                    )
                    return True
                else:
                    await self.log_test("Kafka Streaming", False, f"HTTP {response.status}")
                    return False
        except Exception as e:
            await self.log_test("Kafka Streaming", False, str(e))
            return False
            
    async def test_infrastructure_status(self):
        """Тест статуса инфраструктуры"""
        try:
            async with self.session.get(f"{self.base_url}/infrastructure/status") as response:
                if response.status == 200:
                    infra_status = await response.json()
                    
                    services = infra_status.get('services', {})
                    
                    await self.log_test(
                        "Infrastructure Status",
                        True,
                        f"Services reported: {len(services)}"
                    )
                    return True
                else:
                    await self.log_test("Infrastructure Status", False, f"HTTP {response.status}")
                    return False
        except Exception as e:
            await self.log_test("Infrastructure Status", False, str(e))
            return False
            
    async def run_all_tests(self):
        """Запуск всех тестов"""
        logger.info("🚀 НАЧИНАЕМ ПОЛНОЕ ТЕСТИРОВАНИЕ ETL ASSISTANT")
        logger.info("=" * 60)
        
        # Основные тесты
        await self.test_health_check()
        yandexgpt_configured = await self.test_yandexgpt_status()
        
        if yandexgpt_configured:
            await self.test_yandexgpt_api()
        
        # Тестирование основной функциональности
        filename = await self.test_file_upload_and_analysis()
        
        if filename:
            await self.test_etl_generation(filename)
            await self.test_airflow_dag_generation(filename)
            await self.test_pipeline_execution(filename)
            
        # Дополнительные тесты
        await self.test_kafka_streaming()
        await self.test_infrastructure_status()
        
        # Подводим итоги
        await self.print_test_summary()
        
    async def print_test_summary(self):
        """Вывод итогов тестирования"""
        logger.info("=" * 60)
        logger.info("📊 ИТОГИ ТЕСТИРОВАНИЯ")
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for t in self.test_results if t["success"])
        failed_tests = total_tests - passed_tests
        
        logger.info(f"✅ Пройдено: {passed_tests}/{total_tests}")
        logger.info(f"❌ Провалено: {failed_tests}/{total_tests}")
        logger.info(f"📈 Успешность: {(passed_tests/total_tests)*100:.1f}%")
        
        if failed_tests > 0:
            logger.info("\n❌ НЕУДАЧНЫЕ ТЕСТЫ:")
            for test in self.test_results:
                if not test["success"]:
                    logger.info(f"   - {test['test']}: {test['details']}")
        
        logger.info("=" * 60)
        
        # Рекомендации
        if passed_tests == total_tests:
            logger.info("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! ПРОЕКТ ГОТОВ К ДЕМОНСТРАЦИИ!")
        elif passed_tests >= total_tests * 0.8:
            logger.info("✅ ПРОЕКТ В ОСНОВНОМ РАБОТАЕТ. MINOR ISSUES.")
        else:
            logger.info("⚠️ НАЙДЕНЫ КРИТИЧЕСКИЕ ПРОБЛЕМЫ. НУЖНЫ ИСПРАВЛЕНИЯ.")


async def main():
    """Главная функция тестирования"""
    try:
        async with ETLAssistantTester() as tester:
            await tester.run_all_tests()
    except KeyboardInterrupt:
        logger.info("\n⏹️ Тестирование прервано пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    asyncio.run(main())
