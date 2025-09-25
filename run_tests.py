#!/usr/bin/env python3
"""
Скрипт для запуска всех тестов ETL Assistant
Автоматически сгенерирован ETL Assistant
Дата: 2025-09-25
"""

import asyncio
import subprocess
import sys
import time
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

class TestRunner:
    def __init__(self):
        self.project_root = Path(".")
        
    def check_backend_running(self):
        """Проверка, запущен ли backend"""
        try:
            import requests
            response = requests.get("http://localhost:8000/health", timeout=5)
            return response.status_code == 200
        except:
            return False
            
    def start_backend(self):
        """Запуск backend сервера"""
        logger.info("🚀 Запускаем backend сервер...")
        
        try:
            # Проверяем наличие requirements
            if not (self.project_root / "requirements.txt").exists():
                logger.error("❌ requirements.txt не найден")
                return False
                
            # Устанавливаем зависимости
            logger.info("📦 Устанавливаем зависимости...")
            result = subprocess.run([
                sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "--quiet"
            ], capture_output=True)
            
            if result.returncode != 0:
                logger.error("❌ Ошибка установки зависимостей")
                return False
                
            # Запускаем сервер в фоне
            logger.info("🔄 Запускаем FastAPI сервер...")
            self.backend_process = subprocess.Popen([
                sys.executable, "-m", "uvicorn", "backend.main:app", 
                "--host", "127.0.0.1", "--port", "8000", "--reload"
            ])
            
            # Ждем запуска
            for i in range(30):  # 30 секунд ожидания
                if self.check_backend_running():
                    logger.info("✅ Backend запущен!")
                    return True
                time.sleep(1)
                
            logger.error("❌ Backend не запустился за 30 секунд")
            return False
            
        except Exception as e:
            logger.error(f"❌ Ошибка запуска backend: {e}")
            return False
            
    async def run_yandex_cloud_tests(self):
        """Запуск тестов Yandex Cloud"""
        logger.info("\n" + "=" * 60)
        logger.info("🔍 ТЕСТИРОВАНИЕ YANDEX CLOUD SETUP")
        logger.info("=" * 60)
        
        try:
            from test_yandex_cloud_setup import YandexCloudTester
            tester = YandexCloudTester()
            await tester.run_all_tests()
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка тестирования Yandex Cloud: {e}")
            return False
            
    async def run_system_tests(self):
        """Запуск полных системных тестов"""
        logger.info("\n" + "=" * 60)
        logger.info("🧪 ТЕСТИРОВАНИЕ СИСТЕМЫ")
        logger.info("=" * 60)
        
        try:
            from test_complete_system import ETLAssistantTester
            async with ETLAssistantTester() as tester:
                await tester.run_all_tests()
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка системного тестирования: {e}")
            return False
            
    def cleanup(self):
        """Очистка ресурсов"""
        if hasattr(self, 'backend_process'):
            logger.info("🛑 Останавливаем backend...")
            self.backend_process.terminate()
            self.backend_process.wait(timeout=10)
            
    async def run_all_tests(self):
        """Запуск всех тестов"""
        try:
            logger.info("🎯 ETL ASSISTANT - ПОЛНОЕ ТЕСТИРОВАНИЕ")
            logger.info("=" * 60)
            
            # 1. Тест Yandex Cloud setup
            await self.run_yandex_cloud_tests()
            
            # 2. Запуск backend если нужно
            backend_was_running = self.check_backend_running()
            
            if not backend_was_running:
                if not self.start_backend():
                    logger.error("❌ Не удалось запустить backend")
                    return False
                    
            # 3. Системные тесты
            await self.run_system_tests()
            
            # 4. Итоги
            logger.info("\n" + "=" * 60)
            logger.info("🏁 ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
            logger.info("=" * 60)
            
            logger.info("📋 СЛЕДУЮЩИЕ ШАГИ:")
            logger.info("1. Исправьте найденные ошибки")
            logger.info("2. Настройте переменные окружения в SourceCraft")
            logger.info("3. Запустите Deploy в SourceCraft")
            logger.info("4. Получите production URL для демонстрации!")
            
            return True
            
        except KeyboardInterrupt:
            logger.info("\n⏹️ Тестирование прервано пользователем")
            return False
        except Exception as e:
            logger.error(f"❌ Критическая ошибка тестирования: {e}")
            return False
        finally:
            self.cleanup()


async def main():
    """Главная функция"""
    runner = TestRunner()
    success = await runner.run_all_tests()
    
    if success:
        logger.info("✅ Тестирование завершено успешно!")
        sys.exit(0)
    else:
        logger.error("❌ Тестирование завершено с ошибками!")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
