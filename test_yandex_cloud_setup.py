#!/usr/bin/env python3
"""
Тест настройки Yandex Cloud и YandexGPT
Проверка окружения для модуля 1

Автоматически сгенерирован ETL Assistant
Дата: 2025-09-25

Проверяет:
- Переменные окружения YandexGPT
- Yandex Cloud CLI
- Доступность API
"""

import os
import subprocess
import logging
import asyncio
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YandexCloudTester:
    def __init__(self):
        self.tests_passed = 0
        self.tests_total = 0
        
    def log_test(self, test_name: str, success: bool, details: str = ""):
        """Логирование результатов тестов"""
        self.tests_total += 1
        if success:
            self.tests_passed += 1
            
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"{status} {test_name}: {details}")
        
    def test_environment_variables(self):
        """Проверка переменных окружения"""
        logger.info("🔍 ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ")
        logger.info("-" * 40)
        
        # Обязательные переменные
        required_vars = {
            'YANDEX_CLOUD_FOLDER_ID': 'Folder ID в Yandex Cloud',
            'YANDEX_GPT_API_KEY': 'API ключ для YandexGPT',
            'YANDEX_CLOUD_IAM_TOKEN': 'IAM токен (альтернатива API ключу)'
        }
        
        folder_id = os.getenv('YANDEX_CLOUD_FOLDER_ID')
        api_key = os.getenv('YANDEX_GPT_API_KEY')
        iam_token = os.getenv('YANDEX_CLOUD_IAM_TOKEN')
        
        # Проверяем FOLDER_ID (обязательно)
        if folder_id:
            self.log_test(
                "YANDEX_CLOUD_FOLDER_ID",
                True,
                f"Установлен: {folder_id[:8]}..."
            )
        else:
            self.log_test(
                "YANDEX_CLOUD_FOLDER_ID",
                False,
                "НЕ УСТАНОВЛЕН (обязательно)"
            )
        
        # Проверяем авторизацию (нужен хотя бы один)
        if api_key:
            self.log_test(
                "YANDEX_GPT_API_KEY",
                True,
                f"Установлен: {api_key[:8]}..."
            )
        else:
            self.log_test(
                "YANDEX_GPT_API_KEY",
                False,
                "НЕ УСТАНОВЛЕН"
            )
            
        if iam_token:
            self.log_test(
                "YANDEX_CLOUD_IAM_TOKEN",
                True,
                f"Установлен: {iam_token[:8]}..."
            )
        else:
            self.log_test(
                "YANDEX_CLOUD_IAM_TOKEN",
                False,
                "НЕ УСТАНОВЛЕН"
            )
            
        # Общая проверка конфигурации
        auth_configured = bool(api_key or iam_token)
        fully_configured = bool(folder_id and auth_configured)
        
        self.log_test(
            "YandexGPT Configuration",
            fully_configured,
            "Готов к использованию" if fully_configured else "Нужна настройка"
        )
        
        return fully_configured
        
    def test_yandex_cloud_cli(self):
        """Проверка Yandex Cloud CLI"""
        logger.info("\n🛠️ ПРОВЕРКА YANDEX CLOUD CLI")
        logger.info("-" * 40)
        
        try:
            # Проверяем установку YC CLI
            result = subprocess.run(
                ['yc', '--version'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                version = result.stdout.strip()
                self.log_test(
                    "YC CLI Installation",
                    True,
                    f"Установлен: {version}"
                )
                
                # Проверяем конфигурацию
                return self.test_yc_config()
            else:
                self.log_test(
                    "YC CLI Installation",
                    False,
                    "YC CLI не установлен"
                )
                return False
                
        except FileNotFoundError:
            self.log_test(
                "YC CLI Installation",
                False,
                "YC CLI не найден в PATH"
            )
            return False
        except subprocess.TimeoutExpired:
            self.log_test(
                "YC CLI Installation",
                False,
                "Timeout при проверке YC CLI"
            )
            return False
        except Exception as e:
            self.log_test(
                "YC CLI Installation",
                False,
                f"Ошибка: {e}"
            )
            return False
            
    def test_yc_config(self):
        """Проверка конфигурации YC CLI"""
        try:
            # Проверяем профиль
            result = subprocess.run(
                ['yc', 'config', 'list'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                config = result.stdout.strip()
                has_token = 'token:' in config
                has_folder = 'folder-id:' in config
                
                self.log_test(
                    "YC CLI Configuration",
                    has_token or has_folder,
                    f"Token: {has_token}, Folder: {has_folder}"
                )
                
                if has_folder:
                    # Извлекаем folder-id из конфига
                    for line in config.split('\n'):
                        if 'folder-id:' in line:
                            folder_id = line.split(':', 1)[1].strip()
                            logger.info(f"📁 Folder ID из YC CLI: {folder_id}")
                            break
                            
                return has_token or has_folder
            else:
                self.log_test(
                    "YC CLI Configuration",
                    False,
                    "Нет активного профиля"
                )
                return False
                
        except Exception as e:
            self.log_test(
                "YC CLI Configuration",
                False,
                f"Ошибка: {e}"
            )
            return False
    
    async def test_yandexgpt_api_access(self):
        """Тестирование доступа к YandexGPT API"""
        logger.info("\n🤖 ТЕСТ YANDEXGPT API")
        logger.info("-" * 40)
        
        folder_id = os.getenv('YANDEX_CLOUD_FOLDER_ID')
        api_key = os.getenv('YANDEX_GPT_API_KEY')
        iam_token = os.getenv('YANDEX_CLOUD_IAM_TOKEN')
        
        if not folder_id:
            self.log_test(
                "YandexGPT API Access",
                False,
                "Нет YANDEX_CLOUD_FOLDER_ID"
            )
            return False
            
        if not (api_key or iam_token):
            self.log_test(
                "YandexGPT API Access",
                False,
                "Нет API_KEY или IAM_TOKEN"
            )
            return False
            
        # Формируем запрос
        url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        if iam_token:
            headers["Authorization"] = f"Bearer {iam_token}"
        elif api_key:
            headers["Authorization"] = f"Api-Key {api_key}"
            
        payload = {
            "modelUri": f"gpt://{folder_id}/yandexgpt-lite",
            "completionOptions": {
                "stream": False,
                "temperature": 0.1,
                "maxTokens": 50
            },
            "messages": [
                {
                    "role": "system",
                    "text": "Ты тестируешь подключение. Отвечай кратко."
                },
                {
                    "role": "user",
                    "text": "Привет! Работает ли YandexGPT?"
                }
            ]
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=30
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        alternatives = result.get("result", {}).get("alternatives", [])
                        
                        if alternatives:
                            text = alternatives[0].get("message", {}).get("text", "")
                            self.log_test(
                                "YandexGPT API Access",
                                True,
                                f"Ответ получен ({len(text)} символов)"
                            )
                            logger.info(f"📝 Ответ YandexGPT: {text}")
                            return True
                        else:
                            self.log_test(
                                "YandexGPT API Access",
                                False,
                                "Пустой ответ от API"
                            )
                            return False
                    else:
                        error_text = await response.text()
                        self.log_test(
                            "YandexGPT API Access",
                            False,
                            f"HTTP {response.status}: {error_text[:100]}"
                        )
                        return False
                        
        except asyncio.TimeoutError:
            self.log_test(
                "YandexGPT API Access",
                False,
                "Timeout при запросе к API"
            )
            return False
        except Exception as e:
            self.log_test(
                "YandexGPT API Access",
                False,
                f"Ошибка: {str(e)[:100]}"
            )
            return False
    
    async def run_all_tests(self):
        """Запуск всех тестов"""
        logger.info("🚀 ТЕСТИРОВАНИЕ YANDEX CLOUD SETUP")
        logger.info("=" * 50)
        
        # Тест переменных окружения
        env_ok = self.test_environment_variables()
        
        # Тест YC CLI
        cli_ok = self.test_yandex_cloud_cli()
        
        # Тест API доступа
        if env_ok:
            api_ok = await self.test_yandexgpt_api_access()
        else:
            api_ok = False
            logger.info("\n⏭️ Пропускаем тест API (нет конфигурации)")
            
        # Итоги
        logger.info("\n" + "=" * 50)
        logger.info("📊 ИТОГИ ТЕСТИРОВАНИЯ")
        logger.info(f"✅ Пройдено: {self.tests_passed}/{self.tests_total}")
        logger.info(f"❌ Провалено: {self.tests_total - self.tests_passed}/{self.tests_total}")
        logger.info(f"📈 Успешность: {(self.tests_passed/self.tests_total)*100:.1f}%")
        
        # Рекомендации
        if self.tests_passed == self.tests_total:
            logger.info("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! YANDEX CLOUD НАСТРОЕН КОРРЕКТНО!")
        elif env_ok and api_ok:
            logger.info("\n✅ ОСНОВНАЯ КОНФИГУРАЦИЯ РАБОТАЕТ. YC CLI опционален.")
        elif env_ok:
            logger.info("\n⚠️ ПЕРЕМЕННЫЕ НАСТРОЕНЫ, НО ЕСТЬ ПРОБЛЕМЫ С API ДОСТУПОМ.")
        else:
            logger.info("\n❌ НУЖНА НАСТРОЙКА YANDEX CLOUD КОНФИГУРАЦИИ.")
            
        await self.print_setup_instructions()
        
    async def print_setup_instructions(self):
        """Инструкции по настройке"""
        logger.info("\n📋 ИНСТРУКЦИИ ПО НАСТРОЙКЕ:")
        logger.info("-" * 30)
        
        if not os.getenv('YANDEX_CLOUD_FOLDER_ID'):
            logger.info("1. Создайте Folder в Yandex Cloud:")
            logger.info("   https://console.cloud.yandex.ru/folders")
            logger.info("   Установите: export YANDEX_CLOUD_FOLDER_ID=b1g...")
            
        if not (os.getenv('YANDEX_GPT_API_KEY') or os.getenv('YANDEX_CLOUD_IAM_TOKEN')):
            logger.info("2. Получите API ключ:")
            logger.info("   https://console.cloud.yandex.ru/iam/api-keys")
            logger.info("   Установите: export YANDEX_GPT_API_KEY=AQV...")
            
        logger.info("3. Проверьте настройку: python test_yandex_cloud_setup.py")
        logger.info("4. Добавьте переменные в SourceCraft Environment Variables")


async def main():
    """Главная функция"""
    try:
        tester = YandexCloudTester()
        await tester.run_all_tests()
    except KeyboardInterrupt:
        logger.info("\n⏹️ Тестирование прервано")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    asyncio.run(main())
