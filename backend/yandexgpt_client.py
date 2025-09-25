#!/usr/bin/env python3
"""
YandexGPT API Client для ETL Assistant
Интеграция с Yandex Foundation Models API

Автоматически сгенерирован ETL Assistant
Дата: 2025-09-25

Функциональность:
- Анализ структуры данных с помощью ИИ
- Генерация рекомендаций по хранению
- Создание описаний для полей данных
- Оптимизация ETL процессов
"""

import os
import json
import asyncio
import logging
from typing import Dict, List, Any, Optional
import aiohttp
from datetime import datetime

logger = logging.getLogger(__name__)

class YandexGPTClient:
    """
    Клиент для работы с YandexGPT API
    """
    
    def __init__(self):
        self.folder_id = os.getenv('YANDEX_CLOUD_FOLDER_ID')
        self.api_key = os.getenv('YANDEX_GPT_API_KEY')
        self.iam_token = os.getenv('YANDEX_CLOUD_IAM_TOKEN')
        
        # URLs для YandexGPT API
        self.base_url = "https://llm.api.cloud.yandex.net"
        self.completion_url = f"{self.base_url}/foundationModels/v1/completion"
        
        # Модель YandexGPT
        self.model_uri = f"gpt://{self.folder_id}/yandexgpt-lite"
        
        self.session = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    def _get_headers(self) -> Dict[str, str]:
        """Получение заголовков для API запросов"""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Приоритет: IAM token > API key
        if self.iam_token:
            headers["Authorization"] = f"Bearer {self.iam_token}"
        elif self.api_key:
            headers["Authorization"] = f"Api-Key {self.api_key}"
        else:
            logger.warning("⚠️ Нет токена авторизации для YandexGPT")
            
        return headers
    
    async def _make_request(self, prompt: str, max_tokens: int = 1000) -> Optional[str]:
        """
        Выполнение запроса к YandexGPT API
        
        Args:
            prompt: Промпт для модели
            max_tokens: Максимальное количество токенов в ответе
            
        Returns:
            str: Ответ от модели или None при ошибке
        """
        if not self.session:
            logger.error("❌ Session не инициализирована")
            return None
            
        if not self.folder_id:
            logger.error("❌ YANDEX_CLOUD_FOLDER_ID не настроен")
            return None
            
        headers = self._get_headers()
        
        payload = {
            "modelUri": self.model_uri,
            "completionOptions": {
                "stream": False,
                "temperature": 0.3,
                "maxTokens": max_tokens
            },
            "messages": [
                {
                    "role": "system",
                    "text": "Ты - эксперт по анализу данных и ETL процессам. Отвечай на русском языке, кратко и по делу."
                },
                {
                    "role": "user", 
                    "text": prompt
                }
            ]
        }
        
        try:
            async with self.session.post(
                self.completion_url,
                headers=headers,
                json=payload,
                timeout=30
            ) as response:
                
                if response.status == 200:
                    result = await response.json()
                    
                    # Извлекаем текст из ответа
                    alternatives = result.get("result", {}).get("alternatives", [])
                    if alternatives:
                        return alternatives[0].get("message", {}).get("text", "")
                    
                else:
                    error_text = await response.text()
                    logger.error(f"❌ YandexGPT API Error {response.status}: {error_text}")
                    
        except asyncio.TimeoutError:
            logger.error("⏰ Timeout при запросе к YandexGPT API")
        except Exception as e:
            logger.error(f"❌ Ошибка запроса к YandexGPT: {e}")
            
        return None
    
    async def analyze_data_structure(self, data_sample: Dict[str, Any], 
                                   file_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        ИИ-анализ структуры данных
        
        Args:
            data_sample: Образец данных
            file_info: Информация о файле
            
        Returns:
            dict: Расширенный анализ с ИИ-рекомендациями
        """
        columns_info = []
        for col, sample_vals in data_sample.items():
            columns_info.append(f"- {col}: {sample_vals[:3] if isinstance(sample_vals, list) else sample_vals}")
        
        columns_text = '\n'.join(columns_info[:10])  # Ограничиваем для промпта
        
        prompt = f"""
Проанализируй структуру данных и дай рекомендации:

ФАЙЛ: {file_info.get('filename', 'unknown')}
РАЗМЕР: {file_info.get('size_mb', 0)} MB
СТРОК: {file_info.get('rows', 0)}

СТОЛБЦЫ И ОБРАЗЦЫ ДАННЫХ:
{columns_text}

Дай краткий анализ:
1. Тип данных (транзакционные, аналитические, логи, etc)
2. Рекомендуемое хранилище (ClickHouse/PostgreSQL/HDFS)
3. Ключевые поля для индексации
4. Предложения по партицированию
5. Возможные преобразования данных

Ответь в формате JSON без лишнего текста.
"""
        
        try:
            response = await self._make_request(prompt, max_tokens=800)
            
            if response:
                # Пытаемся распарсить JSON ответ
                try:
                    ai_analysis = json.loads(response)
                    logger.info("✅ YandexGPT анализ получен")
                    return {
                        "ai_analysis": ai_analysis,
                        "timestamp": datetime.now().isoformat(),
                        "model_used": "YandexGPT",
                        "raw_response": response
                    }
                except json.JSONDecodeError:
                    logger.warning("⚠️ YandexGPT ответ не в JSON формате")
                    return {
                        "ai_analysis": {"text_analysis": response},
                        "timestamp": datetime.now().isoformat(),
                        "model_used": "YandexGPT",
                        "raw_response": response
                    }
            else:
                logger.warning("⚠️ Нет ответа от YandexGPT")
                
        except Exception as e:
            logger.error(f"❌ Ошибка анализа YandexGPT: {e}")
            
        return {
            "ai_analysis": {"error": "YandexGPT недоступен"},
            "fallback_analysis": self._fallback_analysis(data_sample, file_info),
            "timestamp": datetime.now().isoformat()
        }
    
    def _fallback_analysis(self, data_sample: Dict[str, Any], 
                          file_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Резервный анализ без ИИ
        """
        size_mb = file_info.get('size_mb', 0)
        rows = file_info.get('rows', 0)
        
        # Простая логика выбора хранилища
        if size_mb > 100:
            storage = "ClickHouse"
            reason = "Большой объем данных"
        elif rows > 1000000:
            storage = "ClickHouse"
            reason = "Много строк - нужна колонночная БД"
        else:
            storage = "PostgreSQL"
            reason = "Средний объем - подойдет PostgreSQL"
            
        return {
            "data_type": "unknown",
            "recommended_storage": storage,
            "reason": reason,
            "indexing_fields": list(data_sample.keys())[:3],
            "partitioning": "by_date" if any("date" in col.lower() for col in data_sample.keys()) else "none",
            "transformations": ["data_cleaning", "type_conversion"]
        }
    
    async def generate_field_descriptions(self, fields: List[str]) -> Dict[str, str]:
        """
        Генерация описаний полей данных с помощью ИИ
        
        Args:
            fields: Список названий полей
            
        Returns:
            dict: Поле -> Описание
        """
        fields_text = '\n'.join([f"- {field}" for field in fields[:20]])
        
        prompt = f"""
Проанализируй названия полей и дай краткие описания на русском языке:

ПОЛЯ:
{fields_text}

Верни JSON объект вида:
{{"field_name": "краткое описание"}}

Только JSON без дополнительного текста.
"""
        
        try:
            response = await self._make_request(prompt, max_tokens=1000)
            
            if response:
                try:
                    descriptions = json.loads(response)
                    return descriptions
                except json.JSONDecodeError:
                    pass
                    
        except Exception as e:
            logger.error(f"❌ Ошибка генерации описаний: {e}")
        
        # Fallback описания
        return {field: f"Поле {field}" for field in fields}
    
    async def optimize_etl_process(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        ИИ-оптимизация ETL процесса
        
        Args:
            analysis: Результаты анализа данных
            
        Returns:
            dict: Рекомендации по оптимизации
        """
        rows = analysis.get('data_info', {}).get('rows', 0)
        size_mb = analysis.get('data_info', {}).get('size_mb', 0)
        
        prompt = f"""
Оптимизируй ETL процесс для:
- Строк: {rows}
- Размер: {size_mb} MB
- Хранилище: {analysis.get('recommended_storage', 'unknown')}

Дай рекомендации:
1. Размер батча для обработки
2. Количество параллельных потоков
3. Стратегия индексирования
4. Оптимизация памяти

Ответ в JSON формате.
"""
        
        try:
            response = await self._make_request(prompt, max_tokens=500)
            
            if response:
                try:
                    return json.loads(response)
                except json.JSONDecodeError:
                    pass
                    
        except Exception as e:
            logger.error(f"❌ Ошибка оптимизации: {e}")
        
        # Fallback оптимизация
        batch_size = min(10000, max(1000, rows // 100))
        threads = min(4, max(1, size_mb // 50))
        
        return {
            "batch_size": batch_size,
            "parallel_threads": threads,
            "memory_optimization": "process_in_chunks",
            "indexing_strategy": "create_after_load"
        }

    def is_configured(self) -> bool:
        """Проверка настройки YandexGPT"""
        return bool(self.folder_id and (self.api_key or self.iam_token))
    
    def get_status(self) -> Dict[str, Any]:
        """Статус подключения к YandexGPT"""
        return {
            "configured": self.is_configured(),
            "folder_id_set": bool(self.folder_id),
            "api_key_set": bool(self.api_key),
            "iam_token_set": bool(self.iam_token),
            "model_uri": self.model_uri if self.folder_id else None
        }


# Глобальный экземпляр клиента
yandex_gpt_client = YandexGPTClient()
