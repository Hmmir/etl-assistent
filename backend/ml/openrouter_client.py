"""
OpenRouter LLM Client

This module provides integration with OpenRouter API for accessing
various LLM models (Claude, GPT-4, Llama, etc.)

OpenRouter: https://openrouter.ai/
"""

import os
import json
import requests
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class OpenRouterClient:
    """
    Client for OpenRouter API
    
    Provides access to multiple LLM models through a single API.
    Supports Claude, GPT-4, Llama, Mistral, and more.
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None
    ):
        """
        Initialize OpenRouter client
        
        Args:
            api_key: OpenRouter API key (defaults to env variable)
            model: Model to use (defaults to Claude 3.5 Sonnet)
        """
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        # Используем бесплатную модель вместо платной Claude
        self.model = model or os.getenv(
            "OPENROUTER_MODEL", 
            "meta-llama/llama-3.2-3b-instruct:free"
        )
        self.base_url = "https://openrouter.ai/api/v1"
        
        if not self.api_key:
            raise ValueError(
                "OpenRouter API key not found. "
                "Set OPENROUTER_API_KEY environment variable."
            )
    
    def _make_request(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 4000
    ) -> Dict[str, Any]:
        """
        Make API request to OpenRouter
        
        Args:
            messages: Chat messages
            temperature: Sampling temperature (0.0 to 2.0)
            max_tokens: Maximum tokens to generate
            
        Returns:
            API response
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "HTTP-Referer": "https://github.com/etl-assistant",
            "X-Title": "ETL Assistant",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=60
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"OpenRouter API error: {str(e)}")
    
    def analyze_data_structure(
        self,
        df_sample: Dict[str, Any],
        columns_info: Dict[str, Dict[str, Any]],
        row_count: int,
        file_size_mb: float
    ) -> Dict[str, Any]:
        """
        Analyze data structure using LLM
        
        Args:
            df_sample: Sample of dataframe (first 10 rows as dict)
            columns_info: Column metadata (types, nulls, unique values)
            row_count: Total number of rows
            file_size_mb: File size in MB
            
        Returns:
            AI analysis with recommendations
        """
        prompt = f"""Ты - эксперт по data engineering и архитектуре данных.
Проанализируй структуру данных и дай рекомендации.

ДАННЫЕ:
- Количество строк: {row_count:,}
- Размер файла: {file_size_mb:.2f} MB
- Количество столбцов: {len(columns_info)}

СТОЛБЦЫ:
{json.dumps(columns_info, indent=2, ensure_ascii=False)}

ПРИМЕРЫ ДАННЫХ (первые строки):
{json.dumps(df_sample, indent=2, ensure_ascii=False)}

ЗАДАЧИ:
1. Определи тип данных (выбери ОДИН):
   - "transactional" - транзакционные операции (заказы, платежи)
   - "analytical" - аналитические данные (агрегаты, метрики)
   - "time_series" - временные ряды (логи, события)
   - "master_data" - справочные данные (пользователи, продукты)
   - "raw_historical" - сырые исторические данные большого объема

2. Предложи оптимальное хранилище (выбери ОДИН):
   - "PostgreSQL" - для транзакций, нормализованных данных, ACID
   - "ClickHouse" - для аналитики, агрегатов, временных рядов
   - "HDFS" - для больших объемов сырых данных (>100GB), архивов

3. Рекомендуй стратегию партицирования:
   - Если есть дата/время - по дате
   - Если есть категории - по категории
   - Если нет - "none"

4. Предложи индексы для оптимизации:
   - Primary key колонки
   - Часто используемые для фильтрации
   - Join колонки

5. Оцени прогноз роста:
   - Малый: <1GB в месяц
   - Средний: 1-10GB в месяц  
   - Большой: >10GB в месяц

6. Предложи расписание обновления:
   - "real_time" - для streaming данных
   - "hourly" - каждый час
   - "daily" - раз в день
   - "weekly" - раз в неделю

ОТВЕТ В JSON (БЕЗ КОММЕНТАРИЕВ):
{{
  "data_type": "...",
  "recommended_storage": "...",
  "reasoning": "Подробное обоснование выбора на русском языке",
  "partitioning_strategy": "...",
  "partitioning_column": "название_колонки или null",
  "indexes": ["колонка1", "колонка2"],
  "growth_estimation": "...",
  "update_schedule": "...",
  "additional_recommendations": [
    "Рекомендация 1",
    "Рекомендация 2"
  ]
}}"""

        messages = [
            {
                "role": "system",
                "content": "Ты эксперт по data engineering, архитектуре данных и выбору систем хранения. Отвечай только в формате JSON без дополнительного текста."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        response = self._make_request(messages, temperature=0.3)
        
        # Extract content
        content = response["choices"][0]["message"]["content"]
        
        # Parse JSON from response
        try:
            # Try to extract JSON if wrapped in markdown code blocks
            if "```json" in content:
                json_str = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                json_str = content.split("```")[1].split("```")[0].strip()
            else:
                json_str = content.strip()
            
            result = json.loads(json_str)
            
            # Add metadata
            result["llm_model"] = self.model
            result["analysis_method"] = "LLM"
            
            return result
            
        except json.JSONDecodeError as e:
            # Fallback: return raw content with error
            return {
                "data_type": "unknown",
                "recommended_storage": "PostgreSQL",
                "reasoning": f"LLM анализ не удался, используется fallback. Ошибка: {str(e)}",
                "partitioning_strategy": "none",
                "partitioning_column": None,
                "indexes": [],
                "growth_estimation": "unknown",
                "update_schedule": "daily",
                "additional_recommendations": [],
                "llm_model": self.model,
                "analysis_method": "LLM_FALLBACK",
                "raw_response": content
            }
    
    def generate_sql_ddl(
        self,
        table_name: str,
        columns_info: Dict[str, Dict[str, Any]],
        storage_type: str,
        partitioning: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Generate SQL DDL using LLM
        
        Args:
            table_name: Name for the table
            columns_info: Column metadata
            storage_type: Target storage (PostgreSQL/ClickHouse/HDFS)
            partitioning: Partitioning configuration
            
        Returns:
            Generated SQL DDL
        """
        prompt = f"""Сгенерируй SQL DDL для создания таблицы.

ПАРАМЕТРЫ:
- Название таблицы: {table_name}
- Целевая БД: {storage_type}
- Партицирование: {json.dumps(partitioning, ensure_ascii=False) if partitioning else "Нет"}

СТОЛБЦЫ:
{json.dumps(columns_info, indent=2, ensure_ascii=False)}

ТРЕБОВАНИЯ:
1. Используй синтаксис для {storage_type}
2. Добавь подходящие типы данных
3. Добавь PRIMARY KEY
4. Добавь индексы для часто используемых колонок
5. Если указано партицирование - добавь PARTITION BY
6. Для ClickHouse используй ENGINE = MergeTree
7. Для PostgreSQL используй стандартный синтаксис

ВЕРНИ ТОЛЬКО SQL КОД БЕЗ КОММЕНТАРИЕВ И ОБЪЯСНЕНИЙ."""

        messages = [
            {
                "role": "system",
                "content": f"Ты эксперт по SQL и {storage_type}. Генерируешь только корректный SQL код."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        response = self._make_request(messages, temperature=0.1)
        content = response["choices"][0]["message"]["content"]
        
        # Extract SQL from markdown code blocks if present
        if "```sql" in content:
            sql = content.split("```sql")[1].split("```")[0].strip()
        elif "```" in content:
            sql = content.split("```")[1].split("```")[0].strip()
        else:
            sql = content.strip()
        
        return sql
    
    def explain_pipeline_logic(
        self,
        source_type: str,
        target_storage: str,
        transformations: List[str]
    ) -> str:
        """
        Generate human-readable explanation of ETL pipeline
        
        Args:
            source_type: Source data type
            target_storage: Target storage
            transformations: List of transformations
            
        Returns:
            Human-readable explanation
        """
        prompt = f"""Объясни простым языком, что делает этот ETL пайплайн.

КОНФИГУРАЦИЯ:
- Источник: {source_type}
- Целевое хранилище: {target_storage}
- Трансформации: {', '.join(transformations) if transformations else 'нет'}

Объясни:
1. Откуда берутся данные
2. Куда они попадут
3. Зачем выбрано именно это хранилище
4. Какие преобразования выполняются
5. Как часто будет обновляться

Ответ должен быть понятен даже человеку без технических знаний.
Используй 2-3 предложения."""

        messages = [
            {
                "role": "system",
                "content": "Ты эксперт, объясняющий сложные технические концепции простым языком."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        response = self._make_request(messages, temperature=0.7)
        return response["choices"][0]["message"]["content"].strip()


# Singleton instance
_client_instance = None


def get_llm_client() -> OpenRouterClient:
    """Get singleton instance of OpenRouter client"""
    global _client_instance
    if _client_instance is None:
        _client_instance = OpenRouterClient()
    return _client_instance
