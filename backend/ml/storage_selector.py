"""
Storage Selector - Intelligent storage recommendation engine

Uses both rule-based logic and LLM to recommend optimal storage.
"""

from typing import Dict, Any, Optional
import logging
from .openrouter_client import get_llm_client

logger = logging.getLogger(__name__)


class StorageSelector:
    """
    Intelligent storage selection combining rules and LLM
    """
    
    def __init__(self, use_llm: bool = True):
        """
        Initialize storage selector
        
        Args:
            use_llm: Whether to use LLM for recommendations (default: True)
        """
        self.use_llm = use_llm
        try:
            self.llm_client = get_llm_client() if use_llm else None
            if self.llm_client:
                logger.info(f"LLM client initialized: model={self.llm_client.model}")
        except Exception as e:
            logger.warning(f"LLM client initialization failed: {e}. Using rule-based only.")
            self.llm_client = None
            self.use_llm = False
    
    def recommend_storage(
        self,
        df_sample: Dict[str, Any],
        profile: Dict[str, Any],
        row_count: int,
        file_size_mb: float
    ) -> Dict[str, Any]:
        """
        Recommend optimal storage based on data characteristics
        
        Args:
            df_sample: Sample of data
            profile: Data profile from DataProfiler
            row_count: Total row count
            file_size_mb: File size in MB
            
        Returns:
            Storage recommendation with reasoning
        """
        # Get rule-based recommendation
        rule_based = self._rule_based_recommendation(profile, row_count, file_size_mb)
        
        # If LLM is enabled, get AI recommendation
        if self.use_llm and self.llm_client:
            try:
                logger.info("Calling LLM for storage recommendation...")
                columns_info = profile.get("column_profiles", {})
                llm_recommendation = self.llm_client.analyze_data_structure(
                    df_sample=df_sample,
                    columns_info=columns_info,
                    row_count=row_count,
                    file_size_mb=file_size_mb
                )
                
                logger.info(f"LLM recommendation received: {llm_recommendation.get('recommended_storage')}")
                # Combine both recommendations
                return self._combine_recommendations(rule_based, llm_recommendation)
                
            except Exception as e:
                logger.error(f"LLM recommendation failed: {e}")
                # Fallback to rule-based
                rule_based["note"] = "LLM analysis unavailable, using rule-based recommendation"
                return rule_based
        else:
            logger.info(f"LLM disabled or unavailable. use_llm={self.use_llm}, llm_client={self.llm_client}")
            return rule_based
    
    def _rule_based_recommendation(
        self,
        profile: Dict[str, Any],
        row_count: int,
        file_size_mb: float
    ) -> Dict[str, Any]:
        """
        Rule-based storage recommendation (fallback)
        """
        patterns = profile.get("patterns_detected", {})
        basic_stats = profile.get("basic_stats", {})
        
        # Decision tree
        
        # Rule 1: Large datasets > 10GB → HDFS
        if file_size_mb > 10240:  # 10GB
            return {
                "recommended_storage": "HDFS",
                "confidence": 0.9,
                "reasoning": f"Большой объем данных ({file_size_mb/1024:.1f}GB) оптимален для HDFS. "
                           "HDFS предназначен для хранения больших объемов сырых данных.",
                "data_type": "raw_historical",
                "analysis_method": "rule_based"
            }
        
        # Rule 2: Time series with many metrics → ClickHouse
        if (len(patterns.get("time_series", [])) > 0 and 
            len(patterns.get("metrics", [])) > 3):
            return {
                "recommended_storage": "ClickHouse",
                "confidence": 0.85,
                "reasoning": "Обнаружены временные ряды и множество метрик. "
                           "ClickHouse оптимален для аналитики по временным данным.",
                "data_type": "analytical",
                "analysis_method": "rule_based",
                "partitioning_strategy": "date",
                "partitioning_column": patterns["time_series"][0] if patterns["time_series"] else None
            }
        
        # Rule 3: High update frequency, transactional → PostgreSQL
        if len(patterns.get("identifiers", [])) > 0:
            return {
                "recommended_storage": "PostgreSQL",
                "confidence": 0.8,
                "reasoning": "Обнаружены идентификаторы и структурированные данные. "
                           "PostgreSQL оптимален для транзакционных данных с ACID гарантиями.",
                "data_type": "transactional",
                "analysis_method": "rule_based"
            }
        
        # Rule 4: Analytical data with aggregates → ClickHouse
        if len(patterns.get("metrics", [])) > 5:
            return {
                "recommended_storage": "ClickHouse",
                "confidence": 0.75,
                "reasoning": "Множество числовых метрик. "
                           "ClickHouse оптимален для аналитических запросов и агрегаций.",
                "data_type": "analytical",
                "analysis_method": "rule_based"
            }
        
        # Rule 5: Archive data > 1GB → HDFS
        if file_size_mb > 1024:  # 1GB
            return {
                "recommended_storage": "HDFS",
                "confidence": 0.7,
                "reasoning": f"Средний объем данных ({file_size_mb/1024:.1f}GB). "
                           "HDFS подходит для архивного хранения.",
                "data_type": "raw_historical",
                "analysis_method": "rule_based"
            }
        
        # Default: PostgreSQL for small structured data
        return {
            "recommended_storage": "PostgreSQL",
            "confidence": 0.6,
            "reasoning": "Небольшой объем структурированных данных. "
                       "PostgreSQL универсален и подходит для большинства случаев.",
            "data_type": "transactional",
            "analysis_method": "rule_based"
        }
    
    def _combine_recommendations(
        self,
        rule_based: Dict[str, Any],
        llm_based: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Combine rule-based and LLM recommendations
        """
        # If both agree - high confidence
        if rule_based["recommended_storage"] == llm_based.get("recommended_storage"):
            return {
                **llm_based,
                "confidence": 0.95,
                "rule_based_agreement": True,
                "combined_reasoning": (
                    f"[LLM Analysis] {llm_based.get('reasoning', '')}\n\n"
                    f"[Rule-Based Analysis] {rule_based['reasoning']}\n\n"
                    f"Оба метода рекомендуют {llm_based.get('recommended_storage')}."
                )
            }
        
        # If disagree - prefer LLM but lower confidence
        else:
            return {
                **llm_based,
                "confidence": 0.75,
                "rule_based_agreement": False,
                "alternative_storage": rule_based["recommended_storage"],
                "combined_reasoning": (
                    f"[LLM Analysis] {llm_based.get('reasoning', '')}\n\n"
                    f"[Alternative] Rule-based анализ предлагает {rule_based['recommended_storage']}: "
                    f"{rule_based['reasoning']}"
                )
            }
    
    def get_storage_config(self, storage_type: str) -> Dict[str, Any]:
        """
        Get configuration details for storage type
        
        Args:
            storage_type: Storage type (PostgreSQL/ClickHouse/HDFS)
            
        Returns:
            Configuration details
        """
        configs = {
            "PostgreSQL": {
                "engine": "PostgreSQL 15",
                "use_cases": [
                    "Транзакционные данные (OLTP)",
                    "Нормализованные таблицы",
                    "Данные с частыми обновлениями",
                    "ACID гарантии"
                ],
                "optimization_tips": [
                    "Используйте индексы для часто запрашиваемых колонок",
                    "Партицирование для больших таблиц",
                    "Vacuum для очистки",
                    "Connection pooling для производительности"
                ],
                "connection_info": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "etl_db"
                }
            },
            "ClickHouse": {
                "engine": "ClickHouse MergeTree",
                "use_cases": [
                    "Аналитические запросы (OLAP)",
                    "Временные ряды",
                    "Логи и события",
                    "Большие объемы данных для агрегации"
                ],
                "optimization_tips": [
                    "Партицирование по дате для временных данных",
                    "ORDER BY для часто фильтруемых колонок",
                    "Сжатие данных (CODEC)",
                    "Материализованные представления для агрегатов"
                ],
                "connection_info": {
                    "host": "localhost",
                    "port": 8123,
                    "database": "etl_analytics"
                }
            },
            "HDFS": {
                "engine": "Hadoop HDFS 3.2",
                "use_cases": [
                    "Большие объемы сырых данных (>10GB)",
                    "Архивное хранение",
                    "Data Lake",
                    "Резервное копирование"
                ],
                "optimization_tips": [
                    "Используйте Parquet или ORC форматы для сжатия",
                    "Партицирование по датам в путях",
                    "Настройте репликацию (default: 3)",
                    "Используйте DistCP для больших копирований"
                ],
                "connection_info": {
                    "namenode": "localhost:9000",
                    "webhdfs": "http://localhost:9870"
                }
            }
        }
        
        return configs.get(storage_type, {})
