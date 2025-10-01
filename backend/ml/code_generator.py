"""
AI Code Generator - Generate production-ready ETL code using LLM

Generates SQL, Python, and Airflow DAG code with best practices.
"""

from typing import Dict, Any, Optional
from .openrouter_client import get_llm_client


class AICodeGenerator:
    """
    Generate ETL code using LLM
    """
    
    def __init__(self):
        self.llm_client = get_llm_client()
    
    def generate_ddl(
        self,
        table_name: str,
        columns_info: Dict[str, Dict[str, Any]],
        storage_recommendation: Dict[str, Any]
    ) -> str:
        """
        Generate SQL DDL with LLM assistance
        
        Args:
            table_name: Table name
            columns_info: Column metadata
            storage_recommendation: Storage recommendation from StorageSelector
            
        Returns:
            SQL DDL code
        """
        storage_type = storage_recommendation.get("recommended_storage", "PostgreSQL")
        partitioning = storage_recommendation.get("partitioning_column")
        
        try:
            sql = self.llm_client.generate_sql_ddl(
                table_name=table_name,
                columns_info=columns_info,
                storage_type=storage_type,
                partitioning={
                    "strategy": storage_recommendation.get("partitioning_strategy", "none"),
                    "column": partitioning
                } if partitioning else None
            )
            return sql
        except Exception as e:
            # Fallback to template-based generation
            print(f"LLM DDL generation failed: {e}, using fallback")
            return self._generate_ddl_fallback(table_name, columns_info, storage_type, partitioning)
    
    def _generate_ddl_fallback(
        self,
        table_name: str,
        columns_info: Dict[str, Dict[str, Any]],
        storage_type: str,
        partitioning_column: Optional[str] = None
    ) -> str:
        """Fallback DDL generation using templates"""
        
        # Map pandas types to SQL types
        type_mapping = {
            "int64": "BIGINT",
            "int32": "INTEGER",
            "float64": "DOUBLE PRECISION",
            "object": "TEXT",
            "datetime64[ns]": "TIMESTAMP",
            "bool": "BOOLEAN"
        }
        
        columns = []
        for col_name, col_info in columns_info.items():
            dtype = col_info.get("dtype", "object")
            sql_type = type_mapping.get(dtype, "TEXT")
            nullable = "" if col_info.get("null_count", 0) > 0 else "NOT NULL"
            columns.append(f"    {col_name} {sql_type} {nullable}".strip())
        
        if storage_type == "ClickHouse":
            ddl = f"""-- ClickHouse DDL for {table_name}
CREATE TABLE IF NOT EXISTS {table_name} (
{chr(10).join(columns)}
)
ENGINE = MergeTree()"""
            
            if partitioning_column:
                ddl += f"\nPARTITION BY toYYYYMM({partitioning_column})"
            ddl += f"\nORDER BY tuple();"
            
        else:  # PostgreSQL
            ddl = f"""-- PostgreSQL DDL for {table_name}
CREATE TABLE IF NOT EXISTS {table_name} (
{chr(10).join(columns)}
);"""
            
            if partitioning_column:
                ddl += f"\n-- Consider partitioning by {partitioning_column}"
        
        return ddl
    
    def generate_pipeline_explanation(
        self,
        source_type: str,
        target_storage: str,
        transformations: list
    ) -> str:
        """
        Generate human-readable pipeline explanation
        
        Args:
            source_type: Source data type
            target_storage: Target storage
            transformations: List of transformations
            
        Returns:
            Human-readable explanation
        """
        try:
            return self.llm_client.explain_pipeline_logic(
                source_type=source_type,
                target_storage=target_storage,
                transformations=transformations
            )
        except Exception as e:
            # Fallback explanation
            return (
                f"Данные из источника '{source_type}' будут загружены в {target_storage}. "
                f"Применяются трансформации: {', '.join(transformations) if transformations else 'нет'}. "
                f"Пайплайн будет обновляться по расписанию."
            )
