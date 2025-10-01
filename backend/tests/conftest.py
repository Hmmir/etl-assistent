"""
Pytest configuration and fixtures
"""

import pytest
import sys
from pathlib import Path

# Добавляем parent directory в Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Импорты для фикстур
from services.data_service import DataAnalyzer
from services.generator_service import PythonETLGenerator
from services.airflow_service import AirflowDAGGenerator
from services.streaming_service import KafkaStreamingGenerator
from ml.storage_selector import StorageSelector
from ml.data_profiler import DataProfiler
from ml.code_generator import AICodeGenerator


@pytest.fixture
def sample_csv_content():
    """Пример CSV контента"""
    return """id,name,age,salary
1,John Doe,30,50000
2,Jane Smith,25,60000
3,Bob Johnson,35,70000"""


@pytest.fixture
def sample_json_content():
    """Пример JSON контента"""
    return '''[
    {"id": 1, "name": "John Doe", "age": 30},
    {"id": 2, "name": "Jane Smith", "age": 25}
]'''


@pytest.fixture
def sample_xml_content():
    """Пример XML контента"""
    return '''<?xml version="1.0"?>
<data>
    <record>
        <id>1</id>
        <name>John Doe</name>
        <age>30</age>
    </record>
</data>'''


@pytest.fixture
def sample_analysis():
    """Пример результата анализа данных"""
    return {
        'filename': 'test_data.csv',
        'format_type': 'CSV',
        'total_rows': 1000,
        'total_columns': 5,
        'total_records': 1000,
        'total_fields': 5,
        'file_size_mb': 0.5,
        'estimated_data_volume': 'small',
        'columns': [
            {'name': 'id', 'data_type': 'integer'},
            {'name': 'name', 'data_type': 'text'},
            {'name': 'created_at', 'data_type': 'datetime'},
            {'name': 'is_active', 'data_type': 'boolean'},
            {'name': 'amount', 'data_type': 'decimal'}
        ],
        'fields': [
            {'name': 'id', 'data_type': 'integer'},
            {'name': 'name', 'data_type': 'text'},
            {'name': 'created_at', 'data_type': 'datetime'},
            {'name': 'is_active', 'data_type': 'boolean'},
            {'name': 'amount', 'data_type': 'decimal'}
        ]
    }


@pytest.fixture
def storage_recommendation():
    """Пример рекомендации хранилища"""
    return {
        'storage': 'PostgreSQL',
        'recommended_storage': 'PostgreSQL',
        'reason': 'Optimal for structured data',
        'reasoning': 'Optimal for structured data',
        'optimization_tips': ['Add indexes', 'Use partitioning'],
        'optimization_suggestions': ['Add indexes', 'Use partitioning'],
        'ddl_script': 'CREATE TABLE test (...)'
    }
