"""
Tests for Code Generator (ML module)
"""
import pytest
from backend.ml.code_generator import AICodeGenerator


class TestCodeGenerator:
    """Test code generation functionality"""
    
    @pytest.fixture
    def generator(self):
        """Create generator instance"""
        return AICodeGenerator()
    
    @pytest.fixture
    def sample_analysis(self):
        """Sample data analysis"""
        return {
            'filename': 'test_data.csv',
            'format_type': 'CSV',
            'total_rows': 5000,
            'columns': [
                {'name': 'id', 'type': 'INTEGER'},
                {'name': 'name', 'type': 'VARCHAR(255)'},
                {'name': 'value', 'type': 'FLOAT'}
            ]
        }
    
    @pytest.fixture
    def storage_recommendation(self):
        """Sample storage recommendation"""
        return {
            'recommended_storage': 'PostgreSQL',
            'data_type': 'transactional',
            'reasoning': 'Small transactional dataset'
        }
    
    def test_generate_python_etl(self, generator, sample_analysis, storage_recommendation):
        """Test Python ETL script generation"""
        code = generator.generate_python_etl(
            analysis=sample_analysis,
            storage=storage_recommendation
        )
        
        # Check it's valid Python-like code
        assert 'import' in code
        assert 'def ' in code or 'class ' in code
        assert len(code) > 100
        
        # Check it references the data
        assert 'csv' in code.lower() or sample_analysis['filename'] in code
    
    def test_generate_sql_ddl(self, generator, sample_analysis):
        """Test SQL DDL generation"""
        ddl = generator.generate_sql_ddl(
            analysis=sample_analysis,
            storage_type='PostgreSQL',
            table_name='test_table'
        )
        
        # Check SQL structure
        assert 'CREATE TABLE' in ddl
        assert 'test_table' in ddl
        assert 'id' in ddl
        assert 'name' in ddl
        assert 'value' in ddl
    
    def test_generate_transformation_code(self, generator, sample_analysis):
        """Test data transformation code generation"""
        transformations = [
            {'type': 'clean_nulls', 'columns': ['name']},
            {'type': 'normalize', 'columns': ['value']}
        ]
        
        code = generator.generate_transformation_code(
            analysis=sample_analysis,
            transformations=transformations
        )
        
        # Should include transformation logic
        assert 'clean' in code.lower() or 'null' in code.lower()
        assert 'normalize' in code.lower() or 'transform' in code.lower()
    
    def test_generate_validation_code(self, generator, sample_analysis):
        """Test data validation code generation"""
        code = generator.generate_validation_code(
            analysis=sample_analysis
        )
        
        # Should include validation logic
        assert 'validate' in code.lower() or 'check' in code.lower() or 'assert' in code.lower()
        assert 'def ' in code
    
    def test_generate_unit_tests(self, generator, sample_analysis):
        """Test unit test generation"""
        test_code = generator.generate_unit_tests(
            analysis=sample_analysis
        )
        
        # Should include pytest structure
        assert 'import pytest' in test_code or 'def test_' in test_code
        assert 'assert' in test_code
    
    def test_code_quality_python(self, generator, sample_analysis, storage_recommendation):
        """Test generated Python code quality"""
        code = generator.generate_python_etl(
            analysis=sample_analysis,
            storage=storage_recommendation
        )
        
        # Check for best practices
        assert 'import' in code  # Has imports
        assert 'def ' in code or 'class ' in code  # Has functions/classes
        assert '#' in code or '"""' in code  # Has comments/docstrings
    
    def test_generate_config_file(self, generator, storage_recommendation):
        """Test configuration file generation"""
        config = generator.generate_config_file(
            storage=storage_recommendation
        )
        
        # Should be valid config format
        assert isinstance(config, str)
        assert len(config) > 10
        # Could be JSON, YAML, or .env format
        assert '=' in config or ':' in config or '{' in config
    
    def test_generate_docker_compose(self, generator, storage_recommendation):
        """Test Docker Compose file generation"""
        docker_compose = generator.generate_docker_compose(
            storage=storage_recommendation
        )
        
        # Check Docker Compose structure
        assert 'version:' in docker_compose
        assert 'services:' in docker_compose
        assert 'postgres' in docker_compose.lower() or 'clickhouse' in docker_compose.lower()
    
    def test_generate_requirements_txt(self, generator, sample_analysis, storage_recommendation):
        """Test requirements.txt generation"""
        requirements = generator.generate_requirements_txt(
            analysis=sample_analysis,
            storage=storage_recommendation
        )
        
        # Should include necessary packages
        assert 'pandas' in requirements or 'numpy' in requirements
        assert 'psycopg2' in requirements or 'sqlalchemy' in requirements or 'clickhouse' in requirements
    
    def test_generate_readme(self, generator, sample_analysis, storage_recommendation):
        """Test README generation"""
        readme = generator.generate_readme(
            analysis=sample_analysis,
            storage=storage_recommendation
        )
        
        # Check README structure
        assert '#' in readme  # Markdown headers
        assert len(readme) > 100
        assert 'ETL' in readme or 'Pipeline' in readme or sample_analysis['filename'] in readme
    
    @pytest.mark.parametrize("language", ['python', 'sql', 'bash'])
    def test_generate_code_different_languages(self, generator, sample_analysis, language):
        """Test code generation for different languages"""
        if language == 'python':
            code = generator.generate_python_etl(sample_analysis, {'recommended_storage': 'PostgreSQL'})
            assert 'import' in code
        elif language == 'sql':
            code = generator.generate_sql_ddl(sample_analysis, 'PostgreSQL', 'test')
            assert 'CREATE' in code
        elif language == 'bash':
            code = generator.generate_bash_script(sample_analysis)
            assert '#!/bin/bash' in code or 'sh' in code.lower()
    
    def test_code_syntax_validation(self, generator, sample_analysis, storage_recommendation):
        """Test that generated code has valid syntax"""
        code = generator.generate_python_etl(
            analysis=sample_analysis,
            storage=storage_recommendation
        )
        
        # Basic syntax checks
        # Count opening and closing brackets
        assert code.count('(') == code.count(')')
        assert code.count('[') == code.count(']')
        assert code.count('{') == code.count('}')
        
        # No obvious syntax errors
        assert 'def ():' not in code  # Function with no name
        assert 'import import' not in code  # Duplicate imports
    
    def test_generate_airflow_dag(self, generator, sample_analysis, storage_recommendation):
        """Test Airflow DAG code generation"""
        dag_code = generator.generate_airflow_dag(
            analysis=sample_analysis,
            storage=storage_recommendation
        )
        
        # Check Airflow structure
        assert 'from airflow import DAG' in dag_code or 'airflow' in dag_code.lower()
        assert 'dag' in dag_code.lower()
    
    def test_generate_kafka_code(self, generator, sample_analysis):
        """Test Kafka streaming code generation"""
        kafka_code = generator.generate_kafka_code(
            analysis=sample_analysis,
            topic='test-topic'
        )
        
        # Check Kafka structure
        assert 'kafka' in kafka_code.lower()
        assert 'producer' in kafka_code.lower() or 'consumer' in kafka_code.lower()
        assert 'test-topic' in kafka_code
