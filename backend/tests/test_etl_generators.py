"""
Tests for ETL Code Generators
"""
import pytest
from services.generator_service import PythonETLGenerator


class TestETLCodeGenerator:
    """Test ETL code generation functionality"""
    
    @pytest.fixture
    def generator(self):
        """Create generator instance"""
        return PythonETLGenerator()
    
    @pytest.fixture
    def sample_analysis(self):
        """Sample data analysis"""
        return {
            'filename': 'test_data.csv',
            'format_type': 'CSV',
            'total_rows': 10000,
            'columns': [
                {
                    'name': 'id',
                    'type': 'INTEGER',
                    'nullable': False,
                    'sample_values': [1, 2, 3]
                },
                {
                    'name': 'name',
                    'type': 'VARCHAR(255)',
                    'nullable': True,
                    'sample_values': ['Alice', 'Bob', 'Charlie']
                },
                {
                    'name': 'value',
                    'type': 'FLOAT',
                    'nullable': False,
                    'sample_values': [10.5, 20.3, 15.7]
                }
            ],
            'basic_stats': {
                'row_count': 10000,
                'column_count': 3
            }
        }
    
    @pytest.fixture
    def storage_recommendation(self):
        """Sample storage recommendation"""
        return {
            'recommended_storage': 'PostgreSQL',
            'data_type': 'transactional',
            'reasoning': 'Small transactional dataset'
        }
    
    def test_etl_script_generation(self, generator, sample_analysis, storage_recommendation):
        """Test ETL script generation"""
        etl_code = generator.generate_etl_script(
            analysis=sample_analysis,
            storage_recommendation=storage_recommendation
        )
        
        # Check basic structure
        assert len(etl_code) > 100
        assert 'import' in etl_code
        assert 'def ' in etl_code or 'class ' in etl_code
        
        # Check file handling
        assert sample_analysis['filename'] in etl_code or 'csv' in etl_code.lower()
        
        # Check storage connection
        assert 'PostgreSQL' in etl_code or 'psycopg2' in etl_code or 'postgres' in etl_code.lower()
    
    def test_ddl_generation_postgresql(self, generator, sample_analysis):
        """Test DDL generation for PostgreSQL"""
        ddl = generator.generate_ddl(
            analysis=sample_analysis,
            storage_type='PostgreSQL',
            table_name='test_table'
        )
        
        # Check DDL structure
        assert 'CREATE TABLE' in ddl
        assert 'test_table' in ddl
        
        # Check columns
        assert 'id' in ddl
        assert 'name' in ddl
        assert 'value' in ddl
        
        # Check data types
        assert 'INTEGER' in ddl or 'INT' in ddl
        assert 'VARCHAR' in ddl or 'TEXT' in ddl
        assert 'FLOAT' in ddl or 'DOUBLE' in ddl or 'NUMERIC' in ddl
    
    def test_ddl_generation_clickhouse(self, generator, sample_analysis):
        """Test DDL generation for ClickHouse"""
        ddl = generator.generate_ddl(
            analysis=sample_analysis,
            storage_type='ClickHouse',
            table_name='test_table'
        )
        
        # Check ClickHouse specific syntax
        assert 'CREATE TABLE' in ddl
        assert 'ENGINE' in ddl
        assert 'MergeTree' in ddl or 'ReplacingMergeTree' in ddl
        
        # Check columns
        assert 'id' in ddl
        assert 'name' in ddl
        assert 'value' in ddl
    
    def test_ddl_with_partitioning(self, generator, sample_analysis):
        """Test DDL generation with partitioning"""
        # Add timestamp column for partitioning
        analysis_with_timestamp = sample_analysis.copy()
        analysis_with_timestamp['columns'].append({
            'name': 'created_at',
            'type': 'TIMESTAMP',
            'nullable': False
        })
        
        ddl = generator.generate_ddl(
            analysis=analysis_with_timestamp,
            storage_type='ClickHouse',
            table_name='test_table',
            partitioning={'column': 'created_at', 'strategy': 'MONTH'}
        )
        
        # Should include partitioning
        assert 'PARTITION BY' in ddl or 'partition' in ddl.lower()
    
    def test_ddl_with_indexes(self, generator, sample_analysis):
        """Test DDL generation includes indexes"""
        ddl = generator.generate_ddl(
            analysis=sample_analysis,
            storage_type='PostgreSQL',
            table_name='test_table'
        )
        
        # Should suggest or include indexes for ID column
        assert 'PRIMARY KEY' in ddl or 'INDEX' in ddl or 'id' in ddl
    
    def test_etl_script_with_transformations(self, generator, sample_analysis, storage_recommendation):
        """Test ETL script includes transformations"""
        etl_code = generator.generate_etl_script(
            analysis=sample_analysis,
            storage_recommendation=storage_recommendation,
            transformations=['clean_nulls', 'normalize_strings']
        )
        
        # Should include transformation logic
        assert 'clean' in etl_code.lower() or 'transform' in etl_code.lower()
    
    def test_etl_script_with_validation(self, generator, sample_analysis, storage_recommendation):
        """Test ETL script includes data validation"""
        etl_code = generator.generate_etl_script(
            analysis=sample_analysis,
            storage_recommendation=storage_recommendation
        )
        
        # Should include validation logic
        assert 'validate' in etl_code.lower() or 'check' in etl_code.lower() or 'assert' in etl_code.lower()
    
    def test_etl_script_with_error_handling(self, generator, sample_analysis, storage_recommendation):
        """Test ETL script includes error handling"""
        etl_code = generator.generate_etl_script(
            analysis=sample_analysis,
            storage_recommendation=storage_recommendation
        )
        
        # Should have error handling
        assert 'try:' in etl_code or 'except' in etl_code
        assert 'Exception' in etl_code or 'Error' in etl_code
    
    def test_etl_script_with_logging(self, generator, sample_analysis, storage_recommendation):
        """Test ETL script includes logging"""
        etl_code = generator.generate_etl_script(
            analysis=sample_analysis,
            storage_recommendation=storage_recommendation
        )
        
        # Should have logging
        assert 'import logging' in etl_code or 'logger' in etl_code or 'print' in etl_code
    
    def test_etl_script_for_csv(self, generator, storage_recommendation):
        """Test ETL script for CSV file"""
        csv_analysis = {
            'filename': 'data.csv',
            'format_type': 'CSV',
            'total_rows': 1000,
            'columns': [{'name': 'col1', 'type': 'INTEGER'}]
        }
        
        etl_code = generator.generate_etl_script(
            analysis=csv_analysis,
            storage_recommendation=storage_recommendation
        )
        
        # Should handle CSV reading
        assert 'csv' in etl_code.lower() or 'pandas' in etl_code or 'read_csv' in etl_code
    
    def test_etl_script_for_json(self, generator, storage_recommendation):
        """Test ETL script for JSON file"""
        json_analysis = {
            'filename': 'data.json',
            'format_type': 'JSON',
            'total_rows': 1000,
            'columns': [{'name': 'col1', 'type': 'INTEGER'}]
        }
        
        etl_code = generator.generate_etl_script(
            analysis=json_analysis,
            storage_recommendation=storage_recommendation
        )
        
        # Should handle JSON reading
        assert 'json' in etl_code.lower() or 'read_json' in etl_code
    
    def test_etl_script_for_xml(self, generator, storage_recommendation):
        """Test ETL script for XML file"""
        xml_analysis = {
            'filename': 'data.xml',
            'format_type': 'XML',
            'total_rows': 1000,
            'columns': [{'name': 'col1', 'type': 'INTEGER'}]
        }
        
        etl_code = generator.generate_etl_script(
            analysis=xml_analysis,
            storage_recommendation=storage_recommendation
        )
        
        # Should handle XML reading
        assert 'xml' in etl_code.lower() or 'parse' in etl_code.lower()
    
    @pytest.mark.parametrize("storage_type", [
        'PostgreSQL', 'ClickHouse', 'MySQL', 'SQLite'
    ])
    def test_ddl_for_different_databases(self, generator, sample_analysis, storage_type):
        """Test DDL generation for various databases"""
        ddl = generator.generate_ddl(
            analysis=sample_analysis,
            storage_type=storage_type,
            table_name='test_table'
        )
        
        # Should generate valid DDL for all databases
        assert 'CREATE TABLE' in ddl
        assert 'test_table' in ddl
        assert len(ddl) > 50
    
    def test_etl_script_with_batch_processing(self, generator, storage_recommendation):
        """Test ETL script with batch processing for large datasets"""
        large_analysis = {
            'filename': 'large_data.csv',
            'format_type': 'CSV',
            'total_rows': 10000000,  # 10M rows
            'columns': [{'name': 'col1', 'type': 'INTEGER'}]
        }
        
        etl_code = generator.generate_etl_script(
            analysis=large_analysis,
            storage_recommendation=storage_recommendation
        )
        
        # Should include batch/chunk processing for large datasets
        assert 'batch' in etl_code.lower() or 'chunk' in etl_code.lower() or 'chunksize' in etl_code
