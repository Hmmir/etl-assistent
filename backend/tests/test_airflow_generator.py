"""
Tests for Airflow DAG Generator
"""
import pytest
from services.airflow_service import AirflowDAGGenerator


class TestAirflowDAGGenerator:
    """Test Airflow DAG generation functionality"""
    
    @pytest.fixture
    def generator(self):
        """Create generator instance"""
        return AirflowDAGGenerator()
    
    @pytest.fixture
    def sample_analysis(self):
        """Sample data analysis"""
        return {
            'filename': 'test_data.csv',
            'total_rows': 50000,
            'total_records': 50000,
            'columns': [
                {'name': 'id', 'type': 'INTEGER'},
                {'name': 'timestamp', 'type': 'TIMESTAMP'},
                {'name': 'value', 'type': 'FLOAT'}
            ],
            'basic_stats': {
                'row_count': 50000,
                'column_count': 3
            }
        }
    
    @pytest.fixture
    def storage_recommendation(self):
        """Sample storage recommendation"""
        return {
            'recommended_storage': 'ClickHouse',
            'data_type': 'analytical',
            'partitioning_strategy': 'BY toYYYYMM(timestamp)',
            'reasoning': 'Time series data with metrics'
        }
    
    @pytest.fixture
    def etl_steps(self):
        """Sample ETL steps"""
        return [
            'Extract: Read CSV file',
            'Transform: Clean and validate data',
            'Load: Insert into ClickHouse'
        ]
    
    def test_dag_generation(self, generator, sample_analysis, storage_recommendation, etl_steps):
        """Test basic DAG generation"""
        dag_code = generator.generate_dag(
            analysis=sample_analysis,
            storage_recommendation=storage_recommendation,
            etl_steps=etl_steps
        )
        
        # Check DAG structure
        assert 'from airflow import DAG' in dag_code
        assert 'from airflow.operators' in dag_code
        assert 'dag_id=' in dag_code
        assert 'test_data_csv' in dag_code  # DAG ID based on filename
        
        # Check default args
        assert 'default_args' in dag_code
        assert 'owner' in dag_code
        assert 'retries' in dag_code
        
        # Check schedule
        assert 'schedule_interval' in dag_code or 'schedule=' in dag_code
    
    def test_dag_id_generation(self, generator):
        """Test DAG ID generation from filename"""
        # Test with CSV
        assert 'test_data_csv' in generator._generate_dag_id('test_data.csv')
        
        # Test with JSON
        assert 'data_json' in generator._generate_dag_id('data.json')
        
        # Test with special characters
        dag_id = generator._generate_dag_id('my-data_file 2024.csv')
        assert dag_id.replace('_', '').replace('-', '').isalnum()
    
    def test_schedule_determination_small_dataset(self, generator, sample_analysis):
        """Test schedule for small datasets"""
        small_analysis = sample_analysis.copy()
        small_analysis['total_rows'] = 1000
        
        schedule = generator._determine_schedule(small_analysis)
        
        # Small datasets should be processed less frequently
        assert schedule in ['@daily', '@weekly', '@monthly', 'None']
    
    def test_schedule_determination_large_dataset(self, generator, sample_analysis):
        """Test schedule for large datasets"""
        large_analysis = sample_analysis.copy()
        large_analysis['total_rows'] = 10000000
        
        schedule = generator._determine_schedule(large_analysis)
        
        # Large datasets might need more frequent processing
        assert schedule in ['@hourly', '@daily', '@weekly']
    
    def test_dag_with_postgresql(self, generator, sample_analysis, etl_steps):
        """Test DAG generation for PostgreSQL"""
        pg_recommendation = {
            'recommended_storage': 'PostgreSQL',
            'data_type': 'transactional',
            'reasoning': 'Transactional data'
        }
        
        dag_code = generator.generate_dag(
            analysis=sample_analysis,
            storage_recommendation=pg_recommendation,
            etl_steps=etl_steps
        )
        
        # Should include PostgreSQL specific imports/operators
        assert 'PostgreSQL' in dag_code or 'postgres' in dag_code.lower()
    
    def test_dag_with_clickhouse(self, generator, sample_analysis, storage_recommendation, etl_steps):
        """Test DAG generation for ClickHouse"""
        dag_code = generator.generate_dag(
            analysis=sample_analysis,
            storage_recommendation=storage_recommendation,
            etl_steps=etl_steps
        )
        
        # Should include ClickHouse specific configuration
        assert 'ClickHouse' in dag_code or 'clickhouse' in dag_code.lower()
    
    def test_dag_with_custom_schedule(self, generator, sample_analysis, storage_recommendation, etl_steps):
        """Test DAG with custom schedule"""
        custom_analysis = sample_analysis.copy()
        custom_analysis['schedule'] = '@hourly'
        
        dag_code = generator.generate_dag(
            analysis=custom_analysis,
            storage_recommendation=storage_recommendation,
            etl_steps=etl_steps
        )
        
        # Should respect custom schedule
        assert '@hourly' in dag_code or 'timedelta(hours=1)' in dag_code
    
    def test_dag_includes_error_handling(self, generator, sample_analysis, storage_recommendation, etl_steps):
        """Test that generated DAG includes error handling"""
        dag_code = generator.generate_dag(
            analysis=sample_analysis,
            storage_recommendation=storage_recommendation,
            etl_steps=etl_steps
        )
        
        # Should have retry configuration
        assert 'retries' in dag_code
        assert 'retry_delay' in dag_code or 'retry' in dag_code
    
    def test_dag_includes_dependencies(self, generator, sample_analysis, storage_recommendation, etl_steps):
        """Test that DAG includes task dependencies"""
        dag_code = generator.generate_dag(
            analysis=sample_analysis,
            storage_recommendation=storage_recommendation,
            etl_steps=etl_steps
        )
        
        # Should define task dependencies
        assert '>>' in dag_code or 'set_downstream' in dag_code or 'set_upstream' in dag_code
    
    def test_dag_with_empty_etl_steps(self, generator, sample_analysis, storage_recommendation):
        """Test DAG generation with empty ETL steps"""
        dag_code = generator.generate_dag(
            analysis=sample_analysis,
            storage_recommendation=storage_recommendation,
            etl_steps=[]
        )
        
        # Should still generate a valid DAG
        assert 'from airflow import DAG' in dag_code
        assert len(dag_code) > 100  # Should have reasonable content
    
    def test_dag_includes_monitoring(self, generator, sample_analysis, storage_recommendation, etl_steps):
        """Test that DAG includes monitoring/alerts"""
        dag_code = generator.generate_dag(
            analysis=sample_analysis,
            storage_recommendation=storage_recommendation,
            etl_steps=etl_steps
        )
        
        # Should have monitoring configuration
        assert 'email_on_failure' in dag_code or 'on_failure_callback' in dag_code
    
    @pytest.mark.parametrize("storage_type", [
        'PostgreSQL', 'ClickHouse', 'HDFS', 'S3'
    ])
    def test_dag_generation_for_different_storages(self, generator, sample_analysis, etl_steps, storage_type):
        """Test DAG generation for various storage types"""
        recommendation = {
            'recommended_storage': storage_type,
            'data_type': 'analytical',
            'reasoning': f'Test for {storage_type}'
        }
        
        dag_code = generator.generate_dag(
            analysis=sample_analysis,
            storage_recommendation=recommendation,
            etl_steps=etl_steps
        )
        
        # Should generate valid DAG for all storage types
        assert 'from airflow import DAG' in dag_code
        assert storage_type.lower() in dag_code.lower() or 'storage' in dag_code.lower()
