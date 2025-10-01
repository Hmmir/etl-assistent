"""
Tests for Kafka Streaming Generator
"""
import pytest
from services.streaming_service import KafkaStreamingGenerator


class TestKafkaStreamingGenerator:
    """Test Kafka streaming code generation"""
    
    @pytest.fixture
    def generator(self):
        """Create generator instance"""
        return KafkaStreamingGenerator()
    
    @pytest.fixture
    def sample_analysis(self):
        """Sample data analysis"""
        return {
            'filename': 'streaming_data.json',
            'total_rows': 100000,
            'columns': [
                {'name': 'event_id', 'type': 'STRING'},
                {'name': 'timestamp', 'type': 'TIMESTAMP'},
                {'name': 'value', 'type': 'FLOAT'}
            ],
            'basic_stats': {
                'row_count': 100000,
                'column_count': 3
            }
        }
    
    def test_consumer_generation(self, generator, sample_analysis):
        """Test Kafka consumer code generation"""
        consumer_code = generator.generate_kafka_consumer(
            topic_name='test-topic',
            target_storage='ClickHouse',
            analysis=sample_analysis
        )
        
        # Check consumer structure
        assert 'from kafka import KafkaConsumer' in consumer_code
        assert 'test-topic' in consumer_code
        assert 'ClickHouse' in consumer_code
        
        # Check configuration
        assert 'bootstrap_servers' in consumer_code
        assert 'group_id' in consumer_code
        assert 'auto_offset_reset' in consumer_code
        
        # Check it's executable Python
        assert 'def ' in consumer_code or 'class ' in consumer_code
    
    def test_producer_generation(self, generator, sample_analysis):
        """Test Kafka producer code generation"""
        producer_code = generator.generate_kafka_producer(
            topic_name='test-topic',
            source_description='Test data source',
            analysis=sample_analysis
        )
        
        # Check producer structure
        assert 'from kafka import KafkaProducer' in producer_code
        assert 'test-topic' in producer_code
        
        # Check configuration
        assert 'bootstrap_servers' in producer_code
        assert 'value_serializer' in producer_code or 'serializer' in producer_code
        
        # Check it's executable Python
        assert 'def ' in producer_code or 'class ' in producer_code
    
    def test_docker_compose_generation(self, generator, sample_analysis):
        """Test Docker Compose Kafka config generation"""
        docker_compose = generator.generate_docker_compose(
            topic_name='test-topic',
            analysis=sample_analysis
        )
        
        # Check Docker Compose structure
        assert 'version:' in docker_compose
        assert 'services:' in docker_compose
        assert 'zookeeper:' in docker_compose
        assert 'kafka:' in docker_compose
        
        # Check Kafka configuration
        assert 'KAFKA_ADVERTISED_LISTENERS' in docker_compose
        assert 'KAFKA_ZOOKEEPER_CONNECT' in docker_compose
        assert '9092' in docker_compose  # Kafka port
    
    def test_consumer_with_postgresql_target(self, generator, sample_analysis):
        """Test consumer generation with PostgreSQL target"""
        consumer_code = generator.generate_kafka_consumer(
            topic_name='pg-topic',
            target_storage='PostgreSQL',
            analysis=sample_analysis
        )
        
        # Should include PostgreSQL connection logic
        assert 'PostgreSQL' in consumer_code or 'psycopg2' in consumer_code or 'postgres' in consumer_code.lower()
    
    def test_consumer_with_clickhouse_target(self, generator, sample_analysis):
        """Test consumer generation with ClickHouse target"""
        consumer_code = generator.generate_kafka_consumer(
            topic_name='ch-topic',
            target_storage='ClickHouse',
            analysis=sample_analysis
        )
        
        # Should include ClickHouse connection logic
        assert 'ClickHouse' in consumer_code or 'clickhouse' in consumer_code.lower()
    
    def test_producer_with_batch_processing(self, generator, sample_analysis):
        """Test producer with batch processing logic"""
        large_analysis = sample_analysis.copy()
        large_analysis['total_rows'] = 10000000
        
        producer_code = generator.generate_kafka_producer(
            topic_name='batch-topic',
            source_description='Large dataset',
            analysis=large_analysis
        )
        
        # Should include batch processing
        assert 'batch' in producer_code.lower() or 'chunk' in producer_code.lower()
    
    def test_consumer_error_handling(self, generator, sample_analysis):
        """Test consumer includes error handling"""
        consumer_code = generator.generate_kafka_consumer(
            topic_name='test-topic',
            target_storage='ClickHouse',
            analysis=sample_analysis
        )
        
        # Should have error handling
        assert 'try:' in consumer_code or 'except' in consumer_code
        assert 'KafkaError' in consumer_code or 'Exception' in consumer_code
    
    def test_producer_error_handling(self, generator, sample_analysis):
        """Test producer includes error handling"""
        producer_code = generator.generate_kafka_producer(
            topic_name='test-topic',
            source_description='Test',
            analysis=sample_analysis
        )
        
        # Should have error handling
        assert 'try:' in producer_code or 'except' in producer_code
    
    def test_docker_compose_includes_kafka_ui(self, generator, sample_analysis):
        """Test Docker Compose includes Kafka UI"""
        docker_compose = generator.generate_docker_compose(
            topic_name='test-topic',
            analysis=sample_analysis
        )
        
        # Should include Kafka UI for monitoring
        assert 'kafka-ui' in docker_compose.lower() or 'kafdrop' in docker_compose.lower() or '8080' in docker_compose
    
    def test_consumer_with_json_deserialization(self, generator, sample_analysis):
        """Test consumer with JSON deserialization"""
        consumer_code = generator.generate_kafka_consumer(
            topic_name='json-topic',
            target_storage='PostgreSQL',
            analysis=sample_analysis
        )
        
        # Should handle JSON deserialization
        assert 'json' in consumer_code.lower()
        assert 'deserialize' in consumer_code.lower() or 'loads' in consumer_code
    
    def test_producer_with_json_serialization(self, generator, sample_analysis):
        """Test producer with JSON serialization"""
        producer_code = generator.generate_kafka_producer(
            topic_name='json-topic',
            source_description='JSON data',
            analysis=sample_analysis
        )
        
        # Should handle JSON serialization
        assert 'json' in producer_code.lower()
        assert 'serialize' in producer_code.lower() or 'dumps' in producer_code
    
    @pytest.mark.parametrize("topic_name", [
        'simple-topic',
        'complex.topic.name',
        'topic_with_underscores',
        'topic-123'
    ])
    def test_various_topic_names(self, generator, sample_analysis, topic_name):
        """Test generation with various topic name formats"""
        consumer_code = generator.generate_kafka_consumer(
            topic_name=topic_name,
            target_storage='PostgreSQL',
            analysis=sample_analysis
        )
        
        # Should handle different topic naming conventions
        assert topic_name in consumer_code
        assert 'KafkaConsumer' in consumer_code
    
    def test_docker_compose_valid_yaml(self, generator, sample_analysis):
        """Test that generated Docker Compose is valid YAML"""
        docker_compose = generator.generate_docker_compose(
            topic_name='test-topic',
            analysis=sample_analysis
        )
        
        # Basic YAML structure checks
        lines = docker_compose.split('\n')
        assert any(line.strip().startswith('version:') for line in lines)
        assert any(line.strip().startswith('services:') for line in lines)
        
        # Check proper indentation (at least some indented lines)
        assert any(line.startswith('  ') or line.startswith('    ') for line in lines)
