"""
Tests for Storage Selector (Hybrid AI)
"""
import pytest
from unittest.mock import Mock, patch
from backend.ml.storage_selector import StorageSelector


class TestStorageSelector:
    """Test intelligent storage selection"""
    
    @pytest.fixture
    def selector_with_llm(self):
        """Create selector with LLM enabled"""
        # Mock the LLM client to prevent real API calls during initialization
        with patch('backend.ml.storage_selector.get_llm_client') as mock_get_client:
            mock_client = Mock()
            mock_get_client.return_value = mock_client
            selector = StorageSelector(use_llm=True)
            # Return both selector and the mock client for test use
            selector._test_mock_client = mock_client
            return selector
    
    @pytest.fixture
    def selector_without_llm(self):
        """Create selector without LLM"""
        return StorageSelector(use_llm=False)
    
    def test_rule_based_large_dataset(self, selector_without_llm):
        """Test rule-based recommendation for large datasets"""
        profile = {
            'basic_stats': {'row_count': 1000000},
            'patterns_detected': {}
        }
        
        recommendation = selector_without_llm.recommend_storage(
            df_sample={},
            profile=profile,
            row_count=1000000,
            file_size_mb=11000  # >10GB
        )
        
        assert recommendation['recommended_storage'] == 'HDFS'
        assert recommendation['data_type'] == 'raw_historical'
        assert recommendation['analysis_method'] == 'rule_based'
    
    def test_rule_based_time_series(self, selector_without_llm):
        """Test rule-based recommendation for time series"""
        profile = {
            'basic_stats': {'row_count': 50000},
            'patterns_detected': {
                'time_series': ['timestamp'],
                'metrics': ['value1', 'value2', 'value3', 'value4']
            }
        }
        
        recommendation = selector_without_llm.recommend_storage(
            df_sample={},
            profile=profile,
            row_count=50000,
            file_size_mb=100
        )
        
        assert recommendation['recommended_storage'] == 'ClickHouse'
        assert recommendation['data_type'] == 'analytical'
        assert 'partitioning_strategy' in recommendation
    
    def test_rule_based_transactional(self, selector_without_llm):
        """Test rule-based recommendation for transactional data"""
        profile = {
            'basic_stats': {'row_count': 10000},
            'patterns_detected': {
                'identifiers': ['user_id', 'order_id'],
                'metrics': ['amount']
            }
        }
        
        recommendation = selector_without_llm.recommend_storage(
            df_sample={},
            profile=profile,
            row_count=10000,
            file_size_mb=5
        )
        
        assert recommendation['recommended_storage'] == 'PostgreSQL'
        assert recommendation['data_type'] == 'transactional'
    
    def test_hybrid_agreement(self, selector_with_llm):
        """Test hybrid when both LLM and rules agree"""
        # Configure the mock LLM client from the fixture
        selector_with_llm.llm_client.analyze_data_structure.return_value = {
            'recommended_storage': 'ClickHouse',
            'reasoning': 'LLM reasoning',
            'data_type': 'analytical'
        }
        
        # Profile that would make rules also recommend ClickHouse
        profile = {
            'basic_stats': {'row_count': 50000},
            'patterns_detected': {
                'time_series': ['timestamp'],
                'metrics': ['value1', 'value2', 'value3', 'value4']
            },
            'column_profiles': {}
        }
        
        recommendation = selector_with_llm.recommend_storage(
            df_sample={},
            profile=profile,
            row_count=50000,
            file_size_mb=100
        )
        
        # Should have high confidence when both agree
        assert recommendation['recommended_storage'] == 'ClickHouse'
        assert recommendation.get('confidence', 0) == 0.95
        assert recommendation.get('rule_based_agreement') == True
    
    def test_hybrid_disagreement(self, selector_with_llm):
        """Test hybrid when LLM and rules disagree"""
        # Configure the mock LLM client from the fixture
        selector_with_llm.llm_client.analyze_data_structure.return_value = {
            'recommended_storage': 'HDFS',
            'reasoning': 'LLM wants HDFS',
            'data_type': 'raw_historical'
        }
        
        # Profile that would make rules recommend PostgreSQL
        profile = {
            'basic_stats': {'row_count': 1000},
            'patterns_detected': {
                'identifiers': ['id']
            },
            'column_profiles': {}
        }
        
        recommendation = selector_with_llm.recommend_storage(
            df_sample={},
            profile=profile,
            row_count=1000,
            file_size_mb=1
        )
        
        # Should prefer LLM but lower confidence
        assert recommendation['recommended_storage'] == 'HDFS'
        assert recommendation.get('confidence', 0) == 0.75
        assert recommendation.get('rule_based_agreement') == False
        assert 'alternative_storage' in recommendation
    
    def test_llm_fallback_on_error(self, selector_with_llm):
        """Test fallback to rules when LLM fails"""
        # Configure the mock LLM client to raise an exception
        selector_with_llm.llm_client.analyze_data_structure.side_effect = Exception("API Error")
        
        profile = {
            'basic_stats': {'row_count': 1000},
            'patterns_detected': {},
            'column_profiles': {}
        }
        
        recommendation = selector_with_llm.recommend_storage(
            df_sample={},
            profile=profile,
            row_count=1000,
            file_size_mb=1
        )
        
        # Should still return a recommendation (rule-based fallback)
        assert 'recommended_storage' in recommendation
        assert recommendation['analysis_method'] == 'rule_based'
        assert 'note' in recommendation
    
    def test_get_storage_config(self, selector_without_llm):
        """Test storage configuration retrieval"""
        # Test PostgreSQL config
        pg_config = selector_without_llm.get_storage_config('PostgreSQL')
        assert 'engine' in pg_config
        assert 'use_cases' in pg_config
        assert 'optimization_tips' in pg_config
        assert 'connection_info' in pg_config
        
        # Test ClickHouse config
        ch_config = selector_without_llm.get_storage_config('ClickHouse')
        assert ch_config['engine'] == 'ClickHouse MergeTree'
        
        # Test HDFS config
        hdfs_config = selector_without_llm.get_storage_config('HDFS')
        assert hdfs_config['engine'] == 'Hadoop HDFS 3.2'
        
        # Test invalid storage
        invalid = selector_without_llm.get_storage_config('InvalidDB')
        assert invalid == {}
