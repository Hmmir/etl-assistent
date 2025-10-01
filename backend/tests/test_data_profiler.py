"""
Tests for Data Profiler
"""
import pytest
import pandas as pd
import numpy as np
from backend.ml.data_profiler import DataProfiler


class TestDataProfiler:
    """Test data profiling functionality"""
    
    @pytest.fixture
    def profiler(self):
        """Create profiler instance"""
        return DataProfiler()
    
    @pytest.fixture
    def sample_df(self):
        """Create sample dataframe"""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', None, 'Eve'],
            'value': [10.5, 20.3, 15.7, 25.1, 30.2],
            'created_at': pd.date_range('2025-01-01', periods=5),
            'category': ['A', 'B', 'A', 'C', 'A']
        })
    
    def test_basic_stats(self, profiler, sample_df):
        """Test basic statistics calculation"""
        profile = profiler.profile_dataframe(sample_df)
        
        basic_stats = profile['basic_stats']
        assert basic_stats['row_count'] == 5
        assert basic_stats['column_count'] == 5
        assert basic_stats['null_cells'] == 1
        assert basic_stats['duplicate_rows'] == 0
    
    def test_column_profiling(self, profiler, sample_df):
        """Test individual column profiling"""
        profile = profiler.profile_dataframe(sample_df)
        
        columns = profile['column_profiles']
        
        # Test numeric column
        assert 'value' in columns
        assert columns['value']['dtype'] == 'float64'
        assert columns['value']['null_count'] == 0
        assert columns['value']['min'] == 10.5
        assert columns['value']['max'] == 30.2
        
        # Test text column
        assert 'name' in columns
        assert columns['name']['dtype'] == 'object'
        assert columns['name']['null_count'] == 1
        assert columns['name']['unique_count'] == 4
    
    def test_pattern_detection(self, profiler, sample_df):
        """Test pattern detection"""
        profile = profiler.profile_dataframe(sample_df)
        
        patterns = profile['patterns_detected']
        
        # Should detect time series
        assert 'created_at' in patterns['time_series']
        
        # Should detect categorical
        assert 'category' in patterns['categorical']
        
        # Should detect metrics
        assert 'value' in patterns['metrics']
        
        # Should detect identifiers
        assert 'id' in patterns['identifiers']
    
    def test_data_quality_assessment(self, profiler, sample_df):
        """Test data quality scoring"""
        profile = profiler.profile_dataframe(sample_df)
        
        quality = profile['data_quality']
        
        # Should have quality score
        assert 'quality_score' in quality
        assert 0 <= quality['quality_score'] <= 100
        
        # Should have issues list
        assert 'issues' in quality
        assert isinstance(quality['issues'], list)
        
        # Should have recommendations
        assert 'recommendations' in quality
    
    def test_correlation_detection(self, profiler):
        """Test correlation finding"""
        # Create dataframe with correlated columns
        df = pd.DataFrame({
            'x': [1, 2, 3, 4, 5],
            'y': [2, 4, 6, 8, 10],  # Perfectly correlated with x
            'z': [5, 4, 3, 2, 1]    # Negatively correlated
        })
        
        profile = profiler.profile_dataframe(df)
        correlations = profile['correlations']
        
        # Should find strong correlation between x and y
        assert len(correlations) > 0
        
        # Check for x-y correlation
        xy_corr = [c for c in correlations if 
                   set([c['column1'], c['column2']]) == {'x', 'y'}]
        assert len(xy_corr) > 0
        assert abs(xy_corr[0]['correlation']) > 0.9
    
    def test_empty_dataframe(self, profiler):
        """Test handling of empty dataframe"""
        df = pd.DataFrame()
        profile = profiler.profile_dataframe(df)
        
        assert profile['basic_stats']['row_count'] == 0
        assert profile['basic_stats']['column_count'] == 0
    
    def test_dataframe_with_nulls(self, profiler):
        """Test handling of dataframes with many nulls"""
        df = pd.DataFrame({
            'col1': [1, None, None, None, 5],
            'col2': [None, None, None, None, None]
        })
        
        profile = profiler.profile_dataframe(df)
        quality = profile['data_quality']
        
        # Should detect high null percentage
        assert quality['quality_score'] < 80
        assert any('null' in issue.lower() for issue in quality['issues'])
    
    def test_suggest_transformations(self, profiler):
        """Test transformation suggestions"""
        df = pd.DataFrame({
            'numeric': [1, 100, 1000, 10000],
            'categorical': ['A', 'B', 'A', 'C'],
            'date_str': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04']
        })
        
        suggestions = profiler.suggest_transformations(df)
        
        # Should suggest normalization for wide-range numeric
        assert any('normalize' in s['transformation'] 
                  for s in suggestions if s['column'] == 'numeric')
        
        # Should suggest encoding for categorical
        assert any('encode' in s['transformation'] 
                  for s in suggestions if s['column'] == 'categorical')
        
        # Should suggest datetime parsing
        assert any('datetime' in s['transformation'] 
                  for s in suggestions if s['column'] == 'date_str')
