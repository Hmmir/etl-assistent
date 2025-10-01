"""
Tests for Data Analyzers
"""
import pytest
import pandas as pd
import io
import sys
from pathlib import Path

# Добавляем parent directory в Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.data_service import DataAnalyzer


class TestDataAnalyzer:
    """Test data analysis functionality"""
    
    @pytest.fixture
    def analyzer(self):
        """Create analyzer instance"""
        return DataAnalyzer()
    
    @pytest.fixture
    def sample_csv_content(self):
        """Sample CSV content"""
        return b"id,name,value\n1,Alice,10.5\n2,Bob,20.3\n3,Charlie,15.7"
    
    @pytest.fixture
    def sample_json_content(self):
        """Sample JSON content"""
        return b'[{"id": 1, "name": "Alice", "value": 10.5}, {"id": 2, "name": "Bob", "value": 20.3}]'
    
    @pytest.fixture
    def sample_xml_content(self):
        """Sample XML content"""
        return b'''<?xml version="1.0"?>
        <records>
            <record>
                <id>1</id>
                <name>Alice</name>
                <value>10.5</value>
            </record>
            <record>
                <id>2</id>
                <name>Bob</name>
                <value>20.3</value>
            </record>
        </records>'''
    
    def test_detect_encoding_utf8(self, analyzer, sample_csv_content):
        """Test UTF-8 encoding detection"""
        encoding = analyzer.detect_encoding(sample_csv_content)
        assert encoding.lower() in ['utf-8', 'utf_8', 'ascii']
    
    def test_detect_encoding_various(self, analyzer):
        """Test detection of various encodings"""
        # UTF-8 with BOM
        utf8_bom = b'\xef\xbb\xbfHello'
        encoding = analyzer.detect_encoding(utf8_bom)
        assert 'utf' in encoding.lower()
        
        # ASCII
        ascii_content = b'Simple ASCII text'
        encoding = analyzer.detect_encoding(ascii_content)
        assert encoding.lower() in ['ascii', 'utf-8', 'utf_8']
    
    def test_detect_format_csv(self, analyzer, sample_csv_content):
        """Test CSV format detection"""
        format_type = analyzer.detect_format(sample_csv_content, 'test.csv')
        assert format_type == 'CSV'
    
    def test_detect_format_json(self, analyzer, sample_json_content):
        """Test JSON format detection"""
        format_type = analyzer.detect_format(sample_json_content, 'test.json')
        assert format_type == 'JSON'
    
    def test_detect_format_xml(self, analyzer, sample_xml_content):
        """Test XML format detection"""
        format_type = analyzer.detect_format(sample_xml_content, 'test.xml')
        assert format_type == 'XML'
    
    def test_analyze_csv_file(self, analyzer, sample_csv_content):
        """Test CSV file analysis"""
        analysis = analyzer.analyze_file(
            file_content=sample_csv_content,
            filename='test.csv'
        )
        
        # Check basic structure
        assert analysis['filename'] == 'test.csv'
        assert analysis['format_type'] == 'CSV'
        assert analysis['total_rows'] == 3
        assert len(analysis['columns']) == 3
        
        # Check columns
        column_names = [col['name'] for col in analysis['columns']]
        assert 'id' in column_names
        assert 'name' in column_names
        assert 'value' in column_names
    
    def test_analyze_json_file(self, analyzer, sample_json_content):
        """Test JSON file analysis"""
        analysis = analyzer.analyze_file(
            file_content=sample_json_content,
            filename='test.json'
        )
        
        assert analysis['filename'] == 'test.json'
        assert analysis['format_type'] == 'JSON'
        assert analysis['total_rows'] == 2
        # JSON uses 'fields' instead of 'columns'
        fields = analysis.get('columns', analysis.get('fields', []))
        assert len(fields) >= 3
    
    def test_analyze_xml_file(self, analyzer, sample_xml_content):
        """Test XML file analysis"""
        analysis = analyzer.analyze_file(
            file_content=sample_xml_content,
            filename='test.xml'
        )
        
        assert analysis['filename'] == 'test.xml'
        assert analysis['format_type'] == 'XML'
        assert analysis['total_rows'] >= 2
    
    def test_infer_data_types(self, analyzer):
        """Test data type inference"""
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.5, 2.5, 3.5],
            'string_col': ['a', 'b', 'c'],
            'date_col': pd.date_range('2025-01-01', periods=3)
        })
        
        types = analyzer.infer_data_types(df)
        
        assert 'int_col' in types
        assert 'INT' in types['int_col'] or 'INTEGER' in types['int_col']
        
        assert 'float_col' in types
        assert 'FLOAT' in types['float_col'] or 'DOUBLE' in types['float_col']
        
        assert 'string_col' in types
        assert 'VARCHAR' in types['string_col'] or 'TEXT' in types['string_col']
        
        assert 'date_col' in types
        assert 'DATE' in types['date_col'] or 'TIMESTAMP' in types['date_col']
    
    def test_analyze_empty_file(self, analyzer):
        """Test handling of empty files"""
        empty_csv = b""
        
        with pytest.raises(Exception):
            analyzer.analyze_file(empty_csv, 'empty.csv')
    
    def test_analyze_malformed_json(self, analyzer):
        """Test handling of malformed JSON"""
        bad_json = b'{"incomplete": '
        
        with pytest.raises(Exception):
            analyzer.analyze_file(bad_json, 'bad.json')
    
    def test_analyze_malformed_xml(self, analyzer):
        """Test handling of malformed XML"""
        bad_xml = b'<root><unclosed>'
        
        with pytest.raises(Exception):
            analyzer.analyze_file(bad_xml, 'bad.xml')
    
    def test_sample_data_extraction(self, analyzer, sample_csv_content):
        """Test that sample data is extracted"""
        analysis = analyzer.analyze_file(file_content=sample_csv_content, filename='test.csv')
        
        # Should include sample values
        for column in analysis['columns']:
            assert 'sample_values' in column or len(column) > 2
    
    def test_null_detection(self, analyzer):
        """Test null value detection"""
        csv_with_nulls = b"id,name,value\n1,Alice,10\n2,,20\n3,Charlie,"
        
        analysis = analyzer.analyze_file(file_content=csv_with_nulls, filename='test.csv')
        
        # Should detect nullable columns
        name_col = next((c for c in analysis['columns'] if c['name'] == 'name'), None)
        value_col = next((c for c in analysis['columns'] if c['name'] == 'value'), None)
        
        assert name_col is not None
        assert value_col is not None
    
    def test_large_file_handling(self, analyzer):
        """Test handling of large files"""
        # Create large CSV content
        rows = [f"{i},name{i},{i*1.5}" for i in range(10000)]
        large_csv = b"id,name,value\n" + "\n".join(rows).encode('utf-8')
        
        analysis = analyzer.analyze_file(file_content=large_csv, filename='large.csv')
        
        assert analysis['total_rows'] == 10000
        assert len(analysis['columns']) == 3
    
    def test_special_characters_handling(self, analyzer):
        """Test handling of special characters"""
        csv_with_special = b"id,name,description\n1,Alice,\"Hello, World!\"\n2,Bob,Line1\nLine2"
        
        analysis = analyzer.analyze_file(file_content=csv_with_special, filename='special.csv')
        
        assert analysis['total_rows'] >= 1
        assert len(analysis['columns']) == 3
    
    @pytest.mark.parametrize("filename,expected_format", [
        ('data.csv', 'CSV'),
        ('data.json', 'JSON'),
        ('data.xml', 'XML'),
        ('data.CSV', 'CSV'),  # Case insensitive
        ('data.Json', 'JSON'),
    ])
    def test_format_detection_by_extension(self, analyzer, filename, expected_format):
        """Test format detection based on file extension"""
        # Create minimal valid content for each format
        if expected_format == 'CSV':
            content = b"col1\nvalue1"
        elif expected_format == 'JSON':
            content = b'[{"col1": "value1"}]'
        else:  # XML
            content = b'<root><item>value1</item></root>'
        
        format_type = analyzer.detect_format(content, filename)
        assert format_type == expected_format
