"""
Tests for OpenRouter LLM Client
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from backend.ml.openrouter_client import OpenRouterClient


class TestOpenRouterClient:
    """Test OpenRouter API integration"""
    
    @pytest.fixture
    def client(self):
        """Create test client with mock API key"""
        with patch.dict('os.environ', {'OPENROUTER_API_KEY': 'test-key'}):
            return OpenRouterClient(api_key='test-key-123')
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client.api_key == 'test-key-123'
        assert client.model == 'meta-llama/llama-3.2-3b-instruct:free'
        assert client.base_url == 'https://openrouter.ai/api/v1'
    
    def test_client_without_api_key(self):
        """Test client raises error without API key"""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError, match="API key not found"):
                OpenRouterClient(api_key=None)
    
    @patch('requests.post')
    def test_analyze_data_structure_success(self, mock_post, client):
        """Test successful data analysis"""
        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "choices": [{
                "message": {
                    "content": '{"data_type": "analytical", "recommended_storage": "ClickHouse", "reasoning": "Test reasoning"}'
                }
            }]
        }
        mock_post.return_value = mock_response
        
        # Test data
        df_sample = {"col1": [1, 2, 3]}
        columns_info = {"col1": {"type": "int64"}}
        
        # Call method
        result = client.analyze_data_structure(
            df_sample=df_sample,
            columns_info=columns_info,
            row_count=100,
            file_size_mb=1.5
        )
        
        # Assertions
        assert result["data_type"] == "analytical"
        assert result["recommended_storage"] == "ClickHouse"
        assert result["reasoning"] == "Test reasoning"
        assert result["llm_model"] == client.model
        assert result["analysis_method"] == "LLM"
        
        # Verify API was called
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "Authorization" in call_args[1]["headers"]
    
    @patch('requests.post')
    def test_analyze_data_structure_fallback(self, mock_post, client):
        """Test fallback when LLM returns invalid JSON"""
        # Mock invalid response
        mock_response = Mock()
        mock_response.json.return_value = {
            "choices": [{
                "message": {
                    "content": "Invalid JSON response"
                }
            }]
        }
        mock_post.return_value = mock_response
        
        # Test
        result = client.analyze_data_structure(
            df_sample={},
            columns_info={},
            row_count=100,
            file_size_mb=1.5
        )
        
        # Should return fallback
        assert result["data_type"] == "unknown"
        assert result["recommended_storage"] == "PostgreSQL"
        assert result["analysis_method"] == "LLM_FALLBACK"
        assert "raw_response" in result
    
    @patch('requests.post')
    def test_generate_sql_ddl(self, mock_post, client):
        """Test SQL DDL generation"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "choices": [{
                "message": {
                    "content": "```sql\nCREATE TABLE test (id INT);\n```"
                }
            }]
        }
        mock_post.return_value = mock_response
        
        result = client.generate_sql_ddl(
            table_name="test_table",
            columns_info={"id": {"type": "int64"}},
            storage_type="PostgreSQL",
            partitioning=None
        )
        
        assert "CREATE TABLE" in result
        assert result.strip() == "CREATE TABLE test (id INT);"
    
    @patch('backend.ml.openrouter_client.requests.post')
    def test_api_timeout_handling(self, mock_post, client):
        """Test handling of API timeout"""
        import requests
        mock_post.side_effect = requests.exceptions.RequestException("Timeout")
        
        with pytest.raises(Exception, match="OpenRouter API error"):
            client._make_request(messages=[])
    
    def test_model_configuration(self):
        """Test different model configurations"""
        with patch.dict('os.environ', {'OPENROUTER_API_KEY': 'test'}):
            # Test with custom model
            client = OpenRouterClient(
                api_key='test',
                model='anthropic/claude-3-opus'
            )
            assert client.model == 'anthropic/claude-3-opus'
            
            # Test with default model
            client2 = OpenRouterClient(api_key='test')
            assert client2.model == 'meta-llama/llama-3.2-3b-instruct:free'
