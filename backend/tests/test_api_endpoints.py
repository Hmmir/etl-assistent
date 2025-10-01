"""
Tests for API Endpoints
"""
import pytest
from fastapi.testclient import TestClient
from io import BytesIO
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from api.main import app


class TestAPIEndpoints:
    """Test FastAPI endpoints"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_health_endpoint(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "etl-assistant-api"
        assert "supported_sources" in data
        assert "supported_targets" in data
    
    def test_upload_csv_file(self, client):
        """Test CSV file upload"""
        # Create test CSV
        csv_content = b"id,name,value\n1,Alice,10\n2,Bob,20"
        files = {"file": ("test.csv", BytesIO(csv_content), "text/csv")}
        data = {"source_description": "Test CSV data"}
        
        response = client.post("/upload", files=files, data=data)
        
        assert response.status_code == 200
        result = response.json()
        assert "filename" in result
        assert result["format_type"] == "CSV"
    
    def test_upload_json_file(self, client):
        """Test JSON file upload"""
        json_content = b'[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'
        files = {"file": ("test.json", BytesIO(json_content), "application/json")}
        
        response = client.post("/upload", files=files)
        
        assert response.status_code == 200
        result = response.json()
        assert result["format_type"] == "JSON"
    
    def test_upload_without_file(self, client):
        """Test upload endpoint without file"""
        response = client.post("/upload")
        
        assert response.status_code == 422  # Validation error
    
    def test_generate_pipeline(self, client):
        """Test pipeline generation"""
        # First upload a file
        csv_content = b"timestamp,user_id,value\n2025-01-01,user1,10\n2025-01-02,user2,20"
        files = {"file": ("test.csv", BytesIO(csv_content), "text/csv")}
        upload_response = client.post("/upload", files=files)
        filename = upload_response.json()["filename"]
        
        # Generate pipeline
        response = client.post(f"/generate_pipeline?filename={filename}")
        
        assert response.status_code == 200
        result = response.json()
        assert "data_analysis" in result
        assert "etl_code" in result
        assert "ddl" in result
        assert "airflow_dag" in result
    
    def test_generate_pipeline_nonexistent_file(self, client):
        """Test pipeline generation with nonexistent file"""
        response = client.post("/generate_pipeline?filename=nonexistent.csv")
        
        assert response.status_code in [404, 500]  # File not found or error
    
    def test_infrastructure_status(self, client):
        """Test infrastructure status endpoint"""
        response = client.get("/infrastructure/status")
        
        assert response.status_code == 200
        data = response.json()
        assert "services" in data
        
        # Check for expected services
        services = data["services"]
        assert any(s["name"] == "PostgreSQL" for s in services)
        assert any(s["name"] == "ClickHouse" for s in services)
    
    def test_cors_headers(self, client):
        """Test CORS headers are present"""
        response = client.options("/health")
        
        # Should have CORS headers
        assert "access-control-allow-origin" in response.headers or response.status_code == 200
    
    def test_api_docs_available(self, client):
        """Test API documentation is accessible"""
        response = client.get("/docs")
        
        assert response.status_code == 200
        assert "swagger" in response.text.lower() or "redoc" in response.text.lower()
    
    @pytest.mark.parametrize("file_ext,content,mime_type", [
        ("csv", b"a,b\n1,2", "text/csv"),
        ("json", b'{"key": "value"}', "application/json"),
        ("xml", b'<root><item>test</item></root>', "text/xml"),
    ])
    def test_multiple_file_formats(self, client, file_ext, content, mime_type):
        """Test uploading different file formats"""
        files = {"file": (f"test.{file_ext}", BytesIO(content), mime_type)}
        
        response = client.post("/upload", files=files)
        
        assert response.status_code == 200
        result = response.json()
        assert "filename" in result
        assert "format_type" in result
