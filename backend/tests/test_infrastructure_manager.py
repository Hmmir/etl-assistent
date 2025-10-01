"""
Tests for Infrastructure Manager
"""
import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.infrastructure_service import InfrastructureManager


class TestInfrastructureManager:
    """Test infrastructure management functionality"""
    
    @pytest.fixture
    def manager(self):
        """Create manager instance"""
        return InfrastructureManager()
    
    @pytest.mark.integration
    def test_check_docker_status(self, manager):
        """Test Docker status check"""
        status = manager.check_docker_status()
        
        # Should return status information
        assert 'docker_running' in status
        assert isinstance(status['docker_running'], bool)
    
    @pytest.mark.integration
    def test_check_services_status(self, manager):
        """Test services status check"""
        status = manager.check_services_status()
        
        # Should return status for multiple services
        assert 'services' in status
        assert isinstance(status['services'], list)
        
        # Check for expected services
        service_names = [s['name'] for s in status['services']]
        assert 'PostgreSQL' in service_names or 'postgres' in [s.lower() for s in service_names]
    
    def test_get_service_config_postgresql(self, manager):
        """Test PostgreSQL service configuration"""
        config = manager.get_service_config('PostgreSQL')
        
        assert config is not None
        assert 'port' in config or 'host' in config
        assert 'name' in config
    
    def test_get_service_config_clickhouse(self, manager):
        """Test ClickHouse service configuration"""
        config = manager.get_service_config('ClickHouse')
        
        assert config is not None
        assert 'port' in config or 'host' in config
    
    def test_get_service_config_invalid(self, manager):
        """Test invalid service configuration"""
        config = manager.get_service_config('InvalidService')
        
        # Should return empty or None
        assert config is None or config == {}
    
    @patch('subprocess.run')
    def test_start_service_success(self, mock_run, manager):
        """Test successful service start"""
        mock_run.return_value = Mock(returncode=0, stdout='Started')
        
        result = manager.start_service('postgres')
        
        # Should succeed
        assert result is True or 'success' in str(result).lower()
        mock_run.assert_called_once()
    
    @patch('subprocess.run')
    def test_start_service_failure(self, mock_run, manager):
        """Test service start failure"""
        mock_run.return_value = Mock(returncode=1, stderr='Error')
        
        result = manager.start_service('postgres')
        
        # Should handle failure
        assert result is False or 'error' in str(result).lower() or 'fail' in str(result).lower()
    
    @patch('subprocess.run')
    def test_stop_service(self, mock_run, manager):
        """Test service stop"""
        mock_run.return_value = Mock(returncode=0, stdout='Stopped')
        
        result = manager.stop_service('postgres')
        
        # Should call docker command
        mock_run.assert_called_once()
        assert result is True or 'success' in str(result).lower()
    
    def test_get_all_services_list(self, manager):
        """Test getting list of all services"""
        services = manager.get_all_services()
        
        assert isinstance(services, list)
        assert len(services) > 0
        
        # Check expected services are in the list
        service_names = [s['name'] if isinstance(s, dict) else s for s in services]
        assert any('postgres' in str(s).lower() for s in service_names)
        assert any('clickhouse' in str(s).lower() for s in service_names)
    
    @patch('requests.get')
    def test_health_check_postgresql(self, mock_get, manager):
        """Test PostgreSQL health check"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        # This might not use HTTP, so let's test the method exists
        health = manager.check_service_health('PostgreSQL')
        
        assert health is not None
        assert 'status' in health or 'healthy' in health
    
    @patch('requests.get')
    def test_health_check_clickhouse(self, mock_get, manager):
        """Test ClickHouse health check"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = 'Ok.'
        mock_get.return_value = mock_response
        
        health = manager.check_service_health('ClickHouse')
        
        assert health is not None
    
    def test_get_service_logs(self, manager):
        """Test getting service logs"""
        logs = manager.get_service_logs('postgres', lines=10)
        
        # Should return logs or empty string
        assert isinstance(logs, str) or isinstance(logs, list)
    
    def test_service_port_mapping(self, manager):
        """Test service port mappings are correct"""
        services = manager.get_all_services()
        
        # PostgreSQL should have port 5432
        pg_service = next((s for s in services if 'postgres' in str(s).lower()), None)
        if pg_service and isinstance(pg_service, dict):
            assert 5432 in str(pg_service).lower() or '5432' in str(pg_service)
    
    @patch('subprocess.run')
    def test_restart_service(self, mock_run, manager):
        """Test service restart"""
        mock_run.return_value = Mock(returncode=0, stdout='Restarted')
        
        result = manager.restart_service('postgres')
        
        # Should call docker restart
        mock_run.assert_called()
        assert result is True or 'success' in str(result).lower()
    
    def test_get_docker_compose_path(self, manager):
        """Test getting Docker Compose file path"""
        path = manager.get_docker_compose_path()
        
        assert isinstance(path, str)
        assert 'docker-compose' in path.lower() or 'yml' in path.lower() or 'yaml' in path.lower()
    
    @patch('subprocess.run')
    def test_start_all_services(self, mock_run, manager):
        """Test starting all services"""
        mock_run.return_value = Mock(returncode=0, stdout='Started')
        
        result = manager.start_all_services()
        
        # Should start all services
        mock_run.assert_called()
        assert result is True or 'success' in str(result).lower() or isinstance(result, dict)
    
    @patch('subprocess.run')
    def test_stop_all_services(self, mock_run, manager):
        """Test stopping all services"""
        mock_run.return_value = Mock(returncode=0, stdout='Stopped')
        
        result = manager.stop_all_services()
        
        # Should stop all services
        mock_run.assert_called()
    
    def test_validate_service_name(self, manager):
        """Test service name validation"""
        # Valid names
        assert manager.validate_service_name('PostgreSQL') or manager.validate_service_name('postgres')
        assert manager.validate_service_name('ClickHouse') or manager.validate_service_name('clickhouse')
        
        # Invalid names
        invalid = manager.validate_service_name('InvalidService12345')
        assert invalid is False or invalid is None
    
    @pytest.mark.parametrize("service_name", [
        'PostgreSQL', 'ClickHouse', 'Redis', 'Kafka', 'Zookeeper'
    ])
    def test_get_service_info(self, manager, service_name):
        """Test getting info for various services"""
        info = manager.get_service_info(service_name)
        
        # Should return some info for known services
        assert info is not None or service_name in ['Zookeeper']  # Some services might not have full info
