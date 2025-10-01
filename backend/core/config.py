"""
Configuration module для ETL Assistant
"""

import os
from pathlib import Path
from typing import Optional


class Settings:
    """Настройки приложения"""
    
    # Пути
    BASE_DIR: Path = Path(__file__).parent.parent.parent
    BACKEND_DIR: Path = BASE_DIR / "backend"
    UPLOAD_DIR: Path = BACKEND_DIR / "uploads"
    
    # API настройки
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    API_RELOAD: bool = os.getenv("API_RELOAD", "true").lower() == "true"
    
    # OpenRouter API
    OPENROUTER_API_KEY: Optional[str] = os.getenv("OPENROUTER_API_KEY")
    # Используем бесплатную модель Qwen Coder для генерации кода
    OPENROUTER_MODEL: str = os.getenv("OPENROUTER_MODEL", "qwen/qwen-2.5-coder-32b-instruct:free")
    
    # Логирование
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # Хранилища данных
    CLICKHOUSE_HOST: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT: int = int(os.getenv("CLICKHOUSE_PORT", "9000"))
    CLICKHOUSE_DB: str = os.getenv("CLICKHOUSE_DB", "default")
    
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "etl_data")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "password")
    
    def __init__(self):
        """Инициализация и создание необходимых директорий"""
        self.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


# Singleton instance
settings = Settings()
