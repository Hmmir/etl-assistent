"""
Core utilities для ETL Assistant

Модуль содержит общие утилиты, конфигурацию и логирование
"""

from .config import settings
from .exceptions import ETLException, ValidationException

__all__ = ['settings', 'ETLException', 'ValidationException']
