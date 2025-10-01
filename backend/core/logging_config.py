"""
Enhanced logging configuration для мониторинга пользователя
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler

# Создаем директорию для логов
LOGS_DIR = Path(__file__).parent.parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Форматы логов
DETAILED_FORMAT = "[%(asctime)s] %(levelname)-8s [%(name)s:%(funcName)s:%(lineno)d] %(message)s"
SIMPLE_FORMAT = "[%(asctime)s] %(levelname)-8s %(message)s"

def setup_logging(log_level: str = "INFO"):
    """
    Настройка детального логирования
    
    Создает 3 лога:
    - app.log - все логи приложения
    - requests.log - HTTP запросы
    - errors.log - только ошибки
    """
    
    # Основной логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Очистка существующих handlers
    root_logger.handlers.clear()
    
    # 1. Console handler (для разработки)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(SIMPLE_FORMAT))
    root_logger.addHandler(console_handler)
    
    # 2. File handler - все логи
    app_log = LOGS_DIR / f"app_{datetime.now().strftime('%Y%m%d')}.log"
    app_handler = RotatingFileHandler(
        app_log, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    app_handler.setLevel(logging.DEBUG)
    app_handler.setFormatter(logging.Formatter(DETAILED_FORMAT))
    root_logger.addHandler(app_handler)
    
    # 3. File handler - только ошибки
    error_log = LOGS_DIR / f"errors_{datetime.now().strftime('%Y%m%d')}.log"
    error_handler = RotatingFileHandler(
        error_log,
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(DETAILED_FORMAT))
    root_logger.addHandler(error_handler)
    
    # 4. File handler - HTTP запросы
    requests_log = LOGS_DIR / f"requests_{datetime.now().strftime('%Y%m%d')}.log"
    requests_handler = RotatingFileHandler(
        requests_log,
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    requests_handler.setLevel(logging.INFO)
    requests_handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(message)s"
    ))
    
    # Создаем отдельный логгер для requests
    requests_logger = logging.getLogger("requests")
    requests_logger.addHandler(requests_handler)
    requests_logger.setLevel(logging.INFO)
    
    logging.info(f"Логирование настроено: {app_log}")
    logging.info(f"Лог ошибок: {error_log}")
    logging.info(f"Лог запросов: {requests_log}")
    
    return root_logger


def log_request(endpoint: str, method: str, params: dict = None, user_data: dict = None):
    """Логирование HTTP запроса"""
    logger = logging.getLogger("requests")
    logger.info(f"{method} {endpoint} | Params: {params} | Data: {user_data}")


def log_user_action(action: str, details: dict = None):
    """Логирование действий пользователя"""
    logger = logging.getLogger("requests")
    logger.info(f"USER_ACTION: {action} | Details: {details}")


def log_error(error: Exception, context: str = ""):
    """Логирование ошибки с контекстом"""
    logger = logging.getLogger()
    logger.error(f"ERROR in {context}: {type(error).__name__}: {str(error)}", exc_info=True)
