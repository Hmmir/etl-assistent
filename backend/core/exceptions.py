"""
Custom exceptions для ETL Assistant
"""


class ETLException(Exception):
    """Базовое исключение для ETL Assistant"""
    
    def __init__(self, message: str, code: str = "ETL_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)


class ValidationException(ETLException):
    """Исключение валидации данных"""
    
    def __init__(self, message: str):
        super().__init__(message, code="VALIDATION_ERROR")


class FileNotFoundException(ETLException):
    """Файл не найден"""
    
    def __init__(self, filename: str):
        super().__init__(f"Файл не найден: {filename}", code="FILE_NOT_FOUND")


class UnsupportedFormatException(ETLException):
    """Неподдерживаемый формат файла"""
    
    def __init__(self, format_type: str):
        super().__init__(
            f"Неподдерживаемый формат файла: {format_type}",
            code="UNSUPPORTED_FORMAT"
        )


class GenerationException(ETLException):
    """Ошибка генерации кода"""
    
    def __init__(self, message: str):
        super().__init__(message, code="GENERATION_ERROR")


class InfrastructureException(ETLException):
    """Ошибка инфраструктуры"""
    
    def __init__(self, message: str):
        super().__init__(message, code="INFRASTRUCTURE_ERROR")
