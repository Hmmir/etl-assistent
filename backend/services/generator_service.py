"""
Модуль генерации ETL кода для ETL Assistant

Модуль 5: Генерация Python/SQL скриптов на основе анализа данных
"""

import os
import textwrap
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path


class PythonETLGenerator:
    """
    Генератор Python скриптов для ETL процессов
    """
    
    def __init__(self):
        self.templates = {
            'csv_reader': self._get_csv_reader_template(),
            'json_reader': self._get_json_reader_template(), 
            'xml_reader': self._get_xml_reader_template(),
            'data_cleaner': self._get_data_cleaner_template(),
            'clickhouse_loader': self._get_clickhouse_loader_template(),
            'postgresql_loader': self._get_postgresql_loader_template(),
            'hdfs_loader': self._get_hdfs_loader_template()
        }
    
    def generate_ddl(self, analysis: Dict[str, Any], storage_type: str, table_name: str = None) -> str:
        """
        Генерация DDL скрипта для создания таблицы в выбранном хранилище
        
        Args:
            analysis: Результат анализа данных
            storage_type: Тип хранилища ('PostgreSQL', 'ClickHouse', 'MySQL', 'SQLite', 'HDFS')
            table_name: Имя таблицы (опционально, будет сгенерировано из filename если не указано)
            
        Returns:
            str: DDL скрипт для создания таблицы
        """
        # Определяем имя таблицы
        if not table_name:
            filename = analysis.get('filename', 'data_table')
            table_name = filename.replace('.', '_').replace('-', '_').lower()
            # Удаляем расширение файла из имени таблицы
            for ext in ['.csv', '.json', '.xml']:
                table_name = table_name.replace(ext, '')
        
        # Получаем список колонок
        columns = analysis.get('columns', analysis.get('fields', []))
        
        if not columns:
            return "-- Не удалось определить структуру данных для генерации DDL"
        
        # Выбираем генератор в зависимости от типа хранилища
        storage_type_upper = storage_type.upper()
        
        if storage_type_upper == 'CLICKHOUSE':
            return self._generate_clickhouse_ddl(table_name, columns, analysis)
        elif storage_type_upper in ['POSTGRESQL', 'POSTGRES']:
            return self._generate_postgresql_ddl(table_name, columns, analysis)
        elif storage_type_upper == 'MYSQL':
            return self._generate_mysql_ddl(table_name, columns, analysis)
        elif storage_type_upper == 'SQLITE':
            return self._generate_sqlite_ddl(table_name, columns, analysis)
        elif storage_type_upper == 'HDFS':
            return self._generate_hive_ddl(table_name, columns, analysis)
        else:
            return f"-- Генерация DDL не поддерживается для хранилища: {storage_type}"
    
    def generate_etl_script(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any]) -> str:
        """
        Генерация полного Python ETL скрипта
        
        Args:
            analysis: Результат анализа данных
            storage_recommendation: Рекомендации по хранилищу
            
        Returns:
            str: Готовый Python код ETL процесса
        """
        format_type = analysis.get('format_type', 'CSV')
        storage = storage_recommendation['recommended_storage']
        filename = analysis.get('filename', 'data.csv')
        
        # Генерируем компоненты
        imports = self._generate_imports(format_type, storage)
        config = self._generate_config(analysis, storage_recommendation)
        reader = self._generate_reader(format_type, analysis)
        cleaner = self._generate_data_cleaner(analysis)
        transformer = self._generate_transformer(analysis)
        loader = self._generate_loader(storage, analysis, storage_recommendation)
        main_function = self._generate_main_function(filename)
        
        # Собираем полный скрипт
        script = f'''#!/usr/bin/env python3
"""
ETL скрипт для обработки {filename}

Автоматически сгенерирован ETL Assistant
Дата создания: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Источник: {format_type} файл ({analysis.get('file_size_mb', 0)} MB)
Целевое хранилище: {storage}
Строк данных: {analysis.get('total_rows', analysis.get('total_records', 0)):,}
Колонок: {analysis.get('total_columns', analysis.get('total_fields', 0))}
"""

{imports}

{config}

{reader}

{cleaner}

{transformer}

{loader}

{main_function}

if __name__ == "__main__":
    main()
'''
        
        return script
    
    def _generate_imports(self, format_type: str, storage: str) -> str:
        """Генерация блока импортов"""
        imports = [
            "import pandas as pd",
            "import logging",
            "import os",
            "from pathlib import Path",
            "from datetime import datetime, timedelta",
            "import sys"
        ]
        
        if format_type == 'JSON':
            imports.append("import json")
        elif format_type == 'XML':
            imports.append("import xml.etree.ElementTree as ET")
        
        if storage == 'ClickHouse':
            imports.extend([
                "from clickhouse_driver import Client",
                "import clickhouse_driver"
            ])
        elif storage == 'PostgreSQL':
            imports.extend([
                "import psycopg2",
                "from psycopg2.extras import execute_batch"
            ])
        elif storage == 'HDFS':
            imports.extend([
                "import hdfs3",
                "import pyarrow as pa",
                "import pyarrow.parquet as pq"
            ])
        
        imports.append("import warnings")
        imports.append("warnings.filterwarnings('ignore')")
        
        return "\\n".join(imports)
    
    def _generate_config(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any]) -> str:
        """Генерация блока конфигурации"""
        storage = storage_recommendation['recommended_storage']
        
        config_lines = [
            "# Конфигурация ETL процесса",
            "logging.basicConfig(",
            "    level=logging.INFO,",
            "    format='%(asctime)s - %(levelname)s - %(message)s'",
            ")",
            "logger = logging.getLogger(__name__)",
            "",
            "# Настройки источника данных",
            f"SOURCE_FILE = '{analysis.get('filename', 'data.csv')}'",
        ]
        
        if analysis.get('encoding'):
            config_lines.append(f"ENCODING = '{analysis.get('encoding')}'")
        
        if analysis.get('separator'):
            config_lines.append(f"SEPARATOR = '{analysis.get('separator')}'")
        
        config_lines.extend([
            f"BATCH_SIZE = {self._get_batch_size(analysis)}",
            "",
            "# Настройки целевого хранилища"
        ])
        
        if storage == 'ClickHouse':
            config_lines.extend([
                "CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')",
                "CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 9000))",
                "CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DB', 'default')",
                f"TARGET_TABLE = '{self._get_table_name(analysis)}'",
            ])
        elif storage == 'PostgreSQL':
            config_lines.extend([
                "POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')",
                "POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))",
                "POSTGRES_DATABASE = os.getenv('POSTGRES_DB', 'etl_data')",
                "POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')",
                "POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')",
                f"TARGET_TABLE = '{self._get_table_name(analysis)}'",
            ])
        elif storage == 'HDFS':
            config_lines.extend([
                "HDFS_HOST = os.getenv('HDFS_HOST', 'localhost')",
                "HDFS_PORT = int(os.getenv('HDFS_PORT', 9000))",
                f"HDFS_PATH = '/data/{self._get_table_name(analysis)}'",
            ])
        
        return "\\n".join(config_lines)
    
    def _generate_reader(self, format_type: str, analysis: Dict[str, Any]) -> str:
        """Генерация функции чтения данных"""
        if format_type == 'CSV':
            return self._generate_csv_reader(analysis)
        elif format_type == 'JSON':
            return self._generate_json_reader(analysis)
        elif format_type == 'XML':
            return self._generate_xml_reader(analysis)
        else:
            return self._get_csv_reader_template()
    
    def _generate_csv_reader(self, analysis: Dict[str, Any]) -> str:
        """Генерация CSV reader с учетом анализа"""
        encoding = analysis.get('encoding', 'utf-8')
        separator = analysis.get('separator', ',')
        total_rows = analysis.get('total_rows', 0)
        
        return f'''
def read_source_data(file_path: str, chunk_size: int = BATCH_SIZE) -> pd.DataFrame:
    """
    Чтение CSV файла с оптимизацией для больших данных
    
    Файл: {total_rows:,} строк, кодировка {encoding}, разделитель '{separator}'
    """
    logger.info(f"Начинаем чтение CSV файла: {{file_path}}")
    
    try:
        # Определение типов колонок для оптимизации
        dtype_map = {{
{self._generate_dtype_map(analysis)}
        }}
        
        # Чтение файла по частям для больших данных
        if chunk_size and {total_rows} > chunk_size:
            logger.info("Используем чтение по частям для большого файла")
            chunks = []
            
            for chunk in pd.read_csv(
                file_path,
                sep='{separator}',
                encoding='{encoding}',
                chunksize=chunk_size,
                dtype=dtype_map,
                low_memory=False
            ):
                logger.info(f"Прочитан чанк размером {{len(chunk)}} строк")
                chunks.append(chunk)
                
                # Ограничение памяти - обрабатываем по частям
                if len(chunks) >= 10:
                    logger.info("Объединяем накопленные чанки")
                    partial_df = pd.concat(chunks, ignore_index=True)
                    chunks = [partial_df]
            
            df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        else:
            # Обычное чтение для небольших файлов
            df = pd.read_csv(
                file_path,
                sep='{separator}',
                encoding='{encoding}',
                dtype=dtype_map,
                low_memory=False
            )
        
        logger.info(f"Успешно прочитано {{len(df):,}} строк, {{len(df.columns)}} колонок")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка чтения CSV файла: {{str(e)}}")
        raise
'''
    
    def _generate_dtype_map(self, analysis: Dict[str, Any]) -> str:
        """Генерация mapping типов данных для pandas"""
        columns = analysis.get('columns', [])
        dtype_lines = []
        
        for col in columns:
            col_name = col['name']
            data_type = col['data_type']
            
            if data_type == 'integer':
                dtype_lines.append(f"            '{col_name}': 'Int64',")
            elif data_type == 'decimal':
                dtype_lines.append(f"            '{col_name}': 'float64',")
            elif data_type == 'boolean':
                dtype_lines.append(f"            '{col_name}': 'boolean',")
            elif data_type == 'datetime':
                # Дата будет обработана отдельно
                dtype_lines.append(f"            '{col_name}': 'str',")
            else:
                dtype_lines.append(f"            '{col_name}': 'str',")
        
        return "\\n".join(dtype_lines)
    
    def _generate_json_reader(self, analysis: Dict[str, Any]) -> str:
        """Генерация JSON reader"""
        structure_type = analysis.get('structure_type', 'array')
        
        if structure_type == 'array':
            return f'''
def read_source_data(file_path: str, chunk_size: int = None) -> pd.DataFrame:
    """
    Чтение JSON файла (массив объектов)
    """
    logger.info(f"Начинаем чтение JSON файла: {{file_path}}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            df = pd.json_normalize(data)
            logger.info(f"Успешно прочитано {{len(df):,}} записей")
            return df
        else:
            raise ValueError("JSON файл должен содержать массив объектов")
            
    except Exception as e:
        logger.error(f"Ошибка чтения JSON файла: {{str(e)}}")
        raise
'''
        else:
            return '''
def read_source_data(file_path: str, chunk_size: int = None) -> pd.DataFrame:
    """
    Чтение JSON файла (одиночный объект)
    """
    logger.info(f"Начинаем чтение JSON файла: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Преобразуем объект в DataFrame
        df = pd.json_normalize([data])
        logger.info(f"Успешно прочитан JSON объект")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка чтения JSON файла: {str(e)}")
        raise
'''
    
    def _generate_xml_reader(self, analysis: Dict[str, Any]) -> str:
        """Генерация XML reader"""
        return '''
def read_source_data(file_path: str, chunk_size: int = None) -> pd.DataFrame:
    """
    Чтение XML файла
    """
    logger.info(f"Начинаем чтение XML файла: {file_path}")
    
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Находим повторяющиеся элементы (записи)
        records = []
        for record_element in root:
            record = {}
            for field in record_element:
                record[field.tag] = field.text
            records.append(record)
        
        df = pd.DataFrame(records)
        logger.info(f"Успешно прочитано {len(df):,} записей из XML")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка чтения XML файла: {str(e)}")
        raise
'''
    
    def _generate_data_cleaner(self, analysis: Dict[str, Any]) -> str:
        """Генерация функции очистки данных"""
        columns = analysis.get('columns', analysis.get('fields', []))
        
        cleaning_steps = []
        datetime_columns = []
        
        for col in columns:
            col_name = col['name']
            data_type = col['data_type']
            null_count = col.get('null_count', 0)
            
            if null_count > 0:
                if data_type in ['integer', 'decimal']:
                    cleaning_steps.append(f"    # Заполнение пропусков в {col_name}")
                    cleaning_steps.append(f"    df['{col_name}'] = df['{col_name}'].fillna(0)")
                elif data_type == 'text':
                    cleaning_steps.append(f"    df['{col_name}'] = df['{col_name}'].fillna('Unknown')")
            
            if data_type == 'datetime':
                datetime_columns.append(col_name)
        
        if datetime_columns:
            cleaning_steps.extend([
                "    # Конвертация datetime колонок",
                "    datetime_columns = [" + ", ".join([f"'{col}'" for col in datetime_columns]) + "]",
                "    for col in datetime_columns:",
                "        try:",
                "            df[col] = pd.to_datetime(df[col], errors='coerce')",
                "            logger.info(f'Конвертирована datetime колонка: {col}')",
                "        except Exception as e:",
                "            logger.warning(f'Не удалось конвертировать {col}: {str(e)}')"
            ])
        
        cleaning_code = "\\n".join(cleaning_steps) if cleaning_steps else "    # Данные не требуют очистки"
        
        return f'''
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Очистка и подготовка данных
    """
    logger.info("Начинаем очистку данных")
    original_rows = len(df)
    
    # Удаление полностью пустых строк
    df = df.dropna(how='all')
    logger.info(f"Удалено {{original_rows - len(df)}} полностью пустых строк")
    
{cleaning_code}
    
    # Удаление дублированных записей
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        df = df.drop_duplicates()
        logger.info(f"Удалено {{duplicates}} дублированных записей")
    
    logger.info(f"Очистка завершена: {{len(df):,}} строк готовы к загрузке")
    return df
'''
    
    def _generate_transformer(self, analysis: Dict[str, Any]) -> str:
        """Генерация функции трансформации данных"""
        return '''
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Трансформация данных перед загрузкой
    """
    logger.info("Начинаем трансформацию данных")
    
    # Добавление служебных колонок
    df['etl_load_date'] = datetime.now()
    df['etl_source_file'] = SOURCE_FILE
    
    # Дополнительные трансформации можно добавить здесь
    logger.info("Трансформация завершена")
    return df
'''
    
    def _generate_loader(self, storage: str, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any]) -> str:
        """Генерация функции загрузки в целевое хранилище"""
        if storage == 'ClickHouse':
            return self._generate_clickhouse_loader(analysis, storage_recommendation)
        elif storage == 'PostgreSQL':
            return self._generate_postgresql_loader(analysis, storage_recommendation)
        elif storage == 'HDFS':
            return self._generate_hdfs_loader(analysis, storage_recommendation)
        else:
            return "# Загрузчик для данного хранилища не реализован"
    
    def _generate_clickhouse_loader(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any]) -> str:
        """Генерация ClickHouse loader"""
        table_name = self._get_table_name(analysis)
        ddl = storage_recommendation.get('ddl_script', '')
        
        return f'''
def load_to_clickhouse(df: pd.DataFrame) -> None:
    """
    Загрузка данных в ClickHouse
    """
    logger.info("Подключаемся к ClickHouse")
    
    try:
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DATABASE
        )
        
        # Проверяем существование таблицы
        table_exists = client.execute(
            "SELECT count() FROM system.tables WHERE database = %(db)s AND name = %(table)s",
            {{'db': CLICKHOUSE_DATABASE, 'table': TARGET_TABLE}}
        )[0][0] > 0
        
        if not table_exists:
            logger.info("Создаем таблицу")
            ddl_script = """
{textwrap.indent(ddl, "            ")}
            """
            client.execute(ddl_script)
            logger.info(f"Таблица {{TARGET_TABLE}} создана")
        
        # Загружаем данные батчами
        batch_size = BATCH_SIZE
        total_rows = len(df)
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]
            
            # Конвертируем DataFrame в список кортежей
            data_tuples = [tuple(row) for row in batch.values]
            
            # Создаем плейсхолдеры для INSERT
            placeholders = "(" + ",".join(["%s"] * len(batch.columns)) + ")"
            
            insert_query = f"INSERT INTO {{TARGET_TABLE}} VALUES {{placeholders}}"
            
            client.execute(insert_query, data_tuples)
            
            logger.info(f"Загружен батч {{i+1}}-{{min(i+batch_size, total_rows)}} из {{total_rows}}")
        
        logger.info(f"Успешно загружено {{total_rows:,}} записей в ClickHouse")
        
    except Exception as e:
        logger.error(f"Ошибка загрузки в ClickHouse: {{str(e)}}")
        raise
'''
    
    def _generate_postgresql_loader(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any]) -> str:
        """Генерация PostgreSQL loader"""
        ddl = storage_recommendation.get('ddl_script', '')
        
        return f'''
def load_to_postgresql(df: pd.DataFrame) -> None:
    """
    Загрузка данных в PostgreSQL
    """
    logger.info("Подключаемся к PostgreSQL")
    
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DATABASE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        
        cur = conn.cursor()
        
        # Проверяем существование таблицы
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = %s
            )
        """, (TARGET_TABLE,))
        
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            logger.info("Создаем таблицу")
            ddl_script = """
{textwrap.indent(ddl, "            ")}
            """
            cur.execute(ddl_script)
            logger.info(f"Таблица {{TARGET_TABLE}} создана")
        
        # Загружаем данные с помощью execute_batch для высокой производительности
        columns = list(df.columns)
        placeholders = ",".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {{TARGET_TABLE}} ({{','.join(columns)}}) VALUES ({{placeholders}})"
        
        data_tuples = [tuple(row) for row in df.values]
        
        execute_batch(
            cur, 
            insert_query, 
            data_tuples, 
            page_size=BATCH_SIZE
        )
        
        conn.commit()
        logger.info(f"Успешно загружено {{len(df):,}} записей в PostgreSQL")
        
    except Exception as e:
        logger.error(f"Ошибка загрузки в PostgreSQL: {{str(e)}}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
'''
    
    def _generate_hdfs_loader(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any]) -> str:
        """Генерация HDFS loader"""
        return '''
def load_to_hdfs(df: pd.DataFrame) -> None:
    """
    Загрузка данных в HDFS в формате Parquet
    """
    logger.info("Подключаемся к HDFS")
    
    try:
        # Подключение к HDFS
        hdfs = hdfs3.HDFileSystem(host=HDFS_HOST, port=HDFS_PORT)
        
        # Создаем папку если не существует
        if not hdfs.exists(HDFS_PATH):
            hdfs.makedirs(HDFS_PATH)
            logger.info(f"Создана папка {HDFS_PATH}")
        
        # Сохраняем в формате Parquet для оптимальной производительности
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"{HDFS_PATH}/data_{timestamp}.parquet"
        
        # Конвертируем в PyArrow Table для эффективного сохранения
        table = pa.Table.from_pandas(df)
        
        with hdfs.open(file_path, 'wb') as hdfs_file:
            pq.write_table(table, hdfs_file)
        
        logger.info(f"Успешно загружено {len(df):,} записей в HDFS: {file_path}")
        
    except Exception as e:
        logger.error(f"Ошибка загрузки в HDFS: {str(e)}")
        raise
'''
    
    def _generate_main_function(self, filename: str) -> str:
        """Генерация main функции"""
        return f'''
def main():
    """
    Основная функция ETL процесса
    """
    start_time = datetime.now()
    logger.info("=== НАЧАЛО ETL ПРОЦЕССА ===")
    logger.info(f"Обрабатываем файл: {{SOURCE_FILE}}")
    
    try:
        # Шаг 1: Чтение исходных данных
        logger.info("Шаг 1: Чтение данных")
        df = read_source_data(SOURCE_FILE)
        
        if df.empty:
            logger.warning("Файл пуст, завершаем процесс")
            return
        
        # Шаг 2: Очистка данных
        logger.info("Шаг 2: Очистка данных")
        df_clean = clean_data(df)
        
        # Шаг 3: Трансформация данных
        logger.info("Шаг 3: Трансформация данных")
        df_transformed = transform_data(df_clean)
        
        # Шаг 4: Загрузка данных
        logger.info("Шаг 4: Загрузка данных")
        load_function_map = {{
            'ClickHouse': load_to_clickhouse,
            'PostgreSQL': load_to_postgresql, 
            'HDFS': load_to_hdfs
        }}
        
        storage_type = globals().get('STORAGE_TYPE', 'ClickHouse')
        load_function = load_function_map.get(storage_type)
        
        if load_function:
            load_function(df_transformed)
        else:
            logger.error(f"Неподдерживаемый тип хранилища: {{storage_type}}")
            return
        
        # Статистика выполнения
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=== ETL ПРОЦЕСС ЗАВЕРШЕН ===")
        logger.info(f"Время выполнения: {{duration}}")
        logger.info(f"Обработано записей: {{len(df_transformed):,}}")
        logger.info(f"Средняя скорость: {{len(df_transformed) / duration.total_seconds():.2f}} записей/сек")
        
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА ETL ПРОЦЕССА: {{str(e)}}")
        sys.exit(1)
'''
    
    def _get_batch_size(self, analysis: Dict[str, Any]) -> int:
        """Определение оптимального размера батча"""
        total_rows = analysis.get('total_rows', analysis.get('total_records', 0))
        
        if total_rows < 10000:
            return 1000
        elif total_rows < 100000:
            return 5000
        elif total_rows < 1000000:
            return 10000
        else:
            return 25000
    
    def _get_table_name(self, analysis: Dict[str, Any]) -> str:
        """Генерация имени таблицы из имени файла"""
        filename = analysis.get('filename', 'data.csv')
        # Убираем расширение и заменяем специальные символы
        table_name = Path(filename).stem.lower()
        table_name = table_name.replace('-', '_').replace(' ', '_')
        # Убираем все кроме букв, цифр и подчеркиваний
        table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
        return table_name or 'etl_data'
    
    def _get_csv_reader_template(self) -> str:
        """Базовый шаблон CSV reader"""
        return "# CSV reader template"
    
    def _get_json_reader_template(self) -> str:
        """Базовый шаблон JSON reader"""
        return "# JSON reader template"
    
    def _get_xml_reader_template(self) -> str:
        """Базовый шаблон XML reader"""
        return "# XML reader template"
    
    def _get_data_cleaner_template(self) -> str:
        """Базовый шаблон data cleaner"""
        return "# Data cleaner template"
    
    def _get_clickhouse_loader_template(self) -> str:
        """Базовый шаблон ClickHouse loader"""
        return "# ClickHouse loader template"
    
    def _get_postgresql_loader_template(self) -> str:
        """Базовый шаблон PostgreSQL loader"""
        return "# PostgreSQL loader template"
    
    def _get_hdfs_loader_template(self) -> str:
        """Базовый шаблон HDFS loader"""
        return "# HDFS loader template"
    
    def _generate_clickhouse_ddl(self, table_name: str, columns: List[Dict], analysis: Dict) -> str:
        """Генерация DDL для ClickHouse"""
        ddl_columns = []
        partition_key = None
        order_keys = []
        
        for col in columns:
            col_name = col['name'].replace(' ', '_').replace('-', '_')
            
            # Маппинг типов данных
            col_type = col.get('data_type', col.get('type', 'text')).lower()
            if 'int' in col_type:
                ch_type = 'Int64'
            elif col_type in ['decimal', 'float', 'double', 'numeric']:
                ch_type = 'Float64'
            elif col_type in ['datetime', 'timestamp']:
                ch_type = 'DateTime'
                if partition_key is None:
                    partition_key = f"toYYYYMM({col_name})"
                order_keys.append(col_name)
            elif col_type in ['date']:
                ch_type = 'Date'
                order_keys.append(col_name)
            elif col_type in ['boolean', 'bool']:
                ch_type = 'UInt8'
            else:
                ch_type = 'String'
            
            ddl_columns.append(f"    {col_name} {ch_type}")
        
        # Если нет даты для партицирования, используем первую колонку для order by
        if not order_keys and columns:
            order_keys.append(columns[0]['name'].replace(' ', '_').replace('-', '_'))
        
        columns_str = ',\n'.join(ddl_columns)
        ddl = f"""CREATE TABLE {table_name} (
{columns_str}
) ENGINE = MergeTree()"""
        
        if order_keys:
            ddl += f"\nORDER BY ({', '.join(order_keys)})"
        
        if partition_key:
            ddl += f"\nPARTITION BY {partition_key}"
        
        ddl += ";"
        
        return ddl
    
    def _generate_postgresql_ddl(self, table_name: str, columns: List[Dict], analysis: Dict) -> str:
        """Генерация DDL для PostgreSQL"""
        ddl_columns = []
        indexes = []
        
        for col in columns:
            col_name = col['name'].replace(' ', '_').replace('-', '_').lower()
            
            # Маппинг типов данных
            col_type = col.get('data_type', col.get('type', 'text')).lower()
            if 'int' in col_type:
                pg_type = 'BIGINT'
            elif col_type in ['decimal', 'float', 'double', 'numeric']:
                pg_type = 'NUMERIC'
            elif col_type in ['datetime', 'timestamp']:
                pg_type = 'TIMESTAMP'
                indexes.append(f"CREATE INDEX idx_{table_name}_{col_name} ON {table_name}({col_name});")
            elif col_type == 'date':
                pg_type = 'DATE'
            elif col_type in ['boolean', 'bool']:
                pg_type = 'BOOLEAN'
            else:
                # Определяем размер VARCHAR на основе примеров
                sample_values = col.get('sample_values', [''])
                max_length = max([len(str(v)) for v in sample_values], default=255)
                if max_length > 255:
                    pg_type = 'TEXT'
                else:
                    pg_type = f'VARCHAR({min(max_length + 50, 500)})'
            
            ddl_columns.append(f"    {col_name} {pg_type}")
        
        columns_str = ',\n'.join(ddl_columns)
        ddl = f"""CREATE TABLE {table_name} (
{columns_str}
);"""
        
        if indexes:
            ddl += "\n\n-- Рекомендуемые индексы:\n" + "\n".join(indexes)
        
        return ddl
    
    def _generate_mysql_ddl(self, table_name: str, columns: List[Dict], analysis: Dict) -> str:
        """Генерация DDL для MySQL"""
        ddl_columns = []
        indexes = []
        
        for col in columns:
            col_name = col['name'].replace(' ', '_').replace('-', '_').lower()
            
            # Маппинг типов данных
            col_type = col.get('data_type', col.get('type', 'text')).lower()
            if 'int' in col_type:
                mysql_type = 'BIGINT'
            elif col_type in ['decimal', 'float', 'double', 'numeric']:
                mysql_type = 'DECIMAL(18,2)'
            elif col_type in ['datetime', 'timestamp']:
                mysql_type = 'DATETIME'
                indexes.append(f"INDEX idx_{col_name} ({col_name})")
            elif col_type == 'date':
                mysql_type = 'DATE'
            elif col_type in ['boolean', 'bool']:
                mysql_type = 'TINYINT(1)'
            else:
                sample_values = col.get('sample_values', [''])
                max_length = max([len(str(v)) for v in sample_values], default=255)
                if max_length > 255:
                    mysql_type = 'TEXT'
                else:
                    mysql_type = f'VARCHAR({min(max_length + 50, 500)})'
            
            ddl_columns.append(f"    {col_name} {mysql_type}")
        
        all_columns = ddl_columns + indexes if indexes else ddl_columns
        columns_str = ',\n'.join(all_columns)
        
        ddl = f"""CREATE TABLE {table_name} (
{columns_str}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"""
        
        return ddl
    
    def _generate_sqlite_ddl(self, table_name: str, columns: List[Dict], analysis: Dict) -> str:
        """Генерация DDL для SQLite"""
        ddl_columns = []
        
        for col in columns:
            col_name = col['name'].replace(' ', '_').replace('-', '_').lower()
            
            # Маппинг типов данных (SQLite имеет гибкую систему типов)
            col_type = col.get('data_type', col.get('type', 'text')).lower()
            if 'int' in col_type:
                sqlite_type = 'INTEGER'
            elif col_type in ['decimal', 'float', 'double', 'numeric']:
                sqlite_type = 'REAL'
            elif col_type in ['datetime', 'timestamp', 'date']:
                sqlite_type = 'TEXT'  # SQLite хранит даты как TEXT
            elif col_type in ['boolean', 'bool']:
                sqlite_type = 'INTEGER'  # SQLite хранит boolean как INTEGER
            else:
                sqlite_type = 'TEXT'
            
            ddl_columns.append(f"    {col_name} {sqlite_type}")
        
        columns_str = ',\n'.join(ddl_columns)
        ddl = f"""CREATE TABLE {table_name} (
{columns_str}
);"""
        
        return ddl
    
    def _generate_hive_ddl(self, table_name: str, columns: List[Dict], analysis: Dict) -> str:
        """Генерация DDL для Hive (HDFS)"""
        ddl_columns = []
        
        for col in columns:
            col_name = col['name'].replace(' ', '_').replace('-', '_').lower()
            
            # Маппинг типов данных для Hive
            col_type = col.get('data_type', col.get('type', 'text')).lower()
            if 'int' in col_type:
                hive_type = 'BIGINT'
            elif col_type in ['decimal', 'float', 'double', 'numeric']:
                hive_type = 'DOUBLE'
            elif col_type in ['datetime', 'timestamp']:
                hive_type = 'TIMESTAMP'
            elif col_type == 'date':
                hive_type = 'DATE'
            elif col_type in ['boolean', 'bool']:
                hive_type = 'BOOLEAN'
            else:
                hive_type = 'STRING'
            
            ddl_columns.append(f"    {col_name} {hive_type}")
        
        columns_str = ',\n'.join(ddl_columns)
        ddl = f"""CREATE EXTERNAL TABLE {table_name} (
{columns_str}
)
STORED AS PARQUET
LOCATION '/data/{table_name}/';"""
        
        return ddl


class SQLScriptGenerator:
    """
    Генератор SQL скриптов для трансформации данных
    """
    
    def __init__(self):
        self.dialects = ['clickhouse', 'postgresql', 'mysql']
    
    def generate_transformation_sql(self, analysis: Dict[str, Any], storage: str) -> Dict[str, str]:
        """
        Генерация SQL скриптов для трансформации данных
        
        Args:
            analysis: Результат анализа данных
            storage: Тип хранилища
            
        Returns:
            dict: SQL скрипты по типам операций
        """
        table_name = self._get_table_name(analysis)
        columns = analysis.get('columns', analysis.get('fields', []))
        
        scripts = {
            'data_quality': self._generate_data_quality_sql(table_name, columns, storage),
            'aggregations': self._generate_aggregation_sql(table_name, columns, storage),
            'transformations': self._generate_transformation_sql(table_name, columns, storage)
        }
        
        return scripts
    
    def _generate_data_quality_sql(self, table_name: str, columns: List[Dict], storage: str) -> str:
        """Генерация SQL для проверки качества данных"""
        checks = []
        
        # Базовые проверки
        checks.extend([
            f"-- Проверка качества данных для таблицы {table_name}",
            f"-- Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "-- Общая статистика",
            f"SELECT ",
            f"    COUNT(*) as total_rows,",
            f"    COUNT(DISTINCT *) as unique_rows,",
            f"    COUNT(*) - COUNT(DISTINCT *) as duplicates"
        ])
        
        if storage.lower() == 'clickhouse':
            checks.append(f"FROM {table_name};")
        else:
            checks.append(f"FROM {table_name};")
        
        # Проверки по колонкам
        for col in columns:
            col_name = col['name']
            data_type = col['data_type']
            
            checks.extend([
                "",
                f"-- Статистика по колонке {col_name} ({data_type})",
                f"SELECT ",
                f"    '{col_name}' as column_name,",
                f"    COUNT(*) as total_count,",
                f"    COUNT({col_name}) as non_null_count,",
                f"    COUNT(*) - COUNT({col_name}) as null_count,",
                f"    ROUND((COUNT({col_name}) * 100.0 / COUNT(*)), 2) as fill_rate"
            ])
            
            if data_type in ['integer', 'decimal']:
                if storage.lower() == 'clickhouse':
                    checks.extend([
                        f"    MIN({col_name}) as min_value,",
                        f"    MAX({col_name}) as max_value,",
                        f"    AVG({col_name}) as avg_value"
                    ])
                else:
                    checks.extend([
                        f"    MIN({col_name}) as min_value,",
                        f"    MAX({col_name}) as max_value,",
                        f"    AVG({col_name}::NUMERIC) as avg_value"
                    ])
            
            checks.append(f"FROM {table_name};")
        
        return "\\n".join(checks)
    
    def _generate_aggregation_sql(self, table_name: str, columns: List[Dict], storage: str) -> str:
        """Генерация SQL для агрегации данных"""
        datetime_cols = [col['name'] for col in columns if col['data_type'] == 'datetime']
        numeric_cols = [col['name'] for col in columns if col['data_type'] in ['integer', 'decimal']]
        
        if not datetime_cols or not numeric_cols:
            return "-- Нет подходящих колонок для агрегации"
        
        date_col = datetime_cols[0]
        scripts = [
            f"-- Агрегация данных по времени для таблицы {table_name}",
            f"-- Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ""
        ]
        
        if storage.lower() == 'clickhouse':
            scripts.extend([
                "-- Агрегация по дням",
                f"SELECT ",
                f"    toDate({date_col}) as date,",
                f"    COUNT(*) as record_count,"
            ])
            
            for col in numeric_cols[:3]:  # Берем первые 3 числовые колонки
                scripts.extend([
                    f"    SUM({col}) as total_{col.lower()},",
                    f"    AVG({col}) as avg_{col.lower()},"
                ])
            
            scripts[-1] = scripts[-1].rstrip(',')  # Убираем последнюю запятую
            scripts.extend([
                f"FROM {table_name}",
                f"GROUP BY toDate({date_col})",
                f"ORDER BY date DESC;",
                "",
                "-- Агрегация по часам за последний день",
                f"SELECT ",
                f"    toStartOfHour({date_col}) as hour,",
                f"    COUNT(*) as record_count"
            ])
            
            for col in numeric_cols[:2]:
                scripts.append(f"    SUM({col}) as total_{col.lower()},")
            
            scripts[-1] = scripts[-1].rstrip(',')
            scripts.extend([
                f"FROM {table_name}",
                f"WHERE {date_col} >= today() - INTERVAL 1 DAY",
                f"GROUP BY toStartOfHour({date_col})",
                f"ORDER BY hour DESC;"
            ])
        
        else:  # PostgreSQL
            scripts.extend([
                "-- Агрегация по дням",
                f"SELECT ",
                f"    DATE({date_col}) as date,",
                f"    COUNT(*) as record_count,"
            ])
            
            for col in numeric_cols[:3]:
                scripts.extend([
                    f"    SUM({col}) as total_{col.lower()},",
                    f"    AVG({col}) as avg_{col.lower()},"
                ])
            
            scripts[-1] = scripts[-1].rstrip(',')
            scripts.extend([
                f"FROM {table_name}",
                f"GROUP BY DATE({date_col})",
                f"ORDER BY date DESC;"
            ])
        
        return "\\n".join(scripts)
    
    def _generate_transformation_sql(self, table_name: str, columns: List[Dict], storage: str) -> str:
        """Генерация SQL для трансформации данных"""
        scripts = [
            f"-- Трансформация данных для таблицы {table_name}",
            f"-- Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "-- Создание представления с очищенными данными",
            f"CREATE OR REPLACE VIEW {table_name}_clean AS",
            "SELECT"
        ]
        
        col_transformations = []
        
        for col in columns:
            col_name = col['name']
            data_type = col['data_type']
            
            if data_type == 'text':
                if storage.lower() == 'clickhouse':
                    col_transformations.append(f"    trim({col_name}) as {col_name}")
                else:
                    col_transformations.append(f"    TRIM({col_name}) as {col_name}")
            elif data_type == 'datetime':
                col_transformations.append(f"    {col_name}")
            else:
                col_transformations.append(f"    {col_name}")
        
        # Добавляем служебные колонки
        col_transformations.extend([
            "    CURRENT_TIMESTAMP as processed_at",
            "    'etl_process' as processed_by"
        ])
        
        scripts.append(",\\n".join(col_transformations))
        scripts.extend([
            f"FROM {table_name}",
            "WHERE 1=1",
            "    -- Фильтр некорректных записей можно добавить здесь",
            ";",
            "",
            "-- Пример создания агрегированной таблицы"
        ])
        
        datetime_cols = [col['name'] for col in columns if col['data_type'] == 'datetime']
        if datetime_cols:
            date_col = datetime_cols[0]
            scripts.extend([
                f"CREATE TABLE IF NOT EXISTS {table_name}_daily_summary AS",
                "SELECT"
            ])
            
            if storage.lower() == 'clickhouse':
                scripts.extend([
                    f"    toDate({date_col}) as summary_date,",
                    "    COUNT(*) as daily_record_count"
                ])
            else:
                scripts.extend([
                    f"    DATE({date_col}) as summary_date,",
                    "    COUNT(*) as daily_record_count"
                ])
            
            scripts.extend([
                f"FROM {table_name}",
                f"GROUP BY summary_date;",
            ])
        
        return "\\n".join(scripts)
    
    def _get_table_name(self, analysis: Dict[str, Any]) -> str:
        """Получение имени таблицы"""
        filename = analysis.get('filename', 'data.csv')
        table_name = Path(filename).stem.lower()
        table_name = table_name.replace('-', '_').replace(' ', '_')
        table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
        return table_name or 'etl_data'


class ConfigGenerator:
    """
    Генератор конфигурационных файлов для ETL процессов
    """
    
    def generate_config_files(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any]) -> Dict[str, str]:
        """
        Генерация конфигурационных файлов
        
        Returns:
            dict: Конфигурационные файлы по типам
        """
        configs = {
            'environment': self._generate_env_file(analysis, storage_recommendation),
            'docker_compose': self._generate_docker_compose(analysis, storage_recommendation),
            'requirements': self._generate_requirements_file(storage_recommendation)
        }
        
        return configs
    
    def _generate_env_file(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any]) -> str:
        """Генерация .env файла"""
        storage = storage_recommendation['recommended_storage']
        
        env_lines = [
            "# ETL Environment Configuration",
            f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "# Source Data Configuration",
            f"SOURCE_FILE={analysis.get('filename', 'data.csv')}",
            f"ENCODING={analysis.get('encoding', 'utf-8')}",
        ]
        
        if analysis.get('separator'):
            env_lines.append(f"SEPARATOR={analysis.get('separator')}")
        
        env_lines.extend([
            "",
            f"# {storage} Configuration"
        ])
        
        if storage == 'ClickHouse':
            env_lines.extend([
                "CLICKHOUSE_HOST=localhost",
                "CLICKHOUSE_PORT=9000", 
                "CLICKHOUSE_DB=etl_data",
                "CLICKHOUSE_USER=default",
                "CLICKHOUSE_PASSWORD="
            ])
        elif storage == 'PostgreSQL':
            env_lines.extend([
                "POSTGRES_HOST=localhost",
                "POSTGRES_PORT=5432",
                "POSTGRES_DB=etl_data", 
                "POSTGRES_USER=postgres",
                "POSTGRES_PASSWORD=password"
            ])
        elif storage == 'HDFS':
            env_lines.extend([
                "HDFS_HOST=localhost",
                "HDFS_PORT=9000",
                "HDFS_PATH=/data/etl"
            ])
        
        env_lines.extend([
            "",
            "# ETL Process Configuration",
            f"BATCH_SIZE={10000}",
            "LOG_LEVEL=INFO"
        ])
        
        return "\\n".join(env_lines)
    
    def _generate_docker_compose(self, analysis: Dict[str, Any], storage_recommendation: Dict[str, Any]) -> str:
        """Генерация docker-compose.yml"""
        storage = storage_recommendation['recommended_storage']
        
        compose = f"""version: '3.8'

# ETL Services Configuration
# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

services:"""
        
        if storage == 'ClickHouse':
            compose += """
  clickhouse:
    image: yandex/clickhouse-server:latest
    container_name: etl_clickhouse
    ports:
      - "9000:9000"
      - "8123:8123"
    environment:
      - CLICKHOUSE_DB=etl_data
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    
  etl_process:
    build: .
    container_name: etl_process
    depends_on:
      - clickhouse
    environment:
      - CLICKHOUSE_HOST=clickhouse
      - CLICKHOUSE_PORT=9000
      - CLICKHOUSE_DB=etl_data
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs

volumes:
  clickhouse_data:"""
        
        elif storage == 'PostgreSQL':
            compose += """
  postgres:
    image: postgres:13
    container_name: etl_postgres
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_DB=etl_data
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    
  etl_process:
    build: .
    container_name: etl_process
    depends_on:
      - postgres
    environment:
      - POSTGRES_HOST=postgres
      - POSTGRES_PORT=5432
      - POSTGRES_DB=etl_data
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs

volumes:
  postgres_data:"""
        
        return compose
    
    def _generate_requirements_file(self, storage_recommendation: Dict[str, Any]) -> str:
        """Генерация requirements.txt для ETL процесса"""
        storage = storage_recommendation['recommended_storage']
        
        requirements = [
            "# ETL Process Dependencies",
            f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "pandas>=1.5.0",
            "python-dotenv>=0.19.0"
        ]
        
        if storage == 'ClickHouse':
            requirements.extend([
                "clickhouse-driver>=0.2.0",
                "clickhouse-connect>=0.5.0"
            ])
        elif storage == 'PostgreSQL':
            requirements.extend([
                "psycopg2-binary>=2.9.0"
            ])
        elif storage == 'HDFS':
            requirements.extend([
                "hdfs3>=0.3.0",
                "pyarrow>=8.0.0"
            ])
        
        return "\\n".join(requirements)
