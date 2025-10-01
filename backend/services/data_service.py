"""
Модуль анализа структуры данных для ETL Assistant

Модуль 4: Работа с входными данными - поддержка CSV/JSON/XML
"""

import pandas as pd
import json
import xml.etree.ElementTree as ET
import os
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import chardet
from datetime import datetime

logger = logging.getLogger(__name__)


class DataAnalyzer:
    """
    Класс для анализа структуры различных типов данных
    """
    
    def __init__(self):
        self.supported_formats = {'.csv', '.json', '.xml'}
    
    def detect_encoding(self, content: Union[bytes, str]) -> str:
        """
        Определение кодировки данных
        
        Args:
            content: Байтовая строка или строка для анализа
            
        Returns:
            str: Определенная кодировка
        """
        if isinstance(content, str):
            content = content.encode('utf-8')
        
        result = chardet.detect(content)
        encoding = result['encoding'] or 'utf-8'
        
        # Нормализация названий кодировок
        encoding = encoding.lower().replace('-', '_')
        
        return encoding
    
    def detect_format(self, content: Union[str, bytes], filename: str) -> str:
        """
        Определение формата данных на основе содержимого и имени файла
        
        Args:
            content: Содержимое файла
            filename: Имя файла
            
        Returns:
            str: Тип формата ('CSV', 'JSON', 'XML')
        """
        if isinstance(content, bytes):
            # Попытка декодирования
            encoding = self.detect_encoding(content)
            try:
                content = content.decode(encoding)
            except:
                content = content.decode('utf-8', errors='ignore')
        
        # Проверка расширения файла
        file_extension = Path(filename).suffix.lower()
        
        if file_extension == '.csv':
            return 'CSV'
        elif file_extension == '.json':
            return 'JSON'
        elif file_extension == '.xml':
            return 'XML'
        
        # Анализ содержимого, если расширение не помогло
        content_stripped = content.strip()
        
        # Проверка на JSON
        if content_stripped.startswith(('{', '[')):
            try:
                json.loads(content_stripped)
                return 'JSON'
            except:
                pass
        
        # Проверка на XML
        if content_stripped.startswith('<'):
            try:
                ET.fromstring(content_stripped)
                return 'XML'
            except:
                pass
        
        # По умолчанию считаем CSV
        return 'CSV'
    
    def infer_data_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Определение типов данных для колонок DataFrame в SQL формате
        
        Args:
            df: DataFrame для анализа
            
        Returns:
            dict: Словарь с SQL типами данных для каждой колонки
        """
        types = {}
        
        # Маппинг внутренних типов в SQL типы
        type_mapping = {
            'integer': 'INTEGER',
            'decimal': 'FLOAT',
            'datetime': 'TIMESTAMP',
            'date': 'DATE',
            'boolean': 'BOOLEAN',
            'text': 'VARCHAR'
        }
        
        for column in df.columns:
            column_type = self._infer_column_type(df[column])
            sql_type = type_mapping.get(column_type, 'VARCHAR')
            types[column] = sql_type
        
        return types
    
    def analyze_file(self, file_path: str = None, file_content: Union[str, bytes] = None, 
                    filename: str = None) -> Dict[str, Any]:
        """
        Универсальный анализатор файлов данных
        
        Args:
            file_path: Путь к файлу для анализа (опционально)
            file_content: Содержимое файла (опционально)
            filename: Имя файла (опционально, используется с file_content)
            
        Returns:
            dict: Структурированная информация о данных
        """
        # Режим работы с содержимым файла
        if file_content is not None:
            if not filename:
                raise ValueError("Параметр filename обязателен при использовании file_content")
            
            # Определяем формат
            format_type = self.detect_format(file_content, filename)
            
            # Конвертируем в строку если байты
            if isinstance(file_content, bytes):
                encoding = self.detect_encoding(file_content)
                file_content = file_content.decode(encoding)
            
            # Создаем временный файл для анализа
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix=Path(filename).suffix, 
                                            delete=False, encoding='utf-8') as tmp:
                tmp.write(file_content)
                tmp_path = tmp.name
            
            try:
                file_path = Path(tmp_path)
                file_size = len(file_content.encode('utf-8'))
                
                # Анализ в зависимости от типа файла
                if format_type == 'CSV':
                    analysis = self.analyze_csv(file_path)
                elif format_type == 'JSON':
                    analysis = self.analyze_json(file_path)
                elif format_type == 'XML':
                    analysis = self.analyze_xml(file_path)
                
                # Добавляем информацию
                analysis.update({
                    'filename': filename,
                    'format_type': format_type,
                    'file_size_bytes': file_size,
                    'file_size_mb': round(file_size / (1024 * 1024), 2),
                    'analysis_timestamp': datetime.now().isoformat()
                })
            finally:
                # Удаляем временный файл
                import os
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            
            return analysis
        
        # Режим работы с путем к файлу
        if file_path:
            file_path = Path(file_path)
            
            if not file_path.exists():
                raise FileNotFoundError(f"Файл не найден: {file_path}")
            
            file_extension = file_path.suffix.lower()
            
            if file_extension not in self.supported_formats:
                raise ValueError(f"Неподдерживаемый формат файла: {file_extension}")
            
            # Получение базовой информации о файле
            file_stats = file_path.stat()
            file_size = file_stats.st_size
            
            logger.info(f"Анализируем файл: {file_path.name} ({file_size} байт)")
            
            # Определяем формат
            format_type = file_extension[1:].upper()  # .csv -> CSV
            
            # Анализ в зависимости от типа файла
            if file_extension == '.csv':
                analysis = self.analyze_csv(file_path)
            elif file_extension == '.json':
                analysis = self.analyze_json(file_path)
            elif file_extension == '.xml':
                analysis = self.analyze_xml(file_path)
            
            # Добавляем общую информацию о файле
            analysis.update({
                'filename': file_path.name,
                'format_type': format_type,
                'file_size_bytes': file_size,
                'file_size_mb': round(file_size / (1024 * 1024), 2),
                'file_format': file_extension,
                'analysis_timestamp': datetime.now().isoformat()
            })
            
            return analysis
        
        raise ValueError("Необходимо указать либо file_path, либо file_content с filename")
    
    def analyze_csv(self, file_path: Path) -> Dict[str, Any]:
        """
        Анализ CSV файла с определением структуры и типов данных
        
        Args:
            file_path: Путь к CSV файлу
            
        Returns:
            dict: Анализ CSV структуры
        """
        try:
            # Определяем кодировку файла
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # Читаем первые 10KB для определения кодировки
                encoding = chardet.detect(raw_data)['encoding'] or 'utf-8'
            
            logger.info(f"Определена кодировка: {encoding}")
            
            # Пробуем разные разделители
            separators = [',', ';', '\t', '|']
            best_df = None
            best_separator = ','
            max_columns = 0
            
            for sep in separators:
                try:
                    # Читаем только первые 1000 строк для анализа структуры
                    df_sample = pd.read_csv(file_path, sep=sep, encoding=encoding, nrows=1000)
                    if len(df_sample.columns) > max_columns:
                        max_columns = len(df_sample.columns)
                        best_df = df_sample
                        best_separator = sep
                except Exception:
                    continue
            
            if best_df is None:
                raise ValueError("Не удалось определить структуру CSV файла")
            
            # Подсчет общего количества строк (без чтения всего файла в память)
            total_rows = sum(1 for _ in open(file_path, 'r', encoding=encoding)) - 1  # -1 для заголовка
            
            # Анализ колонок
            columns_info = []
            for col in best_df.columns:
                col_data = best_df[col]
                
                # Определение типа данных
                data_type = self._infer_column_type(col_data)
                
                # Статистика по колонке
                null_count = col_data.isnull().sum()
                unique_count = col_data.nunique()
                
                # Примеры значений (не null)
                sample_values = col_data.dropna().head(3).tolist()
                
                columns_info.append({
                    'name': col,
                    'data_type': data_type,
                    'null_count': int(null_count),
                    'null_percentage': round((null_count / len(col_data)) * 100, 2),
                    'unique_count': int(unique_count),
                    'sample_values': sample_values
                })
            
            return {
                'format_type': 'CSV',
                'separator': best_separator,
                'encoding': encoding,
                'total_rows': total_rows,
                'total_columns': len(best_df.columns),
                'columns': columns_info,
                'data_preview': best_df.head(5).to_dict('records'),
                'has_header': True,
                'estimated_data_volume': self._estimate_data_volume(total_rows, len(best_df.columns))
            }
            
        except Exception as e:
            logger.error(f"Ошибка анализа CSV файла: {str(e)}")
            raise
    
    def analyze_json(self, file_path: Path) -> Dict[str, Any]:
        """
        Анализ JSON файла с определением структуры
        
        Args:
            file_path: Путь к JSON файлу
            
        Returns:
            dict: Анализ JSON структуры
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Для больших файлов читаем по частям
                first_chars = f.read(1000)
                f.seek(0)
                
                if first_chars.strip().startswith('['):
                    # Массив объектов
                    data = json.load(f)
                    if isinstance(data, list) and len(data) > 0:
                        return self._analyze_json_array(data)
                else:
                    # Один объект или объект с массивами
                    data = json.load(f)
                    if isinstance(data, dict):
                        return self._analyze_json_object(data)
                    
        except Exception as e:
            logger.error(f"Ошибка анализа JSON файла: {str(e)}")
            raise
    
    def analyze_xml(self, file_path: Path) -> Dict[str, Any]:
        """
        Анализ XML файла с определением структуры
        
        Args:
            file_path: Путь к XML файлу
            
        Returns:
            dict: Анализ XML структуры
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Находим повторяющиеся элементы (записи)
            records_element = self._find_records_element(root)
            
            if records_element is None:
                # Если нет повторяющихся элементов, анализируем корневой элемент
                return self._analyze_xml_single_element(root)
            
            # Анализируем структуру записей
            sample_records = list(records_element)[:100]  # Берем первые 100 записей
            total_records = len(list(records_element))
            
            if not sample_records:
                raise ValueError("XML файл не содержит данных для анализа")
            
            # Анализируем поля первой записи
            fields_info = []
            first_record = sample_records[0]
            
            for field in first_record:
                field_values = [rec.find(field.tag).text if rec.find(field.tag) is not None else None 
                              for rec in sample_records[:10]]
                field_values = [v for v in field_values if v is not None]
                
                data_type = self._infer_field_type(field_values)
                
                fields_info.append({
                    'name': field.tag,
                    'data_type': data_type,
                    'sample_values': field_values[:3]
                })
            
            return {
                'format_type': 'XML',
                'root_element': root.tag,
                'record_element': first_record.tag,
                'total_records': total_records,
                'total_rows': total_records,  # Алиас для совместимости
                'total_fields': len(fields_info),
                'fields': fields_info,
                'data_preview': [self._xml_element_to_dict(rec) for rec in sample_records[:5]],
                'estimated_data_volume': self._estimate_data_volume(total_records, len(fields_info))
            }
            
        except Exception as e:
            logger.error(f"Ошибка анализа XML файла: {str(e)}")
            raise
    
    def _infer_column_type(self, series: pd.Series) -> str:
        """Определение типа данных колонки"""
        # Убираем null значения для анализа
        clean_series = series.dropna()
        
        if len(clean_series) == 0:
            return 'text'
        
        # Сначала проверяем pandas datetime типы (не путать с числами)
        if pd.api.types.is_datetime64_any_dtype(series):
            return 'datetime'
        
        # Пробуем определить числовой тип ПЕРЕД датами
        try:
            numeric_series = pd.to_numeric(clean_series, errors='raise')
            # Проверяем, есть ли дробные значения
            if (numeric_series % 1 != 0).any():
                return 'decimal'
            else:
                return 'integer'
        except (ValueError, TypeError):
            pass
        
        # Только теперь пробуем определить дату из строк
        try:
            pd.to_datetime(clean_series, errors='raise')
            return 'datetime'
        except (ValueError, TypeError, AttributeError):
            pass
        
        # Проверяем булевы значения
        unique_vals = set(clean_series.astype(str).str.lower())
        if unique_vals.issubset({'true', 'false', '1', '0', 'yes', 'no'}):
            return 'boolean'
        
        return 'text'
    
    def _infer_field_type(self, values: List[str]) -> str:
        """Определение типа данных поля"""
        if not values:
            return 'text'
        
        # Пробуем числовой тип
        try:
            nums = [float(v) for v in values]
            if all(n.is_integer() for n in nums):
                return 'integer'
            else:
                return 'decimal'
        except (ValueError, TypeError):
            pass
        
        # Пробуем дату
        try:
            [pd.to_datetime(v) for v in values]
            return 'datetime'
        except (ValueError, TypeError):
            pass
        
        return 'text'
    
    def _analyze_json_array(self, data: List[Dict]) -> Dict[str, Any]:
        """Анализ JSON массива объектов"""
        if not data:
            return {'error': 'Пустой JSON массив'}
        
        # Анализируем структуру первых объектов
        sample_objects = data[:100]
        total_records = len(data)
        
        # Собираем все уникальные ключи
        all_keys = set()
        for obj in sample_objects:
            if isinstance(obj, dict):
                all_keys.update(obj.keys())
        
        # Анализируем каждое поле
        fields_info = []
        for key in all_keys:
            values = [obj.get(key) for obj in sample_objects if isinstance(obj, dict)]
            values = [v for v in values if v is not None]
            
            if values:
                data_type = self._infer_field_type([str(v) for v in values])
                fields_info.append({
                    'name': key,
                    'data_type': data_type,
                    'sample_values': values[:3]
                })
        
        return {
            'format_type': 'JSON',
            'structure_type': 'array',
            'total_records': total_records,
            'total_rows': total_records,  # Алиас для совместимости
            'total_fields': len(fields_info),
            'fields': fields_info,
            'data_preview': sample_objects[:5],
            'estimated_data_volume': self._estimate_data_volume(total_records, len(fields_info))
        }
    
    def _analyze_json_object(self, data: Dict) -> Dict[str, Any]:
        """Анализ JSON объекта"""
        fields_info = []
        
        for key, value in data.items():
            if isinstance(value, list):
                if value and isinstance(value[0], dict):
                    # Массив объектов внутри основного объекта
                    subfields = set()
                    for item in value[:10]:
                        if isinstance(item, dict):
                            subfields.update(item.keys())
                    
                    fields_info.append({
                        'name': key,
                        'data_type': 'array_of_objects',
                        'subfields': list(subfields),
                        'array_length': len(value)
                    })
                else:
                    fields_info.append({
                        'name': key,
                        'data_type': 'array',
                        'array_length': len(value),
                        'sample_values': value[:3] if value else []
                    })
            else:
                data_type = self._infer_field_type([str(value)])
                fields_info.append({
                    'name': key,
                    'data_type': data_type,
                    'sample_values': [value]
                })
        
        return {
            'format_type': 'JSON',
            'structure_type': 'object',
            'total_records': 1,  # Один основной объект
            'total_rows': 1,  # Алиас для совместимости
            'total_fields': len(fields_info),
            'fields': fields_info,
            'data_preview': [data] if len(str(data)) < 1000 else [{'note': 'Объект слишком большой для предварительного просмотра'}],
            'estimated_data_volume': 'medium'
        }
    
    def _find_records_element(self, root: ET.Element) -> Optional[ET.Element]:
        """Находит элемент, содержащий записи данных"""
        # Ищем элемент с наибольшим количеством одинаковых дочерних элементов
        max_count = 0
        records_element = None
        
        for element in root.iter():
            if len(element) > 1:
                # Подсчитываем количество одинаковых дочерних тегов
                child_tags = [child.tag for child in element]
                most_common_tag = max(set(child_tags), key=child_tags.count)
                count = child_tags.count(most_common_tag)
                
                if count > max_count and count > 1:
                    max_count = count
                    records_element = element
        
        return records_element
    
    def _analyze_xml_single_element(self, element: ET.Element) -> Dict[str, Any]:
        """Анализ одиночного XML элемента"""
        fields = []
        for child in element:
            fields.append({
                'name': child.tag,
                'data_type': 'text',
                'sample_values': [child.text] if child.text else []
            })
        
        return {
            'format_type': 'XML',
            'root_element': element.tag,
            'total_fields': len(fields),
            'fields': fields,
            'structure_type': 'single_element'
        }
    
    def _xml_element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Преобразование XML элемента в словарь"""
        result = {}
        for child in element:
            result[child.tag] = child.text
        return result
    
    def _estimate_data_volume(self, rows: int, columns: int) -> str:
        """Оценка объема данных для рекомендации хранилища"""
        if rows < 10000:
            return 'small'
        elif rows < 1000000:
            return 'medium'
        elif rows < 100000000:
            return 'large'
        else:
            return 'very_large'


class StorageRecommendationEngine:
    """
    Класс для определения оптимального хранилища данных
    """
    
    def recommend_storage(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Рекомендация хранилища на основе анализа данных
        
        Args:
            analysis: Результат анализа данных
            
        Returns:
            dict: Рекомендации по хранилищу и структуре
        """
        data_volume = analysis.get('estimated_data_volume', 'medium')
        total_rows = analysis.get('total_rows', analysis.get('total_records', 0))
        file_size_mb = analysis.get('file_size_mb', 0)
        
        # Анализируем типы данных
        has_datetime = False
        has_aggregatable_fields = False
        text_heavy = False
        
        columns_or_fields = analysis.get('columns', analysis.get('fields', []))
        
        for col in columns_or_fields:
            if col.get('data_type') in ['datetime', 'date']:
                has_datetime = True
            if col.get('data_type') in ['integer', 'decimal']:
                has_aggregatable_fields = True
            if col.get('data_type') == 'text' and len(str(col.get('sample_values', []))) > 100:
                text_heavy = True
        
        # Логика выбора хранилища
        storage_recommendation = self._choose_storage(
            data_volume, total_rows, file_size_mb, 
            has_datetime, has_aggregatable_fields, text_heavy
        )
        
        return {
            'recommended_storage': storage_recommendation['storage'],
            'reasoning': storage_recommendation['reasoning'],
            'ddl_script': self._generate_ddl(analysis, storage_recommendation['storage']),
            'optimization_suggestions': storage_recommendation['optimizations']
        }
    
    def _choose_storage(self, volume: str, rows: int, size_mb: float, 
                       has_datetime: bool, has_aggregatable: bool, text_heavy: bool) -> Dict[str, Any]:
        """Выбор оптимального хранилища"""
        
        # ClickHouse - для аналитики и больших данных
        if (volume in ['large', 'very_large'] or 
            size_mb > 100 or 
            (has_datetime and has_aggregatable and rows > 100000)):
            return {
                'storage': 'ClickHouse',
                'reasoning': 'Рекомендован ClickHouse из-за большого объема данных и потребности в аналитических запросах. Колонночная архитектура обеспечит высокую производительность для агрегации и фильтрации данных по времени.',
                'optimizations': [
                    'Партицирование по дате для оптимизации запросов',
                    'Сортировка по наиболее часто используемым полям',
                    'Использование движка MergeTree для оптимальной производительности'
                ]
            }
        
        # HDFS - для очень больших неструктурированных данных
        elif volume == 'very_large' and (text_heavy or size_mb > 1000):
            return {
                'storage': 'HDFS',
                'reasoning': 'Рекомендовано HDFS из-за очень большого объема данных и наличия текстовых полей большого размера. Подходит для хранения сырых данных с последующей обработкой через Spark.',
                'optimizations': [
                    'Сохранение в формате Parquet для оптимизации',
                    'Партицирование по ключевым полям',
                    'Использование сжатия данных'
                ]
            }
        
        # PostgreSQL - для операционных данных и умеренных объемов
        else:
            return {
                'storage': 'PostgreSQL',
                'reasoning': 'Рекомендован PostgreSQL для структурированных данных умеренного объема. Реляционная модель обеспечит целостность данных и поддержку сложных запросов с JOIN операциями.',
                'optimizations': [
                    'Создание индексов на часто используемые поля',
                    'Использование подходящих типов данных',
                    'Настройка автовакуума для поддержания производительности'
                ]
            }
    
    def _generate_ddl(self, analysis: Dict[str, Any], storage: str) -> str:
        """Генерация DDL скрипта для выбранного хранилища"""
        
        table_name = analysis.get('filename', 'data_table').replace('.', '_').lower()
        columns_or_fields = analysis.get('columns', analysis.get('fields', []))
        
        if storage == 'ClickHouse':
            return self._generate_clickhouse_ddl(table_name, columns_or_fields, analysis)
        elif storage == 'PostgreSQL':
            return self._generate_postgresql_ddl(table_name, columns_or_fields, analysis)
        elif storage == 'HDFS':
            return self._generate_hive_ddl(table_name, columns_or_fields, analysis)
        
        return "-- DDL генерация не поддержана для данного типа хранилища"
    
    def _generate_clickhouse_ddl(self, table_name: str, columns: List[Dict], analysis: Dict) -> str:
        """Генерация DDL для ClickHouse"""
        
        ddl_columns = []
        partition_key = None
        order_keys = []
        
        for col in columns:
            col_name = col['name'].replace(' ', '_').replace('-', '_')
            
            # Маппинг типов данных
            if col['data_type'] == 'integer':
                ch_type = 'Int64'
            elif col['data_type'] == 'decimal':
                ch_type = 'Float64'
            elif col['data_type'] == 'datetime':
                ch_type = 'DateTime'
                if partition_key is None:
                    partition_key = f"toYYYYMM({col_name})"
                order_keys.append(col_name)
            elif col['data_type'] == 'boolean':
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
            if col['data_type'] == 'integer':
                pg_type = 'BIGINT'
            elif col['data_type'] == 'decimal':
                pg_type = 'NUMERIC'
            elif col['data_type'] == 'datetime':
                pg_type = 'TIMESTAMP'
                indexes.append(f"CREATE INDEX idx_{table_name}_{col_name} ON {table_name}({col_name});")
            elif col['data_type'] == 'boolean':
                pg_type = 'BOOLEAN'
            else:
                # Определяем размер VARCHAR на основе примеров
                max_length = max([len(str(v)) for v in col.get('sample_values', [''])], default=255)
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
    
    def _generate_hive_ddl(self, table_name: str, columns: List[Dict], analysis: Dict) -> str:
        """Генерация DDL для Hive (HDFS)"""
        
        ddl_columns = []
        
        for col in columns:
            col_name = col['name'].replace(' ', '_').replace('-', '_').lower()
            
            # Маппинг типов данных для Hive
            if col['data_type'] == 'integer':
                hive_type = 'BIGINT'
            elif col['data_type'] == 'decimal':
                hive_type = 'DOUBLE'
            elif col['data_type'] == 'datetime':
                hive_type = 'TIMESTAMP'
            elif col['data_type'] == 'boolean':
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
