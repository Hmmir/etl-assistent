"""
Реальный генератор Kafka Streaming кода для ETL Assistant
"""

import logging
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class KafkaStreamingGenerator:
    """
    Генератор реального Python кода для Kafka streaming
    """
    
    def generate_kafka_consumer(self, topic_name: str, target_storage: str, analysis: Dict[str, Any]) -> str:
        """
        Генерирует реальный код Kafka Consumer
        """
        
        return f'''#!/usr/bin/env python3
"""
🌊 Kafka Consumer для ETL Pipeline
Автоматически сгенерирован ETL Assistant
Дата: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Топик: {topic_name}
Целевое хранилище: {target_storage}
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
import time
import sys

# Конфигурация Kafka
KAFKA_CONFIG = {{
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'etl-assistant-consumer',
    'auto_offset_reset': 'earliest',
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    'consumer_timeout_ms': 10000,
    'max_poll_records': 1000,
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 5000
}}

# Топик для обработки
INPUT_TOPIC = '{topic_name}'
ERROR_TOPIC = '{topic_name}_errors'
BATCH_SIZE = 1000

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLKafkaConsumer:
    """
    Kafka Consumer для ETL обработки потоковых данных
    """
    
    def __init__(self):
        """Инициализация consumer"""
        try:
            self.consumer = KafkaConsumer(
                INPUT_TOPIC,
                **KAFKA_CONFIG
            )
            
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            
            logger.info(f"✅ Kafka Consumer инициализован для топика: {{INPUT_TOPIC}}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации Kafka: {{str(e)}}")
            sys.exit(1)
    
    def transform_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Трансформация сообщения
        Добавьте свою бизнес-логику здесь
        """
        try:
            # Базовая трансформация
            transformed = message.copy()
            
            # Добавляем метаданные
            transformed['processed_at'] = datetime.now().isoformat()
            transformed['etl_version'] = '1.0.0'
            
            # Очистка данных
            for key, value in transformed.items():
                if isinstance(value, str):
                    transformed[key] = value.strip()
                elif value is None:
                    transformed[key] = ''
            
            return transformed
            
        except Exception as e:
            logger.error(f"❌ Ошибка трансформации сообщения: {{str(e)}}")
            raise
    
    def load_to_storage(self, batch_data: List[Dict[str, Any]]) -> bool:
        """
        Загрузка батча данных в целевое хранилище
        """
        try:
            logger.info(f"💾 Загружаем {{len(batch_data)}} записей в {target_storage}")
            
            # Конвертируем в DataFrame для удобства
            df = pd.DataFrame(batch_data)
            
            {self._generate_storage_loader(target_storage)}
            
            logger.info(f"✅ Успешно загружено {{len(batch_data)}} записей")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки в хранилище: {{str(e)}}")
            return False
    
    def send_to_error_topic(self, message: Dict[str, Any], error: str):
        """
        Отправка ошибочных сообщений в error топик
        """
        try:
            error_message = {{
                'original_message': message,
                'error': error,
                'timestamp': datetime.now().isoformat(),
                'topic': INPUT_TOPIC
            }}
            
            self.producer.send(ERROR_TOPIC, error_message)
            logger.warning(f"⚠️ Сообщение отправлено в error топик: {{error}}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка отправки в error топик: {{str(e)}}")
    
    def process_stream(self):
        """
        Основной цикл обработки потоковых данных
        """
        logger.info(f"🌊 Начинаем обработку потока из топика: {{INPUT_TOPIC}}")
        
        batch = []
        batch_start_time = time.time()
        
        try:
            for message in self.consumer:
                try:
                    # Получаем данные сообщения
                    raw_data = message.value
                    
                    # Трансформируем данные
                    transformed_data = self.transform_message(raw_data)
                    
                    # Добавляем в батч
                    batch.append(transformed_data)
                    
                    # Обработка батча при достижении лимита
                    if len(batch) >= BATCH_SIZE:
                        if self.load_to_storage(batch):
                            batch_time = time.time() - batch_start_time
                            logger.info(f"📊 Батч обработан за {{batch_time:.2f}}с")
                        else:
                            logger.error("❌ Ошибка загрузки батча")
                        
                        # Сброс батча
                        batch = []
                        batch_start_time = time.time()
                
                except Exception as e:
                    error_msg = f"Ошибка обработки сообщения: {{str(e)}}"
                    logger.error(f"❌ {{error_msg}}")
                    
                    # Отправляем в error топик
                    try:
                        self.send_to_error_topic(message.value, error_msg)
                    except:
                        pass  # Игнорируем ошибки error топика
                        
        except KeyboardInterrupt:
            logger.info("🛑 Получен сигнал остановки")
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в обработке потока: {{str(e)}}")
            
        finally:
            # Обрабатываем оставшиеся данные в батче
            if batch:
                logger.info(f"🔄 Обрабатываем оставшиеся {{len(batch)}} записей")
                self.load_to_storage(batch)
            
            self.cleanup()
    
    def cleanup(self):
        """
        Очистка ресурсов
        """
        try:
            if hasattr(self, 'consumer'):
                self.consumer.close()
                logger.info("✅ Kafka Consumer закрыт")
                
            if hasattr(self, 'producer'):
                self.producer.close()
                logger.info("✅ Kafka Producer закрыт")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при закрытии соединений: {{str(e)}}")


def main():
    """
    Главная функция
    """
    logger.info("🚀 Запускаем ETL Kafka Consumer")
    
    consumer = ETLKafkaConsumer()
    
    try:
        consumer.process_stream()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {{str(e)}}")
        sys.exit(1)
    
    logger.info("✅ ETL Kafka Consumer завершен")


if __name__ == "__main__":
    main()
'''
    
    def generate_kafka_producer(self, topic_name: str, source_config: Dict[str, Any]) -> str:
        """
        Генерирует код Kafka Producer для отправки данных
        """
        
        return f'''#!/usr/bin/env python3
"""
🚀 Kafka Producer для ETL Pipeline
Автоматически сгенерирован ETL Assistant
Дата: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Топик: {topic_name}
"""

import json
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Iterator
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import sys

# Конфигурация Kafka
KAFKA_CONFIG = {{
    'bootstrap_servers': ['localhost:9092'],
    'value_serializer': lambda x: json.dumps(x, default=str).encode('utf-8'),
    'acks': 'all',
    'retries': 3,
    'batch_size': 16384,
    'linger_ms': 10,
    'buffer_memory': 33554432
}}

OUTPUT_TOPIC = '{topic_name}'
BATCH_SIZE = 1000

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLKafkaProducer:
    """
    Kafka Producer для отправки данных в топик
    """
    
    def __init__(self):
        """Инициализация producer"""
        try:
            self.producer = KafkaProducer(**KAFKA_CONFIG)
            logger.info(f"✅ Kafka Producer инициализован для топика: {{OUTPUT_TOPIC}}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации Kafka: {{str(e)}}")
            sys.exit(1)
    
    def read_data_source(self) -> Iterator[Dict[str, Any]]:
        """
        Чтение данных из источника (CSV, Database, API)
        """
        try:
            # Пример чтения CSV файла
            source_file = "data.csv"  # Замените на ваш источник
            
            logger.info(f"📂 Читаем данные из: {{source_file}}")
            
            # Читаем большие файлы чанками
            chunk_size = 1000
            for chunk in pd.read_csv(source_file, chunksize=chunk_size):
                for _, row in chunk.iterrows():
                    yield row.to_dict()
                    
        except Exception as e:
            logger.error(f"❌ Ошибка чтения источника данных: {{str(e)}}")
            raise
    
    def send_to_kafka(self, data: Dict[str, Any]) -> bool:
        """
        Отправка одной записи в Kafka
        """
        try:
            # Добавляем метаданные
            data['kafka_timestamp'] = datetime.now().isoformat()
            data['producer_id'] = 'etl-assistant-producer'
            
            # Отправляем в Kafka
            future = self.producer.send(OUTPUT_TOPIC, value=data)
            
            # Ждем подтверждения (синхронно для надежности)
            record_metadata = future.get(timeout=10)
            
            return True
            
        except KafkaError as e:
            logger.error(f"❌ Ошибка Kafka: {{str(e)}}")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка отправки: {{str(e)}}")
            return False
    
    def stream_data(self):
        """
        Основная функция стриминга данных
        """
        logger.info(f"🌊 Начинаем стриминг данных в топик: {{OUTPUT_TOPIC}}")
        
        sent_count = 0
        error_count = 0
        start_time = time.time()
        
        try:
            for data_record in self.read_data_source():
                if self.send_to_kafka(data_record):
                    sent_count += 1
                else:
                    error_count += 1
                
                # Статистика каждые 1000 записей
                if (sent_count + error_count) % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = sent_count / elapsed if elapsed > 0 else 0
                    logger.info(f"📊 Отправлено: {{sent_count}}, Ошибок: {{error_count}}, Скорость: {{rate:.1f}} записей/сек")
            
            # Финальная статистика
            total_time = time.time() - start_time
            logger.info(f"✅ Стриминг завершен:")
            logger.info(f"   📤 Отправлено: {{sent_count}} записей")
            logger.info(f"   ❌ Ошибок: {{error_count}}")
            logger.info(f"   ⏱️  Время: {{total_time:.2f}} секунд")
            logger.info(f"   🚀 Средняя скорость: {{sent_count/total_time:.1f}} записей/сек")
            
        except KeyboardInterrupt:
            logger.info("🛑 Получен сигнал остановки")
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {{str(e)}}")
            
        finally:
            self.cleanup()
    
    def cleanup(self):
        """
        Очистка ресурсов
        """
        try:
            if hasattr(self, 'producer'):
                self.producer.flush()  # Отправляем все буферизованные сообщения
                self.producer.close()
                logger.info("✅ Kafka Producer закрыт")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при закрытии Producer: {{str(e)}}")


def main():
    """
    Главная функция
    """
    logger.info("🚀 Запускаем ETL Kafka Producer")
    
    producer = ETLKafkaProducer()
    
    try:
        producer.stream_data()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {{str(e)}}")
        sys.exit(1)
    
    logger.info("✅ ETL Kafka Producer завершен")


if __name__ == "__main__":
    main()
'''

    def _generate_storage_loader(self, storage_type: str) -> str:
        """
        Генерирует код загрузки в конкретное хранилище
        """
        if storage_type.lower() == 'clickhouse':
            return '''
            # Загрузка в ClickHouse
            from clickhouse_driver import Client
            
            client = Client(
                host='localhost',
                port=9000,
                database='etl_streaming'
            )
            
            # Подготовка данных для вставки
            columns = list(df.columns)
            data_tuples = [tuple(row) for row in df.values]
            
            # Batch insert
            client.execute(f"INSERT INTO streaming_data ({','.join(columns)}) VALUES", data_tuples)
            '''
        
        elif storage_type.lower() == 'postgresql':
            return '''
            # Загрузка в PostgreSQL
            import psycopg2
            from sqlalchemy import create_engine
            
            engine = create_engine('postgresql://user:password@localhost:5432/etl_streaming')
            
            # Batch insert с помощью pandas
            df.to_sql('streaming_data', engine, if_exists='append', index=False, method='multi')
            '''
        
        else:  # HDFS
            return '''
            # Загрузка в HDFS
            import hdfs3
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            hdfs = hdfs3.HDFileSystem(host='localhost', port=9000)
            
            # Сохранение в Parquet формате
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = f"/etl/streaming/data_{timestamp}.parquet"
            
            table = pa.Table.from_pandas(df)
            with hdfs.open(file_path, 'wb') as f:
                pq.write_table(table, f)
            '''

    def generate_kafka_docker_compose(self) -> str:
        """
        Генерирует Docker Compose файл для Kafka кластера
        """
        return '''version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    container_name: etl_zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:latest
    container_name: etl_kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_HOST://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: true

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: etl_kafka_ui
    depends_on:
      - kafka
    ports:
      - "8090:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: etl-cluster
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181

networks:
  default:
    name: etl-kafka-network
'''
