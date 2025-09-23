#!/usr/bin/env python3
"""
Тестирование полного решения ETL Assistant

Модули 5 + 6: Демонстрация генерации Python ETL кода, SQL скриптов и Airflow DAG
"""

import sys
import os
import json
import time
sys.path.append('backend')

from backend.data_analyzers import DataAnalyzer, StorageRecommendationEngine
from backend.etl_generators import PythonETLGenerator, SQLScriptGenerator, ConfigGenerator
from backend.airflow_generator import AirflowDAGGenerator, AirflowConfigGenerator


def test_full_etl_solution():
    """Тестирование полного ETL решения на реальном файле"""
    
    print("🚀 ТЕСТИРОВАНИЕ ПОЛНОГО ETL РЕШЕНИЯ")
    print("=" * 60)
    
    # Инициализация всех компонентов
    print("\n📋 Инициализируем компоненты...")
    analyzer = DataAnalyzer()
    storage_recommender = StorageRecommendationEngine()
    etl_generator = PythonETLGenerator()
    sql_generator = SQLScriptGenerator()
    config_generator = ConfigGenerator()
    airflow_dag_generator = AirflowDAGGenerator()
    airflow_config_generator = AirflowConfigGenerator()
    
    # Путь к тестовому файлу
    test_file = "backend/uploads/museum_data_sample.csv"
    
    if not os.path.exists(test_file):
        print(f"❌ Файл {test_file} не найден!")
        print("   Сначала запустите demo_analyzer.bat для создания тестового файла")
        return
    
    print(f"📁 Тестовый файл: {test_file}")
    print(f"📏 Размер файла: {os.path.getsize(test_file) / (1024*1024):.1f} MB")
    
    try:
        # ШАГ 1: АНАЛИЗ ДАННЫХ
        print("\n" + "="*60)
        print("📊 ШАГ 1: АНАЛИЗ СТРУКТУРЫ ДАННЫХ")
        print("="*60)
        
        start_time = time.time()
        analysis = analyzer.analyze_file(test_file)
        analysis_time = time.time() - start_time
        
        print(f"✅ Анализ завершен за {analysis_time:.2f} секунд")
        print(f"   📄 Формат: {analysis.get('format_type')}")
        print(f"   📋 Строк: {analysis.get('total_rows', 0):,}")
        print(f"   📊 Колонок: {analysis.get('total_columns', 0)}")
        print(f"   🗂️ Кодировка: {analysis.get('encoding')}")
        print(f"   🔗 Разделитель: '{analysis.get('separator')}'")
        
        # ШАГ 2: РЕКОМЕНДАЦИИ ПО ХРАНИЛИЩУ
        print("\n" + "="*60)
        print("🏗️ ШАГ 2: РЕКОМЕНДАЦИИ ПО ХРАНИЛИЩУ")
        print("="*60)
        
        storage_recommendation = storage_recommender.recommend_storage(analysis)
        
        print(f"🗄️ Рекомендованное хранилище: {storage_recommendation['recommended_storage']}")
        print(f"💡 Обоснование: {storage_recommendation['reasoning']}")
        print(f"🔧 Оптимизации: {len(storage_recommendation['optimization_suggestions'])} предложений")
        
        # ШАГ 3: ГЕНЕРАЦИЯ ETL КОДА
        print("\n" + "="*60)
        print("🐍 ШАГ 3: ГЕНЕРАЦИЯ PYTHON ETL КОДА")
        print("="*60)
        
        start_time = time.time()
        etl_script = etl_generator.generate_etl_script(analysis, storage_recommendation)
        etl_generation_time = time.time() - start_time
        
        print(f"✅ Python ETL скрипт сгенерирован за {etl_generation_time:.2f} секунд")
        print(f"   📝 Размер кода: {len(etl_script):,} символов")
        print(f"   📄 Строк кода: {etl_script.count(chr(10))}")
        
        # Сохраняем ETL скрипт
        with open('generated_etl_script.py', 'w', encoding='utf-8') as f:
            f.write(etl_script)
        print(f"   💾 Сохранен как: generated_etl_script.py")
        
        # ШАГ 4: ГЕНЕРАЦИЯ SQL СКРИПТОВ
        print("\n" + "="*60)
        print("🗄️ ШАГ 4: ГЕНЕРАЦИЯ SQL СКРИПТОВ")
        print("="*60)
        
        sql_scripts = sql_generator.generate_transformation_sql(
            analysis, storage_recommendation['recommended_storage']
        )
        
        print(f"✅ SQL скрипты сгенерированы:")
        for script_type, script_content in sql_scripts.items():
            print(f"   📝 {script_type}: {len(script_content):,} символов")
            
            # Сохраняем каждый SQL скрипт
            filename = f"generated_sql_{script_type}.sql"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(script_content)
            print(f"       💾 Сохранен как: {filename}")
        
        # ШАГ 5: ГЕНЕРАЦИЯ КОНФИГУРАЦИОННЫХ ФАЙЛОВ
        print("\n" + "="*60)
        print("⚙️ ШАГ 5: ГЕНЕРАЦИЯ КОНФИГУРАЦИОННЫХ ФАЙЛОВ")
        print("="*60)
        
        config_files = config_generator.generate_config_files(analysis, storage_recommendation)
        
        print(f"✅ Конфигурационные файлы сгенерированы:")
        for config_type, config_content in config_files.items():
            print(f"   📝 {config_type}: {len(config_content):,} символов")
            
            # Определяем расширение файла
            ext_map = {
                'environment': '.env',
                'docker_compose': '.yml', 
                'requirements': '.txt'
            }
            ext = ext_map.get(config_type, '.txt')
            
            filename = f"generated_config_{config_type}{ext}"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(config_content)
            print(f"       💾 Сохранен как: {filename}")
        
        # ШАГ 6: ГЕНЕРАЦИЯ AIRFLOW DAG
        print("\n" + "="*60)
        print("✈️ ШАГ 6: ГЕНЕРАЦИЯ AIRFLOW DAG")
        print("="*60)
        
        # Генерируем ETL шаги для DAG
        etl_steps = generate_etl_steps_for_dag(analysis, storage_recommendation)
        
        start_time = time.time()
        dag_code = airflow_dag_generator.generate_dag(analysis, storage_recommendation, etl_steps)
        dag_generation_time = time.time() - start_time
        
        dag_id = airflow_dag_generator._generate_dag_id(analysis.get('filename', 'test.csv'))
        schedule = airflow_dag_generator._determine_schedule(analysis)
        
        print(f"✅ Airflow DAG сгенерирован за {dag_generation_time:.2f} секунд")
        print(f"   🆔 DAG ID: {dag_id}")
        print(f"   ⏰ Расписание: {schedule}")
        print(f"   📝 Размер кода: {len(dag_code):,} символов")
        print(f"   📄 Строк кода: {dag_code.count(chr(10))}")
        
        # Сохраняем DAG файл
        dag_filename = f"{dag_id}.py"
        with open(dag_filename, 'w', encoding='utf-8') as f:
            f.write(dag_code)
        print(f"   💾 Сохранен как: {dag_filename}")
        
        # ШАГ 7: ГЕНЕРАЦИЯ КОНФИГУРАЦИИ AIRFLOW
        print("\n" + "="*60)
        print("⚙️ ШАГ 7: ГЕНЕРАЦИЯ КОНФИГУРАЦИИ AIRFLOW")
        print("="*60)
        
        airflow_configs = airflow_config_generator.generate_airflow_configs(analysis, storage_recommendation)
        
        print(f"✅ Конфигурация Airflow сгенерирована:")
        for config_type, config_content in airflow_configs.items():
            print(f"   📝 {config_type}: {len(config_content):,} символов")
            
            # Определяем расширение файла
            ext_map = {
                'airflow_cfg': '.cfg',
                'docker_compose_airflow': '_airflow.yml',
                'connections_script': '.sh',
                'requirements_airflow': '_airflow.txt'
            }
            ext = ext_map.get(config_type, '.txt')
            
            filename = f"generated_{config_type}{ext}"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(config_content)
            print(f"       💾 Сохранен как: {filename}")
        
        # ФИНАЛЬНАЯ СТАТИСТИКА
        print("\n" + "="*60)
        print("🎉 ПОЛНОЕ ETL РЕШЕНИЕ СГЕНЕРИРОВАНО!")
        print("="*60)
        
        # Подсчитываем все созданные файлы
        generated_files = [
            'generated_etl_script.py',
            dag_filename
        ]
        
        # Добавляем SQL файлы
        for script_type in sql_scripts.keys():
            generated_files.append(f"generated_sql_{script_type}.sql")
        
        # Добавляем config файлы
        for config_type in config_files.keys():
            ext = {'environment': '.env', 'docker_compose': '.yml', 'requirements': '.txt'}.get(config_type, '.txt')
            generated_files.append(f"generated_config_{config_type}{ext}")
        
        # Добавляем Airflow config файлы
        for config_type in airflow_configs.keys():
            ext = {
                'airflow_cfg': '.cfg',
                'docker_compose_airflow': '_airflow.yml', 
                'connections_script': '.sh',
                'requirements_airflow': '_airflow.txt'
            }.get(config_type, '.txt')
            generated_files.append(f"generated_{config_type}{ext}")
        
        print(f"📊 СТАТИСТИКА РЕШЕНИЯ:")
        print(f"   📁 Исходный файл: {analysis.get('filename')} ({analysis.get('file_size_mb', 0)} MB)")
        print(f"   📋 Проанализировано: {analysis.get('total_rows', 0):,} строк")
        print(f"   🗄️ Целевое хранилище: {storage_recommendation['recommended_storage']}")
        print(f"   📝 Сгенерировано файлов: {len(generated_files)}")
        print(f"   ⚡ Общее время генерации: {analysis_time + etl_generation_time + dag_generation_time:.2f} секунд")
        
        print(f"\n📂 СГЕНЕРИРОВАННЫЕ ФАЙЛЫ:")
        for i, file in enumerate(generated_files, 1):
            if os.path.exists(file):
                size = os.path.getsize(file)
                print(f"   {i:2d}. {file:40s} ({size:,} байт)")
        
        print(f"\n🚀 ГОТОВО К РАЗВЕРТЫВАНИЮ:")
        print(f"   1️⃣ Запустите Docker: docker-compose -f generated_config_docker_compose.yml up")
        print(f"   2️⃣ Загрузите DAG: cp {dag_filename} ./dags/")
        print(f"   3️⃣ Запустите ETL: python generated_etl_script.py")
        
        # Сохраняем сводку в JSON
        summary = {
            "source_file": analysis.get('filename'),
            "file_size_mb": analysis.get('file_size_mb', 0),
            "total_rows": analysis.get('total_rows', 0),
            "recommended_storage": storage_recommendation['recommended_storage'],
            "dag_id": dag_id,
            "schedule_interval": schedule,
            "generated_files": generated_files,
            "generation_time_seconds": analysis_time + etl_generation_time + dag_generation_time
        }
        
        with open('etl_solution_summary.json', 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"   💾 Сводка сохранена: etl_solution_summary.json")
        
        print(f"\n✨ Полное ETL решение готово для демонстрации жюри! 🏆")
        
    except Exception as e:
        print(f"❌ Ошибка генерации полного решения: {str(e)}")
        import traceback
        traceback.print_exc()


def generate_etl_steps_for_dag(analysis, storage_recommendation):
    """Генерация шагов ETL для DAG (упрощенная версия)"""
    format_type = analysis.get('format_type', 'CSV')
    storage = storage_recommendation['recommended_storage']
    
    steps = [
        f"Extract: Чтение {format_type} файла",
        "Transform: Очистка и трансформация данных", 
        f"Load: Загрузка в {storage}",
        "Quality Check: Проверка качества данных"
    ]
    
    return steps


if __name__ == "__main__":
    test_full_etl_solution()
