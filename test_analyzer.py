#!/usr/bin/env python3
"""
Тестирование анализатора данных для ETL Assistant

Модуль 4: Работа с входными данными - тестирование CSV анализа
"""

import sys
import os
sys.path.append('backend')

from data_analyzers import DataAnalyzer, StorageRecommendationEngine
import json

def test_analyzer():
    """Тестирование анализатора данных на реальном файле"""
    
    # Инициализация анализаторов
    analyzer = DataAnalyzer()
    recommender = StorageRecommendationEngine()
    
    # Путь к тестовому файлу
    test_file = "backend/uploads/museum_data_sample.csv"
    
    if not os.path.exists(test_file):
        print(f"❌ Файл {test_file} не найден!")
        return
    
    print("🔍 Начинаем анализ данных...")
    print(f"📁 Файл: {test_file}")
    
    try:
        # Анализ файла
        print("\n📊 Выполняем анализ структуры данных...")
        analysis = analyzer.analyze_file(test_file)
        
        print("\n✅ Анализ данных завершен:")
        print(f"   📄 Формат: {analysis.get('format_type')}")
        print(f"   📏 Размер: {analysis.get('file_size_mb')} MB")
        print(f"   📋 Строк: {analysis.get('total_rows', 'N/A'):,}")
        print(f"   📊 Колонок: {analysis.get('total_columns', 'N/A')}")
        print(f"   🗂️ Кодировка: {analysis.get('encoding')}")
        print(f"   🔗 Разделитель: '{analysis.get('separator')}'")
        print(f"   📈 Объем данных: {analysis.get('estimated_data_volume')}")
        
        print("\n📋 Структура колонок:")
        for i, col in enumerate(analysis.get('columns', [])[:10], 1):  # Показываем первые 10
            null_pct = col.get('null_percentage', 0)
            print(f"   {i:2d}. {col['name']:25s} | {col['data_type']:10s} | Null: {null_pct:5.1f}% | Примеры: {col.get('sample_values', [])[:2]}")
        
        if len(analysis.get('columns', [])) > 10:
            print(f"   ... и еще {len(analysis.get('columns', [])) - 10} колонок")
        
        # Рекомендация хранилища
        print("\n🎯 Генерируем рекомендации по хранилищу...")
        recommendation = recommender.recommend_storage(analysis)
        
        print("\n🏗️ Рекомендации:")
        print(f"   🗄️ Хранилище: {recommendation['recommended_storage']}")
        print(f"   💡 Обоснование: {recommendation['reasoning']}")
        
        print("\n📝 DDL Скрипт:")
        print("─" * 80)
        print(recommendation['ddl_script'])
        print("─" * 80)
        
        print("\n🔧 Предложения по оптимизации:")
        for i, opt in enumerate(recommendation['optimization_suggestions'], 1):
            print(f"   {i}. {opt}")
        
        print("\n🎉 Тест анализатора успешно завершен!")
        
        # Сохраним результат для отладки
        with open('analysis_result.json', 'w', encoding='utf-8') as f:
            json.dump({
                'analysis': analysis,
                'recommendation': recommendation
            }, f, ensure_ascii=False, indent=2)
        print("\n💾 Результат сохранен в analysis_result.json")
        
    except Exception as e:
        print(f"❌ Ошибка анализа: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_analyzer()
