"""
Генерация реалистичных данных для ETL-пайплайна.
Создает CSV-файл с 50 000 записей о действиях пользователей.
"""
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os


def generate_clickstream_data(num_records=50000, output_path="../data/clickstream.csv"):
    """
    Генерирует данные о кликах пользователей

    Args:
        num_records: количество записей
        output_path: путь для сохранения файла
    """

    # Инициализация генератора случайных данных
    fake = Faker('ru_RU')
    np.random.seed(42)
    random.seed(42)

    print("🚀 Начинаем генерацию данных...")

    # Списки возможных значений
    actions = ['click', 'view', 'purchase', 'login', 'logout', 'search', 'add_to_cart']
    devices = ['mobile', 'desktop', 'tablet']
    regions = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург',
               'Казань', 'Нижний Новгород', 'Челябинск', 'Самара']

    data = []

    # Генерация "хороших" данных
    for i in range(num_records):
        if i % 10000 == 0:
            print(f"  Сгенерировано {i} записей...")

        record = {
            'user_id': fake.uuid4()[:8],
            'session_id': f"sess_{fake.random_number(digits=8)}",
            'action': random.choice(actions),
            'timestamp': (datetime.now() - timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )).strftime('%Y-%m-%d %H:%M:%S'),
            'region': random.choice(regions),
            'device': random.choice(devices),
            'duration_sec': random.randint(1, 600),
            'product_id': f"prod_{random.randint(1000, 9999)}",
            'price': round(random.uniform(10, 1000), 2)
        }
        data.append(record)

    # Добавление "плохих" данных (10% от общего числа)
    bad_records = num_records // 10
    print(f"Добавляем {bad_records} 'плохих' записей для тестирования очистки...")

    for i in range(bad_records):
        # Создаем записи с проблемами
        problems = [
            {'user_id': ''},  # Пустой ID
            {'session_id': None},  # Отсутствующее значение
            {'timestamp': '2024-13-45 25:61:61'},  # Некорректная дата
            {'duration_sec': -random.randint(1, 100)},  # Отрицательное время
            {'device': 'smart_watch'},  # Нестандартное устройство
            {'price': -random.uniform(10, 100)},  # Отрицательная цена
            {'action': 'unknown_action'},  # Неизвестное действие
            {'region': ''}  # Пустой регион
        ]

        base_record = {
            'user_id': fake.uuid4()[:8],
            'session_id': f"sess_{fake.random_number(digits=8)}",
            'action': random.choice(actions),
            'timestamp': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d %H:%M:%S'),
            'region': random.choice(regions),
            'device': random.choice(devices),
            'duration_sec': random.randint(1, 600),
            'product_id': f"prod_{random.randint(1000, 9999)}",
            'price': round(random.uniform(10, 1000), 2)
        }

        # Добавляем случайную проблему
        problem = random.choice(problems)
        base_record.update(problem)
        data.append(base_record)

    # Создаем DataFrame

    df = pd.DataFrame(data)

    # Перемешиваем данные
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    # Сохраняем в CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding='UTF-8')

    # Выводим статистику
    print("\n" + "=" * 60)
    print("✅ ДАННЫЕ УСПЕШНО СГЕНЕРИРОВАНЫ")
    print("=" * 60)
    print(f"📊 Общее количество записей: {len(df):,}")
    print(f"📁 Файл сохранен: {output_path}")
    print("\n📈 Статистика данных:")
    print(f"  - Уникальных пользователей: {df['user_id'].nunique()}")
    print(f"  - Уникальных регионов: {df['region'].nunique()}")
    print(f"  - Диапазон дат: {df['timestamp'].min()} - {df['timestamp'].max()}")
    print(f"  - Пустых user_id: {df['user_id'].isna().sum() + (df['user_id'] == '').sum()}")
    print(f"  - Отрицательных duration_sec: {(df['duration_sec'] < 0).sum()}")

    # Показываем пример данных
    print("\n👀 Пример данных (первые 3 строки):")
    print(df.head(3).to_string())

    return df


if __name__ == "__main__":
    # Генерируем данные
    df = generate_clickstream_data(50000)

    # Дополнительная проверка
    print("\n🔍 Проверка качества данных:")
    print(df.info())