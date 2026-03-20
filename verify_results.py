import os
from pyspark.sql import SparkSession


def verify_results():
    """Проверка результатов выполнения ETL"""

    spark = SparkSession.builder \
        .appName("VerifyResults") \
        .master("local[*]") \
        .getOrCreate()

    print("🔍 ПРОВЕРКА РЕЗУЛЬТАТОВ ETL")
    print("=" * 60)

    # Проверяем наличие файлов
    base_path = "../output"

    if not os.path.exists(base_path):
        print("❌ Папка output не найдена!")
        return

    # 1. Проверяем очищенные данные
    parquet_path = f"{base_path}/cleaned_data/clickstream_cleaned"
    if os.path.exists(parquet_path):
        print("\n1. ОЧИЩЕННЫЕ ДАННЫЕ:")
        df = spark.read.parquet(parquet_path)
        print(f"   Записей: {df.count():,}")
        print(f"   Колонок: {len(df.columns)}")
        print("   Пример данных:")
        df.select("user_id", "action", "event_date").show(3, truncate=False)
    else:
        print("❌ Очищенные данные не найдены")

    # 2. Проверяем агрегированные данные
    aggregated_path = f"{base_path}/aggregated"
    if os.path.exists(aggregated_path):
        print("\n2. АГРЕГИРОВАННЫЕ ДАННЫЕ:")
        for folder in os.listdir(aggregated_path):
            folder_path = os.path.join(aggregated_path, folder)
            if os.path.isdir(folder_path):
                df = spark.read.parquet(folder_path)
                print(f"   {folder}: {df.count()} записей")
    else:
        print("❌ Агрегированные данные не найдены")

    # 3. Проверяем отчеты
    reports_path = f"{base_path}/reports"
    if os.path.exists(reports_path):
        print("\n3. ОТЧЕТЫ:")
        for folder in os.listdir(reports_path):
            folder_path = os.path.join(reports_path, folder)
            if os.path.isdir(folder_path):
                # Ищем CSV файлы
                for file in os.listdir(folder_path):
                    if file.endswith('.csv'):
                        csv_path = os.path.join(folder_path, file)
                        df = spark.read.csv(csv_path, header=True, sep=";")
                        print(f"   {file}: {df.count()} записей")
    else:
        print("❌ Отчеты не найдены")

    # 4. Проверяем текстовый отчет
    report_file = f"{base_path}/execution_report.txt"
    if os.path.exists(report_file):
        print("\n4. ТЕКСТОВЫЙ ОТЧЕТ:")
        with open(report_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines[:10]:  # Показываем первые 10 строк
                print(f"   {line.strip()}")
    else:
        print("❌ Текстовый отчет не найден")

    spark.stop()

    print("\n" + "=" * 60)
    print("✅ ПРОВЕРКА ЗАВЕРШЕНА")
    print("=" * 60)


if __name__ == "__main__":
    verify_results()