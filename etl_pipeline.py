"""
Профессиональный ETL-пайплайн для обработки данных кликов
Выполняет: очистку, трансформацию, агрегацию и сохранение данных
"""
import os
import sys
import time
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)


class SparkETLPipeline:
    """Класс для управления ETL-пайплайном Spark"""

    def __init__(self, master_url="spark://192.168.10.58:7077", app_name="ETL_Pipeline"):
        """
        Инициализация Spark сессии с проверенными настройками для Windows
        """
        self.start_time = datetime.now()

        try:
            spark_builder = SparkSession.builder \
                .appName(app_name) \
                .master(master_url) \
                .config("spark.sql.shuffle.partitions", "8") \
                .config("spark.executor.memory", "2g") \
                .config("spark.driver.memory", "2g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter") \
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
                .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
                .config("spark.sql.parquet.mergeSchema", "false") \
                .config("spark.network.timeout", "600s") \
                .config("spark.executor.heartbeatInterval", "60s") \
                .config("spark.sql.streaming.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

            self.spark = spark_builder.getOrCreate()

            # Раскомментируйте следующую строку, чтобы скрыть ложные предупреждения
            # self.spark.sparkContext.setLogLevel("ERROR")

            self.log("=" * 60)
            self.log(f"🚀 ИНИЦИАЛИЗАЦИЯ SPARK СЕССИИ")
            self.log(f"   Приложение: {app_name}")
            self.log(f"   Master URL: {master_url}")
            self.log(f"   Версия Spark: {self.spark.version}")
            self.log("=" * 60)

        except Exception as e:
            self.log(f"❌ ОШИБКА при создании Spark сессии: {e}", level="ERROR")
            sys.exit(1)

    def log(self, message, level="INFO"):
        """Логирование сообщений"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [{level}] {message}")

    def extract(self, file_path):
        """
        Этап EXTRACT: Загрузка данных из CSV с обработкой некорректных записей
        """
        self.log("📥 Этап EXTRACT: Загрузка данных...")

        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("region", StringType(), True),
            StructField("device", StringType(), True),
            StructField("duration_sec", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("price", DoubleType(), True)
        ])

        try:
            raw_df = self.spark.read \
                .option("header", "true") \
                .option("encoding", "UTF-8") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .schema(schema) \
                .csv(file_path)

            initial_count = raw_df.count()

            # Проверяем наличие колонки _corrupt_record
            if "_corrupt_record" in raw_df.columns:
                corrupt_count = raw_df.filter(F.col("_corrupt_record").isNotNull()).count()
                self.log(f"   Некорректных записей: {corrupt_count}")

                if corrupt_count > 0:
                    self.log(f"   ⚠️  Обнаружены некорректные записи", level="WARN")
                    corrupt_df = raw_df.filter(F.col("_corrupt_record").isNotNull())
                    corrupt_path = "../output/corrupt_records"
                    corrupt_df.select("_corrupt_record") \
                        .write \
                        .mode("overwrite") \
                        .csv(corrupt_path)
                    self.log(f"   Некорректные записи сохранены в: {corrupt_path}")

                # Фильтруем некорректные записи
                clean_df = raw_df.filter(F.col("_corrupt_record").isNull()) \
                    .drop("_corrupt_record")
            else:
                # Нет коррумпированных записей (но это не значит, что данные идеальны!)
                self.log(f"   Некорректных записей: 0")
                clean_df = raw_df

            self.log(f"   Загружено записей: {initial_count:,}")
            return clean_df

        except Exception as e:
            self.log(f"❌ ОШИБКА при загрузке данных: {e}", level="ERROR")
            raise

    def transform(self, df):
        """
        Этап TRANSFORM: Очистка и трансформация данных
        """
        self.log("🔧 Этап TRANSFORM: Обработка данных...")

        # Базовая очистка
        self.log("   1. Базовая очистка...")
        cleaned_df = df \
            .dropDuplicates(["user_id", "session_id", "timestamp"]) \
            .fillna({
                "region": "Неизвестно",
                "device": "unknown",
                "user_id": "unknown_user"
            }) \
            .filter(F.col("user_id") != "") \
            .filter(F.col("duration_sec") > 0) \
            .filter(F.col("price") >= 0)

        # Преобразование типов и добавление полей
        self.log("   2. Преобразование типов...")
        transformed_df = cleaned_df \
            .withColumn("event_timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("event_date", F.to_date("event_timestamp")) \
            .withColumn("event_hour", F.hour("event_timestamp")) \
            .withColumn("event_dayofweek", F.dayofweek("event_timestamp")) \
            .withColumn("session_category",
                        F.when(F.col("duration_sec") < 60, "short")
                         .when(F.col("duration_sec") <= 300, "medium")
                         .otherwise("long")) \
            .withColumn("price_category",
                        F.when(F.col("price") < 100, "low")
                         .when(F.col("price") <= 500, "medium")
                         .otherwise("high"))

        # Удаление записей с некорректной датой
        transformed_df = transformed_df.filter(F.col("event_timestamp").isNotNull())

        self.log(f"   После очистки: {transformed_df.count():,} записей")
        return transformed_df

    def analyze(self, df):
        """
        Этап ANALYZE: Агрегация и анализ данных
        """
        self.log("📊 Этап ANALYZE: Агрегация данных...")
        results = {}

        # 1. Активность по регионам и часам
        self.log("   1. Агрегация по регионам и времени...")
        results["activity_by_region_hour"] = df.groupBy("region", "event_date", "event_hour") \
            .agg(
                F.count("*").alias("total_events"),
                F.countDistinct("user_id").alias("unique_users"),
                F.avg("duration_sec").alias("avg_duration"),
                F.sum("price").alias("total_revenue"),
                F.avg("price").alias("avg_price")
            ) \
            .orderBy("region", "event_date", "event_hour")

        # 2. Статистика по устройствам
        self.log("   2. Статистика по устройствам...")
        results["device_statistics"] = df.groupBy("device", "session_category") \
            .agg(
                F.count("*").alias("session_count"),
                F.avg("duration_sec").alias("avg_duration"),
                F.countDistinct("user_id").alias("unique_users")
            ) \
            .orderBy("device", F.col("session_count").desc())

        # 3. Топ пользователей по активности (без оконной функции)
        self.log("   3. Топ пользователей...")
        # Сначала агрегируем по пользователям
        user_agg = df.groupBy("user_id", "region") \
            .agg(
                F.count("*").alias("total_sessions"),
                F.sum("duration_sec").alias("total_time"),
                F.sum("price").alias("total_spent")
            )
        # Берём топ-100 по числу сессий
        top100 = user_agg.orderBy(F.col("total_sessions").desc()).limit(100)
        # Добавляем ранг с помощью монотонно возрастающего ID
        results["top_users"] = top100.withColumn("rank", F.monotonically_increasing_id() + 1)

        # 4. Ежедневная активность
        self.log("   4. Ежедневная активность...")
        results["daily_activity"] = df.groupBy("event_date") \
            .agg(
                F.count("*").alias("daily_events"),
                F.countDistinct("user_id").alias("daily_users"),
                F.sum("price").alias("daily_revenue")
            ) \
            .orderBy("event_date")

        return results

    def load(self, cleaned_df, results_dict):
        """Этап LOAD: Сохранение результатов и ручная очистка временных папок"""
        self.log("💾 Этап LOAD: Сохранение результатов...")

        timestamp = time.strftime("%Y%m%d_%H%M%S")
        base_path = f"../output/run_{timestamp}"

        os.makedirs(f"{base_path}/cleaned_data", exist_ok=True)
        os.makedirs(f"{base_path}/aggregated", exist_ok=True)
        os.makedirs(f"{base_path}/reports", exist_ok=True)

        # 1. Очищенные данные в Parquet
        self.log("   1. Сохранение очищенных данных...")
        cleaned_path = f"{base_path}/cleaned_data/clickstream_cleaned"
        cleaned_df.write \
            .mode("overwrite") \
            .partitionBy("event_date") \
            .parquet(cleaned_path)
        self._clean_temp_dirs(cleaned_path)  # ручное удаление временных папок

        # 2. Сохраняем агрегированные результаты
        self.log("   2. Сохранение агрегированных данных...")
        for name, df in results_dict.items():
            # Parquet
            agg_path = f"{base_path}/aggregated/{name}"
            df.write \
                .mode("overwrite") \
                .parquet(agg_path)
            self._clean_temp_dirs(agg_path)

            # CSV
            report_path = f"{base_path}/reports/{name}_report"
            df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("header", "true") \
                .option("delimiter", ";") \
                .csv(report_path)
            self._clean_temp_dirs(report_path)

        # 3. Текстовый отчёт
        self.create_report(cleaned_df, results_dict, base_path)

        self.log(f"   📁 Результаты сохранены в: {base_path}/")

    def _clean_temp_dirs(self, path):
        """
        Рекурсивно удаляет все папки _temporary внутри path.
        Spark после этого будет пытаться удалить их сам, но выдаст ложное предупреждение,
        однако на диске мусора не остаётся.
        """
        try:
            for root, dirs, files in os.walk(path):
                if "_temporary" in dirs:
                    temp_path = os.path.join(root, "_temporary")
                    shutil.rmtree(temp_path, ignore_errors=True)
        except Exception:
            pass

    def create_report(self, cleaned_df, results_dict, base_path):
        """Создание текстового отчёта о выполнении"""
        report_path = f"{base_path}/execution_report.txt"

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write("ОТЧЕТ О ВЫПОЛНЕНИИ ETL-ПАЙПЛАЙНА\n")
            f.write("=" * 60 + "\n\n")

            f.write(f"Дата выполнения: {datetime.now()}\n")
            f.write(f"Имя приложения: {self.spark.conf.get('spark.app.name')}\n")
            f.write(f"Версия Spark: {self.spark.version}\n\n")

            f.write("СТАТИСТИКА ДАННЫХ:\n")
            f.write("-" * 40 + "\n")
            f.write(f"Количество записей после очистки: {cleaned_df.count():,}\n")
            f.write(f"Колонок в данных: {len(cleaned_df.columns)}\n")
            min_date = cleaned_df.agg(F.min('event_date')).collect()[0][0]
            max_date = cleaned_df.agg(F.max('event_date')).collect()[0][0]
            f.write(f"Диапазон дат: {min_date} - {max_date}\n")
            f.write(f"Уникальных пользователей: {cleaned_df.select('user_id').distinct().count():,}\n")
            f.write(f"Уникальных регионов: {cleaned_df.select('region').distinct().count()}\n\n")

            f.write("СОХРАНЕННЫЕ ФАЙЛЫ:\n")
            f.write("-" * 40 + "\n")
            f.write("1. Очищенные данные: output/cleaned_data/clickstream_cleaned\n")
            f.write("2. Агрегированные данные: output/aggregated/\n")
            f.write("3. Отчеты в CSV: output/reports/\n")

            # Примеры данных
            f.write("\nПРИМЕРЫ ДАННЫХ:\n")
            f.write("-" * 40 + "\n")
            f.write("Очищенные данные (первые 5 записей):\n")
            sample_data = cleaned_df.limit(5).collect()
            for row in sample_data:
                f.write(str(row) + "\n")

            f.write("\nАгрегированные данные (первые 3 записи из daily_activity):\n")
            sample_agg = results_dict["daily_activity"].limit(3).collect()
            for row in sample_agg:
                f.write(str(row) + "\n")

        self.log(f"   📄 Отчет создан: {report_path}")

    def run(self, input_path):
        """Запуск полного ETL-пайплайна"""
        try:
            raw_data = self.extract(input_path)
            cleaned_data = self.transform(raw_data)
            analysis_results = self.analyze(cleaned_data)
            self.load(cleaned_data, analysis_results)

            execution_time = datetime.now() - self.start_time
            self.log("=" * 60)
            self.log(f"✅ ETL-ПАЙПЛАЙН УСПЕШНО ВЫПОЛНЕН")
            self.log(f"   Общее время выполнения: {execution_time}")
            self.log("=" * 60)

            self.show_samples(cleaned_data, analysis_results)
            return True

        except Exception as e:
            self.log(f"❌ ОШИБКА В ПАЙПЛАЙНЕ: {e}", level="ERROR")
            import traceback
            traceback.print_exc()
            return False

    def show_samples(self, cleaned_df, results_dict):
        """Показ примеров результатов"""
        print("\n" + "=" * 60)
        print("ОБРАЗЦЫ РЕЗУЛЬТАТОВ:")
        print("=" * 60)

        print("\n1. ОЧИЩЕННЫЕ ДАННЫЕ (первые 5 строк):")
        cleaned_df.select("user_id", "action", "region", "device", "duration_sec", "event_date") \
            .show(5, truncate=False)

        print("\n2. АКТИВНОСТЬ ПО РЕГИОНАМ (топ-5):")
        results_dict["activity_by_region_hour"] \
            .groupBy("region") \
            .agg(F.sum("total_events").alias("total_events")) \
            .orderBy(F.col("total_events").desc()) \
            .show(5, truncate=False)

        print("\n3. ЕЖЕДНЕВНАЯ АКТИВНОСТЬ (последние 5 дней):")
        results_dict["daily_activity"] \
            .orderBy(F.col("event_date").desc()) \
            .show(5, truncate=False)

    def stop(self):
        """Быстрая остановка Spark сессии"""
        self.log("Остановка Spark сессии...")
        self.spark.stop()
        self.log("Spark сессия остановлена.")


def main():
    INPUT_FILE = "../data/clickstream.csv"
    MASTER_URL = "spark://192.168.10.58:7077"

    pipeline = SparkETLPipeline(
        master_url=MASTER_URL,
        app_name="Student_ETL_Pipeline"
    )

    try:
        success = pipeline.run(INPUT_FILE)
        if success:
            print("\n🎉 Поздравляем! ETL-пайплайн успешно выполнен!")
            print("   1. Проверьте папку output/")
            print("   2. Откройте Web UI Spark: http://localhost:8080")
        else:
            print("\n💥 В процессе выполнения возникли ошибки")
            print("   Проверьте вывод выше и исправьте ошибки")
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()