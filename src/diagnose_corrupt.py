# diagnose_corrupt.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("DiagnoseCorrupt") \
    .master("local[*]") \
    .getOrCreate()

# Читаем с поддержкой битых записей
df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("/work/data/customers.ndjson")

print("=== Все данные ===")
df.show(truncate=False)

# Покажем битые строки
corrupt_df = df.filter(col("_corrupt_record").isNotNull())
if corrupt_df.count() > 0:
    print("❌ Найдены битые строки:")
    corrupt_df.select("_corrupt_record").show(truncate=False)
else:
    print("✅ Все строки валидны")

# Покажем схему
print("=== Схема ===")
df.printSchema()

spark.stop()