from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DiagnoseJSON") \
    .master("local[*]") \
    .getOrCreate()

# Читаем с режимом PERMISSIVE и колонкой для битых строк
df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("/work/data/customers_nested.json")

# Сохраняем результат чтения во временный Parquet (кэшируем структуру)
df.cache()  # держим в памяти
df.count()  # триггеруем выполнение чтения

print("=== Все строки ===")
df.show(truncate=False)

print("=== Битые строки ===")
try:
    corrupt = df.filter(df["_corrupt_record"].isNotNull())
    if corrupt.count() > 0:
        corrupt.show(truncate=False)
    else:
        print("Битые строки не найдены.")
except Exception as e:
    print("Колонка _corrupt_record отсутствует — все строки валидны.")

spark.stop()