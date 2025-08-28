# orders_etl.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size

spark = SparkSession.builder \
    .appName("OrdersETL") \
    .master("local[*]") \
    .getOrCreate()

# Читаем JSON с вложенными массивами
#df = spark.read.json("/work/data/customers_with_orders.json")
df = spark.read.json("/work/data/output.ndjson")
print("Схема:")
df.printSchema()

print("Исходные данные:")
df.show(truncate=False)

# 1. Развернём массив orders
orders_exploded = df \
    .withColumn("order", explode(col("orders"))) \
    .drop("orders")

print("После explode по заказам:")
orders_exploded.select("name", "customer_id", "order.order_id", "order.amount", "order.status").show()

# 2. Доступ к полям вложенного объекта
expanded = orders_exploded.select(
    col("name"),
    col("customer_id"),
    col("order.order_id"),
    col("order.amount"),
    col("order.status"),
    col("order.items")
)
print("Развернутые поля заказов:")
expanded.show(truncate=False)

# 3. Можно даже explode по items
with_items = expanded.withColumn("item", explode(col("items"))).drop("items")
print("По каждому товару:")
with_items.select("name", "order_id", "item").show()

# 4. Агрегация: общая сумма по статусу
status_stats = with_items.groupBy("status").agg(
    {"amount": "sum", "order_id": "count"}
).withColumnRenamed("sum(amount)", "total_revenue") \
 .withColumnRenamed("count(order_id)", "order_count")

print("Статистика по статусам:")
status_stats.show()

# 5. Сохранение
status_stats.write \
    .mode("overwrite") \
    .parquet("/work/output/order_stats")

print("✅ Результат сохранён в /work/output/order_stats")
#====================================================================
import matplotlib.pyplot as plt
import seaborn as sns

sns.barplot(data=pandas_df, x="status", y="total_revenue")
plt.title("Выручка по статусам заказов")
plt.savefig("plots/revenue_by_status.png")
#====================================================================
spark.stop()