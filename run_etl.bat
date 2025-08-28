@echo on
echo Конвертируем JSON в NDJSON...
jq -c . data/customers_with_orders.json > data/customers.ndjson

echo Запускаем Spark...
docker run -it --rm ^
  -v "%cd%:/work" ^
  --entrypoint="" ^
  apache/spark ^
  /opt/spark/bin/spark-submit ^
  /work/src/etl_orders.py

pause