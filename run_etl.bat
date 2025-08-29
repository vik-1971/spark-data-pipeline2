@echo on

echo Запускаем Spark...
docker run -it --rm ^
  -v "%cd%:/work" ^
  --entrypoint="" ^
  apache/spark ^
  /opt/spark/bin/spark-submit ^
  /work/src/diagnose_json.py

pause