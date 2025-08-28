# tools/json_to_ndjson.py
import json
import sys

def convert_json_to_ndjson(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()

    try:
        # Попробуем распарсить как массив
        data = json.loads(content)
        if isinstance(data, list):
            records = data
        else:
            # Если объект — оборачиваем в список
            records = [data]
    except json.JSONDecodeError as e:
        print(f"❌ Ошибка парсинга JSON: {e}")
        return

    with open(output_path, 'w', encoding='utf-8') as f:
        for record in records:
            # Записываем каждый объект в одну строку (без пробелов)
            f.write(json.dumps(record, separators=(',', ':')) + '\n')

    print(f"✅ Успешно конвертировано: {len(records)} записей → {output_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Использование: python json_to_ndjson.py input.json output.ndjson")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    convert_json_to_ndjson(input_file, output_file)