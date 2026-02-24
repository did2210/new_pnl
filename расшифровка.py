"""
расшифровка.py — Расшифровка новых товаров через мозг.

Использование:
  1. Положите файл с новыми товарами (Excel) в ту же папку
  2. Укажите путь к нему в INPUT_FILE
  3. Укажите имя столбца с названиями товаров в XNAME_COLUMN
  4. Запустите: python расшифровка.py
  5. Результат сохранится в OUTPUT_FILE
"""

import pandas as pd
from brain.core import ProductBrain

# =============================================
#  НАСТРОЙКИ — ИЗМЕНИТЕ ПОД СВОЙ ФАЙЛ
# =============================================

INPUT_FILE = 'новые_товары.xlsx'       # файл с новыми товарами
XNAME_COLUMN = 'xname'                 # название столбца с наименованием товара
OUTPUT_FILE = 'результат_расшифровки.xlsx'

# =============================================

def main():
    # 1) Загружаем мозг (уже построенный)
    print("Загрузка мозга...")
    brain = ProductBrain()
    brain.build('product.xlsx')
    print("Мозг загружен.\n")

    # 2) Читаем файл с новыми товарами
    print(f"Чтение файла: {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)
    print(f"Найдено {len(df)} строк\n")

    if XNAME_COLUMN not in df.columns:
        print(f"ОШИБКА: столбец '{XNAME_COLUMN}' не найден!")
        print(f"Доступные столбцы: {list(df.columns)}")
        return

    # 3) Расшифровываем каждую строку
    results = []
    for idx, row in df.iterrows():
        xname = str(row[XNAME_COLUMN])
        r = brain.lookup(xname)
        results.append({
            'xname': xname,
            'brand2': r.brand2,
            'proizvod2': r.proizvod2,
            'litrag': r.litrag,
            'category': r.category,
            'subcategory': r.subcategory,
            'method': r.method,
            'confidence': r.confidence,
        })

    # 4) Сохраняем результат
    df_result = pd.DataFrame(results)
    df_result.to_excel(OUTPUT_FILE, index=False)

    # 5) Статистика
    total = len(df_result)
    by_method = df_result['method'].value_counts()
    by_category = df_result['category'].value_counts()
    parsed_count = len(df_result[df_result['method'] == 'parsed'])

    print(f"Расшифровано: {total} товаров")
    print(f"Сохранено в: {OUTPUT_FILE}\n")
    print("По методу распознавания:")
    for method, count in by_method.items():
        print(f"  {method:20s} — {count}")
    print(f"\nПо категориям:")
    for cat, count in by_category.items():
        print(f"  {cat:20s} — {count}")
    if parsed_count > 0:
        print(f"\n  {parsed_count} товаров НЕ найдены в базе (method=parsed)")
        print(f"  Проверьте их в файле {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
