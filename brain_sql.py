"""
brain_sql.py — Мозг с подключением к SQL Server.

Запуск:
  python brain_sql.py

Появится меню:
  1 — Ручной ввод xname (для теста)
  2 — Обработать файлы из папки
  0 — Выход
"""

import os
import sys
import glob
import pandas as pd
import logging
from sqlalchemy import create_engine

from brain.core import ProductBrain

# =============================================================================
#  НАСТРОЙКИ
# =============================================================================

SQL_CONN_STR = (
    "mssql+pyodbc://SQL-SERVER-GFD/scandata"
    "?trusted_connection=yes"
    "&driver=ODBC+Driver+17+for+SQL+Server"
)

BRAIN_FOLDER = r"P:\новая обработка магазинов\brain"

# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('brain_sql.log', mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_product_from_sql() -> pd.DataFrame:
    """Читает таблицу product из SQL Server."""
    logger.info("Подключение к SQL Server...")
    engine = create_engine(SQL_CONN_STR, echo=False)

    query = """
        SELECT 
            id, xcode, xname, category, brand, litrag,
            catlitrag, proizvod, brand2, proizvod2,
            packqnt, pack, subcategory
        FROM product
    """

    logger.info("Чтение таблицы product...")
    df = pd.read_sql(query, engine)
    logger.info(f"Загружено {len(df)} записей из SQL")
    return df


def build_brain_from_sql() -> ProductBrain:
    """Строит мозг из данных SQL Server."""
    df = load_product_from_sql()

    brain = ProductBrain()
    brain.build_from_dataframe(df)
    brain.stats()
    return brain


# =============================================================================
#  РЕЖИМ 1: РУЧНОЙ ВВОД
# =============================================================================

def interactive_mode(brain: ProductBrain):
    """Ручной ввод xname для проверки работы мозга."""
    print("\n" + "=" * 60)
    print("  РУЧНОЙ РЕЖИМ — введите xname для расшифровки")
    print("  (пустая строка или 'выход' — назад в меню)")
    print("=" * 60)

    while True:
        print()
        xname = input("  xname > ").strip()

        if not xname or xname.lower() in ('выход', 'exit', 'quit', 'q', '0'):
            break

        result = brain.lookup(xname)

        print(f"\n  {'─' * 50}")
        print(f"  Вход:        {xname}")
        print(f"  {'─' * 50}")
        print(f"  brand2:      {result.brand2}")
        print(f"  proizvod2:   {result.proizvod2}")
        print(f"  litrag:      {result.litrag}")
        print(f"  category:    {result.category}")
        print(f"  subcategory: {result.subcategory}")
        print(f"  {'─' * 50}")
        print(f"  метод:       {result.method}")
        print(f"  уверенность: {result.confidence:.0f}%")

        if result.method == 'parsed':
            if result.category == 'ЭНЕРГЕТИКИ':
                print(f"  >>> НОВЫЙ ЭНЕРГЕТИК — нет в базе")
            elif result.category == 'ГАЗИРОВКА':
                print(f"  >>> Новая газировка — записан как LOCAL")
            else:
                print(f"  >>> Не найден в базе — авторазбор")
        print(f"  {'─' * 50}")


# =============================================================================
#  РЕЖИМ 2: ОБРАБОТКА ФАЙЛОВ
# =============================================================================

def find_files_in_folder(folder: str) -> list[str]:
    """Находит все Excel-файлы в папке."""
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
        logger.info(f"Создана папка: {folder}")

    patterns = ['*.xlsx', '*.xls']
    files = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(folder, pat)))

    files = [f for f in files if not os.path.basename(f).startswith('~')]
    return sorted(files)


def process_file(brain: ProductBrain, file_path: str):
    """Обрабатывает один файл: расшифровывает xname, сохраняет в тот же файл."""
    file_name = os.path.basename(file_path)
    logger.info(f"Обработка файла: {file_name}")

    df = pd.read_excel(file_path)
    logger.info(f"  Строк: {len(df)}")

    if 'xname' not in df.columns:
        logger.error(f"  Столбец 'xname' не найден! Пропускаю файл.")
        return False

    for col in ['brand2', 'proizvod2', 'litrag', 'category', 'subcategory']:
        if col not in df.columns:
            df[col] = None

    updated = 0
    skipped = 0

    for idx, row in df.iterrows():
        xname = row.get('xname', '')
        if pd.isna(xname) or str(xname).strip() == '':
            skipped += 1
            continue

        result = brain.lookup(str(xname))

        if result.found and result.confidence >= 30:
            if result.brand2:
                df.at[idx, 'brand2'] = result.brand2
            if result.proizvod2 and result.proizvod2 != 'UNKNOWN':
                df.at[idx, 'proizvod2'] = result.proizvod2
            if result.litrag and result.litrag > 0:
                df.at[idx, 'litrag'] = result.litrag
            if result.category:
                df.at[idx, 'category'] = result.category.lower()
            if result.subcategory:
                df.at[idx, 'subcategory'] = result.subcategory
            updated += 1
            logger.info(f"    [{result.method} {result.confidence:.0f}%] "
                         f"{str(xname)[:50]} -> {result.brand2} / "
                         f"{result.category} / {result.litrag}")
        else:
            skipped += 1

    logger.info(f"  Обновлено: {updated}, пропущено (пустых): {skipped}")

    df.to_excel(file_path, index=False)
    logger.info(f"  Сохранено обратно в: {file_name}")
    return True


def file_mode(brain: ProductBrain):
    """Обработка файлов из папки."""
    files = find_files_in_folder(BRAIN_FOLDER)

    if not files:
        print(f"\n  Нет файлов в папке: {BRAIN_FOLDER}")
        print(f"  Положите Excel с колонкой 'xname' и запустите снова.")
        return

    print(f"\n  Найдено {len(files)} файлов:")
    for f in files:
        print(f"    {os.path.basename(f)}")

    confirm = input(f"\n  Обработать? (да/нет) > ").strip().lower()
    if confirm not in ('да', 'yes', 'y', 'д'):
        print("  Отменено.")
        return

    success = 0
    for file_path in files:
        try:
            if process_file(brain, file_path):
                success += 1
        except Exception as e:
            logger.error(f"Ошибка: {os.path.basename(file_path)}: {e}")

    print(f"\n  Готово! Обработано: {success}/{len(files)}")
    print(f"  Файлы обновлены в: {BRAIN_FOLDER}")


# =============================================================================
#  ГЛАВНОЕ МЕНЮ
# =============================================================================

def main():
    print("=" * 60)
    print("  МОЗГ + SQL SERVER")
    print("=" * 60)

    brain = build_brain_from_sql()

    while True:
        print("\n" + "=" * 60)
        print("  МЕНЮ")
        print("=" * 60)
        print("  1 — Ручной ввод xname (тест)")
        print(f"  2 — Обработать файлы из папки")
        print(f"      ({BRAIN_FOLDER})")
        print("  0 — Выход")
        print("=" * 60)

        choice = input("\n  Выбор > ").strip()

        if choice == '1':
            interactive_mode(brain)
        elif choice == '2':
            file_mode(brain)
        elif choice in ('0', 'выход', 'exit', 'quit', 'q'):
            print("\n  До свидания!")
            break
        else:
            print("  Неверный выбор. Введите 1, 2 или 0.")


if __name__ == '__main__':
    main()
