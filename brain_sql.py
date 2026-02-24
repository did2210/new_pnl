"""
brain_sql.py — Мозг с подключением к SQL Server.

Что делает:
  1. Подключается к SQL Server, читает таблицу product → строит мозг
  2. Берёт Excel-файл из папки BRAIN_FOLDER
  3. Расшифровывает xname → заполняет brand2, proizvod2, litrag, category, subcategory
  4. Сохраняет результат В ТОТ ЖЕ ФАЙЛ (без создания нового)
  5. После этого можно запустить ваш SQL-загрузчик для обновления базы

Запуск:
  python brain_sql.py
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
    """
    Обрабатывает один файл:
    - Читает Excel
    - Расшифровывает xname через мозг
    - Заполняет brand2, proizvod2, litrag, category, subcategory
    - Сохраняет ОБРАТНО в тот же файл
    """
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


def main():
    print("=" * 60)
    print("  МОЗГ + SQL SERVER")
    print("=" * 60)

    # 1) Строим мозг из SQL
    brain = build_brain_from_sql()

    # 2) Ищем файлы в папке brain
    files = find_files_in_folder(BRAIN_FOLDER)

    if not files:
        logger.info(f"\nНет файлов для обработки в папке: {BRAIN_FOLDER}")
        logger.info("Положите Excel-файлы с колонкой 'xname' в эту папку и запустите снова.")
        return

    logger.info(f"\nНайдено {len(files)} файлов для обработки:")
    for f in files:
        logger.info(f"  {os.path.basename(f)}")

    # 3) Обрабатываем каждый файл
    success = 0
    for file_path in files:
        try:
            if process_file(brain, file_path):
                success += 1
        except Exception as e:
            logger.error(f"Ошибка при обработке {os.path.basename(file_path)}: {e}")

    print(f"\nГотово! Обработано файлов: {success}/{len(files)}")
    print(f"Файлы обновлены в папке: {BRAIN_FOLDER}")
    print(f"Теперь можно запускать SQL-загрузчик для обновления базы.")


if __name__ == '__main__':
    main()
