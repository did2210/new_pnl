"""
Шаг 3: Объединение всех обработанных файлов.

Логика из оригинального кода "3.соеденинение с бд.py":
- Загружает существующую базу данных (если есть)
- Загружает новые обработанные файлы
- Объединяет, сортирует, удаляет дубликаты
- Сохраняет итоговый файл
"""
import os
import logging
import glob

import pandas as pd

logger = logging.getLogger(__name__)


def parse_dates_with_format(df: pd.DataFrame, columns: list, date_format: str = '%d.%m.%Y') -> pd.DataFrame:
    """Конвертация столбцов с датами с явным форматом."""
    for col in columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')
            except Exception as e:
                logger.warning(f"Не удалось конвертировать '{col}' с форматом '{date_format}': {e}")
                df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def clean_and_sort(df: pd.DataFrame) -> pd.DataFrame:
    """Сортировка и удаление дубликатов."""
    sort_cols = ['viveska', 'sku_type_sap', 'pdate', 'start_date']
    available_sort_cols = [c for c in sort_cols if c in df.columns]

    if available_sort_cols:
        ascending = [True] * len(available_sort_cols)
        if 'start_date' in available_sort_cols:
            ascending[available_sort_cols.index('start_date')] = False
        df = df.sort_values(by=available_sort_cols, ascending=ascending)

    key_cols = ['viveska', 'sku_type_sap', 'pdate']
    available_key_cols = [c for c in key_cols if c in df.columns]
    if available_key_cols:
        df = df.drop_duplicates(subset=available_key_cols, keep='first').reset_index(drop=True)

    return df


def load_excel_safe(path: str) -> pd.DataFrame:
    """Безопасная загрузка Excel файла."""
    try:
        df = pd.read_excel(path)
        logger.info(f"Загружен: '{os.path.basename(path)}' [{df.shape[0]} строк x {df.shape[1]} колонок]")
        return df
    except Exception as e:
        logger.error(f"Ошибка загрузки '{path}': {e}")
        return pd.DataFrame()


def merge_dataframes(dataframes: list, existing_db_path: str = None) -> pd.DataFrame:
    """
    Объединяет список DataFrame-ов и (опционально) существующую базу.
    
    Args:
        dataframes: список DataFrame для объединения
        existing_db_path: путь к существующей базе данных (Excel файл), если есть
    
    Returns:
        Объединённый и очищенный DataFrame
    """
    all_dfs = []

    # Загружаем существующую базу, если указана
    if existing_db_path and os.path.exists(existing_db_path):
        base_df = load_excel_safe(existing_db_path)
        if not base_df.empty:
            base_df = parse_dates_with_format(base_df, ['start_date', 'end_date', 'pdate'])
            base_df = clean_and_sort(base_df)
            all_dfs.append(base_df)
            logger.info(f"Загружена существующая база: {len(base_df)} строк")

    # Добавляем новые DataFrame-ы
    for df in dataframes:
        if df is not None and not df.empty:
            df = parse_dates_with_format(df, ['start_date', 'end_date', 'pdate'])
            df = clean_and_sort(df)
            all_dfs.append(df)

    if not all_dfs:
        logger.warning("Нет данных для объединения")
        return pd.DataFrame()

    # Объединяем
    combined = pd.concat(all_dfs, ignore_index=True)
    combined = clean_and_sort(combined)

    logger.info(f"Итоговый размер после объединения: {len(combined)} строк")
    return combined


def merge_files_from_folder(folder_path: str, existing_db_path: str = None) -> pd.DataFrame:
    """
    Объединяет все Excel файлы из папки.
    
    Args:
        folder_path: путь к папке с файлами
        existing_db_path: путь к существующей базе данных
    
    Returns:
        Объединённый DataFrame
    """
    files = glob.glob(os.path.join(folder_path, '*.xlsx'))
    logger.info(f"Найдено {len(files)} файлов в {folder_path}")

    new_dfs = []
    for f in files:
        df = load_excel_safe(f)
        if not df.empty:
            new_dfs.append(df)

    return merge_dataframes(new_dfs, existing_db_path)
