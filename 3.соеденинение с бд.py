import pandas as pd
import logging
import os
import glob

# --- Логирование ---
log_filename = 'merge_log.txt'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

input_file = r'C:\Users\metelkov\Desktop\эцп тест\merged_cleaned_contracts.xlsx'
final_folder = r'C:\Users\metelkov\Desktop\эцп тест\file_new\final'
output_dir = r'C:\Users\metelkov\Desktop\эцп тест\merged_contracts'
os.makedirs(output_dir, exist_ok=True)

def load_excel(path: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(path)
        logger.info(f"Загружен файл: '{path}' с размером {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Ошибка загрузки файла '{path}': {e}")
        return pd.DataFrame()

def parse_dates_with_format(df: pd.DataFrame, columns: list, date_format: str) -> pd.DataFrame:
    """
    Конвертация столбцов с датами с явным форматом,
    чтобы избежать неправильного парсинга дат.
    """
    for col in columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')
            except Exception as e:
                logger.warning(f"Не удалось конвертировать столбец '{col}' с форматом '{date_format}': {e}")
                df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def clean_and_sort(df: pd.DataFrame) -> pd.DataFrame:
    # Сортируем с приоритетом по более позднему start_date
    sort_cols = ['viveska', 'sku_type_sap', 'pdate', 'start_date']
    df = df.sort_values(by=sort_cols, ascending=[True, True, True, False])
    # Удаляем дубликаты по ключам
    key_cols = ['viveska', 'sku_type_sap', 'pdate']
    df = df.drop_duplicates(subset=key_cols, keep='first').reset_index(drop=True)
    return df

if __name__ == '__main__':
    # Загрузка базы
    base_df = load_excel(input_file)
    # Парсим даты в формате "день.месяц.год" (например, 01.07.2024)
    base_df = parse_dates_with_format(base_df, ['start_date', 'end_date', 'pdate'], '%d.%m.%Y')
    base_df = clean_and_sort(base_df)

    # Загрузка новых файлов из папки final
    files = glob.glob(os.path.join(final_folder, '*.xlsx'))
    new_dfs = []
    for f in files:
        df_new = load_excel(f)
        if not df_new.empty:
            df_new = parse_dates_with_format(df_new, ['start_date', 'end_date', 'pdate'], '%d.%m.%Y')
            df_new = clean_and_sort(df_new)
            new_dfs.append(df_new)

    if new_dfs:
        new_df = pd.concat(new_dfs, ignore_index=True)
        new_df = clean_and_sort(new_df)
    else:
        new_df = pd.DataFrame()

    # Объединение и итоговая очистка
    combined = pd.concat([base_df, new_df], ignore_index=True)
    combined = clean_and_sort(combined)

    # Сохраняем в файл
    output_path = os.path.join(output_dir, 'merged_cleaned_contracts.xlsx')
    try:
        combined.to_excel(output_path, index=False)
        logger.info(f"Итоговый файл сохранён: {output_path}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла: {e}")
