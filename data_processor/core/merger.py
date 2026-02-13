# -*- coding: utf-8 -*-
"""
Шаг 3 — Объединение всех обработанных файлов в единую базу.

Ключевой принцип: существует ОДНА постоянная база contracts_db.xlsx,
которая пополняется новыми данными при каждом запуске.
Дедупликация по (viveska, sku_type_sap, pdate) — новые записи приоритетнее.
"""
import os
import glob
import logging
import shutil
from datetime import datetime
from typing import List

import pandas as pd

from .helpers import C

logger = logging.getLogger(__name__)

# Имя постоянной базы
DB_FILENAME = 'contracts_db.xlsx'


def _normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Приводим даты к единому формату — datetime."""
    for col in ('start_date', 'end_date', 'pdate'):
        if col not in df.columns:
            continue
        # Если уже datetime — ничего не делаем
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        # Пробуем формат дд.мм.гггг, потом любой
        df[col] = pd.to_datetime(df[col], format='%d.%m.%Y', errors='coerce')
        # Те что не распарсились — пробуем универсально
        mask = df[col].isna()
        if mask.any():
            df.loc[mask, col] = pd.to_datetime(
                df.loc[mask, col], dayfirst=True, errors='coerce')
    return df


def _dedup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Дедупликация: приоритет у записей с более поздней start_date
    (то есть новые контракты перебивают старые).
    """
    sort_cols = [c for c in ('viveska', 'sku_type_sap', 'pdate', 'start_date')
                 if c in df.columns]
    if sort_cols:
        asc = [True] * len(sort_cols)
        if 'start_date' in sort_cols:
            asc[sort_cols.index('start_date')] = False
        df = df.sort_values(by=sort_cols, ascending=asc)

    key_cols = [c for c in ('viveska', 'sku_type_sap', 'pdate') if c in df.columns]
    if key_cols:
        before = len(df)
        df = df.drop_duplicates(subset=key_cols, keep='first').reset_index(drop=True)
        removed = before - len(df)
        if removed > 0:
            logger.info(f"  Удалено дубликатов: {removed}")
    return df


class MergeResult:
    __slots__ = ('df', 'total_input', 'total_output', 'duplicates_removed',
                 'db_loaded', 'db_rows_before', 'errors')

    def __init__(self):
        self.df = pd.DataFrame()
        self.total_input = 0
        self.total_output = 0
        self.duplicates_removed = 0
        self.db_loaded = False
        self.db_rows_before = 0
        self.errors = []


def get_db_path(output_dir: str) -> str:
    """Путь к постоянной базе."""
    return os.path.join(output_dir, DB_FILENAME)


def load_db(output_dir: str) -> pd.DataFrame:
    """Загрузить постоянную базу (если существует)."""
    path = get_db_path(output_dir)
    if os.path.exists(path):
        try:
            df = pd.read_excel(path)
            df = _normalize_dates(df)
            return df
        except Exception as e:
            logger.warning(f"Ошибка загрузки базы: {e}")
    return pd.DataFrame()


def merge(dataframes: List[pd.DataFrame],
          output_dir: str,
          existing_db: str = None) -> MergeResult:
    """
    Объединяет новые DataFrame-ы с постоянной базой.

    Порядок приоритетов (первый wins при дедупликации):
      1. Новые данные (из текущего запуска)
      2. Данные из existing_db (внешний файл, если указан)
      3. Данные из contracts_db.xlsx (постоянная база)
    """
    res = MergeResult()
    parts_new = []   # новые данные — высший приоритет
    parts_old = []   # старые данные

    # ── загружаем постоянную базу ──
    db_path = get_db_path(output_dir)
    if os.path.exists(db_path):
        try:
            db_df = pd.read_excel(db_path)
            db_df = _normalize_dates(db_df)
            res.db_loaded = True
            res.db_rows_before = len(db_df)
            parts_old.append(db_df)
            logger.info(f"  Постоянная база: {len(db_df)} строк")
        except Exception as e:
            res.errors.append(f"Ошибка загрузки постоянной базы: {e}")

    # ── внешняя существующая база (если указана и это не та же постоянная) ──
    if existing_db and os.path.exists(existing_db):
        abs_ext = os.path.abspath(existing_db)
        abs_db = os.path.abspath(db_path)
        if abs_ext != abs_db:
            try:
                ext_df = pd.read_excel(existing_db)
                ext_df = _normalize_dates(ext_df)
                parts_old.append(ext_df)
                logger.info(f"  Внешняя база: {len(ext_df)} строк")
            except Exception as e:
                res.errors.append(f"Ошибка загрузки внешней базы: {e}")

    # ── новые данные ──
    for df in dataframes:
        if df is not None and not df.empty:
            df = _normalize_dates(df.copy())
            parts_new.append(df)

    if not parts_new and not parts_old:
        res.errors.append("Нет данных для объединения")
        return res

    # Сначала новые (приоритет), потом старые
    all_parts = parts_new + parts_old
    combined = pd.concat(all_parts, ignore_index=True)
    res.total_input = len(combined)
    combined = _dedup(combined)
    res.total_output = len(combined)
    res.duplicates_removed = res.total_input - res.total_output
    res.df = combined

    # ── СОХРАНЯЕМ ПОСТОЯННУЮ БАЗУ ──
    os.makedirs(output_dir, exist_ok=True)
    # Бэкап
    if os.path.exists(db_path):
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup = os.path.join(output_dir, f"contracts_db_backup_{ts}.xlsx")
        try:
            shutil.copy2(db_path, backup)
        except Exception:
            pass
    # Записываем обновлённую базу
    try:
        combined.to_excel(db_path, index=False)
        logger.info(f"  Постоянная база обновлена: {db_path} ({len(combined)} строк)")
    except Exception as e:
        res.errors.append(f"Ошибка сохранения базы: {e}")

    return res
