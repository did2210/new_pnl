# -*- coding: utf-8 -*-
"""
Шаг 3 — Объединение всех обработанных файлов в единую базу.
"""
import os
import glob
import logging
from typing import List, Optional

import pandas as pd

from .helpers import C

logger = logging.getLogger(__name__)


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in ('start_date', 'end_date', 'pdate'):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%d.%m.%Y', errors='coerce')
    return df


def _dedup(df: pd.DataFrame) -> pd.DataFrame:
    sort_cols = [c for c in ('viveska', 'sku_type_sap', 'pdate', 'start_date') if c in df.columns]
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
    __slots__ = ('df', 'total_input', 'total_output', 'duplicates_removed', 'errors')
    def __init__(self):
        self.df = pd.DataFrame()
        self.total_input = 0
        self.total_output = 0
        self.duplicates_removed = 0
        self.errors = []


def merge(dataframes: List[pd.DataFrame], existing_db: str = None) -> MergeResult:
    """
    Объединяет список DataFrame + (опционально) существующую базу.
    Дедуплицирует по (viveska, sku_type_sap, pdate).
    """
    res = MergeResult()
    parts = []

    # существующая база
    if existing_db and os.path.exists(existing_db):
        try:
            base = pd.read_excel(existing_db)
            base = _parse_dates(base)
            parts.append(base)
            logger.info(f"  База загружена: {len(base)} строк")
        except Exception as e:
            res.errors.append(f"Ошибка загрузки базы: {e}")

    for df in dataframes:
        if df is not None and not df.empty:
            df = _parse_dates(df.copy())
            parts.append(df)

    if not parts:
        res.errors.append("Нет данных для объединения")
        return res

    combined = pd.concat(parts, ignore_index=True)
    res.total_input = len(combined)
    combined = _dedup(combined)
    res.total_output = len(combined)
    res.duplicates_removed = res.total_input - res.total_output
    res.df = combined
    return res


def merge_folder(folder: str, existing_db: str = None) -> MergeResult:
    """Объединяет все .xlsx из папки."""
    files = sorted(glob.glob(os.path.join(folder, '*.xlsx')))
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_excel(f))
        except Exception as e:
            logger.warning(f"  Не удалось прочитать {os.path.basename(f)}: {e}")
    return merge(dfs, existing_db)
