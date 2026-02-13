"""
Шаг 2: Парсинг обработанных файлов в структурированный формат.

Логика из оригинального кода "2.обработка обработаных в нужный формат.py":
- Чтение листов GFD Запрос, Условия контракта, Планирование продаж, Планирование инвестиций
- Извлечение метаданных контракта (филиал, вывеска, КАМ, даты и т.д.)
- Парсинг еженедельных данных продаж и инвестиций
- Распределение данных по месяцам контракта
- Формирование итоговых строк по каждому SKU и месяцу
"""
import os
import re
import logging
import traceback
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from datetime import datetime

from utils import (
    safe_to_float, parse_any_date, get_russian_month_name, month_name_to_num,
    get_contract_months, build_ecp_calendar, get_week_number_ecp
)
from config import (
    SKU_MAPPING, SKU_MAPPING_REVERSE, MONTH_NAMES_RU, FINAL_COLUMNS_ORDER
)

logger = logging.getLogger(__name__)

# Глобальный кэш календаря ЭЦП
_ECP_CALENDAR = None


def _get_ecp_calendar():
    global _ECP_CALENDAR
    if _ECP_CALENDAR is None:
        _ECP_CALENDAR = build_ecp_calendar()
    return _ECP_CALENDAR


# =============================================================================
# СТРУКТУРЫ ДАННЫХ ПЛАНА
# =============================================================================

@dataclass
class PlanMonth:
    month_num: int
    month_name: str
    column_indices: list
    week_names: list
    week_numbers: list


@dataclass
class PlanCalendar:
    months: dict
    week_to_month: dict
    week_to_col: dict
    month_row_idx: int
    week_row_idx: int


def parse_plan_calendar(df_sales: pd.DataFrame) -> Optional[PlanCalendar]:
    """Парсит шапку листа 'Планирование продаж'."""
    exclude_markers = ['тотал', 'total', 'итого', 'прирост', 'всего', 'growth']

    month_row_idx = None
    for row_idx in range(len(df_sales)):
        row = df_sales.iloc[row_idx].astype(str)
        row_join = ' '.join([c for c in row if c and c.lower() != 'nan'])
        if any(m in row_join.lower() for m in MONTH_NAMES_RU):
            month_row_idx = row_idx
            break

    if month_row_idx is None:
        logger.warning("Не найдена строка с месяцами")
        return None

    week_row_idx = None
    for check_row in range(month_row_idx + 1, min(month_row_idx + 3, len(df_sales))):
        row = df_sales.iloc[check_row].astype(str)
        week_count = sum(1 for cell in row if re.match(r'^W\d{1,2}$', str(cell).strip().upper()))
        if week_count >= 10:
            week_row_idx = check_row
            break

    if week_row_idx is None:
        week_row_idx = month_row_idx + 1

    if week_row_idx >= len(df_sales):
        return None

    row_months = df_sales.iloc[month_row_idx].astype(str)
    row_weeks = df_sales.iloc[week_row_idx].astype(str)

    months = {}
    week_to_month = {}
    week_to_col = {}
    current_month_num = None
    current_month_name = None

    for col_idx in range(len(row_months)):
        week_cell_raw = str(row_weeks.iloc[col_idx]).strip()
        if any(marker in week_cell_raw.lower() for marker in exclude_markers):
            continue

        month_cell = str(row_months.iloc[col_idx]).strip().lower()
        for idx, month_name in enumerate(MONTH_NAMES_RU):
            if month_name in month_cell:
                current_month_num = idx + 1
                current_month_name = month_name
                if current_month_num not in months:
                    months[current_month_num] = PlanMonth(
                        month_num=current_month_num, month_name=current_month_name,
                        column_indices=[], week_names=[], week_numbers=[]
                    )
                break

        week_cell = week_cell_raw.upper()
        m = re.match(r'^W(\d{1,2})$', week_cell)
        if m and current_month_num is not None:
            week_num = int(m.group(1))
            months[current_month_num].column_indices.append(col_idx)
            months[current_month_num].week_names.append(week_cell)
            months[current_month_num].week_numbers.append(week_num)
            week_to_month[week_num] = current_month_num
            week_to_col[week_num] = col_idx

    if not months:
        return None

    return PlanCalendar(
        months=months, week_to_month=week_to_month, week_to_col=week_to_col,
        month_row_idx=month_row_idx, week_row_idx=week_row_idx
    )


# =============================================================================
# ИЗВЛЕЧЕНИЕ ДАННЫХ ПО НЕДЕЛЯМ
# =============================================================================

def extract_weekly_data(df_sales, sku_name, plan_calendar, row_type="Новый контракт"):
    """Извлекает данные по неделям из плана."""
    result = {}
    for i in range(plan_calendar.week_row_idx + 1, len(df_sales)):
        sku_cell = str(df_sales.iloc[i, 0]).strip() if pd.notna(df_sales.iloc[i, 0]) else ""
        type_cell = str(df_sales.iloc[i, 2]).strip() if len(df_sales.iloc[i]) > 2 and pd.notna(df_sales.iloc[i, 2]) else ""
        if sku_name in sku_cell and row_type in type_cell:
            row_data = df_sales.iloc[i]
            for week_num, col_idx in plan_calendar.week_to_col.items():
                if col_idx < len(row_data):
                    result[week_num] = safe_to_float(row_data.iloc[col_idx])
            break
    return result


def extract_tm_plan_weekly(df_sales, sku_name, plan_calendar):
    """Извлекает ТМ-план по неделям."""
    result = {}
    for i in range(plan_calendar.week_row_idx + 1, len(df_sales)):
        sku_cell = str(df_sales.iloc[i, 0]).strip() if pd.notna(df_sales.iloc[i, 0]) else ""
        type_cell = str(df_sales.iloc[i, 2]).strip() if len(df_sales.iloc[i]) > 2 and pd.notna(df_sales.iloc[i, 2]) else ""
        if sku_name in sku_cell and "ТМ-план" in type_cell:
            row_data = df_sales.iloc[i]
            for week_num, col_idx in plan_calendar.week_to_col.items():
                if col_idx < len(row_data):
                    try:
                        clean_value = str(row_data.iloc[col_idx]).replace('%', '').replace(' ', '').replace(',', '.').strip()
                        if clean_value and clean_value.replace('.', '').replace('-', '').isdigit():
                            result[week_num] = float(clean_value)
                        else:
                            result[week_num] = 0.0
                    except Exception:
                        result[week_num] = 0.0
            break
    return result


def extract_price_weekly(df_sales, sku_name, plan_calendar):
    """Извлекает цену по неделям."""
    result = {}
    for i in range(plan_calendar.week_row_idx + 1, len(df_sales)):
        row = df_sales.iloc[i].astype(str).str.strip()
        sku_cell = str(df_sales.iloc[i, 0]).strip() if pd.notna(df_sales.iloc[i, 0]) else ""
        if sku_name in sku_cell and any("Цена поставки" in cell for cell in row):
            row_data = df_sales.iloc[i]
            for week_num, col_idx in plan_calendar.week_to_col.items():
                if col_idx < len(row_data):
                    result[week_num] = safe_to_float(row_data.iloc[col_idx])
            break
    return result


# =============================================================================
# РАСПРЕДЕЛЕНИЕ ДАННЫХ ПО МЕСЯЦАМ КОНТРАКТА
# =============================================================================

def distribute_weekly_to_months(weekly_data, plan_calendar, start_date, end_date):
    """Распределяет недельные данные по месяцам контракта."""
    ecp_cal = _get_ecp_calendar()
    result = {}

    contract_months = get_contract_months(start_date, end_date)
    for year, month in contract_months:
        result[(year, month)] = 0.0

    start_week_ecp = get_week_number_ecp(start_date)

    start_global_week = None
    for global_week, (cal_year, cal_month, week_in_year) in ecp_cal.items():
        if cal_year == start_date.year and week_in_year == start_week_ecp:
            start_global_week = global_week
            break

    if start_global_week is None:
        return result

    for i in range(52):
        global_week = start_global_week + i
        if global_week not in ecp_cal:
            break
        week_year, week_month, week_in_year = ecp_cal[global_week]
        if (week_year, week_month) not in result:
            break
        result[(week_year, week_month)] += weekly_data.get(week_in_year, 0.0)

    return result


def calculate_monthly_tm(weekly_tm, plan_calendar, start_date, end_date):
    """Вычисляет ТМ-план по месяцам (максимум за месяц)."""
    ecp_cal = _get_ecp_calendar()
    result = {}
    contract_months = get_contract_months(start_date, end_date)
    for y, m in contract_months:
        result[(y, m)] = 0.0

    start_week_ecp = get_week_number_ecp(start_date)
    start_global_week = None
    for global_week, (cal_year, cal_month, week_in_year) in ecp_cal.items():
        if cal_year == start_date.year and week_in_year == start_week_ecp:
            start_global_week = global_week
            break

    if start_global_week is None:
        return result

    month_values = {key: [] for key in result.keys()}
    for i in range(52):
        gw = start_global_week + i
        if gw not in ecp_cal:
            break
        wy, wm, wiy = ecp_cal[gw]
        if (wy, wm) not in month_values:
            break
        month_values[(wy, wm)].append(weekly_tm.get(wiy, 0.0))

    for key, values in month_values.items():
        result[key] = max(values) if values else 0.0
    return result


def calculate_monthly_price(weekly_price, plan_calendar, start_date, end_date):
    """Вычисляет среднюю цену по месяцам."""
    ecp_cal = _get_ecp_calendar()
    result = {}
    contract_months = get_contract_months(start_date, end_date)
    for y, m in contract_months:
        result[(y, m)] = 0.0

    start_week_ecp = get_week_number_ecp(start_date)
    start_global_week = None
    for global_week, (cal_year, cal_month, week_in_year) in ecp_cal.items():
        if cal_year == start_date.year and week_in_year == start_week_ecp:
            start_global_week = global_week
            break

    if start_global_week is None:
        return result

    month_prices = {key: [] for key in result.keys()}
    for i in range(52):
        gw = start_global_week + i
        if gw not in ecp_cal:
            break
        wy, wm, wiy = ecp_cal[gw]
        if (wy, wm) not in month_prices:
            break
        price_value = weekly_price.get(wiy, 0.0)
        if price_value > 0:
            month_prices[(wy, wm)].append(price_value)

    for key, prices in month_prices.items():
        result[key] = round(sum(prices) / len(prices), 2) if prices else 0.0
    return result


def calculate_prom_vol(weekly_vols, weekly_tm, plan_calendar, start_date, end_date):
    """Вычисляет PromVol по месяцам."""
    ecp_cal = _get_ecp_calendar()
    result = {}
    contract_months = get_contract_months(start_date, end_date)
    for y, m in contract_months:
        result[(y, m)] = 0

    start_week_ecp = get_week_number_ecp(start_date)
    start_global_week = None
    for global_week, (cal_year, cal_month, week_in_year) in ecp_cal.items():
        if cal_year == start_date.year and week_in_year == start_week_ecp:
            start_global_week = global_week
            break

    if start_global_week is None:
        return result

    for i in range(52):
        gw = start_global_week + i
        if gw not in ecp_cal:
            break
        wy, wm, wiy = ecp_cal[gw]
        if (wy, wm) not in result:
            break
        if weekly_tm.get(wiy, 0.0) > 0:
            result[(wy, wm)] += int(round(weekly_vols.get(wiy, 0.0)))

    return result


# =============================================================================
# ИЗВЛЕЧЕНИЕ ИНВЕСТИЦИЙ
# =============================================================================

def find_investment_sections(df_sheet):
    """Находит строки и позиции для листинга и маркетинга."""
    listing_month_indices = {}
    marketing_month_indices = {}

    listing_header_variants = ["Период оплаты за Листинг, руб. с НДС 20%"]
    marketing_header_variants = [
        "Период оплаты бюджета Маркетинга, руб. с НДС 20%",
        "Период оплаты бюджета Маркетинга, руб. с НДС  20%",
    ]

    # Поиск объединённой строки
    combined_row_idx = None
    for row_idx in range(len(df_sheet)):
        row_data = df_sheet.iloc[row_idx]
        row_str = ' '.join(str(cell) for cell in row_data if pd.notna(cell))
        has_listing = any(v in row_str for v in listing_header_variants)
        has_marketing = any(v in row_str for v in marketing_header_variants)
        if has_listing and has_marketing:
            combined_row_idx = row_idx
            break

    if combined_row_idx is not None:
        row_data = df_sheet.iloc[combined_row_idx]
        listing_col_start = None
        marketing_col_start = None
        for col_idx, cell in enumerate(row_data):
            if pd.notna(cell):
                cell_str = str(cell)
                if any(v in cell_str for v in listing_header_variants):
                    listing_col_start = col_idx
                if any(v in cell_str for v in marketing_header_variants):
                    marketing_col_start = col_idx

        months_row_idx = combined_row_idx + 1
        if months_row_idx < len(df_sheet):
            months_row = df_sheet.iloc[months_row_idx]
            if listing_col_start is not None:
                end_col = marketing_col_start if marketing_col_start is not None else len(months_row)
                for col_idx in range(listing_col_start, min(end_col, len(months_row))):
                    cell = months_row.iloc[col_idx]
                    if pd.notna(cell):
                        cell_clean = str(cell).strip().lower()
                        if cell_clean in MONTH_NAMES_RU:
                            listing_month_indices[col_idx] = cell_clean

            if marketing_col_start is not None:
                for col_idx in range(marketing_col_start, len(months_row)):
                    cell = months_row.iloc[col_idx]
                    if pd.notna(cell):
                        cell_clean = str(cell).strip().lower()
                        if cell_clean in MONTH_NAMES_RU:
                            marketing_month_indices[col_idx] = cell_clean

            return combined_row_idx, listing_month_indices, combined_row_idx, marketing_month_indices

    # Раздельный поиск
    listing_row_idx = None
    marketing_row_idx = None

    for row_idx in range(len(df_sheet)):
        row_str = ' '.join(str(cell) for cell in df_sheet.iloc[row_idx] if pd.notna(cell))
        if any(v in row_str for v in listing_header_variants):
            listing_row_idx = row_idx
            break

    for row_idx in range(len(df_sheet)):
        row_str = ' '.join(str(cell) for cell in df_sheet.iloc[row_idx] if pd.notna(cell))
        if any(v in row_str for v in marketing_header_variants):
            marketing_row_idx = row_idx
            break

    for row_idx, month_indices in [(listing_row_idx, listing_month_indices),
                                    (marketing_row_idx, marketing_month_indices)]:
        if row_idx is not None:
            months_row_idx = row_idx + 1
            if months_row_idx < len(df_sheet):
                months_row = df_sheet.iloc[months_row_idx]
                for col_idx, cell in enumerate(months_row):
                    if pd.notna(cell):
                        cell_clean = str(cell).strip().lower()
                        if cell_clean in MONTH_NAMES_RU:
                            month_indices[col_idx] = cell_clean

    return listing_row_idx, listing_month_indices, marketing_row_idx, marketing_month_indices


def parse_section_data(df_sheet, section_row_idx, month_indices, section_name):
    """Парсит данные секции инвестиций."""
    results = []
    if section_row_idx is None or not month_indices:
        return results

    sku_start_row_idx = section_row_idx + 2
    end_markers = ["ООО", "Отчет сгенерирован", "ПЛАНИРОВАНИЕ", "УСЛОВИЯ КОНТРАКТА", "SAP-код", "ЗАПРОС НА ЗАКЛЮЧЕНИЕ"]

    for row_idx in range(sku_start_row_idx, len(df_sheet)):
        row = df_sheet.iloc[row_idx]
        first_cell = str(row.iloc[0]).strip() if len(row) > 0 and pd.notna(row.iloc[0]) and str(row.iloc[0]).strip() != '' else ""

        if any(marker in first_cell for marker in end_markers) and first_cell != "Brand":
            break

        if first_cell and first_cell not in end_markers and "Brand" not in first_cell:
            brand_name = first_cell
            for col_idx, month_name in month_indices.items():
                if col_idx < len(row):
                    value = row.iloc[col_idx]
                    if pd.notna(value) and str(value).strip() not in ('', '0'):
                        try:
                            val_str = str(value).replace(chr(160), ' ').replace(' ', '').replace(',', '.')
                            num_val = float(val_str)
                            if num_val != 0:
                                results.append({
                                    'Brand': brand_name,
                                    'Месяц': month_name,
                                    'Значение': num_val
                                })
                        except Exception:
                            pass
    return results


def extract_investments_data(file_path):
    """Извлекает данные по листингу, маркетингу и промо-скидкам."""
    listing_dict = {}
    marketing_dict = {}
    promo_dict = {}

    try:
        df_sheet = pd.read_excel(file_path, sheet_name='Планирование инвестиций', header=None)
        listing_row_idx, listing_month_indices, marketing_row_idx, marketing_month_indices = find_investment_sections(df_sheet)

        data_start_row_idx = max(
            listing_row_idx if listing_row_idx is not None else -1,
            marketing_row_idx if marketing_row_idx is not None else -1
        )

        if data_start_row_idx >= 0:
            listing_data = parse_section_data(df_sheet, data_start_row_idx, listing_month_indices, "Листинг")
            marketing_data = parse_section_data(df_sheet, data_start_row_idx, marketing_month_indices, "Маркетинг")

            for item in listing_data:
                listing_dict[(item['Brand'], item['Месяц'].lower().strip())] = item['Значение']
            for item in marketing_data:
                marketing_dict[(item['Brand'], item['Месяц'].lower().strip())] = item['Значение']

        # Промо-скидки
        _extract_percentage_row(df_sheet, 'Промо-скидки', promo_dict)

        # Листинг (%) — безусловные бонусы
        _extract_percentage_row_combined(df_sheet, 'Листинг', 'Безусловные бонусы', listing_dict)

        # Маркетинг (%)
        _extract_percentage_row_keyword(df_sheet, 'Маркетинг', 'Листинг', marketing_dict)

    except Exception as e:
        logger.error(f"Ошибка при извлечении инвестиций: {e}")

    return listing_dict, marketing_dict, promo_dict


def _extract_percentage_row(df_sheet, keyword, target_dict):
    """Извлекает процентные значения из строки по ключевому слову."""
    row_idx = None
    for ri in range(len(df_sheet)):
        row_str = ' '.join(str(cell) for cell in df_sheet.iloc[ri] if pd.notna(cell))
        if keyword in row_str:
            row_idx = ri
            break

    if row_idx is None:
        return

    brand_row_idx = None
    for search_idx in range(row_idx - 1, max(row_idx - 10, -1), -1):
        search_row_values = df_sheet.iloc[search_idx]
        search_row_text = ' '.join(str(cell).strip() for cell in search_row_values if pd.notna(cell))
        if any(kw in search_row_text for kw in ['Brand', 'Бренд', 'Brand/Статья']):
            brand_row_idx = search_idx
            break

    if brand_row_idx is None and row_idx > 0:
        brand_row_idx = row_idx - 1

    if brand_row_idx is not None and brand_row_idx >= 0:
        brand_row = df_sheet.iloc[brand_row_idx]
        data_row = df_sheet.iloc[row_idx]

        for col_idx in range(1, min(len(brand_row), len(data_row))):
            brand_cell = brand_row.iloc[col_idx] if col_idx < len(brand_row) else None
            value_cell = data_row.iloc[col_idx] if col_idx < len(data_row) else None

            brand_name = str(brand_cell).strip() if pd.notna(brand_cell) else ""
            value_raw = str(value_cell).strip() if pd.notna(value_cell) else ""

            if not brand_name or brand_name.lower() in ['brand/статья', 'brand', 'бренд', '']:
                continue

            percentage = _parse_percentage(value_raw)
            if percentage is not None:
                target_dict[brand_name] = percentage


def _extract_percentage_row_combined(df_sheet, keyword1, keyword2, target_dict):
    """Извлекает % из строки содержащей оба ключевых слова."""
    row_idx = None
    for ri in range(len(df_sheet)):
        row_str = ' '.join(str(cell) for cell in df_sheet.iloc[ri] if pd.notna(cell))
        if keyword1 in row_str and keyword2 in row_str:
            row_idx = ri
            break

    if row_idx is None:
        return

    brand_row_idx = _find_brand_header_above(df_sheet, row_idx)
    if brand_row_idx is not None:
        _extract_brand_percentage_pairs(df_sheet, brand_row_idx, row_idx, target_dict, key_suffix='all')


def _extract_percentage_row_keyword(df_sheet, keyword, exclude_keyword, target_dict):
    """Извлекает % из строки с ключевым словом, исключая другое."""
    row_idx = None
    for ri in range(len(df_sheet)):
        row_str = ' '.join(str(cell) for cell in df_sheet.iloc[ri] if pd.notna(cell))
        if keyword in row_str and exclude_keyword not in row_str:
            row_idx = ri
            break

    if row_idx is None:
        return

    brand_row_idx = _find_brand_header_above(df_sheet, row_idx)
    if brand_row_idx is not None:
        _extract_brand_percentage_pairs(df_sheet, brand_row_idx, row_idx, target_dict, key_suffix='all')


def _find_brand_header_above(df_sheet, row_idx):
    """Находит строку с заголовком Brand выше указанной."""
    for search_idx in range(row_idx - 1, max(row_idx - 10, -1), -1):
        search_row_values = df_sheet.iloc[search_idx]
        search_row_text = ' '.join(str(cell).strip() for cell in search_row_values if pd.notna(cell))
        if any(kw in search_row_text for kw in ['Brand', 'Бренд', 'Brand/Статья']):
            return search_idx
    return row_idx - 1 if row_idx > 0 else None


def _extract_brand_percentage_pairs(df_sheet, brand_row_idx, data_row_idx, target_dict, key_suffix='all'):
    """Извлекает пары бренд-процент."""
    brand_row = df_sheet.iloc[brand_row_idx]
    data_row = df_sheet.iloc[data_row_idx]

    for col_idx in range(1, min(len(brand_row), len(data_row))):
        brand_cell = brand_row.iloc[col_idx] if col_idx < len(brand_row) else None
        value_cell = data_row.iloc[col_idx] if col_idx < len(data_row) else None

        brand_name = str(brand_cell).strip() if pd.notna(brand_cell) else ""
        value_raw = str(value_cell).strip() if pd.notna(value_cell) else ""

        if not brand_name or brand_name.lower() in ['brand/статья', 'brand', 'бренд', '']:
            continue

        percentage = _parse_percentage(value_raw)
        if percentage is not None:
            target_dict[(brand_name, key_suffix)] = percentage


def _parse_percentage(value_raw):
    """Парсит процентное значение из строки."""
    if not value_raw:
        return None
    match = re.search(r'([+-]?\d+[.,]?\d*)\s*%?', value_raw)
    if match:
        try:
            val = float(match.group(1).replace(',', '.'))
            if '%' in value_raw or val > 1:
                val = val / 100.0
            return val
        except ValueError:
            pass
    return None


# =============================================================================
# ОБРАБОТКА ОДНОГО ФАЙЛА
# =============================================================================

def process_parsed_file(file_path):
    """
    Обрабатывает один предобработанный Excel файл (результат step1).
    Возвращает DataFrame с результатами или None.
    """
    try:
        logger.info(f"Парсинг файла: {os.path.basename(file_path)}")

        # Извлекаем инвестиции
        listing_dict, marketing_dict, promo_dict = extract_investments_data(file_path)

        # Читаем GFD Запрос
        try:
            df_gfd = pd.read_excel(file_path, sheet_name='GFD Запрос', header=None)
        except Exception as e:
            logger.error(f"Ошибка чтения 'GFD Запрос': {e}")
            return None

        # Извлекаем метаданные
        forma_value = _extract_forma(df_gfd)
        filial_value = _extract_field_value(df_gfd, 'Филиал')
        viveska_value = _extract_field_value(df_gfd, 'Название на вывеске')

        found_values = {}
        for label_key, var_name in [
            ('Категория клиента', 'client_type'),
            ('Группа сбыта', 'gr_sb'),
            ('Ответственный КАМ, УК', 'kam')
        ]:
            found_values[var_name] = _find_value_by_label(df_gfd, label_key)

        # SAP-код
        sap_code_value = _extract_sap_code(file_path)

        # Условия контракта
        try:
            df_contract = pd.read_excel(file_path, sheet_name='Условия контракта', header=None)
        except Exception:
            df_contract = pd.DataFrame()

        start_date_value, start_date_dt, end_date_value, end_date_dt = _extract_dates(df_contract)

        # Данные SKU из условий контракта
        sku_full_data = _extract_sku_data(df_contract)

        # Планирование продаж
        try:
            df_sales = pd.read_excel(file_path, sheet_name='Планирование продаж', header=None)
        except Exception:
            df_sales = pd.DataFrame()

        plan_calendar = parse_plan_calendar(df_sales) if not df_sales.empty else None
        if plan_calendar is None:
            logger.warning("Не удалось построить календарь плана")
            return None

        # Генерация строк
        data_rows = []
        if start_date_dt and end_date_dt and plan_calendar:
            contract_months = get_contract_months(start_date_dt, end_date_dt)
            base_file_name = os.path.splitext(os.path.basename(file_path))[0]

            for sku_name, sku_data in sku_full_data.items():
                sku_type_key = SKU_MAPPING_REVERSE.get(sku_name, "")

                weekly_volnew = extract_weekly_data(df_sales, sku_name, plan_calendar, "Новый контракт")
                if not weekly_volnew or sum(weekly_volnew.values()) == 0:
                    weekly_volnew = extract_weekly_data(df_sales, sku_name, plan_calendar, "Контракт")

                weekly_tm = extract_tm_plan_weekly(df_sales, sku_name, plan_calendar)
                weekly_price = extract_price_weekly(df_sales, sku_name, plan_calendar)

                volnew_monthly = distribute_weekly_to_months(weekly_volnew, plan_calendar, start_date_dt, end_date_dt)
                tm_monthly = calculate_monthly_tm(weekly_tm, plan_calendar, start_date_dt, end_date_dt)
                price_monthly = calculate_monthly_price(weekly_price, plan_calendar, start_date_dt, end_date_dt)
                prom_vol_monthly = calculate_prom_vol(weekly_volnew, weekly_tm, plan_calendar, start_date_dt, end_date_dt)

                for period_year, period_month in contract_months:
                    pdate_dt = datetime(period_year, period_month, 1)
                    month_rus = get_russian_month_name(period_month)

                    volnew = int(round(volnew_monthly.get((period_year, period_month), 0)))
                    avg_price = price_monthly.get((period_year, period_month), 0.0)
                    tm_plan = tm_monthly.get((period_year, period_month), 0.0)
                    prom_vol = prom_vol_monthly.get((period_year, period_month), 0)

                    listing_inv = listing_dict.get((sku_name, month_rus), "")
                    marketing_inv = marketing_dict.get((sku_name, month_rus), "")
                    promo_pct = promo_dict.get(sku_name, "")
                    listing2_pct = listing_dict.get((sku_name, 'all'), "")
                    marketing2_pct = marketing_dict.get((sku_name, 'all'), "")

                    row = {
                        'FileName': base_file_name,
                        'volnew': str(volnew),
                        'filial': filial_value,
                        'forma': forma_value,
                        'gr_sb': found_values.get('gr_sb', ''),
                        'kam': found_values.get('kam', ''),
                        'viveska': viveska_value,
                        'client_type': found_values.get('client_type', ''),
                        'sap-code': sap_code_value,
                        'start_date': start_date_value,
                        'end_date': end_date_value,
                        'pdate': pdate_dt.strftime('%d.%m.%Y'),
                        'HideStatus': '0',
                        'sku_type': sku_type_key,
                        'sku_type_sap': sku_name,
                        'price': str(avg_price),
                        'price_in': sku_data.get('price_in_col_idx', ''),
                        'retro': sku_data.get('retro_col_idx', ''),
                        'dopmarketing': sku_data.get('dopmarketing_col_idx', ''),
                        'sku': sku_data.get('sku_col_idx', ''),
                        'tt': sku_data.get('tt_col_idx', ''),
                        'listing2': listing2_pct if listing2_pct != "" else "",
                        'marketing2': marketing2_pct if marketing2_pct != "" else "",
                        'listing': listing_inv,
                        'marketing': marketing_inv,
                        'promo2': promo_pct,
                        'promo': str(tm_plan),
                        'PromVol': str(prom_vol) if prom_vol > 0 else "-"
                    }
                    data_rows.append(row)

        if not data_rows:
            logger.warning("Нет данных для генерации")
            return None

        df_output = pd.DataFrame(data_rows)

        # Фильтрация
        df_output['temp_sku_num'] = pd.to_numeric(
            df_output['sku'].astype(str).str.replace(',', '.').str.replace(' ', ''),
            errors='coerce'
        ).fillna(0)
        df_output['temp_tt_num'] = pd.to_numeric(
            df_output['tt'].astype(str).str.replace(',', '.').str.replace(' ', ''),
            errors='coerce'
        ).fillna(0)
        df_filtered = df_output[(df_output['temp_sku_num'] > 0) & (df_output['temp_tt_num'] > 0)].copy()
        df_filtered.drop(columns=['temp_sku_num', 'temp_tt_num'], inplace=True)

        if df_filtered.empty:
            logger.warning("После фильтрации не осталось строк")
            return None

        df_output = df_filtered

        # Замена точки на запятую в числовых колонках
        cols_replace = [
            'price', 'price_in', 'listing', 'listing2', 'marketing', 'marketing2',
            'promo', 'promo2', 'retro', 'volnew', 'PromVol', 'dopmarketing'
        ]
        for col in cols_replace:
            if col in df_output.columns:
                df_output[col] = df_output[col].astype(str).str.replace('.', ',', regex=False).str.replace(' ', '', regex=False)
                df_output[col] = df_output[col].replace('nan', '', regex=False)

        # Финальная фильтрация по volnew > 0
        df_output['volnew_check'] = df_output['volnew'].astype(str).replace(['nan', 'NaN', '-', ''], '0')
        df_output['volnew_numeric'] = pd.to_numeric(df_output['volnew_check'], errors='coerce').fillna(0)
        df_final = df_output[df_output['volnew_numeric'] > 0].copy()
        df_final.drop(columns=['volnew_check', 'volnew_numeric'], inplace=True)

        if df_final.empty:
            return None

        # Упорядочивание колонок
        desired_order = FINAL_COLUMNS_ORDER.copy()
        for col in df_final.columns:
            if col not in desired_order:
                desired_order.append(col)
        df_final = df_final.reindex(columns=desired_order)

        return df_final

    except Exception as e:
        logger.error(f"Критическая ошибка при парсинге: {e}")
        traceback.print_exc()
        return None


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ПАРСИНГА
# =============================================================================

def _extract_forma(df_gfd):
    """Извлекает значение 'Форма от...'."""
    forma_rows = df_gfd[df_gfd.apply(
        lambda row: any(str(cell).startswith('Форма от') for cell in row.astype(str)), axis=1
    )]
    if not forma_rows.empty:
        forma_row_data = forma_rows.iloc[0].astype(str)
        non_nan = [val for val in forma_row_data if val != 'nan']
        if non_nan:
            full_line = " | ".join(non_nan)
            return full_line.split('.')[0] + '.' if '.' in full_line else full_line
    return ""


def _extract_field_value(df, field_name):
    """Извлекает значение поля по имени."""
    field_row = df[df.apply(
        lambda row: row.astype(str).str.contains(field_name, case=False, na=False).any(), axis=1
    )]
    if not field_row.empty:
        row_data = field_row.iloc[0].astype(str)
        col_idx = None
        for idx, cell in enumerate(row_data):
            if field_name in str(cell):
                col_idx = idx
                break
        if col_idx is not None:
            for i in range(col_idx + 3, len(row_data)):
                value = row_data.iloc[i]
                if value != 'nan' and str(value).strip() != '':
                    return value
    return ""


def _find_value_by_label(df, label_key):
    """Находит значение по метке."""
    label_row = df[df.apply(
        lambda row: row.astype(str).str.contains(label_key, case=False, na=False).any(), axis=1
    )]
    if not label_row.empty:
        label_row_data = label_row.iloc[0].astype(str)
        label_col_idx = label_row_data[label_row_data.str.contains(label_key, case=False, na=False)].index
        if not label_col_idx.empty:
            col_idx = label_col_idx[0]
            row_values = label_row_data[col_idx + 1:]
            for val in row_values:
                if val != 'nan':
                    return val
    return ""


def _extract_sap_code(file_path):
    """Извлекает SAP-коды."""
    try:
        df_sap = pd.read_excel(file_path, sheet_name='SAP-код', header=None)
        sap_col_idx = None
        header_row_idx = None

        for row_idx in range(min(5, len(df_sap))):
            row_data = df_sap.iloc[row_idx].astype(str)
            for col_idx, cell_value in enumerate(row_data):
                if 'Коды заказчика клиента' in str(cell_value):
                    sap_col_idx = col_idx
                    header_row_idx = row_idx
                    break

        unique_sap_codes = set()
        if sap_col_idx is not None:
            for r in range(header_row_idx + 1 if header_row_idx is not None else 0, len(df_sap)):
                cell_value = df_sap.iloc[r, sap_col_idx]
                if pd.notna(cell_value):
                    cleaned = str(cell_value).strip()
                    if cleaned:
                        unique_sap_codes.add(cleaned)

        return ";".join(list(unique_sap_codes)) if unique_sap_codes else ""
    except Exception:
        return ""


def _extract_dates(df_contract):
    """Извлекает даты начала и окончания контракта."""
    start_date_value = ""
    end_date_value = ""
    start_date_dt = None
    end_date_dt = None

    if df_contract.empty:
        return start_date_value, start_date_dt, end_date_value, end_date_dt

    for label, offset in [('начало', 2), ('окончание', 2)]:
        rows = df_contract[df_contract.apply(
            lambda row: row.astype(str).str.contains(label, case=False, na=False).any(), axis=1
        )]
        if not rows.empty:
            row_data = rows.iloc[0]
            col_idx = None
            for idx, cell in enumerate(row_data.astype(str)):
                if label in str(cell).lower():
                    col_idx = idx
                    break
            if col_idx is not None and col_idx + offset < len(row_data):
                date_str, date_dt = parse_any_date(row_data.iloc[col_idx + offset])
                if label == 'начало':
                    start_date_value = date_str
                    start_date_dt = date_dt
                else:
                    end_date_value = date_str
                    end_date_dt = date_dt

    return start_date_value, start_date_dt, end_date_value, end_date_dt


def _extract_sku_data(df_contract):
    """Извлекает данные SKU из условий контракта."""
    sku_full_data = {}

    if df_contract.empty:
        return sku_full_data

    brand_rows_mask = df_contract.astype(str).apply(
        lambda row: row.str.contains(r'Brand', case=False, na=False, regex=True).any(), axis=1
    )
    brand_rows = df_contract[brand_rows_mask]

    if brand_rows.empty:
        return sku_full_data

    brand_row_idx = brand_rows.index[0]
    header_row_idx = brand_row_idx
    header_row = df_contract.iloc[brand_row_idx].astype(str)

    if 'Кол-во ТТ с листингом' not in ' '.join(header_row):
        if brand_row_idx + 1 < len(df_contract):
            potential = df_contract.iloc[brand_row_idx + 1].astype(str)
            if 'Кол-во ТТ с листингом' in ' '.join(potential):
                header_row_idx = brand_row_idx + 1
                header_row = potential

    column_index_map = {}
    for idx, cell in enumerate(header_row):
        cell_clean = re.sub(r'\s+', ' ', str(cell).strip()).replace('\n', ' ')
        column_index_map[cell_clean] = idx

    required_columns = {
        'Brand': 'brand_col_idx',
        'Кол-во SKU': 'sku_col_idx',
        'Кол-во ТТ с листингом': 'tt_col_idx',
        'Цена поставки по ИЦМ': 'price_col_idx',
        'ЦМ по категории клиента': 'price_in_col_idx',
        'Бонус за объем/Retro, от ТО без НДС, %': 'retro_col_idx',
        'Маркетинг бюджет 1 от факт. ТО, %': 'dopmarketing_col_idx',
    }

    found_indices = {}
    for col_name, var_name in required_columns.items():
        found_idx = None
        if col_name in column_index_map:
            found_idx = column_index_map[col_name]
        else:
            for header_text, idx in column_index_map.items():
                norm_header = re.sub(r'\s+', ' ', header_text.lower()).strip()
                norm_col = re.sub(r'\s+', ' ', col_name.lower()).strip()
                if norm_col in norm_header or norm_header in norm_col:
                    found_idx = idx
                    break
        found_indices[var_name] = found_idx

    data_start = header_row_idx + 1

    for i in range(data_start, len(df_contract)):
        row_data = df_contract.iloc[i].astype(str)
        first_cell = row_data.iloc[0] if len(row_data) > 0 else 'nan'

        if first_cell != 'nan' and first_cell.strip() != '':
            if 'Отчет сгенерирован' in first_cell:
                break
            if 'Brand' in first_cell and i > data_start + 5:
                break

            brand_col_idx = found_indices.get('brand_col_idx', 0) or 0
            if brand_col_idx < len(row_data):
                sku_name_raw = row_data.iloc[brand_col_idx]
                if sku_name_raw != 'nan' and sku_name_raw.strip() != '' and not str(sku_name_raw).startswith('Brand'):
                    sku_name = sku_name_raw.strip()
                    if sku_name not in sku_full_data:
                        sku_full_data[sku_name] = {}
                        for col_display_name, col_var_name in required_columns.items():
                            col_idx = found_indices.get(col_var_name)
                            value = ""
                            if col_idx is not None and col_idx < len(row_data):
                                cell_val = row_data.iloc[col_idx]
                                if cell_val != 'nan' and str(cell_val).strip() != '':
                                    value = str(cell_val).strip().replace(' ', '').replace(',', '.')
                            sku_full_data[sku_name][col_var_name] = value

    return sku_full_data
