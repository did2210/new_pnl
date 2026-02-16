import pandas as pd
from datetime import datetime, timedelta
from calendar import monthrange
import os
import re
from tqdm import tqdm
import time
import traceback
import logging
from dataclasses import dataclass
from typing import Optional

# --- НАСТРОЙКА ЛОГИРОВАНИЯ ---
log_filename = 'parse_log.txt'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Укажите путь к папке с исходными файлами
input_dir = r'C:\Users\metelkov\Desktop\эцп тест\file_new\map'
output_dir = r'C:\Users\metelkov\Desktop\эцп тест\file_new\final'

# === Полная методичка соответствия ===
sku_mapping = {
    'eon05': 'E-ON 0,45 CAN',
    'tornadopet': 'Tornado 0,473 PET',
    'tornadojb': 'Tornado 0,45 CAN',
    'tornadopet10': 'Tornado 1,0 PET',
    'freshbar048': 'Fresh Bar 0,48 PET',
    'freshbarjb': 'Fresh Bar 0,45 CAN',
    'freshbar15': 'Fresh Bar 1,5 PET',
    'colafreshbar048': 'COLA Fresh Bar 0,48 PET',
    'colafreshbar045': 'COLA Fresh Bar 0,45 CAN',
    'colafreshbar15': 'COLA Fresh Bar 1,5 PET',
    'il': 'ИЛ 0,48 PET',
    'il15': 'ИЛ 1,42 PET',
    'tornadoblack45can': 'Tornado BLACK 0,45 CAN',
    'colafreshbar1pet': 'COLA Fresh Bar 1,0 PET',
    'freshbar1pet': 'Fresh Bar 1,0 PET',
    'tornadomaxcan45can': 'Tornado MAX 0,45 CAN',
    'tornadomaxpet473pet': 'Tornado MAX 0,473 PET',
    'tornadosahar45can': 'Tornado сахар 0,45 CAN',
    'tornadosahar473pet': 'Tornado сахар 0,473 PET'
}

sku_mapping_reverse = {v: k for k, v in sku_mapping.items()}

MONTH_NAMES_RU = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь',
                  'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь']


# =============================================================================
# КАЛЕНДАРЬ НЕДЕЛЬ ЭЦП (2024-2027) - ИСПРАВЛЕННАЯ ВЕРСИЯ
# =============================================================================

def build_ecp_calendar() -> dict[int, tuple[int, int, int]]:
    """
    Строит календарь недель ЭЦП на 2024-2027 годы.
    
    ПРАВИЛО:
    - W1 = дни 1-7 января (независимо от дня недели)
    - W2 = дни 8-14 января
    - W52 = дни 358-364 (или до 31 декабря)
    - Если в течение недели выпадает 1 число месяца, 
      то ВСЯ неделя относится к ЭТОМУ месяцу
    
    Возвращает: {глобальный_номер_недели: (год, месяц, номер_недели_в_году)}
    """
    calendar = {}
    week_counter = 1
    
    for year in range(2024, 2028):
        for week_in_year in range(1, 53):
            start_day_of_year = (week_in_year - 1) * 7 + 1
            end_day_of_year = week_in_year * 7
            
            try:
                week_start = datetime(year, 1, 1) + timedelta(days=start_day_of_year - 1)
                year_end = datetime(year, 12, 31)
                week_end_candidate = datetime(year, 1, 1) + timedelta(days=end_day_of_year - 1)
                week_end = min(week_end_candidate, year_end)
            except Exception:
                break
            
            if week_start.year > year:
                break
            
            week_month = week_start.month
            week_year = week_start.year
            
            current_check_date = week_start
            while current_check_date <= week_end:
                if current_check_date.day == 1:
                    week_month = current_check_date.month
                    week_year = current_check_date.year
                    break
                current_check_date += timedelta(days=1)
            
            if week_year == year:
                calendar[week_counter] = (week_year, week_month, week_in_year)
                week_counter += 1
            else:
                break
    
    return calendar


ECP_CALENDAR = None


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def get_russian_month_name_by_number(month_num: int) -> str:
    if 1 <= month_num <= 12:
        return MONTH_NAMES_RU[month_num - 1]
    return ''


def month_name_to_num(month_name: str) -> Optional[int]:
    if not month_name:
        return None
    m = month_name.strip().lower()
    for idx, name in enumerate(MONTH_NAMES_RU):
        if name == m:
            return idx + 1
    return None


def parse_any_date(value) -> tuple[str, Optional[datetime]]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "", None

    if isinstance(value, datetime):
        return value.strftime('%d.%m.%Y'), value

    s = str(value).strip()
    if not s or s.lower() == 'nan':
        return "", None

    date_formats = ['%m/%d/%y', '%d.%m.%Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']
    for fmt in date_formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime('%d.%m.%Y'), dt
        except ValueError:
            continue

    try:
        ts = pd.to_datetime(value, errors='coerce')
        if pd.notna(ts):
            dt = ts.to_pydatetime()
            return dt.strftime('%d.%m.%Y'), dt
    except Exception:
        pass

    return s, None


def safe_to_float(value) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0
    s = str(value).replace(chr(160), ' ').replace(' ', '').replace(',', '.').strip()
    if not s or s.lower() == 'nan' or s == '-':
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def get_week_number_ecp(date: datetime) -> int:
    """Возвращает номер недели ЭЦП для даты."""
    day_of_year = date.timetuple().tm_yday
    week_num = ((day_of_year - 1) // 7) + 1
    return min(week_num, 52)


def get_date_range_for_week(week_num: int, year: int) -> tuple[datetime, datetime]:
    """Возвращает диапазон дат для недели ЭЦП."""
    start_day = (week_num - 1) * 7 + 1
    end_day = week_num * 7
    
    start_date = datetime(year, 1, 1) + timedelta(days=start_day - 1)
    end_date = datetime(year, 1, 1) + timedelta(days=end_day - 1)
    
    year_end = datetime(year, 12, 31)
    if end_date > year_end:
        end_date = year_end
    
    return start_date, end_date


# =============================================================================
# СТРУКТУРА ПЛАНА
# =============================================================================

@dataclass
class PlanMonth:
    """Представляет один месяц в плане."""
    month_num: int
    month_name: str
    column_indices: list[int]
    week_names: list[str]
    week_numbers: list[int]


@dataclass
class PlanCalendar:
    """Полный календарь плана."""
    months: dict[int, PlanMonth]
    week_to_month: dict[int, int]
    week_to_col: dict[int, int]
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
        logger.warning("Не найдена строка с месяцами.")
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
        logger.warning("Строка с неделями выходит за пределы.")
        return None
    
    row_months = df_sales.iloc[month_row_idx].astype(str)
    row_weeks = df_sales.iloc[week_row_idx].astype(str)
    
    months: dict[int, PlanMonth] = {}
    week_to_month: dict[int, int] = {}
    week_to_col: dict[int, int] = {}
    current_month_num = None
    current_month_name = None
    
    for col_idx in range(len(row_months)):
        # СНАЧАЛА обновляем текущий месяц — даже для столбцов «Итого/Всего».
        # Причина: Excel объединяет ячейки, и «декабрь» может оказаться
        # именно на столбце «Всего» предыдущего месяца (ноябрь).
        # Если пропустить этот столбец целиком (continue), то cur_month
        # не обновится и все последующие недели уйдут в ноябрь.
        month_cell = str(row_months.iloc[col_idx]).strip().lower()
        for idx, month_name in enumerate(MONTH_NAMES_RU):
            if month_name in month_cell:
                current_month_num = idx + 1
                current_month_name = month_name
                if current_month_num not in months:
                    months[current_month_num] = PlanMonth(
                        month_num=current_month_num,
                        month_name=current_month_name,
                        column_indices=[],
                        week_names=[],
                        week_numbers=[]
                    )
                break
        
        # ПОТОМ проверяем, нужно ли пропустить этот столбец
        week_cell_raw = str(row_weeks.iloc[col_idx]).strip()
        if any(marker in week_cell_raw.lower() for marker in exclude_markers):
            continue
        
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
        logger.warning("Не найдено месяцев в плане.")
        return None
    
    logger.info(f"Распарсен календарь плана:")
    for month_num, plan_month in sorted(months.items()):
        logger.info(f"  {plan_month.month_name}: W{plan_month.week_numbers[0]}-W{plan_month.week_numbers[-1]}")
    
    return PlanCalendar(
        months=months,
        week_to_month=week_to_month,
        week_to_col=week_to_col,
        month_row_idx=month_row_idx,
        week_row_idx=week_row_idx
    )


# =============================================================================
# ИЗВЛЕЧЕНИЕ ДАННЫХ ПО НЕДЕЛЯМ
# =============================================================================

def extract_weekly_data_from_plan(
    df_sales: pd.DataFrame,
    sku_name: str,
    plan_calendar: PlanCalendar,
    row_type: str = "Новый контракт"
) -> dict[int, float]:
    """Извлекает данные по неделям из плана."""
    result: dict[int, float] = {}
    
    data_row_idx = None
    for i in range(plan_calendar.week_row_idx + 1, len(df_sales)):
        sku_cell = str(df_sales.iloc[i, 0]).strip() if pd.notna(df_sales.iloc[i, 0]) else ""
        type_cell = str(df_sales.iloc[i, 2]).strip() if len(df_sales.iloc[i]) > 2 and pd.notna(df_sales.iloc[i, 2]) else ""
        
        if sku_name in sku_cell and row_type in type_cell:
            data_row_idx = i
            break
    
    if data_row_idx is None:
        return result
    
    row_data = df_sales.iloc[data_row_idx]
    
    for week_num, col_idx in plan_calendar.week_to_col.items():
        if col_idx < len(row_data):
            val = safe_to_float(row_data.iloc[col_idx])
            result[week_num] = val
    
    return result


def extract_tm_plan_weekly(
    df_sales: pd.DataFrame,
    sku_name: str,
    plan_calendar: PlanCalendar
) -> dict[int, float]:
    """Извлекает ТМ-план по неделям."""
    result: dict[int, float] = {}
    
    data_row_idx = None
    for i in range(plan_calendar.week_row_idx + 1, len(df_sales)):
        sku_cell = str(df_sales.iloc[i, 0]).strip() if pd.notna(df_sales.iloc[i, 0]) else ""
        type_cell = str(df_sales.iloc[i, 2]).strip() if len(df_sales.iloc[i]) > 2 and pd.notna(df_sales.iloc[i, 2]) else ""
        
        if sku_name in sku_cell and "ТМ-план" in type_cell:
            data_row_idx = i
            break
    
    if data_row_idx is None:
        return result
    
    row_data = df_sales.iloc[data_row_idx]
    
    for week_num, col_idx in plan_calendar.week_to_col.items():
        if col_idx < len(row_data):
            cell_value = row_data.iloc[col_idx]
            try:
                clean_value = str(cell_value).replace('%', '').replace(' ', '').replace(',', '.').strip()
                if clean_value and clean_value.replace('.', '').replace('-', '').isdigit():
                    result[week_num] = float(clean_value)
                else:
                    result[week_num] = 0.0
            except:
                result[week_num] = 0.0
    
    return result


def extract_price_weekly(
    df_sales: pd.DataFrame,
    sku_name: str,
    plan_calendar: PlanCalendar
) -> dict[int, float]:
    """Извлекает цену по неделям."""
    result: dict[int, float] = {}
    
    data_row_idx = None
    for i in range(plan_calendar.week_row_idx + 1, len(df_sales)):
        row = df_sales.iloc[i].astype(str).str.strip()
        sku_cell = str(df_sales.iloc[i, 0]).strip() if pd.notna(df_sales.iloc[i, 0]) else ""
        
        if sku_name in sku_cell and any("Цена поставки" in cell for cell in row):
            data_row_idx = i
            break
    
    if data_row_idx is None:
        return result
    
    row_data = df_sales.iloc[data_row_idx]
    
    for week_num, col_idx in plan_calendar.week_to_col.items():
        if col_idx < len(row_data):
            price_val = safe_to_float(row_data.iloc[col_idx])
            result[week_num] = price_val
    
    return result


# =============================================================================
# РАСПРЕДЕЛЕНИЕ ДАННЫХ ПО МЕСЯЦАМ КОНТРАКТА - ИСПРАВЛЕННАЯ ВЕРСИЯ
# =============================================================================

def get_contract_months(start_date: datetime, end_date: datetime) -> list[tuple[int, int]]:
    """Возвращает список (год, месяц) для всех месяцев контракта."""
    months = []
    current = start_date.replace(day=1)
    while current <= end_date:
        months.append((current.year, current.month))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months


def distribute_weekly_to_contract_months(
    weekly_data: dict[int, float],
    plan_calendar: PlanCalendar,
    start_date: datetime,
    end_date: datetime
) -> dict[tuple[int, int], float]:
    """Распределяет недельные данные по месяцам контракта используя календарь ЭЦП."""
    global ECP_CALENDAR
    
    if ECP_CALENDAR is None:
        logger.info("Строим календарь ЭЦП недель (2024-2027)...")
        ECP_CALENDAR = build_ecp_calendar()
        logger.info(f"  Построено {len(ECP_CALENDAR)} недель")
    
    result: dict[tuple[int, int], float] = {}
    
    contract_months = get_contract_months(start_date, end_date)
    for period_year, period_month in contract_months:
        result[(period_year, period_month)] = 0.0
    
    start_week_ecp = get_week_number_ecp(start_date)
    
    logger.info(f"Контракт начинается: {start_date.strftime('%d.%m.%Y')} = W{start_week_ecp} ЭЦП {start_date.year}")
    
    start_global_week = None
    for global_week, (cal_year, cal_month, week_in_year) in ECP_CALENDAR.items():
        if cal_year == start_date.year and week_in_year == start_week_ecp:
            start_global_week = global_week
            logger.info(f"  Стартовая глобальная неделя: {start_global_week} ({cal_year} {get_russian_month_name_by_number(cal_month)})")
            break
    
    if start_global_week is None:
        logger.error("Не найдена стартовая неделя в календаре!")
        return result
    
    for i in range(52):
        global_week = start_global_week + i
        
        if global_week not in ECP_CALENDAR:
            break
        
        week_year, week_month, week_in_year = ECP_CALENDAR[global_week]
        
        if (week_year, week_month) not in result:
            break
        
        plan_week_num = week_in_year
        week_value = weekly_data.get(plan_week_num, 0.0)
        result[(week_year, week_month)] += week_value
        
        logger.debug(f"  Глоб W{global_week} (W{plan_week_num} в {week_year}) → {get_russian_month_name_by_number(week_month)} {week_year}: +{int(week_value):,}")
    
    logger.info(f"Распределение по месяцам:")
    for period_year, period_month in sorted(result.keys()):
        if result[(period_year, period_month)] > 0:
            logger.info(f"  {get_russian_month_name_by_number(period_month)} {period_year}: {int(result[(period_year, period_month)]):,}")
    
    return result


def calculate_monthly_tm_plan(
    weekly_tm: dict[int, float],
    plan_calendar: PlanCalendar,
    start_date: datetime,
    end_date: datetime
) -> dict[tuple[int, int], float]:
    """Вычисляет ТМ-план по месяцам (максимум за месяц)."""
    result: dict[tuple[int, int], float] = {}
    
    contract_months = get_contract_months(start_date, end_date)
    for period_year, period_month in contract_months:
        result[(period_year, period_month)] = 0.0
    
    start_week_ecp = get_week_number_ecp(start_date)
    
    start_global_week = None
    for global_week, (cal_year, cal_month, week_in_year) in ECP_CALENDAR.items():
        if cal_year == start_date.year and week_in_year == start_week_ecp:
            start_global_week = global_week
            break
    
    if start_global_week is None:
        return result
    
    month_tm_values: dict[tuple[int, int], list[float]] = {key: [] for key in result.keys()}
    
    for i in range(52):
        global_week = start_global_week + i
        if global_week not in ECP_CALENDAR:
            break
        
        week_year, week_month, week_in_year = ECP_CALENDAR[global_week]
        
        if (week_year, week_month) not in month_tm_values:
            break
        
        tm_value = weekly_tm.get(week_in_year, 0.0)
        month_tm_values[(week_year, week_month)].append(tm_value)
    
    for key, values in month_tm_values.items():
        result[key] = max(values) if values else 0.0
    
    return result


def calculate_monthly_price(
    weekly_price: dict[int, float],
    plan_calendar: PlanCalendar,
    start_date: datetime,
    end_date: datetime
) -> dict[tuple[int, int], float]:
    """Вычисляет среднюю цену по месяцам."""
    result: dict[tuple[int, int], float] = {}
    
    contract_months = get_contract_months(start_date, end_date)
    for period_year, period_month in contract_months:
        result[(period_year, period_month)] = 0.0
    
    start_week_ecp = get_week_number_ecp(start_date)
    
    start_global_week = None
    for global_week, (cal_year, cal_month, week_in_year) in ECP_CALENDAR.items():
        if cal_year == start_date.year and week_in_year == start_week_ecp:
            start_global_week = global_week
            break
    
    if start_global_week is None:
        return result
    
    month_prices: dict[tuple[int, int], list[float]] = {key: [] for key in result.keys()}
    
    for i in range(52):
        global_week = start_global_week + i
        if global_week not in ECP_CALENDAR:
            break
        
        week_year, week_month, week_in_year = ECP_CALENDAR[global_week]
        
        if (week_year, week_month) not in month_prices:
            break
        
        price_value = weekly_price.get(week_in_year, 0.0)
        if price_value > 0:
            month_prices[(week_year, week_month)].append(price_value)
    
    for key, prices in month_prices.items():
        result[key] = round(sum(prices) / len(prices), 2) if prices else 0.0
    
    return result


def calculate_prom_vol_monthly(
    weekly_vols: dict[int, float],
    weekly_tm: dict[int, float],
    plan_calendar: PlanCalendar,
    start_date: datetime,
    end_date: datetime
) -> dict[tuple[int, int], int]:
    """Вычисляет PromVol по месяцам."""
    result: dict[tuple[int, int], int] = {}
    
    contract_months = get_contract_months(start_date, end_date)
    for period_year, period_month in contract_months:
        result[(period_year, period_month)] = 0
    
    start_week_ecp = get_week_number_ecp(start_date)
    
    start_global_week = None
    for global_week, (cal_year, cal_month, week_in_year) in ECP_CALENDAR.items():
        if cal_year == start_date.year and week_in_year == start_week_ecp:
            start_global_week = global_week
            break
    
    if start_global_week is None:
        return result
    
    for i in range(52):
        global_week = start_global_week + i
        if global_week not in ECP_CALENDAR:
            break
        
        week_year, week_month, week_in_year = ECP_CALENDAR[global_week]
        
        if (week_year, week_month) not in result:
            break
        
        if weekly_tm.get(week_in_year, 0.0) > 0:
            vol_value = weekly_vols.get(week_in_year, 0.0)
            result[(week_year, week_month)] += int(round(vol_value))
    
    return result


# =============================================================================
# ИЗВЛЕЧЕНИЕ ИНВЕСТИЦИЙ
# =============================================================================

def find_investment_sections(df_sheet):
    """Находит строки и позиции для листинга и маркетинга."""
    listing_row_idx = None
    listing_month_indices = {}
    marketing_row_idx = None
    marketing_month_indices = {}

    listing_header_variants = ["Период оплаты за Листинг, руб. с НДС 20%"]
    marketing_header_variants = [
        "Период оплаты бюджета Маркетинга, руб. с НДС 20%",
        "Период оплаты бюджета Маркетинга, руб. с НДС  20%",
    ]

    combined_row_idx = None
    for row_idx in range(len(df_sheet)):
        row_data = df_sheet.iloc[row_idx]
        row_str = ' '.join(str(cell) for cell in row_data if pd.notna(cell))
        has_listing = any(variant in row_str for variant in listing_header_variants)
        has_marketing = any(variant in row_str for variant in marketing_header_variants)
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
                if any(variant in cell_str for variant in listing_header_variants):
                    listing_col_start = col_idx
                if any(variant in cell_str for variant in marketing_header_variants):
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
                        if cell_clean in MONTH_NAMES_RU and col_idx not in listing_month_indices:
                            listing_month_indices[col_idx] = cell_clean

            if marketing_col_start is not None:
                for col_idx in range(marketing_col_start, len(months_row)):
                    cell = months_row.iloc[col_idx]
                    if pd.notna(cell):
                        cell_clean = str(cell).strip().lower()
                        if cell_clean in MONTH_NAMES_RU and col_idx not in marketing_month_indices:
                            marketing_month_indices[col_idx] = cell_clean

            return combined_row_idx, listing_month_indices, combined_row_idx, marketing_month_indices

    for row_idx in range(len(df_sheet)):
        row_str = ' '.join(str(cell) for cell in df_sheet.iloc[row_idx] if pd.notna(cell))
        if any(variant in row_str for variant in listing_header_variants):
            listing_row_idx = row_idx
            break

    for row_idx in range(len(df_sheet)):
        row_str = ' '.join(str(cell) for cell in df_sheet.iloc[row_idx] if pd.notna(cell))
        if any(variant in row_str for variant in marketing_header_variants):
            marketing_row_idx = row_idx
            break

    if listing_row_idx is not None:
        months_row_idx = listing_row_idx + 1
        if months_row_idx < len(df_sheet):
            months_row = df_sheet.iloc[months_row_idx]
            for col_idx, cell in enumerate(months_row):
                if pd.notna(cell):
                    cell_clean = str(cell).strip().lower()
                    if cell_clean in MONTH_NAMES_RU:
                        listing_month_indices[col_idx] = cell_clean

    if marketing_row_idx is not None:
        months_row_idx = marketing_row_idx + 1
        if months_row_idx < len(df_sheet):
            months_row = df_sheet.iloc[months_row_idx]
            for col_idx, cell in enumerate(months_row):
                if pd.notna(cell):
                    cell_clean = str(cell).strip().lower()
                    if cell_clean in MONTH_NAMES_RU:
                        marketing_month_indices[col_idx] = cell_clean

    return listing_row_idx, listing_month_indices, marketing_row_idx, marketing_month_indices


def parse_section_data(df_sheet, section_row_idx, month_indices, section_name):
    """Парсит данные секции."""
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
                        except:
                            pass

    return results


def extract_investments_data(file_path):
    """Извлекает данные по листингу, маркетингу и промо-скидкам."""
    listing_dict = {}
    marketing_dict = {}
    promo_dict = {}

    try:
        TARGET_SHEET_NAME = "Планирование инвестиций"
        df_sheet = pd.read_excel(file_path, sheet_name=TARGET_SHEET_NAME, header=None)

        listing_row_idx, listing_month_indices, marketing_row_idx, marketing_month_indices = find_investment_sections(df_sheet)

        data_start_row_idx = max(
            listing_row_idx if listing_row_idx is not None else -1,
            marketing_row_idx if marketing_row_idx is not None else -1
        )

        if data_start_row_idx == -1:
            return listing_dict, marketing_dict, promo_dict

        listing_data = parse_section_data(df_sheet, data_start_row_idx, listing_month_indices, "Листинг")
        marketing_data = parse_section_data(df_sheet, data_start_row_idx, marketing_month_indices, "Маркетинг")

        for item in listing_data:
            listing_dict[(item['Brand'], item['Месяц'].lower().strip())] = item['Значение']

        for item in marketing_data:
            marketing_dict[(item['Brand'], item['Месяц'].lower().strip())] = item['Значение']

        promo_row_idx = None
        for row_idx in range(len(df_sheet)):
            row_str = ' '.join(str(cell) for cell in df_sheet.iloc[row_idx] if pd.notna(cell))
            if 'Промо-скидки' in row_str:
                promo_row_idx = row_idx
                break

        if promo_row_idx is not None:
            brand_row_idx = None
            for search_idx in range(promo_row_idx - 1, max(promo_row_idx - 10, -1), -1):
                search_row_values = df_sheet.iloc[search_idx]
                search_row_text = ' '.join(str(cell).strip() for cell in search_row_values if pd.notna(cell))
                if any(keyword in search_row_text for keyword in ['Brand', 'Бренд', 'Brand/Статья']):
                    brand_row_idx = search_idx
                    break

            if brand_row_idx is None and promo_row_idx > 0:
                brand_row_idx = promo_row_idx - 1

            if brand_row_idx is not None and brand_row_idx >= 0:
                brand_row = df_sheet.iloc[brand_row_idx]
                promo_row = df_sheet.iloc[promo_row_idx]

                for col_idx in range(1, min(len(brand_row), len(promo_row))):
                    brand_cell = brand_row.iloc[col_idx] if col_idx < len(brand_row) else None
                    promo_cell = promo_row.iloc[col_idx] if col_idx < len(promo_row) else None

                    brand_name = str(brand_cell).strip() if pd.notna(brand_cell) else ""
                    promo_value_raw = str(promo_cell).strip() if pd.notna(promo_cell) else ""

                    if not brand_name or brand_name.lower() in ['brand/статья', 'brand', 'бренд', '']:
                        continue

                    promo_percentage = None
                    if promo_value_raw:
                        match = re.search(r'([+-]?\d+[.,]?\d*)\s*%?', promo_value_raw)
                        if match:
                            try:
                                promo_percentage = float(match.group(1).replace(',', '.'))
                                if '%' in promo_value_raw or promo_percentage > 1:
                                    promo_percentage = promo_percentage / 100.0
                            except ValueError:
                                pass

                    if promo_percentage is not None:
                        promo_dict[brand_name] = promo_percentage

        listing2_row_idx = None
        for row_idx in range(len(df_sheet)):
            row_str = ' '.join(str(cell) for cell in df_sheet.iloc[row_idx] if pd.notna(cell))
            if 'Листинг' in row_str and 'Безусловные бонусы' in row_str:
                listing2_row_idx = row_idx
                break

        if listing2_row_idx is not None:
            brand_row_idx = None
            for search_idx in range(listing2_row_idx - 1, max(listing2_row_idx - 10, -1), -1):
                search_row_values = df_sheet.iloc[search_idx]
                search_row_text = ' '.join(str(cell).strip() for cell in search_row_values if pd.notna(cell))
                if any(keyword in search_row_text for keyword in ['Brand', 'Бренд', 'Brand/Статья']):
                    brand_row_idx = search_idx
                    break

            if brand_row_idx is None and listing2_row_idx > 0:
                brand_row_idx = listing2_row_idx - 1

            if brand_row_idx is not None and brand_row_idx >= 0:
                brand_row = df_sheet.iloc[brand_row_idx]
                listing2_row = df_sheet.iloc[listing2_row_idx]

                for col_idx in range(1, min(len(brand_row), len(listing2_row))):
                    brand_cell = brand_row.iloc[col_idx] if col_idx < len(brand_row) else None
                    listing2_cell = listing2_row.iloc[col_idx] if col_idx < len(listing2_row) else None

                    brand_name = str(brand_cell).strip() if pd.notna(brand_cell) else ""
                    listing2_value_raw = str(listing2_cell).strip() if pd.notna(listing2_cell) else ""

                    if not brand_name or brand_name.lower() in ['brand/статья', 'brand', 'бренд', '']:
                        continue

                    listing2_percentage = None
                    if listing2_value_raw:
                        match = re.search(r'([+-]?\d+[.,]?\d*)\s*%?', listing2_value_raw)
                        if match:
                            try:
                                listing2_percentage = float(match.group(1).replace(',', '.'))
                                if '%' in listing2_value_raw or listing2_percentage > 1:
                                    listing2_percentage = listing2_percentage / 100.0
                            except ValueError:
                                pass

                    if listing2_percentage is not None:
                        listing_dict[(brand_name, 'all')] = listing2_percentage

        marketing2_row_idx = None
        for row_idx in range(len(df_sheet)):
            row_str = ' '.join(str(cell) for cell in df_sheet.iloc[row_idx] if pd.notna(cell))
            if 'Маркетинг' in row_str and 'Листинг' not in row_str:
                marketing2_row_idx = row_idx
                break

        if marketing2_row_idx is not None:
            brand_row_idx = None
            for search_idx in range(marketing2_row_idx - 1, max(marketing2_row_idx - 10, -1), -1):
                search_row_values = df_sheet.iloc[search_idx]
                search_row_text = ' '.join(str(cell).strip() for cell in search_row_values if pd.notna(cell))
                if any(keyword in search_row_text for keyword in ['Brand', 'Бренд', 'Brand/Статья']):
                    brand_row_idx = search_idx
                    break

            if brand_row_idx is None and marketing2_row_idx > 0:
                brand_row_idx = marketing2_row_idx - 1

            if brand_row_idx is not None and brand_row_idx >= 0:
                brand_row = df_sheet.iloc[brand_row_idx]
                marketing2_row = df_sheet.iloc[marketing2_row_idx]

                for col_idx in range(1, min(len(brand_row), len(marketing2_row))):
                    brand_cell = brand_row.iloc[col_idx] if col_idx < len(brand_row) else None
                    marketing2_cell = marketing2_row.iloc[col_idx] if col_idx < len(marketing2_row) else None

                    brand_name = str(brand_cell).strip() if pd.notna(brand_cell) else ""
                    marketing2_value_raw = str(marketing2_cell).strip() if pd.notna(marketing2_cell) else ""

                    if not brand_name or brand_name.lower() in ['brand/статья', 'brand', 'бренд', '']:
                        continue

                    marketing2_percentage = None
                    if marketing2_value_raw:
                        match = re.search(r'([+-]?\d+[.,]?\d*)\s*%?', marketing2_value_raw)
                        if match:
                            try:
                                marketing2_percentage = float(match.group(1).replace(',', '.'))
                                if '%' in marketing2_value_raw or marketing2_percentage > 1:
                                    marketing2_percentage = marketing2_percentage / 100.0
                            except ValueError:
                                pass

                    if marketing2_percentage is not None:
                        marketing_dict[(brand_name, 'all')] = marketing2_percentage

    except Exception as e:
        logger.error(f"Ошибка при извлечении инвестиций: {e}")
        traceback.print_exc()

    return listing_dict, marketing_dict, promo_dict


# =============================================================================
# ОБРАБОТКА ФАЙЛА
# =============================================================================

def process_single_file(file_path):
    """Обрабатывает один Excel-файл."""
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"Обработка файла: {os.path.basename(file_path)}")
        logger.info(f"{'='*60}")
        
        listing_dict, marketing_dict, promo_dict = extract_investments_data(file_path)

        try:
            df_gfd = pd.read_excel(file_path, sheet_name='GFD Запрос', header=None)
        except Exception as e:
            logger.error(f"Ошибка при чтении 'GFD Запрос': {e}")
            return None

        forma_value = ""
        forma_rows = df_gfd[df_gfd.apply(lambda row: any(str(cell).startswith('Форма от') for cell in row.astype(str)), axis=1)]
        if not forma_rows.empty:
            forma_row_data = forma_rows.iloc[0].astype(str)
            non_nan_values = [val for val in forma_row_data if val != 'nan']
            if non_nan_values:
                full_line = " | ".join(non_nan_values)
                forma_value = full_line.split('.')[0] + '.' if '.' in full_line else full_line

        filial_value = ""
        filial_row = df_gfd[df_gfd.apply(lambda row: row.astype(str).str.contains('Филиал', case=False, na=False).any(), axis=1)]
        if not filial_row.empty:
            filial_row_data = filial_row.iloc[0].astype(str)
            filial_col_idx = None
            for idx, cell in enumerate(filial_row_data):
                if 'Филиал' in str(cell):
                    filial_col_idx = idx
                    break
            if filial_col_idx is not None:
                for i in range(filial_col_idx + 3, len(filial_row_data)):
                    value = filial_row_data.iloc[i]
                    if value != 'nan' and str(value).strip() != '':
                        filial_value = value
                        break

        viveska_value = ""
        viveska_row = df_gfd[df_gfd.apply(lambda row: row.astype(str).str.contains('Название на вывеске', case=False, na=False).any(), axis=1)]
        if not viveska_row.empty:
            viveska_row_data = viveska_row.iloc[0].astype(str)
            viveska_col_idx = None
            for idx, cell in enumerate(viveska_row_data):
                if 'Название на вывеске' in str(cell):
                    viveska_col_idx = idx
                    break
            if viveska_col_idx is not None:
                for i in range(viveska_col_idx + 3, len(viveska_row_data)):
                    value = viveska_row_data.iloc[i]
                    if value != 'nan' and str(value).strip() != '':
                        viveska_value = value
                        break

        found_values = {}
        search_columns = {
            'Категория клиента': 'client_type',
            'Группа сбыта': 'gr_sb',
            'Ответственный КАМ, УК': 'kam'
        }

        def find_value_by_label(df, label_key):
            label_row = df[df.apply(lambda row: row.astype(str).str.contains(label_key, case=False, na=False).any(), axis=1)]
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

        for label_key, var_name in search_columns.items():
            found_values[var_name] = find_value_by_label(df_gfd, label_key)

        sap_code_value = ""
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
                        cleaned_value = str(cell_value).strip()
                        if cleaned_value:
                            unique_sap_codes.add(cleaned_value)

            if unique_sap_codes:
                sap_code_value = ";".join(list(unique_sap_codes))
        except Exception as e:
            logger.error(f"Ошибка при чтении 'SAP-код': {e}")

        try:
            df_contract = pd.read_excel(file_path, sheet_name='Условия контракта', header=None)
        except Exception as e:
            logger.error(f"Ошибка при чтении 'Условия контракта': {e}")
            df_contract = pd.DataFrame()

        start_date_value = ""
        end_date_value = ""
        start_date_dt_obj = None
        end_date_dt_obj = None

        if not df_contract.empty:
            start_rows = df_contract[df_contract.apply(lambda row: row.astype(str).str.contains('начало', case=False, na=False).any(), axis=1)]
            if not start_rows.empty:
                start_row_data = start_rows.iloc[0]
                start_col_idx = None
                for idx, cell in enumerate(start_row_data.astype(str)):
                    if 'начало' in str(cell).lower():
                        start_col_idx = idx
                        break
                if start_col_idx is not None and start_col_idx + 2 < len(start_row_data):
                    start_date_value, start_date_dt_obj = parse_any_date(start_row_data.iloc[start_col_idx + 2])

            end_rows = df_contract[df_contract.apply(lambda row: row.astype(str).str.contains('окончание', case=False, na=False).any(), axis=1)]
            if not end_rows.empty:
                end_row_data = end_rows.iloc[0]
                end_col_idx = None
                for idx, cell in enumerate(end_row_data.astype(str)):
                    if 'окончание' in str(cell).lower():
                        end_col_idx = idx
                        break
                if end_col_idx is not None and end_col_idx + 2 < len(end_row_data):
                    end_date_value, end_date_dt_obj = parse_any_date(end_row_data.iloc[end_col_idx + 2])

        logger.info(f"Дата начала: {start_date_value}")
        logger.info(f"Дата окончания: {end_date_value}")

        sku_full_data = {}

        if not df_contract.empty:
            brand_rows_mask = df_contract.astype(str).apply(
                lambda row: row.str.contains(r'Brand', case=False, na=False, regex=True).any(), axis=1
            )
            brand_rows = df_contract[brand_rows_mask]

            if not brand_rows.empty:
                brand_row_idx = brand_rows.index[0]
                header_row_idx = brand_row_idx
                header_row = df_contract.iloc[brand_row_idx].astype(str)

                if 'Кол-во ТТ с листингом' not in ' '.join(header_row):
                    if brand_row_idx + 1 < len(df_contract):
                        potential_header_row = df_contract.iloc[brand_row_idx + 1].astype(str)
                        if 'Кол-во ТТ с листингом' in ' '.join(potential_header_row):
                            header_row_idx = brand_row_idx + 1
                            header_row = potential_header_row

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

                found_column_indices = {}
                for col_name, var_name in required_columns.items():
                    found_idx = None
                    if col_name in column_index_map:
                        found_idx = column_index_map[col_name]
                    else:
                        for header_text, idx in column_index_map.items():
                            norm_header = re.sub(r'\s+', ' ', header_text.lower()).strip()
                            norm_col_name = re.sub(r'\s+', ' ', col_name.lower()).strip()
                            if norm_col_name in norm_header or norm_header in norm_col_name:
                                found_idx = idx
                                break
                    found_column_indices[var_name] = found_idx

                data_start_row = header_row_idx + 1
                table_ended = False

                for i in range(data_start_row, len(df_contract)):
                    if table_ended:
                        break

                    row_data = df_contract.iloc[i].astype(str)
                    first_cell = row_data.iloc[0] if len(row_data) > 0 else 'nan'

                    if first_cell != 'nan' and first_cell.strip() != '':
                        if 'Отчет сгенерирован' in first_cell:
                            table_ended = True
                            break
                        if 'Brand' in first_cell and i > data_start_row + 5:
                            table_ended = True
                            break

                        brand_col_idx = found_column_indices.get('brand_col_idx', 0) or 0
                        if brand_col_idx < len(row_data):
                            sku_name_raw = row_data.iloc[brand_col_idx]
                            if sku_name_raw != 'nan' and sku_name_raw.strip() != '' and not str(sku_name_raw).startswith('Brand'):
                                sku_name = sku_name_raw.strip()
                                if sku_name not in sku_full_data:
                                    sku_full_data[sku_name] = {}
                                    for col_display_name, col_var_name in required_columns.items():
                                        col_idx = found_column_indices.get(col_var_name)
                                        value = ""
                                        if col_idx is not None and col_idx < len(row_data):
                                            cell_val = row_data.iloc[col_idx]
                                            if cell_val != 'nan' and str(cell_val).strip() != '':
                                                value = str(cell_val).strip().replace(' ', '').replace(',', '.')
                                        sku_full_data[sku_name][col_var_name] = value
                                    sku_full_data[sku_name]['listing2_col_idx'] = ""
                                    sku_full_data[sku_name]['marketing2_col_idx'] = ""

        try:
            df_sales = pd.read_excel(file_path, sheet_name='Планирование продаж', header=None)
        except Exception as e:
            logger.error(f"Ошибка при загрузке 'Планирование продаж': {e}")
            df_sales = pd.DataFrame()

        plan_calendar = parse_plan_calendar(df_sales) if not df_sales.empty else None

        if plan_calendar is None:
            logger.warning("Не удалось построить календарь плана.")
            return None

        data_rows = []

        if start_date_dt_obj and end_date_dt_obj and plan_calendar:
            contract_months = get_contract_months(start_date_dt_obj, end_date_dt_obj)
            base_file_name = os.path.splitext(os.path.basename(file_path))[0]

            for sku_name, sku_data in sku_full_data.items():
                sku_type_key = sku_mapping_reverse.get(sku_name, "")
                logger.info(f"\n=== SKU: '{sku_name}' ===")

                weekly_volnew = extract_weekly_data_from_plan(df_sales, sku_name, plan_calendar, "Новый контракт")
                if not weekly_volnew or sum(weekly_volnew.values()) == 0:
                    weekly_volnew = extract_weekly_data_from_plan(df_sales, sku_name, plan_calendar, "Контракт")

                weekly_tm = extract_tm_plan_weekly(df_sales, sku_name, plan_calendar)
                weekly_price = extract_price_weekly(df_sales, sku_name, plan_calendar)

                volnew_by_contract = distribute_weekly_to_contract_months(
                    weekly_volnew, plan_calendar, start_date_dt_obj, end_date_dt_obj
                )
                tm_by_contract = calculate_monthly_tm_plan(
                    weekly_tm, plan_calendar, start_date_dt_obj, end_date_dt_obj
                )
                price_by_contract = calculate_monthly_price(
                    weekly_price, plan_calendar, start_date_dt_obj, end_date_dt_obj
                )
                prom_vol_by_month = calculate_prom_vol_monthly(
                    weekly_volnew, weekly_tm, plan_calendar, start_date_dt_obj, end_date_dt_obj
                )

                for period_year, period_month in contract_months:
                    pdate_dt = datetime(period_year, period_month, 1)
                    pdate_str = pdate_dt.strftime('%d.%m.%Y')
                    month_rus = get_russian_month_name_by_number(period_month)

                    volnew_for_month = int(round(volnew_by_contract.get((period_year, period_month), 0)))
                    avg_price = price_by_contract.get((period_year, period_month), 0.0)
                    tm_plan_percentage = tm_by_contract.get((period_year, period_month), 0.0)
                    prom_vol_value = prom_vol_by_month.get((period_year, period_month), 0)

                    listing_investment = listing_dict.get((sku_name, month_rus), "")
                    marketing_investment = marketing_dict.get((sku_name, month_rus), "")
                    promo_percentage = promo_dict.get(sku_name, "")
                    listing2_percentage = listing_dict.get((sku_name, 'all'), "")
                    marketing2_percentage = marketing_dict.get((sku_name, 'all'), "")

                    row = {
                        'FileName': base_file_name,
                        'volnew': str(volnew_for_month),
                        'filial': filial_value,
                        'forma': forma_value,
                        'gr_sb': found_values.get('gr_sb', ''),
                        'kam': found_values.get('kam', ''),
                        'viveska': viveska_value,
                        'client_type': found_values.get('client_type', ''),
                        'sap-code': sap_code_value,
                        'start_date': start_date_value,
                        'end_date': end_date_value,
                        'pdate': pdate_str,
                        'HideStatus': '0',
                        'sku_type': sku_type_key,
                        'sku_type_sap': sku_name,
                        'price': str(avg_price),
                        'price_in': sku_data.get('price_in_col_idx', ''),
                        'retro': sku_data.get('retro_col_idx', ''),
                        'dopmarketing': sku_data.get('dopmarketing_col_idx', ''),
                        'sku': sku_data.get('sku_col_idx', ''),
                        'tt': sku_data.get('tt_col_idx', ''),
                        'listing2': listing2_percentage if listing2_percentage != "" else sku_data.get('listing2_col_idx', ''),
                        'marketing2': marketing2_percentage if marketing2_percentage != "" else sku_data.get('marketing2_col_idx', ''),
                        'listing': listing_investment,
                        'marketing': marketing_investment,
                        'promo2': promo_percentage,
                        'promo': str(tm_plan_percentage),
                        'PromVol': str(prom_vol_value) if prom_vol_value > 0 else "-"
                    }
                    data_rows.append(row)

        if not data_rows:
            logger.warning("Нет данных для генерации.")
            return None

        df_output = pd.DataFrame(data_rows)

        df_output['temp_sku_num'] = pd.to_numeric(df_output['sku'].astype(str).str.replace(',', '.').str.replace(' ', ''), errors='coerce').fillna(0)
        df_output['temp_tt_num'] = pd.to_numeric(df_output['tt'].astype(str).str.replace(',', '.').str.replace(' ', ''), errors='coerce').fillna(0)
        df_filtered = df_output[(df_output['temp_sku_num'] > 0) & (df_output['temp_tt_num'] > 0)].copy()
        df_filtered.drop(columns=['temp_sku_num', 'temp_tt_num'], inplace=True)

        if df_filtered.empty:
            logger.warning("После фильтрации не осталось строк.")
            return None

        df_output = df_filtered

        columns_to_replace_dot = [
            'price', 'price_in', 'listing', 'listing2', 'marketing', 'marketing2',
            'promo', 'promo2', 'retro', 'volnew', 'PromVol', 'dopmarketing'
        ]
        for col in columns_to_replace_dot:
            if col in df_output.columns:
                df_output[col] = df_output[col].astype(str).str.replace('.', ',', regex=False).str.replace(' ', '', regex=False)
                df_output[col] = df_output[col].replace('nan', '', regex=False)

        df_output['volnew_check'] = df_output['volnew'].astype(str).replace(['nan', 'NaN', '-', ''], '0')
        df_output['volnew_numeric'] = pd.to_numeric(df_output['volnew_check'], errors='coerce').fillna(0)
        df_final = df_output[df_output['volnew_numeric'] > 0].copy()
        df_final.drop(columns=['volnew_check', 'volnew_numeric'], inplace=True)

        if df_final.empty:
            logger.warning("После финальной фильтрации не осталось строк.")
            return None

        desired_order = [
            'FileName', 'filial', 'forma', 'gr_sb', 'kam', 'viveska', 'client_type', 'sap-code',
            'start_date', 'end_date', 'pdate', 'HideStatus', 'sku_type', 'sku_type_sap',
            'price', 'price_in', 'listing', 'listing2', 'marketing', 'marketing2', 'promo', 'promo2',
            'retro', 'volnew', 'PromVol', 'dopmarketing', 'sku', 'tt'
        ]
        for col in df_final.columns:
            if col not in desired_order:
                desired_order.append(col)
        df_final = df_final.reindex(columns=desired_order)

        return df_final

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        traceback.print_exc()
        return None


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    if not os.path.exists(input_dir):
        logger.error(f"Директория {input_dir} не существует!")
        raise SystemExit(1)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_files = os.listdir(input_dir)
    excel_files = [f for f in all_files if (f.endswith('.xlsx') or f.endswith('.xls')) and not f.startswith('~')]

    total_files = len(excel_files)
    logger.info(f"Найдено {total_files} Excel файлов")

    if not excel_files:
        logger.warning("Не найдено Excel файлов")
        raise SystemExit(1)

    success_files = []
    failed_files = []

    with tqdm(total=total_files, desc="Парсинг", unit="файл") as pbar:
        start_time = time.time()

        for idx, file_name in enumerate(excel_files, 1):
            file_path = os.path.join(input_dir, file_name)

            if not os.path.exists(file_path):
                failed_files.append(file_name)
                pbar.update(1)
                continue

            try:
                df_result = process_single_file(file_path)

                if df_result is not None and not df_result.empty:
                    base_name = os.path.splitext(file_name)[0]
                    output_file_name = f"{base_name}_FINAL.xlsx"
                    output_file_path = os.path.join(output_dir, output_file_name)
                    df_result.to_excel(output_file_path, index=False, sheet_name='Результаты')
                    logger.info(f"✅ Записано в: {output_file_path}")
                    success_files.append(file_name)
                else:
                    failed_files.append(file_name)
            except Exception as e:
                failed_files.append(file_name)
                logger.error(f"Ошибка при обработке {file_name}: {e}")

            pbar.update(1)

    logger.info(f"\n{'='*70}")
    logger.info("ЗАВЕРШЕНИЕ")
    logger.info(f"{'='*70}")
    logger.info(f"Успешно: {len(success_files)}")
    logger.info(f"Ошибки: {len(failed_files)}")
    
    if failed_files:
        logger.info(f"\nФайлы с ошибками:")
        for fname in failed_files:
            logger.info(f"  - {fname}")