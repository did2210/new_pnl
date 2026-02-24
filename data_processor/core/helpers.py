# -*- coding: utf-8 -*-
"""
Общие утилиты и константы.
"""
import re
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple, Any

# ─────────────── КОНСТАНТЫ ───────────────

MONTH_RU = [
    'январь', 'февраль', 'март', 'апрель', 'май', 'июнь',
    'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь',
]

SKU_MAP = {
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
    'tornadosahar473pet': 'Tornado сахар 0,473 PET',
}
SKU_MAP_REV = {v: k for k, v in SKU_MAP.items()}

PLANNING_SHEETS = ["NEW CNR 1", "Расчет инвестиций", "NEW CNR", "Расчет инвестиций (2)"]

# ─────────────── ТИПЫ / ПРЕОБРАЗОВАНИЯ ───────────────

def to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, tuple):
        return str(value[0]) if len(value) == 1 else ", ".join(str(x) for x in value)
    return str(value)


def to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return 0.0
        return float(value)
    s = str(value).replace('\xa0', '').replace(' ', '').replace(',', '.').strip()
    if not s or s.lower() in ('nan', '-', 'none', 'null'):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def series_to_num(series: pd.Series) -> pd.Series:
    c = series.astype(str).str.strip()
    c = c.replace(['NULL', 'null', 'Null', '', ' ', '-', 'nan', 'NaN'], '0')
    c = c.str.replace('\xa0', '', regex=False).str.replace(' ', '', regex=False).str.replace(',', '.', regex=False)
    return pd.to_numeric(c, errors='coerce').fillna(0.0)


# ─────────────── ДАТЫ ───────────────

_DATE_FMTS = ['%d.%m.%Y', '%m/%d/%y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']


def parse_date(value: Any) -> Tuple[str, Optional[datetime]]:
    """Возвращает (строка_дд.мм.гггг, datetime | None)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "", None
    if isinstance(value, datetime):
        return value.strftime('%d.%m.%Y'), value
    s = str(value).strip()
    if not s or s.lower() in ('nan', 'none', 'nat'):
        return "", None
    for fmt in _DATE_FMTS:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime('%d.%m.%Y'), dt
        except ValueError:
            continue
    try:
        ts = pd.to_datetime(value, dayfirst=True, errors='coerce')
        if pd.notna(ts):
            dt = ts.to_pydatetime()
            return dt.strftime('%d.%m.%Y'), dt
    except Exception:
        pass
    return s, None


def month_name(num: int) -> str:
    return MONTH_RU[num - 1] if 1 <= num <= 12 else ''


def month_num(name: str) -> Optional[int]:
    n = name.strip().lower()
    for i, m in enumerate(MONTH_RU):
        if m == n:
            return i + 1
    return None


def contract_months(start: datetime, end: datetime) -> list:
    """Список (year, month) за весь период контракта."""
    result = []
    cur = start.replace(day=1)
    while cur <= end:
        result.append((cur.year, cur.month))
        if cur.month == 12:
            cur = cur.replace(month=1, year=cur.year + 1, day=1)
        else:
            cur = cur.replace(month=cur.month + 1)
    return result


# ─────────────── КАЛЕНДАРЬ ЭЦП (2024‑2028) ───────────────

_ecp_cache: Optional[dict] = None


def ecp_calendar() -> dict:
    """
    {global_week: (year, month, week_in_year)}
    W1 = дни 1‑7 января; если в неделю попадает 1-е число — вся неделя к тому месяцу.
    """
    global _ecp_cache
    if _ecp_cache is not None:
        return _ecp_cache
    cal = {}
    gw = 1
    for year in range(2024, 2029):
        for wiy in range(1, 53):
            sd = (wiy - 1) * 7 + 1
            ed = wiy * 7
            try:
                ws = datetime(year, 1, 1) + timedelta(days=sd - 1)
                ye = datetime(year, 12, 31)
                we = min(datetime(year, 1, 1) + timedelta(days=ed - 1), ye)
            except Exception:
                break
            if ws.year > year:
                break
            wm, wy = ws.month, ws.year
            d = ws
            while d <= we:
                if d.day == 1:
                    wm, wy = d.month, d.year
                    break
                d += timedelta(days=1)
            if wy == year:
                cal[gw] = (wy, wm, wiy)
                gw += 1
            else:
                break
    _ecp_cache = cal
    return cal


def ecp_week(dt: datetime) -> int:
    doy = dt.timetuple().tm_yday
    return min(((doy - 1) // 7) + 1, 52)


# ─────────────── ВЫВОД В КОНСОЛЬ ───────────────

class C:
    """ANSI-коды (безопасно: если терминал не поддерживает — ничего страшного)."""
    RESET = '\033[0m'
    BOLD  = '\033[1m'
    RED   = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    CYAN  = '\033[96m'
    DIM   = '\033[2m'

    @staticmethod
    def ok(msg):   return f"{C.GREEN}[OK]{C.RESET} {msg}"
    @staticmethod
    def err(msg):  return f"{C.RED}[ОШИБКА]{C.RESET} {msg}"
    @staticmethod
    def warn(msg): return f"{C.YELLOW}[!]{C.RESET} {msg}"
    @staticmethod
    def info(msg): return f"{C.CYAN}[i]{C.RESET} {msg}"
    @staticmethod
    def step(n, msg): return f"{C.BOLD}{C.CYAN}[Шаг {n}]{C.RESET} {msg}"
    @staticmethod
    def header(msg):
        w = 62
        line = '═' * w
        return f"\n╔{line}╗\n║{msg:^{w}}║\n╚{line}╝"
