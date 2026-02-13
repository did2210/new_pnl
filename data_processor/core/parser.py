# -*- coding: utf-8 -*-
"""
Шаг 2 — Парсинг обработанных файлов в плоскую структуру.

Из листов «GFD Запрос», «Условия контракта», «Планирование продаж»,
«Планирование инвестиций», «SAP-код» извлекаем метаданные контракта,
данные по SKU, еженедельные объёмы/цены/ТМ, инвестиции.
Распределяем по месяцам контракта через календарь ЭЦП.
"""
import os
import re
import traceback
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple

import pandas as pd
from datetime import datetime

from .helpers import (
    to_float, parse_date, month_name, month_num, contract_months,
    ecp_calendar, ecp_week, MONTH_RU, SKU_MAP_REV, C,
)


# ════════════ СТРУКТУРЫ ════════════

@dataclass
class ParseResult:
    df: Optional[pd.DataFrame] = None
    rows: int = 0
    skus_found: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class _PlanCal:
    week_to_col: Dict[int, int] = field(default_factory=dict)
    week_to_month: Dict[int, int] = field(default_factory=dict)
    week_row: int = 0


# ════════════ ШАПКА ПЛАНА ════════════

def _parse_plan_header(df: pd.DataFrame) -> Optional[_PlanCal]:
    skip = {'тотал', 'total', 'итого', 'прирост', 'всего', 'growth'}

    # ищем строку с месяцами
    m_row = None
    for ri in range(min(len(df), 30)):
        txt = ' '.join(str(c) for c in df.iloc[ri] if str(c).lower() != 'nan')
        if any(m in txt.lower() for m in MONTH_RU):
            m_row = ri
            break
    if m_row is None:
        return None

    # ищем строку с W-неделями
    w_row = None
    for ri in range(m_row + 1, min(m_row + 4, len(df))):
        cnt = sum(1 for c in df.iloc[ri].astype(str)
                  if re.match(r'^W\d{1,2}$', c.strip(), re.I))
        if cnt >= 8:
            w_row = ri
            break
    if w_row is None:
        w_row = m_row + 1
    if w_row >= len(df):
        return None

    months_row = df.iloc[m_row].astype(str)
    weeks_row = df.iloc[w_row].astype(str)

    cal = _PlanCal(week_row=w_row)
    cur_month = None

    for ci in range(len(months_row)):
        wc = weeks_row.iloc[ci].strip() if ci < len(weeks_row) else ''
        if any(s in wc.lower() for s in skip):
            continue
        mc = months_row.iloc[ci].strip().lower() if ci < len(months_row) else ''
        for idx, mn in enumerate(MONTH_RU):
            if mn in mc:
                cur_month = idx + 1
                break
        m = re.match(r'^W(\d{1,2})$', wc, re.I)
        if m and cur_month is not None:
            wn = int(m.group(1))
            cal.week_to_col[wn] = ci
            cal.week_to_month[wn] = cur_month
    return cal if cal.week_to_col else None


# ════════════ ИЗВЛЕЧЕНИЕ ПО НЕДЕЛЯМ ════════════

def _weekly(df, sku, cal: _PlanCal, row_match: str) -> Dict[int, float]:
    """Извлечь данные по неделям для SKU по типу строки."""
    for i in range(cal.week_row + 1, len(df)):
        r = df.iloc[i]
        c0 = str(r.iloc[0]).strip() if pd.notna(r.iloc[0]) else ''
        c2 = str(r.iloc[2]).strip() if len(r) > 2 and pd.notna(r.iloc[2]) else ''
        if sku in c0 and row_match in c2:
            return {wn: to_float(r.iloc[ci]) for wn, ci in cal.week_to_col.items() if ci < len(r)}
    return {}


def _weekly_any_col(df, sku, cal: _PlanCal, marker: str) -> Dict[int, float]:
    """Ищет маркер в любом столбце строки."""
    for i in range(cal.week_row + 1, len(df)):
        r = df.iloc[i]
        c0 = str(r.iloc[0]).strip() if pd.notna(r.iloc[0]) else ''
        row_str = ' '.join(r.astype(str))
        if sku in c0 and marker in row_str:
            return {wn: to_float(r.iloc[ci]) for wn, ci in cal.week_to_col.items() if ci < len(r)}
    return {}


def _weekly_tm(df, sku, cal: _PlanCal) -> Dict[int, float]:
    out = {}
    for i in range(cal.week_row + 1, len(df)):
        r = df.iloc[i]
        c0 = str(r.iloc[0]).strip() if pd.notna(r.iloc[0]) else ''
        c2 = str(r.iloc[2]).strip() if len(r) > 2 and pd.notna(r.iloc[2]) else ''
        if sku in c0 and 'ТМ-план' in c2:
            for wn, ci in cal.week_to_col.items():
                if ci < len(r):
                    sv = str(r.iloc[ci]).replace('%', '').replace(' ', '').replace(',', '.').strip()
                    try:
                        out[wn] = float(sv) if sv and sv.replace('.', '').replace('-', '').isdigit() else 0.0
                    except Exception:
                        out[wn] = 0.0
            break
    return out


# ════════════ РАСПРЕДЕЛЕНИЕ ПО МЕСЯЦАМ ════════════

def _distribute(weekly: Dict[int, float], start_dt: datetime, end_dt: datetime,
                agg='sum') -> Dict[Tuple[int, int], float]:
    """agg: 'sum', 'max', 'avg'"""
    cal = ecp_calendar()
    periods = contract_months(start_dt, end_dt)
    buckets: Dict[Tuple[int, int], list] = {p: [] for p in periods}

    sw = ecp_week(start_dt)
    sgw = None
    for gw, (y, m, wiy) in cal.items():
        if y == start_dt.year and wiy == sw:
            sgw = gw
            break
    if sgw is None:
        return {p: 0.0 for p in periods}

    for i in range(52):
        gw = sgw + i
        if gw not in cal:
            break
        wy, wm, wiy = cal[gw]
        key = (wy, wm)
        if key not in buckets:
            break
        val = weekly.get(wiy, 0.0)
        buckets[key].append(val)

    result = {}
    for p, vals in buckets.items():
        if not vals:
            result[p] = 0.0
        elif agg == 'sum':
            result[p] = sum(vals)
        elif agg == 'max':
            result[p] = max(vals)
        elif agg == 'avg':
            pos = [v for v in vals if v > 0]
            result[p] = (sum(pos) / len(pos)) if pos else 0.0
    return result


def _prom_vol(weekly_vol, weekly_tm, start_dt, end_dt):
    cal = ecp_calendar()
    periods = contract_months(start_dt, end_dt)
    result = {p: 0 for p in periods}
    sw = ecp_week(start_dt)
    sgw = None
    for gw, (y, m, wiy) in cal.items():
        if y == start_dt.year and wiy == sw:
            sgw = gw
            break
    if sgw is None:
        return result
    for i in range(52):
        gw = sgw + i
        if gw not in cal:
            break
        wy, wm, wiy = cal[gw]
        key = (wy, wm)
        if key not in result:
            break
        if weekly_tm.get(wiy, 0.0) > 0:
            result[key] += int(round(weekly_vol.get(wiy, 0.0)))
    return result


# ════════════ ИНВЕСТИЦИИ ════════════

def _parse_investments(file_path: str):
    listing_d, marketing_d, promo_d = {}, {}, {}
    try:
        df = pd.read_excel(file_path, sheet_name='Планирование инвестиций', header=None)
    except Exception:
        return listing_d, marketing_d, promo_d

    # ищем заголовки Листинг / Маркетинг
    listing_hdr = ["Период оплаты за Листинг, руб. с НДС 20%"]
    marketing_hdr = ["Период оплаты бюджета Маркетинга, руб. с НДС 20%",
                     "Период оплаты бюджета Маркетинга, руб. с НДС  20%"]

    def _find_section_row(keywords):
        for ri in range(len(df)):
            rs = ' '.join(str(c) for c in df.iloc[ri] if pd.notna(c))
            if any(k in rs for k in keywords):
                return ri
        return None

    def _month_cols(row_idx, col_start=0, col_end=None):
        """Вернуть {col_idx: month_name} для строки row_idx."""
        if row_idx is None or row_idx + 1 >= len(df):
            return {}
        mr = df.iloc[row_idx + 1]
        end = col_end if col_end else len(mr)
        result = {}
        for ci in range(col_start, min(end, len(mr))):
            v = mr.iloc[ci]
            if pd.notna(v) and str(v).strip().lower() in MONTH_RU:
                result[ci] = str(v).strip().lower()
        return result

    def _section_data(section_ri, mcols):
        out = []
        if section_ri is None or not mcols:
            return out
        end_marks = ["ООО", "Отчет сгенерирован", "ПЛАНИРОВАНИЕ", "УСЛОВИЯ", "SAP", "ЗАПРОС"]
        for ri in range(section_ri + 2, len(df)):
            r = df.iloc[ri]
            fc = str(r.iloc[0]).strip() if pd.notna(r.iloc[0]) and str(r.iloc[0]).strip() else ''
            if any(m in fc for m in end_marks) and fc != 'Brand':
                break
            if fc and 'Brand' not in fc:
                for ci, mn in mcols.items():
                    if ci < len(r):
                        v = r.iloc[ci]
                        if pd.notna(v) and str(v).strip() not in ('', '0'):
                            try:
                                nv = float(str(v).replace('\xa0', '').replace(' ', '').replace(',', '.'))
                                if nv != 0:
                                    out.append((fc, mn, nv))
                            except Exception:
                                pass
        return out

    # комбинированная строка или раздельные
    combined_ri = None
    for ri in range(len(df)):
        rs = ' '.join(str(c) for c in df.iloc[ri] if pd.notna(c))
        if any(k in rs for k in listing_hdr) and any(k in rs for k in marketing_hdr):
            combined_ri = ri
            break

    if combined_ri is not None:
        row_data = df.iloc[combined_ri]
        lc = mc = None
        for ci, cell in enumerate(row_data):
            if pd.notna(cell):
                s = str(cell)
                if any(k in s for k in listing_hdr):
                    lc = ci
                if any(k in s for k in marketing_hdr):
                    mc = ci
        l_mcols = _month_cols(combined_ri, lc or 0, mc)
        m_mcols = _month_cols(combined_ri, mc or 0)
        data_ri = combined_ri
    else:
        l_ri = _find_section_row(listing_hdr)
        m_ri = _find_section_row(marketing_hdr)
        l_mcols = _month_cols(l_ri)
        m_mcols = _month_cols(m_ri)
        data_ri = max(l_ri or -1, m_ri or -1)
        if data_ri < 0:
            data_ri = None

    if data_ri is not None:
        for brand, mn, val in _section_data(data_ri, l_mcols):
            listing_d[(brand, mn)] = val
        for brand, mn, val in _section_data(data_ri, m_mcols):
            marketing_d[(brand, mn)] = val

    # Промо-скидки, Листинг%, Маркетинг%
    def _pct_row(keyword, exclude=None):
        for ri in range(len(df)):
            rs = ' '.join(str(c) for c in df.iloc[ri] if pd.notna(c))
            if keyword in rs and (exclude is None or exclude not in rs):
                return ri
        return None

    def _brand_row_above(ri):
        for si in range(ri - 1, max(ri - 10, -1), -1):
            txt = ' '.join(str(c).strip() for c in df.iloc[si] if pd.notna(c))
            if any(k in txt for k in ['Brand', 'Бренд', 'Brand/Статья']):
                return si
        return ri - 1 if ri > 0 else None

    def _pct_val(raw):
        if not raw:
            return None
        m = re.search(r'([+-]?\d+[.,]?\d*)\s*%?', raw)
        if m:
            v = float(m.group(1).replace(',', '.'))
            if '%' in raw or abs(v) > 1:
                v /= 100.0
            return v
        return None

    def _extract_pct_pairs(ri, target, key_suffix=None):
        bri = _brand_row_above(ri)
        if bri is None:
            return
        br = df.iloc[bri]
        dr = df.iloc[ri]
        for ci in range(1, min(len(br), len(dr))):
            bn = str(br.iloc[ci]).strip() if pd.notna(br.iloc[ci]) else ''
            vr = str(dr.iloc[ci]).strip() if pd.notna(dr.iloc[ci]) else ''
            if not bn or bn.lower() in ('brand', 'бренд', 'brand/статья', ''):
                continue
            p = _pct_val(vr)
            if p is not None:
                if key_suffix:
                    target[(bn, key_suffix)] = p
                else:
                    target[bn] = p

    pri = _pct_row('Промо-скидки')
    if pri is not None:
        _extract_pct_pairs(pri, promo_d)

    lri = _pct_row('Листинг', exclude=None)
    if lri is not None:
        rs = ' '.join(str(c) for c in df.iloc[lri] if pd.notna(c))
        if 'Безусловные бонусы' in rs:
            _extract_pct_pairs(lri, listing_d, 'all')

    mri = _pct_row('Маркетинг', exclude='Листинг')
    if mri is not None:
        _extract_pct_pairs(mri, marketing_d, 'all')

    return listing_d, marketing_d, promo_d


# ════════════ МЕТАДАННЫЕ ════════════

def _field(df, label, offset=3):
    """Найти значение поля в GFD / контракте по метке."""
    rows = df[df.apply(lambda r: r.astype(str).str.contains(label, case=False, na=False).any(), axis=1)]
    if rows.empty:
        return ''
    rd = rows.iloc[0].astype(str)
    idx = None
    for i, c in enumerate(rd):
        if label.lower() in c.lower():
            idx = i
            break
    if idx is None:
        return ''
    for i in range(idx + 1, len(rd)):
        v = rd.iloc[i]
        if v != 'nan' and v.strip():
            return v
    return ''


def _forma(df):
    rows = df[df.apply(lambda r: any(str(c).startswith('Форма от') for c in r.astype(str)), axis=1)]
    if rows.empty:
        return ''
    vals = [v for v in rows.iloc[0].astype(str) if v != 'nan']
    full = ' | '.join(vals)
    return full.split('.')[0] + '.' if '.' in full else full


def _sap_codes(file_path):
    try:
        df = pd.read_excel(file_path, sheet_name='SAP-код', header=None)
        ci = None
        hri = None
        for ri in range(min(5, len(df))):
            for c in range(len(df.columns)):
                if 'Коды заказчика клиента' in str(df.iloc[ri, c]):
                    ci, hri = c, ri
                    break
        codes = set()
        if ci is not None:
            for r in range(hri + 1 if hri is not None else 0, len(df)):
                v = df.iloc[r, ci]
                if pd.notna(v) and str(v).strip():
                    codes.add(str(v).strip())
        return ';'.join(codes)
    except Exception:
        return ''


def _dates(df_contract):
    sd_s, sd_d, ed_s, ed_d = '', None, '', None
    if df_contract.empty:
        return sd_s, sd_d, ed_s, ed_d
    for label in ('начало', 'окончание'):
        rows = df_contract[df_contract.apply(
            lambda r: r.astype(str).str.contains(label, case=False, na=False).any(), axis=1)]
        if rows.empty:
            continue
        rd = rows.iloc[0]
        ci = None
        for i, c in enumerate(rd.astype(str)):
            if label in c.lower():
                ci = i
                break
        if ci is not None and ci + 2 < len(rd):
            ds, dt = parse_date(rd.iloc[ci + 2])
            if label == 'начало':
                sd_s, sd_d = ds, dt
            else:
                ed_s, ed_d = ds, dt
    return sd_s, sd_d, ed_s, ed_d


def _sku_data(df_contract):
    """Извлекаем данные SKU из «Условий контракта»."""
    out = {}
    if df_contract.empty:
        return out

    mask = df_contract.astype(str).apply(
        lambda r: r.str.contains('Brand', case=False, na=False).any(), axis=1)
    hits = df_contract[mask]
    if hits.empty:
        return out

    bri = hits.index[0]
    hri = bri
    hrow = df_contract.iloc[bri].astype(str)
    if 'Кол-во ТТ с листингом' not in ' '.join(hrow):
        if bri + 1 < len(df_contract):
            p = df_contract.iloc[bri + 1].astype(str)
            if 'Кол-во ТТ с листингом' in ' '.join(p):
                hri = bri + 1
                hrow = p

    col_map = {}
    for i, c in enumerate(hrow):
        col_map[re.sub(r'\s+', ' ', c.strip().replace('\n', ' '))] = i

    needed = {
        'Brand': 'brand', 'Кол-во SKU': 'sku',
        'Кол-во ТТ с листингом': 'tt',
        'Цена поставки по ИЦМ': 'price',
        'ЦМ по категории клиента': 'price_in',
        'Бонус за объем/Retro, от ТО без НДС, %': 'retro',
        'Маркетинг бюджет 1 от факт. ТО, %': 'dopmarketing',
    }
    found = {}
    for k, v in needed.items():
        if k in col_map:
            found[v] = col_map[k]
        else:
            for ht, idx in col_map.items():
                if k.lower() in ht.lower() or ht.lower() in k.lower():
                    found[v] = idx
                    break

    for i in range(hri + 1, len(df_contract)):
        rd = df_contract.iloc[i].astype(str)
        fc = rd.iloc[0] if len(rd) > 0 else 'nan'
        if fc == 'nan' or not fc.strip():
            continue
        if 'Отчет сгенерирован' in fc:
            break
        if 'Brand' in fc and i > hri + 5:
            break
        bci = found.get('brand', 0)
        if bci < len(rd):
            name = rd.iloc[bci].strip()
            if name and name != 'nan' and not name.startswith('Brand'):
                if name not in out:
                    rec = {}
                    for key, ci in found.items():
                        if ci < len(rd):
                            cv = rd.iloc[ci]
                            rec[key] = cv.strip().replace(' ', '').replace(',', '.') if cv != 'nan' and cv.strip() else ''
                    out[name] = rec
    return out


# ════════════ ГЛАВНАЯ ФУНКЦИЯ ════════════

def parse_file(file_path: str) -> ParseResult:
    """Полный парсинг одного обработанного файла → плоский DataFrame."""
    res = ParseResult()
    try:
        # 1) инвестиции
        listing_d, marketing_d, promo_d = _parse_investments(file_path)

        # 2) GFD
        try:
            df_gfd = pd.read_excel(file_path, sheet_name='GFD Запрос', header=None)
        except Exception as e:
            res.errors.append(f"Лист 'GFD Запрос' не найден: {e}")
            return res
        forma = _forma(df_gfd)
        filial = _field(df_gfd, 'Филиал')
        viveska = _field(df_gfd, 'Название на вывеске')
        gr_sb = _field(df_gfd, 'Группа сбыта')
        kam = _field(df_gfd, 'Ответственный КАМ')
        client_type = _field(df_gfd, 'Категория клиента')
        sap_code = _sap_codes(file_path)

        # 3) даты
        try:
            df_con = pd.read_excel(file_path, sheet_name='Условия контракта', header=None)
        except Exception:
            df_con = pd.DataFrame()
        sd_s, sd_d, ed_s, ed_d = _dates(df_con)
        if not sd_d:
            res.errors.append("Дата начала контракта не найдена")
            return res
        if not ed_d:
            res.errors.append("Дата окончания контракта не найдена")
            return res

        # 4) SKU
        skus = _sku_data(df_con)
        if not skus:
            res.errors.append("Не найдено ни одного SKU в условиях контракта")
            return res
        res.skus_found = len(skus)

        # 5) план продаж
        try:
            df_sales = pd.read_excel(file_path, sheet_name='Планирование продаж', header=None)
        except Exception:
            df_sales = pd.DataFrame()
        cal = _parse_plan_header(df_sales) if not df_sales.empty else None
        if cal is None:
            res.errors.append("Не удалось распарсить календарь плана продаж")
            return res

        # 6) строим строки
        rows = []
        base = os.path.splitext(os.path.basename(file_path))[0]
        c_months = contract_months(sd_d, ed_d)

        for sku_name, sku_rec in skus.items():
            # объём
            wv = _weekly(df_sales, sku_name, cal, 'Новый контракт')
            if not wv or sum(wv.values()) == 0:
                wv = _weekly(df_sales, sku_name, cal, 'Контракт')
            # TM
            wtm = _weekly_tm(df_sales, sku_name, cal)
            # цена
            wp = _weekly_any_col(df_sales, sku_name, cal, 'Цена поставки')

            vol_m = _distribute(wv, sd_d, ed_d, 'sum')
            tm_m = _distribute(wtm, sd_d, ed_d, 'max')
            pr_m = _distribute(wp, sd_d, ed_d, 'avg')
            pv_m = _prom_vol(wv, wtm, sd_d, ed_d)

            for (y, m) in c_months:
                mn = month_name(m)
                vol = int(round(vol_m.get((y, m), 0)))
                if vol <= 0:
                    continue  # пропускаем нулевые периоды
                rows.append({
                    'FileName': base,
                    'filial': filial,
                    'forma': forma,
                    'gr_sb': gr_sb,
                    'kam': kam,
                    'viveska': viveska,
                    'client_type': client_type,
                    'sap-code': sap_code,
                    'start_date': sd_s,
                    'end_date': ed_s,
                    'pdate': datetime(y, m, 1).strftime('%d.%m.%Y'),
                    'HideStatus': '0',
                    'sku_type': SKU_MAP_REV.get(sku_name, ''),
                    'sku_type_sap': sku_name,
                    'price': str(round(pr_m.get((y, m), 0.0), 2)),
                    'price_in': sku_rec.get('price_in', ''),
                    'retro': sku_rec.get('retro', ''),
                    'dopmarketing': sku_rec.get('dopmarketing', ''),
                    'sku': sku_rec.get('sku', ''),
                    'tt': sku_rec.get('tt', ''),
                    'listing2': listing_d.get((sku_name, 'all'), ''),
                    'marketing2': marketing_d.get((sku_name, 'all'), ''),
                    'listing': listing_d.get((sku_name, mn), ''),
                    'marketing': marketing_d.get((sku_name, mn), ''),
                    'promo2': promo_d.get(sku_name, ''),
                    'promo': str(tm_m.get((y, m), 0.0)),
                    'volnew': str(vol),
                    'PromVol': str(pv_m.get((y, m), 0)) if pv_m.get((y, m), 0) > 0 else '-',
                })

        if not rows:
            res.warnings.append("Нет строк с объёмами > 0")
            return res

        df_out = pd.DataFrame(rows)

        # фильтр: sku>0 и tt>0
        for tmp in ('_sn', '_tn'):
            col = 'sku' if tmp == '_sn' else 'tt'
            df_out[tmp] = pd.to_numeric(
                df_out[col].astype(str).str.replace(',', '.').str.replace(' ', ''),
                errors='coerce').fillna(0)
        df_out = df_out[(df_out['_sn'] > 0) & (df_out['_tn'] > 0)].drop(columns=['_sn', '_tn'])

        if df_out.empty:
            res.warnings.append("После фильтрации по sku/tt не осталось строк")
            return res

        # числовые → запятая
        num_cols = ['price', 'price_in', 'listing', 'listing2', 'marketing', 'marketing2',
                    'promo', 'promo2', 'retro', 'volnew', 'PromVol', 'dopmarketing']
        for c in num_cols:
            if c in df_out.columns:
                df_out[c] = (df_out[c].astype(str)
                             .str.replace('.', ',', regex=False)
                             .str.replace(' ', '', regex=False)
                             .replace('nan', ''))

        res.df = df_out
        res.rows = len(df_out)
        return res

    except Exception as e:
        res.errors.append(f"Критическая ошибка: {e}")
        traceback.print_exc()
        return res
