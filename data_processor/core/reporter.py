# -*- coding: utf-8 -*-
"""
Шаг 4 — Генерация итогового Excel P&L-отчёта.
"""
import os
import numpy as np
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from .helpers import series_to_num


def _fmt(wb_path):
    """Красивое форматирование итогового файла."""
    wb = load_workbook(wb_path)
    hdr_fill = PatternFill('solid', fgColor="2F5496")
    hdr_font = Font(name='Calibri', size=10, bold=True, color="FFFFFF")
    plan_fill = PatternFill('solid', fgColor="D6E4F0")
    fact_fill = PatternFill('solid', fgColor="E2EFDA")
    diff_fill = PatternFill('solid', fgColor="FCE4D6")
    red_font = Font(name='Calibri', size=10, color="FF0000", bold=True)
    brd = Border(left=Side('thin'), right=Side('thin'),
                 top=Side('thin'), bottom=Side('thin'))

    for ws in wb.worksheets:
        if ws.max_row <= 1:
            continue
        for ci in range(1, ws.max_column + 1):
            c = ws.cell(row=1, column=ci)
            c.fill = hdr_fill
            c.font = hdr_font
            c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            c.border = brd
        for ri in range(2, ws.max_row + 1):
            for ci in range(1, ws.max_column + 1):
                c = ws.cell(row=ri, column=ci)
                c.border = brd
                c.alignment = Alignment(vertical='center')
                h = str(ws.cell(row=1, column=ci).value or '').lower()
                if 'плановые' in h or 'план' == h.strip():
                    c.fill = plan_fill
                elif 'фактические' in h or 'факт' == h.strip():
                    c.fill = fact_fill
                elif 'разница' in h:
                    c.fill = diff_fill
                    try:
                        if c.value is not None and float(c.value) < 0:
                            c.font = red_font
                    except (ValueError, TypeError):
                        pass
        for ci in range(1, ws.max_column + 1):
            mx = max((len(str(ws.cell(row=r, column=ci).value or ''))
                       for r in range(1, min(ws.max_row + 1, 100))), default=8)
            ws.column_dimensions[get_column_letter(ci)].width = min(mx + 4, 40)
        ws.freeze_panes = 'A2'
    wb.save(wb_path)


def generate_report(merged_df: pd.DataFrame, out_path: str,
                    sales_path=None, costs_np_path=None, costs_ip_path=None,
                    cm_path=None, cogs_path=None) -> str:
    """
    Генерирует итоговый P&L-файл.

    Обязательный: merged_df — объединённая база.
    Опциональные: пути к файлам продаж, затрат, ЦМ, себестоимости.
    """
    df = merged_df.copy()

    # ─── числовые ───
    num = ['price', 'price_in', 'listing', 'listing2', 'marketing', 'marketing2',
           'promo', 'promo2', 'retro', 'volnew', 'PromVol', 'dopmarketing']
    for c in num:
        if c in df.columns:
            df[c] = series_to_num(df[c])
    for c in ('pdate', 'start_date', 'end_date'):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], dayfirst=True, errors='coerce')

    # ─── группировка ───
    gk = [c for c in ('FileName', 'viveska', 'gr_sb', 'sku_type_sap',
                        'pdate', 'start_date', 'end_date') if c in df.columns]
    if 'volnew' in df.columns:
        plan = df.groupby(gk, as_index=False)['volnew'].sum()
        plan.rename(columns={'volnew': 'Плановые продажи, шт'}, inplace=True)
    else:
        plan = df[gk].drop_duplicates()
        plan['Плановые продажи, шт'] = 0
    dc = plan.copy()

    # ─── ЦМ ───
    if cm_path and os.path.exists(cm_path):
        try:
            cm = pd.read_excel(cm_path)
            cm['ЦМ'] = series_to_num(cm['ЦМ'])
            mk = [c for c in ('gr_sb', 'sku_type_sap', 'pdate') if c in cm.columns and c in dc.columns]
            if mk:
                dc = dc.merge(cm[mk + ['ЦМ']], on=mk, how='left')
                dc.rename(columns={'ЦМ': 'price_in'}, inplace=True)
        except Exception:
            pass
    if 'price_in' not in dc.columns and 'price_in' in df.columns:
        pi = df.groupby(gk, as_index=False)['price_in'].first()
        dc = dc.merge(pi, on=gk, how='left')
    if 'price_in' not in dc.columns:
        dc['price_in'] = 0.0
    dc['price_in'] = series_to_num(dc['price_in'])
    dc['Плановые продажи, руб'] = dc['Плановые продажи, шт'] * dc['price_in']

    # ─── инвестиционные колонки ───
    for c in ('listing2', 'listing', 'retro', 'dopmarketing', 'marketing', 'PromVol'):
        if c in df.columns:
            g = df.groupby(gk, as_index=False)[c].sum()
            dc = dc.merge(g, on=gk, how='left')
            dc[c] = series_to_num(dc[c])
        else:
            dc[c] = 0.0

    dc['Плановые затраты «Скидка в цене», руб'] = dc['Плановые продажи, руб'] * dc['listing2']
    dc['Плановые затраты «Листинг/безусловные выплаты», руб'] = dc['listing']
    dc['Плановые затраты «Ретро», руб'] = (dc['Плановые продажи, руб'] / 1.2) * dc['retro']
    dc['Плановые затраты «Маркетинг», руб'] = dc['Плановые продажи, руб'] * dc['dopmarketing'] + dc['marketing']
    dc['Плановые затраты «Промо-скидка», руб'] = dc['PromVol']

    # статус
    if 'pdate' in dc.columns and 'end_date' in dc.columns:
        dc['контракт'] = np.where(dc['pdate'] > dc['end_date'], 'завершенный', 'действующий')
    else:
        dc['контракт'] = 'действующий'

    # ─── факт продаж ───
    dc['Факт продажи, шт.'] = 0.0
    if sales_path and os.path.exists(sales_path):
        try:
            sl = pd.read_excel(sales_path)
            sl['sales_date'] = pd.to_datetime(sl.get('sales_date'), errors='coerce')
            sl['vol_2'] = series_to_num(sl['vol_2'])
            mk = [c for c in ('viveska', 'sku_type_sap', 'pdate') if c in sl.columns and c in dc.columns]
            if mk:
                sa = sl.groupby(mk, as_index=False)['vol_2'].sum()
                dc = dc.merge(sa, on=mk, how='left')
                dc['Факт продажи, шт.'] = series_to_num(dc['vol_2'])
                dc.drop(columns=['vol_2'], errors='ignore', inplace=True)
        except Exception:
            pass
    dc['Факт продажи, руб (от ЦМ)'] = dc['Факт продажи, шт.'] * dc['price_in']
    dc['Разница, шт'] = dc['Факт продажи, шт.'] - dc['Плановые продажи, шт']
    dc['Разница, руб'] = dc['Факт продажи, руб (от ЦМ)'] - dc['Плановые продажи, руб']

    # ─── факт затрат ───
    fact_cols = [
        'Фактические затраты «Листинг/безусловные выплаты», руб',
        'Фактические затраты «Ретро», руб',
        'Фактические затраты «Маркетинг», руб',
        'Фактические затраты «Промо-скидка», руб',
        'Фактические затраты «Скидка в цене», руб',
    ]
    if costs_np_path and os.path.exists(costs_np_path):
        try:
            cnp = pd.read_excel(costs_np_path)
            cnp['Сумма'] = series_to_num(cnp['Сумма'])
            cnp['pdate'] = pd.to_datetime(cnp.get('Месяц/год'), errors='coerce')
            mk = [c for c in ('pdate', 'viveska', 'sku_type_sap') if c in cnp.columns and c in dc.columns]
            if mk:
                for exp, cn in [('Листинг', fact_cols[0]), ('Маркетинг', fact_cols[2]),
                                ('Ретро', fact_cols[1])]:
                    f = cnp[cnp['Статья расходов'].str.contains(exp, case=False, na=False)]
                    if not f.empty:
                        g = f.groupby(mk, as_index=False)['Сумма'].sum()
                        dc = dc.merge(g, on=mk, how='left')
                        dc.rename(columns={'Сумма': cn}, inplace=True)
        except Exception:
            pass
    if costs_ip_path and os.path.exists(costs_ip_path):
        try:
            cip = pd.read_excel(costs_ip_path)
            cip['Сумма в валюте документа'] = series_to_num(cip['Сумма в валюте документа'])
            cip['pdate'] = pd.to_datetime(cip.get('Месяц/год'), errors='coerce')
            mk = [c for c in ('pdate', 'viveska', 'sku_type_sap') if c in cip.columns and c in dc.columns]
            if mk and 'примечание' in cip.columns:
                for pattern, cn in [('промо акция', fact_cols[3]), ('скидка в цене', fact_cols[4])]:
                    f = cip[cip['примечание'].str.contains(pattern, case=False, na=False)]
                    if not f.empty:
                        g = f.groupby(mk, as_index=False)['Сумма в валюте документа'].sum()
                        dc = dc.merge(g, on=mk, how='left')
                        dc.rename(columns={'Сумма в валюте документа': cn}, inplace=True)
        except Exception:
            pass

    for c in fact_cols:
        if c not in dc.columns:
            dc[c] = 0.0
        else:
            dc[c] = series_to_num(dc[c])

    dc['план затраты'] = sum(dc.get(c, 0) for c in [
        'Плановые затраты «Листинг/безусловные выплаты», руб',
        'Плановые затраты «Скидка в цене», руб',
        'Плановые затраты «Ретро», руб',
        'Плановые затраты «Маркетинг», руб',
        'Плановые затраты «Промо-скидка», руб',
    ])
    dc['факт затраты'] = sum(dc.get(c, 0) for c in fact_cols)

    # ─── себестоимость ───
    if cogs_path and os.path.exists(cogs_path):
        try:
            cg = pd.read_excel(cogs_path)
            cg['cogs'] = series_to_num(cg['cogs'])
            mk = [c for c in ('sku_type_sap', 'pdate') if c in cg.columns and c in dc.columns]
            if mk:
                dc = dc.merge(cg[mk + ['cogs']], on=mk, how='left')
        except Exception:
            pass
    if 'cogs' not in dc.columns:
        dc['cogs'] = 0.0
    dc['cogs'] = series_to_num(dc['cogs'])
    dc['продажи по сс план'] = dc['Плановые продажи, шт'] * dc['cogs']
    dc['продажи по сс факт'] = dc['Факт продажи, шт.'] * dc['cogs']
    dc['доход план'] = dc['Плановые продажи, руб'] - dc['продажи по сс план'] - dc['план затраты']
    dc['доход факт'] = dc['Факт продажи, руб (от ЦМ)'] - dc['продажи по сс факт'] - dc['факт затраты']

    # дедуп
    dc.drop_duplicates(inplace=True)
    kk = [c for c in ('pdate', 'FileName', 'viveska', 'gr_sb', 'sku_type_sap',
                        'start_date', 'end_date') if c in dc.columns]
    if kk:
        dc.drop_duplicates(subset=kk, keep='first', inplace=True)

    # порядок
    order = [
        'pdate', 'FileName', 'viveska', 'gr_sb', 'sku_type_sap',
        'start_date', 'end_date', 'контракт',
        'Плановые продажи, шт', 'Факт продажи, шт.', 'Разница, шт',
        'Плановые продажи, руб', 'Факт продажи, руб (от ЦМ)', 'Разница, руб',
        'Плановые затраты «Листинг/безусловные выплаты», руб', fact_cols[0],
        'Плановые затраты «Скидка в цене», руб', fact_cols[4],
        'Плановые затраты «Ретро», руб', fact_cols[1],
        'Плановые затраты «Маркетинг», руб', fact_cols[2],
        'Плановые затраты «Промо-скидка», руб', fact_cols[3],
        'план затраты', 'факт затраты',
        'продажи по сс план', 'продажи по сс факт',
        'доход план', 'доход факт',
    ]
    order = [c for c in order if c in dc.columns]
    for c in dc.columns:
        if c not in order:
            order.append(c)
    dc = dc[order]

    # сохранение
    n_chunks = max(1, len(dc) // 800_000 + (1 if len(dc) % 800_000 else 0))
    with pd.ExcelWriter(out_path, engine='openpyxl') as w:
        for i in range(n_chunks):
            s, e = i * 800_000, min((i + 1) * 800_000, len(dc))
            sn = f'Sheet{i+1}' if n_chunks > 1 else 'Результат'
            dc.iloc[s:e].to_excel(w, sheet_name=sn, index=False)

    try:
        _fmt(out_path)
    except Exception:
        pass
    return out_path
