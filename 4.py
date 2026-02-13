import pandas as pd
import numpy as np
import os

# Путь к данным
BASE_PATH = r'\\FS\Users\Private\GFD\Public\Трейд-маркетинг\7.Общие документы\Гусев\P&L\расчет\расчетт'

# Утилита: безопасное приведение к числу с обработкой NULL и запятой
def to_numeric_safe_with_null(series):
    series_clean = series.astype(str).str.strip()
    series_clean = series_clean.replace(['NULL', 'null', 'Null', '', ' '], '0.0')
    series_clean = series_clean.str.replace(',', '.', regex=False)
    result = pd.to_numeric(series_clean, errors='coerce')
    return result.fillna(0.0)

def save_to_excel_with_chunks(df, output_path, chunk_size=800000):
    n_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for i in range(n_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(df))
            chunk = df.iloc[start_idx:end_idx].copy()
            sheet_name = f'Sheet{i+1}' if n_chunks > 1 else 'Sheet1'
            chunk.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Сохранено {len(chunk)} строк в лист '{sheet_name}'")
    print(f"\n✅ Готово! Сохранено {n_chunks} листов в: {output_path}")

def get_sku_normalizer_from_methodichka():
    methodichka_path = os.path.join(BASE_PATH, 'методичка.xlsx')
    df_methodichka = pd.read_excel(methodichka_path, sheet_name='итог')
    required_columns = ['sku_type_sap', 'itog']
    if not all(col in df_methodichka.columns for col in required_columns):
        raise ValueError(f"Файл {methodichka_path} должен содержать столбцы: {required_columns}")
    mapping_dict = {}
    for _, row in df_methodichka.iterrows():
        source_key = str(row['sku_type_sap']).strip()
        target_value = str(row['itog']).strip()
        if source_key not in mapping_dict:
            mapping_dict[source_key] = target_value
    return mapping_dict

def normalize_sku(x, normalizer_dict):
    if pd.isna(x):
        return x
    x_str = str(x).strip()
    if x_str in normalizer_dict:
        return normalizer_dict[x_str]
    else:
        return x

# --- ОРИГИНАЛЬНАЯ ФУНКЦИЯ load_ecp_map ---
def load_ecp_map():
    ecp_data_path = os.path.join(BASE_PATH, 'ECP_data.xlsx')
    print(f"Загрузка ECP данных из: {ecp_data_path}")
    df = pd.read_excel(ecp_data_path)
    print(f"Размер исходного df: {df.shape}")

    required_cols = ['viveska', 'sap-code']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"В ECP_data.xlsx отсутствуют столбцы: {missing_cols}")

    df_subset = df[['viveska', 'sap-code']].copy()
    print(f"Размер df_subset до разбиения: {df_subset.shape}")

    data = []
    for _, row in df_subset.iterrows():
        sap_codes_str = str(row['sap-code'])
        sap_codes = sap_codes_str.split(';')
        viveska = row['viveska']
        for sap_code in sap_codes:
            clean_sap_code = sap_code.strip()
            if clean_sap_code: # Добавляем только непустые
                data.append({
                    'sap-code': clean_sap_code,
                    'viveska': viveska
                })

    ecp = pd.DataFrame(data)
    print(f"Размер ecp до to_numeric и drop_duplicates: {ecp.shape}")

    if ecp.empty:
        print("ВНИМАНИЕ: После разбиения SAP-кодов не осталось данных.")
        return ecp

    ecp['sap-code'] = to_numeric_safe_with_null(ecp['sap-code'])
    print(f"Размер ecp после to_numeric: {ecp.shape}")

    # Проверяем дубликаты до удаления
    initial_count = len(ecp)
    unique_count = ecp[['sap-code', 'viveska']].drop_duplicates().shape[0]
    print(f"Количество строк до drop_duplicates: {initial_count}, уникальных по ['sap-code', 'viveska']: {unique_count}")

    # Удаляем дубликаты
    ecp = ecp.drop_duplicates(subset=['sap-code', 'viveska'], keep='first')

    print(f"Размер ecp после drop_duplicates (итоговый ecp_map): {ecp.shape}")
    print(f"Примеры строк в ecp_map:\n{ecp.head(10)}")
    if len(ecp) > 10:
        print(f"... и последние строки:\n{ecp.tail(10)}")

    return ecp

# --- ОРИГИНАЛЬНАЯ ФУНКЦИЯ load_ecp_data ---
def load_ecp_data():
    numeric_text_cols = [
        'price', 'price_in', 'listing', 'listing2',
        'marketing', 'marketing2', 'promo', 'promo2',
        'retro', 'volnew', 'volFact', 'valFact', 'PromVol',
        'dopmarketing'
    ]
    dtype_dict = {col: str for col in numeric_text_cols}
    df = pd.read_excel(os.path.join(BASE_PATH, 'ECP_data.xlsx'), dtype=dtype_dict)

    if 'pdate' in df.columns:
        df['pdate'] = pd.to_datetime(df['pdate'], errors='coerce')
    if 'start_date' in df.columns:
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    if 'end_date' in df.columns:
        df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')

    for col in numeric_text_cols:
        if col in df.columns: # Проверяем, существует ли столбец перед преобразованием
            df[col] = to_numeric_safe_with_null(df[col])
        else:
            print(f"ПРЕДУПРЕЖДЕНИЕ: Столбец '{col}' не найден в ECP_data.xlsx.")

    normalizer = get_sku_normalizer_from_methodichka()
    if 'sku_type_sap' in df.columns:
        df['sku_type_sap'] = df['sku_type_sap'].apply(lambda x: normalize_sku(x, normalizer))

    return df

def load_sales(ecp_map):
    df = pd.read_excel(os.path.join(BASE_PATH, 'Sales.xlsx'))
    df['sales_date'] = pd.to_datetime(df['sales_date'], errors='coerce')
    df['zkcode'] = to_numeric_safe_with_null(df['zkcode'])
    df['vol_2'] = to_numeric_safe_with_null(df['vol_2'])
    df_agg = df.groupby(['zkcode', 'brand', 'sales_date'], as_index=False).agg({
        'vol_2': 'sum'
    })
    print(f"Размер df_sales_agg до merge: {df_agg.shape}")
    df_agg = df_agg.merge(ecp_map[['sap-code', 'viveska']], left_on='zkcode', right_on='sap-code', how='inner')
    print(f"Размер df_sales_agg после merge с ecp_map: {df_agg.shape}")

    normalizer = get_sku_normalizer_from_methodichka()
    df_agg['sku_type_sap'] = df_agg['brand'].apply(lambda x: normalize_sku(x, normalizer))

    df_agg.drop(columns=['sap-code'], inplace=True)
    df_agg['pdate'] = df_agg['sales_date']
    return df_agg

def load_cost_not_price(ecp_map):
    df = pd.read_excel(os.path.join(BASE_PATH, 'затраты_вне_цены.xlsx'))
    df['Месяц/год'] = pd.to_datetime(df['Месяц/год'], errors='coerce')
    df['Сумма'] = to_numeric_safe_with_null(df['Сумма'])
    df['Номер заказчика'] = to_numeric_safe_with_null(df['Номер заказчика'])

    normalizer = get_sku_normalizer_from_methodichka()
    df['sku_type_sap'] = df['Продукт'].apply(lambda x: normalize_sku(x, normalizer))
    df['pdate'] = df['Месяц/год']

    print(f"Размер df_cost_not_price до merge: {df.shape}")
    df = df.merge(ecp_map[['sap-code', 'viveska']], left_on='Номер заказчика', right_on='sap-code', how='left')
    print(f"Размер df_cost_not_price после merge с ecp_map: {df.shape}")
    df = df[df['Фонды'].str.contains('нет', case=False, na=False)]

    return df

def load_cost_in_price(ecp_map):
    df = pd.read_excel(os.path.join(BASE_PATH, 'затраты_в_цене.xlsx'))
    df['Месяц/год'] = pd.to_datetime(df['Месяц/год'], errors='coerce')
    df['Сумма в валюте документа'] = to_numeric_safe_with_null(df['Сумма в валюте документа'])
    df['Номер заказчика'] = to_numeric_safe_with_null(df['Номер заказчика'])

    normalizer = get_sku_normalizer_from_methodichka()
    df['sku_type_sap'] = df['Продукт'].apply(lambda x: normalize_sku(x, normalizer))
    df['pdate'] = df['Месяц/год']

    print(f"Размер df_cost_in_price до merge: {df.shape}")
    df = df.merge(ecp_map[['sap-code', 'viveska']], left_on='Номер заказчика', right_on='sap-code', how='left')
    print(f"Размер df_cost_in_price после merge с ecp_map: {df.shape}")

    return df

def load_cm():
    df = pd.read_excel(os.path.join(BASE_PATH, 'ЦМ.xlsx'))
    normalizer = get_sku_normalizer_from_methodichka()
    df['sku_type_sap'] = df['sku_type_sap'].apply(lambda x: normalize_sku(x, normalizer))
    df['ЦМ'] = to_numeric_safe_with_null(df['ЦМ'])
    return df

def load_cogs():
    df = pd.read_excel(os.path.join(BASE_PATH, 'себестоимость.xlsx'))
    normalizer = get_sku_normalizer_from_methodichka()
    df['sku_type_sap'] = df['sku_type_sap'].apply(lambda x: normalize_sku(x, normalizer))
    df['cogs'] = to_numeric_safe_with_null(df['cogs'])
    return df

# Функция load_fonds закомментирована, так как не используется
# def load_fonds(ecp_map):
#     df = pd.read_excel(os.path.join(BASE_PATH, 'фонды.xlsx'))
#     df['дата'] = pd.to_datetime(df['дата'], errors='coerce')
#     df['фонды'] = to_numeric_safe_with_null(df['фонды'])
#     df['номер заказчика'] = to_numeric_safe_with_null(df['номер заказчика'])
# 
#     normalizer = get_sku_normalizer_from_methodichka()
#     df['sku_type_sap'] = df['Бренд'].apply(lambda x: normalize_sku(x, normalizer))
#     df['pdate'] = df['дата']
# 
#     print(f"Размер df_fonds до merge: {df.shape}")
#     df = df.merge(ecp_map[['sap-code', 'viveska']], left_on='номер заказчика', right_on='sap-code', how='inner')
#     print(f"Размер df_fonds после merge с ecp_map: {df.shape}")
# 
#     return df

def calculate_pnl():
    print("1. Загрузка ECP_data.xlsx для получения справочника клиентов...")
    ecp_map = load_ecp_map()

    print("2. Загрузка Sales.xlsx...")
    df_sales = load_sales(ecp_map)

    print("3. Загрузка затрат вне цены...")
    df_cost_not_price = load_cost_not_price(ecp_map)

    print("4. Загрузка затрат в цене...")
    df_cost_in_price = load_cost_in_price(ecp_map)

    print("5. Загрузка ECP_data.xlsx...")
    df_ecp = load_ecp_data()

    print("6. Загрузка ЦМ.xlsx...")
    df_cm = load_cm()

    print("7. Формирование плана...")
    df_plan = df_ecp.groupby(
        ['FileName', 'viveska', 'gr_sb', 'sku_type_sap', 'pdate', 'start_date', 'end_date'],
        as_index=False
    )['volnew'].sum()
    df_plan.rename(columns={'volnew': 'Плановые продажи, шт'}, inplace=True)

    # --- УБРАН ФИЛЬТР: теперь df_plan содержит все строки из группировки ---
    print(f"Размер df_plan после группировки (все строки): {len(df_plan)}")
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    df_combined = pd.merge(df_plan, df_cm, on=['gr_sb', 'sku_type_sap', 'pdate'], how='left')
    df_combined.rename(columns={'ЦМ': 'price_in'}, inplace=True)
    df_combined['price_in'] = to_numeric_safe_with_null(df_combined['price_in'])
    df_combined['Плановые продажи, руб'] = df_combined['Плановые продажи, шт'] * df_combined['price_in']

    # --- ВОССТАНОВЛЕНО: 'dopmarketing' включён ---
    for col in ['listing2', 'listing', 'retro', 'dopmarketing', 'marketing', 'PromVol']:
        grouped = df_ecp.groupby(
            ['FileName', 'viveska', 'gr_sb', 'sku_type_sap', 'pdate', 'start_date', 'end_date'],
            as_index=False
        )[col].sum()
        df_combined = pd.merge(df_combined, grouped, on=[
            'FileName', 'viveska', 'gr_sb', 'sku_type_sap', 'pdate', 'start_date', 'end_date'
        ], how='left')
        df_combined[col] = to_numeric_safe_with_null(df_combined[col])

    df_combined['Плановые затраты «Скидка в цене», руб'] = df_combined['Плановые продажи, руб'] * df_combined['listing2']
    df_combined.rename(columns={'listing': 'Плановые затраты «Листинг/безусловные выплаты», руб'}, inplace=True)
    df_combined['Плановые затраты «Ретро», руб'] = (df_combined['Плановые продажи, руб'] / 1.2) * df_combined['retro']
    # Восстановлена логика с 'dopmarketing'
    df_combined['Плановые затраты «Маркетинг», руб'] = df_combined['Плановые продажи, руб'] * df_combined['dopmarketing'] + df_combined['marketing']
    df_combined.rename(columns={'PromVol': 'Плановые затраты «Промо-скидка», руб'}, inplace=True)
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    df_combined['контракт'] = np.where(
        pd.to_datetime(df_combined['pdate']) > pd.to_datetime(df_combined['end_date']),
        'завершенный',
        'действующий'
    )

    print("8. Присоединение фактических продаж...")
    df_sales_agg = df_sales.groupby(['viveska', 'sku_type_sap', 'pdate'], as_index=False)['vol_2'].sum()
    df_active = df_combined[df_combined['контракт'] == 'действующий']
    merged_active = pd.merge(df_active, df_sales_agg, on=['viveska', 'sku_type_sap', 'pdate'], how='left')
    df_combined = pd.concat([
        df_combined[df_combined['контракт'] != 'действующий'],
        merged_active
    ], ignore_index=True)
    df_combined.rename(columns={'vol_2': 'Факт продажи, шт.'}, inplace=True)
    df_combined['Факт продажи, шт.'] = to_numeric_safe_with_null(df_combined['Факт продажи, шт.'])
    df_combined['Факт продажи, руб (от ЦМ)'] = df_combined['Факт продажи, шт.'] * df_combined['price_in']
    df_combined['Разница, шт'] = df_combined['Факт продажи, шт.'] - df_combined['Плановые продажи, шт']
    df_combined['Разница, руб'] = df_combined['Факт продажи, руб (от ЦМ)'] - df_combined['Плановые продажи, руб']

    print("9. Присоединение фактических затрат...")
    for expense, col_name in [
        ('Листинг', 'Фактические затраты «Листинг/безусловные выплаты», руб'),
        ('Маркетинг', 'Фактические затраты «Маркетинг», руб'),
        ('Ретро', 'Фактические затраты «Ретро», руб')
    ]:
        filtered = df_cost_not_price[df_cost_not_price['Статья расходов'].str.contains(expense, case=False, na=False)]
        grouped = filtered.groupby(['pdate', 'viveska', 'sku_type_sap'], as_index=False)['Сумма'].sum()
        df_combined = pd.merge(df_combined, grouped, on=['viveska', 'sku_type_sap', 'pdate'], how='left')
        df_combined.rename(columns={'Сумма': col_name}, inplace=True)

    promo = df_cost_in_price[df_cost_in_price['примечание'].str.contains('промо акция', case=False, na=False)]
    promo_group = promo.groupby(['pdate', 'viveska', 'sku_type_sap'], as_index=False)['Сумма в валюте документа'].sum()
    df_combined = pd.merge(df_combined, promo_group, on=['viveska', 'sku_type_sap', 'pdate'], how='left')
    df_combined.rename(columns={'Сумма в валюте документа': 'Фактические затраты «Промо-скидка», руб'}, inplace=True)

    skidka = df_cost_in_price[df_cost_in_price['примечание'].str.contains('скидка в цене', case=False, na=False)]
    skidka_group = skidka.groupby(['pdate', 'viveska', 'sku_type_sap'], as_index=False)['Сумма в валюте документа'].sum()
    df_combined = pd.merge(df_combined, skidka_group, on=['viveska', 'sku_type_sap', 'pdate'], how='left')
    df_combined.rename(columns={'Сумма в валюте документа': 'Фактические затраты «Скидка в цене», руб'}, inplace=True)

    # --- УДАЛЕНО: Присоединение фондов (блок 10) ---
    # print("10. Присоединение фондов...")
    # df_fonds = load_fonds(ecp_map)
    # df_combined = pd.merge(df_combined, df_fonds, on=['viveska', 'sku_type_sap', 'pdate'], how='left')
    # cols_to_drop = ['Сеть', 'дебитор', 'ПФМ', 'присвоение', 'клиент', 'номер заказчика', 'sap-code']
    # df_combined.drop(columns=cols_to_drop, inplace=True, errors='ignore')
    # --- КОНЕЦ УДАЛЕНИЯ ---

    # Обработка столбцов фактических затрат (без фондов)
    fact_cols = [
        'Фактические затраты «Листинг/безусловные выплаты», руб',
        'Фактические затраты «Ретро», руб',
        'Фактические затраты «Маркетинг», руб',
        'Фактические затраты «Промо-скидка», руб',
        'Фактические затраты «Скидка в цене», руб'
    ]
    for col in fact_cols:
        if col not in df_combined.columns:
            df_combined[col] = 0.0
        else:
            df_combined[col] = to_numeric_safe_with_null(df_combined[col])

    # --- УДАЛЕНО: Добавление столбца 'фонды' ---
    # if 'фонды' not in df_combined.columns:
    #     df_combined['фонды'] = 0.0
    # else:
    #     df_combined['фонды'] = to_numeric_safe_with_null(df_combined['фонды'])
    # --- КОНЕЦ УДАЛЕНИЯ ---

    # --- ВОССТАНОВЛЕНО: 'dopmarketing' включён в расчёт ---
    df_combined['план затраты'] = (
        df_combined.get('Плановые затраты «Листинг/безусловные выплаты», руб', 0) +
        df_combined.get('Плановые затраты «Скидка в цене», руб', 0) +
        df_combined.get('Плановые затраты «Ретро», руб', 0) +
        df_combined.get('Плановые затраты «Маркетинг», руб', 0) + # Теперь использует 'dopmarketing' и 'marketing'
        df_combined.get('Плановые затраты «Промо-скидка», руб', 0)
    )
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    df_combined['факт затраты'] = (
        df_combined.get('Фактические затраты «Листинг/безусловные выплаты», руб', 0) +
        df_combined.get('Фактические затраты «Скидка в цене», руб', 0) +
        df_combined.get('Фактические затраты «Ретро», руб', 0) +
        df_combined.get('Фактические затраты «Маркетинг», руб', 0) +
        df_combined.get('Фактические затраты «Промо-скидка», руб', 0)
    )

    print("11. Присоединение себестоимости...")
    df_cogs = load_cogs()
    df_combined = pd.merge(df_combined, df_cogs, on=['sku_type_sap', 'pdate'], how='left')
    df_combined['cogs'] = to_numeric_safe_with_null(df_combined['cogs'])

    df_combined['продажи по сс план'] = df_combined['Плановые продажи, шт'] * df_combined['cogs']
    df_combined['продажи по сс факт'] = df_combined['Факт продажи, шт.'] * df_combined['cogs']

    df_combined['доход план'] = df_combined['Плановые продажи, руб'] - df_combined['продажи по сс план'] - df_combined['план затраты']
    df_combined['доход факт'] = df_combined['Факт продажи, руб (от ЦМ)'] - df_combined['продажи по сс факт'] - df_combined['факт затраты']

    df_combined = df_combined[df_combined['контракт'].str.contains('действующий', na=False)]

    print(f"До удаления полных дубликатов: {len(df_combined)} строк")
    df_combined = df_combined.drop_duplicates(keep='first')
    print(f"После удаления полных дубликатов: {len(df_combined)} строк")

    # --- ИСПРАВЛЕНО: Удаление дубликатов ТОЛЬКО по ключевым полям ---
    print(f"Перед удалением дубликатов по ключевым полям: {len(df_combined)} строк")

    key_columns = ['pdate', 'FileName', 'viveska', 'gr_sb', 'sku_type_sap', 'start_date', 'end_date']
    # Оставляем только первую строку для каждой уникальной комбинации ключевых полей
    df_combined = df_combined.drop_duplicates(subset=key_columns, keep='first')

    print(f"После удаления дубликатов по ключевым полям: {len(df_combined)} строк")
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    # --- УДАЛЕНО: столбец 'фонды' из итогового порядка ---
    new_order = [
        'pdate', 'FileName', 'viveska', 'gr_sb', 'sku_type_sap',
        'start_date', 'end_date', 'контракт',
        'Плановые продажи, шт', 'Факт продажи, шт.', 'Разница, шт',
        'Плановые продажи, руб', 'Факт продажи, руб (от ЦМ)', 'Разница, руб',
        'Плановые затраты «Листинг/безусловные выплаты», руб',
        'Фактические затраты «Листинг/безусловные выплаты», руб',
        'Плановые затраты «Скидка в цене», руб',
        'Фактические затраты «Скидка в цене», руб',
        'Плановые затраты «Ретро», руб',
        'Фактические затраты «Ретро», руб',
        'Плановые затраты «Маркетинг», руб', # Теперь правильно ссылается
        'Фактические затраты «Маркетинг», руб',
        'Плановые затраты «Промо-скидка», руб',
        'Фактические затраты «Промо-скидка», руб',
        # 'фонды',  # УДАЛЕНО
        'план затраты', 'факт затраты',
        'продажи по сс план', 'продажи по сс факт',
        'доход план', 'доход факт'
    ]
    # --- КОНЕЦ УДАЛЕНИЯ ---
    
    new_order = [col for col in new_order if col in df_combined.columns]
    df_combined = df_combined[new_order]

    output_path = os.path.join(BASE_PATH, 'PnL_результат.xlsx') # Возвращено оригинальное имя
    save_to_excel_with_chunks(df_combined, output_path)

if __name__ == "__main__":
    calculate_pnl()