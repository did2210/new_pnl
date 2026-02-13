"""
Конфигурация приложения для обработки контрактов GFD.
"""
import os

# Базовая директория приложения
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Директории ввода/вывода
INPUT_DIR = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
TEMP_DIR = os.path.join(BASE_DIR, 'temp')

# Создаём директории при импорте, если не существуют
for d in [INPUT_DIR, OUTPUT_DIR, TEMP_DIR]:
    os.makedirs(d, exist_ok=True)

# Поддерживаемые расширения файлов
SUPPORTED_EXTENSIONS = ('.xlsx', '.xls', '.xlsm')

# Названия листов после обработки (шаг 1)
SHEET_GFD = "GFD Запрос"
SHEET_CONTRACT = "Условия контракта"
SHEET_SALES_PLAN = "Планирование продаж"
SHEET_INVESTMENT_PLAN = "Планирование инвестиций"
SHEET_SAP = "SAP-код"
SHEET_SUMMARY = "Сводка"

# Целевые листы для поиска данных планирования (из исходных файлов)
TARGET_PLANNING_SHEETS = [
    "NEW CNR 1", "Расчет инвестиций", "NEW CNR", "Расчет инвестиций (2)"
]

# Маппинг SKU
SKU_MAPPING = {
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

SKU_MAPPING_REVERSE = {v: k for k, v in SKU_MAPPING.items()}

# Названия месяцев на русском
MONTH_NAMES_RU = [
    'январь', 'февраль', 'март', 'апрель', 'май', 'июнь',
    'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь'
]

# Итоговый порядок колонок
FINAL_COLUMNS_ORDER = [
    'FileName', 'filial', 'forma', 'gr_sb', 'kam', 'viveska', 'client_type', 'sap-code',
    'start_date', 'end_date', 'pdate', 'HideStatus', 'sku_type', 'sku_type_sap',
    'price', 'price_in', 'listing', 'listing2', 'marketing', 'marketing2', 'promo', 'promo2',
    'retro', 'volnew', 'PromVol', 'dopmarketing', 'sku', 'tt'
]

# Настройки логирования
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FILE = os.path.join(BASE_DIR, 'processing.log')
