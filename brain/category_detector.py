"""
category_detector.py — Определение категории и подкатегории товара.

Три отдельных детектора: энергетики, газировка, прочее.
"""

import re
from brain.normalizer import normalize_upper


_ENERGY_STRONG = [
    'ЭНЕРГЕТИК', 'ЭНЕРГЕТИЧЕСК', 'ENERGY',
    'ТОНИЗИРУЮЩ', 'ТОНИЗ',
    'ЭНЕРГ НАП', 'ЭНЕРГЕТ НАП',
]
_ENERGY_MEDIUM = [
    'ЭНЕРГ', 'POWER',
    'BOOST', 'CAFFEIN', 'КОФЕИН',
    'ТАУРИНПЛ', 'ТАУРИНСОД', 'ТАУРИН',
]
_ENERGY_BRANDS = [
    'TORNADO', 'ТОРНАДО', 'E-ON', 'ЕОН', 'EON',
    'MONSTER', 'МОНСТЕР', 'BURN', 'БЕРН',
    'RED BULL', 'РЕД БУЛЛ', 'ADRENALINE', 'АДРЕНАЛИН',
    'FLASH UP', 'ФЛЕШ АП', 'GORILLA', 'ГОРИЛЛА',
    'DRIVE ME', 'ДРАЙВ', 'LIT ENERGY', 'ЛИТ ЭНЕРГ',
    'GENESIS', 'ГЕНЕЗИС', 'ROCKSTAR', 'РОКСТАР',
    'BULLIT', 'БУЛЛИТ', 'BIZON', 'БИЗОН',
    'DYNAMIT', 'BLACK MONSTER', 'BLACK ENERGY',
    'EFFECT', 'ENERGO', 'ENERGY CODE', 'ENERGY DRAGON',
    'FIZRUK', 'ФИЗРУК', 'G-DRIVE', 'VULKAN', 'ВУЛКАН',
    'JAGUAR', 'ЯГУАР', 'ACTIBO', 'HELL',
    'POWERCELL', 'POWER TORR', 'PULSE UP',
    'SHARK', 'STORM', 'VOLT',
]

_SODA_STRONG = [
    'ЛИМОНАД', 'ГАЗИРОВКА', 'ГАЗИРОВАННЫЙ',
    'ДЮШЕС', 'ТАРХУН',
]
_SODA_MEDIUM = [
    'ГАЗ', 'ГАЗИР', 'СИЛЬНОГАЗ', 'СИЛ/ГАЗ', 'СИЛГАЗ',
    'МОХИТО', 'MOJITO', 'БАЙКАЛ', 'BAIKAL',
]
_SODA_BRANDS = [
    'COCA-COLA', 'КОКА-КОЛА', 'КОКА КОЛА',
    'PEPSI', 'ПЕПСИ', 'SPRITE', 'СПРАЙТ',
    'FANTA', 'ФАНТА', 'MIRINDA', 'МИРИНДА',
    'FRESH BAR', 'ФРЕШ БАР',
    'COLA BY FRESH BAR', 'COLA CLASSIC', 'COOL COLA',
    'FANTOLA', 'ФАНТОЛА', 'ROYAL COLA',
    'LAIMON FRESH', 'ЛАЙМОН ФРЕШ',
    'SCHWEPPES', 'EVERVESS', '7 UP', 'MOUNTAIN DEW',
    'ЧЕРНОГОЛОВКА', 'ИЛЬИНСКИЕ ЛИМОНАДЫ',
    'CHILLOUT', 'CHUPA CHUPS', 'ЧУПА ЧУПС',
    'IRN BRU', 'ИСТОЧНИК',
]


def detect_category(text: str, brand2: str = '') -> str:
    """Определяет категорию: ЭНЕРГЕТИКИ, ГАЗИРОВКА или ПРОЧЕЕ."""
    s = normalize_upper(text)
    brand_upper = normalize_upper(brand2)

    for kw in _ENERGY_STRONG:
        if kw in s:
            return 'ЭНЕРГЕТИКИ'

    for brand_kw in _ENERGY_BRANDS:
        if brand_kw in s or brand_kw in brand_upper:
            return 'ЭНЕРГЕТИКИ'

    for kw in _ENERGY_MEDIUM:
        if re.search(r'\b' + re.escape(kw) + r'\b', s):
            return 'ЭНЕРГЕТИКИ'

    for brand_kw in _SODA_BRANDS:
        if brand_kw in s or brand_kw in brand_upper:
            return 'ГАЗИРОВКА'

    for kw in _SODA_STRONG:
        if kw in s:
            return 'ГАЗИРОВКА'

    for kw in _SODA_MEDIUM:
        if kw in s:
            return 'ГАЗИРОВКА'

    return 'ПРОЧЕЕ'


def detect_subcategory(text: str, category: str) -> str:
    """Определяет подкатегорию по тексту и категории."""
    s = normalize_upper(text)

    if category == 'ЭНЕРГЕТИКИ':
        if any(kw in s for kw in ['Ж/Б', 'CAN', 'ЖБ', 'БАНК', 'ЖЕСТЯН']):
            return 'Энергетические напитки ж/б'
        if any(kw in s for kw in ['ПЭТ', 'PET', 'ПЛ/БУТ', 'ПЛАСТИК']):
            return 'Энергетические напитки ПЭТ'
        if any(kw in s for kw in ['СТЕКЛ', 'СТ/Б']):
            return 'Энергетические напитки стекло'
        return 'Энергетические напитки'

    if category == 'ГАЗИРОВКА':
        if any(kw in s for kw in ['КОЛА', 'COLA']):
            if any(kw in s for kw in ['ЗЕРО', 'ZERO', 'БЕЗ САХАР', 'ЛАЙТ', 'LIGHT', 'ДИЕТ']):
                return 'Кола без сахара'
            return 'Кола'
        if 'ДЮШЕС' in s:
            return 'Лимонады дюшес'
        if 'ТАРХУН' in s:
            return 'Лимонады тархун'
        if 'МОХИТО' in s or 'MOJITO' in s:
            return 'Мохито'
        if 'БАЙКАЛ' in s or 'BAIKAL' in s:
            return 'Лимонады Байкал'
        if any(kw in s for kw in ['ЛИМОНАД', 'ЛАЙМ', 'LIME']):
            return 'Лимонады'
        if any(kw in s for kw in ['АПЕЛЬСИН', 'ORANGE', 'ОРАНЖ']):
            return 'Газированные с апельсином'
        if any(kw in s for kw in ['ЛИМОН', 'LEMON']):
            return 'Газированные с лимоном'
        return 'Газированные напитки'

    if any(kw in s for kw in ['СОК', 'НЕКТАР', 'JUICE']):
        return 'Соки и нектары'
    if any(kw in s for kw in ['ВОДА', 'WATER', 'МИНЕРАЛ']):
        return 'Вода'
    if any(kw in s for kw in ['КВАС', 'KVASS']):
        return 'Квас'
    if any(kw in s for kw in ['ЧАЙ', 'TEA']):
        return 'Холодный чай'
    if any(kw in s for kw in ['МОРС']):
        return 'Морс'

    return 'Прочее'


def detect_format(text: str) -> str:
    """Определяет формат упаковки: PET, CAN, GLASS."""
    s = normalize_upper(text)
    if any(kw in s for kw in ['ПЭТ', 'PET', 'ПЛ/БУТ', 'ПЛАСТИК']):
        return 'PET'
    if any(kw in s for kw in ['Ж/Б', 'CAN', 'ЖБ', 'БАНК', 'ЖЕСТЯН']):
        return 'CAN'
    if any(kw in s for kw in ['СТЕКЛ', 'СТ/Б', 'GLASS']):
        return 'GLASS'
    return ''
