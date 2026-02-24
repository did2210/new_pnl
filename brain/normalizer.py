"""
normalizer.py — Продвинутая нормализация текста.

Транслитерация, извлечение литража, очистка шума из xname.
"""

import re
from typing import Optional

# ============================================================
# Транслитерация RU <-> EN (побуквенная)
# ============================================================
_TRANSLIT_RU_EN = {
    'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D',
    'Е': 'E', 'Ё': 'E', 'Ж': 'ZH', 'З': 'Z', 'И': 'I',
    'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M', 'Н': 'N',
    'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T',
    'У': 'U', 'Ф': 'F', 'Х': 'KH', 'Ц': 'TS', 'Ч': 'CH',
    'Ш': 'SH', 'Щ': 'SHCH', 'Ъ': '', 'Ы': 'Y', 'Ь': '',
    'Э': 'E', 'Ю': 'YU', 'Я': 'YA',
}

_TRANSLIT_EN_RU = {}
for ru, en in _TRANSLIT_RU_EN.items():
    if en and en not in _TRANSLIT_EN_RU:
        _TRANSLIT_EN_RU[en] = ru

# Слова, которые часто пишут по-разному
_WORD_TRANSLIT = {
    'ЭНЕРДЖИ': 'ENERGY', 'ЭНЕРГИ': 'ENERGY', 'ЭНЕРГ': 'ENERG',
    'ЭНЕРГЕТ': 'ENERGET', 'ЭНЕРГЕТИК': 'ENERGETIC',
    'ТОРНАДО': 'TORNADO', 'МОНСТЕР': 'MONSTER', 'МОНСТР': 'MONSTER',
    'СПРАЙТ': 'SPRITE', 'ФАНТА': 'FANTA', 'ПЕПСИ': 'PEPSI',
    'КОКА': 'COCA', 'КОЛА': 'COLA', 'БЕРН': 'BURN',
    'ФЛЕШ': 'FLASH', 'ГОРИЛЛА': 'GORILLA', 'ГЕНЕЗИС': 'GENESIS',
    'ДРАЙВ': 'DRIVE', 'РОКЕТ': 'ROCKET', 'РОКСТАР': 'ROCKSTAR',
    'БУЛЛИТ': 'BULLIT', 'БИЗОН': 'BIZON', 'БУЛЛ': 'BULL',
    'РЕД': 'RED', 'БЛЭК': 'BLACK', 'ЧЕРН': 'BLACK',
    'ШТОРМ': 'STORM', 'АКТИВ': 'ACTIVE', 'МАКС': 'MAX',
    'СКИЛЛ': 'SKILL', 'БАТЛ': 'BATTLE', 'БАТТЛ': 'BATTLE',
    'ПАНЧ': 'PUNCH', 'КРАШ': 'CRUSH', 'РАШ': 'RUSH',
    'КОФЕ': 'COFFEE', 'КОФФ': 'COFF', 'ВИТАМИН': 'VITAMIN',
    'ЦИТРУС': 'CITRUS', 'ТРОПИК': 'TROPIC', 'ТРОПИЧ': 'TROPIC',
    'ИМБИР': 'GINGER', 'МИНДАЛ': 'ALMOND',
    'ЛИТ': 'LIT', 'ЛИТР': 'LITR',
    'ФРЕШ': 'FRESH', 'БАР': 'BAR',
    'МОХИТО': 'MOJITO', 'ПИНАКОЛАД': 'PINA COLAD',
    'ЛИМОНАД': 'LIMONAD', 'ДЮШЕС': 'DUSHES', 'ТАРХУН': 'TARKHUN',
    'БАЙКАЛ': 'BAIKAL', 'ЛАЙМ': 'LIME', 'ЛАЙМОН': 'LAIMON',
    'АПЕЛЬСИН': 'ORANGE', 'ОРАНЖ': 'ORANGE',
    'ЛИМОН': 'LEMON', 'ВИШНЯ': 'CHERRY', 'КЛУБНИК': 'STRAWBERRY',
    'ИЛЬИНСК': 'ILIINSK',
    'ЯГУАР': 'JAGUAR',
    'ЧУПА': 'CHUPA', 'ЧУПС': 'CHUPS',
    'ФИЗРУК': 'FIZRUK',
    'КРЫМ': 'KRYM',
    'АШКУДИ': 'HQD', 'АШКДИ': 'HQD',
    'ЕОН': 'E-ON', 'Е-ОН': 'E-ON', 'И-ОН': 'E-ON',
    'ГЛАДИО': 'GLADIO', 'ГРУТ': 'GROOT',
    'БАЗЗ': 'BUZZ', 'ФРУТИНГ': 'FRUITING',
    'МОНСТЕР': 'MONSTER', 'МОНСТР': 'MONSTER',
    'ФЛЭШ': 'FLASH', 'ФЛЕШ': 'FLASH', 'АП': 'UP',
    'МИ': 'ME', 'БАР': 'BAR',
    'БУСТ': 'BOOST', 'РОКЕТ': 'ROCKET',
    'ГОРИЛЛ': 'GORILL', 'ГЕНЕЗ': 'GENEZ',
    'БИТТЕР': 'BITTER', 'БЛЕК': 'BLACK',
}

_BRAND_MULTIWORD_TRANSLIT = {
    'ТОРНАДО ЭНЕРДЖИ': 'TORNADO ENERGY',
    'ТОРНАДО ЭНЕРГ': 'TORNADO ENERGY',
    'ТОРНАДО': 'TORNADO',
    'МОНСТЕР ЭНЕРДЖИ': 'MONSTER ENERGY',
    'МОНСТЕР ЭНЕРГ': 'MONSTER ENERGY',
    'МОНСТЕР': 'MONSTER',
    'КОКА КОЛА': 'COCA-COLA',
    'КОКА-КОЛА': 'COCA-COLA',
    'ЛИТ ЭНЕРДЖИ': 'LIT ENERGY',
    'ЛИТ ЭНЕРГ': 'LIT ENERGY',
    'ФРЕШ БАР': 'FRESH BAR',
    'ФЛЕШ АП': 'FLASH UP',
    'ФЛЭШ АП': 'FLASH UP',
    'ДРАЙВ МИ': 'DRIVE ME',
    'РЕД БУЛЛ': 'RED BULL',
    'РЕД БУЛ': 'RED BULL',
    'БЛЭК МОНСТЕР': 'BLACK MONSTER',
    'БЕРН': 'BURN',
    'СПРАЙТ': 'SPRITE',
    'ФАНТА': 'FANTA',
    'ПЕПСИ': 'PEPSI',
    'ГОРИЛЛА': 'GORILLA',
    'ФИЗРУК': 'FIZRUK',
    'ЯГУАР': 'JAGUAR',
    'ГЕНЕЗИС': 'GENESIS',
    'Е-ОН': 'E-ON',
    'ЕОН': 'E-ON',
    'И-ОН': 'E-ON',
}

# Обратная карта EN -> RU
_WORD_TRANSLIT_REVERSE = {v: k for k, v in _WORD_TRANSLIT.items()}

# Шум в xname: служебные слова, обозначения упаковки, маркировки
_NOISE_WORDS = [
    'НАПИТОК', 'НАПИТ', 'НАП', 'ЭНЕРГЕТИЧЕСКИЙ', 'ЭНЕРГЕТИК',
    'БЕЗАЛКОГОЛЬНЫЙ', 'БЕЗАЛК', 'Б/А', 'Б.А.',
    'СИЛЬНОГАЗ', 'СИЛГАЗ', 'СИЛ/ГАЗ', 'СИЛЬНО/ГАЗ',
    'НЕГАЗ', 'ГАЗИРОВАННЫЙ', 'ГАЗИРОВАНН', 'ГАЗИР',
    'ТОНИЗИРУЮЩИЙ', 'ТОНИЗИР', 'ТОНИЗ',
    'КОКТЕЙЛЬ', 'СОКОСОДЕРЖАЩИЙ', 'СОКОСОД', 'С/СОД',
    'С МЯК', 'ОСВ', 'ОСВЕТЛ', 'ОСВЕТЛЕННЫЙ',
    'ПЛ/БУТ', 'ПЭТ', 'PET', 'Ж/Б', 'СТ/Б', 'CAN',
    'С КЛ', 'С КЛЮЧ', 'ПЛАСТИК', 'СТЕКЛ', 'ЖЕСТЯН', 'БАНК',
    'ОАО', 'ООО', 'ЗАО', 'АО', 'ТД', 'ПАО',
    'ТМ', 'ТОВ',
]

# Шум + regex паттерны
_NOISE_PATTERNS = [
    r':\d+\s*$',           # :12 в конце
    r'\([^)]*\)',           # (Росинка), (Дал), (ООО Сила)
    r'\d{5,}',             # числовые коды типа 00871400
    r'[.,]\d+\s*$',        # .12 в конце
]


def normalize_upper(text: str) -> str:
    """Базовая нормализация: upper + пробелы + ё."""
    if not text:
        return ''
    s = str(text).strip().upper()
    s = s.replace('Ё', 'Е')
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def transliterate_ru_to_en(text: str) -> str:
    """Транслитерация русских слов в английские по словарю."""
    result = text
    for ru_phrase, en_phrase in sorted(_BRAND_MULTIWORD_TRANSLIT.items(), key=lambda x: -len(x[0])):
        if ru_phrase in result:
            result = result.replace(ru_phrase, en_phrase)

    words = result.split()
    out = []
    for word in words:
        for ru_part, en_part in sorted(_WORD_TRANSLIT.items(), key=lambda x: -len(x[0])):
            if ru_part in word:
                word = word.replace(ru_part, en_part)
        out.append(word)
    return ' '.join(out)


def transliterate_en_to_ru(text: str) -> str:
    """Транслитерация английских слов в русские по словарю."""
    words = text.split()
    result = []
    for word in words:
        for en_part, ru_part in sorted(_WORD_TRANSLIT_REVERSE.items(), key=lambda x: -len(x[0])):
            if en_part in word:
                word = word.replace(en_part, ru_part)
        result.append(word)
    return ' '.join(result)


def extract_litrag(text: str) -> Optional[float]:
    """Извлекает литраж из текста. Поддерживает множество форматов."""
    s = normalize_upper(text)

    word_volumes = {
        'ПОЛ ЛИТРА': 0.5, 'ПОЛЛИТРА': 0.5, 'ПОЛУЛИТРА': 0.5, 'ПОЛУЛИТР': 0.5,
        'ПОЛТОРА': 1.5, 'ПОЛТОРА ЛИТРА': 1.5,
        'ЛИТР': 1.0, 'ЛИТРА': 1.0, 'ЛИТРОВ': 1.0,
    }
    for phrase, vol in word_volumes.items():
        if phrase in s:
            prefix = s[:s.index(phrase)].strip()
            m = re.search(r'(\d+[.,]?\d*)\s*$', prefix)
            if m:
                return float(m.group(1).replace(',', '.'))
            return vol

    m = re.search(r'(\d+[.,]\d+)\s*Л(?:\b|[^А-ЯA-Z])', s)
    if m:
        return float(m.group(1).replace(',', '.'))

    m = re.search(r'(\d+)\s*Л(?:\b|[^А-ЯA-Z])', s)
    if m:
        return float(m.group(1))

    m = re.search(r'(\d{3,4})\s*МЛ', s)
    if m:
        return float(m.group(1)) / 1000.0

    m = re.search(r'(\d+[.,]\d+)\s*МЛ', s)
    if m:
        return float(m.group(1).replace(',', '.')) / 1000.0

    m = re.search(r'(\d+[.,]\d+)', s)
    if m:
        val = float(m.group(1).replace(',', '.'))
        if 0.1 <= val <= 10.0:
            return val

    return None


def clean_xname(text: str) -> str:
    """Убирает весь шум из xname, оставляя только значимые слова."""
    s = normalize_upper(text)
    for pat in _NOISE_PATTERNS:
        s = re.sub(pat, '', s)
    s = re.sub(r'\d+[.,]?\d*\s*(Л|МЛ)\b', '', s)
    for noise in sorted(_NOISE_WORDS, key=len, reverse=True):
        s = re.sub(r'\b' + re.escape(noise) + r'\b', '', s)
    s = re.sub(r'[^А-ЯA-Z0-9\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def make_search_key(text: str) -> str:
    """Создаёт максимально очищенный ключ для поиска."""
    cleaned = clean_xname(text)
    en_version = transliterate_ru_to_en(cleaned)
    combined = f"{cleaned} {en_version}"
    combined = re.sub(r'\s+', ' ', combined)
    return combined.strip()


def extract_brand_candidates(text: str) -> list[str]:
    """Извлекает возможные варианты имени бренда из xname."""
    s = normalize_upper(text)
    s = re.sub(r'\([^)]*\)', '', s)
    s = re.sub(r':\d+\s*$', '', s)

    words = s.split()
    candidates = []
    for noise in _NOISE_WORDS:
        if noise in words:
            words = [w for w in words if w != noise]

    if not words:
        return []

    candidates.append(words[0])
    if len(words) >= 2:
        candidates.append(f"{words[0]} {words[1]}")
    if len(words) >= 3:
        candidates.append(f"{words[0]} {words[1]} {words[2]}")

    en_candidates = []
    for c in candidates:
        en = transliterate_ru_to_en(c)
        if en != c:
            en_candidates.append(en)

    return candidates + en_candidates
