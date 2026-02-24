"""
product_brain.py — «Мозг» для расшифровки товарных наименований.

Строит справочную базу из product.xlsx:
  - Таблица эталонов (canonical): уникальные комбинации brand2 + proizvod2 + litrag + category
  - Таблица алиасов (aliases): каждое xname привязано к эталону

Поиск работает без нейронок:
  1. Точное совпадение по нормализованному xname
  2. Нечёткий поиск (fuzzy matching) через rapidfuzz — расстояние Левенштейна
  3. Нераспознанные имена логируются для ручной доработки
"""

import pandas as pd
import os
import re
import logging
from dataclasses import dataclass
from typing import Optional

try:
    from rapidfuzz import fuzz, process as rf_process
    HAS_FUZZY = True
except ImportError:
    HAS_FUZZY = False
    print("ВНИМАНИЕ: rapidfuzz не установлен. Нечёткий поиск недоступен.")
    print("Установите: pip install rapidfuzz")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('product_brain.log', mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BRAIN_FILE = 'product_brain.xlsx'
SOURCE_FILE = 'product.xlsx'
UNRECOGNIZED_FILE = 'unrecognized_xnames.xlsx'


def normalize_text(text: str) -> str:
    """Приводит текст к единому виду для сравнения."""
    if not text or pd.isna(text):
        return ''
    s = str(text).strip()
    s = s.upper()
    s = re.sub(r'\s+', ' ', s)
    s = s.replace('Ё', 'Е')
    s = re.sub(r'[^\w\s.,/]', '', s)
    return s.strip()


def normalize_xname_key(xname: str) -> str:
    """Более агрессивная нормализация для ключа поиска."""
    s = normalize_text(xname)
    s = re.sub(r':\d+$', '', s)
    s = re.sub(r'\(.*?\)', '', s)
    s = re.sub(r'\d+[.,]?\d*\s*Л', '', s)
    s = re.sub(r'Б/А|Б\.А\.|БЕЗАЛК|СИЛЬНОГАЗ|СИЛГАЗ|СИЛ/ГАЗ|НЕГАЗ|ГАЗ|ПЛ/БУТ|ПЭТ|Ж/Б|СТ/Б', '', s)
    s = re.sub(r'НАПИТОК|НАПИТ|НАП|ЭНЕРГ|ЭНЕРГЕТ|КОКТЕЙЛЬ', '', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


@dataclass
class CanonicalProduct:
    """Эталонная запись продукта."""
    canonical_id: int
    brand2: str
    proizvod2: str
    litrag: float
    category: str


@dataclass
class LookupResult:
    """Результат поиска в мозге."""
    found: bool
    method: str  # 'exact', 'normalized', 'fuzzy', 'parsed', 'not_found'
    confidence: float
    canonical_id: Optional[int]
    brand2: str
    proizvod2: str
    litrag: float
    category: str
    matched_alias: str


# Ключевые слова для автоопределения категории из xname
_ENERGY_KEYWORDS = [
    'ЭНЕРГ', 'ENERGY', 'ЭНЕРГЕТИК', 'ЭНЕРГЕТ', 'ТОНИЗИР', 'ТОНИЗ',
    'POWER', 'BOOST', 'CAFFEIN', 'КОФЕИН', 'ТАУРИНПЛ', 'ТАУРИНСОД',
]
_SODA_KEYWORDS = [
    'ГАЗ', 'ГАЗИР', 'ЛИМОНАД', 'ДЮШЕС', 'ТАРХУН', 'БАЙКАЛ', 'КОЛА',
    'COLA', 'МОХИТО', 'MOJITO', 'SPRITE', 'FANTA', 'PEPSI',
    'СИЛЬНОГАЗ', 'СИЛ/ГАЗ', 'СИЛГАЗ',
]


def parse_xname_fields(xname: str) -> dict:
    """
    Разбирает совершенно неизвестный xname по правилам:
      - litrag: ищет паттерн вроде '0,5Л', '1.5л', '473мл'
      - category: определяет по ключевым словам
      - brand2: берёт первое слово/группу слов до служебной части
      - proizvod2: пытается вытащить производителя из скобок
    """
    s = str(xname).strip()
    s_upper = s.upper()

    litrag = 0.0
    m = re.search(r'(\d+[.,]?\d*)\s*Л', s_upper)
    if m:
        litrag = float(m.group(1).replace(',', '.'))
    else:
        m = re.search(r'(\d+)\s*МЛ', s_upper)
        if m:
            litrag = float(m.group(1)) / 1000.0

    category = 'ПРОЧЕЕ'
    for kw in _ENERGY_KEYWORDS:
        if kw in s_upper:
            category = 'ЭНЕРГЕТИКИ'
            break
    if category == 'ПРОЧЕЕ':
        for kw in _SODA_KEYWORDS:
            if kw in s_upper:
                category = 'ГАЗИРОВКА'
                break

    proizvod2 = ''
    m_prod = re.search(r'\(([^)]+)\)', s)
    if m_prod:
        proizvod2 = m_prod.group(1).strip().upper()

    clean = re.sub(r'\(.*?\)', '', s_upper)
    clean = re.sub(r':\d+\s*$', '', clean)
    clean = re.sub(r'\d+[.,]?\d*\s*(Л|МЛ|ПЭТ|PET|CAN|Ж/Б|СТ/Б|ПЛ/БУТ)', '', clean)
    noise = [
        'НАПИТОК', 'НАПИТ', 'НАП', 'ЭНЕРГЕТИЧЕСКИЙ', 'ЭНЕРГЕТИК', 'ЭНЕРГЕТ', 'ЭНЕРГ',
        'Б/А', 'БЕЗАЛК', 'БЕЗАЛКОГОЛЬНЫЙ', 'СИЛЬНОГАЗ', 'СИЛГАЗ', 'СИЛ/ГАЗ',
        'НЕГАЗ', 'ГАЗИРОВАННЫЙ', 'ГАЗИР', 'ГАЗ', 'ТОНИЗИРУЮЩИЙ', 'ТОНИЗ',
        'КОКТЕЙЛЬ', 'СОКОСОДЕРЖАЩИЙ', 'С МЯК', 'ОСВ', 'ОСВЕТЛ',
    ]
    for word in noise:
        clean = clean.replace(word, '')
    clean = re.sub(r'\s+', ' ', clean).strip()

    brand2 = clean.split()[0] if clean.split() else ''

    return {
        'brand2': brand2,
        'proizvod2': proizvod2 if proizvod2 else 'UNKNOWN',
        'litrag': round(litrag, 3),
        'category': category,
    }


class ProductBrain:
    """Справочная база для расшифровки товарных наименований."""

    def __init__(self):
        self.canonicals: dict[int, CanonicalProduct] = {}
        self.aliases_exact: dict[str, int] = {}       # xname -> canonical_id
        self.aliases_normalized: dict[str, int] = {}   # normalized xname -> canonical_id
        self.aliases_key: dict[str, int] = {}          # aggressive normalized -> canonical_id
        self.all_alias_keys: list[str] = []
        self.unrecognized: list[dict] = []
        self._fuzzy_threshold = 80

    def build_from_product_xlsx(self, file_path: str = SOURCE_FILE):
        """Строит мозг из product.xlsx."""
        logger.info(f"Загрузка {file_path}...")
        df = pd.read_excel(file_path)
        logger.info(f"Загружено {len(df)} записей")

        required = ['xname', 'brand2', 'proizvod2', 'litrag', 'category']
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Отсутствуют столбцы: {missing}")

        df_work = df[required].copy()
        df_work['brand2'] = df_work['brand2'].astype(str).str.strip().str.upper()
        df_work['proizvod2'] = df_work['proizvod2'].astype(str).str.strip().str.upper()
        df_work['category'] = df_work['category'].astype(str).str.strip().str.upper()
        df_work['litrag'] = pd.to_numeric(df_work['litrag'], errors='coerce').fillna(0.0)

        canonical_groups = df_work.groupby(
            ['brand2', 'proizvod2', 'litrag', 'category']
        ).agg(alias_count=('xname', 'count')).reset_index()

        canonical_groups = canonical_groups.sort_values(
            'alias_count', ascending=False
        ).reset_index(drop=True)

        self.canonicals = {}
        canonical_key_to_id: dict[tuple, int] = {}

        for idx, row in canonical_groups.iterrows():
            cid = idx + 1
            key = (row['brand2'], row['proizvod2'], row['litrag'], row['category'])
            self.canonicals[cid] = CanonicalProduct(
                canonical_id=cid,
                brand2=row['brand2'],
                proizvod2=row['proizvod2'],
                litrag=row['litrag'],
                category=row['category']
            )
            canonical_key_to_id[key] = cid

        logger.info(f"Создано {len(self.canonicals)} эталонных продуктов")

        self.aliases_exact = {}
        self.aliases_normalized = {}
        self.aliases_key = {}

        local_aliases_exact: dict[str, int] = {}
        local_aliases_norm: dict[str, int] = {}
        local_aliases_key: dict[str, int] = {}

        for _, row in df_work.iterrows():
            xname = str(row['xname']).strip()
            brand2 = row['brand2']
            proizvod2 = row['proizvod2']
            litrag = row['litrag']
            category = row['category']

            key = (brand2, proizvod2, litrag, category)
            cid = canonical_key_to_id.get(key)
            if cid is None:
                continue

            is_local = (brand2 == 'LOCAL')

            xname_upper = xname.upper()
            xname_norm = normalize_text(xname)
            xname_key = normalize_xname_key(xname)

            if is_local:
                if xname_upper not in self.aliases_exact:
                    local_aliases_exact[xname_upper] = cid
                if xname_norm and xname_norm not in self.aliases_normalized:
                    local_aliases_norm[xname_norm] = cid
                if xname_key and xname_key not in self.aliases_key:
                    local_aliases_key[xname_key] = cid
            else:
                self.aliases_exact[xname_upper] = cid
                if xname_norm:
                    self.aliases_normalized[xname_norm] = cid
                if xname_key:
                    self.aliases_key[xname_key] = cid

        for k, v in local_aliases_exact.items():
            if k not in self.aliases_exact:
                self.aliases_exact[k] = v
        for k, v in local_aliases_norm.items():
            if k not in self.aliases_normalized:
                self.aliases_normalized[k] = v
        for k, v in local_aliases_key.items():
            if k not in self.aliases_key:
                self.aliases_key[k] = v

        self.all_alias_keys = list(self.aliases_key.keys())

        logger.info(f"Индексировано алиасов: exact={len(self.aliases_exact)}, "
                     f"normalized={len(self.aliases_normalized)}, "
                     f"key={len(self.aliases_key)}")

    def lookup(self, xname: str) -> LookupResult:
        """Ищет xname в базе. Возвращает LookupResult."""
        if not xname or pd.isna(xname):
            return LookupResult(False, 'not_found', 0.0, None, '', '', 0.0, '', '')

        xname_str = str(xname).strip()

        xname_upper = xname_str.upper()
        cid = self.aliases_exact.get(xname_upper)
        if cid is not None:
            cp = self.canonicals[cid]
            return LookupResult(True, 'exact', 100.0, cid,
                                cp.brand2, cp.proizvod2, cp.litrag, cp.category, xname_str)

        xname_norm = normalize_text(xname_str)
        cid = self.aliases_normalized.get(xname_norm)
        if cid is not None:
            cp = self.canonicals[cid]
            return LookupResult(True, 'normalized', 95.0, cid,
                                cp.brand2, cp.proizvod2, cp.litrag, cp.category, xname_str)

        xname_key = normalize_xname_key(xname_str)
        cid = self.aliases_key.get(xname_key)
        if cid is not None:
            cp = self.canonicals[cid]
            return LookupResult(True, 'normalized', 90.0, cid,
                                cp.brand2, cp.proizvod2, cp.litrag, cp.category, xname_str)

        if HAS_FUZZY and self.all_alias_keys and xname_key:
            match = rf_process.extractOne(
                xname_key,
                self.all_alias_keys,
                scorer=fuzz.token_set_ratio,
                score_cutoff=self._fuzzy_threshold
            )
            if match:
                matched_text, score, _ = match
                cid = self.aliases_key[matched_text]
                cp = self.canonicals[cid]
                return LookupResult(True, 'fuzzy', score, cid,
                                    cp.brand2, cp.proizvod2, cp.litrag, cp.category, matched_text)

        parsed = parse_xname_fields(xname_str)
        self.unrecognized.append({
            'xname': xname_str,
            'parsed_brand2': parsed['brand2'],
            'parsed_proizvod2': parsed['proizvod2'],
            'parsed_litrag': parsed['litrag'],
            'parsed_category': parsed['category'],
        })
        return LookupResult(
            found=True,
            method='parsed',
            confidence=30.0,
            canonical_id=None,
            brand2=parsed['brand2'],
            proizvod2=parsed['proizvod2'],
            litrag=parsed['litrag'],
            category=parsed['category'],
            matched_alias='',
        )

    def lookup_batch(self, xnames: list[str]) -> list[LookupResult]:
        """Пакетный поиск для списка xname."""
        return [self.lookup(x) for x in xnames]

    def save_brain(self, output_path: str = BRAIN_FILE):
        """Сохраняет мозг в Excel для просмотра и ручной правки."""
        logger.info(f"Сохранение мозга в {output_path}...")

        canonical_rows = []
        for cid, cp in sorted(self.canonicals.items()):
            alias_count = sum(1 for v in self.aliases_exact.values() if v == cid)
            canonical_rows.append({
                'canonical_id': cp.canonical_id,
                'brand2': cp.brand2,
                'proizvod2': cp.proizvod2,
                'litrag': cp.litrag,
                'category': cp.category,
                'alias_count': alias_count
            })
        df_canonical = pd.DataFrame(canonical_rows)

        alias_rows = []
        seen = set()
        for xname_upper, cid in self.aliases_exact.items():
            if xname_upper not in seen:
                seen.add(xname_upper)
                cp = self.canonicals[cid]
                alias_rows.append({
                    'xname': xname_upper,
                    'canonical_id': cid,
                    'brand2': cp.brand2,
                    'proizvod2': cp.proizvod2,
                    'litrag': cp.litrag,
                    'category': cp.category
                })
        df_aliases = pd.DataFrame(alias_rows)

        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_canonical.to_excel(writer, sheet_name='Эталоны', index=False)
            df_aliases.to_excel(writer, sheet_name='Алиасы', index=False)

        logger.info(f"Сохранено: {len(df_canonical)} эталонов, {len(df_aliases)} алиасов")

    def save_unrecognized(self, output_path: str = UNRECOGNIZED_FILE):
        """Сохраняет нераспознанные xname с автоматическим разбором для ручной проверки."""
        if not self.unrecognized:
            logger.info("Нераспознанных записей нет")
            return

        df = pd.DataFrame(self.unrecognized).drop_duplicates(subset=['xname'])
        col_order = ['xname', 'parsed_brand2', 'parsed_proizvod2', 'parsed_litrag', 'parsed_category']
        for c in col_order:
            if c not in df.columns:
                df[c] = ''
        df = df[col_order]
        df.columns = ['xname', 'brand2 (авто)', 'proizvod2 (авто)', 'litrag (авто)', 'category (авто)']
        df.to_excel(output_path, index=False)
        logger.info(f"Сохранено {len(df)} нераспознанных xname в {output_path}")

    def load_brain(self, brain_path: str = BRAIN_FILE):
        """Загружает мозг из ранее сохранённого Excel."""
        logger.info(f"Загрузка мозга из {brain_path}...")

        df_canonical = pd.read_excel(brain_path, sheet_name='Эталоны')
        df_aliases = pd.read_excel(brain_path, sheet_name='Алиасы')

        self.canonicals = {}
        for _, row in df_canonical.iterrows():
            cid = int(row['canonical_id'])
            self.canonicals[cid] = CanonicalProduct(
                canonical_id=cid,
                brand2=str(row['brand2']).strip(),
                proizvod2=str(row['proizvod2']).strip(),
                litrag=float(row['litrag']),
                category=str(row['category']).strip()
            )

        self.aliases_exact = {}
        self.aliases_normalized = {}
        self.aliases_key = {}

        for _, row in df_aliases.iterrows():
            xname = str(row['xname']).strip()
            cid = int(row['canonical_id'])
            self.aliases_exact[xname.upper()] = cid
            self.aliases_normalized[normalize_text(xname)] = cid
            xkey = normalize_xname_key(xname)
            if xkey:
                self.aliases_key[xkey] = cid

        self.all_alias_keys = list(self.aliases_key.keys())

        logger.info(f"Загружено: {len(self.canonicals)} эталонов, {len(self.aliases_exact)} алиасов")

    def add_alias(self, xname: str, canonical_id: int):
        """Добавляет новый алиас к существующему эталону."""
        if canonical_id not in self.canonicals:
            raise ValueError(f"canonical_id={canonical_id} не найден")

        xname_upper = xname.strip().upper()
        self.aliases_exact[xname_upper] = canonical_id
        self.aliases_normalized[normalize_text(xname)] = canonical_id
        xkey = normalize_xname_key(xname)
        if xkey:
            self.aliases_key[xkey] = canonical_id
            if xkey not in self.all_alias_keys:
                self.all_alias_keys.append(xkey)

        logger.info(f"Добавлен алиас: '{xname}' -> canonical_id={canonical_id}")

    def stats(self):
        """Выводит статистику мозга."""
        total_canonicals = len(self.canonicals)
        total_aliases = len(self.aliases_exact)
        non_local = sum(1 for c in self.canonicals.values() if c.brand2 != 'LOCAL')
        local = total_canonicals - non_local

        brands = set(c.brand2 for c in self.canonicals.values() if c.brand2 != 'LOCAL')
        categories = set(c.category for c in self.canonicals.values())

        print(f"{'='*50}")
        print(f"  СТАТИСТИКА МОЗГА")
        print(f"{'='*50}")
        print(f"  Эталонных продуктов:     {total_canonicals}")
        print(f"    - с брендом (не LOCAL): {non_local}")
        print(f"    - LOCAL:                {local}")
        print(f"  Алиасов (xname):         {total_aliases}")
        print(f"  Уникальных брендов:      {len(brands)}")
        print(f"  Категорий:               {len(categories)}")
        print(f"    {', '.join(sorted(categories))}")
        print(f"  Нечёткий поиск:          {'ДА' if HAS_FUZZY else 'НЕТ'}")
        print(f"  Порог fuzzy:             {self._fuzzy_threshold}%")
        print(f"{'='*50}")


def build_brain():
    """Полный цикл: загрузка product.xlsx -> построение -> сохранение мозга."""
    brain = ProductBrain()
    brain.build_from_product_xlsx(SOURCE_FILE)
    brain.save_brain(BRAIN_FILE)
    brain.stats()
    return brain


def demo_lookup(brain: ProductBrain):
    """Демонстрация поиска с разными вариантами написания."""
    test_names = [
        # Известные бренды — разные варианты написания
        "TORNADO ENERGY Напиток энергет айс 0,5л(Росинка):12",
        "tornado energy",
        "FRESH BAR Мохито Напит б/а Сильногаз0,48л пл/бут(Росинка):12",
        "фреш бар мохито",
        "E-ON CITRUS PUNCH",
        "eon citrus",
        "COCA-COLA Напиток газированный 1,5л пл/бут(Кока-Кола):9",
        "кока кола 1.5",
        # Совершенно новый бренд — его нет в базе
        "СУПЕРКОЛА Напиток газированный б/а 0,5л ПЭТ(НовыйЗавод):12",
        "МЕГАЭНЕРДЖИ Энергетический напиток тониз 0,45л ж/б(ООО Сила):24",
        "НЕСУЩЕСТВУЮЩИЙ ТОВАР 777",
    ]

    print(f"\n{'='*80}")
    print("  ДЕМОНСТРАЦИЯ ПОИСКА")
    print(f"{'='*80}\n")

    for name in test_names:
        result = brain.lookup(name)
        status = "OK" if result.found else "??"
        print(f"  [{status}] '{name}'")
        if result.found:
            print(f"       -> brand2={result.brand2}, proizvod2={result.proizvod2}, "
                  f"litrag={result.litrag}, category={result.category}")
            print(f"       -> метод: {result.method}, уверенность: {result.confidence}%")
        else:
            print(f"       -> НЕ НАЙДЕНО")
        print()


if __name__ == '__main__':
    brain = build_brain()
    demo_lookup(brain)
    brain.save_unrecognized()
