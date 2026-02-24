"""
core.py — Ядро мозга: ProductBrain с многоуровневым поиском.
"""

import pandas as pd
import re
import logging
from dataclasses import dataclass
from typing import Optional

try:
    from rapidfuzz import fuzz, process as rf_process
    HAS_FUZZY = True
except ImportError:
    HAS_FUZZY = False

from brain.normalizer import (
    normalize_upper, clean_xname, make_search_key,
    extract_litrag, extract_brand_candidates, transliterate_ru_to_en,
)
from brain.brand_index import BrandIndex
from brain.category_detector import detect_category, detect_subcategory, detect_format

logger = logging.getLogger(__name__)


@dataclass
class CanonicalProduct:
    canonical_id: int
    brand2: str
    proizvod2: str
    litrag: float
    category: str
    subcategory: str


@dataclass
class LookupResult:
    found: bool
    method: str
    confidence: float
    canonical_id: Optional[int]
    brand2: str
    proizvod2: str
    litrag: float
    category: str
    subcategory: str
    matched_alias: str


class ProductBrain:
    """Мозг для расшифровки товарных наименований."""

    def __init__(self):
        self.canonicals: dict[int, CanonicalProduct] = {}
        self.brand_index = BrandIndex()

        self.aliases_exact: dict[str, int] = {}
        self.aliases_cleaned: dict[str, int] = {}
        self.aliases_search_key: dict[str, int] = {}

        self.energy_keys: dict[str, int] = {}
        self.soda_keys: dict[str, int] = {}
        self.other_keys: dict[str, int] = {}

        self.brand_to_cids: dict[str, list[int]] = {}

        self.all_search_keys: list[str] = []
        self.unrecognized: list[dict] = []
        self._fuzzy_threshold = 75

    def build(self, file_path: str = 'product.xlsx'):
        """Строит мозг из product.xlsx."""
        logger.info(f"Загрузка {file_path}...")
        df = pd.read_excel(file_path)
        logger.info(f"Загружено {len(df)} записей")

        required = ['xname', 'brand2', 'proizvod2', 'litrag', 'category']
        for c in required:
            if c not in df.columns:
                raise ValueError(f"Отсутствует столбец: {c}")

        df['brand2_norm'] = df['brand2'].astype(str).str.strip().str.upper()
        df['proizvod2_norm'] = df['proizvod2'].astype(str).str.strip().str.upper()
        df['category_norm'] = df['category'].astype(str).str.strip().str.upper()
        df['litrag_num'] = pd.to_numeric(df['litrag'], errors='coerce').fillna(0.0)
        if 'subcategory' in df.columns:
            df['subcategory_clean'] = df['subcategory'].fillna('').astype(str).str.strip()
        else:
            df['subcategory_clean'] = ''

        logger.info("Построение индекса брендов...")
        self.brand_index.build_from_dataframe(df)
        bstats = self.brand_index.stats()
        logger.info(f"  Брендов: {bstats['brands']}, синонимов: {bstats['synonyms']}, "
                     f"аббревиатур: {bstats['abbreviations']}")

        groups = df.groupby(
            ['brand2_norm', 'proizvod2_norm', 'litrag_num', 'category_norm']
        ).agg(
            alias_count=('xname', 'count'),
            subcategory=('subcategory_clean', 'first')
        ).reset_index()
        groups = groups.sort_values('alias_count', ascending=False).reset_index(drop=True)

        self.canonicals = {}
        canonical_key_to_id: dict[tuple, int] = {}
        self.brand_to_cids = {}

        for idx, row in groups.iterrows():
            cid = idx + 1
            key = (row['brand2_norm'], row['proizvod2_norm'], row['litrag_num'], row['category_norm'])
            cp = CanonicalProduct(
                canonical_id=cid,
                brand2=row['brand2_norm'],
                proizvod2=row['proizvod2_norm'],
                litrag=row['litrag_num'],
                category=row['category_norm'],
                subcategory=row['subcategory']
            )
            self.canonicals[cid] = cp
            canonical_key_to_id[key] = cid

            if cp.brand2 not in self.brand_to_cids:
                self.brand_to_cids[cp.brand2] = []
            self.brand_to_cids[cp.brand2].append(cid)

        logger.info(f"Создано {len(self.canonicals)} эталонных продуктов")

        self.aliases_exact = {}
        self.aliases_cleaned = {}
        self.aliases_search_key = {}
        self.energy_keys = {}
        self.soda_keys = {}
        self.other_keys = {}

        local_exact: dict[str, int] = {}
        local_cleaned: dict[str, int] = {}
        local_search: dict[str, int] = {}

        for _, row in df.iterrows():
            xname = str(row['xname']).strip()
            brand2 = row['brand2_norm']
            key = (brand2, row['proizvod2_norm'], row['litrag_num'], row['category_norm'])
            cid = canonical_key_to_id.get(key)
            if cid is None:
                continue

            category = row['category_norm']
            is_local = (brand2 == 'LOCAL')

            x_upper = xname.upper()
            x_cleaned = clean_xname(xname)
            x_search = make_search_key(xname)

            if is_local:
                local_exact.setdefault(x_upper, cid)
                if x_cleaned:
                    local_cleaned.setdefault(x_cleaned, cid)
                if x_search:
                    local_search.setdefault(x_search, cid)
            else:
                self.aliases_exact[x_upper] = cid
                if x_cleaned:
                    self.aliases_cleaned[x_cleaned] = cid
                if x_search:
                    self.aliases_search_key[x_search] = cid

                pool = (self.energy_keys if category == 'ЭНЕРГЕТИКИ'
                        else self.soda_keys if category == 'ГАЗИРОВКА'
                        else self.other_keys)
                if x_search:
                    pool[x_search] = cid

        for d_local, d_main in [(local_exact, self.aliases_exact),
                                 (local_cleaned, self.aliases_cleaned),
                                 (local_search, self.aliases_search_key)]:
            for k, v in d_local.items():
                d_main.setdefault(k, v)

        self.all_search_keys = list(self.aliases_search_key.keys())

        logger.info(f"Индексировано: exact={len(self.aliases_exact)}, "
                     f"cleaned={len(self.aliases_cleaned)}, "
                     f"search_key={len(self.aliases_search_key)}")
        logger.info(f"По категориям: energy={len(self.energy_keys)}, "
                     f"soda={len(self.soda_keys)}, other={len(self.other_keys)}")

    def lookup(self, xname: str) -> LookupResult:
        if not xname or (isinstance(xname, float) and pd.isna(xname)):
            return self._empty_result()

        xname_str = str(xname).strip()
        x_upper = xname_str.upper()

        # 1) Точное совпадение
        cid = self.aliases_exact.get(x_upper)
        if cid is not None:
            return self._result_from_cid(cid, 'exact', 100.0, xname_str)

        # 2) Очищенный текст
        x_cleaned = clean_xname(xname_str)
        cid = self.aliases_cleaned.get(x_cleaned)
        if cid is not None:
            return self._result_from_cid(cid, 'cleaned', 95.0, xname_str)

        # 3) Поисковый ключ (с транслитерацией)
        x_search = make_search_key(xname_str)
        cid = self.aliases_search_key.get(x_search)
        if cid is not None:
            return self._result_from_cid(cid, 'search_key', 92.0, xname_str)

        # 4) Определяем бренд через brand_index
        brand2, brand_conf = self.brand_index.find_brand(xname_str)
        litrag = extract_litrag(xname_str)

        # 5) Если бренд найден — ищем по brand + litrag
        if brand2 and brand_conf >= 85 and brand2 != 'LOCAL':
            cids = self.brand_to_cids.get(brand2, [])
            if cids:
                if litrag is not None:
                    for c in cids:
                        cp = self.canonicals[c]
                        if abs(cp.litrag - litrag) < 0.02:
                            return self._result_from_cid(c, 'brand+litrag', brand_conf, xname_str)

                best_cid = cids[0]
                return self._result_from_cid(best_cid, 'brand_match', brand_conf * 0.9, xname_str)

        # 6) Fuzzy по категорийному пулу
        if HAS_FUZZY and x_search:
            category_guess = detect_category(xname_str, brand2)
            pool = (self.energy_keys if category_guess == 'ЭНЕРГЕТИКИ'
                    else self.soda_keys if category_guess == 'ГАЗИРОВКА'
                    else None)

            fuzzy_cutoff = self._fuzzy_threshold
            if brand2 and brand_conf >= 80:
                fuzzy_cutoff = max(fuzzy_cutoff, 85)

            if pool:
                match = rf_process.extractOne(
                    x_search, list(pool.keys()),
                    scorer=fuzz.token_set_ratio,
                    score_cutoff=fuzzy_cutoff
                )
                if match:
                    matched_text, score, _ = match
                    cid = pool[matched_text]
                    return self._result_from_cid(cid, 'fuzzy_category', score, matched_text)

            match = rf_process.extractOne(
                x_search, self.all_search_keys,
                scorer=fuzz.token_set_ratio,
                score_cutoff=fuzzy_cutoff
            )
            if match:
                matched_text, score, _ = match
                cid = self.aliases_search_key[matched_text]
                return self._result_from_cid(cid, 'fuzzy_global', score, matched_text)

        # 7) Авторазбор для совершенно неизвестного товара
        return self._parse_unknown(xname_str, brand2, litrag)

    def _result_from_cid(self, cid: int, method: str, confidence: float,
                          alias: str) -> LookupResult:
        cp = self.canonicals[cid]
        return LookupResult(
            found=True, method=method, confidence=confidence,
            canonical_id=cid,
            brand2=cp.brand2, proizvod2=cp.proizvod2,
            litrag=cp.litrag, category=cp.category,
            subcategory=cp.subcategory, matched_alias=alias
        )

    def _empty_result(self) -> LookupResult:
        return LookupResult(False, 'not_found', 0.0, None, '', '', 0.0, '', '', '')

    def _parse_unknown(self, xname: str, brand2_hint: str,
                        litrag_hint: Optional[float]) -> LookupResult:
        """Разбирает неизвестный товар по правилам."""
        s_upper = normalize_upper(xname)
        category = detect_category(xname, brand2_hint)
        subcategory = detect_subcategory(xname, category)
        litrag = litrag_hint if litrag_hint is not None else 0.0

        proizvod2 = ''
        m = re.search(r'\(([^)]+)\)', xname)
        if m:
            proizvod2 = m.group(1).strip().upper()

        if category == 'ГАЗИРОВКА':
            brand2 = 'LOCAL'
        elif brand2_hint:
            brand2 = brand2_hint
        else:
            candidates = extract_brand_candidates(xname)
            brand2 = candidates[0] if candidates else ''

        raw_brand = ''
        candidates = extract_brand_candidates(xname)
        if candidates:
            raw_brand = candidates[0]

        self.unrecognized.append({
            'xname': xname,
            'brand2': brand2,
            'raw_brand': raw_brand,
            'proizvod2': proizvod2 or 'UNKNOWN',
            'litrag': litrag,
            'category': category,
            'subcategory': subcategory,
        })

        return LookupResult(
            found=True, method='parsed', confidence=30.0,
            canonical_id=None,
            brand2=brand2, proizvod2=proizvod2 or 'UNKNOWN',
            litrag=litrag, category=category,
            subcategory=subcategory, matched_alias=''
        )

    def save_brain(self, output_path: str = 'product_brain.xlsx'):
        logger.info(f"Сохранение мозга в {output_path}...")
        canonical_rows = []
        for cid, cp in sorted(self.canonicals.items()):
            canonical_rows.append({
                'canonical_id': cp.canonical_id,
                'brand2': cp.brand2,
                'proizvod2': cp.proizvod2,
                'litrag': cp.litrag,
                'category': cp.category,
                'subcategory': cp.subcategory,
            })
        df_can = pd.DataFrame(canonical_rows)

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
                    'category': cp.category,
                    'subcategory': cp.subcategory,
                })
        df_alias = pd.DataFrame(alias_rows)

        with pd.ExcelWriter(output_path, engine='openpyxl') as w:
            df_can.to_excel(w, sheet_name='Эталоны', index=False)
            df_alias.to_excel(w, sheet_name='Алиасы', index=False)

        logger.info(f"Сохранено: {len(df_can)} эталонов, {len(df_alias)} алиасов")

    def save_unrecognized(self, output_path: str = 'unrecognized_xnames.xlsx'):
        if not self.unrecognized:
            return
        df = pd.DataFrame(self.unrecognized).drop_duplicates(subset=['xname'])
        df.to_excel(output_path, index=False)
        logger.info(f"Сохранено {len(df)} нераспознанных xname в {output_path}")

    def stats(self):
        total = len(self.canonicals)
        non_local = sum(1 for c in self.canonicals.values() if c.brand2 != 'LOCAL')
        brands = set(c.brand2 for c in self.canonicals.values() if c.brand2 != 'LOCAL')
        bstats = self.brand_index.stats()
        print(f"\n{'='*55}")
        print(f"  МОЗГ — СТАТИСТИКА")
        print(f"{'='*55}")
        print(f"  Эталонных продуктов:      {total}")
        print(f"    с брендом (не LOCAL):    {non_local}")
        print(f"    LOCAL:                   {total - non_local}")
        print(f"  Алиасов (xname):           {len(self.aliases_exact)}")
        print(f"  Уникальных брендов:        {len(brands)}")
        print(f"  Синонимов брендов:         {bstats['synonyms']}")
        print(f"  Аббревиатур:               {bstats['abbreviations']}")
        print(f"  Fuzzy пулы: energy={len(self.energy_keys)}, "
              f"soda={len(self.soda_keys)}, other={len(self.other_keys)}")
        print(f"  Fuzzy порог:               {self._fuzzy_threshold}%")
        print(f"{'='*55}\n")
