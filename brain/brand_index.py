"""
brand_index.py — Индекс брендов: синонимы, аббревиатуры, транслитерация.

Строится автоматически из product.xlsx.
"""

import re
import pandas as pd
from brain.normalizer import normalize_upper, transliterate_ru_to_en, _BRAND_MULTIWORD_TRANSLIT


_MANUAL_ABBREVIATIONS = {
    'КК': 'COCA-COLA', 'CC': 'COCA-COLA',
    'ФБ': 'FRESH BAR', 'FB': 'FRESH BAR',
    'ИЛ': 'ИЛЬИНСКИЕ ЛИМОНАДЫ',
    'ЛЭ': 'LIT ENERGY', 'LE': 'LIT ENERGY',
    'ТЭ': 'TORNADO', 'TE': 'TORNADO',
    'ЕОН': 'E-ON', 'EON': 'E-ON', 'ИОН': 'E-ON',
    'Е-ОН': 'E-ON', 'Е ОН': 'E-ON',
    'БМ': 'BLACK MONSTER', 'BM': 'BLACK MONSTER',
    'РБ': 'RED BULL', 'RB': 'RED BULL',
    'ДМ': 'DRIVE ME', 'DM': 'DRIVE ME',
    'ГД': 'G-DRIVE', 'GD': 'G-DRIVE',
}


class BrandIndex:
    """Индекс для быстрого определения бренда по фрагменту текста."""

    def __init__(self):
        self.brand2_list: list[str] = []
        self.synonyms: dict[str, str] = {}  # synonym_upper -> canonical brand2
        self.abbreviations: dict[str, str] = {}

    def build_from_dataframe(self, df: pd.DataFrame):
        """Строит индекс из product.xlsx DataFrame."""
        df_work = df[['xname', 'brand2']].copy()
        df_work['brand2'] = df_work['brand2'].astype(str).str.strip().str.upper()
        df_work['xname'] = df_work['xname'].astype(str).str.strip()

        raw_brands = df_work[df_work['brand2'] != 'LOCAL']['brand2'].unique()
        self.brand2_list = sorted(
            [str(b) for b in raw_brands if pd.notna(b) and str(b).strip()],
            key=len, reverse=True
        )

        self.synonyms = {}
        self.abbreviations = dict(_MANUAL_ABBREVIATIONS)

        for brand2 in self.brand2_list:
            self.synonyms[brand2] = brand2

            en = transliterate_ru_to_en(brand2)
            if en != brand2:
                self.synonyms[en] = brand2

            cleaned = re.sub(r'[^А-ЯA-Z0-9\s]', '', brand2).strip()
            if cleaned and cleaned != brand2:
                self.synonyms[cleaned] = brand2

            no_dash = brand2.replace('-', ' ').replace('  ', ' ').strip()
            if no_dash != brand2:
                self.synonyms[no_dash] = brand2
            no_space_dash = brand2.replace('-', '').replace(' ', '')
            if no_space_dash != brand2:
                self.synonyms[no_space_dash] = brand2

        brand2_to_xnames = {}
        for _, row in df_work.iterrows():
            b = row['brand2']
            if b == 'LOCAL':
                continue
            xn = normalize_upper(row['xname'])
            if b not in brand2_to_xnames:
                brand2_to_xnames[b] = []
            brand2_to_xnames[b].append(xn)

        for ru_phrase, en_phrase in _BRAND_MULTIWORD_TRANSLIT.items():
            en_upper = en_phrase.upper()
            if en_upper in brand2_to_xnames or any(en_upper == b for b in self.brand2_list):
                self.synonyms[ru_phrase] = en_upper

        for brand2, xnames in brand2_to_xnames.items():
            russian_starts = set()
            brand2_str = str(brand2)
            for xn in xnames[:50]:
                xn_clean = re.sub(r'\([^)]*\)', '', xn)
                xn_clean = re.sub(r':\d+\s*$', '', xn_clean)
                words = xn_clean.split()

                noise = {'НАПИТОК', 'НАПИТ', 'НАП', 'ЭНЕРГ', 'ЭНЕРГЕТ', 'ЭНЕРГЕТИК',
                         'ЭНЕРГЕТИЧЕСКИЙ', 'БЕЗАЛК', 'Б/А', 'БЕЗАЛКОГОЛЬНЫЙ',
                         'ТОНИЗИР', 'ТОНИЗ', 'ТОНИЗИРУЮЩИЙ',
                         'КОКТЕЙЛЬ', 'ЛИМОНАД', 'КВАС', 'ВОДА', 'НЕКТАР', 'СОК',
                         'МОРС', 'НАПИТКИ', 'ГАЗИРОВАННЫЙ', 'ГАЗИРОВАНН', 'ГАЗИР',
                         'ГАЗИРОВАННАЯ', 'СИЛЬНОГАЗ', 'НЕГАЗ',
                         'МИНЕРАЛЬНАЯ', 'МИНЕРАЛ', 'ПИТЬЕВАЯ',
                         'СЛАДКИЙ', 'СЛАДК',
                         'КОЛА', 'COLA', 'МОХИТО', 'MOJITO',
                         'ДЮШЕС', 'ТАРХУН', 'БАЙКАЛ',
                         'С/СОД', 'СОКОСОД', 'СОКОСОДЕРЖАЩИЙ',
                         'ФРУКТОВЫЙ', 'ФРУКТ', 'ЯГОДНЫЙ',
                         'ДЕТСКИЙ', 'ДЕТСКАЯ', 'ДЕТСКОЕ',
                         'ИЗ', 'НА', 'С', 'В', 'И', 'ДЛЯ', 'ПО', 'ОТ',
                         'МУЛЬТИПАК', 'НАБОР',
                         'ПЭТ', 'PET', 'CAN', 'ПЛ/БУТ', 'Ж/Б', 'ЖБ', 'СТ/Б',
                         'СТЕКЛ', 'ПЛАСТИК', 'БАНКА', 'БУТЫЛКА',
                         'Б/А', 'БА', 'Б', 'А', 'Б А', 'ГАЗ', 'СИЛГАЗ',
                         'НАПИТОК', 'НАПИТ', 'НАП',
                         'ВКС', 'КЛ', 'СИЛ', 'ЗАВ', 'ПР',
                         'ОСНВ', 'ОСВ', 'ОСВЕТЛ', 'МЯК',
                         'ФИЛЬТ', 'ПАСТ', 'ЖИВОЙ',
                         'PREMIUM', 'CLASSIC', 'ORIGINAL', 'ZERO', 'ЗЕРО',
                         'LIGHT', 'ЛАЙТ', 'SUGAR', 'FREE', 'MAX',
                         'НОВЫЙ', 'НОВАЯ', 'НОВОЕ', 'SPECIAL',
                         'МЛ', 'Л', 'КГ', 'ШТ'}
                words = [w for w in words if w not in noise and not re.match(r'^\d', w)]

                if not words:
                    continue

                brand_words = brand2_str.split()
                n_brand_words = len(brand_words)

                for n in range(1, min(n_brand_words + 2, len(words) + 1)):
                    candidate = ' '.join(words[:n])
                    if candidate == brand2:
                        continue
                    has_cyrillic = any('А' <= c <= 'Я' for c in candidate)
                    min_len = 4 if n == 1 else 3
                    if has_cyrillic and len(candidate) >= min_len:
                        russian_starts.add(candidate)

            for ru_name in russian_starts:
                if ru_name not in self.synonyms:
                    self.synonyms[ru_name] = brand2

                en_name = transliterate_ru_to_en(ru_name)
                if en_name != ru_name and en_name not in self.synonyms:
                    self.synonyms[en_name] = brand2

    def find_brand(self, text: str) -> tuple[str, float]:
        """
        Ищет бренд в тексте.
        Возвращает (brand2, confidence).
        """
        s = normalize_upper(text)
        s_clean = re.sub(r'\([^)]*\)', '', s)
        s_clean = re.sub(r':\d+\s*$', '', s_clean).strip()
        s_nopunct = re.sub(r'[^А-ЯA-Z0-9\s]', ' ', s_clean)
        s_nopunct = re.sub(r'\s+', ' ', s_nopunct).strip()

        s_en = transliterate_ru_to_en(s_nopunct)

        words = s_nopunct.split()

        # 0) Аббревиатуры — проверяем первым делом (ФБ, КК, РБ, Е-ОН...)
        s_with_dash = re.sub(r'\s+', ' ', s_clean).strip()
        for abbr, brand_val in sorted(self.abbreviations.items(), key=lambda x: -len(x[0])):
            if s_with_dash.startswith(abbr) or s_nopunct.startswith(abbr):
                return brand_val, 95.0

        if words:
            first = words[0]
            if first in self.abbreviations:
                return self.abbreviations[first], 95.0
            first_en = transliterate_ru_to_en(first)
            if first_en in self.abbreviations:
                return self.abbreviations[first_en], 95.0
            if len(words) >= 2:
                first_two = f"{words[0]} {words[1]}"
                if first_two in self.abbreviations:
                    return self.abbreviations[first_two], 95.0

        # 1) Точное совпадение с полным brand2 (длинные бренды сначала)
        for brand2 in self.brand2_list:
            if len(brand2) < 2:
                continue
            if brand2 in s_clean or brand2 in s:
                return brand2, 100.0

        # 2) Поиск по синонимам (длинные сначала, пропускаем короткие)
        for syn, brand2 in sorted(self.synonyms.items(), key=lambda x: -len(x[0])):
            if len(syn) < 3:
                continue
            if len(syn) <= 5:
                pat = r'(?:^|\s)' + re.escape(syn) + r'(?:\s|$)'
                if re.search(pat, s_nopunct) or re.search(pat, s_en) or re.search(pat, s_clean):
                    return brand2, 95.0
            else:
                if syn in s_nopunct or syn in s_en or syn in s_clean:
                    return brand2, 95.0

        # 3) Пословный поиск (N-gram): 3 слова, 2 слова, 1 слово
        for n_words in range(min(3, len(words)), 0, -1):
            for start in range(len(words) - n_words + 1):
                candidate = ' '.join(words[start:start + n_words])
                if len(candidate) < 3:
                    continue
                if candidate in self.synonyms:
                    return self.synonyms[candidate], 90.0
                en_candidate = transliterate_ru_to_en(candidate)
                if en_candidate != candidate and en_candidate in self.synonyms:
                    return self.synonyms[en_candidate], 88.0

        # 4) Пословный поиск по en-версии
        en_words = s_en.split()
        for n_words in range(min(3, len(en_words)), 0, -1):
            for start in range(len(en_words) - n_words + 1):
                candidate = ' '.join(en_words[start:start + n_words])
                if len(candidate) < 3:
                    continue
                if candidate in self.synonyms:
                    return self.synonyms[candidate], 86.0

        return '', 0.0

    def stats(self) -> dict:
        return {
            'brands': len(self.brand2_list),
            'synonyms': len(self.synonyms),
            'abbreviations': len(self.abbreviations),
        }
