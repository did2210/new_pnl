"""
product_brain.py — Точка входа для «Мозга» расшифровки товарных наименований.

Использование:
    python product_brain.py           — построить мозг + прогнать самотест
    python product_brain.py --build   — только построить и сохранить
"""

import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('product_brain.log', mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

from brain.core import ProductBrain

SOURCE_FILE = 'product.xlsx'
BRAIN_FILE = 'product_brain.xlsx'


def build():
    brain = ProductBrain()
    brain.build(SOURCE_FILE)
    brain.save_brain(BRAIN_FILE)
    brain.stats()
    return brain


def self_test(brain: ProductBrain) -> bool:
    """
    Самотест с тремя уровнями сложности.
    Возвращает True, если все тесты пройдены.
    """
    tests = [
        # =========================================================
        #  ЛЁГКИЕ — полное или почти полное название из базы
        # =========================================================
        {
            'name': 'MONSTER ENERGY НАПИТОК 0,5Л ПЭТ(ЛИДЕР):12',
            'expect_brand': 'MONSTER',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'EASY',
        },
        {
            'name': 'BURN Энерг Напиток 0,5л ПЭТ(Кока-Кола):12',
            'expect_brand': 'BURN',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'EASY',
        },
        {
            'name': 'COCA-COLA Напиток газированный 1,5л пл/бут(Кока-Кола):9',
            'expect_brand': 'COCA-COLA',
            'expect_category': 'ГАЗИРОВКА',
            'level': 'EASY',
        },
        {
            'name': 'FRESH BAR Мохито Напит б/а Сильногаз0,48л пл/бут(Росинка):12',
            'expect_brand': 'FRESH BAR',
            'expect_category': 'ГАЗИРОВКА',
            'level': 'EASY',
        },
        {
            'name': 'E-ON CITRUS PUNCH',
            'expect_brand': 'E-ON',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'EASY',
        },
        {
            'name': 'TORNADO ENERGY Напиток энергет айс 0,5л(Росинка):12',
            'expect_brand': 'TORNADO',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'EASY',
        },
        {
            'name': 'ИЛЬИНСКИЕ ЛИМОНАДЫ Напиток дюшес сил/газ 0,5л пл/бут(Дал):12',
            'expect_brand': 'ИЛЬИНСКИЕ ЛИМОНАДЫ',
            'expect_category': 'ГАЗИРОВКА',
            'level': 'EASY',
        },
        {
            'name': 'LIT ENERGY Напиток энергетический 0,45л ж/б',
            'expect_brand': 'LIT ENERGY',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'EASY',
        },

        # =========================================================
        #  СРЕДНИЕ — сокращения, перестановки, русские варианты
        # =========================================================
        {
            'name': 'tornado energy',
            'expect_brand': 'TORNADO',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'MEDIUM',
        },
        {
            'name': 'фреш бар мохито',
            'expect_brand': 'FRESH BAR',
            'expect_category': 'ГАЗИРОВКА',
            'level': 'MEDIUM',
        },
        {
            'name': 'eon citrus',
            'expect_brand': 'E-ON',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'MEDIUM',
        },
        {
            'name': 'кока кола 1.5',
            'expect_brand': 'COCA-COLA',
            'expect_category': 'ГАЗИРОВКА',
            'level': 'MEDIUM',
        },
        {
            'name': 'торнадо энерджи шторм 473мл пэт',
            'expect_brand': 'TORNADO',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'MEDIUM',
        },
        {
            'name': 'Е-ОН черная сила 450мл банка',
            'expect_brand': 'E-ON',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'MEDIUM',
        },
        {
            'name': 'монстер энерджи 0.5',
            'expect_brand': 'MONSTER',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'MEDIUM',
        },
        {
            'name': 'берн тропик 0,5л',
            'expect_brand': 'BURN',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'MEDIUM',
        },
        {
            'name': 'спрайт газ 1,5 литра',
            'expect_brand': 'SPRITE',
            'expect_category': 'ГАЗИРОВКА',
            'level': 'MEDIUM',
        },
        {
            'name': 'ильинские лимонады дюшес 500мл',
            'expect_brand': 'ИЛЬИНСКИЕ ЛИМОНАДЫ',
            'expect_category': 'ГАЗИРОВКА',
            'level': 'MEDIUM',
        },

        # =========================================================
        #  СЛОЖНЫЕ — аббревиатуры, нестандартный литраж, сленг
        # =========================================================
        {
            'name': 'лит энерг 0.45 жб',
            'expect_brand': 'LIT ENERGY',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'HARD',
        },
        {
            'name': 'ФБ мохито газ пол литра',
            'expect_brand': 'FRESH BAR',
            'expect_category': 'ГАЗИРОВКА',
            'level': 'HARD',
        },
        {
            'name': 'КК зеро 0,33 банка',
            'expect_brand': 'COCA-COLA',
            'expect_category': 'ГАЗИРОВКА',
            'level': 'HARD',
        },
        {
            'name': 'РБ энерг 0.25 жб',
            'expect_brand': 'RED BULL',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'HARD',
        },
        {
            'name': 'горилла энерг банка 0.45',
            'expect_brand': 'GORILLA',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'HARD',
        },
        {
            'name': 'драйв ми 0,449л',
            'expect_brand': 'DRIVE ME',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'HARD',
        },
        {
            'name': 'флеш ап энерг 0.5 пэт',
            'expect_brand': 'FLASH UP',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'HARD',
        },

        # =========================================================
        #  НОВЫЕ БРЕНДЫ — нет в базе, проверяем авторазбор
        # =========================================================
        {
            'name': 'СУПЕРКОЛА Напиток газированный б/а 0,5л ПЭТ(НовыйЗавод):12',
            'expect_brand': 'LOCAL',
            'expect_category': 'ГАЗИРОВКА',
            'level': 'NEW',
        },
        {
            'name': 'МЕГАЭНЕРДЖИ Энергетический напиток тониз 0,45л ж/б(ООО Сила):24',
            'expect_brand': 'МЕГАENERGY',
            'expect_category': 'ЭНЕРГЕТИКИ',
            'level': 'NEW',
        },
    ]

    print(f"\n{'='*80}")
    print(f"  САМОТЕСТ — {len(tests)} тестов")
    print(f"{'='*80}\n")

    passed = 0
    failed = 0
    results_by_level = {'EASY': [], 'MEDIUM': [], 'HARD': [], 'NEW': []}

    for t in tests:
        result = brain.lookup(t['name'])
        level = t['level']

        brand_ok = (result.brand2 == t['expect_brand'])
        cat_ok = (result.category == t['expect_category'])

        if level == 'NEW':
            brand_ok = True
            if t['expect_brand'] == 'LOCAL':
                brand_ok = (result.brand2 == 'LOCAL')
            cat_ok = (result.category == t['expect_category'])

        ok = brand_ok and cat_ok
        status = 'PASS' if ok else 'FAIL'

        if ok:
            passed += 1
        else:
            failed += 1

        results_by_level[level].append((t, result, ok))

    for level in ['EASY', 'MEDIUM', 'HARD', 'NEW']:
        items = results_by_level[level]
        level_pass = sum(1 for _, _, ok in items if ok)
        print(f"  --- {level} ({level_pass}/{len(items)}) ---")
        for t, result, ok in items:
            status = 'PASS' if ok else 'FAIL'
            icon = ' OK ' if ok else 'FAIL'
            print(f"  [{icon}] '{t['name']}'")
            if ok:
                print(f"         brand2={result.brand2}, category={result.category}, "
                      f"method={result.method}, conf={result.confidence:.0f}%")
            else:
                print(f"         GOT:    brand2={result.brand2}, category={result.category}, "
                      f"method={result.method}")
                print(f"         EXPECT: brand2={t['expect_brand']}, "
                      f"category={t['expect_category']}")
        print()

    total = passed + failed
    pct = (passed / total * 100) if total > 0 else 0
    print(f"{'='*80}")
    print(f"  ИТОГО: {passed}/{total} ({pct:.0f}%)")
    if failed == 0:
        print(f"  ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
    else:
        print(f"  ПРОВАЛЕНО: {failed}")
    print(f"{'='*80}\n")

    return failed == 0


if __name__ == '__main__':
    brain = build()

    if '--build' not in sys.argv:
        self_test(brain)

    brain.save_unrecognized()
