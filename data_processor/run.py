#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔════════════════════════════════════════════════════════════════════╗
║     Автоматизированная обработка контрактов GFD — Главное меню    ║
╚════════════════════════════════════════════════════════════════════╝

Интерактивное консольное приложение:
  • Укажите пути к файлам
  • Обрабатывайте по шагам или всё сразу
  • Получайте итоговый Excel с проверками
"""
import os
import sys
import json
import time
import traceback
from datetime import datetime

# Добавляем текущую директорию в path
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from core.helpers import C
from core.extractor import extract_tables, save_combined, ExtractionResult
from core.parser import parse_file, ParseResult
from core.merger import merge as do_merge, MergeResult
from core.reporter import generate_report
from core.validator import validate, save_report as save_validation, Issue

# ═══════════════════════════════════════════════════════════════════
#  НАСТРОЙКИ (сохраняются в settings.json между запусками)
# ═══════════════════════════════════════════════════════════════════

SETTINGS_FILE = os.path.join(HERE, 'settings.json')
DEFAULTS = {
    'input_dir': os.path.join(HERE, 'input'),
    'output_dir': os.path.join(HERE, 'output'),
    'temp_dir': os.path.join(HERE, 'temp'),
    'existing_db': '',
    'sales_file': '',
    'costs_not_price_file': '',
    'costs_in_price_file': '',
    'cm_file': '',
    'cogs_file': '',
}


def load_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            merged = {**DEFAULTS, **saved}
            return merged
        except Exception:
            pass
    return dict(DEFAULTS)


def save_settings(s):
    try:
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(s, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
#  ГЛОБАЛЬНОЕ СОСТОЯНИЕ СЕССИИ
# ═══════════════════════════════════════════════════════════════════

class Session:
    def __init__(self):
        self.cfg = load_settings()
        self.input_files = []          # исходные .xlsx
        self.extracted_files = []      # пути после шага 1
        self.parsed_dfs = []           # DataFrame-ы после шага 2
        self.merged_df = None          # после шага 3
        self.report_path = None        # после шага 4
        self.validation_path = None    # отчёт валидации
        self.log = []                  # лог сообщений

    def add_log(self, msg):
        self.log.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


sess = Session()


# ═══════════════════════════════════════════════════════════════════
#  УТИЛИТЫ ВВОДА-ВЫВОДА
# ═══════════════════════════════════════════════════════════════════

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')


def pause():
    input(f"\n{C.DIM}Нажмите Enter для продолжения...{C.RESET}")


def ask(prompt, default=''):
    d = f" [{default}]" if default else ""
    val = input(f"  {prompt}{d}: ").strip()
    return val if val else default


def ask_yn(prompt, default=True):
    d = "Д/н" if default else "д/Н"
    val = input(f"  {prompt} [{d}]: ").strip().lower()
    if not val:
        return default
    return val in ('д', 'y', 'да', 'yes', '1')


def find_excel(directory):
    """Все .xlsx/.xls в директории."""
    if not os.path.isdir(directory):
        return []
    result = []
    for f in sorted(os.listdir(directory)):
        if (f.lower().endswith(('.xlsx', '.xls', '.xlsm'))
                and not f.startswith('~')):
            result.append(os.path.join(directory, f))
    return result


def banner():
    print(f"""
{C.BOLD}{C.CYAN}╔══════════════════════════════════════════════════════════════╗
║       ОБРАБОТКА КОНТРАКТОВ GFD  —  Интерактивное меню       ║
╚══════════════════════════════════════════════════════════════╝{C.RESET}
""")


def show_status():
    """Показать текущее состояние пайплайна."""
    print(f"\n{C.BOLD}── Текущее состояние ──{C.RESET}")
    states = [
        ("Исходные файлы", len(sess.input_files)),
        ("Извлечено (шаг 1)", len(sess.extracted_files)),
        ("Распарсено (шаг 2)", len(sess.parsed_dfs)),
        ("Объединено (шаг 3)", "Да" if sess.merged_df is not None else "Нет"),
        ("Отчёт (шаг 4)", os.path.basename(sess.report_path) if sess.report_path else "Нет"),
        ("Валидация", os.path.basename(sess.validation_path) if sess.validation_path else "Нет"),
    ]
    for label, val in states:
        icon = C.GREEN + "●" + C.RESET if val and val != "Нет" and val != 0 else C.DIM + "○" + C.RESET
        print(f"  {icon} {label}: {val}")
    print()


# ═══════════════════════════════════════════════════════════════════
#  МЕНЮ: НАСТРОЙКА ПУТЕЙ
# ═══════════════════════════════════════════════════════════════════

def menu_settings():
    clear()
    print(C.header("НАСТРОЙКА ПУТЕЙ"))
    fields = [
        ('input_dir',            'Папка с исходными файлами'),
        ('output_dir',           'Папка для результатов'),
        ('temp_dir',             'Папка для промежуточных файлов'),
        ('existing_db',          'Существующая база (xlsx, пусто = нет)'),
        ('sales_file',           'Файл продаж Sales.xlsx (пусто = нет)'),
        ('costs_not_price_file', 'Затраты вне цены (пусто = нет)'),
        ('costs_in_price_file',  'Затраты в цене (пусто = нет)'),
        ('cm_file',              'Файл ЦМ (пусто = нет)'),
        ('cogs_file',            'Себестоимость (пусто = нет)'),
    ]
    print()
    for i, (key, label) in enumerate(fields, 1):
        cur = sess.cfg.get(key, '')
        indicator = C.GREEN + "✓" + C.RESET if cur else C.DIM + "–" + C.RESET
        print(f"  {indicator} {i}. {label}")
        print(f"     {C.DIM}{cur or '(не указано)'}{C.RESET}")
    print()
    print(f"  {C.BOLD}0.{C.RESET} Вернуться в главное меню")
    print()

    choice = ask("Введите номер для изменения", "0")
    if choice == '0':
        return
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(fields):
            key, label = fields[idx]
            new_val = ask(label, sess.cfg.get(key, ''))
            sess.cfg[key] = new_val
            save_settings(sess.cfg)
            print(C.ok(f"Сохранено: {label} = {new_val}"))
    except ValueError:
        print(C.err("Некорректный ввод"))
    pause()
    menu_settings()


# ═══════════════════════════════════════════════════════════════════
#  ШАГ 1: ИЗВЛЕЧЕНИЕ
# ═══════════════════════════════════════════════════════════════════

def step1_extract():
    clear()
    print(C.header("ШАГ 1: Извлечение таблиц из исходных файлов"))
    inp = sess.cfg['input_dir']
    tmp = sess.cfg['temp_dir']
    os.makedirs(tmp, exist_ok=True)

    sess.input_files = find_excel(inp)
    if not sess.input_files:
        print(C.err(f"Нет Excel-файлов в: {inp}"))
        print(C.info(f"Положите файлы в папку и попробуйте снова."))
        pause()
        return False

    print(C.info(f"Найдено файлов: {len(sess.input_files)}"))
    print()

    sess.extracted_files = []
    ok_count = 0
    for i, fp in enumerate(sess.input_files, 1):
        fname = os.path.basename(fp)
        print(f"  [{i}/{len(sess.input_files)}] {fname}...", end=" ", flush=True)
        try:
            res = extract_tables(fp)
            out_path = os.path.join(tmp, fname)
            if save_combined(res, fp, out_path):
                sess.extracted_files.append(out_path)
                ok_count += 1
                counts = f"GFD={res.gfd_rows} Усл={res.contract_rows} Прод={res.sales_rows} Инв={res.invest_rows}"
                print(C.ok(counts))
            else:
                print(C.err("не удалось сохранить"))
            for w in res.warnings:
                print(f"     {C.warn(w)}")
            for e in res.errors:
                print(f"     {C.err(e)}")
        except Exception as e:
            print(C.err(str(e)))

    print(f"\n{C.BOLD}Итого:{C.RESET} {ok_count}/{len(sess.input_files)} успешно")
    sess.add_log(f"Шаг 1: {ok_count}/{len(sess.input_files)} файлов извлечено")
    pause()
    return ok_count > 0


# ═══════════════════════════════════════════════════════════════════
#  ШАГ 2: ПАРСИНГ
# ═══════════════════════════════════════════════════════════════════

def step2_parse():
    clear()
    print(C.header("ШАГ 2: Парсинг в структурированный формат"))
    tmp = sess.cfg['temp_dir']

    files = sess.extracted_files
    if not files:
        files = find_excel(tmp)
    if not files:
        print(C.err("Нет файлов для парсинга. Сначала выполните Шаг 1."))
        pause()
        return False

    print(C.info(f"Файлов для парсинга: {len(files)}"))
    print()

    sess.parsed_dfs = []
    ok_count = 0
    for i, fp in enumerate(files, 1):
        fname = os.path.basename(fp)
        print(f"  [{i}/{len(files)}] {fname}...", end=" ", flush=True)
        try:
            res = parse_file(fp)
            if res.df is not None and not res.df.empty:
                sess.parsed_dfs.append(res.df)
                ok_count += 1
                print(C.ok(f"{res.rows} строк, {res.skus_found} SKU"))
                # сохраним промежуточный
                base = os.path.splitext(fname)[0]
                res.df.to_excel(os.path.join(tmp, f"{base}_parsed.xlsx"),
                                index=False, sheet_name='Результаты')
            else:
                reason = '; '.join(res.errors + res.warnings) or 'нет данных'
                print(C.warn(reason))
            for w in res.warnings:
                if w not in reason if 'reason' in dir() else True:
                    print(f"     {C.warn(w)}")
            for e in res.errors:
                print(f"     {C.err(e)}")
        except Exception as e:
            print(C.err(str(e)))

    print(f"\n{C.BOLD}Итого:{C.RESET} {ok_count}/{len(files)} успешно")
    sess.add_log(f"Шаг 2: {ok_count}/{len(files)} файлов распарсено")
    pause()
    return ok_count > 0


# ═══════════════════════════════════════════════════════════════════
#  ШАГ 3: ОБЪЕДИНЕНИЕ
# ═══════════════════════════════════════════════════════════════════

def step3_merge():
    clear()
    print(C.header("ШАГ 3: Объединение всех данных"))

    if not sess.parsed_dfs:
        print(C.err("Нет данных для объединения. Сначала выполните Шаг 2."))
        pause()
        return False

    print(C.info(f"DataFrame для объединения: {len(sess.parsed_dfs)}"))

    existing = sess.cfg.get('existing_db', '')
    if existing:
        print(C.info(f"Существующая база: {existing}"))

    res = do_merge(sess.parsed_dfs, existing_db=existing if existing else None)
    if res.errors:
        for e in res.errors:
            print(C.err(e))
    if res.df.empty:
        print(C.err("После объединения нет данных"))
        pause()
        return False

    sess.merged_df = res.df
    print(C.ok(f"Объединено: {res.total_output} строк (дубликатов удалено: {res.duplicates_removed})"))

    # сохраняем
    out_dir = sess.cfg['output_dir']
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    merged_path = os.path.join(out_dir, f"merged_{ts}.xlsx")
    sess.merged_df.to_excel(merged_path, index=False)
    print(C.ok(f"Сохранено: {merged_path}"))

    # валидация
    print(f"\n{C.BOLD}Запускаем валидацию...{C.RESET}")
    issues = validate(sess.merged_df)
    crit = sum(1 for i in issues if i.sev == "КРИТИЧЕСКАЯ")
    warn = sum(1 for i in issues if i.sev == "ПРЕДУПРЕЖДЕНИЕ")
    info = sum(1 for i in issues if i.sev == "ИНФОРМАЦИЯ")
    print(f"  Найдено проблем: {C.RED}{crit} крит.{C.RESET}  "
          f"{C.YELLOW}{warn} предупр.{C.RESET}  "
          f"{C.CYAN}{info} инфо{C.RESET}")
    if issues:
        vpath = os.path.join(out_dir, f"validation_{ts}.xlsx")
        save_validation(issues, vpath)
        sess.validation_path = vpath
        print(C.ok(f"Отчёт валидации: {vpath}"))

    sess.add_log(f"Шаг 3: {res.total_output} строк, {len(issues)} проблем")
    pause()
    return True


# ═══════════════════════════════════════════════════════════════════
#  ШАГ 4: ИТОГОВЫЙ ОТЧЁТ
# ═══════════════════════════════════════════════════════════════════

def step4_report():
    clear()
    print(C.header("ШАГ 4: Генерация итогового P&L-отчёта"))

    if sess.merged_df is None or sess.merged_df.empty:
        print(C.err("Нет объединённых данных. Сначала выполните Шаг 3."))
        pause()
        return False

    out_dir = sess.cfg['output_dir']
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(out_dir, f"PnL_report_{ts}.xlsx")

    print(C.info(f"Строк в базе: {len(sess.merged_df)}"))
    extras = {
        'sales_path': sess.cfg.get('sales_file', ''),
        'costs_np_path': sess.cfg.get('costs_not_price_file', ''),
        'costs_ip_path': sess.cfg.get('costs_in_price_file', ''),
        'cm_path': sess.cfg.get('cm_file', ''),
        'cogs_path': sess.cfg.get('cogs_file', ''),
    }
    has_extras = any(v for v in extras.values())
    if has_extras:
        print(C.info("Дополнительные источники:"))
        for k, v in extras.items():
            if v:
                print(f"    {k}: {v}")
    else:
        print(C.info("Без дополнительных источников (только плановые данные)"))

    print(f"\n  Генерация...", end=" ", flush=True)
    try:
        path = generate_report(sess.merged_df, report_path,
                               **{k: (v if v else None) for k, v in extras.items()})
        sess.report_path = path
        print(C.ok("Готово!"))
        print(f"\n  {C.BOLD}{C.GREEN}Итоговый файл: {path}{C.RESET}")
    except Exception as e:
        print(C.err(str(e)))
        traceback.print_exc()
        pause()
        return False

    sess.add_log(f"Шаг 4: отчёт сохранён {report_path}")
    pause()
    return True


# ═══════════════════════════════════════════════════════════════════
#  ЗАПУСК ВСЕГО ПАЙПЛАЙНА
# ═══════════════════════════════════════════════════════════════════

def run_all():
    clear()
    print(C.header("ПОЛНАЯ ОБРАБОТКА — все шаги подряд"))
    print()
    start = time.time()

    print(f"{C.BOLD}▶ Шаг 1 / 4: Извлечение...{C.RESET}")
    if not step1_extract_silent():
        print(C.err("Шаг 1 не удался. Прерываем."))
        pause()
        return

    print(f"\n{C.BOLD}▶ Шаг 2 / 4: Парсинг...{C.RESET}")
    if not step2_parse_silent():
        print(C.err("Шаг 2 не удался. Прерываем."))
        pause()
        return

    print(f"\n{C.BOLD}▶ Шаг 3 / 4: Объединение + Валидация...{C.RESET}")
    if not step3_merge_silent():
        print(C.err("Шаг 3 не удался. Прерываем."))
        pause()
        return

    print(f"\n{C.BOLD}▶ Шаг 4 / 4: Генерация отчёта...{C.RESET}")
    step4_report_silent()

    elapsed = time.time() - start
    print(f"\n{C.header('ГОТОВО!')}")
    print(f"  Время: {elapsed:.1f} сек")
    if sess.report_path:
        print(f"  Отчёт:     {C.GREEN}{sess.report_path}{C.RESET}")
    if sess.validation_path:
        print(f"  Валидация: {C.YELLOW}{sess.validation_path}{C.RESET}")
    pause()


# ─── «тихие» версии шагов (для run_all) ───

def step1_extract_silent():
    inp = sess.cfg['input_dir']
    tmp = sess.cfg['temp_dir']
    os.makedirs(tmp, exist_ok=True)
    sess.input_files = find_excel(inp)
    if not sess.input_files:
        print(C.err(f"Нет файлов в: {inp}"))
        return False
    sess.extracted_files = []
    ok = 0
    for i, fp in enumerate(sess.input_files, 1):
        fname = os.path.basename(fp)
        print(f"  [{i}/{len(sess.input_files)}] {fname}...", end=" ", flush=True)
        try:
            res = extract_tables(fp)
            out_path = os.path.join(tmp, fname)
            if save_combined(res, fp, out_path):
                sess.extracted_files.append(out_path)
                ok += 1
                print(C.ok(f"GFD={res.gfd_rows} Усл={res.contract_rows} Прод={res.sales_rows} Инв={res.invest_rows}"))
            else:
                print(C.err("ошибка"))
        except Exception as e:
            print(C.err(str(e)))
    print(f"  Извлечено: {ok}/{len(sess.input_files)}")
    return ok > 0


def step2_parse_silent():
    files = sess.extracted_files or find_excel(sess.cfg['temp_dir'])
    if not files:
        return False
    sess.parsed_dfs = []
    ok = 0
    for i, fp in enumerate(files, 1):
        fname = os.path.basename(fp)
        print(f"  [{i}/{len(files)}] {fname}...", end=" ", flush=True)
        try:
            res = parse_file(fp)
            if res.df is not None and not res.df.empty:
                sess.parsed_dfs.append(res.df)
                ok += 1
                print(C.ok(f"{res.rows} строк"))
            else:
                print(C.warn('; '.join(res.errors + res.warnings)[:60] or 'нет данных'))
        except Exception as e:
            print(C.err(str(e)[:60]))
    print(f"  Распарсено: {ok}/{len(files)}")
    return ok > 0


def step3_merge_silent():
    if not sess.parsed_dfs:
        return False
    existing = sess.cfg.get('existing_db', '')
    res = do_merge(sess.parsed_dfs, existing_db=existing if existing else None)
    if res.df.empty:
        return False
    sess.merged_df = res.df
    out_dir = sess.cfg['output_dir']
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    sess.merged_df.to_excel(os.path.join(out_dir, f"merged_{ts}.xlsx"), index=False)
    print(f"  Объединено: {res.total_output} строк (дубл. удалено: {res.duplicates_removed})")
    issues = validate(sess.merged_df)
    crit = sum(1 for i in issues if i.sev == "КРИТИЧЕСКАЯ")
    print(f"  Валидация: {len(issues)} проблем ({crit} крит.)")
    if issues:
        vp = os.path.join(out_dir, f"validation_{ts}.xlsx")
        save_validation(issues, vp)
        sess.validation_path = vp
    return True


def step4_report_silent():
    if sess.merged_df is None or sess.merged_df.empty:
        return False
    out_dir = sess.cfg['output_dir']
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    rp = os.path.join(out_dir, f"PnL_report_{ts}.xlsx")
    extras = {
        'sales_path': sess.cfg.get('sales_file', '') or None,
        'costs_np_path': sess.cfg.get('costs_not_price_file', '') or None,
        'costs_ip_path': sess.cfg.get('costs_in_price_file', '') or None,
        'cm_path': sess.cfg.get('cm_file', '') or None,
        'cogs_path': sess.cfg.get('cogs_file', '') or None,
    }
    try:
        sess.report_path = generate_report(sess.merged_df, rp, **extras)
        print(f"  Отчёт: {sess.report_path}")
        return True
    except Exception as e:
        print(C.err(str(e)))
        return False


# ═══════════════════════════════════════════════════════════════════
#  ГЛАВНОЕ МЕНЮ
# ═══════════════════════════════════════════════════════════════════

def main_menu():
    while True:
        clear()
        banner()
        show_status()

        print(f"  {C.BOLD}── Действия ──{C.RESET}")
        print(f"  {C.CYAN}1.{C.RESET} Настройка путей")
        print(f"  {C.CYAN}2.{C.RESET} Шаг 1: Извлечение таблиц из исходников")
        print(f"  {C.CYAN}3.{C.RESET} Шаг 2: Парсинг в структурированный формат")
        print(f"  {C.CYAN}4.{C.RESET} Шаг 3: Объединение + Валидация")
        print(f"  {C.CYAN}5.{C.RESET} Шаг 4: Генерация итогового P&L-отчёта")
        print()
        print(f"  {C.GREEN}{C.BOLD}0.{C.RESET} {C.GREEN}▶▶ ЗАПУСТИТЬ ВСЁ (шаги 1→4){C.RESET}")
        print()
        print(f"  {C.DIM}q. Выход{C.RESET}")
        print()

        choice = ask("Выберите действие", "0")

        if choice == '1':
            menu_settings()
        elif choice == '2':
            step1_extract()
        elif choice == '3':
            step2_parse()
        elif choice == '4':
            step3_merge()
        elif choice == '5':
            step4_report()
        elif choice == '0':
            run_all()
        elif choice.lower() in ('q', 'й', 'quit', 'exit'):
            print(f"\n{C.DIM}До свидания!{C.RESET}\n")
            break
        else:
            print(C.err("Неизвестная команда"))
            pause()


# ═══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    try:
        main_menu()
    except KeyboardInterrupt:
        print(f"\n{C.DIM}Прервано.{C.RESET}")
    except Exception as e:
        print(C.err(f"Критическая ошибка: {e}"))
        traceback.print_exc()
        sys.exit(1)
