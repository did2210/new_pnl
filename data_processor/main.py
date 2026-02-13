#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=======================================================================
  Автоматизированная обработка контрактов GFD
  Объединение, парсинг, валидация и генерация отчётов
=======================================================================

Использование:
  python main.py                         -- обработать файлы из папки input/
  python main.py --input /путь/к/файлам  -- указать папку с файлами
  python main.py --file файл.xlsx        -- обработать один файл
  python main.py --skip-extract          -- пропустить шаг 1 (файлы уже обработаны)
  python main.py --skip-validation       -- пропустить валидацию
  python main.py --existing-db база.xlsx -- объединить с существующей базой

Полный пайплайн:
  1. Извлечение таблиц из исходных файлов (step1_extract)
  2. Парсинг в структурированный формат (step2_parse)
  3. Объединение всех файлов (step3_merge)
  4. Валидация данных (validators)
  5. Генерация итогового Excel отчёта (step4_report)
"""
import os
import sys
import time
import logging
import argparse
import traceback
from datetime import datetime

# Добавляем текущую директорию в path для импортов
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    INPUT_DIR, OUTPUT_DIR, TEMP_DIR, SUPPORTED_EXTENSIONS,
    LOG_FORMAT, LOG_FILE
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def setup_argparser():
    """Настройка аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description='Автоматизированная обработка контрактов GFD',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python main.py                                    # Обработать все файлы из input/
  python main.py --input /путь/к/папке              # Указать свою папку
  python main.py --file contract.xlsx               # Один файл
  python main.py --skip-extract                     # Файлы уже обработаны (шаг 1)
  python main.py --existing-db merged_data.xlsx     # Объединить с базой
  python main.py --skip-validation                  # Без валидации
  python main.py --sales Sales.xlsx --cogs ss.xlsx  # С доп. данными для P&L
        """
    )
    parser.add_argument('--input', '-i', type=str, default=None,
                        help='Путь к папке с входными файлами (по умолчанию: input/)')
    parser.add_argument('--file', '-f', type=str, default=None,
                        help='Путь к одному файлу для обработки')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Путь к папке для выходных файлов (по умолчанию: output/)')
    parser.add_argument('--skip-extract', action='store_true',
                        help='Пропустить шаг извлечения (файлы уже обработаны)')
    parser.add_argument('--skip-validation', action='store_true',
                        help='Пропустить валидацию данных')
    parser.add_argument('--existing-db', type=str, default=None,
                        help='Путь к существующей базе данных для объединения')
    parser.add_argument('--sales', type=str, default=None,
                        help='Путь к файлу фактических продаж (для P&L)')
    parser.add_argument('--costs-not-price', type=str, default=None,
                        help='Путь к файлу затрат вне цены (для P&L)')
    parser.add_argument('--costs-in-price', type=str, default=None,
                        help='Путь к файлу затрат в цене (для P&L)')
    parser.add_argument('--cm', type=str, default=None,
                        help='Путь к файлу ЦМ (для P&L)')
    parser.add_argument('--cogs', type=str, default=None,
                        help='Путь к файлу себестоимости (для P&L)')
    return parser


def print_banner():
    """Выводит баннер приложения."""
    banner = """
╔══════════════════════════════════════════════════════════════════╗
║           АВТОМАТИЗИРОВАННАЯ ОБРАБОТКА КОНТРАКТОВ GFD           ║
║   Извлечение → Парсинг → Объединение → Валидация → Отчёт       ║
╚══════════════════════════════════════════════════════════════════╝
    """
    print(banner)


def find_excel_files(directory):
    """Находит все Excel файлы в директории."""
    if not os.path.exists(directory):
        logger.error(f"Директория не найдена: {directory}")
        return []

    files = []
    for f in os.listdir(directory):
        if f.lower().endswith(SUPPORTED_EXTENSIONS) and not f.startswith('~'):
            files.append(os.path.join(directory, f))

    files.sort()
    return files


def run_pipeline(args):
    """Запускает полный пайплайн обработки."""
    start_time = time.time()

    # Определяем директории
    input_dir = args.input if args.input else INPUT_DIR
    output_dir = args.output if args.output else OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)

    # Определяем входные файлы
    if args.file:
        if not os.path.exists(args.file):
            logger.error(f"Файл не найден: {args.file}")
            return False
        input_files = [args.file]
        logger.info(f"Режим: один файл — {args.file}")
    else:
        input_files = find_excel_files(input_dir)
        if not input_files:
            logger.error(f"Не найдено Excel файлов в {input_dir}")
            logger.info(f"Поместите файлы в папку: {input_dir}")
            return False
        logger.info(f"Найдено {len(input_files)} файлов в {input_dir}")

    # Импорты шагов (ленивые, чтобы не падать при отсутствии зависимостей)
    from step1_extract import process_raw_file
    from step2_parse import process_parsed_file
    from step3_merge import merge_dataframes
    from step4_report import generate_pnl_report, generate_simple_report
    from validators import validate_data

    success_files = []
    failed_files = []
    all_parsed_dfs = []

    total = len(input_files)

    for idx, file_path in enumerate(input_files, 1):
        file_name = os.path.basename(file_path)
        logger.info(f"\n{'='*60}")
        logger.info(f"[{idx}/{total}] Обработка: {file_name}")
        logger.info(f"{'='*60}")

        try:
            # ===== ШАГ 1: Извлечение таблиц =====
            if not args.skip_extract:
                logger.info("  Шаг 1: Извлечение таблиц из исходного файла...")
                extracted_path = process_raw_file(file_path, TEMP_DIR)
                if not extracted_path:
                    logger.warning(f"  Не удалось извлечь таблицы из {file_name}")
                    failed_files.append((file_name, "Ошибка извлечения"))
                    continue
                working_file = extracted_path
            else:
                working_file = file_path
                logger.info("  Шаг 1: Пропущен (--skip-extract)")

            # ===== ШАГ 2: Парсинг =====
            logger.info("  Шаг 2: Парсинг в структурированный формат...")
            df_result = process_parsed_file(working_file)

            if df_result is not None and not df_result.empty:
                all_parsed_dfs.append(df_result)
                success_files.append(file_name)
                logger.info(f"  Успешно: {len(df_result)} строк данных")

                # Сохраняем промежуточный результат
                base_name = os.path.splitext(file_name)[0]
                interim_path = os.path.join(TEMP_DIR, f"{base_name}_parsed.xlsx")
                df_result.to_excel(interim_path, index=False, sheet_name='Результаты')
                logger.info(f"  Промежуточный файл: {interim_path}")
            else:
                failed_files.append((file_name, "Нет данных после парсинга"))
                logger.warning(f"  Нет данных после парсинга {file_name}")

        except Exception as e:
            failed_files.append((file_name, str(e)))
            logger.error(f"  Ошибка при обработке {file_name}: {e}")
            traceback.print_exc()

    # ===== ШАГ 3: Объединение =====
    logger.info(f"\n{'='*60}")
    logger.info("Шаг 3: Объединение всех обработанных файлов...")
    logger.info(f"{'='*60}")

    if not all_parsed_dfs:
        logger.error("Нет данных для объединения. Все файлы завершились с ошибками.")
        _print_summary(success_files, failed_files, start_time)
        return False

    merged_df = merge_dataframes(all_parsed_dfs, existing_db_path=args.existing_db)
    logger.info(f"Объединено: {len(merged_df)} строк")

    # Сохраняем объединённые данные
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    merged_path = os.path.join(output_dir, f"merged_data_{timestamp}.xlsx")
    merged_df.to_excel(merged_path, index=False)
    logger.info(f"Объединённые данные сохранены: {merged_path}")

    # ===== ШАГ 4: Валидация =====
    if not args.skip_validation:
        logger.info(f"\n{'='*60}")
        logger.info("Шаг 4: Валидация данных...")
        logger.info(f"{'='*60}")

        validation_report_path = os.path.join(output_dir, f"validation_report_{timestamp}.xlsx")
        issues, report_path = validate_data(merged_df, validation_report_path)

        if issues:
            critical = sum(1 for i in issues if i.severity == "КРИТИЧЕСКАЯ")
            warnings = sum(1 for i in issues if i.severity == "ПРЕДУПРЕЖДЕНИЕ")
            infos = sum(1 for i in issues if i.severity == "ИНФОРМАЦИЯ")

            logger.info(f"  Найдено проблем: {len(issues)}")
            logger.info(f"    Критические: {critical}")
            logger.info(f"    Предупреждения: {warnings}")
            logger.info(f"    Информация: {infos}")
            if report_path:
                logger.info(f"  Отчёт валидации: {report_path}")
        else:
            logger.info("  Проблем не найдено!")
    else:
        logger.info("Шаг 4: Валидация пропущена (--skip-validation)")

    # ===== ШАГ 5: Генерация итогового отчёта =====
    logger.info(f"\n{'='*60}")
    logger.info("Шаг 5: Генерация итогового Excel отчёта...")
    logger.info(f"{'='*60}")

    report_path = os.path.join(output_dir, f"PnL_report_{timestamp}.xlsx")

    has_extra_data = any([args.sales, args.costs_not_price, args.costs_in_price, args.cm, args.cogs])

    if has_extra_data:
        result_path = generate_pnl_report(
            merged_df, report_path,
            sales_file=args.sales,
            costs_not_price_file=args.costs_not_price,
            costs_in_price_file=args.costs_in_price,
            cm_file=args.cm,
            cogs_file=args.cogs
        )
    else:
        result_path = generate_simple_report(merged_df, report_path)

    if result_path:
        logger.info(f"Итоговый отчёт: {result_path}")
    else:
        logger.error("Не удалось сгенерировать итоговый отчёт")

    # ===== ИТОГИ =====
    _print_summary(success_files, failed_files, start_time)

    return len(success_files) > 0


def _print_summary(success_files, failed_files, start_time):
    """Выводит итоговую статистику."""
    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print("ИТОГИ ОБРАБОТКИ")
    print(f"{'='*60}")
    print(f"  Время выполнения: {elapsed:.1f} сек")
    print(f"  Успешно обработано: {len(success_files)}")
    print(f"  С ошибками: {len(failed_files)}")

    if success_files:
        print(f"\n  Успешные файлы:")
        for fname in success_files:
            print(f"    + {fname}")

    if failed_files:
        print(f"\n  Файлы с ошибками:")
        for fname, error in failed_files:
            print(f"    - {fname}: {error}")

    print(f"\n  Результаты в папке: {OUTPUT_DIR}")
    print(f"{'='*60}\n")


def main():
    """Главная точка входа."""
    print_banner()

    parser = setup_argparser()
    args = parser.parse_args()

    logger.info(f"Запуск: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")

    try:
        success = run_pipeline(args)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nОбработка прервана пользователем")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
