#!/usr/bin/env python3
"""
sync_all.py — Полная синхронизация каталога подарков.

Скачивает с Fragment:
  • 107 коллекций (оригиналы) — названия + PNG изображения
  • ~6900 моделей — уникальные PNG для каждой модели
  • Fragment floor-цены

Скачивает с GetGems (через tonapi.io):
    • Актуальные рыночные floor-цены для всех коллекций
    • Отдельные floor-цены для моделей (по активным листингам GetGems)

Результат: data/fragment_catalog_cache.json (~4 MB)

Запуск:
  python sync_all.py              — полная синхронизация (Fragment + GetGems)
  python sync_all.py --fast       — только Fragment (без GetGems цен, быстрее)

После синхронизации загрузите файл на PythonAnywhere:
  scp data/fragment_catalog_cache.json rasswetik52@ssh.pythonanywhere.com:/home/rasswetik52/mysite/data/

Или вручную через Files → Upload на pythonanywhere.com
"""

import subprocess
import sys
import os
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SYNC_SCRIPT = os.path.join(SCRIPT_DIR, 'sync_fragment.py')
MODEL_PRICES_SCRIPT = os.path.join(SCRIPT_DIR, 'sync_model_prices.py')
CACHE_FILE = os.path.join(SCRIPT_DIR, 'data', 'fragment_catalog_cache.json')

def main():
    fast_mode = '--fast' in sys.argv or '--skip-getgems' in sys.argv

    print("=" * 60)
    print("  🎁 RasswetGifts — Полная синхронизация каталога")
    print("=" * 60)
    print()
    print("  Источники:")
    print("    • Fragment.com  → изображения, названия, модели")
    print("    • GetGems.io    → актуальные рыночные цены")
    if fast_mode:
        print()
        print("  ⚡ БЫСТРЫЙ РЕЖИМ: без GetGems цен")
    print()
    print("  Это займёт ~5-10 минут (полный) или ~2-3 минуты (быстрый)")
    print("=" * 60)
    print()

    # Check dependencies
    try:
        import requests
    except ImportError:
        print("❌ Не установлен модуль requests. Устанавливаю...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests'])
        print()

    # Run sync_fragment.py
    args = [sys.executable, '-u', SYNC_SCRIPT]
    if fast_mode:
        args.append('--skip-getgems')

    start = time.time()
    result = subprocess.run(args, cwd=SCRIPT_DIR)
    elapsed = time.time() - start

    print()
    if result.returncode != 0:
        print(f"❌ Ошибка синхронизации (код {result.returncode})")
        sys.exit(1)

    # Run sync_model_prices.py (per-model getgems prices)
    if not fast_mode and os.path.exists(MODEL_PRICES_SCRIPT):
        print()
        print("=" * 60)
        print("  🔍 Определение цен моделей на GetGems...")
        print("=" * 60)
        print()
        model_args = [sys.executable, '-u', MODEL_PRICES_SCRIPT]
        result2 = subprocess.run(model_args, cwd=SCRIPT_DIR)
        if result2.returncode != 0:
            print(f"⚠️ sync_model_prices завершился с ошибкой (код {result2.returncode})")
            print("   Коллекционные цены сохранены, модельные цены частично.")
        elapsed = time.time() - start

    # Show summary
    if os.path.exists(CACHE_FILE):
        import json
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            gifts = data.get('gifts', [])
            models = data.get('models', {})
            total_models = sum(len(v) for v in models.values())
            with_gg = sum(1 for g in gifts if g.get('getgems_floor_ton'))
            models_with_price = sum(
                1 for ml in models.values() for m in ml
                if m.get('getgems_model_floor_ton') is not None
            )
            size_kb = os.path.getsize(CACHE_FILE) / 1024

            print("=" * 60)
            print("  ✅ СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА")
            print("=" * 60)
            print(f"  Время:            {elapsed:.0f} сек")
            print(f"  Коллекций:        {len(gifts)}")
            print(f"  Моделей:          {total_models}")
            print(f"  GetGems цен:      {with_gg}/{len(gifts)} коллекций")
            print(f"  Модели с ценой:   {models_with_price}/{total_models}")
            print(f"  Файл:             {CACHE_FILE}")
            print(f"  Размер:           {size_kb:.0f} KB")
            print()
            print("  📤 ЗАГРУЗИТЕ НА СЕРВЕР:")
            print("  Путь на PythonAnywhere:")
            print("    /home/rasswetik52/mysite/data/fragment_catalog_cache.json")
            print()
            print("  Также загрузите обновлённые файлы:")
            print("    • app.py")
            print("    • templates/admin.html")
            print("    • templates/base.html")
            print("    • templates/case.html")
            print("    • templates/inventory.html")
            print("    • templates/upgrade.html")
            print("=" * 60)
        except Exception as e:
            print(f"  Файл создан: {CACHE_FILE}")
            print(f"  (ошибка чтения статистики: {e})")
    else:
        print(f"⚠️ Файл кеша не найден: {CACHE_FILE}")

if __name__ == '__main__':
    main()
