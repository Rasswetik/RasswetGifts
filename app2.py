# -*- coding: utf-8 -*-
"""
fix_app.py — автоматическое исправление app.py
Исправляет:
1. Зависание краша на 1 раунде (после do_crash не создаётся новая игра)
2. Добавляет корректную обработку live_status == 'crashed'

Запуск: python fix_app.py
"""

import os
import re
import shutil
from datetime import datetime

APP_FILE = 'app.py'
BACKUP_SUFFIX = '.backup_' + datetime.now().strftime('%Y%m%d_%H%M%S')


def backup_file(path):
    """Создаёт резервную копию файла"""
    backup_path = path + BACKUP_SUFFIX
    shutil.copy2(path, backup_path)
    print(f"💾 Резервная копия: {backup_path}")
    return backup_path


def fix_crash_loop(content):
    """
    Исправляет игровой цикл краша.
    
    Проблема: после do_crash() переменная live_status остаётся 'crashed',
    а в начале следующей итерации нет обработки этого статуса — цикл
    заходит в блок "НЕ FLYING — читаем состояние из БД", находит в БД
    ту же самую игру со статусом 'crashed', не находит активной игры
    и... ничего не делает. Игра зависает.
    
    Решение: добавить в начало цикла проверку live_status == 'crashed'
    с паузой 2.5 сек и сбросом в 'none', чтобы цикл создал новую игру.
    """
    
    # Ищем начало главного цикла while True внутри game_loop
    # Маркер: "# ─── MAIN LOOP ───"
    marker = "# ─── MAIN LOOP ───"
    if marker not in content:
        print("⚠️  Не найден маркер '# ─── MAIN LOOP ───' — патч краша пропущен")
        return content, False
    
    # Находим строку "while True:" после маркера
    idx_marker = content.find(marker)
    idx_while = content.find("while True:", idx_marker)
    if idx_while == -1:
        print("⚠️  Не найден 'while True:' после маркера — патч краша пропущен")
        return content, False
    
    # Находим начало тела цикла — первую строку с отступом после "while True:"
    after_while = content.find("\n", idx_while) + 1
    # Пропускаем пустые строки
    while after_while < len(content) and content[after_while] in ' \t\r\n':
        after_while += 1
    
    # Определяем отступ тела цикла
    body_indent = ''
    j = after_while
    while j < len(content) and content[j] in ' \t':
        body_indent += content[j]
        j += 1
    
    if not body_indent:
        print("⚠️  Не удалось определить отступ тела цикла — патч краша пропущен")
        return content, False
    
    # Проверяем, не был ли уже применён патч
    check_region = content[after_while:after_while + 2000]
    if "live_status == 'crashed'" in check_region or 'live_status == "crashed"' in check_region:
        print("ℹ️  Патч краша уже применён — пропуск")
        return content, False
    
    # Формируем патч — блок обработки crashed в начале цикла
    patch = (
        f"{body_indent}# ═══════════════════════════════════════════════════════════════\n"
        f"{body_indent}# CRASHED — пауза и сброс, чтобы цикл создал новую игру\n"
        f"{body_indent}# (ФИКС: игра зависала после первого краша)\n"
        f"{body_indent}# ═══════════════════════════════════════════════════════════════\n"
        f"{body_indent}if live_status == 'crashed':\n"
        f"{body_indent}    time.sleep(2.5)\n"
        f"{body_indent}    live_status = 'none'\n"
        f"{body_indent}    live_game_id = 0\n"
        f"{body_indent}    live_mult = 1.0\n"
        f"{body_indent}    live_is_bonus = False\n"
        f"{body_indent}    tick_counter = 0\n"
        f"{body_indent}    continue\n\n"
    )
    
    # Вставляем патч в начало тела цикла
    new_content = content[:after_while] + patch + content[after_while:]
    print("✅ Патч краша применён (live_status == 'crashed')")
    return new_content, True


def fix_crash_reset_status(content):
    """
    Дополнительный фикс: в блоке обработки статуса 'crashed' из БД
    (когда цикл перезапустился) нужно сбрасывать live_status.
    
    Ищем блок с комментарием:
    # CRASHED — обычно уже обработан в flying-блоке
    или
    # ═══════════════════════════════════════════════════════════════
    # НЕ FLYING — читаем состояние из БД
    """
    
    # Ищем фрагмент, где обрабатывается game = cursor.fetchone() и status == 'crashed'
    # Проще: найти строку "live_status = 'none'" в блоке "НЕТ АКТИВНОЙ ИГРЫ"
    marker = "# НЕТ АКТИВНОЙ ИГРЫ"
    if marker not in content:
        print("ℹ️  Маркер 'НЕТ АКТИВНОЙ ИГРЫ' не найден — пропуск вторичного фикса")
        return content, False
    
    # В этом блоке уже есть live_status = 'none' — это нормально.
    # Ничего дополнительно не нужно.
    print("ℹ️  Вторичный фикс краша не требуется")
    return content, False


def main():
    print("=" * 60)
    print("🔧 fix_app.py — исправление app.py")
    print("=" * 60)
    
    if not os.path.exists(APP_FILE):
        print(f"❌ Файл {APP_FILE} не найден в текущей директории!")
        print(f"   Текущая директория: {os.getcwd()}")
        return False
    
    # Читаем файл
    with open(APP_FILE, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    applied = []
    
    # Применяем патчи
    content, changed = fix_crash_loop(content)
    if changed:
        applied.append("crash_loop")
    
    content, changed = fix_crash_reset_status(content)
    if changed:
        applied.append("crash_reset_status")
    
    if not applied:
        print("\nℹ️  Никаких изменений не применено")
        print("   (возможно, патчи уже применены)")
        return True
    
    # Делаем резервную копию и сохраняем
    backup_file(APP_FILE)
    
    with open(APP_FILE, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"\n✅ Применены патчи: {', '.join(applied)}")
    print(f"✅ Файл {APP_FILE} обновлён")
    print("\n📋 Что дальше:")
    print("   1. Перезапустите сервер (Ctrl+C → python app.py)")
    print("   2. Проверьте краш — должен работать без зависаний")
    print("   3. Если что-то не так — восстановите из резервной копии")
    return True


if __name__ == '__main__':
    try:
        success = main()
        exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        exit(1)