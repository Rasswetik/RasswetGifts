#!/usr/bin/env python3
"""
sync_model_prices.py — обновляет цены ВСЕХ моделей подарков по активным продажам GetGems.

Источник цен:
  TonAPI /v2/nfts/collections/{address}/items
  берём только NFT c sale.market, содержащим "getgems"
  и считаем floor по каждому атрибуту Model.

Запуск:
  python sync_model_prices.py
  python sync_model_prices.py --collection artisanbrick
  python sync_model_prices.py --resume
  python sync_model_prices.py --dry-run
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

TON_RATE = int(os.getenv("FRAGMENT_TON_RATE", "100"))
CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "fragment_catalog_cache.json")

TIMEOUT = 25
DELAY = 0.15
PAGE_LIMIT = 1000
MAX_EMPTY_PAGES = 1

DRY_RUN = '--dry-run' in sys.argv
RESUME = '--resume' in sys.argv
ONLY_COLLECTION = None
for i, arg in enumerate(sys.argv):
    if arg == '--collection' and i + 1 < len(sys.argv):
        ONLY_COLLECTION = str(sys.argv[i + 1]).strip().lower()

S = requests.Session()
S.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
})


def slugify(text):
    return re.sub(r'[^a-z0-9]+', '', str(text or '').lower())


def to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def load_cache(path):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_cache(path, payload):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def extract_model_name(item):
    attrs = ((item or {}).get('metadata') or {}).get('attributes') or []
    for attr in attrs:
        trait = str(attr.get('trait_type', '')).strip().lower()
        if trait == 'model':
            return str(attr.get('value', '')).strip()
    return ''


def extract_market_name(item):
    sale = (item or {}).get('sale') or {}
    market = sale.get('market') or {}
    name = str(market.get('name') or '').strip()
    if name:
        return name
    return str(market.get('address') or '').strip()


def extract_price_ton(item):
    sale = (item or {}).get('sale') or {}
    price = sale.get('price') or {}
    raw = price.get('value')
    if raw is None:
        return None
    try:
        return float(raw) / 1e9
    except Exception:
        return None


def fetch_collection_items(address):
    items = []
    offset = 0
    empty_pages = 0

    while True:
        url = f'https://tonapi.io/v2/nfts/collections/{address}/items?limit={PAGE_LIMIT}&offset={offset}'
        time.sleep(DELAY)
        r = S.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f'TonAPI {r.status_code} for {address}')

        data = r.json()
        page_items = data.get('nft_items') or data.get('items') or []
        if not page_items:
            empty_pages += 1
            if empty_pages > MAX_EMPTY_PAGES:
                break
            offset += PAGE_LIMIT
            continue

        items.extend(page_items)
        if len(page_items) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT

    return items


def compute_model_floors_from_items(items):
    floors = {}
    for item in items:
        sale = item.get('sale')
        if not sale:
            continue

        market_name = extract_market_name(item).lower()
        if 'getgems' not in market_name:
            continue

        model_name = extract_model_name(item)
        if not model_name:
            continue

        ton_price = extract_price_ton(item)
        if ton_price is None or ton_price <= 0:
            continue

        prev = floors.get(model_name)
        if prev is None or ton_price < prev:
            floors[model_name] = ton_price

    return floors


def apply_model_prices(models, model_floor_map):
    updated = 0
    no_sale = 0
    not_found = 0

    normalized_map = {slugify(k): (k, v) for k, v in model_floor_map.items()}

    for model in models:
        model_name = str(model.get('model_name') or '').strip()
        if not model_name:
            continue

        model_key = slugify(model_name)
        match = normalized_map.get(model_key)

        if not match:
            not_found += 1
            model['getgems_model_price_source'] = 'not_found_in_listings'
            continue

        floor_ton = to_float(match[1], 0.0)
        if floor_ton <= 0:
            no_sale += 1
            model['getgems_model_price_source'] = 'no_sale'
            continue

        model['getgems_model_floor_ton'] = round(floor_ton, 4)
        model['getgems_floor_ton'] = round(floor_ton, 4)
        model['value'] = int(round(floor_ton * TON_RATE))
        model['getgems_model_price_source'] = 'tonapi_getgems_listings'
        updated += 1

    return updated, no_sale, not_found


def main():
    print('=' * 60)
    print('  GetGems Model Price Sync (TonAPI listings)')
    print('=' * 60)

    if not os.path.exists(CACHE_FILE):
        print(f'Ш: файл не найден: {CACHE_FILE}')
        print('Сначала выполните: python sync_fragment.py')
        sys.exit(1)

    cache = load_cache(CACHE_FILE)
    gifts = cache.get('gifts', [])
    models_by_slug = cache.get('models', {})

    gift_by_slug = {}
    for gift in gifts:
        slug = str(gift.get('fragment_slug') or '').strip().lower()
        if slug:
            gift_by_slug[slug] = gift

    if ONLY_COLLECTION:
        slugs = [ONLY_COLLECTION] if ONLY_COLLECTION in models_by_slug else []
    else:
        slugs = [s for s in models_by_slug.keys() if s in gift_by_slug]

    if not slugs:
        print('Нет коллекций для обработки')
        sys.exit(0)

    total_models = sum(len(models_by_slug.get(s, [])) for s in slugs)
    print(f'Коллекций: {len(slugs)}')
    print(f'Моделей:   {total_models}')
    if RESUME:
        print('(режим --resume включён)')
    if DRY_RUN:
        print('(режим --dry-run: без сохранения)')

    stats = {
        'collections_ok': 0,
        'collections_fail': 0,
        'models_updated': 0,
        'models_skipped': 0,
        'models_no_sale': 0,
        'models_not_found': 0,
    }

    for idx, slug in enumerate(slugs, 1):
        gift = gift_by_slug.get(slug) or {}
        name = gift.get('name') or slug
        address = gift.get('ton_address')
        models = models_by_slug.get(slug) or []

        if not address:
            print(f'[{idx}/{len(slugs)}] {name}: пропуск (нет ton_address)')
            stats['collections_fail'] += 1
            continue

        if RESUME:
            active_models = [m for m in models if m.get('getgems_model_floor_ton') is None]
            stats['models_skipped'] += max(0, len(models) - len(active_models))
        else:
            active_models = models

        if not active_models:
            print(f'[{idx}/{len(slugs)}] {name}: пропуск (все модели уже с ценой)')
            continue

        print(f'[{idx}/{len(slugs)}] {name} ({len(models)} моделей) ...', end=' ', flush=True)

        try:
            items = fetch_collection_items(address)
            floors = compute_model_floors_from_items(items)
            upd, no_sale, not_found = apply_model_prices(active_models, floors)
            stats['collections_ok'] += 1
            stats['models_updated'] += upd
            stats['models_no_sale'] += no_sale
            stats['models_not_found'] += not_found
            print(f'OK: listings={len(items)} floors={len(floors)} updated={upd}')
        except Exception as err:
            stats['collections_fail'] += 1
            print(f'ERR: {err}')

    cache['model_prices_updated_at'] = datetime.now(timezone.utc).isoformat()

    if not DRY_RUN:
        save_cache(CACHE_FILE, cache)

    total_with_price = 0
    total_models_all = 0
    for mlist in models_by_slug.values():
        for model in mlist:
            total_models_all += 1
            if model.get('getgems_model_floor_ton') is not None:
                total_with_price += 1

    print('-' * 60)
    print(f"Коллекций OK:             {stats['collections_ok']}")
    print(f"Коллекций с ошибкой:      {stats['collections_fail']}")
    print(f"Моделей обновлено:        {stats['models_updated']}")
    print(f"Моделей пропущено:        {stats['models_skipped']}")
    print(f"Моделей без продажи:      {stats['models_no_sale']}")
    print(f"Моделей не найдено в sale:{stats['models_not_found']}")
    print(f'Итого моделей с ценой:    {total_with_price}/{total_models_all}')
    if not DRY_RUN:
        print(f'Файл обновлён: {CACHE_FILE}')
    print('=' * 60)


if __name__ == '__main__':
    main()
