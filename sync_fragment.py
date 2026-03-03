#!/usr/bin/env python3
"""
sync_fragment.py — Скачивает каталог подарков с Fragment + актуальные цены с GetGems.

Пайплайн:
  1. Fragment /gifts -> каталог коллекций (имена, изображения)
  2. Fragment /gifts/<slug> -> модели + Fragment floor-цена
  3. tonapi.io -> поиск TON-адреса коллекции по названию
  4. getgems.io/collection/<addr> -> актуальная floor-цена с маркетплейса

Запуск:  python sync_fragment.py
         python sync_fragment.py --skip-getgems   (без getgems цен)
Результат: data/fragment_catalog_cache.json

Загрузите файл на PythonAnywhere:
  /home/rasswetik52/mysite/data/fragment_catalog_cache.json
"""

import json, os, re, sys, time, html as html_lib
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    print("pip install requests"); sys.exit(1)

SKIP_GETGEMS = '--skip-getgems' in sys.argv

BASE = "https://fragment.com"
TIMEOUT = 15
DELAY = 0.3
TON_RATE = int(os.getenv("FRAGMENT_TON_RATE", "100"))
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "fragment_catalog_cache.json")

S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

def fetch(url):
    time.sleep(DELAY)
    r = S.get(url, timeout=TIMEOUT); r.raise_for_status(); return r.text

def slugify(n):
    return re.sub(r'[^a-z0-9]+', '', str(n or '').lower())

def absimg(s):
    s = (s or '').strip()
    if s.startswith('//'): return 'https:' + s
    if s.startswith('/'): return BASE + s
    return s

def si(v, d=0):
    try: return int(float(str(v).replace(',','').strip()))
    except: return d

def gid(slug, model):
    return f'fragment_model:{slugify(slug)}:{slugify(model)}'

# ── Parse catalog ─────────────────────────────────────────────────────────
def get_collections():
    print("[1/3] Каталог...", flush=True)
    html = fetch(f"{BASE}/gifts")
    # Split into <a> blocks by finding each href="/gifts/SLUG"
    # Then for each block extract name/count/image from the next ~600 chars
    gifts = []
    seen = set()
    for m in re.finditer(r'<a\s+href="/gifts/([a-z0-9_-]+)"', html):
        slug = m.group(1).lower()
        if slug in seen: continue
        seen.add(slug)
        chunk = html[m.start():m.start()+800]
        nm = re.search(r'class="[^"]*tm-main-filters-name[^"]*"[^>]*>([^<]+)', chunk)
        name = html_lib.unescape(nm.group(1).strip()) if nm else slug
        img_m = re.search(r'<img[^>]+src="([^"]+)"', chunk)
        image = absimg(img_m.group(1)) if img_m else f'{BASE}/file/gifts/{slug}/thumb.webp'
        gifts.append({
            'name': name,
            'fragment_slug': slug,
            'fragment_url': f'{BASE}/gifts/{slug}',
            'image': image,
        })
    print(f"    {len(gifts)} коллекций", flush=True)
    return gifts

# ── Parse collection page ─────────────────────────────────────────────────
def get_floor_price(html):
    """Min price from icon-ton values on the page."""
    vals = []
    for m in re.finditer(r'class="[^"]*icon-ton[^"]*">([^<]+)', html):
        v = m.group(1).strip().replace(',','')
        if v.isdigit(): vals.append(int(v))
    return min(vals) if vals else None

def get_models(slug, base_name, base_value, base_image, html):
    """Extract models from js-attribute-item blocks with /model. images."""
    if not html: return []
    models = []
    seen = set()
    # Find each js-attribute-item block, take ~500 chars after it
    for m in re.finditer(r'class="[^"]*js-attribute-item[^"]*"[^>]*data-value="([^"]*)"', html):
        data_val = m.group(1).strip()
        chunk = html[m.start():m.start()+600]
        # Only if chunk contains a model image (not backdrop/symbol)
        img_m = re.search(r'src="([^"]*?/model\.[^"]+)"', chunk)
        if not img_m: continue
        # Get display name
        nm = re.search(r'class="[^"]*tm-main-filters-name[^"]*"[^>]*>([^<]+)', chunk)
        model_name = (nm.group(1).strip() if nm else data_val)
        if not model_name: continue
        ms = slugify(model_name)
        if ms in seen: continue
        seen.add(ms)
        # Get count
        cnt = re.search(r'class="[^"]*tm-main-filters-count[^"]*"[^>]*>([^<]+)', chunk)
        count_val = si(cnt.group(1), 0) if cnt else 0
        image_url = absimg(img_m.group(1))
        full_name = f'{base_name} \u2022 {model_name}'
        models.append({
            'id': gid(slug, model_name),
            'gift_key': gid(slug, model_name),
            'name': full_name,
            'base_name': base_name,
            'model_name': model_name,
            'model_count': count_val,
            'type': 'fragment_model',
            'fragment_slug': slug,
            'fragment_url': f'{BASE}/gifts/{slug}',
            'image': image_url or base_image,
            'value': base_value,
        })
    return models

# ── GetGems / TonAPI price fetching ──────────────────────────────────────
def find_collection_address(name):
    """Search tonapi for a TG gift collection by name, return raw address."""
    try:
        r = S.get(
            f"https://tonapi.io/v2/accounts/search?name={requests.utils.quote(name)}",
            timeout=TIMEOUT
        )
        for a in r.json().get('addresses', []):
            aname = a.get('name', '')
            if 'collection' not in aname.lower():
                continue
            clean = aname.split('·')[0].strip().lower()
            if clean == name.lower() or name.lower() in clean or clean in name.lower():
                return a['address']
    except Exception:
        pass
    return None

def get_getgems_floor(address):
    """Fetch getgems.io/collection/{address}, extract floorPrice from SSR gqlCache."""
    try:
        r = S.get(f"https://getgems.io/collection/{address}", timeout=TIMEOUT)
        if r.status_code != 200:
            return None, None
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text)
        if not m:
            return None, None
        cache = json.loads(m.group(1)).get('props',{}).get('pageProps',{}).get('gqlCache',{})

        floor_price = None
        fragment_slug = None

        def search(obj):
            nonlocal floor_price, fragment_slug
            if isinstance(obj, dict):
                if 'floorPrice' in obj and obj['floorPrice'] is not None:
                    floor_price = obj['floorPrice']
                for link in (obj.get('socialLinks') or []):
                    m2 = re.search(r'fragment\.com/gifts/(\w+)', str(link))
                    if m2:
                        fragment_slug = m2.group(1)
                for v in obj.values():
                    search(v)
            elif isinstance(obj, list):
                for item in obj:
                    search(item)

        search(cache)
        return floor_price, fragment_slug
    except Exception:
        return None, None

def fetch_getgems_prices(gifts):
    """For each gift, find TON address via tonapi and floor price via getgems."""
    total = len(gifts)
    print(f"\n[3/4] GetGems цены ({total})...", flush=True)

    found = 0
    by_slug = {g['fragment_slug']: g for g in gifts}

    for i, g in enumerate(gifts, 1):
        name = g['name']
        slug = g['fragment_slug']
        sys.stdout.write(f"  [{i}/{total}] {name}... "); sys.stdout.flush()

        # Step A: find TON collection address via tonapi
        addr = find_collection_address(name)
        time.sleep(DELAY)
        if not addr:
            # Try with "s" suffix (e.g., "Toy Bear" -> "Toy Bears")
            addr = find_collection_address(name + 's')
            time.sleep(DELAY)
        if not addr:
            print("адрес не найден")
            continue

        g['ton_address'] = addr

        # Step B: get floor price from getgems SSR data
        floor, gg_slug = get_getgems_floor(addr)
        time.sleep(DELAY)

        if floor is not None and floor > 0:
            g['getgems_floor_ton'] = float(floor)
            found += 1
            print(f"floor={floor} TON", flush=True)
        else:
            print(f"floor=- (addr={addr[:20]}...)")

        # Verify slug mapping if available
        if gg_slug and gg_slug != slug:
            # Address mapped to a different slug — fix mapping
            if gg_slug in by_slug:
                pass  # Already processed
            else:
                g['getgems_mapped_slug'] = gg_slug

    print(f"    Найдено цен: {found}/{total}", flush=True)
    return found

# ── Main ──────────────────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("  Fragment Gift Catalog Sync")
    if SKIP_GETGEMS:
        print("  (--skip-getgems: без цен с GetGems)")
    print("=" * 50, flush=True)

    gifts = get_collections()
    if not gifts:
        print("FAIL: 0 коллекций"); sys.exit(1)

    all_models = {}
    total = len(gifts)
    print(f"\n[2/4] Fragment цены + модели ({total})...", flush=True)

    for i, g in enumerate(gifts, 1):
        slug = g['fragment_slug']
        sys.stdout.write(f"  [{i}/{total}] {g['name']}... "); sys.stdout.flush()
        try:
            html = fetch(f"{BASE}/gifts/{slug}")
        except Exception as e:
            print(f"ERR: {e}"); continue

        price = get_floor_price(html)
        if price is not None:
            g['fragment_price_ton'] = price
            g['value'] = int(round(price * TON_RATE))
            sys.stdout.write(f"floor={price}TON ")
        else:
            g['value'] = 0
            sys.stdout.write("floor=- ")

        models = get_models(slug, g['name'], g.get('value',0), g.get('image',''), html)
        if models:
            all_models[slug] = models
            print(f"{len(models)} моделей", flush=True)
        else:
            print("", flush=True)

    # ── GetGems prices (актуальные рыночные) ──
    gg_found = 0
    if not SKIP_GETGEMS:
        gg_found = fetch_getgems_prices(gifts)

        # Update value: prefer getgems floor, fallback to fragment
        for g in gifts:
            gg = g.get('getgems_floor_ton')
            if gg and gg > 0:
                g['value'] = int(round(gg * TON_RATE))
                # Also update model values with same getgems price
                slug = g['fragment_slug']
                for model in all_models.get(slug, []):
                    model['value'] = g['value']
                    model['getgems_floor_ton'] = gg

    # Save
    step = "4/4" if not SKIP_GETGEMS else "3/3"
    print(f"\n[{step}] Сохранение...", flush=True)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    payload = {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'gifts': gifts,
        'models': all_models,
    }
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    nm = sum(len(v) for v in all_models.values())
    wp_frag = sum(1 for g in gifts if g.get('fragment_price_ton'))
    wp_gg = sum(1 for g in gifts if g.get('getgems_floor_ton'))
    sz = os.path.getsize(OUT) / 1024
    print(f"\n{'='*50}")
    print(f"  Коллекций: {len(gifts)}")
    print(f"  Fragment цен: {wp_frag}")
    print(f"  GetGems цен:  {wp_gg}")
    print(f"  Моделей: {nm} (в {len(all_models)} коллекциях)")
    print(f"  Файл: {OUT} ({sz:.0f} KB)")
    print(f"\n  Загрузите на PythonAnywhere:")
    print(f"  /home/rasswetik52/mysite/data/fragment_catalog_cache.json")
    print("=" * 50)

if __name__ == '__main__':
    main()
