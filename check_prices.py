import json
d = json.load(open('data/fragment_catalog_cache.json', 'r', encoding='utf-8'))
for g in d.get('gifts', []):
    slug = g.get('fragment_slug', '?')
    frag = g.get('fragment_price_ton', '-')
    gg = g.get('getgems_floor_ton', '-')
    val = g.get('value', '-')
    print(f"{slug}: frag={frag}, gg={gg}, val={val}")
