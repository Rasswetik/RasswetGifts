import json
d = json.load(open('data/fragment_catalog_cache.json', encoding='utf-8'))
gifts = d.get('gifts', [])
models = d.get('models', [])
out = []
for g in gifts:
    slug = g.get('fragment_slug', '?')
    name = g.get('name', '?')
    low = (slug + name).lower()
    if 'valent' in low or 'bear' in low or 'mick' in low or 'mouse' in low:
        out.append(f"GIFT: {slug} | {name} | {g.get('value', 0)}")

out.append(f"---total gifts: {len(gifts)}")
out.append(f"---total models: {len(models)}")

# Also list all gift slugs
out.append("--- ALL GIFT SLUGS ---")
for g in gifts:
    slug = g.get('fragment_slug', '?')
    name = g.get('name', '?')
    val = g.get('value', 0)
    out.append(f"  {slug} | {name} | {val}")

with open('_keys.txt', 'w') as f:
    f.write('\n'.join(out) + '\n')
print('Done')
