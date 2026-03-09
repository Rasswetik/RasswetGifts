#!/usr/bin/env python3
"""Convert gifts.json values from TON to stars and add Woman Bear.
Run ONCE on the original synced gifts.json (values in TON)."""
import json

with open('data/gifts.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

gifts = data.get('gifts', data) if isinstance(data, dict) else data

# Detect if values are in TON (small numbers) vs stars (large numbers)
avg_val = sum(g.get('value', 0) for g in gifts) / max(len(gifts), 1)
print(f'Current avg value: {avg_val:.2f}')

if avg_val < 1000:
    # Values are in TON, convert to stars (x100)
    print('Values appear to be in TON, converting to stars (x100)...')
    for g in gifts:
        old_val = g.get('value', 0)
        g['value'] = int(round(float(old_val) * 100))
else:
    print('Values already appear to be in stars, skipping conversion.')

max_id = max(g.get('id', 0) for g in gifts)
has_wb = any('woman bear' in g.get('name', '').lower() for g in gifts)
print(f'Max ID: {max_id}, Has Woman Bear: {has_wb}')

if not has_wb:
    new_id = max_id + 1
    gifts.append({
        'id': new_id,
        'name': 'Woman Bear',
        'type': 'item',
        'image': '/static/gifs/gifts/Woman_Bear.gif',
        'value': 50
    })
    print(f'Added Woman Bear as id {new_id} (value=50 stars = 0.50 TON)')

with open('data/gifts.json', 'w', encoding='utf-8') as f:
    json.dump({'gifts': gifts}, f, ensure_ascii=False, indent=2)

# Verify
d2 = json.load(open('data/gifts.json', 'r', encoding='utf-8'))
g2 = d2.get('gifts', [])
vals = [g.get('value', 0) for g in g2]
print(f'Saved {len(g2)} gifts. Value range: {min(vals)} - {max(vals)}')
wb = [g for g in g2 if 'woman' in g.get('name', '').lower()]
print(f'Woman Bear: {wb}')
