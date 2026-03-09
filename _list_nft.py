import json
with open('data/gifts.json','r',encoding='utf-8') as f:
    data = json.load(f)
gifts = data.get('gifts', data) if isinstance(data, dict) else data
nft = [g for g in gifts if isinstance(g, dict) and g.get('fragment_slug')]
nft.sort(key=lambda g: g.get('value',0))
for g in nft:
    ton = round(g['value']/100, 2)
    print(f"{g['id']:>3}  {ton:>8.2f} TON  {g['fragment_slug']:<30}  {g['name']}")
print(f"\nTotal NFT gifts: {len(nft)}")
print(f"Total gifts: {len(gifts)}")

# Also show non-NFT gifts
non_nft = [g for g in gifts if isinstance(g, dict) and not g.get('fragment_slug')]
non_nft.sort(key=lambda g: g.get('value',0))
print("\n--- Non-NFT gifts ---")
for g in non_nft:
    ton = round(g['value']/100, 2)
    print(f"{g['id']:>3}  {ton:>8.2f} TON  {g.get('image','')[:50]:<50}  {g['name']}")
