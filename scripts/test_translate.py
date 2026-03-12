from db_wrapper import _translate_query

cases = [
    "UPDATE leaderboard_config SET is_active = 0",
    "UPDATE leaderboard_config SET is_active = 1",
    "UPDATE shop_deals SET is_active = 0 WHERE id = ?",
    "UPDATE inventory SET is_upgraded = 0",
    "UPDATE some_table SET id = 0"  # should not change
]

for c in cases:
    print('IN :', c)
    print('OUT:', _translate_query(c))
    print('---')
