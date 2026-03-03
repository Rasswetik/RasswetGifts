"""
CLI-скрипт для рефанда звёзд через Telegram Bot API.
Не конфликтует с webhook основного бота.

Использование:
  python refund_bot.py list              — последние 25 транзакций
  python refund_bot.py list 50           — последние 50 транзакций
  python refund_bot.py refund CHARGE_ID  — авто-рефанд (ищет user_id в транзакциях)
  python refund_bot.py refund USER_ID CHARGE_ID — ручной рефанд
"""

import sys
import requests
from datetime import datetime, timezone

BOT_TOKEN = '8224991617:AAF2F7ub0XF9N6wsWyn3PmhdZnYt62KmpRE'
API = f'https://api.telegram.org/bot{BOT_TOKEN}'


def tg(method, **kwargs):
    r = requests.post(f'{API}/{method}', json=kwargs, timeout=30)
    data = r.json()
    if not data.get('ok'):
        print(f'  [API ERROR] {data.get("description", r.text)}')
    return data


def list_transactions(limit=25):
    resp = tg('getStarTransactions', offset=0, limit=limit)
    if not resp.get('ok'):
        print('Не удалось получить транзакции.')
        return

    txs = resp['result'].get('transactions', [])
    if not txs:
        print('Нет транзакций.')
        return

    print(f'\n{"="*60}')
    print(f' Последние {len(txs)} транзакций со звёздами')
    print(f'{"="*60}\n')

    for i, tx in enumerate(txs, 1):
        tid = tx.get('id', '?')
        amount = tx.get('amount', 0)
        date = tx.get('date', 0)
        source = tx.get('source', {})
        user = source.get('user', {})
        uid = user.get('id', '—')
        fname = user.get('first_name', '')
        uname = user.get('username', '')
        display = fname or uname or str(uid)
        payload = tx.get('invoice_payload', '—')
        dt = datetime.fromtimestamp(date, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC') if date else '?'

        print(f'  #{i}')
        print(f'  ID:      {tid}')
        print(f'  Юзер:   {display} (id: {uid}, @{uname})')
        print(f'  Сумма:  {amount} stars')
        print(f'  Payload: {payload}')
        print(f'  Дата:    {dt}')
        print(f'  Рефанд:  python refund_bot.py refund {tid}')
        print(f'  {"-"*50}')

    print(f'\nВсего: {len(txs)} транзакций\n')


def do_refund(charge_id, user_id=None):
    if user_id is None:
        print(f'Ищем транзакцию {charge_id}...')
        resp = tg('getStarTransactions', offset=0, limit=100)
        if not resp.get('ok'):
            print('Не удалось получить транзакции.')
            return

        txs = resp['result'].get('transactions', [])
        target = None
        for tx in txs:
            if str(tx.get('id', '')) == str(charge_id):
                target = tx
                break

        if not target:
            print(f'Транзакция {charge_id} не найдена в последних 100.')
            print('Укажите user_id вручную: python refund_bot.py refund USER_ID CHARGE_ID')
            return

        source = target.get('source', {})
        user = source.get('user', {})
        user_id = user.get('id')
        amount = target.get('amount', 0)
        fname = user.get('first_name', '')

        if not user_id:
            print('Не удалось определить user_id из транзакции.')
            print('Укажите вручную: python refund_bot.py refund USER_ID CHARGE_ID')
            return

        print(f'Найдена: {amount} stars от {fname} (id: {user_id})')

    print(f'Выполняем рефанд: user_id={user_id}, charge_id={charge_id}...')
    resp = tg('refundStarPayment', user_id=int(user_id), telegram_payment_charge_id=str(charge_id))

    if resp.get('ok'):
        print(f'Рефанд выполнен! Звёзды возвращены пользователю {user_id}.')
    else:
        print(f'Ошибка: {resp.get("description", "Неизвестная ошибка")}')


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1].lower()

    if cmd == 'list':
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 25
        list_transactions(limit)

    elif cmd == 'refund':
        if len(sys.argv) == 3:
            do_refund(charge_id=sys.argv[2])
        elif len(sys.argv) == 4:
            do_refund(charge_id=sys.argv[3], user_id=sys.argv[2])
        else:
            print('Использование:')
            print('  python refund_bot.py refund CHARGE_ID')
            print('  python refund_bot.py refund USER_ID CHARGE_ID')
    else:
        print(__doc__)


if __name__ == '__main__':
    main()
