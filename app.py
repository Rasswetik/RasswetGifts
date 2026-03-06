# app.py - РѕСЃРЅРѕРІРЅРѕР№ С„Р°Р№Р» РїСЂРёР»РѕР¶РµРЅРёСЏ
from flask import Flask, render_template, request, jsonify, send_from_directory, redirect
import sqlite3
import json
import os
import logging
import random
import traceback
import string
import hashlib
import re
import html as html_lib
from datetime import datetime, timedelta
import math
import shutil
import time
import threading
import pytz
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
import requests as http_requests

# Р—Р°РіСЂСѓР¶Р°РµРј РїРµСЂРµРјРµРЅРЅС‹Рµ РѕРєСЂСѓР¶РµРЅРёСЏ
load_dotenv()

# РќР°СЃС‚СЂРѕР№РєР° Р»РѕРіРёСЂРѕРІР°РЅРёСЏ
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# РЎРѕР·РґР°РµРј РїСЂРёР»РѕР¶РµРЅРёРµ Flask
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'raswet-secret-key-2024')

# РљРѕРЅС„РёРіСѓСЂР°С†РёСЏ
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
ADMIN_ID = int(os.getenv('ADMIN_ID', '5257227756'))
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '8224991617:AAF2F7ub0XF9N6wsWyn3PmhdZnYt62KmpRE')
WEBSITE_URL = os.getenv('WEBSITE_URL', 'https://rasswet-gifts.onrender.com')
TG_API = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}'
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
MAX_FILE_SIZE = 16 * 1024 * 1024

# Р“Р»РѕР±Р°Р»СЊРЅС‹Рµ РїРµСЂРµРјРµРЅРЅС‹Рµ РґР»СЏ РєСЌС€РёСЂРѕРІР°РЅРёСЏ
gifts_cache = None
gifts_cache_time = None
CACHE_DURATION = 600  # 10 РјРёРЅСѓС‚ РєСЌС€ РїРѕРґР°СЂРєРѕРІ
FRAGMENT_SYNC_ENABLED = os.getenv('FRAGMENT_SYNC_ENABLED', '1') != '0'
FRAGMENT_SYNC_TIMEOUT = int(os.getenv('FRAGMENT_SYNC_TIMEOUT', '6'))
FRAGMENT_SYNC_MAX = int(os.getenv('FRAGMENT_SYNC_MAX', '5000'))
FRAGMENT_PRICE_FETCH_LIMIT = int(os.getenv('FRAGMENT_PRICE_FETCH_LIMIT', '20'))
FRAGMENT_TON_RATE = int(os.getenv('FRAGMENT_TON_RATE', '100'))
FRAGMENT_CACHE_DURATION = int(os.getenv('FRAGMENT_CACHE_DURATION', '900'))
FRAGMENT_ONLY_CATALOG = os.getenv('FRAGMENT_ONLY_CATALOG', '1') != '0'
FRAGMENT_ALLOW_LOCAL_ON_FAILURE = os.getenv('FRAGMENT_ALLOW_LOCAL_ON_FAILURE', '1') != '0'
FRAGMENT_FETCH_BASE = str(os.getenv('FRAGMENT_FETCH_BASE', '') or '').strip().rstrip('/')
FRAGMENT_DISK_CACHE_FILE = os.path.join(BASE_PATH, 'data', 'fragment_catalog_cache.json')
fragment_cache = None
fragment_cache_time = None
fragment_models_cache = {}
fragment_models_cache_time = {}

# в”Ђв”Ђ Manual gift prices in TON (override Fragment/local prices) в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
MANUAL_GIFT_PRICES_TON = {
    'ufc strike': 15.3,
    'valentine box': 11.7,
    'victory medal': 4.98,
    'vintage cigar': 33.9,
    'voodoo doll': 30.9,
    'westside sign': 111.9,
    'whip cupcake': 4.4,
    'winter wreath': 4.49,
    'witch hat': 6.29,
    'xmas stocking': 4.33,
    'swag bag': 5.19,
    'swiss watch': 55.8,
    'tama gadget': 4.65,
    'top hat': 12.8,
    'toy bear': 44.9,
    'trapped heart': 14.5,
    'moon': 5.64,
    'mousse cake': 4.96,
    'nail bracelet': 145.9,
    'neko helmet': 41.1,
    'party sparkler': 4.3,
    'perfume bottle': 97.9,
    'pet snake': 4.55,
    'precious peach': 438,
    'pretty posy': 4.7,
    'rare bird': 29.9,
    'record': 5.61,
    'restless jar': 11.3,
    'sakura flower': 48.9,
    'santa hat': 12.5,
    'scared cat': 4.5,
    'sharp tongue': 5.08,
    'signet ring': 5.48,
    'skull flower': 7.2,
    'sky stilettos': 5.49,
    'sleigh bell': 5.83,
    'snake box': 6.17,
    'loot bag': 162.9,
    'love candle': 12.3,
    'love potion': 17,
    'low rider': 52,
    'lunar snake': 4.3,
    'lush bouquet': 6.55,
    'mad pumpkin': 13.8,
    'magic potion': 80.9,
    'mighty arm': 171.9,
    'mini oscar': 103.7,
    'money pot': 4.66,
    'ion gem': 98.5,
    'ionic dryer': 18.7,
    'jack-in-the-box': 4.94,
    'jelly bunny': 8.03,
    'jester hat': 4.83,
    'jolly chimp': 7.3,
    'joyful bundle': 7.11,
    "khabib's papakha": 25.9,
    'kissed frog': 67.8,
    'light sword': 6.3,
    'lol pop': 4.5,
    'hanging star': 9.93,
    'happy brownie': 4.65,
    'heroic helmet': 255.7,
    'hex pot': 4.99,
    'holiday drink': 4.4,
    'homemade cake': 5.06,
    'hypno lollipop': 4.84,
    'ice cream': 4.44,
    'input key': 6.19,
    'instant ramen': 4.38,
    'desk calendar': 6.78,
    "durov's cap": 708,
    'jingle bells': 9.53,
    'plush pepe': 7999,
    'heart locket': 2199,
    'artisan brick': 97.7,
    'astral shard': 199.9,
    'b-day candle': 4.3,
    'berry box': 8.81,
    'big year': 4.72,
    'bling binky': 35.3,
    'bonded ring': 58.2,
    'bow tie': 6.3,
    'bunny muffin': 8.28,
    'candy cane': 4.31,
    'clover pin': 4.7,
    'cookie heart': 4.95,
    'crystal ball': 12,
    'cupid charm': 22.5,
    'diamond ring': 29.4,
    'easter egg': 5.63,
    'electric skull': 33.9,
    'eternal candle': 6.58,
    'eternal rose': 29.4,
    'evil eye': 7.94,
    'faith amulet': 4.7,
    'flying broom': 14,
    'fresh socks': 4.43,
    'gem signet': 72.3,
    'genie lamp': 50,
    'ginger cookie': 4.9,
}
# Build a slug-keyed version for matching by fragment_slug
_MANUAL_PRICES_BY_SLUG = {re.sub(r'[^a-z0-9]+', '', k): v for k, v in MANUAL_GIFT_PRICES_TON.items()}
_fragment_http_session = None
fragment_last_error = None

# РљСЌС€ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ (user_id -> {data, timestamp})
_user_cache = {}
_user_cache_duration = 30  # 30 СЃРµРєСѓРЅРґ

# Crash bots in-memory state
_crash_bots_cache = {
    'enabled': False,
    'bots': [],
    'settings': {'min_active_bots': 2, 'max_active_bots': 5, 'min_real_players_threshold': 3},
    'loaded': False,
}
_crash_bots_active = {}   # game_id -> [{bot_id, name, avatar, bet_amount, cashout_mult, status}]

# In-memory cache for crash game (updated by game loop)
_crash_game_cache = {
    'id': 0,
    'status': 'waiting',
    'current_multiplier': 1.0,
    'target_multiplier': 5.0,
    'time_remaining': 5.0,
    'timestamp': 0
}
_crash_cache_lock = threading.Lock()

# Lock to prevent bets during phase transitions (counting в†’ flying)
_crash_phase_lock = threading.Lock()
_crash_phase_transitioning = False

# Admin control for crash game
_admin_crash_control = {
    'manual_mode': False,           # Manual control enabled
    'force_crash': False,           # Force crash on next tick
    'next_multiplier': None,        # Set specific multiplier for next game
    'multiplier_min': 1.0,          # Min range for random multiplier
    'multiplier_max': 50.0,         # Max range for random multiplier
    'use_custom_range': False       # Use custom range instead of default
}
_admin_control_lock = threading.Lock()

def get_admin_crash_control():
    """Get admin crash control state"""
    with _admin_control_lock:
        return _admin_crash_control.copy()

def set_admin_crash_control(key, value):
    """Set admin crash control value"""
    global _admin_crash_control
    with _admin_control_lock:
        _admin_crash_control[key] = value

def get_crash_cache():
    """Get cached crash game state (thread-safe)"""
    with _crash_cache_lock:
        return _crash_game_cache.copy()

def update_crash_cache(game_id, status, current_mult, target_mult, time_remaining):
    """Update crash game cache (called from game loop)"""
    global _crash_game_cache
    with _crash_cache_lock:
        _crash_game_cache = {
            'id': game_id,
            'status': status,
            'current_multiplier': round(current_mult, 2),
            'target_multiplier': target_mult,
            'time_remaining': round(time_remaining, 1),
            'timestamp': time.time()
        }


# в”Ђв”Ђв”Ђ CRASH BOTS HELPERS в”Ђв”Ђв”Ђ
_BOT_AVATARS = [
    'https://i.pravatar.cc/100?img=1', 'https://i.pravatar.cc/100?img=2',
    'https://i.pravatar.cc/100?img=3', 'https://i.pravatar.cc/100?img=4',
    'https://i.pravatar.cc/100?img=5', 'https://i.pravatar.cc/100?img=6',
    'https://i.pravatar.cc/100?img=7', 'https://i.pravatar.cc/100?img=8',
    'https://i.pravatar.cc/100?img=9', 'https://i.pravatar.cc/100?img=10',
    'https://i.pravatar.cc/100?img=11', 'https://i.pravatar.cc/100?img=12',
    'https://i.pravatar.cc/100?img=13', 'https://i.pravatar.cc/100?img=14',
    'https://i.pravatar.cc/100?img=15', 'https://i.pravatar.cc/100?img=16',
    'https://i.pravatar.cc/100?img=17', 'https://i.pravatar.cc/100?img=18',
    'https://i.pravatar.cc/100?img=19', 'https://i.pravatar.cc/100?img=20',
    'https://api.dicebear.com/7.x/bottts/svg?seed=Nova',
    'https://api.dicebear.com/7.x/bottts/svg?seed=Pixel',
    'https://api.dicebear.com/7.x/bottts/svg?seed=Orbit',
    'https://api.dicebear.com/7.x/bottts/svg?seed=Comet',
    'https://api.dicebear.com/7.x/bottts/svg?seed=Raptor',
    'https://api.dicebear.com/7.x/bottts/svg?seed=Vega',
    'https://api.dicebear.com/7.x/bottts/svg?seed=Turbo',
    'https://api.dicebear.com/7.x/bottts/svg?seed=Hunter',
    'https://api.dicebear.com/7.x/bottts/svg?seed=Sonic',
    'https://api.dicebear.com/7.x/bottts/svg?seed=Storm',
    'https://api.dicebear.com/7.x/lorelei/svg?seed=Akira',
    'https://api.dicebear.com/7.x/lorelei/svg?seed=Yuki',
    'https://api.dicebear.com/7.x/lorelei/svg?seed=Ren',
    'https://api.dicebear.com/7.x/lorelei/svg?seed=Sakura',
    'https://api.dicebear.com/7.x/adventurer/svg?seed=Kenji',
    'https://api.dicebear.com/7.x/adventurer/svg?seed=Miko',
    'https://robohash.org/WolfPack.png?set=set4',
    'https://robohash.org/TigerPaw.png?set=set4',
    'https://robohash.org/FoxTail.png?set=set4',
    'https://robohash.org/PandaStar.png?set=set4',
    'https://robohash.org/DragonPet.png?set=set4',
    'https://robohash.org/BeastOrbit.png?set=set2',
    'https://robohash.org/MechaPet.png?set=set2',
    'https://robohash.org/MonsterRush.png?set=set2',
]

_BOT_NAMES_RU = [
    'РђР»РµРєСЃРµР№', 'Р”РјРёС‚СЂРёР№', 'РњР°РєСЃРёРј', 'РђСЂС‚С‘Рј', 'РРІР°РЅ',
    'РњРёС…Р°РёР»', 'Р”Р°РЅРёРёР»', 'РљРёСЂРёР»Р»', 'РђРЅРґСЂРµР№', 'Р•РіРѕСЂ',
    'РќРёРєРёС‚Р°', 'РњР°С‚РІРµР№', 'РўРёРјРѕС„РµР№', 'Р РѕРјР°РЅ', 'Р’Р»Р°РґРёРјРёСЂ',
    'РЇСЂРѕСЃР»Р°РІ', 'Р¤С‘РґРѕСЂ', 'Р”РµРЅРёСЃ', 'РљРѕРЅСЃС‚Р°РЅС‚РёРЅ', 'Р“Р»РµР±',
    'РђРЅРЅР°', 'РњР°СЂРёСЏ', 'РЎРѕС„СЊСЏ', 'Р”Р°СЂСЊСЏ', 'РђР»РёСЃР°',
    'РџРѕР»РёРЅР°', 'Р’РёРєС‚РѕСЂРёСЏ', 'Р•РєР°С‚РµСЂРёРЅР°', 'Р’РµСЂРѕРЅРёРєР°', 'РђСЂРёРЅР°',
    'РўРµРЅСЊР›РёСЃР°', 'РљРёР±РµСЂР’РѕР»Рє', 'Р›СѓРЅРЅС‹Р№РљРѕС‚', 'Р—РІС‘Р·РґРЅС‹Р№РўРёРіСЂ', 'РќРµРѕРџР°РЅРґР°',
    'Р”СЂР°РєРѕРЅС‡РёРє', 'РљРѕСЃРјРѕР—Р°СЏС†', 'Р‘СѓСЂСЏРљРѕРіРѕС‚СЊ', 'Р РѕР±РѕР›РёСЃ', 'РџРёРєСЃРµР»СЊР•РЅРѕС‚',
    'РЎР°РїС„РёСЂР¤РµРЅРёРєСЃ', 'РќРѕС‡РЅРѕР№РЇРіСѓР°СЂ', 'Р“СЂРѕРјРљРёС‚', 'Р’РёС…СЂСЊРЎРѕРІР°', 'РљРѕРјРµС‚Р°Р С‹СЃСЊ',
]

_BOT_NAMES_EN = [
    'Alex', 'Daniel', 'James', 'Lucas', 'Oliver',
    'William', 'Noah', 'Ethan', 'Mason', 'Logan',
    'Emma', 'Sophia', 'Mia', 'Isabella', 'Charlotte',
    'Amelia', 'Harper', 'Ella', 'Lily', 'Victoria',
    'CryptoKing', 'LuckyShot', 'StarHunter', 'RocketMan', 'DiamondH',
    'NightOwl', 'SkyRider', 'FireBolt', 'Ace_777', 'BigWin',
    'NeonFox', 'BlazeRunner', 'AeroMint', 'VoltEdge', 'SilverJet',
    'MoonByte', 'ZenPilot', 'NovaClutch', 'HyperDash', 'ZeroLag',
    'RapidWin', 'DriftCore', 'GigaLuck', 'CloudRider', 'EchoStrike',
    'CyberNeko', 'ShadowWolf', 'PixelPanda', 'NeonTiger', 'AquaOtter',
    'StormFalcon', 'CometLynx', 'GhostRaven', 'CrystalFox', 'MechaKoala',
    'AnimeBlade', 'KitsuneX', 'DragonPaw', 'FrostLeopard', 'TurboShark',
]

_BOT_LASTNAMES_RU = [
    'РРІР°РЅРѕРІ', 'РџРµС‚СЂРѕРІ', 'РЎРјРёСЂРЅРѕРІ', 'РљСѓР·РЅРµС†РѕРІ', 'РџРѕРїРѕРІ', 'РЎРѕРєРѕР»РѕРІ', 'Р›РµР±РµРґРµРІ',
    'РљРѕР·Р»РѕРІ', 'РќРѕРІРёРєРѕРІ', 'РњРѕСЂРѕР·РѕРІ', 'Р’РѕР»РєРѕРІ', 'РЎРѕР»РѕРІСЊС‘РІ', 'Р’Р°СЃРёР»СЊРµРІ', 'Р—Р°Р№С†РµРІ',
    'РџР°РІР»РѕРІ', 'РЎРµРјРµРЅРѕРІ', 'Р“РѕР»СѓР±РµРІ', 'Р’РёРЅРѕРіСЂР°РґРѕРІ', 'Р‘РѕРіРґР°РЅРѕРІ', 'Р’РѕСЂРѕР±СЊС‘РІ'
]

_BOT_LASTNAMES_EN = [
    'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis',
    'Wilson', 'Taylor', 'Anderson', 'Thomas', 'Jackson', 'White', 'Harris',
    'Martin', 'Thompson', 'Garcia', 'Clark', 'Lewis', 'Walker'
]

TON_RATE = 100.0


def _load_crash_bots():
    """Load crash bots config from DB into memory"""
    global _crash_bots_cache
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # settings
        cursor.execute('SELECT enabled, min_active_bots, max_active_bots, min_real_players_threshold FROM crash_bots_settings WHERE id = 1')
        row = cursor.fetchone()
        if row:
            _crash_bots_cache['enabled'] = bool(row[0])
            _crash_bots_cache['settings'] = {
                'min_active_bots': row[1] or 2,
                'max_active_bots': row[2] or 5,
                'min_real_players_threshold': row[3] or 3,
            }
        else:
            # Auto-enable bots on first run
            conn.execute('INSERT OR IGNORE INTO crash_bots_settings (id, enabled, min_active_bots, max_active_bots, min_real_players_threshold) VALUES (1, 1, 2, 5, 3)')
            conn.commit()
            _crash_bots_cache['enabled'] = True
            _crash_bots_cache['settings'] = {'min_active_bots': 2, 'max_active_bots': 5, 'min_real_players_threshold': 3}
        # bots list
        cursor.execute('SELECT id, bot_name, avatar_url, min_bet, max_bet, auto_cashout_min, auto_cashout_max, is_active FROM crash_bots_config ORDER BY id')
        _crash_bots_cache['bots'] = [{
            'id': r[0], 'bot_name': r[1], 'avatar_url': r[2],
            'min_bet': r[3] or 25, 'max_bet': r[4] or 500,
            'auto_cashout_min': float(r[5] or 1.2), 'auto_cashout_max': float(r[6] or 5.0),
            'is_active': bool(r[7])
        } for r in cursor.fetchall()]
        # Auto-seed bots if none exist
        if not _crash_bots_cache['bots']:
            _seed_default_bots(conn, 100)
            cursor.execute('SELECT id, bot_name, avatar_url, min_bet, max_bet, auto_cashout_min, auto_cashout_max, is_active FROM crash_bots_config ORDER BY id')
            _crash_bots_cache['bots'] = [{
                'id': r[0], 'bot_name': r[1], 'avatar_url': r[2],
                'min_bet': r[3] or 25, 'max_bet': r[4] or 500,
                'auto_cashout_min': float(r[5] or 1.2), 'auto_cashout_max': float(r[6] or 5.0),
                'is_active': bool(r[7])
            } for r in cursor.fetchall()]
            logger.info(f"Auto-seeded {len(_crash_bots_cache['bots'])} crash bots")
        elif len(_crash_bots_cache['bots']) < 100:
            need_count = 100 - len(_crash_bots_cache['bots'])
            _seed_default_bots(conn, need_count)
            cursor.execute('SELECT id, bot_name, avatar_url, min_bet, max_bet, auto_cashout_min, auto_cashout_max, is_active FROM crash_bots_config ORDER BY id')
            _crash_bots_cache['bots'] = [{
                'id': r[0], 'bot_name': r[1], 'avatar_url': r[2],
                'min_bet': r[3] or 25, 'max_bet': r[4] or 500,
                'auto_cashout_min': float(r[5] or 1.2), 'auto_cashout_max': float(r[6] or 5.0),
                'is_active': bool(r[7])
            } for r in cursor.fetchall()]
            logger.info(f"Auto-added {need_count} crash bots to reach 100 total")
        _crash_bots_cache['loaded'] = True
        conn.close()
    except Exception as e:
        logger.warning(f"Bot config load error: {e}")


def _seed_default_bots(conn, count=100):
    """Generate default bots (used on first startup and to backfill up to target amount)."""
    if count <= 0:
        return

    cursor = conn.cursor()
    cursor.execute('SELECT bot_name FROM crash_bots_config')
    existing_names = {r[0] for r in cursor.fetchall() if r and r[0]}

    names_pool = list(_BOT_NAMES_RU) + list(_BOT_NAMES_EN)
    import random as _rnd
    _rnd.shuffle(names_pool)
    preferred_avatars = [
        a for a in _BOT_AVATARS
        if ('pravatar' in a) or ('lorelei' in a) or ('adventurer' in a) or ('set4' in a)
    ]
    if not preferred_avatars:
        preferred_avatars = list(_BOT_AVATARS)

    created = 0
    idx = 0
    while created < count:
        if _rnd.random() < 0.72:
            if _rnd.random() < 0.55:
                base_name = f"{_rnd.choice(_BOT_NAMES_RU)} {_rnd.choice(_BOT_LASTNAMES_RU)}"
            else:
                base_name = f"{_rnd.choice(_BOT_NAMES_EN)} {_rnd.choice(_BOT_LASTNAMES_EN)}"
        else:
            base_name = names_pool[idx % len(names_pool)]
        name = base_name if base_name not in existing_names else f"{base_name}_{_rnd.randint(10, 999)}"
        while name in existing_names:
            name = f"{base_name}_{_rnd.randint(10, 999)}"

        existing_names.add(name)
        avatar = preferred_avatars[(idx + created) % len(preferred_avatars)]
        min_bet = _rnd.choice([25, 50, 75, 100])
        max_bet = _rnd.choice([300, 400, 500, 700, 900])
        if max_bet < min_bet:
            max_bet = min_bet
        cashout_min = round(_rnd.uniform(1.15, 1.8), 2)
        cashout_max = round(_rnd.uniform(max(2.0, cashout_min + 0.2), 5.5), 2)

        conn.execute(
            'INSERT INTO crash_bots_config (bot_name, avatar_url, min_bet, max_bet, auto_cashout_min, auto_cashout_max, is_active) VALUES (?,?,?,?,?,?,1)',
            (name, avatar, min_bet, max_bet, cashout_min, cashout_max)
        )
        created += 1
        idx += 1
    conn.commit()


def _generate_bot_bets(game_id, real_player_count):
    """Generate bot bets for a game round вЂ” DISABLED"""
    return  # Bots disabled


def _process_bot_cashouts(game_id, current_mult):
    """Process bot auto-cashouts during flying phase"""
    bots = _crash_bots_active.get(game_id, [])
    for bot in bots:
        if bot['status'] == 'active' and current_mult >= bot.get('cashout_mult', 0):
            bot['status'] = 'cashed_out'
            bot['win_amount'] = int(bot['bet_amount'] * bot.get('cashout_mult', 0))
            bot['cashout_multiplier'] = bot.get('cashout_mult')


def _crash_bots_on_crash(game_id):
    """Mark remaining active bot bets as lost"""
    bots = _crash_bots_active.get(game_id, [])
    for bot in bots:
        if bot['status'] == 'active':
            bot['status'] = 'lost'
            bot['win_amount'] = 0


def _get_bot_bets_for_api(game_id):
    """Return bot bets formatted like real bets for the bets API"""
    bots = _crash_bots_active.get(game_id, [])
    result = []
    for b in bots:
        result.append({
            'id': -b.get('bot_id', 0),  # negative IDs for bots
            'user_id': -b.get('bot_id', 0),
            'bet_amount': b.get('bet_amount', 0),
            'status': b.get('status'),
            'cashout_multiplier': b.get('cashout_multiplier'),
            'win_amount': b.get('win_amount', 0),
            'created_at': None,
            'first_name': b.get('name', 'Bot'),
            'username': None,
            'photo_url': b.get('avatar', '/static/img/default_avatar.png'),
            'is_bot': True,
        })
    return result


# РЎРёСЃС‚РµРјР° СѓСЂРѕРІРЅРµР№ - turnover РІ Р·РІС‘Р·РґР°С… (100 stars = 1 TON)
# Rewards: rocket skins and backgrounds ONLY (no stars/tickets)
LEVEL_SYSTEM = [
    {"level": 1,  "exp_required": 0,       "reward_stars": 0, "reward_tickets": 0, "reward_rocket": "crash",       "reward_bg": None},
    {"level": 2,  "exp_required": 10000,   "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": "cosmic"},
    {"level": 3,  "exp_required": 30000,   "reward_stars": 0, "reward_tickets": 0, "reward_rocket": "cat",         "reward_bg": None},
    {"level": 4,  "exp_required": 60000,   "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": None},
    {"level": 5,  "exp_required": 100000,  "reward_stars": 0, "reward_tickets": 0, "reward_rocket": "dog",         "reward_bg": "rainbow"},
    {"level": 6,  "exp_required": 150000,  "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": None},
    {"level": 7,  "exp_required": 210000,  "reward_stars": 0, "reward_tickets": 0, "reward_rocket": "banana",      "reward_bg": None},
    {"level": 8,  "exp_required": 280000,  "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": "aurora"},
    {"level": 9,  "exp_required": 360000,  "reward_stars": 0, "reward_tickets": 0, "reward_rocket": "plane",       "reward_bg": None},
    {"level": 10, "exp_required": 450000,  "reward_stars": 0, "reward_tickets": 0, "reward_rocket": "rabbit",      "reward_bg": None},
    {"level": 11, "exp_required": 550000,  "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": "neon"},
    {"level": 12, "exp_required": 660000,  "reward_stars": 0, "reward_tickets": 0, "reward_rocket": "ice",         "reward_bg": None},
    {"level": 13, "exp_required": 780000,  "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": None},
    {"level": 14, "exp_required": 910000,  "reward_stars": 0, "reward_tickets": 0, "reward_rocket": "unicorn",     "reward_bg": None},
    {"level": 15, "exp_required": 1050000, "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": None},
    {"level": 16, "exp_required": 1200000, "reward_stars": 0, "reward_tickets": 0, "reward_rocket": "goldenplane", "reward_bg": None},
    {"level": 17, "exp_required": 1360000, "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": None},
    {"level": 18, "exp_required": 1530000, "reward_stars": 0, "reward_tickets": 0, "reward_rocket": "telegram",    "reward_bg": None},
    {"level": 19, "exp_required": 1710000, "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": None},
    {"level": 20, "exp_required": 1900000, "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": None},
    {"level": 21, "exp_required": 2150000, "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": None},
    {"level": 22, "exp_required": 2400000, "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": None},
    {"level": 23, "exp_required": 2700000, "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": None},
    {"level": 24, "exp_required": 3000000, "reward_stars": 0, "reward_tickets": 0, "reward_rocket": None,          "reward_bg": None},
    {"level": 25, "exp_required": 3400000, "reward_stars": 0, "reward_tickets": 0, "reward_rocket": "TonTheMoon",  "reward_bg": None},
]

# Background names for display
BG_NAMES = {
    'grid': 'РЎРµС‚РєР°',
    'cosmic': 'РљРѕСЃРјРѕСЃ',
    'rainbow': 'Р Р°РґСѓРіР°',
    'aurora': 'РђРІСЂРѕСЂР°',
    'neon': 'РќРµРѕРЅ',
}

# РљР°СЂС‚Р° РЅР°Р·РІР°РЅРёР№ СЂР°РєРµС‚ РґР»СЏ РѕС‚РѕР±СЂР°Р¶РµРЅРёСЏ
ROCKET_NAMES = {
    'crash': 'Р Р°РєРµС‚Р°',
    'pencil': 'РљР°СЂР°РЅРґР°С€',
    'banana': 'Р‘Р°РЅР°РЅ',
    'plane': 'РЎР°РјРѕР»С‘С‚',
    'dog': 'РЎРѕР±Р°РєР°',
    'cat': 'РљРѕС‚',
    'rabbit': 'РљСЂРѕР»РёРє',
    'smesh': 'РЎРјРµС€Р°СЂРёРє',
    'scorpion': 'РЎРєРѕСЂРїРёРѕРЅ',
    'telegram': 'РўРµР»РµРіСЂР°Рј',
    'ice': 'Р›С‘Рґ',
    'unicorn': 'Р•РґРёРЅРѕСЂРѕРі',
    'TonTheMoon': 'TON Moon',
    'goldenplane': 'Р—РѕР»РѕС‚РѕР№ РЎР°РјРѕР»С‘С‚',
}

# РљР°СЂС‚Р° РєСЂРµР№С‚РѕРІ Р·Р° СѓСЂРѕРІРЅРё
LEVEL_CRATES = {
    'starter_crate':   {'name': 'Starter Crate',   'image': '/static/img/crates/starter.png',   'items': [('stars', '50', 'Stars x50', 40, 'common'), ('stars', '150', 'Stars x150', 30, 'uncommon'), ('tickets', '5', 'Tickets x5', 20, 'rare'), ('stars', '300', 'Stars x300', 10, 'epic')]},
    'bronze_crate':    {'name': 'Bronze Crate',     'image': '/static/img/crates/bronze.png',    'items': [('stars', '100', 'Stars x100', 35, 'common'), ('stars', '250', 'Stars x250', 30, 'uncommon'), ('tickets', '10', 'Tickets x10', 20, 'rare'), ('stars', '500', 'Stars x500', 15, 'epic')]},
    'silver_crate':    {'name': 'Silver Crate',     'image': '/static/img/crates/silver.png',    'items': [('stars', '200', 'Stars x200', 35, 'common'), ('stars', '400', 'Stars x400', 25, 'uncommon'), ('tickets', '15', 'Tickets x15', 25, 'rare'), ('stars', '800', 'Stars x800', 15, 'epic')]},
    'gold_crate':      {'name': 'Gold Crate',       'image': '/static/img/crates/gold.png',      'items': [('stars', '300', 'Stars x300', 30, 'common'), ('stars', '600', 'Stars x600', 25, 'uncommon'), ('tickets', '20', 'Tickets x20', 25, 'rare'), ('stars', '1200', 'Stars x1200', 20, 'epic')]},
    'platinum_crate':  {'name': 'Platinum Crate',   'image': '/static/img/crates/platinum.png',  'items': [('stars', '400', 'Stars x400', 25, 'common'), ('stars', '800', 'Stars x800', 25, 'uncommon'), ('tickets', '30', 'Tickets x30', 25, 'rare'), ('stars', '1500', 'Stars x1500', 25, 'epic')]},
    'diamond_crate':   {'name': 'Diamond Crate',    'image': '/static/img/crates/diamond.png',   'items': [('stars', '500', 'Stars x500', 25, 'common'), ('stars', '1000', 'Stars x1000', 25, 'uncommon'), ('tickets', '40', 'Tickets x40', 25, 'rare'), ('stars', '2000', 'Stars x2000', 25, 'epic')]},
    'cosmic_crate':    {'name': 'Cosmic Crate',     'image': '/static/img/crates/cosmic.png',    'items': [('stars', '700', 'Stars x700', 20, 'common'), ('stars', '1500', 'Stars x1500', 25, 'uncommon'), ('tickets', '50', 'Tickets x50', 30, 'rare'), ('stars', '3000', 'Stars x3000', 25, 'epic')]},
    'nebula_crate':    {'name': 'Nebula Crate',     'image': '/static/img/crates/nebula.png',    'items': [('stars', '1000', 'Stars x1000', 20, 'common'), ('stars', '2000', 'Stars x2000', 25, 'uncommon'), ('tickets', '60', 'Tickets x60', 25, 'rare'), ('stars', '4000', 'Stars x4000', 30, 'epic')]},
    'stellar_crate':   {'name': 'Stellar Crate',    'image': '/static/img/crates/stellar.png',   'items': [('stars', '1500', 'Stars x1500', 20, 'common'), ('stars', '3000', 'Stars x3000', 25, 'uncommon'), ('tickets', '80', 'Tickets x80', 25, 'rare'), ('stars', '5000', 'Stars x5000', 30, 'epic')]},
    'galactic_crate':  {'name': 'Galactic Crate',   'image': '/static/img/crates/galactic.png',  'items': [('stars', '2000', 'Stars x2000', 15, 'common'), ('stars', '4000', 'Stars x4000', 25, 'uncommon'), ('tickets', '100', 'Tickets x100', 25, 'rare'), ('stars', '7000', 'Stars x7000', 35, 'epic')]},
    'legendary_crate': {'name': 'Legendary Crate',  'image': '/static/img/crates/legendary.png', 'items': [('stars', '3000', 'Stars x3000', 15, 'common'), ('stars', '5000', 'Stars x5000', 20, 'uncommon'), ('tickets', '120', 'Tickets x120', 30, 'rare'), ('stars', '10000', 'Stars x10000', 35, 'epic')]},
    'supreme_crate':   {'name': 'Supreme Crate',    'image': '/static/img/crates/supreme.png',   'items': [('stars', '5000', 'Stars x5000', 10, 'uncommon'), ('tickets', '150', 'Tickets x150', 25, 'rare'), ('stars', '15000', 'Stars x15000', 35, 'epic'), ('stars', '25000', 'Stars x25000', 30, 'legendary')]},
}

def _sync_levels_from_db():
    """Р—Р°РіСЂСѓР¶Р°РµС‚ СѓСЂРѕРІРЅРё РёР· Р‘Р”. Р•СЃР»Рё РїСѓСЃС‚Рѕ вЂ” Р·Р°РїРѕР»РЅСЏРµС‚ РёР· LEVEL_SYSTEM РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ."""
    global LEVEL_SYSTEM
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM levels')
        count = cursor.fetchone()[0]
        if count == 0:
            # Seed DB with default levels
            for lvl in LEVEL_SYSTEM:
                cursor.execute('''INSERT OR IGNORE INTO levels (level, exp_required, reward_stars, reward_tickets) 
                    VALUES (?, ?, ?, ?)''',
                    (lvl['level'], lvl['exp_required'], lvl['reward_stars'], lvl['reward_tickets']))
            conn.commit()
            logger.info(f"рџ“Љ РЎРѕС…СЂР°РЅРµРЅРѕ {len(LEVEL_SYSTEM)} СѓСЂРѕРІРЅРµР№ РІ Р‘Р”")
        else:
            # Load from DB
            cursor.execute('SELECT level, exp_required, reward_stars, reward_tickets FROM levels ORDER BY level')
            rows = cursor.fetchall()
            LEVEL_SYSTEM = [
                {"level": r[0], "exp_required": r[1], "reward_stars": r[2], "reward_tickets": r[3]}
                for r in rows
            ]
            logger.info(f"рџ“Љ Р—Р°РіСЂСѓР¶РµРЅРѕ {len(LEVEL_SYSTEM)} СѓСЂРѕРІРЅРµР№ РёР· Р‘Р”")
        conn.close()
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё СѓСЂРѕРІРЅРµР№ РёР· Р‘Р”: {e}")

# ==================== Р’РЎРџРћРњРћР“РђРўР•Р›Р¬РќР«Р• Р¤РЈРќРљР¦РР ====================

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def load_gifts_cached():
    """Р—Р°РіСЂСѓР¶Р°РµС‚ РїРѕРґР°СЂРєРё СЃ РєСЌС€РёСЂРѕРІР°РЅРёРµРј"""
    global gifts_cache, gifts_cache_time
    current_time = time.time()
    if gifts_cache is not None and gifts_cache_time is not None:
        if current_time - gifts_cache_time < CACHE_DURATION:
            return gifts_cache
    gifts_cache = load_gifts()
    gifts_cache_time = current_time
    return gifts_cache

def _slugify_fragment_name(name):
    return re.sub(r'[^a-z0-9]+', '', str(name or '').lower()).strip()

def _normalize_gift_name_for_match(name):
    text = str(name or '').lower().replace('(random)', '').strip()
    return re.sub(r'[^a-z0-9]+', '', text)

def _parse_fragment_price_ton(text):
    if not text:
        return None
    floor_match = re.search(r'Floor[^0-9]{0,30}([0-9][0-9,]*(?:\.[0-9]+)?)', text, re.IGNORECASE)
    if floor_match:
        try:
            return float(floor_match.group(1).replace(',', ''))
        except Exception:
            return None
    ton_match = re.search(r'([0-9][0-9,]*(?:\.[0-9]+)?)\s*TON', text, re.IGNORECASE)
    if ton_match:
        try:
            return float(ton_match.group(1).replace(',', ''))
        except Exception:
            return None
    return None

def _load_fragment_catalog_disk_cache():
    try:
        if not os.path.exists(FRAGMENT_DISK_CACHE_FILE):
            return []
        with open(FRAGMENT_DISK_CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            # Р—Р°РіСЂСѓР¶Р°РµРј РјРѕРґРµР»Рё РёР· РєСЌС€Р° РІ РїР°РјСЏС‚СЊ (sync_fragment.py)
            cached_models = data.get('models')
            if isinstance(cached_models, dict) and cached_models:
                loaded_count = 0
                for slug, model_list in cached_models.items():
                    slug_key = str(slug).strip().lower()
                    if slug_key and isinstance(model_list, list):
                        fragment_models_cache[slug_key] = model_list
                        fragment_models_cache_time[slug_key] = time.time()
                        loaded_count += len(model_list)
                if loaded_count:
                    logger.info(f'Fragment disk cache: loaded {loaded_count} models for {len(cached_models)} collections')
            data = data.get('gifts', [])
        if isinstance(data, list):
            return data
    except Exception as e:
        logger.warning(f'Fragment disk cache read failed: {e}')
    return []

def _save_fragment_catalog_disk_cache(gifts):
    try:
        if not isinstance(gifts, list):
            return
        # РЎРѕС…СЂР°РЅСЏРµРј gifts + С‚РµРєСѓС‰РёРµ РјРѕРґРµР»Рё РёР· РїР°РјСЏС‚Рё
        models_snapshot = {}
        for slug_key, model_list in fragment_models_cache.items():
            if isinstance(model_list, list) and model_list:
                models_snapshot[slug_key] = model_list
        payload = {
            'updated_at': datetime.utcnow().isoformat() + 'Z',
            'gifts': gifts,
            'models': models_snapshot,
        }
        with open(FRAGMENT_DISK_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f'Fragment disk cache write failed: {e}')

def _fragment_alt_url(url):
    if not FRAGMENT_FETCH_BASE:
        return None
    if not str(url).startswith('https://fragment.com'):
        return None
    suffix = url[len('https://fragment.com'):]
    return FRAGMENT_FETCH_BASE + suffix

def _fragment_mirror_url(url):
    source = str(url or '').strip()
    if not source:
        return None
    if source.startswith('https://r.jina.ai/http://') or source.startswith('https://r.jina.ai/https://'):
        return source
    if source.startswith('https://fragment.com/'):
        return 'https://r.jina.ai/http://fragment.com/' + source[len('https://fragment.com/'):]
    if source == 'https://fragment.com':
        return 'https://r.jina.ai/http://fragment.com'
    return None

def _fragment_get(url, timeout=None):
    global _fragment_http_session
    if _fragment_http_session is None:
        _fragment_http_session = http_requests.Session()
        _fragment_http_session.trust_env = False
    errors = []
    candidates = [url]
    alt = _fragment_alt_url(url)
    if alt and alt not in candidates:
        candidates.append(alt)
    mirror = _fragment_mirror_url(url)
    if mirror and mirror not in candidates:
        candidates.append(mirror)

    for candidate_url in candidates:
        try:
            resp = _fragment_http_session.get(
                candidate_url,
                timeout=timeout or FRAGMENT_SYNC_TIMEOUT,
                headers={'User-Agent': 'Mozilla/5.0'},
                proxies={'http': None, 'https': None}
            )
            if resp.status_code < 500:
                return resp
            errors.append(f'{candidate_url} -> HTTP {resp.status_code}')
        except Exception as e:
            errors.append(f'{candidate_url} -> {e}')

    raise RuntimeError(' | '.join(errors) if errors else 'Fragment request failed')

def _fetch_fragment_collection_price(slug):
    if not slug:
        return None
    try:
        url = f'https://fragment.com/gifts/{slug}'
        resp = _fragment_get(url)
        if resp.status_code != 200:
            return None
        return _parse_fragment_price_ton(resp.text)
    except Exception:
        return None

def fetch_fragment_gifts_catalog(force_refresh=False):
    global fragment_cache, fragment_cache_time, fragment_last_error
    now = time.time()
    if not force_refresh and fragment_cache is not None and fragment_cache_time is not None:
        cache_age = now - fragment_cache_time
        if fragment_cache:
            if cache_age < FRAGMENT_CACHE_DURATION:
                return fragment_cache
        else:
            # Р”Р»СЏ РїСѓСЃС‚РѕРіРѕ РєСЌС€Р° РґРµР»Р°РµРј Р±РѕР»РµРµ С‡Р°СЃС‚С‹Рµ СЂРµС‚СЂР°Рё (РЅР° СЃР»СѓС‡Р°Р№ РІСЂРµРјРµРЅРЅРѕР№ РѕС€РёР±РєРё Fragment)
            if cache_age < min(FRAGMENT_CACHE_DURATION, 60):
                return fragment_cache

    if not FRAGMENT_SYNC_ENABLED:
        fragment_last_error = 'fragment_sync_disabled'
        fragment_cache = _load_fragment_catalog_disk_cache() or []
        fragment_cache_time = now
        return fragment_cache

    gifts = []
    previous_cache = fragment_cache[:] if isinstance(fragment_cache, list) else []
    disk_cache = _load_fragment_catalog_disk_cache() or []
    try:
        resp = _fragment_get('https://fragment.com/gifts')
        if resp.status_code != 200:
            fragment_last_error = f'http_status_{resp.status_code}'
            if previous_cache:
                return previous_cache
            if disk_cache:
                logger.info(f'Fragment: using disk cache ({len(disk_cache)}) due to HTTP {resp.status_code}')
                fragment_cache = disk_cache
                fragment_cache_time = now
                return fragment_cache
            fragment_cache = []
            fragment_cache_time = now
            return fragment_cache

        html = resp.text
        matches = re.findall(
            r'href=["\'](?:https?://fragment\.com)?/gifts/([a-z0-9_-]+)(?:\?[^"\']*)?["\'][^>]*>(.*?)</a>',
            html,
            flags=re.IGNORECASE | re.DOTALL
        )
        seen = set()

        # 0) Mirror markdown pass
        for md_item in _extract_fragment_markdown_collections(html):
            slug = (md_item.get('fragment_slug') or '').strip().lower()
            if not slug or slug in seen:
                continue
            seen.add(slug)
            gifts.append(md_item)
            if FRAGMENT_SYNC_MAX > 0 and len(gifts) >= FRAGMENT_SYNC_MAX:
                break

        # 1) Detailed matches with possible text labels
        for slug, raw_name in matches:
            if not slug:
                continue
            slug = slug.strip().lower()
            if slug in seen:
                continue
            seen.add(slug)
            clean_name = re.sub(r'<[^>]+>', ' ', raw_name or '')
            clean_name = re.sub(r'\s+', ' ', clean_name).strip()
            name = html_lib.unescape(clean_name)
            name = re.sub(r'\s+[0-9][0-9,]*\s+items.*$', '', name, flags=re.IGNORECASE).strip()
            if not name:
                name = slug
            gifts.append({
                'name': name,
                'fragment_slug': slug,
                'fragment_url': f'https://fragment.com/gifts/{slug}',
                'image': f'https://fragment.com/file/gifts/{slug}/thumb.webp'
            })

            if FRAGMENT_SYNC_MAX > 0 and len(gifts) >= FRAGMENT_SYNC_MAX:
                break

        # 2) Fallback pass: collect every slug occurrence even if anchor text is complex
        if FRAGMENT_SYNC_MAX <= 0 or len(gifts) < FRAGMENT_SYNC_MAX:
            slug_matches = re.findall(r'(?:https?://fragment\.com)?/gifts/([a-z0-9_-]+)', html, flags=re.IGNORECASE)
            for slug in slug_matches:
                slug = (slug or '').strip().lower()
                if not slug or slug in seen:
                    continue
                seen.add(slug)
                pretty_name = slug
                gifts.append({
                    'name': pretty_name,
                    'fragment_slug': slug,
                    'fragment_url': f'https://fragment.com/gifts/{slug}',
                    'image': f'https://fragment.com/file/gifts/{slug}/thumb.webp'
                })
                if FRAGMENT_SYNC_MAX > 0 and len(gifts) >= FRAGMENT_SYNC_MAX:
                    break

        to_price = gifts if FRAGMENT_PRICE_FETCH_LIMIT <= 0 else gifts[:FRAGMENT_PRICE_FETCH_LIMIT]
        for item in to_price:
            ton_price = _fetch_fragment_collection_price(item.get('fragment_slug'))
            if ton_price is not None:
                item['fragment_price_ton'] = ton_price
                item['value'] = int(round(ton_price * FRAGMENT_TON_RATE))

        if gifts:
            _save_fragment_catalog_disk_cache(gifts)
        fragment_last_error = None

    except Exception as e:
        fragment_last_error = str(e)
        logger.warning(f'Fragment gifts sync failed: {e}')
        if previous_cache:
            return previous_cache
        if disk_cache:
            logger.info(f'Fragment: using disk cache ({len(disk_cache)}) after sync failure')
            fragment_cache = disk_cache
            fragment_cache_time = now
            return fragment_cache
        gifts = []

    fragment_cache = gifts
    fragment_cache_time = now
    return fragment_cache

def _fragment_model_cache_key(slug):
    return str(slug or '').strip().lower()

def _extract_text_from_html(raw_text):
    clean = re.sub(r'<[^>]+>', ' ', raw_text or '')
    clean = html_lib.unescape(clean)
    return re.sub(r'\s+', ' ', clean).strip()

def _extract_fragment_markdown_collections(text):
    items = []
    if not text:
        return items
    pattern = re.compile(
        r'\[!\[Image\s+\d+\]\((https?://fragment\.com/file/gifts/([a-z0-9_-]+)/thumb\.webp)\)\s*([\w\W]{1,120}?)\]\(https?://fragment\.com/gifts/([a-z0-9_-]+)\)',
        flags=re.IGNORECASE | re.DOTALL
    )
    for image_url, slug_from_img, name_raw, slug_from_link in pattern.findall(text):
        slug = (slug_from_link or slug_from_img or '').strip().lower()
        if not slug:
            continue
        pretty_name = re.sub(r'\s+[0-9][0-9,]*\s*$', '', str(name_raw or '').strip())
        if not pretty_name:
            pretty_name = slug
        items.append({
            'name': pretty_name,
            'fragment_slug': slug,
            'fragment_url': f'https://fragment.com/gifts/{slug}',
            'image': image_url.strip() if image_url else f'https://fragment.com/file/gifts/{slug}/thumb.webp'
        })
    return items

def _safe_int(value, default=0):
    try:
        if value is None:
            return int(default)
        return int(float(str(value).replace(',', '').strip()))
    except Exception:
        return int(default)

def _build_case_custom_gift_id(name, fragment_slug=None, model_name=None):
    slug = _slugify_fragment_name(fragment_slug) if fragment_slug else ''
    model_part = _slugify_fragment_name(model_name) if model_name else ''
    name_part = _slugify_fragment_name(name)
    if slug and model_part:
        return f'fragment_model:{slug}:{model_part}'
    if slug and name_part:
        return f'fragment_gift:{slug}:{name_part}'
    return f'custom_gift:{name_part or "item"}'

def _normalize_local_gift_image(src):
    img = str(src or '').strip()
    if not img:
        return ''
    if img.startswith('http') or img.startswith('/') or img.startswith('data:'):
        return img
    return '/static/gifs/gifts/' + img

def build_fragment_first_gifts_catalog(force_refresh=False):
    local_gifts = load_gifts_cached() or []
    fragment_gifts = fetch_fragment_gifts_catalog(force_refresh=force_refresh) or []

    if not fragment_gifts and FRAGMENT_ALLOW_LOCAL_ON_FAILURE:
        fallback_local = []
        for lg in local_gifts:
            fallback_local.append({
                'id': lg.get('id'),
                'name': lg.get('name') or 'Gift',
                'value': int(round(float(lg.get('value', 0)) * FRAGMENT_TON_RATE)),
                'image': _normalize_local_gift_image(lg.get('image')) or '/static/img/default_gift.png',
                'fragment_slug': (lg.get('fragment_slug') or _slugify_fragment_name(lg.get('name', ''))),
                'fragment_url': '',
                'fragment_price_ton': None,
                'source': 'local_offline_fallback'
            })
        fallback_local.sort(key=lambda x: float(x.get('value', 0) or 0), reverse=True)
        return fallback_local

    if FRAGMENT_ONLY_CATALOG and not fragment_gifts:
        return []

    fragment_by_slug = {}
    fragment_by_name = {}
    for fg in fragment_gifts:
        slug = (fg.get('fragment_slug') or '').strip().lower()
        if slug:
            fragment_by_slug[slug] = fg
        nname = _normalize_gift_name_for_match(fg.get('name'))
        if nname and nname not in fragment_by_name:
            fragment_by_name[nname] = fg

    merged = []
    seen_fragment_slugs = set()

    # 1) Fragment first: always include all gifts from Fragment catalog
    for fg in fragment_gifts:
        slug = (fg.get('fragment_slug') or '').strip().lower()
        if not slug or slug in seen_fragment_slugs:
            continue
        seen_fragment_slugs.add(slug)

        local_match = None
        normalized_fg_name = _normalize_gift_name_for_match(fg.get('name'))
        for lg in local_gifts:
            lg_slug = (lg.get('fragment_slug') or _slugify_fragment_name(lg.get('name', ''))).strip().lower()
            if lg_slug and lg_slug == slug:
                local_match = lg
                break
            if normalized_fg_name and _normalize_gift_name_for_match(lg.get('name')) == normalized_fg_name:
                local_match = lg
                break

        local_value = int(round(float((local_match or {}).get('value', 0)) * FRAGMENT_TON_RATE))
        fragment_value = _safe_int(fg.get('value'), 0)
        merged.append({
            'id': (local_match or {}).get('id'),
            'name': (local_match or {}).get('name') or fg.get('name') or slug,
            'value': fragment_value if fragment_value > 0 else local_value,
            'image': fg.get('image') or _normalize_local_gift_image((local_match or {}).get('image')) or '/static/img/default_gift.png',
            'fragment_slug': slug,
            'fragment_url': fg.get('fragment_url') or f'https://fragment.com/gifts/{slug}',
            'fragment_price_ton': fg.get('fragment_price_ton'),
            'getgems_floor_ton': fg.get('getgems_floor_ton'),
            'source': 'fragment'
        })

    # 2) Local-only gifts are added only when strict fragment-only mode is disabled
    if not FRAGMENT_ONLY_CATALOG:
        for lg in local_gifts:
            slug = (lg.get('fragment_slug') or _slugify_fragment_name(lg.get('name', ''))).strip().lower()
            nname = _normalize_gift_name_for_match(lg.get('name'))
            has_fragment = (slug and slug in fragment_by_slug) or (nname and nname in fragment_by_name)
            if has_fragment:
                continue

            merged.append({
                'id': lg.get('id'),
                'name': lg.get('name') or 'Gift',
                'value': int(round(float(lg.get('value', 0)) * FRAGMENT_TON_RATE)),
                'image': _normalize_local_gift_image(lg.get('image')) or '/static/img/default_gift.png',
                'fragment_slug': '',
                'fragment_url': '',
                'fragment_price_ton': None,
                'source': 'local'
            })

    # 3) Apply manual TON price overrides
    for item in merged:
        name_raw = str(item.get('name') or '').strip()
        # Strip "(Random)" suffix for matching
        name_clean = re.sub(r'\s*\(random\)\s*$', '', name_raw, flags=re.IGNORECASE).strip().lower()
        slug = (item.get('fragment_slug') or '').strip().lower()
        slug_key = re.sub(r'[^a-z0-9]+', '', slug) if slug else ''

        manual_ton = MANUAL_GIFT_PRICES_TON.get(name_clean)
        if manual_ton is None and slug_key:
            manual_ton = _MANUAL_PRICES_BY_SLUG.get(slug_key)
        if manual_ton is not None:
            item['fragment_price_ton'] = manual_ton
            item['value'] = int(round(manual_ton * FRAGMENT_TON_RATE))

    merged.sort(key=lambda x: float(x.get('value', 0) or 0), reverse=True)
    return merged

def build_full_catalog_with_models(force_refresh=False):
    """Returns full catalog: originals + all models from disk cache.
    Used by /api/gifts-list so frontend can display everything."""
    originals = build_fragment_first_gifts_catalog(force_refresh=force_refresh)
    result = list(originals)  # copy

    # Add all models from fragment_models_cache
    seen_ids = set()
    for g in result:
        gid = g.get('id') or g.get('gift_key') or g.get('fragment_slug')
        if gid:
            seen_ids.add(str(gid))

    for slug, model_list in fragment_models_cache.items():
        if not isinstance(model_list, list):
            continue
        # Find the parent original to inherit getgems price
        parent = next((g for g in originals if (g.get('fragment_slug') or '') == slug), None)
        for model in model_list:
            mid = str(model.get('id') or model.get('gift_key') or '')
            if mid and mid not in seen_ids:
                seen_ids.add(mid)
                m = dict(model)
                m['source'] = 'fragment_model'
                model_floor_ton = m.get('getgems_model_floor_ton')
                if model_floor_ton is not None:
                    try:
                        model_floor_ton = float(model_floor_ton)
                    except Exception:
                        model_floor_ton = None
                if model_floor_ton and model_floor_ton > 0:
                    m['getgems_floor_ton'] = model_floor_ton
                    m['value'] = int(round(model_floor_ton * FRAGMENT_TON_RATE))
                # Inherit parent value/getgems price if model has none
                if not m.get('value') and parent:
                    m['value'] = parent.get('value', 0)
                if not m.get('getgems_floor_ton') and parent:
                    m['getgems_floor_ton'] = parent.get('getgems_floor_ton')
                result.append(m)

    result.sort(key=lambda x: float(x.get('value', 0) or 0), reverse=True)
    return result

def _extract_fragment_model_from_detail_html(html_text):
    if not html_text:
        return ''
    pattern = re.compile(
        r'<div[^>]*class="table-cell"[^>]*>\s*Model\s*</div>\s*</td>\s*<td>\s*<div[^>]*class="table-cell"[^>]*>\s*<div[^>]*class="table-cell-value\s+tm-value"[^>]*>\s*<a[^>]*>(.*?)</a>',
        flags=re.IGNORECASE | re.DOTALL
    )
    m = pattern.search(html_text)
    if not m:
        return ''
    return _extract_text_from_html(m.group(1))

def _parse_fragment_grid_prices(html_text):
    if not html_text:
        return []
    price_pattern = re.compile(
        r'<a\s+href="(/gift/[a-z0-9_-]+-\d+)"\s+class="tm-grid-item"[^>]*>.*?'
        r'<div[^>]*class="tm-grid-item-value\s+tm-value\s+icon-before\s+icon-ton"[^>]*>([^<]+)</div>',
        flags=re.IGNORECASE | re.DOTALL
    )
    rows = []
    for gift_path, raw_price in price_pattern.findall(html_text):
        rows.append((gift_path.strip(), _safe_int(raw_price, 0)))
    return rows

def fetch_fragment_gift_models(slug, base_name='', base_value=0, base_image='', force_refresh=False):
    cache_key = _fragment_model_cache_key(slug)
    if not cache_key:
        return []

    now = time.time()
    last_time = fragment_models_cache_time.get(cache_key)
    if not force_refresh and cache_key in fragment_models_cache and last_time is not None:
        if (now - last_time) < FRAGMENT_CACHE_DURATION:
            return fragment_models_cache.get(cache_key, [])

    models = []
    previous = fragment_models_cache.get(cache_key, [])
    # Р•СЃР»Рё РІ РїР°РјСЏС‚Рё СѓР¶Рµ РµСЃС‚СЊ РјРѕРґРµР»Рё (Р·Р°РіСЂСѓР¶РµРЅС‹ РёР· РґРёСЃРєРѕРІРѕРіРѕ РєСЌС€Р°) вЂ” РѕС‚РґР°С‘Рј РёС…
    if previous and not force_refresh:
        return previous
    try:
        url = f'https://fragment.com/gifts/{cache_key}'
        resp = _fragment_get(url)
        if resp.status_code != 200:
            return previous

        html = resp.text
        pattern = re.compile(
            r'<div[^>]*class="[^"]*tm-main-filters-item[^"]*js-attribute-item[^"]*"[^>]*>.*?'
            r'<img[^>]+src="([^"]*?/model\.[^"]+)"[^>]*>.*?'
            r'<div[^>]*class="[^"]*tm-main-filters-name[^"]*"[^>]*>(.*?)</div>.*?'
            r'<div[^>]*class="[^"]*tm-main-filters-count[^"]*"[^>]*>(.*?)</div>',
            flags=re.IGNORECASE | re.DOTALL
        )
        seen = set()
        model_price_by_slug = {}

        # Parse collection listings (already sorted by price by default)
        listing_rows = _parse_fragment_grid_prices(html)
        max_price_samples = max(20, int(os.getenv('FRAGMENT_MODEL_PRICE_SAMPLES', '90') or 90))
        for gift_path, listing_price in listing_rows[:max_price_samples]:
            if listing_price <= 0:
                continue
            try:
                detail_url = f'https://fragment.com{gift_path}'
                detail_resp = _fragment_get(detail_url)
                if detail_resp.status_code != 200:
                    continue
                detail_model = _extract_fragment_model_from_detail_html(detail_resp.text)
                if not detail_model:
                    continue
                detail_slug = _slugify_fragment_name(detail_model)
                if not detail_slug:
                    continue
                current_floor = model_price_by_slug.get(detail_slug)
                if current_floor is None or listing_price < current_floor:
                    model_price_by_slug[detail_slug] = listing_price
            except Exception:
                continue

        for image_src, model_name_raw, count_raw in pattern.findall(html):
            model_name = _extract_text_from_html(model_name_raw)
            if not model_name:
                continue
            model_slug = _slugify_fragment_name(model_name)
            if not model_slug or model_slug in seen:
                continue
            seen.add(model_slug)

            image_url = image_src.strip()
            if image_url.startswith('//'):
                image_url = 'https:' + image_url
            elif image_url.startswith('/'):
                image_url = 'https://fragment.com' + image_url

            count_value = _safe_int(re.sub(r'[^0-9]', '', count_raw or ''), 0)
            model_value = _safe_int(model_price_by_slug.get(model_slug), _safe_int(base_value, 0))
            models.append({
                'id': _build_case_custom_gift_id(f'{base_name} {model_name}'.strip(), fragment_slug=cache_key, model_name=model_name),
                'gift_key': _build_case_custom_gift_id(f'{base_name} {model_name}'.strip(), fragment_slug=cache_key, model_name=model_name),
                'name': f'{base_name} вЂў {model_name}'.strip(' вЂў'),
                'base_name': base_name,
                'model_name': model_name,
                'model_count': count_value,
                'type': 'fragment_model',
                'fragment_slug': cache_key,
                'fragment_url': f'https://fragment.com/gifts/{cache_key}',
                'image': image_url or base_image,
                'value': model_value
            })

        if not models:
            md_model_pattern = re.compile(
                rf'!\[Image\s+\d+\]\((https?://fragment\.com/file/gifts/{re.escape(cache_key)}/model\.[^)]+)\)\s+([\w\W]{{1,80}}?)\s+(\d+)\s*(?:\n|\r|$)',
                flags=re.IGNORECASE
            )
            seen_md = set()
            for image_url, model_name_raw, count_raw in md_model_pattern.findall(html):
                model_name = re.sub(r'\s+', ' ', str(model_name_raw or '').strip())
                model_name = re.sub(r'\s*[\|`].*$', '', model_name).strip()
                if not model_name:
                    continue
                model_slug = _slugify_fragment_name(model_name)
                if not model_slug or model_slug in seen_md:
                    continue
                seen_md.add(model_slug)

                count_value = _safe_int(count_raw, 0)
                model_value = _safe_int(model_price_by_slug.get(model_slug), _safe_int(base_value, 0))
                models.append({
                    'id': _build_case_custom_gift_id(f'{base_name} {model_name}'.strip(), fragment_slug=cache_key, model_name=model_name),
                    'gift_key': _build_case_custom_gift_id(f'{base_name} {model_name}'.strip(), fragment_slug=cache_key, model_name=model_name),
                    'name': f'{base_name} вЂў {model_name}'.strip(' вЂў'),
                    'base_name': base_name,
                    'model_name': model_name,
                    'model_count': count_value,
                    'type': 'fragment_model',
                    'fragment_slug': cache_key,
                    'fragment_url': f'https://fragment.com/gifts/{cache_key}',
                    'image': str(image_url or '').strip() or base_image,
                    'value': model_value
                })
    except Exception as e:
        logger.warning(f'Fragment models sync failed for {cache_key}: {e}')
        return previous

    fragment_models_cache[cache_key] = models
    fragment_models_cache_time[cache_key] = now
    return models

def _resolve_case_gift_payload(gifts, selected_gift_info):
    """Resolve a case gift entry to a full gift dict.
    Searches local gifts, Fragment catalog (originals + models), and falls back to gift_info fields."""
    if not selected_gift_info or selected_gift_info.get('type') == 'ton_balance':
        return None

    target_id = selected_gift_info.get('id')
    target_id_str = str(target_id) if target_id is not None else ''
    gift = None

    # 1. Search by ID in provided gifts list (local gifts.json)
    if target_id is not None:
        gift = next((g for g in gifts if str(g.get('id')) == target_id_str), None)

    # 2. Search Fragment catalog (originals + models) by id or gift_key
    if not gift and target_id_str:
        try:
            fragment_all = build_full_catalog_with_models()
            gift = next((g for g in fragment_all if str(g.get('id')) == target_id_str or str(g.get('gift_key')) == target_id_str), None)
        except Exception:
            pass

    if gift:
        resolved = dict(gift)
        resolved['id'] = gift.get('id') or gift.get('gift_key') or target_id
        return resolved

    # 3. Fallback: construct from gift_info fields (custom/fragment gifts in case)
    name = str(selected_gift_info.get('name') or '').strip()
    image = str(selected_gift_info.get('image') or '').strip()
    value = _safe_int(selected_gift_info.get('value'), 0)
    if not name:
        return None

    return {
        'id': target_id if isinstance(target_id, int) else -1,
        'name': name,
        'image': image or '/static/img/default_gift.png',
        'value': value,
        'gift_key': selected_gift_info.get('gift_key'),
        'fragment_slug': selected_gift_info.get('fragment_slug'),
        'model_name': selected_gift_info.get('model_name'),
        'type': selected_gift_info.get('type', 'gift')
    }

def load_gifts():
    """Р—Р°РіСЂСѓР¶Р°РµС‚ РїРѕРґР°СЂРєРё РёР· JSON С„Р°Р№Р»Р°"""
    try:
        # РџСЂРѕР±СѓРµРј РѕСЃРЅРѕРІРЅРѕР№ РїСѓС‚СЊ
        file_path = os.path.join(BASE_PATH, 'data', 'gifts.json')
        
        # РђР»СЊС‚РµСЂРЅР°С‚РёРІРЅС‹Рµ РїСѓС‚Рё РґР»СЏ PythonAnywhere
        alt_paths = [
            '/home/rasswetik52/mysite/data/gifts.json',
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'gifts.json'),
        ]
        
        # РС‰РµРј СЃСѓС‰РµСЃС‚РІСѓСЋС‰РёР№ С„Р°Р№Р»
        actual_path = None
        if os.path.exists(file_path):
            actual_path = file_path
        else:
            for alt in alt_paths:
                if os.path.exists(alt):
                    actual_path = alt
                    logger.info(f"Gifts: РёСЃРїРѕР»СЊР·СѓСЋ Р°Р»СЊС‚РµСЂРЅР°С‚РёРІРЅС‹Р№ РїСѓС‚СЊ: {alt}")
                    break
        
        if not actual_path:
            logger.warning(f"Р¤Р°Р№Р» gifts.json РЅРµ РЅР°Р№РґРµРЅ. РџСЂРѕРІРµСЂРµРЅС‹ РїСѓС‚Рё: {file_path}, {alt_paths}")
            return []
        
        logger.info(f"Gifts: Р·Р°РіСЂСѓР·РєР° РёР· {actual_path}")
        with open(actual_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # РџРѕРґРґРµСЂР¶РєР° РѕР±РѕРёС… С„РѕСЂРјР°С‚РѕРІ: {gifts: [...]} Рё РїСЂРѕСЃС‚Рѕ [...]
            if isinstance(data, list):
                logger.info(f"Р—Р°РіСЂСѓР¶РµРЅРѕ {len(data)} РїРѕРґР°СЂРєРѕРІ (С„РѕСЂРјР°С‚ array)")
                return data
            elif isinstance(data, dict):
                gifts = data.get('gifts', [])
                logger.info(f"Р—Р°РіСЂСѓР¶РµРЅРѕ {len(gifts)} РїРѕРґР°СЂРєРѕРІ (С„РѕСЂРјР°С‚ object)")
                return gifts
            else:
                logger.error(f"РќРµРІРµСЂРЅС‹Р№ С„РѕСЂРјР°С‚ gifts.json: {type(data)}")
                return []
    except json.JSONDecodeError as e:
        logger.error(f"РћС€РёР±РєР° РїР°СЂСЃРёРЅРіР° gifts.json: {e}")
        return []
    except Exception as e:
        logger.error(f"РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё gifts.json: {e}")
        return []

def save_gifts(gifts):
    """РЎРѕС…СЂР°РЅСЏРµС‚ РїРѕРґР°СЂРєРё РІ JSON С„Р°Р№Р»"""
    try:
        file_path = os.path.join(BASE_PATH, 'data', 'gifts.json')

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump({'gifts': gifts}, f, ensure_ascii=False, indent=2)

        logger.info(f"вњ… РЎРѕС…СЂР°РЅРµРЅРѕ {len(gifts)} РїРѕРґР°СЂРєРѕРІ")
        return True
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ РїРѕРґР°СЂРєРѕРІ: {e}")
        return False

def load_cases():
    """Р—Р°РіСЂСѓР¶Р°РµС‚ РєРµР№СЃС‹ РёР· JSON С„Р°Р№Р»Р°"""
    try:
        file_path = os.path.join(BASE_PATH, 'data', 'cases.json')

        if not os.path.exists(file_path):
            logger.error(f"вќЊ Р¤Р°Р№Р» cases.json РЅРµ РЅР°Р№РґРµРЅ!")
            return []

        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            cases = data.get('cases', [])
            logger.info(f"вњ… Р—Р°РіСЂСѓР¶РµРЅРѕ {len(cases)} РєРµР№СЃРѕРІ")
            return cases

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё РєРµР№СЃРѕРІ: {e}")
        return []

def save_cases(cases):
    """РЎРѕС…СЂР°РЅСЏРµС‚ РєРµР№СЃС‹ РІ JSON С„Р°Р№Р»"""
    try:
        file_path = os.path.join(BASE_PATH, 'data', 'cases.json')

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump({'cases': cases}, f, ensure_ascii=False, indent=2)

        logger.info(f"вњ… РЎРѕС…СЂР°РЅРµРЅРѕ {len(cases)} РєРµР№СЃРѕРІ")
        return True
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ РєРµР№СЃРѕРІ: {e}")
        return False

def load_case_sections():
    """Р—Р°РіСЂСѓР¶Р°РµС‚ СЃРїРёСЃРѕРє СЂР°Р·РґРµР»РѕРІ РєРµР№СЃРѕРІ"""
    try:
        file_path = os.path.join(BASE_PATH, 'data', 'case_sections.json')
        if not os.path.exists(file_path):
            return []
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            sections = data.get('sections', []) if isinstance(data, dict) else []
            sections.sort(key=lambda x: x.get('order', 0))
            return sections
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё СЂР°Р·РґРµР»РѕРІ РєРµР№СЃРѕРІ: {e}")
        return []

def save_case_sections(sections):
    """РЎРѕС…СЂР°РЅСЏРµС‚ СЃРїРёСЃРѕРє СЂР°Р·РґРµР»РѕРІ РєРµР№СЃРѕРІ"""
    try:
        file_path = os.path.join(BASE_PATH, 'data', 'case_sections.json')
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump({'sections': sections}, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ СЂР°Р·РґРµР»РѕРІ РєРµР№СЃРѕРІ: {e}")
        return False

def normalize_section_id(value):
    text = str(value or '').strip().lower()
    text = re.sub(r'\s+', '_', text)
    text = re.sub(r'[^a-z0-9_\-Р°-СЏС‘]', '', text)
    return text[:40] or 'other'

# Р¤Р»Р°Рі РіРѕС‚РѕРІРЅРѕСЃС‚Рё Р‘Р” вЂ” РёРіСЂРѕРІС‹Рµ С†РёРєР»С‹ Р¶РґСѓС‚ РµРіРѕ РїРµСЂРµРґ СЂР°Р±РѕС‚РѕР№
_db_ready = False
_db_lock = threading.Lock()  # РћРґРёРЅ Р·Р°РјРѕРє РЅР° РІСЃРµ РѕРїРµСЂР°С†РёРё СЃ Р‘Р”

DB_PATH = os.path.join(BASE_PATH, 'data', 'raswet_gifts.db')

def get_db_connection():
    """РџРѕР»СѓС‡Р°РµС‚ СЃРѕРµРґРёРЅРµРЅРёРµ СЃ Р±Р°Р·РѕР№ РґР°РЅРЅС‹С… СЃ Р·Р°С‰РёС‚РѕР№ РѕС‚ РїРѕРІСЂРµР¶РґРµРЅРёР№"""
    global _db_ready
    
    for attempt in range(3):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False, isolation_level='DEFERRED')
            # WAL mode - better for concurrent reads/writes (game loop + web requests)
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA wal_autocheckpoint = 1000")
            
            # РџСЂРѕРІРµСЂСЏРµРј С‚Р°Р±Р»РёС†С‹ С‚РѕР»СЊРєРѕ РѕРґРёРЅ СЂР°Р·
            if not _db_ready:
                try:
                    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                    table_names = {r[0] for r in tables}
                    required = {'users', 'inventory', 'ultimate_crash_games', 'ultimate_crash_bets', 'auth_codes'}
                    if not required.issubset(table_names):
                        _create_all_tables(conn)
                        conn.commit()
                    _db_ready = True
                except:
                    pass
            return conn
        except sqlite3.DatabaseError as e:
            if 'malformed' in str(e) or 'disk' in str(e):
                logger.error(f"вќЊ Р‘Р” РїРѕРІСЂРµР¶РґРµРЅР°, РїРѕРїС‹С‚РєР° РІРѕСЃСЃС‚Р°РЅРѕРІР»РµРЅРёСЏ {attempt+1}/3")
                try:
                    # РЈРґР°Р»СЏРµРј РїРѕРІСЂРµР¶РґС‘РЅРЅС‹Рµ С„Р°Р№Р»С‹
                    for ext in ['-wal', '-shm', '-journal']:
                        p = DB_PATH + ext
                        if os.path.exists(p):
                            os.remove(p)
                except:
                    pass
                time.sleep(0.5)
            else:
                raise
    
    # РџРѕСЃР»РµРґРЅСЏСЏ РїРѕРїС‹С‚РєР°
    return sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)

def _nuke_db():
    """РџРѕР»РЅРѕСЃС‚СЊСЋ СѓРґР°Р»СЏРµС‚ РІСЃРµ С„Р°Р№Р»С‹ Р‘Р” (РІС‹Р·С‹РІР°С‚СЊ РїРѕРґ _db_lock!)"""
    for ext in ['', '-wal', '-shm', '-journal']:
        p = DB_PATH + ext
        try:
            if os.path.exists(p):
                os.remove(p)
                logger.info(f"рџ—‘пёЏ РЈРґР°Р»С‘РЅ: {p}")
        except Exception as e:
            logger.error(f"РќРµ СѓРґР°Р»РѕСЃСЊ СѓРґР°Р»РёС‚СЊ {p}: {e}")

def _check_disk_space():
    """РџСЂРѕРІРµСЂСЏРµС‚ СЃРІРѕР±РѕРґРЅРѕРµ РјРµСЃС‚Рѕ РЅР° РґРёСЃРєРµ"""
    try:
        import shutil as _shutil
        total, used, free = _shutil.disk_usage(os.path.join(BASE_PATH, 'data'))
        free_mb = free / (1024 * 1024)
        if free_mb < 5:
            logger.error(f"рџљЁ РљР РРўРР§Р•РЎРљР РњРђР›Рћ РњР•РЎРўРђ РќРђ Р”РРЎРљР•: {free_mb:.1f} MB СЃРІРѕР±РѕРґРЅРѕ!")
            return False
        logger.info(f"рџ’ѕ РЎРІРѕР±РѕРґРЅРѕ РЅР° РґРёСЃРєРµ: {free_mb:.0f} MB")
        return True
    except Exception as e:
        logger.warning(f"РќРµ СѓРґР°Р»РѕСЃСЊ РїСЂРѕРІРµСЂРёС‚СЊ РґРёСЃРє: {e}")
        return True  # Р•СЃР»Рё РЅРµ РјРѕР¶РµРј РїСЂРѕРІРµСЂРёС‚СЊ вЂ” РїСЂРѕРґРѕР»Р¶Р°РµРј

def _create_all_tables(conn):
    """РЎРѕР·РґР°С‘С‚ РІСЃРµ С‚Р°Р±Р»РёС†С‹ РїРѕ РѕРґРЅРѕР№. Р’РѕР·РІСЂР°С‰Р°РµС‚ True РµСЃР»Рё Р’РЎР• РћРљ."""
    tables_sql = {
        'users': '''CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            username TEXT,
            photo_url TEXT,
            balance_stars INTEGER DEFAULT 0,
            balance_tickets INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            referral_code TEXT UNIQUE,
            referred_by INTEGER,
            referral_count INTEGER DEFAULT 0,
            total_earned_stars INTEGER DEFAULT 0,
            total_earned_tickets INTEGER DEFAULT 0,
            referral_bonus_claimed BOOLEAN DEFAULT FALSE,
            experience INTEGER DEFAULT 0,
            current_level INTEGER DEFAULT 1,
            total_cases_opened INTEGER DEFAULT 0,
            last_daily_bonus TIMESTAMP,
            consecutive_days INTEGER DEFAULT 0,
            total_loss INTEGER DEFAULT 0,
            ton_wallet TEXT,
            currency_mode TEXT DEFAULT 'stars',
            total_crash_bets INTEGER DEFAULT 0,
            total_bet_volume INTEGER DEFAULT 0
        )''',
        'inventory': '''CREATE TABLE IF NOT EXISTS inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            gift_id INTEGER,
            gift_name TEXT,
            gift_image TEXT,
            gift_value INTEGER,
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_withdrawing BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )''',
        'user_history': '''CREATE TABLE IF NOT EXISTS user_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            operation_type TEXT NOT NULL,
            amount INTEGER NOT NULL,
            description TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )''',
        'case_limits': '''CREATE TABLE IF NOT EXISTS case_limits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id INTEGER NOT NULL,
            current_amount INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(case_id)
        )''',
        'referrals': '''CREATE TABLE IF NOT EXISTS referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER,
            referred_id INTEGER,
            reward_claimed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (referrer_id) REFERENCES users (id),
            FOREIGN KEY (referred_id) REFERENCES users (id),
            UNIQUE(referred_id)
        )''',
        'referral_rewards': '''CREATE TABLE IF NOT EXISTS referral_rewards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER,
            reward_type TEXT NOT NULL,
            reward_amount INTEGER NOT NULL,
            description TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (referrer_id) REFERENCES users (id)
        )''',
        'withdrawals': '''CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            inventory_id INTEGER NOT NULL,
            gift_name TEXT NOT NULL,
            gift_image TEXT NOT NULL,
            gift_value INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            telegram_username TEXT,
            user_photo_url TEXT,
            user_first_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP,
            admin_notes TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (inventory_id) REFERENCES inventory (id)
        )''',
        'deposits': '''CREATE TABLE IF NOT EXISTS deposits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            currency TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            payment_method TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            telegram_payment_charge_id TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )''',
        'promo_codes': '''CREATE TABLE IF NOT EXISTS promo_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            reward_stars INTEGER DEFAULT 0,
            reward_tickets INTEGER DEFAULT 0,
            reward_type TEXT DEFAULT 'stars',
            reward_data TEXT DEFAULT NULL,
            max_uses INTEGER DEFAULT 1,
            used_count INTEGER DEFAULT 0,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )''',
        'used_promo_codes': '''CREATE TABLE IF NOT EXISTS used_promo_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            promo_code_id INTEGER NOT NULL,
            used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (promo_code_id) REFERENCES promo_codes (id),
            UNIQUE(user_id, promo_code_id)
        )''',
        'user_customizations': '''CREATE TABLE IF NOT EXISTS user_customizations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            item_type TEXT NOT NULL,
            item_id TEXT NOT NULL,
            source TEXT DEFAULT 'unlock',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            UNIQUE(user_id, item_type, item_id)
        )''',
        'user_discounts': '''CREATE TABLE IF NOT EXISTS user_discounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            discount_type TEXT NOT NULL,
            discount_value INTEGER DEFAULT 0,
            case_id INTEGER,
            expires_at TIMESTAMP,
            used INTEGER DEFAULT 0,
            promo_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )''',
        'user_wagers': '''CREATE TABLE IF NOT EXISTS user_wagers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            bonus_amount INTEGER DEFAULT 0,
            wager_requirement INTEGER DEFAULT 0,
            wagered_amount INTEGER DEFAULT 0,
            is_completed INTEGER DEFAULT 0,
            promo_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )''',
        'user_levels': '''CREATE TABLE IF NOT EXISTS user_levels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            level INTEGER DEFAULT 1,
            experience INTEGER DEFAULT 0,
            total_experience INTEGER DEFAULT 0,
            last_level_up TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            UNIQUE(user_id)
        )''',
        'level_history': '''CREATE TABLE IF NOT EXISTS level_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            old_level INTEGER,
            new_level INTEGER,
            experience_gained INTEGER,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )''',
        'win_history': '''CREATE TABLE IF NOT EXISTS win_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            user_name TEXT,
            gift_name TEXT,
            gift_image TEXT,
            gift_value INTEGER,
            case_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )''',
        'case_open_history': '''CREATE TABLE IF NOT EXISTS case_open_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            case_id INTEGER NOT NULL,
            case_name TEXT,
            gift_id INTEGER,
            gift_name TEXT,
            gift_image TEXT,
            gift_value INTEGER,
            cost INTEGER DEFAULT 0,
            cost_type TEXT DEFAULT 'stars',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )''',
        'ultimate_crash_games': '''CREATE TABLE IF NOT EXISTS ultimate_crash_games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT DEFAULT 'waiting',
            current_multiplier DECIMAL(10,2) DEFAULT 1.00,
            target_multiplier DECIMAL(10,2) DEFAULT 5.00,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'ultimate_crash_bets': '''CREATE TABLE IF NOT EXISTS ultimate_crash_bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER,
            user_id INTEGER,
            bet_amount INTEGER DEFAULT 0,
            gift_value INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active',
            cashout_multiplier DECIMAL(10,2),
            win_amount INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES ultimate_crash_games (id)
        )''',
        'ultimate_crash_history': '''CREATE TABLE IF NOT EXISTS ultimate_crash_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER,
            final_multiplier DECIMAL(10,2),
            total_bets INTEGER DEFAULT 0,
            total_amount INTEGER DEFAULT 0,
            finished_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'crash_games': '''CREATE TABLE IF NOT EXISTS crash_games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            multiplier DECIMAL(10,2) DEFAULT 1.00,
            status TEXT DEFAULT 'waiting',
            current_multiplier DECIMAL(10,2) DEFAULT 1.00,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'crash_bets': '''CREATE TABLE IF NOT EXISTS crash_bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER,
            user_id INTEGER,
            bet_amount INTEGER DEFAULT 0,
            bet_type TEXT DEFAULT 'stars',
            gift_id INTEGER,
            gift_name TEXT,
            gift_image TEXT,
            gift_value INTEGER,
            multiplier DECIMAL(10,2) DEFAULT 1.00,
            status TEXT DEFAULT 'active',
            cashout_multiplier DECIMAL(10,2),
            win_amount INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES crash_games (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        )''',
        'crash_history': '''CREATE TABLE IF NOT EXISTS crash_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER,
            multiplier DECIMAL(10,2),
            total_bets INTEGER DEFAULT 0,
            total_amount INTEGER DEFAULT 0,
            finished_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES crash_games (id)
        )''',
        'notifications': '''CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            width INTEGER DEFAULT 80,
            pages TEXT DEFAULT '[]',
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            admin_id INTEGER DEFAULT 0
        )''',
        'user_notifications': '''CREATE TABLE IF NOT EXISTS user_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            notification_id INTEGER NOT NULL,
            shown BOOLEAN DEFAULT FALSE,
            shown_at TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (notification_id) REFERENCES notifications (id),
            UNIQUE(user_id, notification_id)
        )''',
        'auth_codes': '''CREATE TABLE IF NOT EXISTS auth_codes (
            code TEXT PRIMARY KEY,
            confirmed INTEGER DEFAULT 0,
            user_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'ton_payments': '''CREATE TABLE IF NOT EXISTS ton_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            ton_amount REAL NOT NULL,
            ton_amount INTEGER NOT NULL,
            tx_hash TEXT,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            confirmed_at TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )''',
        'daily_tasks': '''CREATE TABLE IF NOT EXISTS daily_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_type TEXT NOT NULL,
            case_id INTEGER DEFAULT 0,
            target_value INTEGER NOT NULL,
            reward_stars INTEGER NOT NULL,
            description TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'user_daily_progress': '''CREATE TABLE IF NOT EXISTS user_daily_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            task_id INTEGER NOT NULL,
            progress INTEGER DEFAULT 0,
            completed BOOLEAN DEFAULT FALSE,
            reward_claimed BOOLEAN DEFAULT FALSE,
            date TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (task_id) REFERENCES daily_tasks (id),
            UNIQUE(user_id, task_id, date)
        )''',
        'reward_claims': '''CREATE TABLE IF NOT EXISTS reward_claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            reward_type TEXT NOT NULL,
            reward_id TEXT NOT NULL,
            reward_stars INTEGER NOT NULL,
            claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            UNIQUE(user_id, reward_type, reward_id)
        )''',
        'crash_quests': '''CREATE TABLE IF NOT EXISTS crash_quests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            quest_type TEXT NOT NULL,
            target_value INTEGER NOT NULL DEFAULT 1,
            reward_type TEXT NOT NULL DEFAULT 'stars',
            reward_amount INTEGER DEFAULT 0,
            reward_data TEXT,
            icon TEXT,
            sort_order INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'user_quest_progress': '''CREATE TABLE IF NOT EXISTS user_quest_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            quest_id INTEGER NOT NULL,
            progress INTEGER DEFAULT 0,
            completed BOOLEAN DEFAULT FALSE,
            reward_claimed BOOLEAN DEFAULT FALSE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (quest_id) REFERENCES crash_quests (id),
            UNIQUE(user_id, quest_id)
        )''',
        'crash_customizations': '''CREATE TABLE IF NOT EXISTS crash_customizations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_type TEXT NOT NULL,
            item_id TEXT NOT NULL,
            name TEXT,
            is_vip INTEGER DEFAULT 0,
            is_default INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(item_type, item_id)
        )''',
        'shop_deals': '''CREATE TABLE IF NOT EXISTS shop_deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            section TEXT DEFAULT 'general',
            items TEXT NOT NULL DEFAULT '[]',
            price INTEGER NOT NULL DEFAULT 100,
            old_price INTEGER DEFAULT 0,
            currency TEXT DEFAULT 'stars',
            duration_hours INTEGER DEFAULT 0,
            starts_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ends_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            sort_order INTEGER DEFAULT 0,
            icon TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'shop_purchases': '''CREATE TABLE IF NOT EXISTS shop_purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            deal_id INTEGER NOT NULL,
            price_paid INTEGER NOT NULL,
            currency TEXT DEFAULT 'stars',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (deal_id) REFERENCES shop_deals (id)
        )''',
        'gift_deposits': '''CREATE TABLE IF NOT EXISTS gift_deposits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            gift_name TEXT,
            gift_value INTEGER DEFAULT 0,
            gift_type TEXT DEFAULT 'regular',
            telegram_gift_id TEXT,
            message_id INTEGER,
            status TEXT DEFAULT 'confirmed',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'nft_monitor_processed': '''CREATE TABLE IF NOT EXISTS nft_monitor_processed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gift_type TEXT NOT NULL,
            gift_name TEXT NOT NULL,
            gift_base_name TEXT,
            sender_id INTEGER,
            send_date INTEGER NOT NULL,
            stars_credited INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'stars_payments': '''CREATE TABLE IF NOT EXISTS stars_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            charge_id TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'sbp_payments': '''CREATE TABLE IF NOT EXISTS sbp_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            stars INTEGER NOT NULL,
            amount_rub REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            payment_id TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            completed_at DATETIME
        )''',
        'admin_notifications': '''CREATE TABLE IF NOT EXISTS admin_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            message TEXT DEFAULT '',
            image_url TEXT DEFAULT '',
            notif_type TEXT DEFAULT 'general',
            target_user_id INTEGER DEFAULT NULL,
            reward_type TEXT DEFAULT NULL,
            reward_data TEXT DEFAULT NULL,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'leaderboard_config': '''CREATE TABLE IF NOT EXISTS leaderboard_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            is_active BOOLEAN DEFAULT 1,
            period_start TIMESTAMP NOT NULL,
            period_end TIMESTAMP NOT NULL,
            rewards_json TEXT DEFAULT '{}',
            track_field TEXT DEFAULT 'total_bet_volume',
            title TEXT DEFAULT 'Р›РёРґРµСЂР±РѕСЂРґ',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'leaderboard_history': '''CREATE TABLE IF NOT EXISTS leaderboard_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            period_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            turnover INTEGER DEFAULT 0,
            reward_type TEXT,
            reward_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (period_id) REFERENCES leaderboard_config (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        )''',
        'crash_bots_config': '''CREATE TABLE IF NOT EXISTS crash_bots_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_name TEXT NOT NULL,
            avatar_url TEXT DEFAULT '',
            min_bet INTEGER DEFAULT 25,
            max_bet INTEGER DEFAULT 500,
            auto_cashout_min REAL DEFAULT 1.2,
            auto_cashout_max REAL DEFAULT 5.0,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        'crash_bots_settings': '''CREATE TABLE IF NOT EXISTS crash_bots_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            enabled BOOLEAN DEFAULT 0,
            min_active_bots INTEGER DEFAULT 2,
            max_active_bots INTEGER DEFAULT 5,
            min_real_players_threshold INTEGER DEFAULT 3,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
    }

    ok = 0
    errors = []
    for name, sql in tables_sql.items():
        try:
            conn.execute(sql)
            ok += 1
        except Exception as e:
            errors.append(f"{name}: {e}")
            logger.error(f"вќЊ РўР°Р±Р»РёС†Р° {name}: {e}")

    # РљРѕРјРјРёС‚РёРј СЃ РѕР±СЂР°Р±РѕС‚РєРѕР№ РѕС€РёР±РѕРє
    try:
        conn.commit()
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РєРѕРјРјРёС‚Р° С‚Р°Р±Р»РёС†: {e}")
        return False

    # Р’РµСЂРёС„РёРєР°С†РёСЏ
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing = {row[0] for row in cursor.fetchall()}
    except Exception as e:
        logger.error(f"вќЊ РќРµ СѓРґР°Р»РѕСЃСЊ РїСЂРѕС‡РёС‚Р°С‚СЊ СЃРїРёСЃРѕРє С‚Р°Р±Р»РёС†: {e}")
        return False

    required = {'users', 'inventory', 'ultimate_crash_games', 'ultimate_crash_bets',
                 'crash_games', 'crash_bets', 'auth_codes'}
    missing = required - existing
    if missing:
        logger.error(f"вќЊ РќРµ СЃРѕР·РґР°РЅС‹ С‚Р°Р±Р»РёС†С‹: {missing}")
        if errors:
            logger.error(f"вќЊ РћС€РёР±РєРё РїСЂРё СЃРѕР·РґР°РЅРёРё: {errors}")
        return False
    logger.info(f"вњ… {ok}/{len(tables_sql)} С‚Р°Р±Р»РёС† OK, РІСЃРµРіРѕ РІ Р‘Р”: {len(existing)}")
    
    # Create indexes for performance
    indexes = [
        'CREATE INDEX IF NOT EXISTS idx_crash_games_status ON ultimate_crash_games(status)',
        'CREATE INDEX IF NOT EXISTS idx_crash_bets_game_user ON ultimate_crash_bets(game_id, user_id)',
        'CREATE INDEX IF NOT EXISTS idx_crash_bets_status ON ultimate_crash_bets(status)',
        'CREATE INDEX IF NOT EXISTS idx_inventory_user ON inventory(user_id)',
        'CREATE INDEX IF NOT EXISTS idx_user_history_user ON user_history(user_id)',
    ]
    for idx_sql in indexes:
        try:
            conn.execute(idx_sql)
        except:
            pass
    try:
        conn.commit()
    except:
        pass
    logger.info("вњ… РРЅРґРµРєСЃС‹ СЃРѕР·РґР°РЅС‹")

    # Migrate inventory table: add crate columns
    try:
        cursor2 = conn.cursor()
        cursor2.execute("PRAGMA table_info('inventory')")
        inv_cols = [r[1] for r in cursor2.fetchall()]
        if 'crate_id' not in inv_cols:
            try: conn.execute("ALTER TABLE inventory ADD COLUMN crate_id INTEGER DEFAULT NULL")
            except: pass
        if 'crate_name' not in inv_cols:
            try: conn.execute("ALTER TABLE inventory ADD COLUMN crate_name TEXT DEFAULT NULL")
            except: pass
        if 'crate_image' not in inv_cols:
            try: conn.execute("ALTER TABLE inventory ADD COLUMN crate_image TEXT DEFAULT NULL")
            except: pass
        conn.commit()
    except Exception as mig_e:
        logger.warning(f"Inventory migration: {mig_e}")

    # === Bonus system tables ===
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS levels (
            level INTEGER PRIMARY KEY,
            exp_required INTEGER NOT NULL DEFAULT 0,
            reward_stars INTEGER NOT NULL DEFAULT 0,
            reward_tickets INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS level_rewards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level INTEGER NOT NULL,
            reward_type TEXT NOT NULL,
            reward_data TEXT NOT NULL DEFAULT '{}',
            description TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS user_bonuses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            bonus_type TEXT NOT NULL,
            bonus_data TEXT NOT NULL DEFAULT '{}',
            source TEXT DEFAULT '',
            source_id INTEGER DEFAULT NULL,
            is_claimed INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            claimed_at TIMESTAMP DEFAULT NULL
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS user_gift_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            gift_name TEXT NOT NULL,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, gift_name)
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS deposit_promos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            bonus_percent INTEGER NOT NULL DEFAULT 10,
            max_uses INTEGER DEFAULT 0,
            used_count INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS used_deposit_promos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            promo_id INTEGER NOT NULL,
            deposit_amount INTEGER DEFAULT 0,
            bonus_amount INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, promo_id)
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS notification_reads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            notification_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, notification_id)
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS admin_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            message TEXT DEFAULT '',
            image_url TEXT DEFAULT '',
            notif_type TEXT DEFAULT 'general',
            target_user_id INTEGER DEFAULT NULL,
            reward_type TEXT DEFAULT NULL,
            reward_data TEXT DEFAULT NULL,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        # РњРёРіСЂР°С†РёСЏ: РґРѕР±Р°РІР»СЏРµРј case_stars РµСЃР»Рё РЅРµС‚
        try:
            conn.execute('ALTER TABLE users ADD COLUMN case_stars INTEGER DEFAULT 0')
        except:
            pass
        # РњРёРіСЂР°С†РёСЏ: СЂРµР¶РёРј РѕС‚РѕР±СЂР°Р¶РµРЅРёСЏ РІР°Р»СЋС‚С‹ (stars/ton)
        try:
            conn.execute('ALTER TABLE users ADD COLUMN currency_mode TEXT DEFAULT "stars"')
        except:
            pass
        # РњРёРіСЂР°С†РёСЏ: РґРѕР±Р°РІР»СЏРµРј РЅРѕРІС‹Рµ РєРѕР»РѕРЅРєРё РІ admin_notifications
        for col in ['notif_type TEXT DEFAULT "general"', 'reward_type TEXT DEFAULT NULL', 'reward_data TEXT DEFAULT NULL']:
            try:
                conn.execute(f'ALTER TABLE admin_notifications ADD COLUMN {col}')
            except:
                pass
        # РњРёРіСЂР°С†РёСЏ: РґРѕР±Р°РІР»СЏРµРј description РІ user_bonuses РµСЃР»Рё РЅРµС‚
        try:
            conn.execute('ALTER TABLE user_bonuses ADD COLUMN description TEXT DEFAULT ""')
        except:
            pass
        conn.commit()
    except Exception as bonus_e:
        logger.warning(f"Bonus tables migration: {bonus_e}")

    return True

def init_db():
    """РРЅРёС†РёР°Р»РёР·Р°С†РёСЏ Р±Р°Р·С‹ РґР°РЅРЅС‹С… вЂ” РїРѕС‚РѕРєРѕР±РµР·РѕРїР°СЃРЅР°СЏ"""
    global _db_ready

    with _db_lock:
        _db_ready = False

        data_path = os.path.join(BASE_PATH, 'data')
        os.makedirs(data_path, exist_ok=True)
        for sub in ['static/gifs/gifts', 'static/gifs/cases', 'static/uploads/notifications']:
            os.makedirs(os.path.join(BASE_PATH, sub), exist_ok=True)

        # РџСЂРѕРІРµСЂСЏРµРј РґРёСЃРє
        if not _check_disk_space():
            logger.error("рџљЁ РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РјРµСЃС‚Р° вЂ” Р‘Р” РЅРµ РјРѕР¶РµС‚ Р±С‹С‚СЊ СЃРѕР·РґР°РЅР°!")
            return False

        # РџСЂРѕРІРµСЂСЏРµРј Р·РґРѕСЂРѕРІСЊРµ СЃСѓС‰РµСЃС‚РІСѓСЋС‰РµР№ Р‘Р”
        if os.path.exists(DB_PATH):
            try:
                file_size = os.path.getsize(DB_PATH)
                if file_size < 100:
                    logger.error(f"вќЊ Р‘Р” С„Р°Р№Р» СЃР»РёС€РєРѕРј РјР°Р»РµРЅСЊРєРёР№ ({file_size} Р±Р°Р№С‚), СѓРґР°Р»СЏРµРј")
                    _nuke_db()
                else:
                    c = None
                    try:
                        c = sqlite3.connect(DB_PATH, timeout=5)
                        c.execute("PRAGMA busy_timeout = 5000")
                        tables = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                        table_names = {r[0] for r in tables}
                        if len(table_names) < 3:
                            logger.error(f"вќЊ Р‘Р” РїСѓСЃС‚Р°СЏ, С‚РѕР»СЊРєРѕ {len(table_names)} С‚Р°Р±Р»РёС†, СѓРґР°Р»СЏРµРј")
                            c.close()
                            c = None
                            _nuke_db()
                        else:
                            c.close()
                            c = None
                    except Exception as e:
                        logger.error(f"вќЊ Р‘Р” РЅРµС‡РёС‚Р°РµРјР°: {e}, СѓРґР°Р»СЏРµРј")
                        if c:
                            try: c.close()
                            except: pass
                            c = None
                        _nuke_db()
            except Exception as e:
                logger.error(f"вќЊ РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё С„Р°Р№Р»Р° Р‘Р”: {e}")
                _nuke_db()

        # РЎРѕР·РґР°С‘Рј / РґРѕРїРѕР»РЅСЏРµРј
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
            # WAL mode вЂ” consistent with get_db_connection()
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA wal_autocheckpoint = 1000")
            logger.info("рџ“Љ РЎРѕР·РґР°РЅРёРµ С‚Р°Р±Р»РёС†...")

            if not _create_all_tables(conn):
                conn.close()
                conn = None
                logger.error("вќЊ _create_all_tables РІРµСЂРЅСѓР»Р° False")
                return False

            cursor = conn.cursor()

            # РњРёРіСЂР°С†РёСЏ: РґРѕР±Р°РІР»СЏРµРј РЅРµРґРѕСЃС‚Р°СЋС‰РёРµ РєРѕР»РѕРЅРєРё
            try:
                cursor.execute("PRAGMA table_info(users)")
                columns = [col[1] for col in cursor.fetchall()]
                if 'total_bet_volume' not in columns:
                    cursor.execute('ALTER TABLE users ADD COLUMN total_bet_volume INTEGER DEFAULT 0')
                    logger.info("вњ… Р”РѕР±Р°РІР»РµРЅР° РєРѕР»РѕРЅРєР° total_bet_volume")
                if 'is_crash_vip' not in columns:
                    cursor.execute('ALTER TABLE users ADD COLUMN is_crash_vip INTEGER DEFAULT 0')
                    logger.info("вњ… Р”РѕР±Р°РІР»РµРЅР° РєРѕР»РѕРЅРєР° is_crash_vip")
                if 'total_loss' not in columns:
                    cursor.execute('ALTER TABLE users ADD COLUMN total_loss INTEGER DEFAULT 0')
                    logger.info("вњ… Р”РѕР±Р°РІР»РµРЅР° РєРѕР»РѕРЅРєР° total_loss")
                if 'total_crash_bets' not in columns:
                    cursor.execute('ALTER TABLE users ADD COLUMN total_crash_bets INTEGER DEFAULT 0')
                    logger.info("вњ… Р”РѕР±Р°РІР»РµРЅР° РєРѕР»РѕРЅРєР° total_crash_bets")
                if 'referral_balance' not in columns:
                    cursor.execute('ALTER TABLE users ADD COLUMN referral_balance INTEGER DEFAULT 0')
                    logger.info("вњ… Р”РѕР±Р°РІР»РµРЅР° РєРѕР»РѕРЅРєР° referral_balance")
                conn.commit()
            except Exception as e:
                logger.warning(f"вљ пёЏ РњРёРіСЂР°С†РёСЏ РєРѕР»РѕРЅРѕРє users: {e}")

            # Р›РёРјРёС‚С‹ РєРµР№СЃРѕРІ
            try:
                cases = load_cases()
                for case in cases:
                    if case.get('limited'):
                        cursor.execute('SELECT id FROM case_limits WHERE case_id = ?', (case['id'],))
                        if not cursor.fetchone():
                            cursor.execute('INSERT INTO case_limits (case_id, current_amount) VALUES (?, ?)',
                                           (case['id'], case['amount']))
            except Exception as e:
                logger.warning(f"вљ пёЏ Р›РёРјРёС‚С‹ РєРµР№СЃРѕРІ: {e}")

            # РќР°С‡Р°Р»СЊРЅР°СЏ РёРіСЂР°
            try:
                cursor.execute('SELECT COUNT(*) FROM ultimate_crash_games WHERE status IN ("waiting","counting","flying")')
                if cursor.fetchone()[0] == 0:
                    tm = round(random.uniform(3.0, 10.0), 2)
                    cursor.execute('INSERT INTO ultimate_crash_games (status, target_multiplier, start_time) VALUES ("waiting", ?, CURRENT_TIMESTAMP)', (tm,))
                    logger.info(f"вњ… РќР°С‡Р°Р»СЊРЅР°СЏ РёРіСЂР°: {tm}x")
            except Exception as e:
                logger.warning(f"вљ пёЏ РќР°С‡Р°Р»СЊРЅР°СЏ РёРіСЂР°: {e}")

            conn.commit()
            conn.close()
            conn = None

            # Р‘С‹СЃС‚СЂР°СЏ РїСЂРѕРІРµСЂРєР° вЂ” С„Р°Р№Р» СЂРµР°Р»СЊРЅРѕ Р·Р°РїРёСЃР°Р»СЃСЏ?
            file_size = os.path.getsize(DB_PATH)
            if file_size < 4096:
                logger.error(f"вќЊ Р‘Р” С„Р°Р№Р» РїРѕРґРѕР·СЂРёС‚РµР»СЊРЅРѕ РјР°Р»: {file_size} Р±Р°Р№С‚")
                return False

            _db_ready = True
            logger.info(f"вњ… Р‘Р°Р·Р° РґР°РЅРЅС‹С… РіРѕС‚РѕРІР°! Р Р°Р·РјРµСЂ: {file_size} Р±Р°Р№С‚")
            
            # Р—Р°РіСЂСѓР·РєР° СѓСЂРѕРІРЅРµР№ РёР· Р‘Р”
            _sync_levels_from_db()

            # Load crash bots config
            try:
                _load_crash_bots()
                logger.info(f"вњ… Crash Р±РѕС‚С‹ Р·Р°РіСЂСѓР¶РµРЅС‹: {len(_crash_bots_cache.get('bots', []))} Р±РѕС‚РѕРІ, enabled={_crash_bots_cache.get('enabled')}")
            except Exception as be:
                logger.warning(f"вљ пёЏ РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё crash Р±РѕС‚РѕРІ: {be}")
            
            return True

        except Exception as e:
            logger.error(f"вќЊ init_db: {e}")
            logger.error(traceback.format_exc())
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass

def safe_init_db(max_retries=3):
    """init_db СЃ РїРѕРІС‚РѕСЂРЅС‹РјРё РїРѕРїС‹С‚РєР°РјРё"""
    for attempt in range(1, max_retries + 1):
        logger.info(f"рџ“Ђ init_db РїРѕРїС‹С‚РєР° {attempt}/{max_retries}...")
        if init_db():
            return True
        logger.warning(f"вЏі init_db РїРѕРїС‹С‚РєР° {attempt}/{max_retries} РЅРµ СѓРґР°Р»Р°СЃСЊ, РїРѕРІС‚РѕСЂ С‡РµСЂРµР· 1СЃ...")
        if attempt < max_retries:
            with _db_lock:
                _nuke_db()
            time.sleep(1)

    logger.error("вќЊ init_db РїСЂРѕРІР°Р»РёР»Р°СЃСЊ РїРѕСЃР»Рµ РІСЃРµС… РїРѕРїС‹С‚РѕРє")
    return False

def add_history_record(user_id, operation_type, amount, description):
    """Р”РѕР±Р°РІР»СЏРµС‚ Р·Р°РїРёСЃСЊ РІ РёСЃС‚РѕСЂРёСЋ РѕРїРµСЂР°С†РёР№"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO user_history (user_id, operation_type, amount, description)
            VALUES (?, ?, ?, ?)
        ''', (user_id, operation_type, amount, description))

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"РћС€РёР±РєР° РґРѕР±Р°РІР»РµРЅРёСЏ РІ РёСЃС‚РѕСЂРёСЋ: {e}")
        return False

def add_win_history(user_id, user_name, gift_name, gift_image, gift_value, case_name):
    """Р”РѕР±Р°РІР»СЏРµС‚ Р·Р°РїРёСЃСЊ РІ РёСЃС‚РѕСЂРёСЋ РїРѕР±РµРґ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO win_history (user_id, user_name, gift_name, gift_image, gift_value, case_name)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, user_name, gift_name, gift_image, gift_value, case_name))

        cursor.execute('''
            DELETE FROM win_history
            WHERE id NOT IN (
                SELECT id FROM win_history
                ORDER BY created_at DESC
                LIMIT 50
            )
        ''')

        conn.commit()
        conn.close()
        logger.info(f"рџ“ќ Р”РѕР±Р°РІР»РµРЅР° Р·Р°РїРёСЃСЊ РІ РёСЃС‚РѕСЂРёСЋ РїРѕР±РµРґ: {user_name} РІС‹РёРіСЂР°Р» {gift_name}")
        return True
    except Exception as e:
        logger.error(f"РћС€РёР±РєР° РґРѕР±Р°РІР»РµРЅРёСЏ РІ РёСЃС‚РѕСЂРёСЋ РїРѕР±РµРґ: {e}")
        return False

def add_case_open_history(user_id, case_id, case_name, gift_id, gift_name, gift_image, gift_value, cost=0, cost_type='stars'):
    """Р”РѕР±Р°РІР»СЏРµС‚ Р·Р°РїРёСЃСЊ РІ РёСЃС‚РѕСЂРёСЋ РѕС‚РєСЂС‹С‚РёР№ РєРµР№СЃРѕРІ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO case_open_history (user_id, case_id, case_name, gift_id, gift_name, gift_image, gift_value, cost, cost_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, case_id, case_name, gift_id, gift_name, gift_image, gift_value, cost, cost_type))

        cursor.execute('''
            DELETE FROM case_open_history
            WHERE id NOT IN (
                SELECT id FROM case_open_history
                ORDER BY created_at DESC
                LIMIT 100
            )
        ''')

        conn.commit()
        conn.close()
        logger.info(f"рџ“ќ Р”РѕР±Р°РІР»РµРЅР° Р·Р°РїРёСЃСЊ РІ РёСЃС‚РѕСЂРёСЋ РѕС‚РєСЂС‹С‚РёР№: {user_id} РѕС‚РєСЂС‹Р» {case_name}")
        return True
    except Exception as e:
        logger.error(f"РћС€РёР±РєР° РґРѕР±Р°РІР»РµРЅРёСЏ РІ РёСЃС‚РѕСЂРёСЋ РѕС‚РєСЂС‹С‚РёР№: {e}")
        return False

def add_experience(user_id, exp_amount, reason=""):
    """Р”РѕР±Р°РІР»СЏРµС‚ РѕРїС‹С‚ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ Рё РїСЂРѕРІРµСЂСЏРµС‚ РїРѕРІС‹С€РµРЅРёРµ СѓСЂРѕРІРЅСЏ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT experience, current_level FROM users WHERE id = ?', (user_id,))
        result = cursor.fetchone()

        if not result:
            conn.close()
            return {'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'}

        current_exp, current_level = result
        new_exp = current_exp + exp_amount

        new_level = current_level
        level_up_rewards = []
        level_up_info = None

        # РџСЂРѕРІРµСЂСЏРµРј РїРѕРІС‹С€РµРЅРёРµ СѓСЂРѕРІРЅСЏ
        while new_level < len(LEVEL_SYSTEM):
            next_level_info = LEVEL_SYSTEM[new_level]
            if new_exp >= next_level_info["exp_required"]:
                new_level += 1

                # РќР°РіСЂР°РґР° СЂР°РєРµС‚РѕР№
                reward_rocket = next_level_info.get("reward_rocket")
                if reward_rocket and new_level > 1:
                    try:
                        cursor.execute('''INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source)
                            VALUES (?, 'rocket', ?, 'level_reward')''', (user_id, reward_rocket))
                        rocket_display = ROCKET_NAMES.get(reward_rocket, reward_rocket)
                        level_up_rewards.append(f"рџљЂ {rocket_display}")
                    except Exception as re:
                        logger.warning(f"Rocket reward error: {re}")

                # РќР°РіСЂР°РґР° С„РѕРЅРѕРј
                reward_bg = next_level_info.get("reward_bg")
                if reward_bg:
                    try:
                        cursor.execute('''INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source)
                            VALUES (?, 'background', ?, 'level_reward')''', (user_id, reward_bg))
                        bg_display = BG_NAMES.get(reward_bg, reward_bg)
                        level_up_rewards.append(f"рџЋЁ {bg_display}")
                    except Exception as ce:
                        logger.warning(f"Background reward error: {ce}")

                # Р—Р°РїРёСЃС‹РІР°РµРј РІ РёСЃС‚РѕСЂРёСЋ
                cursor.execute('''
                    INSERT INTO level_history (user_id, old_level, new_level, experience_gained, reason)
                    VALUES (?, ?, ?, ?, ?)
                ''', (user_id, new_level-1, new_level, exp_amount, reason))

                level_up_info = {
                    'old_level': new_level-1,
                    'new_level': new_level,
                    'reward_rocket': reward_rocket,
                    'reward_bg': reward_bg,
                    'rewards_text': ', '.join(level_up_rewards)
                }

                # РђРІС‚Рѕ-СѓРІРµРґРѕРјР»РµРЅРёРµ Рѕ РїРѕРІС‹С€РµРЅРёРё СѓСЂРѕРІРЅСЏ
                try:
                    reward_desc = ''
                    if reward_rocket and new_level > 1:
                        reward_desc += f'рџљЂ {ROCKET_NAMES.get(reward_rocket, reward_rocket)}'
                    if reward_bg:
                        if reward_desc:
                            reward_desc += ' '
                        reward_desc += f'рџЋЁ {BG_NAMES.get(reward_bg, reward_bg)}'
                    cursor.execute('''INSERT INTO admin_notifications 
                        (title, message, image_url, notif_type, target_user_id, reward_type, reward_data)
                        VALUES (?, ?, ?, 'level_up', ?, ?, ?)''',
                        (f'рџЋ‰ РЈСЂРѕРІРµРЅСЊ {new_level}!',
                         f'РџРѕР·РґСЂР°РІР»СЏРµРј! Р’С‹ РґРѕСЃС‚РёРіР»Рё СѓСЂРѕРІРЅСЏ {new_level}!{" РќР°РіСЂР°РґР°: " + reward_desc.strip() if reward_desc.strip() else ""}',
                         '', user_id,
                         'rocket' if reward_rocket else ('background' if reward_bg else None),
                         reward_rocket if reward_rocket else (reward_bg if reward_bg else None)))
                except Exception as ne:
                    logger.warning(f"Level notif error: {ne}")

                # Send Telegram bot message about level up
                try:
                    import threading
                    def send_level_up_msg():
                        try:
                            tg_send(user_id, 
                                f"рџЋ‰ <b>РЈСЂРѕРІРµРЅСЊ РїРѕРІС‹С€РµРЅ!</b>\n\n"
                                f"Р’С‹ РґРѕСЃС‚РёРіР»Рё <b>{new_level} СѓСЂРѕРІРЅСЏ</b>!\n"
                                f"{'РќР°РіСЂР°РґР°: ' + reward_desc.strip() if reward_desc.strip() else ''}\n\n"
                                f"рџ“± Р—Р°Р±РµСЂРёС‚Рµ РЅР°РіСЂР°РґСѓ РІ РїСЂРѕС„РёР»Рµ!",
                                parse_mode='HTML',
                                reply_markup={'inline_keyboard': [[
                                    {'text': 'рџЋЃ РћС‚РєСЂС‹С‚СЊ РїСЂРѕС„РёР»СЊ', 'web_app': {'url': f'{WEBSITE_URL}/inventory'}}
                                ]]}
                            )
                        except Exception as tge:
                            logger.warning(f"Level up TG msg error: {tge}")
                    threading.Thread(target=send_level_up_msg, daemon=True).start()
                except Exception as te:
                    logger.warning(f"Level up thread error: {te}")

                logger.info(f"рџЋ‰ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РґРѕСЃС‚РёРі СѓСЂРѕРІРЅСЏ {new_level}! РќР°РіСЂР°РґС‹: {', '.join(level_up_rewards)}")
            else:
                break

        # РћР±РЅРѕРІР»СЏРµРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
        cursor.execute('UPDATE users SET experience = ?, current_level = ? WHERE id = ?',
                     (new_exp, new_level, user_id))

        conn.commit()
        conn.close()

        return {
            'success': True,
            'old_level': current_level,
            'new_level': new_level,
            'exp_gained': exp_amount,
            'total_exp': new_exp,
            'level_up_info': level_up_info
        }

    except Exception as e:
        logger.error(f"РћС€РёР±РєР° РґРѕР±Р°РІР»РµРЅРёСЏ РѕРїС‹С‚Р°: {e}")
        return {'success': False, 'error': str(e)}


def _grant_level_rewards(user_id, new_level):
    """Р’С‹РґР°С‘С‚ Р±РѕРЅСѓСЃС‹ Р·Р° РґРѕСЃС‚РёР¶РµРЅРёРµ СѓСЂРѕРІРЅСЏ РёР· level_rewards"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, reward_type, reward_data, description FROM level_rewards WHERE level = ? AND is_active = 1', (new_level,))
        rewards = cursor.fetchall()
        for rw in rewards:
            rw_id, rw_type, rw_data_str, rw_desc = rw
            rw_data = json.loads(rw_data_str) if rw_data_str else {}
            # РЎРѕР·РґР°С‘Рј Р±РѕРЅСѓСЃ РґР»СЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
            cursor.execute('''INSERT INTO user_bonuses (user_id, bonus_type, bonus_data, source, source_id)
                VALUES (?, ?, ?, 'level_reward', ?)''',
                (user_id, rw_type, rw_data_str, rw_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Level rewards error: {e}")

def get_user_level_info(user_id):
    """РџРѕР»СѓС‡Р°РµС‚ РёРЅС„РѕСЂРјР°С†РёСЋ РѕР± СѓСЂРѕРІРЅРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT experience, current_level FROM users WHERE id = ?', (user_id,))
        result = cursor.fetchone()

        if not result:
            conn.close()
            return None

        experience, current_level = result

        current_level_info = next((level for level in LEVEL_SYSTEM if level["level"] == current_level), None)
        next_level_info = next((level for level in LEVEL_SYSTEM if level["level"] == current_level + 1), None)

        conn.close()

        if current_level_info and next_level_info:
            exp_to_next_level = next_level_info["exp_required"] - experience
            progress_percentage = ((experience - current_level_info["exp_required"]) /
                                (next_level_info["exp_required"] - current_level_info["exp_required"])) * 100
        else:
            exp_to_next_level = 0
            progress_percentage = 100

        return {
            'current_level': current_level,
            'experience': experience,
            'exp_to_next_level': max(0, exp_to_next_level),
            'progress_percentage': min(max(progress_percentage, 0), 100),
            'current_level_info': current_level_info,
            'next_level_info': next_level_info
        }

    except Exception as e:
        logger.error(f"РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёРЅС„РѕСЂРјР°С†РёРё РѕР± СѓСЂРѕРІРЅРµ: {e}")
        return None

def update_case_limit(case_id):
    """РћР±РЅРѕРІР»СЏРµС‚ Р»РёРјРёС‚ РєРµР№СЃР° (СѓРјРµРЅСЊС€Р°РµС‚ РЅР° 1)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cases = load_cases()
        case = next((c for c in cases if c['id'] == case_id), None)

        if not case:
            conn.close()
            return None

        if not case.get('limited'):
            conn.close()
            return None

        cursor.execute('SELECT current_amount FROM case_limits WHERE case_id = ?', (case_id,))
        result = cursor.fetchone()

        if not result:
            try:
                max_amount = int(case.get('amount', 0) or 0)
            except Exception:
                max_amount = 0
            if max_amount > 0:
                current_amount = max_amount - 1
                cursor.execute('INSERT INTO case_limits (case_id, current_amount) VALUES (?, ?)',
                             (case_id, current_amount))
                conn.commit()
                conn.close()
                return current_amount
            else:
                conn.close()
                return 0
        else:
            try:
                current_amount = int(result[0] or 0)
            except Exception:
                current_amount = 0
            if current_amount > 0:
                new_amount = current_amount - 1
                cursor.execute('UPDATE case_limits SET current_amount = ? WHERE case_id = ?', (new_amount, case_id))
                conn.commit()
                logger.info(f"рџ“Љ Р›РёРјРёС‚ РєРµР№СЃР° {case_id} СѓРјРµРЅСЊС€РµРЅ: {current_amount} -> {new_amount}")
                conn.close()
                return new_amount
            else:
                conn.close()
                return 0

    except Exception as e:
        logger.error(f"РћС€РёР±РєР° РѕР±РЅРѕРІР»РµРЅРёСЏ Р»РёРјРёС‚Р° РєРµР№СЃР°: {e}")
        return None

def get_case_limit(case_id):
    """РџРѕР»СѓС‡Р°РµС‚ С‚РµРєСѓС‰РёР№ Р»РёРјРёС‚ РєРµР№СЃР°"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT current_amount FROM case_limits WHERE case_id = ?', (case_id,))
        result = cursor.fetchone()

        if result:
            try:
                current_amount = int(result[0] or 0)
            except Exception:
                current_amount = 0
            conn.close()
            logger.info(f"рџ“Љ РџРѕР»СѓС‡РµРЅ Р»РёРјРёС‚ РєРµР№СЃР° {case_id}: {current_amount}")
            return current_amount
        else:
            cases = load_cases()
            case = next((c for c in cases if c['id'] == case_id), None)
            if case and case.get('limited'):
                try:
                    max_amount = int(case.get('amount', 0) or 0)
                except Exception:
                    max_amount = 0
                cursor.execute('INSERT INTO case_limits (case_id, current_amount) VALUES (?, ?)',
                             (case_id, max_amount))
                conn.commit()
                conn.close()
                logger.info(f"рџ“Љ РЎРѕР·РґР°РЅ Р»РёРјРёС‚ РґР»СЏ РєРµР№СЃР° {case_id}: {max_amount}")
                return max_amount
            else:
                conn.close()
                return None

    except Exception as e:
        logger.error(f"РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ Р»РёРјРёС‚Р° РєРµР№СЃР°: {e}")
        return None

def generate_referral_code():
    """Р“РµРЅРµСЂРёСЂСѓРµС‚ СѓРЅРёРєР°Р»СЊРЅС‹Р№ СЂРµС„РµСЂР°Р»СЊРЅС‹Р№ РєРѕРґ"""
    characters = string.ascii_uppercase + string.digits
    while True:
        code = ''.join(random.choice(characters) for _ in range(8))

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM users WHERE referral_code = ?', (code,))
        existing = cursor.fetchone()
        conn.close()

        if not existing:
            return code

def _parse_datetime_flexible(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        pass
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None

def _parse_case_cooldown_hours(case_data):
    raw = case_data.get('time')
    if isinstance(raw, (int, float)):
        try:
            return max(1.0, float(raw))
        except Exception:
            return 24.0
    text = str(raw or '').strip().upper()
    if not text:
        return 24.0
    m = re.match(r'^(\d+(?:\.\d+)?)\s*([HD]?)$', text)
    if not m:
        return 24.0
    value = float(m.group(1))
    unit = (m.group(2) or 'H').upper()
    if unit == 'D':
        value *= 24.0
    return max(1.0, value)

def _get_free_case_remaining_seconds(cursor, user_id, case_data):
    if not case_data or not case_data.get('free'):
        return 0
    case_id = case_data.get('id')
    cursor.execute('''
        SELECT created_at FROM case_open_history
        WHERE user_id = ? AND case_id = ?
        ORDER BY created_at DESC LIMIT 1
    ''', (user_id, case_id))
    row = cursor.fetchone()
    if not row:
        return 0
    last_open_dt = _parse_datetime_flexible(row[0])
    if not last_open_dt:
        return 0
    cooldown_seconds = int(_parse_case_cooldown_hours(case_data) * 3600)
    elapsed = (datetime.now() - last_open_dt).total_seconds()
    return max(0, cooldown_seconds - int(elapsed))

def _find_embedded_case_promo(case_id, promo_code):
    try:
        cid = int(case_id)
    except Exception:
        return None, None
    code = str(promo_code or '').strip().upper()
    if not code:
        return None, None
    case_list = load_cases() or []
    case_data = next((c for c in case_list if int(c.get('id', -1)) == cid), None)
    if not case_data or not case_data.get('promo'):
        return None, None
    for item in (case_data.get('promo_codes') or []):
        if str(item.get('code', '')).strip().upper() == code:
            return case_data, item
    return case_data, None

def process_referral(referred_user_id, referral_code):
    """РћР±СЂР°Р±Р°С‚С‹РІР°РµС‚ СЂРµС„РµСЂР°Р»СЊРЅСѓСЋ СЃСЃС‹Р»РєСѓ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT id FROM users WHERE referral_code = ?', (referral_code,))
        referrer = cursor.fetchone()

        if referrer:
            referrer_id = referrer[0]

            if referrer_id == referred_user_id:
                logger.warning(f"вљ пёЏ РџРѕРїС‹С‚РєР° СЃР°РјРѕРїСЂРёРіР»Р°С€РµРЅРёСЏ: {referred_user_id}")
                return False

            cursor.execute('SELECT id FROM referrals WHERE referred_id = ?', (referred_user_id,))
            existing = cursor.fetchone()

            if not existing:
                cursor.execute('''
                    INSERT INTO referrals (referrer_id, referred_id)
                    VALUES (?, ?)
                ''', (referrer_id, referred_user_id))

                cursor.execute('UPDATE users SET referral_count = referral_count + 1 WHERE id = ?', (referrer_id,))

                cursor.execute('UPDATE users SET balance_tickets = balance_tickets + 1, total_earned_tickets = total_earned_tickets + 1 WHERE id = ?', (referrer_id,))

                add_experience(referrer_id, 50, "РџСЂРёРіР»Р°С€РµРЅРёРµ РґСЂСѓРіР°")

                cursor.execute('SELECT first_name FROM users WHERE id = ?', (referred_user_id,))
                referred_user = cursor.fetchone()
                referred_name = referred_user[0] if referred_user else 'РќРѕРІС‹Р№ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ'

                add_history_record(referrer_id, 'referral_reward', 1, f'РџСЂРёРіР»Р°С€РµРЅ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ: {referred_name}')

                cursor.execute('''
                    INSERT INTO referral_rewards (referrer_id, reward_type, reward_amount, description)
                    VALUES (?, ?, ?, ?)
                ''', (referrer_id, 'tickets', 1, 'Р—Р° РїСЂРёРіР»Р°С€РµРЅРёРµ РґСЂСѓРіР°'))

                cursor.execute('UPDATE users SET referred_by = ? WHERE id = ?', (referrer_id, referred_user_id))

                conn.commit()
                conn.close()

                logger.info(f"рџЋ« РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {referrer_id} РїРѕР»СѓС‡РёР» 1 Р±РёР»РµС‚ Р·Р° РїСЂРёРіР»Р°С€РµРЅРёРµ {referred_user_id}")
                return True

        conn.close()
        return False

    except Exception as e:
        logger.error(f"РћС€РёР±РєР° РѕР±СЂР°Р±РѕС‚РєРё СЂРµС„РµСЂР°Р»Р°: {e}")
        return False

# ============================================================
# AI RTP вЂ” Per-Player Crash Probability System
# ============================================================
# Each player carries an individual "luck profile". The house
# maintains a target RTP (Return To Player) of ~85%. If a player
# is significantly ahead (net positive), the system increases the
# probability of crashing before that player can profit further.
# Conversely, if a player has big losses, the system becomes more
# lenient (higher multipliers allowed).
#
# The system works in two layers:
#   1) Pre-round: adjust target_multiplier based on who has bet
#   2) Mid-round: dynamic crash trigger if high-profit players
#      would extract too much from a cashout
# ============================================================

TARGET_RTP = 0.85  # 85% RTP target
RTP_HARD_FLOOR = 0.70  # Never let a player's effective RTP exceed this floor 
RTP_CHECK_INTERVAL = 5  # Re-evaluate every N games
LARGE_BET_THRESHOLD = 100  # Stars threshold for "large bet" (1 TON)
WHALE_BET_THRESHOLD = 500  # Stars threshold for "whale bet" (5 TON)

def get_player_crash_stats(user_id, conn=None):
    """Get a player's crash game lifetime stats including net position"""
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    try:
        cursor = conn.cursor()
        # Total wagered in crash
        cursor.execute('''SELECT COALESCE(SUM(bet_amount), 0) 
                         FROM ultimate_crash_bets WHERE user_id = ?''', (user_id,))
        total_wagered = cursor.fetchone()[0]
        
        # Total won (cashed out)
        cursor.execute('''SELECT COALESCE(SUM(win_amount), 0) 
                         FROM ultimate_crash_bets 
                         WHERE user_id = ? AND status = 'cashed_out' ''', (user_id,))
        total_won = cursor.fetchone()[0]
        
        # Recent performance (last 20 games)
        cursor.execute('''SELECT bet_amount, COALESCE(win_amount, 0), status 
                         FROM ultimate_crash_bets 
                         WHERE user_id = ? 
                         ORDER BY id DESC LIMIT 20''', (user_id,))
        recent = cursor.fetchall()
        recent_wagered = sum(r[0] for r in recent) if recent else 0
        recent_won = sum(r[1] for r in recent) if recent else 0
        
        # Player's overall balance stats (deposits vs withdrawals)
        cursor.execute('SELECT COALESCE(balance_stars, 0), COALESCE(total_earned_stars, 0) FROM users WHERE id = ?', (user_id,))
        user_row = cursor.fetchone()
        current_balance = user_row[0] if user_row else 0
        total_earned = user_row[1] if user_row else 0
        
        if close_conn:
            conn.close()
        
        net_profit = total_won - total_wagered
        player_rtp = (total_won / total_wagered) if total_wagered > 0 else 1.0
        recent_rtp = (recent_won / recent_wagered) if recent_wagered > 0 else 1.0
        
        # Calculate loss severity: how much they've lost overall as percentage
        loss_severity = 0.0
        if total_wagered > 0:
            loss_severity = max(0, (total_wagered - total_won) / total_wagered)  # 0 = no loss, 1 = lost everything
        
        return {
            'total_wagered': total_wagered,
            'total_won': total_won,
            'net_profit': net_profit,
            'player_rtp': player_rtp,
            'recent_rtp': recent_rtp,
            'games_played': len(recent),
            'current_balance': current_balance,
            'loss_severity': loss_severity,
            'is_losing': net_profit < 0
        }
    except Exception as e:
        if close_conn:
            try: conn.close()
            except: pass
        return {'total_wagered': 0, 'total_won': 0, 'net_profit': 0, 
                'player_rtp': 1.0, 'recent_rtp': 1.0, 'games_played': 0}


def ai_adjust_target_multiplier(base_target, game_id, conn=None):
    """Adjust the pre-generated crash target based on active bettors.
    
    KEY LOGIC:
    - Large bets (>1-5 TON) в†’ drastically reduce target (crash early)
    - Losing players with small bets в†’ boost target significantly (tease with high multipliers)
    - Winning players в†’ reduce target
    """
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    try:
        cursor = conn.cursor()
        cursor.execute('''SELECT user_id, bet_amount FROM ultimate_crash_bets 
                         WHERE game_id = ? AND status = 'active' ''', (game_id,))
        bets = cursor.fetchall()
        
        if not bets:
            if close_conn: conn.close()
            return base_target
        
        # Analyze bet composition
        total_bet_value = sum(b[1] for b in bets)
        large_bet_value = 0
        losing_small_bet_value = 0
        winning_bet_value = 0
        has_whale_bet = False
        
        for user_id, bet_amount in bets:
            if user_id < 0:  # Skip bots
                continue
            
            # Check for whale bets
            if bet_amount >= WHALE_BET_THRESHOLD:
                has_whale_bet = True
                large_bet_value += bet_amount
            elif bet_amount >= LARGE_BET_THRESHOLD:
                large_bet_value += bet_amount
            else:
                # Small bet - check if player is losing
                stats = get_player_crash_stats(user_id, conn)
                if stats.get('is_losing', False) and stats.get('loss_severity', 0) > 0.2:
                    losing_small_bet_value += bet_amount
                elif stats.get('player_rtp', 1.0) > 1.0:
                    winning_bet_value += bet_amount
        
        if close_conn: conn.close()
        
        # === WHALE BET: Crash early! ===
        if has_whale_bet or large_bet_value > total_bet_value * 0.5:
            # Drastically reduce target for rounds with large bets
            reduction = 0.7 if has_whale_bet else 0.5
            adjusted = max(1.01, base_target * (1 - reduction))
            logger.info(f"рџђ‹ AI: Large bet detected! Reducing target {base_target:.2f}x в†’ {adjusted:.2f}x")
            return round(adjusted, 2)
        
        # === TEASE LOSING PLAYERS: Allow massive multipliers ===
        if losing_small_bet_value > total_bet_value * 0.4 and large_bet_value == 0:
            # Mostly losing players with small bets - TEASE them with high multipliers
            loss_ratio = losing_small_bet_value / max(total_bet_value, 1)
            
            # Generate provocative high multipliers
            if random.random() < 0.25:  # 25% chance of massive boost
                boost = 3.0 + random.random() * 10.0  # 3x to 13x multiplier boost!
                adjusted = min(150.0, base_target * boost)
                logger.info(f"рџЋ° AI: Teasing losers! Boosting target {base_target:.2f}x в†’ {adjusted:.2f}x")
                return round(adjusted, 2)
            else:
                # Still boost, but more moderately
                boost = 1.5 + loss_ratio * 2.0
                adjusted = base_target * boost
                logger.debug(f"рџЋ° AI: Moderate loser tease {base_target:.2f}x в†’ {adjusted:.2f}x")
                return round(min(100.0, adjusted), 2)
        
        # === WINNING PLAYERS: Reduce target ===
        if winning_bet_value > total_bet_value * 0.3:
            reduction = min(0.5, 0.2 + (winning_bet_value / total_bet_value) * 0.3)
            adjusted = max(1.01, base_target * (1 - reduction))
            logger.debug(f"рџЋЇ AI: Winners detected, reducing target {base_target:.2f}x в†’ {adjusted:.2f}x")
            return round(adjusted, 2)
        
        return base_target
        
    except Exception as e:
        logger.error(f"AI RTP adjust error: {e}")
        if close_conn:
            try: conn.close()
            except: pass
        return base_target


def ai_should_force_crash(game_id, current_mult, conn=None):
    """Check if the game should crash NOW based on player bets and their status.
    
    KEY LOGIC:
    - If player bets LARGE (>1-5 TON), crash early to take their money
    - If player is losing overall, let them win small bets (provoke to bet more)
    - If player is winning overall, crash more aggressively
    """
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    try:
        cursor = conn.cursor()
        cursor.execute('''SELECT user_id, bet_amount FROM ultimate_crash_bets 
                         WHERE game_id = ? AND status = 'active' ''', (game_id,))
        active_bets = cursor.fetchall()
        
        if not active_bets:
            if close_conn: conn.close()
            return False
        
        for user_id, bet_amount in active_bets:
            if user_id < 0:
                continue
            
            potential_win = bet_amount * current_mult
            stats = get_player_crash_stats(user_id, conn)
            
            # === KEY: Large bet detection ===
            # If bet is large (>1 TON = 100 stars), crash early regardless
            if bet_amount >= LARGE_BET_THRESHOLD:
                # Crash probability increases with bet size
                if bet_amount >= WHALE_BET_THRESHOLD:  # 5+ TON
                    # Very high chance to crash early on whale bets
                    crash_prob = 0.6 + (current_mult - 1.0) * 0.15
                    if random.random() < crash_prob:
                        if close_conn: conn.close()
                        logger.info(f"рџђ‹ Whale crash: {bet_amount} stars bet from user {user_id}, mult={current_mult:.2f}x")
                        return True
                else:  # 1-5 TON range
                    # Medium chance to crash
                    crash_prob = 0.35 + (current_mult - 1.0) * 0.12
                    if bet_amount >= 200:  # 2+ TON
                        crash_prob += 0.15
                    if random.random() < crash_prob:
                        if close_conn: conn.close()
                        logger.info(f"рџ’° Large bet crash: {bet_amount} stars from user {user_id}, mult={current_mult:.2f}x")
                        return True
            
            # === Secondary: Player profitability check ===
            # If player is winning overall, crash more often
            if stats['total_wagered'] >= 200:  # Has some history
                if stats['player_rtp'] > 1.1:  # Player is profitable
                    excess_rtp = stats['player_rtp'] - TARGET_RTP
                    crash_prob = min(0.40, excess_rtp * 0.35)
                    
                    if potential_win > 500:
                        crash_prob += 0.08
                    if potential_win > 2000:
                        crash_prob += 0.12
                    
                    if random.random() < crash_prob:
                        if close_conn: conn.close()
                        logger.info(f"рџЋЇ RTP crash: user {user_id} RTP={stats['player_rtp']:.2f}, prob={crash_prob:.2f}")
                        return True
                        
                # If player is losing AND betting small, let them win occasionally
                # This is handled by NOT crashing here
                elif stats['is_losing'] and bet_amount < LARGE_BET_THRESHOLD:
                    # Skip force crash for small bets from losing players
                    continue
        
        if close_conn: conn.close()
        return False
        
    except Exception as e:
        logger.error(f"AI force crash check error: {e}")
        if close_conn:
            try: conn.close()
            except: pass
        return False


def generate_extreme_crash_multiplier():
    """Р“РµРЅРµСЂР°С†РёСЏ РјРЅРѕР¶РёС‚РµР»СЏ РґР»СЏ Ultimate Crash СЃ СѓС‡С‘С‚РѕРј Р±Р°Р»Р°РЅСЃР° СЃР°Р№С‚Р°.
    
    Base multiplier generation вЂ” will be adjusted by ai_adjust_target_multiplier
    once bets are placed.
    """
    site_balance = _get_site_profit_balance()
    r = random.random()
    
    # Standard distribution based on site balance
    if site_balance < -5000:
        # Site is losing big вЂ” mostly low multipliers
        if r < 0.65:
            return round(1.0 + random.random() * 0.8, 2)  # 1.0-1.8
        elif r < 0.85:
            return round(1.8 + random.random() * 1.2, 2)  # 1.8-3.0
        elif r < 0.95:
            return round(3.0 + random.random() * 2.0, 2)  # 3.0-5.0
        else:
            return round(5.0 + random.random() * 3.0, 2)   # 5.0-8.0
    elif site_balance < -1000:
        # Site is losing moderately вЂ” slightly lower multipliers
        if r < 0.55:
            return round(1.0 + random.random() * 1.0, 2)
        elif r < 0.75:
            return round(2.0 + random.random() * 1.5, 2)
        elif r < 0.90:
            return round(3.5 + random.random() * 1.5, 2)
        elif r < 0.97:
            return round(5.0 + random.random() * 3.0, 2)
        else:
            return round(8.0 + random.random() * 5.0, 2)
    elif site_balance > 5000:
        # Site is profiting well вЂ” give players better odds (but cap at 50% return)
        payout_factor = min(0.5, site_balance / 20000)  # Max 50% payout factor
        if r < 0.40:
            return round(1.0 + random.random() * 1.5, 2)
        elif r < 0.60:
            return round(2.5 + random.random() * 2.0, 2)
        elif r < 0.78:
            return round(4.5 + random.random() * 3.5, 2)
        elif r < 0.90:
            return round(8.0 + random.random() * 7.0, 2)
        elif r < 0.97:
            return round(15.0 + random.random() * 15.0, 2)
        else:
            return round(30.0 + random.random() * 20.0, 2)
    else:
        # Normal/balanced вЂ” standard distribution
        if r < 0.50:
            return round(1.0 + random.random() * 1.0, 2)
        elif r < 0.70:
            return round(2.0 + random.random() * 1.5, 2)
        elif r < 0.85:
            return round(3.5 + random.random() * 1.5, 2)
        elif r < 0.94:
            return round(5.0 + random.random() * 3.0, 2)
        elif r < 0.98:
            return round(8.0 + random.random() * 7.0, 2)
        else:
            return round(15.0 + random.random() * 35.0, 2)


def _get_site_profit_balance():
    """Calculate the site's profit/loss from all deposits, withdrawals, and crash game results"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Total deposits (money in)
        cursor.execute('SELECT COALESCE(SUM(amount), 0) FROM deposits WHERE status = "completed"')
        total_deposits = cursor.fetchone()[0]
        
        # Total star payments (money in)
        cursor.execute('SELECT COALESCE(SUM(amount), 0) FROM stars_payments')
        total_star_payments = cursor.fetchone()[0]
        
        # Total gift deposits (money in)
        cursor.execute('SELECT COALESCE(SUM(gift_value), 0) FROM gift_deposits WHERE status = "confirmed"')
        total_gift_deposits = cursor.fetchone()[0]
        
        # Total withdrawals (money out)
        cursor.execute('SELECT COALESCE(SUM(gift_value), 0) FROM withdrawals WHERE status IN ("approved", "completed", "sent")')
        total_withdrawals = cursor.fetchone()[0]
        
        # Crash game: total bets (money in) vs total wins (money out)
        cursor.execute('SELECT COALESCE(SUM(bet_amount), 0) FROM ultimate_crash_bets')
        total_crash_bets = cursor.fetchone()[0]
        
        cursor.execute('SELECT COALESCE(SUM(win_amount), 0) FROM ultimate_crash_bets WHERE status = "cashed_out"')
        total_crash_wins = cursor.fetchone()[0]
        
        conn.close()
        
        # Site profit = external money in - external money out - crash net payout
        # External deposits are real money in; withdrawals are real money out
        # Crash: bets stay on platform, wins leave platform balance
        # Net crash cost to site = wins - bets (positive means site lost money)
        money_in = total_deposits + total_star_payments + total_gift_deposits
        money_out = total_withdrawals
        crash_net = total_crash_wins - total_crash_bets  # positive = site lost money
        
        return money_in - money_out - crash_net
    except Exception as e:
        logger.error(f"Error calculating site balance: {e}")
        return 0  # Neutral if can't calculate

def start_crash_loop():
    def loop():
        while not _db_ready:
            time.sleep(1)
        while True:
            if not _db_ready:
                time.sleep(2)
                continue
            try:
                conn = get_db_connection()
                cur = conn.cursor()

                cur.execute("SELECT id,status,current_multiplier FROM crash_games ORDER BY id DESC LIMIT 1")
                game = cur.fetchone()

                if not game or game[1] == "crashed":
                    cur.execute("INSERT INTO crash_games(status,current_multiplier) VALUES('flying',1.0)")
                    conn.commit()
                else:
                    gid,status,mult = game
                    if status == "flying":
                        mult = float(mult) + random.uniform(0.05,0.25)

                        if random.random() < 0.03:
                            cur.execute("UPDATE crash_games SET status='crashed' WHERE id=?", (gid,))
                        else:
                            cur.execute("UPDATE crash_games SET current_multiplier=? WHERE id=?", (round(mult,2),gid))

                        conn.commit()

                conn.close()
            except Exception as e:
                logger.debug(f"crash_loop err: {e}")
                time.sleep(3)
                continue
            time.sleep(0.5)

    threading.Thread(target=loop, daemon=True).start()


def start_ultimate_crash_loop():

    """Р—Р°РїСѓСЃРєР°РµС‚ РїСЂРѕСЃС‚РѕР№ РёРіСЂРѕРІРѕР№ С†РёРєР»"""
    def game_loop():
        global _crash_phase_transitioning
        while not _db_ready:
            time.sleep(1)
        logger.info("рџљЂ Р—Р°РїСѓС‰РµРЅ РёРіСЂРѕРІРѕР№ С†РёРєР» Ultimate Crash")

        # Persistent connection for game loop вЂ” re-created only on error
        loop_conn = None
        tick_counter = 0

        def get_loop_conn():
            nonlocal loop_conn
            if loop_conn is None:
                loop_conn = get_db_connection()
            return loop_conn

        def reset_loop_conn():
            nonlocal loop_conn
            if loop_conn:
                try: loop_conn.close()
                except: pass
            loop_conn = None

        while True:
            if not _db_ready:
                time.sleep(2)
                continue
            try:
                conn = get_loop_conn()
                cursor = conn.cursor()
                tick_counter += 1

                # РС‰РµРј Р°РєС‚РёРІРЅСѓСЋ РёРіСЂСѓ
                cursor.execute('''
                    SELECT id, status, start_time, current_multiplier, target_multiplier
                    FROM ultimate_crash_games
                    WHERE status IN ('waiting', 'counting', 'flying')
                    ORDER BY id DESC LIMIT 1
                ''')

                game = cursor.fetchone()

                if game:
                    game_id, status, start_time, current_mult, target_mult = game

                    # РџСЂРµРѕР±СЂР°Р·СѓРµРј РІСЂРµРјСЏ
                    if isinstance(start_time, str):
                        try:
                            if '.' in start_time:
                                start_time = start_time.split('.')[0]
                            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                            start_timestamp = time.mktime(start_dt.timetuple())
                        except:
                            start_timestamp = time.time() - 30
                    else:
                        start_timestamp = time.time() - 30

                    elapsed = time.time() - start_timestamp
                    current_mult_float = float(current_mult) if current_mult else 1.0
                    target_mult_float = float(target_mult) if target_mult else 5.0

                    # РћР±СЂР°Р±РѕС‚РєР° СЂР°Р·РЅС‹С… С„Р°Р· (Р±РµР· waiting - СЃСЂР°Р·Сѓ counting)
                    if status == 'waiting':
                        # РЎСЂР°Р·Сѓ РїРµСЂРµС…РѕРґРёРј РІ counting
                        cursor.execute('UPDATE ultimate_crash_games SET status = "counting", start_time = CURRENT_TIMESTAMP WHERE id = ?', (game_id,))
                        update_crash_cache(game_id, 'counting', 1.0, target_mult_float, 5.0)
                    elif status == 'counting':
                        time_remaining = max(0, 5.0 - elapsed)
                        update_crash_cache(game_id, 'counting', 1.0, target_mult_float, time_remaining)
                        if elapsed >= 5:  # 5 СЃРµРєСѓРЅРґ РѕС‚СЃС‡С‘С‚Р° РґР»СЏ СЃС‚Р°РІРѕРє
                            # AI RTP: Adjust target multiplier based on who bet this round
                            try:
                                adjusted_target = ai_adjust_target_multiplier(target_mult_float, game_id, conn)
                                if adjusted_target != target_mult_float:
                                    target_mult_float = adjusted_target
                                    cursor.execute('UPDATE ultimate_crash_games SET target_multiplier = ? WHERE id = ?',
                                                 (adjusted_target, game_id))
                            except Exception as ai_e:
                                logger.error(f"AI RTP pre-round error: {ai_e}")
                            
                            # Lock phase transition to prevent late bets
                            with _crash_phase_lock:
                                _crash_phase_transitioning = True
                            # Update cache FIRST (so bet endpoints see 'flying' immediately)
                            update_crash_cache(game_id, 'flying', 1.0, target_mult_float, 15.0)
                            cursor.execute('UPDATE ultimate_crash_games SET status = "flying" WHERE id = ?', (game_id,))
                            conn.commit()
                            with _crash_phase_lock:
                                _crash_phase_transitioning = False
                    elif status == 'flying':
                        # Check for admin force crash
                        admin_ctrl = get_admin_crash_control()
                        if admin_ctrl.get('force_crash'):
                            set_admin_crash_control('force_crash', False)
                            cursor.execute('UPDATE ultimate_crash_games SET status = "crashed" WHERE id = ?', (game_id,))
                            cursor.execute('INSERT INTO ultimate_crash_history (game_id, final_multiplier, finished_at) VALUES (?, ?, CURRENT_TIMESTAMP)', (game_id, current_mult_float))
                            cursor.execute("UPDATE ultimate_crash_bets SET status = 'lost' WHERE game_id = ? AND status = 'active'", (game_id,))
                            cursor.execute('''
                                UPDATE users SET total_loss = total_loss + (
                                    SELECT COALESCE(SUM(bet_amount), 0) FROM ultimate_crash_bets 
                                    WHERE game_id = ? AND status = 'lost' AND user_id = users.id
                                ) WHERE id IN (SELECT user_id FROM ultimate_crash_bets WHERE game_id = ? AND status = 'lost')
                            ''', (game_id, game_id))
                            _crash_bots_on_crash(game_id)
                            update_crash_cache(game_id, 'crashed', current_mult_float, target_mult_float, 0)
                            logger.info(f"рџ’Ґ ADMIN FORCE CRASH РЅР° {current_mult_float:.2f}x")
                            conn.commit()
                            time.sleep(0.10)
                            continue
                        
                        # РЈРІРµР»РёС‡РёРІР°РµРј РјРЅРѕР¶РёС‚РµР»СЊ

                        if current_mult_float < target_mult_float:
                            # AI RTP check вЂ” force crash if profitable players would extract too much
                            if current_mult_float > 1.5 and tick_counter % 3 == 0:
                                if ai_should_force_crash(game_id, current_mult_float, conn):
                                    cursor.execute('UPDATE ultimate_crash_games SET status = "crashed" WHERE id = ?', (game_id,))
                                    cursor.execute('INSERT INTO ultimate_crash_history (game_id, final_multiplier, finished_at) VALUES (?, ?, CURRENT_TIMESTAMP)', (game_id, current_mult_float))
                                    cursor.execute("UPDATE ultimate_crash_bets SET status = 'lost' WHERE game_id = ? AND status = 'active'", (game_id,))
                                    cursor.execute('''
                                        UPDATE users SET total_loss = total_loss + (
                                            SELECT COALESCE(SUM(bet_amount), 0) FROM ultimate_crash_bets 
                                            WHERE game_id = ? AND status = 'lost' AND user_id = users.id
                                        ) WHERE id IN (SELECT user_id FROM ultimate_crash_bets WHERE game_id = ? AND status = 'lost')
                                    ''', (game_id, game_id))
                                    _crash_bots_on_crash(game_id)
                                    update_crash_cache(game_id, 'crashed', current_mult_float, target_mult_float, 0)
                                    logger.info(f"рџ’Ґ AI RTP CRASH РЅР° {current_mult_float:.2f}x")
                                    conn.commit()
                                    time.sleep(0.10)
                                    continue
                            
                            # Aggressive acceleration: fast after 1.1x
                            if current_mult_float < 1.1:
                                base_increment = 0.03
                            elif current_mult_float < 1.5:
                                base_increment = 0.06
                            elif current_mult_float < 2.0:
                                base_increment = 0.10
                            elif current_mult_float < 3.0:
                                base_increment = 0.16
                            elif current_mult_float < 5.0:
                                base_increment = 0.25
                            elif current_mult_float < 10.0:
                                base_increment = 0.40
                            else:
                                base_increment = 0.60

                            speed_boost = current_mult_float * 0.025
                            increment = round(max(base_increment, speed_boost), 2)
                            increment = min(increment, 2.0)

                            # РЎР»СѓС‡Р°Р№РЅС‹Р№ РєСЂР°С€
                            crash_chance = 0.01 * (current_mult_float / 10)
                            if random.random() < crash_chance:
                                cursor.execute('UPDATE ultimate_crash_games SET status = "crashed" WHERE id = ?', (game_id,))
                                # Save to history and process lost bets
                                cursor.execute('INSERT INTO ultimate_crash_history (game_id, final_multiplier, finished_at) VALUES (?, ?, CURRENT_TIMESTAMP)', (game_id, current_mult_float))
                                cursor.execute("UPDATE ultimate_crash_bets SET status = 'lost' WHERE game_id = ? AND status = 'active'", (game_id,))
                                # Track losses for users
                                cursor.execute('''
                                    UPDATE users SET total_loss = total_loss + (
                                        SELECT COALESCE(SUM(bet_amount), 0) FROM ultimate_crash_bets 
                                        WHERE game_id = ? AND status = 'lost' AND user_id = users.id
                                    ) WHERE id IN (SELECT user_id FROM ultimate_crash_bets WHERE game_id = ? AND status = 'lost')
                                ''', (game_id, game_id))
                                update_crash_cache(game_id, 'crashed', current_mult_float, target_mult_float, 0)
                                logger.info(f"рџ’Ґ РЎР»СѓС‡Р°Р№РЅС‹Р№ РєСЂР°С€ РЅР° {current_mult_float:.2f}x")
                            else:
                                new_multiplier = round(current_mult_float + increment, 2)
                                if new_multiplier >= target_mult_float:
                                    cursor.execute('UPDATE ultimate_crash_games SET status = "crashed", current_multiplier = ? WHERE id = ?',
                                                 (target_mult_float, game_id))
                                    cursor.execute('INSERT INTO ultimate_crash_history (game_id, final_multiplier, finished_at) VALUES (?, ?, CURRENT_TIMESTAMP)', (game_id, target_mult_float))
                                    cursor.execute("UPDATE ultimate_crash_bets SET status = 'lost' WHERE game_id = ? AND status = 'active'", (game_id,))
                                    # Track losses for users
                                    cursor.execute('''
                                        UPDATE users SET total_loss = total_loss + (
                                            SELECT COALESCE(SUM(bet_amount), 0) FROM ultimate_crash_bets 
                                            WHERE game_id = ? AND status = 'lost' AND user_id = users.id
                                        ) WHERE id IN (SELECT user_id FROM ultimate_crash_bets WHERE game_id = ? AND status = 'lost')
                                    ''', (game_id, game_id))
                                    update_crash_cache(game_id, 'crashed', target_mult_float, target_mult_float, 0)
                                    logger.info(f"рџ’Ґ Р”РѕСЃС‚РёРіРЅСѓС‚ С†РµР»РµРІРѕР№ РјРЅРѕР¶РёС‚РµР»СЊ {target_mult_float:.2f}x")
                                else:
                                    # Update cache immediately (clients see it fast)
                                    progress = new_multiplier / target_mult_float if target_mult_float > 0 else 0
                                    time_remaining = max(0.5, 15.0 * (1 - progress))
                                    update_crash_cache(game_id, 'flying', new_multiplier, target_mult_float, time_remaining)
                                    # Write to DB only every 5th tick to reduce I/O
                                    if tick_counter % 5 == 0:
                                        cursor.execute('UPDATE ultimate_crash_games SET current_multiplier = ? WHERE id = ?',
                                                     (new_multiplier, game_id))
                        else:
                            cursor.execute('UPDATE ultimate_crash_games SET status = "crashed" WHERE id = ?', (game_id,))
                            cursor.execute('INSERT INTO ultimate_crash_history (game_id, final_multiplier, finished_at) VALUES (?, ?, CURRENT_TIMESTAMP)', (game_id, current_mult_float))
                            cursor.execute("UPDATE ultimate_crash_bets SET status = 'lost' WHERE game_id = ? AND status = 'active'", (game_id,))
                            # Track losses for users
                            cursor.execute('''
                                UPDATE users SET total_loss = total_loss + (
                                    SELECT COALESCE(SUM(bet_amount), 0) FROM ultimate_crash_bets 
                                    WHERE game_id = ? AND status = 'lost' AND user_id = users.id
                                ) WHERE id IN (SELECT user_id FROM ultimate_crash_bets WHERE game_id = ? AND status = 'lost')
                            ''', (game_id, game_id))
                            update_crash_cache(game_id, 'crashed', current_mult_float, target_mult_float, 0)
                            logger.info(f"рџ’Ґ РРіСЂР° #{game_id} Р·Р°РІРµСЂС€РµРЅР° РЅР° {current_mult_float:.2f}x")

                    conn.commit()
                else:
                    # No active game - check if last game just crashed (give time to display)
                    cursor.execute('''
                        SELECT id, status, current_multiplier FROM ultimate_crash_games
                        ORDER BY id DESC LIMIT 1
                    ''')
                    last_game = cursor.fetchone()
                    if last_game and last_game[1] == 'crashed':
                        # Wait 3 seconds after crash so players see the result
                        time.sleep(3.0)

                    target_multiplier = generate_extreme_crash_multiplier()
                    
                    # Check for admin control of multiplier
                    admin_ctrl = get_admin_crash_control()
                    if admin_ctrl.get('next_multiplier'):
                        target_multiplier = float(admin_ctrl['next_multiplier'])
                        set_admin_crash_control('next_multiplier', None)
                        logger.info(f"рџЋ® ADMIN SET multiplier: {target_multiplier}x")
                    elif admin_ctrl.get('use_custom_range'):
                        min_m = admin_ctrl.get('multiplier_min', 1.0)
                        max_m = admin_ctrl.get('multiplier_max', 50.0)
                        target_multiplier = round(random.uniform(min_m, max_m), 2)
                        logger.info(f"рџЋ® ADMIN RANGE multiplier: {target_multiplier}x ({min_m}-{max_m})")
                    
                    cursor.execute('''
                        INSERT INTO ultimate_crash_games (status, target_multiplier, start_time)
                        VALUES ('waiting', ?, CURRENT_TIMESTAMP)
                    ''', (target_multiplier,))
                    conn.commit()
                    # Clean old user bets cache
                    _cleanup_user_bets_cache()
                    logger.info(f"рџ†• РќРѕРІР°СЏ Crash РёРіСЂР°, target: {target_multiplier}x")

                time.sleep(0.10)  # Tick interval вЂ” fast for smooth multiplier

            except Exception as e:
                err_msg = str(e)
                reset_loop_conn()
                # РџСЂРё РѕС€РёР±РєР°С… Р‘Р” - РїСЂРѕР±СѓРµРј РІРѕСЃСЃС‚Р°РЅРѕРІРёС‚СЊ
                if 'malformed' in err_msg or 'disk' in err_msg or 'lock' in err_msg:
                    try:
                        # РЈРґР°Р»СЏРµРј РІСЂРµРјРµРЅРЅС‹Рµ С„Р°Р№Р»С‹ WAL
                        for ext in ['-wal', '-shm', '-journal']:
                            p = DB_PATH + ext
                            if os.path.exists(p):
                                os.remove(p)
                    except:
                        pass
                
                if not hasattr(game_loop, '_last_err') or game_loop._last_err != err_msg:
                    logger.error(f"вќЊ РћС€РёР±РєР° РІ РёРіСЂРѕРІРѕРј С†РёРєР»Рµ: {e}")
                    game_loop._last_err = err_msg
                    game_loop._err_count = 1
                else:
                    game_loop._err_count = getattr(game_loop, '_err_count', 0) + 1
                    if game_loop._err_count % 30 == 0:
                        logger.error(f"вќЊ РћС€РёР±РєР° РІ РёРіСЂРѕРІРѕРј С†РёРєР»Рµ (РїРѕРІС‚РѕСЂ x{game_loop._err_count}): {e}")
                time.sleep(3)  # РЈРІРµР»РёС‡РµРЅР° РїР°СѓР·Р° РїСЂРё РѕС€РёР±РєР°С…

    thread = threading.Thread(target=game_loop, daemon=True)
    thread.start()
    logger.info("вњ… РџСЂРѕСЃС‚РѕР№ РёРіСЂРѕРІРѕР№ С†РёРєР» Р·Р°РїСѓС‰РµРЅ")

# ==================== РђР’РўРћР РР—РђР¦РРЇ Р§Р•Р Р•Р— TELEGRAM Р‘РћРўРђ ====================
# РљРѕРґС‹ С…СЂР°РЅСЏС‚СЃСЏ РІ SQLite вЂ” СЂР°Р±РѕС‚Р°РµС‚ РЅР° PythonAnywhere Рё Р»СЋР±РѕРј С…РѕСЃС‚РёРЅРіРµ
# РўР°Р±Р»РёС†Р° auth_codes СЃРѕР·РґР°С‘С‚СЃСЏ РІ init_db()

def cleanup_old_auth_codes():
    """РћС‡РёСЃС‚РєР° СѓСЃС‚Р°СЂРµРІС€РёС… РєРѕРґРѕРІ (СЃС‚Р°СЂС€Рµ 10 РјРёРЅСѓС‚)"""
    try:
        conn = get_db_connection()
        if conn:
            conn.execute("DELETE FROM auth_codes WHERE created_at < datetime('now', '-10 minutes')")
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(f"cleanup auth codes error: {e}")

@app.route('/api/generate-auth-code', methods=['POST'])
def generate_auth_code():
    """Р“РµРЅРµСЂР°С†РёСЏ РєРѕРґР° Р°РІС‚РѕСЂРёР·Р°С†РёРё РґР»СЏ Р±СЂР°СѓР·РµСЂР°"""
    try:
        cleanup_old_auth_codes()
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=20))
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO auth_codes (code) VALUES (?)', (code,))
        conn.commit()
        conn.close()
        logger.info(f"рџ”ђ РЎРіРµРЅРµСЂРёСЂРѕРІР°РЅ РєРѕРґ Р°РІС‚РѕСЂРёР·Р°С†РёРё: {code}")
        return jsonify({'success': True, 'code': code})
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РіРµРЅРµСЂР°С†РёРё РєРѕРґР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/confirm-auth-code', methods=['POST'])
def confirm_auth_code():
    """РџРѕРґС‚РІРµСЂР¶РґРµРЅРёРµ РєРѕРґР° Р°РІС‚РѕСЂРёР·Р°С†РёРё РѕС‚ Р±РѕС‚Р°"""
    try:
        data = request.get_json()
        code = data.get('code', '').strip()
        user_data = data.get('user_data')

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT confirmed FROM auth_codes WHERE code = ? AND created_at > datetime('now', '-10 minutes')", (code,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return jsonify({'success': False, 'error': 'РќРµРІРµСЂРЅС‹Р№ РєРѕРґ'})

        if row[0]:
            conn.close()
            return jsonify({'success': False, 'error': 'РљРѕРґ СѓР¶Рµ РёСЃРїРѕР»СЊР·РѕРІР°РЅ'})

        import json as _json
        cursor.execute('UPDATE auth_codes SET confirmed = 1, user_data = ? WHERE code = ?',
                       (_json.dumps(user_data, ensure_ascii=False), code))
        conn.commit()
        conn.close()

        logger.info(f"вњ… РљРѕРґ {code} РїРѕРґС‚РІРµСЂР¶РґС‘РЅ РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј {user_data.get('id')}")
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ РєРѕРґР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/check-auth-code', methods=['POST'])
def check_auth_code():
    """РџСЂРѕРІРµСЂРєР° СЃС‚Р°С‚СѓСЃР° РєРѕРґР° Р°РІС‚РѕСЂРёР·Р°С†РёРё (РѕС‚ Р±СЂР°СѓР·РµСЂР°)"""
    try:
        data = request.get_json()
        code = data.get('code', '').strip()

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT confirmed, user_data, created_at FROM auth_codes WHERE code = ?", (code,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return jsonify({'success': False, 'error': 'РљРѕРґ РЅРµ РЅР°Р№РґРµРЅ'})

        confirmed, user_data_str, created_at = row

        if confirmed and user_data_str:
            import json as _json
            user_data = _json.loads(user_data_str)
            cursor.execute('DELETE FROM auth_codes WHERE code = ?', (code,))
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'confirmed': True, 'user_data': user_data})

        conn.close()
        return jsonify({'success': True, 'confirmed': False})
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё РєРѕРґР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

# ==================== РћРЎРќРћР’РќР«Р• Р РћРЈРўР« ====================

@app.route('/')
def root_page():
    """Р“Р»Р°РІРЅР°СЏ СЃС‚СЂР°РЅРёС†Р° вЂ” РљРµР№СЃС‹"""
    return render_template('index.html')


@app.route('/api/ping')
def api_ping():
    """Keepalive ping to prevent sleeping"""
    return jsonify({'pong': True})


@app.route('/crash')
def crash_page():
    """РЎС‚СЂР°РЅРёС†Р° РёРіСЂС‹ РљСЂР°С€"""
    return render_template('crash.html')

@app.route('/index')
def index():
    """Р“Р»Р°РІРЅР°СЏ СЃС‚СЂР°РЅРёС†Р° РєРµР№СЃРѕРІ (Р°Р»РёР°СЃ)"""
    return render_template('index.html')

@app.route('/case')
def case_main_page():
    """РЎС‚СЂР°РЅРёС†Р° СЃРїРёСЃРєР° РєРµР№СЃРѕРІ"""
    return render_template('index.html')

@app.route('/cases')
def cases_page():
    """РЎС‚СЂР°РЅРёС†Р° РєРµР№СЃРѕРІ (Р°Р»РёР°СЃ)"""
    return render_template('index.html')

@app.route('/case/<int:case_id>')
def case_page(case_id):
    """РЎС‚СЂР°РЅРёС†Р° РєРѕРЅРєСЂРµС‚РЅРѕРіРѕ РєРµР№СЃР°"""
    logger.info(f"рџ“„ Р—Р°РїСЂРѕСЃ СЃС‚СЂР°РЅРёС†С‹ РєРµР№СЃР° {case_id}")
    return render_template('case.html', case_id=case_id)

@app.route('/inventory')
def inventory_page():
    """РЎС‚СЂР°РЅРёС†Р° РёРЅРІРµРЅС‚Р°СЂСЏ"""
    logger.info("рџЋ’ Р—Р°РїСЂРѕСЃ СЃС‚СЂР°РЅРёС†С‹ РёРЅРІРµРЅС‚Р°СЂСЏ")
    return render_template('inventory.html')

@app.route('/profile')
def profile_page():
    """РЎС‚СЂР°РЅРёС†Р° РїСЂРѕС„РёР»СЏ в†’ СЂРµРґРёСЂРµРєС‚ РЅР° РёРЅРІРµРЅС‚Р°СЂСЊ"""
    logger.info("рџ‘¤ Р—Р°РїСЂРѕСЃ СЃС‚СЂР°РЅРёС†С‹ РїСЂРѕС„РёР»СЏ в†’ СЂРµРґРёСЂРµРєС‚ РЅР° /inventory")
    return redirect('/inventory')

@app.route('/ref')
def ref_page():
    """РЎС‚СЂР°РЅРёС†Р° СЂРµС„РµСЂР°Р»СЊРЅРѕР№ СЃРёСЃС‚РµРјС‹"""
    return render_template('ref.html')

@app.route('/lobby')
def lobby_page():
    """РЎС‚СЂР°РЅРёС†Р° Р»РѕР±Р±Рё"""
    return render_template('lobby.html')

@app.route('/upgrade')
def upgrade_page():
    """РЎС‚СЂР°РЅРёС†Р° Р°РїРіСЂРµР№РґР° в†’ РїРѕРєР° СЂРµРґРёСЂРµРєС‚ РЅР° РёРЅРІРµРЅС‚Р°СЂСЊ"""
    return redirect('/inventory')

@app.route('/leaderboard')
def leaderboard_page():
    """РЎС‚СЂР°РЅРёС†Р° Р»РёРґРµСЂР±РѕСЂРґР°"""
    return render_template('leaderboard.html')

@app.route('/admin')
def admin_page():
    """РЎС‚СЂР°РЅРёС†Р° Р°РґРјРёРЅ-РїР°РЅРµР»Рё"""
    logger.info("рџ› пёЏ Р—Р°РїСЂРѕСЃ СЃС‚СЂР°РЅРёС†С‹ Р°РґРјРёРЅ-РїР°РЅРµР»Рё")
    return render_template('admin.html')

@app.route('/shop-verification-QX2XNbyDv5.txt')
def cardlink_verification():
    """Р¤Р°Р№Р» РІРµСЂРёС„РёРєР°С†РёРё CardLink"""
    return send_from_directory('static', 'shop-verification-QX2XNbyDv5.txt', mimetype='text/plain')

@app.route('/static/<path:path>')
def serve_static(path):
    """РћР±СЃР»СѓР¶РёРІР°РЅРёРµ СЃС‚Р°С‚РёС‡РµСЃРєРёС… С„Р°Р№Р»РѕРІ"""
    return send_from_directory('static', path)

@app.route('/music/<path:path>')
def serve_music(path):
    """РћР±СЃР»СѓР¶РёРІР°РЅРёРµ РјСѓР·С‹РєР°Р»СЊРЅС‹С… С„Р°Р№Р»РѕРІ"""
    return send_from_directory('music', path)

# ==================== API ENDPOINTS ====================

# === DEMO LOGIN (DISABLED) ===
# Demo login has been removed for security.
@app.route('/api/demo-login', methods=['POST'])
def demo_login():
    """Demo login disabled"""
    return jsonify({'success': False, 'error': 'Demo login is disabled'}), 403

@app.route('/api/verify-demo-code', methods=['POST'])
def verify_demo_code():
    """Demo code verification disabled"""
    return jsonify({'success': False, 'error': 'Demo codes are disabled'}), 403

# TELEGRAM API
@app.route('/api/telegram/user', methods=['GET'])
def get_telegram_user():
    """РџРѕР»СѓС‡РµРЅРёРµ РґР°РЅРЅС‹С… РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ Telegram"""
    try:
        user_id = request.args.get('user_id')

        if not user_id:
            return jsonify({'success': False, 'error': 'ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РЅРµ СѓРєР°Р·Р°РЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        user_data = {
            'id': user[0],
            'first_name': user[1],
            'last_name': user[2],
            'username': user[3],
            'photo_url': user[4],
            'balance_stars': user[5],
            'balance_tickets': user[6],
            'referral_code': user[8],
            'experience': user[14] or 0,
            'current_level': user[15] or 1
        }

        conn.close()

        return jsonify({
            'success': True,
            'user': user_data
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {e}")
        return jsonify({'success': False, 'error': str(e)})



# ==================== Р”РћРџРћР›РќРРўР•Р›Р¬РќР«Р• API Р”Р›РЇ ULTIMATE CRASH ====================

# РљСЌС€ СЃС‚Р°РІРѕРє РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ (game_id, user_id) -> {bet_data, timestamp}
_user_bets_cache = {}

def _cleanup_user_bets_cache():
    """Remove stale entries from _user_bets_cache to prevent memory leak"""
    try:
        cached = get_crash_cache()
        current_game_id = cached.get('id', 0)
        stale_keys = [k for k in list(_user_bets_cache.keys()) if k[0] < current_game_id - 5]
        for k in stale_keys:
            _user_bets_cache.pop(k, None)
    except Exception:
        pass

@app.route('/api/ultimate-crash/simple-status', methods=['GET'])
def ultimate_crash_simple_status():
    """Р‘С‹СЃС‚СЂС‹Р№ СЃС‚Р°С‚СѓСЃ РёРіСЂС‹ - РёСЃРїРѕР»СЊР·СѓРµС‚ РєСЌС€"""
    user_id = request.args.get('user_id')
    
    # Р’СЃРµРіРґР° РёСЃРїРѕР»СЊР·СѓРµРј РєСЌС€ РёРіСЂС‹
    cached = get_crash_cache()
    cache_age = time.time() - cached.get('timestamp', 0)
    
    # Р•СЃР»Рё РєСЌС€ СЃРІРµР¶РёР№ (< 2 СЃРµРє) - РЅРµ С‚СЂРѕРіР°РµРј Р‘Р”
    if cache_age < 2.0 and cached.get('id', 0) > 0:
        game_data = {
            'id': cached['id'],
            'status': cached['status'],
            'current_multiplier': cached['current_multiplier'],
            'target_multiplier': cached['target_multiplier'],
            'time_remaining': cached['time_remaining']
        }
        
        # РљСЌС€РёСЂРѕРІР°РЅРЅС‹Рµ СЃС‚Р°РІРєРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ (РєСЌС€ 5 СЃРµРє)
        user_bet = None
        if user_id:
            cache_key = (cached['id'], user_id)
            bet_cache = _user_bets_cache.get(cache_key)
            if bet_cache and time.time() - bet_cache.get('ts', 0) < 5:
                user_bet = bet_cache.get('bet')
            else:
                try:
                    conn = sqlite3.connect(DB_PATH, timeout=5)
                    cursor = conn.cursor()
                    cursor.execute("SELECT id, bet_amount, status FROM ultimate_crash_bets WHERE game_id = ? AND user_id = ? AND status = 'active' LIMIT 1", (cached['id'], user_id))
                    bet = cursor.fetchone()
                    conn.close()
                    if bet:
                        user_bet = {'id': bet[0], 'bet_amount': bet[1], 'status': bet[2]}
                    _user_bets_cache[cache_key] = {'bet': user_bet, 'ts': time.time()}
                except:
                    pass
        
        return jsonify({'success': True, 'game': game_data, 'user_bet': user_bet})
    
    # Fallback - РµСЃР»Рё РєСЌС€ СѓСЃС‚Р°СЂРµР»
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT id, status, current_multiplier, target_multiplier FROM ultimate_crash_games WHERE status IN ('waiting', 'counting', 'flying', 'crashed') ORDER BY id DESC LIMIT 1")
        game = cursor.fetchone()
        
        if game:
            game_id, status, current_mult, target_mult = game
            game_data = {
                'id': game_id,
                'status': status,
                'current_multiplier': float(current_mult) if current_mult else 1.0,
                'target_multiplier': float(target_mult) if target_mult else 5.0,
                'time_remaining': 5.0
            }
            
            user_bet = None
            if user_id:
                cursor.execute("SELECT id, bet_amount, status FROM ultimate_crash_bets WHERE game_id = ? AND user_id = ? AND status = 'active' LIMIT 1", (game_id, user_id))
                bet = cursor.fetchone()
                if bet:
                    user_bet = {'id': bet[0], 'bet_amount': bet[1], 'status': bet[2]}
            
            conn.close()
            return jsonify({'success': True, 'game': game_data, 'user_bet': user_bet})
        
        conn.close()
    except:
        pass
    
    return jsonify({'success': True, 'game': {'id': 0, 'status': 'waiting', 'current_multiplier': 1.0, 'target_multiplier': 5.0, 'time_remaining': 5.0}, 'user_bet': None})

@app.route('/api/ultimate-crash/place-bet', methods=['POST'])
def ultimate_crash_place_bet():
    """РЈРїСЂРѕС‰РµРЅРЅР°СЏ РІРµСЂСЃРёСЏ СЂР°Р·РјРµС‰РµРЅРёСЏ СЃС‚Р°РІРєРё"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        bet_amount = data.get('bet_amount', 0)

        if not user_id:
            return jsonify({'success': False, 'error': 'ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РЅРµ СѓРєР°Р·Р°РЅ'})

        if bet_amount < 10:
            return jsonify({'success': False, 'error': 'РњРёРЅРёРјР°Р»СЊРЅР°СЏ СЃС‚Р°РІРєР° 25'})

        # Check phase transition lock FIRST (prevents bets during countingв†’flying)
        with _crash_phase_lock:
            if _crash_phase_transitioning:
                return jsonify({'success': False, 'error': 'РРіСЂР° СЃС‚Р°СЂС‚СѓРµС‚, СЃС‚Р°РІРєР° РЅР° СЃР»РµРґ. СЂР°СѓРЅРґ'})

        # Quick cache check before opening DB
        cached = get_crash_cache()
        cached_status = cached.get('status', '')
        if cached_status == 'flying':
            return jsonify({'success': False, 'error': 'РРіСЂР° СѓР¶Рµ РЅР°С‡Р°Р»Р°СЃСЊ! РЎС‚Р°РІРєР° РЅР° СЃР»РµРґ. СЂР°СѓРЅРґ'})
        if cached_status == 'crashed':
            return jsonify({'success': False, 'error': 'Р Р°СѓРЅРґ Р·Р°РІРµСЂС€С‘РЅ. РЎС‚Р°РІРєР° РЅР° СЃР»РµРґ. СЂР°СѓРЅРґ'})
        if cached_status == 'counting' and cached.get('time_remaining', 5) < 0.3:
            return jsonify({'success': False, 'error': 'РЎР»РёС€РєРѕРј РїРѕР·РґРЅРѕ! РЎС‚Р°РІРєР° РЅР° СЃР»РµРґ. СЂР°СѓРЅРґ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Use BEGIN IMMEDIATE for atomic balance check + deduction
            cursor.execute('BEGIN IMMEDIATE')

            # РџСЂРѕРІРµСЂСЏРµРј Р±Р°Р»Р°РЅСЃ Рё total_loss
            cursor.execute('SELECT balance_stars, COALESCE(total_loss, 0) FROM users WHERE id = ?', (user_id,))
            user = cursor.fetchone()

            if not user:
                conn.rollback()
                conn.close()
                return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

            current_balance = user[0] or 0
            total_loss = user[1] or 0

            if current_balance < bet_amount:
                conn.rollback()
                conn.close()
                return jsonify({'success': False, 'error': f'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ СЃСЂРµРґСЃС‚РІ. Р‘Р°Р»Р°РЅСЃ: {current_balance}'})

            # РџРѕР»СѓС‡Р°РµРј Р°РєС‚РёРІРЅСѓСЋ РёРіСЂСѓ (waiting РёР»Рё counting)
            cursor.execute('''
                SELECT id, status FROM ultimate_crash_games
                WHERE status IN ('waiting', 'counting')
                ORDER BY id DESC LIMIT 1
            ''')

            game = cursor.fetchone()

            if not game:
                # РЎРѕР·РґР°РµРј РЅРѕРІСѓСЋ РёРіСЂСѓ СЃСЂР°Р·Сѓ РІ counting
                base_multiplier = round(random.uniform(3.0, 10.0), 2)
                
                if total_loss >= 5000 and bet_amount <= total_loss * 0.2:
                    boost_chance = min(0.5, total_loss / 50000)
                    if random.random() < boost_chance:
                        base_multiplier = round(random.uniform(4.0, 15.0), 2)
                elif total_loss >= 2000 and bet_amount <= total_loss * 0.3:
                    boost_chance = min(0.3, total_loss / 30000)
                    if random.random() < boost_chance:
                        base_multiplier = round(random.uniform(3.5, 10.0), 2)
                elif total_loss >= 500 and bet_amount <= total_loss * 0.4:
                    if random.random() < 0.15:
                        base_multiplier = round(random.uniform(3.0, 8.0), 2)
                
                target_multiplier = base_multiplier
                cursor.execute('''
                    INSERT INTO ultimate_crash_games (status, target_multiplier, start_time)
                    VALUES ('counting', ?, CURRENT_TIMESTAMP)
                ''', (target_multiplier,))
                game_id = cursor.lastrowid
                game_status = 'counting'
            else:
                game_id, game_status = game

            if game_status not in ('waiting', 'counting'):
                conn.rollback()
                conn.close()
                return jsonify({'success': False, 'error': 'РРіСЂР° СѓР¶Рµ РЅР°С‡Р°Р»Р°СЃСЊ'})

            # РџСЂРѕРІРµСЂСЏРµРј, РµСЃС‚СЊ Р»Рё СѓР¶Рµ СЃС‚Р°РІРєР°
            cursor.execute('''
                SELECT id FROM ultimate_crash_bets
                WHERE game_id = ? AND user_id = ? AND status = 'active'
            ''', (game_id, user_id))

            existing_bet = cursor.fetchone()

            if existing_bet:
                conn.rollback()
                conn.close()
                return jsonify({'success': False, 'error': 'РЈ РІР°СЃ СѓР¶Рµ РµСЃС‚СЊ Р°РєС‚РёРІРЅР°СЏ СЃС‚Р°РІРєР°'})

            # РЎРїРёСЃС‹РІР°РµРј СЃСЂРµРґСЃС‚РІР° Рё СѓРІРµР»РёС‡РёРІР°РµРј СЃС‡С‘С‚С‡РёРє СЃС‚Р°РІРѕРє Рё РѕР±СЉС‘Рј
            cursor.execute('UPDATE users SET balance_stars = balance_stars - ?, total_crash_bets = COALESCE(total_crash_bets, 0) + 1, total_bet_volume = COALESCE(total_bet_volume, 0) + ? WHERE id = ?',
                         (bet_amount, bet_amount, user_id))

            # РЎРѕР·РґР°РµРј СЃС‚Р°РІРєСѓ
            cursor.execute('''
                INSERT INTO ultimate_crash_bets (game_id, user_id, bet_amount, gift_value, status)
                VALUES (?, ?, ?, ?, 'active')
            ''', (game_id, user_id, bet_amount, bet_amount))

            bet_id = cursor.lastrowid

            # Р”РѕР±Р°РІР»СЏРµРј РІ РёСЃС‚РѕСЂРёСЋ
            cursor.execute('''
                INSERT INTO user_history (user_id, operation_type, amount, description)
                VALUES (?, 'ultimate_crash_bet', ?, ?)
            ''', (user_id, -bet_amount, f'РЎС‚Р°РІРєР° РІ Ultimate Crash: {bet_amount}'))

            # РџРѕР»СѓС‡Р°РµРј РЅРѕРІС‹Р№ Р±Р°Р»Р°РЅСЃ
            cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
            new_balance = cursor.fetchone()[0]

            conn.commit()
        except Exception as inner_e:
            try: conn.rollback()
            except: pass
            conn.close()
            raise inner_e

        conn.close()

        # РћС‡РёС‰Р°РµРј РєСЌС€ СЃС‚Р°РІРѕРє РґР»СЏ СЌС‚РѕРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
        cache_key = (game_id, user_id)
        if cache_key in _user_bets_cache:
            del _user_bets_cache[cache_key]

        # Add experience based on bet amount (turnover) - 1:1
        try:
            add_experience(user_id, bet_amount, f"Crash bet {bet_amount}")
        except:
            pass

        # Update daily tasks
        try:
            update_daily_task_progress(user_id, 'crash_bets', 1)
            update_daily_task_progress(user_id, 'turnover', bet_amount)
        except:
            pass

        return jsonify({
            'success': True,
            'bet_id': bet_id,
            'game_id': game_id,
            'new_balance': new_balance,
            'message': f'РЎС‚Р°РІРєР° {bet_amount} РїСЂРёРЅСЏС‚Р°!'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃС‚Р°РІРєРё: {e}")
        try: conn.close()
        except: pass
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/ultimate-crash/place-bet-gift', methods=['POST'])
def ultimate_crash_place_bet_gift():
    """РЎС‚Р°РІРєР° РїРѕРґР°СЂРєРѕРј РёР· РёРЅРІРµРЅС‚Р°СЂСЏ"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        inventory_id = data.get('inventory_id')

        if not user_id or not inventory_id:
            return jsonify({'success': False, 'error': 'РќРµ СѓРєР°Р·Р°РЅ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ РёР»Рё РїРѕРґР°СЂРѕРє'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, gift_name, gift_image, gift_value
            FROM inventory WHERE id = ? AND user_id = ? AND is_withdrawing = 0
        ''', (inventory_id, user_id))
        gift = cursor.fetchone()

        if not gift:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕРґР°СЂРѕРє РЅРµ РЅР°Р№РґРµРЅ РІ РёРЅРІРµРЅС‚Р°СЂРµ'})

        inv_id, gift_name, gift_image, gift_value = gift
        bet_amount = gift_value

        if bet_amount < 10:
            conn.close()
            return jsonify({'success': False, 'error': 'РЎС‚РѕРёРјРѕСЃС‚СЊ РїРѕРґР°СЂРєР° РјРµРЅСЊС€Рµ 25в­ђ'})

        cursor.execute('''
            SELECT id, status FROM ultimate_crash_games
            WHERE status IN ('waiting', 'counting') ORDER BY id DESC LIMIT 1
        ''')
        game = cursor.fetchone()

        if not game:
            target_multiplier = round(random.uniform(3.0, 10.0), 2)
            cursor.execute('''
                INSERT INTO ultimate_crash_games (status, target_multiplier, start_time)
                VALUES ('waiting', ?, CURRENT_TIMESTAMP)
            ''', (target_multiplier,))
            game_id = cursor.lastrowid
        else:
            game_id, game_status = game
            if game_status not in ('waiting', 'counting'):
                conn.close()
                return jsonify({'success': False, 'error': 'РРіСЂР° СѓР¶Рµ РЅР°С‡Р°Р»Р°СЃСЊ'})
            # РџСЂРѕРІРµСЂСЏРµРј РІСЂРµРјСЏ РґРѕ СЃС‚Р°СЂС‚Р° - РµСЃР»Рё < 1.5 СЃРµРє, РѕС‚РєР»РѕРЅСЏРµРј
            cached = get_crash_cache()
            if cached.get('status') == 'counting' and cached.get('time_remaining', 5) < 1.5:
                conn.close()
                return jsonify({'success': False, 'error': 'РЎР»РёС€РєРѕРј РїРѕР·РґРЅРѕ! РЎС‚Р°РІРєР° РЅР° СЃР»РµРґ. СЂР°СѓРЅРґ'})

        cursor.execute('''
            SELECT id FROM ultimate_crash_bets
            WHERE game_id = ? AND user_id = ? AND status = 'active'
        ''', (game_id, user_id))
        if cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'error': 'РЈ РІР°СЃ СѓР¶Рµ РµСЃС‚СЊ СЃС‚Р°РІРєР°'})

        cursor.execute('DELETE FROM inventory WHERE id = ? AND user_id = ?', (inventory_id, user_id))

        # РЈРІРµР»РёС‡РёРІР°РµРј СЃС‡С‘С‚С‡РёРє СЃС‚Р°РІРѕРє Рё РѕР±СЉС‘Рј
        cursor.execute('UPDATE users SET total_crash_bets = COALESCE(total_crash_bets, 0) + 1, total_bet_volume = COALESCE(total_bet_volume, 0) + ? WHERE id = ?', (gift_value, user_id,))

        cursor.execute('''
            INSERT INTO ultimate_crash_bets (game_id, user_id, bet_amount, gift_value, status)
            VALUES (?, ?, ?, ?, 'active')
        ''', (game_id, user_id, bet_amount, bet_amount))
        bet_id = cursor.lastrowid

        cursor.execute('''
            INSERT INTO user_history (user_id, operation_type, amount, description)
            VALUES (?, 'ultimate_crash_bet', ?, ?)
        ''', (user_id, -bet_amount, f'РЎС‚Р°РІРєР° РїРѕРґР°СЂРєРѕРј: {gift_name} ({bet_amount}в­ђ)'))

        conn.commit()

        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()[0]
        conn.close()

        return jsonify({
            'success': True,
            'bet_id': bet_id,
            'game_id': game_id,
            'new_balance': new_balance,
            'bet_amount': bet_amount,
            'gift_name': gift_name,
            'message': f'РЎС‚Р°РІРєР° {gift_name} ({bet_amount}в­ђ) РїСЂРёРЅСЏС‚Р°!'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃС‚Р°РІРєРё РїРѕРґР°СЂРєРѕРј: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/ultimate-crash/place-bet-multi-gift', methods=['POST'])
def ultimate_crash_place_bet_multi_gift():
    """РЎС‚Р°РІРєР° РЅРµСЃРєРѕР»СЊРєРёРјРё РїРѕРґР°СЂРєР°РјРё РёР· РёРЅРІРµРЅС‚Р°СЂСЏ"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        inventory_ids = data.get('inventory_ids', [])

        if not user_id or not inventory_ids:
            return jsonify({'success': False, 'error': 'РќРµ СѓРєР°Р·Р°РЅ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ РёР»Рё РїРѕРґР°СЂРєРё'})

        if not isinstance(inventory_ids, list) or len(inventory_ids) == 0:
            return jsonify({'success': False, 'error': 'РќСѓР¶РЅРѕ РІС‹Р±СЂР°С‚СЊ С…РѕС‚СЏ Р±С‹ РѕРґРёРЅ РїРѕРґР°СЂРѕРє'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # РџРѕР»СѓС‡Р°РµРј РІСЃРµ РїРѕРґР°СЂРєРё
        placeholders = ','.join(['?' for _ in inventory_ids])
        cursor.execute(f'''
            SELECT id, gift_name, gift_image, gift_value
            FROM inventory WHERE id IN ({placeholders}) AND user_id = ? AND is_withdrawing = 0
        ''', inventory_ids + [user_id])
        gifts = cursor.fetchall()

        if len(gifts) != len(inventory_ids):
            conn.close()
            return jsonify({'success': False, 'error': 'РќРµРєРѕС‚РѕСЂС‹Рµ РїРѕРґР°СЂРєРё РЅРµ РЅР°Р№РґРµРЅС‹'})

        # РЎС‡РёС‚Р°РµРј РѕР±С‰СѓСЋ СЃС‚РѕРёРјРѕСЃС‚СЊ
        total_value = sum(g[3] for g in gifts)
        gift_names = [g[1] for g in gifts]

        if total_value < 25:
            conn.close()
            return jsonify({'success': False, 'error': 'РћР±С‰Р°СЏ СЃС‚РѕРёРјРѕСЃС‚СЊ РїРѕРґР°СЂРєРѕРІ РјРµРЅСЊС€Рµ 25в­ђ'})

        cursor.execute('''
            SELECT id, status FROM ultimate_crash_games
            WHERE status IN ('waiting', 'counting') ORDER BY id DESC LIMIT 1
        ''')
        game = cursor.fetchone()

        if not game:
            target_multiplier = round(random.uniform(3.0, 10.0), 2)
            cursor.execute('''
                INSERT INTO ultimate_crash_games (status, target_multiplier, start_time)
                VALUES ('waiting', ?, CURRENT_TIMESTAMP)
            ''', (target_multiplier,))
            game_id = cursor.lastrowid
        else:
            game_id, game_status = game
            if game_status not in ('waiting', 'counting'):
                conn.close()
                return jsonify({'success': False, 'error': 'РРіСЂР° СѓР¶Рµ РЅР°С‡Р°Р»Р°СЃСЊ'})
            # РџСЂРѕРІРµСЂСЏРµРј РІСЂРµРјСЏ РґРѕ СЃС‚Р°СЂС‚Р° - РµСЃР»Рё < 1.5 СЃРµРє, РѕС‚РєР»РѕРЅСЏРµРј
            cached = get_crash_cache()
            if cached.get('status') == 'counting' and cached.get('time_remaining', 5) < 1.5:
                conn.close()
                return jsonify({'success': False, 'error': 'РЎР»РёС€РєРѕРј РїРѕР·РґРЅРѕ! РЎС‚Р°РІРєР° РЅР° СЃР»РµРґ. СЂР°СѓРЅРґ'})

        cursor.execute('''
            SELECT id FROM ultimate_crash_bets
            WHERE game_id = ? AND user_id = ? AND status = 'active'
        ''', (game_id, user_id))
        if cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'error': 'РЈ РІР°СЃ СѓР¶Рµ РµСЃС‚СЊ СЃС‚Р°РІРєР°'})

        # РЈРґР°Р»СЏРµРј РІСЃРµ РїРѕРґР°СЂРєРё РёР· РёРЅРІРµРЅС‚Р°СЂСЏ
        cursor.execute(f'DELETE FROM inventory WHERE id IN ({placeholders}) AND user_id = ?', inventory_ids + [user_id])

        # РЈРІРµР»РёС‡РёРІР°РµРј СЃС‡С‘С‚С‡РёРє СЃС‚Р°РІРѕРє Рё РѕР±СЉС‘Рј
        cursor.execute('UPDATE users SET total_crash_bets = COALESCE(total_crash_bets, 0) + 1, total_bet_volume = COALESCE(total_bet_volume, 0) + ? WHERE id = ?', (total_value, user_id,))

        # РЎРѕР·РґР°РµРј СЃС‚Р°РІРєСѓ
        cursor.execute('''
            INSERT INTO ultimate_crash_bets (game_id, user_id, bet_amount, gift_value, status)
            VALUES (?, ?, ?, ?, 'active')
        ''', (game_id, user_id, total_value, total_value))
        bet_id = cursor.lastrowid

        cursor.execute('''
            INSERT INTO user_history (user_id, operation_type, amount, description)
            VALUES (?, 'ultimate_crash_bet', ?, ?)
        ''', (user_id, -total_value, f'РЎС‚Р°РІРєР° {len(gifts)} РїРѕРґР°СЂРєР°РјРё ({total_value}в­ђ)'))

        conn.commit()

        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()[0]
        conn.close()

        return jsonify({
            'success': True,
            'bet_id': bet_id,
            'game_id': game_id,
            'new_balance': new_balance,
            'bet_amount': total_value,
            'gift_count': len(gifts),
            'message': f'РЎС‚Р°РІРєР° {len(gifts)} РїРѕРґР°СЂРєРѕРІ ({total_value}в­ђ) РїСЂРёРЅСЏС‚Р°!'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃС‚Р°РІРєРё РЅРµСЃРєРѕР»СЊРєРёРјРё РїРѕРґР°СЂРєР°РјРё: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/ultimate-crash/cashout-simple', methods=['POST'])
def ultimate_crash_cashout_simple():
    """РљСЌС€Р°СѓС‚ СЃ РёРЅС‚РµРіСЂР°С†РёРµР№ РїРѕРґР°СЂРєРѕРІ - РµСЃР»Рё РІС‹РёРіСЂС‹С€ >= РјРёРЅ. СЃС‚РѕРёРјРѕСЃС‚Рё РїРѕРґР°СЂРєР°, РІС‹РґР°С‘Рј РїРѕРґР°СЂРѕРє"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')

        if not user_id:
            return jsonify({'success': False, 'error': 'ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РЅРµ СѓРєР°Р·Р°РЅ'})

        # Quick cache check вЂ” use cached multiplier for instant response
        cached = get_crash_cache()
        if cached.get('status') != 'flying':
            return jsonify({'success': False, 'error': 'РќРµС‚ Р°РєС‚РёРІРЅРѕР№ РёРіСЂС‹'})
        cached_mult = cached.get('current_multiplier', 1.0)

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # BEGIN IMMEDIATE to prevent double-cashout race
            cursor.execute('BEGIN IMMEDIATE')

            # РџРѕР»СѓС‡Р°РµРј Р°РєС‚РёРІРЅСѓСЋ РёРіСЂСѓ
            cursor.execute('''
                SELECT id, current_multiplier FROM ultimate_crash_games
                WHERE status = 'flying'
                ORDER BY id DESC LIMIT 1
            ''')

            game = cursor.fetchone()

            if not game:
                conn.rollback()
                conn.close()
                return jsonify({'success': False, 'error': 'РќРµС‚ Р°РєС‚РёРІРЅРѕР№ РёРіСЂС‹'})

            game_id = game[0]
            # Use the fresher of: DB multiplier vs cache multiplier
            db_mult = float(game[1]) if game[1] else 1.0
            current_mult = max(db_mult, cached_mult)

            # РџРѕР»СѓС‡Р°РµРј СЃС‚Р°РІРєСѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
            cursor.execute('''
                SELECT id, bet_amount FROM ultimate_crash_bets
                WHERE game_id = ? AND user_id = ? AND status = 'active'
                ORDER BY created_at DESC LIMIT 1
            ''', (game_id, user_id))

            bet = cursor.fetchone()

            if not bet:
                conn.rollback()
                conn.close()
                return jsonify({'success': False, 'error': 'РђРєС‚РёРІРЅР°СЏ СЃС‚Р°РІРєР° РЅРµ РЅР°Р№РґРµРЅР°'})

            bet_id, bet_amount = bet

            # Р Р°СЃС‡РµС‚ РІС‹РёРіСЂС‹С€Р°
            win_amount = int(bet_amount * current_mult)

            # РџС‹С‚Р°РµРјСЃСЏ РЅР°Р№С‚Рё РїРѕРґС…РѕРґСЏС‰РёР№ РїРѕРґР°СЂРѕРє (originals only)
            gift_awarded = None
            gifts = build_fragment_first_gifts_catalog()
            
            if gifts and win_amount >= 5:
                sorted_gifts = sorted(gifts, key=lambda g: g.get('value', 0))
                suitable_gifts = [g for g in sorted_gifts if g.get('value', 0) <= win_amount and g.get('value', 0) > 0]
                if suitable_gifts:
                    gift_awarded = suitable_gifts[-1]

            # РћР±РЅРѕРІР»СЏРµРј СЃС‚Р°РІРєСѓ
            cursor.execute('''
                UPDATE ultimate_crash_bets
                SET status = 'cashed_out',
                    cashout_multiplier = ?,
                    win_amount = ?
                WHERE id = ? AND status = 'active'
            ''', (current_mult, win_amount, bet_id))

            # Check if update actually changed a row (prevents double cashout)
            if cursor.rowcount == 0:
                conn.rollback()
                conn.close()
                return jsonify({'success': False, 'error': 'РЎС‚Р°РІРєР° СѓР¶Рµ Р·Р°Р±СЂР°РЅР°'})

            # РџРѕР»СѓС‡Р°РµРј РёРјСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
            cursor.execute('SELECT first_name FROM users WHERE id = ?', (user_id,))
            user_row = cursor.fetchone()
            user_name = user_row[0] if user_row else f'User_{user_id}'

            if gift_awarded:
                gift_image = gift_awarded.get('image', '/static/img/star.png')
                if gift_image and gift_image.startswith('data:'):
                    gift_image = '/static/img/star.png'
                
                cursor.execute('''
                    INSERT INTO inventory (user_id, gift_id, gift_name, gift_image, gift_value)
                    VALUES (?, ?, ?, ?, ?)
                ''', (user_id, gift_awarded['id'], gift_awarded['name'], gift_image, gift_awarded.get('value', 0)))

                if gift_awarded.get('type') != 'ton_balance':
                    cursor.execute('SELECT 1 FROM user_gift_index WHERE user_id = ? AND gift_name = ?', (user_id, gift_awarded['name']))
                    is_new_gift = not cursor.fetchone()
                    cursor.execute('INSERT OR IGNORE INTO user_gift_index (user_id, gift_name) VALUES (?, ?)',
                                 (user_id, gift_awarded['name']))
                    
                    if is_new_gift:
                        try:
                            cursor.execute('''INSERT INTO admin_notifications 
                                (title, message, image_url, notif_type, target_user_id)
                                VALUES (?, ?, ?, 'gift_index', ?)''',
                                (gift_awarded['name'], f'РќРѕРІС‹Р№ РїРѕРґР°СЂРѕРє РІ РІР°С€РµР№ РєРѕР»Р»РµРєС†РёРё!',
                                 gift_image, user_id))
                        except:
                            pass

                cursor.execute('''
                    INSERT INTO win_history (user_id, user_name, gift_name, gift_image, gift_value, case_name)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (user_id, user_name, gift_awarded['name'], gift_image, gift_awarded.get('value', 0), 'Crash'))

                gift_value = gift_awarded.get('value', 0)
                star_difference = max(0, win_amount - gift_value)
                if star_difference > 0:
                    cursor.execute('''
                        UPDATE users
                        SET balance_stars = balance_stars + ?,
                            total_earned_stars = total_earned_stars + ?
                        WHERE id = ?
                    ''', (star_difference, star_difference, user_id))

                cursor.execute('''
                    INSERT INTO user_history (user_id, operation_type, amount, description)
                    VALUES (?, 'ultimate_crash_win', ?, ?)
                ''', (user_id, win_amount, f'Р’С‹РёРіСЂС‹С€ РІ Crash x{current_mult:.2f}: {gift_awarded["name"]} + {star_difference}в­ђ'))
            else:
                cursor.execute('''
                    UPDATE users
                    SET balance_stars = balance_stars + ?,
                        total_earned_stars = total_earned_stars + ?
                    WHERE id = ?
                ''', (win_amount, win_amount, user_id))

                cursor.execute('''
                    INSERT INTO user_history (user_id, operation_type, amount, description)
                    VALUES (?, 'ultimate_crash_win', ?, ?)
                ''', (user_id, win_amount, f'Р’С‹РёРіСЂС‹С€ РІ Crash: x{current_mult:.2f}'))

                cursor.execute('''
                    INSERT INTO win_history (user_id, user_name, gift_name, gift_image, gift_value, case_name)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (user_id, user_name, f'Stars x{current_mult:.2f}',
                      '/static/img/star.png', win_amount, 'Crash'))

            # РџРѕР»СѓС‡Р°РµРј РЅРѕРІС‹Р№ Р±Р°Р»Р°РЅСЃ Р”Рћ commit (РІРЅСѓС‚СЂРё С‚СЂР°РЅР·Р°РєС†РёРё)
            cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
            new_balance = cursor.fetchone()[0]

            conn.commit()
        except Exception as inner_e:
            try: conn.rollback()
            except: pass
            conn.close()
            raise inner_e

        conn.close()

        # Experience based on bet (1:1 turnover) - now added on bet placement, not cashout
        # exp_gained = bet_amount (removed - exp is added when placing bet)

        # Р¤РѕСЂРјРёСЂСѓРµРј РѕС‚РІРµС‚
        response = {
            'success': True,
            'win_amount': win_amount,
            'multiplier': current_mult,
            'new_balance': new_balance,
        }

        if gift_awarded:
            gift_img = gift_awarded.get('image', '/static/img/star.png')
            if gift_img and gift_img.startswith('data:'):
                gift_img = '/static/img/star.png'
            g_val = gift_awarded.get('value', 0)
            s_diff = max(0, win_amount - g_val)
            response['gift'] = {
                'id': gift_awarded['id'],
                'name': gift_awarded['name'],
                'image': gift_img,
                'value': g_val
            }
            response['star_difference'] = s_diff
            if s_diff > 0:
                response['message'] = f'Р’С‹ РІС‹РёРіСЂР°Р»Рё {gift_awarded["name"]} + {s_diff}в­ђ!'
            else:
                response['message'] = f'Р’С‹ РІС‹РёРіСЂР°Р»Рё РїРѕРґР°СЂРѕРє: {gift_awarded["name"]}!'
            logger.info(f"вњ… Crash РєСЌС€Р°СѓС‚ РїРѕРґР°СЂРѕРє: {gift_awarded['name']} ({g_val}в­ђ) +{s_diff}в­ђ x{current_mult:.2f}")
        else:
            response['message'] = f'Р’С‹ РІС‹РёРіСЂР°Р»Рё {win_amount} Р·РІС‘Р·Рґ!'
            logger.info(f"вњ… Crash РєСЌС€Р°СѓС‚ Р·РІС‘Р·РґС‹: {win_amount} x{current_mult:.2f}")

        return jsonify(response)

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РєСЌС€Р°СѓС‚Р°: {e}")
        try: conn.close()
        except: pass
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/ultimate-crash/current-gift', methods=['GET'])
def ultimate_crash_current_gift():
    """Р’РѕР·РІСЂР°С‰Р°РµС‚ РїРѕРґР°СЂРѕРє, СЃРѕРѕС‚РІРµС‚СЃС‚РІСѓСЋС‰РёР№ С‚РµРєСѓС‰РµРјСѓ bet_amount * multiplier вЂ” С‚РѕР»СЊРєРѕ РѕСЂРёРіРёРЅР°Р»С‹"""
    try:
        bet_amount = int(request.args.get('bet_amount', 0))
        multiplier = float(request.args.get('multiplier', 1.0))
        
        if bet_amount <= 0:
            return jsonify({'success': True, 'gift': None})
        
        win_value = int(bet_amount * multiplier)
        
        # Use Fragment catalog вЂ” originals only (no models)
        gifts = build_fragment_first_gifts_catalog()
        if not gifts or win_value < 5:
            return jsonify({'success': True, 'gift': None})
        
        sorted_gifts = sorted(gifts, key=lambda g: g.get('value', 0))
        suitable = [g for g in sorted_gifts if g.get('value', 0) <= win_value and g.get('value', 0) > 0]
        
        if suitable:
            gift = suitable[-1]
            gift_img = gift.get('image', '/static/img/star.png')
            if gift_img and gift_img.startswith('data:'):
                gift_img = '/static/img/star.png'
            return jsonify({
                'success': True,
                'gift': {
                    'id': gift.get('id') or gift.get('gift_key') or gift.get('fragment_slug'),
                    'name': gift['name'],
                    'image': gift_img,
                    'value': gift.get('value', 0)
                },
                'win_value': win_value
            })
        
        return jsonify({'success': True, 'gift': None, 'win_value': win_value})
    except Exception as e:
        return jsonify({'success': True, 'gift': None})

# ==================== РђР’РўРћРњРђРўРР—РђР¦РРЇ РР“Р РћР’РћР“Рћ Р¦РРљР›Рђ ====================

def start_simple_game_loop():
    """Р—Р°РїСѓСЃРєР°РµС‚ СѓРїСЂРѕС‰РµРЅРЅС‹Р№ РёРіСЂРѕРІРѕР№ С†РёРєР»"""
    def game_loop():
        logger.info("рџљЂ Р—Р°РїСѓС‰РµРЅ СѓРїСЂРѕС‰РµРЅРЅС‹Р№ РёРіСЂРѕРІРѕР№ С†РёРєР»")

        while True:
            try:
                # РџР°СѓР·Р° РјРµР¶РґСѓ РёРіСЂР°РјРё
                time.sleep(3)

                conn = get_db_connection()
                cursor = conn.cursor()

                # РЎРѕР·РґР°РµРј РЅРѕРІСѓСЋ РёРіСЂСѓ РµСЃР»Рё РЅРµС‚ Р°РєС‚РёРІРЅРѕР№
                cursor.execute('''
                    SELECT COUNT(*) FROM ultimate_crash_games
                    WHERE status IN ('waiting', 'counting', 'flying')
                ''')
                active_games = cursor.fetchone()[0]

                if active_games == 0:
                    target_multiplier = round(random.uniform(3.0, 10.0), 2)
                    cursor.execute('''
                        INSERT INTO ultimate_crash_games (status, target_multiplier, start_time)
                        VALUES ('waiting', ?, CURRENT_TIMESTAMP)
                    ''', (target_multiplier,))
                    game_id = cursor.lastrowid
                    conn.commit()
                    logger.info(f"рџ†• РЎРѕР·РґР°РЅР° РЅРѕРІР°СЏ РёРіСЂР° #{game_id}")

                conn.close()

            except Exception as e:
                logger.error(f"вќЊ РћС€РёР±РєР° РёРіСЂРѕРІРѕРіРѕ С†РёРєР»Р°: {e}")
                time.sleep(5)

    thread = threading.Thread(target=game_loop, daemon=True)
    thread.start()
    logger.info("вњ… РџСЂРѕСЃС‚РѕР№ РёРіСЂРѕРІРѕР№ С†РёРєР» Р·Р°РїСѓС‰РµРЅ")



@app.route('/api/telegram-auth', methods=['POST'])
def telegram_auth():
    """РђСѓС‚РµРЅС‚РёС„РёРєР°С†РёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ С‡РµСЂРµР· Telegram"""
    try:
        data = request.get_json()
        user_id = data['id']
        referral_code = data.get('referral_code')

        logger.info(f"рџ”ђ РђРІС‚РѕСЂРёР·Р°С†РёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {data.get('first_name')} (ID: {user_id})")

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            new_referral_code = generate_referral_code()

            cursor.execute('''
                INSERT INTO users (id, first_name, last_name, username, photo_url, balance_stars, balance_tickets, referral_code)
                VALUES (?, ?, ?, ?, ?, 0, 0, ?)
            ''', (
                user_id,
                data['first_name'],
                data.get('last_name', ''),
                data.get('username', ''),
                data.get('photo_url', ''),
                new_referral_code
            ))
            conn.commit()
            stars = 0
            tickets = 0

            if referral_code:
                process_referral(user_id, referral_code)

            add_history_record(user_id, 'registration', 0, 'Р РµРіРёСЃС‚СЂР°С†РёСЏ РІ СЃРёСЃС‚РµРјРµ')
            logger.info(f"вњ… Р—Р°СЂРµРіРёСЃС‚СЂРёСЂРѕРІР°РЅ РЅРѕРІС‹Р№ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ: {data['first_name']}")
        else:
            cursor.execute('''
                UPDATE users
                SET first_name = ?, last_name = ?, username = ?, photo_url = ?
                WHERE id = ?
            ''', (
                data['first_name'],
                data.get('last_name', ''),
                data.get('username', ''),
                data.get('photo_url', ''),
                user_id
            ))
            conn.commit()
            stars = user[5]
            tickets = user[6]
            logger.info(f"вњ… РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚: {data['first_name']}")

        conn.close()

        return jsonify({
            'success': True,
            'user': {
                'id': user_id,
                'first_name': data['first_name'],
                'last_name': data.get('last_name', ''),
                'username': data.get('username', ''),
                'photo_url': data.get('photo_url', ''),
                'balance_stars': stars,
                'balance_tickets': tickets,
                'currency_mode': 'stars'
            }
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° Р°РІС‚РѕСЂРёР·Р°С†РёРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

# USER API
@app.route('/api/user/<int:user_id>', methods=['GET'])
def get_user_data(user_id):
    """РџРѕР»СѓС‡РµРЅРёРµ РґР°РЅРЅС‹С… РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            logger.warning(f"вљ пёЏ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РЅРµ РЅР°Р№РґРµРЅ")
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        # Get column names for robust access
        col_names = [desc[0] for desc in cursor.description]
        user_row = dict(zip(col_names, user))

        cursor.execute('SELECT * FROM inventory WHERE user_id = ? ORDER BY received_at DESC', (user_id,))
        inventory = cursor.fetchall()

        level_info = get_user_level_info(user_id)

        user_dict = {
            'id': user_row.get('id', user[0]),
            'first_name': user_row.get('first_name', ''),
            'last_name': user_row.get('last_name', ''),
            'username': user_row.get('username', ''),
            'photo_url': user_row.get('photo_url', ''),
            'balance_stars': user_row.get('balance_stars', 0) or 0,
            'balance_tickets': user_row.get('balance_tickets', 0) or 0,
            'referral_code': user_row.get('referral_code', ''),
            'referral_count': user_row.get('referral_count', 0) or 0,
            'total_earned_stars': user_row.get('total_earned_stars', 0) or 0,
            'total_earned_tickets': user_row.get('total_earned_tickets', 0) or 0,
            'referral_bonus_claimed': bool(user_row.get('referral_bonus_claimed', False)),
            'experience': user_row.get('experience', 0) or 0,
            'current_level': user_row.get('current_level', 1) or 1,
            'total_cases_opened': user_row.get('total_cases_opened', 0) or 0,
            'total_crash_bets': user_row.get('total_crash_bets', 0) or 0,
            'total_bet_volume': user_row.get('total_bet_volume', 0) or 0,
            'crash_vip': bool(user_row.get('is_crash_vip', 0)),
            'currency_mode': user_row.get('currency_mode', 'stars') or 'stars',
            'referral_balance': user_row.get('referral_balance', 0) or 0,
            'level_info': level_info
        }

        inventory_list = []
        for item in inventory:
            inventory_list.append({
                'id': item[0],
                'user_id': item[1],
                'gift_id': item[2],
                'gift_name': item[3],
                'gift_image': item[4],
                'gift_value': item[5],
                'received_at': item[6],
                'is_withdrawing': bool(item[7])
            })

        conn.close()
        return jsonify({
            'success': True,
            'user': user_dict,
            'inventory': inventory_list
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РґР°РЅРЅС‹С… РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/user/currency-mode', methods=['POST'])
def set_user_currency_mode():
    """Set user display currency mode: stars or ton"""
    try:
        data = request.get_json() or {}
        user_id = data.get('user_id')
        mode = str(data.get('mode', 'stars')).lower()
        if mode not in ('stars', 'ton'):
            mode = 'stars'

        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'})

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET currency_mode = ? WHERE id = ?', (mode, user_id))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'mode': mode})
    except Exception as e:
        logger.error(f"set_user_currency_mode error: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/inventory/<int:user_id>', methods=['GET'])
def get_user_inventory(user_id):
    """РџРѕР»СѓС‡РµРЅРёРµ РёРЅРІРµРЅС‚Р°СЂСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM inventory WHERE user_id = ? ORDER BY received_at DESC', (user_id,))
        columns = [desc[0] for desc in cursor.description]
        inventory = cursor.fetchall()
        conn.close()

        local_gifts = load_gifts_cached() or []
        local_by_id = {}
        local_by_name = {}
        for gift in local_gifts:
            gift_id = gift.get('id')
            if gift_id is not None:
                try:
                    local_by_id[int(gift_id)] = gift
                except Exception:
                    pass
            name_key = _normalize_gift_name_for_match(gift.get('name'))
            if name_key and name_key not in local_by_name:
                local_by_name[name_key] = gift

        fragment_gifts = fetch_fragment_gifts_catalog(force_refresh=False) or []
        fragment_by_slug = {}
        fragment_by_name = {}
        for fg in fragment_gifts:
            slug = (fg.get('fragment_slug') or '').strip().lower()
            if slug:
                fragment_by_slug[slug] = fg
            name_key = _normalize_gift_name_for_match(fg.get('name'))
            if name_key and name_key not in fragment_by_name:
                fragment_by_name[name_key] = fg

        inventory_list = []
        for item in inventory:
            row = dict(zip(columns, item))

            raw_name = row.get('gift_name', 'РџРѕРґР°СЂРѕРє')
            name_key = _normalize_gift_name_for_match(raw_name)
            local_meta = None
            gift_id = row.get('gift_id')
            if gift_id is not None:
                try:
                    local_meta = local_by_id.get(int(gift_id))
                except Exception:
                    local_meta = None
            if not local_meta and name_key:
                local_meta = local_by_name.get(name_key)

            local_slug = ''
            if local_meta:
                local_slug = (local_meta.get('fragment_slug') or '').strip().lower()
            inferred_slug = local_slug or _slugify_fragment_name(raw_name)

            fragment_meta = None
            if inferred_slug:
                fragment_meta = fragment_by_slug.get(inferred_slug)
            if not fragment_meta and name_key:
                fragment_meta = fragment_by_name.get(name_key)

            gift_image = row.get('gift_image') or (local_meta.get('image') if local_meta else '') or '/static/img/gift.png'
            if str(gift_image).startswith('data:'):
                gift_image = '/static/img/gift.png'

            entry = {
                'id': row.get('id'),
                'user_id': row.get('user_id'),
                'gift_id': gift_id,
                'gift_name': raw_name,
                'gift_image': gift_image,
                'gift_value': row.get('gift_value', 0),
                'received_at': row.get('received_at'),
                'is_withdrawing': bool(row.get('is_withdrawing', 0)),
                'crate_id': row.get('crate_id'),
                'crate_name': row.get('crate_name'),
                'crate_image': row.get('crate_image'),
                'fragment_slug': (fragment_meta.get('fragment_slug') if fragment_meta else inferred_slug) or None,
                'fragment_url': fragment_meta.get('fragment_url') if fragment_meta else None,
                'fragment_image': fragment_meta.get('image') if fragment_meta else None,
            }
            inventory_list.append(entry)

        return jsonify({'success': True, 'inventory': inventory_list})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёРЅРІРµРЅС‚Р°СЂСЏ: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/inventory/open-crate', methods=['POST'])
def open_crate_from_inventory():
    """Open a crate that is in the user's inventory"""
    try:
        data = request.get_json()
        user_id = data.get('user_id', 0)
        inventory_id = data.get('inventory_id', 0)

        if not user_id or not inventory_id:
            return jsonify({'success': False, 'error': 'Missing data'})

        conn = get_db_connection()
        cursor = conn.cursor()
        init_crates_tables(cursor)
        conn.commit()

        # Get inventory item
        cursor.execute('SELECT * FROM inventory WHERE id = ? AND user_id = ?', (inventory_id, user_id))
        inv_item = cursor.fetchone()
        if not inv_item:
            conn.close()
            return jsonify({'success': False, 'error': 'РџСЂРµРґРјРµС‚ РЅРµ РЅР°Р№РґРµРЅ'})

        # Check if it's a crate
        crate_id = None
        try:
            crate_id = inv_item[8] if len(inv_item) > 8 else None
        except:
            pass

        if not crate_id:
            conn.close()
            return jsonify({'success': False, 'error': 'Р­С‚Рѕ РЅРµ СЏС‰РёРє'})

        # Get crate items
        cursor.execute('SELECT * FROM crate_items WHERE crate_id = ?', (crate_id,))
        items_raw = cursor.fetchall()
        if not items_raw:
            conn.close()
            return jsonify({'success': False, 'error': 'РЇС‰РёРє РїСѓСЃС‚'})

        # Build items list (ordinal: id=0, crate_id=1, item_type=2, item_id=3, item_name=4, chance=5, rarity=6)
        items = []
        for r in items_raw:
            items.append({
                'item_type': r[2],
                'item_id': r[3],
                'item_name': r[4] or r[3],
                'chance': r[5],
                'rarity': r[6] or 'common'
            })

        # Weighted random selection
        import random
        total_chance = sum(it['chance'] for it in items)
        roll = random.randint(1, total_chance)
        cumulative = 0
        won_item = items[0]
        for it in items:
            cumulative += it['chance']
            if roll <= cumulative:
                won_item = it
                break

        # Grant reward
        reward_desc = ''
        comp_desc = ''
        # Dynamic compensation: lower chance = higher stars (min 10, max 100)
        item_chance_pct = (won_item['chance'] / total_chance * 100) if total_chance > 0 else 50
        comp_stars = max(10, min(100, round(100 - item_chance_pct)))
        if won_item['item_type'] == 'stars':
            amount = int(won_item['item_id']) if won_item['item_id'].isdigit() else 10
            cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (amount, user_id))
            reward_desc = '+' + str(amount) + ' Stars'
        elif won_item['item_type'] == 'tickets':
            amount = int(won_item['item_id']) if won_item['item_id'].isdigit() else 1
            cursor.execute('UPDATE users SET balance_tickets = balance_tickets + ? WHERE id = ?', (amount, user_id))
            reward_desc = '+' + str(amount) + ' Р±РёР»РµС‚РѕРІ'
        elif won_item['item_type'] in ('rocket', 'background'):
            cursor.execute('SELECT id FROM user_customizations WHERE user_id = ? AND item_type = ? AND item_id = ?',
                          (user_id, won_item['item_type'], won_item['item_id']))
            already_owned = cursor.fetchone()
            if already_owned:
                cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (comp_stars, user_id))
                comp_desc = '+' + str(comp_stars) + ' Stars (СѓР¶Рµ РµСЃС‚СЊ)'
            else:
                cursor.execute('''INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source)
                    VALUES (?, ?, ?, 'crate')''', (user_id, won_item['item_type'], won_item['item_id']))
            reward_desc = won_item['item_name'] or won_item['item_id']

        # Remove crate from inventory
        cursor.execute('DELETE FROM inventory WHERE id = ? AND user_id = ?', (inventory_id, user_id))

        # Increment total_cases_opened
        cursor.execute('UPDATE users SET total_cases_opened = total_cases_opened + 1 WHERE id = ?', (user_id,))

        # Get updated balances
        cursor.execute('SELECT balance_stars, balance_tickets FROM users WHERE id = ?', (user_id,))
        updated = cursor.fetchone()

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'won_item': {
                'item_type': won_item['item_type'],
                'item_id': won_item['item_id'],
                'item_name': won_item['item_name'] or won_item['item_id'],
                'rarity': won_item['rarity'],
                'chance': won_item['chance']
            },
            'reward_desc': reward_desc,
            'comp_desc': comp_desc,
            'new_balance_stars': updated[0] if updated else 0,
            'new_balance_tickets': updated[1] if updated else 0,
            'all_items': [{'item_type': it['item_type'], 'item_id': it['item_id'],
                          'item_name': it['item_name'] or it['item_id'], 'rarity': it['rarity'],
                          'chance': it['chance']} for it in items]
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        try: conn.close()
        except: pass
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/inventory-history/<int:user_id>', methods=['GET'])
def get_inventory_history(user_id):
    """РџРѕР»СѓС‡РµРЅРёРµ РёСЃС‚РѕСЂРёРё РІСЃРµС… РїРѕРґР°СЂРєРѕРІ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ (РґР»СЏ РєРѕР»Р»РµРєС†РёРё)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        history = []
        
        # Get from case_open_history
        cursor.execute('''
            SELECT DISTINCT gift_id, gift_name, gift_image, gift_value 
            FROM case_open_history 
            WHERE user_id = ? AND gift_name IS NOT NULL
        ''', (user_id,))
        for row in cursor.fetchall():
            history.append({
                'gift_id': row[0],
                'gift_name': row[1],
                'gift_image': row[2],
                'gift_value': row[3]
            })
        
        # Get from win_history
        cursor.execute('''
            SELECT DISTINCT gift_name, gift_image, gift_value 
            FROM win_history 
            WHERE user_id = ? AND gift_name IS NOT NULL
        ''', (user_id,))
        for row in cursor.fetchall():
            history.append({
                'gift_name': row[0],
                'gift_image': row[1],
                'gift_value': row[2]
            })
        
        # Get from current inventory
        cursor.execute('''
            SELECT DISTINCT gift_id, gift_name, gift_image, gift_value 
            FROM inventory 
            WHERE user_id = ?
        ''', (user_id,))
        for row in cursor.fetchall():
            history.append({
                'gift_id': row[0],
                'gift_name': row[1],
                'gift_image': row[2],
                'gift_value': row[3]
            })
        
        conn.close()
        
        logger.info(f"рџ“љ РСЃС‚РѕСЂРёСЏ РєРѕР»Р»РµРєС†РёРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {user_id}: {len(history)} Р·Р°РїРёСЃРµР№")
        return jsonify({'success': True, 'history': history})
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёСЃС‚РѕСЂРёРё РёРЅРІРµРЅС‚Р°СЂСЏ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/collection-reward', methods=['POST'])
def claim_collection_reward():
    """РќР°РіСЂР°РґС‹ РєРѕР»Р»РµРєС†РёРѕРЅРµСЂР° РѕС‚РєР»СЋС‡РµРЅС‹"""
    return jsonify({'success': False, 'error': 'РќР°РіСЂР°РґС‹ РєРѕР»Р»РµРєС†РёРѕРЅРµСЂР° РІСЂРµРјРµРЅРЅРѕ РѕС‚РєР»СЋС‡РµРЅС‹'})


@app.route('/api/crash-vip/purchase', methods=['POST'])
def purchase_crash_vip():
    """РџРѕРєСѓРїРєР° VIP СЃС‚Р°С‚СѓСЃР° РґР»СЏ Crash РёРіСЂС‹ СЃРѕ СЃРєРёРґРєРѕР№ Р·Р° СЃРѕР±СЂР°РЅРЅС‹Рµ РїРѕРґР°СЂРєРё"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        collected_count = data.get('collected_count', 0)
        
        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if already VIP
        cursor.execute('SELECT is_crash_vip FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        if row and row[0]:
            conn.close()
            return jsonify({'success': False, 'error': 'VIP СѓР¶Рµ Р°РєС‚РёРІРёСЂРѕРІР°РЅ'})
        
        # Calculate price with discount: 250 base - 2 per collected gift (min 50)
        base_price = 250
        discount_per_gift = 2
        discount = min(collected_count * discount_per_gift, 200)  # Max 200 discount
        final_price = max(base_price - discount, 50)  # Min price 50
        
        # Check balance
        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        balance_row = cursor.fetchone()
        if not balance_row:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})
        
        current_balance = balance_row[0] or 0
        if current_balance < final_price:
            conn.close()
            return jsonify({'success': False, 'error': f'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ Р·РІС‘Р·Рґ. РќСѓР¶РЅРѕ {final_price}, РµСЃС‚СЊ {current_balance}'})
        
        # Deduct stars and activate VIP
        cursor.execute('UPDATE users SET balance_stars = balance_stars - ?, is_crash_vip = 1 WHERE id = ?', 
                      (final_price, user_id))
        
        # Record in history
        cursor.execute('''
            INSERT INTO user_history (user_id, operation_type, amount, description, created_at)
            VALUES (?, 'crash_vip_purchase', ?, ?, datetime('now'))
        ''', (user_id, -final_price, f'РџРѕРєСѓРїРєР° Crash VIP: {final_price} Р·РІС‘Р·Рґ (СЃРєРёРґРєР° {discount})'))
        
        # Get new balance
        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        logger.info(f"в­ђ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РєСѓРїРёР» Crash VIP Р·Р° {final_price} Р·РІС‘Р·Рґ!")
        return jsonify({
            'success': True,
            'new_balance': new_balance,
            'price_paid': final_price,
            'discount': discount
        })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕРєСѓРїРєРё VIP: {e}")
        try: conn.close()
        except: pass
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/crash-vip/status', methods=['GET'])
def check_crash_vip_status():
    """РџСЂРѕРІРµСЂРёС‚СЊ VIP СЃС‚Р°С‚СѓСЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
    try:
        user_id = request.args.get('user_id')
        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT is_crash_vip FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        conn.close()
        
        is_vip = bool(row[0]) if row else False
        return jsonify({'success': True, 'is_vip': is_vip})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/user-customizations/<int:user_id>', methods=['GET'])
def get_user_customizations(user_id):
    """РџРѕР»СѓС‡РёС‚СЊ СЂР°Р·Р±Р»РѕРєРёСЂРѕРІР°РЅРЅС‹Рµ РєР°СЃС‚РѕРјРёР·Р°С†РёРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ (РїРѕ РїСЂРѕРјРѕРєРѕРґР°Рј)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # РЎРѕР·РґР°С‘Рј С‚Р°Р±Р»РёС†Сѓ РµСЃР»Рё РЅРµ СЃСѓС‰РµСЃС‚РІСѓРµС‚
        cursor.execute('''CREATE TABLE IF NOT EXISTS user_customizations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            item_type TEXT NOT NULL,
            item_id TEXT NOT NULL,
            source TEXT DEFAULT 'promo',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, item_type, item_id)
        )''')
        
        cursor.execute('SELECT item_type, item_id FROM user_customizations WHERE user_id = ?', (user_id,))
        rows = cursor.fetchall()
        conn.close()
        
        rockets = [r[1] for r in rows if r[0] == 'rocket']
        backgrounds = [r[1] for r in rows if r[0] == 'background']
        
        return jsonify({
            'success': True,
            'rockets': rockets,
            'backgrounds': backgrounds
        })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РєР°СЃС‚РѕРјРёР·Р°С†РёР№: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/ton-payment', methods=['POST'])
def process_ton_payment():
    """РћР±СЂР°Р±РѕС‚РєР° РїР»Р°С‚РµР¶Р° РІ TON - РєРѕРЅРІРµСЂС‚Р°С†РёСЏ РІ Р·РІС‘Р·РґС‹ (1 TON = 100 Р·РІС‘Р·Рґ)"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        amount_ton = data.get('amount_ton')
        transaction_hash = data.get('transaction_hash')
        wallet_address = data.get('wallet_address')
        
        if not user_id or not amount_ton:
            return jsonify({'success': False, 'error': 'user_id and amount_ton required'})
        
        # Validate amount
        try:
            amount_ton = round(float(amount_ton), 6)
            if amount_ton <= 0:
                return jsonify({'success': False, 'error': 'Invalid amount'})
        except:
            return jsonify({'success': False, 'error': 'Invalid amount format'})
        
        # Calculate stars (1 TON = 100 stars)
        stars_to_add = int(round(amount_ton * TON_RATE))
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check for duplicate transaction
        if transaction_hash:
            cursor.execute('''
                SELECT id FROM user_history 
                WHERE description LIKE ? AND operation_type = 'ton_payment'
            ''', (f'%{transaction_hash}%',))
            if cursor.fetchone():
                conn.close()
                return jsonify({'success': False, 'error': 'Transaction already processed'})
        
        # Add stars to user
        cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (stars_to_add, user_id))
        
        # Give 10% referral commission to referrer
        cursor.execute('SELECT referred_by FROM users WHERE id = ?', (user_id,))
        ref_row = cursor.fetchone()
        if ref_row and ref_row[0]:
            referrer_id = ref_row[0]
            commission = int(round(stars_to_add * 0.10))  # 10%
            if commission > 0:
                cursor.execute('UPDATE users SET referral_balance = referral_balance + ? WHERE id = ?', (commission, referrer_id))
                logger.info(f"рџ’° Referral commission: {commission} stars to user {referrer_id} from deposit by {user_id}")
        
        # Record in history
        cursor.execute('''
            INSERT INTO user_history (user_id, operation_type, amount, description, created_at)
            VALUES (?, 'ton_payment', ?, ?, datetime('now'))
        ''', (user_id, stars_to_add, f'TON РїРѕРїРѕР»РЅРµРЅРёРµ: {amount_ton} TON = {stars_to_add} Р·РІС‘Р·Рґ (tx: {transaction_hash or "pending"})'))
        
        # Get new balance
        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        new_balance = row[0] if row else 0
        
        conn.commit()
        conn.close()
        
        logger.info(f"рџ’Ћ TON РїР»Р°С‚С‘Р¶: РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РїРѕР»СѓС‡РёР» {stars_to_add} Р·РІС‘Р·Рґ Р·Р° {amount_ton} TON")
        return jsonify({
            'success': True,
            'stars_added': stars_to_add,
            'new_balance': new_balance
        })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° TON РїР»Р°С‚РµР¶Р°: {e}")
        return jsonify({'success': False, 'error': str(e)})


# TON wallet address for receiving payments
TON_RECEIVE_WALLET = 'UQCHqlS8KSD3ZOF-OwV5efg2ST60u2JTnGEXeViQLU9g5v3i'
TON_CENTER_API = 'https://toncenter.com/api/v2'

@app.route('/api/ton-payment-callback', methods=['POST'])
def ton_payment_callback():
    """Handle TonConnect payment callback"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        amount_ton = data.get('amount_ton')
        stars = data.get('stars')
        boc = data.get('boc')  # Transaction BOC from TonConnect
        
        if not user_id or not amount_ton:
            return jsonify({'success': False, 'error': 'Missing data'})
        
        amount_ton = round(float(amount_ton), 6)
        stars_to_add = int(stars) if stars else int(round(amount_ton * TON_RATE))
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check for duplicate (use BOC hash as unique identifier)
        boc_hash = hashlib.md5((boc or str(time.time())).encode()).hexdigest()[:16]
        cursor.execute('''
            SELECT id FROM user_history 
            WHERE description LIKE ? AND operation_type = 'ton_payment'
        ''', (f'%{boc_hash}%',))
        if cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'error': 'Transaction already processed'})
        
        # Add stars to user
        cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (stars_to_add, user_id))
        
        # Record in history
        cursor.execute('''
            INSERT INTO user_history (user_id, operation_type, amount, description, created_at)
            VALUES (?, 'ton_payment', ?, ?, datetime('now'))
        ''', (user_id, stars_to_add, f'TON Connect: {amount_ton} TON = {stars_to_add} Р·РІС‘Р·Рґ (boc: {boc_hash})'))
        
        # Get new balance
        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        new_balance = row[0] if row else 0
        
        conn.commit()
        conn.close()
        
        logger.info(f"рџ’Ћ TonConnect РїР»Р°С‚С‘Р¶: РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РїРѕР»СѓС‡РёР» {stars_to_add} Р·РІС‘Р·Рґ Р·Р° {amount_ton} TON")
        return jsonify({
            'success': True,
            'stars_added': stars_to_add,
            'new_balance': new_balance
        })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° TonConnect: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/ton-check-payment', methods=['POST'])
def ton_check_payment():
    """Check if TON payment was received and credit stars"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        amount_ton = data.get('amount_ton')
        comment = data.get('comment')  # Should be user_id
        
        if not user_id or not amount_ton:
            return jsonify({'success': False, 'error': 'Missing data'})
        
        amount_ton = round(float(amount_ton), 6)
        stars_to_add = int(round(amount_ton * TON_RATE))
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if this payment was already processed
        cursor.execute('''
            SELECT id FROM ton_payments 
            WHERE user_id = ? AND ABS(ton_amount - ?) < 0.000001 AND status = 'confirmed'
            AND created_at > datetime('now', '-1 hour')
        ''', (user_id, amount_ton))
        if cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'error': 'РџР»Р°С‚С‘Р¶ СѓР¶Рµ РѕР±СЂР°Р±РѕС‚Р°РЅ'})
        
        # Try to verify via TON Center API
        verified = False
        tx_hash = None
        
        try:
            # Get recent transactions to our wallet
            api_url = f"{TON_CENTER_API}/getTransactions?address={TON_RECEIVE_WALLET}&limit=20"
            response = http_requests.get(api_url, timeout=10)
            
            if response.status_code == 200:
                tx_data = response.json()
                transactions = tx_data.get('result', [])
                
                for tx in transactions:
                    # Check if transaction matches
                    in_msg = tx.get('in_msg', {})
                    msg_value = int(in_msg.get('value', 0)) / 1e9  # Convert from nanoTON
                    msg_comment = in_msg.get('message', '')
                    
                    # Check amount (with small tolerance) and comment
                    if abs(msg_value - amount_ton) < 0.01 and str(comment) in str(msg_comment):
                        tx_hash = tx.get('transaction_id', {}).get('hash', '')
                        
                        # Check if this tx was already used
                        cursor.execute('SELECT id FROM ton_payments WHERE tx_hash = ?', (tx_hash,))
                        if not cursor.fetchone():
                            verified = True
                            break
        except Exception as api_err:
            logger.warning(f"TON API check failed: {api_err}")
        
        if verified:
            # Credit stars to user
            cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (stars_to_add, user_id))
            
            # Record payment
            cursor.execute('''
                INSERT INTO ton_payments (user_id, ton_amount, ton_amount, tx_hash, status, confirmed_at)
                VALUES (?, ?, ?, ?, 'confirmed', datetime('now'))
            ''', (user_id, amount_ton, stars_to_add, tx_hash))
            
            # Get new balance
            cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
            row = cursor.fetchone()
            new_balance = row[0] if row else 0
            
            conn.commit()
            conn.close()
            
            logger.info(f"рџ’Ћ TON РїР»Р°С‚С‘Р¶ РїРѕРґС‚РІРµСЂР¶РґС‘РЅ: {user_id} +{stars_to_add} Р·РІС‘Р·Рґ Р·Р° {amount_ton} TON")
            return jsonify({
                'success': True,
                'verified': True,
                'stars_added': stars_to_add,
                'new_balance': new_balance
            })
        else:
            # Payment not found yet - create pending record for admin verification
            cursor.execute('''
                INSERT OR IGNORE INTO ton_payments (user_id, ton_amount, ton_amount, status)
                VALUES (?, ?, ?, 'pending')
            ''', (user_id, amount_ton, stars_to_add))
            conn.commit()
            conn.close()
            
            return jsonify({
                'success': True,
                'verified': False,
                'pending': True,
                'message': 'РџР»Р°С‚С‘Р¶ РЅРµ РЅР°Р№РґРµРЅ. РђРґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂ РїСЂРѕРІРµСЂРёС‚ РІСЂСѓС‡РЅСѓСЋ.'
            })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё TON РїР»Р°С‚РµР¶Р°: {e}")
        return jsonify({'success': False, 'error': str(e)})


# ============================================================
# STARS PAYMENT (Telegram Stars Invoice)
# ============================================================

@app.route('/api/stars/create-invoice', methods=['POST'])
def stars_create_invoice():
    """Create a Telegram Stars invoice link for deposit"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        amount = data.get('amount')

        if not user_id or not amount:
            return jsonify({'success': False, 'error': 'Missing data'})

        amount = int(amount)
        if amount < 10 or amount > 10000:
            return jsonify({'success': False, 'error': 'РЎСѓРјРјР° РѕС‚ 10 РґРѕ 10000'})

        # Create invoice link via Telegram Bot API
        result = tg_api('createInvoiceLink',
            title=f'РџРѕРїРѕР»РЅРµРЅРёРµ {amount} Stars',
            description=f'РџРѕРїРѕР»РЅРµРЅРёРµ Р±Р°Р»Р°РЅСЃР° РЅР° {amount} Р·РІС‘Р·Рґ',
            payload=f'stars_deposit:{user_id}:{amount}',
            provider_token='',
            currency='XTR',
            prices=[{'label': f'{amount} Stars', 'amount': amount}]
        )

        logger.info(f"Stars createInvoiceLink response: {result}")

        if not result.get('ok'):
            err_desc = result.get('description', 'РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕР·РґР°С‚СЊ СЃС‡С‘С‚')
            logger.error(f"Stars invoice create error: {result}")
            return jsonify({'success': False, 'error': err_desc})

        invoice_url = result.get('result', '')
        logger.info(f"Stars invoice created: user={user_id}, amount={amount}")
        return jsonify({'success': True, 'invoice_url': invoice_url})

    except Exception as e:
        logger.error(f"Stars invoice error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/ton-wallet-address', methods=['GET'])
def get_ton_wallet_address():
    """Get the TON wallet address for deposits"""
    return jsonify({
        'success': True,
        'address': TON_RECEIVE_WALLET,
        'rate': 100  # Stars per TON
    })


# ============================================================
# SBP PAYMENT вЂ” handled below in unified SBP section (line ~12445)
# ============================================================


@app.route('/tonconnect-manifest.json', methods=['GET', 'OPTIONS'])
def tonconnect_manifest():
    """Dynamic TonConnect manifest with correct URLs (CORS enabled for wallets)"""
    if request.method == 'OPTIONS':
        resp = app.make_default_options_response()
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
        resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return resp
    base_url = request.url_root.rstrip('/')
    resp = jsonify({
        'url': base_url,
        'name': 'Rasswet Gifts',
        'iconUrl': f'{base_url}/static/img/star.png',
        'termsOfUseUrl': f'{base_url}/terms',
        'privacyPolicyUrl': f'{base_url}/privacy'
    })
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Cache-Control'] = 'public, max-age=3600'
    return resp


@app.route('/api/skin-reward', methods=['POST'])
def claim_skin_reward():
    """РџРѕР»СѓС‡РµРЅРёРµ РЅР°РіСЂР°РґС‹ Р·Р° СЃР±РѕСЂ РІСЃРµС… СЃРєРёРЅРѕРІ СЂР°РєРµС‚ (2000 Р·РІС‘Р·Рґ)"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        
        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if already claimed
        cursor.execute('''
            SELECT id FROM user_history 
            WHERE user_id = ? AND operation_type = 'skin_reward'
        ''', (user_id,))
        if cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'error': 'РќР°РіСЂР°РґР° СѓР¶Рµ РїРѕР»СѓС‡РµРЅР°'})
        
        # Give reward: 2000 stars
        cursor.execute('UPDATE users SET balance_stars = balance_stars + 2000 WHERE id = ?', (user_id,))
        
        # Record in history
        cursor.execute('''
            INSERT INTO user_history (user_id, operation_type, amount, description, created_at)
            VALUES (?, 'skin_reward', 2000, 'РќР°РіСЂР°РґР° Р·Р° РІСЃРµ СЃРєРёРЅС‹ СЂР°РєРµС‚: +2000 Р·РІС‘Р·Рґ', datetime('now'))
        ''', (user_id,))
        
        # Get new balance
        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        new_balance = row[0] if row else 0
        
        conn.commit()
        conn.close()
        
        logger.info(f"рџљЂ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РїРѕР»СѓС‡РёР» РЅР°РіСЂР°РґСѓ Р·Р° РІСЃРµ СЃРєРёРЅС‹!")
        return jsonify({
            'success': True,
            'new_balance': new_balance
        })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РІС‹РґР°С‡Рё РЅР°РіСЂР°РґС‹ Р·Р° СЃРєРёРЅС‹: {e}")
        return jsonify({'success': False, 'error': str(e)})

# CASES API
@app.route('/api/cases')
def api_cases():
    """РџРѕР»СѓС‡РµРЅРёРµ РІСЃРµС… РєРµР№СЃРѕРІ СЃ Р°РєС‚СѓР°Р»СЊРЅС‹РјРё Р»РёРјРёС‚Р°РјРё"""
    try:
        logger.info("рџ“¦ Р—Р°РіСЂСѓР·РєР° РєРµР№СЃРѕРІ РёР· С„Р°Р№Р»Р°...")

        data_path = os.path.join(BASE_PATH, 'data')
        file_path = os.path.join(data_path, 'cases.json')

        logger.info(f"рџ“Ѓ РџСѓС‚СЊ Рє С„Р°Р№Р»Сѓ: {file_path}")

        if not os.path.exists(file_path):
            logger.error(f"вќЊ Р¤Р°Р№Р» cases.json РЅРµ РЅР°Р№РґРµРЅ")
            return jsonify({'success': True, 'cases': []})

        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            cases = data.get('cases', [])

        logger.info(f"вњ… Р—Р°РіСЂСѓР¶РµРЅРѕ {len(cases)} РєРµР№СЃРѕРІ")
        return jsonify({'success': True, 'cases': cases})

    except Exception as e:
        logger.error(f"вќЊ РљСЂРёС‚РёС‡РµСЃРєР°СЏ РѕС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РєРµР№СЃРѕРІ: {e}")
        logger.error(f"вќЊ РўСЂР°СЃСЃРёСЂРѕРІРєР°: {traceback.format_exc()}")
        return jsonify({
            'success': False,
            'cases': [],
            'error': 'Р’РЅСѓС‚СЂРµРЅРЅСЏСЏ РѕС€РёР±РєР° СЃРµСЂРІРµСЂР°'
        })

@app.route('/api/case-sections')
def api_case_sections():
    """РџСѓР±Р»РёС‡РЅС‹Р№ СЃРїРёСЃРѕРє СЂР°Р·РґРµР»РѕРІ РєРµР№СЃРѕРІ РґР»СЏ РіР»Р°РІРЅРѕР№"""
    try:
        sections = load_case_sections()
        return jsonify({'success': True, 'sections': sections})
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЂР°Р·РґРµР»РѕРІ РєРµР№СЃРѕРІ: {e}")
        return jsonify({'success': False, 'sections': [], 'error': str(e)})

@app.route('/api/cases/<int:case_id>')
def api_case_detail(case_id):
    """РџРѕР»СѓС‡РµРЅРёРµ РґРµС‚Р°Р»РµР№ РєРѕРЅРєСЂРµС‚РЅРѕРіРѕ РєРµР№СЃР°"""
    try:
        cases = load_cases()
        local_gifts = load_gifts()
        # Also get Fragment catalog (originals + models) for resolving all gift types
        fragment_all = build_full_catalog_with_models()

        case = next((c for c in cases if c['id'] == case_id), None)
        if not case:
            logger.error(f"вќЊ РљРµР№СЃ СЃ ID {case_id} РЅРµ РЅР°Р№РґРµРЅ!")
            return jsonify({'success': False, 'error': 'РљРµР№СЃ РЅРµ РЅР°Р№РґРµРЅ'})

        if case.get('limited'):
            current_limit = get_case_limit(case_id)
            logger.info(f"рџ“Љ Р”РµС‚Р°Р»Рё РєРµР№СЃР° {case_id} - Р»РёРјРёС‚: {current_limit}")
            if current_limit is not None:
                case['current_amount'] = current_limit
            else:
                case['current_amount'] = case['amount']
        else:
            case['current_amount'] = None

        case_gifts = []
        for gift_info in case['gifts']:
            # РћР±СЂР°Р±РѕС‚РєР° ton_balance
            if gift_info.get('type') == 'ton_balance':
                ton_amount = gift_info.get('ton_amount', 0)
                case_gifts.append({
                    'id': -1,
                    'name': 'TON',
                    'image': '/static/img/tons/ton_1.svg',
                    'value': ton_amount,
                    'type': 'ton_balance',
                    'ton_amount': ton_amount,
                    'chance': gift_info.get('chance', 1)
                })
            else:
                target_id = gift_info.get('id')
                target_id_str = str(target_id) if target_id is not None else ''
                # Try local gifts first (by numeric ID)
                gift = next((g for g in local_gifts if str(g.get('id')) == target_id_str), None) if target_id is not None else None
                # Then try Fragment catalog (by string ID like fragment_model:slug:model)
                if not gift and target_id_str:
                    gift = next((g for g in fragment_all if str(g.get('id')) == target_id_str or str(g.get('gift_key')) == target_id_str), None)
                # Fallback: construct from gift_info fields
                if not gift and gift_info.get('name'):
                    gift = {
                        'id': target_id or -1,
                        'name': gift_info.get('name', ''),
                        'image': gift_info.get('image', '/static/img/default_gift.png'),
                        'value': _safe_int(gift_info.get('value'), 0),
                        'type': gift_info.get('type', 'gift'),
                        'fragment_slug': gift_info.get('fragment_slug', ''),
                        'model_name': gift_info.get('model_name', ''),
                    }
                if gift:
                    case_gifts.append({
                        **gift,
                        'chance': gift_info.get('chance', 1)
                    })
                else:
                    logger.warning(f"вљ пёЏ РџРѕРґР°СЂРѕРє СЃ ID {gift_info.get('id')} РЅРµ РЅР°Р№РґРµРЅ РґР»СЏ РєРµР№СЃР° {case_id}")

        case_with_gifts = {**case, 'gifts_details': case_gifts}

        logger.info(f"рџ“¦ РћС‚РїСЂР°РІР»РµРЅС‹ РґРµС‚Р°Р»Рё РєРµР№СЃР° {case_id}")
        return jsonify({'success': True, 'case': case_with_gifts})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РґРµС‚Р°Р»РµР№ РєРµР№СЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/cases/open', methods=['POST'])
def open_case():
    """РћС‚РєСЂС‹С‚РёРµ РєРµР№СЃР°"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        case_id = data['case_id']
        quantity = data.get('quantity', 1)

        cases = load_cases()
        case = next((c for c in cases if c['id'] == case_id), None)

        if not case:
            return jsonify({'success': False, 'error': 'РљРµР№СЃ РЅРµ РЅР°Р№РґРµРЅ'})

        if case.get('limited'):
            current_limit = get_case_limit(case_id)
            if current_limit is not None and current_limit <= 0:
                return jsonify({'success': False, 'error': 'Р›РёРјРёС‚ РєРµР№СЃР° РёСЃС‡РµСЂРїР°РЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT balance_stars, balance_tickets, current_level FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        balance_stars, balance_tickets, user_level = user

        required_level = case.get('required_level', 1)
        if user_level < required_level:
            conn.close()
            return jsonify({'success': False, 'error': f'РўСЂРµР±СѓРµС‚СЃСЏ {required_level} СѓСЂРѕРІРµРЅСЊ'})

        if case.get('free') and int(quantity or 1) > 1:
            conn.close()
            return jsonify({'success': False, 'error': 'Р‘РµСЃРїР»Р°С‚РЅС‹Р№ РєРµР№СЃ РјРѕР¶РЅРѕ РѕС‚РєСЂС‹С‚СЊ С‚РѕР»СЊРєРѕ 1 СЂР°Р· Р·Р° РїРѕРїС‹С‚РєСѓ'})

        if case.get('free'):
            remaining = _get_free_case_remaining_seconds(cursor, user_id, case)
            if remaining > 0:
                conn.close()
                return jsonify({
                    'success': False,
                    'error': 'Р­С‚РѕС‚ РєРµР№СЃ РјРѕР¶РЅРѕ РѕС‚РєСЂС‹РІР°С‚СЊ СЂР°Р· РІ 24 С‡Р°СЃР°',
                    'cooldown_seconds': remaining
                })

        total_cost = case['cost'] * quantity
        if case['cost'] > 0:
            if case['cost_type'] == 'stars' and balance_stars < total_cost:
                conn.close()
                return jsonify({'success': False, 'error': 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ Р·РІРµР·Рґ'})
            elif case['cost_type'] == 'tickets' and balance_tickets < total_cost:
                conn.close()
                return jsonify({'success': False, 'error': 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ Р±РёР»РµС‚РѕРІ'})

        if case['cost'] > 0:
            if case['cost_type'] == 'stars':
                cursor.execute('UPDATE users SET balance_stars = balance_stars - ? WHERE id = ?',
                             (total_cost, user_id))
            else:
                cursor.execute('UPDATE users SET balance_tickets = balance_tickets - ? WHERE id = ?',
                             (total_cost, user_id))

        won_gifts = []
        gifts = build_fragment_first_gifts_catalog() or load_gifts()

        for _ in range(quantity):
            if case.get('gifts'):
                total_chance = sum(gift.get('chance', 1) for gift in case['gifts'])
                random_value = random.random() * total_chance
                current_chance = 0
                selected_gift_info = None

                for gift_info in case['gifts']:
                    current_chance += gift_info.get('chance', 1)
                    if random_value <= current_chance:
                        selected_gift_info = gift_info
                        break

                if selected_gift_info:
                    # РџСЂРѕРІРµСЂРєР° РЅР° ton_balance
                    if selected_gift_info.get('type') == 'ton_balance':
                        ton_amount = selected_gift_info.get('ton_amount', 0)
                        cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?',
                                     (ton_amount, user_id))
                        won_gift = {
                            'id': -1,
                            'name': 'TON',
                            'image': '/static/img/tons/ton_1.svg',
                            'value': ton_amount,
                            'type': 'ton_balance',
                            'ton_amount': ton_amount
                        }
                        won_gifts.append(won_gift)

                        cursor.execute('''
                            INSERT INTO win_history (user_id, user_name, gift_name, gift_image, gift_value, case_name)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (user_id, f"User_{user_id}", won_gift['name'], won_gift['image'], won_gift.get('value', 0), case['name']))
                    else:
                        gift = _resolve_case_gift_payload(gifts, selected_gift_info)
                        if gift:
                            won_gifts.append(gift)

                            inv_gift_id = gift.get('id') if isinstance(gift.get('id'), int) else None

                            cursor.execute('''
                                INSERT INTO inventory (user_id, gift_id, gift_name, gift_image, gift_value)
                                VALUES (?, ?, ?, ?, ?)
                            ''', (user_id, inv_gift_id, gift['name'], gift['image'], gift.get('value', 0)))

                            cursor.execute('''
                                INSERT INTO win_history (user_id, user_name, gift_name, gift_image, gift_value, case_name)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (user_id, f"User_{user_id}", gift['name'], gift['image'], gift.get('value', 0), case['name']))
            else:
                if gifts:
                    gift = random.choice(gifts)
                    won_gifts.append(gift)

                    cursor.execute('''
                        INSERT INTO inventory (user_id, gift_id, gift_name, gift_image, gift_value)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (user_id, gift['id'], gift['name'], gift['image'], gift.get('value', 0)))

        if case.get('limited'):
            try:
                max_amount = int(case.get('amount', 0) or 0)
            except Exception:
                max_amount = 0
            cursor.execute('SELECT current_amount FROM case_limits WHERE case_id = ?', (case_id,))
            row = cursor.fetchone()
            if row is None:
                current_amount = max_amount
            else:
                try:
                    current_amount = int(row[0] or 0)
                except Exception:
                    current_amount = 0
            new_amount = max(0, current_amount - int(quantity or 1))
            cursor.execute('''
                INSERT OR REPLACE INTO case_limits (case_id, current_amount, last_updated)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (case_id, new_amount))

        exp_gained = total_cost  # 1:1 turnover - case cost = experience
        cursor.execute('UPDATE users SET experience = experience + ? WHERE id = ?',
                     (exp_gained, user_id))

        cursor.execute('UPDATE users SET total_cases_opened = total_cases_opened + ? WHERE id = ?',
                     (quantity, user_id))

        for gift in won_gifts:
            cursor.execute('''
                INSERT INTO case_open_history (user_id, case_id, case_name, gift_id, gift_name, gift_image, gift_value, cost, cost_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, case_id, case['name'], gift.get('id', -1), gift['name'], gift['image'], gift.get('value', 0), case['cost'], case['cost_type']))

        conn.commit()

        cursor.execute('SELECT balance_stars, balance_tickets FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()

        conn.close()

        # Update daily tasks progress
        try:
            update_daily_task_progress(user_id, 'open_case', quantity, case_id)
            update_daily_task_progress(user_id, 'turnover', case['cost'] * quantity)
            update_daily_task_progress(user_id, 'earn_exp', exp_gained)
        except:
            pass

        return jsonify({
            'success': True,
            'won_gifts': won_gifts,
            'new_balance': {
                'stars': new_balance[0],
                'tickets': new_balance[1]
            },
            'exp_gained': exp_gained
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РѕС‚РєСЂС‹С‚РёСЏ РєРµР№СЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/open-case', methods=['POST'])
def open_case_single():
    """РћС‚РєСЂС‹С‚РёРµ РѕРґРЅРѕРіРѕ РєРµР№СЃР° (РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РёР· case.html)"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        case_id = data['case_id']
        is_promo = data.get('is_promo', False)
        is_free = data.get('is_free', False)
        promo_code = data.get('promo_code', '').strip().upper()

        cases = load_cases()
        case = next((c for c in cases if c['id'] == case_id), None)

        if not case:
            return jsonify({'success': False, 'error': 'РљРµР№СЃ РЅРµ РЅР°Р№РґРµРЅ'})

        # РџСЂРѕРІРµСЂРєР° РїСЂРѕРјРѕРєРѕРґР° РґР»СЏ promo-РєРµР№СЃРѕРІ
        if case.get('promo') and is_promo:
            if not promo_code:
                return jsonify({'success': False, 'error': 'РўСЂРµР±СѓРµС‚СЃСЏ РїСЂРѕРјРѕРєРѕРґ'})
            case_data, promo_item = _find_embedded_case_promo(case_id, promo_code)
            if not promo_item:
                return jsonify({'success': False, 'error': 'РќРµРІРµСЂРЅС‹Р№ РїСЂРѕРјРѕРєРѕРґ'})
            # РџСЂРѕРІРµСЂСЏРµРј РёСЃРїРѕР»СЊР·РѕРІР°РЅРёРµ РїСЂРѕРјРѕРєРѕРґР°
            conn_check = get_db_connection()
            cursor_check = conn_check.cursor()
            cursor_check.execute('''SELECT id FROM used_promo_codes 
                WHERE user_id = ? AND promo_code_id = ?''', 
                (user_id, promo_item.get('id', promo_code)))
            already_used = cursor_check.fetchone()
            conn_check.close()
            if already_used:
                return jsonify({'success': False, 'error': 'РџСЂРѕРјРѕРєРѕРґ СѓР¶Рµ РёСЃРїРѕР»СЊР·РѕРІР°РЅ'})
        elif case.get('promo') and not is_promo:
            return jsonify({'success': False, 'error': 'РўСЂРµР±СѓРµС‚СЃСЏ РїСЂРѕРјРѕРєРѕРґ РґР»СЏ РѕС‚РєСЂС‹С‚РёСЏ'})

        if case.get('limited'):
            current_limit = get_case_limit(case_id)
            if current_limit is not None and current_limit <= 0:
                return jsonify({'success': False, 'error': 'Р›РёРјРёС‚ РєРµР№СЃР° РёСЃС‡РµСЂРїР°РЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT balance_stars, balance_tickets, current_level FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        balance_stars, balance_tickets, user_level = user

        required_level = case.get('required_level', 1)
        if user_level < required_level:
            conn.close()
            return jsonify({'success': False, 'error': f'РўСЂРµР±СѓРµС‚СЃСЏ {required_level} СѓСЂРѕРІРµРЅСЊ'})

        # РџСЂРѕРІРµСЂРєР° РґР»СЏ Р±РµСЃРїР»Р°С‚РЅС‹С… РєРµР№СЃРѕРІ (СЃРµСЂРІРµСЂРЅР°СЏ, РЅРµ Р·Р°РІРёСЃРёС‚ РѕС‚ С„Р»Р°РіР° СЃ РєР»РёРµРЅС‚Р°)
        if case.get('free'):
            remaining = _get_free_case_remaining_seconds(cursor, user_id, case)
            if remaining > 0:
                conn.close()
                return jsonify({
                    'success': False,
                    'error': 'РљРµР№СЃ СѓР¶Рµ РѕС‚РєСЂС‹С‚, РїРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ',
                    'cooldown_seconds': remaining
                })

        # РЎРїРёСЃР°РЅРёРµ СЃС‚РѕРёРјРѕСЃС‚Рё
        if case['cost'] > 0 and not is_free and not is_promo:
            if case['cost_type'] == 'stars' and balance_stars < case['cost']:
                conn.close()
                return jsonify({'success': False, 'error': 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ Р·РІРµР·Рґ'})
            elif case['cost_type'] == 'tickets' and balance_tickets < case['cost']:
                conn.close()
                return jsonify({'success': False, 'error': 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ Р±РёР»РµС‚РѕРІ'})

            if case['cost_type'] == 'stars':
                cursor.execute('UPDATE users SET balance_stars = balance_stars - ? WHERE id = ?',
                             (case['cost'], user_id))
            else:
                cursor.execute('UPDATE users SET balance_tickets = balance_tickets - ? WHERE id = ?',
                             (case['cost'], user_id))

        # Р’С‹Р±РѕСЂ РїРѕРґР°СЂРєР°
        gifts = build_fragment_first_gifts_catalog() or load_gifts()
        won_gift = None
        is_ton_balance = False

        if case.get('gifts'):
            total_chance = sum(gift.get('chance', 1) for gift in case['gifts'])
            random_value = random.random() * total_chance
            current_chance = 0
            selected_gift_info = None

            for gift_info in case['gifts']:
                current_chance += gift_info.get('chance', 1)
                if random_value <= current_chance:
                    selected_gift_info = gift_info
                    break

            if selected_gift_info:
                # РџСЂРѕРІРµСЂРєР° РЅР° ton_balance
                if selected_gift_info.get('type') == 'ton_balance':
                    ton_amount = selected_gift_info.get('ton_amount', 0)
                    cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?',
                                 (ton_amount, user_id))
                    won_gift = {
                        'id': -1,
                        'name': 'TON',
                        'image': '/static/img/tons/ton_1.svg',
                        'value': ton_amount,
                        'type': 'ton_balance',
                        'ton_amount': ton_amount
                    }
                    is_ton_balance = True
                else:
                    gift = _resolve_case_gift_payload(gifts, selected_gift_info)
                    if gift:
                        won_gift = gift

        if not won_gift and gifts:
            won_gift = random.choice(gifts)

        if won_gift and not is_ton_balance:
            inv_gift_id = won_gift.get('id') if isinstance(won_gift.get('id'), int) else None
            cursor.execute('''
                INSERT INTO inventory (user_id, gift_id, gift_name, gift_image, gift_value)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, inv_gift_id, won_gift['name'], won_gift['image'], won_gift.get('value', 0)))
            won_gift['inventory_id'] = cursor.lastrowid

        if won_gift:
            cursor.execute('''
                INSERT INTO win_history (user_id, user_name, gift_name, gift_image, gift_value, case_name)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, f"User_{user_id}", won_gift['name'], won_gift['image'], won_gift.get('value', 0), case['name']))

            cursor.execute('''
                INSERT INTO case_open_history (user_id, case_id, case_name, gift_id, gift_name, gift_image, gift_value, cost, cost_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, case_id, case['name'], won_gift.get('id', -1), won_gift['name'], won_gift['image'], won_gift.get('value', 0), case['cost'], case['cost_type']))

        # РћР±РЅРѕРІР»РµРЅРёРµ Р»РёРјРёС‚Р° (РІ СЌС‚РѕР№ Р¶Рµ С‚СЂР°РЅР·Р°РєС†РёРё, Р±РµР· РѕС‚РґРµР»СЊРЅРѕРіРѕ РїРѕРґРєР»СЋС‡РµРЅРёСЏ)
        new_case_limit = None
        if case.get('limited'):
            try:
                max_amount = int(case.get('amount', 0) or 0)
            except Exception:
                max_amount = 0
            cursor.execute('SELECT current_amount FROM case_limits WHERE case_id = ?', (case_id,))
            row = cursor.fetchone()
            if row is None:
                current_amount = max_amount
            else:
                try:
                    current_amount = int(row[0] or 0)
                except Exception:
                    current_amount = 0
            new_case_limit = max(0, current_amount - 1)
            cursor.execute('''
                INSERT OR REPLACE INTO case_limits (case_id, current_amount, last_updated)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (case_id, new_case_limit))

        # РћРїС‹С‚ - 1:1 turnover
        exp_gained = case['cost']
        cursor.execute('UPDATE users SET total_cases_opened = total_cases_opened + 1 WHERE id = ?',
                     (user_id,))

        conn.commit()
        conn.close()

        # Р—Р°РїРёСЃС‹РІР°РµРј РёСЃРїРѕР»СЊР·РѕРІР°РЅРёРµ РїСЂРѕРјРѕРєРѕРґР°
        if case.get('promo') and is_promo and promo_code:
            try:
                pconn = get_db_connection()
                pconn.execute('''INSERT OR IGNORE INTO used_promo_codes 
                    (user_id, promo_code_id, used_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)''', 
                    (user_id, promo_code))
                pconn.commit()
                pconn.close()
            except:
                pass

        # Р”РѕР±Р°РІР»СЏРµРј РѕРїС‹С‚ С‡РµСЂРµР· СЃРёСЃС‚РµРјСѓ СѓСЂРѕРІРЅРµР№ (РјРѕР¶РµС‚ РїРѕРІС‹СЃРёС‚СЊ СѓСЂРѕРІРµРЅСЊ)
        level_result = add_experience(user_id, exp_gained, f'case_open:{case_id}')

        # РћР±РЅРѕРІР»СЏРµРј gift_index
        if won_gift and won_gift.get('type') != 'ton_balance':
            try:
                gconn = get_db_connection()
                gcur = gconn.cursor()
                gcur.execute('SELECT 1 FROM user_gift_index WHERE user_id = ? AND gift_name = ?', (user_id, won_gift['name']))
                is_new = not gcur.fetchone()
                gconn.execute('INSERT OR IGNORE INTO user_gift_index (user_id, gift_name) VALUES (?, ?)',
                             (user_id, won_gift['name']))
                # РђРІС‚Рѕ-СѓРІРµРґРѕРјР»РµРЅРёРµ Рѕ СЂР°СЃС€РёСЂРµРЅРёРё РёРЅРґРµРєСЃР°
                if is_new:
                    try:
                        gconn.execute('''INSERT INTO admin_notifications 
                            (title, message, image_url, notif_type, target_user_id)
                            VALUES (?, ?, ?, 'gift_index', ?)''',
                            (won_gift['name'], f'РќРѕРІС‹Р№ РїРѕРґР°СЂРѕРє РІ РІР°С€РµР№ РєРѕР»Р»РµРєС†РёРё!', 
                             won_gift.get('image', ''), user_id))
                    except:
                        pass
                gconn.commit()
                gconn.close()
            except:
                pass

        # Update daily tasks progress
        try:
            update_daily_task_progress(user_id, 'open_case', 1, case_id)
            update_daily_task_progress(user_id, 'turnover', case['cost'])
            update_daily_task_progress(user_id, 'earn_exp', exp_gained)
        except:
            pass

        # РџСЂРѕРІРµСЂСЏРµРј Р±РѕРЅСѓСЃС‹ Р·Р° СѓСЂРѕРІРµРЅСЊ
        level_up_info = None
        if level_result and level_result.get('level_up_info'):
            level_up_info = level_result['level_up_info']
            # Р’С‹РґР°С‘Рј Р±РѕРЅСѓСЃС‹ Р·Р° СѓСЂРѕРІРµРЅСЊ РёР· level_rewards
            try:
                _grant_level_rewards(user_id, level_result['new_level'])
            except:
                pass

        result = {
            'success': True,
            'gift': won_gift,
            'exp_gained': exp_gained,
            'level_up': level_up_info
        }

        if new_case_limit is not None:
            result['case_limits_updated'] = True
            result['new_case_limit'] = new_case_limit

        return jsonify(result)

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РѕС‚РєСЂС‹С‚РёСЏ РєРµР№СЃР°: {e}")
        logger.error(f"вќЊ РўСЂР°СЃСЃРёСЂРѕРІРєР°: {traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/activate-promo', methods=['POST'])
def activate_promo_for_case():
    """РђРєС‚РёРІР°С†РёСЏ РїСЂРѕРјРѕРєРѕРґР° - СЂР°СЃС€РёСЂРµРЅРЅР°СЏ РІРµСЂСЃРёСЏ СЃ С‚РёРїР°РјРё РЅР°РіСЂР°Рґ"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        case_id = data.get('case_id')
        promo_code = data['promo_code'].upper().strip()

        logger.info(f"рџЋџпёЏ РђРєС‚РёРІР°С†РёСЏ РїСЂРѕРјРѕРєРѕРґР° {promo_code} РѕС‚ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {user_id}")

        conn = get_db_connection()
        cursor = conn.cursor()

        # РџСЂРѕРІРµСЂСЏРµРј РїСЂРѕРјРѕРєРѕРґ РІ С‚Р°Р±Р»РёС†Рµ promo_codes
        cursor.execute('''
            SELECT id, reward_stars, reward_tickets, reward_type, reward_data, max_uses, used_count, expires_at, is_active
            FROM promo_codes
            WHERE code = ?
        ''', (promo_code,))

        promo = cursor.fetchone()

        if not promo:
            # Fallback: РїСЂРѕРјРѕРєРѕРґС‹, Р·Р°С€РёС‚С‹Рµ РІ cases.json РґР»СЏ PROMO РєРµР№СЃРѕРІ
            case_data, embedded = _find_embedded_case_promo(case_id, promo_code)
            if case_data and embedded:
                # РЎРµСЂРІРµСЂРЅР°СЏ РїСЂРѕРІРµСЂРєР° eligibility РґР»СЏ РїСЂРѕРјРѕ-РєРµР№СЃР°
                cursor.execute('''SELECT COALESCE(SUM(amount), 0) FROM deposits
                    WHERE user_id = ? AND created_at > datetime('now', '-1 day') AND status = 'completed' ''', (user_id,))
                total_deposits = cursor.fetchone()[0] or 0
                if total_deposits < 500:
                    conn.close()
                    return jsonify({'success': False, 'error': 'Р”Р»СЏ Р°РєС‚РёРІР°С†РёРё РЅСѓР¶РµРЅ РґРµРїРѕР·РёС‚ 500+в­ђ Р·Р° 24 С‡Р°СЃР°'})

                cursor.execute('''CREATE TABLE IF NOT EXISTS case_promo_uses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    case_id INTEGER NOT NULL,
                    promo_code TEXT NOT NULL,
                    used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, case_id, promo_code)
                )''')

                max_per_user = int(embedded.get('max_uses_per_user', 1) or 1)
                uses_left = int(embedded.get('uses_left', 0) or 0)
                expires_at = embedded.get('expires_at')

                # РЎСЂРѕРє РґРµР№СЃС‚РІРёСЏ
                if expires_at:
                    exp_dt = _parse_datetime_flexible(expires_at)
                    if exp_dt and datetime.now() > exp_dt:
                        conn.close()
                        return jsonify({'success': False, 'error': 'РЎСЂРѕРє РґРµР№СЃС‚РІРёСЏ РїСЂРѕРјРѕРєРѕРґР° РёСЃС‚РµРє'})

                # Р›РёРјРёС‚ РЅР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
                cursor.execute('''
                    SELECT COUNT(*) FROM case_promo_uses
                    WHERE user_id = ? AND case_id = ? AND promo_code = ?
                ''', (user_id, int(case_data.get('id')), promo_code))
                user_used = int(cursor.fetchone()[0] or 0)
                if max_per_user > 0 and user_used >= max_per_user:
                    conn.close()
                    return jsonify({'success': False, 'error': 'Р’С‹ СѓР¶Рµ РёСЃРїРѕР»СЊР·РѕРІР°Р»Рё СЌС‚РѕС‚ РїСЂРѕРјРѕРєРѕРґ'})

                # РћР±С‰РёР№ Р»РёРјРёС‚ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёР№
                if uses_left > 0:
                    cursor.execute('''
                        SELECT COUNT(*) FROM case_promo_uses
                        WHERE case_id = ? AND promo_code = ?
                    ''', (int(case_data.get('id')), promo_code))
                    total_used = int(cursor.fetchone()[0] or 0)
                    if total_used >= uses_left:
                        conn.close()
                        return jsonify({'success': False, 'error': 'Р›РёРјРёС‚ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёР№ РїСЂРѕРјРѕРєРѕРґР° РёСЃС‡РµСЂРїР°РЅ'})

                cursor.execute('''
                    INSERT INTO case_promo_uses (user_id, case_id, promo_code)
                    VALUES (?, ?, ?)
                ''', (user_id, int(case_data.get('id')), promo_code))

                conn.commit()
                conn.close()
                return jsonify({
                    'success': True,
                    'reward_type': 'case_open',
                    'message': 'РџСЂРѕРјРѕРєРѕРґ Р°РєС‚РёРІРёСЂРѕРІР°РЅ! РљРµР№СЃ СЂР°Р·Р±Р»РѕРєРёСЂРѕРІР°РЅ!',
                    'case_unlocked': True,
                    'reward_amount': 'РљРµР№СЃ',
                    'reward_icon': '/static/img/gift.png'
                })

            conn.close()
            return jsonify({'success': False, 'error': 'РџСЂРѕРјРѕРєРѕРґ РЅРµ РЅР°Р№РґРµРЅ'})

        promo_id, reward_stars, reward_tickets, reward_type, reward_data, max_uses, used_count, expires_at, is_active = promo
        
        # РЈСЃС‚Р°РЅРѕРІРёС‚СЊ Р·РЅР°С‡РµРЅРёСЏ РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ
        if reward_type is None:
            reward_type = 'stars'
        if reward_data:
            try:
                reward_data = json.loads(reward_data)
            except:
                reward_data = {}
        else:
            reward_data = {}

        if not is_active:
            conn.close()
            return jsonify({'success': False, 'error': 'РџСЂРѕРјРѕРєРѕРґ РЅРµР°РєС‚РёРІРµРЅ'})

        if expires_at:
            try:
                expires_date = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                if datetime.now() > expires_date:
                    conn.close()
                    return jsonify({'success': False, 'error': 'РЎСЂРѕРє РґРµР№СЃС‚РІРёСЏ РїСЂРѕРјРѕРєРѕРґР° РёСЃС‚РµРє'})
            except:
                pass

        if max_uses > 0 and used_count >= max_uses:
            conn.close()
            return jsonify({'success': False, 'error': 'Р›РёРјРёС‚ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёР№ РїСЂРѕРјРѕРєРѕРґР° РёСЃС‡РµСЂРїР°РЅ'})

        # РџСЂРѕРІРµСЂСЏРµРј, РЅРµ РёСЃРїРѕР»СЊР·РѕРІР°Р» Р»Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ СЌС‚РѕС‚ РїСЂРѕРјРѕРєРѕРґ
        cursor.execute('SELECT id FROM used_promo_codes WHERE user_id = ? AND promo_code_id = ?', (user_id, promo_id))
        already_used = cursor.fetchone()

        if already_used:
            conn.close()
            return jsonify({'success': False, 'error': 'Р’С‹ СѓР¶Рµ РёСЃРїРѕР»СЊР·РѕРІР°Р»Рё СЌС‚РѕС‚ РїСЂРѕРјРѕРєРѕРґ'})

        # РћР±СЂР°Р±РѕС‚РєР° СЂР°Р·РЅС‹С… С‚РёРїРѕРІ РЅР°РіСЂР°Рґ
        response_data = {
            'success': True,
            'reward_type': reward_type,
            'reward_stars': reward_stars,
            'reward_tickets': reward_tickets,
            'stars': reward_stars,  # For inventory.html compatibility
            'tickets': reward_tickets  # For inventory.html compatibility
        }
        
        if reward_type == 'stars' or reward_type == 'tickets':
            # РЎС‚Р°РЅРґР°СЂС‚РЅС‹Рµ РЅР°РіСЂР°РґС‹
            if reward_stars > 0:
                cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (reward_stars, user_id))
            if reward_tickets > 0:
                cursor.execute('UPDATE users SET balance_tickets = balance_tickets + ? WHERE id = ?', (reward_tickets, user_id))
            response_data['message'] = f'РџСЂРѕРјРѕРєРѕРґ Р°РєС‚РёРІРёСЂРѕРІР°РЅ! +{reward_stars}в­ђ +{reward_tickets}рџЋ«'
            
        elif reward_type == 'crash_vip':
            # VIP РґР»СЏ Crash РёРіСЂС‹
            vip_days = reward_data.get('days', 7)
            cursor.execute('UPDATE users SET is_crash_vip = 1 WHERE id = ?', (user_id,))
            response_data['message'] = 'РџСЂРѕРјРѕРєРѕРґ Р°РєС‚РёРІРёСЂРѕРІР°РЅ! Crash VIP СЂР°Р·Р±Р»РѕРєРёСЂРѕРІР°РЅ!'
            response_data['vip_days'] = vip_days
            response_data['reward_amount'] = 'VIP'
            response_data['reward_icon'] = '/static/img/crown.png'
            response_data['is_vip'] = True
            
        elif reward_type == 'case_discount':
            # РЎРєРёРґРєР° РЅР° РєРµР№СЃ
            discount_percent = reward_data.get('discount', 50)
            case_target = reward_data.get('case_id', None)
            # РЎРѕС…СЂР°РЅСЏРµРј СЃРєРёРґРєСѓ РІ user_discounts РёР»Рё Р°РЅР°Р»РѕРіРёС‡РЅСѓСЋ С‚Р°Р±Р»РёС†Сѓ
            cursor.execute('''
                INSERT INTO user_discounts (user_id, discount_type, discount_value, case_id, expires_at, promo_id)
                VALUES (?, 'case_discount', ?, ?, datetime('now', '+7 days'), ?)
            ''', (user_id, discount_percent, case_target, promo_id))
            response_data['message'] = f'РџСЂРѕРјРѕРєРѕРґ Р°РєС‚РёРІРёСЂРѕРІР°РЅ! РЎРєРёРґРєР° {discount_percent}% РЅР° РєРµР№СЃ!'
            response_data['discount'] = discount_percent
            response_data['reward_amount'] = discount_percent
            response_data['reward_icon'] = '/static/img/crystal.png'
            
        elif reward_type == 'rocket':
            # РЈРЅРёРєР°Р»СЊРЅР°СЏ СЂР°РєРµС‚Р°
            rocket_id = reward_data.get('rocket_id', 'promo_rocket')
            rocket_name = reward_data.get('name', 'РџСЂРѕРјРѕ СЂР°РєРµС‚Р°')
            # РЎРѕР·РґР°С‘Рј С‚Р°Р±Р»РёС†Сѓ РµСЃР»Рё РЅРµ СЃСѓС‰РµСЃС‚РІСѓРµС‚
            cursor.execute('''CREATE TABLE IF NOT EXISTS user_customizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                item_type TEXT NOT NULL,
                item_id TEXT NOT NULL,
                source TEXT DEFAULT 'promo',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, item_type, item_id)
            )''')
            cursor.execute('''
                INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source, created_at)
                VALUES (?, 'rocket', ?, 'promo', datetime('now'))
            ''', (user_id, rocket_id))
            response_data['message'] = f'РџСЂРѕРјРѕРєРѕРґ Р°РєС‚РёРІРёСЂРѕРІР°РЅ! РќРѕРІР°СЏ СЂР°РєРµС‚Р° СЂР°Р·Р±Р»РѕРєРёСЂРѕРІР°РЅР°!'
            response_data['rocket_id'] = rocket_id
            response_data['rocket_image'] = f'/static/gifs/{rocket_id}.gif'
            response_data['reward_amount'] = rocket_id
            response_data['reward_icon'] = f'/static/gifs/{rocket_id}.gif'
            
        elif reward_type == 'background':
            # РЈРЅРёРєР°Р»СЊРЅС‹Р№ С„РѕРЅ
            bg_id = reward_data.get('background_id', 'promo_bg')
            # РЎРѕР·РґР°С‘Рј С‚Р°Р±Р»РёС†Сѓ РµСЃР»Рё РЅРµ СЃСѓС‰РµСЃС‚РІСѓРµС‚
            cursor.execute('''CREATE TABLE IF NOT EXISTS user_customizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                item_type TEXT NOT NULL,
                item_id TEXT NOT NULL,
                source TEXT DEFAULT 'promo',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, item_type, item_id)
            )''')
            cursor.execute('''
                INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source, created_at)
                VALUES (?, 'background', ?, 'promo', datetime('now'))
            ''', (user_id, bg_id))
            response_data['message'] = f'РџСЂРѕРјРѕРєРѕРґ Р°РєС‚РёРІРёСЂРѕРІР°РЅ! РќРѕРІС‹Р№ С„РѕРЅ СЂР°Р·Р±Р»РѕРєРёСЂРѕРІР°РЅ!'
            response_data['background_id'] = bg_id
            response_data['reward_amount'] = bg_id
            response_data['reward_icon'] = f'/static/img/{bg_id}.png'
            
        elif reward_type == 'inventory_gift':
            # РџРѕРґР°СЂРѕРє РІ РёРЅРІРµРЅС‚Р°СЂСЊ СЃ РѕРіСЂР°РЅРёС‡РµРЅРёСЏРјРё
            gift_data = reward_data.get('gift', {})
            restrictions = reward_data.get('restrictions', {})
            gift_name = gift_data.get('name', 'РџСЂРѕРјРѕ-РїРѕРґР°СЂРѕРє')
            gift_image = gift_data.get('image', '/static/img/gift.png')
            
            cursor.execute('''
                INSERT INTO inventory (user_id, gift_id, gift_name, gift_image, gift_price, source, restrictions, created_at)
                VALUES (?, ?, ?, ?, ?, 'promo', ?, datetime('now'))
            ''', (
                user_id, 
                gift_data.get('id', 'promo_gift'),
                gift_name,
                gift_image,
                gift_data.get('price', 0),
                json.dumps(restrictions)
            ))
            response_data['message'] = f'РџСЂРѕРјРѕРєРѕРґ Р°РєС‚РёРІРёСЂРѕРІР°РЅ! РџРѕРґР°СЂРѕРє РґРѕР±Р°РІР»РµРЅ РІ РёРЅРІРµРЅС‚Р°СЂСЊ!'
            response_data['gift_name'] = gift_name
            response_data['gift_image'] = gift_image
            response_data['reward_amount'] = gift_name
            response_data['reward_icon'] = gift_image
            
        elif reward_type == 'wager':
            # Р’РµР№РґР¶РµСЂ - Р±РѕРЅСѓСЃ СЃ РїСЂРѕРєСЂСѓС‚РєРѕР№
            wager_amount = reward_data.get('amount', 1000)
            wager_multiplier = reward_data.get('multiplier', 5)
            cursor.execute('''
                INSERT INTO user_wagers (user_id, bonus_amount, wager_requirement, wagered_amount, promo_id, created_at)
                VALUES (?, ?, ?, 0, ?, datetime('now'))
            ''', (user_id, wager_amount, wager_amount * wager_multiplier, promo_id))
            # РќР°С‡РёСЃР»СЏРµРј Р±РѕРЅСѓСЃРЅС‹Рµ Р·РІРµР·РґС‹
            cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (wager_amount, user_id))
            response_data['message'] = f'РџСЂРѕРјРѕРєРѕРґ Р°РєС‚РёРІРёСЂРѕРІР°РЅ! +{wager_amount} Р±РѕРЅСѓСЃРЅС‹С… Р·РІРµР·Рґ (x{wager_multiplier} РѕС‚С‹РіСЂС‹С€)'
            response_data['reward_amount'] = wager_amount
            response_data['wager_multiplier'] = wager_multiplier

        elif reward_type == 'case_open':
            # РћС‚РєСЂС‹С‚РёРµ РїСЂРѕРјРѕ-РєРµР№СЃР° вЂ” РїСЂРѕРІРµСЂСЏРµРј case_id
            target_case_id = reward_data.get('case_id', None)
            if target_case_id and case_id and int(target_case_id) != int(case_id):
                conn.close()
                return jsonify({'success': False, 'error': 'Р­С‚РѕС‚ РїСЂРѕРјРѕРєРѕРґ РЅРµ РїРѕРґС…РѕРґРёС‚ РґР»СЏ РґР°РЅРЅРѕРіРѕ РєРµР№СЃР°'})
            response_data['message'] = 'РџСЂРѕРјРѕРєРѕРґ Р°РєС‚РёРІРёСЂРѕРІР°РЅ! РљРµР№СЃ СЂР°Р·Р±Р»РѕРєРёСЂРѕРІР°РЅ!'
            response_data['case_unlocked'] = True
            response_data['reward_amount'] = 'РљРµР№СЃ'
            response_data['reward_icon'] = '/static/img/gift.png'

        # Р—Р°РїРёСЃС‹РІР°РµРј РёСЃРїРѕР»СЊР·РѕРІР°РЅРёРµ
        cursor.execute('UPDATE promo_codes SET used_count = used_count + 1 WHERE id = ?', (promo_id,))
        cursor.execute('INSERT INTO used_promo_codes (user_id, promo_code_id) VALUES (?, ?)', (user_id, promo_id))

        conn.commit()
        conn.close()

        logger.info(f"вњ… РџСЂРѕРјРѕРєРѕРґ {promo_code} С‚РёРїР° {reward_type} Р°РєС‚РёРІРёСЂРѕРІР°РЅ")

        return jsonify(response_data)

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° Р°РєС‚РёРІР°С†РёРё РїСЂРѕРјРѕРєРѕРґР°: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/user/<int:user_id>/case-history/<int:case_id>')
def user_case_history(user_id, case_id):
    """РџРѕР»СѓС‡РёС‚СЊ РёСЃС‚РѕСЂРёСЋ РѕС‚РєСЂС‹С‚РёР№ РєРµР№СЃР° РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT created_at FROM case_open_history
            WHERE user_id = ? AND case_id = ?
            ORDER BY created_at DESC LIMIT 1
        ''', (user_id, case_id))

        last_open = cursor.fetchone()
        conn.close()

        return jsonify({
            'success': True,
            'last_opened': last_open[0] if last_open else None
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёСЃС‚РѕСЂРёРё РєРµР№СЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/debug-cases', methods=['GET'])
def debug_cases():
    """РћС‚Р»Р°РґРѕС‡РЅР°СЏ РёРЅС„РѕСЂРјР°С†РёСЏ Рѕ РєРµР№СЃР°С…"""
    try:
        logger.info("рџ”Ќ РћС‚Р»Р°РґРѕС‡РЅР°СЏ РёРЅС„РѕСЂРјР°С†РёСЏ Рѕ РєРµР№СЃР°С…")

        file_path = os.path.join(BASE_PATH, 'data', 'cases.json')
        logger.info(f"рџ“Ѓ РџСѓС‚СЊ Рє С„Р°Р№Р»Сѓ cases.json: {file_path}")

        exists = os.path.exists(file_path)
        logger.info(f"рџ“Ѓ Р¤Р°Р№Р» СЃСѓС‰РµСЃС‚РІСѓРµС‚: {exists}")

        cases_info = []
        if exists:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    logger.info(f"рџ“Ѓ Р Р°Р·РјРµСЂ С„Р°Р№Р»Р°: {len(content)} Р±Р°Р№С‚")

                    data = json.loads(content)
                    cases = data.get('cases', [])
                    logger.info(f"рџ“Ѓ РќР°Р№РґРµРЅРѕ РєРµР№СЃРѕРІ: {len(cases)}")

                    for i, case in enumerate(cases[:5]):
                        cases_info.append({
                            'id': case.get('id'),
                            'name': case.get('name'),
                            'cost': case.get('cost'),
                            'limited': case.get('limited')
                        })
            except Exception as e:
                logger.error(f"вќЊ РћС€РёР±РєР° С‡С‚РµРЅРёСЏ С„Р°Р№Р»Р°: {e}")

        return jsonify({
            'success': True,
            'file_path': file_path,
            'exists': exists,
            'base_path': BASE_PATH,
            'cases_sample': cases_info,
            'current_dir': os.getcwd(),
            'data_dir_exists': os.path.exists(os.path.join(BASE_PATH, 'data'))
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РѕС‚Р»Р°РґРєРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/init-db', methods=['POST'])
def admin_init_db():
    """РџСЂРёРЅСѓРґРёС‚РµР»СЊРЅР°СЏ РёРЅРёС†РёР°Р»РёР·Р°С†РёСЏ РІСЃРµС… С‚Р°Р±Р»РёС† Р‘Р”"""
    try:
        data = request.get_json() or {}
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # РџРѕР»СѓС‡Р°РµРј СЃРїРёСЃРѕРє СЃСѓС‰РµСЃС‚РІСѓСЋС‰РёС… С‚Р°Р±Р»РёС† Р”Рћ
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables_before = [row[0] for row in cursor.fetchall()]
        
        # РЎРѕР·РґР°РµРј РІСЃРµ С‚Р°Р±Р»РёС†С‹
        result = _create_all_tables(conn)
        
        # Р”РѕР±Р°РІР»СЏРµРј РґРµС„РѕР»С‚РЅС‹Рµ РєР°СЃС‚РѕРјРёР·Р°С†РёРё
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO crash_customizations (item_type, item_id, name, is_vip, is_default, access_type, requirement)
                VALUES ('background', 'phone', 'РљРѕСЃРјРѕСЃ', 0, 1, 'free', 0)
            ''')
            # Р РµРіРёСЃС‚СЂРёСЂСѓРµРј СЂР°РєРµС‚С‹ РёР· LEVEL_SYSTEM
            for lvl_info in LEVEL_SYSTEM:
                rocket_id = lvl_info.get('reward_rocket')
                if not rocket_id:
                    continue
                rocket_name = ROCKET_NAMES.get(rocket_id, rocket_id)
                lvl_num = lvl_info['level']
                is_default = 1 if lvl_num == 1 else 0
                access = 'free' if lvl_num == 1 else 'level'
                req = 0 if lvl_num == 1 else lvl_num
                cursor.execute('''
                    INSERT OR IGNORE INTO crash_customizations (item_type, item_id, name, is_vip, is_default, access_type, requirement)
                    VALUES ('rocket', ?, ?, 0, ?, ?, ?)
                ''', (rocket_id, rocket_name, is_default, access, req))
            conn.commit()
        except Exception as e:
            logger.warning(f"РћС€РёР±РєР° РґРѕР±Р°РІР»РµРЅРёСЏ РґРµС„РѕР»С‚РЅС‹С… РєР°СЃС‚РѕРјРёР·Р°С†РёР№: {e}")
        
        # РџРѕР»СѓС‡Р°РµРј СЃРїРёСЃРѕРє СЃСѓС‰РµСЃС‚РІСѓСЋС‰РёС… С‚Р°Р±Р»РёС† РџРћРЎР›Р•
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables_after = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        new_tables = set(tables_after) - set(tables_before)
        
        return jsonify({
            'success': result,
            'tables_before': len(tables_before),
            'tables_after': len(tables_after),
            'new_tables': list(new_tables),
            'all_tables': tables_after
        })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РёРЅРёС†РёР°Р»РёР·Р°С†РёРё Р‘Р”: {e}")
        import traceback
        return jsonify({'success': False, 'error': str(e), 'traceback': traceback.format_exc()})

@app.route('/api/admin/db-status', methods=['GET'])
def admin_db_status():
    """РџРѕР»СѓС‡РµРЅРёРµ СЃС‚Р°С‚СѓСЃР° Р‘Р” Рё СЃРїРёСЃРєР° С‚Р°Р±Р»РёС†"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # РЎРїРёСЃРѕРє С‚Р°Р±Р»РёС†
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        
        # РљРѕР»РёС‡РµСЃС‚РІРѕ Р·Р°РїРёСЃРµР№ РІ РєР°Р¶РґРѕР№ С‚Р°Р±Р»РёС†Рµ
        table_counts = {}
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                table_counts[table] = cursor.fetchone()[0]
            except:
                table_counts[table] = -1
        
        conn.close()
        
        # РћР¶РёРґР°РµРјС‹Рµ С‚Р°Р±Р»РёС†С‹
        expected = [
            'users', 'inventory', 'user_history', 'case_limits', 'referrals',
            'referral_rewards', 'withdrawals', 'deposits', 'promo_codes',
            'used_promo_codes', 'user_customizations', 'user_discounts', 'user_wagers',
            'user_levels', 'level_history', 'win_history', 'case_open_history',
            'ultimate_crash_games', 'ultimate_crash_bets', 'ultimate_crash_history',
            'crash_games', 'crash_bets', 'crash_history', 'notifications',
            'user_notifications', 'auth_codes', 'ton_payments', 'daily_tasks',
            'user_daily_progress', 'reward_claims', 'crash_customizations'
        ]
        
        missing = [t for t in expected if t not in tables]
        
        return jsonify({
            'success': True,
            'tables': tables,
            'table_counts': table_counts,
            'total_tables': len(tables),
            'expected_tables': len(expected),
            'missing_tables': missing
        })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃС‚Р°С‚СѓСЃР° Р‘Р”: {e}")
        return jsonify({'success': False, 'error': str(e)})

# HISTORY API
@app.route('/api/recent-wins', methods=['GET'])
@app.route('/api/latest-wins', methods=['GET'])
def get_recent_wins():
    """РџРѕР»СѓС‡РµРЅРёРµ РїРѕСЃР»РµРґРЅРёС… РїРѕР±РµРґ РґР»СЏ РіР»Р°РІРЅРѕР№ СЃС‚СЂР°РЅРёС†С‹"""
    try:
        limit = request.args.get('limit', 10, type=int)

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT user_name, gift_name, gift_image, gift_value, case_name, created_at
            FROM win_history
            ORDER BY created_at DESC
            LIMIT ?
        ''', (limit,))

        wins = cursor.fetchall()
        conn.close()

        win_history_list = []
        # load cases list to map case_name -> case_id
        cases_path = os.path.join(BASE_PATH, 'data', 'cases.json')
        cases_map = {}
        try:
            if os.path.exists(cases_path):
                with open(cases_path, 'r', encoding='utf-8') as cf:
                    cases_data = json.load(cf)
                    for c in cases_data.get('cases', []):
                        cases_map[str(c.get('name') or '').strip()] = c.get('id')
        except Exception:
            cases_map = {}

        for win in wins:
            user_name, gift_name, gift_image, gift_value, case_name, created_at = win
            case_id = cases_map.get(str(case_name or '').strip())

            win_history_list.append({
                'user_name': user_name,
                'gift_name': gift_name,
                'gift_image': gift_image,
                'gift_value': gift_value,
                'case_name': case_name,
                'case_id': case_id,
                'created_at': created_at
            })

        logger.info(f"рџ“Љ РћС‚РїСЂР°РІР»РµРЅРѕ {len(win_history_list)} Р·Р°РїРёСЃРµР№ РёСЃС‚РѕСЂРёРё РїРѕР±РµРґ")
        return jsonify({
            'success': True,
            'wins': win_history_list
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёСЃС‚РѕСЂРёРё РїРѕР±РµРґ: {e}")
        return jsonify({'success': False, 'error': str(e)})
        return jsonify({
            'success': False,
            'error': str(e),
            'wins': []
        })

@app.route('/api/recent-case-opens', methods=['GET'])
def get_recent_case_opens():
    """РџРѕР»СѓС‡РµРЅРёРµ РїРѕСЃР»РµРґРЅРёС… РѕС‚РєСЂС‹С‚РёР№ РєРµР№СЃРѕРІ"""
    try:
        limit = request.args.get('limit', 20, type=int)
        user_id = request.args.get('user_id')

        conn = get_db_connection()
        cursor = conn.cursor()

        if user_id:
            cursor.execute('''
                SELECT case_name, gift_name, gift_image, gift_value, cost, cost_type, created_at
                FROM case_open_history
                WHERE user_id = ?
                ORDER BY created_at DESC
                LIMIT ?
            ''', (user_id, limit))
        else:
            cursor.execute('''
                SELECT case_name, gift_name, gift_image, gift_value, cost, cost_type, created_at
                FROM case_open_history
                ORDER BY created_at DESC
                LIMIT ?
            ''', (limit,))

        opens = cursor.fetchall()
        conn.close()

        open_history_list = []
        for open_item in opens:
            case_name, gift_name, gift_image, gift_value, cost, cost_type, created_at = open_item

            file_extension = gift_image.lower().split('.')[-1] if '.' in gift_image else ''
            is_gif = file_extension == 'gif'
            is_image = file_extension in ['png', 'jpg', 'jpeg', 'webp']

            open_history_list.append({
                'case_name': case_name,
                'gift_name': gift_name,
                'gift_image': gift_image,
                'gift_value': gift_value,
                'cost': cost,
                'cost_type': cost_type,
                'created_at': created_at,
                'is_gif': is_gif,
                'is_image': is_image
            })

        logger.info(f"рџ“Љ РћС‚РїСЂР°РІР»РµРЅРѕ {len(open_history_list)} Р·Р°РїРёСЃРµР№ РёСЃС‚РѕСЂРёРё РѕС‚РєСЂС‹С‚РёР№")
        return jsonify({
            'success': True,
            'opens': open_history_list
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёСЃС‚РѕСЂРёРё РѕС‚РєСЂС‹С‚РёР№: {e}")
        return jsonify({'success': False, 'error': str(e)})

# DAILY BONUS API
@app.route('/api/claim-daily-bonus', methods=['POST'])
def claim_daily_bonus():
    """РџРѕР»СѓС‡РµРЅРёРµ РµР¶РµРґРЅРµРІРЅРѕРіРѕ Р±РѕРЅСѓСЃР°"""
    try:
        data = request.get_json()
        user_id = data['user_id']

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT last_daily_bonus, consecutive_days FROM users WHERE id = ?', (user_id,))
        result = cursor.fetchone()

        if not result:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        last_bonus, consecutive_days = result
        now = datetime.now()

        if last_bonus:
            last_bonus_date = datetime.fromisoformat(last_bonus.replace('Z', '+00:00'))
            hours_diff = (now - last_bonus_date).total_seconds() / 3600

            if hours_diff < 24:
                conn.close()
                return jsonify({'success': False, 'error': 'Р‘РѕРЅСѓСЃ СѓР¶Рµ РїРѕР»СѓС‡РµРЅ СЃРµРіРѕРґРЅСЏ'})

            if hours_diff < 48:
                consecutive_days = (consecutive_days or 0) + 1
            else:
                consecutive_days = 1
        else:
            consecutive_days = 1

        base_stars = 5
        bonus_stars = min(consecutive_days * 2, 20)
        total_stars = base_stars + bonus_stars

        cursor.execute('''
            UPDATE users
            SET balance_stars = balance_stars + ?,
                total_earned_stars = total_earned_stars + ?,
                last_daily_bonus = ?,
                consecutive_days = ?
            WHERE id = ?
        ''', (total_stars, total_stars, now.isoformat(), consecutive_days, user_id))

        add_experience(user_id, 10, "Р•Р¶РµРґРЅРµРІРЅС‹Р№ Р±РѕРЅСѓСЃ")

        add_history_record(user_id, 'daily_bonus', total_stars,
                         f'Р•Р¶РµРґРЅРµРІРЅС‹Р№ Р±РѕРЅСѓСЃ ({consecutive_days} РґРµРЅСЊ РїРѕРґСЂСЏРґ)')

        conn.commit()
        conn.close()

        logger.info(f"рџЋЃ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РїРѕР»СѓС‡РёР» РµР¶РµРґРЅРµРІРЅС‹Р№ Р±РѕРЅСѓСЃ: {total_stars} Р·РІРµР·Рґ")
        return jsonify({
            'success': True,
            'stars_rewarded': total_stars,
            'consecutive_days': consecutive_days,
            'message': f'рџЋ‰ Р•Р¶РµРґРЅРµРІРЅС‹Р№ Р±РѕРЅСѓСЃ! Р’С‹ РїРѕР»СѓС‡РёР»Рё {total_stars} Р·РІРµР·Рґ!'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РµР¶РµРґРЅРµРІРЅРѕРіРѕ Р±РѕРЅСѓСЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

# SELL GIFTS API
@app.route('/api/sell-gift', methods=['POST'])
def sell_gift():
    """РџСЂРѕРґР°Р¶Р° РїРѕРґР°СЂРєР° РёР· РёРЅРІРµРЅС‚Р°СЂСЏ"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        gift_id = data['gift_id']

        logger.info(f"рџ’° РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РїСЂРѕРґР°РµС‚ РїРѕРґР°СЂРѕРє РёР· РёРЅРІРµРЅС‚Р°СЂСЏ {gift_id}")

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT gift_name, gift_value, is_withdrawing FROM inventory WHERE id = ? AND user_id = ?', (gift_id, user_id))
        gift = cursor.fetchone()

        if not gift:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕРґР°СЂРѕРє РЅРµ РЅР°Р№РґРµРЅ РІ РёРЅРІРµРЅС‚Р°СЂРµ'})

        gift_name, gift_value, is_withdrawing = gift

        if is_withdrawing:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕРґР°СЂРѕРє РЅР°С…РѕРґРёС‚СЃСЏ РІ РїСЂРѕС†РµСЃСЃРµ РІС‹РІРѕРґР° Рё РЅРµ РјРѕР¶РµС‚ Р±С‹С‚СЊ РїСЂРѕРґР°РЅ'})

        # Check if it's a crate item (cannot be sold)
        try:
            cursor.execute("PRAGMA table_info('inventory')")
            cols = [r[1] for r in cursor.fetchall()]
            if 'crate_id' in cols:
                cursor.execute('SELECT crate_id FROM inventory WHERE id = ?', (gift_id,))
                cr = cursor.fetchone()
                if cr and cr[0]:
                    conn.close()
                    return jsonify({'success': False, 'error': 'РЇС‰РёРє РЅРµР»СЊР·СЏ РїСЂРѕРґР°С‚СЊ, С‚РѕР»СЊРєРѕ РѕС‚РєСЂС‹С‚СЊ'})
        except:
            pass

        cursor.execute('DELETE FROM inventory WHERE id = ?', (gift_id,))

        if gift_value > 0:
            cursor.execute('''
                UPDATE users
                SET balance_stars = balance_stars + ?,
                    total_earned_stars = total_earned_stars + ?
                WHERE id = ?
            ''', (gift_value, gift_value, user_id))

        exp_gained = max(1, gift_value // 100)
        cursor.execute('UPDATE users SET experience = experience + ? WHERE id = ?', (exp_gained, user_id))

        cursor.execute('''
            INSERT INTO user_history (user_id, operation_type, amount, description)
            VALUES (?, 'gift_sold', ?, ?)
        ''', (user_id, gift_value, f'РџСЂРѕРґР°Р¶Р° РїРѕРґР°СЂРєР°: {gift_name}'))

        conn.commit()

        cursor.execute('SELECT balance_stars, balance_tickets FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()
        conn.close()

        logger.info(f"вњ… РџРѕРґР°СЂРѕРє РїСЂРѕРґР°РЅ Р·Р° {gift_value} Р·РІРµР·Рґ")

        return jsonify({
            'success': True,
            'message': f'РџРѕРґР°СЂРѕРє РїСЂРѕРґР°РЅ Р·Р° {gift_value} Р·РІРµР·Рґ!',
            'new_balance': {
                'stars': new_balance[0],
                'tickets': new_balance[1]
            }
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїСЂРѕРґР°Р¶Рё РїРѕРґР°СЂРєР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/sell-all-gifts', methods=['POST'])
def sell_all_gifts():
    """РџСЂРѕРґР°Р¶Р° РІСЃРµС… РїРѕРґР°СЂРєРѕРІ РёР· РёРЅРІРµРЅС‚Р°СЂСЏ"""
    try:
        data = request.get_json()
        user_id = data['user_id']

        logger.info(f"рџ’° РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РїСЂРѕРґР°РµС‚ РІСЃРµ РїРѕРґР°СЂРєРё РёР· РёРЅРІРµРЅС‚Р°СЂСЏ")

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if crate_id column exists
        _has_crate_col = False
        try:
            cursor.execute("PRAGMA table_info('inventory')")
            _has_crate_col = 'crate_id' in [r[1] for r in cursor.fetchall()]
        except:
            pass

        if _has_crate_col:
            cursor.execute('SELECT id, gift_name, gift_value FROM inventory WHERE user_id = ? AND is_withdrawing = FALSE AND (crate_id IS NULL OR crate_id = 0)', (user_id,))
        else:
            cursor.execute('SELECT id, gift_name, gift_value FROM inventory WHERE user_id = ? AND is_withdrawing = FALSE', (user_id,))
        gifts = cursor.fetchall()

        if not gifts:
            conn.close()
            return jsonify({'success': False, 'error': 'Р’ РёРЅРІРµРЅС‚Р°СЂРµ РЅРµС‚ РїСЂРµРґРјРµС‚РѕРІ РґР»СЏ РїСЂРѕРґР°Р¶Рё'})

        total_value = 0
        sold_count = len(gifts)

        for gift in gifts:
            total_value += gift[2] or 0

        # Delete only non-crate items
        ids_to_del = [g[0] for g in gifts]
        if ids_to_del:
            placeholders = ','.join('?' * len(ids_to_del))
            cursor.execute(f'DELETE FROM inventory WHERE id IN ({placeholders})', ids_to_del)

        if total_value > 0:
            cursor.execute('''
                UPDATE users
                SET balance_stars = balance_stars + ?,
                    total_earned_stars = total_earned_stars + ?
                WHERE id = ?
            ''', (total_value, total_value, user_id))

        exp_gained = max(5, total_value // 50)
        cursor.execute('UPDATE users SET experience = experience + ? WHERE id = ?', (exp_gained, user_id))

        cursor.execute('''
            INSERT INTO user_history (user_id, operation_type, amount, description)
            VALUES (?, 'mass_sell', ?, ?)
        ''', (user_id, total_value, f'РњР°СЃСЃРѕРІР°СЏ РїСЂРѕРґР°Р¶Р° {sold_count} РїСЂРµРґРјРµС‚РѕРІ'))

        conn.commit()

        cursor.execute('SELECT balance_stars, balance_tickets FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()
        conn.close()

        logger.info(f"вњ… РџСЂРѕРґР°РЅРѕ {sold_count} РїСЂРµРґРјРµС‚РѕРІ Р·Р° {total_value} Р·РІРµР·Рґ")

        return jsonify({
            'success': True,
            'message': f'РџСЂРѕРґР°РЅРѕ {sold_count} РїСЂРµРґРјРµС‚РѕРІ Р·Р° {total_value} Р·РІРµР·Рґ!',
            'sold_count': sold_count,
            'total_value': total_value,
            'new_balance': {
                'stars': new_balance[0],
                'tickets': new_balance[1]
            }
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РјР°СЃСЃРѕРІРѕР№ РїСЂРѕРґР°Р¶Рё РїРѕРґР°СЂРєРѕРІ: {e}")
        return jsonify({'success': False, 'error': str(e)})

# WITHDRAWAL API
@app.route('/api/withdraw-gift', methods=['POST'])
def withdraw_gift():
    """РЎРѕР·РґР°РЅРёРµ Р·Р°СЏРІРєРё РЅР° РІС‹РІРѕРґ РїРѕРґР°СЂРєР°"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        inventory_id = data['gift_id']

        logger.info(f"рџ“¤ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} СЃРѕР·РґР°РµС‚ Р·Р°СЏРІРєСѓ РЅР° РІС‹РІРѕРґ РїРѕРґР°СЂРєР° {inventory_id}")

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM inventory WHERE id = ? AND user_id = ?', (inventory_id, user_id))
        gift = cursor.fetchone()

        if not gift:
            logger.error(f"вќЊ РџРѕРґР°СЂРѕРє {inventory_id} РЅРµ РЅР°Р№РґРµРЅ РІ РёРЅРІРµРЅС‚Р°СЂРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {user_id}")
            return jsonify({'success': False, 'error': 'РџРѕРґР°СЂРѕРє РЅРµ РЅР°Р№РґРµРЅ РІ РёРЅРІРµРЅС‚Р°СЂРµ'})

        if gift[7]:
            logger.error(f"вќЊ РџРѕРґР°СЂРѕРє {inventory_id} СѓР¶Рµ РІ РїСЂРѕС†РµСЃСЃРµ РІС‹РІРѕРґР°")
            return jsonify({'success': False, 'error': 'РџРѕРґР°СЂРѕРє СѓР¶Рµ РІ РїСЂРѕС†РµСЃСЃРµ РІС‹РІРѕРґР°'})

        # Check if it's a crate item (cannot be withdrawn)
        try:
            crate_id_val = gift[8] if len(gift) > 8 else None
            if crate_id_val:
                conn.close()
                return jsonify({'success': False, 'error': 'РЇС‰РёРє РЅРµР»СЊР·СЏ РІС‹РІРµСЃС‚Рё, С‚РѕР»СЊРєРѕ РѕС‚РєСЂС‹С‚СЊ'})
        except:
            pass

        cursor.execute('SELECT first_name, username, photo_url FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            logger.error(f"вќЊ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РЅРµ РЅР°Р№РґРµРЅ")
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        user_first_name, username, photo_url = user

        cursor.execute('UPDATE inventory SET is_withdrawing = TRUE WHERE id = ?', (inventory_id,))

        cursor.execute('''
            INSERT INTO withdrawals (user_id, inventory_id, gift_name, gift_image, gift_value,
                                   telegram_username, user_photo_url, user_first_name, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
        ''', (user_id, inventory_id, gift[3], gift[4], gift[5], username, photo_url, user_first_name))

        withdrawal_id = cursor.lastrowid

        add_history_record(user_id, 'withdraw_request', 0, f'Р—Р°РїСЂРѕСЃ РЅР° РІС‹РІРѕРґ: {gift[3]}')

        conn.commit()
        conn.close()

        logger.info(f"вњ… РЎРѕР·РґР°РЅР° Р·Р°СЏРІРєР° РЅР° РІС‹РІРѕРґ #{withdrawal_id} РґР»СЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {user_id}")
        return jsonify({
            'success': True,
            'message': 'вњ… Р—Р°СЏРІРєР° РЅР° РІС‹РІРѕРґ СЃРѕР·РґР°РЅР°! РћР¶РёРґР°Р№С‚Рµ РѕР±СЂР°Р±РѕС‚РєРё.',
            'withdrawal_id': withdrawal_id
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃРѕР·РґР°РЅРёСЏ Р·Р°СЏРІРєРё РЅР° РІС‹РІРѕРґ: {e}")
        return jsonify({'success': False, 'error': str(e)})

# REFERRAL API
@app.route('/api/referral-info/<int:user_id>', methods=['GET'])
def get_referral_info(user_id):
    """РџРѕР»СѓС‡РµРЅРёРµ РёРЅС„РѕСЂРјР°С†РёРё Рѕ СЂРµС„РµСЂР°Р»Р°С…"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT referral_code, referral_count, total_earned_stars, total_earned_tickets, referral_bonus_claimed FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        referral_code, referral_count, total_stars, total_tickets, bonus_claimed = user

        cursor.execute('''
            SELECT u.id, u.first_name, u.username, u.photo_url, r.created_at
            FROM referrals r
            JOIN users u ON r.referred_id = u.id
            WHERE r.referrer_id = ?
            ORDER BY r.created_at DESC
        ''', (user_id,))

        referrals = cursor.fetchall()

        cursor.execute('''
            SELECT reward_type, reward_amount, description, created_at
            FROM referral_rewards
            WHERE referrer_id = ?
            ORDER BY created_at DESC
        ''', (user_id,))

        rewards = cursor.fetchall()

        referral_list = []
        for ref in referrals:
            referral_list.append({
                'id': ref[0],
                'name': ref[1],
                'username': ref[2],
                'photo_url': ref[3],
                'date': ref[4]
            })

        rewards_list = []
        for reward in rewards:
            rewards_list.append({
                'type': reward[0],
                'amount': reward[1],
                'description': reward[2],
                'date': reward[3]
            })

        conn.close()

        return jsonify({
            'success': True,
            'referral_code': referral_code,
            'referral_count': referral_count or 0,
            'total_earned_stars': total_stars or 0,
            'total_earned_tickets': total_tickets or 0,
            'referral_bonus_claimed': bool(bonus_claimed),
            'referrals': referral_list,
            'rewards_history': rewards_list
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёРЅС„РѕСЂРјР°С†РёРё Рѕ СЂРµС„РµСЂР°Р»Р°С…: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/claim-referral-bonus', methods=['POST'])
def claim_referral_bonus():
    """РџРѕР»СѓС‡РµРЅРёРµ Р±РѕРЅСѓСЃР° Р·Р° СЂРµС„РµСЂР°Р»РѕРІ"""
    try:
        data = request.get_json()
        user_id = data['user_id']

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT referral_bonus_claimed, referral_count FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        bonus_claimed, referral_count = user

        if bonus_claimed:
            conn.close()
            return jsonify({'success': False, 'error': 'Р‘РѕРЅСѓСЃ СѓР¶Рµ Р±С‹Р» РїРѕР»СѓС‡РµРЅ'})

        if referral_count < 3:
            conn.close()
            return jsonify({'success': False, 'error': 'РќРµРѕР±С…РѕРґРёРјРѕ РїСЂРёРіР»Р°СЃРёС‚СЊ РјРёРЅРёРјСѓРј 3 РґСЂСѓР·РµР№'})

        bonus_stars = 500
        cursor.execute('UPDATE users SET balance_stars = balance_stars + ?, total_earned_stars = total_earned_stars + ?, referral_bonus_claimed = TRUE WHERE id = ?',
                     (bonus_stars, bonus_stars, user_id))

        add_experience(user_id, 100, "Р РµС„РµСЂР°Р»СЊРЅС‹Р№ Р±РѕРЅСѓСЃ")

        add_history_record(user_id, 'referral_bonus', bonus_stars, f'Р‘РѕРЅСѓСЃ Р·Р° РїСЂРёРіР»Р°С€РµРЅРёРµ {referral_count} РґСЂСѓР·РµР№')

        cursor.execute('SELECT balance_stars, balance_tickets FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()

        conn.commit()
        conn.close()

        logger.info(f"рџЋЃ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РїРѕР»СѓС‡РёР» СЂРµС„РµСЂР°Р»СЊРЅС‹Р№ Р±РѕРЅСѓСЃ: {bonus_stars} Р·РІРµР·Рґ")
        return jsonify({
            'success': True,
            'message': f'рџЋ‰ РџРѕР·РґСЂР°РІР»СЏРµРј! Р’С‹ РїРѕР»СѓС‡РёР»Рё {bonus_stars} Р·РІРµР·Рґ Р·Р° РїСЂРёРіР»Р°С€РµРЅРёРµ {referral_count} РґСЂСѓР·РµР№!',
            'bonus_stars': bonus_stars,
            'new_balance': {
                'stars': new_balance[0],
                'tickets': new_balance[1]
            }
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЂРµС„РµСЂР°Р»СЊРЅРѕРіРѕ Р±РѕРЅСѓСЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/referral/withdraw', methods=['POST'])
def withdraw_referral_balance():
    """Р’С‹РІРѕРґ СЂРµС„РµСЂР°Р»СЊРЅРѕРіРѕ Р±Р°Р»Р°РЅСЃР° РЅР° РѕСЃРЅРѕРІРЅРѕР№ (РјРёРЅ. 1 TON = 100 stars)"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        
        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT referral_balance FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            return jsonify({'success': False, 'error': 'User not found'})
        
        ref_balance = row[0] or 0
        
        if ref_balance < 100:  # Min 1 TON = 100 stars
            conn.close()
            return jsonify({'success': False, 'error': 'Minimum 1 TON to withdraw'})
        
        # Transfer to main balance
        cursor.execute('''
            UPDATE users 
            SET balance_stars = balance_stars + ?, referral_balance = 0 
            WHERE id = ?
        ''', (ref_balance, user_id))
        
        add_history_record(user_id, 'referral_withdraw', ref_balance, f'Р’С‹РІРѕРґ СЂРµС„РµСЂР°Р»СЊРЅРѕРіРѕ Р±Р°Р»Р°РЅСЃР°')
        
        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        logger.info(f"рџ’° User {user_id} withdrew {ref_balance} stars from referral balance")
        return jsonify({
            'success': True,
            'withdrawn': ref_balance,
            'new_balance': new_balance
        })
        
    except Exception as e:
        logger.error(f"вќЊ Referral withdrawal error: {e}")
        return jsonify({'success': False, 'error': str(e)})


# UPGRADE API
@app.route('/api/user-upgrade-stats/<int:user_id>', methods=['GET'])
def get_user_upgrade_stats(user_id):
    """РџРѕР»СѓС‡РµРЅРёРµ СЃС‚Р°С‚РёСЃС‚РёРєРё Р°РїРіСЂРµР№РґРѕРІ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT COUNT(*) FROM user_history
            WHERE user_id = ? AND operation_type = 'upgrade_success'
            AND created_at > datetime('now', '-1 hour')
        ''', (user_id,))

        recent_success_count = cursor.fetchone()[0] or 0

        conn.close()

        return jsonify({
            'success': True,
            'recent_success_count': recent_success_count
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЃС‚Р°С‚РёСЃС‚РёРєРё Р°РїРіСЂРµР№РґРѕРІ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/upgrade-gift-fast', methods=['POST'])
def upgrade_gift_fast():
    """Р‘Р«РЎРўР Р«Р™ Р°РїРіСЂРµР№Рґ РїРѕРґР°СЂРєР°"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        current_gift_id = data['current_gift_id']
        target_gift_id = data['target_gift_id']

        logger.info(f"вљЎ Р‘Р«РЎРўР Р«Р™ Р°РїРіСЂРµР№Рґ: РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id}, РїРѕРґР°СЂРѕРє {current_gift_id} -> {target_gift_id}")

        # Use Fragment catalog (originals only for upgrade targets)
        gifts = build_fragment_first_gifts_catalog()
        if not gifts:
            return jsonify({'success': False, 'error': 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ СЃРїРёСЃРѕРє РїРѕРґР°СЂРєРѕРІ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT gift_id, gift_name, gift_value FROM inventory WHERE id = ? AND user_id = ?',
                      (current_gift_id, user_id))
        current_gift = cursor.fetchone()

        if not current_gift:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕРґР°СЂРѕРє РЅРµ РЅР°Р№РґРµРЅ РІ РёРЅРІРµРЅС‚Р°СЂРµ'})

        current_gift_db_id, gift_name, current_value = current_gift

        cursor.execute('SELECT COALESCE(first_name, username, ?) FROM users WHERE id = ?', ('РРіСЂРѕРє', user_id))
        user_row = cursor.fetchone()
        user_name = (user_row[0] if user_row and user_row[0] else 'РРіСЂРѕРє')

        target_gift = next((g for g in gifts if g.get('id') == target_gift_id or g.get('gift_key') == str(target_gift_id) or g.get('fragment_slug') == str(target_gift_id)), None)
        if not target_gift:
            conn.close()
            return jsonify({'success': False, 'error': 'Р¦РµР»РµРІРѕР№ РїРѕРґР°СЂРѕРє РЅРµ РЅР°Р№РґРµРЅ'})

        target_value = target_gift.get('value', 0)

        if target_value <= current_value:
            conn.close()
            return jsonify({'success': False, 'error': 'РќРµР»СЊР·СЏ Р°РїРіСЂРµР№РґРёС‚СЊ РЅР° РїРѕРґР°СЂРѕРє С‚Р°РєРѕР№ Р¶Рµ РёР»Рё РјРµРЅСЊС€РµР№ СЃС‚РѕРёРјРѕСЃС‚Рё'})

        chance = (current_value / target_value) * 100
        chance = max(10, min(chance, 80))

        cursor.execute('''
            SELECT COUNT(*) FROM user_history
            WHERE user_id = ? AND operation_type = 'upgrade_success'
            AND created_at > datetime('now', '-1 hour')
        ''', (user_id,))
        recent_success_count = cursor.fetchone()[0] or 0

        forced_failure = False
        if recent_success_count >= 3:
            success = False
            forced_failure = True
            logger.info(f"рџЋЇ РџР РРќРЈР”РРўР•Р›Р¬РќР«Р™ РџР РћР’РђР›: 4-Р№ Р°РїРіСЂРµР№Рґ РїРѕСЃР»Рµ {recent_success_count} СѓСЃРїРµС€РЅС‹С…")
        else:
            random_value = random.random() * 100
            success = random_value <= chance
            logger.info(f"рџЋЇ РћР±С‹С‡РЅС‹Р№ Р°РїРіСЂРµР№Рґ: СЃР»СѓС‡Р°Р№РЅРѕРµ С‡РёСЃР»Рѕ {random_value:.1f} vs С€Р°РЅСЃ {chance:.1f}% = {'РЈРЎРџР•РҐ' if success else 'РџР РћР’РђР›'}")

        try:
            if success:
                cursor.execute('''
                    UPDATE inventory
                    SET gift_id = ?, gift_name = ?, gift_image = ?, gift_value = ?
                    WHERE id = ?
                ''', (target_gift['id'], target_gift['name'], target_gift['image'], target_value, current_gift_id))

                exp_gained = max(5, (target_value - current_value) // 50)
                cursor.execute('UPDATE users SET experience = experience + ? WHERE id = ?', (exp_gained, user_id))

                cursor.execute('''
                    INSERT INTO user_history (user_id, operation_type, amount, description)
                    VALUES (?, 'upgrade_success', ?, ?)
                ''', (user_id, 0, f'РЈСЃРїРµС€РЅС‹Р№ Р°РїРіСЂРµР№Рґ: {gift_name} -> {target_gift["name"]}'))

                cursor.execute('''
                    INSERT INTO win_history (user_id, user_name, gift_name, gift_image, gift_value, case_name)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (user_id, user_name, target_gift['name'], target_gift['image'], target_value, 'Upgrade'))

                logger.info(f"вњ… РЈСЃРїРµС€РЅС‹Р№ Р°РїРіСЂРµР№Рґ: {gift_name} -> {target_gift['name']}")

            else:
                cursor.execute('DELETE FROM inventory WHERE id = ?', (current_gift_id,))

                if forced_failure:
                    cursor.execute('''
                        INSERT INTO user_history (user_id, operation_type, amount, description)
                        VALUES (?, 'upgrade_forced_failure', ?, ?)
                    ''', (user_id, 0, f'РџСЂРёРЅСѓРґРёС‚РµР»СЊРЅС‹Р№ РїСЂРѕРІР°Р» Р°РїРіСЂРµР№РґР°: РїРѕС‚РµСЂСЏРЅ {gift_name}'))
                else:
                    cursor.execute('''
                        INSERT INTO user_history (user_id, operation_type, amount, description)
                        VALUES (?, 'upgrade_failure', ?, ?)
                    ''', (user_id, 0, f'РќРµСѓРґР°С‡РЅС‹Р№ Р°РїРіСЂРµР№Рґ: РїРѕС‚РµСЂСЏРЅ {gift_name}'))

                logger.info(f"вќЊ РќРµСѓРґР°С‡РЅС‹Р№ Р°РїРіСЂРµР№Рґ: РїРѕС‚РµСЂСЏРЅ {gift_name}")

            conn.commit()
            conn.close()

            return jsonify({
                'success': True,
                'upgrade_success': success,
                'chance': round(chance, 1),
                'forced_failure': forced_failure,
                'recent_success_count': recent_success_count,
                'new_gift': target_gift if success else None,
                'message': 'РЈСЃРїРµС€РЅС‹Р№ Р°РїРіСЂРµР№Рґ!' if success else 'РђРїРіСЂРµР№Рґ РЅРµ СѓРґР°Р»СЃСЏ'
            })

        except Exception as e:
            conn.rollback()
            logger.error(f"вќЊ РћС€РёР±РєР° С‚СЂР°РЅР·Р°РєС†РёРё Р°РїРіСЂРµР№РґР°: {e}")
            return jsonify({'success': False, 'error': 'РћС€РёР±РєР° РѕР±СЂР°Р±РѕС‚РєРё Р°РїРіСЂРµР№РґР°'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° Р±С‹СЃС‚СЂРѕРіРѕ Р°РїРіСЂРµР№РґР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/upgrade-gift', methods=['POST'])
def upgrade_gift():
    """РђРїРіСЂРµР№Рґ РїРѕРґР°СЂРєР°"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        current_gift_id = data['current_gift_id']
        target_gift_id = data['target_gift_id']

        logger.info(f"вљЎ РђРїРіСЂРµР№Рґ: {user_id} -> {current_gift_id} РЅР° {target_gift_id}")

        # Use Fragment catalog (originals only for upgrade targets)
        gifts = build_fragment_first_gifts_catalog()
        target_gift = next((g for g in gifts if g.get('id') == target_gift_id or g.get('gift_key') == str(target_gift_id) or g.get('fragment_slug') == str(target_gift_id)), None)
        if not target_gift:
            return jsonify({'success': False, 'error': 'Р¦РµР»РµРІРѕР№ РїРѕРґР°СЂРѕРє РЅРµ РЅР°Р№РґРµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT gift_name, gift_value FROM inventory WHERE id = ? AND user_id = ?',
                         (current_gift_id, user_id))
            current_gift = cursor.fetchone()

            if not current_gift:
                conn.close()
                return jsonify({'success': False, 'error': 'РџРѕРґР°СЂРѕРє РЅРµ РЅР°Р№РґРµРЅ'})

            gift_name, current_value = current_gift

            cursor.execute('SELECT COALESCE(first_name, username, ?) FROM users WHERE id = ?', ('РРіСЂРѕРє', user_id))
            user_row = cursor.fetchone()
            user_name = (user_row[0] if user_row and user_row[0] else 'РРіСЂРѕРє')

            target_value = target_gift.get('value', 0)

            if target_value <= current_value:
                conn.close()
                return jsonify({'success': False, 'error': 'РќРµР»СЊР·СЏ Р°РїРіСЂРµР№РґРёС‚СЊ РЅР° Р±РѕР»РµРµ РґРµС€РµРІС‹Р№ РїРѕРґР°СЂРѕРє'})

            base_chance = max(2, min((current_value / target_value) * 100, 75))
            displayed_chance = round(base_chance, 1)

            price_ratio = target_value / current_value
            real_chance = base_chance

            if target_value > 10000:
                real_chance = base_chance * 0.3
            elif target_value > 5000:
                real_chance = base_chance * 0.4
            elif target_value > 2000:
                real_chance = base_chance * 0.6
            elif target_value > 1000:
                real_chance = base_chance * 0.8

            real_chance = max(5, real_chance)

            logger.info(f"рџЋЇ РЁР°РЅСЃС‹: РѕС‚РѕР±СЂР°Р¶Р°РµРјС‹Р№ {displayed_chance}%, СЂРµР°Р»СЊРЅС‹Р№ {real_chance:.1f}%, С†РµРЅР°: {current_value} -> {target_value}")

            success = random.random() * 100 <= real_chance

            if success:
                cursor.execute('''
                    UPDATE inventory
                    SET gift_id = ?, gift_name = ?, gift_image = ?, gift_value = ?
                    WHERE id = ?
                ''', (target_gift['id'], target_gift['name'], target_gift['image'], target_value, current_gift_id))

                exp_gained = max(5, (target_value - current_value) // 50)
                cursor.execute('UPDATE users SET experience = experience + ? WHERE id = ?', (exp_gained, user_id))

                cursor.execute('''
                    INSERT INTO user_history (user_id, operation_type, amount, description)
                    VALUES (?, 'upgrade_success', 0, ?)
                ''', (user_id, f'РЈСЃРїРµС€РЅС‹Р№ Р°РїРіСЂРµР№Рґ: {gift_name} -> {target_gift["name"]}'))

                cursor.execute('''
                    INSERT INTO win_history (user_id, user_name, gift_name, gift_image, gift_value, case_name)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (user_id, user_name, target_gift['name'], target_gift['image'], target_value, 'Upgrade'))

                logger.info(f"вњ… РЈСЃРїРµС€РЅС‹Р№ Р°РїРіСЂРµР№Рґ: {gift_name} -> {target_gift['name']} (С€Р°РЅСЃ: {real_chance:.1f}%)")
            else:
                cursor.execute('DELETE FROM inventory WHERE id = ?', (current_gift_id,))

                cursor.execute('''
                    INSERT INTO user_history (user_id, operation_type, amount, description)
                    VALUES (?, 'upgrade_failure', 0, ?)
                ''', (user_id, f'РќРµСѓРґР°С‡РЅС‹Р№ Р°РїРіСЂРµР№Рґ: {gift_name}'))

                logger.info(f"вќЊ РќРµСѓРґР°С‡РЅС‹Р№ Р°РїРіСЂРµР№Рґ: {gift_name} (С€Р°РЅСЃ Р±С‹Р»: {real_chance:.1f}%)")

            conn.commit()
            conn.close()

            return jsonify({
                'success': True,
                'upgrade_success': success,
                'chance': displayed_chance,
                'real_chance': round(real_chance, 1),
                'new_gift': target_gift if success else None,
                'message': 'РЈСЃРїРµС€РЅС‹Р№ Р°РїРіСЂРµР№Рґ!' if success else 'РђРїРіСЂРµР№Рґ РЅРµ СѓРґР°Р»СЃСЏ'
            })

        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning("рџ”’ Р‘Р°Р·Р° Р·Р°Р±Р»РѕРєРёСЂРѕРІР°РЅР°, РїРѕРІС‚РѕСЂСЏРµРј Р·Р°РїСЂРѕСЃ...")
                conn.close()
                time.sleep(0.1)
                return upgrade_gift()
            raise e
        except Exception as e:
            conn.rollback()
            logger.error(f"вќЊ РћС€РёР±РєР° С‚СЂР°РЅР·Р°РєС†РёРё: {e}")
            return jsonify({'success': False, 'error': 'РћС€РёР±РєР° РѕР±СЂР°Р±РѕС‚РєРё Р°РїРіСЂРµР№РґР°'})
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° Р°РїРіСЂРµР№РґР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/debug-upgrade/<int:inventory_id>', methods=['GET'])
def debug_upgrade(inventory_id):
    """РћС‚Р»Р°РґРѕС‡РЅР°СЏ РёРЅС„РѕСЂРјР°С†РёСЏ РґР»СЏ Р°РїРіСЂРµР№РґР°"""
    try:
        user_id = request.args.get('user_id')

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM inventory WHERE id = ? AND user_id = ?', (inventory_id, user_id))
        current_gift = cursor.fetchone()

        gifts = build_fragment_first_gifts_catalog()

        conn.close()

        if not current_gift:
            return jsonify({
                'success': False,
                'error': 'РџРѕРґР°СЂРѕРє РЅРµ РЅР°Р№РґРµРЅ',
                'debug_info': {
                    'inventory_id': inventory_id,
                    'user_id': user_id,
                    'total_gifts_loaded': len(gifts) if gifts else 0
                }
            })

        return jsonify({
            'success': True,
            'debug_info': {
                'current_gift': {
                    'inventory_id': current_gift[0],
                    'user_id': current_gift[1],
                    'gift_id': current_gift[2],
                    'gift_name': current_gift[3],
                    'gift_value': current_gift[5]
                },
                'total_gifts_available': len(gifts) if gifts else 0,
                'gifts_sample': [{'id': g['id'], 'name': g['name'], 'value': g.get('value', 0)} for g in gifts[:5]] if gifts else []
            }
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/upgrade-possible-gifts', methods=['POST'])
def get_upgrade_possible_gifts():
    """РџРѕР»СѓС‡РµРЅРёРµ РІРѕР·РјРѕР¶РЅС‹С… РїРѕРґР°СЂРєРѕРІ РґР»СЏ Р°РїРіСЂРµР№РґР° вЂ” С‚РѕР»СЊРєРѕ РѕСЂРёРіРёРЅР°Р»С‹ РєРѕР»Р»РµРєС†РёР№ (Р±РµР· РјРѕРґРµР»РµР№)"""
    try:
        data = request.get_json()
        current_gift_id = data['current_gift_id']
        user_id = data['user_id']

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT gift_value FROM inventory WHERE id = ? AND user_id = ?',
                     (current_gift_id, user_id))
        result = cursor.fetchone()
        conn.close()

        if not result:
            return jsonify({'success': False, 'error': 'РџРѕРґР°СЂРѕРє РЅРµ РЅР°Р№РґРµРЅ'})

        current_value = result[0]
        # Use Fragment catalog вЂ” originals only (no models)
        gifts = build_fragment_first_gifts_catalog()

        if not gifts:
            return jsonify({'success': False, 'error': 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ РїРѕРґР°СЂРєРё'})

        min_target_value = current_value * 1.2
        possible_gifts = []

        for gift in gifts:
            if gift.get('value', 0) > min_target_value:
                base_chance = (current_value / gift['value']) * 100
                displayed_chance = max(2, min(base_chance, 75))

                possible_gifts.append({
                    **gift,
                    'upgrade_chance': round(displayed_chance, 1)
                })

        possible_gifts.sort(key=lambda x: x.get('value', 0))
        possible_gifts = possible_gifts[:15]

        return jsonify({
            'success': True,
            'current_gift_value': current_value,
            'possible_gifts': possible_gifts
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РїРѕРґР°СЂРєРѕРІ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/gifts')
def api_gifts():
    """РџРѕР»СѓС‡РµРЅРёРµ РєР°С‚Р°Р»РѕРіР° РїРѕРґР°СЂРєРѕРІ: СЃРЅР°С‡Р°Р»Р° Fragment, Р·Р°С‚РµРј local-only fallback"""
    global gifts_cache, gifts_cache_time
    try:
        force_reload = request.args.get('reload', '').lower() in ('true', '1', 'yes')
        if force_reload:
            gifts_cache = None
            gifts_cache_time = None
            global fragment_cache, fragment_cache_time
            fragment_cache = None
            fragment_cache_time = None
            logger.info("API gifts: РїСЂРёРЅСѓРґРёС‚РµР»СЊРЅР°СЏ РїРµСЂРµР·Р°РіСЂСѓР·РєР° РєСЌС€Р°")

        file_path = os.path.join(BASE_PATH, 'data', 'gifts.json')
        file_exists = os.path.exists(file_path)

        include_models = request.args.get('models', '0') in ('1', 'true', 'yes')
        if include_models:
            gifts = build_full_catalog_with_models(force_refresh=force_reload)
        else:
            gifts = build_fragment_first_gifts_catalog(force_refresh=force_reload)
        logger.info(f"API gifts: total={len(gifts)} (models={'yes' if include_models else 'no'})")

        return jsonify({
            'success': True,
            'gifts': gifts,
            'total': len(gifts),
            'fragment_only_mode': FRAGMENT_ONLY_CATALOG,
            'fragment_error': fragment_last_error,
            'offline_fallback_used': any(str(g.get('source')) == 'local_offline_fallback' for g in (gifts or [])),
            'debug': {
                'file_path': file_path,
                'file_exists': file_exists,
                'base_path': BASE_PATH,
                'raw_count': len(gifts) if gifts else 0,
                'flow': 'fragment_first_then_local_only'
            }
        })
    except Exception as e:
        logger.error(f"РћС€РёР±РєР° API gifts: {e}")
        import traceback
        return jsonify({
            'success': False, 
            'error': str(e), 
            'gifts': [],
            'traceback': traceback.format_exc()
        })

# LEVEL API
@app.route('/api/level-info/<int:user_id>', methods=['GET'])
def get_level_info(user_id):
    """РџРѕР»СѓС‡РµРЅРёРµ РёРЅС„РѕСЂРјР°С†РёРё РѕР± СѓСЂРѕРІРЅРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
    try:
        level_info = get_user_level_info(user_id)

        if not level_info:
            return jsonify({'success': False, 'error': 'РРЅС„РѕСЂРјР°С†РёСЏ РѕР± СѓСЂРѕРІРЅРµ РЅРµ РЅР°Р№РґРµРЅР°'})

        return jsonify({
            'success': True,
            'level_info': level_info,
            'level_system': LEVEL_SYSTEM
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёРЅС„РѕСЂРјР°С†РёРё РѕР± СѓСЂРѕРІРЅРµ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/level-history/<int:user_id>', methods=['GET'])
def get_level_history(user_id):
    """РџРѕР»СѓС‡РµРЅРёРµ РёСЃС‚РѕСЂРёРё РїРѕРІС‹С€РµРЅРёСЏ СѓСЂРѕРІРЅРµР№"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT old_level, new_level, experience_gained, reason, created_at
            FROM level_history
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT 20
        ''', (user_id,))

        history = cursor.fetchall()
        conn.close()

        history_list = []
        for item in history:
            history_list.append({
                'old_level': item[0],
                'new_level': item[1],
                'experience_gained': item[2],
                'reason': item[3],
                'date': item[4]
            })

        return jsonify({
            'success': True,
            'history': history_list
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёСЃС‚РѕСЂРёРё СѓСЂРѕРІРЅРµР№: {e}")
        return jsonify({'success': False, 'error': str(e)})

# PAYMENT API
@app.route('/api/create-stars-payment', methods=['POST'])
def create_stars_payment():
    """РЎРѕР·РґР°РЅРёРµ РїР»Р°С‚РµР¶Р° С‡РµСЂРµР· Telegram Stars"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        amount = data['amount']

        logger.info(f"в­ђ РЎРѕР·РґР°РЅРёРµ РїР»Р°С‚РµР¶Р° Telegram Stars: РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id}, СЃСѓРјРјР° {amount}")

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO deposits (user_id, amount, currency, status, payment_method)
            VALUES (?, ?, 'stars', 'pending', 'telegram_stars')
        ''', (user_id, amount))

        deposit_id = cursor.lastrowid

        add_history_record(user_id, 'stars_payment_created', 0, f'РЎРѕР·РґР°РЅ РїР»Р°С‚РµР¶ Telegram Stars: {amount} Р·РІРµР·Рґ')

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'вњ… РџР»Р°С‚РµР¶ СЃРѕР·РґР°РЅ! РСЃРїРѕР»СЊР·СѓР№С‚Рµ РєРЅРѕРїРєСѓ РЅРёР¶Рµ РґР»СЏ РѕРїР»Р°С‚С‹ С‡РµСЂРµР· Telegram Stars.',
            'deposit_id': deposit_id,
            'payment_url': f'https://t.me/your_bot_name?start=stars_{deposit_id}'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃРѕР·РґР°РЅРёСЏ РїР»Р°С‚РµР¶Р° Stars: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/complete-stars-payment', methods=['POST'])
def complete_stars_payment():
    """Р—Р°РІРµСЂС€РµРЅРёРµ РїР»Р°С‚РµР¶Р° Telegram Stars"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        deposit_id = data.get('deposit_id')

        if admin_id and int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT user_id, amount, status FROM deposits WHERE id = ?', (deposit_id,))
        deposit = cursor.fetchone()

        if not deposit:
            conn.close()
            return jsonify({'success': False, 'error': 'РџР»Р°С‚РµР¶ РЅРµ РЅР°Р№РґРµРЅ'})

        user_id, amount, status = deposit

        if status == 'completed':
            conn.close()
            return jsonify({'success': False, 'error': 'РџР»Р°С‚РµР¶ СѓР¶Рµ Р·Р°РІРµСЂС€РµРЅ'})

        cursor.execute('UPDATE users SET balance_stars = balance_stars + ?, total_earned_stars = total_earned_stars + ? WHERE id = ?',
                     (amount, amount, user_id))

        add_experience(user_id, amount // 10, f"РџРѕРїРѕР»РЅРµРЅРёРµ Р±Р°Р»Р°РЅСЃР° РЅР° {amount} Р·РІРµР·Рґ")

        cursor.execute('UPDATE deposits SET status = "completed", completed_at = CURRENT_TIMESTAMP WHERE id = ?', (deposit_id,))

        add_history_record(user_id, 'stars_payment_completed', amount, f'РџРѕРїРѕР»РЅРµРЅРёРµ С‡РµСЂРµР· Telegram Stars: {amount} Р·РІРµР·Рґ')

        conn.commit()

        cursor.execute('SELECT balance_stars, balance_tickets FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()
        conn.close()

        logger.info(f"вњ… РџР»Р°С‚РµР¶ Stars #{deposit_id} Р·Р°РІРµСЂС€РµРЅ, РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РїРѕР»СѓС‡РёР» {amount} Р·РІРµР·Рґ")
        return jsonify({
            'success': True,
            'message': f'Р‘Р°Р»Р°РЅСЃ РїРѕРїРѕР»РЅРµРЅ РЅР° {amount} Р·РІРµР·Рґ!',
            'new_balance': {
                'stars': new_balance[0],
                'tickets': new_balance[1]
            }
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° Р·Р°РІРµСЂС€РµРЅРёСЏ РїР»Р°С‚РµР¶Р° Stars: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/check-stars-payment/<int:deposit_id>', methods=['GET'])
def check_stars_payment(deposit_id):
    """РџСЂРѕРІРµСЂРєР° СЃС‚Р°С‚СѓСЃР° РїР»Р°С‚РµР¶Р° Telegram Stars"""
    try:
        user_id = request.args.get('user_id')

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT status, amount, user_id FROM deposits WHERE id = ?', (deposit_id,))
        deposit = cursor.fetchone()

        if not deposit:
            conn.close()
            return jsonify({'success': False, 'error': 'РџР»Р°С‚РµР¶ РЅРµ РЅР°Р№РґРµРЅ'})

        status, amount, deposit_user_id = deposit

        if str(deposit_user_id) != str(user_id):
            conn.close()
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn.close()

        return jsonify({
            'success': True,
            'status': status,
            'amount': amount,
            'message': f'РЎС‚Р°С‚СѓСЃ РїР»Р°С‚РµР¶Р°: {status}'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё РїР»Р°С‚РµР¶Р°: {e}")
        return jsonify({'success': False, 'error': str(e)})

# PROMO CODE API
@app.route('/api/use-promo-code', methods=['POST'])
def use_promo_code():
    """РђРєС‚РёРІР°С†РёСЏ РїСЂРѕРјРѕРєРѕРґР°"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        promo_code = data['promo_code'].upper().strip()

        logger.info(f"рџЋџпёЏ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} Р°РєС‚РёРІРёСЂСѓРµС‚ РїСЂРѕРјРѕРєРѕРґ: {promo_code}")

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, reward_stars, reward_tickets, max_uses, used_count, expires_at, is_active
            FROM promo_codes
            WHERE code = ?
        ''', (promo_code,))

        promo = cursor.fetchone()

        if not promo:
            conn.close()
            return jsonify({'success': False, 'error': 'РџСЂРѕРјРѕРєРѕРґ РЅРµ РЅР°Р№РґРµРЅ'})

        promo_id, reward_stars, reward_tickets, max_uses, used_count, expires_at, is_active = promo

        if not is_active:
            conn.close()
            return jsonify({'success': False, 'error': 'РџСЂРѕРјРѕРєРѕРґ РЅРµР°РєС‚РёРІРµРЅ'})

        if expires_at:
            expires_date = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
            if datetime.now() > expires_date:
                conn.close()
                return jsonify({'success': False, 'error': 'РЎСЂРѕРє РґРµР№СЃС‚РІРёСЏ РїСЂРѕРјРѕРєРѕРґР° РёСЃС‚РµРє'})

        if max_uses > 0 and used_count >= max_uses:
            conn.close()
            return jsonify({'success': False, 'error': 'Р›РёРјРёС‚ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёР№ РїСЂРѕРјРѕРєРѕРґР° РёСЃС‡РµСЂРїР°РЅ'})

        cursor.execute('SELECT id FROM used_promo_codes WHERE user_id = ? AND promo_code_id = ?', (user_id, promo_id))
        already_used = cursor.fetchone()

        if already_used:
            conn.close()
            return jsonify({'success': False, 'error': 'Р’С‹ СѓР¶Рµ РёСЃРїРѕР»СЊР·РѕРІР°Р»Рё СЌС‚РѕС‚ РїСЂРѕРјРѕРєРѕРґ'})

        if reward_stars > 0:
            cursor.execute('UPDATE users SET balance_stars = balance_stars + ?, total_earned_stars = total_earned_stars + ? WHERE id = ?',
                         (reward_stars, reward_stars, user_id))

        if reward_tickets > 0:
            cursor.execute('UPDATE users SET balance_tickets = balance_tickets + ?, total_earned_tickets = total_earned_tickets + ? WHERE id = ?',
                         (reward_tickets, reward_tickets, user_id))

        cursor.execute('UPDATE users SET experience = experience + 25 WHERE id = ?', (user_id,))

        cursor.execute('UPDATE promo_codes SET used_count = used_count + 1 WHERE id = ?', (promo_id,))

        cursor.execute('INSERT INTO used_promo_codes (user_id, promo_code_id) VALUES (?, ?)', (user_id, promo_id))

        rewards_text = []
        if reward_stars > 0:
            rewards_text.append(f'{reward_stars}в­ђ')
        if reward_tickets > 0:
            rewards_text.append(f'{reward_tickets}рџЋ«')

        cursor.execute('''
            INSERT INTO user_history (user_id, operation_type, amount, description)
            VALUES (?, 'promo_code', ?, ?)
        ''', (user_id, reward_stars + reward_tickets, f'РђРєС‚РёРІР°С†РёСЏ РїСЂРѕРјРѕРєРѕРґР° {promo_code}: {", ".join(rewards_text)}'))

        conn.commit()
        conn.close()

        logger.info(f"вњ… РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} Р°РєС‚РёРІРёСЂРѕРІР°Р» РїСЂРѕРјРѕРєРѕРґ {promo_code}")
        return jsonify({
            'success': True,
            'message': f'РџСЂРѕРјРѕРєРѕРґ Р°РєС‚РёРІРёСЂРѕРІР°РЅ! Р’С‹ РїРѕР»СѓС‡РёР»Рё: {reward_stars}в­ђ Рё {reward_tickets}рџЋ«',
            'rewards': {
                'stars': reward_stars,
                'tickets': reward_tickets
            }
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° Р°РєС‚РёРІР°С†РёРё РїСЂРѕРјРѕРєРѕРґР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

# ==================== ULTIMATE CRASH API ====================

@app.route('/api/ultimate-crash/status', methods=['GET'])
def ultimate_crash_status():
    """РџРѕР»СѓС‡РµРЅРёРµ СЃС‚Р°С‚СѓСЃР° Ultimate Crash"""
    try:
        user_id = request.args.get('user_id')

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, status, current_multiplier, target_multiplier,
                   start_time, created_at
            FROM ultimate_crash_games
            WHERE status IN ('waiting', 'counting', 'flying')
            ORDER BY id DESC LIMIT 1
        ''')

        game = cursor.fetchone()

        if not game:
            target_multiplier = round(random.uniform(3.0, 10.0), 2)
            cursor.execute('''
                INSERT INTO ultimate_crash_games (status, target_multiplier, start_time)
                VALUES ('waiting', ?, CURRENT_TIMESTAMP)
            ''', (target_multiplier,))
            game_id = cursor.lastrowid
            conn.commit()

            cursor.execute('''
                SELECT id, status, current_multiplier, target_multiplier,
                       start_time, created_at
                FROM ultimate_crash_games
                WHERE id = ?
            ''', (game_id,))
            game = cursor.fetchone()

        game_id, status, current_mult, target_mult, start_time, created_at = game

        cursor.execute('''
            SELECT
                ucb.id,
                ucb.user_id,
                ucb.bet_amount,
                ucb.status,
                ucb.cashout_multiplier,
                ucb.win_amount,
                ucb.created_at,
                u.first_name,
                u.username,
                u.photo_url
            FROM ultimate_crash_bets ucb
            LEFT JOIN users u ON ucb.user_id = u.id
            WHERE ucb.game_id = ? AND ucb.status = 'active'
            ORDER BY ucb.created_at DESC
        ''', (game_id,))

        bets = cursor.fetchall()

        user_bet = None
        if user_id:
            cursor.execute('''
                SELECT * FROM ultimate_crash_bets
                WHERE game_id = ? AND user_id = ? AND status = 'active'
                ORDER BY created_at DESC LIMIT 1
            ''', (game_id, user_id))
            user_bet_data = cursor.fetchone()

            if user_bet_data:
                user_bet = {
                    'id': user_bet_data[0],
                    'game_id': user_bet_data[1],
                    'user_id': user_bet_data[2],
                    'bet_amount': user_bet_data[3],
                    'gift_value': user_bet_data[4],
                    'status': user_bet_data[5],
                    'cashout_multiplier': user_bet_data[6],
                    'win_amount': user_bet_data[7],
                    'created_at': user_bet_data[8]
                }

        conn.close()

        bets_list = []
        for bet in bets:
            if len(bet) >= 10:
                bet_data = {
                    'id': bet[0],
                    'user_id': bet[1],
                    'bet_amount': bet[2],
                    'status': bet[3],
                    'cashout_multiplier': float(bet[4]) if bet[4] else None,
                    'win_amount': bet[5],
                    'created_at': bet[6],
                    'first_name': bet[7],
                    'username': bet[8],
                    'photo_url': bet[9] or '/static/img/default_avatar.png',
                    'user_name': bet[7] or f'РРіСЂРѕРє {bet[1]}'
                }
                bets_list.append(bet_data)

        game_data = {
            'id': game_id,
            'status': status,
            'current_multiplier': float(current_mult) if current_mult else 1.0,
            'target_multiplier': float(target_mult) if target_mult else 5.0,
            'start_time': start_time,
            'created_at': created_at
        }

        return jsonify({
            'success': True,
            'game': game_data,
            'active_bets': bets_list,
            'user_bet': user_bet
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЃС‚Р°С‚СѓСЃР° Ultimate Crash: {e}")
        logger.error(f"вќЊ РўСЂР°СЃСЃРёСЂРѕРІРєР°: {traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/ultimate-crash/bet', methods=['POST'])
def ultimate_crash_bet():
    """Р Р°Р·РјРµС‰РµРЅРёРµ СЃС‚Р°РІРєРё РІ Ultimate Crash"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        bet_amount = data.get('bet_amount', 0)

        logger.info(f"рџЋЇ РЎС‚Р°РІРєР° Ultimate Crash: user {user_id}, СЃСѓРјРјР° {bet_amount}")

        if not user_id:
            return jsonify({'success': False, 'error': 'ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РЅРµ СѓРєР°Р·Р°РЅ'})

        if bet_amount < 10:
            return jsonify({'success': False, 'error': 'РњРёРЅРёРјР°Р»СЊРЅР°СЏ СЃС‚Р°РІРєР° 25в­ђ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        if user[0] < bet_amount:
            conn.close()
            return jsonify({'success': False, 'error': 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ Р·РІРµР·Рґ'})

        cursor.execute('''
            SELECT id, status FROM ultimate_crash_games
            WHERE status = 'waiting'
            ORDER BY id DESC LIMIT 1
        ''')

        game = cursor.fetchone()

        if not game:
            conn.close()
            return jsonify({'success': False, 'error': 'РќРµС‚ Р°РєС‚РёРІРЅС‹С… РёРіСЂ РґР»СЏ СЃС‚Р°РІРѕРє'})

        game_id, game_status = game

        if game_status != 'waiting':
            conn.close()
            return jsonify({'success': False, 'error': 'РРіСЂР° СѓР¶Рµ РЅР°С‡Р°Р»Р°СЃСЊ'})

        cursor.execute('UPDATE users SET balance_stars = balance_stars - ? WHERE id = ?',
                     (bet_amount, user_id))

        cursor.execute('''
            INSERT INTO ultimate_crash_bets (game_id, user_id, bet_amount, gift_value, status)
            VALUES (?, ?, ?, ?, 'active')
        ''', (game_id, user_id, bet_amount, bet_amount))

        bet_id = cursor.lastrowid

        add_history_record(user_id, 'ultimate_crash_bet', -bet_amount,
                         f'РЎС‚Р°РІРєР° РІ Ultimate Crash: {bet_amount}в­ђ')

        conn.commit()
        conn.close()

        logger.info(f"вњ… РЎС‚Р°РІРєР° СЂР°Р·РјРµС‰РµРЅР°: {bet_amount} (ID: {bet_id})")

        return jsonify({
            'success': True,
            'bet_id': bet_id,
            'game_id': game_id,
            'message': 'РЎС‚Р°РІРєР° СЂР°Р·РјРµС‰РµРЅР°!'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃС‚Р°РІРєРё Ultimate Crash: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/ultimate-crash/cashout', methods=['POST'])
def ultimate_crash_cashout():
    """Р—Р°Р±СЂР°С‚СЊ РІС‹РёРіСЂС‹С€ РІ Ultimate Crash"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        cashout_multiplier = data.get('cashout_multiplier', 1.0)

        if not user_id:
            return jsonify({'success': False, 'error': 'ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РЅРµ СѓРєР°Р·Р°РЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, current_multiplier FROM ultimate_crash_games
            WHERE status = 'flying'
            ORDER BY id DESC LIMIT 1
        ''')

        game = cursor.fetchone()

        if not game:
            conn.close()
            return jsonify({'success': False, 'error': 'РќРµС‚ Р°РєС‚РёРІРЅРѕР№ РёРіСЂС‹'})

        game_id, current_mult = game[0], float(game[1]) if game[1] else 1.0

        cursor.execute('''
            SELECT id, bet_amount FROM ultimate_crash_bets
            WHERE game_id = ? AND user_id = ? AND status = 'active'
            ORDER BY created_at DESC LIMIT 1
        ''', (game_id, user_id))

        bet = cursor.fetchone()

        if not bet:
            conn.close()
            return jsonify({'success': False, 'error': 'РђРєС‚РёРІРЅР°СЏ СЃС‚Р°РІРєР° РЅРµ РЅР°Р№РґРµРЅР°'})

        bet_id, bet_amount = bet

        final_multiplier = min(cashout_multiplier, current_mult)
        win_amount = int(bet_amount * final_multiplier)

        cursor.execute('''
            UPDATE ultimate_crash_bets
            SET status = 'cashed_out',
                cashout_multiplier = ?,
                win_amount = ?
            WHERE id = ?
        ''', (final_multiplier, win_amount, bet_id))

        cursor.execute('''
            UPDATE users
            SET balance_stars = balance_stars + ?,
                total_earned_stars = total_earned_stars + ?
            WHERE id = ?
        ''', (win_amount, win_amount, user_id))

        exp_gained = max(5, win_amount // 100)
        cursor.execute('UPDATE users SET experience = experience + ? WHERE id = ?',
                     (exp_gained, user_id))

        add_history_record(user_id, 'ultimate_crash_win', win_amount,
                         f'Р’С‹РёРіСЂС‹С€ РІ Ultimate Crash: x{final_multiplier:.2f}')

        conn.commit()
        conn.close()

        logger.info(f"вњ… РљСЌС€Р°СѓС‚: {win_amount} (x{final_multiplier:.2f})")

        return jsonify({
            'success': True,
            'win_amount': win_amount,
            'multiplier': final_multiplier,
            'message': f'Р’С‹ РІС‹РёРіСЂР°Р»Рё {win_amount}в­ђ!'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РєСЌС€Р°СѓС‚Р° Ultimate Crash: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/ultimate-crash/game-state', methods=['GET'])
def ultimate_crash_game_state():
    """РџРѕР»РЅРѕРµ СЃРѕСЃС‚РѕСЏРЅРёРµ РёРіСЂС‹ СЃ СѓС‡РµС‚РѕРј РІСЂРµРјРµРЅРё (СЃ РѕР±СЂР°Р±РѕС‚РєРѕР№ Р±Р»РѕРєРёСЂРѕРІРѕРє)"""
    max_retries = 3
    retry_delay = 0.1

    for attempt in range(max_retries):
        try:
            user_id = request.args.get('user_id')

            conn = get_db_connection()
            cursor = conn.cursor()

            # Read game state (no need for IMMEDIATE on reads)
            cursor.execute('''
                SELECT id, status, current_multiplier, target_multiplier,
                       start_time, created_at
                FROM ultimate_crash_games
                WHERE status IN ('waiting', 'counting', 'flying')
                ORDER BY id DESC LIMIT 1
            ''')

            game = cursor.fetchone()

            if not game:
                target_multiplier = generate_extreme_crash_multiplier()
                cursor.execute('''
                    INSERT INTO ultimate_crash_games (status, target_multiplier, start_time)
                    VALUES ('waiting', ?, datetime('now'))
                ''', (target_multiplier,))
                game_id = cursor.lastrowid
                conn.commit()

                cursor.execute('''
                    SELECT id, status, current_multiplier, target_multiplier,
                           start_time, created_at
                    FROM ultimate_crash_games
                    WHERE id = ?
                ''', (game_id,))
                game = cursor.fetchone()

            game_id, status, current_mult, target_mult, start_time, created_at = game

            # РЈРїСЂРѕС‰РµРЅРЅР°СЏ РѕР±СЂР°Р±РѕС‚РєР° РІСЂРµРјРµРЅРё
            import time as time_module

            if start_time:
                try:
                    # РџСЂРµРѕР±СЂР°Р·СѓРµРј СЃС‚СЂРѕРєСѓ РІСЂРµРјРµРЅРё РІ timestamp
                    if isinstance(start_time, str):
                        # РЈР±РёСЂР°РµРј РјРёР»Р»РёСЃРµРєСѓРЅРґС‹ РµСЃР»Рё РµСЃС‚СЊ
                        if '.' in start_time:
                            start_time = start_time.split('.')[0]

                        # РџР°СЂСЃРёРј РІСЂРµРјСЏ
                        try:
                            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        except:
                            # РџСЂРѕР±СѓРµРј РґСЂСѓРіРёРµ С„РѕСЂРјР°С‚С‹
                            try:
                                start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                            except:
                                start_dt = datetime.now()
                    else:
                        start_dt = datetime.now()

                    start_timestamp = time_module.mktime(start_dt.timetuple())
                except Exception as e:
                    logger.error(f"РћС€РёР±РєР° РїР°СЂСЃРёРЅРіР° РІСЂРµРјРµРЅРё: {e}")
                    start_timestamp = time_module.time() - 30
            else:
                start_timestamp = time_module.time() - 30

            current_time = time_module.time()
            elapsed = current_time - start_timestamp

            time_remaining = 0
            next_phase = status

            # Р¤Р°Р·Р° 1: РћР¶РёРґР°РЅРёРµ (15 СЃРµРєСѓРЅРґ)
            if status == 'waiting':
                time_remaining = max(0, 15 - elapsed)
                if time_remaining <= 0:
                    next_phase = 'counting'
                    cursor.execute('UPDATE ultimate_crash_games SET status = "counting" WHERE id = ?', (game_id,))
                    conn.commit()
                    time_remaining = 5
            # Р¤Р°Р·Р° 2: РћС‚СЃС‡РµС‚ (5 СЃРµРєСѓРЅРґ)
            elif status == 'counting':
                time_remaining = max(0, 5 - (elapsed - 15))
                if time_remaining <= 0:
                    next_phase = 'flying'
                    cursor.execute('UPDATE ultimate_crash_games SET status = "flying" WHERE id = ?', (game_id,))
                    conn.commit()
                    time_remaining = 30  # РњР°РєСЃРёРјР°Р»СЊРЅРѕРµ РІСЂРµРјСЏ РїРѕР»РµС‚Р°
            # Р¤Р°Р·Р° 3: РџРѕР»РµС‚
            elif status == 'flying':
                current_mult_float = float(current_mult) if current_mult else 1.0
                target_mult_float = float(target_mult) if target_mult else 5.0

                # Р Р°СЃС‡РµС‚ РѕСЃС‚Р°РІС€РµРіРѕСЃСЏ РІСЂРµРјРµРЅРё РїРѕР»РµС‚Р° СЃ СЂР°Р·РЅРѕР№ СЃРєРѕСЂРѕСЃС‚СЊСЋ
                if current_mult_float >= target_mult_float:
                    time_remaining = 0
                elif current_mult_float < 1.5:
                    # 5 СЃРµРєСѓРЅРґ РґРѕ 1.5x
                    progress = (current_mult_float - 1.0) / 0.5
                    time_remaining = (1.5 - current_mult_float) * (5 / 0.5)
                elif current_mult_float < 2.0:
                    # 3 СЃРµРєСѓРЅРґС‹ РѕС‚ 1.5 РґРѕ 2.0
                    progress = 1.0 + (current_mult_float - 1.5) / 0.5
                    time_remaining = (2.0 - current_mult_float) * (3 / 0.5)
                elif current_mult_float < 4.0:
                    # 6 СЃРµРєСѓРЅРґ РѕС‚ 2.0 РґРѕ 4.0
                    progress = 2.0 + (current_mult_float - 2.0) / 2.0
                    time_remaining = (4.0 - current_mult_float) * (6 / 2.0)
                else:
                    # Р‘С‹СЃС‚СЂРµРµ РїРѕСЃР»Рµ 4.0
                    progress = 3.0 + (current_mult_float - 4.0)
                    time_remaining = (target_mult_float - current_mult_float) * 1.5

                time_remaining = max(0.1, time_remaining)

                # РњРµРґР»РµРЅРЅРѕ СѓРІРµР»РёС‡РёРІР°РµРј РјРЅРѕР¶РёС‚РµР»СЊ
                if current_mult_float < target_mult_float:
                    increment = 0.02  # Р‘Р°Р·РѕРІРѕРµ СѓРІРµР»РёС‡РµРЅРёРµ

                    # Р Р°Р·РЅР°СЏ СЃРєРѕСЂРѕСЃС‚СЊ РЅР° СЂР°Р·РЅС‹С… РёРЅС‚РµСЂРІР°Р»Р°С…
                    if current_mult_float < 1.5:
                        increment = 0.02 * (5 / 15)  # РњРµРґР»РµРЅРЅРµРµ
                    elif current_mult_float < 2.0:
                        increment = 0.016 * (3 / 15)  # РЎСЂРµРґРЅСЏСЏ СЃРєРѕСЂРѕСЃС‚СЊ
                    elif current_mult_float < 4.0:
                        increment = 0.033 * (6 / 15)  # Р‘С‹СЃС‚СЂРµРµ
                    else:
                        increment = 0.066 * (2 / 15)  # РћС‡РµРЅСЊ Р±С‹СЃС‚СЂРѕ

                    # РЈС‡РёС‚С‹РІР°РµРј РІСЂРµРјСЏ РјРµР¶РґСѓ Р·Р°РїСЂРѕСЃР°РјРё
                    time_since_last_update = min(elapsed, 2.0)  # РњР°РєСЃ 2 СЃРµРєСѓРЅРґС‹
                    increment = increment * time_since_last_update * 10  # РњР°СЃС€С‚Р°Р±РёСЂСѓРµРј

                    new_multiplier = round(current_mult_float + increment, 2)
                    if new_multiplier > target_mult_float:
                        new_multiplier = target_mult_float

                    cursor.execute('UPDATE ultimate_crash_games SET current_multiplier = ? WHERE id = ?',
                                 (new_multiplier, game_id))
                    conn.commit()

            user_bet = None
            if user_id:
                cursor.execute('''
                    SELECT id, bet_amount, status FROM ultimate_crash_bets
                    WHERE game_id = ? AND user_id = ? AND status = 'active'
                    ORDER BY created_at DESC LIMIT 1
                ''', (game_id, user_id))

                bet = cursor.fetchone()

                if bet:
                    user_bet = {
                        'id': bet[0],
                        'bet_amount': bet[1],
                        'status': bet[2]
                    }

            conn.close()

            game_data = {
                'id': game_id,
                'status': next_phase,
                'current_multiplier': float(current_mult) if current_mult else 1.0,
                'target_multiplier': float(target_mult) if target_mult else 5.0,
                'time_remaining': round(time_remaining, 1),
                'can_bet': next_phase == 'waiting'
            }

            return jsonify({
                'success': True,
                'game': game_data,
                'user_bet': user_bet
            })

        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                logger.warning(f"рџ”’ Р‘Р°Р·Р° Р·Р°Р±Р»РѕРєРёСЂРѕРІР°РЅР°, РїРѕРІС‚РѕСЂРЅР°СЏ РїРѕРїС‹С‚РєР° {attempt + 1}/{max_retries}")
                try:
                    if 'conn' in locals():
                        conn.close()
                except:
                    pass
                time.sleep(retry_delay * (attempt + 1))  # Р­РєСЃРїРѕРЅРµРЅС†РёР°Р»СЊРЅР°СЏ Р·Р°РґРµСЂР¶РєР°
                continue
            else:
                logger.error(f"вќЊ РљСЂРёС‚РёС‡РµСЃРєР°СЏ РѕС€РёР±РєР° Р±Р°Р·С‹ РґР°РЅРЅС‹С…: {e}")
                return jsonify({'success': False, 'error': 'РћС€РёР±РєР° РґРѕСЃС‚СѓРїР° Рє Р±Р°Р·Рµ РґР°РЅРЅС‹С…'})

        except Exception as e:
            logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЃРѕСЃС‚РѕСЏРЅРёСЏ РёРіСЂС‹: {e}")
            logger.error(f"вќЊ РўСЂР°СЃСЃРёСЂРѕРІРєР°: {traceback.format_exc()}")
            try:
                if 'conn' in locals():
                    conn.close()
            except:
                pass
            return jsonify({'success': False, 'error': str(e)})

@app.route('/api/ultimate-crash/user-bet', methods=['GET'])
def get_user_ultimate_crash_bet():
    """РџРѕР»СѓС‡РµРЅРёРµ Р°РєС‚РёРІРЅРѕР№ СЃС‚Р°РІРєРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
    try:
        user_id = request.args.get('user_id')

        if not user_id:
            return jsonify({'success': False, 'error': 'ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РЅРµ СѓРєР°Р·Р°РЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id FROM ultimate_crash_games
            WHERE status IN ('waiting', 'counting', 'flying')
            ORDER BY id DESC LIMIT 1
        ''')
        game = cursor.fetchone()

        user_bet = None
        if game:
            game_id = game[0]

            cursor.execute('''
                SELECT * FROM ultimate_crash_bets
                WHERE game_id = ? AND user_id = ? AND status = 'active'
                ORDER BY created_at DESC LIMIT 1
            ''', (game_id, user_id))

            bet = cursor.fetchone()

            if bet:
                user_bet = {
                    'id': bet[0],
                    'game_id': bet[1],
                    'user_id': bet[2],
                    'bet_amount': bet[3],
                    'gift_value': bet[4],
                    'status': bet[5],
                    'cashout_multiplier': float(bet[6]) if bet[6] else None,
                    'win_amount': bet[7],
                    'created_at': bet[8]
                }

        conn.close()

        return jsonify({
            'success': True,
            'user_bet': user_bet
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЃС‚Р°РІРєРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/ultimate-crash/place-bet-final', methods=['POST'])
def place_bet_final():
    """Р Р°Р·РјРµС‰РµРЅРёРµ СЃС‚Р°РІРєРё СЃ РїСЂРѕРІРµСЂРєРѕР№ Р±Р°Р»Р°РЅСЃР°"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        bet_amount = data.get('bet_amount', 0)

        logger.info(f"рџЋЇ РЎС‚Р°РІРєР° Ultimate Crash: user {user_id}, СЃСѓРјРјР° {bet_amount}")

        if not user_id:
            return jsonify({'success': False, 'error': 'ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РЅРµ СѓРєР°Р·Р°РЅ'})

        if bet_amount < 10:
            return jsonify({'success': False, 'error': 'РњРёРЅРёРјР°Р»СЊРЅР°СЏ СЃС‚Р°РІРєР° 25'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        current_balance = user[0] or 0

        if current_balance < bet_amount:
            conn.close()
            return jsonify({'success': False, 'error': f'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ СЃСЂРµРґСЃС‚РІ. Р‘Р°Р»Р°РЅСЃ: {current_balance}'})

        cursor.execute('''
            SELECT id, status FROM ultimate_crash_games
            WHERE status = 'waiting'
            ORDER BY id DESC LIMIT 1
        ''')

        game = cursor.fetchone()

        if not game:
            target_multiplier = round(random.uniform(3.0, 10.0), 2)
            cursor.execute('''
                INSERT INTO ultimate_crash_games (status, target_multiplier, start_time)
                VALUES ('waiting', ?, CURRENT_TIMESTAMP)
            ''', (target_multiplier,))
            game_id = cursor.lastrowid
            game_status = 'waiting'
        else:
            game_id, game_status = game

        if game_status not in ('waiting', 'counting'):
            conn.close()
            return jsonify({'success': False, 'error': 'РРіСЂР° СѓР¶Рµ РЅР°С‡Р°Р»Р°СЃСЊ'})

        # РџСЂРѕРІРµСЂСЏРµРј РІСЂРµРјСЏ РґРѕ СЃС‚Р°СЂС‚Р°
        cached = get_crash_cache()
        if cached.get('status') == 'counting' and cached.get('time_remaining', 5) < 0.5:
            conn.close()
            return jsonify({'success': False, 'error': 'РЎР»РёС€РєРѕРј РїРѕР·РґРЅРѕ! РЎС‚Р°РІРєР° РЅР° СЃР»РµРґ. СЂР°СѓРЅРґ'})

        cursor.execute('''
            SELECT id FROM ultimate_crash_bets
            WHERE game_id = ? AND user_id = ? AND status = 'active'
        ''', (game_id, user_id))

        existing_bet = cursor.fetchone()

        if existing_bet:
            conn.close()
            return jsonify({'success': False, 'error': 'РЈ РІР°СЃ СѓР¶Рµ РµСЃС‚СЊ Р°РєС‚РёРІРЅР°СЏ СЃС‚Р°РІРєР°'})

        cursor.execute('UPDATE users SET balance_stars = balance_stars - ?, total_crash_bets = COALESCE(total_crash_bets, 0) + 1, total_bet_volume = COALESCE(total_bet_volume, 0) + ? WHERE id = ?',
                     (bet_amount, bet_amount, user_id))

        cursor.execute('''
            INSERT INTO ultimate_crash_bets (game_id, user_id, bet_amount, gift_value, status)
            VALUES (?, ?, ?, ?, 'active')
        ''', (game_id, user_id, bet_amount, bet_amount))

        bet_id = cursor.lastrowid

        add_history_record(user_id, 'ultimate_crash_bet', -bet_amount,
                         f'РЎС‚Р°РІРєР° РІ Ultimate Crash: {bet_amount}')

        conn.commit()

        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()[0]

        conn.close()

        logger.info(f"вњ… РЎС‚Р°РІРєР° СЂР°Р·РјРµС‰РµРЅР°: {bet_amount} (ID: {bet_id})")

        return jsonify({
            'success': True,
            'bet_id': bet_id,
            'game_id': game_id,
            'new_balance': new_balance,
            'message': f'РЎС‚Р°РІРєР° {bet_amount} РїСЂРёРЅСЏС‚Р°!'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃС‚Р°РІРєРё: {e}")
        try: conn.close()
        except: pass
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/ultimate-crash/cashout-final', methods=['POST'])
def cashout_final():
    """Р—Р°Р±СЂР°С‚СЊ РІС‹РёРіСЂС‹С€ СЃ Р·Р°РїРёСЃСЊСЋ РІ РёСЃС‚РѕСЂРёСЋ"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')

        if not user_id:
            return jsonify({'success': False, 'error': 'ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РЅРµ СѓРєР°Р·Р°РЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, current_multiplier FROM ultimate_crash_games
            WHERE status = 'flying'
            ORDER BY id DESC LIMIT 1
        ''')

        game = cursor.fetchone()

        if not game:
            conn.close()
            return jsonify({'success': False, 'error': 'РќРµС‚ Р°РєС‚РёРІРЅРѕР№ РёРіСЂС‹'})

        game_id, current_mult = game[0], float(game[1]) if game[1] else 1.0

        cursor.execute('''
            SELECT id, bet_amount FROM ultimate_crash_bets
            WHERE game_id = ? AND user_id = ? AND status = 'active'
            ORDER BY created_at DESC LIMIT 1
        ''', (game_id, user_id))

        bet = cursor.fetchone()

        if not bet:
            conn.close()
            return jsonify({'success': False, 'error': 'РђРєС‚РёРІРЅР°СЏ СЃС‚Р°РІРєР° РЅРµ РЅР°Р№РґРµРЅР°'})

        bet_id, bet_amount = bet

        win_amount = int(bet_amount * current_mult)

        cursor.execute('''
            UPDATE ultimate_crash_bets
            SET status = 'cashed_out',
                cashout_multiplier = ?,
                win_amount = ?
            WHERE id = ?
        ''', (current_mult, win_amount, bet_id))

        cursor.execute('''
            UPDATE users
            SET balance_stars = balance_stars + ?,
                total_earned_stars = total_earned_stars + ?
            WHERE id = ?
        ''', (win_amount, win_amount, user_id))

        exp_gained = max(5, win_amount // 100)
        add_experience(user_id, exp_gained, f"Р’С‹РёРіСЂС‹С€ РІ Ultimate Crash x{current_mult:.2f}")

        add_history_record(user_id, 'ultimate_crash_win', win_amount,
                         f'Р’С‹РёРіСЂС‹С€ РІ Ultimate Crash: x{current_mult:.2f}')

        cursor.execute('SELECT first_name FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()
        user_name = user[0] if user else f'User_{user_id}'

        cursor.execute('''
            INSERT INTO win_history (user_id, user_name, gift_name, gift_image, gift_value, case_name)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, user_name, f'Р’С‹РёРіСЂС‹С€ РІ Crash x{current_mult:.2f}',
              '/static/img/star.png', win_amount, 'Ultimate Crash'))

        cursor.execute('''
            INSERT INTO case_open_history (user_id, case_id, case_name, gift_id, gift_name, gift_image, gift_value, cost, cost_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, 0, 'Ultimate Crash', 0, f'Р’С‹РёРіСЂС‹С€ x{current_mult:.2f}',
              '/static/img/star.png', win_amount, bet_amount, 'stars'))

        conn.commit()

        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()[0]

        conn.close()

        logger.info(f"вњ… РљСЌС€Р°СѓС‚: {win_amount} (x{current_mult:.2f})")

        return jsonify({
            'success': True,
            'win_amount': win_amount,
            'multiplier': current_mult,
            'new_balance': new_balance,
            'message': f'Р’С‹ РІС‹РёРіСЂР°Р»Рё {win_amount}!'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РєСЌС€Р°СѓС‚Р°: {e}")
        try: conn.close()
        except: pass
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/ultimate-crash/history', methods=['GET'])
def get_ultimate_crash_history_api():
    """РџРѕР»СѓС‡РµРЅРёРµ РёСЃС‚РѕСЂРёРё РјРЅРѕР¶РёС‚РµР»РµР№"""
    try:
        limit = request.args.get('limit', 10, type=int)

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, final_multiplier, finished_at
            FROM ultimate_crash_history
            ORDER BY finished_at DESC
            LIMIT ?
        ''', (limit,))

        history = cursor.fetchall()
        conn.close()

        history_list = []
        for item in history:
            history_list.append({
                'id': item[0],
                'final_multiplier': float(item[1]),
                'finished_at': item[2]
            })

        logger.info(f"рџ“Љ РћС‚РїСЂР°РІР»РµРЅРѕ {len(history_list)} Р·Р°РїРёСЃРµР№ РёСЃС‚РѕСЂРёРё")
        return jsonify({
            'success': True,
            'history': history_list
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёСЃС‚РѕСЂРёРё: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'history': []
        })


@app.route('/api/ultimate-crash/quick-status', methods=['GET'])
def ultimate_crash_quick_status():
    """Р‘С‹СЃС‚СЂС‹Р№ СЃС‚Р°С‚СѓСЃ Р±РµР· Р±Р»РѕРєРёСЂРѕРІРѕРє Р±Р°Р·С‹ РґР°РЅРЅС‹С…"""
    try:
        # РСЃРїРѕР»СЊР·СѓРµРј РєСЌС€РёСЂРѕРІР°РЅРёРµ РёР»Рё С„Р°Р№Р»РѕРІСѓСЋ СЃРёСЃС‚РµРјСѓ РґР»СЏ РјРёРЅРёРјР°Р»СЊРЅРѕР№ Р±Р»РѕРєРёСЂРѕРІРєРё
        status_file = os.path.join(BASE_PATH, 'data', 'crash_status.json')

        # РџС‹С‚Р°РµРјСЃСЏ РїСЂРѕС‡РёС‚Р°С‚СЊ РёР· С„Р°Р№Р»Р°
        if os.path.exists(status_file):
            try:
                with open(status_file, 'r', encoding='utf-8') as f:
                    cached_status = json.load(f)

                # РџСЂРѕРІРµСЂСЏРµРј, РЅРµ СѓСЃС‚Р°СЂРµР»Рё Р»Рё РґР°РЅРЅС‹Рµ (РјР°РєСЃРёРјСѓРј 2 СЃРµРєСѓРЅРґС‹)
                cache_time = cached_status.get('timestamp', 0)
                current_time = time.time()

                if current_time - cache_time < 2:  # 2 СЃРµРєСѓРЅРґС‹ РєСЌС€
                    return jsonify({
                        'success': True,
                        'game': cached_status.get('game', {
                            'id': 1,
                            'status': 'waiting',
                            'current_multiplier': 1.0,
                            'target_multiplier': 5.0,
                            'time_remaining': 10.0
                        }),
                        'cached': True
                    })
            except:
                pass

        # Р•СЃР»Рё РєСЌС€ СѓСЃС‚Р°СЂРµР» РёР»Рё РµРіРѕ РЅРµС‚, РїРѕР»СѓС‡Р°РµРј РёР· Р±Р°Р·С‹ СЃ Р±С‹СЃС‚СЂС‹Рј СЃРѕРµРґРёРЅРµРЅРёРµРј
        conn = sqlite3.connect(os.path.join(BASE_PATH, 'data', 'raswet_gifts.db'), timeout=5)
        conn.execute("PRAGMA busy_timeout = 5000")
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, status, current_multiplier, target_multiplier
            FROM ultimate_crash_games
            WHERE status IN ('waiting', 'counting', 'flying')
            ORDER BY id DESC LIMIT 1
        ''')

        game = cursor.fetchone()
        conn.close()

        if game:
            game_id, status, current_mult, target_mult = game

            # РџСЂРѕСЃС‚РѕР№ СЂР°СЃС‡РµС‚ РІСЂРµРјРµРЅРё
            time_remaining = 10.0
            if status == 'counting':
                time_remaining = 5.0
            elif status == 'flying':
                current_mult_float = float(current_mult) if current_mult else 1.0
                target_mult_float = float(target_mult) if target_mult else 5.0
                time_remaining = max(1.0, (target_mult_float - current_mult_float) * 2)

            game_data = {
                'id': game_id,
                'status': status,
                'current_multiplier': float(current_mult) if current_mult else 1.0,
                'target_multiplier': float(target_mult) if target_mult else 5.0,
                'time_remaining': round(time_remaining, 1)
            }
        else:
            # Р”РµРјРѕ-РґР°РЅРЅС‹Рµ
            game_data = {
                'id': 1,
                'status': 'waiting',
                'current_multiplier': 1.0,
                'target_multiplier': 5.0,
                'time_remaining': 10.0
            }

        # РљСЌС€РёСЂСѓРµРј СЂРµР·СѓР»СЊС‚Р°С‚
        try:
            cache_data = {
                'timestamp': time.time(),
                'game': game_data
            }
            with open(status_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f)
        except:
            pass

        return jsonify({
            'success': True,
            'game': game_data,
            'cached': False
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° quick-status: {e}")
        # Р’СЃРµРіРґР° РІРѕР·РІСЂР°С‰Р°РµРј СѓСЃРїРµС… СЃ РґРµРјРѕ-РґР°РЅРЅС‹РјРё
        return jsonify({
            'success': True,
            'game': {
                'id': 1,
                'status': 'waiting',
                'current_multiplier': 1.0,
                'target_multiplier': 5.0,
                'time_remaining': 10.0
            },
            'error': 'РСЃРїРѕР»СЊР·СѓСЋС‚СЃСЏ РґРµРјРѕ-РґР°РЅРЅС‹Рµ'
        })

@app.route('/api/ultimate-crash/recent-bets', methods=['GET'])
def get_recent_ultimate_crash_bets():
    """РџРѕР»СѓС‡РµРЅРёРµ СЃС‚Р°РІРѕРє С‚РµРєСѓС‰РµРіРѕ СЂР°СѓРЅРґР°"""
    try:
        limit = request.args.get('limit', 20, type=int)

        conn = get_db_connection()
        cursor = conn.cursor()

        # РџСЂРµРґРїРѕС‡РёС‚Р°РµРј Р°РєС‚РёРІРЅСѓСЋ РёРіСЂСѓ; РµСЃР»Рё РµС‘ РЅРµС‚, Р±РµСЂС‘Рј РїРѕСЃР»РµРґРЅСЋСЋ
        cursor.execute('''
            SELECT id, status, current_multiplier
            FROM ultimate_crash_games
            WHERE status IN ('waiting', 'counting', 'flying')
            ORDER BY id DESC LIMIT 1
        ''')
        current_game = cursor.fetchone()
        if not current_game:
            cursor.execute('''
                SELECT id, status, current_multiplier FROM ultimate_crash_games
                ORDER BY id DESC LIMIT 1
            ''')
            current_game = cursor.fetchone()

        current_game_id = current_game[0] if current_game else 0
        current_game_status = current_game[1] if current_game else None
        current_game_mult = float(current_game[2]) if current_game and current_game[2] else 1.0

        # Self-heal: РµСЃР»Рё Р±РѕС‚С‹ РЅРµ СЃРіРµРЅРµСЂРёСЂРѕРІР°РЅС‹ РґР»СЏ Р°РєС‚РёРІРЅРѕРіРѕ СЂР°СѓРЅРґР°, СЃРѕР·РґР°С‘Рј РёС… on-demand
        if current_game_id and current_game_status in ('waiting', 'counting', 'flying'):
            if not _crash_bots_cache.get('loaded'):
                _load_crash_bots()
            if _crash_bots_cache.get('enabled') and current_game_id not in _crash_bots_active:
                cursor.execute('SELECT COUNT(*) FROM ultimate_crash_bets WHERE game_id = ?', (current_game_id,))
                real_count = cursor.fetchone()[0] or 0
                _generate_bot_bets(current_game_id, real_count)

        cursor.execute('''
            SELECT
                ucb.id,
                ucb.user_id,
                ucb.bet_amount,
                ucb.status,
                ucb.cashout_multiplier,
                ucb.win_amount,
                ucb.created_at,
                u.first_name,
                u.username,
                u.photo_url
            FROM ultimate_crash_bets ucb
            LEFT JOIN users u ON ucb.user_id = u.id
            WHERE ucb.game_id = ?
            ORDER BY ucb.created_at DESC
            LIMIT ?
        ''', (current_game_id, limit,))

        bets = cursor.fetchall()
        conn.close()

        bets_list = []
        for bet in bets:
            bets_list.append({
                'id': bet[0],
                'user_id': bet[1],
                'bet_amount': bet[2],
                'status': bet[3],
                'cashout_multiplier': float(bet[4]) if bet[4] else None,
                'win_amount': bet[5],
                'created_at': bet[6],
                'first_name': bet[7],
                'username': bet[8],
                'photo_url': bet[9] or '/static/img/default_avatar.png'
            })

        # Bots disabled вЂ” skip fallback and bot bets
        # bot_bets = _get_bot_bets_for_api(current_game_id)
        # bets_list.extend(bot_bets)

        return jsonify({
            'success': True,
            'bets': bets_list
        })

    except Exception as e:
        # РќРµ Р»РѕРіРёСЂСѓРµРј РєР°Р¶РґСѓСЋ РѕС€РёР±РєСѓ - СЃР»РёС€РєРѕРј РјРЅРѕРіРѕ СЃРїР°РјР°
        return jsonify({
            'success': True,
            'bets': []
        })

# ==================== ADMIN API ====================

@app.route('/api/admin/crash/status', methods=['GET'])
def admin_crash_status():
    """Get crash game admin control status"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        game_cache = get_crash_cache()
        admin_ctrl = get_admin_crash_control()
        
        return jsonify({
            'success': True,
            'game': game_cache,
            'admin_control': admin_ctrl
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/crash/force-crash', methods=['POST'])
def admin_force_crash():
    """Force crash the current game"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        game_cache = get_crash_cache()
        if game_cache.get('status') != 'flying':
            return jsonify({'success': False, 'error': 'РРіСЂР° РЅРµ РІ РїРѕР»С‘С‚Рµ'})
        
        set_admin_crash_control('force_crash', True)
        logger.info(f"рџЋ® ADMIN {admin_id} triggered force crash")
        
        return jsonify({
            'success': True,
            'message': 'РљСЂР°С€ Р°РєС‚РёРІРёСЂРѕРІР°РЅ!',
            'current_multiplier': game_cache.get('current_multiplier')
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/crash/set-next', methods=['POST'])
def admin_set_next_multiplier():
    """Set multiplier for the next game"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        multiplier = data.get('multiplier')
        if not multiplier or float(multiplier) < 1.01:
            return jsonify({'success': False, 'error': 'РњРЅРѕР¶РёС‚РµР»СЊ РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ >= 1.01'})
        
        set_admin_crash_control('next_multiplier', float(multiplier))
        logger.info(f"рџЋ® ADMIN {admin_id} set next multiplier: {multiplier}x")
        
        return jsonify({
            'success': True,
            'message': f'РЎР»РµРґСѓСЋС‰РёР№ РєСЂР°С€ Р±СѓРґРµС‚ РЅР° {multiplier}x'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/crash/set-range', methods=['POST'])
def admin_set_multiplier_range():
    """Set custom multiplier range"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        min_mult = float(data.get('min', 1.0))
        max_mult = float(data.get('max', 50.0))
        enabled = data.get('enabled', True)
        
        if min_mult < 1.01:
            min_mult = 1.01
        if max_mult < min_mult:
            max_mult = min_mult + 1
        
        global _admin_crash_control
        with _admin_control_lock:
            _admin_crash_control['multiplier_min'] = min_mult
            _admin_crash_control['multiplier_max'] = max_mult
            _admin_crash_control['use_custom_range'] = enabled
        
        logger.info(f"рџЋ® ADMIN {admin_id} set range: {min_mult}x - {max_mult}x (enabled: {enabled})")
        
        return jsonify({
            'success': True,
            'message': f'Р”РёР°РїР°Р·РѕРЅ {min_mult}x - {max_mult}x ({"Р’РљР›" if enabled else "Р’Р«РљР›"})'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/crash/toggle-manual', methods=['POST'])
def admin_toggle_manual():
    """Toggle manual control mode"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        admin_ctrl = get_admin_crash_control()
        new_state = not admin_ctrl.get('manual_mode', False)
        set_admin_crash_control('manual_mode', new_state)
        
        logger.info(f"рџЋ® ADMIN {admin_id} manual mode: {new_state}")
        
        return jsonify({
            'success': True,
            'manual_mode': new_state,
            'message': f'Р СѓС‡РЅРѕР№ СЂРµР¶РёРј {"Р’РљР›" if new_state else "Р’Р«РљР›"}'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# в”Ђв”Ђв”Ђ CRASH BOTS ADMIN API в”Ђв”Ђв”Ђ

@app.route('/api/admin/crash-bots/settings', methods=['GET', 'POST'])
def api_admin_crash_bots_settings():
    """Get or update crash bots global settings"""
    try:
        admin_id = request.args.get('admin_id') or (request.get_json() or {}).get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        if request.method == 'GET':
            return jsonify({
                'success': True,
                'enabled': _crash_bots_cache.get('enabled', False),
                'settings': _crash_bots_cache.get('settings', {}),
                'bots': _crash_bots_cache.get('bots', []),
                'active_game_bots': {str(k): len(v) for k, v in _crash_bots_active.items()},
            })

        # POST вЂ” update settings
        data = request.get_json()
        enabled = data.get('enabled')
        min_bots = data.get('min_active_bots')
        max_bots = data.get('max_active_bots')
        threshold = data.get('min_real_players_threshold')

        conn = get_db_connection()
        conn.execute('''INSERT INTO crash_bots_settings (id, enabled, min_active_bots, max_active_bots, min_real_players_threshold, updated_at)
            VALUES (1, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                enabled = COALESCE(excluded.enabled, enabled),
                min_active_bots = COALESCE(excluded.min_active_bots, min_active_bots),
                max_active_bots = COALESCE(excluded.max_active_bots, max_active_bots),
                min_real_players_threshold = COALESCE(excluded.min_real_players_threshold, min_real_players_threshold),
                updated_at = CURRENT_TIMESTAMP
        ''', (
            1 if enabled else 0 if enabled is not None else None,
            min_bots, max_bots, threshold
        ))
        conn.commit()
        conn.close()
        _load_crash_bots()
        return jsonify({'success': True, 'message': 'РќР°СЃС‚СЂРѕР№РєРё Р±РѕС‚РѕРІ РѕР±РЅРѕРІР»РµРЅС‹'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/crash-bots/list', methods=['GET'])
def api_admin_crash_bots_list():
    """List all crash bots"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        return jsonify({
            'success': True,
            'bots': _crash_bots_cache.get('bots', []),
            'enabled': _crash_bots_cache.get('enabled', False),
            'settings': _crash_bots_cache.get('settings', {}),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/crash-bots/add', methods=['POST'])
def api_admin_crash_bots_add():
    """Add a new crash bot"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        bot_name = data.get('bot_name', '').strip()
        if not bot_name:
            # Generate random name
            if random.random() < 0.5:
                bot_name = random.choice(_BOT_NAMES_RU)
            else:
                bot_name = random.choice(_BOT_NAMES_EN)

        avatar_url = data.get('avatar_url', '').strip()
        if not avatar_url:
            avatar_url = random.choice(_BOT_AVATARS)

        min_bet = int(data.get('min_bet', 25))
        max_bet = int(data.get('max_bet', 500))
        cashout_min = float(data.get('auto_cashout_min', 1.2))
        cashout_max = float(data.get('auto_cashout_max', 5.0))

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''INSERT INTO crash_bots_config (bot_name, avatar_url, min_bet, max_bet, auto_cashout_min, auto_cashout_max, is_active)
            VALUES (?, ?, ?, ?, ?, ?, 1)''',
            (bot_name, avatar_url, min_bet, max_bet, cashout_min, cashout_max))
        bot_id = cursor.lastrowid
        conn.commit()
        conn.close()
        _load_crash_bots()
        return jsonify({'success': True, 'bot_id': bot_id, 'message': f'Р‘РѕС‚ "{bot_name}" РґРѕР±Р°РІР»РµРЅ'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/crash-bots/update', methods=['POST'])
def api_admin_crash_bots_update():
    """Update an existing crash bot"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        bot_id = data.get('bot_id')
        if not bot_id:
            return jsonify({'success': False, 'error': 'bot_id required'})

        conn = get_db_connection()
        updates = []
        params = []
        for field, col in [('bot_name','bot_name'), ('avatar_url','avatar_url'),
                           ('min_bet','min_bet'), ('max_bet','max_bet'),
                           ('auto_cashout_min','auto_cashout_min'), ('auto_cashout_max','auto_cashout_max'),
                           ('is_active','is_active')]:
            if field in data:
                updates.append(f'{col} = ?')
                params.append(data[field])
        if updates:
            params.append(bot_id)
            conn.execute(f'UPDATE crash_bots_config SET {", ".join(updates)} WHERE id = ?', params)
            conn.commit()
        conn.close()
        _load_crash_bots()
        return jsonify({'success': True, 'message': 'Р‘РѕС‚ РѕР±РЅРѕРІР»С‘РЅ'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/crash-bots/delete', methods=['POST'])
def api_admin_crash_bots_delete():
    """Delete a crash bot"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        bot_id = data.get('bot_id')
        conn = get_db_connection()
        conn.execute('DELETE FROM crash_bots_config WHERE id = ?', (bot_id,))
        conn.commit()
        conn.close()
        _load_crash_bots()
        return jsonify({'success': True, 'message': 'Р‘РѕС‚ СѓРґР°Р»С‘РЅ'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/crash-bots/generate', methods=['POST'])
def api_admin_crash_bots_generate():
    """Generate multiple bots at once"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        count = min(int(data.get('count', 100)), 200)
        min_bet = int(data.get('min_bet', 25))
        max_bet = int(data.get('max_bet', 500))
        cashout_min = float(data.get('auto_cashout_min', 1.2))
        cashout_max = float(data.get('auto_cashout_max', 5.0))

        conn = get_db_connection()
        used_names = set()
        all_names = _BOT_NAMES_RU + _BOT_NAMES_EN
        random.shuffle(all_names)
        created = 0
        for i in range(count):
            if i < len(all_names):
                name = all_names[i]
            else:
                name = random.choice(_BOT_NAMES_EN) + str(random.randint(10, 99))
            if name in used_names:
                name = name + str(random.randint(1, 9))
            used_names.add(name)
            avatar = _BOT_AVATARS[i % len(_BOT_AVATARS)]
            conn.execute('''INSERT INTO crash_bots_config (bot_name, avatar_url, min_bet, max_bet, auto_cashout_min, auto_cashout_max, is_active)
                VALUES (?, ?, ?, ?, ?, ?, 1)''',
                (name, avatar, min_bet, max_bet, cashout_min, cashout_max))
            created += 1
        conn.commit()
        conn.close()
        _load_crash_bots()
        return jsonify({'success': True, 'created': created, 'message': f'РЎРѕР·РґР°РЅРѕ {created} Р±РѕС‚РѕРІ'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/users', methods=['GET'])
def get_all_users():
    """РџРѕР»СѓС‡РµРЅРёРµ СЃРїРёСЃРєР° РІСЃРµС… РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№"""
    try:
        # Admin check is optional for convenience (admin page itself is protected)
        limit = request.args.get('limit', 500, type=int)
        offset = request.args.get('offset', 0, type=int)

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, first_name, username, balance_stars, balance_tickets,
                   referral_count, created_at, total_earned_stars, total_earned_tickets,
                   experience, current_level, total_cases_opened, photo_url
            FROM users
            ORDER BY balance_stars DESC
            LIMIT ? OFFSET ?
        ''', (limit, offset))
        users = cursor.fetchall()
        conn.close()

        users_list = []
        for user in users:
            users_list.append({
                'id': user[0],
                'first_name': user[1],
                'username': user[2],
                'balance_stars': user[3],
                'balance_tickets': user[4],
                'referral_count': user[5],
                'created_at': user[6],
                'total_earned_stars': user[7] or 0,
                'total_earned_tickets': user[8] or 0,
                'experience': user[9] or 0,
                'level': user[10] or 1,
                'total_cases_opened': user[11] or 0,
                'photo_url': user[12] or ''
            })

        return jsonify({'success': True, 'users': users_list})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЃРїРёСЃРєР° РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/stats', methods=['GET'])
def get_admin_stats():
    """РџРѕР»СѓС‡РµРЅРёРµ СЃС‚Р°С‚РёСЃС‚РёРєРё РґР»СЏ Р°РґРјРёРЅ-РїР°РЅРµР»Рё"""
    try:
        # Admin check optional

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0]

        cursor.execute('SELECT SUM(balance_stars) FROM users')
        total_stars = cursor.fetchone()[0] or 0

        cursor.execute('SELECT SUM(balance_tickets) FROM users')
        total_tickets = cursor.fetchone()[0] or 0

        cursor.execute('SELECT COUNT(*) FROM inventory')
        total_inventory = cursor.fetchone()[0]

        cursor.execute('SELECT status, COUNT(*) FROM withdrawals GROUP BY status')
        withdrawal_stats = cursor.fetchall()

        withdrawal_counts = {}
        for status, count in withdrawal_stats:
            withdrawal_counts[status] = count

        cursor.execute('SELECT COUNT(*) FROM referrals')
        total_referrals = cursor.fetchone()[0]

        cursor.execute('SELECT status, COUNT(*) FROM deposits GROUP BY status')
        deposit_stats = cursor.fetchall()

        deposit_counts = {}
        for status, count in deposit_stats:
            deposit_counts[status] = count

        cursor.execute('SELECT COUNT(*) FROM promo_codes')
        total_promos = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM used_promo_codes')
        total_promo_uses = cursor.fetchone()[0]

        cursor.execute('SELECT AVG(current_level), MAX(current_level) FROM users')
        level_stats = cursor.fetchone()
        avg_level = level_stats[0] or 1
        max_level = level_stats[1] or 1

        cursor.execute('SELECT SUM(total_cases_opened) FROM users')
        total_cases_opened = cursor.fetchone()[0] or 0

        conn.close()

        return jsonify({
            'success': True,
            'stats': {
                'total_users': total_users,
                'total_stars': total_stars,
                'total_tickets': total_tickets,
                'total_inventory': total_inventory,
                'total_referrals': total_referrals,
                'total_promos': total_promos,
                'total_promo_uses': total_promo_uses,
                'total_cases_opened': total_cases_opened,
                'average_level': round(avg_level, 2),
                'max_level': max_level,
                'withdrawals': withdrawal_counts,
                'deposits': deposit_counts
            }
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЃС‚Р°С‚РёСЃС‚РёРєРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/used-promos', methods=['GET'])
def get_admin_used_promos():
    """РџРѕР»СѓС‡РµРЅРёРµ РёСЃРїРѕР»СЊР·РѕРІР°РЅРЅС‹С… РїСЂРѕРјРѕРєРѕРґРѕРІ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id, case_id, promo_code, used_at FROM used_promo_codes ORDER BY used_at DESC LIMIT 100')
        rows = cursor.fetchall()
        conn.close()
        
        promos = [{'user_id': r[0], 'case_id': r[1], 'promo_code': r[2], 'used_at': r[3]} for r in rows]
        return jsonify({'success': True, 'promos': promos})
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РїСЂРѕРјРѕРєРѕРґРѕРІ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/stats-optimized', methods=['GET'])
def get_admin_stats_optimized():
    """РћРїС‚РёРјРёР·РёСЂРѕРІР°РЅРЅР°СЏ СЃС‚Р°С‚РёСЃС‚РёРєР° РґР»СЏ Р°РґРјРёРЅ-РїР°РЅРµР»Рё"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0]

        cursor.execute('SELECT SUM(balance_stars), SUM(balance_tickets) FROM users')
        stars_tickets = cursor.fetchone()
        total_stars, total_tickets = stars_tickets[0] or 0, stars_tickets[1] or 0

        cursor.execute('SELECT COUNT(*) FROM inventory')
        total_inventory = cursor.fetchone()[0]

        cursor.execute('''
            SELECT
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved,
                SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) as rejected
            FROM withdrawals
        ''')
        withdrawal_stats = cursor.fetchone()

        cursor.execute('SELECT AVG(current_level), MAX(current_level) FROM users')
        level_stats = cursor.fetchone()

        conn.close()

        return jsonify({
            'success': True,
            'stats': {
                'total_users': total_users,
                'total_stars': total_stars,
                'total_tickets': total_tickets,
                'total_inventory': total_inventory,
                'withdrawals': {
                    'pending': withdrawal_stats[0] or 0,
                    'approved': withdrawal_stats[1] or 0,
                    'rejected': withdrawal_stats[2] or 0
                },
                'average_level': round(level_stats[0] or 1, 2),
                'max_level': level_stats[1] or 1
            }
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РѕРїС‚РёРјРёР·РёСЂРѕРІР°РЅРЅРѕР№ СЃС‚Р°С‚РёСЃС‚РёРєРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/set-balance', methods=['POST'])
def admin_set_balance():
    """РЈСЃС‚Р°РЅРѕРІРєР° С‚РѕС‡РЅРѕРіРѕ Р±Р°Р»Р°РЅСЃР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        target_user_id = data.get('user_id')
        stars = data.get('stars', 0)
        tickets = data.get('tickets', 0)

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT first_name FROM users WHERE id = ?', (target_user_id,))
        user = cursor.fetchone()

        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        cursor.execute('SELECT balance_stars, balance_tickets FROM users WHERE id = ?', (target_user_id,))
        old_balance = cursor.fetchone()

        cursor.execute('UPDATE users SET balance_stars = ?, balance_tickets = ? WHERE id = ?',
                     (stars, tickets, target_user_id))

        stars_diff = stars - old_balance[0]
        tickets_diff = tickets - old_balance[1]

        add_history_record(target_user_id, 'admin_set_balance',
                         stars_diff,
                         f'РђРґРјРёРЅ СѓСЃС‚Р°РЅРѕРІРёР» Р±Р°Р»Р°РЅСЃ: {stars}в­ђ Рё {tickets}рџЋ« (Р±С‹Р»Рѕ: {old_balance[0]}в­ђ Рё {old_balance[1]}рџЋ«)')

        conn.commit()
        conn.close()

        logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} СѓСЃС‚Р°РЅРѕРІРёР» Р±Р°Р»Р°РЅСЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {target_user_id}: {stars}в­ђ Рё {tickets}рџЋ«")
        return jsonify({
            'success': True,
            'message': f'Р‘Р°Р»Р°РЅСЃ СѓСЃС‚Р°РЅРѕРІР»РµРЅ: {stars}в­ђ Рё {tickets}рџЋ«'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СѓСЃС‚Р°РЅРѕРІРєРё Р±Р°Р»Р°РЅСЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/update-balance', methods=['POST'])
def admin_update_balance():
    """РћР±РЅРѕРІР»РµРЅРёРµ Р±Р°Р»Р°РЅСЃР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        target_user_id = data.get('user_id')
        stars = data.get('stars', 0)
        tickets = data.get('tickets', 0)
        operation = data.get('operation', 'add')

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT first_name FROM users WHERE id = ?', (target_user_id,))
        user = cursor.fetchone()

        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        if operation == 'add':
            cursor.execute('UPDATE users SET balance_stars = balance_stars + ?, balance_tickets = balance_tickets + ? WHERE id = ?',
                         (stars, tickets, target_user_id))
            operation_text = 'РЅР°С‡РёСЃР»РµРЅРѕ'
        else:
            cursor.execute('UPDATE users SET balance_stars = balance_stars - ?, balance_tickets = balance_tickets - ? WHERE id = ?',
                         (stars, tickets, target_user_id))
            operation_text = 'СЃРїРёСЃР°РЅРѕ'

        add_history_record(target_user_id, 'admin_operation',
                         stars if operation == 'add' else -stars,
                         f'РђРґРјРёРЅ РѕРїРµСЂР°С†РёСЏ: {operation_text} {stars}в­ђ Рё {tickets}рџЋ«')

        conn.commit()
        conn.close()

        logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} РёР·РјРµРЅРёР» Р±Р°Р»Р°РЅСЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {target_user_id}: {operation_text} {stars}в­ђ Рё {tickets}рџЋ«")
        return jsonify({
            'success': True,
            'message': f'Р‘Р°Р»Р°РЅСЃ РѕР±РЅРѕРІР»РµРЅ: {operation_text} {stars}в­ђ Рё {tickets}рџЋ«'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РѕР±РЅРѕРІР»РµРЅРёСЏ Р±Р°Р»Р°РЅСЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/add-inventory-item', methods=['POST'])
def admin_add_inventory_item():
    """Р”РѕР±Р°РІР»РµРЅРёРµ РїРѕРґР°СЂРєР° РІ РёРЅРІРµРЅС‚Р°СЂСЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ (Р°РґРјРёРЅ)"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        user_id = data.get('user_id')
        gift_id = data.get('gift_id')
        quantity = data.get('quantity', 1)

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        gifts = load_gifts_cached()
        gift = next((g for g in gifts if g.get('id') == gift_id), None)

        # Also try Fragment catalog if not found in local gifts
        if not gift:
            frag_gifts = build_full_catalog_with_models()
            gift = next((g for g in frag_gifts if g.get('id') == gift_id or g.get('gift_key') == str(gift_id) or g.get('fragment_slug') == str(gift_id)), None)

        if not gift:
            return jsonify({'success': False, 'error': 'РџРѕРґР°СЂРѕРє РЅРµ РЅР°Р№РґРµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # РџСЂРѕРІРµСЂСЏРµРј С‡С‚Рѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ СЃСѓС‰РµСЃС‚РІСѓРµС‚
        cursor.execute('SELECT id FROM users WHERE id = ?', (user_id,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        for _ in range(min(quantity, 100)):
            cursor.execute('''
                INSERT INTO inventory (user_id, gift_id, gift_name, gift_image, gift_value)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, gift_id, gift['name'], gift.get('image', ''), gift['value']))

        conn.commit()
        conn.close()

        logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} РґРѕР±Р°РІРёР» {quantity}x '{gift['name']}' РІ РёРЅРІРµРЅС‚Р°СЂСЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {user_id}")
        return jsonify({
            'success': True,
            'message': f'Р”РѕР±Р°РІР»РµРЅРѕ {quantity}x {gift["name"]} РІ РёРЅРІРµРЅС‚Р°СЂСЊ'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РґРѕР±Р°РІР»РµРЅРёСЏ РІ РёРЅРІРµРЅС‚Р°СЂСЊ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/remove-inventory-item', methods=['POST'])
def admin_remove_inventory_item():
    """РЈРґР°Р»РµРЅРёРµ РїСЂРµРґРјРµС‚Р° РёР· РёРЅРІРµРЅС‚Р°СЂСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ (Р°РґРјРёРЅ)"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        inventory_id = data.get('inventory_id')

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT id, gift_name FROM inventory WHERE id = ?', (inventory_id,))
        item = cursor.fetchone()

        if not item:
            conn.close()
            return jsonify({'success': False, 'error': 'РџСЂРµРґРјРµС‚ РЅРµ РЅР°Р№РґРµРЅ'})

        cursor.execute('DELETE FROM inventory WHERE id = ?', (inventory_id,))
        conn.commit()
        conn.close()

        logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} СѓРґР°Р»РёР» РїСЂРµРґРјРµС‚ #{inventory_id} РёР· РёРЅРІРµРЅС‚Р°СЂСЏ")
        return jsonify({
            'success': True,
            'message': 'РџСЂРµРґРјРµС‚ СѓРґР°Р»РµРЅ РёР· РёРЅРІРµРЅС‚Р°СЂСЏ'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СѓРґР°Р»РµРЅРёСЏ РёР· РёРЅРІРµРЅС‚Р°СЂСЏ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/reset-case-limits', methods=['POST'])
def admin_reset_all_case_limits():
    """РЎР±СЂРѕСЃ РІСЃРµС… Р»РёРјРёС‚РѕРІ РєРµР№СЃРѕРІ (Р°РґРјРёРЅ)"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        cases = load_cases()
        for case in cases:
            if case.get('limited'):
                case['current_amount'] = 0

        save_cases(cases)

        logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} СЃР±СЂРѕСЃРёР» РІСЃРµ Р»РёРјРёС‚С‹ РєРµР№СЃРѕРІ")
        return jsonify({
            'success': True,
            'message': 'Р’СЃРµ Р»РёРјРёС‚С‹ РєРµР№СЃРѕРІ СЃР±СЂРѕС€РµРЅС‹'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃР±СЂРѕСЃР° Р»РёРјРёС‚РѕРІ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/user-inventory', methods=['GET'])
def admin_get_user_inventory():
    """РџРѕР»СѓС‡РµРЅРёРµ РёРЅРІРµРЅС‚Р°СЂСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РґР»СЏ Р°РґРјРёРЅРєРё"""
    try:
        user_id = request.args.get('user_id')
        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, gift_id, gift_name, gift_image, gift_value, acquired_at
            FROM inventory WHERE user_id = ? ORDER BY acquired_at DESC
        ''', (user_id,))
        items = cursor.fetchall()
        conn.close()

        # Р—Р°РіСЂСѓР¶Р°РµРј РїРѕРґР°СЂРєРё РґР»СЏ РїРѕР»СѓС‡РµРЅРёСЏ slug
        gifts = load_gifts_cached()
        gifts_map = {g['id']: g for g in gifts}

        inventory = []
        for item in items:
            g = gifts_map.get(item[1], {})
            inventory.append({
                'id': item[0],
                'gift_id': item[1],
                'gift_name': item[2],
                'gift_image_url': item[3],
                'value': item[4] or 0,
                'slug': g.get('slug', ''),
                'acquired_at': item[5]
            })

        return jsonify({'success': True, 'inventory': inventory})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёРЅРІРµРЅС‚Р°СЂСЏ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/update-user', methods=['POST'])
def admin_update_user():
    """РћР±РЅРѕРІР»РµРЅРёРµ РґР°РЅРЅС‹С… РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ (Р±Р°Р»Р°РЅСЃ, СѓСЂРѕРІРµРЅСЊ)"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        balance_stars = data.get('balance_stars')
        level = data.get('level')

        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # РџСЂРѕРІРµСЂСЏРµРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
        cursor.execute('SELECT id FROM users WHERE id = ?', (user_id,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})

        updates = []
        params = []
        if balance_stars is not None:
            updates.append('balance_stars = ?')
            params.append(int(balance_stars))
        if level is not None:
            updates.append('current_level = ?')
            params.append(int(level))

        if updates:
            params.append(user_id)
            cursor.execute(f'UPDATE users SET {", ".join(updates)} WHERE id = ?', params)
            conn.commit()

        conn.close()
        logger.info(f"рџ› пёЏ РђРґРјРёРЅ РѕР±РЅРѕРІРёР» РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {user_id}: balance={balance_stars}, level={level}")
        return jsonify({'success': True, 'message': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РѕР±РЅРѕРІР»РµРЅ'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РѕР±РЅРѕРІР»РµРЅРёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/notifications', methods=['GET', 'POST', 'PUT', 'DELETE'])
def admin_notifications_management():
    """РЈРїСЂР°РІР»РµРЅРёРµ РѕРїРѕРІРµС‰РµРЅРёСЏРјРё (Р°РґРјРёРЅ)"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # РЎРѕР·РґР°РµРј С‚Р°Р±Р»РёС†Сѓ РµСЃР»Рё РЅРµ СЃСѓС‰РµСЃС‚РІСѓРµС‚
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                width INTEGER DEFAULT 80,
                pages TEXT DEFAULT '[]',
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()

        if request.method == 'GET':
            cursor.execute('SELECT * FROM notifications ORDER BY created_at DESC')
            notifications = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            result = []
            for n in notifications:
                item = dict(zip(columns, n))
                try:
                    item['pages'] = json.loads(item.get('pages', '[]'))
                except:
                    item['pages'] = []
                result.append(item)
            conn.close()
            return jsonify({'success': True, 'notifications': result})

        elif request.method == 'POST':
            data = request.get_json()
            title = data.get('title', '')
            width = data.get('width', 80)
            pages = json.dumps(data.get('pages', []))

            cursor.execute('''
                INSERT INTO notifications (title, width, pages)
                VALUES (?, ?, ?)
            ''', (title, width, pages))
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'message': 'РћРїРѕРІРµС‰РµРЅРёРµ СЃРѕР·РґР°РЅРѕ'})

        elif request.method == 'PUT':
            data = request.get_json()
            notif_id = data.get('id')
            title = data.get('title', '')
            width = data.get('width', 80)
            pages = json.dumps(data.get('pages', []))

            cursor.execute('''
                UPDATE notifications SET title = ?, width = ?, pages = ?
                WHERE id = ?
            ''', (title, width, pages, notif_id))
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'message': 'РћРїРѕРІРµС‰РµРЅРёРµ РѕР±РЅРѕРІР»РµРЅРѕ'})

        elif request.method == 'DELETE':
            data = request.get_json()
            notif_id = data.get('id')
            cursor.execute('DELETE FROM notifications WHERE id = ?', (notif_id,))
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'message': 'РћРїРѕРІРµС‰РµРЅРёРµ СѓРґР°Р»РµРЅРѕ'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СѓРїСЂР°РІР»РµРЅРёСЏ РѕРїРѕРІРµС‰РµРЅРёСЏРјРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/create-notification', methods=['POST'])
def admin_create_notification():
    """РЎРѕР·РґР°РЅРёРµ РѕРїРѕРІРµС‰РµРЅРёСЏ (Р°Р»РёР°СЃ)"""
    try:
        admin_id = request.args.get('admin_id') or (request.get_json() or {}).get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        data = request.get_json()
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                width INTEGER DEFAULT 80,
                pages TEXT DEFAULT '[]',
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        title = data.get('title', '')
        width = data.get('width', 80)
        pages = json.dumps(data.get('pages', []))

        cursor.execute('''
            INSERT INTO notifications (title, width, pages)
            VALUES (?, ?, ?)
        ''', (title, width, pages))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'РћРїРѕРІРµС‰РµРЅРёРµ СЃРѕР·РґР°РЅРѕ'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃРѕР·РґР°РЅРёСЏ РѕРїРѕРІРµС‰РµРЅРёСЏ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/toggle-notification', methods=['POST'])
def admin_toggle_notification():
    """РџРµСЂРµРєР»СЋС‡РµРЅРёРµ СЃС‚Р°С‚СѓСЃР° РѕРїРѕРІРµС‰РµРЅРёСЏ"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        notification_id = data.get('notification_id')
        is_active = data.get('is_active', False)

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE notifications SET is_active = ? WHERE id = ?
        ''', (1 if is_active else 0, notification_id))
        conn.commit()
        conn.close()

        return jsonify({'success': True, 'message': f'РћРїРѕРІРµС‰РµРЅРёРµ {"Р°РєС‚РёРІРёСЂРѕРІР°РЅРѕ" if is_active else "РґРµР°РєС‚РёРІРёСЂРѕРІР°РЅРѕ"}'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРµСЂРµРєР»СЋС‡РµРЅРёСЏ РѕРїРѕРІРµС‰РµРЅРёСЏ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/gifts-management', methods=['GET', 'POST', 'PUT', 'DELETE'])
def admin_gifts_management():
    """РЈРїСЂР°РІР»РµРЅРёРµ РїРѕРґР°СЂРєР°РјРё (Р°РґРјРёРЅ)"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        gifts = load_gifts()

        if request.method == 'GET':
            return jsonify({'success': True, 'gifts': gifts})

        elif request.method == 'POST':
            data = request.get_json()
            new_id = max([g['id'] for g in gifts], default=0) + 1
            new_gift = {
                'id': new_id,
                'name': data.get('name', ''),
                'value': data.get('value', 0),
                'image': data.get('image', ''),
                'category': data.get('category', 'other'),
                'description': data.get('description', '')
            }
            gifts.append(new_gift)
            save_gifts(gifts)
            global gifts_cache, gifts_cache_time
            gifts_cache = None
            gifts_cache_time = None
            return jsonify({'success': True, 'message': 'РџРѕРґР°СЂРѕРє СЃРѕР·РґР°РЅ', 'gift': new_gift})

        elif request.method == 'PUT':
            data = request.get_json()
            gift_id = data.get('id')
            for i, g in enumerate(gifts):
                if str(g['id']) == str(gift_id):
                    gifts[i]['name'] = data.get('name', g['name'])
                    gifts[i]['value'] = data.get('value', g['value'])
                    gifts[i]['image'] = data.get('image', g['image'])
                    gifts[i]['category'] = data.get('category', g.get('category', 'other'))
                    gifts[i]['description'] = data.get('description', g.get('description', ''))
                    break
            save_gifts(gifts)
            gifts_cache = None
            gifts_cache_time = None
            return jsonify({'success': True, 'message': 'РџРѕРґР°СЂРѕРє РѕР±РЅРѕРІР»РµРЅ'})

        elif request.method == 'DELETE':
            data = request.get_json()
            gift_id = data.get('id')
            gifts = [g for g in gifts if str(g['id']) != str(gift_id)]
            save_gifts(gifts)
            gifts_cache = None
            gifts_cache_time = None
            return jsonify({'success': True, 'message': 'РџРѕРґР°СЂРѕРє СѓРґР°Р»РµРЅ'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СѓРїСЂР°РІР»РµРЅРёСЏ РїРѕРґР°СЂРєР°РјРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/withdrawals', methods=['GET'])
def get_withdrawals():
    """РџРѕР»СѓС‡РµРЅРёРµ СЃРїРёСЃРєР° Р·Р°СЏРІРѕРє РЅР° РІС‹РІРѕРґ (РґР»СЏ Р°РґРјРёРЅР°)"""
    try:
        admin_id = request.args.get('admin_id')
        status = request.args.get('status', 'all')

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        if status == 'all':
            cursor.execute('''
                SELECT * FROM withdrawals
                ORDER BY
                    CASE status
                        WHEN 'pending' THEN 1
                        WHEN 'processing' THEN 2
                        WHEN 'approved' THEN 3
                        WHEN 'rejected' THEN 4
                        WHEN 'error' THEN 5
                        ELSE 6
                    END,
                    created_at DESC
            ''')
        else:
            cursor.execute('''
                SELECT * FROM withdrawals
                WHERE status = ?
                ORDER BY created_at DESC
            ''', (status,))

        withdrawals = cursor.fetchall()
        conn.close()

        withdrawals_list = []
        for w in withdrawals:
            withdrawals_list.append({
                'id': w[0],
                'user_id': w[1],
                'inventory_id': w[2],
                'gift_name': w[3],
                'gift_image': w[4],
                'gift_value': w[5],
                'status': w[6],
                'telegram_username': w[7],
                'user_photo_url': w[8],
                'user_first_name': w[9],
                'created_at': w[10],
                'processed_at': w[11],
                'admin_notes': w[12]
            })

        return jsonify({'success': True, 'withdrawals': withdrawals_list})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ Р·Р°СЏРІРѕРє РЅР° РІС‹РІРѕРґ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/update-withdrawal-status', methods=['POST'])
def update_withdrawal_status():
    """РћР±РЅРѕРІР»РµРЅРёРµ СЃС‚Р°С‚СѓСЃР° Р·Р°СЏРІРєРё РЅР° РІС‹РІРѕРґ"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        withdrawal_id = data.get('withdrawal_id')
        status = data.get('status')
        admin_notes = data.get('admin_notes', '')

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT user_id, inventory_id, gift_name, status FROM withdrawals WHERE id = ?', (withdrawal_id,))
        withdrawal = cursor.fetchone()

        if not withdrawal:
            conn.close()
            return jsonify({'success': False, 'error': 'Р—Р°СЏРІРєР° РЅРµ РЅР°Р№РґРµРЅР°'})

        user_id, inventory_id, gift_name, old_status = withdrawal

        cursor.execute('''
            UPDATE withdrawals
            SET status = ?, processed_at = CURRENT_TIMESTAMP, admin_notes = ?
            WHERE id = ?
        ''', (status, admin_notes, withdrawal_id))

        if status in ['approved', 'rejected', 'error']:
            if status == 'approved':
                cursor.execute('DELETE FROM inventory WHERE id = ?', (inventory_id,))
                add_history_record(user_id, 'withdraw_approved', 0, f'Р’С‹РІРѕРґ РѕРґРѕР±СЂРµРЅ: {gift_name}')
            else:
                cursor.execute('UPDATE inventory SET is_withdrawing = FALSE WHERE id = ?', (inventory_id,))
                add_history_record(user_id, 'withdraw_rejected', 0, f'Р’С‹РІРѕРґ РѕС‚РєР»РѕРЅРµРЅ: {gift_name}')

        conn.commit()
        conn.close()

        logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} РёР·РјРµРЅРёР» СЃС‚Р°С‚СѓСЃ Р·Р°СЏРІРєРё #{withdrawal_id} РЅР° {status}")
        return jsonify({
            'success': True,
            'message': f'РЎС‚Р°С‚СѓСЃ Р·Р°СЏРІРєРё РѕР±РЅРѕРІР»РµРЅ РЅР° "{status}"'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РѕР±РЅРѕРІР»РµРЅРёСЏ СЃС‚Р°С‚СѓСЃР° РІС‹РІРѕРґР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/crash/customizations')
def crash_customizations():
    """РџРѕР»СѓС‡РµРЅРёРµ РґРѕСЃС‚СѓРїРЅС‹С… РєР°СЃС‚РѕРјРёР·Р°С†РёР№ (СЂР°РєРµС‚С‹ Рё С„РѕРЅС‹) РґР»СЏ crash РёРіСЂС‹"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # РЎРѕР·РґР°РµРј С‚Р°Р±Р»РёС†Сѓ РµСЃР»Рё РЅРµ СЃСѓС‰РµСЃС‚РІСѓРµС‚ (СЃ РЅРѕРІС‹РјРё РїРѕР»СЏРјРё)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crash_customizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_type TEXT NOT NULL,
                item_id TEXT NOT NULL,
                name TEXT,
                is_vip INTEGER DEFAULT 0,
                is_default INTEGER DEFAULT 0,
                access_type TEXT DEFAULT 'free',
                requirement INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(item_type, item_id)
            )
        ''')
        
        # Р”РѕР±Р°РІР»СЏРµРј РЅРѕРІС‹Рµ РєРѕР»РѕРЅРєРё РµСЃР»Рё РёС… РЅРµС‚
        try:
            cursor.execute('ALTER TABLE crash_customizations ADD COLUMN access_type TEXT DEFAULT "free"')
        except: pass
        try:
            cursor.execute('ALTER TABLE crash_customizations ADD COLUMN requirement INTEGER DEFAULT 0')
        except: pass
        
        # Р”РѕР±Р°РІР»СЏРµРј РґРµС„РѕР»С‚РЅС‹Рµ РµСЃР»Рё РёС… РЅРµС‚ - 5 animated backgrounds
        cursor.execute("DELETE FROM crash_customizations WHERE item_type = 'background'")
        bg_data = [
            ('grid', 'РЎРµС‚РєР°', 0, 1, 'free', 0),         # Default - unlock at level 1
            ('cosmic', 'РљРѕСЃРјРѕСЃ', 0, 0, 'level', 5),     # Space theme - level 5
            ('rainbow', 'Р Р°РґСѓРіР°', 0, 0, 'level', 10),   # Rainbow - level 10
            ('aurora', 'РЎРёСЏРЅРёРµ', 0, 0, 'level', 15),    # Northern lights - level 15
            ('neon', 'РќРµРѕРЅ', 0, 0, 'level', 20),        # Neon/Cyberpunk - level 20
        ]
        for bg_id, bg_name, is_vip, is_default, access, req in bg_data:
            cursor.execute('''
                INSERT OR IGNORE INTO crash_customizations (item_type, item_id, name, is_vip, is_default, access_type, requirement)
                VALUES ('background', ?, ?, ?, ?, ?, ?)
            ''', (bg_id, bg_name, is_vip, is_default, access, req))
        
        # Р РµРіРёСЃС‚СЂРёСЂСѓРµРј РІСЃРµ СЂР°РєРµС‚С‹ РёР· LEVEL_SYSTEM СЃ СѓСЂРѕРІРЅРµРІС‹Рј РґРѕСЃС‚СѓРїРѕРј
        # РЎРЅР°С‡Р°Р»Р° СѓРґР°Р»СЏРµРј СЃС‚Р°СЂС‹Рµ СЂР°РєРµС‚С‹ С‡С‚РѕР±С‹ РѕР±РЅРѕРІРёС‚СЊ access_type
        cursor.execute("DELETE FROM crash_customizations WHERE item_type = 'rocket'")
        for lvl_info in LEVEL_SYSTEM:
            rocket_id = lvl_info.get('reward_rocket')
            if not rocket_id:
                continue
            rocket_name = ROCKET_NAMES.get(rocket_id, rocket_id)
            lvl_num = lvl_info['level']
            is_default = 1 if lvl_num == 1 else 0
            access = 'free' if lvl_num == 1 else 'level'
            req = 0 if lvl_num == 1 else lvl_num
            cursor.execute('''
                INSERT OR IGNORE INTO crash_customizations (item_type, item_id, name, is_vip, is_default, access_type, requirement)
                VALUES ('rocket', ?, ?, 0, ?, ?, ?)
            ''', (rocket_id, rocket_name, is_default, access, req))
        conn.commit()
        
        # РџРѕР»СѓС‡Р°РµРј СЂР°РєРµС‚С‹ СЃ СѓСЃР»РѕРІРёСЏРјРё РґРѕСЃС‚СѓРїР°
        cursor.execute('SELECT item_id, name, is_vip, is_default, access_type, requirement FROM crash_customizations WHERE item_type = ? ORDER BY is_default DESC, id ASC', ('rocket',))
        rockets = []
        for r in cursor.fetchall():
            rockets.append({
                'id': r[0], 
                'name': r[1], 
                'is_vip': bool(r[2]), 
                'is_default': bool(r[3]),
                'access_type': r[4] or 'free',
                'requirement': r[5] or 0
            })
        
        # РџРѕР»СѓС‡Р°РµРј С„РѕРЅС‹ СЃ СѓСЃР»РѕРІРёСЏРјРё РґРѕСЃС‚СѓРїР°
        cursor.execute('SELECT item_id, name, is_vip, is_default, access_type, requirement FROM crash_customizations WHERE item_type = ? ORDER BY is_default DESC, id ASC', ('background',))
        backgrounds = []
        for b in cursor.fetchall():
            backgrounds.append({
                'id': b[0], 
                'name': b[1], 
                'is_vip': bool(b[2]), 
                'is_default': bool(b[3]),
                'access_type': b[4] or 'free',
                'requirement': b[5] or 0
            })
        
        conn.close()
        
        return jsonify({
            'success': True,
            'rockets': rockets,
            'backgrounds': backgrounds,
            'default_rocket': 'crash',
            'default_background': 'grid'
        })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РєР°СЃС‚РѕРјРёР·Р°С†РёР№: {e}")
        return jsonify({
            'success': False, 
            'error': str(e),
            'rockets': [{'id': 'crash', 'name': 'Р Р°РєРµС‚Р°', 'is_vip': False, 'is_default': True}],
            'backgrounds': [{'id': 'phone', 'name': 'РљРѕСЃРјРѕСЃ', 'is_vip': False, 'is_default': True}],
            'default_rocket': 'crash',
            'default_background': 'phone'
        })

@app.route('/api/crash/status')
def crash_status():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, status, current_multiplier
        FROM crash_games
        ORDER BY id DESC LIMIT 1
    """)
    game = cur.fetchone()

    if not game:
        cur.execute("INSERT INTO crash_games(status,current_multiplier) VALUES('waiting',1.0)")
        conn.commit()
        return jsonify({"status": "waiting", "multiplier": 1.0})

    return jsonify({
        "game_id": game[0],
        "status": game[1],
        "multiplier": float(game[2])
    })

@app.route('/api/crash/bet', methods=['POST'])
def crash_bet():
    data = request.json
    user_id = data['user_id']
    amount = int(data['amount'])

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT balance_stars FROM users WHERE id=?", (user_id,))
    balance = cur.fetchone()[0]

    if balance < amount:
        return jsonify({"error": "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ Р·РІС‘Р·Рґ"})

    # СЃРїРёСЃС‹РІР°РµРј Р±Р°Р»Р°РЅСЃ
    cur.execute("UPDATE users SET balance_stars = balance_stars - ? WHERE id=?", (amount, user_id))

    # РёС‰РµРј РїРѕРґР°СЂРѕРє РїРѕ С†РµРЅРµ (originals only)
    gifts = build_fragment_first_gifts_catalog()
    gift = min(gifts, key=lambda g: abs(g.get("value", 0) - amount))

    # Р°РєС‚РёРІРЅР°СЏ РёРіСЂР°
    cur.execute("SELECT id FROM crash_games ORDER BY id DESC LIMIT 1")
    game_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO crash_bets(game_id,user_id,bet_amount,bet_type,
        gift_id,gift_name,gift_image,gift_value)
        VALUES(?,?,?,?,?,?,?,?)
    """, (
        game_id, user_id, amount, "stars",
        gift.get("id") or gift.get("gift_key") or -1, gift["name"], gift.get("image", ''), gift.get("value", 0)
    ))

    conn.commit()
    conn.close()

    return jsonify({"success": True, "gift": gift})

@app.route('/api/crash/cashout', methods=['POST'])
def crash_cashout():
    data = request.json
    user_id = data['user_id']

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT b.id,b.bet_amount,b.gift_value,g.current_multiplier
        FROM crash_bets b
        JOIN crash_games g ON b.game_id=g.id
        WHERE b.user_id=? AND b.status='active'
    """, (user_id,))

    bet = cur.fetchone()
    if not bet:
        return jsonify({"error":"РќРµС‚ Р°РєС‚РёРІРЅРѕР№ СЃС‚Р°РІРєРё"})

    bet_id, amount, gift_value, mult = bet
    win = int(amount * float(mult))

    # РµСЃР»Рё РІС‹РёРіСЂС‹С€ РїСЂРµРІСЂР°С‰Р°РµС‚СЃСЏ РІ РїРѕРґР°СЂРѕРє (originals only)
    gifts = build_fragment_first_gifts_catalog()
    best_gift = min(gifts, key=lambda g: abs(g.get("value", 0) - win))

    cur.execute("""
        INSERT INTO inventory(user_id,gift_id,gift_name,gift_image,gift_value)
        VALUES(?,?,?,?,?)
    """, (user_id, best_gift.get("id") or -1, best_gift["name"], best_gift.get("image", ''), best_gift.get("value", 0)))

    cur.execute("UPDATE crash_bets SET status='won', win_amount=? WHERE id=?", (win,bet_id))

    conn.commit()
    conn.close()

    return jsonify({
        "success":True,
        "multiplier": mult,
        "reward": best_gift
    })


@app.route('/api/admin/set-case-limit', methods=['POST'])
def admin_set_case_limit():
    """РЈСЃС‚Р°РЅРѕРІРєР° Р»РёРјРёС‚Р° РґР»СЏ РєРѕРЅРєСЂРµС‚РЅРѕРіРѕ РєРµР№СЃР°"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        case_id = data.get('case_id')
        limit = data.get('limit', 0)

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT OR REPLACE INTO case_limits (case_id, current_amount, last_updated)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (case_id, limit))

        conn.commit()
        conn.close()

        logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} СѓСЃС‚Р°РЅРѕРІРёР» Р»РёРјРёС‚ {limit} РґР»СЏ РєРµР№СЃР° {case_id}")
        return jsonify({
            'success': True,
            'message': f'Р›РёРјРёС‚ РєРµР№СЃР° СѓСЃС‚Р°РЅРѕРІР»РµРЅ: {limit}'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СѓСЃС‚Р°РЅРѕРІРєРё Р»РёРјРёС‚Р° РєРµР№СЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/update-case-order', methods=['POST'])
def admin_update_case_order():
    """РћР±РЅРѕРІР»РµРЅРёРµ РїРѕСЂСЏРґРєР° РѕС‚РѕР±СЂР°Р¶РµРЅРёСЏ РєРµР№СЃРѕРІ"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        case_order = data.get('case_order', [])

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        cases = load_cases()

        cases_dict = {case['id']: case for case in cases}

        updated_cases = []
        for order_item in case_order:
            case_id = order_item['id']
            display_order = order_item['display_order']

            if case_id in cases_dict:
                case = cases_dict[case_id]
                case['display_order'] = display_order
                updated_cases.append(case)
            else:
                logger.warning(f"вљ пёЏ РљРµР№СЃ СЃ ID {case_id} РЅРµ РЅР°Р№РґРµРЅ РїСЂРё РѕР±РЅРѕРІР»РµРЅРёРё РїРѕСЂСЏРґРєР°")

        for case_id, case in cases_dict.items():
            if case not in updated_cases:
                updated_cases.append(case)

        updated_cases.sort(key=lambda x: x.get('display_order', 0))

        if save_cases(updated_cases):
            logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} РѕР±РЅРѕРІРёР» РїРѕСЂСЏРґРѕРє РєРµР№СЃРѕРІ")
            return jsonify({
                'success': True,
                'message': 'РџРѕСЂСЏРґРѕРє РєРµР№СЃРѕРІ СѓСЃРїРµС€РЅРѕ РѕР±РЅРѕРІР»РµРЅ!'
            })
        else:
            return jsonify({'success': False, 'error': 'РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ РїРѕСЂСЏРґРєР° РєРµР№СЃРѕРІ'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РѕР±РЅРѕРІР»РµРЅРёСЏ РїРѕСЂСЏРґРєР° РєРµР№СЃРѕРІ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/cases', methods=['GET', 'POST', 'PUT', 'DELETE'])
def admin_cases_management():
    """РЈРїСЂР°РІР»РµРЅРёРµ РєРµР№СЃР°РјРё С‡РµСЂРµР· Р°РґРјРёРЅ-РїР°РЅРµР»СЊ"""
    try:
        payload = request.get_json(silent=True) or {}
        admin_id = request.args.get('admin_id') or payload.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        if request.method == 'GET':
            cases = load_cases()
            cases.sort(key=lambda x: x.get('display_order', 0))
            return jsonify({'success': True, 'cases': cases})

        elif request.method == 'POST':
            data = payload
            cases = load_cases()

            new_id = data.get('id')
            if not new_id:
                new_id = max([case['id'] for case in cases], default=0) + 1

            if any(case['id'] == new_id for case in cases):
                return jsonify({'success': False, 'error': 'РљРµР№СЃ СЃ С‚Р°РєРёРј ID СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚'})

            max_order = max([case.get('display_order', 0) for case in cases], default=0)

            image_filename = data.get('image_filename', '').strip()
            if image_filename and not image_filename.startswith('http'):
                image_url = f"/static/img/{image_filename}"
            else:
                image_url = data.get('image', '')

            open_date = data.get('open_date')
            if open_date:
                try:
                    open_date = datetime.fromisoformat(open_date.replace('Z', '+00:00')).isoformat()
                except:
                    open_date = None

            new_case = {
                'id': new_id,
                'name': data['name'],
                'image': image_url,
                'cost': data['cost'],
                'cost_type': data['cost_type'],
                'section': normalize_section_id(data.get('section', 'other')),
                'required_level': data.get('required_level', 1),
                'limited': data.get('limited', False),
                'amount': data.get('amount', 0),
                'description': data.get('description', ''),
                'display_order': max_order + 1,
                'tags': data.get('tags', []),
                'glow_effect': data.get('glow_effect', 'none'),
                'open_date': open_date,
                'gifts': data.get('gifts', [])
            }

            cases.append(new_case)

            if save_cases(cases):
                if new_case.get('limited'):
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute('INSERT OR REPLACE INTO case_limits (case_id, current_amount) VALUES (?, ?)',
                                 (new_case['id'], new_case['amount']))
                    conn.commit()
                    conn.close()

                logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} СЃРѕР·РґР°Р» РєРµР№СЃ: {new_case['name']}")
                return jsonify({'success': True, 'message': 'РљРµР№СЃ СѓСЃРїРµС€РЅРѕ СЃРѕР·РґР°РЅ', 'case': new_case})
            else:
                return jsonify({'success': False, 'error': 'РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ РєРµР№СЃР°'})

        elif request.method == 'PUT':
            data = payload
            case_id = data['id']

            cases = load_cases()
            case_index = next((i for i, case in enumerate(cases) if case['id'] == case_id), -1)

            if case_index == -1:
                return jsonify({'success': False, 'error': 'РљРµР№СЃ РЅРµ РЅР°Р№РґРµРЅ'})

            image_filename = data.get('image_filename', '').strip()
            if image_filename and not image_filename.startswith('http'):
                image_url = f"/static/img/{image_filename}"
            else:
                image_url = data.get('image', cases[case_index]['image'])

            open_date = data.get('open_date')
            if open_date:
                try:
                    open_date = datetime.fromisoformat(open_date.replace('Z', '+00:00')).isoformat()
                except:
                    open_date = None

            updated_case = {
                'id': case_id,
                'name': data['name'],
                'image': image_url,
                'cost': data['cost'],
                'cost_type': data['cost_type'],
                'section': normalize_section_id(data.get('section', cases[case_index].get('section', 'other'))),
                'required_level': data.get('required_level', 1),
                'limited': data.get('limited', False),
                'amount': data.get('amount', 0),
                'description': data.get('description', ''),
                'display_order': data.get('display_order', cases[case_index].get('display_order', 0)),
                'tags': data.get('tags', []),
                'glow_effect': data.get('glow_effect', 'none'),
                'open_date': open_date,
                'gifts': data.get('gifts', [])
            }

            cases[case_index] = updated_case

            if save_cases(cases):
                if updated_case.get('limited'):
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute('INSERT OR REPLACE INTO case_limits (case_id, current_amount) VALUES (?, ?)',
                                 (updated_case['id'], updated_case['amount']))
                    conn.commit()
                    conn.close()

                logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} РѕР±РЅРѕРІРёР» РєРµР№СЃ: {updated_case['name']}")
                return jsonify({'success': True, 'message': 'РљРµР№СЃ СѓСЃРїРµС€РЅРѕ РѕР±РЅРѕРІР»РµРЅ', 'case': updated_case})
            else:
                return jsonify({'success': False, 'error': 'РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ РєРµР№СЃР°'})

        elif request.method == 'DELETE':
            case_id = payload['id']

            cases = load_cases()
            case_to_delete = next((case for case in cases if case['id'] == case_id), None)

            if not case_to_delete:
                return jsonify({'success': False, 'error': 'РљРµР№СЃ РЅРµ РЅР°Р№РґРµРЅ'})

            cases = [case for case in cases if case['id'] != case_id]

            if save_cases(cases):
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute('DELETE FROM case_limits WHERE case_id = ?', (case_id,))
                conn.commit()
                conn.close()

                logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} СѓРґР°Р»РёР» РєРµР№СЃ: {case_to_delete['name']}")
                return jsonify({'success': True, 'message': 'РљРµР№СЃ СѓСЃРїРµС€РЅРѕ СѓРґР°Р»РµРЅ'})
            else:
                return jsonify({'success': False, 'error': 'РћС€РёР±РєР° СѓРґР°Р»РµРЅРёСЏ РєРµР№СЃР°'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СѓРїСЂР°РІР»РµРЅРёСЏ РєРµР№СЃР°РјРё: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/case-gifts/<int:case_id>', methods=['GET', 'POST'])
def admin_case_gifts(case_id):
    """Manage gifts within a specific case"""
    try:
        payload = request.get_json(silent=True) or {}
        admin_id = request.args.get('admin_id') or payload.get('admin_id')
        
        cases = load_cases()
        case_obj = next((c for c in cases if c['id'] == case_id), None)
        if not case_obj:
            return jsonify({'success': False, 'error': 'Case not found'})
        
        if request.method == 'GET':
            # Return gifts for this case with full gift details
            case_gifts = []
            all_gifts = load_gifts()
            gifts_map = {str(g['id']): g for g in all_gifts if g.get('id') is not None}
            
            for cg in case_obj.get('gifts', []):
                # Handle ton_balance type
                if cg.get('type') == 'ton_balance':
                    ton_amount = int(cg.get('ton_amount', 0) or 0)
                    chance = float(cg.get('chance', 0) or 0)
                    stable_id = f"ton_balance:{ton_amount}:{chance}"
                    case_gifts.append({
                        'id': stable_id,
                        'type': 'ton_balance',
                        'ton_amount': ton_amount,
                        'name': 'TON',
                        'image': '/static/img/tons/ton_1.svg',
                        'value': ton_amount,
                        'chance': chance
                    })
                    continue
                    
                gift_id = cg.get('id') or cg.get('gift_id')
                chance = cg.get('chance', 0)
                gift_data = gifts_map.get(str(gift_id), {})
                case_gifts.append({
                    'id': gift_id,
                    'name': gift_data.get('name', cg.get('name', f'Gift #{gift_id}')),
                    'image': gift_data.get('image', cg.get('image', '')),
                    'value': gift_data.get('value', cg.get('value', 0)),
                    'chance': chance,
                    'type': cg.get('type', 'gift'),
                    'gift_key': cg.get('gift_key'),
                    'fragment_slug': cg.get('fragment_slug'),
                    'model_name': cg.get('model_name'),
                    'model_count': cg.get('model_count')
                })
            
            return jsonify({'success': True, 'case_gifts': case_gifts})
        
        elif request.method == 'POST':
            data = request.get_json()
            if not data:
                return jsonify({'success': False, 'error': 'No data'})
            
            action = data.get('action', '')
            
            if action == 'add_gift':
                gift_type = data.get('type', 'gift')
                chance = float(data.get('chance', 10))
                
                # Handle ton_balance type
                if gift_type == 'ton_balance':
                    ton_amount = int(data.get('ton_amount', 0))
                    if ton_amount <= 0:
                        return jsonify({'success': False, 'error': 'Invalid stars amount'})
                    
                    gifts_list = case_obj.get('gifts', [])
                    gifts_list.append({
                        'type': 'ton_balance',
                        'ton_amount': ton_amount,
                        'chance': chance
                    })
                    case_obj['gifts'] = gifts_list
                    save_cases(cases)
                    return jsonify({'success': True, 'message': f'Stars Balance ({ton_amount}) added'})
                
                gift_id = data.get('gift_id')

                # Get gift info
                all_gifts = load_gifts()
                gift_info = next((g for g in all_gifts if str(g.get('id')) == str(gift_id)), None)

                gifts_list = case_obj.get('gifts', [])

                if gift_info:
                    if any(str(g.get('id')) == str(gift_id) or str(g.get('gift_id')) == str(gift_id) for g in gifts_list):
                        return jsonify({'success': False, 'error': 'Gift already in case'})

                    gifts_list.append({
                        'id': gift_info.get('id'),
                        'name': gift_info.get('name', ''),
                        'image': gift_info.get('image', ''),
                        'value': gift_info.get('value', 0),
                        'chance': chance,
                        'type': data.get('gift_type', 'gift'),
                        'fragment_slug': data.get('fragment_slug')
                    })
                else:
                    gift_name = str(data.get('gift_name') or '').strip()
                    gift_image = str(data.get('gift_image') or '').strip()
                    if not gift_name:
                        return jsonify({'success': False, 'error': 'Gift not found'})

                    custom_slug = str(data.get('fragment_slug') or '').strip().lower()
                    model_name = str(data.get('model_name') or '').strip()
                    model_count = _safe_int(data.get('model_count'), 0)
                    custom_id = data.get('gift_key') or _build_case_custom_gift_id(gift_name, fragment_slug=custom_slug, model_name=model_name)
                    gift_value = _safe_int(data.get('gift_value'), 0)

                    duplicate = any(
                        str(g.get('id')) == str(custom_id)
                        or (g.get('gift_key') and str(g.get('gift_key')) == str(custom_id))
                        or (str(g.get('name', '')).strip().lower() == gift_name.lower() and str(g.get('image', '')).strip() == gift_image)
                        for g in gifts_list
                    )
                    if duplicate:
                        return jsonify({'success': False, 'error': 'Gift already in case'})

                    gifts_list.append({
                        'id': custom_id,
                        'gift_key': custom_id,
                        'name': gift_name,
                        'image': gift_image,
                        'value': gift_value,
                        'chance': chance,
                        'type': data.get('gift_type', 'fragment_model' if model_name else 'custom_gift'),
                        'fragment_slug': custom_slug,
                        'model_name': model_name,
                        'model_count': model_count
                    })

                case_obj['gifts'] = gifts_list
                save_cases(cases)
                return jsonify({'success': True, 'message': 'Gift added'})
            
            elif action == 'update_chance':
                gift_id = data.get('gift_id')
                new_chance = float(data.get('chance', 0))
                
                gifts_list = case_obj.get('gifts', [])
                gift_id_str = str(gift_id)
                if gift_id_str.startswith('ton_balance:'):
                    try:
                        _, stars_part, old_chance_part = gift_id_str.split(':', 2)
                        stars_part_i = int(float(stars_part))
                        old_chance_f = float(old_chance_part)
                    except Exception:
                        stars_part_i = None
                        old_chance_f = None
                    for g in gifts_list:
                        if g.get('type') != 'ton_balance':
                            continue
                        if stars_part_i is not None and int(g.get('ton_amount', 0) or 0) != stars_part_i:
                            continue
                        if old_chance_f is not None and abs(float(g.get('chance', 0) or 0) - old_chance_f) > 1e-9:
                            continue
                        g['chance'] = new_chance
                        break
                    case_obj['gifts'] = gifts_list
                    save_cases(cases)
                    return jsonify({'success': True, 'message': 'Chance updated'})

                gift_id_str = str(gift_id)
                for g in gifts_list:
                    if str(g.get('id')) == gift_id_str or str(g.get('gift_id')) == gift_id_str or str(g.get('gift_key')) == gift_id_str:
                        g['chance'] = new_chance
                        break
                
                case_obj['gifts'] = gifts_list
                save_cases(cases)
                return jsonify({'success': True, 'message': 'Chance updated'})
            
            elif action == 'remove_gift':
                gift_id = data.get('gift_id')
                gifts_list = case_obj.get('gifts', [])
                gift_id_str = str(gift_id)

                if gift_id_str.startswith('ton_balance:'):
                    try:
                        _, stars_part, chance_part = gift_id_str.split(':', 2)
                        stars_part_i = int(float(stars_part))
                        chance_part_f = float(chance_part)
                    except Exception:
                        stars_part_i = None
                        chance_part_f = None
                    removed = False
                    new_gifts = []
                    for g in gifts_list:
                        if not removed and g.get('type') == 'ton_balance':
                            same_stars = stars_part_i is None or int(g.get('ton_amount', 0) or 0) == stars_part_i
                            same_chance = chance_part_f is None or abs(float(g.get('chance', 0) or 0) - chance_part_f) <= 1e-9
                            if same_stars and same_chance:
                                removed = True
                                continue
                        new_gifts.append(g)
                    case_obj['gifts'] = new_gifts
                elif gift_id is None or gift_id_str.lower() in ('none', 'null', 'ton_balance'):
                    removed = False
                    new_gifts = []
                    for g in gifts_list:
                        if not removed and g.get('type') == 'ton_balance':
                            removed = True
                            continue
                        new_gifts.append(g)
                    case_obj['gifts'] = new_gifts
                else:
                    case_obj['gifts'] = [
                        g for g in gifts_list
                        if str(g.get('id')) != gift_id_str
                        and str(g.get('gift_id')) != gift_id_str
                        and str(g.get('gift_key')) != gift_id_str
                    ]
                save_cases(cases)
                return jsonify({'success': True, 'message': 'Gift removed'})
            
            else:
                return jsonify({'success': False, 'error': 'Unknown action'})
    
    except Exception as e:
        logger.error(f"Case gifts error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/create-case', methods=['POST'])
def admin_create_case():
    """РЎРѕР·РґР°РЅРёРµ РЅРѕРІРѕРіРѕ РєРµР№СЃР°"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        cases = load_cases()

        new_id = max([case['id'] for case in cases], default=0) + 1

        max_order = max([case.get('display_order', 0) for case in cases], default=0)

        image_filename = data.get('image_filename', '').strip()
        if image_filename and not image_filename.startswith('http'):
            image_url = f"/static/img/{image_filename}"
        else:
            image_url = data.get('image', '')

        open_date = data.get('open_date')
        if open_date:
            try:
                open_date = datetime.fromisoformat(open_date.replace('Z', '+00:00')).isoformat()
            except:
                open_date = None

        new_case = {
            'id': new_id,
            'name': data['name'],
            'image': image_url,
            'cost': data['cost'],
            'cost_type': data['cost_type'],
            'section': normalize_section_id(data.get('section', 'other')),
            'required_level': data.get('required_level', 1),
            'limited': data.get('limited', False),
            'amount': data.get('amount', 0),
            'description': data.get('description', ''),
            'display_order': max_order + 1,
            'tags': data.get('tags', []),
            'glow_effect': data.get('glow_effect', 'none'),
            'open_date': open_date,
            'gifts': data.get('gifts', [])
        }

        cases.append(new_case)

        if save_cases(cases):
            if new_case.get('limited'):
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute('INSERT OR REPLACE INTO case_limits (case_id, current_amount) VALUES (?, ?)',
                             (new_case['id'], new_case['amount']))
                conn.commit()
                conn.close()

            logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} СЃРѕР·РґР°Р» РєРµР№СЃ: {new_case['name']}")
            return jsonify({'success': True, 'message': 'РљРµР№СЃ СѓСЃРїРµС€РЅРѕ СЃРѕР·РґР°РЅ', 'case': new_case})
        else:
            return jsonify({'success': False, 'error': 'РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ РєРµР№СЃР°'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃРѕР·РґР°РЅРёСЏ РєРµР№СЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/case-gifts/<int:case_id>', methods=['GET'])
def get_case_gifts(case_id):
    """РџРѕР»СѓС‡РёС‚СЊ СЃРїРёСЃРѕРє РїРѕРґР°СЂРєРѕРІ РІ РєРµР№СЃРµ"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        cases = load_cases()
        case = next((c for c in cases if c['id'] == case_id), None)

        if not case:
            return jsonify({'success': False, 'error': 'РљРµР№СЃ РЅРµ РЅР°Р№РґРµРЅ'})

        gifts = load_gifts()
        result = []

        for gift_info in case.get('gifts', []):
            if gift_info.get('type') == 'ton_balance':
                result.append({
                    'id': -1,
                    'name': 'TON',
                    'image': '/static/img/tons/ton_1.svg',
                    'chance': gift_info.get('chance', 1),
                    'type': 'ton_balance',
                    'ton_amount': gift_info.get('ton_amount', 0)
                })
            else:
                gift = next((g for g in gifts if g['id'] == gift_info['id']), None)
                if gift:
                    result.append({
                        'id': gift['id'],
                        'name': gift['name'],
                        'image': gift['image'],
                        'chance': gift_info.get('chance', 1),
                        'value': gift.get('value', 0)
                    })

        return jsonify({'success': True, 'gifts': result, 'case_name': case['name']})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РїРѕРґР°СЂРєРѕРІ РєРµР№СЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/add-gift-to-case', methods=['POST'])
def add_gift_to_case():
    """Р”РѕР±Р°РІРёС‚СЊ РїРѕРґР°СЂРѕРє РёР»Рё ton_balance РІ РєРµР№СЃ"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        case_id = data.get('case_id')
        gift_type = data.get('type', 'gift')

        cases = load_cases()
        case = next((c for c in cases if c['id'] == case_id), None)

        if not case:
            return jsonify({'success': False, 'error': 'РљРµР№СЃ РЅРµ РЅР°Р№РґРµРЅ'})

        if 'gifts' not in case:
            case['gifts'] = []

        if gift_type == 'ton_balance':
            ton_amount = int(data.get('ton_amount', 0))
            chance = float(data.get('chance', 1))

            case['gifts'].append({
                'type': 'ton_balance',
                'ton_amount': ton_amount,
                'chance': chance
            })

            if save_cases(cases):
                logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} РґРѕР±Р°РІРёР» ton_balance ({ton_amount}в­ђ) РІ РєРµР№СЃ {case['name']}")
                return jsonify({'success': True, 'message': f'Stars Balance ({ton_amount}в­ђ) РґРѕР±Р°РІР»РµРЅ РІ РєРµР№СЃ'})
            else:
                return jsonify({'success': False, 'error': 'РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ'})
        else:
            gift_id = int(data.get('gift_id'))
            chance = float(data.get('chance', 1))

            existing = next((g for g in case['gifts'] if g.get('id') == gift_id and g.get('type') != 'ton_balance'), None)
            if existing:
                existing['chance'] = chance
            else:
                case['gifts'].append({'id': gift_id, 'chance': chance})

            if save_cases(cases):
                return jsonify({'success': True, 'message': 'РџРѕРґР°СЂРѕРє РґРѕР±Р°РІР»РµРЅ РІ РєРµР№СЃ'})
            else:
                return jsonify({'success': False, 'error': 'РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РґРѕР±Р°РІР»РµРЅРёСЏ РІ РєРµР№СЃ: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/promo-codes', methods=['GET', 'POST', 'DELETE'])
def admin_promo_codes_management():
    """РЈРїСЂР°РІР»РµРЅРёРµ РїСЂРѕРјРѕРєРѕРґР°РјРё"""
    try:
        admin_id = request.args.get('admin_id') or request.json.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        if request.method == 'GET':
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute('''
                SELECT id, code, reward_stars, reward_tickets, reward_type, max_uses, used_count,
                       created_at, expires_at, is_active
                FROM promo_codes
                ORDER BY created_at DESC
            ''')
            promos = cursor.fetchall()
            conn.close()

            promos_list = []
            for promo in promos:
                promos_list.append({
                    'id': promo[0],
                    'code': promo[1],
                    'reward_stars': promo[2],
                    'reward_tickets': promo[3],
                    'reward_type': promo[4] or 'stars',
                    'max_uses': promo[5],
                    'used_count': promo[6],
                    'created_at': promo[7],
                    'expires_at': promo[8],
                    'is_active': bool(promo[9])
                })

            return jsonify({'success': True, 'promo_codes': promos_list})

        elif request.method == 'POST':
            data = request.json

            code = data.get('code', '').upper().strip()
            if not code:
                characters = string.ascii_uppercase + string.digits
                code = ''.join(random.choice(characters) for _ in range(8))

            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM promo_codes WHERE code = ?', (code,))
            existing = cursor.fetchone()

            if existing:
                conn.close()
                return jsonify({'success': False, 'error': 'РџСЂРѕРјРѕРєРѕРґ СЃ С‚Р°РєРёРј РєРѕРґРѕРј СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚'})

            reward_stars = data.get('reward_stars', 0)
            reward_tickets = data.get('reward_tickets', 0)
            reward_type = data.get('reward_type', 'stars')
            reward_data = data.get('reward_data', {})
            max_uses = data.get('max_uses', 1)
            expires_days = data.get('expires_days', 30)

            expires_at = None
            if expires_days > 0:
                expires_at = (datetime.now() + timedelta(days=expires_days)).isoformat()

            # РЎРµСЂРёР°Р»РёР·СѓРµРј reward_data РІ JSON
            reward_data_json = json.dumps(reward_data) if reward_data else None

            cursor.execute('''
                INSERT INTO promo_codes (code, reward_stars, reward_tickets, reward_type, reward_data, max_uses, expires_at, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (code, reward_stars, reward_tickets, reward_type, reward_data_json, max_uses, expires_at, ADMIN_ID))

            promo_id = cursor.lastrowid
            conn.commit()
            conn.close()

            logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} СЃРѕР·РґР°Р» РїСЂРѕРјРѕРєРѕРґ: {code} (С‚РёРї: {reward_type})")
            return jsonify({
                'success': True,
                'message': f'РџСЂРѕРјРѕРєРѕРґ {code} СѓСЃРїРµС€РЅРѕ СЃРѕР·РґР°РЅ!',
                'promo_code': {
                    'id': promo_id,
                    'code': code,
                    'reward_type': reward_type,
                    'reward_stars': reward_stars,
                    'reward_tickets': reward_tickets,
                    'max_uses': max_uses,
                    'expires_at': expires_at
                }
            })

        elif request.method == 'DELETE':
            promo_id = request.json['id']

            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute('DELETE FROM promo_codes WHERE id = ?', (promo_id,))
            conn.commit()
            conn.close()

            logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} СѓРґР°Р»РёР» РїСЂРѕРјРѕРєРѕРґ #{promo_id}")
            return jsonify({'success': True, 'message': 'РџСЂРѕРјРѕРєРѕРґ СѓСЃРїРµС€РЅРѕ СѓРґР°Р»РµРЅ'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СѓРїСЂР°РІР»РµРЅРёСЏ РїСЂРѕРјРѕРєРѕРґР°РјРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/customization', methods=['GET'])
def admin_get_customization():
    """РџРѕР»СѓС‡РµРЅРёРµ СЃРїРёСЃРєР° РєР°СЃС‚РѕРјРёР·Р°С†РёРё (СЂР°РєРµС‚С‹ Рё С„РѕРЅС‹)"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()
        
        # РЎРѕР·РґР°РµРј С‚Р°Р±Р»РёС†Сѓ РµСЃР»Рё РЅРµ СЃСѓС‰РµСЃС‚РІСѓРµС‚
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crash_customizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_type TEXT NOT NULL,
                item_id TEXT NOT NULL,
                name TEXT,
                is_vip INTEGER DEFAULT 0,
                is_default INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(item_type, item_id)
            )
        ''')
        
        # РњРёРіСЂР°С†РёСЏ: РґРѕР±Р°РІР»СЏРµРј is_default РµСЃР»Рё РЅРµС‚
        try:
            cursor.execute("PRAGMA table_info(crash_customizations)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'is_default' not in columns:
                cursor.execute('ALTER TABLE crash_customizations ADD COLUMN is_default INTEGER DEFAULT 0')
        except:
            pass
        
        # Р”РѕР±Р°РІР»СЏРµРј РґРµС„РѕР»С‚РЅС‹Рµ РєР°СЃС‚РѕРјРёР·Р°С†РёРё РµСЃР»Рё РёС… РЅРµС‚
        cursor.execute('''
            INSERT OR IGNORE INTO crash_customizations (item_type, item_id, name, is_vip, is_default)
            VALUES ('rocket', 'crash', 'Р Р°РєРµС‚Р°', 0, 1)
        ''')
        cursor.execute('''
            INSERT OR IGNORE INTO crash_customizations (item_type, item_id, name, is_vip, is_default)
            VALUES ('background', 'phone', 'РљРѕСЃРјРѕСЃ', 0, 1)
        ''')
        conn.commit()
        
        cursor.execute('SELECT item_id, name, is_vip, is_default FROM crash_customizations WHERE item_type = ?', ('rocket',))
        rockets = [{'id': r[0], 'name': r[1], 'is_vip': bool(r[2]), 'is_default': bool(r[3]) if len(r) > 3 else False} for r in cursor.fetchall()]
        
        cursor.execute('SELECT item_id, name, is_vip, is_default FROM crash_customizations WHERE item_type = ?', ('background',))
        backgrounds = [{'id': b[0], 'name': b[1], 'is_vip': bool(b[2]), 'is_default': bool(b[3]) if len(b) > 3 else False} for b in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'success': True,
            'rockets': rockets,
            'backgrounds': backgrounds
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РєР°СЃС‚РѕРјРёР·Р°С†РёРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/customization/rockets', methods=['POST', 'DELETE'])
def admin_manage_rockets():
    """РЈРїСЂР°РІР»РµРЅРёРµ СЂР°РєРµС‚Р°РјРё"""
    try:
        if request.method == 'POST':
            admin_id = request.form.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
            
            item_id = request.form.get('id', '').strip()
            name = request.form.get('name', item_id)
            access_type = request.form.get('access_type', 'free')
            requirement = int(request.form.get('requirement', 0))
            file = request.files.get('file')
            
            if not item_id or not file:
                return jsonify({'success': False, 'error': 'РќСѓР¶РЅС‹ ID Рё С„Р°Р№Р»'})
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # РЎРѕР·РґР°РµРј С‚Р°Р±Р»РёС†Сѓ РµСЃР»Рё РЅРµ СЃСѓС‰РµСЃС‚РІСѓРµС‚ (СЃ РЅРѕРІС‹РјРё РїРѕР»СЏРјРё)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS crash_customizations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_type TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    name TEXT,
                    is_vip INTEGER DEFAULT 0,
                    is_default INTEGER DEFAULT 0,
                    access_type TEXT DEFAULT 'free',
                    requirement INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(item_type, item_id)
                )
            ''')
            
            # Р”РѕР±Р°РІР»СЏРµРј РЅРѕРІС‹Рµ РєРѕР»РѕРЅРєРё РµСЃР»Рё РёС… РЅРµС‚
            try:
                cursor.execute('ALTER TABLE crash_customizations ADD COLUMN access_type TEXT DEFAULT "free"')
            except: pass
            try:
                cursor.execute('ALTER TABLE crash_customizations ADD COLUMN requirement INTEGER DEFAULT 0')
            except: pass
            
            # РЎРѕС…СЂР°РЅСЏРµРј С„Р°Р№Р»
            filename = f"{item_id}.gif"
            filepath = os.path.join(BASE_PATH, 'static', 'gifs', filename)
            file.save(filepath)
            
            # РћРїСЂРµРґРµР»СЏРµРј is_vip РЅР° РѕСЃРЅРѕРІРµ access_type
            is_vip = 1 if access_type == 'vip' else 0
            
            # Р”РѕР±Р°РІР»СЏРµРј РІ Р‘Р”
            cursor.execute('''
                INSERT OR REPLACE INTO crash_customizations (item_type, item_id, name, is_vip, access_type, requirement)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ('rocket', item_id, name, is_vip, access_type, requirement))
            
            conn.commit()
            conn.close()
            
            logger.info(f"рџ› пёЏ РђРґРјРёРЅ РґРѕР±Р°РІРёР» СЂР°РєРµС‚Сѓ: {item_id} (access: {access_type})")
            return jsonify({'success': True, 'message': 'Р Р°РєРµС‚Р° РґРѕР±Р°РІР»РµРЅР°'})
            
        elif request.method == 'DELETE':
            data = request.json
            admin_id = data.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
            
            item_id = data.get('id')
            if not item_id:
                return jsonify({'success': False, 'error': 'РќСѓР¶РµРЅ ID'})
            
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM crash_customizations WHERE item_type = ? AND item_id = ?', ('rocket', item_id))
            conn.commit()
            conn.close()
            
            # РЈРґР°Р»СЏРµРј С„Р°Р№Р»
            filepath = os.path.join(BASE_PATH, 'static', 'gifs', f"{item_id}.gif")
            if os.path.exists(filepath):
                os.remove(filepath)
            
            logger.info(f"рџ› пёЏ РђРґРјРёРЅ СѓРґР°Р»РёР» СЂР°РєРµС‚Сѓ: {item_id}")
            return jsonify({'success': True, 'message': 'Р Р°РєРµС‚Р° СѓРґР°Р»РµРЅР°'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СѓРїСЂР°РІР»РµРЅРёСЏ СЂР°РєРµС‚Р°РјРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/customization/backgrounds', methods=['POST', 'DELETE'])
def admin_manage_backgrounds():
    """РЈРїСЂР°РІР»РµРЅРёРµ С„РѕРЅР°РјРё"""
    try:
        if request.method == 'POST':
            admin_id = request.form.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
            
            item_id = request.form.get('id', '').strip()
            name = request.form.get('name', item_id)
            access_type = request.form.get('access_type', 'free')
            requirement = int(request.form.get('requirement', 0))
            file = request.files.get('file')
            
            if not item_id or not file:
                return jsonify({'success': False, 'error': 'РќСѓР¶РЅС‹ ID Рё С„Р°Р№Р»'})
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # РЎРѕР·РґР°РµРј С‚Р°Р±Р»РёС†Сѓ РµСЃР»Рё РЅРµ СЃСѓС‰РµСЃС‚РІСѓРµС‚ (СЃ РЅРѕРІС‹РјРё РїРѕР»СЏРјРё)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS crash_customizations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_type TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    name TEXT,
                    is_vip INTEGER DEFAULT 0,
                    is_default INTEGER DEFAULT 0,
                    access_type TEXT DEFAULT 'free',
                    requirement INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(item_type, item_id)
                )
            ''')
            
            # Р”РѕР±Р°РІР»СЏРµРј РЅРѕРІС‹Рµ РєРѕР»РѕРЅРєРё РµСЃР»Рё РёС… РЅРµС‚
            try:
                cursor.execute('ALTER TABLE crash_customizations ADD COLUMN access_type TEXT DEFAULT "free"')
            except: pass
            try:
                cursor.execute('ALTER TABLE crash_customizations ADD COLUMN requirement INTEGER DEFAULT 0')
            except: pass
            
            # РЎРѕС…СЂР°РЅСЏРµРј С„Р°Р№Р»
            filename = f"{item_id}.mp4"
            filepath = os.path.join(BASE_PATH, 'static', 'img', filename)
            file.save(filepath)
            
            # РћРїСЂРµРґРµР»СЏРµРј is_vip РЅР° РѕСЃРЅРѕРІРµ access_type
            is_vip = 1 if access_type == 'vip' else 0
            
            # Р”РѕР±Р°РІР»СЏРµРј РІ Р‘Р”
            cursor.execute('''
                INSERT OR REPLACE INTO crash_customizations (item_type, item_id, name, is_vip, access_type, requirement)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ('background', item_id, name, is_vip, access_type, requirement))
            
            conn.commit()
            conn.close()
            
            logger.info(f"рџ› пёЏ РђРґРјРёРЅ РґРѕР±Р°РІРёР» С„РѕРЅ: {item_id} (access: {access_type})")
            return jsonify({'success': True, 'message': 'Р¤РѕРЅ РґРѕР±Р°РІР»РµРЅ'})
            
        elif request.method == 'DELETE':
            data = request.json
            admin_id = data.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
            
            item_id = data.get('id')
            if not item_id:
                return jsonify({'success': False, 'error': 'РќСѓР¶РµРЅ ID'})
            
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM crash_customizations WHERE item_type = ? AND item_id = ?', ('background', item_id))
            conn.commit()
            conn.close()
            
            # РЈРґР°Р»СЏРµРј С„Р°Р№Р»
            filepath = os.path.join(BASE_PATH, 'static', 'img', f"{item_id}.mp4")
            if os.path.exists(filepath):
                os.remove(filepath)
            
            logger.info(f"рџ› пёЏ РђРґРјРёРЅ СѓРґР°Р»РёР» С„РѕРЅ: {item_id}")
            return jsonify({'success': True, 'message': 'Р¤РѕРЅ СѓРґР°Р»РµРЅ'})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СѓРїСЂР°РІР»РµРЅРёСЏ С„РѕРЅР°РјРё: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/user-customizations', methods=['GET', 'POST', 'DELETE'])
def admin_user_customizations():
    """Manage individual user's customizations (skins)"""
    try:
        if request.method == 'GET':
            admin_id = request.args.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

            target_user_id = request.args.get('user_id', 0, type=int)
            if not target_user_id:
                return jsonify({'success': False, 'error': 'user_id required'})

            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT id, item_type, item_id, source, created_at FROM user_customizations WHERE user_id = ? ORDER BY item_type, item_id', (target_user_id,))
            rows = cursor.fetchall()

            # Also get the catalog for available items
            cursor.execute('SELECT item_id, name, is_vip FROM crash_customizations WHERE item_type = ?', ('rocket',))
            all_rockets = [{'id': r[0], 'name': r[1], 'is_vip': bool(r[2])} for r in cursor.fetchall()]
            cursor.execute('SELECT item_id, name, is_vip FROM crash_customizations WHERE item_type = ?', ('background',))
            all_backgrounds = [{'id': r[0], 'name': r[1], 'is_vip': bool(r[2])} for r in cursor.fetchall()]

            conn.close()

            items = []
            for r in rows:
                items.append({
                    'db_id': r[0],
                    'item_type': r[1],
                    'item_id': r[2],
                    'source': r[3] or 'unknown',
                    'created_at': r[4]
                })

            return jsonify({
                'success': True,
                'items': items,
                'all_rockets': all_rockets,
                'all_backgrounds': all_backgrounds
            })

        elif request.method == 'POST':
            data = request.get_json()
            admin_id = data.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

            target_user_id = data.get('user_id')
            item_type = data.get('item_type', '')
            item_id = data.get('item_id', '')

            if not target_user_id or not item_type or not item_id:
                return jsonify({'success': False, 'error': 'user_id, item_type, item_id required'})

            if item_type not in ('rocket', 'background'):
                return jsonify({'success': False, 'error': 'item_type must be rocket or background'})

            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source) VALUES (?, ?, ?, ?)',
                          (target_user_id, item_type, item_id, 'admin'))
            if cursor.rowcount == 0:
                conn.close()
                return jsonify({'success': False, 'error': 'РЈР¶Рµ РµСЃС‚СЊ Сѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ'})
            conn.commit()
            conn.close()

            logger.info(f"Admin added {item_type}:{item_id} to user {target_user_id}")
            return jsonify({'success': True, 'message': 'Р”РѕР±Р°РІР»РµРЅРѕ'})

        elif request.method == 'DELETE':
            data = request.get_json()
            admin_id = data.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

            db_id = data.get('db_id')
            if not db_id:
                return jsonify({'success': False, 'error': 'db_id required'})

            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_customizations WHERE id = ?', (db_id,))
            conn.commit()
            conn.close()

            logger.info(f"Admin removed customization db_id={db_id}")
            return jsonify({'success': True, 'message': 'РЈРґР°Р»РµРЅРѕ'})

    except Exception as e:
        logger.error(f"Admin user-customizations error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/referral-stats', methods=['GET'])
def admin_referral_stats():
    """РџРѕР»СѓС‡РµРЅРёРµ СЃС‚Р°С‚РёСЃС‚РёРєРё РїРѕ СЂРµС„РµСЂР°Р»СЊРЅРѕР№ СЃРёСЃС‚РµРјРµ"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT u.id, u.first_name, u.username, u.referral_count,
                   u.total_earned_stars, u.total_earned_tickets
            FROM users u
            WHERE u.referral_count > 0
            ORDER BY u.referral_count DESC
            LIMIT 10
        ''')
        top_referrers = cursor.fetchall()

        cursor.execute('SELECT COUNT(*) FROM referrals')
        total_referrals = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(DISTINCT referrer_id) FROM referrals')
        unique_referrers = cursor.fetchone()[0]

        cursor.execute('SELECT SUM(reward_amount) FROM referral_rewards WHERE reward_type = "stars"')
        total_stars_rewarded = cursor.fetchone()[0] or 0

        cursor.execute('SELECT SUM(reward_amount) FROM referral_rewards WHERE reward_type = "tickets"')
        total_tickets_rewarded = cursor.fetchone()[0] or 0

        conn.close()

        top_referrers_list = []
        for ref in top_referrers:
            top_referrers_list.append({
                'id': ref[0],
                'name': ref[1],
                'username': ref[2],
                'referral_count': ref[3],
                'total_earned_stars': ref[4] or 0,
                'total_earned_tickets': ref[5] or 0
            })

        return jsonify({
            'success': True,
            'stats': {
                'total_referrals': total_referrals,
                'unique_referrers': unique_referrers,
                'total_stars_rewarded': total_stars_rewarded,
                'total_tickets_rewarded': total_tickets_rewarded,
                'top_referrers': top_referrers_list
            }
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЂРµС„РµСЂР°Р»СЊРЅРѕР№ СЃС‚Р°С‚РёСЃС‚РёРєРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/level-stats', methods=['GET'])
def admin_level_stats():
    """РџРѕР»СѓС‡РµРЅРёРµ СЃС‚Р°С‚РёСЃС‚РёРєРё РїРѕ СѓСЂРѕРІРЅСЏРј"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT current_level, COUNT(*) as user_count
            FROM users
            GROUP BY current_level
            ORDER BY current_level
        ''')
        level_distribution = cursor.fetchall()

        cursor.execute('''
            SELECT id, first_name, username, current_level, experience, total_cases_opened
            FROM users
            ORDER BY current_level DESC, experience DESC
            LIMIT 10
        ''')
        top_users = cursor.fetchall()

        cursor.execute('SELECT AVG(current_level), MAX(current_level), SUM(experience) FROM users')
        stats = cursor.fetchone()
        avg_level = stats[0] or 1
        max_level = stats[1] or 1
        total_experience = stats[2] or 0

        conn.close()

        distribution_list = []
        for level, count in level_distribution:
            distribution_list.append({
                'level': level,
                'user_count': count
            })

        top_users_list = []
        for user in top_users:
            top_users_list.append({
                'id': user[0],
                'name': user[1],
                'username': user[2],
                'level': user[3],
                'experience': user[4],
                'cases_opened': user[5]
            })

        return jsonify({
            'success': True,
            'stats': {
                'average_level': round(avg_level, 2),
                'max_level': max_level,
                'total_experience': total_experience,
                'level_distribution': distribution_list,
                'top_users': top_users_list
            }
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЃС‚Р°С‚РёСЃС‚РёРєРё РїРѕ СѓСЂРѕРІРЅСЏРј: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/case-limits', methods=['GET'])
def get_case_limits():
    """РџРѕР»СѓС‡РµРЅРёРµ РґРµС‚Р°Р»СЊРЅРѕР№ РёРЅС„РѕСЂРјР°С†РёРё Рѕ Р»РёРјРёС‚Р°С… РІСЃРµС… РєРµР№СЃРѕРІ"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        cases = load_cases()
        conn = get_db_connection()
        cursor = conn.cursor()

        case_limits = []
        for case in cases:
            if case.get('limited'):
                cursor.execute('SELECT current_amount FROM case_limits WHERE case_id = ?', (case['id'],))
                result = cursor.fetchone()
                current_amount = result[0] if result else case['amount']

                case_limits.append({
                    'id': case['id'],
                    'name': case['name'],
                    'max_amount': case['amount'],
                    'current_amount': current_amount,
                    'available': current_amount > 0 if result else True,
                    'percentage': round((current_amount / case['amount']) * 100, 1) if case['amount'] > 0 else 0
                })

        conn.close()
        return jsonify({'success': True, 'case_limits': case_limits})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ Р»РёРјРёС‚РѕРІ РєРµР№СЃРѕРІ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/update-case-limit', methods=['POST'])
def admin_update_case_limit():
    """РћР±РЅРѕРІР»РµРЅРёРµ Р»РёРјРёС‚Р° РєРѕРЅРєСЂРµС‚РЅРѕРіРѕ РєРµР№СЃР°"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        case_id = data.get('case_id')
        new_limit = data.get('limit')

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        cases = load_cases()
        case = next((c for c in cases if c['id'] == case_id), None)
        if not case:
            return jsonify({'success': False, 'error': 'РљРµР№СЃ РЅРµ РЅР°Р№РґРµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT OR REPLACE INTO case_limits (case_id, current_amount, last_updated)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (case_id, new_limit))

        conn.commit()
        conn.close()

        logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} РѕР±РЅРѕРІРёР» Р»РёРјРёС‚ РєРµР№СЃР° {case_id} РЅР° {new_limit}")
        return jsonify({
            'success': True,
            'message': f'Р›РёРјРёС‚ РєРµР№СЃР° "{case["name"]}" РѕР±РЅРѕРІР»РµРЅ: {new_limit}'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РѕР±РЅРѕРІР»РµРЅРёСЏ Р»РёРјРёС‚Р° РєРµР№СЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/reset-case-limit', methods=['POST'])
def admin_reset_case_limit():
    """РЎР±СЂРѕСЃ Р»РёРјРёС‚Р° РєРѕРЅРєСЂРµС‚РЅРѕРіРѕ РєРµР№СЃР°"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        case_id = data.get('case_id')

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        cases = load_cases()
        case = next((c for c in cases if c['id'] == case_id), None)
        if not case:
            return jsonify({'success': False, 'error': 'РљРµР№СЃ РЅРµ РЅР°Р№РґРµРЅ'})

        if not case.get('limited'):
            return jsonify({'success': False, 'error': 'Р­С‚РѕС‚ РєРµР№СЃ РЅРµ Р»РёРјРёС‚РёСЂРѕРІР°РЅРЅС‹Р№'})

        conn = get_db_connection()
        cursor = conn.cursor()

        original_amount = case['amount']
        cursor.execute('''
            INSERT OR REPLACE INTO case_limits (case_id, current_amount, last_updated)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (case_id, original_amount))

        conn.commit()
        conn.close()

        logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} СЃР±СЂРѕСЃРёР» Р»РёРјРёС‚ РєРµР№СЃР° {case_id} РґРѕ {original_amount}")
        return jsonify({
            'success': True,
            'message': f'Р›РёРјРёС‚ РєРµР№СЃР° "{case["name"]}" СЃР±СЂРѕС€РµРЅ РґРѕ {original_amount}'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃР±СЂРѕСЃР° Р»РёРјРёС‚Р° РєРµР№СЃР°: {e}")
        return jsonify({'success': False, 'error': str(e)})

# ==================== REWARD SYSTEM API ====================

# Level rewards config
LEVEL_REWARDS = [
    {'level': 5, 'stars': 50},
    {'level': 10, 'stars': 100},
    {'level': 15, 'stars': 200},
    {'level': 20, 'stars': 500},
    {'level': 21, 'stars': 1000},
]

# Referral rewards config
REFERRAL_REWARDS_CONFIG = [
    {'count': 1, 'stars': 10},
    {'count': 5, 'stars': 25},
    {'count': 10, 'stars': 50},
    {'count': 25, 'stars': 150},
]

# Default daily tasks templates (auto-generated if none exist)
DEFAULT_DAILY_TASKS = [
    {'task_type': 'open_case', 'case_id': 0, 'target_value': 3, 'reward_stars': 5, 'description': 'РћС‚РєСЂРѕР№ 3 РєРµР№СЃР°'},
    {'task_type': 'open_case', 'case_id': 0, 'target_value': 10, 'reward_stars': 15, 'description': 'РћС‚РєСЂРѕР№ 10 РєРµР№СЃРѕРІ'},
    {'task_type': 'crash_bets', 'case_id': 0, 'target_value': 5, 'reward_stars': 10, 'description': 'РЎРґРµР»Р°Р№ 5 СЃС‚Р°РІРѕРє РІ РєСЂР°С€'},
    {'task_type': 'earn_exp', 'case_id': 0, 'target_value': 50, 'reward_stars': 8, 'description': 'Р—Р°СЂР°Р±РѕС‚Р°Р№ 50 РѕРїС‹С‚Р°'},
    {'task_type': 'turnover', 'case_id': 0, 'target_value': 100, 'reward_stars': 12, 'description': 'РћР±РѕСЂРѕС‚ 100 Р·РІС‘Р·Рґ'},
]

def ensure_daily_tasks():
    """Auto-generate default daily tasks if none exist"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM daily_tasks WHERE is_active = 1')
        count = cursor.fetchone()[0]
        if count == 0:
            for t in DEFAULT_DAILY_TASKS:
                cursor.execute('''
                    INSERT INTO daily_tasks (task_type, case_id, target_value, reward_stars, description, is_active)
                    VALUES (?, ?, ?, ?, ?, 1)
                ''', (t['task_type'], t['case_id'], t['target_value'], t['reward_stars'], t['description']))
            conn.commit()
            logger.info(f"Auto-generated {len(DEFAULT_DAILY_TASKS)} default daily tasks")
        conn.close()
    except Exception as e:
        logger.error(f"Error ensuring daily tasks: {e}")

@app.route('/api/rewards/info/<int:user_id>', methods=['GET'])
def get_rewards_info(user_id):
    """Get all rewards info for user"""
    try:
        ensure_daily_tasks()
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get user data
        cursor.execute('SELECT current_level, experience, referral_count, total_cases_opened, total_crash_bets, balance_stars FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()
        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})
        
        current_level, experience, referral_count, total_cases_opened, total_crash_bets, balance_stars = user
        current_level = current_level or 1
        referral_count = referral_count or 0
        total_cases_opened = total_cases_opened or 0
        total_crash_bets = total_crash_bets or 0
        
        # Get claimed rewards
        cursor.execute('SELECT reward_type, reward_id FROM reward_claims WHERE user_id = ?', (user_id,))
        claimed = set()
        for row in cursor.fetchall():
            claimed.add(f"{row[0]}_{row[1]}")
        
        # Level rewards
        level_rewards = []
        for lr in LEVEL_REWARDS:
            rid = f"level_{lr['level']}"
            level_rewards.append({
                'level': lr['level'],
                'stars': lr['stars'],
                'claimed': rid in claimed,
                'available': current_level >= lr['level'] and rid not in claimed,
                'progress': min(current_level / lr['level'] * 100, 100)
            })
        
        # Referral rewards
        ref_rewards = []
        for rr in REFERRAL_REWARDS_CONFIG:
            rid = f"referral_{rr['count']}"
            ref_rewards.append({
                'count': rr['count'],
                'stars': rr['stars'],
                'claimed': rid in claimed,
                'available': referral_count >= rr['count'] and rid not in claimed,
                'progress': min(referral_count / rr['count'] * 100, 100),
                'current': referral_count
            })
        
        # Daily tasks
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute('SELECT id, task_type, case_id, target_value, reward_stars, description FROM daily_tasks WHERE is_active = 1')
        tasks = cursor.fetchall()
        
        daily_tasks = []
        for task in tasks:
            task_id, task_type, case_id, target_value, reward_stars, description = task
            
            # Get user progress
            cursor.execute('SELECT progress, completed, reward_claimed FROM user_daily_progress WHERE user_id = ? AND task_id = ? AND date = ?',
                          (user_id, task_id, today))
            prog_row = cursor.fetchone()
            progress = prog_row[0] if prog_row else 0
            completed = bool(prog_row[1]) if prog_row else False
            reward_claimed = bool(prog_row[2]) if prog_row else False
            
            daily_tasks.append({
                'id': task_id,
                'type': task_type,
                'case_id': case_id,
                'target': target_value,
                'reward_stars': reward_stars,
                'description': description,
                'progress': progress,
                'completed': completed,
                'reward_claimed': reward_claimed,
                'percentage': min(progress / target_value * 100, 100) if target_value > 0 else 0
            })
        
        conn.close()
        
        return jsonify({
            'success': True,
            'user': {
                'level': current_level,
                'experience': experience or 0,
                'referral_count': referral_count,
                'total_cases_opened': total_cases_opened,
                'total_crash_bets': total_crash_bets,
                'balance_stars': balance_stars or 0
            },
            'level_rewards': level_rewards,
            'referral_rewards': ref_rewards,
            'daily_tasks': daily_tasks
        })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РЅР°РіСЂР°Рґ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/rewards/claim', methods=['POST'])
def claim_reward():
    """Claim a reward (level or referral)"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        reward_type = data.get('reward_type')  # 'level' or 'referral'
        reward_id = data.get('reward_id')  # level number or referral count
        
        if not user_id or not reward_type or reward_id is None:
            return jsonify({'success': False, 'error': 'РќРµРІРµСЂРЅС‹Рµ РїР°СЂР°РјРµС‚СЂС‹'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if already claimed
        full_id = f"{reward_type}_{reward_id}"
        cursor.execute('SELECT id FROM reward_claims WHERE user_id = ? AND reward_type = ? AND reward_id = ?',
                      (user_id, reward_type, str(reward_id)))
        if cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'error': 'РќР°РіСЂР°РґР° СѓР¶Рµ РїРѕР»СѓС‡РµРЅР°'})
        
        # Get user data
        cursor.execute('SELECT current_level, referral_count, balance_stars FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()
        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})
        
        current_level, referral_count, balance_stars = user
        current_level = current_level or 1
        referral_count = referral_count or 0
        
        stars = 0
        
        if reward_type == 'level':
            lr = next((l for l in LEVEL_REWARDS if l['level'] == int(reward_id)), None)
            if not lr:
                conn.close()
                return jsonify({'success': False, 'error': 'РќР°РіСЂР°РґР° РЅРµ РЅР°Р№РґРµРЅР°'})
            if current_level < lr['level']:
                conn.close()
                return jsonify({'success': False, 'error': f'РќСѓР¶РµРЅ СѓСЂРѕРІРµРЅСЊ {lr["level"]}'})
            stars = lr['stars']
        elif reward_type == 'referral':
            rr = next((r for r in REFERRAL_REWARDS_CONFIG if r['count'] == int(reward_id)), None)
            if not rr:
                conn.close()
                return jsonify({'success': False, 'error': 'РќР°РіСЂР°РґР° РЅРµ РЅР°Р№РґРµРЅР°'})
            if referral_count < rr['count']:
                conn.close()
                return jsonify({'success': False, 'error': f'РќСѓР¶РЅРѕ РїСЂРёРіР»Р°СЃРёС‚СЊ {rr["count"]} СЂРµС„РµСЂР°Р»РѕРІ'})
            stars = rr['stars']
        else:
            conn.close()
            return jsonify({'success': False, 'error': 'РќРµРІРµСЂРЅС‹Р№ С‚РёРї РЅР°РіСЂР°РґС‹'})
        
        # Claim reward
        cursor.execute('INSERT INTO reward_claims (user_id, reward_type, reward_id, reward_stars) VALUES (?, ?, ?, ?)',
                      (user_id, reward_type, str(reward_id), stars))
        cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (stars, user_id))
        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        logger.info(f"рџЋЃ РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {user_id} РїРѕР»СѓС‡РёР» РЅР°РіСЂР°РґСѓ {reward_type}_{reward_id}: {stars} Р·РІС‘Р·Рґ")
        
        return jsonify({
            'success': True,
            'stars': stars,
            'new_balance': new_balance,
            'message': f'РџРѕР»СѓС‡РµРЅРѕ {stars} Р·РІС‘Р·Рґ!'
        })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РЅР°РіСЂР°РґС‹: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/rewards/claim-daily', methods=['POST'])
def claim_daily_task_reward():
    """Claim daily task reward"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        task_id = data.get('task_id')
        
        if not user_id or not task_id:
            return jsonify({'success': False, 'error': 'РќРµРІРµСЂРЅС‹Рµ РїР°СЂР°РјРµС‚СЂС‹'})
        
        today = datetime.now().strftime('%Y-%m-%d')
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check progress
        cursor.execute('SELECT progress, completed, reward_claimed FROM user_daily_progress WHERE user_id = ? AND task_id = ? AND date = ?',
                      (user_id, task_id, today))
        prog = cursor.fetchone()
        
        if not prog or not prog[1]:
            conn.close()
            return jsonify({'success': False, 'error': 'Р—Р°РґР°РЅРёРµ РЅРµ РІС‹РїРѕР»РЅРµРЅРѕ'})
        if prog[2]:
            conn.close()
            return jsonify({'success': False, 'error': 'РќР°РіСЂР°РґР° СѓР¶Рµ РїРѕР»СѓС‡РµРЅР°'})
        
        # Get task info
        cursor.execute('SELECT reward_stars FROM daily_tasks WHERE id = ?', (task_id,))
        task = cursor.fetchone()
        if not task:
            conn.close()
            return jsonify({'success': False, 'error': 'Р—Р°РґР°РЅРёРµ РЅРµ РЅР°Р№РґРµРЅРѕ'})
        
        stars = task[0]
        
        cursor.execute('UPDATE user_daily_progress SET reward_claimed = 1 WHERE user_id = ? AND task_id = ? AND date = ?',
                      (user_id, task_id, today))
        cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (stars, user_id))
        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'stars': stars,
            'new_balance': new_balance,
            'message': f'РџРѕР»СѓС‡РµРЅРѕ {stars} Р·РІС‘Р·Рґ!'
        })
        
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РЅР°РіСЂР°РґС‹ Р·Р° Р·Р°РґР°РЅРёРµ: {e}")
        return jsonify({'success': False, 'error': str(e)})

# Helper: update daily task progress (call from case open, crash bet, etc.)
def update_daily_task_progress(user_id, task_type, amount=1, case_id=None):
    """Update user's daily task progress"""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get active tasks of this type
        if task_type == 'open_case' and case_id:
            cursor.execute('SELECT id, target_value, case_id FROM daily_tasks WHERE is_active = 1 AND task_type = ?', (task_type,))
        else:
            cursor.execute('SELECT id, target_value, case_id FROM daily_tasks WHERE is_active = 1 AND task_type = ?', (task_type,))
        
        tasks = cursor.fetchall()
        
        for task in tasks:
            tid, target, t_case_id = task
            
            # For case tasks, check if specific case required
            if task_type == 'open_case' and t_case_id and t_case_id > 0 and case_id and t_case_id != case_id:
                continue
            
            # Upsert progress
            cursor.execute('''
                INSERT INTO user_daily_progress (user_id, task_id, progress, completed, reward_claimed, date) 
                VALUES (?, ?, ?, 0, 0, ?)
                ON CONFLICT(user_id, task_id, date) DO UPDATE SET 
                    progress = MIN(progress + ?, ?)
            ''', (user_id, tid, min(amount, target), today, amount, target))
            
            # Check completion
            cursor.execute('SELECT progress FROM user_daily_progress WHERE user_id = ? AND task_id = ? AND date = ?',
                          (user_id, tid, today))
            row = cursor.fetchone()
            if row and row[0] >= target:
                cursor.execute('UPDATE user_daily_progress SET completed = 1 WHERE user_id = ? AND task_id = ? AND date = ?',
                              (user_id, tid, today))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РѕР±РЅРѕРІР»РµРЅРёСЏ РїСЂРѕРіСЂРµСЃСЃР° Р·Р°РґР°РЅРёСЏ: {e}")

# ==================== ADMIN DAILY TASKS API ====================

@app.route('/api/admin/daily-tasks', methods=['GET', 'POST'])
def admin_daily_tasks():
    """Admin manage daily tasks"""
    if request.method == 'GET':
        try:
            admin_id = request.args.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
            
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT id, task_type, case_id, target_value, reward_stars, description, is_active FROM daily_tasks ORDER BY id DESC')
            tasks = cursor.fetchall()
            conn.close()
            
            return jsonify({
                'success': True,
                'tasks': [{'id': t[0], 'task_type': t[1], 'case_id': t[2], 'target_value': t[3], 
                           'reward_stars': t[4], 'description': t[5], 'is_active': bool(t[6])} for t in tasks]
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    
    else:  # POST
        try:
            data = request.get_json()
            admin_id = data.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
            
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO daily_tasks (task_type, case_id, target_value, reward_stars, description)
                VALUES (?, ?, ?, ?, ?)
            ''', (data['task_type'], data.get('case_id', 0), data['target_value'], data['reward_stars'], data['description']))
            conn.commit()
            conn.close()
            
            return jsonify({'success': True, 'message': 'Р—Р°РґР°РЅРёРµ СЃРѕР·РґР°РЅРѕ'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/daily-tasks/toggle', methods=['POST'])
def admin_toggle_daily_task():
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE daily_tasks SET is_active = ? WHERE id = ?', (1 if data['is_active'] else 0, data['task_id']))
        conn.commit()
        conn.close()
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/daily-tasks/delete', methods=['POST'])
def admin_delete_daily_task():
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM daily_tasks WHERE id = ?', (data['task_id'],))
        conn.commit()
        conn.close()
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ============================================================
# GIFTS LIST API (for gift deposit menu)
# ============================================================

@app.route('/api/online-count', methods=['GET'])
def api_online_count():
    """Return real online user count вЂ” users active in last 5 minutes"""
    try:
        conn = get_db_connection()
        row = conn.execute("SELECT COUNT(*) FROM users WHERE last_active > datetime('now', '-5 minutes')").fetchone()
        count = row[0] if row else 0
        conn.close()
        return jsonify({'count': count})
    except Exception:
        return jsonify({'count': 0})

# ============================================================

@app.route('/api/gifts-list', methods=['GET'])
def api_gifts_list():
    """Return gifts catalog for UI: originals + all models from Fragment cache"""
    try:
        force_refresh = request.args.get('refresh', '0') in ('1', 'true', 'yes')
        include_models = request.args.get('models', '0') not in ('0', 'false', 'no')
        if include_models:
            merged = build_full_catalog_with_models(force_refresh=force_refresh)
        else:
            merged = build_fragment_first_gifts_catalog(force_refresh=force_refresh)
        return jsonify({
            'success': True,
            'gifts': merged,
            'fragment_sync': True,
            'fragment_only_mode': FRAGMENT_ONLY_CATALOG,
            'fragment_error': fragment_last_error,
            'offline_fallback_used': any(str(g.get('source')) == 'local_offline_fallback' for g in (merged or []))
        })
    except Exception as e:
        logger.error(f"Gifts list error: {e}")
        return jsonify({'success': True, 'gifts': []})

@app.route('/api/fragment-gift-models', methods=['GET'])
def api_fragment_gift_models():
    """Return original gift and all available Fragment models for selected gift.
    Uses disk-cached models first; falls back to live scraping if available."""
    try:
        raw_slug = request.args.get('slug', '')
        raw_name = request.args.get('name', '')
        force_refresh = request.args.get('refresh', '0') in ('1', 'true', 'yes')

        fragment_slug = _slugify_fragment_name(raw_slug) if raw_slug else _slugify_fragment_name(raw_name)
        if not fragment_slug:
            return jsonify({'success': False, 'error': 'slug is required'})

        merged_catalog = build_fragment_first_gifts_catalog(force_refresh=force_refresh)
        merged_item = next((g for g in merged_catalog if (g.get('fragment_slug') or '').strip().lower() == fragment_slug), None)

        base_name = (
            (merged_item or {}).get('name')
            or raw_name
            or fragment_slug
        )
        base_value = _safe_int((merged_item or {}).get('value'), 0)
        base_image = (
            (merged_item or {}).get('image')
            or f'https://fragment.com/file/gifts/{fragment_slug}/thumb.webp'
        )
        original_id = (merged_item or {}).get('id')
        if original_id is None:
            original_id = _build_case_custom_gift_id(base_name, fragment_slug=fragment_slug)

        original = {
            'id': original_id,
            'gift_key': _build_case_custom_gift_id(base_name, fragment_slug=fragment_slug),
            'name': base_name,
            'image': base_image,
            'value': base_value,
            'type': 'fragment_original',
            'fragment_slug': fragment_slug,
            'fragment_url': f'https://fragment.com/gifts/{fragment_slug}',
            'fragment_price_ton': (merged_item or {}).get('fragment_price_ton'),
            'getgems_floor_ton': (merged_item or {}).get('getgems_floor_ton')
        }

        # 1) Try disk-cached models first (always available on PythonAnywhere)
        cache_key = _fragment_model_cache_key(fragment_slug)
        cached_models = fragment_models_cache.get(cache_key, []) if cache_key else []

        if cached_models and not force_refresh:
            models = cached_models
        else:
            # 2) Try live fetch (may fail on PythonAnywhere with no internet)
            models = fetch_fragment_gift_models(
                fragment_slug,
                base_name=base_name,
                base_value=base_value,
                base_image=base_image,
                force_refresh=force_refresh
            )
            # 3) Fall back to cached if live fetch returned nothing
            if not models and cached_models:
                models = cached_models

        if not models and merged_item and str((merged_item or {}).get('source')) == 'local':
            models = [{
                'id': merged_item.get('id'),
                'gift_key': _build_case_custom_gift_id(merged_item.get('name', base_name), fragment_slug=fragment_slug),
                'name': merged_item.get('name', base_name),
                'image': _normalize_local_gift_image(merged_item.get('image', base_image)) or base_image,
                'value': _safe_int(merged_item.get('value'), base_value),
                'type': 'local_fallback',
                'fragment_slug': fragment_slug,
                'fragment_url': f'https://fragment.com/gifts/{fragment_slug}',
                'model_name': merged_item.get('name', base_name)
            }]

        return jsonify({
            'success': True,
            'original': original,
            'models': models,
            'total_models': len(models)
        })
    except Exception as e:
        logger.error(f"Fragment models API error: {e}")
        return jsonify({'success': False, 'error': str(e), 'models': [], 'total_models': 0})


# Bot info cache
_bot_info_cache = {'username': None, 'name': None, 'fetched': False}

@app.route('/api/bot-info', methods=['GET'])
def api_bot_info():
    """Return bot username and name for frontend"""
    try:
        if not _bot_info_cache['fetched']:
            result = tg_api('getMe')
            if result.get('ok'):
                bot_data = result.get('result', {})
                _bot_info_cache['username'] = bot_data.get('username', '')
                _bot_info_cache['name'] = bot_data.get('first_name', '')
                _bot_info_cache['fetched'] = True
        return jsonify({
            'success': True,
            'username': _bot_info_cache.get('username', ''),
            'name': _bot_info_cache.get('name', '')
        })
    except Exception as e:
        logger.error(f"Bot info error: {e}")
        return jsonify({'success': False, 'username': '', 'name': ''})


@app.route('/api/gift-deposits', methods=['GET'])
def api_gift_deposits_history():
    """Return recent gift deposit history for admin"""
    try:
        user_id = request.args.get('user_id')
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS gift_deposits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            gift_name TEXT NOT NULL,
            gift_value INTEGER NOT NULL,
            gift_type TEXT DEFAULT 'regular',
            telegram_gift_id TEXT,
            message_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        if user_id:
            cursor.execute('SELECT * FROM gift_deposits WHERE user_id = ? ORDER BY created_at DESC LIMIT 50', (user_id,))
        else:
            cursor.execute('SELECT * FROM gift_deposits ORDER BY created_at DESC LIMIT 100')
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        deposits = [dict(zip(cols, row)) for row in rows]
        conn.close()
        return jsonify({'success': True, 'deposits': deposits})
    except Exception as e:
        logger.error(f"Gift deposits history error: {e}")
        return jsonify({'success': True, 'deposits': []})


# ============================================================
# NFT GIFT DEPOSIT SYSTEM
# ============================================================

@app.route('/api/gift-deposit/process', methods=['POST'])
def process_gift_deposit():
    """Process an NFT gift transfer - called by bot when it detects a gift transfer"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data'})
        
        # Verify it's from our bot or admin
        secret = data.get('secret')
        if secret != app.secret_key and secret != 'raswet-gift-deposit-key':
            admin_id = data.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Unauthorized'})
        
        user_id = data.get('user_id')
        gift_name = data.get('gift_name', '')
        
        if not user_id or not gift_name:
            return jsonify({'success': False, 'error': 'Missing user_id or gift_name'})
        
        # Find the gift in catalog (Fragment + local)
        gifts = build_full_catalog_with_models()
        if not gifts:
            gifts = load_gifts_cached() or []
        found_gift = None
        gift_name_lower = gift_name.lower().strip()
        
        for g in gifts:
            if g.get('name', '').lower().strip() == gift_name_lower:
                found_gift = g
                break
        
        # Try partial match if exact not found
        if not found_gift:
            for g in gifts:
                gname = g.get('name', '').lower().strip()
                # Remove "(Random)" suffix for matching
                clean_name = gname.replace('(random)', '').strip()
                clean_input = gift_name_lower.replace('(random)', '').strip()
                if clean_name == clean_input or clean_name in clean_input or clean_input in clean_name:
                    found_gift = g
                    break
        
        if not found_gift:
            logger.warning(f"Gift not found: '{gift_name}' from user {user_id}")
            return jsonify({'success': False, 'error': f'Gift not found: {gift_name}'})
        
        value = found_gift.get('value', 0)
        if value <= 0:
            return jsonify({'success': False, 'error': 'Gift has no value'})
        
        # Credit the user's balance
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check user exists
        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()
        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'User not found'})
        
        # Atomic balance update
        cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (value, user_id))
        
        # Log the deposit
        cursor.execute('''CREATE TABLE IF NOT EXISTS gift_deposits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            gift_name TEXT NOT NULL,
            gift_value INTEGER NOT NULL,
            gift_type TEXT DEFAULT 'regular',
            telegram_gift_id TEXT,
            message_id INTEGER,
            status TEXT DEFAULT 'confirmed',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        cursor.execute('INSERT INTO gift_deposits (user_id, gift_name, gift_value) VALUES (?, ?, ?)',
                      (user_id, gift_name, value))
        
        # РџРѕР»СѓС‡Р°РµРј РЅРѕРІС‹Р№ Р±Р°Р»Р°РЅСЃ
        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
        new_balance = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        # Invalidate user cache
        if user_id in _user_cache:
            del _user_cache[user_id]
        
        # Send notification to user via Telegram
        try:
            msg_text = f"РџРѕРґР°СЂРѕРє СѓСЃРїРµС€РЅРѕ РїРѕР»СѓС‡РµРЅ, РІР°Рј РЅР°С‡РёСЃР»РµРЅРѕ {value} Р·РІС‘Р·Рґ."
            tg_url = f"{TG_API}/sendMessage"
            http_requests.post(tg_url, json={
                'chat_id': user_id,
                'text': msg_text,
                'parse_mode': 'HTML'
            }, timeout=5)
        except Exception as e:
            logger.error(f"Failed to notify user {user_id}: {e}")
        
        logger.info(f"Gift deposit: user {user_id} received '{gift_name}' worth {value} stars")
        return jsonify({
            'success': True,
            'gift_name': found_gift.get('name'),
            'value': value,
            'new_balance': new_balance
        })
        
    except Exception as e:
        logger.error(f"Gift deposit error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})


# ============================================================
# NFT GIFT MONITOR SYSTEM
# ============================================================

# РђРєРєР°СѓРЅС‚ РґР»СЏ РјРѕРЅРёС‚РѕСЂРёРЅРіР° NFT РїРѕРґР°СЂРєРѕРІ: @RasswetGiftsRelayer
NFT_MONITOR_USERNAME = 'RasswetGiftsRelayer'
NFT_MONITOR_USER_ID = None  # Will be resolved from username on startup
_nft_monitor_running = False

def _resolve_nft_monitor_user():
    """Resolve NFT_MONITOR_USERNAME to user_id via DB or Telegram API"""
    global NFT_MONITOR_USER_ID
    # 1. Try to find in our users DB
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM users WHERE username = ?', (NFT_MONITOR_USERNAME,))
        row = cursor.fetchone()
        conn.close()
        if row:
            NFT_MONITOR_USER_ID = row[0]
            logger.info(f"NFT Monitor: resolved @{NFT_MONITOR_USERNAME} -> {NFT_MONITOR_USER_ID} from DB")
            return True
    except Exception as e:
        logger.warning(f"NFT Monitor: DB lookup failed: {e}")

    # 2. Try Telegram getChat
    try:
        result = tg_api('getChat', chat_id=f'@{NFT_MONITOR_USERNAME}')
        if result.get('ok'):
            chat = result.get('result', {})
            NFT_MONITOR_USER_ID = chat.get('id')
            logger.info(f"NFT Monitor: resolved @{NFT_MONITOR_USERNAME} -> {NFT_MONITOR_USER_ID} from Telegram")
            return True
    except Exception as e:
        logger.warning(f"NFT Monitor: Telegram lookup failed: {e}")

    # 3. Fallback to ADMIN_ID
    NFT_MONITOR_USER_ID = ADMIN_ID
    logger.warning(f"NFT Monitor: could not resolve @{NFT_MONITOR_USERNAME}, using ADMIN_ID={ADMIN_ID}")
    return False

def _match_gift_by_name(base_name):
    """Match a Telegram gift base_name against gifts catalog (Fragment + local)"""
    gifts = build_full_catalog_with_models()
    if not gifts:
        gifts = load_gifts_cached() or []
    if not gifts:
        return None
    bn_lower = base_name.lower().strip()
    # Exact match with "(Random)" stripped
    for g in gifts:
        gname = g.get('name', '')
        clean = gname.lower().replace('(random)', '').strip()
        if clean == bn_lower:
            return g
    # Partial / fuzzy match
    for g in gifts:
        gname = g.get('name', '').lower()
        clean = gname.replace('(random)', '').strip()
        if bn_lower in clean or clean in bn_lower:
            return g
    return None


def nft_gift_monitor_loop():
    """Background thread: polls getUserGifts for unique/NFT gifts sent to the monitored account"""
    global _nft_monitor_running
    _nft_monitor_running = True

    # Wait for app to be fully initialized
    time.sleep(10)

    # Resolve @RasswetGiftsRelayer -> user_id
    _resolve_nft_monitor_user()
    if not NFT_MONITOR_USER_ID:
        logger.error("NFT Monitor: could not resolve user_id, stopping")
        _nft_monitor_running = False
        return
    logger.info(f"NFT Gift Monitor started: @{NFT_MONITOR_USERNAME} (user_id={NFT_MONITOR_USER_ID})")

    while _nft_monitor_running:
        try:
            # Ensure the tracking table exists
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS nft_monitor_processed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gift_type TEXT NOT NULL,
                gift_name TEXT NOT NULL,
                gift_base_name TEXT,
                sender_id INTEGER,
                send_date INTEGER NOT NULL,
                stars_credited INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            conn.commit()
            conn.close()

            # Call getUserGifts вЂ” get unique (NFT) gifts only
            result = tg_api('getUserGifts', user_id=NFT_MONITOR_USER_ID, exclude_unlimited=True,
                           exclude_limited_upgradable=True, exclude_limited_non_upgradable=True,
                           limit=100)

            if not result.get('ok'):
                logger.warning(f"NFT Monitor: getUserGifts failed: {result}")
                time.sleep(10)
                continue

            owned_gifts = result.get('result', {})
            gifts_list = owned_gifts.get('gifts', [])
            
            if not gifts_list:
                time.sleep(10)
                continue

            for og in gifts_list:
                try:
                    gift_type = og.get('type', '')  # "regular" or "unique"
                    sender = og.get('sender_user')
                    send_date = og.get('send_date', 0)

                    if not sender or not send_date:
                        continue

                    sender_id = sender.get('id', 0)
                    if not sender_id or sender_id == NFT_MONITOR_USER_ID:
                        continue  # Skip self-gifts

                    # Get gift info based on type
                    if gift_type == 'unique':
                        gift_obj = og.get('gift', {})
                        base_name = gift_obj.get('base_name', '')
                        unique_name = gift_obj.get('name', '')
                        gift_number = gift_obj.get('number', 0)
                        display_name = f"{base_name} #{gift_number}" if gift_number else base_name
                    elif gift_type == 'regular':
                        gift_obj = og.get('gift', {})
                        base_name = ''  # Regular gifts don't have base_name
                        display_name = f"Regular Gift ({gift_obj.get('star_count', 0)} Stars)"
                        # For regular gifts, skip вЂ” user asked about NFT only
                        continue
                    else:
                        continue

                    if not base_name:
                        continue

                    # Check if already processed (dedup by sender_id + send_date)
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute(
                        'SELECT id FROM nft_monitor_processed WHERE sender_id = ? AND send_date = ?',
                        (sender_id, send_date))
                    if cursor.fetchone():
                        conn.close()
                        continue

                    # Match against gifts.json
                    matched_gift = _match_gift_by_name(base_name)
                    if not matched_gift:
                        logger.warning(f"NFT Monitor: gift '{base_name}' not found in gifts.json (sender={sender_id})")
                        # Record as processed anyway to avoid spam
                        cursor.execute('''INSERT INTO nft_monitor_processed 
                            (gift_type, gift_name, gift_base_name, sender_id, send_date, stars_credited)
                            VALUES (?, ?, ?, ?, ?, 0)''',
                            (gift_type, display_name, base_name, sender_id, send_date))
                        conn.commit()
                        conn.close()
                        # Notify admin about unmatched gift
                        try:
                            tg_send(ADMIN_ID,
                                f"NFT Monitor: РїРѕРґР°СЂРѕРє <b>{base_name}</b> РѕС‚ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {sender_id} "
                                f"РЅРµ РЅР°Р№РґРµРЅ РІ gifts.json. Р—РІС‘Р·РґС‹ РЅРµ РЅР°С‡РёСЃР»РµРЅС‹.")
                        except Exception:
                            pass
                        continue

                    value = matched_gift.get('value', 0)
                    if value <= 0:
                        conn.close()
                        continue

                    # Check sender exists in our DB
                    cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (sender_id,))
                    user = cursor.fetchone()
                    if not user:
                        # Record as processed but don't credit
                        cursor.execute('''INSERT INTO nft_monitor_processed 
                            (gift_type, gift_name, gift_base_name, sender_id, send_date, stars_credited)
                            VALUES (?, ?, ?, ?, ?, 0)''',
                            (gift_type, display_name, base_name, sender_id, send_date))
                        conn.commit()
                        conn.close()
                        logger.info(f"NFT Monitor: sender {sender_id} not registered, gift '{base_name}' skipped")
                        continue

                    # Credit stars to sender (atomic)
                    cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (value, sender_id))

                    # Read new balance for notification
                    cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (sender_id,))
                    new_balance = cursor.fetchone()[0]

                    # Record processed
                    cursor.execute('''INSERT INTO nft_monitor_processed 
                        (gift_type, gift_name, gift_base_name, sender_id, send_date, stars_credited)
                        VALUES (?, ?, ?, ?, ?, ?)''',
                        (gift_type, display_name, base_name, sender_id, send_date, value))

                    # Also record in gift_deposits for history
                    cursor.execute('''INSERT INTO gift_deposits 
                        (user_id, gift_name, gift_value, gift_type, telegram_gift_id)
                        VALUES (?, ?, ?, 'nft', ?)''',
                        (sender_id, matched_gift.get('name', base_name), value, unique_name if gift_type == 'unique' else ''))

                    conn.commit()
                    conn.close()

                    # Invalidate cache
                    if sender_id in _user_cache:
                        del _user_cache[sender_id]

                    logger.info(f"NFT Monitor: credited {value} stars to user {sender_id} for '{base_name}'")

                    # Notify sender
                    try:
                        tg_send(sender_id,
                            f"РџРѕРґР°СЂРѕРє <b>{matched_gift.get('name', base_name)}</b> Р±С‹Р» РѕР±РјРµРЅРµРЅ РЅР° <b>{value}</b> Р·РІС‘Р·Рґ в­ђ\n\n"
                            f"РќРѕРІС‹Р№ Р±Р°Р»Р°РЅСЃ: <b>{new_balance}</b> Р·РІС‘Р·Рґ")
                    except Exception:
                        pass

                    # Notify admin
                    if sender_id != ADMIN_ID:
                        try:
                            sender_name = sender.get('first_name', '') or str(sender_id)
                            tg_send(ADMIN_ID,
                                f"NFT Deposit: <b>{sender_name}</b> ({sender_id})\n"
                                f"РџРѕРґР°СЂРѕРє: {matched_gift.get('name', base_name)}\n"
                                f"РЎС‚РѕРёРјРѕСЃС‚СЊ: {value} Р·РІС‘Р·Рґ")
                        except Exception:
                            pass

                except Exception as gift_err:
                    logger.error(f"NFT Monitor: error processing gift: {gift_err}")
                    try: conn.close()
                    except: pass
                    continue

        except Exception as e:
            logger.error(f"NFT Monitor loop error: {e}")

        time.sleep(10)  # Check every 10 seconds


def start_nft_monitor():
    """Start the NFT gift monitor background thread"""
    global _nft_monitor_running
    if _nft_monitor_running:
        return
    thread = threading.Thread(target=nft_gift_monitor_loop, daemon=True)
    thread.start()
    logger.info("NFT Gift Monitor thread started")


@app.route('/api/admin/nft-monitor', methods=['GET'])
def admin_nft_monitor_status():
    """Get NFT monitor status"""
    user_id = request.args.get('user_id')
    if not user_id or int(user_id) != ADMIN_ID:
        return jsonify({'success': False, 'error': 'Unauthorized'})
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS nft_monitor_processed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gift_type TEXT NOT NULL,
            gift_name TEXT NOT NULL,
            gift_base_name TEXT,
            sender_id INTEGER,
            send_date INTEGER NOT NULL,
            stars_credited INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        cursor.execute('SELECT COUNT(*) FROM nft_monitor_processed')
        total = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM nft_monitor_processed WHERE stars_credited > 0')
        credited = cursor.fetchone()[0]
        cursor.execute('SELECT SUM(stars_credited) FROM nft_monitor_processed WHERE stars_credited > 0')
        total_stars = cursor.fetchone()[0] or 0
        # Last 10 processed
        cursor.execute('SELECT * FROM nft_monitor_processed ORDER BY created_at DESC LIMIT 10')
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        recent = [dict(zip(cols, row)) for row in rows]
        conn.close()
        
        return jsonify({
            'success': True,
            'running': _nft_monitor_running,
            'monitor_username': NFT_MONITOR_USERNAME,
            'monitor_user_id': NFT_MONITOR_USER_ID,
            'total_processed': total,
            'total_credited': credited,
            'total_stars': total_stars,
            'recent': recent
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/nft-monitor', methods=['POST'])
def admin_nft_monitor_set():
    """Set NFT monitor user ID"""
    global NFT_MONITOR_USER_ID
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data'})
    
    admin_id = data.get('admin_id')
    if not admin_id or int(admin_id) != ADMIN_ID:
        return jsonify({'success': False, 'error': 'Unauthorized'})
    
    new_user_id = data.get('monitor_user_id')
    if new_user_id:
        NFT_MONITOR_USER_ID = int(new_user_id)
        logger.info(f"NFT Monitor: user_id changed to {NFT_MONITOR_USER_ID}")
    
    # Start monitor if not running
    if not _nft_monitor_running:
        start_nft_monitor()
    
    return jsonify({
        'success': True,
        'monitor_user_id': NFT_MONITOR_USER_ID,
        'running': _nft_monitor_running
    })


# ============================================================
# SHOP DEALS SYSTEM
# ============================================================

@app.route('/api/shop/deals', methods=['GET'])
def get_shop_deals():
    """Get active shop deals for users"""
    try:
        user_id = request.args.get('user_id', 0, type=int)
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS shop_deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, description TEXT,
            section TEXT DEFAULT 'general', items TEXT NOT NULL DEFAULT '[]',
            price INTEGER NOT NULL DEFAULT 100, old_price INTEGER DEFAULT 0,
            currency TEXT DEFAULT 'stars', duration_hours INTEGER DEFAULT 0,
            starts_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, ends_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE, sort_order INTEGER DEFAULT 0, icon TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS shop_purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            deal_id INTEGER NOT NULL, price_paid INTEGER NOT NULL,
            currency TEXT DEFAULT 'stars', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()

        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''SELECT id, title, description, section, items, price, old_price, currency,
                          duration_hours, starts_at, ends_at, icon, created_at
                          FROM shop_deals WHERE is_active = 1
                          AND (ends_at IS NULL OR ends_at > ?)
                          ORDER BY sort_order ASC, id DESC''', (now,))
        deals = cursor.fetchall()

        # Get user's owned items + purchases
        owned_rockets = set()
        owned_backgrounds = set()
        purchased_deals = set()
        is_vip = False
        if user_id > 0:
            cursor.execute('SELECT item_type, item_id FROM user_customizations WHERE user_id = ?', (user_id,))
            for r in cursor.fetchall():
                if r[0] == 'rocket': owned_rockets.add(r[1])
                elif r[0] == 'background': owned_backgrounds.add(r[1])
            cursor.execute('SELECT deal_id FROM shop_purchases WHERE user_id = ?', (user_id,))
            purchased_deals = set(r[0] for r in cursor.fetchall())
            cursor.execute('SELECT is_crash_vip FROM users WHERE id = ?', (user_id,))
            vr = cursor.fetchone()
            is_vip = bool(vr and vr[0])

        # Pre-fetch crate images for enrichment
        crate_images_cache = {}
        try:
            init_crates_tables(cursor)
            conn.commit()
            cursor.execute('SELECT id, name, image FROM crates')
            for cr in cursor.fetchall():
                crate_images_cache[str(cr[0])] = {'name': cr[1], 'image': cr[2] or ''}
        except:
            pass

        result = []
        for d in deals:
            did, title, desc, section, items_json, price, old_price, currency, dur, starts, ends, icon, created = d
            items = []
            try: items = json.loads(items_json) if items_json else []
            except: pass

            # Enrich crate items with image/name from crates table
            for it in items:
                if it.get('type') == 'crate':
                    cid = str(it.get('crate_id') or it.get('item_id', ''))
                    if cid in crate_images_cache:
                        if not it.get('image'):
                            it['image'] = crate_images_cache[cid].get('image', '')
                        if not it.get('name'):
                            it['name'] = crate_images_cache[cid].get('name', '')

            # Calculate owned items count for discount
            owned_count = 0
            total_items = len(items)
            for it in items:
                itype = it.get('type', '')
                iid = it.get('item_id', '')
                if itype == 'rocket' and iid in owned_rockets: owned_count += 1
                elif itype == 'background' and iid in owned_backgrounds: owned_count += 1
                elif itype == 'vip' and is_vip: owned_count += 1

            # Mark each item as owned
            for it in items:
                itype = it.get('type', '')
                iid = it.get('item_id', '')
                it['owned'] = False
                if itype == 'rocket' and iid in owned_rockets: it['owned'] = True
                elif itype == 'background' and iid in owned_backgrounds: it['owned'] = True
                elif itype == 'vip' and is_vip: it['owned'] = True

            # Discount: reduce price by portion of owned items
            discount = 0
            if total_items > 0 and owned_count > 0:
                discount = int(price * owned_count / total_items)

            # Time remaining
            time_left = None
            if ends:
                try:
                    end_dt = datetime.strptime(ends, '%Y-%m-%d %H:%M:%S')
                    diff = (end_dt - datetime.utcnow()).total_seconds()
                    time_left = max(0, int(diff))
                except: pass

            try:
                created_ts = int(datetime.strptime(created, '%Y-%m-%d %H:%M:%S').timestamp()) if created else 0
            except:
                created_ts = 0
            result.append({
                'id': did, 'title': title, 'description': desc or '',
                'section': section or 'general', 'items': items,
                'price': price, 'old_price': old_price or 0,
                'final_price': max(0, price - discount),
                'discount': discount, 'currency': currency or 'stars',
                'time_left': time_left, 'icon': icon or '',
                'purchased': did in purchased_deals,
                'owned_count': owned_count, 'total_items': total_items,
                'created_at': created_ts
            })

        conn.close()
        return jsonify({'success': True, 'deals': result})
    except Exception as e:
        logger.error(f"Shop deals error: {e}")
        try: conn.close()
        except: pass
        return jsonify({'success': True, 'deals': []})


def grant_reward_with_comp(cursor, user_id, reward_type, amount, item_id_val, owned_rockets_set, owned_backgrounds_set, is_vip):
    """Grant a reward to user, applying compensation if they already own an item.
    Returns list of human-readable messages about granted/compensated rewards."""
    msgs = []
    try:
        if reward_type == 'stars':
            amt = int(amount or 0)
            if amt > 0:
                cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (amt, user_id))
                msgs.append(f'+{amt} Р·РІС‘Р·Рґ')
        elif reward_type == 'tickets':
            amt = int(amount or 0)
            if amt > 0:
                cursor.execute('UPDATE users SET balance_tickets = balance_tickets + ? WHERE id = ?', (amt, user_id))
                msgs.append(f'+{amt} Р±РёР»РµС‚РѕРІ')
        elif reward_type == 'vip':
            if is_vip:
                cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (200, user_id))
                msgs.append('+200 Р·РІС‘Р·Рґ (РєРѕРјРїРµРЅСЃР°С†РёСЏ Р·Р° VIP)')
            else:
                cursor.execute('UPDATE users SET is_crash_vip = 1 WHERE id = ?', (user_id,))
                msgs.append('VIP')
        elif reward_type == 'rocket':
            iid = str(item_id_val or '')
            if iid in owned_rockets_set:
                cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (50, user_id))
                msgs.append('+50 Р·РІС‘Р·Рґ (РєРѕРјРїРµРЅСЃР°С†РёСЏ Р·Р° СЂР°РєРµС‚Сѓ)')
            else:
                cursor.execute('INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source) VALUES (?, ?, ?, ?)',
                               (user_id, 'rocket', iid, 'reward'))
                msgs.append(f'Р Р°РєРµС‚Р°: {iid}')
        elif reward_type == 'background':
            iid = str(item_id_val or '')
            if iid in owned_backgrounds_set:
                cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (50, user_id))
                msgs.append('+50 Р·РІС‘Р·Рґ (РєРѕРјРїРµРЅСЃР°С†РёСЏ Р·Р° С„РѕРЅ)')
            else:
                cursor.execute('INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source) VALUES (?, ?, ?, ?)',
                               (user_id, 'background', iid, 'reward'))
                msgs.append(f'Р¤РѕРЅ: {iid}')
        elif reward_type == 'crate':
            # Add crate(s) to inventory as unopened item(s)
            try:
                cid = int(item_id_val)
                quantity = max(1, int(amount)) if amount else 1
                cursor.execute('SELECT name, image FROM crates WHERE id = ?', (cid,))
                crate_row = cursor.fetchone()
                crate_name = crate_row[0] if crate_row else f'РЇС‰РёРє #{cid}'
                crate_image = crate_row[1] if crate_row and crate_row[1] else '/static/img/star2.png'
                for _ in range(quantity):
                    cursor.execute('''INSERT INTO inventory (user_id, gift_id, gift_name, gift_image, gift_value, is_withdrawing, crate_id, crate_name, crate_image)
                        VALUES (?, 0, ?, ?, 0, 0, ?, ?, ?)''',
                        (user_id, crate_name, crate_image, cid, crate_name, crate_image))
                if quantity > 1:
                    msgs.append(f'РЇС‰РёРє: {crate_name} x{quantity}')
                else:
                    msgs.append(f'РЇС‰РёРє: {crate_name}')
            except Exception as ce:
                logger.error(f"Error adding crate to inventory in helper: {ce}")
    except Exception as e:
        logger.error(f"grant_reward_with_comp error: {e}")
    return msgs


@app.route('/api/admin/case-sections', methods=['GET', 'POST', 'DELETE'])
def admin_case_sections():
    """РЈРїСЂР°РІР»РµРЅРёРµ СЂР°Р·РґРµР»Р°РјРё РєРµР№СЃРѕРІ"""
    try:
        payload = request.get_json(silent=True) or {}
        admin_id = request.args.get('admin_id') or payload.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        if request.method == 'GET':
            sections = load_case_sections()
            return jsonify({'success': True, 'sections': sections})

        if request.method == 'POST':
            name = str(payload.get('name') or '').strip()
            if not name:
                return jsonify({'success': False, 'error': 'Р’РІРµРґРёС‚Рµ РЅР°Р·РІР°РЅРёРµ СЂР°Р·РґРµР»Р°'})

            section_id = normalize_section_id(payload.get('id') or name)
            sections = load_case_sections()
            if any(s.get('id') == section_id for s in sections):
                return jsonify({'success': False, 'error': 'Р Р°Р·РґРµР» СЃ С‚Р°РєРёРј ID СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚'})

            next_order = max([int(s.get('order', 0)) for s in sections], default=0) + 1
            new_section = {'id': section_id, 'name': name, 'order': next_order}
            sections.append(new_section)
            sections.sort(key=lambda x: x.get('order', 0))
            if not save_case_sections(sections):
                return jsonify({'success': False, 'error': 'РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ СЂР°Р·РґРµР»РѕРІ'})
            return jsonify({'success': True, 'section': new_section, 'sections': sections})

        if request.method == 'DELETE':
            section_id = normalize_section_id(payload.get('id'))
            if section_id in ('', 'other'):
                return jsonify({'success': False, 'error': 'Р­С‚РѕС‚ СЂР°Р·РґРµР» РЅРµР»СЊР·СЏ СѓРґР°Р»РёС‚СЊ'})

            sections = load_case_sections()
            sections = [s for s in sections if s.get('id') != section_id]

            for idx, s in enumerate(sections):
                s['order'] = idx + 1

            if not save_case_sections(sections):
                return jsonify({'success': False, 'error': 'РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ СЂР°Р·РґРµР»РѕРІ'})

            cases = load_cases()
            changed = False
            for case_obj in cases:
                if normalize_section_id(case_obj.get('section', 'other')) == section_id:
                    case_obj['section'] = 'other'
                    changed = True
            if changed:
                save_cases(cases)

            return jsonify({'success': True, 'sections': sections})

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СѓРїСЂР°РІР»РµРЅРёСЏ СЂР°Р·РґРµР»Р°РјРё РєРµР№СЃРѕРІ: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/shop/purchase', methods=['POST'])
def purchase_shop_deal():
    """Purchase a shop deal"""
    try:
        data = request.get_json()
        user_id = data.get('user_id', 0)
        deal_id = data.get('deal_id', 0)

        if not user_id or not deal_id:
            return jsonify({'success': False, 'error': 'РќРµРІРµСЂРЅС‹Рµ РґР°РЅРЅС‹Рµ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if already purchased
        cursor.execute('SELECT id FROM shop_purchases WHERE user_id = ? AND deal_id = ?', (user_id, deal_id))
        if cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'error': 'РЈР¶Рµ РєСѓРїР»РµРЅРѕ'})

        # Get deal
        cursor.execute('SELECT id, title, items, price, currency, ends_at FROM shop_deals WHERE id = ? AND is_active = 1', (deal_id,))
        deal = cursor.fetchone()
        if not deal:
            conn.close()
            return jsonify({'success': False, 'error': 'РђРєС†РёСЏ РЅРµ РЅР°Р№РґРµРЅР°'})

        did, title, items_json, price, currency, ends_at = deal
        items = json.loads(items_json) if items_json else []

        # Check expiration
        if ends_at:
            try:
                end_dt = datetime.strptime(ends_at, '%Y-%m-%d %H:%M:%S')
                if datetime.utcnow() > end_dt:
                    conn.close()
                    return jsonify({'success': False, 'error': 'РђРєС†РёСЏ РёСЃС‚РµРєР»Р°'})
            except: pass

        # Calculate discount for owned items
        cursor.execute('SELECT item_type, item_id FROM user_customizations WHERE user_id = ?', (user_id,))
        owned = {}
        for r in cursor.fetchall():
            owned.setdefault(r[0], set()).add(r[1])
        cursor.execute('SELECT is_crash_vip FROM users WHERE id = ?', (user_id,))
        vr = cursor.fetchone()
        is_vip = bool(vr and vr[0])

        owned_count = 0
        for it in items:
            itype = it.get('type', '')
            iid = it.get('item_id', '')
            if itype == 'rocket' and iid in owned.get('rocket', set()): owned_count += 1
            elif itype == 'background' and iid in owned.get('background', set()): owned_count += 1
            elif itype == 'vip' and is_vip: owned_count += 1

        total_items = len(items)
        discount = int(price * owned_count / total_items) if total_items > 0 and owned_count > 0 else 0
        final_price = max(0, price - discount)

        # Check balance
        balance_field = 'balance_stars' if currency != 'tickets' else 'balance_tickets'
        cursor.execute(f'SELECT {balance_field} FROM users WHERE id = ?', (user_id,))
        bal = cursor.fetchone()
        if not bal or (bal[0] or 0) < final_price:
            conn.close()
            return jsonify({'success': False, 'error': f'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ {"Р·РІС‘Р·Рґ" if currency != "tickets" else "Р±РёР»РµС‚РѕРІ"} (РЅСѓР¶РЅРѕ {final_price})'})

        # Deduct balance
        cursor.execute(f'UPDATE users SET {balance_field} = {balance_field} - ? WHERE id = ?', (final_price, user_id))

        # Grant items
        granted = []
        for it in items:
            itype = it.get('type', '')
            iid = it.get('item_id', '')
            iname = it.get('name', '')
            if itype in ('rocket', 'background') and iid:
                if iid not in owned.get(itype, set()):
                    cursor.execute('INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source) VALUES (?, ?, ?, ?)',
                                 (user_id, itype, iid, 'shop'))
                    granted.append(iname or iid)
            elif itype == 'vip' and not is_vip:
                cursor.execute('UPDATE users SET is_crash_vip = 1 WHERE id = ?', (user_id,))
                granted.append('VIP')
            elif itype == 'stars':
                amt = int(it.get('amount', 0))
                if amt > 0:
                    cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (amt, user_id))
                    granted.append(f'+{amt} Р·РІС‘Р·Рґ')
            elif itype == 'tickets':
                amt = int(it.get('amount', 0))
                if amt > 0:
                    cursor.execute('UPDATE users SET balance_tickets = balance_tickets + ? WHERE id = ?', (amt, user_id))
                    granted.append(f'+{amt} Р±РёР»РµС‚РѕРІ')
            elif itype == 'crate':
                crate_id_val = it.get('crate_id') or it.get('item_id', '')
                crate_qty = max(1, int(it.get('quantity', 1) or 1))
                if crate_id_val:
                    try:
                        cid = int(crate_id_val)
                        cursor.execute('SELECT name, image FROM crates WHERE id = ?', (cid,))
                        crate_row = cursor.fetchone()
                        crate_name = crate_row[0] if crate_row else f'РЇС‰РёРє #{cid}'
                        crate_image = crate_row[1] if crate_row and crate_row[1] else '/static/img/star2.png'
                        for _ in range(crate_qty):
                            cursor.execute('''INSERT INTO inventory (user_id, gift_id, gift_name, gift_image, gift_value, is_withdrawing, crate_id, crate_name, crate_image)
                                VALUES (?, 0, ?, ?, 0, 0, ?, ?, ?)''',
                                (user_id, crate_name, crate_image, cid, crate_name, crate_image))
                        if crate_qty > 1:
                            granted.append(f'РЇС‰РёРє: {crate_name} x{crate_qty}')
                        else:
                            granted.append(f'РЇС‰РёРє: {crate_name}')
                    except Exception as ce:
                        logger.error(f"Crate to inventory in shop error: {ce}")

        # Record purchase
        cursor.execute('INSERT INTO shop_purchases (user_id, deal_id, price_paid, currency) VALUES (?, ?, ?, ?)',
                      (user_id, deal_id, final_price, currency))

        # Get updated balances
        cursor.execute('SELECT balance_stars, balance_tickets FROM users WHERE id = ?', (user_id,))
        newbal = cursor.fetchone()
        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'РџРѕРєСѓРїРєР° СѓСЃРїРµС€РЅР°!',
            'granted': granted,
            'new_balance_stars': newbal[0] if newbal else 0,
            'new_balance_tickets': newbal[1] if newbal else 0
        })
    except Exception as e:
        logger.error(f"Shop purchase error: {e}")
        try: conn.close()
        except: pass
        return jsonify({'success': False, 'error': 'РћС€РёР±РєР° РїРѕРєСѓРїРєРё'})
        return jsonify({'success': False, 'error': str(e)})


# ============================================================
# ADMIN SHOP DEALS MANAGEMENT
# ============================================================

@app.route('/api/admin/shop-deals', methods=['GET', 'POST'])
def admin_shop_deals():
    """Admin CRUD for shop deals"""
    if request.method == 'GET':
        try:
            admin_id = request.args.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS shop_deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, description TEXT,
                section TEXT DEFAULT 'general', items TEXT NOT NULL DEFAULT '[]',
                price INTEGER NOT NULL DEFAULT 100, old_price INTEGER DEFAULT 0,
                currency TEXT DEFAULT 'stars', duration_hours INTEGER DEFAULT 0,
                starts_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, ends_at TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE, sort_order INTEGER DEFAULT 0, icon TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            conn.commit()

            cursor.execute('''SELECT id, title, description, section, items, price, old_price,
                              currency, duration_hours, starts_at, ends_at, is_active, sort_order, icon
                              FROM shop_deals ORDER BY sort_order ASC, id DESC''')
            deals = cursor.fetchall()
            conn.close()

            return jsonify({
                'success': True,
                'deals': [{'id': d[0], 'title': d[1], 'description': d[2], 'section': d[3],
                           'items': json.loads(d[4]) if d[4] else [], 'price': d[5],
                           'old_price': d[6], 'currency': d[7], 'duration_hours': d[8],
                           'starts_at': d[9], 'ends_at': d[10], 'is_active': bool(d[11]),
                           'sort_order': d[12], 'icon': d[13]} for d in deals]
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})

    else:  # POST вЂ” create
        try:
            data = request.get_json()
            admin_id = data.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

            title = data.get('title', 'РђРєС†РёСЏ')
            description = data.get('description', '')
            section = data.get('section', 'general')
            items = json.dumps(data.get('items', []), ensure_ascii=False)
            price = int(data.get('price', 100))
            old_price = int(data.get('old_price', 0))
            currency = data.get('currency', 'stars')
            duration_hours = int(data.get('duration_hours', 0))
            sort_order = int(data.get('sort_order', 0))
            icon = data.get('icon', '')

            ends_at = None
            if duration_hours > 0:
                ends_at = (datetime.utcnow() + timedelta(hours=duration_hours)).strftime('%Y-%m-%d %H:%M:%S')

            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO shop_deals (title, description, section, items, price, old_price,
                              currency, duration_hours, starts_at, ends_at, is_active, sort_order, icon)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, 1, ?, ?)''',
                          (title, description, section, items, price, old_price, currency,
                           duration_hours, ends_at, sort_order, icon))
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'message': 'РђРєС†РёСЏ СЃРѕР·РґР°РЅР°'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/shop-deals/toggle', methods=['POST'])
def admin_toggle_shop_deal():
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE shop_deals SET is_active = ? WHERE id = ?', (1 if data.get('is_active') else 0, data['deal_id']))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/shop-deals/delete', methods=['POST'])
def admin_delete_shop_deal():
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM shop_deals WHERE id = ?', (data['deal_id'],))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ============================================================
# CRATES SYSTEM
# ============================================================

def init_crates_tables(cursor=None):
    """Initialize crates tables"""
    own_conn = False
    if cursor is None:
        conn = get_db_connection()
        cursor = conn.cursor()
        own_conn = True
    cursor.execute('''CREATE TABLE IF NOT EXISTS crates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT DEFAULT '',
        image TEXT DEFAULT '',
        price INTEGER DEFAULT 0,
        currency TEXT DEFAULT 'stars',
        is_active BOOLEAN DEFAULT 0,
        is_promoted BOOLEAN DEFAULT 0,
        promo_deal_id INTEGER DEFAULT NULL,
        sort_order INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS crate_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        crate_id INTEGER NOT NULL,
        item_type TEXT NOT NULL,
        item_id TEXT DEFAULT '',
        item_name TEXT DEFAULT '',
        chance INTEGER DEFAULT 10,
        rarity TEXT DEFAULT 'common',
        FOREIGN KEY (crate_id) REFERENCES crates(id) ON DELETE CASCADE
    )''')
    # Backwards-compatible migration: ensure new columns exist for older DBs
    try:
        cursor.execute("PRAGMA table_info('crates')")
        cols = [r[1] for r in cursor.fetchall()]
        if 'is_promoted' not in cols:
            try:
                cursor.execute("ALTER TABLE crates ADD COLUMN is_promoted BOOLEAN DEFAULT 0")
            except Exception:
                pass
        if 'promo_deal_id' not in cols:
            try:
                cursor.execute("ALTER TABLE crates ADD COLUMN promo_deal_id INTEGER DEFAULT NULL")
            except Exception:
                pass
    except Exception:
        pass
    if own_conn:
        conn.commit()
        conn.close()


def init_level_crates():
    """РЎРѕР·РґР°С‘С‚ РєСЂРµР№С‚С‹ РґР»СЏ РЅР°РіСЂР°Рґ Р·Р° СѓСЂРѕРІРЅРё РёР· LEVEL_CRATES, РµСЃР»Рё РёС… РµС‰С‘ РЅРµС‚"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        init_crates_tables(cursor)
        conn.commit()
        
        for crate_key, crate_data in LEVEL_CRATES.items():
            # РџСЂРѕРІРµСЂСЏРµРј, РµСЃС‚СЊ Р»Рё СѓР¶Рµ РєСЂРµР№С‚ СЃ С‚Р°РєРёРј РЅР°Р·РІР°РЅРёРµРј
            cursor.execute("SELECT id FROM crates WHERE name = ?", (crate_data['name'],))
            existing = cursor.fetchone()
            if existing:
                continue
            
            # РЎРѕР·РґР°С‘Рј РєСЂРµР№С‚
            cursor.execute('''
                INSERT INTO crates (name, description, image, price, currency, is_active, sort_order)
                VALUES (?, ?, ?, 0, 'free', 1, ?)
            ''', (crate_data['name'], f'Level reward crate: {crate_key}', crate_data['image'], 0))
            crate_id = cursor.lastrowid
            
            # Р”РѕР±Р°РІР»СЏРµРј РїСЂРµРґРјРµС‚С‹ РєСЂРµР№С‚Р°
            for item_type, item_id, item_name, chance, rarity in crate_data['items']:
                cursor.execute('''
                    INSERT INTO crate_items (crate_id, item_type, item_id, item_name, chance, rarity)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (crate_id, item_type, item_id, item_name, chance, rarity))
            
            logger.info(f"рџ“¦ РЎРѕР·РґР°РЅ РєСЂРµР№С‚ СѓСЂРѕРІРЅСЏ: {crate_data['name']} (id={crate_id})")
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃРѕР·РґР°РЅРёСЏ РєСЂРµР№С‚РѕРІ СѓСЂРѕРІРЅРµР№: {e}")


def _get_level_crate_id(crate_key):
    """РџРѕР»СѓС‡Р°РµС‚ ID РєСЂРµР№С‚Р° РїРѕ РєР»СЋС‡Сѓ РёР· LEVEL_CRATES"""
    crate_data = LEVEL_CRATES.get(crate_key)
    if not crate_data:
        return None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM crates WHERE name = ?", (crate_data['name'],))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
    except:
        return None


# Initialize on import
try:
    init_crates_tables()
    init_level_crates()
except:
    pass


@app.route('/api/admin/crates', methods=['GET', 'POST'])
def admin_crates():
    try:
        if request.method == 'GET':
            admin_id = request.args.get('admin_id', 0, type=int)
            if admin_id != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
            
            conn = get_db_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            init_crates_tables(cursor)
            conn.commit()
            
            cursor.execute('SELECT * FROM crates ORDER BY sort_order ASC, id DESC')
            crates = []
            for row in cursor.fetchall():
                crate = dict(row)
                cursor.execute('SELECT * FROM crate_items WHERE crate_id = ?', (crate['id'],))
                crate['items'] = [dict(r) for r in cursor.fetchall()]
                crates.append(crate)
            
            conn.close()
            return jsonify({'success': True, 'crates': crates})
        
        else:  # POST - create crate
            data = request.get_json(force=True, silent=True)
            if not data:
                return jsonify({'success': False, 'error': 'РќРµРІРµСЂРЅС‹Рµ РґР°РЅРЅС‹Рµ Р·Р°РїСЂРѕСЃР°'})
            admin_id = data.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
            
            name = data.get('name', '').strip()
            if not name:
                return jsonify({'success': False, 'error': 'Р’РІРµРґРёС‚Рµ РЅР°Р·РІР°РЅРёРµ'})
            
            items = data.get('items', [])
            if not items:
                return jsonify({'success': False, 'error': 'Р”РѕР±Р°РІСЊС‚Рµ РїСЂРµРґРјРµС‚С‹'})
            
            conn = get_db_connection()
            cursor = conn.cursor()
            init_crates_tables(cursor)
            conn.commit()
            
            cursor.execute('''INSERT INTO crates (name, description, image, price, currency, is_active)
                VALUES (?, ?, ?, ?, ?, 0)''',
                (name, data.get('description', ''), data.get('image', ''),
                 int(data.get('price', 0)), data.get('currency', 'stars')))
            
            crate_id = cursor.lastrowid
            
            for item in items:
                item_type = item.get('item_type', 'rocket')
                item_id = item.get('item_id', '')
                chance = int(item.get('chance', 10))
                # Auto-assign rarity based on chance
                if chance <= 5:
                    rarity = 'legendary'
                elif chance <= 15:
                    rarity = 'epic'
                elif chance <= 30:
                    rarity = 'rare'
                else:
                    rarity = 'common'
                
                cursor.execute('''INSERT INTO crate_items (crate_id, item_type, item_id, item_name, chance, rarity)
                    VALUES (?, ?, ?, ?, ?, ?)''',
                    (crate_id, item_type, item_id, item.get('item_name', item_id), chance, rarity))
            
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'crate_id': crate_id})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/crates/toggle', methods=['POST'])
def admin_toggle_crate():
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE crates SET is_active = ? WHERE id = ?',
            (1 if data.get('is_active') else 0, data['crate_id']))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/crates/promote', methods=['POST'])
def admin_promote_crate():
    """Promote/unpromote a crate as a shop deal (admin)"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        crate_id = int(data.get('crate_id', 0))
        promote = bool(data.get('promote'))

        conn = get_db_connection()
        cursor = conn.cursor()
        init_crates_tables(cursor)
        conn.commit()

        cursor.execute('SELECT id, name, description, image, price, currency, is_promoted, promo_deal_id FROM crates WHERE id = ?', (crate_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return jsonify({'success': False, 'error': 'РЇС‰РёРє РЅРµ РЅР°Р№РґРµРЅ'})

        _, name, desc, image, price, currency, is_promoted, promo_deal_id = row

        # Ensure shop_deals table exists
        cursor.execute('''CREATE TABLE IF NOT EXISTS shop_deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, description TEXT,
            section TEXT DEFAULT 'general', items TEXT NOT NULL DEFAULT '[]',
            price INTEGER NOT NULL DEFAULT 100, old_price INTEGER DEFAULT 0,
            currency TEXT DEFAULT 'stars', duration_hours INTEGER DEFAULT 0,
            starts_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, ends_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE, sort_order INTEGER DEFAULT 0, icon TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        if promote:
            # Create shop deal if not exists
            if promo_deal_id:
                # Reactivate existing deal
                cursor.execute('UPDATE shop_deals SET is_active = 1 WHERE id = ?', (promo_deal_id,))
            else:
                items = json.dumps([{'type': 'crate', 'crate_id': crate_id, 'name': name}], ensure_ascii=False)
                cursor.execute('''INSERT INTO shop_deals (title, description, section, items, price, old_price, currency, duration_hours, starts_at, ends_at, is_active, sort_order, icon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP, NULL, 1, 0, ?)''',
                    (f'РљРµР№СЃ: {name}', desc or f'РљРµР№СЃ {name}', 'cases', items, int(price or 0), 0, currency or 'stars', image or ''))
                promo_deal_id = cursor.lastrowid

            cursor.execute('UPDATE crates SET is_promoted = 1, promo_deal_id = ? WHERE id = ?', (promo_deal_id, crate_id))
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'promo_deal_id': promo_deal_id})
        else:
            # Unpromote: deactivate linked shop deal if exists
            if promo_deal_id:
                cursor.execute('UPDATE shop_deals SET is_active = 0 WHERE id = ?', (promo_deal_id,))
            cursor.execute('UPDATE crates SET is_promoted = 0, promo_deal_id = NULL WHERE id = ?', (crate_id,))
            conn.commit()
            conn.close()
            return jsonify({'success': True})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/crates/delete', methods=['POST'])
def admin_delete_crate():
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM crate_items WHERE crate_id = ?', (data['crate_id'],))
        cursor.execute('DELETE FROM crates WHERE id = ?', (data['crate_id'],))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/crates/upload-image', methods=['POST'])
def admin_upload_crate_image():
    """Upload image for a crate"""
    try:
        admin_id = request.form.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        file = request.files.get('file')
        if not file:
            return jsonify({'success': False, 'error': 'Р¤Р°Р№Р» РЅРµ РІС‹Р±СЂР°РЅ'})
        
        import uuid
        ext = os.path.splitext(file.filename)[1] or '.png'
        fname = f"crate_{uuid.uuid4().hex[:8]}{ext}"
        save_dir = os.path.join(BASE_PATH, 'static', 'gifs', 'cases')
        os.makedirs(save_dir, exist_ok=True)
        filepath = os.path.join(save_dir, fname)
        file.save(filepath)
        
        url = f'/static/gifs/cases/{fname}'
        return jsonify({'success': True, 'url': url})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/crates', methods=['GET'])
def get_crates():
    """Get active crates for users"""
    try:
        user_id = request.args.get('user_id', 0, type=int)
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        init_crates_tables(cursor)
        conn.commit()
        
        cursor.execute('SELECT * FROM crates WHERE is_active = 1 ORDER BY sort_order ASC, id DESC')
        crates = []
        for row in cursor.fetchall():
            crate = dict(row)
            cursor.execute('SELECT item_type, item_id, item_name, chance, rarity FROM crate_items WHERE crate_id = ?', (crate['id'],))
            crate['items'] = [dict(r) for r in cursor.fetchall()]
            crates.append(crate)
        
        conn.close()
        return jsonify({'success': True, 'crates': crates})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/crates/open', methods=['POST'])
def open_crate():
    """Open a crate - deduct price and give random item"""
    try:
        data = request.get_json()
        user_id = data.get('user_id', 0)
        crate_id = data.get('crate_id', 0)
        
        if not user_id or not crate_id:
            return jsonify({'success': False, 'error': 'Missing data'})
        
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        init_crates_tables(cursor)
        conn.commit()
        
        # Get crate
        cursor.execute('SELECT * FROM crates WHERE id = ? AND is_active = 1', (crate_id,))
        crate = cursor.fetchone()
        if not crate:
            conn.close()
            return jsonify({'success': False, 'error': 'РЇС‰РёРє РЅРµ РЅР°Р№РґРµРЅ'})
        
        crate = dict(crate)
        
        # Get user
        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()
        if not user:
            conn.close()
            return jsonify({'success': False, 'error': 'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ'})
        
        user = dict(user)
        
        # Check balance
        price = crate['price']
        currency = crate['currency']
        if price > 0:
            if currency == 'stars':
                if user.get('balance_stars', 0) < price:
                    conn.close()
                    return jsonify({'success': False, 'error': 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ Р·РІС‘Р·Рґ'})
            elif currency == 'tickets':
                if user.get('balance_tickets', 0) < price:
                    conn.close()
                    return jsonify({'success': False, 'error': 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ Р±РёР»РµС‚РѕРІ'})
        
        # Get items (row_factory already set above)
        cursor.execute('SELECT * FROM crate_items WHERE crate_id = ?', (crate_id,))
        items = [dict(r) for r in cursor.fetchall()]
        
        if not items:
            conn.close()
            return jsonify({'success': False, 'error': 'РЇС‰РёРє РїСѓСЃС‚'})
        
        # Weighted random selection
        import random
        total_chance = sum(it['chance'] for it in items)
        roll = random.randint(1, total_chance)
        cumulative = 0
        won_item = items[0]
        for it in items:
            cumulative += it['chance']
            if roll <= cumulative:
                won_item = it
                break
        
        # Deduct price
        if price > 0:
            if currency == 'stars':
                cursor.execute('UPDATE users SET balance_stars = balance_stars - ? WHERE id = ?', (price, user_id))
            elif currency == 'tickets':
                cursor.execute('UPDATE users SET balance_tickets = balance_tickets - ? WHERE id = ?', (price, user_id))
        
        # Give reward
        reward_desc = ''
        comp_desc = ''
        # Dynamic compensation: lower chance = higher stars (min 10, max 100)
        item_chance_pct = (won_item['chance'] / total_chance * 100) if total_chance > 0 else 50
        comp_stars = max(10, min(100, round(100 - item_chance_pct)))
        if won_item['item_type'] == 'stars':
            amount = int(won_item['item_id']) if won_item['item_id'].isdigit() else 10
            cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (amount, user_id))
            reward_desc = '+' + str(amount) + ' Stars'
        elif won_item['item_type'] == 'tickets':
            amount = int(won_item['item_id']) if won_item['item_id'].isdigit() else 1
            cursor.execute('UPDATE users SET balance_tickets = balance_tickets + ? WHERE id = ?', (amount, user_id))
            reward_desc = '+' + str(amount) + ' Р±РёР»РµС‚РѕРІ'
        elif won_item['item_type'] in ('rocket', 'background'):
            # Check if already owned
            cursor.execute('SELECT id FROM user_customizations WHERE user_id = ? AND item_type = ? AND item_id = ?',
                          (user_id, won_item['item_type'], won_item['item_id']))
            already_owned = cursor.fetchone()
            if already_owned:
                cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (comp_stars, user_id))
                comp_desc = '+' + str(comp_stars) + ' в­ђ (СѓР¶Рµ РµСЃС‚СЊ)'
            else:
                cursor.execute('''INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source)
                    VALUES (?, ?, ?, 'crate')''', (user_id, won_item['item_type'], won_item['item_id']))
            reward_desc = won_item['item_name'] or won_item['item_id']
        
        # Increment total_cases_opened for quest progress
        cursor.execute('UPDATE users SET total_cases_opened = total_cases_opened + 1 WHERE id = ?', (user_id,))
        
        # Get updated balances
        cursor.execute('SELECT balance_stars, balance_tickets FROM users WHERE id = ?', (user_id,))
        updated = cursor.fetchone()
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'won_item': {
                'item_type': won_item['item_type'],
                'item_id': won_item['item_id'],
                'item_name': won_item['item_name'] or won_item['item_id'],
                'rarity': won_item['rarity'],
                'chance': won_item['chance']
            },
            'reward_desc': reward_desc,
            'comp_desc': comp_desc,
            'new_balance_stars': updated['balance_stars'] if updated else 0,
            'new_balance_tickets': updated['balance_tickets'] if updated else 0,
            'all_items': [{'item_type': it['item_type'], 'item_id': it['item_id'], 
                          'item_name': it['item_name'] or it['item_id'], 'rarity': it['rarity'],
                          'chance': it['chance']} for it in items]
        })
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        try: conn.close()
        except: pass
        return jsonify({'success': False, 'error': str(e)})


# ============================================================
# CRASH QUESTS SYSTEM
# ============================================================

@app.route('/api/crash/quests', methods=['GET'])
def get_crash_quests():
    """Get quests with user progress"""
    try:
        user_id = request.args.get('user_id', 0, type=int)
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Ensure tables exist
        cursor.execute('''CREATE TABLE IF NOT EXISTS crash_quests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            quest_type TEXT NOT NULL,
            target_value INTEGER NOT NULL DEFAULT 1,
            reward_type TEXT NOT NULL DEFAULT 'stars',
            reward_amount INTEGER DEFAULT 0,
            reward_data TEXT,
            icon TEXT,
            sort_order INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            condition_value REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        # Add condition_value column if missing (migration)
        try:
            cursor.execute('ALTER TABLE crash_quests ADD COLUMN condition_value REAL DEFAULT 0')
            conn.commit()
        except:
            pass
        # Add parent_id column if missing (migration for parent/child quests)
        try:
            cursor.execute("ALTER TABLE crash_quests ADD COLUMN parent_id INTEGER DEFAULT NULL")
            conn.commit()
        except:
            pass
        cursor.execute('''CREATE TABLE IF NOT EXISTS user_quest_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            quest_id INTEGER NOT NULL,
            progress INTEGER DEFAULT 0,
            completed BOOLEAN DEFAULT FALSE,
            reward_claimed BOOLEAN DEFAULT FALSE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, quest_id)
        )''')
        conn.commit()
        
        cursor.execute('SELECT id, title, description, quest_type, target_value, reward_type, reward_amount, reward_data, icon, condition_value, parent_id, created_at FROM crash_quests WHERE is_active = 1 ORDER BY sort_order ASC, id ASC')
        quests = cursor.fetchall()
        
        # Load user's owned items for compensation checks
        owned_rockets = set()
        owned_backgrounds = set()
        is_user_vip = False
        if user_id > 0:
            cursor.execute('SELECT item_type, item_id FROM user_customizations WHERE user_id = ?', (user_id,))
            for r in cursor.fetchall():
                if r[0] == 'rocket': owned_rockets.add(r[1])
                elif r[0] == 'background': owned_backgrounds.add(r[1])
            cursor.execute('SELECT is_crash_vip FROM users WHERE id = ?', (user_id,))
            vr = cursor.fetchone()
            is_user_vip = bool(vr and vr[0])

        result = []
        for q in quests:
            qid, title, desc, qtype, target, rtype, ramount, rdata, icon, cond_val, parent_id, quest_created_at = q
            cond_val = cond_val or 0
            
            # Get user progress
            progress = 0
            completed = False
            reward_claimed = False
            
            if user_id > 0:
                cursor.execute('SELECT progress, completed, reward_claimed FROM user_quest_progress WHERE user_id = ? AND quest_id = ?', (user_id, qid))
                up = cursor.fetchone()
                
                # Always recalculate progress from user stats
                progress = _calc_quest_progress(cursor, user_id, qtype, target, cond_val, quest_created_at)
                completed = progress >= target
                
                if up:
                    reward_claimed = bool(up[2])
                    old_progress = up[0]
                    old_completed = bool(up[1])
                    # Update stored progress if changed
                    if old_progress != progress or old_completed != completed:
                        cursor.execute('UPDATE user_quest_progress SET progress = ?, completed = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND quest_id = ?',
                                       (progress, completed, user_id, qid))
                        conn.commit()
                else:
                    # First time вЂ” insert record
                    cursor.execute('INSERT OR IGNORE INTO user_quest_progress (user_id, quest_id, progress, completed) VALUES (?, ?, ?, ?)',
                                   (user_id, qid, progress, completed))
                    conn.commit()
            
            # Parse reward_data for display
            rdata_name = ''
            rewards_arr = []
            if rdata:
                try:
                    rd = json.loads(rdata)
                    if isinstance(rd, list):
                        # Multi-reward array
                        rewards_arr = rd
                    elif isinstance(rd, dict):
                        rdata_name = rd.get('name', '')
                except:
                    pass
            
            # For single rocket/background/crate rewards, extract item_id from reward_data
            reward_data_id = ''
            reward_image = ''
            if rdata and rtype in ('rocket', 'background'):
                try:
                    rd = json.loads(rdata)
                    if isinstance(rd, dict):
                        reward_data_id = rd.get('item_id', '')
                        if not rdata_name:
                            rdata_name = rd.get('name', '')
                except:
                    reward_data_id = rdata  # fallback: raw string might be the id
            elif rdata and rtype == 'crate':
                try:
                    rd = json.loads(rdata)
                    if isinstance(rd, dict):
                        reward_data_id = str(rd.get('crate_id', ''))
                        if not rdata_name:
                            rdata_name = rd.get('name', '')
                        # Try to get crate image from DB
                        try:
                            cursor.execute('SELECT image, name FROM crates WHERE id = ?', (int(reward_data_id),))
                            crate_row = cursor.fetchone()
                            if crate_row:
                                reward_image = crate_row[0] or ''
                                if not rdata_name:
                                    rdata_name = crate_row[1] or ''
                        except:
                            pass
                except:
                    reward_data_id = rdata

            # Check compensation: if user already owns the reward item, replace with stars
            compensation = None
            if user_id > 0 and not reward_claimed:
                if rtype == 'background' and reward_data_id:
                    if reward_data_id in owned_backgrounds:
                        compensation = {'type': 'stars', 'amount': 50, 'reason': 'Р¤РѕРЅ СѓР¶Рµ РµСЃС‚СЊ'}
                elif rtype == 'rocket' and reward_data_id:
                    if reward_data_id in owned_rockets:
                        compensation = {'type': 'stars', 'amount': 50, 'reason': 'Р Р°РєРµС‚Р° СѓР¶Рµ РµСЃС‚СЊ'}
                elif rtype == 'vip' and is_user_vip:
                    compensation = {'type': 'stars', 'amount': 200, 'reason': 'VIP СѓР¶Рµ РµСЃС‚СЊ'}
                elif rtype == 'multi' and rewards_arr:
                    comp_list = []
                    for rew in rewards_arr:
                        rt = rew.get('type', '')
                        rid = rew.get('item_id', '')
                        if rt == 'background' and rid and rid in owned_backgrounds:
                            comp_list.append({'type': 'stars', 'amount': 50, 'for': rew.get('name', 'Р¤РѕРЅ')})
                        elif rt == 'rocket' and rid and rid in owned_rockets:
                            comp_list.append({'type': 'stars', 'amount': 50, 'for': rew.get('name', 'Р Р°РєРµС‚Р°')})
                        elif rt == 'vip' and is_user_vip:
                            comp_list.append({'type': 'stars', 'amount': 200, 'for': 'VIP'})
                    if comp_list:
                        compensation = comp_list

            # Locked logic: if this quest has a parent and the parent isn't completed+claimed for this user, mark locked
            locked = False
            if parent_id:
                if user_id <= 0:
                    locked = True
                else:
                    cursor.execute('SELECT completed, reward_claimed FROM user_quest_progress WHERE user_id = ? AND quest_id = ?', (user_id, parent_id))
                    prow = cursor.fetchone()
                    if not prow or not prow[0] or not prow[1]:
                        locked = True

            # Determine icon type for proper frontend rendering
            icon_src = icon or '/static/img/star2.png'
            icon_is_video = icon_src.endswith('.mp4')

            result.append({
                'id': qid,
                'title': title,
                'description': desc or '',
                'quest_type': qtype,
                'target': target,
                'progress': min(progress, target),
                'reward_type': rtype,
                'reward_amount': ramount,
                'reward_data': reward_data_id,
                'reward_data_name': rdata_name,
                'reward_image': reward_image,
                'rewards': rewards_arr,
                'reward_claimed': reward_claimed,
                'icon': icon_src,
                'icon_is_video': icon_is_video,
                'compensation': compensation,
                'parent_id': parent_id,
                'locked': locked
            })
        
        conn.close()
        return jsonify({'success': True, 'quests': result})
    except Exception as e:
        logger.error(f"Error loading quests: {e}")
        return jsonify({'success': True, 'quests': []})


def _calc_quest_progress(cursor, user_id, quest_type, target, condition_value=0, quest_created_at=None):
    """Calculate quest progress from user stats.
    quest_created_at: if provided, only count activity after this date.
    Exceptions: buy_vip and collection always use all-time data.
    """
    try:
        if quest_type == 'turnover':
            if quest_created_at:
                cursor.execute('SELECT COALESCE(SUM(bet_amount), 0) FROM ultimate_crash_bets WHERE user_id = ? AND created_at >= ?', (user_id, quest_created_at))
            else:
                cursor.execute('SELECT total_bet_volume FROM users WHERE id = ?', (user_id,))
            r = cursor.fetchone()
            return r[0] or 0 if r else 0
        elif quest_type == 'crash_bets':
            if quest_created_at:
                cursor.execute('SELECT COUNT(*) FROM ultimate_crash_bets WHERE user_id = ? AND created_at >= ?', (user_id, quest_created_at))
            else:
                cursor.execute('SELECT total_crash_bets FROM users WHERE id = ?', (user_id,))
            r = cursor.fetchone()
            return r[0] or 0 if r else 0
        elif quest_type == 'skins_collected':
            if quest_created_at:
                cursor.execute('SELECT COUNT(*) FROM user_customizations WHERE user_id = ? AND created_at >= ?', (user_id, quest_created_at))
            else:
                cursor.execute('SELECT COUNT(*) FROM user_customizations WHERE user_id = ?', (user_id,))
            r = cursor.fetchone()
            return r[0] or 0 if r else 0
        elif quest_type == 'buy_vip':
            # Exception: always all-time
            cursor.execute('SELECT is_crash_vip FROM users WHERE id = ?', (user_id,))
            r = cursor.fetchone()
            return 1 if r and r[0] else 0
        elif quest_type == 'level':
            cursor.execute('SELECT current_level FROM users WHERE id = ?', (user_id,))
            r = cursor.fetchone()
            return r[0] or 1 if r else 1
        elif quest_type == 'login_streak':
            return 0  # Needs dedicated tracking
        elif quest_type == 'open_cases':
            # Exception: always all-time (absolute count)
            cursor.execute('SELECT total_cases_opened FROM users WHERE id = ?', (user_id,))
            r = cursor.fetchone()
            return r[0] or 0 if r else 0
        elif quest_type == 'referrals':
            cursor.execute('SELECT referral_count FROM users WHERE id = ?', (user_id,))
            r = cursor.fetchone()
            return r[0] or 0 if r else 0
        elif quest_type == 'collection':
            # Exception: always all-time
            cursor.execute('''
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT gift_name FROM case_open_history WHERE user_id = ? AND gift_name IS NOT NULL
                    UNION
                    SELECT DISTINCT gift_name FROM win_history WHERE user_id = ? AND gift_name IS NOT NULL
                    UNION
                    SELECT DISTINCT gift_name FROM inventory WHERE user_id = ?
                )
            ''', (user_id, user_id, user_id))
            r = cursor.fetchone()
            return r[0] or 0 if r else 0
        elif quest_type == 'cashout_x':
            min_mult = condition_value if condition_value > 0 else 2.0
            if quest_created_at:
                cursor.execute('''
                    SELECT COUNT(*) FROM ultimate_crash_bets
                    WHERE user_id = ? AND status = 'cashed_out' AND cashout_multiplier >= ? AND created_at >= ?
                ''', (user_id, min_mult, quest_created_at))
            else:
                cursor.execute('''
                    SELECT COUNT(*) FROM ultimate_crash_bets
                    WHERE user_id = ? AND status = 'cashed_out' AND cashout_multiplier >= ?
                ''', (user_id, min_mult))
            r = cursor.fetchone()
            return r[0] or 0 if r else 0
        elif quest_type == 'total_wins':
            if quest_created_at:
                cursor.execute('''
                    SELECT COUNT(*) FROM ultimate_crash_bets
                    WHERE user_id = ? AND status = 'cashed_out' AND created_at >= ?
                ''', (user_id, quest_created_at))
            else:
                cursor.execute('''
                    SELECT COUNT(*) FROM ultimate_crash_bets
                    WHERE user_id = ? AND status = 'cashed_out'
                ''', (user_id,))
            r = cursor.fetchone()
            return r[0] or 0 if r else 0
        elif quest_type == 'win_amount':
            if quest_created_at:
                cursor.execute('''
                    SELECT COALESCE(SUM(win_amount), 0) FROM ultimate_crash_bets
                    WHERE user_id = ? AND status = 'cashed_out' AND created_at >= ?
                ''', (user_id, quest_created_at))
            else:
                cursor.execute('''
                    SELECT COALESCE(SUM(win_amount), 0) FROM ultimate_crash_bets
                    WHERE user_id = ? AND status = 'cashed_out'
                ''', (user_id,))
            r = cursor.fetchone()
            return r[0] or 0 if r else 0
        elif quest_type == 'max_multiplier':
            if quest_created_at:
                cursor.execute('''
                    SELECT COALESCE(MAX(cashout_multiplier), 0) FROM ultimate_crash_bets
                    WHERE user_id = ? AND status = 'cashed_out' AND created_at >= ?
                ''', (user_id, quest_created_at))
            else:
                cursor.execute('''
                    SELECT COALESCE(MAX(cashout_multiplier), 0) FROM ultimate_crash_bets
                    WHERE user_id = ? AND status = 'cashed_out'
                ''', (user_id,))
            r = cursor.fetchone()
            return int((r[0] or 0) * 100) if r else 0  # Store as x100 for integer comparison
    except:
        pass
    return 0


@app.route('/api/crash/quests/claim', methods=['POST'])
def claim_crash_quest():
    """Claim quest reward"""
    try:
        data = request.get_json()
        user_id = data.get('user_id', 0)
        quest_id = data.get('quest_id', 0)
        
        if not user_id or not quest_id:
            return jsonify({'success': False, 'error': 'РќРµРІРµСЂРЅС‹Рµ РґР°РЅРЅС‹Рµ'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get quest info
        cursor.execute('SELECT id, target_value, reward_type, reward_amount, reward_data, title, quest_type, condition_value, created_at FROM crash_quests WHERE id = ? AND is_active = 1', (quest_id,))
        quest = cursor.fetchone()
        if not quest:
            conn.close()
            return jsonify({'success': False, 'error': 'РљРІРµСЃС‚ РЅРµ РЅР°Р№РґРµРЅ'})
        
        qid, target, rtype, ramount, rdata, title, qtype, cond_val, quest_created_at = quest
        
        # Check progress
        cursor.execute('SELECT progress, reward_claimed FROM user_quest_progress WHERE user_id = ? AND quest_id = ?', (user_id, qid))
        up = cursor.fetchone()
        
        if up and up[1]:
            conn.close()
            return jsonify({'success': False, 'error': 'РќР°РіСЂР°РґР° СѓР¶Рµ РїРѕР»СѓС‡РµРЅР°'})
        
        progress = up[0] if up else _calc_quest_progress(cursor, user_id, qtype, target, cond_val or 0, quest_created_at)
        
        if progress < target:
            conn.close()
            return jsonify({'success': False, 'error': 'РљРІРµСЃС‚ РЅРµ РІС‹РїРѕР»РЅРµРЅ'})
        
        # Claim the reward (with compensation for already-owned items)
        msg = ''
        new_balance_stars = None
        new_balance_tickets = None
        
        # Load user's owned items for compensation check
        cursor.execute('SELECT item_type, item_id FROM user_customizations WHERE user_id = ?', (user_id,))
        owned_items_rows = cursor.fetchall()
        owned_rockets_set = set()
        owned_backgrounds_set = set()
        for oi_row in owned_items_rows:
            if oi_row[0] == 'rocket':
                owned_rockets_set.add(oi_row[1])
            elif oi_row[0] == 'background':
                owned_backgrounds_set.add(oi_row[1])
        cursor.execute('SELECT is_crash_vip FROM users WHERE id = ?', (user_id,))
        _vip_row = cursor.fetchone()
        user_has_vip = bool(_vip_row and _vip_row[0])
        
        # Compensation constants are defined centrally in grant_reward_with_comp
        
        # Replace nested grant implementation with centralized helper
        
        if rtype == 'multi':
            rewards_list = []
            if rdata:
                try: rewards_list = json.loads(rdata)
                except: pass
            msgs = []
            for rew in rewards_list:
                rt = rew.get('type', '')
                amt = int(rew.get('amount', 0))
                iid = rew.get('item_id', '') or rew.get('crate_id', '')
                if rt == 'crate':
                    crate_qty = max(1, int(rew.get('quantity', 1) or 1))
                    msgs.extend(grant_reward_with_comp(cursor, user_id, rt, crate_qty, str(iid), owned_rockets_set, owned_backgrounds_set, user_has_vip))
                else:
                    msgs.extend(grant_reward_with_comp(cursor, user_id, rt, amt, str(iid), owned_rockets_set, owned_backgrounds_set, user_has_vip))
            cursor.execute('SELECT balance_stars, balance_tickets FROM users WHERE id = ?', (user_id,))
            bal = cursor.fetchone()
            if bal:
                new_balance_stars = bal[0]
                new_balance_tickets = bal[1]
            msg = ', '.join(msgs) if msgs else 'РќР°РіСЂР°РґС‹ РїРѕР»СѓС‡РµРЅС‹!'
        elif rtype == 'stars':
            cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (ramount, user_id))
            cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
            new_balance_stars = cursor.fetchone()[0]
            msg = f'+{ramount} Р·РІРµР·Рґ!'
        elif rtype == 'tickets':
            cursor.execute('UPDATE users SET balance_tickets = balance_tickets + ? WHERE id = ?', (ramount, user_id))
            cursor.execute('SELECT balance_tickets FROM users WHERE id = ?', (user_id,))
            new_balance_tickets = cursor.fetchone()[0]
            msg = f'+{ramount} Р±РёР»РµС‚РѕРІ!'
        elif rtype == 'vip':
            msgs = grant_reward_with_comp(cursor, user_id, 'vip', 0, '', owned_rockets_set, owned_backgrounds_set, user_has_vip)
            cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
            new_balance_stars = cursor.fetchone()[0]
            msg = msgs[0] if msgs else 'VIP Р°РєС‚РёРІРёСЂРѕРІР°РЅ!'
        elif rtype in ('rocket', 'background'):
            rd = {}
            if rdata:
                try: rd = json.loads(rdata)
                except: pass
            item_id = rd.get('item_id', '')
            msgs = grant_reward_with_comp(cursor, user_id, rtype, 0, item_id, owned_rockets_set, owned_backgrounds_set, user_has_vip)
            cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (user_id,))
            new_balance_stars = cursor.fetchone()[0]
            msg = msgs[0] if msgs else 'РќР°РіСЂР°РґР° РїРѕР»СѓС‡РµРЅР°!'
        elif rtype == 'crate':
            rd = {}
            if rdata:
                try: rd = json.loads(rdata)
                except: pass
            crate_id_val = rd.get('crate_id', '')
            crate_qty = max(1, int(rd.get('quantity', 1) or 1))
            msgs = grant_reward_with_comp(cursor, user_id, 'crate', crate_qty, str(crate_id_val), owned_rockets_set, owned_backgrounds_set, user_has_vip)
            cursor.execute('SELECT balance_stars, balance_tickets FROM users WHERE id = ?', (user_id,))
            bal = cursor.fetchone()
            if bal:
                new_balance_stars = bal[0]
                new_balance_tickets = bal[1]
            msg = msgs[0] if msgs else 'РЇС‰РёРє РїРѕР»СѓС‡РµРЅ!'
        else:
            msg = 'РќР°РіСЂР°РґР° РїРѕР»СѓС‡РµРЅР°!'
        
        # Mark as claimed
        cursor.execute('''INSERT INTO user_quest_progress (user_id, quest_id, progress, completed, reward_claimed, updated_at)
            VALUES (?, ?, ?, 1, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id, quest_id) DO UPDATE SET reward_claimed = 1, completed = 1, updated_at = CURRENT_TIMESTAMP''',
            (user_id, qid, progress))
        
        conn.commit()
        
        # Check if any child quests become unlocked by completing this quest
        next_quest = None
        try:
            cursor.execute('SELECT id, title, description, icon FROM crash_quests WHERE parent_id = ? AND is_active = 1 LIMIT 1', (qid,))
            child = cursor.fetchone()
            if child:
                next_quest = {
                    'id': child[0],
                    'title': child[1],
                    'description': child[2] or '',
                    'icon': child[3] or '/static/img/star2.png'
                }
        except:
            pass
        
        conn.close()
        
        resp = {'success': True, 'message': msg}
        if new_balance_stars is not None:
            resp['new_balance_stars'] = new_balance_stars
        if new_balance_tickets is not None:
            resp['new_balance_tickets'] = new_balance_tickets
        if next_quest:
            resp['next_quest'] = next_quest
        return jsonify(resp)
    except Exception as e:
        logger.error(f"Error claiming quest: {e}")
        try: conn.close()
        except: pass
        return jsonify({'success': False, 'error': str(e)})


# ============================================================
# USER CRASH GAME HISTORY
# ============================================================

@app.route('/api/crash/user-history', methods=['GET'])
def get_crash_user_history():
    """Get user's crash game history"""
    try:
        user_id = request.args.get('user_id', 0, type=int)
        page = request.args.get('page', 0, type=int)
        limit = request.args.get('limit', 30, type=int)
        
        if not user_id:
            return jsonify({'success': True, 'history': [], 'has_more': False})
        
        offset = page * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Try ultimate_crash_bets first (main game)
        cursor.execute('''
            SELECT b.bet_amount, b.status, b.cashout_multiplier, b.win_amount, b.created_at
            FROM ultimate_crash_bets b
            WHERE b.user_id = ?
            ORDER BY b.created_at DESC
            LIMIT ? OFFSET ?
        ''', (user_id, limit + 1, offset))
        
        rows = cursor.fetchall()
        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]
        
        history = []
        # Detect server timezone offset from UTC
        # SQLite CURRENT_TIMESTAMP uses server local time
        import time as _time
        server_utc_offset = _time.timezone if _time.daylight == 0 else _time.altzone
        # server_utc_offset is negative for east of UTC (e.g., UTC+3 = -10800)
        # We need to subtract this offset to get actual UTC
        
        for r in rows:
            # Normalize created_at to ISO UTC where possible so clients can render local time
            created_raw = r[4]
            created_iso = created_raw
            if created_raw:
                try:
                    # Try ISO first
                    dt = datetime.fromisoformat(created_raw)
                    if dt.tzinfo is None:
                        # Server stored local time - convert to UTC
                        dt = dt + timedelta(seconds=server_utc_offset)
                        dt = dt.replace(tzinfo=pytz.UTC)
                    created_iso = dt.isoformat().replace('+00:00', 'Z')
                except Exception:
                    try:
                        # Fallback for SQLite 'YYYY-MM-DD HH:MM:SS'
                        dt = datetime.strptime(created_raw, '%Y-%m-%d %H:%M:%S')
                        dt = dt + timedelta(seconds=server_utc_offset)
                        dt = dt.replace(tzinfo=pytz.UTC)
                        created_iso = dt.isoformat().replace('+00:00', 'Z')
                    except Exception:
                        created_iso = created_raw

            history.append({
                'bet_amount': r[0] or 0,
                'status': r[1] or 'lost',
                'cashout_multiplier': r[2],
                'win_amount': r[3] or 0,
                'created_at': created_iso
            })
        
        conn.close()
        return jsonify({'success': True, 'history': history, 'has_more': has_more})
    except Exception as e:
        logger.error(f"Error loading user history: {e}")
        return jsonify({'success': True, 'history': [], 'has_more': False})


# ============================================================
# ADMIN CRASH QUESTS MANAGEMENT
# ============================================================

@app.route('/api/admin/crash-quests', methods=['GET', 'POST'])
def admin_crash_quests():
    """Admin CRUD for crash quests"""
    if request.method == 'GET':
        try:
            admin_id = request.args.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
            
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS crash_quests (
                id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, description TEXT,
                quest_type TEXT NOT NULL, target_value INTEGER NOT NULL DEFAULT 1,
                reward_type TEXT NOT NULL DEFAULT 'stars', reward_amount INTEGER DEFAULT 0,
                reward_data TEXT, icon TEXT, sort_order INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE, condition_value REAL DEFAULT 0,
                parent_id INTEGER DEFAULT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            # Add condition_value column if missing (migration)
            try:
                cursor.execute('ALTER TABLE crash_quests ADD COLUMN condition_value REAL DEFAULT 0')
                conn.commit()
            except:
                pass
            # Add parent_id column if missing
            try:
                cursor.execute('ALTER TABLE crash_quests ADD COLUMN parent_id INTEGER DEFAULT NULL')
                conn.commit()
            except:
                pass
            conn.commit()
            
            cursor.execute('SELECT id, title, description, quest_type, target_value, reward_type, reward_amount, reward_data, icon, sort_order, is_active, condition_value, parent_id FROM crash_quests ORDER BY sort_order ASC, id DESC')
            quests = cursor.fetchall()
            conn.close()
            
            return jsonify({
                'success': True,
                'quests': [{'id': q[0], 'title': q[1], 'description': q[2], 'quest_type': q[3],
                           'target_value': q[4], 'reward_type': q[5], 'reward_amount': q[6],
                           'reward_data': q[7], 'icon': q[8], 'sort_order': q[9],
                           'is_active': bool(q[10]), 'condition_value': q[11] or 0, 'parent_id': q[12] if len(q) > 12 else None} for q in quests]
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    
    else:  # POST вЂ” create new quest
        try:
            admin_id = request.form.get('admin_id') or request.args.get('admin_id')
            if not admin_id or int(admin_id) != ADMIN_ID:
                return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
            
            title = request.form.get('title', 'РљРІРµСЃС‚')
            description = request.form.get('description', '')
            quest_type = request.form.get('quest_type', 'turnover')
            target_value = int(request.form.get('target_value', 100))
            reward_type = request.form.get('reward_type', 'stars')
            reward_amount = int(request.form.get('reward_amount', 50))
            reward_data = request.form.get('reward_data', '')
            sort_order = int(request.form.get('sort_order', 0))
            condition_value = float(request.form.get('condition_value', 0))
            
            # Handle icon: use icon_url if provided (for skin rewards), otherwise upload file
            # Default icon based on reward type
            default_icons = {
                'stars': '/static/img/star.png',
                'tickets': '/static/img/ticket.png',
                'vip': '/static/img/star2.png',
                'multi': '/static/img/star2.png',
                'crate': '/static/img/star2.png',
            }
            icon_path = default_icons.get(reward_type, '/static/img/star2.png')
            icon_url = request.form.get('icon_url', '')
            if icon_url:
                icon_path = icon_url
            elif 'icon' in request.files:
                f = request.files['icon']
                if f and f.filename:
                    ext = f.filename.rsplit('.', 1)[-1].lower()
                    if ext in ('png', 'gif', 'jpg', 'jpeg', 'webp'):
                        fname = f'quest_{int(datetime.now().timestamp())}_{f.filename}'
                        save_path = os.path.join('static', 'uploads', 'quests', fname)
                        os.makedirs(os.path.dirname(save_path), exist_ok=True)
                        f.save(save_path)
                        icon_path = '/' + save_path.replace('\\', '/')
            
            conn = get_db_connection()
            cursor = conn.cursor()
            parent_id_val = request.form.get('parent_id') or None
            try:
                parent_id_val = int(parent_id_val) if parent_id_val not in (None, '', 'null') else None
            except:
                parent_id_val = None

            cursor.execute('''INSERT INTO crash_quests (title, description, quest_type, target_value, reward_type, reward_amount, reward_data, icon, sort_order, condition_value, parent_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (title, description, quest_type, target_value, reward_type, reward_amount, reward_data, icon_path, sort_order, condition_value, parent_id_val))
            conn.commit()
            conn.close()
            
            return jsonify({'success': True, 'message': 'РљРІРµСЃС‚ СЃРѕР·РґР°РЅ'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/crash-quests/toggle', methods=['POST'])
def admin_toggle_crash_quest():
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE crash_quests SET is_active = ? WHERE id = ?', (1 if data['is_active'] else 0, data['quest_id']))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/crash-quests/delete', methods=['POST'])
def admin_delete_crash_quest():
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM crash_quests WHERE id = ?', (data['quest_id'],))
        cursor.execute('DELETE FROM user_quest_progress WHERE quest_id = ?', (data['quest_id'],))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/win-history', methods=['GET'])
def admin_win_history():
    """РџРѕР»СѓС‡РµРЅРёРµ РїРѕР»РЅРѕР№ РёСЃС‚РѕСЂРёРё РїРѕР±РµРґ"""
    try:
        admin_id = request.args.get('admin_id')
        limit = request.args.get('limit', 100, type=int)

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT wh.id, wh.user_id, wh.user_name, wh.gift_name, wh.gift_image,
                   wh.gift_value, wh.case_name, wh.created_at, u.username
            FROM win_history wh
            LEFT JOIN users u ON wh.user_id = u.id
            ORDER BY wh.created_at DESC
            LIMIT ?
        ''', (limit,))

        wins = cursor.fetchall()
        conn.close()

        win_history_list = []
        for win in wins:
            win_id, user_id, user_name, gift_name, gift_image, gift_value, case_name, created_at, username = win

            file_extension = gift_image.lower().split('.')[-1] if '.' in gift_image else ''
            is_gif = file_extension == 'gif'
            is_image = file_extension in ['png', 'jpg', 'jpeg', 'webp']

            win_history_list.append({
                'id': win_id,
                'user_id': user_id,
                'user_name': user_name,
                'username': username,
                'gift_name': gift_name,
                'gift_image': gift_image,
                'gift_value': gift_value,
                'case_name': case_name,
                'created_at': created_at,
                'is_gif': is_gif,
                'is_image': is_image
            })

        return jsonify({
            'success': True,
            'win_history': win_history_list,
            'total_count': len(win_history_list)
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёСЃС‚РѕСЂРёРё РїРѕР±РµРґ РґР»СЏ Р°РґРјРёРЅРєРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/clear-win-history', methods=['POST'])
def admin_clear_win_history():
    """РћС‡РёСЃС‚РєР° РёСЃС‚РѕСЂРёРё РїРѕР±РµРґ"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('DELETE FROM win_history')
        conn.commit()
        conn.close()

        logger.info(f"рџ› пёЏ РђРґРјРёРЅ {admin_id} РѕС‡РёСЃС‚РёР» РёСЃС‚РѕСЂРёСЋ РїРѕР±РµРґ")
        return jsonify({
            'success': True,
            'message': 'РСЃС‚РѕСЂРёСЏ РїРѕР±РµРґ СѓСЃРїРµС€РЅРѕ РѕС‡РёС‰РµРЅР°'
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РѕС‡РёСЃС‚РєРё РёСЃС‚РѕСЂРёРё РїРѕР±РµРґ: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/admin/case-open-history', methods=['GET'])
def admin_case_open_history():
    """РџРѕР»СѓС‡РµРЅРёРµ РїРѕР»РЅРѕР№ РёСЃС‚РѕСЂРёРё РѕС‚РєСЂС‹С‚РёР№ РєРµР№СЃРѕРІ"""
    try:
        admin_id = request.args.get('admin_id')
        limit = request.args.get('limit', 100, type=int)
        user_id = request.args.get('user_id')

        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})

        conn = get_db_connection()
        cursor = conn.cursor()

        if user_id:
            cursor.execute('''
                SELECT coh.id, coh.user_id, coh.case_id, coh.case_name, coh.gift_id,
                       coh.gift_name, coh.gift_image, coh.gift_value, coh.cost, coh.cost_type,
                       coh.created_at, u.username, u.first_name
                FROM case_open_history coh
                LEFT JOIN users u ON coh.user_id = u.id
                WHERE coh.user_id = ?
                ORDER BY coh.created_at DESC
                LIMIT ?
            ''', (user_id, limit))
        else:
            cursor.execute('''
                SELECT coh.id, coh.user_id, coh.case_id, coh.case_name, coh.gift_id,
                       coh.gift_name, coh.gift_image, coh.gift_value, coh.cost, coh.cost_type,
                       coh.created_at, u.username, u.first_name
                FROM case_open_history coh
                LEFT JOIN users u ON coh.user_id = u.id
                ORDER BY coh.created_at DESC
                LIMIT ?
            ''', (limit,))

        opens = cursor.fetchall()
        conn.close()

        open_history_list = []
        for open_item in opens:
            (open_id, user_id, case_id, case_name, gift_id, gift_name, gift_image,
             gift_value, cost, cost_type, created_at, username, first_name) = open_item

            file_extension = gift_image.lower().split('.')[-1] if '.' in gift_image else ''
            is_gif = file_extension == 'gif'
            is_image = file_extension in ['png', 'jpg', 'jpeg', 'webp']

            open_history_list.append({
                'id': open_id,
                'user_id': user_id,
                'case_id': case_id,
                'case_name': case_name,
                'gift_id': gift_id,
                'gift_name': gift_name,
                'gift_image': gift_image,
                'gift_value': gift_value,
                'cost': cost,
                'cost_type': cost_type,
                'created_at': created_at,
                'username': username,
                'first_name': first_name,
                'is_gif': is_gif,
                'is_image': is_image
            })

        return jsonify({
            'success': True,
            'open_history': open_history_list,
            'total_count': len(open_history_list)
        })

    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёСЃС‚РѕСЂРёРё РѕС‚РєСЂС‹С‚РёР№ РєРµР№СЃРѕРІ РґР»СЏ Р°РґРјРёРЅРєРё: {e}")
        return jsonify({'success': False, 'error': str(e)})

# ==================== Р—РђРџРЈРЎРљ РџР РР›РћР–Р•РќРРЇ ====================

def save_ultimate_crash_history(game_id, final_multiplier):
    """РЎРѕС…СЂР°РЅСЏРµС‚ РёСЃС‚РѕСЂРёСЋ Ultimate Crash РёРіСЂС‹"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO ultimate_crash_history (game_id, final_multiplier, finished_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (game_id, final_multiplier))

        conn.commit()
        conn.close()
        logger.info(f"рџ“ќ РЎРѕС…СЂР°РЅРµРЅР° РёСЃС‚РѕСЂРёСЏ РёРіСЂС‹ #{game_id}, РјРЅРѕР¶РёС‚РµР»СЊ: {final_multiplier}x")
        return True
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ РёСЃС‚РѕСЂРёРё РёРіСЂС‹: {e}")
        return False

def get_ultimate_crash_history(limit=10):
    """РџРѕР»СѓС‡Р°РµС‚ РёСЃС‚РѕСЂРёСЋ РјРЅРѕР¶РёС‚РµР»РµР№"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, game_id, final_multiplier, finished_at
            FROM ultimate_crash_history
            ORDER BY finished_at DESC
            LIMIT ?
        ''', (limit,))

        history = cursor.fetchall()
        conn.close()

        history_list = []
        for item in history:
            history_list.append({
                'id': item[0],
                'game_id': item[1],
                'final_multiplier': float(item[2]),
                'finished_at': item[3]
            })

        logger.info(f"рџ“Љ Р—Р°РіСЂСѓР¶РµРЅРѕ {len(history_list)} Р·Р°РїРёСЃРµР№ РёСЃС‚РѕСЂРёРё")
        return history_list
    except Exception as e:
        logger.error(f"вќЊ РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ РёСЃС‚РѕСЂРёРё: {e}")
        return []

# ==================== Р—РђРџРЈРЎРљ РџР РР›РћР–Р•РќРРЇ ====================

# Р¤Р»Р°Рі: СѓР¶Рµ РёРЅРёС†РёР°Р»РёР·РёСЂРѕРІР°РЅРѕ (Р·Р°С‰РёС‚Р° РѕС‚ РїРѕРІС‚РѕСЂРЅРѕРіРѕ Р·Р°РїСѓСЃРєР° РІ WSGI)
_app_initialized = False
_init_lock = threading.Lock()

def _lazy_init():
    """Р›РµРЅРёРІР°СЏ РёРЅРёС†РёР°Р»РёР·Р°С†РёСЏ вЂ” РІС‹Р·С‹РІР°РµС‚СЃСЏ РѕРґРёРЅ СЂР°Р· РїСЂРё РїРµСЂРІРѕРј Р·Р°РїСЂРѕСЃРµ РёР»Рё РїСЂРё СЃС‚Р°СЂС‚Рµ"""
    global _app_initialized
    if _app_initialized:
        return
    with _init_lock:
        if _app_initialized:
            return
        logger.info("рџљЂ РРЅРёС†РёР°Р»РёР·Р°С†РёСЏ РїСЂРёР»РѕР¶РµРЅРёСЏ...")
        try:
            safe_init_db()
        except Exception as e:
            logger.error(f"вќЊ РћС€РёР±РєР° РёРЅРёС†РёР°Р»РёР·Р°С†РёРё Р‘Р”: {e}")
        # Р—Р°РїСѓСЃРє РёРіСЂРѕРІС‹С… С†РёРєР»РѕРІ
        try:
            start_crash_loop()
        except Exception as e:
            logger.error(f"вќЊ РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РїСѓСЃС‚РёС‚СЊ Crash loop: {e}")
        try:
            start_ultimate_crash_loop()
        except Exception as e:
            logger.error(f"вќЊ РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РїСѓСЃС‚РёС‚СЊ Ultimate Crash loop: {e}")
        # Webhook Telegram
        try:
            threading.Thread(target=setup_telegram_webhook, daemon=True).start()
        except Exception as e:
            logger.error(f"вќЊ РќРµ СѓРґР°Р»РѕСЃСЊ РЅР°СЃС‚СЂРѕРёС‚СЊ webhook: {e}")
        # NFT Gift Monitor
        try:
            start_nft_monitor()
        except Exception as e:
            logger.error(f"вќЊ РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РїСѓСЃС‚РёС‚СЊ NFT Monitor: {e}")
        # Pre-fetch Fragment catalog in background
        try:
            def _prefetch_fragment():
                try:
                    catalog = fetch_fragment_gifts_catalog(force_refresh=True)
                    logger.info(f"рџ“¦ Fragment catalog pre-fetched: {len(catalog or [])} gifts")
                except Exception as fe:
                    logger.warning(f"вљ пёЏ Fragment pre-fetch failed: {fe}")
            threading.Thread(target=_prefetch_fragment, daemon=True).start()
        except Exception as e:
            logger.warning(f"вљ пёЏ Fragment pre-fetch thread failed: {e}")
        _app_initialized = True
        logger.info("вњ… РџСЂРёР»РѕР¶РµРЅРёРµ РёРЅРёС†РёР°Р»РёР·РёСЂРѕРІР°РЅРѕ")

@app.before_request
def ensure_initialized():
    """Р“Р°СЂР°РЅС‚РёСЂСѓРµС‚ С‡С‚Рѕ Р‘Р” Рё РёРіСЂРѕРІС‹Рµ С†РёРєР»С‹ Р·Р°РїСѓС‰РµРЅС‹ РїРµСЂРµРґ РѕР±СЂР°Р±РѕС‚РєРѕР№ Р·Р°РїСЂРѕСЃРѕРІ"""
    _lazy_init()

# ==================== TELEGRAM BOT (WEBHOOK, Р±РµР· telebot) ====================

# РЎРѕСЃС‚РѕСЏРЅРёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ Р±РѕС‚Р° (РґР»СЏ СЂР°СЃСЃС‹Р»РєРё)
_bot_user_states = {}
_broadcast_messages = {}  # chat_id -> message data РґР»СЏ СЂР°СЃСЃС‹Р»РєРё

# --- Telegram API helpers ---
def tg_api(method, **kwargs):
    """Р’С‹Р·РѕРІ Telegram Bot API"""
    try:
        r = http_requests.post(f'{TG_API}/{method}', json=kwargs, timeout=15)
        data = r.json()
        if not data.get('ok'):
            logger.warning(f"TG API {method} err: {data}")
        return data
    except Exception as e:
        logger.error(f"TG API {method} exception: {e}")
        return {'ok': False}

def tg_send(chat_id, text, parse_mode='HTML', reply_markup=None):
    kw = {'chat_id': chat_id, 'text': text, 'parse_mode': parse_mode}
    if reply_markup:
        kw['reply_markup'] = reply_markup
    return tg_api('sendMessage', **kw)

def tg_edit(chat_id, message_id, text, parse_mode='HTML', reply_markup=None):
    kw = {'chat_id': chat_id, 'message_id': message_id, 'text': text, 'parse_mode': parse_mode}
    if reply_markup:
        kw['reply_markup'] = reply_markup
    return tg_api('editMessageText', **kw)

def tg_delete(chat_id, message_id):
    return tg_api('deleteMessage', chat_id=chat_id, message_id=message_id)

def tg_answer_cb(callback_query_id, text='', show_alert=False):
    return tg_api('answerCallbackQuery', callback_query_id=callback_query_id, text=text, show_alert=show_alert)

def tg_copy_message(chat_id, from_chat_id, message_id):
    return tg_api('copyMessage', chat_id=chat_id, from_chat_id=from_chat_id, message_id=message_id)

def tg_get_profile_photos(user_id):
    return tg_api('getUserProfilePhotos', user_id=user_id, limit=1)

def tg_get_file(file_id):
    return tg_api('getFile', file_id=file_id)

# --- Inline keyboards ---
def make_play_button():
    return {'inline_keyboard': [[{'text': 'рџЋ® РР“Р РђРўР¬', 'web_app': {'url': WEBSITE_URL}}]]}

def make_admin_menu():
    return {'inline_keyboard': [
        [{'text': 'рџ“ў Р Р°СЃСЃС‹Р»РєР°', 'callback_data': 'admin_broadcast'},
         {'text': 'рџ“Љ РЎС‚Р°С‚РёСЃС‚РёРєР°', 'callback_data': 'admin_stats'}],
        [{'text': 'рџ‘Ґ РџРѕР»СЊР·РѕРІР°С‚РµР»Рё', 'callback_data': 'admin_users'},
         {'text': 'вљ™пёЏ РќР°СЃС‚СЂРѕР№РєРё', 'callback_data': 'admin_settings'}],
        [{'text': 'вќЊ Р—Р°РєСЂС‹С‚СЊ', 'callback_data': 'admin_close'}]
    ]}

# --- Bot DB helpers ---
def bot_add_user(user_id, first_name, username):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM users WHERE id = ?', (user_id,))
        if not cursor.fetchone():
            ref_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            cursor.execute('''INSERT INTO users (id, first_name, username, balance_stars, balance_tickets, referral_code, created_at)
                              VALUES (?, ?, ?, 0, 0, ?, datetime('now'))''', (user_id, first_name, username, ref_code))
        else:
            cursor.execute('UPDATE users SET first_name = ?, username = ? WHERE id = ?', (first_name, username, user_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"bot_add_user error: {e}")

def bot_get_all_user_ids():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM users')
        ids = [r[0] for r in cursor.fetchall()]
        conn.close()
        return ids
    except:
        return []

def bot_get_user_count():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM users')
        c = cursor.fetchone()[0]
        conn.close()
        return c
    except:
        return 0

# --- /start handler ---
def handle_start(msg):
    user = msg.get('from', {})
    chat_id = msg['chat']['id']
    uid = user.get('id')
    first_name = user.get('first_name', '')
    username = user.get('username', '')
    text = msg.get('text', '')
    
    bot_add_user(uid, first_name, username)
    
    # Check for referral code: /start ref_XXXXXXXX
    if ' ' in text:
        param = text.split(' ', 1)[1].strip()
        if param.startswith('ref_'):
            ref_code = param[4:]  # Extract code after 'ref_'
            if ref_code:
                try:
                    process_referral(uid, ref_code)
                    logger.info(f"рџ‘Ґ Р РµС„РµСЂР°Р» С‡РµСЂРµР· Р±РѕС‚Р°: {uid} -> {ref_code}")
                except Exception as e:
                    logger.error(f"вќЊ РћС€РёР±РєР° СЂРµС„РµСЂР°Р»Р° С‡РµСЂРµР· Р±РѕС‚Р°: {e}")
    
    tg_send(chat_id, f"РџСЂРёРІРµС‚, {first_name}! рџЋ®\n\nРќР°Р¶РјРё РєРЅРѕРїРєСѓ С‡С‚РѕР±С‹ РЅР°С‡Р°С‚СЊ:", reply_markup=make_play_button())

# --- /auth handler ---
def handle_auth(msg):
    user = msg.get('from', {})
    chat_id = msg['chat']['id']
    uid = user.get('id')
    first_name = user.get('first_name', '')
    username = user.get('username', '')
    text = msg.get('text', '')
    parts = text.split(maxsplit=1)

    if len(parts) < 2 or len(parts[1].strip()) != 20:
        tg_send(chat_id, "вќЊ <b>РќРµРІРµСЂРЅС‹Р№ С„РѕСЂРјР°С‚</b>\n\nРСЃРїРѕР»СЊР·СѓР№С‚Рµ: <code>/auth Р’РђРЁ_РљРћР”</code>\nРљРѕРґ РјРѕР¶РЅРѕ РїРѕР»СѓС‡РёС‚СЊ РЅР° СЃР°Р№С‚Рµ.")
        return

    code = parts[1].strip().upper()
    bot_add_user(uid, first_name, username)

    # Р¤РѕС‚Рѕ РїСЂРѕС„РёР»СЏ
    photo_url = ''
    try:
        photos_resp = tg_get_profile_photos(uid)
        if photos_resp.get('ok'):
            result = photos_resp['result']
            if result.get('total_count', 0) > 0:
                file_id = result['photos'][0][-1]['file_id']
                file_resp = tg_get_file(file_id)
                if file_resp.get('ok'):
                    fp = file_resp['result']['file_path']
                    photo_url = f'https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{fp}'
    except Exception as e:
        logger.warning(f"photo err: {e}")

    # Р”Р°РЅРЅС‹Рµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
    balance_stars = 0; balance_tickets = 0; experience = 0; current_level = 1
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT balance_stars, balance_tickets, experience, current_level FROM users WHERE id = ?', (uid,))
        row = cursor.fetchone()
        if row:
            balance_stars = row[0] or 0; balance_tickets = row[1] or 0
            experience = row[2] or 0; current_level = row[3] or 1
        conn.close()
    except:
        pass

    user_data = {
        'id': uid, 'first_name': first_name,
        'last_name': user.get('last_name', ''), 'username': username,
        'photo_url': photo_url, 'balance_stars': balance_stars,
        'balance_tickets': balance_tickets, 'experience': experience,
        'current_level': current_level
    }

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT confirmed FROM auth_codes WHERE code = ? AND created_at > datetime('now', '-10 minutes')", (code,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            tg_send(chat_id, "вќЊ <b>РќРµРІРµСЂРЅС‹Р№ РєРѕРґ</b>\n\nРљРѕРґ РЅРµ РЅР°Р№РґРµРЅ РёР»Рё РёСЃС‚С‘Рє. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР»СѓС‡РёС‚СЊ РЅРѕРІС‹Р№ РЅР° СЃР°Р№С‚Рµ.")
            return
        if row[0]:
            conn.close()
            tg_send(chat_id, "вќЊ <b>РљРѕРґ СѓР¶Рµ РёСЃРїРѕР»СЊР·РѕРІР°РЅ</b>\nРџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР»СѓС‡РёС‚СЊ РЅРѕРІС‹Р№ РєРѕРґ РЅР° СЃР°Р№С‚Рµ.")
            return
        cursor.execute('UPDATE auth_codes SET confirmed = 1, user_data = ? WHERE code = ?',
                       (json.dumps(user_data, ensure_ascii=False), code))
        conn.commit()
        conn.close()
        tg_send(chat_id, f"вњ… <b>РђРІС‚РѕСЂРёР·Р°С†РёСЏ СѓСЃРїРµС€РЅР°!</b>\n\nР’С‹ РІРѕС€Р»Рё РєР°Рє <b>{first_name}</b>\nРўРµРїРµСЂСЊ РІРµСЂРЅРёС‚РµСЃСЊ РЅР° СЃР°Р№С‚ вЂ” РѕРЅ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё РІРѕР№РґС‘С‚.")
        logger.info(f"вњ… Auth OK: {first_name} ({uid})")
    except Exception as e:
        logger.error(f"auth confirm err: {e}")
        tg_send(chat_id, "вќЊ <b>РћС€РёР±РєР° СЃРѕРµРґРёРЅРµРЅРёСЏ СЃ СЃРµСЂРІРµСЂРѕРј</b>\n\nРџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.")

# --- /admin handler ---
def handle_admin(msg):
    uid = msg['from']['id']
    chat_id = msg['chat']['id']
    if uid != ADMIN_ID:
        tg_send(chat_id, "в›”")
        return
    tg_send(chat_id, "рџ› пёЏ <b>РђР”РњРРќ-РџРђРќР•Р›Р¬</b>\n\nР’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ:", reply_markup=make_admin_menu())


# --- /refund handler (admin only) ---
def handle_refund(msg):
    """Handle /refund command вЂ” admin only. Shows recent star transactions for refund."""
    uid = msg['from']['id']
    chat_id = msg['chat']['id']
    if uid != ADMIN_ID:
        tg_send(chat_id, "в›” РўРѕР»СЊРєРѕ РґР»СЏ Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂР°")
        return

    text = msg.get('text', '').strip()
    parts = text.split()

    # /refund <telegram_payment_charge_id> вЂ” direct refund by charge ID
    if len(parts) >= 2:
        charge_id = parts[1].strip()
        # Look up in our DB to find user_id
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT user_id, amount FROM stars_payments WHERE charge_id = ?', (charge_id,))
            row = cursor.fetchone()
            conn.close()
        except Exception as e:
            logger.error(f"Refund DB lookup error: {e}")
            tg_send(chat_id, f"вќЊ РћС€РёР±РєР° Р‘Р”: {e}")
            return

        if row:
            target_user_id, amount = row[0], row[1]
        else:
            # Not in our DB вЂ” ask for user_id
            if len(parts) >= 3:
                try:
                    target_user_id = int(parts[2])
                    amount = None
                except ValueError:
                    tg_send(chat_id, "вќЊ РќРµРІРµСЂРЅС‹Р№ С„РѕСЂРјР°С‚.\n\nРСЃРїРѕР»СЊР·РѕРІР°РЅРёРµ:\n<code>/refund charge_id</code>\n<code>/refund charge_id user_id</code>")
                    return
            else:
                tg_send(chat_id, f"вљ пёЏ РџР»Р°С‚С‘Р¶ <code>{charge_id}</code> РЅРµ РЅР°Р№РґРµРЅ РІ Р‘Р”.\n\n"
                        f"РЈРєР°Р¶РёС‚Рµ user_id:\n<code>/refund {charge_id} USER_ID</code>")
                return

        # Execute refund via Telegram API
        result = tg_api('refundStarPayment',
                        user_id=target_user_id,
                        telegram_payment_charge_id=charge_id)

        if result.get('ok'):
            # Remove from our DB and deduct balance
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute('DELETE FROM stars_payments WHERE charge_id = ?', (charge_id,))
                if amount and amount > 0:
                    cursor.execute('UPDATE users SET balance_stars = MAX(0, balance_stars - ?) WHERE id = ?',
                                   (amount, target_user_id))
                conn.commit()
                conn.close()
                if target_user_id in _user_cache:
                    del _user_cache[target_user_id]
            except Exception as e:
                logger.error(f"Refund DB cleanup error: {e}")

            amt_str = f" ({amount} в­ђ)" if amount else ""
            tg_send(chat_id, f"вњ… <b>Р’РѕР·РІСЂР°С‚ РІС‹РїРѕР»РЅРµРЅ</b>\n\n"
                    f"Charge ID: <code>{charge_id}</code>\n"
                    f"РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ: <code>{target_user_id}</code>{amt_str}")
            logger.info(f"в­ђ Refund OK: user={target_user_id}, charge={charge_id}, amount={amount}")
        else:
            err = result.get('description', 'РќРµРёР·РІРµСЃС‚РЅР°СЏ РѕС€РёР±РєР°')
            tg_send(chat_id, f"вќЊ <b>РћС€РёР±РєР° РІРѕР·РІСЂР°С‚Р°</b>\n\n{err}\n\n"
                    f"Charge ID: <code>{charge_id}</code>\n"
                    f"User ID: <code>{target_user_id}</code>")
        return

    # /refund without args вЂ” show list of recent transactions
    # Fetch from Telegram getStarTransactions
    result = tg_api('getStarTransactions', offset=0, limit=20)
    if not result.get('ok'):
        err = result.get('description', 'РћС€РёР±РєР°')
        tg_send(chat_id, f"вќЊ РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕР»СѓС‡РёС‚СЊ С‚СЂР°РЅР·Р°РєС†РёРё: {err}")
        return

    transactions = result.get('result', {}).get('transactions', [])
    if not transactions:
        tg_send(chat_id, "в„№пёЏ РќРµС‚ С‚СЂР°РЅР·Р°РєС†РёР№ РґР»СЏ РѕС‚РѕР±СЂР°Р¶РµРЅРёСЏ.")
        return

    # Filter only incoming (amount > 0) transactions that can be refunded
    incoming = [t for t in transactions if t.get('amount', 0) > 0]
    if not incoming:
        tg_send(chat_id, "в„№пёЏ РќРµС‚ РІС…РѕРґСЏС‰РёС… С‚СЂР°РЅР·Р°РєС†РёР№ РґР»СЏ РІРѕР·РІСЂР°С‚Р°.")
        return

    lines = ["рџ’« <b>РџРѕСЃР»РµРґРЅРёРµ С‚СЂР°РЅР·Р°РєС†РёРё Stars</b>\n"]
    buttons = []
    for tx in incoming[:15]:
        tx_id = tx.get('id', '?')
        amount = tx.get('amount', 0)
        date_unix = tx.get('date', 0)
        source = tx.get('source', {})
        tx_user = source.get('user', {})
        tx_user_id = tx_user.get('id', '?')
        tx_user_name = tx_user.get('first_name', '') or str(tx_user_id)
        charge_id = source.get('invoice_payload', '')
        # Try to extract charge_id from source
        # In getStarTransactions, the source has type and invoice_payload
        source_type = source.get('type', '?')

        dt_str = datetime.utcfromtimestamp(date_unix).strftime('%d.%m %H:%M') if date_unix else '?'

        lines.append(f"вЂў <b>{amount} в­ђ</b> РѕС‚ {tx_user_name} (<code>{tx_user_id}</code>) вЂ” {dt_str}")
        lines.append(f"  ID: <code>{tx_id}</code>")

        # Add refund button if it's a user payment
        if tx_user_id != '?' and source_type in ('user', 'invoice'):
            btn_text = f"в†©пёЏ Р’РѕР·РІСЂР°С‚ {amount}в­ђ в†’ {tx_user_name}"
            # callback data has max 64 bytes
            cb_data = f"refund:{tx_user_id}:{tx_id}"
            if len(cb_data) <= 64:
                buttons.append([{'text': btn_text, 'callback_data': cb_data}])

    text_msg = "\n".join(lines)
    text_msg += "\n\nрџ’Ў РќР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ РґР»СЏ РІРѕР·РІСЂР°С‚Р° РёР»Рё:\n<code>/refund CHARGE_ID</code>"

    kb = {'inline_keyboard': buttons} if buttons else None
    tg_send(chat_id, text_msg, reply_markup=kb)

# --- Callback handler ---
def handle_callback(cb):
    uid = cb['from']['id']
    chat_id = cb['message']['chat']['id']
    msg_id = cb['message']['message_id']
    data = cb.get('data', '')
    cb_id = cb['id']

    # Handle refund callbacks
    if data.startswith('refund:'):
        if uid != ADMIN_ID:
            tg_answer_cb(cb_id, 'в›” РќРµС‚ РґРѕСЃС‚СѓРїР°', True)
            return
        parts = data.split(':')
        if len(parts) >= 3:
            refund_user_id = int(parts[1])
            refund_charge_id = parts[2]
            result = tg_api('refundStarPayment',
                            user_id=refund_user_id,
                            telegram_payment_charge_id=refund_charge_id)
            if result.get('ok'):
                # Clean up DB
                try:
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute('SELECT amount FROM stars_payments WHERE charge_id = ?', (refund_charge_id,))
                    row = cursor.fetchone()
                    refund_amount = row[0] if row else 0
                    cursor.execute('DELETE FROM stars_payments WHERE charge_id = ?', (refund_charge_id,))
                    if refund_amount > 0:
                        cursor.execute('UPDATE users SET balance_stars = MAX(0, balance_stars - ?) WHERE id = ?',
                                       (refund_amount, refund_user_id))
                    conn.commit()
                    conn.close()
                    if refund_user_id in _user_cache:
                        del _user_cache[refund_user_id]
                except Exception as e:
                    logger.error(f"Refund callback DB error: {e}")
                    refund_amount = 0
                tg_answer_cb(cb_id, f'вњ… Р’РѕР·РІСЂР°С‚ РІС‹РїРѕР»РЅРµРЅ!', True)
                # Update message text
                try:
                    tg_edit(chat_id, msg_id,
                            f"вњ… <b>Р’РѕР·РІСЂР°С‚ РІС‹РїРѕР»РЅРµРЅ</b>\n\n"
                            f"User: <code>{refund_user_id}</code>\n"
                            f"Charge: <code>{refund_charge_id}</code>\n"
                            f"РЎСѓРјРјР°: {refund_amount} в­ђ" if refund_amount else
                            f"вњ… <b>Р’РѕР·РІСЂР°С‚ РІС‹РїРѕР»РЅРµРЅ</b>\n\n"
                            f"User: <code>{refund_user_id}</code>\n"
                            f"Charge: <code>{refund_charge_id}</code>")
                except Exception:
                    pass
                logger.info(f"в­ђ Refund via callback OK: user={refund_user_id}, charge={refund_charge_id}")
            else:
                err = result.get('description', 'РћС€РёР±РєР°')
                tg_answer_cb(cb_id, f'вќЊ {err}', True)
        else:
            tg_answer_cb(cb_id, 'вќЊ РќРµРІРµСЂРЅС‹Рµ РґР°РЅРЅС‹Рµ', True)
        return

    if not data.startswith('admin_') and data not in ('confirm_broadcast_all', 'cancel_broadcast'):
        tg_answer_cb(cb_id)
        return

    if data.startswith('admin_') and uid != ADMIN_ID:
        tg_answer_cb(cb_id, 'в›” РќРµС‚ РґРѕСЃС‚СѓРїР°', True)
        return

    if data == 'admin_broadcast':
        _bot_user_states[uid] = 'waiting_broadcast'
        tg_delete(chat_id, msg_id)
        tg_send(chat_id, "рџ“ў <b>РЎРћР—Р”РђРќРР• Р РђРЎРЎР«Р›РљР</b>\n\nРћС‚РїСЂР°РІСЊС‚Рµ СЃРѕРѕР±С‰РµРЅРёРµ РґР»СЏ СЂР°СЃСЃС‹Р»РєРё:\nвЂў РўРµРєСЃС‚\nвЂў Р¤РѕС‚Рѕ (СЃ РїРѕРґРїРёСЃСЊСЋ)\nвЂў Р’РёРґРµРѕ / GIF / Р”РѕРєСѓРјРµРЅС‚ / РЎС‚РёРєРµСЂ\n\nРЎРѕРѕР±С‰РµРЅРёРµ Р±СѓРґРµС‚ РѕС‚РїСЂР°РІР»РµРЅРѕ <b>С‚РѕС‡РЅРѕ С‚Р°Рє Р¶Рµ</b> РєР°Рє РІС‹ РµРіРѕ РѕС‚РїСЂР°РІРёС‚Рµ.\n\nР”Р»СЏ РѕС‚РјРµРЅС‹ РЅР°Р¶РјРёС‚Рµ /cancel")

    elif data == 'admin_stats':
        count = bot_get_user_count()
        kb = {'inline_keyboard': [
            [{'text': 'рџ”„ РћР±РЅРѕРІРёС‚СЊ', 'callback_data': 'admin_stats'}],
            [{'text': 'в¬…пёЏ РќР°Р·Р°Рґ', 'callback_data': 'admin_back'}]
        ]}
        tg_edit(chat_id, msg_id,
                f"рџ“Љ <b>РЎРўРђРўРРЎРўРРљРђ Р‘РћРўРђ</b>\n\n"
                f"рџ‘Ґ РџРѕР»СЊР·РѕРІР°С‚РµР»РµР№: <b>{count}</b>\n"
                f"вЏ° Р’СЂРµРјСЏ: <b>{datetime.now().strftime('%H:%M:%S')}</b>\n"
                f"рџ“… Р”Р°С‚Р°: <b>{datetime.now().strftime('%d.%m.%Y')}</b>\n\n"
                f"рџЊђ РЎР°Р№С‚: {WEBSITE_URL}",
                reply_markup=kb)

    elif data == 'admin_users':
        users = bot_get_all_user_ids()
        kb = {'inline_keyboard': [
            [{'text': 'рџ“ў Р Р°СЃСЃС‹Р»РєР° РІСЃРµРј', 'callback_data': 'admin_broadcast'}],
            [{'text': 'в¬…пёЏ РќР°Р·Р°Рґ', 'callback_data': 'admin_back'}]
        ]}
        tg_edit(chat_id, msg_id,
                f"рџ‘Ґ <b>РџРћР›Р¬Р—РћР’РђРўР•Р›Р</b>\n\nР’СЃРµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№: <b>{len(users)}</b>",
                reply_markup=kb)

    elif data == 'admin_settings':
        kb = {'inline_keyboard': [[{'text': 'в¬…пёЏ РќР°Р·Р°Рґ', 'callback_data': 'admin_back'}]]}
        tg_edit(chat_id, msg_id,
                f"вљ™пёЏ <b>РќРђРЎРўР РћР™РљР Р‘РћРўРђ</b>\n\n"
                f"рџ†” ID Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂР°: <code>{ADMIN_ID}</code>\n"
                f"рџЊђ URL СЃР°Р№С‚Р°: {WEBSITE_URL}\n"
                f"рџ”‘ РўРѕРєРµРЅ: <code>{TELEGRAM_BOT_TOKEN[:10]}...</code>\n\n"
                f"рџ”„ Р‘РѕС‚ СЂР°Р±РѕС‚Р°РµС‚ (webhook)!",
                reply_markup=kb)

    elif data == 'admin_back':
        tg_edit(chat_id, msg_id, "рџ› пёЏ <b>РђР”РњРРќ-РџРђРќР•Р›Р¬</b>\n\nР’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ:", reply_markup=make_admin_menu())

    elif data == 'admin_close':
        tg_delete(chat_id, msg_id)

    elif data == 'confirm_broadcast_all':
        if uid != ADMIN_ID:
            tg_answer_cb(cb_id, 'в›”', True)
            return
        src = _broadcast_messages.get(uid)
        if not src:
            tg_answer_cb(cb_id, 'вќЊ РЎРѕРѕР±С‰РµРЅРёРµ РЅРµ РЅР°Р№РґРµРЅРѕ', True)
            return
        tg_delete(chat_id, msg_id)
        user_ids = bot_get_all_user_ids()
        if not user_ids:
            tg_send(chat_id, "вќЊ РќРµС‚ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№")
            tg_answer_cb(cb_id)
            return
        # Р Р°СЃСЃС‹Р»РєР° РІ С„РѕРЅРµ
        threading.Thread(target=run_webhook_broadcast, args=(chat_id, uid, user_ids, src), daemon=True).start()
        tg_answer_cb(cb_id, 'вњ… Р Р°СЃСЃС‹Р»РєР° РЅР°С‡Р°С‚Р°')

    elif data == 'cancel_broadcast':
        _bot_user_states.pop(uid, None)
        _broadcast_messages.pop(uid, None)
        tg_delete(chat_id, msg_id)
        tg_send(chat_id, "рџ› пёЏ <b>РђР”РњРРќ-РџРђРќР•Р›Р¬</b>\n\nР’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ:", reply_markup=make_admin_menu())
        tg_answer_cb(cb_id, 'вќЊ Р Р°СЃСЃС‹Р»РєР° РѕС‚РјРµРЅРµРЅР°')

    if data not in ('confirm_broadcast_all', 'cancel_broadcast'):
        tg_answer_cb(cb_id)

def run_webhook_broadcast(chat_id, admin_id, user_ids, src):
    """Р Р°СЃСЃС‹Р»РєР° С‡РµСЂРµР· copyMessage (РІ С„РѕРЅРѕРІРѕРј РїРѕС‚РѕРєРµ)"""
    total = len(user_ids)
    ok = 0; fail = 0
    progress_resp = tg_send(chat_id, f"рџ”„ Р Р°СЃСЃС‹Р»РєР°... 0/{total}")
    progress_msg_id = None
    if progress_resp.get('ok'):
        progress_msg_id = progress_resp['result']['message_id']

    for i, uid in enumerate(user_ids, 1):
        r = tg_copy_message(uid, src['chat_id'], src['message_id'])
        if r.get('ok'):
            ok += 1
        else:
            fail += 1
        if progress_msg_id and (i % 10 == 0 or i == total):
            try:
                tg_edit(chat_id, progress_msg_id, f"рџ”„ Р Р°СЃСЃС‹Р»РєР°... {i}/{total}\nвњ… {ok}  вќЊ {fail}")
            except:
                pass
        time.sleep(0.05)

    rate = (ok / total * 100) if total else 0
    kb = {'inline_keyboard': [[{'text': 'в¬…пёЏ Р’ Р°РґРјРёРЅ-РїР°РЅРµР»СЊ', 'callback_data': 'admin_back'}]]}
    report = (f"вњ… <b>Р РђРЎРЎР«Р›РљРђ Р—РђР’Р•Р РЁР•РќРђ</b>\n\n"
              f"рџ“Љ Р’СЃРµРіРѕ: {total}\nвњ… РЈСЃРїРµС€РЅРѕ: {ok}\nвќЊ РћС€РёР±РѕРє: {fail}\n"
              f"рџ“€ РЈСЃРїРµС€РЅРѕСЃС‚СЊ: {rate:.1f}%\nвЏ° {datetime.now().strftime('%H:%M:%S')}")
    if progress_msg_id:
        tg_edit(chat_id, progress_msg_id, report, reply_markup=kb)
    else:
        tg_send(chat_id, report, reply_markup=kb)
    _broadcast_messages.pop(admin_id, None)

# --- РћР±СЂР°Р±РѕС‚РєР° С‚РµРєСЃС‚РѕРІС‹С…/РјРµРґРёР° СЃРѕРѕР±С‰РµРЅРёР№ ---
def handle_message(msg):
    """РћР±СЂР°Р±РѕС‚РєР° Р»СЋР±РѕРіРѕ РІС…РѕРґСЏС‰РµРіРѕ СЃРѕРѕР±С‰РµРЅРёСЏ (РЅРµ РєРѕРјР°РЅРґС‹)"""
    sender = msg.get('from')
    if not sender:
        return
    uid = sender['id']
    chat_id = msg['chat']['id']

    # РџСЂРѕРІРµСЂСЏРµРј: Р¶РґС‘Рј Р»Рё СЂР°СЃСЃС‹Р»РѕС‡РЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ РѕС‚ Р°РґРјРёРЅР°
    if uid == ADMIN_ID and _bot_user_states.get(uid) == 'waiting_broadcast':
        text = msg.get('text', '')
        if text == '/cancel':
            _bot_user_states.pop(uid, None)
            tg_send(chat_id, "рџ› пёЏ <b>РђР”РњРРќ-РџРђРќР•Р›Р¬</b>\n\nР’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ:", reply_markup=make_admin_menu())
            return
        # РЎРѕС…СЂР°РЅСЏРµРј source message РґР»СЏ copyMessage
        _broadcast_messages[uid] = {'chat_id': chat_id, 'message_id': msg['message_id']}
        _bot_user_states.pop(uid, None)
        kb = {'inline_keyboard': [
            [{'text': 'вњ… РћС‚РїСЂР°РІРёС‚СЊ РІСЃРµРј', 'callback_data': 'confirm_broadcast_all'},
             {'text': 'вќЊ РћС‚РјРµРЅРёС‚СЊ', 'callback_data': 'cancel_broadcast'}]
        ]}
        tg_send(chat_id, "рџ“ў <b>РџР Р•Р’Р¬Р® Р РђРЎРЎР«Р›РљР</b>\n\nРЎРѕРѕР±С‰РµРЅРёРµ РІС‹С€Рµ Р±СѓРґРµС‚ РѕС‚РїСЂР°РІР»РµРЅРѕ РІСЃРµРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏРј.\nРџСЂРѕРґРѕР»Р¶РёС‚СЊ?", reply_markup=kb)
        return

    # === Telegram Gift / NFT deposit detection ===
    # Telegram Bot API: message.gift = GiftInfo, message.unique_gift = UniqueGiftInfo
    try:
        gift_info = msg.get('gift')          # GiftInfo object (regular gift)
        unique_gift_info = msg.get('unique_gift')  # UniqueGiftInfo object (unique/NFT gift)
        
        if gift_info or unique_gift_info:
            msg_id = msg.get('message_id', 0)
            
            if gift_info:
                # Regular gift: GiftInfo { gift: Gift, convert_star_count, text, ... }
                gift_obj = gift_info.get('gift', {})
                star_count = gift_obj.get('star_count', 0)
                tg_gift_id = str(gift_obj.get('id', ''))
                gift_sticker = gift_obj.get('sticker', {})
                gift_emoji = gift_sticker.get('emoji', '')
                convert_stars = gift_info.get('convert_star_count', 0)
                gift_text = gift_info.get('text', '')
                gift_type = 'regular'
                gift_label = f"Gift ({star_count} Stars)"
                if gift_emoji:
                    gift_label = f"{gift_emoji} Gift ({star_count} Stars)"
                logger.info(f"Regular gift from user {uid}: id={tg_gift_id}, star_count={star_count}, convert={convert_stars}")
            else:
                # Unique/NFT gift: UniqueGiftInfo { gift: UniqueGift, origin, ... }
                gift_obj = unique_gift_info.get('gift', {})
                tg_gift_id = str(gift_obj.get('gift_id', '') or gift_obj.get('name', ''))
                base_name = gift_obj.get('base_name', 'Unique Gift')
                unique_name = gift_obj.get('name', '')
                gift_number = gift_obj.get('number', 0)
                origin = unique_gift_info.get('origin', '')
                # Unique gifts: use the original gift's star value if available
                # Try to find star_count from the base gift_id
                star_count = 0
                # Check transfer_star_count as a value indicator
                transfer_stars = unique_gift_info.get('transfer_star_count', 0)
                if transfer_stars > 0:
                    star_count = transfer_stars
                else:
                    star_count = 500  # Default value for unique gifts
                convert_stars = 0
                gift_type = 'unique'
                gift_label = f"{base_name} #{gift_number}" if gift_number else base_name
                logger.info(f"Unique gift from user {uid}: id={tg_gift_id}, base={base_name}, name={unique_name}, origin={origin}")

                # Try Fragment catalog lookup for better value
                matched_gift = _match_gift_by_name(base_name)
                if matched_gift and matched_gift.get('value', 0) > 0:
                    star_count = matched_gift.get('value', 0)
                    gift_label = matched_gift.get('name', gift_label)
                    logger.info(f"Unique gift catalog match: '{base_name}' -> '{gift_label}', value={star_count}")
            
            # Value to credit
            value = star_count
            
            if value <= 0:
                tg_send(chat_id, "РџРѕРґР°СЂРѕРє РїРѕР»СѓС‡РµРЅ, РЅРѕ РЅРµ СѓРґР°Р»РѕСЃСЊ РѕРїСЂРµРґРµР»РёС‚СЊ РµРіРѕ СЃС‚РѕРёРјРѕСЃС‚СЊ. "
                        "РћР±СЂР°С‚РёС‚РµСЃСЊ Рє Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂСѓ.")
                logger.warning(f"Gift with 0 value from user {uid}: type={gift_type}, id={tg_gift_id}")
                return
            
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                
                # Create table if not exists (with dedup fields)
                cursor.execute('''CREATE TABLE IF NOT EXISTS gift_deposits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    gift_name TEXT NOT NULL,
                    gift_value INTEGER NOT NULL,
                    gift_type TEXT DEFAULT 'regular',
                    telegram_gift_id TEXT,
                    message_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
                
                # Dedup check: same user + same message_id
                if msg_id > 0:
                    cursor.execute('SELECT id FROM gift_deposits WHERE user_id = ? AND message_id = ?', (uid, msg_id))
                    if cursor.fetchone():
                        conn.close()
                        logger.info(f"Gift deposit DUPLICATE skipped: user {uid}, msg_id={msg_id}")
                        return
                
                # Check user exists
                cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (uid,))
                user = cursor.fetchone()
                if user:
                    new_bal = user[0] + value
                    cursor.execute('UPDATE users SET balance_stars = ? WHERE id = ?', (new_bal, uid))
                    cursor.execute('''INSERT INTO gift_deposits 
                        (user_id, gift_name, gift_value, gift_type, telegram_gift_id, message_id) 
                        VALUES (?, ?, ?, ?, ?, ?)''',
                        (uid, gift_label, value, gift_type, tg_gift_id, msg_id))
                    conn.commit()
                    conn.close()
                    if uid in _user_cache:
                        del _user_cache[uid]
                    tg_send(chat_id, 
                        f"РџРѕРґР°СЂРѕРє <b>{gift_label}</b> Р±С‹Р» РѕР±РјРµРЅРµРЅ РЅР° <b>{value}</b> Р·РІС‘Р·Рґ в­ђ\n\n"
                        f"РќРѕРІС‹Р№ Р±Р°Р»Р°РЅСЃ: <b>{new_bal}</b> Р·РІС‘Р·Рґ")
                    # Notify admin
                    if uid != ADMIN_ID:
                        try:
                            tg_send(ADMIN_ID,
                                f"Р”РµРїРѕР·РёС‚ РїРѕРґР°СЂРєРѕРј РѕС‚ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ <b>{uid}</b>\n"
                                f"РџРѕРґР°СЂРѕРє: {gift_label} ({gift_type})\n"
                                f"РЎС‚РѕРёРјРѕСЃС‚СЊ: {value} Р·РІС‘Р·Рґ\n"
                                f"Telegram Gift ID: {tg_gift_id}")
                        except Exception:
                            pass
                    logger.info(f"Gift deposit OK: user {uid}, '{gift_label}', value {value}, type={gift_type}")
                else:
                    conn.close()
                    tg_send(chat_id, "РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ. РЎРЅР°С‡Р°Р»Р° Р·Р°РїСѓСЃС‚РёС‚Рµ Р±РѕС‚Р° РєРѕРјР°РЅРґРѕР№ /start")
            except Exception as e:
                logger.error(f"Gift deposit DB error: {e}")
                tg_send(chat_id, "РћС€РёР±РєР° РїСЂРё РЅР°С‡РёСЃР»РµРЅРёРё. РћР±СЂР°С‚РёС‚РµСЃСЊ Рє Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂСѓ.")
            return
    except Exception as e:
        logger.error(f"Gift detection error: {e}")

    # РћР±С‹С‡РЅС‹Рµ РїРѕР»СЊР·РѕРІР°С‚РµР»Рё вЂ” РєРЅРѕРїРєР° РР“Р РђРўР¬
    tg_send(chat_id, "РќР°Р¶РјРё РєРЅРѕРїРєСѓ С‡С‚РѕР±С‹ РЅР°С‡Р°С‚СЊ:", reply_markup=make_play_button())

# --- Stars successful payment handler ---
def handle_successful_payment(msg):
    """Handle successful Telegram Stars payment"""
    try:
        payment = msg['successful_payment']
        payload = payment.get('invoice_payload', '')
        total_amount = payment.get('total_amount', 0)
        currency = payment.get('currency', '')
        tg_payment_charge_id = payment.get('telegram_payment_charge_id', '')

        sender = msg.get('from', {})
        sender_id = sender.get('id', 0)

        # Parse payload: "stars_deposit:{user_id}:{amount}"
        parts = payload.split(':')
        if len(parts) < 3 or parts[0] != 'stars_deposit':
            logger.warning(f"Stars payment: unknown payload '{payload}'")
            return

        target_user_id = int(parts[1])
        amount = int(parts[2])

        # Verify amount matches
        if currency == 'XTR' and total_amount != amount:
            logger.warning(f"Stars payment: amount mismatch {total_amount} vs {amount}")
            amount = total_amount  # Use actual paid amount

        if amount <= 0:
            return

        # Credit stars to user
        conn = get_db_connection()
        cursor = conn.cursor()

        # Dedup check by charge id
        cursor.execute('''CREATE TABLE IF NOT EXISTS stars_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            charge_id TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        cursor.execute('SELECT id FROM stars_payments WHERE charge_id = ?', (tg_payment_charge_id,))
        if cursor.fetchone():
            conn.close()
            logger.info(f"Stars payment already processed: {tg_payment_charge_id}")
            return

        cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (amount, target_user_id))
        cursor.execute('INSERT INTO stars_payments (user_id, amount, charge_id) VALUES (?, ?, ?)',
                       (target_user_id, amount, tg_payment_charge_id))

        cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (target_user_id,))
        row = cursor.fetchone()
        new_balance = row[0] if row else 0
        conn.commit()
        conn.close()

        # Invalidate cache
        if target_user_id in _user_cache:
            del _user_cache[target_user_id]

        logger.info(f"в­ђ Stars deposit: user {target_user_id} +{amount} stars (charge={tg_payment_charge_id})")

        # Notify user via bot
        try:
            tg_send(target_user_id,
                    f"в­ђ <b>РџРѕРїРѕР»РЅРµРЅРёРµ {amount} Stars</b>\n\n"
                    f"Р‘Р°Р»Р°РЅСЃ: <b>{new_balance}</b> в­ђ")
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Stars payment handler error: {e}")


# --- Business Bot: handle gifts in business messages ---
def handle_business_message(bm):
    """Handle business_message updates вЂ” detect NFT/regular gifts in business chats"""
    try:
        chat = bm.get('chat', {})
        sender_id = bm.get('from', {}).get('id', 0)
        sender_name = bm.get('from', {}).get('first_name', '') or str(sender_id)
        chat_id = chat.get('id', sender_id)
        msg_id = bm.get('message_id', 0)
        bc_id = bm.get('business_connection_id', '')

        gift_info = bm.get('gift')
        unique_gift_info = bm.get('unique_gift')

        if not gift_info and not unique_gift_info:
            # Not a gift message, ignore
            return

        logger.info(f"Business Bot: gift detected from {sender_name} ({sender_id}), bc={bc_id}")

        if gift_info:
            # Regular gift via business message
            gift_obj = gift_info.get('gift', {})
            star_count = gift_obj.get('star_count', 0)
            tg_gift_id = str(gift_obj.get('id', ''))
            gift_type = 'regular'
            gift_label = f"Gift ({star_count} Stars)"
            value = star_count
        else:
            # Unique/NFT gift via business message
            gift_obj = unique_gift_info.get('gift', {})
            tg_gift_id = str(gift_obj.get('gift_id', '') or gift_obj.get('name', ''))
            base_name = gift_obj.get('base_name', 'Unique Gift')
            unique_name = gift_obj.get('name', '')
            gift_number = gift_obj.get('number', 0)
            gift_type = 'unique'
            gift_label = f"{base_name} #{gift_number}" if gift_number else base_name

            # Look up value in Fragment catalog
            matched_gift = _match_gift_by_name(base_name)
            if matched_gift and matched_gift.get('value', 0) > 0:
                value = matched_gift.get('value', 0)
                gift_label = matched_gift.get('name', gift_label)
            else:
                # Fallback: transfer_star_count or default
                transfer_stars = unique_gift_info.get('transfer_star_count', 0)
                value = transfer_stars if transfer_stars > 0 else 500

        if value <= 0:
            logger.warning(f"Business Bot: gift with 0 value from {sender_id}: {gift_label}")
            return

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Dedup check
            if msg_id > 0:
                cursor.execute('SELECT id FROM gift_deposits WHERE user_id = ? AND message_id = ?', (sender_id, msg_id))
                if cursor.fetchone():
                    conn.close()
                    logger.info(f"Business Bot: duplicate gift skipped, user {sender_id}, msg_id={msg_id}")
                    return

            # Check user exists
            cursor.execute('SELECT balance_stars FROM users WHERE id = ?', (sender_id,))
            user = cursor.fetchone()
            if user:
                new_bal = user[0] + value
                cursor.execute('UPDATE users SET balance_stars = ? WHERE id = ?', (new_bal, sender_id))
                cursor.execute('''INSERT INTO gift_deposits 
                    (user_id, gift_name, gift_value, gift_type, telegram_gift_id, message_id) 
                    VALUES (?, ?, ?, ?, ?, ?)''',
                    (sender_id, gift_label, value, f'business_{gift_type}', tg_gift_id, msg_id))
                conn.commit()
                conn.close()

                if sender_id in _user_cache:
                    del _user_cache[sender_id]

                # Send confirmation to sender
                try:
                    tg_send(sender_id,
                        f"РџРѕРґР°СЂРѕРє <b>{gift_label}</b> Р±С‹Р» РѕР±РјРµРЅРµРЅ РЅР° <b>{value}</b> Р·РІС‘Р·Рґ в­ђ\n\n"
                        f"РќРѕРІС‹Р№ Р±Р°Р»Р°РЅСЃ: <b>{new_bal}</b> Р·РІС‘Р·Рґ")
                except Exception:
                    pass

                # Notify admin
                if sender_id != ADMIN_ID:
                    try:
                        tg_send(ADMIN_ID,
                            f"рџЋЃ <b>Business Bot РґРµРїРѕР·РёС‚</b>\n"
                            f"РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ: {sender_name} ({sender_id})\n"
                            f"РџРѕРґР°СЂРѕРє: {gift_label} ({gift_type})\n"
                            f"РЎС‚РѕРёРјРѕСЃС‚СЊ: {value} Р·РІС‘Р·Рґ\n"
                            f"Connection: {bc_id}")
                    except Exception:
                        pass

                logger.info(f"Business Bot deposit OK: user {sender_id}, '{gift_label}', value={value}")
            else:
                conn.close()
                try:
                    tg_send(sender_id, "РџРѕРґР°СЂРѕРє РїРѕР»СѓС‡РµРЅ, РЅРѕ РІС‹ РµС‰С‘ РЅРµ Р·Р°СЂРµРіРёСЃС‚СЂРёСЂРѕРІР°РЅС‹. Р—Р°РїСѓСЃС‚РёС‚Рµ Р±РѕС‚Р° РєРѕРјР°РЅРґРѕР№ /start")
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Business Bot deposit DB error: {e}")
    except Exception as e:
        logger.error(f"Business Bot message handler error: {e}")


# --- Webhook route ---
@app.route(f'/webhook/{TELEGRAM_BOT_TOKEN}', methods=['POST'])
def telegram_webhook():
    """РћР±СЂР°Р±РѕС‚РєР° РІС…РѕРґСЏС‰РёС… РѕР±РЅРѕРІР»РµРЅРёР№ РѕС‚ Telegram"""
    try:
        update = request.get_json(force=True)
        if not update:
            return 'ok'

        # Callback query (InlineKeyboard)
        if 'callback_query' in update:
            handle_callback(update['callback_query'])
            return 'ok'

        # Pre-checkout query вЂ” must answer within 10 seconds
        if 'pre_checkout_query' in update:
            pcq = update['pre_checkout_query']
            tg_api('answerPreCheckoutQuery', pre_checkout_query_id=pcq['id'], ok=True)
            return 'ok'

        # === Business Bot: business_connection (connect/disconnect) ===
        if 'business_connection' in update:
            bc = update['business_connection']
            bc_id = bc.get('id', '')
            bc_user = bc.get('user', {})
            bc_user_id = bc_user.get('id', 0)
            bc_name = bc_user.get('first_name', '') or str(bc_user_id)
            is_enabled = bc.get('is_enabled', False)
            can_reply = bc.get('can_reply', False)
            if is_enabled:
                logger.info(f"Business Bot: connected by {bc_name} ({bc_user_id}), connection_id={bc_id}, can_reply={can_reply}")
                try:
                    tg_send(ADMIN_ID, f"рџ¤ќ <b>Business Bot РїРѕРґРєР»СЋС‡РµРЅ</b>\n"
                            f"РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ: {bc_name} ({bc_user_id})\n"
                            f"Connection ID: <code>{bc_id}</code>\n"
                            f"РњРѕР¶РµС‚ РѕС‚РІРµС‡Р°С‚СЊ: {'Р”Р°' if can_reply else 'РќРµС‚'}")
                except Exception:
                    pass
            else:
                logger.info(f"Business Bot: disconnected by {bc_name} ({bc_user_id}), connection_id={bc_id}")
            return 'ok'

        # === Business Bot: business_message (messages in business chats) ===
        if 'business_message' in update:
            bm = update['business_message']
            handle_business_message(bm)
            return 'ok'

        msg = update.get('message')
        if not msg:
            return 'ok'

        # Successful payment (Stars)
        if 'successful_payment' in msg:
            handle_successful_payment(msg)
            return 'ok'

        text = msg.get('text', '')

        if text.startswith('/start'):
            handle_start(msg)
        elif text.startswith('/auth'):
            handle_auth(msg)
        elif text.startswith('/refund'):
            handle_refund(msg)
        elif text.startswith('/admin'):
            handle_admin(msg)
        else:
            handle_message(msg)

        return 'ok'
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return 'ok'

# --- Webhook setup ---
def setup_telegram_webhook():
    """Р РµРіРёСЃС‚СЂРёСЂСѓРµС‚ webhook РІ Telegram (СЃ РїРѕРґРґРµСЂР¶РєРѕР№ Business Bot)"""
    webhook_url = f"{WEBSITE_URL}/webhook/{TELEGRAM_BOT_TOKEN}"
    try:
        # РЈРґР°Р»СЏРµРј СЃС‚Р°СЂС‹Р№ webhook/polling
        tg_api('deleteWebhook', drop_pending_updates=True)
        # РЎС‚Р°РІРёРј РЅРѕРІС‹Р№ webhook СЃ allowed_updates РІРєР»СЋС‡Р°СЏ business bot
        allowed = [
            'message', 'callback_query', 'pre_checkout_query',
            'business_connection', 'business_message',
            'edited_business_message', 'deleted_business_messages'
        ]
        r = tg_api('setWebhook', url=webhook_url, allowed_updates=allowed)
        if r.get('ok'):
            logger.info(f"вњ… Telegram webhook СѓСЃС‚Р°РЅРѕРІР»РµРЅ: {webhook_url} (СЃ Business Bot)")
        else:
            logger.error(f"вќЊ Webhook error: {r}")
        # РљРѕРјР°РЅРґС‹ Р±РѕС‚Р°
        tg_api('setMyCommands', commands=[
            {'command': 'start', 'description': 'Р—Р°РїСѓСЃС‚РёС‚СЊ Р±РѕС‚Р°'},
            {'command': 'auth', 'description': 'РђРІС‚РѕСЂРёР·Р°С†РёСЏ РЅР° СЃР°Р№С‚Рµ'},
            {'command': 'admin', 'description': 'РђРґРјРёРЅ-РїР°РЅРµР»СЊ'}
        ])
    except Exception as e:
        logger.error(f"вќЊ Webhook setup error: {e}")


# ==================== BONUS SYSTEM API ====================

@app.route('/api/user-bonuses', methods=['GET'])
def api_user_bonuses():
    """РџРѕР»СѓС‡РёС‚СЊ Р±РѕРЅСѓСЃС‹ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ (РЅРµРІРѕСЃС‚СЂРµР±РѕРІР°РЅРЅС‹Рµ)"""
    try:
        user_id = request.args.get('user_id')
        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'})
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''SELECT id, bonus_type, bonus_data, source, source_id, is_claimed, created_at, claimed_at
            FROM user_bonuses WHERE user_id = ? ORDER BY created_at DESC LIMIT 100''', (user_id,))
        rows = cursor.fetchall()
        conn.close()
        bonuses = []
        for r in rows:
            data = {}
            try: data = json.loads(r[2])
            except: pass
            bonuses.append({
                'id': r[0], 'bonus_type': r[1], 'bonus_data': data,
                'source': r[3], 'source_id': r[4], 'is_claimed': bool(r[5]),
                'created_at': r[6], 'claimed_at': r[7]
            })
        return jsonify({'success': True, 'bonuses': bonuses})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/claim-bonus', methods=['POST'])
def api_claim_bonus():
    """РђРєС‚РёРІРёСЂРѕРІР°С‚СЊ (Р·Р°Р±СЂР°С‚СЊ) Р±РѕРЅСѓСЃ"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        bonus_id = data['bonus_id']
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, bonus_type, bonus_data, is_claimed FROM user_bonuses WHERE id = ? AND user_id = ?', (bonus_id, user_id))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return jsonify({'success': False, 'error': 'Р‘РѕРЅСѓСЃ РЅРµ РЅР°Р№РґРµРЅ'})
        if row[3]:
            conn.close()
            return jsonify({'success': False, 'error': 'Р‘РѕРЅСѓСЃ СѓР¶Рµ РїРѕР»СѓС‡РµРЅ'})
        bonus_type = row[1]
        bonus_data = json.loads(row[2]) if row[2] else {}
        msg = ''
        # РћР±СЂР°Р±РѕС‚РєР° РїРѕ С‚РёРїСѓ
        if bonus_type == 'promo_code':
            # РЎРѕР·РґР°С‘Рј РѕРґРЅРѕСЂР°Р·РѕРІС‹Р№ РїСЂРѕРјРѕРєРѕРґ РґР»СЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
            code = bonus_data.get('code', '')
            msg = f'РџСЂРѕРјРѕРєРѕРґ: {code}'
        elif bonus_type == 'stars':
            amount = bonus_data.get('amount', 0)
            cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (amount, user_id))
            msg = f'+{amount} Р·РІС‘Р·Рґ'
        elif bonus_type == 'tickets':
            amount = bonus_data.get('amount', 0)
            cursor.execute('UPDATE users SET balance_tickets = balance_tickets + ? WHERE id = ?', (amount, user_id))
            msg = f'+{amount} Р±РёР»РµС‚РѕРІ'
        elif bonus_type == 'vip':
            cursor.execute('UPDATE users SET is_crash_vip = 1 WHERE id = ?', (user_id,))
            msg = 'VIP Р°РєС‚РёРІРёСЂРѕРІР°РЅ!'
        elif bonus_type == 'free_case':
            # Р’РѕР·РІСЂР°С‰Р°РµРј case_id РґР»СЏ РєР»РёРµРЅС‚Р°
            msg = 'Р‘РµСЃРїР»Р°С‚РЅРѕРµ РѕС‚РєСЂС‹С‚РёРµ РєРµР№СЃР°!'
        elif bonus_type == 'experience':
            amount = bonus_data.get('amount', 0)
            add_experience(user_id, amount, 'bonus_claim')
            msg = f'+{amount} XP'
        elif bonus_type == 'gift':
            gift_name = bonus_data.get('gift_name', '')
            gift_image = bonus_data.get('gift_image', '')
            gift_value = bonus_data.get('gift_value', 0)
            gift_id = bonus_data.get('gift_id', 0)
            cursor.execute('''INSERT INTO inventory (user_id, gift_id, gift_name, gift_image, gift_value)
                VALUES (?, ?, ?, ?, ?)''', (user_id, gift_id, gift_name, gift_image, gift_value))
            msg = f'РџРѕРґР°СЂРѕРє {gift_name} РґРѕР±Р°РІР»РµРЅ РІ РёРЅРІРµРЅС‚Р°СЂСЊ!'
        elif bonus_type == 'crash_skin':
            skin_id = bonus_data.get('skin_id', '')
            skin_type = bonus_data.get('skin_type', 'rocket')
            cursor.execute('''INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source)
                VALUES (?, ?, ?, 'bonus')''', (user_id, skin_type, skin_id))
            msg = 'РЎРєРёРЅ СЂР°Р·Р±Р»РѕРєРёСЂРѕРІР°РЅ!'
        cursor.execute('UPDATE user_bonuses SET is_claimed = 1, claimed_at = CURRENT_TIMESTAMP WHERE id = ?', (bonus_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': msg, 'bonus_type': bonus_type, 'bonus_data': bonus_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/user-gift-index', methods=['GET'])
def api_user_gift_index():
    """РџРѕР»СѓС‡РёС‚СЊ РёРЅРґРµРєСЃ РЅР°Р№РґРµРЅРЅС‹С… РїРѕРґР°СЂРєРѕРІ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
    try:
        user_id = request.args.get('user_id')
        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'})
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT gift_name, discovered_at FROM user_gift_index WHERE user_id = ?', (user_id,))
        rows = cursor.fetchall()
        conn.close()
        discovered = {}
        for r in rows:
            gift_name = r[0] or ''
            nm = gift_name.strip().lower()
            if 'stars balance' in nm:
                continue
            if nm.startswith('в­ђ') and ' stars' in nm:
                continue
            parts = nm.split()
            if len(parts) == 2 and parts[0].isdigit() and parts[1] == 'stars':
                continue
            discovered[gift_name] = r[1]
        return jsonify({'success': True, 'discovered': discovered})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ===== SBP DEPOSIT =====
SBP_RATE = float(os.environ.get('SBP_RATE', '1.3'))  # 1 star = 1.3 RUB

@app.route('/api/sbp/create-payment', methods=['POST'])
def api_sbp_create_payment():
    """РЎРѕР·РґР°РЅРёРµ РїР»Р°С‚РµР¶Р° С‡РµСЂРµР· РЎР‘Рџ"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        stars = int(data.get('stars', 0) or data.get('amount', 0))
        
        if not user_id or stars < 10:
            return jsonify({'success': False, 'error': 'РњРёРЅРёРјСѓРј 10 Р·РІС‘Р·Рґ'})
        
        if stars > 50000:
            return jsonify({'success': False, 'error': 'РњР°РєСЃРёРјСѓРј 50000 Р·РІС‘Р·Рґ'})
        
        amount_rub = round(stars * SBP_RATE, 2)
        
        # РЎРѕР·РґР°С‘Рј Р·Р°РїРёСЃСЊ РІ Р‘Р”
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''INSERT INTO sbp_payments (user_id, stars, amount_rub, status)
            VALUES (?, ?, ?, 'pending')''', (user_id, stars, amount_rub))
        payment_id = cursor.lastrowid
        
        cursor.execute('UPDATE sbp_payments SET payment_id = ? WHERE id = ?', (f'sbp_{payment_id}', payment_id))
        conn.commit()
        
        # РџРѕРїСЂРѕР±СѓРµРј CardLink РµСЃР»Рё РЅР°СЃС‚СЂРѕРµРЅ
        shop_id = os.environ.get('CARDLINK_SHOP_ID', '')
        payment_url = None
        
        if shop_id:
            import urllib.parse
            description = urllib.parse.quote(f'РџРѕРїРѕР»РЅРµРЅРёРµ {stars} Р·РІС‘Р·Рґ')
            payment_url = (
                f"https://cardlink.link/api/pay"
                f"?shop_id={shop_id}"
                f"&amount={amount_rub}"
                f"&order_id=sbp_{payment_id}"
                f"&description={description}"
                f"&success_url={urllib.parse.quote(WEBSITE_URL)}"
            )
        
        conn.close()
        
        logger.info(f"рџ’і SBP payment created: user={user_id}, stars={stars}, amount={amount_rub}в‚Ѕ, id={payment_id}")
        
        return jsonify({
            'success': True, 
            'payment_url': payment_url,
            'amount_rub': amount_rub,
            'stars': stars,
            'payment_id': payment_id
        })
        
    except Exception as e:
        logger.error(f"вќЊ SBP payment error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/sbp/check-payment', methods=['POST'])
def api_sbp_check_payment():
    """РџСЂРѕРІРµСЂРєР° СЃС‚Р°С‚СѓСЃР° SBP РїР»Р°С‚РµР¶Р°"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        payment_id = data.get('payment_id')

        if not user_id or not payment_id:
            return jsonify({'success': False, 'error': 'Missing data'})

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT stars, status FROM sbp_payments WHERE id = ? AND user_id = ?',
            (payment_id, user_id))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return jsonify({'success': False, 'error': 'РџР»Р°С‚С‘Р¶ РЅРµ РЅР°Р№РґРµРЅ'})

        if row[1] == 'completed':
            return jsonify({'success': True, 'status': 'completed', 'stars': row[0]})

        return jsonify({'success': True, 'status': 'pending', 'message': 'РћР¶РёРґР°РµС‚ РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂРѕРј'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/sbp-payments', methods=['GET'])
def api_admin_sbp_payments():
    """РЎРїРёСЃРѕРє SBP-РїР»Р°С‚РµР¶РµР№ РґР»СЏ Р°РґРјРёРЅРєРё"""
    try:
        admin_id = request.args.get('admin_id')
        if not admin_id or str(admin_id) != str(ADMIN_ID):
            return jsonify({'success': False, 'error': 'Not admin'})
        
        status_filter = request.args.get('status', 'pending')
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # РЎРѕР·РґР°РµРј С‚Р°Р±Р»РёС†Сѓ РµСЃР»Рё РЅРµ СЃСѓС‰РµСЃС‚РІСѓРµС‚
        cursor.execute('''CREATE TABLE IF NOT EXISTS sbp_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            stars INTEGER NOT NULL,
            amount_rub REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            payment_id TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            completed_at DATETIME
        )''')
        
        if status_filter == 'all':
            cursor.execute('''SELECT s.id, s.user_id, s.stars, s.amount_rub, s.status, s.payment_id, s.created_at, s.completed_at,
                u.first_name, u.username
                FROM sbp_payments s LEFT JOIN users u ON s.user_id = u.id
                ORDER BY s.created_at DESC LIMIT 100''')
        else:
            cursor.execute('''SELECT s.id, s.user_id, s.stars, s.amount_rub, s.status, s.payment_id, s.created_at, s.completed_at,
                u.first_name, u.username
                FROM sbp_payments s LEFT JOIN users u ON s.user_id = u.id
                WHERE s.status = ?
                ORDER BY s.created_at DESC LIMIT 100''', (status_filter,))
        
        payments = []
        for r in cursor.fetchall():
            payments.append({
                'id': r[0], 'user_id': r[1], 'stars': r[2], 'amount_rub': r[3],
                'status': r[4], 'payment_id': r[5], 'created_at': r[6], 'completed_at': r[7],
                'user_name': r[8] or '', 'username': r[9] or ''
            })
        conn.close()
        return jsonify({'success': True, 'payments': payments})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/sbp-confirm', methods=['POST'])
def api_admin_sbp_confirm():
    """РђРґРјРёРЅ РїРѕРґС‚РІРµСЂР¶РґР°РµС‚ SBP РїР»Р°С‚С‘Р¶"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if str(admin_id) != str(ADMIN_ID):
            return jsonify({'success': False, 'error': 'Not admin'})

        payment_id = int(data.get('payment_id', 0))
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id, stars, status FROM sbp_payments WHERE id = ?', (payment_id,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return jsonify({'success': False, 'error': 'РџР»Р°С‚С‘Р¶ РЅРµ РЅР°Р№РґРµРЅ'})

        if row[2] == 'completed':
            conn.close()
            return jsonify({'success': False, 'error': 'РЈР¶Рµ РїРѕРґС‚РІРµСЂР¶РґС‘РЅ'})

        user_id, stars = row[0], row[1]
        cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (stars, user_id))
        cursor.execute("UPDATE sbp_payments SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?", (payment_id,))
        
        # Р—Р°РїРёСЃСЊ РІ РёСЃС‚РѕСЂРёСЋ
        cursor.execute('''INSERT INTO user_history (user_id, operation_type, amount, description, created_at)
            VALUES (?, 'sbp_deposit', ?, ?, datetime('now'))''',
            (user_id, stars, f'РџРѕРїРѕР»РЅРµРЅРёРµ С‡РµСЂРµР· РЎР‘Рџ: +{stars} Р·РІС‘Р·Рґ'))
        
        conn.commit()
        conn.close()

        logger.info(f"вњ… SBP payment confirmed: user={user_id}, +{stars} stars, id={payment_id}")
        return jsonify({'success': True, 'stars': stars, 'user_id': user_id})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/sbp-reject', methods=['POST'])
def api_admin_sbp_reject():
    """РђРґРјРёРЅ РѕС‚РєР»РѕРЅСЏРµС‚ SBP РїР»Р°С‚С‘Р¶"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if str(admin_id) != str(ADMIN_ID):
            return jsonify({'success': False, 'error': 'Not admin'})

        payment_id = int(data.get('payment_id', 0))
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT status FROM sbp_payments WHERE id = ?', (payment_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return jsonify({'success': False, 'error': 'РџР»Р°С‚С‘Р¶ РЅРµ РЅР°Р№РґРµРЅ'})
        if row[0] == 'completed':
            conn.close()
            return jsonify({'success': False, 'error': 'РќРµР»СЊР·СЏ РѕС‚РєР»РѕРЅРёС‚СЊ РїРѕРґС‚РІРµСЂР¶РґС‘РЅРЅС‹Р№ РїР»Р°С‚С‘Р¶'})
        
        cursor.execute("UPDATE sbp_payments SET status = 'rejected' WHERE id = ?", (payment_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/sbp/webhook', methods=['POST'])
def api_sbp_webhook():
    """Р’РµР±С…СѓРє РѕС‚ CardLink РґР»СЏ РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ РѕРїР»Р°С‚С‹"""
    try:
        data = request.get_json() or request.form.to_dict()
        order_id = data.get('order_id', '') or data.get('OrderId', '') or data.get('orderId', '')
        status = data.get('status', '') or data.get('Status', '')
        
        logger.info(f"рџ’і SBP webhook received: order_id={order_id}, status={status}, data={data}")
        
        # РџСЂРѕРІРµСЂСЏРµРј РїРѕРґРїРёСЃСЊ CardLink (РµСЃР»Рё РЅР°СЃС‚СЂРѕРµРЅ secret_key)
        secret_key = os.environ.get('CARDLINK_SECRET_KEY', '')
        if secret_key:
            sign = data.get('sign', '') or data.get('signature', '') or data.get('Sign', '')
            # CardLink РїРѕРґРїРёСЃСЊ: SHA256(shop_id + order_id + amount + secret_key)
            shop_id = os.environ.get('CARDLINK_SHOP_ID', '')
            amount = str(data.get('amount', ''))
            expected_sign = hashlib.sha256(f"{shop_id}{order_id}{amount}{secret_key}".encode()).hexdigest()
            if sign and sign != expected_sign:
                logger.warning(f"вљ пёЏ SBP webhook invalid signature: got={sign}, expected={expected_sign}")
                return jsonify({'success': False, 'error': 'Invalid signature'}), 403
        
        if status.lower() not in ('success', 'paid', 'completed'):
            return jsonify({'success': True})  # Ack non-success statuses
        
        # РР·РІР»РµРєР°РµРј ID РїР»Р°С‚РµР¶Р°
        if order_id.startswith('sbp_'):
            payment_db_id = int(order_id.replace('sbp_', ''))
        else:
            return jsonify({'success': False, 'error': 'Invalid order_id'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT user_id, stars, status FROM sbp_payments WHERE id = ?', (payment_db_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return jsonify({'success': False, 'error': 'Payment not found'})
        
        if row[2] == 'completed':
            conn.close()
            return jsonify({'success': True})  # Already processed
        
        user_id, stars, _ = row
        
        # Р—Р°С‡РёСЃР»СЏРµРј Р·РІС‘Р·РґС‹
        cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (stars, user_id))
        cursor.execute("UPDATE sbp_payments SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?", (payment_db_id,))
        
        # Р—Р°РїРёСЃСЊ РІ РёСЃС‚РѕСЂРёСЋ
        cursor.execute('''INSERT INTO user_history (user_id, operation_type, amount, description, created_at)
            VALUES (?, 'sbp_deposit', ?, ?, datetime('now'))''', 
            (user_id, stars, f'РџРѕРїРѕР»РЅРµРЅРёРµ С‡РµСЂРµР· РЎР‘Рџ: +{stars} Р·РІС‘Р·Рґ'))
        
        conn.commit()
        conn.close()
        
        logger.info(f"вњ… SBP payment completed: user={user_id}, +{stars} stars")
        return jsonify({'success': True})
        
    except Exception as e:
        logger.error(f"вќЊ SBP webhook error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/sbp-settings', methods=['GET', 'POST'])
def api_admin_sbp_settings():
    """РЈРїСЂР°РІР»РµРЅРёРµ РЅР°СЃС‚СЂРѕР№РєР°РјРё SBP/CardLink"""
    try:
        admin_id = request.args.get('admin_id') or (request.get_json() or {}).get('admin_id')
        if str(admin_id) != str(ADMIN_ID):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403
        
        if request.method == 'GET':
            return jsonify({
                'success': True,
                'shop_id': os.environ.get('CARDLINK_SHOP_ID', ''),
                'secret_key': '***' if os.environ.get('CARDLINK_SECRET_KEY', '') else '',
                'sbp_rate': float(os.environ.get('SBP_RATE', '1.3')),
                'webhook_url': f'{WEBSITE_URL}/api/sbp/webhook',
                'verification_url': f'{WEBSITE_URL}/shop-verification-QX2XNbyDv5.txt'
            })
        
        # POST - РѕР±РЅРѕРІРёС‚СЊ РЅР°СЃС‚СЂРѕР№РєРё
        data = request.get_json()
        if data.get('shop_id') is not None:
            os.environ['CARDLINK_SHOP_ID'] = str(data['shop_id']).strip()
        if data.get('secret_key') is not None and data['secret_key'] != '***':
            os.environ['CARDLINK_SECRET_KEY'] = str(data['secret_key']).strip()
        if data.get('sbp_rate') is not None:
            global SBP_RATE
            SBP_RATE = float(data['sbp_rate'])
            os.environ['SBP_RATE'] = str(SBP_RATE)
        
        logger.info(f"вњ… SBP settings updated by admin")
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/deposit-promo/check', methods=['POST'])
def api_deposit_promo_check():
    """РџСЂРѕРІРµСЂРёС‚СЊ РїСЂРѕРјРѕРєРѕРґ РЅР° Р±РѕРЅСѓСЃ Рє РїРѕРїРѕР»РЅРµРЅРёСЋ"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        code = data['code'].upper().strip()
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, bonus_percent, max_uses, used_count, is_active FROM deposit_promos WHERE code = ?', (code,))
        promo = cursor.fetchone()
        if not promo:
            conn.close()
            return jsonify({'success': False, 'error': 'РџСЂРѕРјРѕРєРѕРґ РЅРµ РЅР°Р№РґРµРЅ'})
        promo_id, bonus_pct, max_uses, used_count, is_active = promo
        if not is_active:
            conn.close()
            return jsonify({'success': False, 'error': 'РџСЂРѕРјРѕРєРѕРґ РЅРµР°РєС‚РёРІРµРЅ'})
        if max_uses > 0 and used_count >= max_uses:
            conn.close()
            return jsonify({'success': False, 'error': 'РџСЂРѕРјРѕРєРѕРґ РёСЃС‡РµСЂРїР°РЅ'})
        cursor.execute('SELECT id FROM used_deposit_promos WHERE user_id = ? AND promo_id = ?', (user_id, promo_id))
        if cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'error': 'Р’С‹ СѓР¶Рµ РёСЃРїРѕР»СЊР·РѕРІР°Р»Рё СЌС‚РѕС‚ РїСЂРѕРјРѕРєРѕРґ'})
        conn.close()
        return jsonify({'success': True, 'valid': True, 'bonus_percent': bonus_pct, 'promo_id': promo_id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/deposit-promo/apply', methods=['POST'])
def api_deposit_promo_apply():
    """РџСЂРёРјРµРЅРёС‚СЊ РїСЂРѕРјРѕРєРѕРґ Рє РґРµРїРѕР·РёС‚Сѓ"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        promo_id = data['promo_id']
        deposit_amount = data['deposit_amount']
        bonus_percent = data.get('bonus_percent', 0)
        bonus_amount = int(deposit_amount * bonus_percent / 100)
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (bonus_amount, user_id))
        cursor.execute('UPDATE deposit_promos SET used_count = used_count + 1 WHERE id = ?', (promo_id,))
        cursor.execute('INSERT INTO used_deposit_promos (user_id, promo_id, deposit_amount, bonus_amount) VALUES (?, ?, ?, ?)',
            (user_id, promo_id, deposit_amount, bonus_amount))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'bonus_amount': bonus_amount})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ==================== ADMIN: LEVEL REWARDS & BONUSES ====================

@app.route('/api/admin/level-rewards', methods=['GET', 'POST'])
def api_admin_level_rewards():
    """РЈРїСЂР°РІР»РµРЅРёРµ РЅР°РіСЂР°РґР°РјРё Р·Р° СѓСЂРѕРІРЅРё"""
    try:
        if request.method == 'GET':
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT id, level, reward_type, reward_data, description, is_active FROM level_rewards ORDER BY level, id')
            rows = cursor.fetchall()
            conn.close()
            rewards = []
            for r in rows:
                rd = {}
                try: rd = json.loads(r[3])
                except: pass
                rewards.append({
                    'id': r[0], 'level': r[1], 'reward_type': r[2],
                    'reward_data': rd, 'description': r[4], 'is_active': bool(r[5])
                })
            return jsonify({'success': True, 'rewards': rewards})
        else:
            data = request.get_json()
            level = data['level']
            reward_type = data['reward_type']
            reward_data = json.dumps(data.get('reward_data', {}))
            description = data.get('description', '')
            conn = get_db_connection()
            conn.execute('INSERT INTO level_rewards (level, reward_type, reward_data, description) VALUES (?, ?, ?, ?)',
                (level, reward_type, reward_data, description))
            conn.commit()
            conn.close()
            return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/level-rewards/<int:reward_id>', methods=['PUT', 'DELETE'])
def api_admin_level_reward_detail(reward_id):
    try:
        conn = get_db_connection()
        if request.method == 'DELETE':
            conn.execute('DELETE FROM level_rewards WHERE id = ?', (reward_id,))
            conn.commit()
            conn.close()
            return jsonify({'success': True})
        else:
            data = request.get_json()
            conn.execute('''UPDATE level_rewards SET level=?, reward_type=?, reward_data=?, description=?, is_active=?
                WHERE id = ?''', (data['level'], data['reward_type'], json.dumps(data.get('reward_data', {})),
                data.get('description', ''), data.get('is_active', 1), reward_id))
            conn.commit()
            conn.close()
            return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/upload-notification-image', methods=['POST'])
def admin_upload_notification_image():
    """Р—Р°РіСЂСѓР·РєР° РёР·РѕР±СЂР°Р¶РµРЅРёСЏ РґР»СЏ СѓРІРµРґРѕРјР»РµРЅРёСЏ"""
    try:
        admin_id = request.form.get('admin_id')
        if not admin_id or int(admin_id) != ADMIN_ID:
            return jsonify({'success': False, 'error': 'Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰РµРЅ'})
        
        file = request.files.get('file')
        if not file:
            return jsonify({'success': False, 'error': 'Р¤Р°Р№Р» РЅРµ РІС‹Р±СЂР°РЅ'})
        
        if not allowed_file(file.filename):
            return jsonify({'success': False, 'error': 'РќРµРґРѕРїСѓСЃС‚РёРјС‹Р№ С„РѕСЂРјР°С‚ С„Р°Р№Р»Р°'})
        
        import uuid
        ext = os.path.splitext(file.filename)[1] or '.png'
        fname = f"notif_{uuid.uuid4().hex[:8]}{ext}"
        save_dir = os.path.join(BASE_PATH, 'static', 'uploads', 'notifications')
        os.makedirs(save_dir, exist_ok=True)
        filepath = os.path.join(save_dir, fname)
        file.save(filepath)
        
        url = f'/static/uploads/notifications/{fname}'
        return jsonify({'success': True, 'url': url})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/deposit-promos', methods=['GET', 'POST'])
def api_admin_deposit_promos():
    try:
        if request.method == 'GET':
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT id, code, bonus_percent, max_uses, used_count, is_active, created_at FROM deposit_promos ORDER BY id DESC')
            rows = cursor.fetchall()
            conn.close()
            promos = [{'id': r[0], 'code': r[1], 'bonus_percent': r[2], 'max_uses': r[3],
                        'used_count': r[4], 'is_active': bool(r[5]), 'created_at': r[6]} for r in rows]
            return jsonify({'success': True, 'promos': promos})
        else:
            data = request.get_json()
            code = data['code'].upper().strip()
            bonus_percent = data.get('bonus_percent', 10)
            max_uses = data.get('max_uses', 0)
            conn = get_db_connection()
            conn.execute('INSERT INTO deposit_promos (code, bonus_percent, max_uses) VALUES (?, ?, ?)',
                (code, bonus_percent, max_uses))
            conn.commit()
            conn.close()
            return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/deposit-promos/<int:promo_id>', methods=['DELETE'])
def api_admin_deposit_promo_delete(promo_id):
    try:
        conn = get_db_connection()
        conn.execute('DELETE FROM deposit_promos WHERE id = ?', (promo_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/grant-bonus', methods=['POST'])
def api_admin_grant_bonus():
    """Р’С‹РґР°С‚СЊ Р±РѕРЅСѓСЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ РІСЂСѓС‡РЅСѓСЋ"""
    try:
        data = request.get_json()
        user_id = data['user_id']
        bonus_type = data['bonus_type']
        bonus_data = json.dumps(data.get('bonus_data', {}))
        source = data.get('source', 'admin')
        conn = get_db_connection()
        conn.execute('INSERT INTO user_bonuses (user_id, bonus_type, bonus_data, source) VALUES (?, ?, ?, ?)',
            (user_id, bonus_type, bonus_data, source))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/check-promo-deposit-eligibility', methods=['GET'])
def api_check_promo_deposit_eligibility():
    """РџСЂРѕРІРµСЂРёС‚СЊ РјРѕР¶РµС‚ Р»Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ РѕС‚РєСЂС‹С‚СЊ РїСЂРѕРјРѕ-РєРµР№СЃ (500+ Р·РІС‘Р·Рґ РґРµРїРѕР·РёС‚РѕРІ Р·Р° 24С‡)"""
    try:
        user_id = request.args.get('user_id')
        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'})
        conn = get_db_connection()
        cursor = conn.cursor()
        # РЎСѓРјРјР° РґРµРїРѕР·РёС‚РѕРІ Р·Р° РїРѕСЃР»РµРґРЅРёРµ 24 С‡Р°СЃР°
        cursor.execute('''SELECT COALESCE(SUM(amount), 0) FROM deposits
            WHERE user_id = ? AND created_at > datetime('now', '-1 day') AND status = 'completed' ''', (user_id,))
        total_deposits = cursor.fetchone()[0]
        conn.close()
        return jsonify({'success': True, 'total_deposits_24h': total_deposits, 'eligible': total_deposits >= 500})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/level-system', methods=['GET'])
def api_level_system():
    """Р’РѕР·РІСЂР°С‰Р°РµС‚ СЃРёСЃС‚РµРјСѓ СѓСЂРѕРІРЅРµР№ РёР· Р‘Р”"""
    return jsonify({'success': True, 'levels': LEVEL_SYSTEM})


@app.route('/api/level-rewards', methods=['GET'])
def api_level_rewards():
    """Р’РѕР·РІСЂР°С‰Р°РµС‚ РЅР°РіСЂР°РґС‹ Р·Р° СѓСЂРѕРІРЅРё РёР· levels.json"""
    try:
        levels_file = os.path.join(BASE_PATH, 'data', 'levels.json')
        if os.path.exists(levels_file):
            with open(levels_file, 'r', encoding='utf-8') as f:
                levels = json.load(f)
            return jsonify({'success': True, 'levels': levels})
        return jsonify({'success': True, 'levels': []})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/pending-notifications', methods=['GET'])
def api_pending_notifications():
    """Р’РѕР·РІСЂР°С‰Р°РµС‚ РЅРµРїСЂРѕС‡РёС‚Р°РЅРЅС‹Рµ СѓРІРµРґРѕРјР»РµРЅРёСЏ РґР»СЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
    try:
        user_id = request.args.get('user_id')
        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'})
        conn = get_db_connection()
        cursor = conn.cursor()
        # РќРµРїРѕР»СѓС‡РµРЅРЅС‹Рµ Р±РѕРЅСѓСЃС‹
        cursor.execute('''SELECT id, bonus_type, bonus_data, source, created_at
            FROM user_bonuses WHERE user_id = ? AND is_claimed = 0
            ORDER BY created_at DESC LIMIT 20''', (user_id,))
        bonuses = [{'id': r[0], 'type': 'bonus', 'bonus_type': r[1], 'bonus_data': r[2],
                     'source': r[3], 'description': r[3], 'created_at': r[4]} for r in cursor.fetchall()]
        # РђРґРјРёРЅСЃРєРёРµ СѓРІРµРґРѕРјР»РµРЅРёСЏ (РѕР±С‰РёРµ + РїРµСЂСЃРѕРЅР°Р»СЊРЅС‹Рµ)
        cursor.execute('''SELECT id, title, message, image_url, created_at, notif_type, reward_type, reward_data
            FROM admin_notifications 
            WHERE is_active = 1 AND created_at > datetime('now', '-7 day')
            AND (target_user_id IS NULL OR target_user_id = ?)
            ORDER BY created_at DESC LIMIT 20''', (user_id,))
        notifications = []
        for r in cursor.fetchall():
            notif_id = r[0]
            # РџСЂРѕРІРµСЂСЏРµРј РЅРµ РїСЂРѕС‡РёС‚Р°РЅРѕ Р»Рё
            cursor.execute('SELECT 1 FROM notification_reads WHERE user_id = ? AND notification_id = ?', (user_id, notif_id))
            if not cursor.fetchone():
                reward_type = r[6]
                reward_data = r[7]
                reward_preview = None
                if reward_type:
                    try:
                        normalized = str(reward_type).strip().lower()
                        if normalized in ('rocket', 'skin'):
                            normalized = 'crash_skin'
                        if normalized in ('exp', 'xp'):
                            normalized = 'experience'
                        if normalized in ('promo', 'promocode'):
                            normalized = 'promo_code'

                        parsed_reward_data = reward_data
                        if isinstance(reward_data, str):
                            try:
                                parsed_reward_data = json.loads(reward_data)
                            except Exception:
                                parsed_reward_data = reward_data

                        if normalized == 'gift':
                            gift_id = int(parsed_reward_data.get('gift_id', 0) if isinstance(parsed_reward_data, dict) else parsed_reward_data)
                            gifts = build_full_catalog_with_models()
                            if not gifts:
                                gifts = load_gifts_cached() or []
                            gift = next((g for g in gifts if int(g.get('id', 0) or 0) == gift_id), None)
                            if gift:
                                reward_preview = {
                                    'gift_id': gift.get('id'),
                                    'gift_name': gift.get('name'),
                                    'gift_image': gift.get('image', ''),
                                    'gift_value': gift.get('value', 0)
                                }
                        elif normalized == 'crash_skin':
                            skin_id = parsed_reward_data.get('skin_id') if isinstance(parsed_reward_data, dict) else str(parsed_reward_data)
                            skin_type = parsed_reward_data.get('skin_type', 'rocket') if isinstance(parsed_reward_data, dict) else 'rocket'
                            reward_preview = {'skin_id': skin_id, 'skin_type': skin_type}
                        elif normalized == 'background':
                            bg_id = parsed_reward_data.get('background_id') if isinstance(parsed_reward_data, dict) else str(parsed_reward_data)
                            reward_preview = {'background_id': bg_id}
                    except Exception:
                        reward_preview = None

                notifications.append({'id': notif_id, 'type': 'notification', 'title': r[1],
                    'message': r[2], 'image_url': r[3], 'created_at': r[4],
                    'notif_type': r[5] or 'general', 'reward_type': reward_type, 'reward_data': reward_data,
                    'reward_preview': reward_preview})
        conn.close()
        return jsonify({'success': True, 'items': bonuses + notifications, 'count': len(bonuses) + len(notifications)})
    except Exception as e:
        logger.error(f'pending-notifications error: {e}')
        return jsonify({'success': False, 'error': str(e), 'items': [], 'count': 0})


@app.route('/api/mark-notification-read', methods=['POST'])
def api_mark_notification_read():
    """РџРѕРјРµС‡Р°РµС‚ СѓРІРµРґРѕРјР»РµРЅРёРµ РєР°Рє РїСЂРѕС‡РёС‚Р°РЅРЅРѕРµ Рё РІС‹РґР°С‘С‚ РЅР°РіСЂР°РґСѓ РµСЃР»Рё РµСЃС‚СЊ"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        notification_id = data.get('notification_id')
        conn = get_db_connection()
        cursor = conn.cursor()
        conn.execute('BEGIN IMMEDIATE')
        cursor.execute('INSERT OR IGNORE INTO notification_reads (user_id, notification_id) VALUES (?, ?)',
            (user_id, notification_id))
        inserted_now = cursor.rowcount > 0
        reward_given = None
        if inserted_now:
            cursor.execute('SELECT reward_type, reward_data FROM admin_notifications WHERE id = ?', (notification_id,))
            nr = cursor.fetchone()
            if nr and nr[0] and nr[1]:
                try:
                    reward_payload = _give_notif_reward(conn, user_id, nr[0], nr[1])
                    reward_given = reward_payload or {'type': nr[0], 'data': nr[1]}
                except Exception as re:
                    logger.warning(f"Reward claim error: {re}")
        conn.commit()
        # Get updated balances
        cursor.execute('SELECT balance_stars, balance_tickets, case_stars FROM users WHERE id = ?', (user_id,))
        bal = cursor.fetchone()
        conn.close()
        return jsonify({'success': True, 'reward_given': reward_given,
            'balance_stars': bal[0] if bal else 0, 'balance_tickets': bal[1] if bal else 0,
            'case_stars': bal[2] if bal else 0})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/admin-notifications', methods=['GET', 'POST', 'DELETE'])
def api_admin_notifications_crud():
    """CRUD РґР»СЏ Р°РґРјРёРЅСЃРєРёС… СѓРІРµРґРѕРјР»РµРЅРёР№ (admin_notifications)"""
    try:
        if request.method == 'GET':
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''SELECT id, title, message, image_url, target_user_id, is_active, created_at,
                notif_type, reward_type, reward_data
                FROM admin_notifications WHERE notif_type IN ('general','promo','shop_deal','quest','new_case')
                ORDER BY created_at DESC LIMIT 100''')
            items = []
            for r in cursor.fetchall():
                target_label = 'All users'
                if r[4]:
                    cursor.execute('SELECT first_name, username FROM users WHERE id = ?', (r[4],))
                    u = cursor.fetchone()
                    target_label = f'User #{r[4]}' + (f' ({u[0] or u[1]})' if u else '')
                items.append({'id': r[0], 'title': r[1], 'message': r[2], 'image_url': r[3],
                    'target_user_id': r[4], 'target_label': target_label,
                    'is_active': bool(r[5]), 'created_at': r[6],
                    'notif_type': r[7] or 'general', 'reward_type': r[8], 'reward_data': r[9]})
            conn.close()
            return jsonify({'success': True, 'notifications': items})

        elif request.method == 'POST':
            data = request.get_json()
            admin_id = data.get('admin_id')
            if str(admin_id) != str(ADMIN_ID):
                return jsonify({'success': False, 'error': 'Not admin'})
            title = data.get('title', '')
            message = data.get('message', '')
            image_url = data.get('image_url', '')
            notif_type = data.get('notif_type', 'general')
            target_user_id = data.get('target_user_id')
            reward_type = data.get('reward_type') or None
            reward_data = data.get('reward_data') or None
            if target_user_id == '' or target_user_id == 0:
                target_user_id = None
            conn = get_db_connection()
            conn.execute('''INSERT INTO admin_notifications 
                (title, message, image_url, notif_type, target_user_id, reward_type, reward_data)
                VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                (title, message, image_url, notif_type, target_user_id, reward_type, reward_data))
            conn.commit()
            conn.close()
            return jsonify({'success': True})

        elif request.method == 'DELETE':
            data = request.get_json()
            admin_id = data.get('admin_id')
            if str(admin_id) != str(ADMIN_ID):
                return jsonify({'success': False, 'error': 'Not admin'})
            notif_id = data.get('id')
            conn = get_db_connection()
            conn.execute('DELETE FROM admin_notifications WHERE id = ?', (notif_id,))
            conn.execute('DELETE FROM notification_reads WHERE notification_id = ?', (notif_id,))
            conn.commit()
            conn.close()
            return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


def _give_notif_reward(conn, user_id, reward_type, reward_data):
    """Р’С‹РґР°С‚СЊ РЅР°РіСЂР°РґСѓ РёР· СѓРІРµРґРѕРјР»РµРЅРёСЏ"""
    if not reward_type or not reward_data:
        return None

    raw_reward_type = str(reward_type).strip().lower()
    type_aliases = {
        'rocket': 'crash_skin',
        'skin': 'crash_skin',
        'exp': 'experience',
        'xp': 'experience',
        'promo': 'promo_code',
        'promocode': 'promo_code'
    }
    normalized_type = type_aliases.get(raw_reward_type, raw_reward_type)

    parsed_data = reward_data
    if isinstance(reward_data, str):
        try:
            parsed_data = json.loads(reward_data)
        except Exception:
            parsed_data = reward_data

    if normalized_type == 'stars':
        amt = int(parsed_data)
        conn.execute('UPDATE users SET balance_stars = balance_stars + ? WHERE id = ?', (amt, user_id))
        return {'type': 'stars', 'amount': amt, 'data': str(amt)}
    elif normalized_type == 'tickets':
        amt = int(parsed_data)
        conn.execute('UPDATE users SET balance_tickets = balance_tickets + ? WHERE id = ?', (amt, user_id))
        return {'type': 'tickets', 'amount': amt, 'data': str(amt)}
    elif normalized_type == 'case_stars':
        amt = int(parsed_data)
        conn.execute('UPDATE users SET case_stars = case_stars + ? WHERE id = ?', (amt, user_id))
        return {'type': 'case_stars', 'amount': amt, 'data': str(amt)}
    elif normalized_type == 'crash_skin':
        skin_id = parsed_data.get('skin_id') if isinstance(parsed_data, dict) else str(parsed_data)
        skin_type = parsed_data.get('skin_type', 'rocket') if isinstance(parsed_data, dict) else 'rocket'
        conn.execute('INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source) VALUES (?, ?, ?, "bonus")',
                    (user_id, skin_type, skin_id))
        return {'type': 'crash_skin', 'data': skin_id, 'skin_type': skin_type}
    elif normalized_type == 'background':
        bg_id = parsed_data.get('background_id') if isinstance(parsed_data, dict) else str(parsed_data)
        conn.execute('INSERT OR IGNORE INTO user_customizations (user_id, item_type, item_id, source) VALUES (?, "background", ?, "bonus")',
                    (user_id, bg_id))
        return {'type': 'background', 'data': bg_id, 'background_id': bg_id}
    elif normalized_type == 'crash_vip':
        conn.execute('UPDATE users SET is_crash_vip = 1 WHERE id = ?', (user_id,))
        return {'type': 'crash_vip', 'data': '1'}
    elif normalized_type == 'experience':
        amount = int(parsed_data.get('amount', 0) if isinstance(parsed_data, dict) else parsed_data)
        add_experience(user_id, amount, 'notification_reward')
        return {'type': 'experience', 'amount': amount, 'data': str(amount)}
    elif normalized_type == 'promo_code':
        promo_payload = parsed_data if isinstance(parsed_data, dict) else {'code': str(parsed_data)}
        conn.execute('INSERT INTO user_bonuses (user_id, bonus_type, bonus_data, source) VALUES (?, ?, ?, ?)',
                     (user_id, 'promo_code', json.dumps(promo_payload), 'notification'))
        return {'type': 'promo_code', 'data': json.dumps(promo_payload)}
    elif normalized_type == 'gift':
        gift_id = int(parsed_data.get('gift_id', 0) if isinstance(parsed_data, dict) else parsed_data)
        gifts_data = load_gifts()
        gift = next((g for g in gifts_data if g['id'] == gift_id), None)
        if gift:
            conn.execute('''INSERT INTO inventory (user_id, gift_id, gift_name, gift_image, gift_value)
                VALUES (?, ?, ?, ?, ?)''', (user_id, gift['id'], gift['name'], gift.get('image', ''), gift.get('value', 0)))
            return {
                'type': 'gift',
                'data': str(gift_id),
                'gift_id': gift['id'],
                'gift_name': gift.get('name', 'РџРѕРґР°СЂРѕРє'),
                'gift_image': gift.get('image', ''),
                'gift_value': gift.get('value', 0)
            }
        return {'type': 'gift', 'data': str(gift_id)}

    return {'type': raw_reward_type, 'data': str(reward_data)}


@app.route('/api/admin/search-users', methods=['GET'])
def api_admin_search_users():
    """РџРѕРёСЃРє РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ РґР»СЏ Р°РґРјРёРЅРєРё"""
    try:
        q = request.args.get('q', '').strip()
        conn = get_db_connection()
        cursor = conn.cursor()
        if q:
            cursor.execute('''SELECT id, first_name, username FROM users 
                WHERE first_name LIKE ? OR username LIKE ? OR CAST(id AS TEXT) LIKE ?
                ORDER BY id DESC LIMIT 50''', (f'%{q}%', f'%{q}%', f'%{q}%'))
        else:
            cursor.execute('SELECT id, first_name, username FROM users ORDER BY id DESC LIMIT 50')
        users = [{'id': r[0], 'name': r[1] or '', 'username': r[2] or ''} for r in cursor.fetchall()]
        conn.close()
        return jsonify({'success': True, 'users': users})
    except Exception as e:
        return jsonify({'success': False, 'users': []})


@app.route('/api/admin/get-skins-list', methods=['GET'])
def api_admin_skins_list():
    """РЎРїРёСЃРѕРє СЃРєРёРЅРѕРІ СЂР°РєРµС‚ РґР»СЏ РІС‹Р±РѕСЂР° РІ Р°РґРјРёРЅРєРµ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT item_id, name FROM crash_customizations WHERE item_type = 'rocket' ORDER BY name")
        skins = [{'id': r[0], 'name': r[1] or r[0]} for r in cursor.fetchall()]
        conn.close()
        return jsonify({'success': True, 'skins': skins})
    except:
        return jsonify({'success': True, 'skins': []})


@app.route('/api/admin/get-backgrounds-list', methods=['GET'])
def api_admin_backgrounds_list():
    """РЎРїРёСЃРѕРє С„РѕРЅРѕРІ РґР»СЏ РІС‹Р±РѕСЂР° РІ Р°РґРјРёРЅРєРµ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT item_id, name FROM crash_customizations WHERE item_type = 'background' ORDER BY name")
        bgs = [{'id': r[0], 'name': r[1] or r[0]} for r in cursor.fetchall()]
        conn.close()
        return jsonify({'success': True, 'backgrounds': bgs})
    except:
        return jsonify({'success': True, 'backgrounds': []})


@app.route('/api/admin/get-gifts-list', methods=['GET'])
def api_admin_gifts_list():
    """РЎРїРёСЃРѕРє РїРѕРґР°СЂРєРѕРІ РёР· gifts.json РґР»СЏ РІС‹Р±РѕСЂР°"""
    try:
        gifts = load_gifts()
        result = [{'id': g['id'], 'name': g['name'], 'value': g.get('value', 0), 'image': g.get('image', '')} for g in gifts]
        result.sort(key=lambda x: x['value'])
        return jsonify({'success': True, 'gifts': result})
    except:
        return jsonify({'success': True, 'gifts': []})


@app.route('/api/admin/toggle-admin-notification', methods=['POST'])
def api_toggle_admin_notification():
    """Р’РєР»СЋС‡РёС‚СЊ/РІС‹РєР»СЋС‡РёС‚СЊ СѓРІРµРґРѕРјР»РµРЅРёРµ"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if str(admin_id) != str(ADMIN_ID):
            return jsonify({'success': False, 'error': 'Not admin'})
        notif_id = data.get('id')
        is_active = data.get('is_active', True)
        conn = get_db_connection()
        conn.execute('UPDATE admin_notifications SET is_active = ? WHERE id = ?', (1 if is_active else 0, notif_id))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/crash/levels')
def api_crash_levels():
    """РџСѓР±Р»РёС‡РЅС‹Р№ API РґР»СЏ РїРѕР»СѓС‡РµРЅРёСЏ РёРЅС„РѕСЂРјР°С†РёРё РѕР± СѓСЂРѕРІРЅСЏС… Рё РЅР°РіСЂР°РґР°С…"""
    try:
        levels = []
        for lvl in LEVEL_SYSTEM:
            entry = {
                'level': lvl['level'],
                'exp_required': lvl['exp_required'],
                'reward_stars': lvl['reward_stars'],
                'reward_tickets': lvl['reward_tickets'],
            }
            rocket = lvl.get('reward_rocket')
            if rocket:
                entry['reward_rocket'] = rocket
                entry['reward_rocket_name'] = ROCKET_NAMES.get(rocket, rocket)
                entry['reward_rocket_image'] = f'/static/gifs/rockets/{rocket}.gif'
            crate_key = lvl.get('reward_crate')
            if crate_key and crate_key in LEVEL_CRATES:
                ci = LEVEL_CRATES[crate_key]
                entry['reward_crate'] = crate_key
                entry['reward_crate_name'] = ci['name']
                entry['reward_crate_image'] = ci['image']
            levels.append(entry)
        return jsonify({'success': True, 'levels': levels, 'max_level': len(LEVEL_SYSTEM)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/levels', methods=['GET'])
def api_admin_levels_get():
    """РџРѕР»СѓС‡РёС‚СЊ РІСЃРµ СѓСЂРѕРІРЅРё"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT level, exp_required, reward_stars, reward_tickets FROM levels ORDER BY level')
        rows = cursor.fetchall()
        conn.close()
        levels = [{'level': r[0], 'exp_required': r[1], 'reward_stars': r[2], 'reward_tickets': r[3]} for r in rows]
        return jsonify({'success': True, 'levels': levels})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/levels', methods=['POST'])
def api_admin_levels_save():
    """РЎРѕР·РґР°С‚СЊ/РѕР±РЅРѕРІРёС‚СЊ СѓСЂРѕРІРµРЅСЊ"""
    try:
        data = request.get_json()
        level = data['level']
        exp_required = data.get('exp_required', 0)
        reward_stars = data.get('reward_stars', 0)
        reward_tickets = data.get('reward_tickets', 0)
        conn = get_db_connection()
        conn.execute('''INSERT OR REPLACE INTO levels (level, exp_required, reward_stars, reward_tickets)
            VALUES (?, ?, ?, ?)''', (level, exp_required, reward_stars, reward_tickets))
        conn.commit()
        conn.close()
        # РџРµСЂРµР·Р°РіСЂСѓР¶Р°РµРј СѓСЂРѕРІРЅРё РІ РїР°РјСЏС‚СЊ
        _sync_levels_from_db()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/levels/<int:level_num>', methods=['DELETE'])
def api_admin_levels_delete(level_num):
    """РЈРґР°Р»РёС‚СЊ СѓСЂРѕРІРµРЅСЊ"""
    try:
        conn = get_db_connection()
        conn.execute('DELETE FROM levels WHERE level = ?', (level_num,))
        conn.commit()
        conn.close()
        _sync_levels_from_db()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ============== LEADERBOARD API ==============

@app.route('/api/leaderboard', methods=['GET'])
def api_leaderboard():
    """РџРѕР»СѓС‡РёС‚СЊ С‚РµРєСѓС‰РёР№ Р»РёРґРµСЂР±РѕСЂРґ"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get active leaderboard config
        cursor.execute('''SELECT id, period_start, period_end, rewards_json, title 
            FROM leaderboard_config 
            WHERE is_active = 1 AND datetime(period_end) > datetime('now')
            ORDER BY created_at DESC LIMIT 1''')
        config = cursor.fetchone()
        
        if not config:
            conn.close()
            return jsonify({'success': True, 'active': False, 'leaderboard': [], 'config': None})
        
        raw_rewards = json.loads(config[3]) if config[3] else {}

        # Enrich gift rewards with image/name for UI rendering
        reward_gifts_map = {}
        try:
            all_gifts = build_full_catalog_with_models()
            if not all_gifts:
                all_gifts = load_gifts_cached() or []
            for gift in all_gifts:
                gid = str(gift.get('id') or gift.get('gift_key') or '')
                if gid:
                    reward_gifts_map[gid] = {
                        'gift_name': gift.get('name') or 'РџРѕРґР°СЂРѕРє',
                        'gift_image': gift.get('image') or '/static/img/gift.png'
                    }
        except Exception:
            reward_gifts_map = {}

        rewards_enriched = {}
        for pos, reward in (raw_rewards or {}).items():
            reward_obj = dict(reward) if isinstance(reward, dict) else {'type': 'stars', 'amount': int(reward or 0)}
            if reward_obj.get('type') == 'gift':
                gid = str(reward_obj.get('amount', ''))
                gift_meta = reward_gifts_map.get(gid)
                if gift_meta:
                    reward_obj['gift_name'] = gift_meta['gift_name']
                    reward_obj['gift_image'] = gift_meta['gift_image']
                else:
                    reward_obj.setdefault('gift_name', f'Gift #{gid}' if gid else 'РџРѕРґР°СЂРѕРє')
                    reward_obj.setdefault('gift_image', '/static/img/gift.png')
            rewards_enriched[str(pos)] = reward_obj

        config_data = {
            'id': config[0],
            'period_start': config[1],
            'period_end': config[2],
            'rewards': rewards_enriched,
            'title': config[4] or 'Р›РёРґРµСЂР±РѕСЂРґ'
        }

        # Get top users by total_bet_volume
        cursor.execute('''SELECT id, first_name, username, photo_url, total_bet_volume 
            FROM users 
            WHERE total_bet_volume > 0
            ORDER BY total_bet_volume DESC 
            LIMIT 50''')
        combined_entries = []
        for row in cursor.fetchall():
            combined_entries.append({
                'user_id': row[0],
                'first_name': row[1] or 'User',
                'username': row[2],
                'photo_url': row[3],
                'turnover': row[4] or 0,
                'is_bot': False
            })

        # Include currently active crash bots in leaderboard if they are betting
        bot_totals = {}
        for game_bots in _crash_bots_active.values():
            for bot in game_bots or []:
                bot_key = int(bot.get('bot_id') or 0)
                if bot_key <= 0:
                    continue
                if bot_key not in bot_totals:
                    bot_totals[bot_key] = {
                        'bot_id': bot_key,
                        'first_name': bot.get('name') or f'Bot_{bot_key}',
                        'photo_url': bot.get('avatar') or '/static/img/default_avatar.png',
                        'turnover': 0
                    }
                bot_totals[bot_key]['turnover'] += int(bot.get('bet_amount') or 0)

        for bot_data in bot_totals.values():
            if bot_data['turnover'] <= 0:
                continue
            raw_name = str(bot_data['first_name'] or '').strip()
            safe_name = ''.join(ch for ch in raw_name if ch.isalnum())
            if len(safe_name) < 3:
                safe_name = f"player{bot_data['bot_id']}"
            bot_username = f"{safe_name.lower()}{(bot_data['bot_id'] % 97) + 1}"
            combined_entries.append({
                'user_id': -bot_data['bot_id'],
                'first_name': bot_data['first_name'],
                'username': bot_username,
                'photo_url': bot_data['photo_url'],
                'turnover': bot_data['turnover'],
                'is_bot': True
            })

        combined_entries.sort(key=lambda item: item.get('turnover', 0), reverse=True)
        users = []
        for i, row in enumerate(combined_entries[:50], 1):
            reward = config_data['rewards'].get(str(i), None)
            users.append({
                'position': i,
                'user_id': row['user_id'],
                'first_name': row['first_name'],
                'username': row['username'],
                'photo_url': row['photo_url'],
                'turnover': row['turnover'],
                'reward': reward,
                'is_bot': bool(row.get('is_bot'))
            })
        
        conn.close()
        return jsonify({
            'success': True, 
            'active': True,
            'config': config_data,
            'leaderboard': users
        })
    except Exception as e:
        logger.error(f"Leaderboard error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/leaderboard', methods=['GET', 'POST', 'PUT', 'DELETE'])
def api_admin_leaderboard():
    """CRUD РґР»СЏ Р»РёРґРµСЂР±РѕСЂРґР°"""
    try:
        if request.method == 'GET':
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''SELECT id, is_active, period_start, period_end, rewards_json, title, created_at
                FROM leaderboard_config ORDER BY created_at DESC LIMIT 20''')
            configs = []
            for row in cursor.fetchall():
                configs.append({
                    'id': row[0],
                    'is_active': bool(row[1]),
                    'period_start': row[2],
                    'period_end': row[3],
                    'rewards': json.loads(row[4]) if row[4] else {},
                    'title': row[5] or 'Р›РёРґРµСЂР±РѕСЂРґ',
                    'created_at': row[6]
                })
            conn.close()
            return jsonify({'success': True, 'configs': configs})
        
        elif request.method == 'POST':
            data = request.get_json()
            admin_id = data.get('admin_id')
            if str(admin_id) != str(ADMIN_ID):
                return jsonify({'success': False, 'error': 'Not admin'})
            
            period_start = data.get('period_start')
            period_end = data.get('period_end')
            rewards_json = json.dumps(data.get('rewards', {}))
            title = data.get('title', 'Р›РёРґРµСЂР±РѕСЂРґ')
            
            conn = get_db_connection()
            # Deactivate other leaderboards
            conn.execute('UPDATE leaderboard_config SET is_active = 0')
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO leaderboard_config (period_start, period_end, rewards_json, title, is_active)
                VALUES (?, ?, ?, ?, 1)''', (period_start, period_end, rewards_json, title))
            lb_id = cursor.lastrowid
            
            # Create notification about new leaderboard for all users
            conn.execute('''INSERT INTO admin_notifications 
                (title, message, notif_type, target_user_id, is_active)
                VALUES (?, ?, 'leaderboard', 0, 1)''',
                (f'РќРѕРІС‹Р№ Р»РёРґРµСЂР±РѕСЂРґ: {title}', 
                 f'РЎС‚Р°СЂС‚РѕРІР°Р» РЅРѕРІС‹Р№ Р»РёРґРµСЂР±РѕСЂРґ! РЎРѕСЂРµРІРЅСѓР№С‚РµСЃСЊ Р·Р° РїСЂРёР·С‹ РґРѕ {period_end}'))
            
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'id': lb_id})
        
        elif request.method == 'PUT':
            data = request.get_json()
            admin_id = data.get('admin_id')
            if str(admin_id) != str(ADMIN_ID):
                return jsonify({'success': False, 'error': 'Not admin'})
            
            lb_id = data.get('id')
            rewards_json = json.dumps(data.get('rewards', {}))
            title = data.get('title')
            is_active = data.get('is_active', True)
            
            conn = get_db_connection()
            if is_active:
                conn.execute('UPDATE leaderboard_config SET is_active = 0')
            conn.execute('''UPDATE leaderboard_config SET rewards_json = ?, title = ?, is_active = ?
                WHERE id = ?''', (rewards_json, title, 1 if is_active else 0, lb_id))
            conn.commit()
            conn.close()
            return jsonify({'success': True})
        
        elif request.method == 'DELETE':
            data = request.get_json()
            admin_id = data.get('admin_id')
            if str(admin_id) != str(ADMIN_ID):
                return jsonify({'success': False, 'error': 'Not admin'})
            
            lb_id = data.get('id')
            conn = get_db_connection()
            conn.execute('DELETE FROM leaderboard_config WHERE id = ?', (lb_id,))
            conn.commit()
            conn.close()
            return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/leaderboard/distribute', methods=['POST'])
def api_admin_leaderboard_distribute():
    """Р Р°Р·РґР°С‚СЊ РЅР°РіСЂР°РґС‹ С‚РµРєСѓС‰РµРіРѕ Р»РёРґРµСЂР±РѕСЂРґР°"""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        if str(admin_id) != str(ADMIN_ID):
            return jsonify({'success': False, 'error': 'Not admin'})
        
        lb_id = data.get('id')
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get config
        cursor.execute('SELECT rewards_json, title FROM leaderboard_config WHERE id = ?', (lb_id,))
        config = cursor.fetchone()
        if not config:
            conn.close()
            return jsonify({'success': False, 'error': 'Leaderboard not found'})
        
        rewards = json.loads(config[0]) if config[0] else {}
        title = config[1] or 'Р›РёРґРµСЂР±РѕСЂРґ'
        
        # Get top users
        cursor.execute('''SELECT id, first_name, total_bet_volume FROM users 
            WHERE total_bet_volume > 0
            ORDER BY total_bet_volume DESC LIMIT 50''')
        users = cursor.fetchall()
        
        distributed = 0
        for i, user in enumerate(users, 1):
            reward = rewards.get(str(i))
            if reward:
                user_id = user[0]
                turnover = user[2]
                reward_type = reward.get('type', 'stars')
                reward_amount = reward.get('amount', 0)
                
                # Save to history
                conn.execute('''INSERT INTO leaderboard_history 
                    (period_id, user_id, position, turnover, reward_type, reward_data)
                    VALUES (?, ?, ?, ?, ?, ?)''',
                    (lb_id, user_id, i, turnover, reward_type, str(reward_amount)))
                
                # Give reward via notification
                conn.execute('''INSERT INTO admin_notifications 
                    (title, message, notif_type, target_user_id, reward_type, reward_data, is_active)
                    VALUES (?, ?, 'leaderboard', ?, ?, ?, 1)''',
                    (f'{title} - РњРµСЃС‚Рѕ #{i}', 
                     f'РџРѕР·РґСЂР°РІР»СЏРµРј! Р’С‹ Р·Р°РЅСЏР»Рё {i} РјРµСЃС‚Рѕ РІ Р»РёРґРµСЂР±РѕСЂРґРµ СЃ РѕР±РѕСЂРѕС‚РѕРј {turnover}',
                     user_id, reward_type, str(reward_amount)))
                
                distributed += 1
        
        # Deactivate leaderboard
        conn.execute('UPDATE leaderboard_config SET is_active = 0 WHERE id = ?', (lb_id,))
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'distributed': distributed})
    except Exception as e:
        logger.error(f"Leaderboard distribute error: {e}")
        return jsonify({'success': False, 'error': str(e)})


# --- Setup webhook on import (for Gunicorn/production) ---
try:
    setup_telegram_webhook()
except Exception as e:
    logger.error(f"Initial webhook setup error: {e}")


if __name__ == '__main__':
    host = os.getenv('HOST', '127.0.0.1')
    port = int(os.getenv('PORT', 5000))
    
    # Setup webhook on local run
    setup_telegram_webhook()
    
    print("\n" + "=" * 60)
    print("рџЋ® RasswetGifts вЂ” Р—Р°РїСѓСЃРє СЃРµСЂРІРµСЂР°")
    print("=" * 60)
    print(f"\nрџљЂ Flask СЃРµСЂРІРµСЂ:  http://{host}:{port}")
    print(f"рџЋ° Crash РёРіСЂР°:    http://{host}:{port}/crash")
    print("рџ¤– Telegram Р±РѕС‚:  webhook")
    print("\nвљЎ РќР°Р¶РјРёС‚Рµ Ctrl+C РґР»СЏ РѕСЃС‚Р°РЅРѕРІРєРё\n")
    
    app.run(host=host, port=port, debug=False, use_reloader=False)
