# Merged single-file created on 2025-12-12T02:21:30.004846
# This file is an automated concatenation of the uploaded files in the order:
# === BEGIN FILE: bs.py ===
import asyncio
import concurrent.futures
import hashlib
import hmac
import html
import io
import json
import logging
import os
import pathlib
import random
import re
import requests
import signal
import sqlite3
import string
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import gc
import atexit
from datetime import datetime
from functools import wraps
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import parse_qs, unquote, urlparse, quote

import aiohttp
import phonenumbers
import PIL.Image
import psutil
import pytz
from phonenumbers import carrier, geocoder
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    logging.warning("Google Generative AI not available")

try:
    from bs4 import BeautifulSoup
    from fake_useragent import UserAgent
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    logging.warning("BeautifulSoup4/fake_useragent not available")

try:
    from gtts import gTTS
    import qrcode
    TTS_QR_AVAILABLE = True
except ImportError:
    TTS_QR_AVAILABLE = False
    logging.warning("gTTS/qrcode not available")

try:
    from aiogram import Bot, Dispatcher, Router
    from aiogram.client.default import DefaultBotProperties
    from aiogram.enums import ParseMode
    from aiogram.exceptions import (
        TelegramBadRequest,
        TelegramForbiddenError,
        TelegramNetworkError
    )
    from aiogram.filters import Command
    from aiogram.types import (
        BotCommand,
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        InputMediaPhoto,
        Message,
        User,
    )
    AIOGRAM_AVAILABLE = True
except ImportError:
    AIOGRAM_AVAILABLE = False
    logging.warning("aiogram không khả dụng - sử dụng dự phòng")

    class Router:
        pass

    class Message:
        pass

    class User:
        pass

    class BotCommand:
        pass

    class InlineKeyboardButton:
        pass

    class InlineKeyboardMarkup:
        pass

    class InputMediaPhoto:
        pass

try:
    from moviepy.editor import VideoFileClip
    MOVIEPY_AVAILABLE = True
except ImportError:
    MOVIEPY_AVAILABLE = False
    logging.warning("moviepy not available")

try:
    from telebot import TeleBot, types
    from telebot.async_telebot import AsyncTeleBot
    TELEBOT_AVAILABLE = True
except ImportError:
    TELEBOT_AVAILABLE = False
    logging.warning("telebot not available")

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s:%(lineno)d: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os. getenv('BOT_TOKEN', "8413179871:AAGR-mZMPrccK8aUIY1GUkWmwKrAymCz5lw")
ADMIN_IDS = [7679054753, 6993504486]
OWNER_USERNAME = "tg_mediavip"
GROUP_ID = -1002598824850
DB_FILE = "ultimate_premium. db"
LOG_FILE = "ultimate_bot.log"
DATA_DIR = "./data"
VIETNAM_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

os.makedirs(DATA_DIR, exist_ok=True)

GEMINI_API_KEY = "AIzaSyAWp3AxiFF5OL1rFD_3WmdTe3lMRPgEWVw"
OPENWEATHER_API_KEY = "e707d13f116e5f7ac80bd21c37883e5e"
WEATHERAPI_KEY = "fe221e3a25734f0297994922240611"
ZING_API_KEY = "X5BM3w8N7MKozC0B85o4KMlzLZKhV00y"
ZING_SECRET_KEY = "acOrvUS15XRW2o9JksiK1KgQ6Vbds8ZW"
ZING_VERSION = "1.11.11"
ZING_URL = "https://zingmp3.vn"
TOMORROW_API_KEY = "mdTWQAInBIDB3mHiDtkwuTlwhVB50rqn"

START_BALANCE = 10000
BANK_INFO = "💰 Hướng dẫn nạp tiền:\n• Chủ TK: *NGUYEN TIEN DO*\n• Số TK: `68609666778899`\n• Ngân hàng: *MBBANK - QUÂN ĐỘI*"
QR_CODE_IMAGE_URL = "https://ibb.co/W4pcDM7Q"

RANDOM_THANKS = [
    "Chân thành cảm ơn bạn đã tin tưởng và đồng hành cùng chúng tôi! ",
    "Lòng biết ơn sâu sắc vì sự hỗ trợ tuyệt vời của bạn.  Giao dịch thành công!",
    "Cảm ơn!  Sự ủng hộ của bạn là động lực lớn nhất của chúng tôi."
]

API_SEARCH_BASE = "https://bj-microsoft-search-ai.vercel.app/"
API_XOSO_URL = "https://nguyenmanh.name. vn/api/xsmb? apikey=OUEaxPOl"
API_ANH_GAI = "https://api.zeidteam.xyz/images/gai"
API_VD_GAI = "https://api.zeidteam.xyz/videos/gai"
API_FB_INFO = "https://api.zeidteam.xyz/facebook/info? uid={uid}"
API_TT_INFO = "https://api. zeidteam.xyz/tiktok/user-info?username={username}"
API_SCL_DOWN = "https://adidaphat.site/scl/download? url={url}"
API_NGL_SPAM = "https://adidaphat.site/ngl? username={username}&message={message}&amount={amount}"

PROXY_APIS = [
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
]

LCG_MULTIPLIER = 1337
LCG_INCREMENT = 42069
LCG_MODULUS = 16**8

LOCAL_VIDEO_PATH = "vd.mp4"
IPLOOKUP_API = "http://ip-api.com/json/{ip}? fields=status,message,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org,as,query"
REQUEST_TIMEOUT = 25
TIMEOUT_SHORT = 180
TIMEOUT_MEDIUM = 360
TIMEOUT_LONG = 3600

AI_MODELS = {
    "gemini-2. 0-flash": "⚡ Flash 2.0",
    "gemini-2.5-pro": "💎 Pro 2.5",
    "gemini-3-pro": "📱 Vip 3",
}
CURRENT_MODEL = "gemini-2.0-flash"

TRIGGERS_MUSIC = [
    "nhạc", "nhac", "music", "play", "nghe", "song", "bài hát", "bai hat",
    "track", "sound", "scl", "mp3", "tìm bài", "tim bai", "audio"
]

TRIGGERS_VOICE = [
    "tách", "tach", "lấy nhạc", "lay nhac", "crvoice", "voice", "âm thanh",
    "am thanh", "convert", "chuyển đổi", "chuyen doi", "mp3", "audio", "lấy tiếng"
]

TRIGGERS_TIKTOK_SEARCH = [
    "tiktok", "tt", "douyin", "video", "vid", "clip", "xem"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537. 36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5. 0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
]

BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

SC_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://soundcloud.com",
    "Referer": "https://soundcloud.com/",
    "Connection": "keep-alive",
}

SESSION = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=0.6,
    status_forcelist=(403, 429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"])
)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))
SESSION. headers.update(SC_HEADERS)

SEARCH_CONTEXT = {}
CONTEXT_TIMESTAMP = {}
CONTEXT_TTL = 15 * 60

PLAYER_STATE = {}
PLAYER_LOCK = threading.Lock()

ZINGMP3_DATA = {}

BANK_CODES = {
    "vcb": {"bin": "970436", "name": "VIETCOMBANK", "short_name": "Vietcombank"},
    "vietcombank": {"bin": "970436", "name": "VIETCOMBANK", "short_name": "Vietcombank"},
    "tcb": {"bin": "970407", "name": "TECHCOMBANK", "short_name": "Techcombank"},
    "techcombank": {"bin": "970407", "name": "TECHCOMBANK", "short_name": "Techcombank"},
    "mb": {"bin": "970422", "name": "MB BANK", "short_name": "MBBank"},
    "mbbank": {"bin": "970422", "name": "MB BANK", "short_name": "MBBank"},
    "mb bank": {"bin": "970422", "name": "MB BANK", "short_name": "MBBank"},
    "acb": {"bin": "970416", "name": "ACB", "short_name": "ACB"},
    "vib": {"bin": "970441", "name": "VIB", "short_name": "VIB"},
    "bidv": {"bin": "970418", "name": "BIDV", "short_name": "BIDV"},
    "vietinbank": {"bin": "970415", "name": "VIETINBANK", "short_name": "VietinBank"},
    "vtb": {"bin": "970415", "name": "VIETINBANK", "short_name": "VietinBank"},
    "tpbank": {"bin": "970423", "name": "TPBANK", "short_name": "TPBank"},
    "vpbank": {"bin": "970432", "name": "VPBANK", "short_name": "VPBank"},
    "agribank": {"bin": "970405", "name": "AGRIBANK", "short_name": "Agribank"},
    "sacombank": {"bin": "970403", "name": "SACOMBANK", "short_name": "Sacombank"},
    "scb": {"bin": "970429", "name": "SCB", "short_name": "SCB"},
    "hdbank": {"bin": "970437", "name": "HDBANK", "short_name": "HDBank"},
}

WEATHER_CODES = {
    1000: "Quang đãng",
    1100: "Có mây nhẹ",
    1101: "Có mây",
    1102: "Nhiều mây",
    1001: "Âm u",
    2000: "Sương mù",
    2100: "Sương mù nhẹ",
    4000: "Mưa nhỏ",
    4001: "Mưa",
    4200: "Mưa nhẹ",
    4201: "Mưa vừa",
    4202: "Mưa to",
    5000: "Tuyết",
    5001: "Tuyết rơi nhẹ",
    5100: "Mưa tuyết nhẹ",
    6000: "Mưa đá",
    6200: "Mưa đá nhẹ",
    6201: "Mưa đá nặng",
    7000: "Sấm sét",
    7101: "Sấm sét mạnh",
    7102: "Giông bão",
    8000: "Một vài cơn mưa rào"
}

SCRIPT_SMS_DIRECT = ["vip_0. py"]
SCRIPT_CALL_DIRECT = ["vip1_min.py"]
SCRIPT_SPAM_DIRECT = ["spam_0.py"]
SCRIPT_VIP_DIRECT = ["sms_1.py"]
SCRIPT_FREE = ["spam_0.py"]

SCRIPT_CACHE = {}
SCRIPT_CACHE_TIME = {}

FULL_STATUS = {}
FULL_LOCK = threading.Lock()

LOCKED_COMMANDS = {"call"}

COOLDOWN_COMMAND = {
    'xu_ly_ddos': {'admin': 60, 'vip': 180, 'member': 1800},
    'xu_ly_vip': {'admin': 90, 'vip': 180, 'member': 900},
    'xu_ly_spam': {'admin': 60, 'vip': 180, 'member': 180},
    'xu_ly_sms': {'admin': 60, 'vip': 180, 'member': 450},
    'xu_ly_call': {'admin': 30, 'vip': 180, 'member': 1800},
    'xu_ly_full': {'admin': 3600, 'vip': 3600, 'member': 3600},
    'xu_ly_tiktok': {'admin': 180, 'vip': 300, 'member': 900},
    'xu_ly_ngl': {'admin': 180, 'vip': 300, 'member': 900},
    'xu_ly_free': {'admin': 600, 'vip': 200, 'member': 300},
}


class TTLCache:
    def __init__(self, ttl_sec=600, max_size=256):
        self.ttl = ttl_sec
        self. max = max_size
        self.data = {}
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            v = self.data.get(key)
            if not v:
                return None
            val, exp = v
            if exp < time.time():
                self.data.pop(key, None)
                return None
            return val

    def set(self, key, val):
        with self.lock:
            if len(self. data) >= self.max:
                self.data.pop(next(iter(self.data. keys())), None)
            self.data[key] = (val, time.time() + self.ttl)


class PermissionCache:
    def __init__(self):
        self.cache = {}
        self.max_size = 500

    def get_permission(self, user_id):
        if user_id in self.cache:
            entry = self.cache[user_id]
            if time.time() - entry['timestamp'] < 3600:
                return entry['permission']
            else:
                del self.cache[user_id]
        return None

    def set_permission(self, user_id, permission):
        if len(self.cache) >= self. max_size:
            now = time.time()
            old_keys = [k for k, v in self. cache.items() if now - v['timestamp'] > 1800]
            for key in old_keys[:100]:
                self.cache.pop(key, None)
        self.cache[user_id] = {'permission': permission, 'timestamp': time.time()}


class CooldownManager:
    def __init__(self):
        self.cache = {}
        self._lock = threading.RLock()

    def check_cooldown(self, user_id, command):
        key = f"{command}:{user_id}"
        current_time = time.time()
        if key not in self.cache:
            return False, 0, None
        with self._lock:
            last_use = self.cache[key]
            permission = get_user_permission(user_id)
            cooldown_time = COOLDOWN_COMMAND.get(command, {}).get(permission, 60)
            if current_time - last_use < cooldown_time:
                remaining_time = cooldown_time - (current_time - last_use)
                return True, max(0, remaining_time), "command_specific"
        return False, 0, None

    def set_cooldown(self, user_id, command):
        key = f"{command}:{user_id}"
        with self._lock:
            self.cache[key] = time. time()


CACHE_SEARCH = TTLCache(ttl_sec=300, max_size=256)
CACHE_TRACK = TTLCache(ttl_sec=900, max_size=512)
CACHE_RESOLVE = TTLCache(ttl_sec=900, max_size=1024)

permission_cache = PermissionCache()
cooldown_manager = CooldownManager()

executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=os.cpu_count() * 5 if os.cpu_count() else 30
)

if AIOGRAM_AVAILABLE:
    try:
        bot_aiogram = Bot(
            token=TELEGRAM_BOT_TOKEN,
            default=DefaultBotProperties(
                parse_mode=ParseMode.HTML,
                link_preview_is_disabled=True
            )
        )
    except Exception as e:
        logger.error(f"Error initializing aiogram bot: {e}")
        bot_aiogram = None
else:
    bot_aiogram = None

if TELEBOT_AVAILABLE:
    try:
        bot_telebot = TeleBot(TELEGRAM_BOT_TOKEN, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error initializing telebot: {e}")
        bot_telebot = None
else:
    bot_telebot = None

if GEMINI_AVAILABLE:
    try:
        genai. configure(api_key=GEMINI_API_KEY)
    except Exception as e:
        logger.error(f"Error configuring Gemini: {e}")

PHONE_CACHE = {}
PHONE_CACHE_LOCK = threading.Lock()


def create_db_connection():
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=8.0)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        return conn


def blocking_db_execute(sql: str, params: tuple = ()) -> Optional[List[Any]]:
    conn = None
    try:
        conn = create_db_connection()
        c = conn.cursor()
        c.execute(sql, params)
        conn.commit()
        result = c.fetchall()
        return result
    except sqlite3.Error as e:
        logger.error(f"DB Execute Error: {e} - SQL: {sql}", exc_info=True)
        return None
    except Exception as e:
        logger. error(f"DB Execute Error (other): {e} - SQL: {sql}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def blocking_db_fetchone(sql: str, params: tuple = ()) -> Optional[tuple]:
    conn = None
    try:
        conn = create_db_connection()
        c = conn.cursor()
        c. execute(sql, params)
        result = c.fetchone()
        return result
    except sqlite3. Error as e:
        logger. error(f"DB Fetchone Error: {e} - SQL: {sql}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"DB Fetchone Error (other): {e} - SQL: {sql}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


async def async_db_execute(sql: str, params: tuple = ()) -> Optional[List[Any]]:
    return await asyncio.to_thread(blocking_db_execute, sql, params)


async def async_db_fetchone(sql: str, params: tuple = ()) -> Optional[tuple]:
    return await asyncio.to_thread(blocking_db_fetchone, sql, params)


async def setup_database():
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            balance INTEGER DEFAULT 0,
            is_admin BOOLEAN DEFAULT FALSE,
            is_approved BOOLEAN DEFAULT FALSE
        )
    """)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            task_id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT,
            reward INTEGER
        )
    """)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS nap_request (
            request_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount INTEGER,
            status TEXT DEFAULT 'pending',
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS groups (
            chat_id INTEGER PRIMARY KEY
        )
    """)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS admin (
            user_id TEXT PRIMARY KEY,
            name TEXT,
            role TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS vip_lists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            list_name TEXT NOT NULL,
            phone_numbers TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(user_id, list_name)
        )
    """)

    for admin_id in ADMIN_IDS:
        await async_db_execute(
            """INSERT INTO users (user_id, balance, is_admin, is_approved) VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET is_admin=excluded.is_admin, is_approved=excluded.is_approved""",
            (admin_id, 99999999, True, True)
        )
        await async_db_execute(
            """INSERT INTO admin (user_id, name, role) VALUES (?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET role=excluded.role""",
            (str(admin_id), OWNER_USERNAME, 'admin')
        )

    logger.info("✅ Database setup completed successfully")


def cleanup_old_cache():
    current_time = time.time()
    keys_to_remove = []
    for key, timestamp in SCRIPT_CACHE_TIME.items():
        if current_time - timestamp > 600:
            keys_to_remove.append(key)
    for key in keys_to_remove:
        SCRIPT_CACHE.pop(key, None)
        SCRIPT_CACHE_TIME. pop(key, None)


def get_available_scripts(script_list, cache_key):
    current_time = time.time()
    if len(SCRIPT_CACHE) > 20:
        cleanup_old_cache()
    if (cache_key in SCRIPT_CACHE and
        cache_key in SCRIPT_CACHE_TIME and
        current_time - SCRIPT_CACHE_TIME[cache_key] < 600):
        return SCRIPT_CACHE[cache_key]
    available = [s for s in script_list if os.path.exists(s)]
    SCRIPT_CACHE[cache_key] = available
    SCRIPT_CACHE_TIME[cache_key] = current_time
    return available


def set_full_status(user_id, phone_number):
    with FULL_LOCK:
        key = f"{user_id}:{phone_number}"
        FULL_STATUS[key] = time.time() + 24 * 3600


def remove_full_status(user_id, phone_number):
    with FULL_LOCK:
        key = f"{user_id}:{phone_number}"
        FULL_STATUS.pop(key, None)


def check_full_status(user_id, phone_number):
    with FULL_LOCK:
        key = f"{user_id}:{phone_number}"
        if key in FULL_STATUS and FULL_STATUS[key] > time.time():
            return True
        FULL_STATUS.pop(key, None)
        return False


def run_background_process_sync(command, timeout=None, user_id=None):
    try:
        if not command or not isinstance(command, str):
            return False, None, None
        command = command.strip()
        if len(command) > 1000:
            return False, None, None
        full_command = f"setsid {command} > /dev/null 2>&1 & echo $!"
        result = subprocess.run(
            full_command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=15
        )
        if result.returncode == 0 and result.stdout. strip():
            pid = int(result.stdout.strip())
            time.sleep(0.5)
            try:
                proc = psutil.Process(pid)
                if proc.is_running():
                    logger.info(f"Created process PID {pid} for user {user_id}: {command[:50]}...")
                    try:
                        os.setpgid(pid, pid)
                    except (OSError, ProcessLookupError):
                        pass
                    return True, pid, None
            except psutil.NoSuchProcess:
                logger.warning(f"Process {pid} exited immediately after creation")
        return False, None, None
    except Exception as e:
        logger.error(f"Error run_background_process_sync: {e}")
        return False, None, None


def count_processes_sync(user_id=None):
    try:
        count = 0
        for proc in psutil.process_iter(['cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if 'python' in cmdline and any(script in cmdline for script in ['spam_', 'sms_', 'vip_', 'call']):
                    if user_id is None or str(user_id) in cmdline:
                        count += 1
            except:
                continue
        return count
    except:
        return 0


def kill_processes_sync(pattern):
    killed_count = 0
    try:
        processes_to_kill = []
        process_families = {}
        for proc in psutil. process_iter(['pid', 'ppid', 'cmdline', 'name', 'status', 'create_time']):
            try:
                proc_info = proc.info
                if not proc_info['cmdline']:
                    continue
                cmdline = ' '.join(proc_info['cmdline'])
                proc_name = proc_info. get('name', '')
                proc_status = proc_info.get('status', '')

                if proc_status == psutil.STATUS_ZOMBIE:
                    processes_to_kill.append(proc)
                    continue

                is_target_process = (
                    ('python' in proc_name. lower() or 'python' in cmdline.lower()) and
                    any(script in cmdline for script in [
                        'spam_', 'sms_', 'vip_', 'call', 'lenh', 'tcp. py', 'tt.py',
                        'ngl.py', 'pro24h.py', 'vip11122.py', 'mlm.py', 'vip1_min.py',
                        'master222.py'
                    ])
                )

                if proc_info. get('create_time'):
                    process_age = time.time() - proc_info['create_time']
                    if process_age > 21600 and is_target_process:
                        logger.warning(f"Detected old process {proc_info['pid']}: {process_age/3600:.1f}h - {cmdline[:100]}")

                if not is_target_process:
                    continue

                should_kill = False
                if pattern == "python.*lenh":
                    should_kill = True
                elif "lenh.*" in pattern:
                    parts = pattern.split('.*')
                    if len(parts) >= 3:
                        user_id = parts[-1]
                        if user_id and user_id in cmdline:
                            should_kill = True
                else:
                    pattern_clean = pattern.replace('.*', '').replace('python3', 'python')
                    if pattern_clean in cmdline:
                        should_kill = True

                if should_kill:
                    processes_to_kill.append(proc)
                    try:
                        children = proc.children(recursive=True)
                        process_families[proc. pid] = children
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        for proc in processes_to_kill:
            try:
                if proc.status() == psutil.STATUS_ZOMBIE:
                    try:
                        parent = proc.parent()
                        if parent and parent.pid != 1:
                            parent.terminate()
                            parent.wait(timeout=2)
                    except:
                        pass
                    killed_count += 1
                    continue

                children = process_families.get(proc.pid, [])
                for child in children:
                    try:
                        if child.is_running():
                            child. terminate()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                time.sleep(0.5)
                for child in children:
                    try:
                        if child.is_running():
                            child.kill()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                proc.terminate()
                try:
                    proc.wait(timeout=8)
                    killed_count += 1
                except psutil.TimeoutExpired:
                    proc.kill()
                    try:
                        proc.wait(timeout=5)
                        killed_count += 1
                    except:
                        try:
                            os.kill(proc.pid, 9)
                            killed_count += 1
                        except:
                            pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                killed_count += 1
                continue

        if killed_count == 0:
            try:
                commands = []
                if 'lenh.*' in pattern and len(pattern.split('.*')) > 2:
                    user_id = pattern.split('.*')[-1]
                    commands = [
                        f"pkill -15 -f 'python.*{user_id}'",
                        f"pkill -9 -f 'python.*{user_id}'",
                        "pkill -9 -f 'spam_|sms_|vip_|call|tcp.py|tt.py|ngl.py|pro24h. py'"
                    ]
                else:
                    commands = [
                        "pkill -15 -f 'python.*lenh'",
                        "pkill -9 -f 'python.*lenh'",
                        "pkill -9 -f 'spam_|sms_|vip_|call|tcp.py|tt.py|ngl.py|pro24h.py'",
                        "pkill -9 -f 'python3.*vip'",
                        "pkill -9 -f 'python.*pro24h'"
                    ]

                for cmd in commands:
                    try:
                        result = subprocess.run(cmd, shell=True, timeout=5, capture_output=True)
                        if result.returncode == 0:
                            killed_count += 1
                        time.sleep(0.2)
                    except:
                        continue
            except Exception:
                pass

        try:
            subprocess.run("ps aux | grep '<defunct>' | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true",
                         shell=True, timeout=8, capture_output=True)
            subprocess.run("ps -eo pid,etime,cmd | grep python | awk '$2 ~ /^[0-9]+-/ || $2 ~ /^[0-6][0-9]:[0-5][0-9]:[0-5][0-9]/ {print $1}' | head -20 | xargs -r kill -9 2>/dev/null || true",
                         shell=True, timeout=10, capture_output=True)
            subprocess.run("find /tmp -name '*.py*' -mmin +60 -delete 2>/dev/null || true",
                         shell=True, timeout=10, capture_output=True)
            subprocess.run("find .  -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true",
                         shell=True, timeout=10, capture_output=True)
            subprocess.run("sync", shell=True, timeout=3, capture_output=True)
        except Exception as e:
            logger.error(f"Error enhanced cleanup: {e}")

    except Exception as e:
        logger.error(f"Error kill_processes_sync: {e}")
        return False

    logger.info(f"Cleaned up {killed_count} processes with pattern: {pattern}")
    return killed_count > 0


async def get_user(user_id: int, username: Optional[str] = None) -> Optional[Dict[str, Any]]:
    user_data = await async_db_fetchone(
        "SELECT user_id, username, balance, is_admin, is_approved FROM users WHERE user_id = ?",
        (user_id,)
    )
    if user_data is None:
        username = username if username else f"user_{user_id}"
        await async_db_execute(
            "INSERT INTO users (user_id, username, balance, is_approved) VALUES (?, ?, ?, ?)",
            (user_id, username, 0, False)
        )
        logger.info(f"Created new user: {user_id} - @{username}")
        return {"user_id": user_id, "username": username, "balance": 0, "is_admin": False, "is_approved": False}
    elif user_data:
        return {
            "user_id": user_data[0],
            "username": user_data[1],
            "balance": user_data[2],
            "is_admin": bool(user_data[3]),
            "is_approved": bool(user_data[4])
        }
    else:
        return None


async def update_balance(user_id: int, amount: int):
    user_exists = await async_db_fetchone("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
    if user_exists:
        await async_db_execute("UPDATE users SET balance = balance + ? WHERE user_id = ? ", (amount, user_id))
        logger.info(f"Updated balance for {user_id} by {amount}")
    else:
        logger.warning(f"Attempted to update balance for non-existent user: {user_id}")


async def get_all_group_ids() -> List[int]:
    groups_data = await async_db_execute("SELECT chat_id FROM groups")
    if groups_data is None:
        return []
    return [row[0] for row in groups_data]


def get_user_mention(user) -> str:
    if hasattr(user, 'username') and user.username:
        return f"@{user.username}"
    if hasattr(user, 'first_name'):
        safe_name = escape_markdown_v2(user.first_name)
        return f"[{safe_name}](tg://user?id={user. id})"
    return f"User_{user.id}"


def get_vietnam_time():
    try:
        tz = pytz.timezone("Asia/Ho_Chi_Minh")
        now = datetime.now(tz)
        return now.strftime("%H:%M:%S"), now.strftime("%d/%m/%Y")
    except Exception as e:
        logger.error(f"Error getting Vietnam time: {e}")
        now = datetime.now()
        return now.strftime("%H:%M:%S"), now.strftime("%d/%m/%Y")


def escape_markdown_v2(text):
    if text is None:
        return ""
    escape_chars = r'([_*\[\]()~`>#+-=|{}.!])'
    text = str(text). replace('\\', '\\\\')
    return re.sub(escape_chars, r'\\\1', text)


def escape_html(text):
    if text is None:
        return ""
    return html.escape(str(text))


def format_cooldown_time(seconds):
    if seconds <= 0:
        return "0 giây"
    if seconds < 60:
        return f"{int(seconds)} giây"
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)
    if remaining_seconds == 0:
        return f"{minutes} phút"
    else:
        return f"{minutes} phút {remaining_seconds} giây"


def format_user_link(user):
    try:
        if not user:
            return "Unknown User"
        user_id = user.id if hasattr(user, 'id') else None
        full_name = user.full_name if hasattr(user, 'full_name') else (user.first_name if hasattr(user, 'first_name') else None)
        if not user_id:
            return escape_html(full_name or "Unknown User")
        if full_name:
            return f'<a href="tg://user? id={user_id}">{escape_html(full_name)}</a>'
        else:
            return f'<a href="tg://user?id={user_id}">ID: {user_id}</a>'
    except Exception as e:
        logger.error(f"Error formatting user link: {e}")
        return "Unknown User"


def get_permission_title(user_id):
    level = get_user_permission(user_id)
    titles = {
        'admin': "╭━━⊰⿗𓆰☯︎ 🎩 𝓐𝓭𝓶𝓲𝓷  ☯︎𓆪⿘━━╮",
        'vip': "╭━━₊༺𓆰🧞‍♂️🅥🅘🅟🧜🏻‍♀️𓆪༻₊━━╮",
        'member': "╭━━━━༉Members༉━━━━╮"
    }
    return titles. get(level, titles['member'])


def get_user_permission(user_id):
    user_id = str(user_id)
    if user_id == str(ADMIN_IDS[0]):
        return 'admin'

    cached_permission = permission_cache.get_permission(user_id)
    if cached_permission is not None:
        return cached_permission

    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT role FROM admin WHERE user_id = ?  LIMIT 1", (user_id,))
        admin_result = cursor.fetchone()
        conn.close()

        if admin_result:
            permission = admin_result['role']
        else:
            permission = 'member'

        permission_cache.set_permission(user_id, permission)
        return permission
    except Exception as e:
        logger.error(f"Error getting user permission {user_id}: {e}")
        permission_cache.set_permission(user_id, 'member')
        return 'member'


def is_admin(user_id):
    return get_user_permission(user_id) == 'admin'


def is_vip_permanent(user_id):
    level = get_user_permission(user_id)
    return level in ('admin', 'vip')


def is_valid_phone(phone):
    if not phone:
        return False

    with PHONE_CACHE_LOCK:
        if phone in PHONE_CACHE:
            return PHONE_CACHE[phone]

    try:
        if not phone.isdigit() or len(phone) not in [10, 11]:
            with PHONE_CACHE_LOCK:
                PHONE_CACHE[phone] = False
            return False

        number = phonenumbers.parse(phone, "VN")
        valid = phonenumbers.is_valid_number(number)

        with PHONE_CACHE_LOCK:
            PHONE_CACHE[phone] = valid

        return valid
    except Exception:
        with PHONE_CACHE_LOCK:
            PHONE_CACHE[phone] = False
        return False


def validate_phone_with_carrier(phone):
    try:
        if not phone or not isinstance(phone, str):
            return False, "Số điện thoại không hợp lệ"

        clean_phone = ''.join(filter(str.isdigit, phone))

        if not is_valid_phone(clean_phone):
            return False, "Số điện thoại không hợp lệ"

        parsed_number = phonenumbers.parse(clean_phone, "VN")

        if not phonenumbers.is_valid_number(parsed_number):
            return False, "Số điện thoại không hợp lệ"

        try:
            carrier_name = carrier. name_for_number(parsed_number, "vi")
        except ImportError:
            carrier_name = get_carrier(clean_phone)

        if not carrier_name or carrier_name == "Không rõ":
            carrier_name = get_carrier(clean_phone)

        return True, carrier_name
    except phonenumbers.NumberParseException:
        return False, "Số không hợp lệ"
    except Exception:
        return False, "Số không hợp lệ"


def get_carrier(phone):
    if not phone:
        return "Không xác định"

    phone = str(phone). strip()

    if phone.startswith("+84"):
        phone = "0" + phone[3:]
    elif phone.startswith("84"):
        phone = "0" + phone[2:]

    if len(phone) < 3:
        return "Không xác định"

    prefix = phone[:3]

    viettel = {"086", "096", "097", "098", "032", "033", "034", "035", "036", "037", "038", "039"}
    mobifone = {"089", "090", "093", "070", "079", "077", "076", "078"}
    vinaphone = {"088", "091", "094", "083", "084", "085", "081", "082"}
    vietnamobile = {"092", "056", "058"}
    gmobile = {"099", "059"}

    if prefix in viettel:
        return "Viettel"
    elif prefix in mobifone:
        return "Mobifone"
    elif prefix in vinaphone:
        return "Vinaphone"
    elif prefix in vietnamobile:
        return "Vietnamobile"
    elif prefix in gmobile:
        return "Gmobile"

    return "Không xác định"


def get_phone_limit(user_id):
    level = get_user_permission(user_id)
    limits = {'admin': 50, 'vip': 50, 'member': 2}
    return limits.get(level, 2)


def log_command(user_id: int, command: str, target: str):
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] User: {user_id} | Command: {command} | Target: {target}\n"
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_entry)
    except IOError as e:
        logger. warning(f"Cannot write log (IOError): {e}")
    except Exception as e:
        logger.warning(f"Cannot write log (other): {e}")


def predict_md5_logic(md5_hash: str) -> Dict[str, Any]:
    try:
        md5_hash = md5_hash.strip(). lower()
        if not re.fullmatch(r"^[0-9a-f]{32}$", md5_hash):
            return {"ok": False, "error": "Invalid MD5 format"}

        seed = int(md5_hash[:8], 16)
        next_seed = (seed * LCG_MULTIPLIER + LCG_INCREMENT) % LCG_MODULUS
        predicted_md5 = hashlib.md5(str(next_seed).encode()).hexdigest()
        result_hex = predicted_md5[-8:]
        value = int(result_hex, 16)
        dice = [((value >> (i * 4)) % 6) + 1 for i in range(3)]
        total = sum(dice)
        result = "TÀI" if total > 10 else "XỈU"

        return {
            "ok": True,
            "predicted_md5": predicted_md5,
            "dice": dice,
            "total": total,
            "result": result,
            "seed_next": next_seed
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def generate_qr_code_sync(text: str):
    if not TTS_QR_AVAILABLE or not qrcode:
        return "⚠️ Missing qrcode library"
    try:
        qr_img = qrcode.make(text)
        buffer = io.BytesIO()
        qr_img.save(buffer, format="PNG")
        buffer.seek(0)
        return buffer
    except Exception as e:
        return f"Error creating QR: {e}"


def text_to_speech_sync(text: str):
    if not TTS_QR_AVAILABLE or not gTTS:
        return "⚠️ Missing gTTS library"
    try:
        tts = gTTS(text=text[:250], lang='vi')
        buffer = io.BytesIO()
        tts.write_to_fp(buffer)
        buffer. seek(0)
        return buffer
    except Exception as e:
        return f"Error creating Voice: {e}"


def get_api_result_sync(url: str, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        response. raise_for_status()
        content_type = response.headers.get('Content-Type', '').lower()

        if 'application/json' in content_type:
            return response.json()
        elif 'text/' in content_type:
            return {"status": True, "_content": response.text}
        else:
            logger.warning(f"API {url} returned undefined Content-Type: {content_type}")
            return {"status": True, "_content": response.text}
    except requests.exceptions.JSONDecodeError:
        return {
            "status": False,
            "message": f"API returned non-JSON.  (Code: {response.status_code if 'response' in locals() else 'N/A'})"
        }
    except requests.exceptions.RequestException as e:
        return {"status": False, "message": f"API connection error: {e}"}
    except Exception as e:
        return {"status": False, "message": str(e)}


def create_group_link_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🥷🏿   ⋰ 𓊈 𝐴𝑑𝑚𝑖𝑛 24/7 𓊉 ⋱   🛰️",
                url=f"https://t.me/{OWNER_USERNAME}"
            )
        ]
    ])
    return keyboard


def read_js_file(filename):
    try:
        if not os.path.exists(filename):
            return []

        with open(filename, 'r', encoding='utf-8') as file:
            content = file.read()

        pattern = r'\[([^\]]+)\]'
        match = re.search(pattern, content, re.DOTALL)

        if match:
            array_content = match.group(1)
            urls = []
            for line in array_content.split('\n'):
                line = line.strip()
                if line. startswith('"') and line.endswith('",'):
                    url = line[1:-2]
                    urls.append(url)
                elif line.startswith('"') and line.endswith('"'):
                    url = line[1:-1]
                    urls.append(url)
            return urls

        return []
    except Exception as e:
        logger.error(f"Error reading JS file {filename}: {e}")
        return []
        
async def cleanup_full_status_safe():
    if 'FULL_STATUS' not in globals() or 'FULL_LOCK' not in globals():
        return

    try:
        current_time = time.time()
        keys_to_remove = []
        with FULL_LOCK:
            keys_to_remove = [k for k, v in FULL_STATUS.items() 
                             if v < current_time - 3600]
        if keys_to_remove:
            batch_size = 50
            removed_total = 0

            for i in range(0, len(keys_to_remove), batch_size):
                batch = keys_to_remove[i:i + batch_size]
                with FULL_LOCK:
                    for key in batch:
                        FULL_STATUS.pop(key, None)
                        removed_total += 1

                if i + batch_size < len(keys_to_remove):
                    await asyncio.sleep(0.01)

            logger.info(f"🧹 Removed {removed_total} old entries from FULL_STATUS")

    except Exception as e:
        logger.error(f"Error cleanup FULL_STATUS: {e}")

def extract_params(message):
    text = getattr(message, "text", None)
    if not text:
        return []
    parts = text.split()
    if len(parts) < 2:
        return []
    return parts[1:]

async def check_command_locked(message, command: str) -> bool:
    if command in LOCKED_COMMANDS:
        await send_response(
            message,
            "🔒 Hệ thống đang được nâng cấp để mang đến trải nghiệm tốt hơn.\n"
            "Vui lòng sử dụng lệnh /free !\n\n"
            "Cảm ơn bạn đã kiên nhẫn chờ đợi! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return True
    return False

async def send_response(
    message: Message,
    title: str,
    content: str = "",
    processing_msg: Optional[types.Message] = None,
    delete_user_msg: bool = False,
    auto_delete_after: int = 0,
    keep_forever: bool = False,
    with_keyboard: bool = False
):
    try:
        current_time = get_vietnam_time()
        time_str, date_str = current_time

        safe_title = escape_markdown_v2(title. upper() if isinstance(title, str) else str(title))
        
        text_limit = 1000 - len(title) - len(time_str) - 100
        safe_text = escape_markdown_v2(
            (content if isinstance(content, str) else str(content))[:text_limit] + 
            ('...' if len(str(content)) > text_limit else '')
        )
        safe_time = escape_markdown_v2(time_str)
        safe_owner = escape_markdown_v2(f"@{OWNER_USERNAME}")

        formatted_caption = (
            f"┏ 💎 *{safe_title}* ┓\n"
            f"┣{chr(8213)*20}\n"
            f"┣ {safe_text}\n"
            f"┣{chr(8213)*20}\n"
            f"┗ ⏱️ *{safe_time}* \\| Bot by {safe_owner}"
        )

        video_sent_successfully = False
        if os.path.exists(LOCAL_VIDEO_PATH):
            try:
                if processing_msg:
                    try:
                        await bot_aiogram.delete_message(processing_msg.chat.id, processing_msg.message_id)
                    except Exception:
                        pass

                with open(LOCAL_VIDEO_PATH, 'rb') as video_file:
                    keyboard = create_group_link_keyboard() if with_keyboard else None
                    await bot_aiogram.send_video(
                        chat_id=message.chat.id,
                        video=video_file,
                        caption=formatted_caption,
                        reply_to_message_id=message.message_id,
                        parse_mode="MarkdownV2",
                        reply_markup=keyboard
                    )
                video_sent_successfully = True
            except Exception as e:
                logger.error(f"Error sending video '{LOCAL_VIDEO_PATH}': {e}", exc_info=True)
        else:
            logger.warning(f"Video file '{LOCAL_VIDEO_PATH}' not found")
            if processing_msg:
                try:
                    await bot_aiogram.delete_message(processing_msg.chat.id, processing_msg.message_id)
                except Exception:
                    pass

        if not video_sent_successfully:
            logger.info("Video send failed or file not found, using text fallback")
            error_prefix = f"⚠️ *Lỗi Video* \\(File `{escape_markdown_v2(LOCAL_VIDEO_PATH)}` lỗi hoặc không tồn tại\\)\n\n"
            fallback_text = error_prefix + formatted_caption
            try:
                keyboard = create_group_link_keyboard() if with_keyboard else None
                await bot_aiogram.send_message(
                    chat_id=message.chat.id,
                    text=fallback_text,
                    parse_mode="MarkdownV2",
                    reply_markup=keyboard
                )
            except Exception as e_fallback:
                logger.error(f"Error sending text fallback: {e_fallback}", exc_info=True)
                try:
                    await bot_aiogram.send_message(
                        chat_id=message.chat.id,
                        text=f"{title}\n---\n{content[:4000]}\n---\n{time_str}"
                    )
                except Exception as e_final:
                    logger.critical(f"Error sending final fallback: {e_final}", exc_info=True)

        if delete_user_msg:
            try:
                await bot_aiogram. delete_message(chat_id=message.chat.id, message_id=message.message_id)
            except Exception:
                pass

    except Exception as e:
        logger.error(f"Error in send_response: {e}", exc_info=True)

async def auto_delete_message(chat_id: int, message_id: int, delay: int = 10):
    try:
        await asyncio.sleep(delay)
        await bot_aiogram.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.error(f"Error auto-deleting message ({chat_id}, {message_id}): {e}")

def user_cooldown(default_seconds: int = 60):
    def decorator(func):
        @wraps(func)
        async def wrapper(message: Message, *args, **kwargs):
            if not message.from_user:
                return False
            user_id = message.from_user.id
            func_name = func.__name__

            level = get_user_permission(user_id)

            required_perm = getattr(func, '_required_permission', None)
            if required_perm:
                if required_perm == 'admin' and level != 'admin':
                    await send_response(message, "TRUY CẬP BỊ TỪ CHỐI", "Không đủ quyền!", delete_user_msg=True, auto_delete_after=3)
                    return False
                elif required_perm == 'vip_permanent' and level not in ('admin', 'vip'):
                    await send_response(message, "TRUY CẬP BỊ TỪ CHỐI", "Không đủ quyền!", delete_user_msg=True, auto_delete_after=3)
                    return False

            if level != 'admin':
                on_cooldown, remaining, _ = cooldown_manager.check_cooldown(user_id, func_name)
                if on_cooldown:
                    formatted_time = format_cooldown_time(remaining)
                    await send_response(
                        message,
                        "COOLDOWN",
                        f"🏓 Bạn cần chờ {formatted_time} nữa để sử dụng lệnh này! ",
                        delete_user_msg=True,
                        auto_delete_after=5
                    )
                    return False

            result = await func(message, *args, **kwargs)

            if result is True and level != 'admin':
                cooldown_manager.set_cooldown(user_id, func_name)

            return result
        return wrapper
    return decorator

def group_only(func):
    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if not message.from_user:
            return False
        user_id = message.from_user.id
        if is_admin(user_id):
            return await func(message, *args, **kwargs)
        if message.chat.id != GROUP_ID:
            return False
        return await func(message, *args, **kwargs)
    return wrapper

def admin_only(func):
    func._required_permission = 'admin'
    return func

def vip_only(func):
    func._required_permission = 'vip_permanent'
    return func

async def handle_sms(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if not params:
        phone_limit = get_phone_limit(user_id)
        await send_response(
            message,
            "SMS HELP",
            f"👼🏻 /sms 0987654321 0987654322... Tối đa {phone_limit} số theo quyền hạn của bạn! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    phone_limit = get_phone_limit(user_id)
    if len(params) > phone_limit:
        await send_response(
            message,
            "SMS LIMIT",
            f"👼🏻 Lệnh /sms chỉ cho phép nhập tối đa {phone_limit} số! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    valid_phones = []
    for phone in params[:phone_limit]:
        phone = phone.strip()
        if is_valid_phone(phone) and not check_full_status(user_id, phone) and phone not in valid_phones:
            valid_phones.append(phone)

    if not valid_phones:
        await send_response(
            message,
            "SMS ERROR",
            "👼🏻 Các số điện thoại không hợp lệ hoặc đang chạy full 24h!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    available_scripts = get_available_scripts(SCRIPT_VIP_DIRECT, 'sms')
    if not available_scripts:
        await send_response(
            message,
            "SMS ERROR",
            "👼🏻 Không có script SMS khả dụng!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    success_pids = []
    for phone in valid_phones:
        script = random.choice(available_scripts)
        command = f"proxychains4 python3 {script} {phone} 50"
        success, pid, _ = run_background_process_sync(command, timeout=TIMEOUT_MEDIUM, user_id=user_id)
        if success and pid:
            success_pids.append(pid)

    if not success_pids:
        await send_response(
            message,
            "SMS ERROR",
            "👼🏻 Không thể khởi tạo tiến trình! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    log_command(user_id, "/sms", f"{len(valid_phones)} numbers")

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━━〖⨧✧✧⩩〗\n"
        f" • 𝑁ℎ𝑎̣̂𝑝 𝑇𝑎𝑦          :      {len(valid_phones)} Số Hợp lệ\n"
        f" • 𝑇𝑎̂́𝑛 𝐶𝑜̂𝑛𝑔           :       60 phút\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́                  :      V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛           :       {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦                :       {date_str}\n"
        f"╰━━━━━〖⨧✧𝐒𝐌𝐒✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m. jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "SMS", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
async def handle_spam(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "SPAM HELP",
            "👼🏻 Cú pháp: /spam 0987654321",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    phone = params[0].strip()

    valid, carrier_name = validate_phone_with_carrier(phone)
    if not valid:
        await send_response(
            message,
            "SPAM ERROR",
            f"👼🏻 {carrier_name}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    if check_full_status(user_id, phone):
        await send_response(
            message,
            "SPAM ERROR",
            f"👼🏻 Số {phone} đang chạy full 24h!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    available_scripts = get_available_scripts(SCRIPT_SPAM_DIRECT, 'spam')
    if not available_scripts:
        await send_response(
            message,
            "SPAM ERROR",
            "👼🏻 Không có script Spam khả dụng!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    script = random.choice(available_scripts)

    command = f"timeout 180s python3 {script} {phone} 5"
    success, pid, _ = run_background_process_sync(command, timeout=TIMEOUT_SHORT, user_id=user_id)

    if not success:
        await send_response(
            message,
            "SPAM ERROR",
            "👼🏻 Lỗi khi khởi động tiến trình! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    log_command(user_id, "/spam", phone)

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━〖⨧✧✧⩩〗\n"
        f" • 𝑃ℎ𝑜𝑛𝑒 𝑉𝑁        :      {phone}\n"
        f" • 𝑇𝑎̂́𝑛 𝐶𝑜̂𝑛𝑔        :      1 Giờ liên tục\n"
        f" • 𝑁ℎ𝑎̀ 𝑚𝑎̣𝑛𝑔       :      {carrier_name}\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́                :      V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛         :      {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦              :      {date_str}\n"
        f"╰━━━━〖⨧✧𝐒𝐏𝐀𝐌✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat. id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "SPAM", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
async def handle_free(message: Message):
    if not message.from_user:
        return False
    user = message. from_user
    user_id = user.id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "FREE HELP",
            "👼🏻 Cú pháp: /free 0987654321",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    phone = params[0].strip()

    valid, carrier_name = validate_phone_with_carrier(phone)
    if not valid:
        await send_response(
            message,
            "FREE ERROR",
            f"👼🏻 {carrier_name}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    if check_full_status(user_id, phone):
        await send_response(
            message,
            "FREE ERROR",
            f"👼🏻 Số {phone} đang chạy full 24h!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    script = random.choice(SCRIPT_FREE)

    command = f"timeout 180s python3 {script} {phone} 1"
    success, pid, _ = run_background_process_sync(command, timeout=TIMEOUT_SHORT, user_id=user_id)

    if not success:
        await send_response(
            message,
            "FREE ERROR",
            "👼🏻 Lỗi khi khởi động tiến trình!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    log_command(user_id, "/free", phone)

    user_link = format_user_link(user)

    content = (
        f"👼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟   :     {user_link}\n"
        f"🎫 𝑀𝑎̃ 𝐼𝐷      :     {user_id}\n"
        f"🚀 𝐿𝑒̣̂𝑛ℎ 𝑑𝑎̃ 𝑐ℎ𝑎̣𝑦 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔 ! 🎯\n"
        f"𝐴𝐸 𝑡𝑒𝑠𝑡 𝑡ℎ𝑢̛̉ 𝑠𝑜̂́ 𝑟𝑜̂̀𝑖 𝑐ℎ𝑜 𝑚𝑖̀𝑛ℎ 𝑥𝑖𝑛 𝑦́ 𝑘𝑖𝑒̂́𝑛 !"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "FREE", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
@vip_only
async def handle_full(message: Message):
    if await check_command_locked(message, "full"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if not params:
        await send_response(
            message,
            "FULL HELP",
            "👼🏻 Cú pháp: /full 0987654321 0987654322.. .\nChạy liên tục 24h - VIP tối đa 3 số mỗi lần ! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    if len(params) > 3:
        await send_response(
            message,
            "FULL LIMIT",
            "👼🏻 VIP chỉ được phép nhập tối đa 3 Số cho lệnh full! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    valid_phones = []
    for phone in params:
        phone = phone.strip()
        if is_valid_phone(phone) and not check_full_status(user_id, phone) and phone not in valid_phones:
            valid_phones.append(phone)

    if not valid_phones:
        await send_response(
            message,
            "FULL ERROR",
            "👼🏻 Không có số điện thoại hợp lệ! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    with FULL_LOCK:
        user_full_count = sum(1 for key in FULL_STATUS.keys() if key. startswith(f"{user_id}:"))
        if user_full_count + len(valid_phones) > 3:
            await send_response(
                message,
                "FULL ERROR",
                f"👼🏻 Bạn đã có {user_full_count} số đang Full.  VIP chỉ được tối đa 3 số! ",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

    success_pids = []
    success_phones = []

    for phone in valid_phones:
        set_full_status(user_id, phone)

        command = f"timeout 1200s python3 pro24h.py {phone}"
        success, pid, _ = run_background_process_sync(command, timeout=TIMEOUT_LONG, user_id=user_id)

        if success and pid:
            success_pids.append(pid)
            success_phones.append(phone)
        else:
            remove_full_status(user_id, phone)

    if not success_pids:
        await send_response(
            message,
            "FULL ERROR",
            "👼🏻 Không thể khởi tạo tiến trình full nào!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    log_command(user_id, "/full", f"{len(success_phones)} numbers")

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)
    phone_list = ", ".join(success_phones)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━━〖⨧✧✧⩩〗\n"
        f" • 𝑃ℎ𝑜𝑛𝑒 𝐵𝑙𝑜𝑐𝑘     :      {len(success_phones)} số Hợp lệ\n"
        f" • 𝐷𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ        :      {phone_list}\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛          :      24 Giờ liên tục\n"
        f" • 𝑇𝑟𝑎̣𝑛𝑔 𝑡ℎ𝑎́𝑖        :       Đang gửi OTP\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́                  :      V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛           :      {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦                :      {date_str}\n"
        f" • 📵 𝑈𝑛𝑙𝑜𝑐𝑘         :      /kill 𝐷𝑢̛̀𝑛𝑔 𝑠𝑜̂́\n"
        f"╰━━━〖⨧✧𝐅𝐮𝐥𝐥 𝟐𝟒/𝟕✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "FULL", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
@vip_only
async def handle_ddos(message: Message):
    if await check_command_locked(message, "ddos"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user. id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "DDOS HELP",
            "👼🏻 Cú pháp: /ddos [link web]",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    target_url = params[0].strip()
    if not any(target_url.startswith(proto) for proto in ['http://', 'https://']):
        target_url = 'http://' + target_url

    log_command(user_id, "/ddos", target_url[:50])

    success, pid, _ = run_background_process_sync(
        f"python3 tcp. py {target_url} 1000",
        timeout=TIMEOUT_MEDIUM
    )

    if not success:
        await send_response(
            message,
            "DDOS ERROR",
            "👼🏻 Lỗi khi khởi động lệnh ddos!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━━〖⨧✧✧⩩〗\n"
        f" • Target       :     {escape_html(target_url[:25])}...\n"
        f" • 𝑆𝑜̂́ vòng          :     Liên tục\n"
        f" • Power          :     High Performance\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́                :      V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛        :      {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦              :      {date_str}\n"
        f"╰━━━━〖⨧✧𝗗𝗗𝗢𝗦✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat. id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "DDOS", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
@vip_only
async def handle_tiktok(message: Message):
    if await check_command_locked(message, "tiktok"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "TIKTOK HELP",
            "👼🏻 Cú pháp: /tiktok [link video tiktok]",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    tiktok_link = params[0].strip()

    if not ("tiktok. com" in tiktok_link or "vm.tiktok.com" in tiktok_link):
        await send_response(
            message,
            "TIKTOK ERROR",
            "👼🏻 Link TikTok không hợp lệ!",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    log_command(user_id, "/tiktok", tiktok_link)

    success, pid, _ = run_background_process_sync(
        f"python3 tt.py {tiktok_link} 1000",
        timeout=TIMEOUT_LONG
    )

    if not success:
        await send_response(
            message,
            "TIKTOK ERROR",
            "👼🏻 Lỗi khi khởi động lệnh tiktok!",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━━〖⨧✧✧⩩〗\n"
        f" • Link          :     {escape_html(tiktok_link[:30])}...\n"
        f" • Target          :      1000+ views\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́        :     V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛      :      {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦             :      {date_str}\n"
        f"╰━━━━〖⨧✧𝐓𝐢𝐤𝐓𝐨𝐤✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat. id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "TIKTOK", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
@vip_only
async def handle_ngl(message: Message):
    if await check_command_locked(message, "ngl"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user. id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "NGL HELP",
            "👼🏻 Cú pháp: /ngl [link ngl]",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    ngl_link = params[0]. strip()

    if not ("ngl.link" in ngl_link):
        await send_response(
            message,
            "NGL ERROR",
            "👼🏻 Link NGL không hợp lệ!",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    log_command(user_id, "/ngl", ngl_link)

    success, pid, _ = run_background_process_sync(
        f"python3 spamngl.py {ngl_link} 1000",
        timeout=TIMEOUT_LONG
    )

    if not success:
        await send_response(
            message,
            "NGL ERROR",
            "👼🏻 Lỗi khi khởi động lệnh NGL!",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━━〖⨧✧✧⩩〗\n"
        f" • Link         :     {escape_html(ngl_link[:30])}.. .\n"
        f" • Target           :     1000+ messages\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́        :     V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛      :     {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦             :     {date_str}\n"
        f"╰━━━━〖⨧✧𝐍𝐆𝐋✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat. id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "NGL", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@group_only
async def handle_ping(message: Message):
    if not message.from_user:
        return False
    user = message. from_user
    user_id = user.id

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀ã 𝐼𝐷       :      {user_id}\n\n"
        f"🤖 𝑇𝑟𝑎̣𝑛𝑔 𝑡ℎ𝑎́𝑖 𝐵𝑜𝑡 : ℎ𝑜𝑎̣𝑡 𝑑𝑜̣̂𝑛𝑔 🛰️\n\n"
        f"🚀 𝑆𝐴̆̃𝑁 𝑆𝐴̀𝑁𝐺 𝑁𝐻𝐴̣̂𝑁 𝐿𝐸̣̂𝑁𝐻 !  🎯"
    )

    await send_response(message, "PING", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)
    return True

async def create_router():
    router = Router()

    router.message. register(handle_ping, Command("ping"))
    router.message.register(handle_checkid, Command("checkid"))
    router.message.register(handle_sms, Command("sms"))
    router.message.register(handle_spam, Command("spam"))
    router.message.register(handle_free, Command("free"))
    router.message.register(handle_vip, Command("vip"))
    router.message.register(handle_call, Command("call"))
    router.message.register(handle_ddos, Command("ddos"))
    router.message.register(handle_full, Command("full"))
    router.message.register(handle_tiktok, Command("tiktok"))
    router.message.register(handle_ngl, Command("ngl"))
    router.message.register(handle_kill_process, Command("kill"))
    router.message.register(handle_kill_all_processes, Command("killall"))
    router.message.register(handle_random_image, Command("img"))
    router.message.register(handle_random_video, Command("vid"))
    router.message.register(handle_non_command_message)

    return router
    
@user_cooldown()
@group_only
@admin_only
async def handle_add_vip(message: Message):
    if not message.from_user:
        return False
    user = message.from_user

    params = extract_params(message)

    if len(params) < 1:
        await send_response(
            message,
            "ADD VIP HELP",
            "👼🏻 Cú pháp: /themvip USER_ID [TÊN]",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    target_id = params[0]. strip()
    target_name = " ".join(params[1:]) if len(params) > 1 else "VIP User"

    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO admin (user_id, name, role) VALUES (?, ?, ?)",
            (target_id, target_name, 'vip')
        )
        conn.commit()
        conn.close()

        permission_cache.cache. pop(str(target_id), None)

        log_command(user. id, "/themvip", f"{target_id}")

        content = f"✅ Đã thêm VIP: {target_id}\n👤 Tên: {target_name}"
        await send_response(message, "ADD VIP SUCCESS", content, delete_user_msg=True, keep_forever=True)
        return True

    except Exception as e:
        logger.error(f"Error adding VIP {target_id}: {e}")
        await send_response(
            message,
            "ADD VIP ERROR",
            f"Lỗi khi thêm VIP: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@user_cooldown()
@group_only
@admin_only
async def handle_remove_vip(message: Message):
    if not message.from_user:
        return False
    user = message.from_user

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "REMOVE VIP HELP",
            "👼🏻 Cú pháp: /xoavip USER_ID",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    target_id = params[0].strip()

    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor. execute("DELETE FROM admin WHERE user_id = ?  AND role = 'vip'", (target_id,))
        rows_deleted = cursor.rowcount
        conn.commit()
        conn.close()

        permission_cache.cache.pop(str(target_id), None)

        log_command(user.id, "/xoavip", target_id)

        if rows_deleted > 0:
            content = f"✅ Đã xóa VIP: {target_id}"
        else:
            content = f"⚠️ Không tìm thấy VIP: {target_id}"

        await send_response(message, "REMOVE VIP", content, delete_user_msg=True, keep_forever=True)
        return True

    except Exception as e:
        logger.error(f"Error removing VIP {target_id}: {e}")
        await send_response(
            message,
            "REMOVE VIP ERROR",
            f"Lỗi khi xóa VIP: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@user_cooldown()
@group_only
@admin_only
async def handle_add_admin(message: Message):
    if not message.from_user:
        return False
    user = message.from_user

    params = extract_params(message)

    if len(params) < 1:
        await send_response(
            message,
            "ADD ADMIN HELP",
            "👼🏻 Cú pháp: /themadmin USER_ID [TÊN]",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    target_id = params[0]. strip()
    target_name = " ".join(params[1:]) if len(params) > 1 else "Admin User"

    if target_id == str(user.id):
        await send_response(
            message,
            "ADD ADMIN ERROR",
            "❌ Không thể tự thêm admin cho chính mình! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO admin (user_id, name, role) VALUES (?, ?, ?)",
            (target_id, target_name, 'admin')
        )
        conn.commit()
        conn.close()

        permission_cache.cache.pop(str(target_id), None)

        log_command(user. id, "/themadmin", target_id)

        content = f"✅ Đã thêm Admin: {target_id}\n👤 Tên: {target_name}"
        await send_response(message, "ADD ADMIN SUCCESS", content, delete_user_msg=True, keep_forever=True)
        return True

    except Exception as e:
        logger.error(f"Error adding admin {target_id}: {e}")
        await send_response(
            message,
            "ADD ADMIN ERROR",
            f"Lỗi khi thêm Admin: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@user_cooldown()
@group_only
@admin_only
async def handle_remove_admin(message: Message):
    if not message.from_user:
        return False
    user = message.from_user

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "REMOVE ADMIN HELP",
            "👼🏻 Cú pháp: /xoaadmin USER_ID",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    target_id = params[0].strip()

    if target_id == str(ADMIN_IDS[0]):
        await send_response(
            message,
            "REMOVE ADMIN ERROR",
            "❌ Không thể xóa Super Admin!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    if target_id == str(user.id):
        await send_response(
            message,
            "REMOVE ADMIN ERROR",
            "❌ Không thể tự xóa admin của chính mình!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM admin WHERE user_id = ? AND role = 'admin'", (target_id,))
        rows_deleted = cursor.rowcount
        conn. commit()
        conn.close()

        permission_cache.cache. pop(str(target_id), None)

        log_command(user.id, "/xoaadmin", target_id)

        if rows_deleted > 0:
            content = f"✅ Đã xóa Admin: {target_id}"
        else:
            content = f"⚠️ Không tìm thấy Admin: {target_id}"

        await send_response(message, "REMOVE ADMIN", content, delete_user_msg=True, keep_forever=True)
        return True

    except Exception as e:
        logger.error(f"Error removing admin {target_id}: {e}")
        await send_response(
            message,
            "REMOVE ADMIN ERROR",
            f"Lỗi khi xóa Admin: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@group_only
@admin_only
async def handle_list_vip(message: Message):
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, name, role FROM admin ORDER BY role, user_id")
        admin_list = cursor.fetchall()
        conn.close()

        if not admin_list:
            await send_response(
                message,
                "ADMIN LIST",
                "📋 Chưa có VIP/Admin nào trong hệ thống! ",
                delete_user_msg=True,
                auto_delete_after=15
            )
            return False

        content = "📋 DANH SÁCH VIP & ADMIN:\n\n"

        admin_users = []
        vip_users = []

        for item in admin_list:
            if item['role'] == 'admin':
                admin_users.append(item)
            elif item['role'] == 'vip':
                vip_users.append(item)

        if admin_users:
            content += "👑 ADMIN:\n"
            for i, admin in enumerate(admin_users, 1):
                content += f"  {i}. {admin['name']} ({admin['user_id']})\n"
            content += "\n"

        if vip_users:
            content += "🎖️ VIP:\n"
            for i, vip in enumerate(vip_users, 1):
                content += f"  {i}. {vip['name']} ({vip['user_id']})\n"

        content += f"\nTổng: {len(admin_users)} Admin, {len(vip_users)} VIP"

        await send_response(message, "ADMIN LIST", content, delete_user_msg=True, keep_forever=True)
        return True

    except Exception as e:
        logger.error(f"Error getting admin list: {e}")
        await send_response(
            message,
            "LIST ERROR",
            f"Lỗi khi lấy danh sách: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@user_cooldown()
@group_only
@admin_only
async def handle_broadcast_all(message: Message):
    try:
        params = extract_params(message)

        if not params or not " ".join(params):
            await send_response(
                message,
                "BROADCAST HELP",
                "👼🏻 Cú pháp: /broadcast_all <nội dung>",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        broadcast_text = " ".join(params)

        users_data = await async_db_execute(
            "SELECT user_id FROM users WHERE is_approved = TRUE"
        )
        group_ids = await get_all_group_ids()

        if users_data is None:
            await send_response(
                message,
                "BROADCAST ERROR",
                "❌ Không thể lấy danh sách user",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        all_user_ids = [u[0] for u in users_data]
        total_targets = len(all_user_ids) + len(group_ids)

        if total_targets == 0:
            await send_response(
                message,
                "BROADCAST ERROR",
                "❌ Không có user/nhóm nào để gửi",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        send_msg = await send_response(
            message,
            "BROADCAST",
            f"📢 Đang gửi tới {len(all_user_ids)} user và {len(group_ids)} nhóm.. .",
            delete_user_msg=False
        )

        send_count = 0
        fail_count = 0

        safe_text = escape_markdown_v2(broadcast_text)
        time_str, date_str = get_vietnam_time()
        safe_time = escape_markdown_v2(time_str)

        formatted_text = (
            f"┏ 📢 *THÔNG BÁO ADMIN* ┓\n"
            f"┣{chr(8213)*20}\n"
            f"┣ {safe_text}\n"
            f"┣{chr(8213)*20}\n"
            f"┗ ⏱️ *{safe_time}*"
        )

        target_ids = list(all_user_ids) + list(group_ids)
        random.shuffle(target_ids)

        for target_id in target_ids:
            try:
                await bot_aiogram.send_message(
                    target_id,
                    formatted_text,
                    parse_mode="MarkdownV2"
                )
                send_count += 1
                await asyncio.sleep(0.15)
            except TelegramForbiddenError:
                fail_count += 1
                if target_id < 0:
                    try:
                        await async_db_execute(
                            "DELETE FROM groups WHERE chat_id = ?",
                            (target_id,)
                        )
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"Error broadcasting to {target_id}: {e}")
                fail_count += 1

        log_command(message.from_user.id, "/broadcast_all", f"{send_count}/{total_targets}")

        result_content = f"✅ Đã gửi: **{send_count}/{total_targets}**\n❌ Lỗi: {fail_count}"

        await send_response(
            message,
            "BROADCAST RESULT",
            result_content,
            processing_msg=send_msg,
            delete_user_msg=False,
            keep_forever=True
        )

        return True

    except Exception as e:
        logger.error(f"Error broadcasting: {e}", exc_info=True)
        await send_response(
            message,
            "BROADCAST ERROR",
            f"Lỗi broadcast: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@group_only
@admin_only
async def handle_add_group(message: Message):
    try:
        params = extract_params(message)

        if not params:
            await send_response(
                message,
                "ADD GROUP HELP",
                "👼🏻 Cú pháp: /addgr <chat_id>\n(ID nhóm thường bắt đầu bằng dấu -)",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        chat_id_str = params[0]. strip()

        if not chat_id_str. startswith('-') or not chat_id_str[1:].isdigit():
            await send_response(
                message,
                "ADD GROUP ERROR",
                "⚠️ ID nhóm chat không hợp lệ (thường bắt đầu bằng dấu -)",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        chat_id = int(chat_id_str)

        try:
            chat_info = await bot_aiogram.get_chat(chat_id)
            if chat_info.type not in ['group', 'supergroup']:
                await send_response(
                    message,
                    "ADD GROUP ERROR",
                    f"⚠️ ID {chat_id} không phải là nhóm hoặc siêu nhóm",
                    delete_user_msg=True,
                    auto_delete_after=8
                )
                return False
        except Exception as e:
            await send_response(
                message,
                "ADD GROUP ERROR",
                f"❌ Không thể lấy thông tin nhóm {chat_id}.  Bot đã ở trong nhóm chưa?\nLỗi: {e}",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        result = await async_db_execute(
            "INSERT OR IGNORE INTO groups (chat_id) VALUES (?)",
            (chat_id,)
        )

        if result is not None:
            check = await async_db_fetchone(
                "SELECT 1 FROM groups WHERE chat_id = ?",
                (chat_id,)
            )
            if check:
                log_command(message.from_user.id, "/addgr", str(chat_id))
                content = f"✅ Đã thêm nhóm chat ID: `{chat_id}` vào danh sách broadcast"
                await send_response(message, "ADD GROUP SUCCESS", content, delete_user_msg=True, keep_forever=True)
                return True
            else:
                await send_response(
                    message,
                    "ADD GROUP ERROR",
                    f"❌ Không thể thêm nhóm {chat_id}",
                    delete_user_msg=True,
                    auto_delete_after=8
                )
                return False
        else:
            await send_response(
                message,
                "ADD GROUP ERROR",
                f"❌ Lỗi DB khi thêm nhóm {chat_id}",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

    except ValueError:
        await send_response(
            message,
            "ADD GROUP ERROR",
            "⚠️ ID nhóm không hợp lệ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    except Exception as e:
        logger.error(f"Error adding group: {e}", exc_info=True)
        await send_response(
            message,
            "ADD GROUP ERROR",
            f"❌ Lỗi: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@group_only
@admin_only
async def handle_remove_group(message: Message):
    try:
        params = extract_params(message)

        if not params:
            await send_response(
                message,
                "DEL GROUP HELP",
                "👼🏻 Cú pháp: /delgr <chat_id>",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        chat_id_str = params[0].strip()

        if not chat_id_str.startswith('-') or not chat_id_str[1:].isdigit():
            await send_response(
                message,
                "DEL GROUP ERROR",
                "⚠️ ID nhóm chat không hợp lệ",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        chat_id = int(chat_id_str)

        result = await async_db_execute(
            "DELETE FROM groups WHERE chat_id = ?",
            (chat_id,)
        )

        if result is not None:
            check = await async_db_fetchone(
                "SELECT 1 FROM groups WHERE chat_id = ?",
                (chat_id,)
            )
            if not check:
                log_command(message.from_user.id, "/delgr", str(chat_id))
                content = f"✅ Đã xóa nhóm chat ID: `{chat_id}` khỏi danh sách"
                await send_response(message, "DEL GROUP SUCCESS", content, delete_user_msg=True, keep_forever=True)
                return True
            else:
                await send_response(
                    message,
                    "DEL GROUP ERROR",
                    f"❌ Không thể xóa nhóm {chat_id}",
                    delete_user_msg=True,
                    auto_delete_after=8
                )
                return False
        else:
            await send_response(
                message,
                "DEL GROUP ERROR",
                f"❌ Lỗi DB khi xóa nhóm {chat_id}",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

    except ValueError:
        await send_response(
            message,
            "DEL GROUP ERROR",
            "⚠️ ID nhóm không hợp lệ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    except Exception as e:
        logger.error(f"Error removing group: {e}", exc_info=True)
        await send_response(
            message,
            "DEL GROUP ERROR",
            f"❌ Lỗi: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@group_only
@admin_only
async def handle_list_groups(message: Message):
    try:
        groups_data = await async_db_execute(
            "SELECT chat_id FROM groups ORDER BY chat_id ASC"
        )

        if groups_data is None:
            await send_response(
                message,
                "LIST GROUPS ERROR",
                "❌ Lỗi lấy danh sách nhóm",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        if not groups_data:
            await send_response(
                message,
                "LIST GROUPS",
                "📋 Chưa có nhóm nào được thêm.  Dùng `/addgr`",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        content = f"📋 Tổng {len(groups_data)} nhóm đã thêm:\n\n"
        for row in groups_data:
            content += f"- `{row[0]}`\n"

        if len(content) > 3500:
            content = content[:3500] + "\n...  (Quá dài)"

        log_command(message.from_user.id, "/allgr", "list_groups")

        await send_response(
            message,
            "LIST GROUPS",
            content,
            delete_user_msg=True,
            keep_forever=True
        )
        return True

    except Exception as e:
        logger.error(f"Error listing groups: {e}")
        await send_response(
            message,
            "LIST GROUPS ERROR",
            f"❌ Lỗi: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

async def handle_start(message: Message):
    if not message.from_user:
        return False

    user = message.from_user
    user_id = user.id
    is_admin_flag = is_admin(user_id)

    user_info = await get_user(user_id, user. username)
    if not user_info:
        await send_response(
            message,
            "ERROR",
            "Lỗi: Không thể tải thông tin tài khoản. Vui lòng thử lại.",
            delete_user_msg=False
        )
        return False

    mention = get_user_mention(user)
    status = "Admin 👑" if user_info["is_admin"] else ("Thành viên ⭐" if user_info["is_approved"] else "Khách ⚠️")

    menu_text = (
        f"🎯 Chào mừng {mention} đến với Bot Tổng Hợp (Premium VIP)!\n"
        f"Bot quản lý bởi @{OWNER_USERNAME}.\n\n"
        f"👤 *TÀI KHOẢN:*\n"
        f"   - Status: **{status}**\n"
        f"   - Số dư: **{user_info['balance']:,}** VNĐ 💵\n"
    )

    if user_info["is_approved"]:
        menu_text += (
            f"\n🔥 *LỆNH CÔNG KHAI:*\n"
            f"   • `/ping` - Xem trạng thái Bot\n"
            f"   • `/checkid` - Xem thông tin ID\n"
            f"   • `/sms` - Gửi SMS 50 số\n"
            f"   • `/spam` - Spam liên tục\n"
            f"   • `/free` - Spam SMS Zalo\n"
            f"\n💫 *VIP PERMANENT:*\n"
            f"   • `/vip` - SMS + Call 10 số/lần\n"
            f"   • `/call` - Gọi 1 số\n"
            f"   • `/ddos` - Đánh sập Web\n"
            f"   • `/full` - Chạy Full 24h\n"
            f"   • `/tiktok` - Tăng View TikTok\n"
            f"   • `/ngl` - Spam NGL\n"
            f"   • `/img` - Random ảnh\n"
            f"   • `/vid` - Random video\n"
            f"   • `/kill` - Dừng lệnh"
        )
    else:
        menu_text += (
            f"\n⚠️ *Tài khoản chưa duyệt.*\n"
            f"Liên hệ Admin @{OWNER_USERNAME} (ID: `{user_id}`) để kích hoạt + `{START_BALANCE:,}` VNĐ."
        )

    if is_admin_flag:
        menu_text += (
            f"\n\n👑 *ADMIN MENU:*\n"
            f"   • `/themvip` - Thêm VIP\n"
            f"   • `/xoavip` - Xóa VIP\n"
            f"   • `/themadmin` - Thêm Admin\n"
            f"   • `/xoaadmin` - Xóa Admin\n"
            f"   • `/listvip` - Danh sách VIP/Admin\n"
            f"   • `/addgr` - Thêm nhóm\n"
            f"   • `/delgr` - Xóa nhóm\n"
            f"   • `/allgr` - Danh sách nhóm\n"
            f"   • `/broadcast_all` - Gửi tin nhắn toàn bộ\n"
            f"   • `/killall` - Dừng tất cả lệnh"
        )

    keyboard = None
    if is_admin_flag:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="👑 ADMIN CONTROL",
                url=f"https://t.me/{OWNER_USERNAME}"
            )]
        ])
    elif not user_info["is_approved"]:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"Liên hệ @{OWNER_USERNAME}",
                url=f"https://t.me/{OWNER_USERNAME}"
            )]
        ])

    try:
        await bot_aiogram.send_message(
            chat_id=message.chat.id,
            text=menu_text,
            parse_mode="Markdown",
            reply_markup=keyboard,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Error sending start message: {e}")

    return True

@user_cooldown()
@group_only
async def handle_nap(message: Message):
    if not message.from_user:
        return False

    user_info = await get_user(message. from_user.id)
    if not user_info:
        await send_response(
            message,
            "ERROR",
            "Lỗi: Không thể lấy thông tin tài khoản",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    random_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    username = user_info["username"] if user_info["username"] else f"user_{user_info['user_id']}"

    nap_text = (
        f"**1.  THÔNG TIN CK:**\n{BANK_INFO}\n\n"
        f"**2. NỘI DUNG CK (BẮT BUỘC):**\n   `NAP {username. upper()} {random_code}`\n\n"
        f"**3. MÃ QR:** [Bấm xem ảnh QR]({QR_CODE_IMAGE_URL})\n\n"
        f"**4. XÁC NHẬN:** Sau khi CK, dùng: `/nap_request <số tiền>`\n\n"
        f"💰 *Số dư hiện tại*: **{user_info['balance']:,}** VNĐ.\n\n"
        f"*{random. choice(RANDOM_THANKS)}*"
    )

    try:
        await bot_aiogram.send_message(
            message.chat.id,
            nap_text,
            parse_mode="Markdown",
            disable_web_page_preview=False
        )
    except Exception as e:
        await send_response(
            message,
            "NAP ERROR",
            f"Không thể hiển thị thông tin.  Lỗi: {e}",
            delete_user_msg=False
        )

    log_command(message. from_user.id, "/nap", "request_info")
    return True

@user_cooldown()
@group_only
async def handle_nap_request(message: Message):
    if not message.from_user:
        return False

    user_info = await get_user(message.from_user.id)
    if not user_info:
        await send_response(
            message,
            "ERROR",
            "Lỗi: Không thể lấy thông tin tài khoản",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    params = extract_params(message)

    if not params:
        await send_response(
            message,
            "NAP REQUEST HELP",
            "Cú pháp: `/nap_request <số tiền>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    try:
        amount = int(params[0])
        if amount <= 0:
            raise ValueError("Số tiền phải lớn hơn 0")
    except (ValueError, IndexError):
        await send_response(
            message,
            "NAP REQUEST ERROR",
            "Số tiền không hợp lệ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    try:
        await async_db_execute(
            "INSERT INTO nap_request (user_id, amount) VALUES (?, ?)",
            (user_info["user_id"], amount)
        )

        log_command(message.from_user.id, "/nap_request", str(amount))

        content = f"✅ Đã gửi yêu cầu nạp **{amount:,}** VNĐ.\n⏳ Chờ Admin duyệt."

        await send_response(
            message,
            "NAP REQUEST SENT",
            content,
            delete_user_msg=True,
            keep_forever=True
        )

        for admin_id in ADMIN_IDS:
            try:
                admin_msg = (
                    f"🔔 YÊU CẦU NẠP TIỀN MỚI:\n"
                    f"User: `{user_info['user_id']}` (@{user_info['username']})\n"
                    f"Số tiền: **{amount:,}** VNĐ\n"
                    f"Dùng: `/duyet_nap <request_id>`"
                )
                await bot_aiogram.send_message(
                    admin_id,
                    admin_msg,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.warning(f"Error notifying admin {admin_id}: {e}")

        return True

    except Exception as e:
        logger.error(f"Error processing nap request: {e}")
        await send_response(
            message,
            "NAP REQUEST ERROR",
            f"Lỗi xử lý yêu cầu: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

async def update_router_with_handlers(router: Router):
    router.message. register(handle_start, Command("start"))
    router.message. register(handle_add_vip, Command("themvip"))
    router.message.register(handle_remove_vip, Command("xoavip"))
    router.message.register(handle_add_admin, Command("themadmin"))
    router.message.register(handle_remove_admin, Command("xoaadmin"))
    router.message.register(handle_list_vip, Command("listvip"))
    router.message.register(handle_add_group, Command("addgr"))
    router.message.register(handle_remove_group, Command("delgr"))
    router.message.register(handle_list_groups, Command("allgr"))
    router.message.register(handle_broadcast_all, Command("broadcast_all"))
    router.message.register(handle_nap, Command("nap"))
    router.message.register(handle_nap_request, Command("nap_request"))

    return router
    
@user_cooldown()
@group_only
async def handle_ask_ai(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or not " ". join(params):
        await send_response(
            message,
            "ASK AI HELP",
            "Cú pháp: `/ask <câu hỏi>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    query = " ".join(params). strip()
    
    msg = await send_response(
        message,
        "AI PROCESSING",
        f"⏳ Đang xử lý: `{query[:50]}...`",
        delete_user_msg=False
    )
    
    try:
        quoted_query = quote(query)
        data = await asyncio.to_thread(
            get_api_result_sync,
            f"{API_SEARCH_BASE}?chat={quoted_query}"
        )
        
        if not data. get("ok"):
            await send_response(
                message,
                "AI ERROR",
                f"❌ {data.get('error', 'Không rõ')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        response_text = data.get("text", "_Không có nội dung._")
        
        if len(response_text) > 3500:
            response_text = response_text[:3500] + "\n.. .(Đã cắt bớt)"
        
        log_command(user_id, "/ask", query[:50])
        
        await send_response(
            message,
            "AI RESPONSE",
            response_text,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error in AI query: {e}")
        await send_response(
            message,
            "AI ERROR",
            f"❌ Lỗi kết nối: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_md5_prediction(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if len(params) != 1 or not re.fullmatch(r"^[0-9a-f]{32}$", params[0]. lower()):
        await send_response(
            message,
            "MD5 HELP",
            "Cú pháp: `/tx <md5_hash_32_ký_tự>`\n\nVí dụ: `/tx 5d41402abc4b2a76b9719d911017c592`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    md5_hash = params[0]. strip(). lower()
    
    msg = await send_response(
        message,
        "MD5 PROCESSING",
        f"🔮 Đang giải mã: `{md5_hash}`.. .",
        delete_user_msg=False
    )
    
    try:
        md5_analysis = await asyncio.to_thread(predict_md5_logic, md5_hash)
        
        if not md5_analysis.get("ok"):
            await send_response(
                message,
                "MD5 ERROR",
                f"❌ Lỗi: {md5_analysis.get('error', 'Không rõ')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        emoji_dice = {1: '⚀', 2: '⚁', 3: '⚂', 4: '⚃', 5: '⚄', 6: '⚅'}
        dice_display = f"{emoji_dice[md5_analysis['dice'][0]]} {emoji_dice[md5_analysis['dice'][1]]} {emoji_dice[md5_analysis['dice'][2]]}"
        seed_next_hex = f"{md5_analysis['seed_next']:08X}"
        
        result_card = (
            f"🔑 *MD5 Đầu Vào:* `{md5_hash}`\n\n"
            f"**🔬 PHÂN TÍCH THUẬT TOÁN (LCG v2. 0):**\n"
            f"   • Seed Hiện Tại: `{md5_hash[:8]}`\n"
            f"   • Seed Tiếp Theo: `{seed_next_hex}`\n"
            f"   • MD5 Vòng Sau (Dự đoán): `{md5_analysis['predicted_md5']}`\n\n"
            f"🎲 *DỰ ĐOÁN XÚC XẮC (Vòng Sau)*:\n"
            f"   - Xúc Xắc: **{dice_display}**\n"
            f"   - Tổng Điểm: **{md5_analysis['total']}**\n"
            f"   - **KẾT QUẢ:** **{md5_analysis['result']}** 🥇"
        )
        
        log_command(user_id, "/tx", md5_hash[:16])
        
        await send_response(
            message,
            "MD5 RESULT",
            result_card,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error in MD5 prediction: {e}")
        await send_response(
            message,
            "MD5 ERROR",
            f"❌ Lỗi xử lý: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_qrcode(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or not " ".join(params):
        await send_response(
            message,
            "QRCODE HELP",
            "Cú pháp: `/qrcode <nội dung>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    if not TTS_QR_AVAILABLE or not qrcode:
        await send_response(
            message,
            "QRCODE ERROR",
            "⚠️ Thiếu thư viện `qrcode`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    text = " ".join(params)
    
    msg = await send_response(
        message,
        "QRCODE PROCESSING",
        f"🔳 Đang tạo mã QR.. .",
        delete_user_msg=False
    )
    
    try:
        qr_data = await asyncio.to_thread(generate_qr_code_sync, text)
        
        if isinstance(qr_data, str):
            await send_response(
                message,
                "QRCODE ERROR",
                f"❌ {qr_data}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/qrcode", text[:50])
        
        try:
            await bot_aiogram.send_photo(
                message.chat.id,
                qr_data,
                caption=f"✅ *Mã QR cho:* `{escape_markdown_v2(text[:50])}...`",
                parse_mode="MarkdownV2"
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat.id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except Exception as e:
            await send_response(
                message,
                "QRCODE ERROR",
                f"❌ Không thể gửi QR: {str(e)}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating QR code: {e}")
        await send_response(
            message,
            "QRCODE ERROR",
            f"❌ Lỗi tạo QR: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_voice(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not TTS_QR_AVAILABLE or not gTTS:
        await send_response(
            message,
            "VOICE ERROR",
            "⚠️ Thiếu thư viện `gTTS`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    if not params or not " ".join(params):
        await send_response(
            message,
            "VOICE HELP",
            "Cú pháp: `/voice <văn bản>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    text = " ".join(params)
    
    msg = await send_response(
        message,
        "VOICE PROCESSING",
        "🎤 Đang tạo giọng nói...",
        delete_user_msg=False
    )
    
    try:
        audio_data = await asyncio.to_thread(text_to_speech_sync, text)
        
        if isinstance(audio_data, str):
            await send_response(
                message,
                "VOICE ERROR",
                f"❌ {audio_data}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/voice", text[:50])
        
        try:
            await bot_aiogram.send_voice(
                message.chat. id,
                audio_data,
                caption=f"🗣️ *Văn bản:* `{escape_markdown_v2(text[:50])}... `",
                parse_mode="MarkdownV2"
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat.id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except Exception as e:
            await send_response(
                message,
                "VOICE ERROR",
                f"❌ Không thể gửi Voice: {str(e)}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating voice: {e}")
        await send_response(
            message,
            "VOICE ERROR",
            f"❌ Lỗi tạo Voice: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_weather(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    location = " ".join(params) if params else random. choice(["Hà Nội", "Hồ Chí Minh", "Đà Nẵng"])
    
    msg = await send_response(
        message,
        "WEATHER PROCESSING",
        f"🌤️ Đang lấy thời tiết cho: `{location}`.. .",
        delete_user_msg=False
    )
    
    try:
        geo_response = requests.get(
            f"https://geocoding-api.open-meteo.com/v1/search?name={location}&count=1&language=vi&format=json",
            timeout=REQUEST_TIMEOUT
        )
        geo_data = geo_response.json()
        
        if not geo_data.get("results"):
            await send_response(
                message,
                "WEATHER ERROR",
                f"❌ Không tìm thấy địa điểm: `{location}`",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        result = geo_data["results"][0]
        lat, lon = result["latitude"], result["longitude"]
        location_name = result["name"]
        
        tomorrow_data = await asyncio.to_thread(
            get_api_result_sync,
            f"https://api.tomorrow.io/v4/weather/forecast?location={lat},{lon}&apikey={TOMORROW_API_KEY}"
        )
        
        weather_data = await asyncio.to_thread(
            get_api_result_sync,
            f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric&lang=vi"
        )
        
        weather_api_data = await asyncio.to_thread(
            get_api_result_sync,
            f"http://api.weatherapi.com/v1/forecast.json?key={WEATHERAPI_KEY}&q={lat},{lon}&days=1&aqi=yes&lang=vi"
        )
        
        if weather_data.get("status") is False:
            await send_response(
                message,
                "WEATHER ERROR",
                f"❌ Không thể lấy dữ liệu thời tiết",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        current = weather_data. get("main", {})
        weather_info = weather_data.get("weather", [{}])[0]
        wind = weather_data.get("wind", {})
        
        def get_uv_level(index):
            if index <= 2:
                return "Thấp"
            elif index <= 5:
                return "Trung bình"
            elif index <= 7:
                return "Cao"
            elif index <= 10:
                return "Rất cao"
            return "Nguy hiểm"
        
        def get_wind_direction(degrees):
            directions = ["Bắc", "Đông Bắc", "Đông", "Đông Nam", "Nam", "Tây Nam", "Tây", "Tây Bắc"]
            return directions[round(degrees / 45) % 8]
        
        uv_index = weather_api_data. get("current", {}).get("uv", 0) if weather_api_data.get("status") else 0
        
        content = (
            f"📍 *Địa điểm:* {location_name. upper()}\n"
            f"🌡️ *Nhiệt độ:* {current.get('temp', 'N/A')}°C (Cảm giác: {current.get('feels_like', 'N/A')}°C)\n"
            f"☁️ *Thời tiết:* {weather_info.get('description', 'N/A'). capitalize()}\n"
            f"💧 *Độ ẩm:* {current.get('humidity', 'N/A')}%\n"
            f"💨 *Gió:* {wind.get('speed', 'N/A')} m/s ({get_wind_direction(wind.get('deg', 0))})\n"
            f"☀️ *UV Index:* {uv_index} ({get_uv_level(uv_index)})\n"
            f"👁️ *Tầm nhìn:* {weather_data.get('visibility', 0) / 1000} km\n"
            f"🔽 *Áp suất:* {current.get('pressure', 'N/A')} hPa"
        )
        
        log_command(user_id, "/weather", location[:30])
        
        await send_response(
            message,
            "WEATHER REPORT",
            content,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error getting weather: {e}")
        await send_response(
            message,
            "WEATHER ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_xoso(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    msg = await send_response(
        message,
        "XOSO PROCESSING",
        "🎟️ Đang lấy KQXS Miền Bắc...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_XOSO_URL
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "XOSO ERROR",
                f"❌ {data.get('message', 'Lỗi API')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        result = data.get("result", "Không có dữ liệu")
        
        log_command(user_id, "/kqxs", "XSMB")
        
        await send_response(
            message,
            "KQXS MIỀN BẮC",
            result if isinstance(result, str) else json.dumps(result, ensure_ascii=False),
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error getting XOSO: {e}")
        await send_response(
            message,
            "XOSO ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_ip_lookup(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params:
        await send_response(
            message,
            "IP LOOKUP HELP",
            "Cú pháp: `/ip <địa_chỉ_IP>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    ip_address = params[0].strip()
    
    msg = await send_response(
        message,
        "IP LOOKUP PROCESSING",
        f"🌐 Đang tra cứu IP: `{ip_address}`...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            IPLOOKUP_API. format(ip=ip_address)
        )
        
        if not data.get("status") or data.get("message") != "success":
            await send_response(
                message,
                "IP LOOKUP ERROR",
                f"❌ IP không tồn tại hoặc lỗi API",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        ip_data = data.get("data", {})
        
        content = (
            f"🌐 *IP:* `{ip_data.get('query', 'N/A')}`\n"
            f"📊 *Loại:* {ip_data. get('type', 'N/A')}\n"
            f"🏳️ *Quốc gia:* {ip_data.get('country', 'N/A')} ({ip_data.get('countryCode', 'N/A')})\n"
            f"🏙️ *Thành phố:* {ip_data. get('city', 'N/A')}, {ip_data.get('regionName', 'N/A')}\n"
            f"📌 *Tọa độ:* {ip_data. get('lat', 'N/A')}, {ip_data.get('lon', 'N/A')}\n"
            f"🏢 *ISP:* {ip_data. get('isp', 'N/A')}\n"
            f"🏭 *Organization:* {ip_data.get('org', 'N/A')}\n"
            f"🕰️ *Múi giờ:* {ip_data.get('timezone', 'N/A')}\n"
            f"💰 *Tiền tệ:* {ip_data.get('currency', 'N/A')}"
        )
        
        log_command(user_id, "/ip", ip_address)
        
        await send_response(
            message,
            "IP LOOKUP RESULT",
            content,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error in IP lookup: {e}")
        await send_response(
            message,
            "IP LOOKUP ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_facebook_info(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or not params[0]. isdigit():
        await send_response(
            message,
            "FB INFO HELP",
            "Cú pháp: `/fb <UID_Facebook>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    fb_uid = params[0].strip()
    
    msg = await send_response(
        message,
        "FB PROCESSING",
        f"🔍 Đang tìm UID: `{fb_uid}`...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_FB_INFO.format(uid=fb_uid)
        )
        
        if not data. get("status"):
            await send_response(
                message,
                "FB ERROR",
                f"❌ {data.get('message', 'Không tìm thấy')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        fb_data = data.get("data", {})
        profile_url = fb_data.get("profile_url", f"https://www.facebook.com/{fb_uid}")
        
        content = (
            f"👤 *Tên:* {fb_data.get('name', 'N/A')}\n"
            f"🆔 *UID:* `{fb_data.get('uid', 'N/A')}`\n"
            f"✅ *Verified:* {'Có ✓' if fb_data.get('is_verified') else 'Không'}\n"
            f"👥 *Followers:* `{fb_data.get('followers', 'N/A')}`\n"
            f"🔗 [Xem Profile]({profile_url})"
        )
        
        photo_sent = False
        if fb_data.get("avatar"):
            try:
                await bot_aiogram.send_photo(
                    message.chat. id,
                    fb_data. get("avatar")
                )
                photo_sent = True
            except Exception as e:
                logger.warning(f"Error sending FB avatar: {e}")
        
        log_command(user_id, "/fb", fb_uid)
        
        await send_response(
            message,
            "FACEBOOK INFO",
            content,
            processing_msg=msg if not photo_sent else None,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger. error(f"Error getting Facebook info: {e}")
        await send_response(
            message,
            "FB ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_tiktok_info(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params:
        await send_response(
            message,
            "TT INFO HELP",
            "Cú pháp: `/tt <username_TikTok>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    tt_username = params[0].strip(). replace("@", "")
    
    msg = await send_response(
        message,
        "TT PROCESSING",
        f"🔍 Đang tìm TikTok: `@{tt_username}`...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_TT_INFO. format(username=tt_username)
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "TT ERROR",
                f"❌ {data.get('message', 'Không tìm thấy')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        tt_data = data.get("data", {})
        profile_url = f"https://www.tiktok.com/@{tt_username}"
        
        content = (
            f"👤 *Tên:* {tt_data.get('nickname', 'N/A')}\n"
            f"🔗 *Username:* `@{tt_data.get('username', 'N/A')}`\n"
            f"✅ *Verified:* {'Có ✓' if tt_data.get('verified') else 'Không'}\n"
            f"👥 *Followers:* `{tt_data.get('followerCount', 'N/A')}`\n"
            f"➡️ *Following:* `{tt_data. get('followingCount', 'N/A')}`\n"
            f"🎥 *Videos:* `{tt_data.get('totalVideos', 'N/A')}`\n"
            f"❤️ *Likes:* `{tt_data.get('totalFavorite', 'N/A')}`\n"
            f"📝 *Bio:* _{tt_data.get('signature', 'N/A')}_\n"
            f"🔗 [Xem Profile]({profile_url})"
        )
        
        photo_sent = False
        if tt_data. get("avatar"):
            try:
                await bot_aiogram. send_photo(
                    message.chat.id,
                    tt_data.get("avatar")
                )
                photo_sent = True
            except Exception as e:
                logger.warning(f"Error sending TT avatar: {e}")
        
        log_command(user_id, "/tt", tt_username)
        
        await send_response(
            message,
            "TIKTOK INFO",
            content,
            processing_msg=msg if not photo_sent else None,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error getting TikTok info: {e}")
        await send_response(
            message,
            "TT ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

def extract_params(message: Message) -> List[str]:
    if not message.text:
        return []
    
    parts = message.text.split()
    return parts[1:] if len(parts) > 1 else []

async def send_response(
    message: Message,
    title: str,
    text: str,
    processing_msg: Optional[Message] = None,
    delete_user_msg: bool = True,
    auto_delete_after: int = 0,
    keep_forever: bool = False,
    with_keyboard: bool = False
) -> Optional[Message]:
    try:
        current_time = get_vietnam_time()
        time_str = f"{current_time[0]} | {current_time[1]}"
        
        safe_title = escape_markdown_v2(title. upper())
        text_limit = 1000 - len(title) - len(time_str) - 100
        safe_text = escape_markdown_v2(text[:text_limit] + ('...' if len(text) > text_limit else ''))
        safe_time = escape_markdown_v2(time_str)
        safe_owner = escape_markdown_v2(OWNER_USERNAME)
        
        formatted_caption = (
            f"┏ 💎 *{safe_title}* ┓\n"
            f"┣{chr(8213)*20}\n"
            f"┣ {safe_text}\n"
            f"┣{chr(8213)*20}\n"
            f"┗ ⏱️ *{safe_time}* \\| Bot by {safe_owner}"
        )
        
        keyboard = create_group_link_keyboard() if with_keyboard else None
        
        if processing_msg:
            try:
                await bot_aiogram.delete_message(
                    chat_id=processing_msg.chat.id,
                    message_id=processing_msg.message_id
                )
            except Exception:
                pass
        
        if delete_user_msg:
            try:
                await bot_aiogram.delete_message(
                    chat_id=message.chat.id,
                    message_id=message.message_id
                )
            except Exception:
                pass
        
        sent_message = await bot_aiogram.send_message(
            chat_id=message.chat.id,
            text=formatted_caption,
            parse_mode="MarkdownV2",
            reply_markup=keyboard
        )
        
        if auto_delete_after > 0 and not keep_forever:
            asyncio.create_task(
                auto_delete_message(
                    sent_message.chat.id,
                    sent_message.message_id,
                    auto_delete_after
                )
            )
        
        return sent_message
        
    except Exception as e:
        logger.error(f"Error sending response: {e}")
        return None

async def auto_delete_message(chat_id: int, message_id: int, delay: int):
    try:
        await asyncio.sleep(delay)
        await bot_aiogram.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.debug(f"Error auto-deleting message: {e}")

async def cleanup_full_status_safe():
    if 'FULL_STATUS' not in globals() or 'FULL_LOCK' not in globals():
        return
    
    try:
        current_time = time.time()
        keys_to_remove = []
        
        with FULL_LOCK:
            keys_to_remove = [
                k for k, v in FULL_STATUS.items()
                if v < current_time - 3600
            ]
        
        if keys_to_remove:
            batch_size = 50
            removed_total = 0
            
            for i in range(0, len(keys_to_remove), batch_size):
                batch = keys_to_remove[i:i + batch_size]
                with FULL_LOCK:
                    for key in batch:
                        FULL_STATUS.pop(key, None)
                        removed_total += 1
                
                if i + batch_size < len(keys_to_remove):
                    await asyncio.sleep(0.01)
            
            logger.info(f"🧹 Deleted {removed_total} old entries from FULL_STATUS")
    
    except Exception as e:
        logger.error(f"Error cleanup FULL_STATUS: {e}")

def create_router() -> Router:
    router = Router()
    
    router.message. register(handle_start, Command("start"))
    router.message.register(handle_ping, Command("ping"))
    router.message.register(handle_checkid, Command("checkid"))
    router.message.register(handle_vip, Command("vip"))
    router.message.register(handle_call, Command("call"))
    router.message.register(handle_kill_process, Command("kill"))
    router.message.register(handle_kill_all_processes, Command("killall"))
    router.message.register(handle_random_image, Command("img"))
    router.message.register(handle_random_video, Command("vid"))
    router.message.register(handle_add_vip, Command("themvip"))
    router.message. register(handle_remove_vip, Command("xoavip"))
    router.message. register(handle_add_admin, Command("themadmin"))
    router.message.register(handle_remove_admin, Command("xoaadmin"))
    router.message.register(handle_list_vip, Command("listvip"))
    router.message.register(handle_add_group, Command("addgr"))
    router.message.register(handle_remove_group, Command("delgr"))
    router.message.register(handle_list_groups, Command("allgr"))
    router.message. register(handle_broadcast_all, Command("broadcast_all"))
    router.message. register(handle_nap, Command("nap"))
    router.message.register(handle_nap_request, Command("nap_request"))
    router.message.register(handle_ask_ai, Command("ask"))
    router.message.register(handle_md5_prediction, Command("tx"))
    router.message.register(handle_qrcode, Command("qrcode"))
    router.message. register(handle_voice, Command("voice"))
    router. message.register(handle_weather, Command("weather"))
    router.message.register(handle_xoso, Command("kqxs"))
    router.message.register(handle_ip_lookup, Command("ip"))
    router.message.register(handle_facebook_info, Command("fb"))
    router.message.register(handle_tiktok_info, Command("tt"))
    router.message.register(handle_non_command_message)
    
    return router

async def handle_ping(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)
    
    content = (
        f"{permission_title}\n"
        f"┃• 💼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"\n🤖 Trạng thái Bot: hoạt động 🛰️\n\n"
        f"🚀 Sẵn sàng nhận lệnh!"
    )
    
    await send_response(
        message,
        "BOT STATUS",
        content,
        delete_user_msg=True,
        keep_forever=True,
        with_keyboard=True
    )
    
    log_command(user_id, "/ping", "status_check")
    return True
    
async def handle_soundcloud_search(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or not " ". join(params):
        await send_response(
            message,
            "SOUNDCLOUD HELP",
            "Cú pháp: `/scl <tên_bài_hát>`\nVí dụ: `/scl son tung mtp`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    query = " ". join(params). strip()
    
    msg = await send_response(
        message,
        "SOUNDCLOUD SEARCH",
        f"🔍 Đang tìm: `{query}`.. .",
        delete_user_msg=False
    )
    
    try:
        cid = await asyncio.to_thread(get_client_id)
        ctime = str(int(time.time()))
        
        path = "/api/v2/search"
        params_dict = {
            "q": query,
            "type": "song",
            "count": 10,
            "ctime": ctime,
            "version": ZING_VERSION,
            "apiKey": ZING_API_KEY
        }
        
        search_results = await asyncio.to_thread(
            requests.get,
            f"https://api-v2.soundcloud.com/search/tracks",
            params={
                "q": query,
                "client_id": cid,
                "limit": 10,
                "offset": 0,
                "app_locale": "en"
            },
            timeout=REQUEST_TIMEOUT,
            headers=SC_HEADERS
        )
        
        search_results. raise_for_status()
        search_data = search_results.json()
        
        tracks = []
        for item in search_data.get("collection", []):
            user_info = item.get("user", {})
            track = {
                "id": item. get("id"),
                "title": item.get("title", "Unknown"),
                "duration": item.get("full_duration") or item.get("duration", 0),
                "permalink_url": item.get("permalink_url"),
                "artwork_url": item.get("artwork_url"),
                "artist": user_info.get("username", "Unknown"),
                "likes": item.get("likes_count", 0),
                "plays": item.get("playback_count", 0),
                "genre": item.get("genre", "Unknown"),
                "created": item.get("created_at", "")[:10]
            }
            tracks.append(track)
        
        if not tracks:
            await send_response(
                message,
                "SOUNDCLOUD NOT FOUND",
                f"😿 Không tìm thấy: `{query}`",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        lines = []
        for idx, track in enumerate(tracks, 1):
            duration_str = f"{int(track['duration'] / 1000)}s"
            lines.append(f"<b>{idx}. </b> 🎵 {escape_html(track['title'])}")
            lines.append(f"   👤 <i>{escape_html(track['artist'])}</i> | 🕒 {duration_str}")
            lines.append(f"   ❤️ {track['likes']:,} | 🎧 {track['plays']:,}")
            lines.append(f"   ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        
        content = "\n".join(lines)
        content += "\n\n💡 <b>Reply theo số thứ tự bài mày muốn! </b>"
        
        SEARCH_CONTEXT[message.message_id] = tracks
        CONTEXT_TIMESTAMP[message.message_id] = time.time()
        
        log_command(user_id, "/scl", query[:50])
        
        total_count = search_data.get("total_results", len(tracks))
        
        await send_response(
            message,
            f"Found {total_count} Results",
            content,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error searching SoundCloud: {e}")
        await send_response(
            message,
            "SOUNDCLOUD ERROR",
            f"❌ Lỗi API: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_soundcloud_download(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or "soundcloud. com" not in params[0]:
        await send_response(
            message,
            "SOUNDCLOUD DOWNLOAD HELP",
            "Cú pháp: `/scl_down <link_SoundCloud>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    url = params[0].strip()
    
    msg = await send_response(
        message,
        "SOUNDCLOUD DOWNLOAD",
        "🎶 Đang tải SoundCloud...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_SCL_DOWN. format(url=quote(url))
        )
        
        if not data. get("status"):
            await send_response(
                message,
                "SOUNDCLOUD ERROR",
                f"❌ {data.get('message', 'Không tải được')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        audio_url = data.get("data")
        
        if not isinstance(audio_url, str) or not audio_url.startswith(('http://', 'https://')):
            await send_response(
                message,
                "SOUNDCLOUD ERROR",
                "❌ URL không hợp lệ từ API",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/scl_down", url[:50])
        
        title = data.get("title", "Track")
        
        try:
            await bot_aiogram.send_audio(
                message. chat.id,
                audio_url,
                caption=f"✅ *Tải OK! *\n🎵 `{escape_markdown_v2(title[:50])}`",
                parse_mode="MarkdownV2"
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat.id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except Exception as e:
            await send_response(
                message,
                "SOUNDCLOUD DOWNLOAD",
                f"✅ Tải OK (LINK)\nLỗi gửi audio: {str(e)}\n🔗 Link: {audio_url}",
                processing_msg=msg,
                delete_user_msg=True
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Error downloading SoundCloud: {e}")
        await send_response(
            message,
            "SOUNDCLOUD ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_tiktok_download(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or "tiktok. com" not in params[0]:
        await send_response(
            message,
            "TIKTOK DOWNLOAD HELP",
            "Cú pháp: `/tiktok <link_video>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    url = params[0].strip()
    
    msg = await send_response(
        message,
        "TIKTOK DOWNLOAD",
        "🎬 Đang tải video...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            "https://www.tikwm.com/api/",
            params={"url": url, "count": 12, "cursor": 0, "web": 1, "hd": 1},
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Referer': 'https://www.tikwm.com/',
            }
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "TIKTOK ERROR",
                f"❌ {data.get('message', 'Lỗi tải')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        video_data = data.get("data", {})
        video_url = video_data.get("play")
        
        if not video_url:
            await send_response(
                message,
                "TIKTOK ERROR",
                "❌ Không tìm thấy video URL",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        title = video_data.get("title", "TikTok Video")
        author = video_data.get("author", {}).get("nickname", "Unknown")
        views = video_data.get("play_count", 0)
        likes = video_data.get("digg_count", 0)
        comments = video_data.get("comment_count", 0)
        shares = video_data.get("share_count", 0)
        
        log_command(user_id, "/tiktok", url[:50])
        
        caption = (
            f"<blockquote>\n"
            f"🎬 <b>{escape_html(title)}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>{escape_html(author)}</b>\n"
            f"❤️ {likes:,} | 💬 {comments:,} | 🔗 {shares:,}\n"
            f"▶️ {views:,} views\n"
            f"</blockquote>"
        )
        
        try:
            await asyncio.wait_for(
                bot_aiogram.send_video(
                    message.chat.id,
                    video_url,
                    caption=caption,
                    parse_mode="HTML"
                ),
                timeout=60
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat. id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except asyncio.TimeoutError:
            await send_response(
                message,
                "TIKTOK DOWNLOAD",
                f"⚠️ Timeout khi tải video\n🔗 Link: {video_url}",
                processing_msg=msg,
                delete_user_msg=True
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Error downloading TikTok: {e}")
        await send_response(
            message,
            "TIKTOK ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_girl_image(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    msg = await send_response(
        message,
        "GIRL IMAGE",
        "🩷 Đang tìm ảnh.. .",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_ANH_GAI
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "IMAGE ERROR",
                f"❌ {data.get('message', 'Không tải được')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        image_url = data.get("data")
        
        if not image_url:
            await send_response(
                message,
                "IMAGE ERROR",
                "❌ Không có ảnh",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/anhgai", "random")
        
        try:
            await asyncio.wait_for(
                bot_aiogram.send_photo(
                    message.chat. id,
                    image_url,
                    caption="✨ Ảnh gái xinh"
                ),
                timeout=30
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat. id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except asyncio.TimeoutError:
            await send_response(
                message,
                "IMAGE TIMEOUT",
                f"⚠️ Timeout khi tải ảnh\n🔗 {image_url}",
                processing_msg=msg,
                delete_user_msg=True
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Error getting girl image: {e}")
        await send_response(
            message,
            "IMAGE ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_girl_video(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user. id
    
    msg = await send_response(
        message,
        "GIRL VIDEO",
        "🎬 Đang tìm video...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_VD_GAI
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "VIDEO ERROR",
                f"❌ {data.get('message', 'Không tải được')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        video_url = data.get("data")
        
        if not video_url:
            await send_response(
                message,
                "VIDEO ERROR",
                "❌ Không có video",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/vdgai", "random")
        
        try:
            await asyncio.wait_for(
                bot_aiogram.send_video(
                    message.chat.id,
                    video_url,
                    caption="✨ Video gái xinh",
                    supports_streaming=True
                ),
                timeout=60
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat. id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except asyncio.TimeoutError:
            await send_response(
                message,
                "VIDEO TIMEOUT",
                f"⚠️ Timeout khi tải video\n🔗 {video_url}",
                processing_msg=msg,
                delete_user_msg=True
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Error getting girl video: {e}")
        await send_response(
            message,
            "VIDEO ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_ngl_spam(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if len(params) < 3:
        await send_response(
            message,
            "NGL SPAM HELP",
            "Cú pháp: `/ngl <username> <message> <số_lượng>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    username = params[0]. strip()
    msg_text = params[1].strip()
    
    try:
        amount = int(params[2])
        if not (1 <= amount <= 100):
            raise ValueError("Số lượng 1-100")
    except (ValueError, IndexError):
        await send_response(
            message,
            "NGL SPAM ERROR",
            "❌ Số lượng không hợp lệ (1-100)",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    msg = await send_response(
        message,
        "NGL SPAM",
        f"✉️ Đang spam NGL: `{username}`.. .",
        delete_user_msg=False
    )
    
    try:
        api_url = API_NGL_SPAM.format(
            username=username,
            message=quote(msg_text),
            amount=amount
        )
        
        data = await asyncio.to_thread(
            get_api_result_sync,
            api_url
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "NGL SPAM ERROR",
                f"❌ {data.get('message', 'Thất bại')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/ngl", f"{username} - {amount}")
        
        content = (
            f"✅ Spam hoàn tất!\n"
            f"👤 Username: `{username}`\n"
            f"✉️ Tin nhắn gửi: {data.get('success', 0)}\n"
            f"❌ Lỗi: {data.get('failed', 0)}"
        )
        
        await send_response(
            message,
            "NGL SPAM RESULT",
            content,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error NGL spam: {e}")
        await send_response(
            message,
            "NGL SPAM ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_donate(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    caption = (
        f"💝 Cảm ơn bạn đã ủng hộ Bot!\n\n"
        f"Giúp duy trì và nâng cấp các API.\n\n"
        f"*[Bấm để xem mã QR]({QR_CODE_IMAGE_URL})*"
    )
    
    try:
        await bot_aiogram.send_photo(
            message.chat.id,
            QR_CODE_IMAGE_URL,
            caption=caption,
            parse_mode="Markdown"
        )
    except Exception as e:
        await send_response(
            message,
            "DONATE",
            caption,
            delete_user_msg=False
        )
    
    log_command(user_id, "/donate", "qr_request")
    return True

def get_client_id():
    try:
        response = requests.get(
            "https://soundcloud.com/",
            headers=SC_HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        
        script_tags = re.findall(
            r'<script crossorigin src="([^"]+)"',
            response.text
        )
        script_urls = [
            url for url in script_tags
            if url.startswith("https")
        ]
        
        if not script_urls:
            return 'W00nmY7TLer3uyoEo1sWK3Hhke5Ahdl9'
        
        script_response = requests.get(
            script_urls[-1],
            headers=SC_HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        script_response.raise_for_status()
        
        client_id_match = re.search(
            r',client_id:"([^"]+)"',
            script_response.text
        )
        
        if not client_id_match:
            return 'W00nmY7TLer3uyoEo1sWK3Hhke5Ahdl9'
        
        return client_id_match.group(1)
    
    except Exception:
        return 'W00nmY7TLer3uyoEo1sWK3Hhke5Ahdl9'

def update_router_handlers(router: Router) -> Router:
    router.message. register(handle_soundcloud_search, Command("scl"))
    router.message. register(handle_soundcloud_download, Command("scl_down"))
    router.message. register(handle_tiktok_download, Command("tiktok"))
    router.message.register(handle_girl_image, Command("anhgai"))
    router.message.register(handle_girl_video, Command("vdgai"))
    router.message.register(handle_ngl_spam, Command("ngl"))
    router. message.register(handle_donate, Command("donate"))
    
    return router

async def main():
    logger.info(f"🚀 Bot Premium VIP (@{OWNER_USERNAME}) đang khởi động...")
    
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'w', encoding='utf-8') as f:
                f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Log created.\n")
    except IOError as e:
        logger. critical(f"❌ Không thể tạo log file: {e}")
        return
    
    await setup_database()
    
    try:
        await bot_aiogram.delete_webhook(timeout=5)
        logger.info("✅ Xóa Webhook cũ OK")
    except Exception as e:
        logger.warning(f"⚠️ Không thể xóa Webhook: {e}")
    
    try:
        commands = [
            BotCommand(command="start", description="📋 Menu chính"),
            BotCommand(command="ping", description="🤖 Kiểm tra trạng thái"),
            BotCommand(command="checkid", description="🆔 Xem ID"),
            BotCommand(command="ask", description="🤖 Hỏi AI"),
            BotCommand(command="tx", description="🔮 Giải mã MD5"),
            BotCommand(command="qrcode", description="🔳 Tạo QR"),
            BotCommand(command="voice", description="🗣️ Text-to-Speech"),
            BotCommand(command="weather", description="🌤️ Dự báo thời tiết"),
            BotCommand(command="kqxs", description="🎟️ Kết quả XSMB"),
            BotCommand(command="ip", description="🌐 Tra cứu IP"),
            BotCommand(command="fb", description="👤 Info Facebook"),
            BotCommand(command="tt", description="🎵 Info TikTok"),
            BotCommand(command="scl", description="🎶 Tìm nhạc SoundCloud"),
            BotCommand(command="tiktok", description="🎬 Tải video TikTok"),
            BotCommand(command="anhgai", description="🖼️ Ảnh gái xinh"),
            BotCommand(command="vdgai", description="🎬 Video gái xinh"),
            BotCommand(command="ngl", description="✉️ Spam NGL"),
            BotCommand(command="donate", description="💖 Ủng hộ Bot"),
            BotCommand(command="nap", description="💳 Hướng dẫn nạp"),
            BotCommand(command="vip", description="🔥 VIP Commands"),
            BotCommand(command="call", description="📞 Gọi điện"),
            BotCommand(command="kill", description="🛑 Dừng lệnh"),
            BotCommand(command="themvip", description="➕ Thêm VIP"),
            BotCommand(command="xoavip", description="➖ Xóa VIP"),
            BotCommand(command="listvip", description="📋 Danh sách"),
        ]
        
        await bot_aiogram.set_my_commands(commands)
        logger.info("✅ Menu lệnh đã được cài đặt")
    except Exception as e:
        logger. warning(f"⚠️ Không thể cài Menu lệnh: {e}")
    
    dp = Dispatcher()
    router = create_router()
    router = update_router_handlers(router)
    dp.include_router(router)
    
    cleanup_task = asyncio.create_task(periodic_cleanup())
    
    try:
        bot_info = await asyncio.wait_for(
            bot_aiogram.get_me(),
            timeout=30
        )
        logger.info(f"✅ Bot kết nối thành công: @{bot_info.username}")
    except Exception as e:
        logger.critical(f"❌ Không thể kết nối tới Telegram: {e}")
        cleanup_task.cancel()
        return
    
    logger.info("🔄 Bắt đầu polling...")
    
    try:
        await dp.start_polling(
            bot_aiogram,
            drop_pending_updates=True,
            timeout=20,
            relax=0.1,
            fast=True,
            handle_as_tasks=True,
            allowed_updates=['message', 'callback_query']
        )
    finally:
        if cleanup_task and not cleanup_task.done():
            cleanup_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("✅ Bot dừng thành công")

def run_bot():
    import signal
    
    def signal_handler(signum, frame):
        signal_name = {
            signal.SIGINT: "SIGINT (Ctrl+C)",
            signal.SIGTERM: "SIGTERM (Kill)"
        }. get(signum, f"Signal {signum}")
        
        logger.info(f"🛑 Nhận {signal_name}, đang dừng bot...")
        try:
            kill_processes_sync("python.*lenh")
        except Exception as e:
            logger.error(f"Lỗi cleanup: {e}")
        exit(0)
    
    for sig in [signal.SIGINT, signal. SIGTERM]:
        signal. signal(sig, signal_handler)
    
    max_retries = 10
    restart_count = 0
    start_time = time.time()
    
    logger.info("🤖 Bot hệ thống đang khởi động...")
    
    while restart_count < max_retries:
        bot_start_time = time.time()
        
        try:
            if os.name == 'nt':
                asyncio.set_event_loop_policy(
                    asyncio.WindowsProactorEventLoopPolicy()
                )
            
            asyncio. run(main())
            logger.info("✅ Bot kết thúc bình thường")
            break
        
        except KeyboardInterrupt:
            logger. info("⏹️ Bot bị dừng bởi người dùng")
            break
        
        except Exception as e:
            runtime = time.time() - bot_start_time
            total_runtime = time.time() - start_time
            
            logger.error(
                f"💥 Bot crash sau {runtime:.1f}s (tổng: {total_runtime/3600:.1f}h): {e}"
            )
            restart_count += 1
            
            try:
                kill_processes_sync("python.*lenh")
            except Exception as cleanup_error:
                logger.error(f"Lỗi cleanup: {cleanup_error}")
            
            if restart_count < max_retries:
                wait_time = min(30, restart_count * 5)
                logger.info(
                    f"⏳ Chờ {wait_time}s trước khi restart "
                    f"(lần {restart_count}/{max_retries})"
                )
                time.sleep(wait_time)
            else:
                logger.error("❌ Đã đạt giới hạn restart, dừng bot")
                break
    
    total_runtime = time.time() - start_time
    logger.info(
        f"🏁 Bot dừng hoàn toàn sau {total_runtime/3600:.1f} giờ"
    )

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info(f"BOT PREMIUM VIP - @{OWNER_USERNAME}")
    logger.info(f"Phiên bản: 2.0 PRODUCTION")
    logger.info(f"Thời gian khởi động: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    if not os.path.exists(LOCAL_VIDEO_PATH):
        logger.warning(
            f"⚠️ File video '{LOCAL_VIDEO_PATH}' không tồn tại.  "
            "Bot sẽ gửi text thay thế."
        )
    
    try:
        run_bot()
    except KeyboardInterrupt:
        logger.info("⏹️ Bot dừng bởi Ctrl+C")
    except Exception as e:
        logger.critical(f"❌ CRITICAL ERROR: {e}", exc_info=True)
# === END FILE: bs.py ===

# === BEGIN FILE: cpp.py ===
import os
import io
import re
import ssl
import sys
import time
import json
import uuid
import socket
import base64
import random
import asyncio
import logging
import hashlib
import datetime
import threading
import traceback
import urllib.request
import urllib.parse
import html
from typing import List, Dict, Optional, Any, Tuple, Set
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    pass

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance, ImageOps
except ImportError:
    print("ERROR: Pillow not installed")
    sys.exit(1)

try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
    from telegram.constants import ParseMode
    from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, CallbackQueryHandler, Defaults, MessageHandler, filters
    from telegram.error import BadRequest, TimedOut, NetworkError
except ImportError:
    print("ERROR: python-telegram-bot not installed")
    sys.exit(1)

try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout, Page, BrowserContext
except ImportError:
    print("ERROR: playwright not installed")
    sys.exit(1)

try:
    import requests
    import dns.resolver
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("ERROR: requests or dnspython not installed")
    sys.exit(1)

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("CyberSecIntel")

class Cfg:
    TOKEN = "7563192441:AAFJ5hdMtr09ckZE_gq9PblKKMfSC6P7Zuw"
    ADMINS = [7679054753]
    NAME = "🛡️ ELITE CYBER INTELLIGENCE PLATFORM"
    VER = "5.0.0-QUANTUM-PRO"
    HOST = "0.0.0.0"
    PORT = int(os.environ.get("PORT", 8080))
    TIMEOUT = 120000
    VIEWPORT = {'width': 1920, 'height': 1080}
    MAX_RETRIES = 3
    
    PROXY_APIS = [
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/shiftytr/proxy-list/master/proxy.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt"
    ]
    
    C_BG_DARK = (6, 8, 15)
    C_BG_PANEL = (15, 20, 32)
    C_BG_ACCENT = (22, 28, 42)
    C_CYAN = (0, 255, 255)
    C_MAGENTA = (255, 0, 255)
    C_GOLD = (255, 215, 0)
    C_ORANGE = (255, 140, 0)
    C_TEXT_MAIN = (245, 250, 255)
    C_TEXT_SUB = (155, 165, 185)
    C_TEXT_DIM = (120, 130, 150)
    C_SUCCESS = (46, 213, 115)
    C_WARN = (255, 159, 64)
    C_DANGER = (255, 71, 87)
    C_INFO = (52, 152, 219)
    C_GRID = (25, 35, 50)
    C_BORDER = (40, 50, 70)
    
    COUNTRIES = {
        "US": "🇺🇸", "GB": "🇬🇧", "DE": "🇩🇪", "FR": "🇫🇷", "JP": "🇯🇵", "CN": "🇨🇳",
        "RU": "🇷🇺", "BR": "🇧🇷", "IN": "🇮🇳", "CA": "🇨🇦", "AU": "🇦🇺", "KR": "🇰🇷",
        "IT": "🇮🇹", "ES": "🇪🇸", "NL": "🇳🇱", "SE": "🇸🇪", "SG": "🇸🇬", "HK": "🇭🇰",
        "PL": "🇵🇱", "TR": "🇹🇷", "MX": "🇲🇽", "AR": "🇦🇷", "ZA": "🇿🇦", "ID": "🇮🇩",
        "TH": "🇹🇭", "MY": "🇲🇾", "PH": "🇵🇭", "VN": "🇻🇳", "UA": "🇺🇦", "RO": "🇷🇴",
        "CZ": "🇨🇿", "GR": "🇬🇷", "PT": "🇵🇹", "BE": "🇧🇪", "CH": "🇨🇭", "AT": "🇦🇹",
        "NO": "🇳🇴", "DK": "🇩🇰", "FI": "🇫🇮", "IE": "🇮🇪", "NZ": "🇳🇿", "IL": "🇮🇱",
        "AE": "🇦🇪", "SA": "🇸🇦", "EG": "🇪🇬", "NG": "🇳🇬", "KE": "🇰🇪", "CL": "🇨🇱",
        "CO": "🇨🇴", "PE": "🇵🇪", "VE": "🇻🇪", "BD": "🇧🇩", "PK": "🇵🇰", "IR": "🇮🇷"
    }
    
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
    ]

class ProxyMgr:
    _proxies: List[str] = []
    _working_proxies: Set[str] = set()
    _failed_proxies: Set[str] = set()
    _last_fetch: float = 0
    _lock = threading.Lock()
    _test_lock = threading.Lock()
    _fetch_in_progress = False
    
    @classmethod
    async def get_proxy(cls) -> Optional[str]:
        with cls._lock:
            now = time.time()
            if (now - cls._last_fetch > 150 or not cls._proxies) and not cls._fetch_in_progress:
                asyncio.create_task(cls._fetch_proxies())
            
            for _ in range(10):
                if not cls._proxies:
                    break
                
                proxy = cls._proxies.pop(0)
                if proxy not in cls._failed_proxies:
                    return f"http://{proxy}"
            
            if cls._working_proxies:
                working_list = list(cls._working_proxies)
                random.shuffle(working_list)
                return f"http://{working_list[0]}"
            
            return None
    
    @classmethod
    async def _fetch_proxies(cls):
        with cls._lock:
            if cls._fetch_in_progress:
                return
            cls._fetch_in_progress = True
        
        try:
            all_proxies = []
            
            for api_url in Cfg.PROXY_APIS:
                try:
                    resp = await asyncio.to_thread(
                        requests.get, 
                        api_url, 
                        timeout=10,
                        headers={"User-Agent": random.choice(Cfg.USER_AGENTS)}
                    )
                    
                    if resp.status_code == 200:
                        proxies = [
                            p.strip() 
                            for p in resp.text.strip().split('\n') 
                            if p.strip() and ':' in p and len(p.strip().split(':')) == 2
                        ]
                        all_proxies.extend(proxies)
                        
                        if len(all_proxies) > 300:
                            break
                            
                except Exception as e:
                    logger.debug(f"Failed to fetch from {api_url}: {e}")
                    continue
            
            if all_proxies:
                unique_proxies = list(set(all_proxies))
                random.shuffle(unique_proxies)
                
                with cls._lock:
                    cls._proxies = unique_proxies[:250]
                    cls._last_fetch = time.time()
                    cls._failed_proxies.clear()
                    
                logger.info(f"Loaded {len(cls._proxies)} proxies")
            
        except Exception as e:
            logger.error(f"Proxy fetch error: {e}")
        finally:
            with cls._lock:
                cls._fetch_in_progress = False
    
    @classmethod
    def mark_failed(cls, proxy: str):
        with cls._lock:
            clean_proxy = proxy.replace("http://", "").replace("https://", "")
            cls._failed_proxies.add(clean_proxy)
            if clean_proxy in cls._working_proxies:
                cls._working_proxies.remove(clean_proxy)
    
    @classmethod
    def mark_working(cls, proxy: str):
        with cls._lock:
            clean_proxy = proxy.replace("http://", "").replace("https://", "")
            cls._working_proxies.add(clean_proxy)

class Utils:
    @staticmethod
    def fix_url(url: str) -> str:
        u = url.strip()
        if not u:
            return ""
        
        if not re.match(r'^https?://', u):
            u = 'https://' + u
        
        try:
            parsed = urllib.parse.urlparse(u)
            if not parsed.netloc:
                return ""
            
            if parsed.netloc.count('.') == 0:
                return ""
            
            return u
        except:
            return ""
    
    @staticmethod
    def gen_sess() -> str:
        timestamp = str(int(time.time() * 1000))
        random_part = str(uuid.uuid4())
        combined = timestamp + random_part
        return hashlib.sha256(combined.encode()).hexdigest()[:20].upper()
    
    @staticmethod
    def time_now() -> str:
        return datetime.datetime.now().strftime("%H:%M:%S")
    
    @staticmethod
    def time_now_ms() -> str:
        return datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    
    @staticmethod
    def date_now() -> str:
        return datetime.datetime.now().strftime("%Y-%m-%d")
    
    @staticmethod
    def datetime_now() -> str:
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    def fmt_size(size: int) -> str:
        if size < 0:
            return "0B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.1f}{unit}"
            size /= 1024.0
        return f"{size:.1f}PB"
    
    @staticmethod
    def fmt_number(num: int) -> str:
        if num < 1000:
            return str(num)
        elif num < 1000000:
            return f"{num/1000:.1f}K"
        else:
            return f"{num/1000000:.1f}M"
    
    @staticmethod
    def color_scale(value: float, min_val: float, max_val: float) -> Tuple[int, int, int]:
        ratio = max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))
        
        r = int(255 * ratio)
        g = int(255 * (1 - ratio))
        b = 50
        
        return (r, g, b)
    
    @staticmethod
    def get_domain(url: str) -> str:
        try:
            parsed = urllib.parse.urlparse(url)
            return parsed.netloc
        except:
            return ""
    
    @staticmethod
    def is_valid_ip(ip: str) -> bool:
        try:
            parts = ip.split('.')
            if len(parts) != 4:
                return False
            for part in parts:
                if not 0 <= int(part) <= 255:
                    return False
            return True
        except:
            return False
    
    @staticmethod
    def sanitize_text(text: str, max_length: int = 100) -> str:
        if not text:
            return "N/A"
        
        text = str(text).strip()
        
        if len(text) > max_length:
            text = text[:max_length - 3] + "..."
        
        return text

class CoreNet:
    @staticmethod
    def resolve_dns(domain: str) -> Dict[str, List[str]]:
        dns_records = {
            "A": [], "AAAA": [], "MX": [], "NS": [], "TXT": [], 
            "CNAME": [], "SOA": [], "SPF": [], "DMARC": [], "CAA": []
        }
        
        try:
            resolver = dns.resolver.Resolver()
            resolver.timeout = 5
            resolver.lifetime = 5
            resolver.nameservers = ['8.8.8.8', '1.1.1.1', '8.8.4.4']
            
            record_types = ["A", "AAAA", "MX", "NS", "TXT", "CNAME", "SOA", "CAA"]
            
            for record_type in record_types:
                try:
                    answers = resolver.resolve(domain, record_type)
                    
                    for rdata in answers:
                        data_str = str(rdata).strip('"').strip()
                        dns_records[record_type].append(data_str)
                        
                        if record_type == "TXT":
                            if "v=spf1" in data_str.lower():
                                dns_records["SPF"].append(data_str)
                            if "v=dmarc1" in data_str.lower():
                                dns_records["DMARC"].append(data_str)
                                
                except dns.resolver.NXDOMAIN:
                    logger.debug(f"Domain {domain} does not exist")
                    break
                except dns.resolver.NoAnswer:
                    pass
                except dns.resolver.Timeout:
                    logger.debug(f"DNS timeout for {record_type} record")
                    pass
                except Exception as e:
                    logger.debug(f"DNS query error for {record_type}: {e}")
                    pass
                    
        except Exception as e:
            logger.error(f"DNS resolution error for {domain}: {e}")
        
        return dns_records
    
    @staticmethod
    def get_geoip(ip: str) -> Dict[str, Any]:
        default_data = {
            "country": "Unknown", "city": "Unknown", "isp": "Unknown",
            "lat": 0.0, "lon": 0.0, "flag": "🏳️", "org": "N/A",
            "tz": "UTC", "asn": "N/A", "postal": "N/A", "region": "N/A",
            "continent": "N/A", "currency": "N/A", "mobile": False,
            "proxy": False, "hosting": False, "vpn": False,
            "tor": False, "relay": False
        }
        
        if not ip or not Utils.is_valid_ip(ip):
            return default_data
        
        if ip.startswith("127.") or ip.startswith("192.168.") or ip.startswith("10.") or ip == "0.0.0.0":
            default_data["country"] = "Private/Local"
            return default_data
        
        try:
            api_url = f"http://ip-api.com/json/{ip}?fields=status,message,continent,continentCode,country,countryCode,region,regionName,city,district,zip,lat,lon,timezone,offset,currency,isp,org,as,asname,reverse,mobile,proxy,hosting,query"
            
            with urllib.request.urlopen(api_url, timeout=8) as response:
                data = json.loads(response.read().decode())
                
                if data.get("status") == "success":
                    country_code = data.get("countryCode", "")
                    
                    geo_data = {
                        "country": data.get("country", "Unknown"),
                        "city": data.get("city", "Unknown"),
                        "isp": data.get("isp", "Unknown"),
                        "lat": data.get("lat", 0.0),
                        "lon": data.get("lon", 0.0),
                        "org": data.get("org", "N/A"),
                        "tz": data.get("timezone", "UTC"),
                        "asn": data.get("as", "N/A"),
                        "postal": data.get("zip", "N/A"),
                        "region": data.get("regionName", "N/A"),
                        "continent": data.get("continent", "N/A"),
                        "currency": data.get("currency", "N/A"),
                        "mobile": data.get("mobile", False),
                        "proxy": data.get("proxy", False),
                        "hosting": data.get("hosting", False),
                        "vpn": False,
                        "tor": False,
                        "relay": False,
                        "flag": Cfg.COUNTRIES.get(country_code, "🏳️")
                    }
                    
                    return geo_data
                    
        except urllib.error.URLError as e:
            logger.debug(f"GeoIP API error: {e}")
        except Exception as e:
            logger.error(f"GeoIP lookup error: {e}")
        
        return default_data
    
    @staticmethod
    def analyze_ssl(host: str, port: int = 443) -> Dict[str, Any]:
        ssl_data = {
            "valid": False, "issuer": "N/A", "subject": "N/A",
            "version": "N/A", "cipher": "N/A", "expiry": "N/A",
            "days_left": 0, "serial": "N/A", "san": [],
            "protocol": "N/A", "key_size": 0, "signature_algo": "N/A",
            "ocsp": [], "issuer_country": "N/A", "not_before": "N/A",
            "issuer_cn": "N/A", "subject_cn": "N/A", "chain_length": 0,
            "self_signed": False, "wildcard": False
        }
        
        try:
            context = ssl.create_default_context()
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED
            
            with socket.create_connection((host, port), timeout=8) as sock:
                with context.wrap_socket(sock, server_hostname=host) as ssock:
                    ssl_data["valid"] = True
                    ssl_data["version"] = ssock.version()
                    
                    cipher_info = ssock.cipher()
                    if cipher_info:
                        ssl_data["cipher"] = cipher_info[0]
                        ssl_data["protocol"] = cipher_info[1]
                        ssl_data["key_size"] = cipher_info[2]
                    
                    cert = ssock.getpeercert()
                    
                    issuer_dict = dict(x[0] for x in cert.get('issuer', []))
                    subject_dict = dict(x[0] for x in cert.get('subject', []))
                    
                    ssl_data["issuer"] = issuer_dict.get('organizationName', 
                                                         issuer_dict.get('commonName', 'Unknown'))
                    ssl_data["issuer_cn"] = issuer_dict.get('commonName', 'N/A')
                    ssl_data["issuer_country"] = issuer_dict.get('countryName', 'N/A')
                    
                    ssl_data["subject"] = subject_dict.get('commonName', 'Unknown')
                    ssl_data["subject_cn"] = subject_dict.get('commonName', 'N/A')
                    
                    if ssl_data["subject_cn"].startswith("*."):
                        ssl_data["wildcard"] = True
                    
                    ssl_data["serial"] = cert.get('serialNumber', 'N/A')
                    
                    if 'subjectAltName' in cert:
                        ssl_data["san"] = [x[1] for x in cert['subjectAltName']]
                    
                    if 'OCSP' in cert:
                        ssl_data["ocsp"] = cert['OCSP'] if isinstance(cert['OCSP'], list) else [cert['OCSP']]
                    
                    not_before_str = cert.get('notBefore')
                    if not_before_str:
                        try:
                            not_before_dt = datetime.datetime.strptime(not_before_str, '%b %d %H:%M:%S %Y %Z')
                            ssl_data["not_before"] = not_before_dt.strftime("%Y-%m-%d")
                        except:
                            pass
                    
                    not_after_str = cert.get('notAfter')
                    if not_after_str:
                        try:
                            expiry_dt = datetime.datetime.strptime(not_after_str, '%b %d %H:%M:%S %Y %Z')
                            ssl_data["expiry"] = expiry_dt.strftime("%Y-%m-%d")
                            ssl_data["days_left"] = (expiry_dt - datetime.datetime.utcnow()).days
                        except:
                            pass
                    
                    if ssl_data["issuer_cn"] == ssl_data["subject_cn"]:
                        ssl_data["self_signed"] = True
                        
        except ssl.SSLError as e:
            ssl_data["error"] = f"SSL Error: {str(e)[:80]}"
            logger.debug(f"SSL Error for {host}: {e}")
        except socket.timeout:
            ssl_data["error"] = "Connection timeout"
        except socket.gaierror:
            ssl_data["error"] = "DNS resolution failed"
        except Exception as e:
            ssl_data["error"] = f"Error: {str(e)[:80]}"
            logger.debug(f"SSL analysis error for {host}: {e}")
        
        return ssl_data
    
    @staticmethod
    def tcp_ping(host: str, port: int = 443, attempts: int = 5) -> Dict[str, float]:
        latencies = []
        successful_attempts = 0
        
        for attempt in range(attempts):
            try:
                start_time = time.perf_counter()
                
                with socket.create_connection((host, port), timeout=6) as sock:
                    sock.send(b'\x00')
                
                latency = (time.perf_counter() - start_time) * 1000
                latencies.append(latency)
                successful_attempts += 1
                
                time.sleep(0.1)
                
            except (socket.timeout, ConnectionRefusedError, OSError):
                latencies.append(9999.0)
            except Exception as e:
                logger.debug(f"TCP ping error: {e}")
                latencies.append(9999.0)
        
        valid_latencies = [l for l in latencies if l < 9000]
        
        if valid_latencies:
            avg_latency = sum(valid_latencies) / len(valid_latencies)
            min_latency = min(valid_latencies)
            max_latency = max(valid_latencies)
            jitter = max_latency - min_latency if len(valid_latencies) > 1 else 0
            packet_loss = ((attempts - successful_attempts) / attempts) * 100
            
            return {
                "avg": round(avg_latency, 2),
                "min": round(min_latency, 2),
                "max": round(max_latency, 2),
                "jitter": round(jitter, 2),
                "packet_loss": round(packet_loss, 1),
                "success_rate": round((successful_attempts / attempts) * 100, 1)
            }
        
        return {
            "avg": 9999.0,
            "min": 9999.0,
            "max": 9999.0,
            "jitter": 0.0,
            "packet_loss": 100.0,
            "success_rate": 0.0
        }
    
    @staticmethod
    def deep_headers(url: str, proxy: Optional[str] = None) -> Dict[str, Any]:
        header_analysis = {
            "status": 0,
            "server": "Unknown",
            "content_type": "Unknown",
            "cookies": {},
            "cookie_count": 0,
            "headers": {},
            "security": [],
            "redirects": 0,
            "redirect_chain": [],
            "ttfb": 0,
            "load_time": 0,
            "content_length": 0,
            "encoding": "N/A",
            "compression": "None",
            "cache_control": "N/A",
            "cdn": "Unknown",
            "waf": "Unknown",
            "powered_by": [],
            "frameworks": [],
            "server_timing": [],
            "cors": False,
            "hsts": False,
            "hsts_max_age": 0,
            "csp": False,
            "x_frame": "Not Set"
        }
        
        try:
            session = requests.Session()
            
            retry_strategy = Retry(
                total=2,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
            session.max_redirects = 10
            session.verify = False
            
            if proxy:
                session.proxies = {
                    "http": proxy,
                    "https": proxy
                }
            
            request_headers = {
                "User-Agent": random.choice(Cfg.USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Cache-Control": "max-age=0"
            }
            
            start_time = time.time()
            response = session.get(
                url,
                headers=request_headers,
                timeout=20,
                allow_redirects=True,
                stream=False
            )
            total_time = time.time() - start_time
            
            header_analysis["status"] = response.status_code
            header_analysis["load_time"] = round(total_time * 1000, 1)
            
            if response.history:
                header_analysis["redirects"] = len(response.history)
                header_analysis["redirect_chain"] = [r.url for r in response.history]
            
            header_analysis["headers"] = dict(response.headers)
            
            header_analysis["server"] = response.headers.get("Server", "Obscured")
            header_analysis["content_type"] = response.headers.get("Content-Type", "Unknown")
            
            header_analysis["cookies"] = response.cookies.get_dict()
            header_analysis["cookie_count"] = len(header_analysis["cookies"])
            
            content_length = response.headers.get("Content-Length")
            if content_length:
                header_analysis["content_length"] = int(content_length)
            else:
                header_analysis["content_length"] = len(response.content)
            
            header_analysis["encoding"] = response.headers.get("Content-Encoding", "None")
            
            if header_analysis["encoding"].lower() in ["gzip", "deflate", "br"]:
                header_analysis["compression"] = header_analysis["encoding"]
            
            header_analysis["cache_control"] = response.headers.get("Cache-Control", "Not Set")
            
            cdn_headers = {
                "cf-ray": "Cloudflare",
                "x-amz-cf-id": "Amazon CloudFront",
                "x-amz-cf-pop": "Amazon CloudFront",
                "x-akamai-transformed": "Akamai",
                "x-cache": "Generic CDN",
                "x-fastly-request-id": "Fastly",
                "x-cdn": "Generic CDN",
                "server-timing": "CDN Timing"
            }
            
            for header_key, cdn_name in cdn_headers.items():
                if any(header_key in k.lower() for k in response.headers.keys()):
                    header_analysis["cdn"] = cdn_name
                    break
            
            if "cloudflare" in header_analysis["server"].lower():
                header_analysis["cdn"] = "Cloudflare"
            
            waf_indicators = {
                "x-sucuri-id": "Sucuri",
                "x-sucuri-cache": "Sucuri",
                "x-waf": "Generic WAF",
                "x-mod-security": "ModSecurity",
                "x-defender": "Defender",
                "x-akamai-protection": "Akamai",
                "cf-ray": "Cloudflare WAF"
            }
            
            for header_key, waf_name in waf_indicators.items():
                if any(header_key in k.lower() for k in response.headers.keys()):
                    header_analysis["waf"] = waf_name
                    break
            
            powered_by_header = response.headers.get("X-Powered-By", "")
            if powered_by_header:
                header_analysis["powered_by"].append(powered_by_header)
            
            generator_header = response.headers.get("X-Generator", "")
            if generator_header:
                header_analysis["frameworks"].append(generator_header)
            
            security_headers = {
                "Strict-Transport-Security": "HSTS",
                "Content-Security-Policy": "CSP",
                "X-Frame-Options": "X-Frame-Options",
                "X-XSS-Protection": "XSS Protection",
                "X-Content-Type-Options": "Content-Type Options",
                "Referrer-Policy": "Referrer Policy",
                "Permissions-Policy": "Permissions Policy",
                "Cross-Origin-Embedder-Policy": "COEP",
                "Cross-Origin-Opener-Policy": "COOP",
                "Cross-Origin-Resource-Policy": "CORP",
                "Expect-CT": "Certificate Transparency"
            }
            
            for header_name, friendly_name in security_headers.items():
                if header_name in response.headers:
                    header_analysis["security"].append(friendly_name)
                    
                    if header_name == "Strict-Transport-Security":
                        header_analysis["hsts"] = True
                        hsts_value = response.headers[header_name]
                        max_age_match = re.search(r'max-age=(\d+)', hsts_value)
                        if max_age_match:
                            header_analysis["hsts_max_age"] = int(max_age_match.group(1))
                    
                    if header_name == "Content-Security-Policy":
                        header_analysis["csp"] = True
                    
                    if header_name == "X-Frame-Options":
                        header_analysis["x_frame"] = response.headers[header_name]
            
            cors_headers = ["Access-Control-Allow-Origin", "Access-Control-Allow-Methods"]
            if any(h in response.headers for h in cors_headers):
                header_analysis["cors"] = True
            
            server_timing = response.headers.get("Server-Timing", "")
            if server_timing:
                header_analysis["server_timing"] = server_timing.split(',')
            
            if proxy:
                ProxyMgr.mark_working(proxy)
                
        except requests.exceptions.ProxyError:
            if proxy:
                ProxyMgr.mark_failed(proxy)
            header_analysis["error"] = "Proxy connection failed"
        except requests.exceptions.SSLError as e:
            header_analysis["error"] = f"SSL Error: {str(e)[:60]}"
        except requests.exceptions.Timeout:
            header_analysis["error"] = "Request timeout"
        except requests.exceptions.ConnectionError as e:
            header_analysis["error"] = f"Connection error: {str(e)[:60]}"
        except Exception as e:
            header_analysis["error"] = f"Error: {str(e)[:80]}"
            logger.error(f"Header analysis error: {e}")
        
        return header_analysis
    
    @staticmethod
    async def check_host_global(url: str, max_workers: int = 50) -> Dict[str, Dict]:
        results = {}
        countries = list(Cfg.COUNTRIES.keys())
        
        async def check_from_country(country_code: str) -> Dict:
            proxy = await ProxyMgr.get_proxy()
            
            result = {
                "country": country_code,
                "flag": Cfg.COUNTRIES[country_code],
                "status": 0,
                "latency": 9999.0,
                "success": False,
                "server": "N/A",
                "size": 0,
                "cdn": "Unknown",
                "error": None
            }
            
            try:
                session = requests.Session()
                session.verify = False
                
                if proxy:
                    session.proxies = {
                        "http": proxy,
                        "https": proxy
                    }
                
                headers = {
                    "User-Agent": random.choice(Cfg.USER_AGENTS),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                }
                
                start_time = time.time()
                
                response = session.get(
                    url,
                    headers=headers,
                    timeout=15,
                    allow_redirects=True
                )
                
                latency = (time.time() - start_time) * 1000
                
                result["status"] = response.status_code
                result["latency"] = round(latency, 1)
                result["success"] = True
                result["server"] = response.headers.get("Server", "N/A")[:25]
                result["size"] = len(response.content)
                
                if "cf-ray" in response.headers:
                    result["cdn"] = "Cloudflare"
                elif "x-amz-cf-id" in response.headers:
                    result["cdn"] = "CloudFront"
                
                if proxy:
                    ProxyMgr.mark_working(proxy)
                
            except requests.exceptions.ProxyError:
                if proxy:
                    ProxyMgr.mark_failed(proxy)
                result["error"] = "Proxy failed"
            except requests.exceptions.Timeout:
                result["error"] = "Timeout"
            except requests.exceptions.ConnectionError:
                result["error"] = "Connection refused"
            except Exception as e:
                result["error"] = str(e)[:50]
            
            return result
        
        tasks = [check_from_country(cc) for cc in countries[:50]]
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results_list:
            if isinstance(result, dict) and "country" in result:
                results[result["country"]] = result
        
        return results
    
    @staticmethod
    def get_tech_stack(headers: Dict, html_content: str = "") -> List[str]:
        technologies = []
        
        server_header = headers.get("Server", "").lower()
        if "nginx" in server_header:
            version_match = re.search(r'nginx/([\d.]+)', server_header)
            if version_match:
                technologies.append(f"Nginx {version_match.group(1)}")
            else:
                technologies.append("Nginx")
        
        if "apache" in server_header:
            version_match = re.search(r'apache/([\d.]+)', server_header)
            if version_match:
                technologies.append(f"Apache {version_match.group(1)}")
            else:
                technologies.append("Apache")
        
        if "microsoft-iis" in server_header or "iis" in server_header:
            technologies.append("Microsoft IIS")
        
        if "cloudflare" in server_header or any("cf-" in k.lower() for k in headers.keys()):
            technologies.append("Cloudflare")
        
        if "LiteSpeed" in server_header:
            technologies.append("LiteSpeed")
        
        powered_by = headers.get("X-Powered-By", "")
        if powered_by:
            if "PHP" in powered_by:
                technologies.append(powered_by)
            elif "ASP.NET" in powered_by:
                technologies.append("ASP.NET")
            elif powered_by and powered_by not in technologies:
                technologies.append(powered_by[:35])
        
        if html_content:
            html_lower = html_content.lower()
            
            cms_signatures = {
                "wp-content": "WordPress",
                "wp-includes": "WordPress",
                "/joomla/": "Joomla",
                "joomla!": "Joomla",
                "drupal": "Drupal",
                "sites/default": "Drupal",
                "typo3": "TYPO3",
                "magento": "Magento",
                "shopify": "Shopify",
                "wix.com": "Wix",
                "squarespace": "Squarespace",
                "webflow": "Webflow"
            }
            
            for signature, tech_name in cms_signatures.items():
                if signature in html_lower:
                    if tech_name not in technologies:
                        technologies.append(tech_name)
                    break
            
            js_frameworks = {
                "react": "React",
                "_react": "React",
                "reactjs": "React",
                "vue": "Vue.js",
                "vuejs": "Vue.js",
                "angular": "Angular",
                "ng-app": "Angular",
                "ng-controller": "Angular",
                "ember": "Ember.js",
                "backbone": "Backbone.js",
                "svelte": "Svelte",
                "next": "Next.js",
                "__next": "Next.js",
                "nuxt": "Nuxt.js",
                "gatsby": "Gatsby"
            }
            
            for signature, framework in js_frameworks.items():
                if signature in html_lower:
                    if framework not in technologies:
                        technologies.append(framework)
            
            if "jquery" in html_lower:
                version_match = re.search(r'jquery[/-]?([\d.]+)', html_lower)
                if version_match:
                    technologies.append(f"jQuery {version_match.group(1)}")
                else:
                    technologies.append("jQuery")
            
            css_frameworks = {
                "bootstrap": "Bootstrap",
                "tailwind": "Tailwind CSS",
                "bulma": "Bulma",
                "foundation": "Foundation",
                "semantic-ui": "Semantic UI",
                "materialize": "Materialize"
            }
            
            for signature, framework in css_frameworks.items():
                if signature in html_lower and framework not in technologies:
                    technologies.append(framework)
            
            analytics_services = {
                "google-analytics": "Google Analytics",
                "gtag": "Google Analytics",
                "googletagmanager": "Google Tag Manager",
                "facebook.com/tr": "Facebook Pixel",
                "hotjar": "Hotjar",
                "mixpanel": "Mixpanel",
                "segment": "Segment"
            }
            
            for signature, service in analytics_services.items():
                if signature in html_lower and service not in technologies:
                    technologies.append(service)
        
        if not technologies:
            technologies.append("Unknown Stack")
        
        return technologies[:12]
    
    @staticmethod
    def port_scan(host: str, ports: List[int] = None) -> Dict[int, bool]:
        if ports is None:
            ports = [21, 22, 25, 80, 443, 3306, 5432, 8080, 8443]
        
        open_ports = {}
        
        for port in ports:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex((host, port))
                open_ports[port] = (result == 0)
                sock.close()
            except:
                open_ports[port] = False
        
        return open_ports

class BrowserBrain:
    @staticmethod
    async def capture(url: str, y_offset: int = 0, full_page: bool = False) -> Dict[str, Any]:
        capture_result = {
            "ss": None,
            "title": "",
            "url": url,
            "final_url": url,
            "err": None,
            "cookies": 0,
            "html": "",
            "meta_tags": {},
            "links_count": 0,
            "images_count": 0,
            "scripts_count": 0,
            "stylesheets_count": 0,
            "forms_count": 0,
            "iframes_count": 0,
            "performance": {},
            "console_errors": [],
            "network_requests": 0,
            "page_size": 0
        }
        
        proxy = await ProxyMgr.get_proxy()
        
        for attempt in range(2):
            try:
                async with async_playwright() as playwright:
                    browser_args = [
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',
                        '--disable-accelerated-2d-canvas',
                        '--disable-gpu',
                        '--ignore-certificate-errors',
                        '--disable-web-security',
                        '--disable-features=IsolateOrigins,site-per-process',
                        '--window-size=1920,1080',
                        '--disable-infobars',
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-renderer-backgrounding'
                    ]
                    
                    use_proxy = proxy and attempt == 0
                    if use_proxy:
                        browser_args.append(f'--proxy-server={proxy}')
                    
                    browser = await playwright.chromium.launch(
                        headless=True,
                        args=browser_args
                    )
                
                context = await browser.new_context(
                    viewport=Cfg.VIEWPORT,
                    user_agent=random.choice(Cfg.USER_AGENTS),
                    device_scale_factor=1,
                    has_touch=False,
                    locale='en-US',
                    timezone_id='America/New_York',
                    ignore_https_errors=True,
                    java_script_enabled=True,
                    bypass_csp=True,
                    extra_http_headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1'
                    }
                )
                
                await context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
                    Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
                    
                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };
                    
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' 
                            ? Promise.resolve({state: Notification.permission}) 
                            : originalQuery(parameters)
                    );
                    
                    delete navigator.__proto__.webdriver;
                    
                    Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
                    Object.defineProperty(navigator, 'vendor', {get: () => 'Google Inc.'});
                """)
                
                page = await context.new_page()
                
                request_count = 0
                def count_requests(request):
                    nonlocal request_count
                    request_count += 1
                
                page.on('request', count_requests)
                
                console_errors = []
                def log_console(msg):
                    if msg.type in ['error', 'warning']:
                        console_errors.append(f"{msg.type}: {msg.text}")
                
                page.on('console', log_console)
                
                try:
                    await page.set_extra_http_headers({
                        'Cache-Control': 'no-cache',
                        'Pragma': 'no-cache'
                    })
                    
                    navigation_start = time.time()
                    
                    response = await page.goto(
                        url,
                        timeout=Cfg.TIMEOUT,
                        wait_until="domcontentloaded"
                    )
                    
                    await asyncio.sleep(2)
                    
                    try:
                        await page.wait_for_load_state("networkidle", timeout=10000)
                    except:
                        pass
                    
                    cookie_selectors = [
                        "button:has-text('Accept')",
                        "button:has-text('Accept all')",
                        "button:has-text('Accept All')",
                        "button:has-text('I agree')",
                        "button:has-text('Agree')",
                        "button:has-text('OK')",
                        "button:has-text('Allow')",
                        "button:has-text('Allow all')",
                        "[id*='accept']",
                        "[class*='accept']",
                        "[id*='cookie']",
                        "[class*='cookie-accept']"
                    ]
                    
                    for selector in cookie_selectors:
                        try:
                            button = await page.query_selector(selector)
                            if button and await button.is_visible():
                                await button.click(timeout=1500)
                                await asyncio.sleep(0.8)
                                break
                        except:
                            pass
                    
                    try:
                        cloudflare_challenge = await page.query_selector("#challenge-running")
                        if cloudflare_challenge:
                            await asyncio.sleep(7)
                            try:
                                await page.wait_for_selector(
                                    "#challenge-running",
                                    state="hidden",
                                    timeout=20000
                                )
                                await asyncio.sleep(2)
                            except:
                                pass
                    except:
                        pass
                    
                    if y_offset > 0:
                        await page.evaluate(f"window.scrollTo({{top: {y_offset}, behavior: 'smooth'}})")
                        await asyncio.sleep(1.2)
                    else:
                        for scroll_step in range(3):
                            await page.mouse.wheel(0, 500)
                            await asyncio.sleep(0.4)
                        
                        await page.evaluate("window.scrollTo({top: 0, behavior: 'smooth'})")
                        await asyncio.sleep(0.8)
                    
                    await page.mouse.move(
                        random.randint(200, 1000),
                        random.randint(200, 800)
                    )
                    
                    await page.add_style_tag(content="""
                        ::-webkit-scrollbar { display: none !important; }
                        body { overflow-x: hidden !important; }
                        * { scrollbar-width: none !important; }
                    """)
                    
                    capture_result["title"] = await page.title()
                    capture_result["final_url"] = page.url
                    
                    page_cookies = await context.cookies()
                    capture_result["cookies"] = len(page_cookies)
                    
                    try:
                        html_content = await page.content()
                        capture_result["html"] = html_content
                        capture_result["page_size"] = len(html_content.encode('utf-8'))
                        
                        capture_result["links_count"] = html_content.count('<a ')
                        capture_result["images_count"] = html_content.count('<img ')
                        capture_result["scripts_count"] = html_content.count('<script')
                        capture_result["stylesheets_count"] = html_content.count('<link') + html_content.count('<style')
                        capture_result["forms_count"] = html_content.count('<form')
                        capture_result["iframes_count"] = html_content.count('<iframe')
                        
                        meta_tags = {}
                        meta_matches = re.finditer(r'<meta\s+([^>]+)>', html_content, re.IGNORECASE)
                        for match in meta_matches:
                            attrs = match.group(1)
                            name_match = re.search(r'name=["\']([^"\']+)["\']', attrs)
                            content_match = re.search(r'content=["\']([^"\']+)["\']', attrs)
                            if name_match and content_match:
                                meta_tags[name_match.group(1)] = content_match.group(1)
                        capture_result["meta_tags"] = meta_tags
                        
                    except Exception as e:
                        logger.debug(f"HTML extraction error: {e}")
                    
                    try:
                        performance_data = await page.evaluate("""
                            () => {
                                const perf = window.performance;
                                const navigation = perf.getEntriesByType('navigation')[0];
                                return {
                                    domContentLoaded: navigation ? navigation.domContentLoadedEventEnd - navigation.domContentLoadedEventStart : 0,
                                    loadComplete: navigation ? navigation.loadEventEnd - navigation.loadEventStart : 0,
                                    domInteractive: navigation ? navigation.domInteractive : 0
                                };
                            }
                        """)
                        capture_result["performance"] = performance_data
                    except:
                        pass
                    
                    capture_result["network_requests"] = request_count
                    capture_result["console_errors"] = console_errors[:10]
                    
                    screenshot_bytes = await page.screenshot(
                        type="png",
                        full_page=full_page
                    )
                    capture_result["ss"] = screenshot_bytes
                    
                    if use_proxy and proxy:
                        ProxyMgr.mark_working(proxy)
                    
                    break
                    
                except PlaywrightTimeout:
                    if attempt == 0 and proxy:
                        logger.warning(f"Timeout with proxy, retrying without proxy...")
                        if proxy:
                            ProxyMgr.mark_failed(proxy)
                        continue
                    capture_result["err"] = "Page load timeout"
                    break
                except Exception as e:
                    if attempt == 0 and proxy and "TUNNEL" in str(e):
                        logger.warning(f"Proxy tunnel failed, retrying without proxy...")
                        if proxy:
                            ProxyMgr.mark_failed(proxy)
                        continue
                    capture_result["err"] = f"Page error: {str(e)[:120]}"
                    logger.error(f"Page capture error: {e}")
                    break
                finally:
                    try:
                        await context.close()
                        await browser.close()
                    except:
                        pass
                    
        except Exception as e:
            capture_result["err"] = f"Browser error: {str(e)[:120]}"
            logger.error(f"Browser launch error: {e}")
            if proxy:
                ProxyMgr.mark_failed(proxy)
        
        return capture_result

class Db:
    _storage: Dict[str, Dict] = {}
    _lock = threading.Lock()
    _max_age = 7200
    
    @classmethod
    def put(cls, key: str, value: Dict) -> None:
        with cls._lock:
            value['timestamp'] = time.time()
            cls._storage[key] = value
            cls._cleanup()
    
    @classmethod
    def get(cls, key: str) -> Optional[Dict]:
        with cls._lock:
            return cls._storage.get(key)
    
    @classmethod
    def delete(cls, key: str) -> bool:
        with cls._lock:
            if key in cls._storage:
                del cls._storage[key]
                return True
            return False
    
    @classmethod
    def _cleanup(cls):
        current_time = time.time()
        expired_keys = [
            k for k, v in cls._storage.items()
            if current_time - v.get('timestamp', 0) > cls._max_age
        ]
        for key in expired_keys:
            del cls._storage[key]
    
    @classmethod
    def get_stats(cls) -> Dict:
        with cls._lock:
            return {
                "total_sessions": len(cls._storage),
                "memory_usage": sys.getsizeof(cls._storage)
            }
class HUD:
    @staticmethod
    def load_font(size: int, bold: bool = False):
        font_paths = [
            ("arialbd.ttf" if bold else "arial.ttf"),
            ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
            ("C:\\Windows\\Fonts\\arialbd.ttf" if bold else "C:\\Windows\\Fonts\\arial.ttf")
        ]
        
        for font_path in font_paths:
            try:
                return ImageFont.truetype(font_path, size)
            except:
                continue
        
        return ImageFont.load_default()
    
    @staticmethod
    def draw_grid(draw, width, height, step=50, color=None):
        if color is None:
            color = Cfg.C_GRID
        
        for x in range(0, width, step):
            draw.line([(x, 0), (x, height)], fill=color, width=1)
        
        for y in range(0, height, step):
            draw.line([(0, y), (width, y)], fill=color, width=1)
    
    @staticmethod
    def draw_corners(draw, x, y, w, h, color, length=30, thickness=4):
        draw.line([(x, y), (x + length, y)], fill=color, width=thickness)
        draw.line([(x, y), (x, y + length)], fill=color, width=thickness)
        
        draw.line([(x + w, y), (x + w - length, y)], fill=color, width=thickness)
        draw.line([(x + w, y), (x + w, y + length)], fill=color, width=thickness)
        
        draw.line([(x, y + h), (x + length, y + h)], fill=color, width=thickness)
        draw.line([(x, y + h), (x, y + h - length)], fill=color, width=thickness)
        
        draw.line([(x + w, y + h), (x + w - length, y + h)], fill=color, width=thickness)
        draw.line([(x + w, y + h), (x + w, y + h - length)], fill=color, width=thickness)
    
    @staticmethod
    def draw_progress_bar(draw, x, y, w, h, value, max_value, fill_color, bg_color=None):
        if bg_color is None:
            bg_color = (30, 35, 45)
        
        draw.rectangle([(x, y), (x + w, y + h)], fill=bg_color, outline=fill_color, width=1)
        
        if max_value > 0:
            ratio = min(1.0, max(0.0, value / max_value))
            fill_width = int(w * ratio)
            
            if fill_width > 0:
                draw.rectangle([(x, y), (x + fill_width, y + h)], fill=fill_color)
    
    @staticmethod
    def draw_gradient_rect(draw, x, y, w, h, color1, color2):
        for i in range(h):
            ratio = i / h
            r = int(color1[0] * (1 - ratio) + color2[0] * ratio)
            g = int(color1[1] * (1 - ratio) + color2[1] * ratio)
            b = int(color1[2] * (1 - ratio) + color2[2] * ratio)
            draw.line([(x, y + i), (x + w, y + i)], fill=(r, g, b))
    
    @staticmethod
    def render_interface(screenshot: bytes, data: Dict) -> bytes:
        try:
            source_img = Image.open(io.BytesIO(screenshot)).convert("RGBA")
            src_width, src_height = source_img.size
            
            padding_top = 200
            padding_bottom = 350
            padding_side = 100
            
            total_width = src_width + (padding_side * 2)
            total_height = src_height + padding_top + padding_bottom
            
            canvas = Image.new("RGBA", (total_width, total_height), Cfg.C_BG_DARK)
            draw = ImageDraw.Draw(canvas)
            
            HUD.draw_grid(draw, total_width, total_height, step=50)
            
            screenshot_x = padding_side
            screenshot_y = padding_top
            
            draw.rectangle(
                [(screenshot_x - 10, screenshot_y - 10),
                 (screenshot_x + src_width + 10, screenshot_y + src_height + 10)],
                outline=Cfg.C_CYAN,
                width=4
            )
            
            HUD.draw_corners(
                draw,
                screenshot_x - 25,
                screenshot_y - 25,
                src_width + 50,
                src_height + 50,
                Cfg.C_CYAN,
                length=50,
                thickness=6
            )
            
            canvas.paste(source_img, (screenshot_x, screenshot_y))
            
            header_points = [
                (0, 0),
                (total_width, 0),
                (total_width, 110),
                (total_width - 100, 170),
                (100, 170),
                (0, 110)
            ]
            draw.polygon(header_points, fill=Cfg.C_BG_PANEL)
            draw.line(
                [(0, 110), (100, 170), (total_width - 100, 170), (total_width, 110)],
                fill=Cfg.C_CYAN,
                width=5
            )
            
            title_text = data.get("title", "UNKNOWN TARGET")[:55].upper()
            draw.text(
                (padding_side + 35, 30),
                title_text,
                font=HUD.load_font(52, True),
                fill=Cfg.C_TEXT_MAIN
            )
            
            url_display = data.get("url", "")[:75]
            draw.text(
                (padding_side + 40, 105),
                f"🎯 TARGET: {url_display}",
                font=HUD.load_font(20),
                fill=Cfg.C_CYAN
            )
            
            session_id = data.get("sid", "N/A")
            draw.text(
                (total_width - 450, 30),
                f"SESSION ID: {session_id}",
                font=HUD.load_font(24, True),
                fill=Cfg.C_TEXT_SUB
            )
            
            draw.text(
                (total_width - 450, 65),
                f"⏱️ TIME: {Utils.time_now()}",
                font=HUD.load_font(20),
                fill=Cfg.C_TEXT_SUB
            )
            
            draw.text(
                (total_width - 450, 95),
                f"📅 DATE: {Utils.date_now()}",
                font=HUD.load_font(20),
                fill=Cfg.C_TEXT_SUB
            )
            
            draw.text(
                (total_width - 450, 125),
                "🔐 QUANTUM SECURE",
                font=HUD.load_font(18),
                fill=Cfg.C_SUCCESS
            )
            
            footer_y = total_height - padding_bottom
            footer_points = [
                (0, total_height),
                (total_width, total_height),
                (total_width, footer_y + 80),
                (total_width - 100, footer_y),
                (100, footer_y),
                (0, footer_y + 80)
            ]
            draw.polygon(footer_points, fill=Cfg.C_BG_PANEL)
            draw.line(
                [(0, footer_y + 80), (100, footer_y), (total_width - 100, footer_y), (total_width, footer_y + 80)],
                fill=Cfg.C_CYAN,
                width=5
            )
            
            num_sections = 7
            section_width = (total_width - 200) // num_sections
            section_x = 110
            section_y = footer_y + 70
            
            draw.text(
                (section_x, section_y),
                "🌐 NETWORK",
                font=HUD.load_font(21, True),
                fill=Cfg.C_MAGENTA
            )
            draw.line(
                [(section_x, section_y + 32), (section_x + section_width - 50, section_y + 32)],
                fill=Cfg.C_MAGENTA,
                width=3
            )
            
            network_info = [
                f"IP: {data.get('ip', 'N/A')}",
                f"LOC: {data.get('city', 'N/A')}, {data.get('country', 'N/A')} {data.get('flag', '')}",
                f"ISP: {Utils.sanitize_text(data.get('isp', 'N/A'), 22)}",
                f"ASN: {Utils.sanitize_text(data.get('asn', 'N/A'), 24)}",
                f"PING: {data.get('ping_avg', 0):.1f}ms",
                f"TYPE: {'☁️ Host' if data.get('hosting') else '🖥️ Standard'}"
            ]
            
            for idx, info_line in enumerate(network_info):
                draw.text(
                    (section_x, section_y + 55 + (idx * 32)),
                    info_line,
                    font=HUD.load_font(16),
                    fill=Cfg.C_TEXT_MAIN
                )
            
            section_x += section_width
            draw.text(
                (section_x, section_y),
                "🔒 SSL/TLS",
                font=HUD.load_font(21, True),
                fill=Cfg.C_GOLD
            )
            draw.line(
                [(section_x, section_y + 32), (section_x + section_width - 50, section_y + 32)],
                fill=Cfg.C_GOLD,
                width=3
            )
            
            ssl_valid = data.get('ssl', False)
            ssl_color = Cfg.C_SUCCESS if ssl_valid else Cfg.C_DANGER
            
            ssl_info = [
                f"STATUS: {'✅ VALID' if ssl_valid else '❌ INVALID'}",
                f"TLS: {data.get('tls_ver', 'N/A')}",
                f"ISSUER: {Utils.sanitize_text(data.get('issuer', 'N/A'), 20)}",
                f"CIPHER: {Utils.sanitize_text(data.get('cipher', 'N/A'), 22)}",
                f"EXPIRES: {data.get('days_left', 0)}d",
                f"DOMAINS: {len(data.get('san', []))}"
            ]
            
            for idx, info_line in enumerate(ssl_info):
                text_color = ssl_color if idx == 0 else Cfg.C_TEXT_MAIN
                draw.text(
                    (section_x, section_y + 55 + (idx * 32)),
                    info_line,
                    font=HUD.load_font(16),
                    fill=text_color
                )
            
            section_x += section_width
            draw.text(
                (section_x, section_y),
                "⚙️ SERVER",
                font=HUD.load_font(21, True),
                fill=Cfg.C_CYAN
            )
            draw.line(
                [(section_x, section_y + 32), (section_x + section_width - 50, section_y + 32)],
                fill=Cfg.C_CYAN,
                width=3
            )
            
            status_code = data.get('status', 0)
            status_color = Cfg.C_SUCCESS if status_code < 400 else Cfg.C_DANGER
            
            server_info = [
                f"HTTP: {status_code}",
                f"SRV: {Utils.sanitize_text(data.get('server', 'N/A'), 22)}",
                f"CDN: {Utils.sanitize_text(data.get('cdn', 'None'), 20)}",
                f"WAF: {Utils.sanitize_text(data.get('waf', 'None'), 20)}",
                f"TIME: {data.get('load_time', 0)}ms",
                f"SIZE: {data.get('size', 'N/A')}"
            ]
            
            for idx, info_line in enumerate(server_info):
                text_color = status_color if idx == 0 else Cfg.C_TEXT_MAIN
                draw.text(
                    (section_x, section_y + 55 + (idx * 32)),
                    info_line,
                    font=HUD.load_font(16),
                    fill=text_color
                )
            
            section_x += section_width
            draw.text(
                (section_x, section_y),
                "🛡️ SECURITY",
                font=HUD.load_font(21, True),
                fill=Cfg.C_SUCCESS
            )
            draw.line(
                [(section_x, section_y + 32), (section_x + section_width - 50, section_y + 32)],
                fill=Cfg.C_SUCCESS,
                width=3
            )
            
            security_score = data.get('security_score', 0)
            
            draw.text(
                (section_x, section_y + 55),
                f"SCORE: {security_score}%",
                font=HUD.load_font(16),
                fill=Cfg.C_TEXT_MAIN
            )
            HUD.draw_progress_bar(
                draw,
                section_x,
                section_y + 85,
                section_width - 50,
                16,
                security_score,
                100,
                Cfg.C_SUCCESS
            )
            
            sec_headers_count = data.get('sec_headers_count', 0)
            draw.text(
                (section_x, section_y + 110),
                f"HEADERS: {sec_headers_count}",
                font=HUD.load_font(16),
                fill=Cfg.C_TEXT_MAIN
            )
            HUD.draw_progress_bar(
                draw,
                section_x,
                section_y + 140,
                section_width - 50,
                16,
                sec_headers_count,
                10,
                Cfg.C_GOLD
            )
            
            jitter_value = data.get('jitter', 0)
            draw.text(
                (section_x, section_y + 165),
                f"JITTER: {jitter_value:.1f}ms",
                font=HUD.load_font(16),
                fill=Cfg.C_TEXT_MAIN
            )
            HUD.draw_progress_bar(
                draw,
                section_x,
                section_y + 195,
                section_width - 50,
                16,
                min(jitter_value, 100),
                100,
                Cfg.C_WARN
            )
            
            section_x += section_width
            draw.text(
                (section_x, section_y),
                "📊 TECH",
                font=HUD.load_font(21, True),
                fill=Cfg.C_MAGENTA
            )
            draw.line(
                [(section_x, section_y + 32), (section_x + section_width - 50, section_y + 32)],
                fill=Cfg.C_MAGENTA,
                width=3
            )
            
            tech_stack = data.get('tech_stack', ['Unknown'])[:7]
            for idx, tech in enumerate(tech_stack):
                draw.text(
                    (section_x, section_y + 55 + (idx * 32)),
                    f"▸ {Utils.sanitize_text(tech, 20)}",
                    font=HUD.load_font(16),
                    fill=Cfg.C_TEXT_MAIN
                )
            
            section_x += section_width
            draw.text(
                (section_x, section_y),
                "📡 DNS",
                font=HUD.load_font(21, True),
                fill=Cfg.C_CYAN
            )
            draw.line(
                [(section_x, section_y + 32), (section_x + section_width - 50, section_y + 32)],
                fill=Cfg.C_CYAN,
                width=3
            )
            
            dns_counts = data.get('dns_counts', {})
            dns_info = [
                f"A: {dns_counts.get('A', 0)}",
                f"AAAA: {dns_counts.get('AAAA', 0)}",
                f"MX: {dns_counts.get('MX', 0)}",
                f"NS: {dns_counts.get('NS', 0)}",
                f"TXT: {dns_counts.get('TXT', 0)}",
                f"SPF: {'✅' if dns_counts.get('SPF', 0) > 0 else '❌'}"
            ]
            
            for idx, info_line in enumerate(dns_info):
                draw.text(
                    (section_x, section_y + 55 + (idx * 32)),
                    info_line,
                    font=HUD.load_font(16),
                    fill=Cfg.C_TEXT_MAIN
                )
            
            section_x += section_width
            draw.text(
                (section_x, section_y),
                "📈 METRICS",
                font=HUD.load_font(21, True),
                fill=Cfg.C_INFO
            )
            draw.line(
                [(section_x, section_y + 32), (section_x + section_width - 50, section_y + 32)],
                fill=Cfg.C_INFO,
                width=3
            )
            
            metrics_info = [
                f"COOKIES: {data.get('cookies', 0)}",
                f"REQUESTS: {data.get('network_requests', 0)}",
                f"LINKS: {data.get('links_count', 0)}",
                f"IMAGES: {data.get('images_count', 0)}",
                f"SCRIPTS: {data.get('scripts_count', 0)}",
                f"FORMS: {data.get('forms_count', 0)}"
            ]
            
            for idx, info_line in enumerate(metrics_info):
                draw.text(
                    (section_x, section_y + 55 + (idx * 32)),
                    info_line,
                    font=HUD.load_font(16),
                    fill=Cfg.C_TEXT_MAIN
                )
            
            scanline_overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
            scanline_draw = ImageDraw.Draw(scanline_overlay)
            
            for y in range(0, total_height, 4):
                scanline_draw.line(
                    [(0, y), (total_width, y)],
                    fill=(0, 0, 0, 30),
                    width=1
                )
            
            canvas = Image.alpha_composite(canvas, scanline_overlay)
            
            output_buffer = io.BytesIO()
            canvas.save(output_buffer, format='PNG', quality=95, optimize=True)
            
            return output_buffer.getvalue()
            
        except Exception as e:
            logger.error(f"HUD rendering error: {e}")
            traceback.print_exc()
            return screenshot

class NexusBot:
    @staticmethod
    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        welcome_message = (
            f"<blockquote>"
            f"<b>🛡️ ELITE CYBER INTELLIGENCE PLATFORM</b>\n"
            f"<code>{'═' * 35}</code>\n\n"
            f"<b>VERSION: </b> <code>{Cfg.VER}</code>\n"
            f"<b>STATUS:</b> <tg-spoiler>🟢 FULLY OPERATIONAL</tg-spoiler>\n"
            f"<b>CLEARANCE:</b> QUANTUM-LEVEL ACCESS GRANTED\n"
            f"<b>PROTOCOL:</b> DEEP PENETRATION ANALYSIS\n\n"
            f"<b>🔬 ANALYSIS CAPABILITIES:</b>\n"
            f"├ 🌐 Advanced DNS Intelligence\n"
            f"├ 🔒 SSL/TLS Deep Inspection\n"
            f"├ 🗺️ Geographic IP Tracing\n"
            f"├ 🌍 Global Host Checking (50 Countries)\n"
            f"├ 🛡️ CDN & WAF Detection\n"
            f"├ 💻 Technology Stack Profiling\n"
            f"├ 📊 Security Headers Analysis\n"
            f"├ ⚡ Performance Metrics\n"
            f"├ 🔐 Cloudflare Bypass System\n"
            f"└ 🎯 Real-Time Threat Assessment\n\n"
            f"<b>🎯 DATA COLLECTION:</b>\n"
            f"├ 80+ Data Points Per Scan\n"
            f"├ Multi-Country Availability Test\n"
            f"├ Advanced Proxy Rotation\n"
            f"├ Network Performance Analysis\n"
            f"└ Browser Fingerprinting\n\n"
            f"<b>⚙️ SYSTEM FEATURES:</b>\n"
            f"├ Automated Cookie Handling\n"
            f"├ Challenge Bypass Technology\n"
            f"├ Stealth Mode Operations\n"
            f"└ High-Speed Data Processing\n\n"
            f"<code>{'═' * 35}</code>\n"
            f"<i>📡 Send target URL to initiate quantum scan</i>\n"
            f"<i>🔐 All operations are secure and anonymous</i>\n"
            f"</blockquote>"
        )
        
        await update.message. reply_text(
            welcome_message,
            parse_mode=ParseMode.HTML
        )
    
    @staticmethod
    async def msg_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_input = update.message.text. strip()
        target_url = Utils.fix_url(user_input)
        
        if not target_url:
            await update.message.reply_text(
                "<blockquote>"
                "⚠️ <b>INVALID TARGET FORMAT</b>\n"
                "<code>────────────────────</code>\n"
                "Please provide a valid URL.\n\n"
                "<b>Examples:</b>\n"
                "• https://example.com\n"
                "• example.com\n"
                "• www.example.com"
                "</blockquote>",
                parse_mode=ParseMode.HTML
            )
            return
        
        init_message = await update.message.reply_text(
            f"<blockquote>"
            f"🚀 <b>INITIALIZING QUANTUM SCAN PROTOCOL</b>\n"
            f"<code>{'═' * 30}</code>\n\n"
            f"🎯 <b>TARGET: </b> <code>{target_url[: 65]}</code>\n"
            f"🔐 <b>SESSION: </b> <code>{Utils.gen_sess()[:12]}</code>\n\n"
            f"⚡ Bypassing security layers...\n"
            f"🔄 Acquiring stealth proxy node...\n"
            f"🌐 Establishing secure connection...\n"
            f"🛡️ Enabling countermeasures...\n\n"
            f"<i>⏳ Estimated time: 15-30 seconds</i>\n"
            f"</blockquote>",
            parse_mode=ParseMode.HTML
        )
        
        session_id = Utils.gen_sess()
        
        asyncio.create_task(
            NexusBot.process_scan(
                context. bot,
                update.message.chat.id,
                init_message. message_id,
                target_url,
                session_id
            )
        )
    
    @staticmethod
    async def process_scan(bot, chat_id:  int, msg_id: int, url: str, sid: str, y_offset: int = 0):
        try:
            domain = Utils.get_domain(url)
            
            if not domain:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text="<blockquote>❌ <b>INVALID DOMAIN</b>\nCould not extract domain from URL</blockquote>",
                    parse_mode=ParseMode.HTML
                )
                return
            
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=(
                    f"<blockquote>"
                    f"🔍 <b>PHASE 1/5:  NETWORK RECONNAISSANCE</b>\n"
                    f"<code>{'═' * 30}</code>\n\n"
                    f"🌐 Resolving DNS records...\n"
                    f"📡 Tracing network topology...\n"
                    f"🗺️ Mapping infrastructure...\n"
                    f"⚡ Analyzing routing paths...\n"
                    f"</blockquote>"
                ),
                parse_mode=ParseMode. HTML
            )
            
            event_loop = asyncio.get_event_loop()
            
            with ThreadPoolExecutor(max_workers=12) as executor:
                try:
                    resolved_ip = await event_loop.run_in_executor(
                        executor,
                        socket.gethostbyname,
                        domain
                    )
                except: 
                    resolved_ip = "0.0.0.0"
                
                dns_future = event_loop.run_in_executor(executor, CoreNet.resolve_dns, domain)
                geo_future = event_loop.run_in_executor(executor, CoreNet.get_geoip, resolved_ip)
                ssl_future = event_loop.run_in_executor(executor, CoreNet.analyze_ssl, domain, 443)
                ping_future = event_loop.run_in_executor(executor, CoreNet.tcp_ping, domain, 443, 5)
                
                proxy = await ProxyMgr.get_proxy()
                headers_future = event_loop.run_in_executor(executor, CoreNet.deep_headers, url, proxy)
                
                dns_data, geo_data, ssl_data, ping_data, headers_data = await asyncio.gather(
                    dns_future,
                    geo_future,
                    ssl_future,
                    ping_future,
                    headers_future,
                    return_exceptions=True
                )
                
                if isinstance(dns_data, Exception):
                    dns_data = {}
                if isinstance(geo_data, Exception):
                    geo_data = CoreNet.get_geoip("0.0.0.0")
                if isinstance(ssl_data, Exception):
                    ssl_data = {}
                if isinstance(ping_data, Exception):
                    ping_data = {"avg":  9999, "min": 9999, "max": 9999, "jitter": 0}
                if isinstance(headers_data, Exception):
                    headers_data = {}
            
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=(
                    f"<blockquote>"
                    f"🎯 <b>PHASE 2/5: DEEP PENETRATION TEST</b>\n"
                    f"<code>{'═' * 30}</code>\n\n"
                    f"🖼️ Capturing visual snapshot...\n"
                    f"🛡️ Bypassing Cloudflare.. .\n"
                    f"🔐 Extracting page metadata...\n"
                    f"🌐 Analyzing DOM structure...\n"
                    f"</blockquote>"
                ),
                parse_mode=ParseMode. HTML
            )
            
            browser_result = await BrowserBrain.capture(url, y_offset, full_page=False)
            
            if browser_result. get("err"):
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=(
                        f"<blockquote>"
                        f"❌ <b>SCAN FAILED</b>\n"
                        f"<code>{'═' * 30}</code>\n\n"
                        f"<b>Error:</b> {browser_result['err']}\n\n"
                        f"<i>The target may be blocking automated access. </i>"
                        f"</blockquote>"
                    ),
                    parse_mode=ParseMode.HTML
                )
                return
            
            await bot. edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=(
                    f"<blockquote>"
                    f"⚡ <b>PHASE 3/5: INTELLIGENCE ANALYSIS</b>\n"
                    f"<code>{'═' * 30}</code>\n\n"
                    f"🔬 Analyzing technology stack...\n"
                    f"📊 Computing security metrics...\n"
                    f"🧬 Profiling digital signatures...\n"
                    f"💾 Aggregating data points...\n"
                    f"</blockquote>"
                ),
                parse_mode=ParseMode. HTML
            )
            
            tech_stack = CoreNet.get_tech_stack(
                headers_data.get('headers', {}),
                browser_result.get('html', '')
            )
            
            security_score = 30
            if ssl_data.get('valid'):
                security_score += 30
            security_score += min(25, len(headers_data.get('security', [])) * 2.5)
            if headers_data.get('status', 0) < 400:
                security_score += 5
            if headers_data.get('cdn') not in ["Unknown", "None"]:
                security_score += 5
            if headers_data.get('hsts'):
                security_score += 5
            
            security_score = min(100, int(security_score))
            
            dns_counts = {
                "A": len(dns_data.get("A", [])),
                "AAAA": len(dns_data.get("AAAA", [])),
                "MX": len(dns_data. get("MX", [])),
                "NS": len(dns_data.get("NS", [])),
                "TXT": len(dns_data. get("TXT", [])),
                "SPF": len(dns_data. get("SPF", []))
            }
            
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=(
                    f"<blockquote>"
                    f"🎨 <b>PHASE 4/5: QUANTUM HUD RENDERING</b>\n"
                    f"<code>{'═' * 30}</code>\n\n"
                    f"🖼️ Generating tactical interface...\n"
                    f"📊 Rendering data visualizations...\n"
                    f"✨ Applying quantum effects...\n"
                    f"🎯 Finalizing presentation layer...\n"
                    f"</blockquote>"
                ),
                parse_mode=ParseMode.HTML
            )
            
            hud_data = {
                "sid": sid,
                "title": browser_result.get("title", "Unknown"),
                "url": url,
                "ip": resolved_ip,
                "city": geo_data.get("city", "Unknown"),
                "country": geo_data.get("country", "Unknown"),
                "flag": geo_data.get("flag", "🏳️"),
                "isp": geo_data.get("isp", "Unknown"),
                "asn": geo_data.get("asn", "N/A"),
                "hosting": geo_data.get("hosting", False),
                "ping_avg": ping_data.get("avg", 0),
                "jitter":  ping_data.get("jitter", 0),
                "ssl":  ssl_data.get("valid", False),
                "tls_ver": ssl_data.get("version", "N/A"),
                "issuer": ssl_data.get("issuer", "N/A"),
                "cipher": ssl_data.get("cipher", "N/A"),
                "days_left": ssl_data.get("days_left", 0),
                "san": ssl_data.get("san", []),
                "status": headers_data.get("status", 0),
                "server": headers_data.get("server", "Unknown"),
                "cdn": headers_data.get("cdn", "Unknown"),
                "waf": headers_data.get("waf", "Unknown"),
                "load_time": headers_data.get("load_time", 0),
                "size": Utils.fmt_size(headers_data.get("content_length", 0)),
                "security_score": security_score,
                "sec_headers_count": len(headers_data. get("security", [])),
                "tech_stack": tech_stack,
                "dns_counts": dns_counts,
                "cookies": browser_result.get("cookies", 0),
                "network_requests": browser_result.get("network_requests", 0),
                "links_count": browser_result.get("links_count", 0),
                "images_count": browser_result.get("images_count", 0),
                "scripts_count": browser_result.get("scripts_count", 0),
                "forms_count": browser_result.get("forms_count", 0)
            }
            
            Db.put(sid, hud_data)
            
            final_image = await asyncio.get_event_loop().run_in_executor(
                None,
                HUD. render_interface,
                browser_result["ss"],
                hud_data
            )
            
            await bot. edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=(
                    f"<blockquote>"
                    f"✅ <b>PHASE 5/5: MISSION COMPLETE</b>\n"
                    f"<code>{'═' * 30}</code>\n\n"
                    f"📤 Transmitting secure intelligence report...\n"
                    f"🔐 Encrypting classified data...\n"
                    f"🚀 Deploying final payload...\n"
                    f"</blockquote>"
                ),
                parse_mode=ParseMode. HTML
            )
            
            report_caption = (
                f"<blockquote>"
                f"<b>🎯 QUANTUM INTELLIGENCE REPORT</b>\n"
                f"<code>{'═' * 40}</code>\n\n"
                f"<b>📋 SESSION: </b> <code>{sid}</code>\n"
                f"<b>🌐 TARGET:</b> <code>{domain}</code>\n"
                f"<b>🔍 IP: </b> <code>{resolved_ip}</code>\n"
                f"<b>📍 LOCATION:</b> {geo_data.get('flag', '')} {geo_data.get('city', 'Unknown')}, {geo_data.get('country', 'Unknown')}\n"
                f"<b>🏢 ISP:</b> <code>{Utils.sanitize_text(geo_data.get('isp', 'Unknown'), 35)}</code>\n\n"
                f"<b>🔒 SSL: </b> {'✅ Valid' if ssl_data.get('valid') else '❌ Invalid'}\n"
                f"<b>⚡ PING:</b> <code>{ping_data.get('avg', 0):.1f}ms</code>\n"
                f"<b>📊 STATUS:</b> <code>{headers_data.get('status', 0)}</code>\n"
                f"<b>🛡️ SECURITY:</b> <code>{security_score}%</code>\n"
                f"<b>⏱️ LOAD: </b> <code>{headers_data.get('load_time', 0)}ms</code>\n\n"
                f"<b>🔧 TECH STACK:</b>\n"
            )
            
            for idx, tech in enumerate(tech_stack[: 5], 1):
                report_caption += f"  {idx}. <code>{Utils.sanitize_text(tech, 30)}</code>\n"
            
            report_caption += (
                f"\n<code>{'═' * 40}</code>\n"
                f"<i>⏰ Scan completed at {Utils.time_now()}</i>\n"
                f"<i>🔐 Data classified as QUANTUM-SECURE</i>\n"
                f"</blockquote>"
            )
            
            keyboard = [
                [
                    InlineKeyboardButton("🌍 Global Check", callback_data=f"global_{sid}"),
                    InlineKeyboardButton("🔄 Re-scan", callback_data=f"rescan_{sid}")
                ],
                [
                    InlineKeyboardButton("📊 Full DNS", callback_data=f"dns_{sid}"),
                    InlineKeyboardButton("🔒 SSL Details", callback_data=f"ssl_{sid}")
                ],
                [
                    InlineKeyboardButton("📈 Headers", callback_data=f"headers_{sid}"),
                    InlineKeyboardButton("🗺️ GeoIP", callback_data=f"geo_{sid}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # FIX: Xóa message text, gửi photo mới thay vì edit
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except: 
                pass
            
            await bot.send_photo(
                chat_id=chat_id,
                photo=final_image,
                caption=report_caption,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Scan processing error: {e}")
            traceback.print_exc()
            
            try:
                await bot. edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=(
                        f"<blockquote>"
                        f"❌ <b>CRITICAL ERROR</b>\n"
                        f"<code>{'═' * 30}</code>\n\n"
                        f"<b>Error:</b> {str(e)[:100]}\n\n"
                        f"<i>System encountered unexpected exception. </i>\n"
                        f"<i>Please try again or contact support.</i>"
                        f"</blockquote>"
                    ),
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
    
    @staticmethod
    async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        data_parts = query.data.split('_', 1)
        action = data_parts[0]
        session_id = data_parts[1] if len(data_parts) > 1 else None
        
        if not session_id:
            await query.edit_message_caption(
                caption="<blockquote>❌ <b>SESSION EXPIRED</b>\nPlease initiate a new scan. </blockquote>",
                parse_mode=ParseMode.HTML
            )
            return
        
        session_data = Db.get(session_id)
        
        if not session_data: 
            await query.edit_message_caption(
                caption="<blockquote>❌ <b>SESSION NOT FOUND</b>\nData may have expired.  Please scan again.</blockquote>",
                parse_mode=ParseMode. HTML
            )
            return
        
        if action == "global":
            await NexusBot.handle_global_check(query, session_data)
        elif action == "rescan":
            await NexusBot.handle_rescan(query, session_data, context)
        elif action == "dns":
            await NexusBot.handle_dns_info(query, session_data)
        elif action == "ssl": 
            await NexusBot.handle_ssl_info(query, session_data)
        elif action == "headers":
            await NexusBot.handle_headers_info(query, session_data)
        elif action == "geo": 
            await NexusBot.handle_geo_info(query, session_data)
    
    @staticmethod
    async def handle_global_check(query, session_data):
        # FIX: Chỉ edit caption, không edit photo
        await query.edit_message_caption(
            caption=(
                f"<blockquote>"
                f"🌍 <b>INITIATING GLOBAL AVAILABILITY CHECK</b>\n"
                f"<code>{'═' * 35}</code>\n\n"
                f"🔄 Testing from 50 countries worldwide...\n"
                f"⏱️ This may take 30-60 seconds...\n"
                f"</blockquote>"
            ),
            parse_mode=ParseMode.HTML
        )
        
        url = session_data. get("url", "")
        
        try:
            global_results = await CoreNet.check_host_global(url, max_workers=50)
            
            success_count = sum(1 for r in global_results. values() if r.get("success", False))
            total_count = len(global_results)
            success_rate = (success_count / total_count * 100) if total_count > 0 else 0
            
            avg_latency = sum(r.get("latency", 9999) for r in global_results.values() if r.get("success")) / success_count if success_count > 0 else 0
            
            report = (
                f"<blockquote>"
                f"<b>🌍 GLOBAL AVAILABILITY REPORT</b>\n"
                f"<code>{'═' * 35}</code>\n\n"
                f"<b>📊 OVERVIEW:</b>\n"
                f"• Success Rate: <code>{success_rate:.1f}%</code>\n"
                f"• Countries Tested: <code>{total_count}</code>\n"
                f"• Successful:  <code>{success_count}</code>\n"
                f"• Failed: <code>{total_count - success_count}</code>\n"
                f"• Avg Latency: <code>{avg_latency:.1f}ms</code>\n\n"
                f"<b>🌐 TOP REGIONS:</b>\n"
            )
            
            successful_regions = sorted(
                [r for r in global_results.values() if r.get("success")],
                key=lambda x:  x.get("latency", 9999)
            )[:10]
            
            for idx, region in enumerate(successful_regions, 1):
                report += (
                    f"{idx}. {region.get('flag', '🏳️')} {region.get('country', 'XX')} - "
                    f"<code>{region.get('latency', 0):.0f}ms</code>\n"
                )
            
            report += (
                f"\n<code>{'═' * 35}</code>\n"
                f"<i>⏰ Completed at {Utils.time_now()}</i>\n"
                f"</blockquote>"
            )
            
            await query.edit_message_caption(
                caption=report,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Global check error: {e}")
            await query.edit_message_caption(
                caption=f"<blockquote>❌ <b>ERROR</b>\n{str(e)[:100]}</blockquote>",
                parse_mode=ParseMode.HTML
            )
    
    @staticmethod
    async def handle_rescan(query, session_data, context):
        url = session_data.get("url", "")
        new_session_id = Utils.gen_sess()
        
        # FIX: Xóa message cũ, gửi message mới
        try:
            await query.message.delete()
        except:
            pass
        
        init_message = await query.message.reply_text(
            f"<blockquote>"
            f"🔄 <b>RE-SCANNING TARGET</b>\n"
            f"<code>{'═' * 30}</code>\n\n"
            f"🎯 Target: <code>{url[: 50]}</code>\n"
            f"🆕 New Session:  <code>{new_session_id[: 12]}</code>\n\n"
            f"⏳ Please wait.. .\n"
            f"</blockquote>",
            parse_mode=ParseMode.HTML
        )
        
        asyncio.create_task(
            NexusBot.process_scan(
                context.bot,
                query.message.chat.id,
                init_message.message_id,
                url,
                new_session_id
            )
        )
    
    @staticmethod
    async def handle_dns_info(query, session_data):
        # FIX: Placeholder implementation
        try:
            url = session_data.get("url", "")
            domain = Utils.get_domain(url)
            
            await query.edit_message_caption(
                caption=(
                    f"<blockquote>"
                    f"📊 <b>FETCHING DNS RECORDS</b>\n"
                    f"<code>{'═' * 30}</code>\n\n"
                    f"🔄 Querying DNS servers...\n"
                    f"</blockquote>"
                ),
                parse_mode=ParseMode. HTML
            )
            
            event_loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=1) as executor:
                dns_data = await event_loop.run_in_executor(executor, CoreNet.resolve_dns, domain)
            
            dns_report = (
                f"<blockquote>"
                f"<b>🌐 DNS RECORDS FOR {domain. upper()}</b>\n"
                f"<code>{'═' * 40}</code>\n\n"
            )
            
            record_types = ["A", "AAAA", "MX", "NS", "TXT", "CNAME", "SOA", "CAA"]
            for record_type in record_types: 
                records = dns_data.get(record_type, [])
                if records:
                    dns_report += f"<b>{record_type} RECORDS:</b>\n"
                    for record in records[: 5]: 
                        dns_report += f"  • <code>{Utils.sanitize_text(record, 50)}</code>\n"
                    dns_report += "\n"
            
            dns_report += (
                f"<code>{'═' * 40}</code>\n"
                f"<i>✅ DNS Lookup Complete</i>\n"
                f"</blockquote>"
            )
            
            await query.edit_message_caption(
                caption=dns_report,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"DNS info error: {e}")
            await query.edit_message_caption(
                caption=f"<blockquote>❌ <b>ERROR</b>\n{str(e)[:100]}</blockquote>",
                parse_mode=ParseMode.HTML
            )
    
    @staticmethod
    async def handle_ssl_info(query, session_data):
        # FIX:  Placeholder implementation
        try:
            url = session_data.get("url", "")
            domain = Utils.get_domain(url)
            
            await query.edit_message_caption(
                caption=(
                    f"<blockquote>"
                    f"🔒 <b>ANALYZING SSL CERTIFICATE</b>\n"
                    f"<code>{'═' * 30}</code>\n\n"
                    f"🔄 Fetching certificate details...\n"
                    f"</blockquote>"
                ),
                parse_mode=ParseMode.HTML
            )
            
            event_loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=1) as executor:
                ssl_data = await event_loop. run_in_executor(executor, CoreNet.analyze_ssl, domain, 443)
            
            ssl_report = (
                f"<blockquote>"
                f"<b>🔒 SSL/TLS CERTIFICATE ANALYSIS</b>\n"
                f"<code>{'═' * 40}</code>\n\n"
                f"<b>Status:</b> {'✅ VALID' if ssl_data.get('valid') else '❌ INVALID'}\n"
                f"<b>Protocol:</b> <code>{ssl_data.get('version', 'N/A')}</code>\n"
                f"<b>Issuer:</b> <code>{Utils.sanitize_text(ssl_data.get('issuer', 'N/A'), 40)}</code>\n"
                f"<b>Subject:</b> <code>{Utils.sanitize_text(ssl_data.get('subject', 'N/A'), 40)}</code>\n"
                f"<b>Cipher Suite:</b> <code>{Utils.sanitize_text(ssl_data.get('cipher', 'N/A'), 40)}</code>\n"
                f"<b>Key Size:</b> <code>{ssl_data.get('key_size', 0)} bits</code>\n"
                f"<b>Expires:</b> <code>{ssl_data.get('expiry', 'N/A')}</code>\n"
                f"<b>Days Left:</b> <code>{ssl_data.get('days_left', 0)}</code>\n"
                f"<b>Self-Signed:</b> {'Yes' if ssl_data.get('self_signed') else 'No'}\n"
                f"<b>Wildcard:</b> {'Yes' if ssl_data.get('wildcard') else 'No'}\n"
            )
            
            san_list = ssl_data.get('san', [])
            if san_list:
                ssl_report += f"\n<b>SANs ({len(san_list)}):</b>\n"
                for san in san_list[:5]: 
                    ssl_report += f"  • <code>{Utils.sanitize_text(san, 45)}</code>\n"
                if len(san_list) > 5:
                    ssl_report += f"  • ... and {len(san_list) - 5} more\n"
            
            ssl_report += (
                f"\n<code>{'═' * 40}</code>\n"
                f"<i>✅ SSL Analysis Complete</i>\n"
                f"</blockquote>"
            )
            
            await query.edit_message_caption(
                caption=ssl_report,
                parse_mode=ParseMode. HTML
            )
        except Exception as e:
            logger.error(f"SSL info error: {e}")
            await query.edit_message_caption(
                caption=f"<blockquote>❌ <b>ERROR</b>\n{str(e)[:100]}</blockquote>",
                parse_mode=ParseMode.HTML
            )
    
    @staticmethod
    async def handle_headers_info(query, session_data):
        try:
            url = session_data.get("url", "")
            
            await query.edit_message_caption(
                caption=(
                    f"<blockquote>"
                    f"📊 <b>ANALYZING HTTP HEADERS</b>\n"
                    f"<code>{'═' * 30}</code>\n\n"
                    f"🔄 Fetching header information...\n"
                    f"</blockquote>"
                ),
                parse_mode=ParseMode.HTML
            )
            
            event_loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=1) as executor:
                headers_data = await event_loop.run_in_executor(executor, CoreNet. deep_headers, url, None)
            
            headers_report = (
                f"<blockquote>"
                f"<b>📊 HTTP HEADERS ANALYSIS</b>\n"
                f"<code>{'═' * 40}</code>\n\n"
                f"<b>Status Code:</b> <code>{headers_data.get('status', 0)}</code>\n"
                f"<b>Server:</b> <code>{Utils.sanitize_text(headers_data.get('server', 'Unknown'), 35)}</code>\n"
                f"<b>Content-Type:</b> <code>{Utils. sanitize_text(headers_data.get('content_type', 'Unknown'), 35)}</code>\n"
                f"<b>Content-Length:</b> <code>{Utils.fmt_size(headers_data.get('content_length', 0))}</code>\n"
                f"<b>Load Time:</b> <code>{headers_data.get('load_time', 0)}ms</code>\n"
                f"<b>CDN:</b> <code>{headers_data.get('cdn', 'Unknown')}</code>\n"
                f"<b>WAF:</b> <code>{headers_data.get('waf', 'Unknown')}</code>\n"
                f"<b>Cookies:</b> <code>{headers_data.get('cookie_count', 0)}</code>\n"
                f"<b>Redirects:</b> <code>{headers_data.get('redirects', 0)}</code>\n"
                f"<b>HSTS:</b> {'✅ Enabled' if headers_data.get('hsts') else '❌ Disabled'}\n"
                f"<b>CSP:</b> {'✅ Enabled' if headers_data.get('csp') else '❌ Disabled'}\n\n"
            )
            
            security_headers = headers_data.get('security', [])
            if security_headers: 
                headers_report += f"<b>Security Headers ({len(security_headers)}):</b>\n"
                for header in security_headers[:10]:
                    headers_report += f"  ✅ {header}\n"
            
            headers_report += (
                f"\n<code>{'═' * 40}</code>\n"
                f"<i>✅ Headers Analysis Complete</i>\n"
                f"</blockquote>"
            )
            
            await query.edit_message_caption(
                caption=headers_report,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger. error(f"Headers info error:  {e}")
            await query. edit_message_caption(
                caption=f"<blockquote>❌ <b>ERROR</b>\n{str(e)[:100]}</blockquote>",
                parse_mode=ParseMode.HTML
            )
    
    @staticmethod
    async def handle_geo_info(query, session_data):
        # FIX: Placeholder implementation
        try:
            ip = session_data.get("ip", "")
            
            await query.edit_message_caption(
                caption=(
                    f"<blockquote>"
                    f"🗺️ <b>ANALYZING GEOLOCATION</b>\n"
                    f"<code>{'═' * 30}</code>\n\n"
                    f"🔄 Fetching GeoIP data...\n"
                    f"</blockquote>"
                ),
                parse_mode=ParseMode.HTML
            )
            
            event_loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=1) as executor:
                geo_data = await event_loop.run_in_executor(executor, CoreNet.get_geoip, ip)
            
            geo_report = (
                f"<blockquote>"
                f"<b>🗺️ GEOLOCATION INTELLIGENCE</b>\n"
                f"<code>{'═' * 40}</code>\n\n"
                f"<b>IP Address:</b> <code>{ip}</code>\n"
                f"<b>Flag:</b> {geo_data.get('flag', '🏳️')}\n"
                f"<b>Country:</b> <code>{geo_data.get('country', 'Unknown')}</code>\n"
                f"<b>City:</b> <code>{geo_data.get('city', 'Unknown')}</code>\n"
                f"<b>Region:</b> <code>{geo_data.get('region', 'Unknown')}</code>\n"
                f"<b>ISP:</b> <code>{Utils.sanitize_text(geo_data.get('isp', 'Unknown'), 40)}</code>\n"
                f"<b>Organization:</b> <code>{Utils. sanitize_text(geo_data.get('org', 'N/A'), 40)}</code>\n"
                f"<b>ASN:</b> <code>{geo_data.get('asn', 'N/A')}</code>\n"
                f"<b>Timezone:</b> <code>{geo_data.get('tz', 'UTC')}</code>\n"
                f"<b>Latitude: </b> <code>{geo_data.get('lat', 0)}</code>\n"
                f"<b>Longitude:</b> <code>{geo_data.get('lon', 0)}</code>\n"
                f"<b>Continent:</b> <code>{geo_data.get('continent', 'Unknown')}</code>\n"
                f"<b>Postal Code:</b> <code>{geo_data.get('postal', 'N/A')}</code>\n\n"
                f"<b>Flags:</b>\n"
                f"  🏢 Hosting: {'Yes' if geo_data.get('hosting') else 'No'}\n"
                f"  🔌 Proxy: {'Yes' if geo_data.get('proxy') else 'No'}\n"
                f"  🔐 VPN: {'Yes' if geo_data.get('vpn') else 'No'}\n"
                f"  🧅 Tor: {'Yes' if geo_data.get('tor') else 'No'}\n"
                f"  📱 Mobile: {'Yes' if geo_data.get('mobile') else 'No'}\n"
                f"<code>{'═' * 40}</code>\n"
                f"<i>✅ Geolocation Analysis Complete</i>\n"
                f"</blockquote>"
            )
            
            await query. edit_message_caption(
                caption=geo_report,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Geo info error: {e}")
            await query.edit_message_caption(
                caption=f"<blockquote>❌ <b>ERROR</b>\n{str(e)[:100]}</blockquote>",
                parse_mode=ParseMode.HTML
            )

class HealthServer(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health' or self.path == '/': 
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            stats = Db.get_stats()
            
            response = {
                "status": "operational",
                "service":  Cfg.NAME,
                "version": Cfg.VER,
                "uptime": time.time(),
                "stats":  stats
            }
            
            self. wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        pass

def run_health_server():
    server = HTTPServer((Cfg.HOST, Cfg. PORT), HealthServer)
    logger.info(f"Health server running on {Cfg.HOST}:{Cfg.PORT}")
    server.serve_forever()

async def main():
    logger.info(f"🚀 Starting {Cfg.NAME} v{Cfg.VER}")
    
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    await ProxyMgr._fetch_proxies()
    
    defaults = Defaults(parse_mode=ParseMode.HTML)
    
    app = ApplicationBuilder().token(Cfg.TOKEN).defaults(defaults).build()
    
    app.add_handler(CommandHandler("start", NexusBot.start))
    app.add_handler(MessageHandler(filters. TEXT & ~filters.COMMAND, NexusBot.msg_handler))
    app.add_handler(CallbackQueryHandler(NexusBot.callback_handler))
    
    logger.info("✅ Bot initialized successfully")
    logger.info(f"📡 Listening for commands...")
    
    await app.initialize()
    await app.start()
    await app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Bot stopped by user")
    except Exception as e: 
        logger.critical(f"💥 Fatal error: {e}")
        traceback.print_exc()
# === END FILE: cpp.py ===

# === BEGIN FILE: bsfix.py ===
import asyncio
import concurrent.futures
import hashlib
import hmac
import html
import io
import json
import logging
import os
import pathlib
import random
import re
import requests
import signal
import sqlite3
import string
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import gc
import atexit
from datetime import datetime
from functools import wraps
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import parse_qs, unquote, urlparse, quote

import aiohttp
import phonenumbers
import PIL.Image
import psutil
import pytz
from phonenumbers import carrier, geocoder
from requests. adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _aiogram_available_check():
    return globals().get('AIOGRAM_AVAILABLE', False) and globals().get('bot_aiogram', None) is not None

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    logging.warning("Google Generative AI not available")

try:
    from bs4 import BeautifulSoup
    from fake_useragent import UserAgent
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    logging. warning("BeautifulSoup4/fake_useragent not available")

try:
    from gtts import gTTS
    import qrcode
    TTS_QR_AVAILABLE = True
except ImportError:
    TTS_QR_AVAILABLE = False
    logging.warning("gTTS/qrcode not available")

try:
    from aiogram import Bot, Dispatcher, Router
    from aiogram.client.default import DefaultBotProperties
    from aiogram.enums import ParseMode
    from aiogram.exceptions import (
        TelegramBadRequest,
        TelegramForbiddenError,
        TelegramNetworkError
    )
    from aiogram. filters import Command
    from aiogram.types import (
        BotCommand,
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        InputMediaPhoto,
        Message,
        User,
    )
    AIOGRAM_AVAILABLE = True
except ImportError:
    AIOGRAM_AVAILABLE = False
    logging.warning('aiogram not available — fallback enabled')

    class Router:
        def __init__(self):
            pass

    class Message:
        pass

    class User:
        pass

    class BotCommand:
        def __init__(self, command=None, description=None):
            self.command = command
            self.description = description

    class InlineKeyboardButton:
        def __init__(self, text=None, url=None):
            self.text = text
            self.url = url

    class InlineKeyboardMarkup:
        def __init__(self, inline_keyboard=None):
            self.inline_keyboard = inline_keyboard

    class InputMediaPhoto:
        pass

    class Command:
        def __init__(self, cmd):
            self.cmd = cmd

    AIOGRAM_AVAILABLE = False
    logging.warning("aiogram not available")

try:
    from moviepy.editor import VideoFileClip
    MOVIEPY_AVAILABLE = True
except ImportError:
    MOVIEPY_AVAILABLE = False
    logging.warning("moviepy not available")

try:
    from telebot import TeleBot, types
    from telebot.async_telebot import AsyncTeleBot
    TELEBOT_AVAILABLE = True
except ImportError:
    TELEBOT_AVAILABLE = False
    logging.warning("telebot not available")

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s:%(lineno)d: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os. getenv('BOT_TOKEN', "8413179871:AAGR-mZMPrccK8aUIY1GUkWmwKrAymCz5lw")
ADMIN_IDS = [7679054753, 6993504486]
OWNER_USERNAME = "tg_mediavip"
GROUP_ID = -1002598824850
DB_FILE = "ultimate_premium. db"
LOG_FILE = "ultimate_bot.log"
DATA_DIR = "./data"
VIETNAM_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

os.makedirs(DATA_DIR, exist_ok=True)

GEMINI_API_KEY = "AIzaSyAWp3AxiFF5OL1rFD_3WmdTe3lMRPgEWVw"
OPENWEATHER_API_KEY = "e707d13f116e5f7ac80bd21c37883e5e"
WEATHERAPI_KEY = "fe221e3a25734f0297994922240611"
ZING_API_KEY = "X5BM3w8N7MKozC0B85o4KMlzLZKhV00y"
ZING_SECRET_KEY = "acOrvUS15XRW2o9JksiK1KgQ6Vbds8ZW"
ZING_VERSION = "1.11.11"
ZING_URL = "https://zingmp3.vn"
TOMORROW_API_KEY = "mdTWQAInBIDB3mHiDtkwuTlwhVB50rqn"

START_BALANCE = 10000
BANK_INFO = "💰 Hướng dẫn nạp tiền:\n• Chủ TK: *NGUYEN TIEN DO*\n• Số TK: `68609666778899`\n• Ngân hàng: *MBBANK - QUÂN ĐỘI*"
QR_CODE_IMAGE_URL = "https://ibb.co/W4pcDM7Q"

RANDOM_THANKS = [
    "Chân thành cảm ơn bạn đã tin tưởng và đồng hành cùng chúng tôi! ",
    "Lòng biết ơn sâu sắc vì sự hỗ trợ tuyệt vời của bạn.  Giao dịch thành công!",
    "Cảm ơn!  Sự ủng hộ của bạn là động lực lớn nhất của chúng tôi."
]

API_SEARCH_BASE = "https://bj-microsoft-search-ai.vercel.app/"
API_XOSO_URL = "https://nguyenmanh.name. vn/api/xsmb? apikey=OUEaxPOl"
API_ANH_GAI = "https://api.zeidteam.xyz/images/gai"
API_VD_GAI = "https://api.zeidteam.xyz/videos/gai"
API_FB_INFO = "https://api.zeidteam.xyz/facebook/info? uid={uid}"
API_TT_INFO = "https://api. zeidteam.xyz/tiktok/user-info?username={username}"
API_SCL_DOWN = "https://adidaphat.site/scl/download? url={url}"
API_NGL_SPAM = "https://adidaphat.site/ngl? username={username}&message={message}&amount={amount}"

PROXY_APIS = [
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
]

LCG_MULTIPLIER = 1337
LCG_INCREMENT = 42069
LCG_MODULUS = 16**8

LOCAL_VIDEO_PATH = "vd. mp4"
IPLOOKUP_API = "http://ip-api.com/json/{ip}? fields=status,message,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org,as,query"
REQUEST_TIMEOUT = 25
TIMEOUT_SHORT = 180
TIMEOUT_MEDIUM = 360
TIMEOUT_LONG = 3600

AI_MODELS = {
    "gemini-2. 0-flash": "⚡ Flash 2.0",
    "gemini-2.5-pro": "💎 Pro 2.5",
    "gemini-3-pro": "📱 Vip 3",
}
CURRENT_MODEL = "gemini-2.0-flash"

TRIGGERS_MUSIC = [
    "nhạc", "nhac", "music", "play", "nghe", "song", "bài hát", "bai hat",
    "track", "sound", "scl", "mp3", "tìm bài", "tim bai", "audio"
]

TRIGGERS_VOICE = [
    "tách", "tach", "lấy nhạc", "lay nhac", "crvoice", "voice", "âm thanh",
    "am thanh", "convert", "chuyển đổi", "chuyen doi", "mp3", "audio", "lấy tiếng"
]

TRIGGERS_TIKTOK_SEARCH = [
    "tiktok", "tt", "douyin", "video", "vid", "clip", "xem"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537. 36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5. 0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
]

BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

SC_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://soundcloud.com",
    "Referer": "https://soundcloud.com/",
    "Connection": "keep-alive",
}

SESSION = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=0.6,
    status_forcelist=(403, 429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"])
)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))
SESSION. headers.update(SC_HEADERS)

SEARCH_CONTEXT = {}
CONTEXT_TIMESTAMP = {}
CONTEXT_TTL = 15 * 60

PLAYER_STATE = {}
PLAYER_LOCK = threading.Lock()

ZINGMP3_DATA = {}

BANK_CODES = {
    "vcb": {"bin": "970436", "name": "VIETCOMBANK", "short_name": "Vietcombank"},
    "vietcombank": {"bin": "970436", "name": "VIETCOMBANK", "short_name": "Vietcombank"},
    "tcb": {"bin": "970407", "name": "TECHCOMBANK", "short_name": "Techcombank"},
    "techcombank": {"bin": "970407", "name": "TECHCOMBANK", "short_name": "Techcombank"},
    "mb": {"bin": "970422", "name": "MB BANK", "short_name": "MBBank"},
    "mbbank": {"bin": "970422", "name": "MB BANK", "short_name": "MBBank"},
    "mb bank": {"bin": "970422", "name": "MB BANK", "short_name": "MBBank"},
    "acb": {"bin": "970416", "name": "ACB", "short_name": "ACB"},
    "vib": {"bin": "970441", "name": "VIB", "short_name": "VIB"},
    "bidv": {"bin": "970418", "name": "BIDV", "short_name": "BIDV"},
    "vietinbank": {"bin": "970415", "name": "VIETINBANK", "short_name": "VietinBank"},
    "vtb": {"bin": "970415", "name": "VIETINBANK", "short_name": "VietinBank"},
    "tpbank": {"bin": "970423", "name": "TPBANK", "short_name": "TPBank"},
    "vpbank": {"bin": "970432", "name": "VPBANK", "short_name": "VPBank"},
    "agribank": {"bin": "970405", "name": "AGRIBANK", "short_name": "Agribank"},
    "sacombank": {"bin": "970403", "name": "SACOMBANK", "short_name": "Sacombank"},
    "scb": {"bin": "970429", "name": "SCB", "short_name": "SCB"},
    "hdbank": {"bin": "970437", "name": "HDBANK", "short_name": "HDBank"},
}

WEATHER_CODES = {
    1000: "Quang đãng",
    1100: "Có mây nhẹ",
    1101: "Có mây",
    1102: "Nhiều mây",
    1001: "Âm u",
    2000: "Sương mù",
    2100: "Sương mù nhẹ",
    4000: "Mưa nhỏ",
    4001: "Mưa",
    4200: "Mưa nhẹ",
    4201: "Mưa vừa",
    4202: "Mưa to",
    5000: "Tuyết",
    5001: "Tuyết rơi nhẹ",
    5100: "Mưa tuyết nhẹ",
    6000: "Mưa đá",
    6200: "Mưa đá nhẹ",
    6201: "Mưa đá nặng",
    7000: "Sấm sét",
    7101: "Sấm sét mạnh",
    7102: "Giông bão",
    8000: "Một vài cơn mưa rào"
}

SCRIPT_SMS_DIRECT = ["vip_0. py"]
SCRIPT_CALL_DIRECT = ["vip1_min.py"]
SCRIPT_SPAM_DIRECT = ["spam_0.py"]
SCRIPT_VIP_DIRECT = ["sms_1.py"]
SCRIPT_FREE = ["spam_0.py"]

SCRIPT_CACHE = {}
SCRIPT_CACHE_TIME = {}

FULL_STATUS = {}
FULL_LOCK = threading.Lock()

LOCKED_COMMANDS = {"call"}

COOLDOWN_COMMAND = {
    'xu_ly_ddos': {'admin': 60, 'vip': 180, 'member': 1800},
    'xu_ly_vip': {'admin': 90, 'vip': 180, 'member': 900},
    'xu_ly_spam': {'admin': 60, 'vip': 180, 'member': 180},
    'xu_ly_sms': {'admin': 60, 'vip': 180, 'member': 450},
    'xu_ly_call': {'admin': 30, 'vip': 180, 'member': 1800},
    'xu_ly_full': {'admin': 3600, 'vip': 3600, 'member': 3600},
    'xu_ly_tiktok': {'admin': 180, 'vip': 300, 'member': 900},
    'xu_ly_ngl': {'admin': 180, 'vip': 300, 'member': 900},
    'xu_ly_free': {'admin': 600, 'vip': 200, 'member': 300},
}

class TTLCache:
    def __init__(self, ttl_sec=600, max_size=256):
        self.ttl = ttl_sec
        self. max = max_size
        self.data = {}
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            v = self.data.get(key)
            if not v:
                return None
            val, exp = v
            if exp < time.time():
                self.data.pop(key, None)
                return None
            return val

    def set(self, key, val):
        with self.lock:
            if len(self. data) >= self.max:
                self.data.pop(next(iter(self.data. keys())), None)
            self.data[key] = (val, time.time() + self.ttl)

class PermissionCache:
    def __init__(self):
        self.cache = {}
        self.max_size = 500

    def get_permission(self, user_id):
        if user_id in self.cache:
            entry = self.cache[user_id]
            if time.time() - entry['timestamp'] < 3600:
                return entry['permission']
            else:
                del self.cache[user_id]
        return None

    def set_permission(self, user_id, permission):
        if len(self.cache) >= self. max_size:
            now = time.time()
            old_keys = [k for k, v in self. cache.items() if now - v['timestamp'] > 1800]
            for key in old_keys[:100]:
                self.cache.pop(key, None)
        self.cache[user_id] = {'permission': permission, 'timestamp': time.time()}

class CooldownManager:
    def __init__(self):
        self.cache = {}
        self._lock = threading.RLock()

    def check_cooldown(self, user_id, command):
        key = f"{command}:{user_id}"
        current_time = time.time()
        if key not in self.cache:
            return False, 0, None
        with self._lock:
            last_use = self.cache[key]
            permission = get_user_permission(user_id)
            cooldown_time = COOLDOWN_COMMAND.get(command, {}).get(permission, 60)
            if current_time - last_use < cooldown_time:
                remaining_time = cooldown_time - (current_time - last_use)
                return True, max(0, remaining_time), "command_specific"
        return False, 0, None

    def set_cooldown(self, user_id, command):
        key = f"{command}:{user_id}"
        with self._lock:
            self.cache[key] = time. time()

CACHE_SEARCH = TTLCache(ttl_sec=300, max_size=256)
CACHE_TRACK = TTLCache(ttl_sec=900, max_size=512)
CACHE_RESOLVE = TTLCache(ttl_sec=900, max_size=1024)

permission_cache = PermissionCache()
cooldown_manager = CooldownManager()

executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=os.cpu_count() * 5 if os.cpu_count() else 30
)

if AIOGRAM_AVAILABLE:
    try:
        bot_aiogram = Bot(
            token=TELEGRAM_BOT_TOKEN,
            default=DefaultBotProperties(
                parse_mode=ParseMode.HTML,
                link_preview_is_disabled=True
            )
        )
    except Exception as e:
        logger.error(f"Error initializing aiogram bot: {e}")
        bot_aiogram = None
else:
    bot_aiogram = None

if TELEBOT_AVAILABLE:
    try:
        bot_telebot = TeleBot(TELEGRAM_BOT_TOKEN, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error initializing telebot: {e}")
        bot_telebot = None
else:
    bot_telebot = None

if GEMINI_AVAILABLE:
    try:
        genai. configure(api_key=GEMINI_API_KEY)
    except Exception as e:
        logger.error(f"Error configuring Gemini: {e}")

PHONE_CACHE = {}
PHONE_CACHE_LOCK = threading.Lock()

def create_db_connection():
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=8.0)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        return conn

def blocking_db_execute(sql: str, params: tuple = ()) -> Optional[List[Any]]:
    conn = None
    try:
        conn = create_db_connection()
        c = conn.cursor()
        c.execute(sql, params)
        conn.commit()
        result = c.fetchall()
        return result
    except sqlite3.Error as e:
        logger.error(f"DB Execute Error: {e} - SQL: {sql}", exc_info=True)
        return None
    except Exception as e:
        logger. error(f"DB Execute Error (other): {e} - SQL: {sql}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()

def blocking_db_fetchone(sql: str, params: tuple = ()) -> Optional[tuple]:
    conn = None
    try:
        conn = create_db_connection()
        c = conn.cursor()
        c. execute(sql, params)
        result = c.fetchone()
        return result
    except sqlite3. Error as e:
        logger. error(f"DB Fetchone Error: {e} - SQL: {sql}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"DB Fetchone Error (other): {e} - SQL: {sql}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()

async def async_db_execute(sql: str, params: tuple = ()) -> Optional[List[Any]]:
    return await asyncio.to_thread(blocking_db_execute, sql, params)

async def async_db_fetchone(sql: str, params: tuple = ()) -> Optional[tuple]:
    return await asyncio.to_thread(blocking_db_fetchone, sql, params)

async def setup_database():
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            balance INTEGER DEFAULT 0,
            is_admin BOOLEAN DEFAULT FALSE,
            is_approved BOOLEAN DEFAULT FALSE
        )
    """)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            task_id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT,
            reward INTEGER
        )
    """)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS nap_request (
            request_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount INTEGER,
            status TEXT DEFAULT 'pending',
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS groups (
            chat_id INTEGER PRIMARY KEY
        )
    """)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS admin (
            user_id TEXT PRIMARY KEY,
            name TEXT,
            role TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS vip_lists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            list_name TEXT NOT NULL,
            phone_numbers TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(user_id, list_name)
        )
    """)

    for admin_id in ADMIN_IDS:
        await async_db_execute(
            """INSERT INTO users (user_id, balance, is_admin, is_approved) VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET is_admin=excluded.is_admin, is_approved=excluded.is_approved""",
            (admin_id, 99999999, True, True)
        )
        await async_db_execute(
            """INSERT INTO admin (user_id, name, role) VALUES (?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET role=excluded.role""",
            (str(admin_id), OWNER_USERNAME, 'admin')
        )

    logger.info("✅ Database setup completed successfully")

def cleanup_old_cache():
    current_time = time.time()
    keys_to_remove = []
    for key, timestamp in SCRIPT_CACHE_TIME.items():
        if current_time - timestamp > 600:
            keys_to_remove.append(key)
    for key in keys_to_remove:
        SCRIPT_CACHE.pop(key, None)
        SCRIPT_CACHE_TIME. pop(key, None)

def get_available_scripts(script_list, cache_key):
    current_time = time.time()
    if len(SCRIPT_CACHE) > 20:
        cleanup_old_cache()
    if (cache_key in SCRIPT_CACHE and
        cache_key in SCRIPT_CACHE_TIME and
        current_time - SCRIPT_CACHE_TIME[cache_key] < 600):
        return SCRIPT_CACHE[cache_key]
    available = [s for s in script_list if os.path.exists(s)]
    SCRIPT_CACHE[cache_key] = available
    SCRIPT_CACHE_TIME[cache_key] = current_time
    return available

def set_full_status(user_id, phone_number):
    with FULL_LOCK:
        key = f"{user_id}:{phone_number}"
        FULL_STATUS[key] = time.time() + 24 * 3600

def remove_full_status(user_id, phone_number):
    with FULL_LOCK:
        key = f"{user_id}:{phone_number}"
        FULL_STATUS.pop(key, None)

def check_full_status(user_id, phone_number):
    with FULL_LOCK:
        key = f"{user_id}:{phone_number}"
        if key in FULL_STATUS and FULL_STATUS[key] > time.time():
            return True
        FULL_STATUS.pop(key, None)
        return False

def run_background_process_sync(command, timeout=None, user_id=None):
    try:
        if not command or not isinstance(command, str):
            return False, None, None
        command = command.strip()
        if len(command) > 1000:
            return False, None, None
        full_command = f"setsid {command} > /dev/null 2>&1 & echo $!"
        result = subprocess.run(
            full_command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=15
        )
        if result.returncode == 0 and result.stdout. strip():
            pid = int(result.stdout.strip())
            time.sleep(0.5)
            try:
                proc = psutil.Process(pid)
                if proc.is_running():
                    logger.info(f"Created process PID {pid} for user {user_id}: {command[:50]}...")
                    try:
                        os.setpgid(pid, pid)
                    except (OSError, ProcessLookupError):
                        pass
                    return True, pid, None
            except psutil.NoSuchProcess:
                logger.warning(f"Process {pid} exited immediately after creation")
        return False, None, None
    except Exception as e:
        logger.error(f"Error run_background_process_sync: {e}")
        return False, None, None

def count_processes_sync(user_id=None):
    try:
        count = 0
        for proc in psutil.process_iter(['cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if 'python' in cmdline and any(script in cmdline for script in ['spam_', 'sms_', 'vip_', 'call']):
                    if user_id is None or str(user_id) in cmdline:
                        count += 1
            except:
                continue
        return count
    except:
        return 0

def kill_processes_sync(pattern):
    killed_count = 0
    try:
        processes_to_kill = []
        process_families = {}
        for proc in psutil. process_iter(['pid', 'ppid', 'cmdline', 'name', 'status', 'create_time']):
            try:
                proc_info = proc.info
                if not proc_info['cmdline']:
                    continue
                cmdline = ' '.join(proc_info['cmdline'])
                proc_name = proc_info. get('name', '')
                proc_status = proc_info.get('status', '')

                if proc_status == psutil.STATUS_ZOMBIE:
                    processes_to_kill.append(proc)
                    continue

                is_target_process = (
                    ('python' in proc_name. lower() or 'python' in cmdline.lower()) and
                    any(script in cmdline for script in [
                        'spam_', 'sms_', 'vip_', 'call', 'lenh', 'tcp. py', 'tt.py',
                        'ngl.py', 'pro24h.py', 'vip11122.py', 'mlm.py', 'vip1_min.py',
                        'master222.py'
                    ])
                )

                if proc_info. get('create_time'):
                    process_age = time.time() - proc_info['create_time']
                    if process_age > 21600 and is_target_process:
                        logger.warning(f"Detected old process {proc_info['pid']}: {process_age/3600:.1f}h - {cmdline[:100]}")

                if not is_target_process:
                    continue

                should_kill = False
                if pattern == "python.*lenh":
                    should_kill = True
                elif "lenh.*" in pattern:
                    parts = pattern.split('.*')
                    if len(parts) >= 3:
                        user_id = parts[-1]
                        if user_id and user_id in cmdline:
                            should_kill = True
                else:
                    pattern_clean = pattern.replace('.*', '').replace('python3', 'python')
                    if pattern_clean in cmdline:
                        should_kill = True

                if should_kill:
                    processes_to_kill.append(proc)
                    try:
                        children = proc.children(recursive=True)
                        process_families[proc. pid] = children
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        for proc in processes_to_kill:
            try:
                if proc.status() == psutil.STATUS_ZOMBIE:
                    try:
                        parent = proc.parent()
                        if parent and parent.pid != 1:
                            parent.terminate()
                            parent.wait(timeout=2)
                    except:
                        pass
                    killed_count += 1
                    continue

                children = process_families.get(proc.pid, [])
                for child in children:
                    try:
                        if child.is_running():
                            child. terminate()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                time.sleep(0.5)
                for child in children:
                    try:
                        if child.is_running():
                            child.kill()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                proc.terminate()
                try:
                    proc.wait(timeout=8)
                    killed_count += 1
                except psutil.TimeoutExpired:
                    proc.kill()
                    try:
                        proc.wait(timeout=5)
                        killed_count += 1
                    except:
                        try:
                            os.kill(proc.pid, 9)
                            killed_count += 1
                        except:
                            pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                killed_count += 1
                continue

        if killed_count == 0:
            try:
                commands = []
                if 'lenh.*' in pattern and len(pattern.split('.*')) > 2:
                    user_id = pattern.split('.*')[-1]
                    commands = [
                        f"pkill -15 -f 'python.*{user_id}'",
                        f"pkill -9 -f 'python.*{user_id}'",
                        "pkill -9 -f 'spam_|sms_|vip_|call|tcp.py|tt.py|ngl.py|pro24h. py'"
                    ]
                else:
                    commands = [
                        "pkill -15 -f 'python.*lenh'",
                        "pkill -9 -f 'python.*lenh'",
                        "pkill -9 -f 'spam_|sms_|vip_|call|tcp.py|tt.py|ngl.py|pro24h.py'",
                        "pkill -9 -f 'python3.*vip'",
                        "pkill -9 -f 'python.*pro24h'"
                    ]

                for cmd in commands:
                    try:
                        result = subprocess.run(cmd, shell=True, timeout=5, capture_output=True)
                        if result.returncode == 0:
                            killed_count += 1
                        time.sleep(0.2)
                    except:
                        continue
            except Exception:
                pass

        try:
            subprocess.run("ps aux | grep '<defunct>' | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true",
                         shell=True, timeout=8, capture_output=True)
            subprocess.run("ps -eo pid,etime,cmd | grep python | awk '$2 ~ /^[0-9]+-/ || $2 ~ /^[0-6][0-9]:[0-5][0-9]:[0-5][0-9]/ {print $1}' | head -20 | xargs -r kill -9 2>/dev/null || true",
                         shell=True, timeout=10, capture_output=True)
            subprocess.run("find /tmp -name '*.py*' -mmin +60 -delete 2>/dev/null || true",
                         shell=True, timeout=10, capture_output=True)
            subprocess.run("find .  -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true",
                         shell=True, timeout=10, capture_output=True)
            subprocess.run("sync", shell=True, timeout=3, capture_output=True)
        except Exception as e:
            logger.error(f"Error enhanced cleanup: {e}")

    except Exception as e:
        logger.error(f"Error kill_processes_sync: {e}")
        return False

    logger.info(f"Cleaned up {killed_count} processes with pattern: {pattern}")
    return killed_count > 0

async def get_user(user_id: int, username: Optional[str] = None) -> Optional[Dict[str, Any]]:
    user_data = await async_db_fetchone(
        "SELECT user_id, username, balance, is_admin, is_approved FROM users WHERE user_id = ?",
        (user_id,)
    )
    if user_data is None:
        username = username if username else f"user_{user_id}"
        await async_db_execute(
            "INSERT INTO users (user_id, username, balance, is_approved) VALUES (?, ?, ?, ?)",
            (user_id, username, 0, False)
        )
        logger.info(f"Created new user: {user_id} - @{username}")
        return {"user_id": user_id, "username": username, "balance": 0, "is_admin": False, "is_approved": False}
    elif user_data:
        return {
            "user_id": user_data[0],
            "username": user_data[1],
            "balance": user_data[2],
            "is_admin": bool(user_data[3]),
            "is_approved": bool(user_data[4])
        }
    else:
        return None

async def update_balance(user_id: int, amount: int):
    user_exists = await async_db_fetchone("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
    if user_exists:
        await async_db_execute("UPDATE users SET balance = balance + ? WHERE user_id = ? ", (amount, user_id))
        logger.info(f"Updated balance for {user_id} by {amount}")
    else:
        logger.warning(f"Attempted to update balance for non-existent user: {user_id}")

async def get_all_group_ids() -> List[int]:
    groups_data = await async_db_execute("SELECT chat_id FROM groups")
    if groups_data is None:
        return []
    return [row[0] for row in groups_data]

def get_user_mention(user) -> str:
    if hasattr(user, 'username') and user.username:
        return f"@{user.username}"
    if hasattr(user, 'first_name'):
        safe_name = escape_markdown_v2(user.first_name)
        return f"[{safe_name}](tg://user?id={user. id})"
    return f"User_{user.id}"

def get_vietnam_time():
    try:
        tz = pytz.timezone("Asia/Ho_Chi_Minh")
        now = datetime.now(tz)
        return now.strftime("%H:%M:%S"), now.strftime("%d/%m/%Y")
    except Exception as e:
        logger.error(f"Error getting Vietnam time: {e}")
        now = datetime.now()
        return now.strftime("%H:%M:%S"), now.strftime("%d/%m/%Y")

def escape_markdown_v2(text):
    if text is None:
        return ""
    escape_chars = r'([_*\[\]()~`>#+-=|{}.!])'
    text = str(text). replace('\\', '\\\\')
    return re.sub(escape_chars, r'\\\1', text)

def escape_html(text):
    if text is None:
        return ""
    return html.escape(str(text))

def format_cooldown_time(seconds):
    if seconds <= 0:
        return "0 giây"
    if seconds < 60:
        return f"{int(seconds)} giây"
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)
    if remaining_seconds == 0:
        return f"{minutes} phút"
    else:
        return f"{minutes} phút {remaining_seconds} giây"

def format_user_link(user):
    try:
        if not user:
            return "Unknown User"
        user_id = user.id if hasattr(user, 'id') else None
        full_name = user.full_name if hasattr(user, 'full_name') else (user.first_name if hasattr(user, 'first_name') else None)
        if not user_id:
            return escape_html(full_name or "Unknown User")
        if full_name:
            return f'<a href="tg://user? id={user_id}">{escape_html(full_name)}</a>'
        else:
            return f'<a href="tg://user?id={user_id}">ID: {user_id}</a>'
    except Exception as e:
        logger.error(f"Error formatting user link: {e}")
        return "Unknown User"

def get_permission_title(user_id):
    level = get_user_permission(user_id)
    titles = {
        'admin': "╭━━⊰⿗𓆰☯︎ 🎩 𝓐𝓭𝓶𝓲𝓷  ☯︎𓆪⿘━━╮",
        'vip': "╭━━₊༺𓆰🧞‍♂️🅥🅘🅟🧜🏻‍♀️𓆪༻₊━━╮",
        'member': "╭━━━━༉Members༉━━━━╮"
    }
    return titles. get(level, titles['member'])

def get_user_permission(user_id):
    user_id = str(user_id)
    if user_id == str(ADMIN_IDS[0]):
        return 'admin'

    cached_permission = permission_cache.get_permission(user_id)
    if cached_permission is not None:
        return cached_permission

    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT role FROM admin WHERE user_id = ?  LIMIT 1", (user_id,))
        admin_result = cursor.fetchone()
        conn.close()

        if admin_result:
            permission = admin_result['role']
        else:
            permission = 'member'

        permission_cache.set_permission(user_id, permission)
        return permission
    except Exception as e:
        logger.error(f"Error getting user permission {user_id}: {e}")
        permission_cache.set_permission(user_id, 'member')
        return 'member'

def is_admin(user_id):
    return get_user_permission(user_id) == 'admin'

def is_vip_permanent(user_id):
    level = get_user_permission(user_id)
    return level in ('admin', 'vip')

def is_valid_phone(phone):
    if not phone:
        return False

    with PHONE_CACHE_LOCK:
        if phone in PHONE_CACHE:
            return PHONE_CACHE[phone]

    try:
        if not phone.isdigit() or len(phone) not in [10, 11]:
            with PHONE_CACHE_LOCK:
                PHONE_CACHE[phone] = False
            return False

        number = phonenumbers.parse(phone, "VN")
        valid = phonenumbers.is_valid_number(number)

        with PHONE_CACHE_LOCK:
            PHONE_CACHE[phone] = valid

        return valid
    except Exception:
        with PHONE_CACHE_LOCK:
            PHONE_CACHE[phone] = False
        return False

def validate_phone_with_carrier(phone):
    try:
        if not phone or not isinstance(phone, str):
            return False, "Số điện thoại không hợp lệ"

        clean_phone = ''.join(filter(str.isdigit, phone))

        if not is_valid_phone(clean_phone):
            return False, "Số điện thoại không hợp lệ"

        parsed_number = phonenumbers.parse(clean_phone, "VN")

        if not phonenumbers.is_valid_number(parsed_number):
            return False, "Số điện thoại không hợp lệ"

        try:
            carrier_name = carrier. name_for_number(parsed_number, "vi")
        except ImportError:
            carrier_name = get_carrier(clean_phone)

        if not carrier_name or carrier_name == "Không rõ":
            carrier_name = get_carrier(clean_phone)

        return True, carrier_name
    except phonenumbers.NumberParseException:
        return False, "Số không hợp lệ"
    except Exception:
        return False, "Số không hợp lệ"

def get_carrier(phone):
    if not phone:
        return "Không xác định"

    phone = str(phone). strip()

    if phone.startswith("+84"):
        phone = "0" + phone[3:]
    elif phone.startswith("84"):
        phone = "0" + phone[2:]

    if len(phone) < 3:
        return "Không xác định"

    prefix = phone[:3]

    viettel = {"086", "096", "097", "098", "032", "033", "034", "035", "036", "037", "038", "039"}
    mobifone = {"089", "090", "093", "070", "079", "077", "076", "078"}
    vinaphone = {"088", "091", "094", "083", "084", "085", "081", "082"}
    vietnamobile = {"092", "056", "058"}
    gmobile = {"099", "059"}

    if prefix in viettel:
        return "Viettel"
    elif prefix in mobifone:
        return "Mobifone"
    elif prefix in vinaphone:
        return "Vinaphone"
    elif prefix in vietnamobile:
        return "Vietnamobile"
    elif prefix in gmobile:
        return "Gmobile"

    return "Không xác định"

def get_phone_limit(user_id):
    level = get_user_permission(user_id)
    limits = {'admin': 50, 'vip': 50, 'member': 2}
    return limits.get(level, 2)

def log_command(user_id: int, command: str, target: str):
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] User: {user_id} | Command: {command} | Target: {target}\n"
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_entry)
    except IOError as e:
        logger. warning(f"Cannot write log (IOError): {e}")
    except Exception as e:
        logger.warning(f"Cannot write log (other): {e}")

def predict_md5_logic(md5_hash: str) -> Dict[str, Any]:
    try:
        md5_hash = md5_hash.strip(). lower()
        if not re.fullmatch(r"^[0-9a-f]{32}$", md5_hash):
            return {"ok": False, "error": "Invalid MD5 format"}

        seed = int(md5_hash[:8], 16)
        next_seed = (seed * LCG_MULTIPLIER + LCG_INCREMENT) % LCG_MODULUS
        predicted_md5 = hashlib.md5(str(next_seed).encode()).hexdigest()
        result_hex = predicted_md5[-8:]
        value = int(result_hex, 16)
        dice = [((value >> (i * 4)) % 6) + 1 for i in range(3)]
        total = sum(dice)
        result = "TÀI" if total > 10 else "XỈU"

        return {
            "ok": True,
            "predicted_md5": predicted_md5,
            "dice": dice,
            "total": total,
            "result": result,
            "seed_next": next_seed
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

def generate_qr_code_sync(text: str):
    if not TTS_QR_AVAILABLE or not qrcode:
        return "⚠️ Missing qrcode library"
    try:
        qr_img = qrcode.make(text)
        buffer = io.BytesIO()
        qr_img.save(buffer, format="PNG")
        buffer.seek(0)
        return buffer
    except Exception as e:
        return f"Error creating QR: {e}"

def text_to_speech_sync(text: str):
    if not TTS_QR_AVAILABLE or not gTTS:
        return "⚠️ Missing gTTS library"
    try:
        tts = gTTS(text=text[:250], lang='vi')
        buffer = io.BytesIO()
        tts.write_to_fp(buffer)
        buffer. seek(0)
        return buffer
    except Exception as e:
        return f"Error creating Voice: {e}"

def get_api_result_sync(url: str, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        response. raise_for_status()
        content_type = response.headers.get('Content-Type', '').lower()

        if 'application/json' in content_type:
            return response.json()
        elif 'text/' in content_type:
            return {"status": True, "_content": response.text}
        else:
            logger.warning(f"API {url} returned undefined Content-Type: {content_type}")
            return {"status": True, "_content": response.text}
    except requests.exceptions.JSONDecodeError:
        return {
            "status": False,
            "message": f"API returned non-JSON.  (Code: {response.status_code if 'response' in locals() else 'N/A'})"
        }
    except requests.exceptions.RequestException as e:
        return {"status": False, "message": f"API connection error: {e}"}
    except Exception as e:
        return {"status": False, "message": str(e)}

def create_group_link_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🥷🏿   ⋰ 𓊈 𝐴𝑑𝑚𝑖𝑛 24/7 𓊉 ⋱   🛰️",
                url=f"https://t.me/{OWNER_USERNAME}"
            )
        ]
    ])
    return keyboard

def read_js_file(filename):
    try:
        if not os.path.exists(filename):
            return []

        with open(filename, 'r', encoding='utf-8') as file:
            content = file.read()

        pattern = r'\[([^\]]+)\]'
        match = re.search(pattern, content, re.DOTALL)

        if match:
            array_content = match.group(1)
            urls = []
            for line in array_content.split('\n'):
                line = line.strip()
                if line. startswith('"') and line.endswith('",'):
                    url = line[1:-2]
                    urls.append(url)
                elif line.startswith('"') and line.endswith('"'):
                    url = line[1:-1]
                    urls.append(url)
            return urls

        return []
    except Exception as e:
        logger.error(f"Error reading JS file {filename}: {e}")
        return []
        
async def cleanup_full_status_safe():
    if 'FULL_STATUS' not in globals() or 'FULL_LOCK' not in globals():
        return

    try:
        current_time = time.time()
        keys_to_remove = []
        with FULL_LOCK:
            keys_to_remove = [k for k, v in FULL_STATUS.items() 
                             if v < current_time - 3600]
        if keys_to_remove:
            batch_size = 50
            removed_total = 0

            for i in range(0, len(keys_to_remove), batch_size):
                batch = keys_to_remove[i:i + batch_size]
                with FULL_LOCK:
                    for key in batch:
                        FULL_STATUS.pop(key, None)
                        removed_total += 1

                if i + batch_size < len(keys_to_remove):
                    await asyncio.sleep(0.01)

            logger.info(f"🧹 Removed {removed_total} old entries from FULL_STATUS")

    except Exception as e:
        logger.error(f"Error cleanup FULL_STATUS: {e}")

def extract_params(message):
    text = getattr(message, "text", None)
    if not text:
        return []
    parts = text.split()
    if len(parts) < 2:
        return []
    return parts[1:]

async def check_command_locked(message, command: str) -> bool:
    if command in LOCKED_COMMANDS:
        await send_response(
            message,
            "🔒 Hệ thống đang được nâng cấp để mang đến trải nghiệm tốt hơn.\n"
            "Vui lòng sử dụng lệnh /free !\n\n"
            "Cảm ơn bạn đã kiên nhẫn chờ đợi! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return True
    return False

async def send_response(
    message: Message,
    title: str,
    content: str = "",
    processing_msg: Optional[types.Message] = None,
    delete_user_msg: bool = False,
    auto_delete_after: int = 0,
    keep_forever: bool = False,
    with_keyboard: bool = False
):
    try:
        current_time = get_vietnam_time()
        time_str, date_str = current_time

        safe_title = escape_markdown_v2(title. upper() if isinstance(title, str) else str(title))
        
        text_limit = 1000 - len(title) - len(time_str) - 100
        safe_text = escape_markdown_v2(
            (content if isinstance(content, str) else str(content))[:text_limit] + 
            ('...' if len(str(content)) > text_limit else '')
        )
        safe_time = escape_markdown_v2(time_str)
        safe_owner = escape_markdown_v2(f"@{OWNER_USERNAME}")

        formatted_caption = (
            f"┏ 💎 *{safe_title}* ┓\n"
            f"┣{chr(8213)*20}\n"
            f"┣ {safe_text}\n"
            f"┣{chr(8213)*20}\n"
            f"┗ ⏱️ *{safe_time}* \\| Bot by {safe_owner}"
        )

        video_sent_successfully = False
        if os.path.exists(LOCAL_VIDEO_PATH):
            try:
                if processing_msg:
                    try:
                        await bot_aiogram.delete_message(processing_msg.chat.id, processing_msg.message_id)
                    except Exception:
                        pass

                with open(LOCAL_VIDEO_PATH, 'rb') as video_file:
                    keyboard = create_group_link_keyboard() if with_keyboard else None
                    await bot_aiogram.send_video(
                        chat_id=message.chat.id,
                        video=video_file,
                        caption=formatted_caption,
                        reply_to_message_id=message.message_id,
                        parse_mode="MarkdownV2",
                        reply_markup=keyboard
                    )
                video_sent_successfully = True
            except Exception as e:
                logger.error(f"Error sending video '{LOCAL_VIDEO_PATH}': {e}", exc_info=True)
        else:
            logger.warning(f"Video file '{LOCAL_VIDEO_PATH}' not found")
            if processing_msg:
                try:
                    await bot_aiogram.delete_message(processing_msg.chat.id, processing_msg.message_id)
                except Exception:
                    pass

        if not video_sent_successfully:
            logger.info("Video send failed or file not found, using text fallback")
            error_prefix = f"⚠️ *Lỗi Video* \\(File `{escape_markdown_v2(LOCAL_VIDEO_PATH)}` lỗi hoặc không tồn tại\\)\n\n"
            fallback_text = error_prefix + formatted_caption
            try:
                keyboard = create_group_link_keyboard() if with_keyboard else None
                await bot_aiogram.send_message(
                    chat_id=message.chat.id,
                    text=fallback_text,
                    parse_mode="MarkdownV2",
                    reply_markup=keyboard
                )
            except Exception as e_fallback:
                logger.error(f"Error sending text fallback: {e_fallback}", exc_info=True)
                try:
                    await bot_aiogram.send_message(
                        chat_id=message.chat.id,
                        text=f"{title}\n---\n{content[:4000]}\n---\n{time_str}"
                    )
                except Exception as e_final:
                    logger.critical(f"Error sending final fallback: {e_final}", exc_info=True)

        if delete_user_msg:
            try:
                await bot_aiogram. delete_message(chat_id=message.chat.id, message_id=message.message_id)
            except Exception:
                pass

    except Exception as e:
        logger.error(f"Error in send_response: {e}", exc_info=True)

async def auto_delete_message(chat_id: int, message_id: int, delay: int = 10):
    try:
        await asyncio.sleep(delay)
        await bot_aiogram.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.error(f"Error auto-deleting message ({chat_id}, {message_id}): {e}")

def user_cooldown(default_seconds: int = 60):
    def decorator(func):
        @wraps(func)
        async def wrapper(message: Message, *args, **kwargs):
            if not message.from_user:
                return False
            user_id = message.from_user.id
            func_name = func.__name__

            level = get_user_permission(user_id)

            required_perm = getattr(func, '_required_permission', None)
            if required_perm:
                if required_perm == 'admin' and level != 'admin':
                    await send_response(message, "TRUY CẬP BỊ TỪ CHỐI", "Không đủ quyền!", delete_user_msg=True, auto_delete_after=3)
                    return False
                elif required_perm == 'vip_permanent' and level not in ('admin', 'vip'):
                    await send_response(message, "TRUY CẬP BỊ TỪ CHỐI", "Không đủ quyền!", delete_user_msg=True, auto_delete_after=3)
                    return False

            if level != 'admin':
                on_cooldown, remaining, _ = cooldown_manager.check_cooldown(user_id, func_name)
                if on_cooldown:
                    formatted_time = format_cooldown_time(remaining)
                    await send_response(
                        message,
                        "COOLDOWN",
                        f"🏓 Bạn cần chờ {formatted_time} nữa để sử dụng lệnh này! ",
                        delete_user_msg=True,
                        auto_delete_after=5
                    )
                    return False

            result = await func(message, *args, **kwargs)

            if result is True and level != 'admin':
                cooldown_manager.set_cooldown(user_id, func_name)

            return result
        return wrapper
    return decorator

def group_only(func):
    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if not message.from_user:
            return False
        user_id = message.from_user.id
        if is_admin(user_id):
            return await func(message, *args, **kwargs)
        if message.chat.id != GROUP_ID:
            return False
        return await func(message, *args, **kwargs)
    return wrapper

def admin_only(func):
    func._required_permission = 'admin'
    return func

def vip_only(func):
    func._required_permission = 'vip_permanent'
    return func

async def handle_sms(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if not params:
        phone_limit = get_phone_limit(user_id)
        await send_response(
            message,
            "SMS HELP",
            f"👼🏻 /sms 0987654321 0987654322... Tối đa {phone_limit} số theo quyền hạn của bạn! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    phone_limit = get_phone_limit(user_id)
    if len(params) > phone_limit:
        await send_response(
            message,
            "SMS LIMIT",
            f"👼🏻 Lệnh /sms chỉ cho phép nhập tối đa {phone_limit} số! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    valid_phones = []
    for phone in params[:phone_limit]:
        phone = phone.strip()
        if is_valid_phone(phone) and not check_full_status(user_id, phone) and phone not in valid_phones:
            valid_phones.append(phone)

    if not valid_phones:
        await send_response(
            message,
            "SMS ERROR",
            "👼🏻 Các số điện thoại không hợp lệ hoặc đang chạy full 24h!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    available_scripts = get_available_scripts(SCRIPT_VIP_DIRECT, 'sms')
    if not available_scripts:
        await send_response(
            message,
            "SMS ERROR",
            "👼🏻 Không có script SMS khả dụng!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    success_pids = []
    for phone in valid_phones:
        script = random.choice(available_scripts)
        command = f"proxychains4 python3 {script} {phone} 50"
        success, pid, _ = run_background_process_sync(command, timeout=TIMEOUT_MEDIUM, user_id=user_id)
        if success and pid:
            success_pids.append(pid)

    if not success_pids:
        await send_response(
            message,
            "SMS ERROR",
            "👼🏻 Không thể khởi tạo tiến trình! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    log_command(user_id, "/sms", f"{len(valid_phones)} numbers")

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━━〖⨧✧✧⩩〗\n"
        f" • 𝑁ℎ𝑎̣̂𝑝 𝑇𝑎𝑦          :      {len(valid_phones)} Số Hợp lệ\n"
        f" • 𝑇𝑎̂́𝑛 𝐶𝑜̂𝑛𝑔           :       60 phút\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́                  :      V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛           :       {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦                :       {date_str}\n"
        f"╰━━━━━〖⨧✧𝐒𝐌𝐒✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m. jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "SMS", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
async def handle_spam(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "SPAM HELP",
            "👼🏻 Cú pháp: /spam 0987654321",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    phone = params[0].strip()

    valid, carrier_name = validate_phone_with_carrier(phone)
    if not valid:
        await send_response(
            message,
            "SPAM ERROR",
            f"👼🏻 {carrier_name}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    if check_full_status(user_id, phone):
        await send_response(
            message,
            "SPAM ERROR",
            f"👼🏻 Số {phone} đang chạy full 24h!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    available_scripts = get_available_scripts(SCRIPT_SPAM_DIRECT, 'spam')
    if not available_scripts:
        await send_response(
            message,
            "SPAM ERROR",
            "👼🏻 Không có script Spam khả dụng!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    script = random.choice(available_scripts)

    command = f"timeout 180s python3 {script} {phone} 5"
    success, pid, _ = run_background_process_sync(command, timeout=TIMEOUT_SHORT, user_id=user_id)

    if not success:
        await send_response(
            message,
            "SPAM ERROR",
            "👼🏻 Lỗi khi khởi động tiến trình! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    log_command(user_id, "/spam", phone)

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━〖⨧✧✧⩩〗\n"
        f" • 𝑃ℎ𝑜𝑛𝑒 𝑉𝑁        :      {phone}\n"
        f" • 𝑇𝑎̂́𝑛 𝐶𝑜̂𝑛𝑔        :      1 Giờ liên tục\n"
        f" • 𝑁ℎ𝑎̀ 𝑚𝑎̣𝑛𝑔       :      {carrier_name}\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́                :      V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛         :      {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦              :      {date_str}\n"
        f"╰━━━━〖⨧✧𝐒𝐏𝐀𝐌✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat. id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "SPAM", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
async def handle_free(message: Message):
    if not message.from_user:
        return False
    user = message. from_user
    user_id = user.id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "FREE HELP",
            "👼🏻 Cú pháp: /free 0987654321",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    phone = params[0].strip()

    valid, carrier_name = validate_phone_with_carrier(phone)
    if not valid:
        await send_response(
            message,
            "FREE ERROR",
            f"👼🏻 {carrier_name}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    if check_full_status(user_id, phone):
        await send_response(
            message,
            "FREE ERROR",
            f"👼🏻 Số {phone} đang chạy full 24h!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    script = random.choice(SCRIPT_FREE)

    command = f"timeout 180s python3 {script} {phone} 1"
    success, pid, _ = run_background_process_sync(command, timeout=TIMEOUT_SHORT, user_id=user_id)

    if not success:
        await send_response(
            message,
            "FREE ERROR",
            "👼🏻 Lỗi khi khởi động tiến trình!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    log_command(user_id, "/free", phone)

    user_link = format_user_link(user)

    content = (
        f"👼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟   :     {user_link}\n"
        f"🎫 𝑀𝑎̃ 𝐼𝐷      :     {user_id}\n"
        f"🚀 𝐿𝑒̣̂𝑛ℎ 𝑑𝑎̃ 𝑐ℎ𝑎̣𝑦 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔 ! 🎯\n"
        f"𝐴𝐸 𝑡𝑒𝑠𝑡 𝑡ℎ𝑢̛̉ 𝑠𝑜̂́ 𝑟𝑜̂̀𝑖 𝑐ℎ𝑜 𝑚𝑖̀𝑛ℎ 𝑥𝑖𝑛 𝑦́ 𝑘𝑖𝑒̂́𝑛 !"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "FREE", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
@vip_only
async def handle_full(message: Message):
    if await check_command_locked(message, "full"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if not params:
        await send_response(
            message,
            "FULL HELP",
            "👼🏻 Cú pháp: /full 0987654321 0987654322.. .\nChạy liên tục 24h - VIP tối đa 3 số mỗi lần ! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    if len(params) > 3:
        await send_response(
            message,
            "FULL LIMIT",
            "👼🏻 VIP chỉ được phép nhập tối đa 3 Số cho lệnh full! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    valid_phones = []
    for phone in params:
        phone = phone.strip()
        if is_valid_phone(phone) and not check_full_status(user_id, phone) and phone not in valid_phones:
            valid_phones.append(phone)

    if not valid_phones:
        await send_response(
            message,
            "FULL ERROR",
            "👼🏻 Không có số điện thoại hợp lệ! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    with FULL_LOCK:
        user_full_count = sum(1 for key in FULL_STATUS.keys() if key. startswith(f"{user_id}:"))
        if user_full_count + len(valid_phones) > 3:
            await send_response(
                message,
                "FULL ERROR",
                f"👼🏻 Bạn đã có {user_full_count} số đang Full.  VIP chỉ được tối đa 3 số! ",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

    success_pids = []
    success_phones = []

    for phone in valid_phones:
        set_full_status(user_id, phone)

        command = f"timeout 1200s python3 pro24h.py {phone}"
        success, pid, _ = run_background_process_sync(command, timeout=TIMEOUT_LONG, user_id=user_id)

        if success and pid:
            success_pids.append(pid)
            success_phones.append(phone)
        else:
            remove_full_status(user_id, phone)

    if not success_pids:
        await send_response(
            message,
            "FULL ERROR",
            "👼🏻 Không thể khởi tạo tiến trình full nào!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    log_command(user_id, "/full", f"{len(success_phones)} numbers")

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)
    phone_list = ", ".join(success_phones)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━━〖⨧✧✧⩩〗\n"
        f" • 𝑃ℎ𝑜𝑛𝑒 𝐵𝑙𝑜𝑐𝑘     :      {len(success_phones)} số Hợp lệ\n"
        f" • 𝐷𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ        :      {phone_list}\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛          :      24 Giờ liên tục\n"
        f" • 𝑇𝑟𝑎̣𝑛𝑔 𝑡ℎ𝑎́𝑖        :       Đang gửi OTP\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́                  :      V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛           :      {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦                :      {date_str}\n"
        f" • 📵 𝑈𝑛𝑙𝑜𝑐𝑘         :      /kill 𝐷𝑢̛̀𝑛𝑔 𝑠𝑜̂́\n"
        f"╰━━━〖⨧✧𝐅𝐮𝐥𝐥 𝟐𝟒/𝟕✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "FULL", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
@vip_only
async def handle_ddos(message: Message):
    if await check_command_locked(message, "ddos"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user. id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "DDOS HELP",
            "👼🏻 Cú pháp: /ddos [link web]",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    target_url = params[0].strip()
    if not any(target_url.startswith(proto) for proto in ['http://', 'https://']):
        target_url = 'http://' + target_url

    log_command(user_id, "/ddos", target_url[:50])

    success, pid, _ = run_background_process_sync(
        f"python3 tcp. py {target_url} 1000",
        timeout=TIMEOUT_MEDIUM
    )

    if not success:
        await send_response(
            message,
            "DDOS ERROR",
            "👼🏻 Lỗi khi khởi động lệnh ddos!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━━〖⨧✧✧⩩〗\n"
        f" • Target       :     {escape_html(target_url[:25])}...\n"
        f" • 𝑆𝑜̂́ vòng          :     Liên tục\n"
        f" • Power          :     High Performance\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́                :      V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛        :      {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦              :      {date_str}\n"
        f"╰━━━━〖⨧✧𝗗𝗗𝗢𝗦✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat. id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "DDOS", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
@vip_only
async def handle_tiktok(message: Message):
    if await check_command_locked(message, "tiktok"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "TIKTOK HELP",
            "👼🏻 Cú pháp: /tiktok [link video tiktok]",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    tiktok_link = params[0].strip()

    if not ("tiktok. com" in tiktok_link or "vm.tiktok.com" in tiktok_link):
        await send_response(
            message,
            "TIKTOK ERROR",
            "👼🏻 Link TikTok không hợp lệ!",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    log_command(user_id, "/tiktok", tiktok_link)

    success, pid, _ = run_background_process_sync(
        f"python3 tt.py {tiktok_link} 1000",
        timeout=TIMEOUT_LONG
    )

    if not success:
        await send_response(
            message,
            "TIKTOK ERROR",
            "👼🏻 Lỗi khi khởi động lệnh tiktok!",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━━〖⨧✧✧⩩〗\n"
        f" • Link          :     {escape_html(tiktok_link[:30])}...\n"
        f" • Target          :      1000+ views\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́        :     V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛      :      {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦             :      {date_str}\n"
        f"╰━━━━〖⨧✧𝐓𝐢𝐤𝐓𝐨𝐤✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat. id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "TIKTOK", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@user_cooldown()
@group_only
@vip_only
async def handle_ngl(message: Message):
    if await check_command_locked(message, "ngl"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user. id
    time_str, date_str = get_vietnam_time()

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "NGL HELP",
            "👼🏻 Cú pháp: /ngl [link ngl]",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    ngl_link = params[0]. strip()

    if not ("ngl.link" in ngl_link):
        await send_response(
            message,
            "NGL ERROR",
            "👼🏻 Link NGL không hợp lệ!",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    log_command(user_id, "/ngl", ngl_link)

    success, pid, _ = run_background_process_sync(
        f"python3 spamngl.py {ngl_link} 1000",
        timeout=TIMEOUT_LONG
    )

    if not success:
        await send_response(
            message,
            "NGL ERROR",
            "👼🏻 Lỗi khi khởi động lệnh NGL!",
            delete_user_msg=True,
            auto_delete_after=8,
            with_keyboard=True
        )
        return False

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"╭━━━━━〖⨧✧✧⩩〗\n"
        f" • Link         :     {escape_html(ngl_link[:30])}.. .\n"
        f" • Target           :     1000+ messages\n"
        f" • 𝑉𝑖̣ 𝑡𝑟𝑖́        :     V/N Online\n"
        f" • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛      :     {time_str}\n"
        f" • 𝑇𝑜𝑑𝑎𝑦             :     {date_str}\n"
        f"╰━━━━〖⨧✧𝐍𝐆𝐋✧⩩〗"
    )

    try:
        keyboard = create_group_link_keyboard()
        await bot_aiogram.send_photo(
            chat_id=message.chat. id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{content}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot_aiogram.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error sending image: {e}")
        await send_response(message, "NGL", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)

    return True

@group_only
async def handle_ping(message: Message):
    if not message.from_user:
        return False
    user = message. from_user
    user_id = user.id

    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)

    content = (
        f"{permission_title}\n"
        f"┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀ã 𝐼𝐷       :      {user_id}\n\n"
        f"🤖 𝑇𝑟𝑎̣𝑛𝑔 𝑡ℎ𝑎́𝑖 𝐵𝑜𝑡 : ℎ𝑜𝑎̣𝑡 𝑑𝑜̣̂𝑛𝑔 🛰️\n\n"
        f"🚀 𝑆𝐴̆̃𝑁 𝑆𝐴̀𝑁𝐺 𝑁𝐻𝐴̣̂𝑁 𝐿𝐸̣̂𝑁𝐻 !  🎯"
    )

    await send_response(message, "PING", content, delete_user_msg=True, keep_forever=True, with_keyboard=True)
    return True

async def create_router():
    router = Router()

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_ping, Command("ping"))
        else:
            logger.debug('Skipping registration for handle_ping (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_checkid, Command("checkid"))
        else:
            logger.debug('Skipping registration for handle_checkid (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_sms, Command("sms"))
        else:
            logger.debug('Skipping registration for handle_sms (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_spam, Command("spam"))
        else:
            logger.debug('Skipping registration for handle_spam (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_free, Command("free"))
        else:
            logger.debug('Skipping registration for handle_free (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_vip, Command("vip"))
        else:
            logger.debug('Skipping registration for handle_vip (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_call, Command("call"))
        else:
            logger.debug('Skipping registration for handle_call (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_ddos, Command("ddos"))
        else:
            logger.debug('Skipping registration for handle_ddos (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_full, Command("full"))
        else:
            logger.debug('Skipping registration for handle_full (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_tiktok, Command("tiktok"))
        else:
            logger.debug('Skipping registration for handle_tiktok (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_ngl, Command("ngl"))
        else:
            logger.debug('Skipping registration for handle_ngl (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_kill_process, Command("kill"))
        else:
            logger.debug('Skipping registration for handle_kill_process (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_kill_all_processes, Command("killall"))
        else:
            logger.debug('Skipping registration for handle_kill_all_processes (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_random_image, Command("img"))
        else:
            logger.debug('Skipping registration for handle_random_image (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_random_video, Command("vid"))
        else:
            logger.debug('Skipping registration for handle_random_video (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    try:
        if AIOGRAM_AVAILABLE and isinstance(router, Router):
            router.message.register(handle_non_command_message, Command("handle_non_command_message"))
        else:
            logger.debug('Skipping registration for handle_non_command_message (aiogram not available)')
    except Exception as e:
        logger.debug(f'Skipping registration due to error: {e}')

    return router
    
@user_cooldown()
@group_only
@admin_only
async def handle_add_vip(message: Message):
    if not message.from_user:
        return False
    user = message.from_user

    params = extract_params(message)

    if len(params) < 1:
        await send_response(
            message,
            "ADD VIP HELP",
            "👼🏻 Cú pháp: /themvip USER_ID [TÊN]",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    target_id = params[0]. strip()
    target_name = " ".join(params[1:]) if len(params) > 1 else "VIP User"

    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO admin (user_id, name, role) VALUES (?, ?, ?)",
            (target_id, target_name, 'vip')
        )
        conn.commit()
        conn.close()

        permission_cache.cache. pop(str(target_id), None)

        log_command(user. id, "/themvip", f"{target_id}")

        content = f"✅ Đã thêm VIP: {target_id}\n👤 Tên: {target_name}"
        await send_response(message, "ADD VIP SUCCESS", content, delete_user_msg=True, keep_forever=True)
        return True

    except Exception as e:
        logger.error(f"Error adding VIP {target_id}: {e}")
        await send_response(
            message,
            "ADD VIP ERROR",
            f"Lỗi khi thêm VIP: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@user_cooldown()
@group_only
@admin_only
async def handle_remove_vip(message: Message):
    if not message.from_user:
        return False
    user = message.from_user

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "REMOVE VIP HELP",
            "👼🏻 Cú pháp: /xoavip USER_ID",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    target_id = params[0].strip()

    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor. execute("DELETE FROM admin WHERE user_id = ?  AND role = 'vip'", (target_id,))
        rows_deleted = cursor.rowcount
        conn.commit()
        conn.close()

        permission_cache.cache.pop(str(target_id), None)

        log_command(user.id, "/xoavip", target_id)

        if rows_deleted > 0:
            content = f"✅ Đã xóa VIP: {target_id}"
        else:
            content = f"⚠️ Không tìm thấy VIP: {target_id}"

        await send_response(message, "REMOVE VIP", content, delete_user_msg=True, keep_forever=True)
        return True

    except Exception as e:
        logger.error(f"Error removing VIP {target_id}: {e}")
        await send_response(
            message,
            "REMOVE VIP ERROR",
            f"Lỗi khi xóa VIP: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@user_cooldown()
@group_only
@admin_only
async def handle_add_admin(message: Message):
    if not message.from_user:
        return False
    user = message.from_user

    params = extract_params(message)

    if len(params) < 1:
        await send_response(
            message,
            "ADD ADMIN HELP",
            "👼🏻 Cú pháp: /themadmin USER_ID [TÊN]",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    target_id = params[0]. strip()
    target_name = " ".join(params[1:]) if len(params) > 1 else "Admin User"

    if target_id == str(user.id):
        await send_response(
            message,
            "ADD ADMIN ERROR",
            "❌ Không thể tự thêm admin cho chính mình! ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO admin (user_id, name, role) VALUES (?, ?, ?)",
            (target_id, target_name, 'admin')
        )
        conn.commit()
        conn.close()

        permission_cache.cache.pop(str(target_id), None)

        log_command(user. id, "/themadmin", target_id)

        content = f"✅ Đã thêm Admin: {target_id}\n👤 Tên: {target_name}"
        await send_response(message, "ADD ADMIN SUCCESS", content, delete_user_msg=True, keep_forever=True)
        return True

    except Exception as e:
        logger.error(f"Error adding admin {target_id}: {e}")
        await send_response(
            message,
            "ADD ADMIN ERROR",
            f"Lỗi khi thêm Admin: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@user_cooldown()
@group_only
@admin_only
async def handle_remove_admin(message: Message):
    if not message.from_user:
        return False
    user = message.from_user

    params = extract_params(message)

    if len(params) != 1:
        await send_response(
            message,
            "REMOVE ADMIN HELP",
            "👼🏻 Cú pháp: /xoaadmin USER_ID",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    target_id = params[0].strip()

    if target_id == str(ADMIN_IDS[0]):
        await send_response(
            message,
            "REMOVE ADMIN ERROR",
            "❌ Không thể xóa Super Admin!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    if target_id == str(user.id):
        await send_response(
            message,
            "REMOVE ADMIN ERROR",
            "❌ Không thể tự xóa admin của chính mình!",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM admin WHERE user_id = ? AND role = 'admin'", (target_id,))
        rows_deleted = cursor.rowcount
        conn. commit()
        conn.close()

        permission_cache.cache. pop(str(target_id), None)

        log_command(user.id, "/xoaadmin", target_id)

        if rows_deleted > 0:
            content = f"✅ Đã xóa Admin: {target_id}"
        else:
            content = f"⚠️ Không tìm thấy Admin: {target_id}"

        await send_response(message, "REMOVE ADMIN", content, delete_user_msg=True, keep_forever=True)
        return True

    except Exception as e:
        logger.error(f"Error removing admin {target_id}: {e}")
        await send_response(
            message,
            "REMOVE ADMIN ERROR",
            f"Lỗi khi xóa Admin: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@group_only
@admin_only
async def handle_list_vip(message: Message):
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, name, role FROM admin ORDER BY role, user_id")
        admin_list = cursor.fetchall()
        conn.close()

        if not admin_list:
            await send_response(
                message,
                "ADMIN LIST",
                "📋 Chưa có VIP/Admin nào trong hệ thống! ",
                delete_user_msg=True,
                auto_delete_after=15
            )
            return False

        content = "📋 DANH SÁCH VIP & ADMIN:\n\n"

        admin_users = []
        vip_users = []

        for item in admin_list:
            if item['role'] == 'admin':
                admin_users.append(item)
            elif item['role'] == 'vip':
                vip_users.append(item)

        if admin_users:
            content += "👑 ADMIN:\n"
            for i, admin in enumerate(admin_users, 1):
                content += f"  {i}. {admin['name']} ({admin['user_id']})\n"
            content += "\n"

        if vip_users:
            content += "🎖️ VIP:\n"
            for i, vip in enumerate(vip_users, 1):
                content += f"  {i}. {vip['name']} ({vip['user_id']})\n"

        content += f"\nTổng: {len(admin_users)} Admin, {len(vip_users)} VIP"

        await send_response(message, "ADMIN LIST", content, delete_user_msg=True, keep_forever=True)
        return True

    except Exception as e:
        logger.error(f"Error getting admin list: {e}")
        await send_response(
            message,
            "LIST ERROR",
            f"Lỗi khi lấy danh sách: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@user_cooldown()
@group_only
@admin_only
async def handle_broadcast_all(message: Message):
    try:
        params = extract_params(message)

        if not params or not " ".join(params):
            await send_response(
                message,
                "BROADCAST HELP",
                "👼🏻 Cú pháp: /broadcast_all <nội dung>",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        broadcast_text = " ".join(params)

        users_data = await async_db_execute(
            "SELECT user_id FROM users WHERE is_approved = TRUE"
        )
        group_ids = await get_all_group_ids()

        if users_data is None:
            await send_response(
                message,
                "BROADCAST ERROR",
                "❌ Không thể lấy danh sách user",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        all_user_ids = [u[0] for u in users_data]
        total_targets = len(all_user_ids) + len(group_ids)

        if total_targets == 0:
            await send_response(
                message,
                "BROADCAST ERROR",
                "❌ Không có user/nhóm nào để gửi",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        send_msg = await send_response(
            message,
            "BROADCAST",
            f"📢 Đang gửi tới {len(all_user_ids)} user và {len(group_ids)} nhóm.. .",
            delete_user_msg=False
        )

        send_count = 0
        fail_count = 0

        safe_text = escape_markdown_v2(broadcast_text)
        time_str, date_str = get_vietnam_time()
        safe_time = escape_markdown_v2(time_str)

        formatted_text = (
            f"┏ 📢 *THÔNG BÁO ADMIN* ┓\n"
            f"┣{chr(8213)*20}\n"
            f"┣ {safe_text}\n"
            f"┣{chr(8213)*20}\n"
            f"┗ ⏱️ *{safe_time}*"
        )

        target_ids = list(all_user_ids) + list(group_ids)
        random.shuffle(target_ids)

        for target_id in target_ids:
            try:
                await bot_aiogram.send_message(
                    target_id,
                    formatted_text,
                    parse_mode="MarkdownV2"
                )
                send_count += 1
                await asyncio.sleep(0.15)
            except TelegramForbiddenError:
                fail_count += 1
                if target_id < 0:
                    try:
                        await async_db_execute(
                            "DELETE FROM groups WHERE chat_id = ?",
                            (target_id,)
                        )
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"Error broadcasting to {target_id}: {e}")
                fail_count += 1

        log_command(message.from_user.id, "/broadcast_all", f"{send_count}/{total_targets}")

        result_content = f"✅ Đã gửi: **{send_count}/{total_targets}**\n❌ Lỗi: {fail_count}"

        await send_response(
            message,
            "BROADCAST RESULT",
            result_content,
            processing_msg=send_msg,
            delete_user_msg=False,
            keep_forever=True
        )

        return True

    except Exception as e:
        logger.error(f"Error broadcasting: {e}", exc_info=True)
        await send_response(
            message,
            "BROADCAST ERROR",
            f"Lỗi broadcast: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@group_only
@admin_only
async def handle_add_group(message: Message):
    try:
        params = extract_params(message)

        if not params:
            await send_response(
                message,
                "ADD GROUP HELP",
                "👼🏻 Cú pháp: /addgr <chat_id>\n(ID nhóm thường bắt đầu bằng dấu -)",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        chat_id_str = params[0]. strip()

        if not chat_id_str. startswith('-') or not chat_id_str[1:].isdigit():
            await send_response(
                message,
                "ADD GROUP ERROR",
                "⚠️ ID nhóm chat không hợp lệ (thường bắt đầu bằng dấu -)",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        chat_id = int(chat_id_str)

        try:
            chat_info = await bot_aiogram.get_chat(chat_id)
            if chat_info.type not in ['group', 'supergroup']:
                await send_response(
                    message,
                    "ADD GROUP ERROR",
                    f"⚠️ ID {chat_id} không phải là nhóm hoặc siêu nhóm",
                    delete_user_msg=True,
                    auto_delete_after=8
                )
                return False
        except Exception as e:
            await send_response(
                message,
                "ADD GROUP ERROR",
                f"❌ Không thể lấy thông tin nhóm {chat_id}.  Bot đã ở trong nhóm chưa?\nLỗi: {e}",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        result = await async_db_execute(
            "INSERT OR IGNORE INTO groups (chat_id) VALUES (?)",
            (chat_id,)
        )

        if result is not None:
            check = await async_db_fetchone(
                "SELECT 1 FROM groups WHERE chat_id = ?",
                (chat_id,)
            )
            if check:
                log_command(message.from_user.id, "/addgr", str(chat_id))
                content = f"✅ Đã thêm nhóm chat ID: `{chat_id}` vào danh sách broadcast"
                await send_response(message, "ADD GROUP SUCCESS", content, delete_user_msg=True, keep_forever=True)
                return True
            else:
                await send_response(
                    message,
                    "ADD GROUP ERROR",
                    f"❌ Không thể thêm nhóm {chat_id}",
                    delete_user_msg=True,
                    auto_delete_after=8
                )
                return False
        else:
            await send_response(
                message,
                "ADD GROUP ERROR",
                f"❌ Lỗi DB khi thêm nhóm {chat_id}",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

    except ValueError:
        await send_response(
            message,
            "ADD GROUP ERROR",
            "⚠️ ID nhóm không hợp lệ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    except Exception as e:
        logger.error(f"Error adding group: {e}", exc_info=True)
        await send_response(
            message,
            "ADD GROUP ERROR",
            f"❌ Lỗi: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@group_only
@admin_only
async def handle_remove_group(message: Message):
    try:
        params = extract_params(message)

        if not params:
            await send_response(
                message,
                "DEL GROUP HELP",
                "👼🏻 Cú pháp: /delgr <chat_id>",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        chat_id_str = params[0].strip()

        if not chat_id_str.startswith('-') or not chat_id_str[1:].isdigit():
            await send_response(
                message,
                "DEL GROUP ERROR",
                "⚠️ ID nhóm chat không hợp lệ",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        chat_id = int(chat_id_str)

        result = await async_db_execute(
            "DELETE FROM groups WHERE chat_id = ?",
            (chat_id,)
        )

        if result is not None:
            check = await async_db_fetchone(
                "SELECT 1 FROM groups WHERE chat_id = ?",
                (chat_id,)
            )
            if not check:
                log_command(message.from_user.id, "/delgr", str(chat_id))
                content = f"✅ Đã xóa nhóm chat ID: `{chat_id}` khỏi danh sách"
                await send_response(message, "DEL GROUP SUCCESS", content, delete_user_msg=True, keep_forever=True)
                return True
            else:
                await send_response(
                    message,
                    "DEL GROUP ERROR",
                    f"❌ Không thể xóa nhóm {chat_id}",
                    delete_user_msg=True,
                    auto_delete_after=8
                )
                return False
        else:
            await send_response(
                message,
                "DEL GROUP ERROR",
                f"❌ Lỗi DB khi xóa nhóm {chat_id}",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

    except ValueError:
        await send_response(
            message,
            "DEL GROUP ERROR",
            "⚠️ ID nhóm không hợp lệ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    except Exception as e:
        logger.error(f"Error removing group: {e}", exc_info=True)
        await send_response(
            message,
            "DEL GROUP ERROR",
            f"❌ Lỗi: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

@group_only
@admin_only
async def handle_list_groups(message: Message):
    try:
        groups_data = await async_db_execute(
            "SELECT chat_id FROM groups ORDER BY chat_id ASC"
        )

        if groups_data is None:
            await send_response(
                message,
                "LIST GROUPS ERROR",
                "❌ Lỗi lấy danh sách nhóm",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        if not groups_data:
            await send_response(
                message,
                "LIST GROUPS",
                "📋 Chưa có nhóm nào được thêm.  Dùng `/addgr`",
                delete_user_msg=True,
                auto_delete_after=8
            )
            return False

        content = f"📋 Tổng {len(groups_data)} nhóm đã thêm:\n\n"
        for row in groups_data:
            content += f"- `{row[0]}`\n"

        if len(content) > 3500:
            content = content[:3500] + "\n...  (Quá dài)"

        log_command(message.from_user.id, "/allgr", "list_groups")

        await send_response(
            message,
            "LIST GROUPS",
            content,
            delete_user_msg=True,
            keep_forever=True
        )
        return True

    except Exception as e:
        logger.error(f"Error listing groups: {e}")
        await send_response(
            message,
            "LIST GROUPS ERROR",
            f"❌ Lỗi: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

async def handle_start(message: Message):
    if not message.from_user:
        return False

    user = message.from_user
    user_id = user.id
    is_admin_flag = is_admin(user_id)

    user_info = await get_user(user_id, user. username)
    if not user_info:
        await send_response(
            message,
            "ERROR",
            "Lỗi: Không thể tải thông tin tài khoản. Vui lòng thử lại.",
            delete_user_msg=False
        )
        return False

    mention = get_user_mention(user)
    status = "Admin 👑" if user_info["is_admin"] else ("Thành viên ⭐" if user_info["is_approved"] else "Khách ⚠️")

    menu_text = (
        f"🎯 Chào mừng {mention} đến với Bot Tổng Hợp (Premium VIP)!\n"
        f"Bot quản lý bởi @{OWNER_USERNAME}.\n\n"
        f"👤 *TÀI KHOẢN:*\n"
        f"   - Status: **{status}**\n"
        f"   - Số dư: **{user_info['balance']:,}** VNĐ 💵\n"
    )

    if user_info["is_approved"]:
        menu_text += (
            f"\n🔥 *LỆNH CÔNG KHAI:*\n"
            f"   • `/ping` - Xem trạng thái Bot\n"
            f"   • `/checkid` - Xem thông tin ID\n"
            f"   • `/sms` - Gửi SMS 50 số\n"
            f"   • `/spam` - Spam liên tục\n"
            f"   • `/free` - Spam SMS Zalo\n"
            f"\n💫 *VIP PERMANENT:*\n"
            f"   • `/vip` - SMS + Call 10 số/lần\n"
            f"   • `/call` - Gọi 1 số\n"
            f"   • `/ddos` - Đánh sập Web\n"
            f"   • `/full` - Chạy Full 24h\n"
            f"   • `/tiktok` - Tăng View TikTok\n"
            f"   • `/ngl` - Spam NGL\n"
            f"   • `/img` - Random ảnh\n"
            f"   • `/vid` - Random video\n"
            f"   • `/kill` - Dừng lệnh"
        )
    else:
        menu_text += (
            f"\n⚠️ *Tài khoản chưa duyệt.*\n"
            f"Liên hệ Admin @{OWNER_USERNAME} (ID: `{user_id}`) để kích hoạt + `{START_BALANCE:,}` VNĐ."
        )

    if is_admin_flag:
        menu_text += (
            f"\n\n👑 *ADMIN MENU:*\n"
            f"   • `/themvip` - Thêm VIP\n"
            f"   • `/xoavip` - Xóa VIP\n"
            f"   • `/themadmin` - Thêm Admin\n"
            f"   • `/xoaadmin` - Xóa Admin\n"
            f"   • `/listvip` - Danh sách VIP/Admin\n"
            f"   • `/addgr` - Thêm nhóm\n"
            f"   • `/delgr` - Xóa nhóm\n"
            f"   • `/allgr` - Danh sách nhóm\n"
            f"   • `/broadcast_all` - Gửi tin nhắn toàn bộ\n"
            f"   • `/killall` - Dừng tất cả lệnh"
        )

    keyboard = None
    if is_admin_flag:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="👑 ADMIN CONTROL",
                url=f"https://t.me/{OWNER_USERNAME}"
            )]
        ])
    elif not user_info["is_approved"]:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"Liên hệ @{OWNER_USERNAME}",
                url=f"https://t.me/{OWNER_USERNAME}"
            )]
        ])

    try:
        await bot_aiogram.send_message(
            chat_id=message.chat.id,
            text=menu_text,
            parse_mode="Markdown",
            reply_markup=keyboard,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Error sending start message: {e}")

    return True

@user_cooldown()
@group_only
async def handle_nap(message: Message):
    if not message.from_user:
        return False

    user_info = await get_user(message. from_user.id)
    if not user_info:
        await send_response(
            message,
            "ERROR",
            "Lỗi: Không thể lấy thông tin tài khoản",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    random_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    username = user_info["username"] if user_info["username"] else f"user_{user_info['user_id']}"

    nap_text = (
        f"**1.  THÔNG TIN CK:**\n{BANK_INFO}\n\n"
        f"**2. NỘI DUNG CK (BẮT BUỘC):**\n   `NAP {username. upper()} {random_code}`\n\n"
        f"**3. MÃ QR:** [Bấm xem ảnh QR]({QR_CODE_IMAGE_URL})\n\n"
        f"**4. XÁC NHẬN:** Sau khi CK, dùng: `/nap_request <số tiền>`\n\n"
        f"💰 *Số dư hiện tại*: **{user_info['balance']:,}** VNĐ.\n\n"
        f"*{random. choice(RANDOM_THANKS)}*"
    )

    try:
        await bot_aiogram.send_message(
            message.chat.id,
            nap_text,
            parse_mode="Markdown",
            disable_web_page_preview=False
        )
    except Exception as e:
        await send_response(
            message,
            "NAP ERROR",
            f"Không thể hiển thị thông tin.  Lỗi: {e}",
            delete_user_msg=False
        )

    log_command(message. from_user.id, "/nap", "request_info")
    return True

@user_cooldown()
@group_only
async def handle_nap_request(message: Message):
    if not message.from_user:
        return False

    user_info = await get_user(message.from_user.id)
    if not user_info:
        await send_response(
            message,
            "ERROR",
            "Lỗi: Không thể lấy thông tin tài khoản",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    params = extract_params(message)

    if not params:
        await send_response(
            message,
            "NAP REQUEST HELP",
            "Cú pháp: `/nap_request <số tiền>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    try:
        amount = int(params[0])
        if amount <= 0:
            raise ValueError("Số tiền phải lớn hơn 0")
    except (ValueError, IndexError):
        await send_response(
            message,
            "NAP REQUEST ERROR",
            "Số tiền không hợp lệ",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

    try:
        await async_db_execute(
            "INSERT INTO nap_request (user_id, amount) VALUES (?, ?)",
            (user_info["user_id"], amount)
        )

        log_command(message.from_user.id, "/nap_request", str(amount))

        content = f"✅ Đã gửi yêu cầu nạp **{amount:,}** VNĐ.\n⏳ Chờ Admin duyệt."

        await send_response(
            message,
            "NAP REQUEST SENT",
            content,
            delete_user_msg=True,
            keep_forever=True
        )

        for admin_id in ADMIN_IDS:
            try:
                admin_msg = (
                    f"🔔 YÊU CẦU NẠP TIỀN MỚI:\n"
                    f"User: `{user_info['user_id']}` (@{user_info['username']})\n"
                    f"Số tiền: **{amount:,}** VNĐ\n"
                    f"Dùng: `/duyet_nap <request_id>`"
                )
                await bot_aiogram.send_message(
                    admin_id,
                    admin_msg,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.warning(f"Error notifying admin {admin_id}: {e}")

        return True

    except Exception as e:
        logger.error(f"Error processing nap request: {e}")
        await send_response(
            message,
            "NAP REQUEST ERROR",
            f"Lỗi xử lý yêu cầu: {str(e)}",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False

async def update_router_with_handlers(router: Router):
    router.message. register(handle_start, Command("start"))
    router.message. register(handle_add_vip, Command("themvip"))
    router.message.register(handle_remove_vip, Command("xoavip"))
    router.message.register(handle_add_admin, Command("themadmin"))
    router.message.register(handle_remove_admin, Command("xoaadmin"))
    router.message.register(handle_list_vip, Command("listvip"))
    router.message.register(handle_add_group, Command("addgr"))
    router.message.register(handle_remove_group, Command("delgr"))
    router.message.register(handle_list_groups, Command("allgr"))
    router.message.register(handle_broadcast_all, Command("broadcast_all"))
    router.message.register(handle_nap, Command("nap"))
    router.message.register(handle_nap_request, Command("nap_request"))

    return router
    
@user_cooldown()
@group_only
async def handle_ask_ai(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or not " ". join(params):
        await send_response(
            message,
            "ASK AI HELP",
            "Cú pháp: `/ask <câu hỏi>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    query = " ".join(params). strip()
    
    msg = await send_response(
        message,
        "AI PROCESSING",
        f"⏳ Đang xử lý: `{query[:50]}...`",
        delete_user_msg=False
    )
    
    try:
        quoted_query = quote(query)
        data = await asyncio.to_thread(
            get_api_result_sync,
            f"{API_SEARCH_BASE}?chat={quoted_query}"
        )
        
        if not data. get("ok"):
            await send_response(
                message,
                "AI ERROR",
                f"❌ {data.get('error', 'Không rõ')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        response_text = data.get("text", "_Không có nội dung._")
        
        if len(response_text) > 3500:
            response_text = response_text[:3500] + "\n.. .(Đã cắt bớt)"
        
        log_command(user_id, "/ask", query[:50])
        
        await send_response(
            message,
            "AI RESPONSE",
            response_text,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error in AI query: {e}")
        await send_response(
            message,
            "AI ERROR",
            f"❌ Lỗi kết nối: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_md5_prediction(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if len(params) != 1 or not re.fullmatch(r"^[0-9a-f]{32}$", params[0]. lower()):
        await send_response(
            message,
            "MD5 HELP",
            "Cú pháp: `/tx <md5_hash_32_ký_tự>`\n\nVí dụ: `/tx 5d41402abc4b2a76b9719d911017c592`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    md5_hash = params[0]. strip(). lower()
    
    msg = await send_response(
        message,
        "MD5 PROCESSING",
        f"🔮 Đang giải mã: `{md5_hash}`.. .",
        delete_user_msg=False
    )
    
    try:
        md5_analysis = await asyncio.to_thread(predict_md5_logic, md5_hash)
        
        if not md5_analysis.get("ok"):
            await send_response(
                message,
                "MD5 ERROR",
                f"❌ Lỗi: {md5_analysis.get('error', 'Không rõ')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        emoji_dice = {1: '⚀', 2: '⚁', 3: '⚂', 4: '⚃', 5: '⚄', 6: '⚅'}
        dice_display = f"{emoji_dice[md5_analysis['dice'][0]]} {emoji_dice[md5_analysis['dice'][1]]} {emoji_dice[md5_analysis['dice'][2]]}"
        seed_next_hex = f"{md5_analysis['seed_next']:08X}"
        
        result_card = (
            f"🔑 *MD5 Đầu Vào:* `{md5_hash}`\n\n"
            f"**🔬 PHÂN TÍCH THUẬT TOÁN (LCG v2. 0):**\n"
            f"   • Seed Hiện Tại: `{md5_hash[:8]}`\n"
            f"   • Seed Tiếp Theo: `{seed_next_hex}`\n"
            f"   • MD5 Vòng Sau (Dự đoán): `{md5_analysis['predicted_md5']}`\n\n"
            f"🎲 *DỰ ĐOÁN XÚC XẮC (Vòng Sau)*:\n"
            f"   - Xúc Xắc: **{dice_display}**\n"
            f"   - Tổng Điểm: **{md5_analysis['total']}**\n"
            f"   - **KẾT QUẢ:** **{md5_analysis['result']}** 🥇"
        )
        
        log_command(user_id, "/tx", md5_hash[:16])
        
        await send_response(
            message,
            "MD5 RESULT",
            result_card,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error in MD5 prediction: {e}")
        await send_response(
            message,
            "MD5 ERROR",
            f"❌ Lỗi xử lý: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_qrcode(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or not " ".join(params):
        await send_response(
            message,
            "QRCODE HELP",
            "Cú pháp: `/qrcode <nội dung>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    if not TTS_QR_AVAILABLE or not qrcode:
        await send_response(
            message,
            "QRCODE ERROR",
            "⚠️ Thiếu thư viện `qrcode`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    text = " ".join(params)
    
    msg = await send_response(
        message,
        "QRCODE PROCESSING",
        f"🔳 Đang tạo mã QR.. .",
        delete_user_msg=False
    )
    
    try:
        qr_data = await asyncio.to_thread(generate_qr_code_sync, text)
        
        if isinstance(qr_data, str):
            await send_response(
                message,
                "QRCODE ERROR",
                f"❌ {qr_data}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/qrcode", text[:50])
        
        try:
            await bot_aiogram.send_photo(
                message.chat.id,
                qr_data,
                caption=f"✅ *Mã QR cho:* `{escape_markdown_v2(text[:50])}...`",
                parse_mode="MarkdownV2"
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat.id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except Exception as e:
            await send_response(
                message,
                "QRCODE ERROR",
                f"❌ Không thể gửi QR: {str(e)}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating QR code: {e}")
        await send_response(
            message,
            "QRCODE ERROR",
            f"❌ Lỗi tạo QR: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_voice(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not TTS_QR_AVAILABLE or not gTTS:
        await send_response(
            message,
            "VOICE ERROR",
            "⚠️ Thiếu thư viện `gTTS`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    if not params or not " ".join(params):
        await send_response(
            message,
            "VOICE HELP",
            "Cú pháp: `/voice <văn bản>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    text = " ".join(params)
    
    msg = await send_response(
        message,
        "VOICE PROCESSING",
        "🎤 Đang tạo giọng nói...",
        delete_user_msg=False
    )
    
    try:
        audio_data = await asyncio.to_thread(text_to_speech_sync, text)
        
        if isinstance(audio_data, str):
            await send_response(
                message,
                "VOICE ERROR",
                f"❌ {audio_data}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/voice", text[:50])
        
        try:
            await bot_aiogram.send_voice(
                message.chat. id,
                audio_data,
                caption=f"🗣️ *Văn bản:* `{escape_markdown_v2(text[:50])}... `",
                parse_mode="MarkdownV2"
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat.id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except Exception as e:
            await send_response(
                message,
                "VOICE ERROR",
                f"❌ Không thể gửi Voice: {str(e)}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating voice: {e}")
        await send_response(
            message,
            "VOICE ERROR",
            f"❌ Lỗi tạo Voice: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_weather(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    location = " ".join(params) if params else random. choice(["Hà Nội", "Hồ Chí Minh", "Đà Nẵng"])
    
    msg = await send_response(
        message,
        "WEATHER PROCESSING",
        f"🌤️ Đang lấy thời tiết cho: `{location}`.. .",
        delete_user_msg=False
    )
    
    try:
        geo_response = requests.get(
            f"https://geocoding-api.open-meteo.com/v1/search?name={location}&count=1&language=vi&format=json",
            timeout=REQUEST_TIMEOUT
        )
        geo_data = geo_response.json()
        
        if not geo_data.get("results"):
            await send_response(
                message,
                "WEATHER ERROR",
                f"❌ Không tìm thấy địa điểm: `{location}`",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        result = geo_data["results"][0]
        lat, lon = result["latitude"], result["longitude"]
        location_name = result["name"]
        
        tomorrow_data = await asyncio.to_thread(
            get_api_result_sync,
            f"https://api.tomorrow.io/v4/weather/forecast?location={lat},{lon}&apikey={TOMORROW_API_KEY}"
        )
        
        weather_data = await asyncio.to_thread(
            get_api_result_sync,
            f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric&lang=vi"
        )
        
        weather_api_data = await asyncio.to_thread(
            get_api_result_sync,
            f"http://api.weatherapi.com/v1/forecast.json?key={WEATHERAPI_KEY}&q={lat},{lon}&days=1&aqi=yes&lang=vi"
        )
        
        if weather_data.get("status") is False:
            await send_response(
                message,
                "WEATHER ERROR",
                f"❌ Không thể lấy dữ liệu thời tiết",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        current = weather_data. get("main", {})
        weather_info = weather_data.get("weather", [{}])[0]
        wind = weather_data.get("wind", {})
        
        def get_uv_level(index):
            if index <= 2:
                return "Thấp"
            elif index <= 5:
                return "Trung bình"
            elif index <= 7:
                return "Cao"
            elif index <= 10:
                return "Rất cao"
            return "Nguy hiểm"
        
        def get_wind_direction(degrees):
            directions = ["Bắc", "Đông Bắc", "Đông", "Đông Nam", "Nam", "Tây Nam", "Tây", "Tây Bắc"]
            return directions[round(degrees / 45) % 8]
        
        uv_index = weather_api_data. get("current", {}).get("uv", 0) if weather_api_data.get("status") else 0
        
        content = (
            f"📍 *Địa điểm:* {location_name. upper()}\n"
            f"🌡️ *Nhiệt độ:* {current.get('temp', 'N/A')}°C (Cảm giác: {current.get('feels_like', 'N/A')}°C)\n"
            f"☁️ *Thời tiết:* {weather_info.get('description', 'N/A'). capitalize()}\n"
            f"💧 *Độ ẩm:* {current.get('humidity', 'N/A')}%\n"
            f"💨 *Gió:* {wind.get('speed', 'N/A')} m/s ({get_wind_direction(wind.get('deg', 0))})\n"
            f"☀️ *UV Index:* {uv_index} ({get_uv_level(uv_index)})\n"
            f"👁️ *Tầm nhìn:* {weather_data.get('visibility', 0) / 1000} km\n"
            f"🔽 *Áp suất:* {current.get('pressure', 'N/A')} hPa"
        )
        
        log_command(user_id, "/weather", location[:30])
        
        await send_response(
            message,
            "WEATHER REPORT",
            content,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error getting weather: {e}")
        await send_response(
            message,
            "WEATHER ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_xoso(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    msg = await send_response(
        message,
        "XOSO PROCESSING",
        "🎟️ Đang lấy KQXS Miền Bắc...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_XOSO_URL
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "XOSO ERROR",
                f"❌ {data.get('message', 'Lỗi API')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        result = data.get("result", "Không có dữ liệu")
        
        log_command(user_id, "/kqxs", "XSMB")
        
        await send_response(
            message,
            "KQXS MIỀN BẮC",
            result if isinstance(result, str) else json.dumps(result, ensure_ascii=False),
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error getting XOSO: {e}")
        await send_response(
            message,
            "XOSO ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_ip_lookup(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params:
        await send_response(
            message,
            "IP LOOKUP HELP",
            "Cú pháp: `/ip <địa_chỉ_IP>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    ip_address = params[0].strip()
    
    msg = await send_response(
        message,
        "IP LOOKUP PROCESSING",
        f"🌐 Đang tra cứu IP: `{ip_address}`...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            IPLOOKUP_API. format(ip=ip_address)
        )
        
        if not data.get("status") or data.get("message") != "success":
            await send_response(
                message,
                "IP LOOKUP ERROR",
                f"❌ IP không tồn tại hoặc lỗi API",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        ip_data = data.get("data", {})
        
        content = (
            f"🌐 *IP:* `{ip_data.get('query', 'N/A')}`\n"
            f"📊 *Loại:* {ip_data. get('type', 'N/A')}\n"
            f"🏳️ *Quốc gia:* {ip_data.get('country', 'N/A')} ({ip_data.get('countryCode', 'N/A')})\n"
            f"🏙️ *Thành phố:* {ip_data. get('city', 'N/A')}, {ip_data.get('regionName', 'N/A')}\n"
            f"📌 *Tọa độ:* {ip_data. get('lat', 'N/A')}, {ip_data.get('lon', 'N/A')}\n"
            f"🏢 *ISP:* {ip_data. get('isp', 'N/A')}\n"
            f"🏭 *Organization:* {ip_data.get('org', 'N/A')}\n"
            f"🕰️ *Múi giờ:* {ip_data.get('timezone', 'N/A')}\n"
            f"💰 *Tiền tệ:* {ip_data.get('currency', 'N/A')}"
        )
        
        log_command(user_id, "/ip", ip_address)
        
        await send_response(
            message,
            "IP LOOKUP RESULT",
            content,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error in IP lookup: {e}")
        await send_response(
            message,
            "IP LOOKUP ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_facebook_info(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or not params[0]. isdigit():
        await send_response(
            message,
            "FB INFO HELP",
            "Cú pháp: `/fb <UID_Facebook>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    fb_uid = params[0].strip()
    
    msg = await send_response(
        message,
        "FB PROCESSING",
        f"🔍 Đang tìm UID: `{fb_uid}`...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_FB_INFO.format(uid=fb_uid)
        )
        
        if not data. get("status"):
            await send_response(
                message,
                "FB ERROR",
                f"❌ {data.get('message', 'Không tìm thấy')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        fb_data = data.get("data", {})
        profile_url = fb_data.get("profile_url", f"https://www.facebook.com/{fb_uid}")
        
        content = (
            f"👤 *Tên:* {fb_data.get('name', 'N/A')}\n"
            f"🆔 *UID:* `{fb_data.get('uid', 'N/A')}`\n"
            f"✅ *Verified:* {'Có ✓' if fb_data.get('is_verified') else 'Không'}\n"
            f"👥 *Followers:* `{fb_data.get('followers', 'N/A')}`\n"
            f"🔗 [Xem Profile]({profile_url})"
        )
        
        photo_sent = False
        if fb_data.get("avatar"):
            try:
                await bot_aiogram.send_photo(
                    message.chat. id,
                    fb_data. get("avatar")
                )
                photo_sent = True
            except Exception as e:
                logger.warning(f"Error sending FB avatar: {e}")
        
        log_command(user_id, "/fb", fb_uid)
        
        await send_response(
            message,
            "FACEBOOK INFO",
            content,
            processing_msg=msg if not photo_sent else None,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger. error(f"Error getting Facebook info: {e}")
        await send_response(
            message,
            "FB ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

@user_cooldown()
@group_only
async def handle_tiktok_info(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params:
        await send_response(
            message,
            "TT INFO HELP",
            "Cú pháp: `/tt <username_TikTok>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    tt_username = params[0].strip(). replace("@", "")
    
    msg = await send_response(
        message,
        "TT PROCESSING",
        f"🔍 Đang tìm TikTok: `@{tt_username}`...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_TT_INFO. format(username=tt_username)
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "TT ERROR",
                f"❌ {data.get('message', 'Không tìm thấy')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        tt_data = data.get("data", {})
        profile_url = f"https://www.tiktok.com/@{tt_username}"
        
        content = (
            f"👤 *Tên:* {tt_data.get('nickname', 'N/A')}\n"
            f"🔗 *Username:* `@{tt_data.get('username', 'N/A')}`\n"
            f"✅ *Verified:* {'Có ✓' if tt_data.get('verified') else 'Không'}\n"
            f"👥 *Followers:* `{tt_data.get('followerCount', 'N/A')}`\n"
            f"➡️ *Following:* `{tt_data. get('followingCount', 'N/A')}`\n"
            f"🎥 *Videos:* `{tt_data.get('totalVideos', 'N/A')}`\n"
            f"❤️ *Likes:* `{tt_data.get('totalFavorite', 'N/A')}`\n"
            f"📝 *Bio:* _{tt_data.get('signature', 'N/A')}_\n"
            f"🔗 [Xem Profile]({profile_url})"
        )
        
        photo_sent = False
        if tt_data. get("avatar"):
            try:
                await bot_aiogram. send_photo(
                    message.chat.id,
                    tt_data.get("avatar")
                )
                photo_sent = True
            except Exception as e:
                logger.warning(f"Error sending TT avatar: {e}")
        
        log_command(user_id, "/tt", tt_username)
        
        await send_response(
            message,
            "TIKTOK INFO",
            content,
            processing_msg=msg if not photo_sent else None,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error getting TikTok info: {e}")
        await send_response(
            message,
            "TT ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

def extract_params(message: Message) -> List[str]:
    if not message.text:
        return []
    
    parts = message.text.split()
    return parts[1:] if len(parts) > 1 else []

async def send_response(
    message: Message,
    title: str,
    text: str,
    processing_msg: Optional[Message] = None,
    delete_user_msg: bool = True,
    auto_delete_after: int = 0,
    keep_forever: bool = False,
    with_keyboard: bool = False
) -> Optional[Message]:
    try:
        current_time = get_vietnam_time()
        time_str = f"{current_time[0]} | {current_time[1]}"
        
        safe_title = escape_markdown_v2(title. upper())
        text_limit = 1000 - len(title) - len(time_str) - 100
        safe_text = escape_markdown_v2(text[:text_limit] + ('...' if len(text) > text_limit else ''))
        safe_time = escape_markdown_v2(time_str)
        safe_owner = escape_markdown_v2(OWNER_USERNAME)
        
        formatted_caption = (
            f"┏ 💎 *{safe_title}* ┓\n"
            f"┣{chr(8213)*20}\n"
            f"┣ {safe_text}\n"
            f"┣{chr(8213)*20}\n"
            f"┗ ⏱️ *{safe_time}* \\| Bot by {safe_owner}"
        )
        
        keyboard = create_group_link_keyboard() if with_keyboard else None
        
        if processing_msg:
            try:
                await bot_aiogram.delete_message(
                    chat_id=processing_msg.chat.id,
                    message_id=processing_msg.message_id
                )
            except Exception:
                pass
        
        if delete_user_msg:
            try:
                await bot_aiogram.delete_message(
                    chat_id=message.chat.id,
                    message_id=message.message_id
                )
            except Exception:
                pass
        
        sent_message = await bot_aiogram.send_message(
            chat_id=message.chat.id,
            text=formatted_caption,
            parse_mode="MarkdownV2",
            reply_markup=keyboard
        )
        
        if auto_delete_after > 0 and not keep_forever:
            asyncio.create_task(
                auto_delete_message(
                    sent_message.chat.id,
                    sent_message.message_id,
                    auto_delete_after
                )
            )
        
        return sent_message
        
    except Exception as e:
        logger.error(f"Error sending response: {e}")
        return None

async def auto_delete_message(chat_id: int, message_id: int, delay: int):
    try:
        await asyncio.sleep(delay)
        await bot_aiogram.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.debug(f"Error auto-deleting message: {e}")

async def cleanup_full_status_safe():
    if 'FULL_STATUS' not in globals() or 'FULL_LOCK' not in globals():
        return
    
    try:
        current_time = time.time()
        keys_to_remove = []
        
        with FULL_LOCK:
            keys_to_remove = [
                k for k, v in FULL_STATUS.items()
                if v < current_time - 3600
            ]
        
        if keys_to_remove:
            batch_size = 50
            removed_total = 0
            
            for i in range(0, len(keys_to_remove), batch_size):
                batch = keys_to_remove[i:i + batch_size]
                with FULL_LOCK:
                    for key in batch:
                        FULL_STATUS.pop(key, None)
                        removed_total += 1
                
                if i + batch_size < len(keys_to_remove):
                    await asyncio.sleep(0.01)
            
            logger.info(f"🧹 Deleted {removed_total} old entries from FULL_STATUS")
    
    except Exception as e:
        logger.error(f"Error cleanup FULL_STATUS: {e}")

def create_router() -> Router:
    router = Router()
    
    router.message. register(handle_start, Command("start"))
    router.message.register(handle_ping, Command("ping"))
    router.message.register(handle_checkid, Command("checkid"))
    router.message.register(handle_vip, Command("vip"))
    router.message.register(handle_call, Command("call"))
    router.message.register(handle_kill_process, Command("kill"))
    router.message.register(handle_kill_all_processes, Command("killall"))
    router.message.register(handle_random_image, Command("img"))
    router.message.register(handle_random_video, Command("vid"))
    router.message.register(handle_add_vip, Command("themvip"))
    router.message. register(handle_remove_vip, Command("xoavip"))
    router.message. register(handle_add_admin, Command("themadmin"))
    router.message.register(handle_remove_admin, Command("xoaadmin"))
    router.message.register(handle_list_vip, Command("listvip"))
    router.message.register(handle_add_group, Command("addgr"))
    router.message.register(handle_remove_group, Command("delgr"))
    router.message.register(handle_list_groups, Command("allgr"))
    router.message. register(handle_broadcast_all, Command("broadcast_all"))
    router.message. register(handle_nap, Command("nap"))
    router.message.register(handle_nap_request, Command("nap_request"))
    router.message.register(handle_ask_ai, Command("ask"))
    router.message.register(handle_md5_prediction, Command("tx"))
    router.message.register(handle_qrcode, Command("qrcode"))
    router.message. register(handle_voice, Command("voice"))
    router. message.register(handle_weather, Command("weather"))
    router.message.register(handle_xoso, Command("kqxs"))
    router.message.register(handle_ip_lookup, Command("ip"))
    router.message.register(handle_facebook_info, Command("fb"))
    router.message.register(handle_tiktok_info, Command("tt"))
    router.message.register(handle_non_command_message)
    
    return router

async def handle_ping(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    permission_title = get_permission_title(user_id)
    user_link = format_user_link(user)
    
    content = (
        f"{permission_title}\n"
        f"┃• 💼🏻 𝑀𝑟. 𝑈𝑠𝑒𝑟    :      {user_link}\n"
        f"┃• 🎫 𝑀𝑎̃ 𝐼𝐷       :      {user_id}\n"
        f"\n🤖 Trạng thái Bot: hoạt động 🛰️\n\n"
        f"🚀 Sẵn sàng nhận lệnh!"
    )
    
    await send_response(
        message,
        "BOT STATUS",
        content,
        delete_user_msg=True,
        keep_forever=True,
        with_keyboard=True
    )
    
    log_command(user_id, "/ping", "status_check")
    return True
    
async def handle_soundcloud_search(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or not " ". join(params):
        await send_response(
            message,
            "SOUNDCLOUD HELP",
            "Cú pháp: `/scl <tên_bài_hát>`\nVí dụ: `/scl son tung mtp`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    query = " ". join(params). strip()
    
    msg = await send_response(
        message,
        "SOUNDCLOUD SEARCH",
        f"🔍 Đang tìm: `{query}`.. .",
        delete_user_msg=False
    )
    
    try:
        cid = await asyncio.to_thread(get_client_id)
        ctime = str(int(time.time()))
        
        path = "/api/v2/search"
        params_dict = {
            "q": query,
            "type": "song",
            "count": 10,
            "ctime": ctime,
            "version": ZING_VERSION,
            "apiKey": ZING_API_KEY
        }
        
        search_results = await asyncio.to_thread(
            requests.get,
            f"https://api-v2.soundcloud.com/search/tracks",
            params={
                "q": query,
                "client_id": cid,
                "limit": 10,
                "offset": 0,
                "app_locale": "en"
            },
            timeout=REQUEST_TIMEOUT,
            headers=SC_HEADERS
        )
        
        search_results. raise_for_status()
        search_data = search_results.json()
        
        tracks = []
        for item in search_data.get("collection", []):
            user_info = item.get("user", {})
            track = {
                "id": item. get("id"),
                "title": item.get("title", "Unknown"),
                "duration": item.get("full_duration") or item.get("duration", 0),
                "permalink_url": item.get("permalink_url"),
                "artwork_url": item.get("artwork_url"),
                "artist": user_info.get("username", "Unknown"),
                "likes": item.get("likes_count", 0),
                "plays": item.get("playback_count", 0),
                "genre": item.get("genre", "Unknown"),
                "created": item.get("created_at", "")[:10]
            }
            tracks.append(track)
        
        if not tracks:
            await send_response(
                message,
                "SOUNDCLOUD NOT FOUND",
                f"😿 Không tìm thấy: `{query}`",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        lines = []
        for idx, track in enumerate(tracks, 1):
            duration_str = f"{int(track['duration'] / 1000)}s"
            lines.append(f"<b>{idx}. </b> 🎵 {escape_html(track['title'])}")
            lines.append(f"   👤 <i>{escape_html(track['artist'])}</i> | 🕒 {duration_str}")
            lines.append(f"   ❤️ {track['likes']:,} | 🎧 {track['plays']:,}")
            lines.append(f"   ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        
        content = "\n".join(lines)
        content += "\n\n💡 <b>Reply theo số thứ tự bài mày muốn! </b>"
        
        SEARCH_CONTEXT[message.message_id] = tracks
        CONTEXT_TIMESTAMP[message.message_id] = time.time()
        
        log_command(user_id, "/scl", query[:50])
        
        total_count = search_data.get("total_results", len(tracks))
        
        await send_response(
            message,
            f"Found {total_count} Results",
            content,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error searching SoundCloud: {e}")
        await send_response(
            message,
            "SOUNDCLOUD ERROR",
            f"❌ Lỗi API: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_soundcloud_download(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or "soundcloud. com" not in params[0]:
        await send_response(
            message,
            "SOUNDCLOUD DOWNLOAD HELP",
            "Cú pháp: `/scl_down <link_SoundCloud>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    url = params[0].strip()
    
    msg = await send_response(
        message,
        "SOUNDCLOUD DOWNLOAD",
        "🎶 Đang tải SoundCloud...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_SCL_DOWN. format(url=quote(url))
        )
        
        if not data. get("status"):
            await send_response(
                message,
                "SOUNDCLOUD ERROR",
                f"❌ {data.get('message', 'Không tải được')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        audio_url = data.get("data")
        
        if not isinstance(audio_url, str) or not audio_url.startswith(('http://', 'https://')):
            await send_response(
                message,
                "SOUNDCLOUD ERROR",
                "❌ URL không hợp lệ từ API",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/scl_down", url[:50])
        
        title = data.get("title", "Track")
        
        try:
            await bot_aiogram.send_audio(
                message. chat.id,
                audio_url,
                caption=f"✅ *Tải OK! *\n🎵 `{escape_markdown_v2(title[:50])}`",
                parse_mode="MarkdownV2"
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat.id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except Exception as e:
            await send_response(
                message,
                "SOUNDCLOUD DOWNLOAD",
                f"✅ Tải OK (LINK)\nLỗi gửi audio: {str(e)}\n🔗 Link: {audio_url}",
                processing_msg=msg,
                delete_user_msg=True
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Error downloading SoundCloud: {e}")
        await send_response(
            message,
            "SOUNDCLOUD ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_tiktok_download(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if not params or "tiktok. com" not in params[0]:
        await send_response(
            message,
            "TIKTOK DOWNLOAD HELP",
            "Cú pháp: `/tiktok <link_video>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    url = params[0].strip()
    
    msg = await send_response(
        message,
        "TIKTOK DOWNLOAD",
        "🎬 Đang tải video...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            "https://www.tikwm.com/api/",
            params={"url": url, "count": 12, "cursor": 0, "web": 1, "hd": 1},
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Referer': 'https://www.tikwm.com/',
            }
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "TIKTOK ERROR",
                f"❌ {data.get('message', 'Lỗi tải')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        video_data = data.get("data", {})
        video_url = video_data.get("play")
        
        if not video_url:
            await send_response(
                message,
                "TIKTOK ERROR",
                "❌ Không tìm thấy video URL",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        title = video_data.get("title", "TikTok Video")
        author = video_data.get("author", {}).get("nickname", "Unknown")
        views = video_data.get("play_count", 0)
        likes = video_data.get("digg_count", 0)
        comments = video_data.get("comment_count", 0)
        shares = video_data.get("share_count", 0)
        
        log_command(user_id, "/tiktok", url[:50])
        
        caption = (
            f"<blockquote>\n"
            f"🎬 <b>{escape_html(title)}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>{escape_html(author)}</b>\n"
            f"❤️ {likes:,} | 💬 {comments:,} | 🔗 {shares:,}\n"
            f"▶️ {views:,} views\n"
            f"</blockquote>"
        )
        
        try:
            await asyncio.wait_for(
                bot_aiogram.send_video(
                    message.chat.id,
                    video_url,
                    caption=caption,
                    parse_mode="HTML"
                ),
                timeout=60
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat. id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except asyncio.TimeoutError:
            await send_response(
                message,
                "TIKTOK DOWNLOAD",
                f"⚠️ Timeout khi tải video\n🔗 Link: {video_url}",
                processing_msg=msg,
                delete_user_msg=True
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Error downloading TikTok: {e}")
        await send_response(
            message,
            "TIKTOK ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_girl_image(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    msg = await send_response(
        message,
        "GIRL IMAGE",
        "🩷 Đang tìm ảnh.. .",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_ANH_GAI
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "IMAGE ERROR",
                f"❌ {data.get('message', 'Không tải được')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        image_url = data.get("data")
        
        if not image_url:
            await send_response(
                message,
                "IMAGE ERROR",
                "❌ Không có ảnh",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/anhgai", "random")
        
        try:
            await asyncio.wait_for(
                bot_aiogram.send_photo(
                    message.chat. id,
                    image_url,
                    caption="✨ Ảnh gái xinh"
                ),
                timeout=30
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat. id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except asyncio.TimeoutError:
            await send_response(
                message,
                "IMAGE TIMEOUT",
                f"⚠️ Timeout khi tải ảnh\n🔗 {image_url}",
                processing_msg=msg,
                delete_user_msg=True
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Error getting girl image: {e}")
        await send_response(
            message,
            "IMAGE ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_girl_video(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user. id
    
    msg = await send_response(
        message,
        "GIRL VIDEO",
        "🎬 Đang tìm video...",
        delete_user_msg=False
    )
    
    try:
        data = await asyncio.to_thread(
            get_api_result_sync,
            API_VD_GAI
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "VIDEO ERROR",
                f"❌ {data.get('message', 'Không tải được')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        video_url = data.get("data")
        
        if not video_url:
            await send_response(
                message,
                "VIDEO ERROR",
                "❌ Không có video",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/vdgai", "random")
        
        try:
            await asyncio.wait_for(
                bot_aiogram.send_video(
                    message.chat.id,
                    video_url,
                    caption="✨ Video gái xinh",
                    supports_streaming=True
                ),
                timeout=60
            )
            
            try:
                await bot_aiogram.delete_message(
                    chat_id=msg.chat. id,
                    message_id=msg.message_id
                )
            except Exception:
                pass
                
        except asyncio.TimeoutError:
            await send_response(
                message,
                "VIDEO TIMEOUT",
                f"⚠️ Timeout khi tải video\n🔗 {video_url}",
                processing_msg=msg,
                delete_user_msg=True
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Error getting girl video: {e}")
        await send_response(
            message,
            "VIDEO ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_ngl_spam(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    params = extract_params(message)
    
    if len(params) < 3:
        await send_response(
            message,
            "NGL SPAM HELP",
            "Cú pháp: `/ngl <username> <message> <số_lượng>`",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    username = params[0]. strip()
    msg_text = params[1].strip()
    
    try:
        amount = int(params[2])
        if not (1 <= amount <= 100):
            raise ValueError("Số lượng 1-100")
    except (ValueError, IndexError):
        await send_response(
            message,
            "NGL SPAM ERROR",
            "❌ Số lượng không hợp lệ (1-100)",
            delete_user_msg=True,
            auto_delete_after=8
        )
        return False
    
    msg = await send_response(
        message,
        "NGL SPAM",
        f"✉️ Đang spam NGL: `{username}`.. .",
        delete_user_msg=False
    )
    
    try:
        api_url = API_NGL_SPAM.format(
            username=username,
            message=quote(msg_text),
            amount=amount
        )
        
        data = await asyncio.to_thread(
            get_api_result_sync,
            api_url
        )
        
        if not data.get("status"):
            await send_response(
                message,
                "NGL SPAM ERROR",
                f"❌ {data.get('message', 'Thất bại')}",
                processing_msg=msg,
                delete_user_msg=True
            )
            return False
        
        log_command(user_id, "/ngl", f"{username} - {amount}")
        
        content = (
            f"✅ Spam hoàn tất!\n"
            f"👤 Username: `{username}`\n"
            f"✉️ Tin nhắn gửi: {data.get('success', 0)}\n"
            f"❌ Lỗi: {data.get('failed', 0)}"
        )
        
        await send_response(
            message,
            "NGL SPAM RESULT",
            content,
            processing_msg=msg,
            delete_user_msg=True,
            keep_forever=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error NGL spam: {e}")
        await send_response(
            message,
            "NGL SPAM ERROR",
            f"❌ Lỗi: {str(e)}",
            processing_msg=msg,
            delete_user_msg=True
        )
        return False

async def handle_donate(message: Message):
    if not message.from_user:
        return False
    
    user = message.from_user
    user_id = user.id
    
    caption = (
        f"💝 Cảm ơn bạn đã ủng hộ Bot!\n\n"
        f"Giúp duy trì và nâng cấp các API.\n\n"
        f"*[Bấm để xem mã QR]({QR_CODE_IMAGE_URL})*"
    )
    
    try:
        await bot_aiogram.send_photo(
            message.chat.id,
            QR_CODE_IMAGE_URL,
            caption=caption,
            parse_mode="Markdown"
        )
    except Exception as e:
        await send_response(
            message,
            "DONATE",
            caption,
            delete_user_msg=False
        )
    
    log_command(user_id, "/donate", "qr_request")
    return True

def get_client_id():
    try:
        response = requests.get(
            "https://soundcloud.com/",
            headers=SC_HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        
        script_tags = re.findall(
            r'<script crossorigin src="([^"]+)"',
            response.text
        )
        script_urls = [
            url for url in script_tags
            if url.startswith("https")
        ]
        
        if not script_urls:
            return 'W00nmY7TLer3uyoEo1sWK3Hhke5Ahdl9'
        
        script_response = requests.get(
            script_urls[-1],
            headers=SC_HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        script_response.raise_for_status()
        
        client_id_match = re.search(
            r',client_id:"([^"]+)"',
            script_response.text
        )
        
        if not client_id_match:
            return 'W00nmY7TLer3uyoEo1sWK3Hhke5Ahdl9'
        
        return client_id_match.group(1)
    
    except Exception:
        return 'W00nmY7TLer3uyoEo1sWK3Hhke5Ahdl9'

def update_router_handlers(router: Router) -> Router:
    router.message. register(handle_soundcloud_search, Command("scl"))
    router.message. register(handle_soundcloud_download, Command("scl_down"))
    router.message. register(handle_tiktok_download, Command("tiktok"))
    router.message.register(handle_girl_image, Command("anhgai"))
    router.message.register(handle_girl_video, Command("vdgai"))
    router.message.register(handle_ngl_spam, Command("ngl"))
    router. message.register(handle_donate, Command("donate"))
    
    return router

async def main():
    logger.info(f"🚀 Bot Premium VIP (@{OWNER_USERNAME}) đang khởi động...")
    
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'w', encoding='utf-8') as f:
                f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Log created.\n")
    except IOError as e:
        logger. critical(f"❌ Không thể tạo log file: {e}")
        return
    
    await setup_database()
    
    try:
        if AIOGRAM_AVAILABLE and globals().get('bot_aiogram') is not None:
        await bot_aiogram.delete_webhook()
    else:
        logger.debug('Skipped delete_webhook (aiogram not available)')
        logger.info("✅ Xóa Webhook cũ OK")
    except Exception as e:
        logger.warning(f"⚠️ Không thể xóa Webhook: {e}")
    
    try:
        commands = [
            BotCommand(command="start", description="📋 Menu chính"),
            BotCommand(command="ping", description="🤖 Kiểm tra trạng thái"),
            BotCommand(command="checkid", description="🆔 Xem ID"),
            BotCommand(command="ask", description="🤖 Hỏi AI"),
            BotCommand(command="tx", description="🔮 Giải mã MD5"),
            BotCommand(command="qrcode", description="🔳 Tạo QR"),
            BotCommand(command="voice", description="🗣️ Text-to-Speech"),
            BotCommand(command="weather", description="🌤️ Dự báo thời tiết"),
            BotCommand(command="kqxs", description="🎟️ Kết quả XSMB"),
            BotCommand(command="ip", description="🌐 Tra cứu IP"),
            BotCommand(command="fb", description="👤 Info Facebook"),
            BotCommand(command="tt", description="🎵 Info TikTok"),
            BotCommand(command="scl", description="🎶 Tìm nhạc SoundCloud"),
            BotCommand(command="tiktok", description="🎬 Tải video TikTok"),
            BotCommand(command="anhgai", description="🖼️ Ảnh gái xinh"),
            BotCommand(command="vdgai", description="🎬 Video gái xinh"),
            BotCommand(command="ngl", description="✉️ Spam NGL"),
            BotCommand(command="donate", description="💖 Ủng hộ Bot"),
            BotCommand(command="nap", description="💳 Hướng dẫn nạp"),
            BotCommand(command="vip", description="🔥 VIP Commands"),
            BotCommand(command="call", description="📞 Gọi điện"),
            BotCommand(command="kill", description="🛑 Dừng lệnh"),
            BotCommand(command="themvip", description="➕ Thêm VIP"),
            BotCommand(command="xoavip", description="➖ Xóa VIP"),
            BotCommand(command="listvip", description="📋 Danh sách"),
        ]
        
        if AIOGRAM_AVAILABLE and globals().get('bot_aiogram') is not None:
        await bot_aiogram.set_my_commands(commands)
    else:
        logger.debug('Skipped set_my_commands (aiogram not available)')
        logger.info("✅ Menu lệnh đã được cài đặt")
    except Exception as e:
        logger. warning(f"⚠️ Không thể cài Menu lệnh: {e}")
    
    dp = Dispatcher()
    router = create_router()
    router = update_router_handlers(router)
    dp.include_router(router)
    
    cleanup_task = asyncio.create_task(periodic_cleanup())
    
    try:
        bot_info = await asyncio.wait_for(
            bot_aiogram.get_me(),
            timeout=30
        )
        logger.info(f"✅ Bot kết nối thành công: @{bot_info.username}")
    except Exception as e:
        logger.critical(f"❌ Không thể kết nối tới Telegram: {e}")
        cleanup_task.cancel()
        return
    
    logger.info("🔄 Bắt đầu polling...")
    
    try:
        if AIOGRAM_AVAILABLE and globals().get('dp') is not None:
        await dp.start_polling(bot_aiogram)
    else:
        logger.info('Aiogram Dispatcher not available; skipping polling.')
    finally:
        if cleanup_task and not cleanup_task.done():
            cleanup_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("✅ Bot dừng thành công")

def run_bot():
    import signal
    
    def signal_handler(signum, frame):
        signal_name = {
            signal.SIGINT: "SIGINT (Ctrl+C)",
            signal.SIGTERM: "SIGTERM (Kill)"
        }. get(signum, f"Signal {signum}")
        
        logger.info(f"🛑 Nhận {signal_name}, đang dừng bot...")
        try:
            kill_processes_sync("python.*lenh")
        except Exception as e:
            logger.error(f"Lỗi cleanup: {e}")
        exit(0)
    
    for sig in [signal.SIGINT, signal. SIGTERM]:
        signal. signal(sig, signal_handler)
    
    max_retries = 10
    restart_count = 0
    start_time = time.time()
    
    logger.info("🤖 Bot hệ thống đang khởi động...")
    
    while restart_count < max_retries:
        bot_start_time = time.time()
        
        try:
            if os.name == 'nt':
                asyncio.set_event_loop_policy(
                    asyncio.WindowsProactorEventLoopPolicy()
                )
            
            asyncio. run(main())
            logger.info("✅ Bot kết thúc bình thường")
            break
        
        except KeyboardInterrupt:
            logger. info("⏹️ Bot bị dừng bởi người dùng")
            break
        
        except Exception as e:
            runtime = time.time() - bot_start_time
            total_runtime = time.time() - start_time
            
            logger.error(
                f"💥 Bot crash sau {runtime:.1f}s (tổng: {total_runtime/3600:.1f}h): {e}"
            )
            restart_count += 1
            
            try:
                kill_processes_sync("python.*lenh")
            except Exception as cleanup_error:
                logger.error(f"Lỗi cleanup: {cleanup_error}")
            
            if restart_count < max_retries:
                wait_time = min(30, restart_count * 5)
                logger.info(
                    f"⏳ Chờ {wait_time}s trước khi restart "
                    f"(lần {restart_count}/{max_retries})"
                )
                time.sleep(wait_time)
            else:
                logger.error("❌ Đã đạt giới hạn restart, dừng bot")
                break
    
    total_runtime = time.time() - start_time
    logger.info(
        f"🏁 Bot dừng hoàn toàn sau {total_runtime/3600:.1f} giờ"
    )

async def handle_call(message, *args, **kwargs):
    logger.warning('Stub handler handle_call called - not implemented')
    return False

async def handle_checkid(message, *args, **kwargs):
    logger.warning('Stub handler handle_checkid called - not implemented')
    return False

async def handle_kill_all_processes(message, *args, **kwargs):
    logger.warning('Stub handler handle_kill_all_processes called - not implemented')
    return False

async def handle_kill_process(message, *args, **kwargs):
    logger.warning('Stub handler handle_kill_process called - not implemented')
    return False

async def handle_non_command_message(message, *args, **kwargs):
    logger.warning('Stub handler handle_non_command_message called - not implemented')
    return False

async def handle_random_image(message, *args, **kwargs):
    logger.warning('Stub handler handle_random_image called - not implemented')
    return False

async def handle_random_video(message, *args, **kwargs):
    logger.warning('Stub handler handle_random_video called - not implemented')
    return False

async def handle_vip(message, *args, **kwargs):
    logger.warning('Stub handler handle_vip called - not implemented')
    return False
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info(f"BOT PREMIUM VIP - @{OWNER_USERNAME}")
    logger.info(f"Phiên bản: 2.0 PRODUCTION")
    logger.info(f"Thời gian khởi động: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    if not os.path.exists(LOCAL_VIDEO_PATH):
        logger.warning(
            f"⚠️ File video '{LOCAL_VIDEO_PATH}' không tồn tại.  "
            "Bot sẽ gửi text thay thế."
        )
    
    try:
        run_bot()
    except KeyboardInterrupt:
        logger.info("⏹️ Bot dừng bởi Ctrl+C")
    except Exception as e:
        logger.critical(f"❌ CRITICAL ERROR: {e}", exc_info=True)
# === END FILE: bsfix.py ===

# === BEGIN FILE: ant.py ===
import asyncio
import os
import sys
import json
import re
import time
import uuid
import logging
import random
import string
import sqlite3
import hashlib
import hmac
import base64
import threading
import traceback
import platform
import html
import io
import tempfile
import pathlib
import secrets
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Tuple
from urllib.parse import unquote, urlparse, parse_qs, quote
from threading import Lock, Thread
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from enum import Enum

import requests
import aiohttp
from PIL import Image, ImageEnhance, ImageFilter, ImageOps
from flask import Flask
import telebot
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from aiohttp import web
import google.generativeai as genai

try:
    from moviepy.editor import VideoFileClip
except ImportError:
    VideoFileClip = None

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, ForeignKey, BigInteger, Float, JSON, Enum as SQLEnum
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, scoped_session
from sqlalchemy.sql import func

try:
    from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError as PWTimeoutError, Error as PWError
except ImportError:
    async_playwright = None

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class Config:
    BOT_TOKEN = "8413179871:AAHIHWYvoHM4an3XdPXobbl6Bmn2GGGwFtg"
    ADMIN_IDS = [123456789, 7679054753]
    DB_FILE = "bot_database.db"
    LOG_FILE = "bot_activity.log"
    GEMINI_API_KEY = "AIzaSyAWp3AxiFF5OL1rFD_3WmdTe3lMRPgEWVw"
    TOMORROW_API_KEY = "mdTWQAInBIDB3mHiDtkwuTlwhVB50rqn"
    OPENWEATHER_API_KEY = "e707d13f116e5f7ac80bd21c37883e5e"
    WEATHERAPI_KEY = "fe221e3a25734f0297994922240611"
    ZING_API_KEY = "X5BM3w8N7MKozC0B85o4KMlzLZKhV00y"
    ZING_SECRET_KEY = "acOrvUS15XRW2o9JksiK1KgQ6Vbds8ZW"
    ZING_VERSION = "1.11.11"
    ZING_URL = "https://zingmp3.vn"
    TIKWM_API = "https://www.tikwm.com/api"
    SOUNDCLOUD_API = "https://api-v2.soundcloud.com"
    WEB_HOST = "0.0.0.0"
    WEB_PORT = 8080
    CACHE_TTL = 3600
    CACHE_MAX_SIZE = 256
    BROWSER_VIEWPORT = {"width": 1920, "height": 1080}
    BROWSER_TIMEOUT = 60000
    HEADLESS_MODE = True

class Icons:
    CROWN = "👑"
    VERIFY = "💠"
    LOCK = "🔒"
    GLOBE = "🌏"
    CHART = "📈"
    SHOP = "🛍️"
    FIRE = "🔥"
    STAR = "⭐"
    GHOST = "👻"
    EYE = "👁️"
    BOX = "📦"
    PIN = "📌"
    BULB = "💡"
    WARN = "⚠️"
    CHECK = "☑️"
    ID = "🆔"
    USER = "👤"
    TIME = "⏳"
    LINK = "🔗"
    MAIL = "📧"
    PHONE = "📞"
    MUSIC = "🎵"
    VID = "🎥"
    ROCKET = "🚀"
    COIN = "💰"
    SETTINGS = "⚙️"
    SHIELD = "🛡️"
    STAT = "📊"
    ERROR = "❌"
    SUCCESS = "✅"
    CLOCK = "⏰"
    HEART = "❤️"
    LIKE = "👍"
    SHARE = "🔄"
    DOWNLOAD = "⬇️"
    UPLOAD = "⬆️"

class ActionType(Enum):
    VIEW = "view"
    LIKE = "like"
    FOLLOW = "follow"
    SHARE = "share"
    COMMENT = "comment"

class UserRole(Enum):
    USER = "user"
    VIP = "vip"
    PREMIUM = "premium"
    ADMIN = "admin"

BANK_CODES = {
    "vcb": {"bin": "970436", "name": "VIETCOMBANK", "short": "Vietcombank"},
    "tcb": {"bin": "970407", "name": "TECHCOMBANK", "short": "Techcombank"},
    "mb": {"bin": "970422", "name": "MB BANK", "short": "MBBank"},
    "acb": {"bin": "970416", "name": "ACB", "short": "ACB"},
    "vib": {"bin": "970441", "name": "VIB", "short": "VIB"},
    "bidv": {"bin": "970418", "name": "BIDV", "short": "BIDV"},
}

WEATHER_CODES = {
    1000: "Quang đãng", 1100: "Có mây nhẹ", 2000: "Sương mù", 4000: "Mưa nhỏ",
    5000: "Tuyết", 7000: "Sấm sét", 1101: "Có mây", 1102: "Nhiều mây",
    2100: "Sương mù nhẹ", 4001: "Mưa", 4200: "Mưa nhẹ", 4201: "Mưa vừa",
}

TRIGGERS_MUSIC = ["nhạc", "nhac", "music", "play", "nghe", "song", "bài hát", "bai hat", "track", "sound", "scl", "mp3", "tìm bài", "tim bai", "audio"]
TRIGGERS_VOICE = ["tách", "tach", "lấy nhạc", "lay nhac", "crvoice", "voice", "âm thanh", "am thanh", "convert", "chuyển đổi", "chuyen doi", "mp3", "audio"]
TRIGGERS_TIKTOK = ["tiktok", "tt", "douyin", "video", "vid", "clip", "xem"]

UA_WINDOWS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

UA_MAC = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

class TTLCache:
    def __init__(self, ttl_sec=600, max_size=256):
        self.ttl = ttl_sec
        self. max = max_size
        self.data = {}
        self.lock = threading.Lock()
    
    def get(self, key):
        with self.lock:
            if key not in self.data:
                return None
            val, exp = self.data[key]
            if exp < time.time():
                self.data.pop(key, None)
                return None
            return val
    
    def set(self, key, val):
        with self.lock:
            if len(self.data) >= self.max:
                self.data.pop(next(iter(self. data.keys())), None)
            self.data[key] = (val, time.time() + self.ttl)
    
    def clear(self):
        with self.lock:
            self.data.clear()

class Database:
    def __init__(self):
        self.db_file = Config.DB_FILE
        self. lock = Lock()
        self._init_tables()
    
    def _init_tables(self):
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                role TEXT DEFAULT 'user',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_banned BOOLEAN DEFAULT 0,
                total_requests INTEGER DEFAULT 0,
                total_spent REAL DEFAULT 0,
                vip_until TIMESTAMP
            )''')
            cursor. execute('''CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                tiktok_id TEXT,
                username TEXT,
                title TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                current_views INTEGER DEFAULT 0,
                target_views INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                FOREIGN KEY(created_by) REFERENCES users(user_id)
            )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS actions (
                action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                video_id TEXT,
                action_type TEXT,
                amount INTEGER,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )''')
            cursor. execute('''CREATE TABLE IF NOT EXISTS analytics (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT,
                details TEXT,
                ip_address TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS spam_log (
                spam_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                reason TEXT,
                severity TEXT,
                action TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )''')
            conn.commit()
    
    def add_or_update_user(self, user_id:  int, username: str, role:  str = "user"):
        with self.lock:
            try:
                with sqlite3.connect(self.db_file) as conn:
                    cursor = conn.cursor()
                    cursor. execute('SELECT * FROM users WHERE user_id = ? ', (user_id,))
                    if cursor.fetchone():
                        cursor.execute('UPDATE users SET username = ? WHERE user_id = ?', (username, user_id))
                    else:
                        cursor.execute('INSERT INTO users (user_id, username, role) VALUES (?, ?, ?)', (user_id, username, role))
                    conn.commit()
                    return True
            except Exception as e:
                logger.error(f"Error: {e}")
                return False
    
    def is_user_banned(self, user_id: int) -> bool:
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT is_banned FROM users WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            return result[0] if result else False
    
    def get_user_role(self, user_id: int) -> str:
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT role FROM users WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            return result[0] if result else "user"
    
    def log_action(self, user_id: int, action:  str, details: str):
        with self.lock:
            try:
                with sqlite3.connect(self.db_file) as conn:
                    cursor = conn.cursor()
                    cursor.execute('INSERT INTO analytics (user_id, action, details) VALUES (?, ?, ?)',
                                 (user_id, action, details))
                    conn.commit()
                    return True
            except: 
                return False
    
    def detect_spam(self, user_id: int) -> Tuple[bool, str]:
        with sqlite3.connect(self. db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT total_requests FROM users WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            if result and result[0] > 100:
                return True, "Quá nhiều yêu cầu"
            
            cursor.execute('SELECT COUNT(*) FROM actions WHERE user_id = ? AND created_at > datetime("now", "-1 hour") AND status = "pending"', (user_id,))
            recent_actions = cursor.fetchone()[0]
            if recent_actions > 50:
                return True, "Quá nhiều hành động trong 1 giờ"
            
            return False, ""
    
    def log_spam(self, user_id: int, reason: str, severity: str, action: str):
        with self.lock:
            try:
                with sqlite3.connect(self.db_file) as conn:
                    cursor = conn.cursor()
                    cursor.execute('INSERT INTO spam_log (user_id, reason, severity, action) VALUES (?, ?, ?, ?)',
                                 (user_id, reason, severity, action))
                    conn.commit()
                    return True
            except: 
                return False
    
    def get_user_stats(self, user_id: int) -> Dict:
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT total_requests, total_spent, created_at FROM users WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            if result: 
                return {"requests": result[0], "spent":  result[1], "joined": result[2]}
            return {"requests": 0, "spent": 0, "joined": ""}

class Utils:
    @staticmethod
    def fmt(num):
        if num is None:
            return "0"
        try:
            n = int(num)
            if n >= 1_000_000_000:
                return f"{n/1_000_000_000:. 2f}B"
            elif n >= 1_000_000:
                return f"{n/1_000_000:.2f}M"
            elif n >= 1_000:
                return f"{n/1_000:.2f}K"
            return str(n)
        except:
            return str(num)
    
    @staticmethod
    def tags(text):
        if not text:
            return []
        return re.findall(r"(@[a-zA-Z0-9_\.]+)", text)
    
    @staticmethod
    def emails(text):
        if not text:
            return []
        return re.findall(r"[\w\.-]+@[\w\.-]+", text)
    
    @staticmethod
    def phones(text):
        if not text:
            return []
        return re.findall(r"(0\d{9,10})", text)
    
    @staticmethod
    def escape_html(text):
        return html.escape(str(text or ""), quote=False)

class Chronos:
    @staticmethod
    def now():
        return datetime.now().strftime("%H:%M:%S - %d/%m/%Y")
    
    @staticmethod
    def age(uid):
        try:
            binary = "{0:b}".format(int(uid))
            timestamp = int(binary[:31], 2)
            c_date = datetime.fromtimestamp(timestamp)
            now = datetime.now()
            delta = now - c_date
            return c_date.strftime("%d/%m/%Y"), f"{delta.days} ngày"
        except:
            return "N/A", "N/A"

class Network:
    def __init__(self):
        self.session = requests.Session()
        self.api = "https://www.tikwm.com/api/user/info"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json"
        }
    
    def fetch(self, query):
        params = {"unique_id": query}
        for _ in range(3):
            try:
                res = self.session.get(self.api, headers=self.headers, params=params, timeout=15)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("code") == 0:
                        return data
            except:
                time.sleep(1)
        return None

class Analyst:
    def __init__(self, user, stats):
        self.u = user
        self.s = stats
        self.flw = int(stats.get("followerCount", 0))
        self.hrt = int(stats.get("heartCount", 0))
        self.vid = int(stats.get("videoCount", 0))
    
    def health(self):
        score = 100
        if self.vid > 0:
            avg = self.hrt / self.vid
            er = (avg / self.flw * 100) if self.flw > 0 else 0
        else:
            avg, er = 0, 0
        
        if self.vid > 5 and avg < 10:
            score -= 20
        if self.flw > 10000 and er < 0.5:
            score -= 30
        if self.vid > 20 and self.hrt < 100:
            score -= 40
        if not self.u.get("verified"):
            score -= 5
        if not self.u.get("signature"):
            score -= 5
        
        if score >= 90:
            rank = "S (Huyền Thoại)"
        elif score >= 70:
            rank = "A (Xuất Sắc)"
        elif score >= 50:
            rank = "B (Ổn Định)"
        else:
            rank = "C (Cần Tối Ưu)"
        
        return {"score": max(0, score), "rank": rank, "er": er, "avg": avg}
    
    def content_type(self):
        bio = self.u.get("signature", "").lower()
        nick = self.u.get("nickname", "").lower()
        full_text = bio + " " + nick
        
        if any(k in full_text for k in ["shop", "sỉ", "lẻ", "order", "mua", "bán", "store"]):
            return "Kinh Doanh / Bán Hàng"
        if any(k in full_text for k in ["game", "liên quân", "pubg", "free fire", "gaming"]):
            return "Gaming / Streamer"
        if any(k in full_text for k in ["vlog", "daily", "cuộc sống", "travel"]):
            return "Vlog / Đời Sống"
        if any(k in full_text for k in ["review", "đánh giá", "food", "ăn"]):
            return "Reviewer / Ẩm Thực"
        if any(k in full_text for k in ["nhảy", "dance", "music", "nhạc"]):
            return "Nghệ Thuật / Giải Trí"
        if any(k in full_text for k in ["share", "tips", "hướng dẫn", "học"]):
            return "Giáo Dục / Chia Sẻ"
        if any(k in full_text for k in ["edit", "video", "design"]):
            return "Editor / Creator"
        
        if self.flw > 100000:
            return "Người Nổi Tiếng (KOL)"
        if self.vid > 0:
            return "Sáng Tạo Nội Dung"
        return "Người Dùng Cá Nhân"
    
    def commerce(self):
        c_info = self.u.get("commerceUserInfo", {})
        is_shop = c_info.get("commerceUser", False)
        ads = "Có" if self.u.get("verified") or is_shop or self.flw > 10000 else "Không"
        return {"shop": is_shop, "ads":  ads}

class Interface:
    def __init__(self, data):
        self.u = data['user']
        self.s = data['stats']
        self. h = data['health']
        self.c = data['commerce']
        self.cat = data['category']
    
    def _row(self, icon, label, value):
        return f"{icon} {label}: <code>{value}</code>"
    
    def render(self):
        su = Utils
        c_date, c_age = Chronos.age(self.u.get("id"))
        verified = "Đã xác minh" if self.u.get("verified") else "Chưa xác minh"
        privacy = "Riêng tư" if self.u.get("secret") else "Công khai"
        
        bio = self.u.get("signature", "")
        tags = su.tags(bio)
        mails = su.emails(bio)
        phones = su.phones(bio)
        
        contact_info = ""
        if mails:
            contact_info += f"\n{Icons.MAIL} Email: <code>{', '.join(mails)}</code>"
        if phones:
            contact_info += f"\n{Icons.PHONE} SĐT: <code>{', '. join(phones)}</code>"
        
        report = f"""<b>{Icons.CROWN} HỒ SƠ PHÂN TÍCH TOÀN DIỆN {Icons.CROWN}</b>

<blockquote><b>{Icons.USER} ĐỊNH DANH KÊNH</b>
{self._row(Icons.ID, "Họ Tên", self.u.get("nickname"))}
{self._row(Icons.VERIFY, "ID Gốc", self.u.get("id"))}
{self._row(Icons. PIN, "Username", f"@{self.u.get('uniqueId')}")}
{self._row(Icons.GLOBE, "Khu Vực", self.u.get("region", "VN"))}
{self._row(Icons.CHECK, "Tích Xanh", verified)}
{self._row(Icons.LOCK, "Quyền", privacy)}
{self._row(Icons.TIME, "Tuổi Kênh", c_age)}
</blockquote>

<blockquote><b>{Icons. BULB} PHÂN TÍCH NỘI DUNG</b>
{self._row(Icons.STAR, "Chủ Đề", self.cat)}
{self._row(Icons.VID, "Video", su.fmt(self.s.get('videoCount')))}
{self._row(Icons.EYE, "View/Vid", su.fmt(self.h['avg']))}
{self._row(Icons.CHART, "Tương Tác", f"{self.h['er']:.2f}%")}
{self._row(Icons.FIRE, "Điểm Số", f"{self.h['score']}/100")}
{self._row(Icons.CROWN, "Xếp Hạng", self.h['rank'])}
</blockquote>

<blockquote><b>{Icons.CHART} CHỈ SỐ TĂNG TRƯỞNG</b>
{self._row(Icons.USER, "Followers", su.fmt(self.s.get('followerCount')))}
{self._row(Icons.GHOST, "Following", su.fmt(self.s.get('followingCount')))}
{self._row(Icons.FIRE, "Tổng Tim", su.fmt(self.s.get('heartCount')))}
{self._row(Icons.BOX, "Bạn Bè", su.fmt(self. s.get('friendCount')))}
{self._row(Icons.STAR, "Đã Thích", su.fmt(self.s.get('diggCount')))}
</blockquote>

<blockquote><b>{Icons.SHOP} THƯƠNG MẠI & SETTING</b>
{self._row(Icons.SHOP, "TikTok Shop", "Đang Bật" if self.c['shop'] else "Chưa Có")}
{self._row(Icons.BOX, "Giỏ Hàng", "Hiển Thị" if self.c['shop'] else "Ẩn")}
{self._row(Icons.STAR, "Chạy Ads", self.c['ads'])}
{self._row(Icons.LOCK, "Tải Video", "Cho Phép")}
{self._row(Icons.MUSIC, "Duet/Stitch", "Cho Phép")}
</blockquote>

<blockquote><b>{Icons.PIN} TIỂU SỬ & LIÊN HỆ</b>
<i>{html.escape(bio) if bio else "Chưa cập nhật tiểu sử."}</i>
{contact_info}
{self._row(Icons.USER, "Tags", ", ".join(tags) if tags else "Không")}
{self._row(Icons.LINK, "Link Bio", self.u.get("bioLink", {}).get("link", "Không"))}
</blockquote>

<i>{Chronos.now()} | 🚀 Powered by Omni-AI VIP</i>
"""
        return report

class SoundCloudClient:
    def __init__(self):
        self.session = requests.Session()
        self.client_id = self._get_client_id()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'vi-VN,vi;q=0.9',
        }
    
    def _get_client_id(self) -> str:
        try:
            resp = requests.get("https://soundcloud.com/", timeout=15)
            match = re.search(r'client_id["\']?\s*[:=]\s*["\']([^"\']+)', resp.text)
            if match:
                return match.group(1)
        except:
            pass
        return 'W00nmY7TLer3uyoEo1sWK3Hhke5Ahdl9'
    
    def search_tracks(self, query: str, limit: int = 10) -> List[Dict]:
        try:
            params = {
                'q': query,
                'client_id': self.client_id,
                'limit': limit,
                'offset': 0,
                'app_locale': 'en'
            }
            
            resp = self.session.get(
                f"{Config.SOUNDCLOUD_API}/search/tracks",
                params=params,
                headers=self.headers,
                timeout=15
            )
            resp.raise_for_status()
            
            tracks = []
            for item in resp.json().get('collection', []):
                user = item.get('user', {})
                tracks.append({
                    'id': item. get('id'),
                    'title': item.get('title', ''),
                    'duration': item.get('duration', 0),
                    'url': item.get('permalink_url'),
                    'artwork': item.get('artwork_url'),
                    'artist': user.get('username', 'Unknown'),
                    'plays': item.get('playback_count', 0),
                    'likes': item.get('likes_count', 0),
                })
            
            return tracks
        except: 
            return []
    
    def get_download_url(self, track_id: int) -> Optional[str]:
        try: 
            params = {'client_id': self.client_id}
            resp = self.session.get(
                f"{Config.SOUNDCLOUD_API}/tracks/{track_id}/streams",
                params=params,
                headers=self.headers,
                timeout=15
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get('http_mp3_128_url')
        except:
            return None

class ZingMP3Client:
    def __init__(self):
        self.api_key = Config. ZING_API_KEY
        self.secret_key = Config.ZING_SECRET_KEY
        self.api_url = Config.ZING_URL
        self.session = requests.Session()
    
    def _get_hash256(self, string: str) -> str:
        return hashlib.sha256(string. encode()).hexdigest()
    
    def _get_hmac512(self, string: str) -> str:
        return hmac.new(self.secret_key. encode(), string.encode(), hashlib.sha512).hexdigest()
    
    def _get_sig(self, path: str, params: Dict) -> str:
        param_str = ''.join(f"{k}={params[k]}" for k in sorted(params.keys()))
        return self._get_hmac512(path + self._get_hash256(param_str))
    
    def search_music(self, keyword: str) -> List[Dict]:
        try:
            ctime = str(int(time.time()))
            path = "/api/v2/search"
            params = {
                "q": keyword,
                "type": "song",
                "count": 10,
                "ctime": ctime,
                "version": Config.ZING_VERSION,
                "apiKey": self.api_key,
                "sig": self._get_sig(path, {"ctime": ctime, "type": "song", "count": 10})
            }
            
            resp = self.session.get(f"{self.api_url}{path}", params=params, timeout=15)
            resp.raise_for_status()
            
            tracks = []
            for item in resp.json().get('data', {}).get('songs', []):
                artists = item.get('artists', [])
                artist_name = ', '.join([a.get('name', '') for a in artists])
                
                tracks.append({
                    'id': item.get('encodeId'),
                    'title':  item.get('title', ''),
                    'artist': artist_name,
                    'duration':  item.get('duration', 0),
                    'thumbnail': item.get('thumbnail', ''),
                })
            
            return tracks
        except:
            return []

class TikTokDownloader:
    def __init__(self):
        self.api_url = Config.TIKWM_API
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.tikwm.com/',
            'Origin': 'https://www.tikwm.com',
        }
    
    def search_videos(self, keyword: str, limit: int = 12) -> Optional[Dict]:
        try:
            data = {
                'keywords': keyword,
                'count': limit,
                'cursor': 0,
                'web': 1,
                'hd': 1
            }
            
            for attempt in range(3):
                resp = self.session.post(
                    f"{self. api_url}/feed/search",
                    data=data,
                    headers=self.headers,
                    timeout=15
                )
                
                if resp.status_code == 200:
                    result = resp.json()
                    if result.get('code') == 0:
                        videos = result.get('data', {}).get('videos', [])
                        if videos:
                            return random.choice(videos)
                
                time.sleep(1)
            
            return None
        except: 
            return None
    
    def get_video_info(self, url: str) -> Optional[Dict]:
        try:
            data = {'url': url, 'count': 1, 'cursor': 0, 'web': 1, 'hd': 1}
            
            resp = self.session.post(
                f"{self.api_url}/",
                data=data,
                headers=self.headers,
                timeout=15
            )
            
            if resp.status_code == 200:
                result = resp.json()
                if result.get('code') == 0:
                    return result.get('data', {})
            
            return None
        except:
            return None
    
    def download_video(self, video_url: str) -> Optional[str]:
        filename = f"tiktok_{int(time.time())}_{random.randint(100,999)}.mp4"
        
        try:
            resp = requests.get(
                video_url,
                stream=True,
                headers=self.headers,
                timeout=60
            )
            resp.raise_for_status()
            
            with open(filename, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size_mb = os.path.getsize(filename) / (1024 * 1024)
            
            if file_size_mb > 49.5:
                os.remove(filename)
                return "TOO_LARGE"
            
            if file_size_mb < 0.01:
                os.remove(filename)
                return None
            
            return filename
        except:
            if os.path.exists(filename):
                os.remove(filename)
            return None

class TikTokAnalyzer:
    def __init__(self):
        self.api_url = "https://www.tikwm.com/api/user/info"
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
    
    def get_user_info(self, username: str) -> Optional[Dict]:
        try:
            params = {"unique_id": username}
            
            for attempt in range(3):
                resp = self.session.get(
                    self.api_url,
                    headers=self.headers,
                    params=params,
                    timeout=15
                )
                
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('code') == 0:
                        return data.get('data', {})
                
                time.sleep(1)
            
            return None
        except:
            return None

class UltimateBot:
    def __init__(self):
        self.bot = telebot.TeleBot(Config.BOT_TOKEN, parse_mode="HTML")
        self.async_bot = AsyncTeleBot(Config.BOT_TOKEN, parse_mode="HTML")
        self.db = Database()
        self.tiktok_dl = TikTokDownloader()
        self.tiktok_analyzer = TikTokAnalyzer()
        self.sc_client = SoundCloudClient()
        self.zing_client = ZingMP3Client()
        self.net = Network()
        self.user_states = {}
        self.search_cache = TTLCache(ttl_sec=300, max_size=256)
        self.app = Flask(__name__)
        
        self._setup_handlers()
        self._setup_routes()
    
    def _setup_handlers(self):
        self.bot.message_handler(commands=['start', 'help'])(self.cmd_start)
        self.bot.message_handler(commands=['tiktok', 'tt'])(self.cmd_tiktok)
        self.bot.message_handler(commands=['music', 'scl'])(self.cmd_music)
        self.bot.message_handler(commands=['weather'])(self.cmd_weather)
        self.bot.message_handler(commands=['github'])(self.cmd_github)
        self.bot.message_handler(commands=['idfb'])(self.cmd_idfb)
        self.bot.message_handler(commands=['qrbank'])(self.cmd_qrbank)
        self.bot.message_handler(func=lambda m: True)(self.handle_message)
        self.bot.callback_query_handler(func=lambda c: True)(self.handle_callback)
    
    def _setup_routes(self):
        @self.app.route('/')
        def health():
            return {
                "status": "🟢 ONLINE",
                "service": "Ultimate Bot System",
                "version": "1.0.0",
                "timestamp": datetime.now().isoformat()
            }, 200
        
        @self.app.route('/stats')
        def stats():
            return {
                "users": "N/A",
                "uptime": "Running",
                "features": ["TikTok", "Music", "Weather", "GitHub", "QR Bank"]
            }, 200
    
    def cmd_start(self, message):
        user = message.from_user
        self.db.add_or_update_user(user.id, user.username or "NoUsername")
        
        is_admin = user.id in Config.ADMIN_IDS
        
        menu_text = f"""
<blockquote>
<b>{Icons.CROWN} ULTIMATE BOT SYSTEM {Icons.CROWN}</b>

<b>✨ CHỨC NĂNG CHÍNH:</b>
{Icons.VID} TikTok: Tải video, phân tích, tăng view
{Icons.MUSIC} Music: Tìm kiếm nhạc SoundCloud, ZingMP3
{Icons.ROCKET} Tools: Weather, GitHub, QR Bank, AI
{Icons.CHART} Analytics: Thống kê người dùng

<b>📝 HƯỚNG DẪN:</b>
• <code>/tiktok [url/keyword]</code> - Tải video TikTok
• <code>/music [song]</code> - Tìm nhạc
• <code>/weather [city]</code> - Thời tiết
• <code>/github [user]</code> - Info GitHub
• <code>/idfb [link]</code> - Lấy UID Facebook
• <code>/qrbank [stk] [bank]</code> - QR chuyển khoản
• Gửi link TikTok để auto-download

<b>👤 Thông tin:</b>
ID: <code>{user.id}</code>
Username: <code>{user. username or 'N/A'}</code>
Role: <code>{'ADMIN' if is_admin else 'USER'}</code>
Time: <code>{Chronos.now()}</code>
</blockquote>
"""
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types. InlineKeyboardButton(f"{Icons.ROCKET} Bắt Đầu", callback_data="start_using"))
        markup.add(types.InlineKeyboardButton(f"{Icons.SETTINGS} Hỗ Trợ", callback_data="support"))
        
        self.bot.reply_to(message, menu_text, reply_markup=markup)
    
    def cmd_tiktok(self, message):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            self.bot.reply_to(
                message,
                f"{Icons.WARN} <b>Cách dùng:</b>\n<code>/tiktok [url hoặc từ khóa]</code>\n\n"
                f"<b>Ví dụ:</b>\n"
                f"<code>/tiktok https://tiktok.com/@user/video/123</code>\n"
                f"<code>/tiktok phong cách thời trang</code>"
            )
            return
        
        query = args[1].strip()
        processing_msg = self.bot.reply_to(message, f"{Icons.CLOCK} Đang xử lý... <b>0%</b>")
        
        try:
            self.bot.edit_message_text(
                f"{Icons.CLOCK} Đang xử lý... <b>25%</b>",
                processing_msg.chat.id,
                processing_msg.message_id
            )
            
            if "tiktok. com" in query:
                video_info = self.tiktok_dl.get_video_info(query)
            else:
                video_info = self.tiktok_dl.search_videos(query)
            
            if not video_info:
                self.bot.edit_message_text(
                    f"{Icons.ERROR} <b>Không tìm thấy video</b>",
                    processing_msg.chat.id,
                    processing_msg.message_id
                )
                return
            
            self.bot.edit_message_text(
                f"{Icons.CLOCK} Đang xử lý... <b>50%</b>",
                processing_msg.chat.id,
                processing_msg.message_id
            )
            
            download_url = video_info.get('play')
            if not download_url: 
                self.bot.edit_message_text(
                    f"{Icons.ERROR} <b>Không thể lấy link video</b>",
                    processing_msg.chat.id,
                    processing_msg.message_id
                )
                return
            
            self.bot.edit_message_text(
                f"{Icons.DOWNLOAD} Đang tải video... <b>75%</b>",
                processing_msg.chat.id,
                processing_msg.message_id
            )
            
            video_file = self.tiktok_dl.download_video(download_url)
            
            if video_file == "TOO_LARGE":
                self.bot.edit_message_text(
                    f"{Icons. WARN} <b>File quá lớn (>50MB)</b>\n"
                    f"{Icons.LINK} <a href='{download_url}'>Tải trực tiếp</a>",
                    processing_msg.chat.id,
                    processing_msg. message_id,
                    disable_web_page_preview=True
                )
                return
            
            if not video_file:
                self.bot.edit_message_text(
                    f"{Icons.ERROR} <b>Lỗi tải video</b>",
                    processing_msg.chat.id,
                    processing_msg. message_id
                )
                return
            
            self.bot.edit_message_text(
                f"{Icons.UPLOAD} Đang gửi... <b>90%</b>",
                processing_msg.chat.id,
                processing_msg.message_id
            )
            
            with open(video_file, 'rb') as video:
                stats = video_info.get('stats', {})
                author = video_info.get('author', {})
                
                caption = f"""
<blockquote>
🎬 <b>{video_info.get('title', 'Video TikTok')[:50]}</b>
━━━━━━━━━━━━━━━━━━━━━
👤 <b>By:</b> @{author.get('uniqueId', 'Unknown')}
❤️ <b>Likes:</b> {Utils.fmt(stats.get('diggCount', 0))}
💬 <b>Comments:</b> {Utils.fmt(stats.get('commentCount', 0))}
🔄 <b>Shares:</b> {Utils.fmt(stats.get('shareCount', 0))}
👁️ <b>Views:</b> {Utils.fmt(stats.get('playCount', 0))}
🕒 <b>Duration:</b> {video_info.get('duration', 0)}s
</blockquote>
"""
                self.bot.send_video(
                    message.chat.id,
                    video,
                    caption=caption,
                    reply_to_message_id=message.message_id
                )
            
            self.bot.delete_message(processing_msg.chat.id, processing_msg.message_id)
            self.db.log_action(message.from_user.id, "tiktok_download", query[:100])
            
            if os.path.exists(video_file):
                os.remove(video_file)
            
        except Exception as e:
            logger.error(f"TikTok error: {e}")
            self.bot.edit_message_text(
                f"{Icons.ERROR} <b>Lỗi:</b> {str(e)[:50]}",
                processing_msg.chat.id,
                processing_msg.message_id
            )
    
    def cmd_music(self, message):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            self.bot.reply_to(
                message,
                f"{Icons.WARN} <b>Cách dùng:</b>\n<code>/music [tên bài hát]</code>\n\n"
                f"<b>Ví dụ:</b>\n<code>/music sơn tùng MTP</code>"
            )
            return
        
        keyword = args[1].strip()
        processing = self.bot.reply_to(message, f"{Icons.CLOCK} Tìm kiếm:  <b>{keyword}</b>...")
        
        try:
            tracks = self.sc_client.search_tracks(keyword, limit=10)
            
            if not tracks:
                self.bot.edit_message_text(
                    f"{Icons. ERROR} <b>Không tìm thấy bài hát</b>",
                    processing.chat.id,
                    processing.message_id
                )
                return
            
            lines = []
            for idx, track in enumerate(tracks, 1):
                title = Utils.escape_html(track['title'][:40])
                artist = Utils.escape_html(track['artist'][: 30])
                plays = Utils.fmt(track['plays'])
                duration_sec = int(track['duration'] / 1000)
                duration = f"{duration_sec//60}:{duration_sec%60:02d}"
                
                lines.append(f"<b>{idx}.</b> 🎵 {title}")
                lines.append(f"   👤 {artist} | 🕒 {duration} | 🎧 {plays}")
                lines. append("")
            
            content = "\n".join(lines)
            content += "\n💡 <b>Reply theo số thứ tự để tải nhạc</b>"
            
            result_msg = self.bot.send_message(
                message.chat.id,
                f"<blockquote>\n{content}\n</blockquote>",
                reply_to_message_id=message.message_id
            )
            
            self. search_cache.set(f"music_{result_msg.message_id}", tracks)
            self.user_states[result_msg.message_id] = "waiting_music_choice"
            
            self.bot.delete_message(processing.chat.id, processing.message_id)
            
        except Exception as e:
            logger.error(f"Music search error: {e}")
            self.bot.edit_message_text(
                f"{Icons.ERROR} <b>Lỗi tìm kiếm</b>",
                processing.chat.id,
                processing.message_id
            )
    
    def cmd_weather(self, message):
        args = message.text.split(maxsplit=1)
        location = args[1].strip() if len(args) > 1 else "Hanoi"
        
        try:
            geo = requests.get(
                f"https://geocoding-api.open-meteo.com/v1/search?name={location}&count=1&language=vi&format=json",
                timeout=15
            ).json()
            
            if not geo.get("results"):
                self.bot.reply_to(message, f"{Icons.ERROR} <b>Địa điểm không hợp lệ</b>")
                return
            
            lat = geo["results"][0]["latitude"]
            lon = geo["results"][0]["longitude"]
            name_loc = geo["results"][0]["name"]
            
            ow = requests.get(
                f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}"
                f"&appid={Config. OPENWEATHER_API_KEY}&units=metric&lang=vi",
                timeout=15
            ).json()
            
            cur = ow["weather"][0]
            main = ow["main"]
            wind = ow["wind"]
            
            content = f"""
<blockquote>
📍 <b>{name_loc.upper()}</b>
━━━━━━━━━━━━━━━━━━━━━
🌡️ <b>Nhiệt độ:</b> {main['temp']}°C (Cảm thấy: {main['feels_like']}°C)
☁️ <b>Bầu trời:</b> {cur['description']. capitalize()}
💧 <b>Độ ẩm:</b> {main['humidity']}%
💨 <b>Gió:</b> {wind['speed']} m/s
🔍 <b>Áp suất:</b> {main['pressure']} hPa
👁️ <b>Tầm nhìn:</b> {ow. get('visibility', 0)/1000:.1f} km
⏰ <b>Cập nhật:</b> {Chronos.now()}
</blockquote>
"""
            self.bot.reply_to(message, content)
        except Exception as e:
            logger.error(f"Weather error: {e}")
            self.bot.reply_to(message, f"{Icons.ERROR} <b>Lỗi API thời tiết</b>")
    
    def cmd_github(self, message):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            self.bot.reply_to(
                message,
                f"{Icons.WARN} <b>Cách dùng:</b>\n<code>/github [username]</code>\n\n"
                f"<b>Ví dụ:</b>\n<code>/github torvalds</code>"
            )
            return
        
        username = args[1].strip()
        
        try:
            resp = requests.get(
                f"https://api.github.com/users/{username}",
                headers={"Accept": "application/vnd.github.v3+json"},
                timeout=15
            ).json()
            
            if resp.get('message') == 'Not Found':
                self.bot.reply_to(message, f"{Icons. ERROR} <b>Không tìm thấy user GitHub</b>")
                return
            
            content = f"""
<blockquote>
👤 <b>Profile GitHub</b>
━━━━━━━━━━━━━━━━━━━━━
📛 <b>Name:</b> {resp. get('name', 'N/A')}
🔗 <b>Username:</b> <code>{resp.get('login')}</code>
🆔 <b>ID:</b> <code>{resp.get('id')}</code>
📝 <b>Bio:</b> {resp.get('bio', 'N/A')}
📦 <b>Public Repos:</b> {resp.get('public_repos')}
👥 <b>Followers:</b> {resp.get('followers')}
🔗 <b>Following:</b> {resp.get('following')}
📍 <b>Location:</b> {resp.get('location', 'N/A')}
🏢 <b>Company:</b> {resp.get('company', 'N/A')}
🔗 <b>Blog:</b> {resp.get('blog', 'N/A')}
📅 <b>Joined:</b> {resp.get('created_at', '')[:10]}
</blockquote>
"""
            self.bot.reply_to(message, content)
        except Exception as e:
            logger.error(f"GitHub error: {e}")
            self.bot.reply_to(message, f"{Icons.ERROR} <b>Lỗi API GitHub</b>")
    
    def cmd_idfb(self, message):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            self.bot.reply_to(
                message,
                f"{Icons.WARN} <b>Cách dùng:</b>\n<code>/idfb [facebook_link]</code>\n\n"
                f"<b>Ví dụ:</b>\n<code>/idfb https://facebook.com/zuck</code>"
            )
            return
        
        fb_link = args[1].strip()
        
        try:
            resp = requests.get(
                f"https://keyherlyswar.x10.mx/Apidocs/getuidfb.php?link={quote(fb_link)}",
                timeout=15
            ).json()
            
            if resp.get("status") == "success":
                content = f"""
<blockquote>
✅ <b>Trích xuất thành công</b>
━━━━━━━━━━━━━━━━━━━━━
🔗 <b>Link:</b> {fb_link}
🆔 <b>UID:</b> <code>{resp['uid']}</code>
⏰ <b>Cập nhật:</b> {Chronos.now()}
</blockquote>
"""
                self.bot.reply_to(message, content)
            else:
                self.bot.reply_to(message, f"{Icons.ERROR} <b>Không thể trích UID</b>")
        except Exception as e:
            logger.error(f"Facebook error: {e}")
            self.bot.reply_to(message, f"{Icons.ERROR} <b>Lỗi API Facebook</b>")
    
    def cmd_qrbank(self, message):
        args = message.text.split()
        if len(args) < 3:
            bank_list = "\n".join([f"• <code>{k}</code> - {v['name']}" for k, v in list(BANK_CODES.items())[:5]])
            self.bot.reply_to(
                message,
                f"{Icons.WARN} <b>Cách dùng:</b>\n<code>/qrbank [stk] [bank]</code>\n\n"
                f"<b>Các ngân hàng:</b>\n{bank_list}"
            )
            return
        
        acc = args[1]
        bank_code = args[2]. lower()
        
        bank_info = BANK_CODES.get(bank_code)
        if not bank_info:
            self.bot.reply_to(message, f"{Icons.ERROR} <b>Mã ngân hàng không hợp lệ</b>")
            return
        
        qr_url = f"https://img.vietqr.io/image/{bank_info['bin']}-{acc}-qr_only.jpg"
        
        content = f"""
<blockquote>
💳 <b>QR Chuyển Khoản</b>
━━━━━━━━━━━━━━━━━━━━━
🏦 <b>Ngân hàng:</b> {bank_info['name']}
🔢 <b>Số tài khoản:</b> <code>{acc}</code>
</blockquote>
"""
        
        try:
            resp = requests.head(qr_url, timeout=5)
            if resp.status_code == 200:
                self.bot.send_photo(
                    message.chat.id,
                    qr_url,
                    caption=content,
                    reply_to_message_id=message.message_id
                )
            else:
                self.bot.reply_to(message, content)
        except:
            self.bot.reply_to(message, content)
    
    def handle_message(self, message):
        text = message.text.strip() if message.text else ""
        
        if not text:
            return
        
        if "tiktok.com" in text:
            message.text = f"/tiktok {text}"
            self.cmd_tiktok(message)
            return
        
        for trigger in TRIGGERS_MUSIC:
            if text.lower().startswith(trigger + " "):
                query = text[len(trigger):].strip()
                message.text = f"/music {query}"
                self.cmd_music(message)
                return
        
        for trigger in TRIGGERS_TIKTOK:
            if text. lower().startswith(trigger + " "):
                query = text[len(trigger):].strip()
                message.text = f"/tiktok {query}"
                self.cmd_tiktok(message)
                return
        
        self.db.log_action(message.from_user.id, "message", text[: 100])
    
    def handle_callback(self, call):
        if call.data == "start_using":
            self.bot.answer_callback_query(call.id, "✅ Đã bắt đầu!")
            self.bot.send_message(
                call.message.chat.id,
                f"{Icons.ROCKET} <b>Gửi link TikTok hoặc nhập từ khóa để bắt đầu!</b>"
            )
        elif call.data == "support":
            self.bot.answer_callback_query(call.id)
            self.bot.send_message(
                call.message.chat.id,
                f"{Icons.MAIL} <b>Liên hệ hỗ trợ: </b>\n"
                f"📧 Email: support@example.com\n"
                f"💬 Telegram: @support_bot"
            )
        else:
            self.bot.answer_callback_query(call.id)
    
    def run_flask(self):
        try:
            self.app.run(
                host=Config.WEB_HOST,
                port=Config.WEB_PORT,
                debug=False,
                use_reloader=False,
                threaded=True
            )
        except Exception as e:
            logger.error(f"Flask error: {e}")
    
    def run(self):
        try:
            flask_thread = Thread(target=self.run_flask, daemon=True)
            flask_thread.start()
            logger.info(f"🌐 Flask Server: http://{Config.WEB_HOST}:{Config.WEB_PORT}")
            
            logger.info("🤖 Bot đang khởi động...")
            print("""
╔══════════════════════════════════════════╗
║    ULTIMATE BOT SYSTEM v1.0.0            ║
║    Integrated All-in-One Solution        ║
║    © 2024 Duckiencoder                   ║
║    Status: ONLINE ✅                     ║
╚══════════════════════════════════════════╝
            """)
            
            self.bot.infinity_polling(timeout=20, long_polling_timeout=10)
        except KeyboardInterrupt:
            logger.info("❌ Bot dừng bởi người dùng")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Bot error: {e}")
            sys.exit(1)

if __name__ == "__main__": 
    bot = UltimateBot()
    bot.run()
    
    
    
    
    
    
    
    
#2


import logging
import datetime
import time
import random
import requests
import html
import json
import re
import asyncio
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, CallbackQueryHandler, filters

BOT_TOKEN = "8413179871:AAHIHWYvoHM4an3XdPXobbl6Bmn2GGGwFtg"

TRIGGER_KEYWORDS = ["scan", "check", "soi", "info", "tìm", "profile", "tiktok", "ttcheck", "tìm", "s"]

class Icons:
    CROWN = "👑"
    VERIFY = "💠"
    LOCK = "🔒"
    GLOBE = "🌏"
    CHART = "📈"
    SHOP = "🛍️"
    FIRE = "🔥"
    STAR = "⭐"
    GHOST = "👻"
    EYE = "👁️"
    BOX = "📦"
    PIN = "📌"
    BULB = "💡"
    WARN = "⚠️"
    CHECK = "☑️"
    ID = "🆔"
    USER = "👤"
    TIME = "⏳"
    LINK = "🔗"
    MAIL = "📧"
    PHONE = "📞"
    MUSIC = "🎵"
    VID = "🎥"

class Utils:
    @staticmethod
    def fmt(num):
        if num is None: return "0"
        try:
            n = int(num)
            if n >= 1_000_000_000: return f"{n/1_000_000_000:.2f}B"
            elif n >= 1_000_000: return f"{n/1_000_000:.2f}M"
            elif n >= 1_000: return f"{n/1_000:.2f}K"
            return str(n)
        except:
            return str(num)

    @staticmethod
    def tags(text):
        if not text: return []
        return re.findall(r"(@[a-zA-Z0-9_\.]+)", text)

    @staticmethod
    def emails(text):
        if not text: return []
        return re.findall(r"[\w\.-]+@[\w\.-]+", text)

    @staticmethod
    def phones(text):
        if not text: return []
        return re.findall(r"(0\d{9,10})", text)

class Chronos:
    @staticmethod
    def now():
        return datetime.datetime.now().strftime("%H:%M:%S - %d/%m/%Y")

    @staticmethod
    def age(uid):
        try:
            binary = "{0:b}".format(int(uid))
            timestamp = int(binary[:31], 2)
            c_date = datetime.datetime.fromtimestamp(timestamp)
            now = datetime.datetime.now()
            delta = now - c_date
            return c_date.strftime("%d/%m/%Y"), f"{delta.days} ngày"
        except:
            return "N/A", "N/A"

class Network:
    def __init__(self):
        self.session = requests.Session()
        self.api = "https://www.tikwm.com/api/user/info"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json"
        }

    def fetch(self, query):
        params = {"unique_id": query}
        for _ in range(3):
            try:
                res = self.session.get(self.api, headers=self.headers, params=params, timeout=15)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("code") == 0:
                        return data
            except:
                time.sleep(1)
        return None

class Analyst:
    def __init__(self, user, stats):
        self.u = user
        self.s = stats
        self.flw = int(stats.get("followerCount", 0))
        self.hrt = int(stats.get("heartCount", 0))
        self.vid = int(stats.get("videoCount", 0))
    
    def health(self):
        score = 100
        if self.vid > 0:
            avg = self.hrt / self.vid
            er = (avg / self.flw * 100) if self.flw > 0 else 0
        else:
            avg, er = 0, 0
            
        if self.vid > 5 and avg < 10: score -= 20
        if self.flw > 10000 and er < 0.5: score -= 30
        if self.vid > 20 and self.hrt < 100: score -= 40
        if not self.u.get("verified"): score -= 5
        if not self.u.get("signature"): score -= 5
            
        if score >= 90: rank = "S (Huyền Thoại)"
        elif score >= 70: rank = "A (Xuất Sắc)"
        elif score >= 50: rank = "B (Ổn Định)"
        else: rank = "C (Cần Tối Ưu)"
        
        return {"score": max(0, score), "rank": rank, "er": er, "avg": avg}

    def content_type(self):
        bio = self.u.get("signature", "").lower()
        nick = self.u.get("nickname", "").lower()
        full_text = bio + " " + nick
        
        if any(k in full_text for k in ["shop", "sỉ", "lẻ", "order", "mua", "bán", "store"]): return "Kinh Doanh / Bán Hàng"
        if any(k in full_text for k in ["game", "liên quân", "pubg", "free fire", "gaming"]): return "Gaming / Streamer"
        if any(k in full_text for k in ["vlog", "daily", "cuộc sống", "travel"]): return "Vlog / Đời Sống"
        if any(k in full_text for k in ["review", "đánh giá", "food", "ăn"]): return "Reviewer / Ẩm Thực"
        if any(k in full_text for k in ["nhảy", "dance", "music", "nhạc"]): return "Nghệ Thuật / Giải Trí"
        if any(k in full_text for k in ["share", "tips", "hướng dẫn", "học"]): return "Giáo Dục / Chia Sẻ"
        if any(k in full_text for k in ["edit", "video", "design"]): return "Editor / Creator"
        
        if self.flw > 100000: return "Người Nổi Tiếng (KOL)"
        if self.vid > 0: return "Sáng Tạo Nội Dung"
        return "Người Dùng Cá Nhân"

    def commerce(self):
        c_info = self.u.get("commerceUserInfo", {})
        is_shop = c_info.get("commerceUser", False)
        ads = "Có" if self.u.get("verified") or is_shop or self.flw > 10000 else "Không"
        return {"shop": is_shop, "ads": ads}

class Interface:
    def __init__(self, data):
        self.u = data['user']
        self.s = data['stats']
        self.h = data['health']
        self.c = data['commerce']
        self.cat = data['category']

    def _row(self, icon, label, value):
        return f"{icon} {label}: <code>{value}</code>"

    def render(self):
        su = Utils
        c_date, c_age = Chronos.age(self.u.get("id"))
        verified = "Đã xác minh" if self.u.get("verified") else "Chưa xác minh"
        privacy = "Riêng tư" if self.u.get("secret") else "Công khai"
        
        bio = self.u.get("signature", "")
        tags = su.tags(bio)
        mails = su.emails(bio)
        phones = su.phones(bio)
        
        contact_info = ""
        if mails: contact_info += f"\n{Icons.MAIL} Email: <code>{', '.join(mails)}</code>"
        if phones: contact_info += f"\n{Icons.PHONE} SĐT: <code>{', '.join(phones)}</code>"
        
        report = f"""<b>{Icons.CROWN} HỒ SƠ PHÂN TÍCH TOÀN DIỆN {Icons.CROWN}</b>

<blockquote><b>{Icons.USER} ĐỊNH DANH KÊNH</b>
{self._row(Icons.ID, "Họ Tên", self.u.get("nickname"))}
{self._row(Icons.VERIFY, "ID Gốc", self.u.get("id"))}
{self._row(Icons.PIN, "Username", f"@{self.u.get('uniqueId')}")}
{self._row(Icons.GLOBE, "Khu Vực", self.u.get("region", "VN"))}
{self._row(Icons.CHECK, "Tích Xanh", verified)}
{self._row(Icons.LOCK, "Quyền", privacy)}
{self._row(Icons.TIME, "Tuổi Kênh", c_age)}
</blockquote>
<blockquote><b>{Icons.BULB} PHÂN TÍCH NỘI DUNG</b>
{self._row(Icons.STAR, "Chủ Đề", self.cat)}
{self._row(Icons.VID, "Video", su.fmt(self.s.get('videoCount')))}
{self._row(Icons.EYE, "View/Vid", su.fmt(self.h['avg']))}
{self._row(Icons.CHART, "Tương Tác", f"{self.h['er']:.2f}%")}
{self._row(Icons.FIRE, "Điểm Số", f"{self.h['score']}/100")}
{self._row(Icons.CROWN, "Xếp Hạng", self.h['rank'])}
</blockquote>
<blockquote><b>{Icons.CHART} CHỈ SỐ TĂNG TRƯỞNG</b>
{self._row(Icons.USER, "Followers", su.fmt(self.s.get('followerCount')))}
{self._row(Icons.GHOST, "Following", su.fmt(self.s.get('followingCount')))}
{self._row(Icons.FIRE, "Tổng Tim", su.fmt(self.s.get('heartCount')))}
{self._row(Icons.BOX, "Bạn Bè", su.fmt(self.s.get('friendCount')))}
{self._row(Icons.STAR, "Đã Thích", su.fmt(self.s.get('diggCount')))}
</blockquote>
<blockquote><b>{Icons.SHOP} THƯƠNG MẠI & SETTING</b>
{self._row(Icons.SHOP, "TikTok Shop", "Đang Bật" if self.c['shop'] else "Chưa Có")}
{self._row(Icons.BOX, "Giỏ Hàng", "Hiển Thị" if self.c['shop'] else "Ẩn")}
{self._row(Icons.STAR, "Chạy Ads", self.c['ads'])}
{self._row(Icons.LOCK, "Tải Video", "Cho Phép")}
{self._row(Icons.MUSIC, "Duet/Stitch", "Cho Phép")}
</blockquote>
<blockquote><b>{Icons.PIN} TIỂU SỬ & LIÊN HỆ</b>
<i>{html.escape(bio) if bio else "Chưa cập nhật tiểu sử."}</i>
{contact_info}
{self._row(Icons.USER, "Tags", ", ".join(tags) if tags else "Không")}
{self._row(Icons.LINK, "Link Bio", self.u.get("bioLink", {}).get("link", "Không"))}
</blockquote>
<i>{Chronos.now()} | Powered by Omni-AI</i>
"""
        return report

class Bot:
    def __init__(self):
        self.app = ApplicationBuilder().token(BOT_TOKEN).build()
        self.net = Network()

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "<b>Omni-Present System Online.</b>\nGửi lệnh <code>check [username]</code> để phân tích.",
            parse_mode=ParseMode.HTML
        )

    async def process(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        valid = any(text.lower().startswith(k) for k in TRIGGER_KEYWORDS)
        if not valid: return

        query = text.split(" ")[1] if " " in text else text
        for k in TRIGGER_KEYWORDS: query = query.replace(k, "")
        
        if "tiktok.com" in query:
            try: query = query.split("@")[1].split("?")[0].split("/")[0]
            except: pass
        query = query.replace("@", "").strip()

        if not query:
            await update.message.reply_text("⚠️ Vui lòng nhập Username.")
            return

        msg = await update.message.reply_text("💻.")
        
        raw = await asyncio.to_thread(self.net.fetch, query)
        if not raw:
            await msg.edit_text(f"❌ Không tìm thấy: <b>{query}</b>", parse_mode=ParseMode.HTML)
            return

        u = raw.get("data", {}).get("user", {})
        s = raw.get("data", {}).get("stats", {})
        
        analyst = Analyst(u, s)
        data = {
            "user": u, "stats": s,
            "health": analyst.health(),
            "category": analyst.content_type(),
            "commerce": analyst.commerce()
        }

        report = Interface(data).render()
        avt = u.get("avatarLarger")
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Truy Cập Profile", url=f"https://www.tiktok.com/@{u.get('uniqueId')}")],
            [InlineKeyboardButton("Tải Ảnh Gốc", callback_data="dl"), InlineKeyboardButton("Quét Lại", callback_data=f"re|{query}")]
        ])

        try:
            await update.message.reply_photo(photo=avt, caption=report, parse_mode=ParseMode.HTML, reply_markup=kb)
            await msg.delete()
        except:
            
            await update.message.reply_photo(photo=avt)
            await update.message.reply_text(report, parse_mode=ParseMode.HTML, reply_markup=kb)
            await msg.delete()

    async def cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        if q.data == "dl": await q.message.reply_text(" ")
        elif q.data.startswith("re"): await q.message.reply_text("Đang làm mới...")

    def run(self):
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.process))
        self.app.add_handler(CallbackQueryHandler(self.cb))
        self.app.run_polling()

if __name__ == "__main__":
    Bot().run()
    
    
    
    
    
    
    
#3


import google.generativeai as genai
import html
import io
import json
import os
import random 
import pathlib
import PIL.Image
import random
import re
import requests
import sys
import telebot
import tempfile
import threading
import time
import uuid
from datetime import datetime
from flask import Flask
from moviepy.editor import VideoFileClip
from requests.adapters import HTTPAdapter
from telebot import TeleBot, types
from telebot.types import (
    InputMediaPhoto,
    InlineKeyboardButton,
    InlineKeyboardMarkup
)
from urllib.parse import urlparse, parse_qs, unquote
from urllib3.util.retry import Retry

BOT_TOKEN = "8097478643:AAHfOJ2cJF4hubidaNM9paAbwTW5eNaJIHw"
ADMIN_ID = 7679054753
GEMINI_API_KEY = "AIzaSyAWp3AxiFF5OL1rFD_3WmdTe3lMRPgEWVw"
WEB_PORT = 2026

AI_MODELS = {
    "gemini-2.0-flash": "⚡ Flash 2.0",
    "gemini-2.5-pro": "💎 Pro 2.5",
    "gemini-3-pro": "🔱 Vip 3",
}
CURRENT_MODEL = "gemini-2.0-flash"

TRIGGERS_MUSIC = [
    "nhạc", "nhac", "music", "play", "nghe", "song", "bài hát", "bai hat", 
    "track", "sound", "scl", "mp3", "tìm bài", "tim bai", "audio"
]
TRIGGERS_VOICE = [
    "tách", "tach", "lấy nhạc", "lay nhac", "crvoice", "voice", "âm thanh", 
    "am thanh", "convert", "chuyển đổi", "chuyen doi", "mp3", "audio", "lấy tiếng"
]
TRIGGERS_TIKTOK_SEARCH = [
    "tiktok", "tt", "douyin", "video", "vid", "clip", "xem"
]

bot = TeleBot(BOT_TOKEN)
app = Flask(__name__)
genai.configure(api_key=GEMINI_API_KEY)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
]

BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

SC_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://soundcloud.com",
    "Referer": "https://soundcloud.com/",
    "Connection": "keep-alive",
}

SESSION = requests.Session()
retries = Retry(total=5, backoff_factor=0.6,
                status_forcelist=(403, 429, 500, 502, 503, 504),
                allowed_methods=frozenset(["GET"]))
SESSION.mount("https://", HTTPAdapter(max_retries=retries))
SESSION.headers.update(SC_HEADERS)

SEARCH_CONTEXT = {}
CONTEXT_TIMESTAMP = {}
CONTEXT_TTL = 15 * 60
PLAYER_STATE = {}
PLAYER_LOCK = threading.Lock()

class TTLCache:
    def __init__(self, ttl_sec=600, max_size=256):
        self.ttl = ttl_sec
        self.max = max_size
        self.data = {}
        self.lock = threading.Lock()
    def get(self, key):
        with self.lock:
            v = self.data.get(key)
            if not v: return None
            val, exp = v
            if exp < time.time():
                self.data.pop(key, None); return None
            return val
    def set(self, key, val):
        with self.lock:
            if len(self.data) >= self.max:
                self.data.pop(next(iter(self.data.keys())), None)
            self.data[key] = (val, time.time() + self.ttl)

CACHE_SEARCH = TTLCache(ttl_sec=300, max_size=256)
CACHE_TRACK  = TTLCache(ttl_sec=900, max_size=512)
CACHE_RESOLVE= TTLCache(ttl_sec=900, max_size=1024)

@app.route('/')
def index():
    return "<h1>BOT IS RUNNING - DUCKIENCODER</h1>"

def run_web():
    try: app.run(host='0.0.0.0', port=WEB_PORT, use_reloader=False)
    except: pass

def check_internet_connection():
     try:
        requests.get("https://www.google.com", timeout=5)
        return True
     except requests.ConnectionError:
        return False

def get_random_element(array):
    return random.choice(array)

def get_client_id():
    try:
        response = requests.get("https://soundcloud.com/", headers=SC_HEADERS)
        response.raise_for_status()
        script_tags = re.findall(r'<script crossorigin src="([^"]+)"', response.text)
        script_urls = [url for url in script_tags if url.startswith("https")]
        if not script_urls:
            return 'W00nmY7TLer3uyoEo1sWK3Hhke5Ahdl9'
        script_response = requests.get(script_urls[-1], headers=SC_HEADERS)
        script_response.raise_for_status()
        client_id_match = re.search(r',client_id:"([^"]+)"', script_response.text)
        if not client_id_match:
            return 'W00nmY7TLer3uyoEo1sWK3Hhke5Ahdl9'
        return client_id_match.group(1)
    except Exception:
        return 'W00nmY7TLer3uyoEo1sWK3Hhke5Ahdl9'

def http_get_robust(url, *, params=None, timeout=20):
    UA_POOL = USER_AGENTS
    for _ in range(5):
        headers = {"User-Agent": random.choice(UA_POOL)}
        try:
            r = SESSION.get(url, params=params or {}, timeout=timeout, headers={**SC_HEADERS, **headers})
            if r.status_code in (429, 503):
                time.sleep(0.8)
                continue
            if r.status_code == 403:
                time.sleep(0.2)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException:
            time.sleep(0.25)
    return None

def ui_frame(title, user, content, is_admin=False):
    time_now = datetime.now().strftime("%H:%M %d/%m")
    name = html.escape(user.first_name)
    if is_admin:
        icon, label, footer, theme = " ⚜️ ", "👑 Ower:", "@tg_mdediavip", "⚜"
    else:
        icon, label, footer, theme = "💠", "👤 User:", "Vip Layer", "💠"
    
    return f"""
<b>╔════{icon}{title.upper()}{icon}════╗</b>

{label} <a href="tg://user?id={user.id}">{name}</a>
⏰ <b>Time:</b> <code>{time_now}</code>
🧠 <b>Core:</b> <code>{CURRENT_MODEL}</code>

<b>📊Data📊:</b>
<blockquote>{content}</blockquote>

<b>╚═════{theme}{footer}{theme}═════╝</b>
"""

def reply_vip(message, title, content, markup=None):
    user = message.from_user
    name = html.escape(user.first_name)
    time_now = datetime.now().strftime("%H:%M:%S %d/%m/%Y")
    text = f"""
<b>╔═══ ⚜ {title.upper()} ⚜ ═══╗</b>

👤 <b>User:</b> <a href="tg://user?id={user.id}">{name}</a>
🆔 <b>ID:</b> <code>{user.id}</code>
⏰ <b>Time:</b> <code>{time_now}</code>

<b>DATA:</b>
<blockquote>{content}</blockquote>

<b>╚═══ ⚜ @tg_mediavip⚜ ═══╝</b>
"""
    return bot.reply_to(message, text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)

def ai_reply(text, image_data=None, is_admin=False):
    try:
        sys_prompt = f"You are an AI assistant using {CURRENT_MODEL}. You are a professional female assistant."
        if is_admin:
            sys_prompt += " You must always address the user as 'anh Kiên' in a polite, professional, and slightly affectionate manner suitable for a personal assistant."
        else:
            sys_prompt += " Act slightly superior, concise, and edgy."
        
        model = genai.GenerativeModel(CURRENT_MODEL, system_instruction=sys_prompt)
        
        response_text = ""
        if image_data:
            img = PIL.Image.open(io.BytesIO(image_data))
            response_text = model.generate_content([text or "Analyze this image", img]).text
        else:
            response_text = model.generate_content(text).text
            
        if is_admin and "anh Kiên" not in response_text:
            response_text = f"Chào anh Kiên,\n{response_text}"
            
        return response_text
    except Exception as e: return f"AI Error: {str(e)}"

def esc(x) -> str:
    return html.escape(str(x or ""), quote=False)

def ms_to_mmss(ms: int) -> str:
    sec = max(0, int(round((ms or 0) / 1000)))
    return f"{sec//60}:{sec%60:02d}"

def fmt_int(n) -> str:
    try: return f"{int(n):,}".replace(",", ".")
    except Exception: return "0"

def best_artwork(url: str | None) -> str | None:
    if not url: return None
    return re.sub(r"-large(\.\w+)$", r"-t500x500\1", url)

def download_to_temp(url: str, suffix: str = ".mp3") -> str:
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    tmp = pathlib.Path(tempfile.gettempdir()) / f"sc_{uuid.uuid4().hex}{suffix}"
    with open(tmp, "wb") as f:
        for chunk in r.iter_content(262144):
            if chunk: f.write(chunk)
    return str(tmp)

def sc_widget_resolve(track_url: str) -> dict:
    r = http_get_robust("https://api-widget.soundcloud.com/resolve", params={"url": track_url, "format": "json"}, timeout=20)
    data = r.json()
    data.setdefault("permalink_url", track_url)
    return data

def sc_get_direct_mp3_from_widget_json(j: dict) -> str | None:
    streams = j.get("streams") or {}
    if streams.get("http_mp3_128_url"):
        return streams["http_mp3_128_url"]
    media = j.get("media") or {}
    for t in (media.get("transcodings") or []):
        fmt = t.get("format") or {}
        if fmt.get("protocol") == "progressive" and t.get("url"):
            try:
                rr = http_get_robust(t["url"], timeout=15)
                u = rr.json().get("url")
                if u and u.endswith(".mp3"): return u
            except Exception:
                continue
    return None

def _sc_search_api(query: str, limit: int = 8, offset: int = 0):
    cid = get_client_id()
    params = {"q": query, "client_id": cid, "limit": limit, "offset": offset, "app_locale": "en"}
    r = http_get_robust("https://api-v2.soundcloud.com/search/tracks", params=params, timeout=15)
    data = r.json()
    total = data.get("total_results", len(data.get("collection", [])))
    tracks = []
    for it in data.get("collection", []):
        user = (it.get("user") or {})
        tracks.append({
            "id": it.get("id"), 
            "title": it.get("title") or "",
            "duration": it.get("full_duration") or it.get("duration") or 0,
            "permalink_url": it.get("permalink_url"), 
            "artwork_url": it.get("artwork_url"),
            "user": user.get("username", "Unknown"),
            "likes_count": it.get("likes_count") or it.get("favoritings_count") or 0,
            "playback_count": it.get("playback_count") or 0,
            "genre": it.get("genre", "Unknown"),
            "created_at": it.get("created_at", "")[:10]
        })
    return tracks, int(total)

def sc_search_tracks_fallback_no_api(query: str, limit: int = 8):
    r = http_get_robust("https://soundcloud.com/search", params={"q": query, "filter": "tracks"}, timeout=20)
    html_txt = r.text
    links = []
    for m in re.finditer(r'https://soundcloud\.com/[A-Za-z0-9_\-\.]+/[\w\-%]+', html_txt):
        url = m.group(0).split('?')[0].split('#')[0]; links.append(url)
    seen = set(); uniq = []
    for u in links:
        if u not in seen:
            seen.add(u); uniq.append(u)
        if len(uniq) >= limit: break
    tracks = []
    for url in uniq:
        try:
            info = sc_widget_resolve(url)
            user = (info.get("user") or {})
            tracks.append({
                "id": info.get("id"), 
                "title": info.get("title") or "",
                "duration": info.get("duration") or info.get("full_duration") or 0,
                "permalink_url": info.get("permalink_url") or url,
                "artwork_url": info.get("artwork_url"), 
                "user": user.get("username", "Unknown"),
                "likes_count": info.get("likes_count") or 0, 
                "playback_count": info.get("playback_count") or 0,
                "genre": info.get("genre", "Unknown"),
                "created_at": info.get("created_at", "")[:10]
            })
        except Exception:
            continue
    return tracks, len(tracks)

def sc_search_tracks(query: str, limit: int = 8, offset: int = 0):
    ck = f"{query}|{limit}|{offset}"
    cached = CACHE_SEARCH.get(ck)
    if cached: return cached
    try:
        _limit = min(limit, 6)
        tracks, total = _sc_search_api(query, limit=_limit, offset=offset)
        if len(tracks) < limit and offset == 0:
            more, _ = _sc_search_api(query, limit=limit, offset=len(tracks))
            tracks = (tracks + more)[:limit]
        result = (tracks, total)
        CACHE_SEARCH.set(ck, result)
        return result
    except Exception:
        result = sc_search_tracks_fallback_no_api(query, limit=limit)
        CACHE_SEARCH.set(ck, result)
        return result

def sc_track_detail(track_id: int) -> dict:
    ck = f"track:{track_id}"
    cached = CACHE_TRACK.get(ck)
    if cached: return cached
    try:
        cid = get_client_id()
        r = http_get_robust(f"https://api-v2.soundcloud.com/tracks/{track_id}",
                             params={"client_id": cid}, timeout=15)
        it = r.json(); user = (it.get("user") or {})
        data = {
            "id": it.get("id"), 
            "title": it.get("title") or "",
            "duration": it.get("full_duration") or it.get("duration") or 0,
            "permalink_url": it.get("permalink_url"), 
            "artwork_url": it.get("artwork_url"),
            "user": user.get("username", "Unknown"),
            "likes_count": it.get("likes_count") or it.get("favoritings_count") or 0,
            "playback_count": it.get("playback_count") or 0,
            "genre": it.get("genre", "Unknown"),
            "created_at": it.get("created_at", "")[:10]
        }
        CACHE_TRACK.set(ck, data)
        return data
    except Exception:
        data = {"id": track_id}
        CACHE_TRACK.set(ck, data)
        return data

def sc_resolve_progressive_url(track_id: int, permalink_url: str | None = None) -> str | None:
    ck = f"res:{track_id}"
    cached = CACHE_RESOLVE.get(ck)
    if cached: return cached
    try:
        cid = get_client_id()
        if cid:
            r = http_get_robust(f"https://api-v2.soundcloud.com/tracks/{track_id}",
                                 params={"client_id": cid}, timeout=15)
            item = r.json()
            media = (item.get("media") or {})
            for t in (media.get("transcodings") or []):
                fmt = t.get("format") or {}
                if fmt.get("protocol") == "progressive" and t.get("url"):
                    rr = http_get_robust(t["url"], params={"client_id": cid}, timeout=15)
                    url = rr.json().get("url")
                    if url:
                        CACHE_RESOLVE.set(ck, url); return url
            s = http_get_robust(f"https://api-v2.soundcloud.com/tracks/{track_id}/streams",
                                 params={"client_id": cid}, timeout=15)
            j = s.json()
            if j.get("http_mp3_128_url"):
                url = j["http_mp3_128_url"]
                CACHE_RESOLVE.set(ck, url); return url
    except Exception:
        pass
    if permalink_url:
        try:
            info = sc_widget_resolve(permalink_url)
            url = sc_get_direct_mp3_from_widget_json(info)
            if url:
                CACHE_RESOLVE.set(ck, url); return url
        except Exception:
            return None
    return None

def cleanup_context():
    now = time.time()
    expired = [mid for mid, ts in CONTEXT_TIMESTAMP.items() if now - ts > CONTEXT_TTL]
    for mid in expired:
        SEARCH_CONTEXT.pop(mid, None)
        CONTEXT_TIMESTAMP.pop(mid, None)

def build_player_keyboard(playing: bool):
    play_label = "⏸ Pause" if playing else "▶️ Continue"
    kb = types.InlineKeyboardMarkup(row_width=3)
    kb.row(
        types.InlineKeyboardButton("⏮ Back", callback_data="player:prev"),
        types.InlineKeyboardButton(play_label, callback_data="player:toggle"),
        types.InlineKeyboardButton("Next ⏭", callback_data="player:next"),
    )
    kb.add(types.InlineKeyboardButton("⭐ Favourite ", callback_data="player:fav"))
    kb.add(types.InlineKeyboardButton("❌ Close Player", callback_data="player:close"))
    return kb

def build_ai_menu():
    kb = InlineKeyboardMarkup()
    for key, name in AI_MODELS.items():
        mark = "✅" if key == CURRENT_MODEL else "⚪"
        kb.add(InlineKeyboardButton(f"{mark} {name}", callback_data=f"setmodel:{key}"))
    kb.add(InlineKeyboardButton("❌ Close", callback_data="close_menu"))
    return kb

def set_player_state(chat_id: int, message_id: int, state: dict):
    with PLAYER_LOCK:
        PLAYER_STATE[(chat_id, message_id)] = state

def get_player_state(chat_id: int, message_id: int):
    with PLAYER_LOCK:
        return PLAYER_STATE.get((chat_id, message_id))

def pop_player_state(chat_id: int, message_id: int):
    with PLAYER_LOCK:
        return PLAYER_STATE.pop((chat_id, message_id), None)

def get_tiktok_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.tikwm.com/',
        'Origin': 'https://www.tikwm.com',
        'Accept-Language': 'en-US,en;q=0.9',
    }

def fix_tiktok_url(url):
    if url.startswith('/'):
        return "https://www.tikwm.com" + url
    return url
def fix_tiktok_url(url):
    if url.startswith('/'):
        return "https://www.tikwm.com" + url
    return url

def tikwm_api(keyword_or_url, mode='search'):
    url = "https://www.tikwm.com/api/feed/search" if mode == 'search' else "https://www.tikwm.com/api/"
    data = {'count': 12, 'cursor': 0, 'web': 1, 'hd': 1}
    if mode == 'search':
        data['keywords'] = keyword_or_url
    else:
        data['url'] = keyword_or_url

    try:
        for _ in range(3):
            resp = requests.post(url, data=data, headers=get_tiktok_headers(), timeout=15)
            if resp.status_code == 200:
                js = resp.json()
                if js.get('code') == 0:
                    if mode == 'search':
                        videos = js.get('data', {}).get('videos', [])
                        if videos:
                            v = random.choice(videos)
                            return {
                                'url': fix_tiktok_url(v.get('play')),
                                'title': v.get('title', 'Video TikTok'),
                                'author': v.get('author', {}).get('nickname', 'Unknown'),
                                'key': keyword_or_url,
                                'raw': v,
                                'views': v.get('play_count', 0),
                                'likes': v.get('digg_count', 0),
                                'comments': v.get('comment_count', 0),
                                'shares': v.get('share_count', 0),
                                'saves': v.get('download_count', 0),
                                'duration': v.get('duration', 0),
                                'create_time': v.get('create_time', ''),
                                'cover_url': v.get('cover', ''),
                                'description': v.get('title', ''),
                            }
                        else:
                            return {'error': 'No videos found'}
                    else:
                        v = js.get('data', {})
                        return {
                            'url': fix_tiktok_url(v.get('play')),
                            'title': v.get('title', 'Video TikTok'),
                            'author': v.get('author', {}).get('nickname', 'Unknown'),
                            'key': None,
                            'raw': v,
                            'views': v.get('play_count', 0),
                            'likes': v.get('digg_count', 0),
                            'comments': v.get('comment_count', 0),
                            'shares': v.get('share_count', 0),
                            'saves': v.get('download_count', 0),
                            'duration': v.get('duration', 0),
                            'create_time': v.get('create_time', ''),
                            'cover_url': v.get('cover', ''),
                            'description': v.get('title', ''),
                        }
            time.sleep(1)  
        return {'error': 'Failed after 3 attempts'}
    except Exception as e:
        print(f"API Error: {e}")
        return None

def download_video_super_vip(url):
    filename = f"super_{int(time.time())}_{random.randint(100,999)}.mp4"
    try:
        with requests.get(url, stream=True, headers=get_tiktok_headers(), timeout=60) as r:
            r.raise_for_status()
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        
        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        
        if file_size_mb > 49.5:
            os.remove(filename)
            return "TOO_LARGE"
            
        if file_size_mb < 0.01:
            os.remove(filename)
            return None
            
        return filename
    except Exception as e:
        if os.path.exists(filename): os.remove(filename)
        return None

def process_and_send_tiktok(chat_id, data, reply_id=None):
    if not data:
        bot.send_message(chat_id, "❌ Không tìm thấy dữ liệu hoặc link lỗi.")
        return

    msg_wait = bot.send_message(chat_id, f"⏳ Đang tải xuống server... (0%)")
    result = download_video_super_vip(data['url'])
    
    if result == "TOO_LARGE":
        bot.edit_message_text(
            f"⚠️ <b>File lớn hơn 50MB!</b>\nTelegram không cho phép bot gửi file này.\n\n🔗 <b>Link xem/tải trực tiếp:</b>\n{data['url']}", 
            chat_id, msg_wait.message_id, parse_mode="HTML"
        )
        return

    if not result:
        bot.edit_message_text(f"❌ Lỗi tải video. Link gốc có vấn đề.\n🔗 Link: {data['url']}", chat_id, msg_wait.message_id)
        return

    markup = types.InlineKeyboardMarkup()
    if data['key']:
        short_key = data['key'][:30]
        markup.add(types.InlineKeyboardButton(f"🔄 Video khác: {short_key}", callback_data=f"next|{short_key}"))

    raw = data.get('raw', {})
    stats = raw.get('stats', {}) if 'stats' in raw else raw
    author = raw.get('author', {})
    
    digg_count = fmt_int(raw.get('digg_count', stats.get('digg_count', 0)))
    comment_count = fmt_int(raw.get('comment_count', stats.get('comment_count', 0)))
    share_count = fmt_int(raw.get('share_count', stats.get('share_count', 0)))
    download_count = fmt_int(raw.get('download_count', stats.get('download_count', 0)))
    
    author_name = author.get('nickname', data['author'])
    author_id = author.get('unique_id', 'unknown')
    
    caption_text = f"""
<blockquote>
🎬 <b>{data['title']}</b>
━━━━━━━━━━━━━━━━━━━━━
👤 <b>{data['author']}</b>
❤️ <b>Like:</b> {data['likes']} | 💬 <b>Cmt:</b> {data['comments']}
🔗 <b>Share:</b> {data['shares']} | 💾 <b>Save:</b> {data['saves']}
▶️ <b>Views:</b> {data['views']}
🕒 <b>Duration:</b> {data['duration']} giây
📅 <b>Created:</b> {data['create_time']}
🔎 <b>Key:</b> <code>{data.get('key', 'Link Direct')}</code>
{data.get('description', 'Cực chất!')}
</blockquote>
"""
    try:
        bot.edit_message_text("⬆️ Gần Xong Rồi!...", chat_id, msg_wait.message_id)
        with open(result, 'rb') as v:
            bot.send_video(
                chat_id, v, 
                caption=caption_text, 
                parse_mode="HTML",
                reply_to_message_id=reply_id,
                reply_markup=markup,
                timeout=60
            )
        bot.delete_message(chat_id, msg_wait.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Lỗi gửi Telegram: {str(e)}\n🔗 Link xem tạm: {data['url']}", chat_id, msg_wait.message_id)
    finally:
        if os.path.exists(result):
            os.remove(result)

def process_music_search(message, query):
    msg = bot.reply_to(message, f"🔍 <b>Searching: {html.escape(query)}...</b>", parse_mode="HTML")
    try:
        limit = 10
        results, total_count = sc_search_tracks(query, limit=limit, offset=0)
        bot.delete_message(message.chat.id, msg.message_id)
    except Exception as e:
        bot.delete_message(message.chat.id, msg.message_id)
        reply_vip(message, "Bình Tĩnh", f"⚠️ Api Lỏm: {str(e)}")
        return

    if not results:
        reply_vip(message, "Làm Lồn Gì Có", f"😿 Bố Ạ Mày: <b>{esc(query)}</b>")
        return

    lines = []
    for idx, t in enumerate(results, start=1):
        title = esc(t.get("title", ""))
        artist = esc(t.get("user", "Unknown"))
        dur = ms_to_mmss(t.get("duration", 0))
        plays = fmt_int(t.get("playback_count", 0))
        likes = fmt_int(t.get("likes_count", 0))
        
        lines.append(f"<b>{idx}.</b> 🎵 {title}")
        lines.append(f"   👤 <i>{artist}</i> | 🕒 {dur}")
        lines.append(f"   ❤️ {likes} | 🎧 {plays}")
        lines.append(f"   ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")

    content = "\n".join(lines)
    content += "\n\n💡 <b>Reply theo số thứ tự bài mày muốn💗!</b>"
    
    sent = reply_vip(message, f"Tao Thấy🤖: {fmt_int(total_count)} Bài", content)
    SEARCH_CONTEXT[sent.message_id] = results
    CONTEXT_TIMESTAMP[sent.message_id] = time.time()

def process_voice_extract(message):
    chat_id = message.chat.id
    target_message = message.reply_to_message
    if target_message is None or target_message.content_type != 'video':
        bot.reply_to(message, "⚠️ Bạn cần Reply (trả lời) vào một video để tách âm thanh.")
        return

    msg_loading = bot.reply_to(message, "⏳ Đã nhận lệnh! Đang tải video từ tin nhắn gốc...")

    try:
        file_info = bot.get_file(target_message.video.file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        video_filename = f"video_{target_message.message_id}.mp4"
        audio_filename = f"audio_{target_message.message_id}.mp3"

        with open(video_filename, 'wb') as new_file:
            new_file.write(downloaded_file)
        bot.edit_message_text(f"⚙️ Đang tách âm thanh...", chat_id, msg_loading.message_id)        
        video_clip = VideoFileClip(video_filename)        
        if video_clip.audio is None:
            bot.edit_message_text("❌ Video gốc không có âm thanh!", chat_id, msg_loading.message_id)
            video_clip.close()
            os.remove(video_filename)
            return

        video_clip.audio.write_audiofile(audio_filename, verbose=False, logger=None)
        video_clip.close()
        bot.edit_message_text(f"⬆️ Đang upload...", chat_id, msg_loading.message_id)
        
        with open(audio_filename, 'rb') as audio:
            bot.send_audio(
                chat_id, 
                audio, 
                reply_to_message_id=target_message.message_id, 
                title="Extracted Audio", 
                caption="✅ Đã tách xong!"
            )
        bot.delete_message(chat_id, msg_loading.message_id)

    except Exception as e:
        bot.reply_to(message, f"❌ Lỗi: {str(e)}")
        print(e)

    finally:
        if os.path.exists(video_filename):
            os.remove(video_filename)
        if os.path.exists(audio_filename):
            os.remove(audio_filename)

@bot.message_handler(commands=['start', 'help', 'menu'])
def start(message: types.Message):
    is_admin = message.from_user.id == ADMIN_ID
    menu = """
<b>🎵Music Good🎵</b>

✨ <b>Music Search:</b>
<code>/scl [song name]</code> - High Quality Audio
Hoặc gõ: <code>nhạc [tên]</code>, <code>play [tên]</code>

🎥 <b>TikTok Svc:</b>
Send TikTok Link -> Auto Download No Watermark.
Hoặc gõ: <code>tiktok [từ khóa]</code>, <code>video [từ khóa]</code>

🎙 <b>Voice Extractor:</b>
Reply video với <code>/crvoice</code> -> Get Audio.
Hoặc Reply video gõ: <code>tách</code>, <code>lấy nhạc</code>, <code>mp3</code>

🤖 <b>Gemini AI:</b>
Send text or photo to chat directly.

⚙️ <b>Control:</b>
Use the menu below.
"""
    kb = InlineKeyboardMarkup()
    if is_admin:
        kb.add(InlineKeyboardButton("🛠 AI Config", callback_data="open_ai_menu"))
    bot.reply_to(message, ui_frame("DASHBOARD", message.from_user, menu, is_admin), parse_mode="HTML", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "open_ai_menu")
def menu_ai_open(call):
    if call.from_user.id != ADMIN_ID: return
    bot.edit_message_text(
        ui_frame("AI CONFIG", call.from_user, "Select Model:", True),
        call.message.chat.id, call.message.message_id,
        parse_mode="HTML", reply_markup=build_ai_menu()
    )

@bot.callback_query_handler(func=lambda c: c.data.startswith("setmodel:"))
def set_model(call):
    if call.from_user.id != ADMIN_ID: return
    global CURRENT_MODEL
    CURRENT_MODEL = call.data.split(":")[1]
    bot.answer_callback_query(call.id, f"Model set to {CURRENT_MODEL}")
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=build_ai_menu())

@bot.callback_query_handler(func=lambda c: c.data == "close_menu")
def close_menu(call):
    bot.delete_message(call.message.chat.id, call.message.message_id)

@bot.message_handler(commands=["scl"])
def cmd_scl(message: types.Message):
    cleanup_context()
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        reply_vip(message, "LỖI CÚ PHÁP", "⚠️ Vui lòng nhập tên bài hát!\n👉 Ví dụ: <code>/scl son tung mtp</code>")
        return
    query = parts[1].strip()
    process_music_search(message, query)

@bot.message_handler(commands=['crvoice'])
def cmd_convert_voice(message):
    process_voice_extract(message)

@bot.message_handler(content_types=['text', 'photo'])
def master_handler(message):
    is_admin = message.from_user.id == ADMIN_ID
    text = message.text.strip() if message.text else ""
    text_lower = text.lower()

    if message.reply_to_message and message.reply_to_message.message_id in SEARCH_CONTEXT:
        if re.fullmatch(r"\d{1,2}", text):
            handle_number_reply(message)
            return

    if message.reply_to_message and message.reply_to_message.content_type == 'video':
        for trigger in TRIGGERS_VOICE:
             if trigger in text_lower:
                 process_voice_extract(message)
                 return

    if "tiktok.com" in text:
        if "search" in text:
             try:
                parsed = urlparse(text)
                qs = parse_qs(parsed.query)
                if 'q' in qs:
                    k = unquote(qs['q'][0])
                    bot.reply_to(message, f"🔎 Tìm kiếm TikTok: {k}")
                    data = tikwm_api(k, mode='search')
                    process_and_send_tiktok(message.chat.id, data, message.message_id)
                    return
             except: pass
        else:
            data = tikwm_api(text, mode='convert')
            process_and_send_tiktok(message.chat.id, data, message.message_id)
            return

    for trigger in TRIGGERS_TIKTOK_SEARCH:
        if text_lower.startswith(trigger + " "):
            query = text[len(trigger):].strip()
            if query:
                bot.reply_to(message, f"🔎 Smart Video Search: {query}")
                data = tikwm_api(query, mode='search')
                process_and_send_tiktok(message.chat.id, data, message.message_id)
                return

    for trigger in TRIGGERS_MUSIC:
        if text_lower.startswith(trigger + " "):
            query = text[len(trigger):].strip()
            if query:
                process_music_search(message, query)
                return

    if message.photo:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        res = ai_reply(message.caption, img_data, is_admin)
        bot.reply_to(message, ui_frame("AI VISION", message.from_user, res, is_admin), parse_mode="HTML")
        return

    if text:
        res = ai_reply(text, is_admin=is_admin)
        bot.reply_to(message, ui_frame("Tao Là Gemini", message.from_user, res, is_admin), parse_mode="HTML")

def handle_number_reply(message):
    cleanup_context()
    replied = message.reply_to_message
    if replied.message_id not in SEARCH_CONTEXT:
        return
    tracks = SEARCH_CONTEXT.get(replied.message_id, [])
    try:
        n = int(message.text.strip())
    except ValueError: return
    
    if not (1 <= n <= len(tracks)):
        bot.reply_to(message, "⚠️ Biết đếm không?!")
        return
        
    bot.send_chat_action(message.chat.id, 'typing')
    chosen = tracks[n - 1]
    
    try:
        detail = sc_track_detail(chosen.get("id"))
        for k in ("permalink_url", "title", "duration", "artwork_url", "user", "likes_count", "playback_count", "genre", "created_at"):
            if not detail.get(k) and chosen.get(k):
                detail[k] = chosen[k]
    except Exception:
        detail = chosen

    title = esc(detail.get("title", ""))
    user_name = esc(detail.get("user", "Unknown"))
    dur_str = ms_to_mmss(detail.get("duration", 0))
    likes = fmt_int(detail.get("likes_count", 0))
    plays = fmt_int(detail.get("playback_count", 0))
    genre = esc(detail.get("genre", "Unknown"))
    date = esc(detail.get("created_at", "Unknown"))
    
    link = detail.get("permalink_url") or "https://soundcloud.com/"
    art_url = best_artwork(detail.get("artwork_url"))

    caption = (
    f"<blockquote>"
    f"💿 <b>Playing Music</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"🎵 <b>Music:</b> {title}\n"
    f"👤 <b>Author:</b> {user_name}\n"
    f"⏱ <b>Duration:</b> {dur_str}\n"
    f"❤️ <b>Likes:</b> {likes} | 🎧 <b>Plays:</b> {plays}\n"
    f"🔥 <b>Genre:</b> {genre} | 📅 <b>Date:</b> {date}\n"
    f"🔗 <a href=\"{link}\">Nghe trên SoundCloud</a>"
    f"</blockquote>"
)

    kb = build_player_keyboard(playing=True)
    
    try:
        if art_url:
            sent_msg = bot.send_photo(message.chat.id, photo=art_url, caption=caption, reply_markup=kb, parse_mode="HTML")
        else:
            sent_msg = bot.reply_to(message, caption, reply_markup=kb, parse_mode="HTML")
    except Exception:
        sent_msg = bot.reply_to(message, caption, reply_markup=kb, parse_mode="HTML")

    set_player_state(sent_msg.chat.id, sent_msg.message_id, {
        "results": tracks, "index": n - 1, "playing": True, "detail": detail
    })
    
    bot.send_chat_action(message.chat.id, 'upload_audio')
    audio_path = None
    
    try:
        prog_url = sc_resolve_progressive_url(detail["id"], permalink_url=link)
        if not prog_url:
            bot.reply_to(message, "⚠️ Api lon lỏm rồi")
            return
            
        audio_path = download_to_temp(prog_url, suffix=".mp3")
        
        with open(audio_path, "rb") as audio:
            bot.send_audio(
                chat_id=message.chat.id, 
                audio=audio, 
                caption=f"📥 <b>{title}</b>\nDone✅",
                performer=user_name,
                title=title,
                duration=int(detail.get("duration", 0)/1000),
                parse_mode="HTML"
            )
    except Exception as e:
        bot.reply_to(message, f"⚠️ Lỗi tải xuống: {str(e)}")
    finally:
        if audio_path: pathlib.Path(audio_path).unlink(missing_ok=True)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("player:"))
def player_callback(call: types.CallbackQuery):
    data = call.data.split(":", 1)[1]
    chat_id = call.message.chat.id
    msg_id = call.message.message_id
    state = get_player_state(chat_id, msg_id)
    
    if not state and data != "close":
        bot.answer_callback_query(call.id, "⚠️ Ditt me dang hay thi het phiên!", show_alert=True)
        return

    if data == "close":
        bot.delete_message(chat_id, msg_id)
        pop_player_state(chat_id, msg_id)
        return

    if data == "toggle":
        playing = not state.get("playing", True)
        state["playing"] = playing
        set_player_state(chat_id, msg_id, state)
        new_kb = build_player_keyboard(playing=playing)
        bot.edit_message_reply_markup(chat_id, msg_id, reply_markup=new_kb)
        bot.answer_callback_query(call.id, "⏯ " + ("Đang phát" if playing else "Tạm dừng"))

    elif data in ("prev", "next"):
        results = state.get("results", [])
        idx = int(state.get("index", 0))
        new_idx = (idx + 1) % len(results) if data == "next" else (idx - 1) % len(results)
        
        new_choice = results[new_idx]
        detail = sc_track_detail(new_choice.get("id"))
        if not detail.get("title"): detail.update(new_choice)
        
        state["index"] = new_idx
        state["detail"] = detail
        state["playing"] = True
        set_player_state(chat_id, msg_id, state)

        title = esc(detail.get("title", ""))
        user_name = esc(detail.get("user", "Unknown"))
        dur_str = ms_to_mmss(detail.get("duration", 0))
        likes = fmt_int(detail.get("likes_count", 0))
        plays = fmt_int(detail.get("playback_count", 0))
        genre = esc(detail.get("genre", "Unknown"))
        date = esc(detail.get("created_at", "Unknown"))
        link = detail.get("permalink_url") or "https://soundcloud.com/"
        art_url = best_artwork(detail.get("artwork_url"))

        new_caption = (
    f"<blockquote>"
    f"💿 <b>Now Playing Premium</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"🎵 <b>Music:</b> {title}\n"
    f"👤 <b>Author:</b> {user_name}\n"
    f"⏱ <b>Duration:</b> {dur_str}\n"
    f"❤️ <b>Likes:</b> {likes} | 🎧 <b>Plays:</b> {plays}\n"
    f"🔥 <b>Genre:</b> {genre} | 📅 <b>Date:</b> {date}\n"
    f"🔗 <a href=\"{link}\">Nghe trên SoundCloud</a>"
    f"</blockquote>"
)
        
        media = InputMediaPhoto(media=art_url, caption=new_caption, parse_mode="HTML") if art_url else None
        kb = build_player_keyboard(True)
        
        try:
            if media:
                bot.edit_message_media(media=media, chat_id=chat_id, message_id=msg_id, reply_markup=kb)
            else:
                bot.edit_message_caption(caption=new_caption, chat_id=chat_id, message_id=msg_id, parse_mode="HTML", reply_markup=kb)
            bot.answer_callback_query(call.id, f"🎵 Chuyển bài: {title[:20]}...")
        except Exception:
            pass
            
    elif data == "fav":
        bot.answer_callback_query(call.id, "❤️ Đã thêm vào danh sách yêu thích!")

@bot.callback_query_handler(func=lambda call: call.data.startswith('next|'))
def handle_next_tiktok(call):
    keyword = call.data.split('|')[1]
    bot.answer_callback_query(call.id, "OK, đợi xíu...")
    data = tikwm_api(keyword, mode='search')
    process_and_send_tiktok(call.message.chat.id, data)

if __name__ == "__main__":
    if not check_internet_connection():
        sys.exit(1)
    threading.Thread(target=run_web, daemon=True).start()
    print("🚀 All-in-One Bot (Gemini + SC + TikTok + Voice) Is Running...")
    bot.infinity_polling(skip_pending=True)
    
    
#

import asyncio
import os
import sys
import json
import re
import time
import uuid
import logging
import random
import string
import datetime
import traceback
import platform
import hashlib
from typing import Dict, List, Optional, Any, Union
from urllib.parse import unquote, urlparse
from io import BytesIO
from flask import Flask
import telebot
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from aiohttp import web
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, ForeignKey, BigInteger, Float, JSON, Enum
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, scoped_session
from sqlalchemy.sql import func
from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError as PWTimeoutError, Error as PWError
from PIL import Image, ImageEnhance, ImageFilter, ImageOps

SYS_CONF_TOKEN = "8468137663:AAEPQr8XrMLwWpS5CU1ejXDj6ypMhD-eFV4"
SYS_CONF_COOKIE = """vpd=v1%3B582x360x2; pas=61560503393881%3AKPzS28Ooza; fr=0CxsrPwoNcHMtvyWT.AWfoLEvGwmhEwHdTLedt5tiuzaU--I0KuJ0Upzu7UD1K6KwxIrg.BpIKbC..AAA.0.0.BpIKd3.AWcxT48f6G4UcN9mkahlxCSqwDQ; m_pixel_ratio=2; locale=vi_VN; fbl_st=101718972%3BT%3A29395795; xs=17%3AL49MrIAnigo9Xw%3A2%3A1763747668%3A-1%3A-1; wl_cbv=v2%3Bclient_version%3A2990%3Btimestamp%3A1763747703; c_user=61560503393881; sb=wqYgaS_dGjvOkzzpqUQ3ZMZA; wd=360x806; datr=wqYgaSl9Z85GJSoGp-dyuw0D;"""
SYS_ADMIN_NAME = "Duckiencoder"
SYS_CHANNEL = "@tg_mediavip"
SYS_DB_FILE = "sqlite:///leviathan_injection_v11.db"
SYS_HOST = "0.0.0.0"
SYS_PORT = 8080

UA_DATASET_WINDOWS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/125.0.0.0 Safari/537.36"
]

UA_DATASET_MAC = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0"
]

CSS_REALISM_PATCH = """
    div[aria-label='Chat'], 
    div[aria-label='Messenger'], 
    div[aria-label='Close'],
    div[data-pagelet='StickyHeader'], 
    .fixed_elem,
    div[role='complementary'],
    div[aria-label='Tạo bài viết'],
    .video_call_button,
    div[aria-label='Nhắn tin'],
    div[aria-label='Gửi tin nhắn'],
    div[class*='fixed'],
    div[data-pagelet='RightRail'],
    iframe,
    .fbChatTypeahead,
    div[aria-label='Tạo tin'],
    div[aria-label='Create Story']
    { display: none !important; opacity: 0 !important; visibility: hidden !important; }
    
    body { 
        overflow-x: hidden !important; 
        background-color: #F0F2F5 !important;
    }

    div[role='main'] {
        margin: 0 auto !important;
        width: 100% !important;
        max-width: 100% !important;
        display: flex !important;
        justify-content: center !important;
        padding-top: 10px !important;
    }

    div[role='banner'] {
        display: block !important;
        opacity: 1 !important;
        visibility: visible !important;
        position: sticky !important;
        top: 0;
        z-index: 999;
    }

    div[data-pagelet='LeftRail'], div[data-pagelet='RightRail'] {
        display: none !important;
    }
"""

logging.basicConfig(level=logging.ERROR)
Base = declarative_base()

class SystemException(Exception):
    pass

class UserProfile(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, index=True)
    username = Column(String(255))
    full_name = Column(String(255))
    vip_status = Column(Boolean, default=False)
    total_requests = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_active = Column(DateTime(timezone=True), onupdate=func.now())

class RequestAudit(Base):
    __tablename__ = 'request_audits'
    id = Column(Integer, primary_key=True)
    trace_id = Column(String(64), unique=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    target_url = Column(Text)
    status_code = Column(String(50))
    latency_ms = Column(Float)
    meta_data = Column(JSON)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())

class DataLayer:
    def __init__(self):
        self.engine = create_engine(
            SYS_DB_FILE, 
            connect_args={'check_same_thread': False}, 
            pool_size=20, 
            max_overflow=30,
            pool_recycle=3600
        )
        Base.metadata.create_all(self.engine)
        self.Session = scoped_session(sessionmaker(bind=self.engine, expire_on_commit=False))

    def get_session(self):
        return self.Session()

    def close_session(self):
        self.Session.remove()

    def register_user(self, tg_user):
        s = self.get_session()
        try:
            u = s.query(UserProfile).filter_by(telegram_id=tg_user.id).first()
            if not u:
                u = UserProfile(
                    telegram_id=tg_user.id, 
                    username=tg_user.username, 
                    full_name=tg_user.first_name
                )
                s.add(u)
            else:
                u.username = tg_user.username
                u.full_name = tg_user.first_name
                u.last_active = func.now()
            s.commit()
            return u
        except Exception:
            s.rollback()
            return None
        finally:
            self.close_session()

    def log_transaction(self, user_id, url, trace, status, time_ms, metadata=None):
        s = self.get_session()
        try:
            u = s.query(UserProfile).filter_by(telegram_id=user_id).first()
            if u:
                u.total_requests += 1
                log = RequestAudit(
                    trace_id=trace,
                    user_id=u.id,
                    target_url=url,
                    status_code=status,
                    latency_ms=time_ms,
                    meta_data=metadata or {}
                )
                s.add(log)
                s.commit()
        except Exception:
            s.rollback()
        finally:
            self.close_session()

class StringUtils:
    @staticmethod
    def generate_trace():
        return hashlib.md5(str(uuid.uuid4()).encode()).hexdigest()[:12].upper()

    @staticmethod
    def sanitize_url(url: str) -> str:
        url = url.strip()
        if not url.startswith("http"):
            url = "https://" + url
        parsed = urlparse(url)
        if "facebook.com" not in parsed.netloc and "fb.com" not in parsed.netloc:
            raise ValueError("INVALID_DOMAIN")
        return url

    @staticmethod
    def parse_cookies(raw_cookie: str) -> List[Dict]:
        cookies = []
        if not raw_cookie: return cookies
        try:
            decoded = unquote(raw_cookie)
            for part in decoded.split(';'):
                if '=' in part:
                    k, v = part.strip().split('=', 1)
                    cookies.append({
                        'name': k.strip(), 
                        'value': v.strip(), 
                        'domain': '.facebook.com',
                        'path': '/', 
                        'secure': True, 
                        'httpOnly': False, 
                        'sameSite': 'Lax'
                    })
        except Exception:
            pass
        return cookies

class BrowserConfig:
    VIEWPORT = {'width': 1536, 'height': 864}
    TIMEOUT_MS = 60000
    LOCALE = 'vi-VN'
    TIMEZONE = 'Asia/Ho_Chi_Minh'

class StealthMechanic:
    @staticmethod
    async def apply(context: BrowserContext):
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en-US', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            window.chrome = { runtime: {} };
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                Promise.resolve({ state: 'denied' }) :
                originalQuery(parameters)
            );
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) return 'Intel Inc.';
                if (parameter === 37446) return 'Intel(R) Iris(R) Xe Graphics';
                return getParameter(parameter);
            };
        """)

class DOMArchitect:
    @staticmethod
    async def reconstruct_layout(page: Page):
        await page.add_style_tag(content=CSS_REALISM_PATCH)
        await page.evaluate("""
            () => {
                try {
                    document.body.style.zoom = '0.9';
                    
                    const dialogs = document.querySelectorAll('div[role="dialog"]');
                    dialogs.forEach(d => d.remove());
                    
                    const overlays = document.querySelectorAll('div[class*="overlay"]');
                    overlays.forEach(o => o.remove());
                    
                    const scrollWrapper = document.querySelector('div[data-pagelet="ProfileTiles"]')?.parentElement?.parentElement;
                    if(scrollWrapper) {
                        scrollWrapper.style.justifyContent = "center";
                    }
                } catch(e) {}
            }
        """)

    @staticmethod
    async def inject_vip_elements(page: Page):
        await page.evaluate("""
            () => {
                const target = document.querySelector("div[data-pagelet='ProfileTiles']");
                if(target){
                    const container = document.createElement('div');
                    container.style.marginBottom = "15px";
                    container.style.background = "#fff";
                    container.style.borderRadius = "8px";
                    container.style.boxShadow = "0 1px 2px rgba(0,0,0,0.1)";
                    container.style.padding = "15px";
                    
                    container.innerHTML = `
                        <div style="font-family: Segoe UI, Helvetica, Arial, sans-serif;">
                            <div style="font-size: 20px; font-weight: 700; margin-bottom: 12px; color: #050505;">Giới thiệu</div>
                            <div style="margin-bottom: 10px; font-size: 15px; display: flex; align-items: center;">
                                <span style="margin-right: 8px;">Platform Services 💸 :</span>
                                <a href="#" style="color:#0064d1; font-weight:600; text-decoration: none;">subgiagoc.com</a> 
                                <span style="margin-left: 5px;">📈</span>
                            </div>
                            <div style="margin-bottom: 10px; font-size: 15px; display: flex; align-items: center;">
                                <span style="margin-right: 8px;">Code Service 💸 :</span>
                                <a href="#" style="color:#0064d1; font-weight:600; text-decoration: none;">hoccodeai.com</a> 
                                <span style="margin-left: 5px;">📄</span>
                            </div>
                            <div style="margin-bottom: 10px; font-size: 15px; display: flex; align-items: center;">
                                <span style="margin-right: 8px;">Api Tool Service 💸 :</span>
                                <span style="font-weight:600;">_ inbox</span> 
                                <span style="margin-left: 5px;">📞</span>
                            </div>
                            <div style="margin-top: 15px; padding-top: 15px; border-top: 1px solid #ced0d4;">
                                <div style="display: flex; align-items: center; margin-bottom: 8px;">
                                    <img src="https://static.xx.fbcdn.net/rsrc.php/v3/yC/r/yBqJ3N3Xq-A.png" width="20" height="20" style="margin-right: 10px; filter: invert(0.4);">
                                    <span style="font-size: 15px; color: #65676b;">Trang cá nhân · Người sáng tạo nội dung số</span>
                                </div>
                                <div style="display: flex; align-items: center;">
                                    <img src="https://static.xx.fbcdn.net/rsrc.php/v3/yE/r/47650024480.png" width="20" height="20" style="margin-right: 10px; filter: invert(0.4);">
                                    <span style="font-size: 15px; color: #65676b;">Sống tại <b style="color: #050505;">Ha-Nam, Hà Nam, Vietnam</b></span>
                                </div>
                            </div>
                        </div>
                    `;
                    
                    target.prepend(container);
                }
            }
        """)
        await asyncio.sleep(0.5)

    @staticmethod
    async def cinematic_scroll(page: Page):
        await page.evaluate("""
            async () => {
                await new Promise((resolve) => {
                    let totalHeight = 0;
                    let distance = 200;
                    let timer = setInterval(() => {
                        let scrollHeight = document.body.scrollHeight;
                        window.scrollBy(0, distance);
                        totalHeight += distance;
                        if(totalHeight >= 1000){ 
                            clearInterval(timer);
                            window.scrollTo(0, 0);
                            resolve();
                        }
                    }, 100);
                });
            }
        """)
        await asyncio.sleep(1.5)

class DataHarvester:
    @staticmethod
    async def extract_real_meta(page: Page) -> Dict[str, Any]:
        return await page.evaluate("""
            () => {
                let d = {
                    title: document.title,
                    verified: false,
                    followers: '0',
                    bio: '',
                    meta_list: [],
                    external_links: []
                };
                try {
                    const h1 = document.querySelector('h1');
                    if(h1) {
                        d.title = h1.innerText;
                        const svg = h1.parentElement.querySelector('svg[aria-label="Đã xác minh"], svg[aria-label="Verified"]');
                        if(svg) d.verified = true;
                    }
                    const bioNode = document.querySelector("div[data-pagelet='ProfileTiles'] span[dir='auto']");
                    if(bioNode) d.bio = bioNode.innerText;
                    const tiles = document.querySelector("div[data-pagelet='ProfileTiles']");
                    if(tiles) {
                         const lines = tiles.querySelectorAll("span, div[dir='auto']");
                         lines.forEach(line => {
                            const txt = line.innerText;
                            if(!txt) return;
                            
                            const low = txt.toLowerCase();
                            
                            if (low.includes('người theo dõi') || low.includes('followers')) {
                                d.followers = txt.replace(/[^0-9.,KkMm]/g, '');
                            }
                            else if (txt.includes('.') && !txt.includes(' ') && txt.length > 4) {
                                d.external_links.push(txt);
                            }
                            else if (txt.length > 5 && !txt.includes('http') && !txt.includes('Instagram')) {
                                if (!d.meta_list.includes(txt) && !d.title.includes(txt)) {
                                     d.meta_list.push(txt);
                                }
                            }
                         });
                    }
                    
                    d.external_links.push('subgiagoc.com');
                    d.external_links.push('hoccodeai.com');

                } catch(e) {}
                return d;
            }
        """)

class VisualRenderer:
    @staticmethod
    def process_realistic_image(image_bytes: bytes) -> BytesIO:
        img = Image.open(BytesIO(image_bytes))
        img = img.convert("RGB")
        
        width, height = img.size
        target_height = int(width * 0.65) 
        
        if height > target_height:
            img = img.crop((0, 0, width, target_height))
                    
        enhancer = ImageEnhance.Sharpness(img)
        img = enhancer.enhance(1.3)
        
        color_enhancer = ImageEnhance.Color(img)
        img = color_enhancer.enhance(1.1)
        
        bio = BytesIO()
        img.save(bio, format='JPEG', quality=100, optimize=True, subsampling=0)
        bio.seek(0)
        return bio

class CoreEngine:
    def __init__(self):
        self.cookies = StringUtils.parse_cookies(SYS_CONF_COOKIE)
        
    async def execute_mission(self, url: str, trace: str) -> Dict[str, Any]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--window-size=1920,1080', 
                    '--force-device-scale-factor=1.5',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            
            platform_os = platform.system()
            if platform_os == 'Darwin':
                ua = random.choice(UA_DATASET_MAC)
            else:
                ua = random.choice(UA_DATASET_WINDOWS)
                
            context = await browser.new_context(
                viewport=BrowserConfig.VIEWPORT,
                user_agent=ua,
                locale=BrowserConfig.LOCALE,
                timezone_id=BrowserConfig.TIMEZONE,
                device_scale_factor=1.25,
                has_touch=False,
                is_mobile=False
            )
            
            await StealthMechanic.apply(context)
            if self.cookies:
                await context.add_cookies(self.cookies)
                
            page = await context.new_page()
            page.set_default_timeout(BrowserConfig.TIMEOUT_MS)
            
            result = {
                "success": False,
                "payload": None,
                "screenshot": None,
                "error": None,
                "trace": trace,
                "meta": {"ua": ua}
            }
            
            try:
                await page.goto(url, wait_until='domcontentloaded')
                
                try:
                    await page.wait_for_selector('div[role="main"]', state='visible', timeout=15000)
                except: pass
                    
                if "login" in page.url or "checkpoint" in page.url:
                    raise AuthenticationException("COOKIE_EXPIRED_OR_CHECKPOINT")
                
                await DOMArchitect.reconstruct_layout(page)
                await DOMArchitect.inject_vip_elements(page)
                await DOMArchitect.cinematic_scroll(page)
                
                meta_data = await DataHarvester.extract_real_meta(page)
                
                screenshot_bytes = await page.screenshot(
                    full_page=False, 
                    type='jpeg', 
                    quality=100
                )
                
                processed_img = VisualRenderer.process_realistic_image(screenshot_bytes)
                
                result["payload"] = meta_data
                result["screenshot"] = processed_img
                result["success"] = True
                
            except AuthenticationException:
                result["error"] = "AUTH_FAIL"
            except Exception as e:
                result["error"] = str(e)
            finally:
                await context.close()
                await browser.close()
                
            return result

class MessageBuilder:
    @staticmethod
    def build_vip_response(data: Dict, url: str, trace: str, duration: float) -> str:
        name = data.get('title', 'Unknown').replace(' | Facebook', '').strip().upper()
        
        verified_mark = "☑️ UNVERIFIED"
        if data.get('verified'):
            verified_mark = "✅ VERIFIED IDENTITY"
            
        followers = data.get('followers', 'HIDDEN')
        bio = data.get('bio', '')
        if not bio: bio = "N/A"
        
        links = data.get('external_links', [])
        link_str = ""
        if links:
            for l in links:
                clean = l.replace('https://', '').replace('http://', '').replace('www.', '')
                link_str += f"  🔗 {clean}\n"
        else:
            link_str = "  🔒 No External Links"
            
        meta_info = data.get('meta_list', [])
        meta_str = ""
        
        valid_meta = [m for m in meta_info if len(m) > 3][:5]
        if valid_meta:
            for m in valid_meta:
                meta_str += f"  ▫️ {m}\n"
        else:
            meta_str = "  ▫️ Limited Information"

        return (
            f"<blockquote>"
            f"<b>💠 REAL-TIME TARGET INTELLIGENCE</b>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"👤 <b>IDENTITY</b>\n"
            f"  NAME: <code>{name}</code>\n"
            f"  STATUS: <b>{verified_mark}</b>\n"
            f"  AUDIENCE: <code>{followers}</code>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"🧬 <b>BIO-DATA</b>\n"
            f"  <i>{bio}</i>\n\n"
            f"📂 <b>DETAILS</b>\n"
            f"{meta_str}\n"
            f"🌐 <b>DIGITAL FOOTPRINT</b>\n"
            f"{link_str}"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"📡 <b>SYSTEM METRICS</b>\n"
            f"  🎯 LINK: <a href='{url}'>Direct Access</a>\n"
            f"  ⚡ TIME: <code>{duration:.3f}s</code>\n"
            f"  🆔 TRACE: <code>{trace}</code>\n"
            f"  💻 MODE: <b>Native Desktop (1920x1080)</b>\n"
            f"  👑 ADMIN: <b>{SYS_ADMIN_NAME}</b>\n"
            f"</blockquote>"
        )

class CommandHandler:
    def __init__(self, bot: AsyncTeleBot, db: DataLayer, core: CoreEngine):
        self.bot = bot
        self.db = db
        self.core = core
        
    async def on_start(self, message):
        self.db.register_user(message.from_user)
        markup = types.InlineKeyboardMarkup()
        btn = types.InlineKeyboardButton("💎 PREMIUM CHANNEL", url="https://t.me/tg_mediavip")
        markup.add(btn)
        
        txt = (
            f"<blockquote>"
            f"<b>🛡️ LEVIATHAN V11 - INJECTION MODE</b>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"👋 Welcome, <b>{message.from_user.first_name}</b>\n"
            f"✅ Rendering Engine: Native Desktop + UI Injection.\n"
            f"✅ Data Extraction: Deep Scan.\n\n"
            f"💠 <b>USAGE:</b>\n"
            f"Send target URL for Full HD extraction.\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"</blockquote>"
        )
        await self.bot.reply_to(message, txt, reply_markup=markup)
        
    async def on_message(self, message):
        txt = message.text.strip()
        user_id = message.from_user.id
        self.db.register_user(message.from_user)
        
        try:
            url = StringUtils.sanitize_url(txt)
        except ValueError:
            return 
            
        trace = StringUtils.generate_trace()
        
        init_msg = await self.bot.reply_to(
            message, 
            f"<blockquote>📡 <b>ESTABLISHING CONNECTION...</b>\nTarget: {url}\nTrace: {trace}</blockquote>"
        )
        
        start_ts = time.time()
        
        try:
            await asyncio.sleep(random.uniform(0.5, 1.0))
            await self.bot.edit_message_text(
                f"<blockquote>🔄 <b>INJECTING ASSETS...</b>\nRendering Viewport...</blockquote>", 
                chat_id=message.chat.id, 
                message_id=init_msg.message_id
            )
            
            data = await self.core.execute_mission(url, trace)
            end_ts = time.time()
            duration = end_ts - start_ts
            
            await self.bot.delete_message(message.chat.id, init_msg.message_id)
            
            if data['success']:
                self.db.log_transaction(user_id, url, trace, "SUCCESS", duration * 1000, data['meta'])
                caption = MessageBuilder.build_vip_response(data['payload'], url, trace, duration)
                await self.bot.send_photo(
                    message.chat.id, 
                    data['screenshot'], 
                    caption=caption
                )
            else:
                self.db.log_transaction(user_id, url, trace, f"FAIL_{data['error']}", duration * 1000)
                err_text = (
                    f"<blockquote>"
                    f"⛔ <b>ACCESS DENIED</b>\n"
                    f"▬▬▬▬▬▬▬▬▬▬\n"
                    f"⚠️ Reason: <code>{data['error']}</code>\n"
                    f"🆔 Trace: <code>{trace}</code>\n"
                    f"🔄 Check Cookie or Target URL."
                    f"</blockquote>"
                )
                await self.bot.send_message(message.chat.id, err_text)
                
        except Exception as e:
            traceback.print_exc()
            self.db.log_transaction(user_id, url, trace, "CRITICAL_ERROR", 0)
            await self.bot.send_message(message.chat.id, "⚠️ SERVER ERROR")

class WebServer:
    def __init__(self, bot_controller: CommandHandler):
        self.controller = bot_controller
        
    async def health_check(self, request):
        return web.Response(text=f"LEVIATHAN V11 RUNNING | {datetime.datetime.now()}")
        
    async def start(self):
        app = web.Application()
        app.router.add_get('/', self.health_check)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, SYS_HOST, SYS_PORT)
        await site.start()
        print(f"Server active on {SYS_HOST}:{SYS_PORT}")

async def main():
    bot = AsyncTeleBot(SYS_CONF_TOKEN, parse_mode="HTML")
    db = DataLayer()
    core = CoreEngine()
    handler = CommandHandler(bot, db, core)
    server = WebServer(handler)
    
    bot.message_handler(commands=['start'])(handler.on_start)
    bot.message_handler(func=lambda m: True)(handler.on_message)
    
    await server.start()
    print("SYSTEM ONLINE")
    await bot.polling(non_stop=True, request_timeout=60)

if __name__ == "__main__":
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        traceback.print_exc()
    
    
    
    
#4

import PIL
import asyncio
import concurrent.futures
import hashlib
import io
import json
import logging
import os
import random
import re
import string
import sqlite3
import time
import datetime
import pytz
import socket
import subprocess
import platform
from typing import Dict, Any, Optional, List, Union, Tuple, Set
from urllib.parse import urlparse # Thêm để phân tích URL

# --- Thư viện bên ngoài ---
import aiohttp
import requests
from telebot import types
from telebot.async_telebot import AsyncTeleBot
try:
    from bs4 import BeautifulSoup
    from fake_useragent import UserAgent
    _bs4_available = True
except ImportError:
    logging.warning("⚠️ THIẾU LIB: beautifulsoup4/fake_useragent. /getproxy hạn chế.")
    BeautifulSoup = None; UserAgent = None; _bs4_available = False
try:
    from gtts import gTTS
    import qrcode
    _tts_qr_available = True
except ImportError:
    logging.warning("⚠️ THIẾU LIB: gTTS/qrcode. /voice & /qrcode không hoạt động.")
    gTTS = None; qrcode = None; _tts_qr_available = False

# Thiết lập Logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s:%(lineno)d: %(message)s', datefmt='%H:%M:%S')

# ==============================================================================
# 2. CẤU HÌNH & THAM SỐ TOÀN DIỆN (PREMIUM VIP)
# ==============================================================================

# --- Cấu hình Bot & Admin ---
TELEGRAM_BOT_TOKEN: str = "8413179871:AAGR-mZMPrccK8aUIY1GUkWmwKrAymCz5lw"
ADMIN_IDS: List[int] = [7679054753, 6993504486]
OWNER_USERNAME: str = "tg_mediavip"
DB_FILE: str = "titan_ultimate_premium.db" # Đổi tên DB
LOG_FILE: str = "bot_usage_premium.log" # Đổi tên Log
REQUEST_TIMEOUT: int = 25 # Tăng timeout
VIETNAM_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

# --- Cấu hình Yêu cầu Mới ---
LOCAL_VIDEO_PATH: str = "vd.mp4" # Path video cục bộ
IPLOOKUP_API: str = "http://ip-api.com/json/{ip}?fields=status,message,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org,as,query"

# --- Cấu hình Kinh tế & Game ---
START_BALANCE: int = 10000
MIN_BET: int = 500 # Loại bỏ game bet, nhưng giữ lại phòng trường hợp dùng sau
BANK_INFO: str = "💰 Hướng dẫn nạp tiền:\n• Chủ TK: *NGUYEN TIEN DO*\n• Số TK: `68609666778899`\n• Ngân hàng: *MBBANK - QUÂN ĐỘI *"
QR_CODE_IMAGE_URL: str = "https://ibb.co/W4pcDM7Q"

# Lời cảm ơn
RANDOM_THANKS: List[str] = [
    "Chân thành cảm ơn bạn đã tin tưởng và đồng hành cùng chúng tôi!",
    "Lòng biết ơn sâu sắc vì sự hỗ trợ tuyệt vời của bạn. Giao dịch thành công!",
    "Cảm ơn! Sự ủng hộ của bạn là động lực lớn nhất của chúng tôi."
]

# --- Cấu hình API Tiện Ích ---
API_SEARCH_BASE: str = "https://bj-microsoft-search-ai.vercel.app/"
API_XOSO_URL: str = "https://nguyenmanh.name.vn/api/xsmb?apikey=OUEaxPOl"
API_ANH_GAI: str = "https://api.zeidteam.xyz/images/gai"
API_VD_GAI: str = "https://api.zeidteam.xyz/videos/gai"
API_FB_INFO: str = "https://api.zeidteam.xyz/facebook/info?uid={uid}"
API_TT_INFO: str = "https://api.zeidteam.xyz/tiktok/user-info?username={username}"
API_SCL_DOWN: str = "https://adidaphat.site/scl/download?url={url}"
API_NGL_SPAM: str = "https://adidaphat.site/ngl?username={username}&message={message}&amount={amount}"
# Public Proxy APIs
PROXY_APIS: List[str] = [
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
]

# --- Tham số MD5 LCG ---
LCG_MULTIPLIER: int = 1337
LCG_INCREMENT: int = 42069
LCG_MODULUS: int = 16**8

# ==============================================================================
# 3. KHỞI TẠO CÁC ĐỐI TƯỢNG TOÀN CỤC
# ==============================================================================

bot = AsyncTeleBot(TELEGRAM_BOT_TOKEN, parse_mode=None) # parse_mode=None để tránh xung đột với MarkdownV2 trong send_response
executor = concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() * 5 if os.cpu_count() else 30)

# ==============================================================================
# 4. HỆ THỐNG DATABASE
# ==============================================================================
# (Thêm bảng groups)
def blocking_db_execute(sql: str, params: tuple = ()) -> Optional[List[Any]]:
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10); c = conn.cursor()
        c.execute(sql, params); conn.commit()
        result = c.fetchall(); return result
    except sqlite3.Error as e: logging.error(f"Lỗi DB Execute: {e} - SQL: {sql}", exc_info=True); return None
    except Exception as e: logging.error(f"Lỗi DB Execute (khác): {e} - SQL: {sql}", exc_info=True); return None
    finally:
        if conn: conn.close()

def blocking_db_fetchone(sql: str, params: tuple = ()) -> Optional[tuple]:
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10); c = conn.cursor()
        c.execute(sql, params); result = c.fetchone(); return result
    except sqlite3.Error as e: logging.error(f"Lỗi DB Fetchone: {e} - SQL: {sql}", exc_info=True); return None
    except Exception as e: logging.error(f"Lỗi DB Fetchone (khác): {e} - SQL: {sql}", exc_info=True); return None
    finally:
        if conn: conn.close()

async def async_db_execute(sql: str, params: tuple = ()) -> Optional[List[Any]]:
    return await asyncio.to_thread(blocking_db_execute, sql, params)

async def async_db_fetchone(sql: str, params: tuple = ()) -> Optional[tuple]:
    return await asyncio.to_thread(blocking_db_fetchone, sql, params)

async def setup_database() -> None:
    # Bảng users (loại bỏ is_nv)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, balance INTEGER DEFAULT 0,
            is_admin BOOLEAN DEFAULT FALSE, is_approved BOOLEAN DEFAULT FALSE
        )
    """)
    await async_db_execute("CREATE TABLE IF NOT EXISTS tasks (task_id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT, reward INTEGER)")
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS nap_request (
            request_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount INTEGER,
            status TEXT DEFAULT 'pending', timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Bảng groups (Mới)
    await async_db_execute("""
        CREATE TABLE IF NOT EXISTS groups (chat_id INTEGER PRIMARY KEY)
    """)
    for admin_id in ADMIN_IDS:
        await async_db_execute(
            """INSERT INTO users (user_id, balance, is_admin, is_approved) VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET is_admin=excluded.is_admin, is_approved=excluded.is_approved""",
            (admin_id, 99999999, True, True)
        )
    logging.info("✅ Database đã được thiết lập thành công (có bảng groups).")

async def get_user(user_id: int, username: Optional[str] = None) -> Optional[Dict[str, Any]]: # Có thể trả về None nếu lỗi DB
    user_data = await async_db_fetchone("SELECT user_id, username, balance, is_admin, is_approved FROM users WHERE user_id = ?", (user_id,)) # Bỏ is_nv
    if user_data is None and sqlite3.Error not in [type(e) for e in asyncio.all_tasks()]: # Check if fetch failed vs user not found
        username = username if username else f"user_{user_id}"
        await async_db_execute("INSERT INTO users (user_id, username, balance, is_approved) VALUES (?, ?, ?, ?)", (user_id, username, 0, False))
        logging.info(f"Tạo người dùng mới: {user_id} - @{username}")
        return {"user_id": user_id, "username": username, "balance": 0, "is_admin": False, "is_approved": False} # is_nv bỏ đi
    elif user_data:
        return {"user_id": user_data[0], "username": user_data[1], "balance": user_data[2],
                "is_admin": bool(user_data[3]), "is_approved": bool(user_data[4])} # is_nv bỏ đi
    else: # Lỗi DB khi fetch
        return None

async def update_balance(user_id: int, amount: int) -> None:
    user_exists = await async_db_fetchone("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
    if user_exists:
        await async_db_execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
        logging.info(f"Updated balance for {user_id} by {amount}")
    else:
        logging.warning(f"Attempted to update balance for non-existent user: {user_id}")

# Hàm lấy danh sách group IDs (Mới)
async def get_all_group_ids() -> List[int]:
    groups_data = await async_db_execute("SELECT chat_id FROM groups")
    if groups_data is None: return [] # Handle DB error
    return [row[0] for row in groups_data]

def get_user_mention(user: types.User) -> str:
    if user.username: return f"@{user.username}"
    safe_name = escape_markdown_v2(user.first_name)
    return f"[{safe_name}](tg://user?id={user.id})"

# ==============================================================================
# 5. HÀM HỖ TRỢ LÕI & DECORATORS (PREMIUM VIP)
# ==============================================================================

def get_current_vietnam_time() -> str:
    return datetime.datetime.now(VIETNAM_TZ).strftime("%H:%M:%S | %d/%m/%Y")

def escape_markdown_v2(text: str) -> str:
    escape_chars = r'([_*\[\]()~`>#+-=|{}.!])'
    # Phải escape dấu \ trước
    text = str(text).replace('\\', '\\\\')
    return re.sub(escape_chars, r'\\\1', text)

async def send_response(message: types.Message, title: str, text: str, processing_msg: Optional[types.Message] = None) -> None:
    """Hàm gửi phản hồi VIP: Luôn gửi video vd.mp4 kèm caption."""
    current_time = get_current_vietnam_time()
    safe_title = escape_markdown_v2(title.upper())
    # Giới hạn caption để tránh lỗi Telegram (1024 chars)
    text_limit = 1000 - len(title) - len(current_time) - 100 # Ước lượng khoảng trống cho format
    safe_text = escape_markdown_v2(text[:text_limit] + ('...' if len(text) > text_limit else ''))
    safe_time = escape_markdown_v2(current_time)
    safe_owner = escape_markdown_v2(f"@{OWNER_USERNAME}")
    formatted_caption = (
        f"┏ 💎 *{safe_title}* ┓\n"
        f"┣{chr(8213)*20}\n"
        f"┣ {safe_text}\n"
        f"┣{chr(8213)*20}\n"
        f"┗ ⏱️ *{safe_time}* \\| Bot by {safe_owner}"
    )

    # --- Logic gửi Video ---
    video_sent_successfully = False
    if os.path.exists(LOCAL_VIDEO_PATH):
        try:
            if processing_msg:
                try: await bot.delete_message(processing_msg.chat.id, processing_msg.message_id)
                except Exception: pass # Ignore delete error

            with open(LOCAL_VIDEO_PATH, 'rb') as video_file:
                await bot.send_video(
                    chat_id=message.chat.id, video=video_file, caption=formatted_caption,
                    reply_to_message_id=message.message_id, parse_mode="MarkdownV2"
                )
            video_sent_successfully = True
        except Exception as e:
            logging.error(f"Lỗi gửi video cục bộ '{LOCAL_VIDEO_PATH}': {e}", exc_info=True)
            # Không cần xóa processing_msg ở đây vì sẽ gửi text fallback
    else:
        logging.warning(f"File video cục bộ '{LOCAL_VIDEO_PATH}' không tồn tại.")
        if processing_msg: # Xóa tin nhắn chờ nếu có
             try: await bot.delete_message(processing_msg.chat.id, processing_msg.message_id)
             except Exception: pass

    # --- Fallback gửi Text nếu gửi video lỗi HOẶC file video không tồn tại ---
    if not video_sent_successfully:
        logging.info("Gửi video thất bại hoặc file không tồn tại, gửi text thay thế.")
        error_prefix = f"⚠️ *Lỗi Video* \\(File `{escape_markdown_v2(LOCAL_VIDEO_PATH)}` lỗi hoặc không tồn tại\\)\n\n"
        fallback_text = error_prefix + formatted_caption # Giữ nguyên format text
        try:
            await bot.reply_to(message, fallback_text, parse_mode="MarkdownV2")
        except Exception as e_fallback:
            logging.error(f"Lỗi gửi Text fallback: {e_fallback}", exc_info=True)
            # Fallback cuối cùng nếu Markdown lỗi
            try: await bot.reply_to(message, f"{title}\n---\n{text[:4000]}\n---\n{current_time}")
            except Exception as e_final: logging.critical(f"Lỗi gửi fallback cuối cùng: {e_final}", exc_info=True)


def log_command(user_id: int, command: str, target: str) -> None:
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] User: {user_id} | Lệnh: {command} | Target: {target}\n"
        with open(LOG_FILE, "a", encoding="utf-8") as f: f.write(log_entry)
    except IOError as e: logging.warning(f"Không thể ghi log (IOError): {e}")
    except Exception as e: logging.warning(f"Không thể ghi log (khác): {e}")

# --- Decorators ---
def admin_required(func):
    async def wrapper(message: types.Message, *args, **kwargs): # Thêm *args, **kwargs
        user_info = await get_user(message.from_user.id)
        if not user_info:
             logging.error(f"Không thể lấy thông tin user {message.from_user.id} cho admin_required")
             await send_response(message, "LỖI HỆ THỐNG", "Không thể xác thực quyền Admin.")
             return
        if not user_info["is_admin"]:
            await send_response(message, "TRUY CẬP BỊ TỪ CHỐI", "Chức năng này chỉ dành cho Admin.")
            return
        # Truyền user_info nếu hàm gốc cần
        if 'user_info' in func.__code__.co_varnames:
            kwargs['user_info'] = user_info
        await func(message, *args, **kwargs)
    return wrapper

def approval_required(func):
    async def wrapper(message: types.Message, *args, **kwargs): # Thêm *args, **kwargs
        user_info = await get_user(message.from_user.id, message.from_user.username)
        if not user_info:
            logging.error(f"Không thể lấy thông tin user {message.from_user.id} cho approval_required")
            await send_response(message, "LỖI HỆ THỐNG", "Không thể xác thực quyền người dùng.")
            return
        if not user_info["is_approved"]:
            mention = get_user_mention(message.from_user)
            await send_response(message, "CHƯA ĐƯỢC DUYỆT", f"{mention}, bạn chưa được phép.\nLiên hệ Admin @{OWNER_USERNAME} (ID: `{message.from_user.id}`).")
            return
        try:
            # Truyền user_info nếu hàm gốc cần
            if 'user_info' in func.__code__.co_varnames:
                 kwargs['user_info'] = user_info
            await func(message, *args, **kwargs)
        except Exception as handler_error:
            logging.error(f"Lỗi trong handler {func.__name__}: {handler_error}", exc_info=True)
            await send_response(message, "LỖI XỬ LÝ LỆNH", f"Đã xảy ra lỗi. Thử lại hoặc báo Admin.\nLỗi: {type(handler_error).__name__}")
    return wrapper
# ==============================================================================
# 6. CÁC HÀM TÁC VỤ BLOCKING (CHO EXECUTOR)
# ==============================================================================

# --- MD5 & Tiện ích Cơ bản ---
# (Giữ nguyên)
def predict_md5_logic(md5_hash: str) -> Dict[str, Any]: #
    try:
        md5_hash = md5_hash.strip().lower() #
        if not re.fullmatch(r"^[0-9a-f]{32}$", md5_hash): return {"ok": False, "error": "Định dạng MD5 không hợp lệ."} #
        seed = int(md5_hash[:8], 16) #
        next_seed = (seed * LCG_MULTIPLIER + LCG_INCREMENT) % LCG_MODULUS #
        predicted_md5 = hashlib.md5(str(next_seed).encode()).hexdigest() #
        result_hex = predicted_md5[-8:] #
        value = int(result_hex, 16) #
        dice = [((value >> (i * 4)) % 6) + 1 for i in range(3)] #
        total = sum(dice); result = "TÀI" if total > 10 else "XỈU" #
        return {"ok": True, "predicted_md5": predicted_md5, "dice": dice, "total": total, "result": result, "seed_next": next_seed} #
    except Exception as e: return {"ok": False, "error": str(e)} #

def generate_qr_code_sync(text: str) -> Union[io.BytesIO, str]: #
    if not _tts_qr_available or not qrcode : return "⚠️ Thiếu thư viện `qrcode`." #
    try:
        qr_img = qrcode.make(text); buffer = io.BytesIO() #
        qr_img.save(buffer, format="PNG"); buffer.seek(0); return buffer #
    except Exception as e: return f"Lỗi tạo QR: {e}" #

def text_to_speech_sync(text: str) -> Union[io.BytesIO, str]: #
    if not _tts_qr_available or not gTTS: return "⚠️ Thiếu thư viện `gTTS`." #
    try:
        tts = gTTS(text=text[:250], lang='vi'); buffer = io.BytesIO() #
        tts.write_to_fp(buffer); buffer.seek(0); return buffer #
    except Exception as e: return f"Lỗi tạo Voice: {e}" #

def get_api_result_sync(url: str, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]: #
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers) #
        response.raise_for_status() #
        content_type = response.headers.get('Content-Type', '').lower()
        if 'application/json' in content_type: return response.json() #
        elif 'text/' in content_type: return {"status": True, "_content": response.text} # Check generic text type
        else: # Handle other content types
             logging.warning(f"API {url} trả về Content-Type không xác định: {content_type}")
             return {"status": True, "_content": response.text} # Treat as text
    except requests.exceptions.JSONDecodeError: return {"status": False, "message": f"API trả về không phải JSON. (Code: {response.status_code if 'response' in locals() else 'N/A'})"} #
    except requests.exceptions.RequestException as e: return {"status": False, "message": f"Lỗi kết nối API: {e}"} #
    except Exception as e: return {"status": False, "message": str(e)} #
# --- Tiện ích mạng ---
# (Giữ nguyên)
def check_tcp_port_sync(host: str, port: int, timeout: int = 5) -> Dict[str, Any]: #
    ip: Optional[str] = None
    try:
        ip = socket.gethostbyname(host)
        with socket.create_connection((ip, port), timeout=timeout): pass
        return {"ok": True, "status": "Mở (Open)", "ip": ip} #
    except socket.timeout: return {"ok": False, "status": "Đóng (Timeout)", "ip": ip} # Include IP if resolved
    except socket.gaierror: return {"ok": False, "status": "Lỗi (Không tìm thấy host)"} #
    except (socket.error, ConnectionRefusedError): return {"ok": False, "status": "Đóng (Refused)", "ip": ip} # Include IP if resolved
    except Exception as e: return {"ok": False, "status": f"Lỗi: {e}", "ip": ip} # Include IP if resolved

def check_dns_sync(host: str) -> Dict[str, Any]: #
    try:
        hostname, aliaslist, ipaddrlist = socket.gethostbyname_ex(host) #
        return {"ok": True, "hostname": hostname, "aliases": aliaslist, "ips": ipaddrlist} #
    except socket.gaierror: return {"ok": False, "error": "Không thể phân giải."} #
    except Exception as e: return {"ok": False, "error": str(e)} #

def lookup_ip_sync(ip: str) -> Dict[str, Any]: #
    try:
        data = get_api_result_sync(IPLOOKUP_API.format(ip=ip)) #
        if data.get("status") == "success": return {"ok": True, "data": data} #
        else: return {"ok": False, "error": data.get("message", "API lỗi")} #
    except Exception as e: return {"ok": False, "error": str(e)} #

def check_udp_port_sync(host: str, port: int, timeout: int = 3) -> Dict[str, Any]: #
    sock: Optional[socket.socket] = None #
    ip_addr: str = 'N/A' #
    try:
        ip_addr = socket.gethostbyname(host) #
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #
        sock.settimeout(timeout) #
        sock.sendto(b'ping', (ip_addr, port)) #
        data, addr = sock.recvfrom(1024) #
        return {"ok": True, "status": "Mở (Có phản hồi)", "ip": ip_addr} #
    except socket.timeout: return {"ok": True, "status": "Mở hoặc Bị chặn (Timeout)", "ip": ip_addr} #
    except socket.gaierror: return {"ok": False, "status": "Lỗi (Không tìm thấy host)"} #
    except (socket.error, ConnectionRefusedError) as e: return {"ok": False, "status": f"Đóng hoặc Lỗi ({type(e).__name__})", "ip": ip_addr} #
    except Exception as e: return {"ok": False, "status": f"Lỗi: {e}", "ip": ip_addr} #
    finally:
        if sock: sock.close() #

def ping_host_sync(host: str, count: int = 4) -> Dict[str, Any]: #
    param = '-n' if platform.system().lower() == 'windows' else '-c' #
    command = ['ping', param, str(count), host] #
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10, check=False, encoding='utf-8', errors='ignore') #
        output = result.stdout + result.stderr #
        ok = False #
        ip: str = host #
        status: str = "Thất bại (Không rõ)" #
        output_lower = output.lower() #
        ip_match = re.search(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", output) #
        if ip_match: ip = ip_match.group(1) #
        if platform.system().lower() == 'windows': #
            if "destination host unreachable" in output_lower or "could not find host" in output_lower: status = "Thất bại (Unreachable)" #
            elif "request timed out." in output_lower and ('100% loss' in output_lower or 'received = 0' in output_lower): status = "Thất bại (Timeout 100%)" #
            elif 'minimum =' in output_lower and 'maximum =' in output_lower: ok = True; status = "Thành công" #
            elif 'received =' in output_lower and 'lost =' in output_lower: #
                 match_loss = re.search(r"Lost = \d+ \((\d+)% loss\)", output) #
                 loss_percent = int(match_loss.group(1)) if match_loss else 100 #
                 if loss_percent == 0: ok = True; status = "Thành công" #
                 else: status = f"Thất bại ({loss_percent}% Packet Loss)" #
        else: #
            if "unreachable" in output_lower or "unknown host" in output_lower: status = "Thất bại (Unreachable/Unknown)" #
            elif "100% packet loss" in output_lower or "100.0% packet loss" in output_lower: status = "Thất bại (Timeout 100%)" #
            elif (" 0% packet loss" in output_lower or " 0.0% packet loss" in output_lower) and "packets transmitted" in output_lower: ok = True; status = "Thành công" #
            elif "packet loss" in output_lower: #
                 match_loss = re.search(r"(\d+)% packet loss", output_lower) or re.search(r"(\d+\.\d+)% packet loss", output_lower) #
                 loss_percent_str = match_loss.group(1) if match_loss else "100" #
                 try: loss_percent = float(loss_percent_str) #
                 except ValueError: loss_percent = 100.0 #
                 status = f"Thất bại ({loss_percent:.0f}% Packet Loss)" #
        return {"ok": ok, "status": status, "ip": ip, "output": output} #
    except subprocess.TimeoutExpired: return {"ok": False, "status": "Thất bại (Timeout)", "output": "Ping command timed out."} #
    except FileNotFoundError: return {"ok": False, "status": "Lỗi Cấu hình", "output": "Lệnh 'ping' không tìm thấy."} #
    except Exception as e: return {"ok": False, "status": f"Lỗi: {e}", "output": str(e)} #
# --- Tiện ích Proxy ---
# (Giữ nguyên)
def get_proxies_sync() -> Dict[str, Any]: #
    proxies: Set[str] = set(); errors: List[str] = []; ua: str = 'Mozilla/5.0' #
    if _bs4_available and UserAgent: ua = UserAgent().random #
    headers = {'User-Agent': ua} #
    for url in PROXY_APIS: #
        try:
            result = get_api_result_sync(url, headers=headers) #
            if result.get("status") and "_content" in result: #
                found = {p.strip() for p in result["_content"].splitlines() if re.match(r"^\d{1,3}(\.\d{1,3}){3}:\d+$", p.strip())} #
                proxies.update(found) #
            elif not result.get("status"): errors.append(f"API {url}: {result.get('message', 'Không rõ')}") #
        except Exception as e: errors.append(f"Xử lý {url}: {e}") #
    if proxies: return {"ok": True, "proxies": list(proxies), "errors": errors} #
    else: return {"ok": False, "errors": errors if errors else ["Không lấy được proxy."]} #

def check_single_proxy_sync(proxy: str) -> Dict[str, Any]: #
    match = re.match(r"(\d{1,3}(?:\.\d{1,3}){3}):(\d+)", proxy) #
    if not match: return {"proxy": proxy, "ok": False, "status": "Sai định dạng"} #
    ip, port_str = match.group(1), match.group(2); port = int(port_str) #
    result = check_tcp_port_sync(ip, port, timeout=3) #
    result["proxy"] = proxy; return result #

# ==============================================================================
# 7. HANDLERS LỆNH CÔNG KHAI
# ==============================================================================
# (Giữ nguyên)
@bot.message_handler(commands=["start", "help", "menu"])
async def handle_start_menu(message: types.Message): #
    user_info = await get_user(message.from_user.id, message.from_user.username)
    if not user_info: # Xử lý lỗi get_user
        await bot.reply_to(message, "⚠️ Lỗi: Không thể tải thông tin tài khoản. Vui lòng thử lại.")
        return
    mention = get_user_mention(message.from_user)
    status = "Admin 👑" if user_info["is_admin"] else ("Thành viên ⭐" if user_info["is_approved"] else "Khách ⚠️") # Bỏ NV
    welcome_text = (f"Chào mừng {mention} đến với Bot Tổng Hợp (Premium Vip)!\n"
                    f"Bot quản lý bởi @{OWNER_USERNAME}.\n\n"
                    f"👤 *TÀI KHOẢN:*\n   - Status: **{status}**\n   - Số dư: **{user_info['balance']:,}** VNĐ 💵")
    markup = types.InlineKeyboardMarkup(row_width=2)
    if user_info["is_approved"]:
        markup.add(types.InlineKeyboardButton("🔮 Giải Mã MD5", callback_data="menu:tx"), # Đổi tên nút
                   types.InlineKeyboardButton("🏦 Nạp Tiền", callback_data="menu:nap"),
                   types.InlineKeyboardButton("🧠 Hỏi AI", callback_data="menu:ask"),
                   types.InlineKeyboardButton("🔗 Check All", callback_data="menu:checkall"),
                   types.InlineKeyboardButton("🛠️ Tiện Ích", callback_data="menu:other_utils"))
    else:
         welcome_text += (f"\n⚠️ *Tài khoản chưa duyệt.*\n"
                          f"Liên hệ Admin @{OWNER_USERNAME} (ID: `{message.from_user.id}`) để kích hoạt + `{START_BALANCE:,}` VNĐ.")
         markup.add(types.InlineKeyboardButton(f"Liên hệ @{OWNER_USERNAME}", url=f"https://t.me/{OWNER_USERNAME}"))
    if user_info["is_admin"]: markup.add(types.InlineKeyboardButton("👑 ADMIN MENU", callback_data="menu:admin"))
    await bot.reply_to(message, welcome_text, parse_mode="MarkdownV2", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('menu:'))
async def handle_menu_callbacks(call: types.CallbackQuery): #
    await bot.answer_callback_query(call.id)
    command = call.data.split(':')[1]
    responses = {
        "tx": "OK, dùng lệnh: `/tx <mã_md5>`", # Cập nhật hướng dẫn
        "nap": "OK, xem hướng dẫn nạp: `/nap`",
        "ask": "OK, hỏi AI: `/ask <câu hỏi>`",
        "checkall": "OK, kiểm tra toàn diện: `/checkall <host_or_ip_or_url> [port]`",
        "other_utils": "OK, các lệnh khác:\n`/info` `/echo <text>` `/kqxs`\n`/anhgai` `/vdgai` `/fb <uid>` `/tt <user>`\n`/scl <link>` `/ngl <user> <msg> <sl>`\n`/voice <text>` `/qrcode <text>`\n`/getproxy` `/checkproxy <ip:port>`",
        "admin": "OK, Admin dùng `/adminmenu`."
    }
    # Không dùng send_response cho callback để tránh gửi video liên tục
    await bot.send_message(call.message.chat.id, escape_markdown_v2(responses.get(command, "Lỗi")), parse_mode="MarkdownV2", reply_to_message_id=call.message.message_id)

@bot.message_handler(func=lambda message: message.text and re.fullmatch(r"^[0-9a-f]{32}$", message.text.strip().lower()))
async def handle_md5_input(message: types.Message): #
    # Chức năng này giống /tx mới, nên gọi thẳng handler kia
    await handle_tx_md5(message, await get_user(message.from_user.id)) # Giả lập user_info nếu cần
# ==============================================================================
# 8. HANDLERS LỆNH NGƯỜI DÙNG
# ==============================================================================

# --- Lệnh /tx (GIẢI MÃ MD5) ---
@bot.message_handler(commands=["tx"])
@approval_required # Vẫn yêu cầu duyệt để dùng lệnh này
async def handle_tx_md5(message: types.Message, user_info: Dict[str, Any]):
    parts = message.text.split()
    if len(parts) != 2 or not re.fullmatch(r"^[0-9a-f]{32}$", parts[1].strip().lower()):
        await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/tx <mã_md5_32_ký_tự>`")
        return

    md5_hash = parts[1].strip().lower()
    log_command(user_info["user_id"], "/tx (md5)", md5_hash)
    msg = await bot.reply_to(message, f"🔮 Đang giải mã MD5: `{md5_hash}`...")

    md5_analysis = await asyncio.to_thread(predict_md5_logic, md5_hash)

    if not md5_analysis.get("ok"):
        await send_response(message, "LỖI GIẢI MÃ MD5", f"Lý do: {md5_analysis.get('error', 'Không rõ')}", processing_msg=msg)
        return

    emoji_dice = {1: '⚀', 2: '⚁', 3: '⚂', 4: '⚃', 5: '⚄', 6: '⚅'}
    dice_display = f"{emoji_dice[md5_analysis['dice'][0]]} {emoji_dice[md5_analysis['dice'][1]]} {emoji_dice[md5_analysis['dice'][2]]}"
    seed_next_hex = f"{md5_analysis['seed_next']:08X}"

    result_card = (
        f"🔑 *MD5 Đầu Vào:* `{md5_hash}`\n\n"
        f"**🔬 PHÂN TÍCH THUẬT TOÁN (LCG v2.0):**\n"
        f"   • Seed Hiện Tại: `{md5_hash[:8]}`\n"
        f"   • Seed Tiếp Theo: `{seed_next_hex}`\n"
        f"   • MD5 Vòng Sau (Dự đoán): `{md5_analysis['predicted_md5']}`\n\n"
        f"🎲 *DỰ ĐOÁN XÚC XẮC (Vòng Sau)*:\n"
        f"   - Xúc Xắc: **{dice_display}**\n"
        f"   - Tổng Điểm: **{md5_analysis['total']}**\n"
        f"   - **KẾT QUẢ:** **__{md5_analysis['result']}__** 🥇"
    )
    await send_response(message, "KẾT QUẢ GIẢI MÃ MD5", result_card, processing_msg=msg)

# --- Lệnh Kinh tế ---
# (Giữ nguyên nap, nap_request, rut, rut_request, nhiemvu, donate)
@bot.message_handler(commands=["nap"])
@approval_required
async def handle_nap(message: types.Message, user_info: Dict[str, Any]): #
    random_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    username = user_info["username"] if user_info["username"] else f"user_{user_info['user_id']}"
    nap_text = (f"**1. THÔNG TIN CK:**\n{BANK_INFO}\n"
                f"**2. NỘI DUNG CK (BẮT BUỘC):**\n   `NAP {username.upper()} {random_code}`\n"
                f"**3. MÃ QR:** [Bấm xem ảnh QR]({QR_CODE_IMAGE_URL})\n"
                f"**4. XÁC NHẬN:** Sau khi CK, dùng: `/nap_request <số tiền>`\n\n"
                f"💰 *Số dư*: **{user_info['balance']:,}** VNĐ.\n\n*{random.choice(RANDOM_THANKS)}*")
    try: await bot.reply_to(message, nap_text, parse_mode="Markdown", disable_web_page_preview=False)
    except Exception as e: await send_response(message, "LỖI", f"Không thể hiển thị thông tin. Lỗi: {e}")

@bot.message_handler(commands=["nap_request"])
@approval_required
async def handle_nap_request_user(message: types.Message, user_info: Dict[str, Any]): #
    parts = message.text.split();
    if len(parts) != 2: await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/nap_request <số tiền>`"); return
    try: amount = int(parts[1]); assert amount > 0
    except (ValueError, AssertionError): await send_response(message, "LỖI", "Số tiền không hợp lệ."); return
    await async_db_execute("INSERT INTO nap_request (user_id, amount) VALUES (?, ?)", (user_info["user_id"], amount))
    await send_response(message, "GỬI YÊU CẦU", f"*Đã gửi yêu cầu nạp* **{amount:,}** VNĐ. Chờ Admin duyệt.")

@bot.message_handler(commands=["rut"])
@approval_required
async def handle_rut(message: types.Message, user_info: Dict[str, Any]): #
    rut_text = (f"Số dư: **{user_info['balance']:,}** VNĐ.\n\n👉 Cú pháp:\n"
                f"`/rut_request <số tiền> <Tên NH> <Số TK> <Chủ TK>`\n*(Admin duyệt thủ công)*")
    await send_response(message, "YÊU CẦU RÚT TIỀN", rut_text)

@bot.message_handler(commands=["rut_request"])
@approval_required
async def handle_rut_request_user(message: types.Message, user_info: Dict[str, Any]): #
    parts = message.text.split(None, 4)
    if len(parts) < 5: await send_response(message, "SAI CÚ PHÁP", "`/rut_request <số tiền> <Tên NH> <Số TK> <Chủ TK>`"); return
    try:
        amount = int(parts[1]); bank_name, account_number, account_name = parts[2], parts[3], parts[4]
        if amount <= 0: raise ValueError("Số tiền phải lớn hơn 0")
        if amount > user_info["balance"]: raise ValueError("Số dư không đủ")
        request_details = (f"💸 **Yêu cầu rút tiền MỚI:**\n"
                           f"   - User: `{user_info['user_id']}` (@{user_info.get('username', 'N/A')})\n"
                           f"   - Tiền: **{amount:,}** VNĐ\n   - NH: `{bank_name}`\n   - STK: `{account_number}`\n   - Tên TK: `{account_name}`")
        sent_to_admin = False
        for admin_id in ADMIN_IDS:
            try: await bot.send_message(admin_id, escape_markdown_v2(request_details), parse_mode="MarkdownV2"); sent_to_admin = True
            except Exception as e: logging.error(f"Lỗi gửi YC rút tới admin {admin_id}: {e}")
        if sent_to_admin:
             await update_balance(user_info["user_id"], -amount)
             await send_response(message, "GỬI YÊU CẦU RÚT TIỀN", f"Đã gửi YC rút **{amount:,}** VNĐ. Số dư đã trừ. Chờ Admin xử lý.")
        else: await send_response(message, "LỖI GỬI YÊU CẦU", "Không thể báo Admin. Thử lại sau hoặc liên hệ trực tiếp.")
    except ValueError as ve: await send_response(message, "LỖI", str(ve))
    except Exception as e: logging.error(f"Lỗi /rut_request: {e}", exc_info=True); await send_response(message, "LỖI", "Lỗi xử lý yêu cầu.")

@bot.message_handler(commands=["nhiemvu"])
@approval_required
async def handle_nhiemvu_user(message: types.Message, user_info: Dict[str, Any]): #
    tasks = await async_db_execute("SELECT task_id, content, reward FROM tasks")
    task_list = "*Hiện không có nhiệm vụ.*" if not tasks else "\n".join(
        [f"ID `{t[0]}`: Thưởng **{t[2]:,}** VNĐ. ND: *{t[1]}*" for t in tasks])
    await send_response(message, "DANH SÁCH NHIỆM VỤ", task_list)

@bot.message_handler(commands=["donate"])
@approval_required
async def handle_donate(message: types.Message, user_info: Dict[str, Any]): #
    caption = (f"Cảm ơn bạn đã ủng hộ Bot! Giúp duy trì API.\n\n"
               f"*Quét QR hoặc [bấm vào đây]({QR_CODE_IMAGE_URL})*")
    try: await bot.send_photo(message.chat.id, QR_CODE_IMAGE_URL, caption=caption, parse_mode="Markdown")
    except Exception: await send_response(message, "THÔNG TIN ỦNG HỘ", caption)

# --- Lệnh Tiện ích ---
# (Giữ nguyên)
@bot.message_handler(commands=["ask"])
@approval_required
async def handle_ask(message: types.Message, user_info: Dict[str, Any]): #
    query = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else None
    if not query: await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/ask <câu hỏi>`"); return
    msg = await bot.reply_to(message, f"⏳ _Đang xử lý AI: {escape_markdown_v2(query[:50])}..._", parse_mode="MarkdownV2"); log_command(user_info["user_id"], "/ask", query[:50])
    try:
        quoted_query = requests.utils.quote(query)
        data = await asyncio.to_thread(get_api_result_sync, API_SEARCH_BASE + f"?chat={quoted_query}")
        if not data.get("ok"): await send_response(message, "LỖI AI", f"{data.get('error', 'Không rõ')}", processing_msg=msg); return
        response_text = data.get("text", "_Không có nội dung._")
        if len(response_text) > 3500: response_text = response_text[:3500] + "\n...(Đã cắt bớt)"
        await send_response(message, "PHẢN HỒI TỪ AI", response_text, processing_msg=msg)
    except Exception as e: await send_response(message, "LỖI KẾT NỐI AI", f"Lỗi: {e}", processing_msg=msg)

@bot.message_handler(commands=['voice'])
@approval_required
async def handle_voice(message: types.Message, user_info: Dict[str, Any]): #
    text = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else None
    if not _tts_qr_available or not gTTS: await send_response(message, "LỖI", "`gTTS` chưa cài đặt."); return
    if not text: await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/voice <văn bản>`"); return
    msg = await bot.reply_to(message, "🎤 _Đang tạo giọng nói..._"); log_command(user_info["user_id"], "/voice", text[:50])
    audio_data = await asyncio.to_thread(text_to_speech_sync, text)
    if isinstance(audio_data, str): await send_response(message, "LỖI TẠO VOICE", f"Lỗi: {audio_data}", processing_msg=msg); return
    try:
        await bot.send_voice(message.chat.id, audio_data, caption=f"🗣️ *Văn bản:* _{escape_markdown_v2(text[:50])}..._", parse_mode="MarkdownV2")
        await bot.delete_message(msg.chat.id, msg.message_id)
    except Exception as e: await send_response(message, "LỖI GỬI FILE", f"Không thể gửi Voice. Lỗi: {e}", processing_msg=msg)

@bot.message_handler(commands=['qrcode'])
@approval_required
async def handle_qrcode(message: types.Message, user_info: Dict[str, Any]): #
    text = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else None
    if not _tts_qr_available or not qrcode: await send_response(message, "LỖI", "`qrcode` chưa cài đặt."); return
    if not text: await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/qrcode <nội dung>`"); return
    msg = await bot.reply_to(message, "🔳 _Đang tạo mã QR..._"); log_command(user_info["user_id"], "/qrcode", text[:50])
    qr_data = await asyncio.to_thread(generate_qr_code_sync, text)
    if isinstance(qr_data, str): await send_response(message, "LỖI TẠO QR", f"Lỗi: {qr_data}", processing_msg=msg); return
    try:
        await bot.send_photo(message.chat.id, qr_data, caption=f"✅ *Mã QR cho:* `{escape_markdown_v2(text[:50])}...`", parse_mode="MarkdownV2")
        await bot.delete_message(msg.chat.id, msg.message_id)
    except Exception as e: await send_response(message, "LỖI GỬI FILE", f"Không thể gửi QR. Lỗi: {e}", processing_msg=msg)

@bot.message_handler(commands=['kqxs'])
@approval_required
async def handle_kqxs(message: types.Message, user_info: Dict[str, Any]): #
    msg = await bot.reply_to(message, "🎟️ _Đang lấy KQXS Miền Bắc..._"); log_command(user_info["user_id"], "/kqxs", "MB")
    data = await asyncio.to_thread(get_api_result_sync, API_XOSO_URL)
    if not data.get("status"): await send_response(message, "LỖI XỔ SỐ", f"`{data.get('message', 'Không lấy được.')}`", processing_msg=msg); return
    await send_response(message, "KẾT QUẢ XỔ SỐ MIỀN BẮC", data.get("result", "Không có."), processing_msg=msg)

@bot.message_handler(commands=['anhgai'])
@approval_required
async def handle_anhgai(message: types.Message, user_info: Dict[str, Any]): #
    msg = await bot.reply_to(message, "🩷 Đang tìm ảnh..."); log_command(user_info["user_id"], "/anhgai", "random")
    data = await asyncio.to_thread(get_api_result_sync, API_ANH_GAI)
    if not data.get("status"): await send_response(message, "LỖI API", f"`{data.get('message', 'Không lấy được.')}`", processing_msg=msg); return
    try:
        await bot.send_photo(message.chat.id, data["data"], caption=f"✨ Ảnh gái xinh (Tổng {data.get('count', '?')})")
        await bot.delete_message(msg.chat.id, msg.message_id)
    except Exception as e: await send_response(message, "LỖI GỬI ẢNH", f"Lỗi: {e}\nURL: {data.get('data', 'N/A')}", processing_msg=msg)

@bot.message_handler(commands=['vdgai'])
@approval_required
async def handle_vdgai(message: types.Message, user_info: Dict[str, Any]): #
    msg = await bot.reply_to(message, "🎬 Đang tìm video..."); log_command(user_info["user_id"], "/vdgai", "random")
    data = await asyncio.to_thread(get_api_result_sync, API_VD_GAI)
    if not data.get("status"): await send_response(message, "LỖI API", f"`{data.get('message', 'Không lấy được.')}`", processing_msg=msg); return
    try:
        await bot.send_video(message.chat.id, data["data"], caption=f"✨ Video gái xinh (Tổng {data.get('count', '?')})", supports_streaming=True)
        await bot.delete_message(msg.chat.id, msg.message_id)
    except Exception as e: await send_response(message, "LỖI GỬI VIDEO", f"Lỗi: {e}\nURL: {data.get('data', 'N/A')}", processing_msg=msg)

@bot.message_handler(commands=['fb'])
@approval_required
async def handle_fb(message: types.Message, user_info: Dict[str, Any]): #
    uid = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else None
    if not uid or not uid.isdigit(): await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/fb <UID Facebook>`"); return
    msg = await bot.reply_to(message, f"🔍 Đang tìm UID: `{uid}`..."); log_command(user_info["user_id"], "/fb", uid)
    data = await asyncio.to_thread(get_api_result_sync, API_FB_INFO.format(uid=uid))
    if not data.get("status"): await send_response(message, "LỖI API", f"`{data.get('message', 'Không tìm thấy.')}`", processing_msg=msg); return
    fb_data = data.get('data', {}); profile_url = fb_data.get('profile_url', f"https://www.facebook.com/{uid}")
    fb_text = (f"Tên: **{fb_data.get('name', 'N/A')}**\nUID: `{fb_data.get('uid', 'N/A')}`\n"
               f"Verified: {'✅' if fb_data.get('is_verified') else '❌'}\nFollowers: `{fb_data.get('followers', 'N/A')}`\n"
               f"Link: [Profile]({profile_url})")
    photo_sent = False
    try:
        if fb_data.get('avatar'): await bot.send_photo(message.chat.id, fb_data.get('avatar')); photo_sent = True
    except Exception as e_photo: logging.warning(f"Lỗi gửi ảnh FB: {e_photo}")
    await send_response(message, "THÔNG TIN FACEBOOK", fb_text, processing_msg=msg if not photo_sent else None)

@bot.message_handler(commands=['tt'])
@approval_required
async def handle_tt(message: types.Message, user_info: Dict[str, Any]): #
    username = message.text.split(None, 1)[1].strip().replace("@","") if len(message.text.split(None, 1)) > 1 else None
    if not username: await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/tt <username TikTok>`"); return
    msg = await bot.reply_to(message, f"🔍 Đang tìm TikTok: `@{username}`..."); log_command(user_info["user_id"], "/tt", username)
    data = await asyncio.to_thread(get_api_result_sync, API_TT_INFO.format(username=username))
    if not data.get("status"): await send_response(message, "LỖI API", f"`{data.get('message', 'Không tìm thấy.')}`", processing_msg=msg); return
    tt_data = data.get('data', {}); profile_url = f"https://www.tiktok.com/@{username}"
    tt_text = (f"Tên: **{tt_data.get('nickname', 'N/A')}**\nUsername: `@{tt_data.get('username', 'N/A')}`\n"
               f"Verified: {'✅' if tt_data.get('verified') else '❌'}\nFollowers: `{tt_data.get('followerCount', 'N/A')}` | Following: `{tt_data.get('followingCount', 'N/A')}`\n"
               f"Videos: `{tt_data.get('totalVideos', 'N/A')}` | Likes: `{tt_data.get('totalFavorite', 'N/A')}`\nBio: _{tt_data.get('signature', 'N/A')}_\n"
               f"Link: [Profile]({profile_url})")
    photo_sent = False
    try:
        if tt_data.get('avatar'): await bot.send_photo(message.chat.id, tt_data.get('avatar')); photo_sent = True
    except Exception as e_photo: logging.warning(f"Lỗi gửi ảnh TT: {e_photo}")
    await send_response(message, "THÔNG TIN TIKTOK", tt_text, processing_msg=msg if not photo_sent else None)

@bot.message_handler(commands=['scl'])
@approval_required
async def handle_scl(message: types.Message, user_info: Dict[str, Any]): #
    url = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else None
    if not url or "soundcloud.com" not in url: await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/scl <link SoundCloud>`"); return
    msg = await bot.reply_to(message, "🎶 Đang tải SoundCloud..."); log_command(user_info["user_id"], "/scl", url)
    data = await asyncio.to_thread(get_api_result_sync, API_SCL_DOWN.format(url=url))
    if not data.get("status"): await send_response(message, "LỖI API", f"`{data.get('message', 'Không tải được.')}`", processing_msg=msg); return
    try:
        audio_url = data.get("data")
        if not isinstance(audio_url, str) or not audio_url.startswith(('http://', 'https://')): raise ValueError("API không trả về URL hợp lệ")
        await bot.send_audio(message.chat.id, audio_url, caption=f"✅ *Tải OK!*\n🎵 `{escape_markdown_v2(data.get('title', 'Track'))}`", parse_mode="MarkdownV2")
        await bot.delete_message(msg.chat.id, msg.message_id)
    except Exception as e: await send_response(message, "TẢI OK (LINK)", f"Lỗi gửi audio ({e}), link:\n{data.get('data', 'N/A')}", processing_msg=msg)

@bot.message_handler(commands=['ngl'])
@approval_required
async def handle_ngl(message: types.Message, user_info: Dict[str, Any]): #
    parts = message.text.split(None, 3)
    if len(parts) < 4: await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/ngl <username> <message> <amount>`"); return
    username, msg_text, amount_str = parts[1], parts[2], parts[3]
    try: amount = int(amount_str); assert 1 <= amount <= 100
    except (ValueError, AssertionError): await send_response(message, "LỖI", "Số lượng 1-100."); return
    msg = await bot.reply_to(message, f"✉️ Đang spam NGL: `{username}`..."); log_command(user_info["user_id"], "/ngl", f"{username} - {amount}")
    api_url = API_NGL_SPAM.format(username=username, message=msg_text, amount=amount)
    data = await asyncio.to_thread(get_api_result_sync, api_url)
    if not data.get("status"): await send_response(message, "LỖI SPAM", f"`{data.get('message', 'Thất bại.')}`", processing_msg=msg); return
    await send_response(message, "SPAM NGL HOÀN TẤT", f"OK: `{data.get('success', 0)}`\nLỗi: `{data.get('failed', 0)}`", processing_msg=msg)

@bot.message_handler(commands=['echo'])
@approval_required
async def handle_echo(message: types.Message, user_info: Dict[str, Any]): #
    text_to_echo = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else None
    if not text_to_echo: await send_response(message, "SAI CÚ PHÁP", "/echo <nội dung>"); return
    log_command(user_info["user_id"], "/echo", text_to_echo[:50])
    await bot.reply_to(message, text=escape_markdown_v2(text_to_echo), parse_mode="MarkdownV2")

@bot.message_handler(commands=['info', 'me'])
@approval_required
async def handle_info(message: types.Message, user_info: Dict[str, Any]): #
    user = message.from_user
    info_text = (f"**Thông tin:**\nID: `{user.id}`\nTên: `{user.first_name}`\nUsername: `@{user.username}`\n"
                 f"Quyền Bot: {'Admin' if user_info['is_admin'] else 'Member'}") # Bỏ NV
    log_command(user_info["user_id"], "/info", str(user.id))
    await send_response(message, "THÔNG TIN TÀI KHOẢN", info_text)

# --- Tiện ích mạng & Proxy ---
# (Giữ nguyên getproxy, checkproxy)
@bot.message_handler(commands=['getproxy'])
@approval_required
async def handle_getproxy(message: types.Message, user_info: Dict[str, Any]): #
    msg = await bot.reply_to(message, "🌐 Đang tìm proxy public..."); log_command(user_info["user_id"], "/getproxy", "public")
    result = await asyncio.to_thread(get_proxies_sync)
    if not result.get("ok"):
        error_msg = "Lỗi lấy proxy.\n" + "\n".join(result.get("errors", ["Lỗi không rõ."]))
        await send_response(message, "LỖI GET PROXY", error_msg, processing_msg=msg); return
    proxies, errors = result.get("proxies", []), result.get("errors", [])
    display_proxies = random.sample(proxies, min(len(proxies), 20))
    proxy_text = f"✅ Tìm thấy {len(proxies)} proxy. Hiển thị {len(display_proxies)}:\n```\n" + "\n".join(display_proxies) + "\n```\n"
    if errors: proxy_text += "\n⚠️ *Lỗi nguồn:*\n- " + "\n- ".join(errors)
    proxy_text += "\n*Proxy public không ổn định. Dùng `/checkproxy`.*"
    await send_response(message, "DANH SÁCH PROXY", proxy_text, processing_msg=msg)

@bot.message_handler(commands=['checkproxy'])
@approval_required
async def handle_checkproxy(message: types.Message, user_info: Dict[str, Any]): #
    proxy = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else None
    if not proxy: await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/checkproxy <ip:port>`"); return
    msg = await bot.reply_to(message, f"🔎 Kiểm tra: `{proxy}`..."); log_command(user_info["user_id"], "/checkproxy", proxy)
    result = await asyncio.to_thread(check_single_proxy_sync, proxy)
    status = result.get("status", "Lỗi");
    if result.get("ok"): text = f"✅ **Proxy:** `{result.get('proxy')}`\n**Status:** `{status}`\n**IP:** `{result.get('ip')}`"
    else: text = f"❌ **Proxy:** `{result.get('proxy')}`\n**Status:** `{status}`"
    await send_response(message, "KIỂM TRA PROXY", text, processing_msg=msg)

@bot.message_handler(commands=['checkall'])
@approval_required
async def handle_checkall(message: types.Message, user_info: Dict[str, Any]): #
    input_target = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else None
    if not input_target:
        await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/checkall <host_or_ip_or_url> [port]`")
        return

    target_host: str = input_target
    port_tcp: int = 80
    port_udp: int = 80 # Mặc định UDP check cùng port TCP
    http_url: str = f"http://{target_host}"
    https_url: str = f"https://{target_host}"

    # Phân tích nếu là URL
    if re.match(r"^[a-zA-Z]+://", input_target):
        try:
            parsed_url = urlparse(input_target)
            target_host = parsed_url.hostname or input_target # Lấy hostname, fallback về input nếu lỗi
            if parsed_url.port:
                port_tcp = parsed_url.port
                port_udp = parsed_url.port
            elif parsed_url.scheme == 'https':
                port_tcp = 443
                port_udp = 443
            # Giữ nguyên scheme cho check HTTP/S
            if parsed_url.scheme == 'http': https_url = f"https://{target_host}" # Vẫn thử check HTTPS
            elif parsed_url.scheme == 'https': http_url = f"http://{target_host}" # Vẫn thử check HTTP

        except Exception as e:
            logging.warning(f"Lỗi phân tích URL '{input_target}': {e}")
            # Nếu lỗi, coi như input là hostname/IP

    # Kiểm tra nếu có port đi kèm hostname/IP (vd: example.com:8080)
    elif ':' in target_host and not ':' in target_host.split(':', 1)[0] : # Chỉ check port nếu không phải IPv6
        try:
             host_part, port_part = target_host.rsplit(':', 1)
             port_val = int(port_part)
             if 1 <= port_val <= 65535:
                  target_host = host_part
                  port_tcp = port_val
                  port_udp = port_val
             # Nếu port không hợp lệ, bỏ qua và dùng mặc định
        except ValueError:
             pass # Bỏ qua port không hợp lệ


    msg = await bot.reply_to(message, f"🔄 Check all: `{escape_markdown_v2(target_host)}` (TCP/UDP: {port_tcp})...", parse_mode="MarkdownV2"); log_command(user_info["user_id"], "/checkall", f"{target_host}:{port_tcp}")
    loop = asyncio.get_event_loop()
    results = await asyncio.gather(
        asyncio.to_thread(lookup_ip_sync, target_host),
        asyncio.to_thread(ping_host_sync, target_host),
        asyncio.to_thread(check_dns_sync, target_host),
        asyncio.to_thread(check_tcp_port_sync, target_host, port_tcp),
        asyncio.to_thread(check_udp_port_sync, target_host, port_udp),
        asyncio.to_thread(get_api_result_sync, https_url),
        asyncio.to_thread(get_api_result_sync, http_url),
        return_exceptions=True
    )
    ip_lookup_res, ping_res, dns_res, tcp_res, udp_res, https_res, http_res = results
    final_text = f"🎯 **Target:** `{target_host}`\n{'-'*35}\n"
    # IP Lookup
    final_text += "📍 **IP LOOKUP:**\n"
    if isinstance(ip_lookup_res, Exception): final_text += f"   ❌ Lỗi: {ip_lookup_res}\n"
    elif not ip_lookup_res.get("ok"): final_text += f"   ❌ Lỗi: {ip_lookup_res.get('error', '?')}\n"
    else: data = ip_lookup_res.get("data", {}); final_text += (f"   ✅ IP: `{data.get('query')}` | `{data.get('country')} ({data.get('countryCode')})`\n   ✅ `{data.get('city')}, {data.get('regionName')}`\n   ✅ ISP: `{data.get('isp')}` ({data.get('org')})\n   ✅ AS: `{data.get('as')}`\n")
    final_text += f"{'-'*35}\n"
    # Ping
    final_text += "📡 **PING (ICMP):**\n"
    if isinstance(ping_res, Exception): final_text += f"   ❌ Lỗi: {ping_res}\n"
    elif not ping_res.get("ok"): final_text += f"   ❌ {ping_res.get('status', 'Thất bại')}\n"
    else: final_text += f"   ✅ {ping_res.get('status', 'OK')} (IP: `{ping_res.get('ip')}`)\n"
    final_text += f"{'-'*35}\n"
    # DNS
    final_text += "🌐 **DNS LOOKUP:**\n"
    if isinstance(dns_res, Exception): final_text += f"   ❌ Lỗi: {dns_res}\n"
    elif not dns_res.get("ok"): final_text += f"   ❌ Lỗi: {dns_res.get('error', '?')}\n"
    else: final_text += f"   ✅ Host: `{dns_res.get('hostname')}`\n"; final_text += (f"   ✅ IPs: `{', '.join(dns_res.get('ips'))}`\n" if dns_res.get('ips') else "") + (f"   ✅ Aliases: `{', '.join(dns_res.get('aliases'))}`\n" if dns_res.get('aliases') else "")
    final_text += f"{'-'*35}\n"
    # TCP
    final_text += f"🔌 **TCP PORT ({port_tcp}):**\n"
    if isinstance(tcp_res, Exception): final_text += f"   ❌ Lỗi: {tcp_res}\n"
    elif not tcp_res.get("ok"): final_text += f"   ❌ {tcp_res.get('status', 'Lỗi')}\n"
    else: final_text += f"   ✅ {tcp_res.get('status', 'Mở')} (IP: `{tcp_res.get('ip')}`)\n"
    final_text += f"{'-'*35}\n"
    # UDP
    final_text += f"💧 **UDP PORT ({port_udp}):**\n"
    if isinstance(udp_res, Exception): final_text += f"   ⚠️ Lỗi: {udp_res}\n"
    elif not udp_res.get("ok"): final_text += f"   ⚠️ {udp_res.get('status', 'Lỗi')}\n"
    else: final_text += f"   ⚠️ {udp_res.get('status', 'Mở/Chặn?')} (IP: `{udp_res.get('ip')}`)\n"
    final_text += f"{'-'*35}\n"
    # HTTPS
    final_text += f"🔒 **HTTPS ({escape_markdown_v2(https_url)}):**\n"
    if isinstance(https_res, Exception): final_text += f"   ❌ Lỗi kết nối: {type(https_res).__name__}\n"
    elif not https_res.get("status"): final_text += f"   ❌ Lỗi kết nối: {https_res.get('message', '?')}\n"
    else: final_text += f"   ✅ Kết nối OK.\n"
    final_text += f"{'-'*35}\n"
    # HTTP
    final_text += f"🔗 **HTTP ({escape_markdown_v2(http_url)}):**\n"
    if isinstance(http_res, Exception): final_text += f"   ❌ Lỗi kết nối: {type(http_res).__name__}\n"
    elif not http_res.get("status"): final_text += f"   ❌ Lỗi kết nối: {http_res.get('message', '?')}\n"
    else: final_text += f"   ✅ Kết nối OK.\n"
    await send_response(message, f"KIỂM TRA TOÀN DIỆN: {target_host}", final_text, processing_msg=msg)

# ==============================================================================
# 9. HANDLERS LỆNH ADMIN
# ==============================================================================
# (Loại bỏ /addnv, /delnv, /upmoney; Thêm /addgr, /delgr, /allgr; Sửa /broadcast_all)
@bot.message_handler(commands=["adminmenu"])
@admin_required
async def handle_admin_menu(message: types.Message): #
    admin_text = ("👤 *User:*\n/add | /remove | /allusers | /clearusers\n\n"
                  "💵 *GD:*\n/nap_request_admin | /duyetnap\n\n"
                  "📝 *NVụ:*\n/setnhiemvu | /delnhiemvu | /listnhiemvu_admin\n\n"
                  "💬 *Nhóm Chat:*\n/addgr | /delgr | /allgr\n\n" # Thêm mục nhóm
                  "📢 *Hệ thống:*\n/broadcast_all | /logs | /clearlogs") # Đổi tên broadcast
    await send_response(message, "MENU ADMIN", admin_text)

# --- Quản lý User ---
# (Giữ nguyên add, remove, allusers, clearusers - clearusers có xác nhận)
@bot.message_handler(commands=['add'])
@admin_required
async def handle_admin_add_user(message: types.Message): #
    try: target_id = int(message.text.split()[1]); user_info = await get_user(target_id)
    except (IndexError, ValueError): await send_response(message, "SAI CÚ PHÁP", "/add <user_id>"); return
    if not user_info: await send_response(message, "LỖI DB", f"Không thể lấy/tạo user {target_id}"); return
    if user_info["is_approved"]: await send_response(message, "LỖI", f"ID `{target_id}` đã duyệt."); return
    await async_db_execute("UPDATE users SET is_approved = TRUE, balance = ? WHERE user_id = ?", (START_BALANCE, target_id))
    await send_response(message, "THÀNH CÔNG", f"Đã duyệt ID `{target_id}` & tặng **{START_BALANCE:,}** VNĐ.")
    try:
        safe_text=escape_markdown_v2(f"🎉 *TK đã duyệt!* +**{START_BALANCE:,}** VNĐ. Dùng `/menu`."); safe_time=escape_markdown_v2(get_current_vietnam_time())
        await bot.send_message(target_id, f"┏ 💎 *TÀI KHOẢN ĐÃ DUYỆT* ┓\n┣{chr(8213)*20}\n┣ {safe_text}\n┣{chr(8213)*20}\n┗ ⏱️ *{safe_time}*", parse_mode="MarkdownV2")
    except Exception as e: logging.warning(f"Không thể gửi thông báo duyệt cho {target_id}: {e}")

@bot.message_handler(commands=['remove'])
@admin_required
async def handle_admin_remove_user(message: types.Message): #
    try: target_id = int(message.text.split()[1]); assert target_id not in ADMIN_IDS
    except (IndexError, ValueError): await send_response(message, "SAI CÚ PHÁP", "/remove <user_id>"); return
    except AssertionError: await send_response(message, "LỖI", "Không thể xóa Admin."); return
    # Đổi thành DELETE để xóa hẳn user thay vì chỉ tước quyền
    result = await async_db_execute("DELETE FROM users WHERE user_id = ?", (target_id,))
    if result is not None: # Kiểm tra DB có lỗi không
         await send_response(message, "THÀNH CÔNG", f"Đã xóa hoàn toàn User ID `{target_id}` khỏi database.")
    else:
         await send_response(message, "LỖI DB", f"Không thể xóa User ID `{target_id}`.")


@bot.message_handler(commands=['allusers'])
@admin_required
async def handle_admin_all_users(message: types.Message): #
    users = await async_db_execute("SELECT user_id, username, is_approved, balance FROM users WHERE is_admin = FALSE") # Bỏ is_nv
    if users is None: await send_response(message, "LỖI DB", "Không thể lấy danh sách user."); return
    if not users: await send_response(message, "DS USER", "*Không có user (ngoài Admin).*"); return
    report = f"Tổng {len(users)} user:\n"; report += "\n".join([f"`{u[0]}` (@{u[1]}) - *{'✅User' if u[2] else '❌Chờ'}* - **{u[3]:,}** VNĐ" for u in users]) # Bỏ is_nv
    if len(report) > 3500: report = report[:3500] + "\n... (Quá dài)"
    await send_response(message, "DANH SÁCH USER", report)

@bot.message_handler(commands=['clearusers'])
@admin_required
async def handle_admin_clear_users(message: types.Message): #
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✅ CÓ, XÓA NGAY!", callback_data="confirm_clear_users:yes"),
               types.InlineKeyboardButton("❌ KHÔNG", callback_data="confirm_clear_users:no"))
    await bot.reply_to(message, "⚠️ *XÁC NHẬN:* Xóa TẤT CẢ user (trừ Admin)? KHÔNG thể hoàn tác.", parse_mode="Markdown", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('confirm_clear_users:'))
async def handle_confirm_clear_users(call: types.CallbackQuery): #
    await bot.answer_callback_query(call.id)
    user_info = await get_user(call.from_user.id)
    if not user_info or not user_info["is_admin"]:
         await bot.edit_message_text("🚫 Bạn không có quyền.", call.message.chat.id, call.message.message_id)
         return
    action = call.data.split(':')[1]
    if action == 'yes':
        try:
            result = await async_db_execute("DELETE FROM users WHERE is_admin = FALSE") # await the async call
            if result is not None:
                await bot.edit_message_text("✅ Đã xóa tất cả user (trừ Admin).", call.message.chat.id, call.message.message_id)
            else:
                 await bot.edit_message_text("❌ Lỗi DB khi xóa user.", call.message.chat.id, call.message.message_id)
        except Exception as e: await bot.edit_message_text(f"❌ Lỗi khi xóa user: {e}", call.message.chat.id, call.message.message_id)
    else: await bot.edit_message_text("👍 Đã hủy xóa user.", call.message.chat.id, call.message.message_id)

# --- Quản lý Giao dịch & Nhiệm vụ ---
# (Giữ nguyên)
@bot.message_handler(commands=['nap_request_admin'])
@admin_required
async def handle_admin_nap_requests(message: types.Message): #
    reqs = await async_db_execute("SELECT request_id, user_id, amount, timestamp FROM nap_request WHERE status = 'pending' ORDER BY timestamp ASC")
    if reqs is None: await send_response(message, "LỖI DB", "Không thể truy vấn YC nạp."); return
    if not reqs: await send_response(message, "QUẢN LÝ NẠP", "*Không có yêu cầu chờ duyệt.*"); return
    report = "YC nạp chờ duyệt:\n"; report += "\n".join([f"ID YC: `{r[0]}` | User: `{r[1]}` | Tiền: **{r[2]:,}** | Time: {r[3]}" for r in reqs])
    report += "\n\nDùng: `/duyetnap <req_id>`"
    await send_response(message, "YÊU CẦU NẠP TIỀN", report)

@bot.message_handler(commands=['duyetnap'])
@admin_required
async def handle_admin_duyet_nap(message: types.Message): #
    try: req_id = int(message.text.split()[1]); req_data = await async_db_fetchone("SELECT user_id, amount, status FROM nap_request WHERE request_id = ?", (req_id,))
    except (IndexError, ValueError): await send_response(message, "SAI CÚ PHÁP", "/duyetnap <request_id>"); return
    if req_data is None: await send_response(message, "LỖI DB", f"Không thể truy vấn YC ID `{req_id}`."); return
    if not req_data: await send_response(message, "LỖI", f"YC ID `{req_id}` không tồn tại."); return
    if req_data[2] != 'pending': await send_response(message, "LỖI", f"YC ID `{req_id}` đã xử lý."); return
    user_id, amount = req_data[0], req_data[1]; await update_balance(user_id, amount)
    await async_db_execute("UPDATE nap_request SET status = 'approved' WHERE request_id = ?", (req_id,))
    await send_response(message, "DUYỆT NẠP OK", f"Đã duyệt YC `{req_id}`. Cộng **{amount:,}** VNĐ cho User `{user_id}`.")
    try:
        safe_text=escape_markdown_v2(f"🎉 *YC Nạp (ID: {req_id}) đã duyệt!* +**{amount:,}** VNĐ."); safe_time=escape_markdown_v2(get_current_vietnam_time())
        await bot.send_message(user_id, f"┏ 💎 *GIAO DỊCH OK* ┓\n┣{chr(8213)*20}\n┣ {safe_text}\n┣{chr(8213)*20}\n┗ ⏱️ *{safe_time}*", parse_mode="MarkdownV2")
    except Exception as e: logging.warning(f"Không thể gửi thông báo duyệt nạp cho {user_id}: {e}")

@bot.message_handler(commands=['setnhiemvu'])
@admin_required
async def handle_admin_set_task(message: types.Message): #
    try: parts = message.text.split(None, 2); reward, content = int(parts[1]), parts[2]
    except (IndexError, ValueError): await send_response(message, "SAI CÚ PHÁP", "/setnhiemvu <thưởng> <nội dung>"); return
    await async_db_execute("INSERT INTO tasks (content, reward) VALUES (?, ?)", (content, reward))
    await send_response(message, "TẠO NHIỆM VỤ", f"OK:\n*Thưởng:* **{reward:,}** VNĐ\n*ND:* {content}")

@bot.message_handler(commands=['delnhiemvu'])
@admin_required
async def handle_admin_del_task(message: types.Message): #
    try: task_id = int(message.text.split()[1])
    except (IndexError, ValueError): await send_response(message, "SAI CÚ PHÁP", "/delnhiemvu <task_id>"); return
    await async_db_execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
    await send_response(message, "XÓA NHIỆM VỤ", f"Đã xóa NV ID: `{task_id}`.")

@bot.message_handler(commands=['listnhiemvu_admin'])
@admin_required
async def handle_admin_list_tasks(message: types.Message): #
    tasks = await async_db_execute("SELECT task_id, content, reward FROM tasks")
    if tasks is None: await send_response(message, "LỖI DB", "Không thể lấy danh sách nhiệm vụ."); return
    if not tasks: await send_response(message, "DS NHIỆM VỤ", "*Không có NV nào.*"); return
    task_list = "NV hoạt động:\n" + "\n".join([f"ID `{t[0]}`: Thưởng **{t[2]:,}**. ND: {t[1][:50]}..." for t in tasks])
    await send_response(message, "DANH SÁCH NHIỆM VỤ", task_list)

# --- Quản lý Nhóm Chat (Mới) ---
@bot.message_handler(commands=['addgr'])
@admin_required
async def handle_admin_add_group(message: types.Message):
    try:
        chat_id_str = message.text.split(None, 1)[1].strip()
        # ID nhóm chat thường là số âm
        if not chat_id_str.startswith('-') or not chat_id_str[1:].isdigit():
            raise ValueError("ID nhóm chat không hợp lệ (thường bắt đầu bằng dấu -).")
        chat_id = int(chat_id_str)

        # Kiểm tra xem bot có trong nhóm không (không hoàn toàn chính xác nhưng là bước kiểm tra cơ bản)
        try:
            chat_info = await bot.get_chat(chat_id)
            if chat_info.type not in ['group', 'supergroup']:
                 raise ValueError(f"ID {chat_id} không phải là nhóm hoặc siêu nhóm.")
        except Exception as e:
            await send_response(message, "LỖI KIỂM TRA NHÓM", f"Không thể lấy thông tin nhóm {chat_id}. Bot đã ở trong nhóm chưa?\nLỗi: {e}")
            return

        result = await async_db_execute("INSERT OR IGNORE INTO groups (chat_id) VALUES (?)", (chat_id,))
        if result is not None:
            # Kiểm tra xem có row nào được thêm không (nếu ID đã tồn tại thì affected_rows = 0)
            # cursor.rowcount không đáng tin cậy lắm với INSERT OR IGNORE, kiểm tra lại bằng SELECT
            check = await async_db_fetchone("SELECT 1 FROM groups WHERE chat_id = ?", (chat_id,))
            if check:
                 await send_response(message, "THÊM NHÓM OK", f"Đã thêm/cập nhật nhóm chat ID: `{chat_id}` vào danh sách broadcast.")
            else: # SHOULD NOT HAPPEN WITH OR IGNORE but handle anyway
                 await send_response(message, "LỖI DB", f"Không thể thêm nhóm {chat_id} (lỗi không rõ).")
        else:
            await send_response(message, "LỖI DB", f"Không thể thêm nhóm {chat_id}.")

    except IndexError:
        await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/addgr <chat_id>` (Lấy ID bằng bot @RawDataBot hoặc tương tự)")
    except ValueError as ve:
        await send_response(message, "SAI CÚ PHÁP", str(ve))
    except Exception as e:
        logging.error(f"Lỗi /addgr: {e}", exc_info=True)
        await send_response(message, "LỖI", f"Đã xảy ra lỗi không mong muốn: {e}")

@bot.message_handler(commands=['delgr'])
@admin_required
async def handle_admin_del_group(message: types.Message):
    try:
        chat_id_str = message.text.split(None, 1)[1].strip()
        if not chat_id_str.startswith('-') or not chat_id_str[1:].isdigit():
            raise ValueError("ID nhóm chat không hợp lệ.")
        chat_id = int(chat_id_str)

        result = await async_db_execute("DELETE FROM groups WHERE chat_id = ?", (chat_id,))
        if result is not None:
             # Kiểm tra lại xem đã xóa chưa
             check = await async_db_fetchone("SELECT 1 FROM groups WHERE chat_id = ?", (chat_id,))
             if not check:
                 await send_response(message, "XÓA NHÓM OK", f"Đã xóa nhóm chat ID: `{chat_id}` khỏi danh sách broadcast.")
             else: # Should not happen if DELETE was successful
                  await send_response(message, "XÓA NHÓM THẤT BẠI", f"Không thể xóa nhóm {chat_id} (có thể ID không tồn tại?).")
        else:
            await send_response(message, "LỖI DB", f"Không thể xóa nhóm {chat_id}.")

    except IndexError:
        await send_response(message, "SAI CÚ PHÁP", "Cú pháp: `/delgr <chat_id>`")
    except ValueError as ve:
        await send_response(message, "SAI CÚ PHÁP", str(ve))
    except Exception as e:
        logging.error(f"Lỗi /delgr: {e}", exc_info=True)
        await send_response(message, "LỖI", f"Đã xảy ra lỗi không mong muốn: {e}")


@bot.message_handler(commands=['allgr'])
@admin_required
async def handle_admin_all_groups(message: types.Message):
    groups = await async_db_execute("SELECT chat_id FROM groups ORDER BY chat_id ASC")
    if groups is None: await send_response(message, "LỖI DB", "Không thể lấy danh sách nhóm."); return
    if not groups: await send_response(message, "DS NHÓM", "*Chưa có nhóm nào được thêm.* Dùng `/addgr`."); return
    report = f"Tổng {len(groups)} nhóm đã thêm:\n"; report += "\n".join([f"- `{g[0]}`" for g in groups])
    if len(report) > 3500: report = report[:3500] + "\n... (Quá dài)"
    await send_response(message, "DANH SÁCH NHÓM", report)


# --- Quản lý Hệ thống ---
# (Sửa broadcast thành broadcast_all)
@bot.message_handler(commands=['broadcast_all'])
@admin_required
async def handle_admin_broadcast_all(message: types.Message):
    try: msg_text = message.text.split(None, 1)[1]
    except IndexError: await send_response(message, "SAI CÚ PHÁP", "/broadcast_all <nội dung>"); return

    # Lấy danh sách users và groups song song
    users_task = asyncio.create_task(async_db_execute("SELECT user_id FROM users WHERE is_approved = TRUE"))
    groups_task = asyncio.create_task(get_all_group_ids())
    all_users_data, all_group_ids = await asyncio.gather(users_task, groups_task)

    if all_users_data is None: await send_response(message, "LỖI DB", "Không thể lấy danh sách user."); return
    # all_group_ids đã xử lý lỗi bên trong get_all_group_ids

    all_user_ids = [u[0] for u in all_users_data]
    total_targets = len(all_user_ids) + len(all_group_ids)

    if total_targets == 0: await send_response(message, "LỖI BROADCAST", "Không có user/nhóm nào để gửi."); return

    msg = await bot.reply_to(message, f"📢 _Đang gửi tới {len(all_user_ids)} user và {len(all_group_ids)} nhóm..._")
    send_count, fail_count = 0, 0
    safe_text, safe_time = escape_markdown_v2(msg_text), escape_markdown_v2(get_current_vietnam_time())
    # Chỉ gửi text cho broadcast để tránh lỗi video
    formatted_text = (f"┏ 📢 *THÔNG BÁO ADMIN* ┓\n┣{chr(8213)*20}\n┣ {safe_text}\n┣{chr(8213)*20}\n┗ ⏱️ *{safe_time}*")

    # Gộp danh sách IDs
    target_ids = list(all_user_ids) + list(all_group_ids)
    random.shuffle(target_ids) # Gửi ngẫu nhiên để tránh burst

    for target_id in target_ids:
        try:
            await bot.send_message(target_id, formatted_text, parse_mode="MarkdownV2")
            send_count += 1
            await asyncio.sleep(0.15) # Delay lớn hơn một chút
        except Exception as e:
            logging.warning(f"Lỗi gửi broadcast tới {target_id}: {e}")
            fail_count += 1
            # Có thể xóa group khỏi DB nếu lỗi do bot bị kick
            if isinstance(e, types.ApiTelegramException) and ('bot was kicked' in str(e) or 'chat not found' in str(e)) and target_id < 0:
                 logging.info(f"Bot bị kick khỏi nhóm {target_id}, đang xóa khỏi DB.")
                 await async_db_execute("DELETE FROM groups WHERE chat_id = ?", (target_id,))

    await send_response(message, "BROADCAST OK", f"Gửi tới **{send_count}/{total_targets}** targets (Lỗi: {fail_count}).", processing_msg=msg)

@bot.message_handler(commands=['logs'])
@admin_required
async def handle_admin_logs(message: types.Message): #
    if not os.path.exists(LOG_FILE): await send_response(message, "LỖI LOGS", "File logs không tồn tại."); return
    try:
        with open(LOG_FILE, 'r', encoding="utf-8") as f: lines = f.readlines()
        if not lines: await send_response(message, "LOGS", "File logs trống."); return
        log_content = "".join(lines[-100:])
        if len(log_content) > 3500: log_content = "..." + log_content[-3500:]
        await send_response(message, "100 LỆNH GẦN NHẤT", f"```\n{log_content}\n```")
    except IOError as e: await send_response(message, "LỖI ĐỌC LOGS", f"Lỗi IO: {e}")
    except Exception as e: await send_response(message, "LỖI ĐỌC LOGS", f"Lỗi: {e}")

@bot.message_handler(commands=['clearlogs'])
@admin_required
async def handle_admin_clear_logs(message: types.Message): #
    try:
        with open(LOG_FILE, 'w', encoding='utf-8') as f: f.truncate(0)
        await send_response(message, "THÀNH CÔNG", "Đã xóa sạch nhật ký.")
    except IOError as e: await send_response(message, "LỖI XÓA LOGS", f"Lỗi IO: {e}")
    except Exception as e: await send_response(message, "LỖI XÓA LOGS", f"Lỗi: {e}")

# ==============================================================================
# 10. HANDLER LỖI
# ==============================================================================
# (Giữ nguyên)
@bot.message_handler(func=lambda message: message.text and message.text.startswith('/'))
async def handle_unknown_command(message: types.Message): #
    await send_response(message, "LỆNH KHÔNG TỒN TẠI", f"Lệnh `{escape_markdown_v2(message.text)}` không hợp lệ. Dùng `/menu`.") # Escape lệnh user nhập

# ==============================================================================
# 11. KHỞI ĐỘNG BOT
# ==============================================================================

async def main() -> None: #
    """Hàm chính để thiết lập và chạy bot."""
    logging.info(f"👑 Bot Hợp Nhất (Premium Vip by @{OWNER_USERNAME}) đang khởi động...") #
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'w', encoding='utf-8') as f: f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Log created.\n")
    except IOError as e: logging.critical(f"LỖI: Không thể tạo/ghi log '{LOG_FILE}': {e}. Bot thoát.", exc_info=True); return

    await setup_database() #
    try: await bot.delete_webhook(timeout=5); logging.info("✅ Xóa Webhook cũ OK.") #
    except Exception as e: logging.warning(f"Không thể xóa Webhook: {e}. Tiếp tục Polling.") #
    try:
        await bot.set_my_commands([ #
            types.BotCommand("/menu", "📋 Menu chính & Số dư"),
            types.BotCommand("/tx", "🔮 Giải mã MD5"), # Đổi mô tả
            types.BotCommand("/ask", "🤖 Hỏi AI"),
            types.BotCommand("/kqxs", "🎟️ Kết quả XSMB"),
            types.BotCommand("/nap", "💳 Hướng dẫn nạp tiền"),
            types.BotCommand("/rut", "💸 Yêu cầu rút tiền"),
            types.BotCommand("/nhiemvu", "📝 Nhiệm vụ kiếm tiền"),
            types.BotCommand("/anhgai", "🖼️ Ảnh gái xinh"),
            types.BotCommand("/vdgai", "🎬 Video gái xinh"),
            types.BotCommand("/fb", "👤 Check info Facebook"),
            types.BotCommand("/tt", "🎵 Check info TikTok"),
            types.BotCommand("/scl", "🎶 Tải nhạc SoundCloud"),
            types.BotCommand("/ngl", "✉️ Spam NGL"),
            types.BotCommand("/voice", "🗣️ Text-to-Speech"),
            types.BotCommand("/qrcode", "🔳 Tạo mã QR"),
            types.BotCommand("/checkall", "🔍 Check Host/IP/URL All"), # Đổi mô tả
            types.BotCommand("/getproxy", "🌐 Lấy Proxy Public"),
            types.BotCommand("/checkproxy","🔎 Kiểm tra Proxy"),
            types.BotCommand("/echo", "🗣️ Lặp lại tin nhắn"),
            types.BotCommand("/info", "ℹ️ Thông tin tài khoản"),
            types.BotCommand("/donate", "💖 Ủng hộ Bot"),
            types.BotCommand("/adminmenu", "👑 Menu Admin (Admin only)")
        ])
        logging.info("✅ Menu lệnh đã được cài đặt.") #
    except Exception as e: logging.warning(f"Cảnh báo: Không thể cài Menu lệnh. Lỗi: {e}.") #

    logging.info(f"✅ Bot Hợp Nhất (Premium Vip) đã sẵn sàng!") #
    while True: # Vòng lặp tự khởi động lại
        try:
            await bot.polling(non_stop=True, request_timeout=30, skip_pending=True, timeout=20) # Thêm timeout polling
        except requests.exceptions.ReadTimeout: logging.warning("Polling ReadTimeout. Restarting in 5s..."); await asyncio.sleep(5)
        except requests.exceptions.ConnectionError: logging.warning("Polling ConnectionError. Restarting in 15s..."); await asyncio.sleep(15)
        except asyncio.exceptions.TimeoutError: logging.warning("Asyncio TimeoutError in polling. Restarting in 5s..."); await asyncio.sleep(5)
        except Exception as e: logging.error(f"Lỗi Polling không xác định: {e}. Restarting in 30s...", exc_info=True); await asyncio.sleep(30)

if __name__ == "__main__": #
    if not os.path.exists(LOCAL_VIDEO_PATH): logging.error(f"❌ LỖI NGHIÊM TRỌNG: File video '{LOCAL_VIDEO_PATH}' không tồn tại! Bot sẽ không thể gửi video.") # Báo lỗi nghiêm trọng hơn

    try: asyncio.run(main()) #
    except KeyboardInterrupt: logging.info("🛑 Bot đã dừng.") #
    except Exception as e: logging.critical(f"LỖI NGHIÊM TRỌNG KHỞI ĐỘNG BOT: {e}", exc_info=True) #







#5


import asyncio
import html
import json
import logging
import os
import random
import sqlite3
import subprocess
import threading
import time
import gc
import signal
from datetime import datetime
from functools import wraps
from typing import Optional
import phonenumbers
import psutil
import pytz
from phonenumbers import carrier, geocoder
import atexit
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.types import Message, User, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup

MA_TOKEN_BOT = os.getenv('BOT_TOKEN', "7738916419:AAHwuQPWRybYRaHA2tWvG4KQ9MmGnhnhqzw")

ID_ADMIN_MAC_DINH = "7679054753"
TEN_ADMIN_MAC_DINH = "@tg_mediavip"
ID_NHOM_CHO_PHEP = -1002598824850
THU_MUC_DU_LIEU = "./data"
os.makedirs(THU_MUC_DU_LIEU, exist_ok=True)
logging.disable(logging.NOTSET)  # bật lại toàn bộ log

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)   # mức chi tiết nhất
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

DUONG_DAN_DB = os.path.join(THU_MUC_DU_LIEU, "bot_data.db")

SCRIPT_SMS_DIRECT = ["vip_0.py"]

SCRIPT_CALL_DIRECT = ["vip1_min.py"]

SCRIPT_SPAM_DIRECT = ["spam_0.py"]

SCRIPT_VIP_DIRECT = ["sms_1.py"]

SCRIPT_FREE = ["spam_0.py"]

SCRIPT_CACHE = {}
SCRIPT_CACHE_TIME = {}

def cleanup_old_cache():
    current_time = time.time()
    keys_to_remove = []

    for key, timestamp in SCRIPT_CACHE_TIME.items():
        if current_time - timestamp > 600:  # 10 phút
            keys_to_remove.append(key)

    for key in keys_to_remove:
        SCRIPT_CACHE.pop(key, None)
        SCRIPT_CACHE_TIME.pop(key, None)

def get_available_scripts(script_list, cache_key):
    current_time = time.time()

    if len(SCRIPT_CACHE) > 20:
        cleanup_old_cache()

    if (cache_key in SCRIPT_CACHE and
        cache_key in SCRIPT_CACHE_TIME and
        current_time - SCRIPT_CACHE_TIME[cache_key] < 600):
        return SCRIPT_CACHE[cache_key]

    available = [s for s in script_list if os.path.exists(s)]
    SCRIPT_CACHE[cache_key] = available
    SCRIPT_CACHE_TIME[cache_key] = current_time
    return available

TIMEOUT_NGAN = 180
TIMEOUT_TRUNG_BINH = 360
TIMEOUT_MO_RONG = 3600

# Khởi tạo bot
try:
    bot = Bot(
        token=MA_TOKEN_BOT,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML,
            link_preview_is_disabled=True
        )
    )
except Exception as e:
    logger.error(f"Lỗi khởi tạo bot: {e}")
    raise

def tao_ket_noi_db():
    try:
        conn = sqlite3.connect(DUONG_DAN_DB, check_same_thread=False, timeout=8.0)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        os.makedirs(os.path.dirname(DUONG_DAN_DB), exist_ok=True)
        conn = sqlite3.connect(DUONG_DAN_DB)
        conn.row_factory = sqlite3.Row
        return conn

# ============ CÁC LỚP QUẢN LÝ CACHE ============
class QuanLyQuyenCache:
    def __init__(self):
        self.cache = {}
        self.kich_thuoc_toi_da = 500  # Tăng cache size

    def lay_quyen(self, user_id):
        if user_id in self.cache:
            entry = self.cache[user_id]
            if time.time() - entry['thoi_gian_luu'] < 3600:  # Tăng cache time lên 1 giờ
                return entry['quyen']
            else:
                del self.cache[user_id]
        return None

    def dat_quyen(self, user_id, quyen):
        if len(self.cache) >= self.kich_thuoc_toi_da:
            # Xóa batch cũ hơn thay vì xóa random
            now = time.time()
            old_keys = [k for k, v in self.cache.items() if now - v['thoi_gian_luu'] > 1800]
            for key in old_keys[:100]:
                self.cache.pop(key, None)
        self.cache[user_id] = {'quyen': quyen, 'thoi_gian_luu': time.time()}

class QuanLyCooldown:
    def __init__(self):
        self.cache = {}
        self._lock = threading.RLock()

    def kiem_tra_cooldown(self, user_id, lenh):
        key = f"{lenh}:{user_id}"
        thoi_gian_hien_tai = time.time()

        if key not in self.cache:
            return False, 0, None

        with self._lock:
            lan_su_dung_cuoi = self.cache[key]
            # Inline cooldown calculation để giảm function calls
            quyen = lay_cap_do_quyen_nguoi_dung(user_id)
            thoi_gian_cooldown = COOLDOWN_LENH.get(lenh, {}).get(quyen, 60)

            if thoi_gian_hien_tai - lan_su_dung_cuoi < thoi_gian_cooldown:
                thoi_gian_con_lai = thoi_gian_cooldown - (thoi_gian_hien_tai - lan_su_dung_cuoi)
                return True, max(0, thoi_gian_con_lai), "command_specific"
        return False, 0, None

    def dat_cooldown(self, user_id, lenh):
        key = f"{lenh}:{user_id}"
        with self._lock:
            self.cache[key] = time.time()

FULL_STATUS = {}
FULL_LOCK = threading.Lock()

def dat_trang_thai_full(user_id, so_dien_thoai):
    with FULL_LOCK:
        key = f"{user_id}:{so_dien_thoai}"
        FULL_STATUS[key] = time.time() + 24 * 3600

def xoa_trang_thai_full(user_id, so_dien_thoai):
    with FULL_LOCK:
        key = f"{user_id}:{so_dien_thoai}"
        FULL_STATUS.pop(key, None)

def kiem_tra_so_full(user_id, so_dien_thoai):
    with FULL_LOCK:
        key = f"{user_id}:{so_dien_thoai}"
        if key in FULL_STATUS and FULL_STATUS[key] > time.time():
            return True
        FULL_STATUS.pop(key, None)
        return False

quan_ly_quyen_cache = QuanLyQuyenCache()
quan_ly_cooldown = QuanLyCooldown()

def chay_tien_trinh_nen_sync(command, timeout=None, user_id=None):
    """Chạy tiến trình nền đồng bộ với tracking tốt hơn"""
    try:
        if not command or not isinstance(command, str):
            return False, None, None
        command = command.strip()
        if len(command) > 1000:
            return False, None, None

        # Sử dụng setsid để tránh orphaned processes
        full_command = f"setsid {command} > /dev/null 2>&1 & echo $!"

        result = subprocess.run(
            full_command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=15  # Tăng timeout tạo process
        )

        if result.returncode == 0 and result.stdout.strip():
            pid = int(result.stdout.strip())

            # Kiểm tra process có tồn tại và track nó
            time.sleep(0.5)  # Chờ lâu hơn để process khởi động ổn định
            try:
                proc = psutil.Process(pid)
                if proc.is_running():
                    # Log để tracking
                    logger.info(f"Tạo process PID {pid} cho user {user_id}: {command[:50]}...")

                    # Đặt process group để dễ cleanup
                    try:
                        os.setpgid(pid, pid)
                    except (OSError, ProcessLookupError):
                        pass  # Process có thể đã set pgid

                    return True, pid, None
            except psutil.NoSuchProcess:
                logger.warning(f"Process {pid} đã thoát ngay sau khi tạo")

        return False, None, None
    except Exception as e:
        logger.error(f"Lỗi chay_tien_trinh_nen_sync: {e}")
        return False, None, None

def dem_tien_trinh_dong_bo(user_id=None):
    """Đếm tiến trình"""
    try:
        count = 0
        for proc in psutil.process_iter(['cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if 'python' in cmdline and any(script in cmdline for script in ['spam_', 'sms_', 'vip_', 'call']):
                    if user_id is None or str(user_id) in cmdline:
                        count += 1
            except:
                continue
        return count
    except:
        return 0

def tat_tien_trinh_dong_bo(pattern):
    killed_count = 0
    try:
        processes_to_kill = []
        process_families = {}  # Track process families để kill đệ quy

        for proc in psutil.process_iter(['pid', 'ppid', 'cmdline', 'name', 'status', 'create_time']):
            try:
                proc_info = proc.info
                if not proc_info['cmdline']:
                    continue

                cmdline = ' '.join(proc_info['cmdline'])
                proc_name = proc_info.get('name', '')
                proc_status = proc_info.get('status', '')

                # Kiểm tra zombie process
                if proc_status == psutil.STATUS_ZOMBIE:
                    processes_to_kill.append(proc)
                    continue

                # Kiểm tra python processes liên quan - mở rộng pattern matching
                is_target_process = (
                    ('python' in proc_name.lower() or 'python' in cmdline.lower()) and
                    any(script in cmdline for script in [
                        'spam_', 'sms_', 'vip_', 'call', 'lenh', 'tcp.py', 'tt.py', 
                        'ngl.py', 'pro24h.py', 'vip11122.py', 'mlm.py', 'vip1_min.py', 
                        'master222.py'
                    ])
                )
                if proc_info.get('create_time'):
                    process_age = time.time() - proc_info['create_time']
                    if process_age > 21600 and is_target_process:  # 6 giờ = 21600 giây
                        logger.warning(f"Phát hiện process cũ {proc_info['pid']}: {process_age/3600:.1f} giờ - {cmdline[:100]}")

                if not is_target_process:
                    continue

                should_kill = False
                if pattern == "python.*lenh":
                    should_kill = True
                elif "lenh.*" in pattern:
                    parts = pattern.split('.*')
                    if len(parts) >= 3:
                        user_id = parts[-1]
                        if user_id and user_id in cmdline:
                            should_kill = True
                else:
                    pattern_clean = pattern.replace('.*', '').replace('python3', 'python')
                    if pattern_clean in cmdline:
                        should_kill = True

                if should_kill:
                    processes_to_kill.append(proc)

                    # Thu thập process family để kill đệ quy
                    try:
                        children = proc.children(recursive=True)
                        process_families[proc.pid] = children
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        for proc in processes_to_kill:
            try:
                if proc.status() == psutil.STATUS_ZOMBIE:
                    # Xử lý zombie bằng cách kill parent
                    try:
                        parent = proc.parent()
                        if parent and parent.pid != 1:
                            parent.terminate()
                            parent.wait(timeout=2)
                    except:
                        pass
                    killed_count += 1
                    continue
                children = process_families.get(proc.pid, [])
                for child in children:
                    try:
                        if child.is_running():
                            child.terminate()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                time.sleep(0.5)
                for child in children:
                    try:
                        if child.is_running():
                            child.kill()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                proc.terminate()
                try:
                    proc.wait(timeout=8)  # Tăng từ 3s lên 8s
                    killed_count += 1
                except psutil.TimeoutExpired:
                    # Force kill nếu không terminate được
                    proc.kill()
                    try:
                        proc.wait(timeout=5)  # Tăng từ 2s lên 5s
                        killed_count += 1
                    except:
                        try:
                            os.kill(proc.pid, 9)
                            killed_count += 1
                        except:
                            pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                killed_count += 1
                continue
        if killed_count == 0:
            try:
                commands = []
                if 'lenh.*' in pattern and len(pattern.split('.*')) > 2:
                    user_id = pattern.split('.*')[-1]
                    commands = [
                        f"pkill -15 -f 'python.*{user_id}'",
                        f"pkill -9 -f 'python.*{user_id}'",
                        "pkill -9 -f 'spam_|sms_|vip_|call|tcp.py|tt.py|ngl.py|pro24h.py'"
                    ]
                else:
                    commands = [
                        "pkill -15 -f 'python.*lenh'",
                        "pkill -9 -f 'python.*lenh'", 
                        "pkill -9 -f 'spam_|sms_|vip_|call|tcp.py|tt.py|ngl.py|pro24h.py'"
                        # Thêm lệnh aggressive cleanup
                        "pkill -9 -f 'python3.*vip'",
                        "pkill -9 -f 'python.*pro24h'"
                    ]

                for cmd in commands:
                    try:
                        result = subprocess.run(cmd, shell=True, timeout=5, capture_output=True)
                        if result.returncode == 0:
                            killed_count += 1
                        time.sleep(0.2)  # Delay nhỏ giữa các lệnh
                    except:
                        continue

            except Exception:
                pass

        try:
            # Cleanup zombies mạnh hơn
            subprocess.run("ps aux | grep '<defunct>' | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true",
                         shell=True, timeout=8, capture_output=True)

            subprocess.run("ps -eo pid,etime,cmd | grep python | awk '$2 ~ /^[0-9]+-/ || $2 ~ /^[0-6][0-9]:[0-5][0-9]:[0-5][0-9]/ {print $1}' | head -20 | xargs -r kill -9 2>/dev/null || true",
                         shell=True, timeout=10, capture_output=True)

            subprocess.run("find /tmp -name '*.py*' -mmin +60 -delete 2>/dev/null || true",
                         shell=True, timeout=10, capture_output=True)

            subprocess.run("find . -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true",
                         shell=True, timeout=10, capture_output=True)

            subprocess.run("sync", shell=True, timeout=3, capture_output=True)

        except Exception as e:
            logger.error(f"Lỗi enhanced cleanup: {e}")

    except Exception as e:
        logger.error(f"Lỗi tat_tien_trinh_dong_bo: {e}")
        return False

    logger.info(f"Đã dọn dẹp {killed_count} processes với pattern: {pattern}")
    return killed_count > 0

def khoi_tao_database():
    conn = None
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin (
                user_id TEXT PRIMARY KEY,
                name TEXT,
                role TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vip_lists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                list_name TEXT NOT NULL,
                phone_numbers TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(user_id, list_name)
            )
        ''')
        conn.commit()
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo database: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.error(f"Lỗi khi đóng kết nối DB: {e}")

def khoi_tao_admin_mac_dinh():
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM admin WHERE user_id = ?", (ID_ADMIN_MAC_DINH,))
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO admin (user_id, name, role) VALUES (?, ?, ?)",
                (ID_ADMIN_MAC_DINH, TEN_ADMIN_MAC_DINH, 'admin')
            )
            conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo admin mặc định: {e}")

# ============ HỆ THỐNG PHÂN QUYỀN ============
def lay_cap_do_quyen_nguoi_dung(user_id):
    user_id = str(user_id)
    if user_id == ID_ADMIN_MAC_DINH:
        return 'admin'

    cached_quyen = quan_ly_quyen_cache.lay_quyen(user_id)
    if cached_quyen is not None:
        return cached_quyen

    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute("SELECT role FROM admin WHERE user_id = ? LIMIT 1", (user_id,))
        admin_result = cursor.fetchone()
        conn.close()

        if admin_result:
            quyen = admin_result['role']
        else:
            quyen = 'member'

        quan_ly_quyen_cache.dat_quyen(user_id, quyen)
        return quyen
    except Exception as e:
        logger.error(f"Lỗi khi lấy quyền người dùng {user_id}: {e}")
        quan_ly_quyen_cache.dat_quyen(user_id, 'member')
        return 'member'

def la_admin(user_id):
    return lay_cap_do_quyen_nguoi_dung(user_id) == 'admin'

def la_vip_vinh_vien(user_id):
    cap_do = lay_cap_do_quyen_nguoi_dung(user_id)
    return cap_do in ('admin', 'vip')

CACHE_SO_DIEN_THOAI = {}
KHOA_CACHE_SO_DIEN_THOAI = threading.Lock()

def la_so_dien_thoai_hop_le(so_dien_thoai):
    if not so_dien_thoai:
        return False

    with KHOA_CACHE_SO_DIEN_THOAI:
        if so_dien_thoai in CACHE_SO_DIEN_THOAI:
            return CACHE_SO_DIEN_THOAI[so_dien_thoai]

    try:
        if not so_dien_thoai.isdigit() or len(so_dien_thoai) not in [10, 11]:
            with KHOA_CACHE_SO_DIEN_THOAI:
                CACHE_SO_DIEN_THOAI[so_dien_thoai] = False
            return False

        so = phonenumbers.parse(so_dien_thoai, "VN")
        hop_le = phonenumbers.is_valid_number(so)

        with KHOA_CACHE_SO_DIEN_THOAI:
            CACHE_SO_DIEN_THOAI[so_dien_thoai] = hop_le
        return hop_le
    except Exception:
        with KHOA_CACHE_SO_DIEN_THOAI:
            CACHE_SO_DIEN_THOAI[so_dien_thoai] = False
        return False

def xac_thuc_so_voi_nha_mang(so_dien_thoai):
    try:
        if not so_dien_thoai or not isinstance(so_dien_thoai, str):
            return False, "𝑆𝑜̂́ điện thoại không hợp lệ"

        so_sach = ''.join(filter(str.isdigit, so_dien_thoai))

        if not la_so_dien_thoai_hop_le(so_sach):
            return False, "𝑆𝑜̂́ điện thoại không hợp lệ"

        so_da_phan_tich = phonenumbers.parse(so_sach, "VN")

        if not phonenumbers.is_valid_number(so_da_phan_tich):
            return False, "𝑆𝑜̂́ điện thoại không hợp lệ"

        try:
            ten_nha_mang = carrier.name_for_number(so_da_phan_tich, "vi")
        except ImportError:
            ten_nha_mang = get_carrier(so_sach)

        if not ten_nha_mang or ten_nha_mang == "Không rõ":
            ten_nha_mang = get_carrier(so_sach)

        return True, ten_nha_mang
    except phonenumbers.NumberParseException:
        return False, "𝑆𝑜̂́ không hợp lệ"
    except Exception:
        return False, "𝑆𝑜̂́ không hợp lệ"

# ============ GIỚI HẠN & COOLDOWN ============
# Tối ưu cooldown - giảm thời gian chờ để bot phản hồi nhanh hơn
COOLDOWN_LENH = {
    'xu_ly_ddos': {'admin': 60, 'vip': 180, 'member': 1800},
    'xu_ly_vip': {'admin': 90, 'vip': 180, 'member': 900},
    'xu_ly_spam': {'admin': 60, 'vip': 180, 'member': 180},
    'xu_ly_sms': {'admin': 60, 'vip': 180, 'member': 450},
    'xu_ly_call': {'admin': 30, 'vip': 180, 'member': 1800},
    'xu_ly_full': {'admin': 3600, 'vip': 3600, 'member': 3600},  # Giảm từ 7200->3600
    'xu_ly_tiktok': {'admin': 180, 'vip': 300, 'member': 900},
    'xu_ly_ngl': {'admin': 180, 'vip': 300, 'member': 900},
    'xu_ly_free': {'admin': 600, 'vip': 200, 'member': 300},  # 10 phút cho tất cả
}

def lay_gioi_han_so_dien_thoai(user_id):
    cap_do = lay_cap_do_quyen_nguoi_dung(user_id)
    gioi_han = {'admin': 50, 'vip': 50, 'member': 2}
    return gioi_han.get(cap_do, 2)

# ============ KHÓA LỆNH VÀ BẢO TRÌ ============
# Danh sách các lệnh đang bị khóa để bảo trì
LOCKED_COMMANDS = {"call"}

async def kiem_tra_lenh_bi_khoa(message: Message, ten_lenh: str) -> bool:
    """
    Kiểm tra xem lệnh có bị khóa không và gửi thông báo nếu bị khóa
    Returns True nếu lệnh bị khóa, False nếu không bị khóa
    """
    if ten_lenh in LOCKED_COMMANDS:
        await gui_phan_hoi(
            message,
            "🔒 Hệ thống đang được nâng cấp để mang đến trải nghiệm tốt hơn.\n"
            "Vui lòng sử dụng lệnh /free !\n\n"
            "Cảm ơn bạn đã kiên nhẫn chờ đợi!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return True
    return False

# ============ CÁC HÀM TIỆN ÍCH ============
def lay_thoi_gian_vn():
    """Lấy thời gian Việt Nam"""
    try:
        mui_gio_vn = pytz.timezone("Asia/Ho_Chi_Minh")
        hien_tai = datetime.now(mui_gio_vn)
        return hien_tai.strftime("%H:%M:%S"), hien_tai.strftime("%d/%m/%Y")
    except Exception as e:
        logger.error(f"Lỗi lấy giờ Việt Nam: {e}")
        hien_tai = datetime.now()
        return hien_tai.strftime("%H:%M:%S"), hien_tai.strftime("%d/%m/%Y")

def escape_html(text):
    if text is None:
        return ""
    return html.escape(str(text))

def dinh_dang_thoi_gian_cooldown(giay):
    """Định dạng thời gian cooldown"""
    if giay <= 0:
        return "0 𝑔𝑖𝑎̂𝑦"
    if giay < 60:
        return f"{int(giay)} 𝑔𝑖𝑎̂𝑦"
    phut = int(giay // 60)
    giay_con_lai = int(giay % 60)
    if giay_con_lai == 0:
        return f"{phut} 𝑝ℎ𝑢́𝑡"
    else:
        return f"{phut} 𝑝ℎ𝑢́𝑡 {giay_con_lai} 𝑔𝑖𝑎̂𝑦"

def dinh_dang_lien_ket_nguoi_dung(user):
    """Định dạng liên kết người dùng"""
    try:
        if not user:
            return "Người dùng không rõ"
        user_id = user.id
        ten_day_du = user.full_name
        if not user_id:
            return escape_html(ten_day_du or "Người dùng không rõ")
        if ten_day_du:
            return f'<a href="tg://user?id={user_id}">{escape_html(ten_day_du)}</a>'
        else:
            return f'<a href="tg://user?id={user_id}">ID: {user_id}</a>'
    except Exception as e:
        logger.error(f"Lỗi định dạng liên kết người dùng: {e}")
        return "Người dùng không rõ"

def lay_tieu_de_quyen(user_id):
    """Lấy tiêu đề quyền"""
    cap_do = lay_cap_do_quyen_nguoi_dung(user_id)
    tieu_de = {
        'admin': "╭━━࿗𓆰☯︎ 🎩 𝓐𝓭𝓶𝓲𝓷  ☯︎𓆪࿘━━╮",
        'vip': "╭━━❂༺𓆰🧞‍♂️🅥🅘🅟🧜🏻‍♀️𓆪༻❂━━╮",
        'member': "╭━━━━༉Members༉━━━━╮"
    }
    return tieu_de.get(cap_do, tieu_de['member'])

def get_carrier(phone):
    """Xác định nhà mạng từ số điện thoại"""
    if not phone:
        return "Không xác định"
    phone = str(phone).strip()
    if phone.startswith("+84"):
        phone = "0" + phone[3:]
    elif phone.startswith("84"):
        phone = "0" + phone[2:]
    if len(phone) < 3:
        return "Không xác định"
    prefix = phone[:3]
    viettel = {"086", "096", "097", "098", "032", "033", "034", "035", "036", "037", "038", "039"}
    mobifone = {"089", "090", "093", "070", "079", "077", "076", "078"}
    vinaphone = {"088", "091", "094", "083", "084", "085", "081", "082"}
    vietnamobile = {"092", "056", "058"}
    gmobile = {"099", "059"}
    if prefix in viettel:
        return "𝑉𝑖𝑒𝑡𝑡𝑒𝑙"
    elif prefix in mobifone:
        return "𝑀𝑜𝑏𝑖𝑓𝑜𝑛𝑒"
    elif prefix in vinaphone:
        return "𝑉𝑖𝑛𝑎𝑝ℎ𝑜𝑛𝑒"
    elif prefix in vietnamobile:
        return "𝑉𝑖𝑒𝑡𝑛𝑎𝑚𝑜𝑏𝑖𝑙𝑒"
    elif prefix in gmobile:
        return "𝐺𝑚𝑜𝑏𝑖𝑙𝑒"
    return "𝐾ℎ𝑜̂𝑛𝑔 𝑥𝑎́𝑐 𝑑𝑖̣𝑛ℎ"

def doc_file_js(ten_file):
    """Đọc danh sách từ file JavaScript"""
    try:
        if not os.path.exists(ten_file):
            return []

        with open(ten_file, 'r', encoding='utf-8') as file:
            noi_dung = file.read()

        # Tìm array trong file JS
        import re
        pattern = r'\[([^\]]+)\]'
        match = re.search(pattern, noi_dung, re.DOTALL)

        if match:
            array_content = match.group(1)
            # Tách các URL từ array
            urls = []
            for line in array_content.split('\n'):
                line = line.strip()
                if line.startswith('"') and line.endswith('",'):
                    url = line[1:-2]  # Bỏ dấu " và ,
                    urls.append(url)
                elif line.startswith('"') and line.endswith('"'):
                    url = line[1:-1]  # Bỏ dấu "
                    urls.append(url)
            return urls
        return []
    except Exception as e:
        logger.error(f"Lỗi đọc file JS {ten_file}: {e}")
        return []

def tao_keyboard_lien_ket_nhom():
    """Tạo inline keyboard với liên kết đến nhóm khác"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🥷🏿   ㋰ 𓊈 𝐴𝑑𝑚𝑖𝑛 𝟸𝟺/𝟽 𓊉 ㋰   🛰️",
                url="https://t.me/@tg_mediavip"
            )
        ]
    ])
    return keyboard

def create_router():
    """Tạo router mới với tất cả handlers"""
    router = Router()

    # Đăng ký tất cả handlers
    router.message.register(xu_ly_start, Command("start"))
    router.message.register(xu_ly_ping, Command("ping"))
    router.message.register(xu_ly_sms, Command("sms"))
    router.message.register(xu_ly_spam, Command("spam"))
    router.message.register(xu_ly_free, Command("free"))
    router.message.register(xu_ly_vip, Command("vip"))
    router.message.register(xu_ly_call, Command("call"))
    router.message.register(xu_ly_ddos, Command("ddos"))
    router.message.register(xu_ly_full, Command("full"))
    router.message.register(xu_ly_tiktok, Command("tiktok"))
    router.message.register(xu_ly_ngl, Command("ngl"))
    router.message.register(xu_ly_kill_tien_trinh, Command("kill"))
    router.message.register(xu_ly_checkid, Command("checkid"))
    router.message.register(xu_ly_kill_tat_ca_tien_trinh, Command("killall"))
    router.message.register(xu_ly_them_vip, Command("themvip"))
    router.message.register(xu_ly_xoa_vip, Command("xoavip"))
    router.message.register(xu_ly_them_admin, Command("themadmin"))
    router.message.register(xu_ly_xoa_admin, Command("xoaadmin"))
    router.message.register(xu_ly_xem_danh_sach_vip, Command("listvip"))
    router.message.register(xu_ly_don_dep_vps, Command("vps"))
    router.message.register(xu_ly_proxy, Command("prx"))
    router.message.register(xu_ly_random_anh, Command("img"))
    router.message.register(xu_ly_random_video, Command("vid"))
    router.message.register(xu_ly_tin_nhan_khong_phai_lenh)

    return router

async def gui_phan_hoi(message: Message, noi_dung: str, xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8, luu_vinh_vien=False, co_keyboard=False):
    try:
        chat_id = message.chat.id
        text = f"<blockquote>{noi_dung.strip()}</blockquote>"
        keyboard = tao_keyboard_lien_ket_nhom() if co_keyboard else None
        tasks = []
        send_task = bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=keyboard
        )
        tasks.append(send_task)

        # Task xóa tin nhắn người dùng (nếu cần)
        if xoa_tin_nguoi_dung:
            delete_task = bot.delete_message(chat_id=chat_id, message_id=message.message_id)
            tasks.append(delete_task)

        # Chạy song song các tasks
        results = await asyncio.gather(*tasks, return_exceptions=True)
        sent_message = results[0] if not isinstance(results[0], Exception) else None

        if (sent_message and not isinstance(sent_message, Exception) and 
            tu_dong_xoa_sau_giay > 0 and not luu_vinh_vien):
            asyncio.create_task(tu_dong_xoa_tin_nhan(sent_message.chat.id, sent_message.message_id, tu_dong_xoa_sau_giay))

        return sent_message
    except Exception as e:
        logger.error(f"Lỗi khi gửi phản hồi: {e}")
        return None

async def tu_dong_xoa_tin_nhan(chat_id, message_id, tre=10):
    try:
        await asyncio.sleep(tre)
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.error(f"Lỗi khi tự động xóa tin nhắn ({chat_id}, {message_id}): {e}")

def them_vip(user_id, ten):
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO admin (user_id, name, role) VALUES (?, ?, ?)",
            (str(user_id), ten, 'vip')
        )
        conn.commit()
        conn.close()

        # Xóa cache quyền ngay lập tức để cập nhật nhanh
        quan_ly_quyen_cache.cache.pop(str(user_id), None)
    except Exception as e:
        logger.error(f"Lỗi khi thêm VIP {user_id}: {e}")

def them_admin(user_id, ten):
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO admin (user_id, name, role) VALUES (?, ?, ?)",
            (str(user_id), ten, 'admin')
        )
        conn.commit()
        conn.close()

        # Xóa cache quyền ngay lập tức để cập nhật nhanh
        quan_ly_quyen_cache.cache.pop(str(user_id), None)
    except Exception as e:
        logger.error(f"Lỗi khi thêm Admin {user_id}: {e}")

# ============ DECORATOR ============
def cooldown_nguoi_dung(giay_mac_dinh=60):
    """Decorator cooldown thống nhất - Tối ưu hóa"""
    def decorator(func):
        @wraps(func)
        async def wrapper(message: Message, *args, **kwargs):
            if not message.from_user:
                return False
            user_id = message.from_user.id
            ten_ham = func.__name__

            # Lấy quyền 1 lần và cache để tránh gọi lại
            cap_do = lay_cap_do_quyen_nguoi_dung(user_id)

            # Kiểm tra quyền required trước cooldown (nhanh hơn)
            quyen_yeu_cau = getattr(func, '_quyen_yeu_cau', None)
            if quyen_yeu_cau:
                if quyen_yeu_cau == 'admin' and cap_do != 'admin':
                    await gui_phan_hoi(message, "Không đủ quyền!", xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=3)
                    return False
                elif quyen_yeu_cau == 'vip_vinh_vien' and cap_do not in ('admin', 'vip'):
                    await gui_phan_hoi(message, "Không đủ quyền!", xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=3)
                    return False

            # Kiểm tra cooldown - admin bỏ qua một số cooldown
            if cap_do != 'admin':  # Admin skip cooldown check cho tốc độ
                dang_cooldown, thoi_gian_con_lai, _ = quan_ly_cooldown.kiem_tra_cooldown(user_id, ten_ham)
                if dang_cooldown:
                    thoi_gian_formatted = dinh_dang_thoi_gian_cooldown(thoi_gian_con_lai)
                    await gui_phan_hoi(
                        message,
                        f"🏓 𝐵𝑎̣𝑛 𝑐𝑎̂̀𝑛 𝑐ℎ𝑜̛̀ {thoi_gian_formatted} 𝑛𝑢̛̃𝑎 𝑑𝑒̂̉ 𝑠𝑢̛̉ 𝑑𝑢̣𝑛𝑔 𝑙𝑒̣̂𝑛ℎ 𝑛𝑎̀𝑦 !",
                        xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=5
                    )
                    return False

            # Thực thi lệnh
            ket_qua = await func(message, *args, **kwargs)

            # Chỉ đặt cooldown khi lệnh thành công và không phải admin
            if ket_qua is True and cap_do != 'admin':
                quan_ly_cooldown.dat_cooldown(user_id, ten_ham)

            return ket_qua
        return wrapper
    return decorator

def chi_nhom(func):
    """Decorator chỉ cho phép sử dụng trong nhóm"""
    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if not message.from_user:
            return False
        user = message.from_user
        chat = message.chat
        if la_admin(user.id):
            return await func(message, *args, **kwargs)
        if not chat or chat.id != ID_NHOM_CHO_PHEP:
            return False
        return await func(message, *args, **kwargs)
    return wrapper

def chi_admin(func):
    func._quyen_yeu_cau = 'admin'
    return func

def chi_vip_vinh_vien(func):
    func._quyen_yeu_cau = 'vip_vinh_vien'
    return func

# ============ CÁC HANDLER LỆNH ============
def trich_xuat_tham_so(message: Message):
    if not message.text:
        return []
    return message.text.split()[1:]

@chi_nhom
async def xu_ly_start(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    noi_dung = f"""𝑀𝑟.𝑈𝑠𝑒𝑟   :    {lien_ket_nguoi_dung}

🚀 𝐿𝐸̣̂𝑁𝐻 𝐶𝑂̛ 𝐵𝐴̉𝑁:
 • /ping    -    𝑋𝑒𝑚 𝑇𝑟𝑎̣𝑛𝑔 𝑇ℎ𝑎́𝑖 𝐵𝑂𝑇
 • /checkid    -    𝑋𝑒𝑚 𝑇ℎ𝑜̂𝑛𝑔 𝑇𝑖𝑛 𝐼𝐷
 • /free    -    𝑆𝑝𝑎𝑚 𝑆𝑀𝑆 𝑍𝑎𝑙𝑜  

🔥 𝐿𝐸̣̂𝑁𝐻 𝑇𝐴̂́𝑁 𝐶𝑂̂𝑁𝐺:
 • /sms    -    𝑆𝑀𝑆 𝟻𝟶 𝑆𝑜̂́
 • /spam    -    𝑆𝑝𝑎𝑚 𝑙𝑖𝑒̂𝑛 𝑇𝑢̣𝑐
 • /ngl    -    𝑆𝑝𝑎𝑚 𝑁𝐺𝐿

💫 𝑉𝐼𝑃 𝑉𝐼̃𝑁𝐻 𝑉𝐼𝐸̂̃𝑁:
 • /call    -    𝐺𝑜̣𝑖 𝟷 𝑆𝑜̂́
 • /ddos    -    𝐷𝑎́𝑛ℎ 𝑆𝑎̣̂𝑝 𝑊𝑒𝑏
 • /vip    -    𝑆𝑀𝑆 𝐶𝑎𝑙𝑙 𝟷𝟶 𝑠𝑜̂́/𝑙𝑎̂̀𝑛
 • /full    -    𝐶ℎ𝑎̣𝑦 𝐹𝑢𝑙𝑙 ②④ⓗ
 • /tiktok    -    𝑇𝑎̆𝑛𝑔 𝑉𝑖𝑒𝑤 𝑇𝑖𝑘𝑇𝑜𝑘
 • /kill    -    𝐷𝑢̛̀𝑛𝑔 𝐿𝑒̣̂𝑛ℎ

🎬 𝐺𝐼𝐴̉𝐼 𝑇𝑅𝐼́:
 • /img    -    𝑅𝑎𝑛𝑑𝑜𝑚 𝐴̉𝑛ℎ
 • /vid    -    𝑅𝑎𝑛𝑑𝑜𝑚 𝑉𝑖𝑑𝑒𝑜
"""

    await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)
    return True

@chi_nhom
async def xu_ly_ping(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    tieu_de_quyen = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    noi_dung = f"""{tieu_de_quyen}
┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {lien_ket_nguoi_dung}
┃• 🎫 𝑀ã 𝐼𝐷       :      {user_id}

🤖 𝑇𝑟𝑎̣𝑛𝑔 𝑡ℎ𝑎́𝑖 𝐵𝑜𝑡 : ℎ𝑜𝑎̣𝑡 𝑑𝑜̣̂𝑛𝑔 🛰️

🚀 𝑆𝐴̆̃𝑁 𝑆𝐴̀𝑁𝐺 𝑁𝐻𝐴̣̂𝑁 𝐿𝐸̣̂𝑁𝐻 ! 🎯"""

    await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)
    return True

@cooldown_nguoi_dung()
@chi_nhom
async def xu_ly_sms(message: Message):
    # Kiểm tra lệnh có bị khóa không
    if await kiem_tra_lenh_bi_khoa(message, "sms"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    cac_tham_so = trich_xuat_tham_so(message)

    if not cac_tham_so:
        gioi_han_so = lay_gioi_han_so_dien_thoai(user_id)
        await gui_phan_hoi(
            message,
            f"👼🏻 /𝑠𝑚𝑠 𝟶𝟿𝟾𝟿𝟿𝟿𝟶𝟶𝟶 𝟶𝟿𝟾𝟿𝟿𝟿𝟶𝟶𝟷..𝑇𝑜̂́𝑖 𝑑𝑎 {gioi_han_so} 𝑆𝑜̂́ 𝑡ℎ𝑒𝑜 𝑞𝑢𝑦𝑒̂̀𝑛 ℎ𝑖𝑒̣̂𝑛 𝑡𝑎̣𝑖 𝑐𝑢̉𝑎 𝑏𝑎̣𝑛 !",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    # Xử lý danh sách số trực tiếp
    gioi_han_so = lay_gioi_han_so_dien_thoai(user_id)
    if len(cac_tham_so) > gioi_han_so:
        await gui_phan_hoi(
            message,
            f"👼🏻 𝐵𝑎̣𝑛 𝑐ℎ𝑖̉ 𝑑𝑢̛𝑜̛̣𝑐 𝑝ℎ𝑒́𝑝 𝑛ℎ𝑎̣̂𝑝 𝑡𝑜̂́𝑖 𝑑𝑎 {gioi_han_so} 𝑆𝑜̂́!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    cac_so_hop_le = []
    for so in cac_tham_so:
        so = so.strip()
        if la_so_dien_thoai_hop_le(so) and not kiem_tra_so_full(user_id, so) and so not in cac_so_hop_le:
            cac_so_hop_le.append(so)

    if not cac_so_hop_le:
        await gui_phan_hoi(
            message,
            "👼🏻 Các số điện thoại đang chạy trong lệnh full 24h hoặc không hợp lệ!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    # Chọn script SMS với cache
    available_scripts = get_available_scripts(SCRIPT_VIP_DIRECT, 'sms')
    if not available_scripts:
        await gui_phan_hoi(
            message,
            f"👼🏻 Script SMS không khả dụng!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    script_duoc_chon = random.choice(available_scripts)

    danh_sach_so_chuoi = " ".join(cac_so_hop_le)
    command = f"proxychains4 python3 {script_duoc_chon} {danh_sach_so_chuoi}"

    thanh_cong, pid, _ = chay_tien_trinh_nen_sync(command, timeout=TIMEOUT_TRUNG_BINH, user_id=user_id)

    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "👼🏻 Không thể khởi tạo tiến trình!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    tieu_de_quyen = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    noi_dung = f"""{tieu_de_quyen}
┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟     :      {lien_ket_nguoi_dung}
┃• 🎫 𝑀ã 𝐼𝐷        :      {user_id}
╭━━━━━〔❨✧✧❩〕
 • 𝑁ℎ𝑎̣̂𝑝 𝑇𝑎𝑦          :      {len(cac_so_hop_le)} 𝑆𝑜̂́ 𝐻𝑜̛̣𝑝 𝑙𝑒̣̂
 • 𝑇𝑎̂́𝑛 𝐶𝑜̂𝑛𝑔           :       𝟼𝟶 𝑝ℎ𝑢́𝑡
 • 𝑉𝑖̣ 𝑡𝑟𝑖́                  :      𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒
 • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛           :       {chuoi_gio}
 • 𝑇𝑜𝑑𝑎𝑦                :       {chuoi_ngay}
╰━━━━━〔❨✧𝐒𝐌𝐒✧❩〕"""

    # Gửi ảnh với keyboard
    try:
        keyboard = tao_keyboard_lien_ket_nhom()
        await bot.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{noi_dung}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        # Xóa tin nhắn người dùng
        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass

    except Exception as e:
        # Fallback về text nếu không gửi được ảnh
        logger.error(f"Lỗi gửi ảnh: {e}")
        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)
        
    return True

@cooldown_nguoi_dung()
@chi_nhom
async def xu_ly_spam(message: Message):
    # Kiểm tra lệnh có bị khóa không
    if await kiem_tra_lenh_bi_khoa(message, "spam"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    cac_tham_so = trich_xuat_tham_so(message)

    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "👼🏻 𝐶𝑢́ 𝑝ℎ𝑎́𝑝: /spam 𝟶𝟿𝟶𝟿𝟽𝟽𝟾𝟿𝟿𝟾",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    so_dien_thoai = cac_tham_so[0].strip()

    hop_le, thong_diep = xac_thuc_so_voi_nha_mang(so_dien_thoai)
    if not hop_le:
        await gui_phan_hoi(
            message,
            f"👼🏻 {thong_diep}",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    if kiem_tra_so_full(user_id, so_dien_thoai):
        await gui_phan_hoi(
            message,
            f"👼🏻 Số {so_dien_thoai} 𝑑𝑎𝑛𝑔 𝑐ℎ𝑎̣𝑦 𝑓𝑢𝑙𝑙 𝟸𝟺ℎ!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    available_scripts = get_available_scripts(SCRIPT_SPAM_DIRECT, 'spam')
    if not available_scripts:
        await gui_phan_hoi(
            message,
            "👼🏻 Script Spam không khả dụng!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    script = random.choice(available_scripts)

    command = f"timeout 180s python3 {script} {so_dien_thoai} 5"
    thanh_cong, pid, _ = chay_tien_trinh_nen_sync(command, timeout=TIMEOUT_NGAN, user_id=user_id)

    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "👼🏻 Lỗi khi khởi động tiến trình!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    nha_mang = get_carrier(so_dien_thoai)
    tieu_de_quyen = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    noi_dung = f"""{tieu_de_quyen}
┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {lien_ket_nguoi_dung}
┃• 🎫 𝑀ã 𝐼𝐷       :      {user_id}
╭━━━━〔❨✧✧❩〕
 • 𝑃ℎ𝑜𝑛𝑒 𝑉𝑁        :      {so_dien_thoai}
 • 𝑇𝑎̂́𝑛 𝐶𝑜̂𝑛𝑔        :      𝟷 𝐺𝑖𝑜̛̀ 𝑙𝑖𝑒̂𝑛 𝑡𝑢̣𝑐
 • 𝑁ℎ𝑎̀ 𝑚𝑎̣𝑛𝑔       :      {nha_mang}
 • 𝑉𝑖̣ 𝑡𝑟𝑖́                :      𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒
 • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛         :      {chuoi_gio}
 • 𝑇𝑜𝑑𝑎𝑦              :      {chuoi_ngay}
╰━━━━〔❨✧𝐒𝐏𝐀𝐌✧❩〕"""

    try:
        keyboard = tao_keyboard_lien_ket_nhom()
        await bot.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{noi_dung}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        # Xóa tin nhắn người dùng
        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass

    except Exception as e:
        # Fallback về text nếu không gửi được ảnh
        logger.error(f"Lỗi gửi ảnh: {e}")
        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)

    return True

@cooldown_nguoi_dung()
@chi_nhom
async def xu_ly_free(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    cac_tham_so = trich_xuat_tham_so(message)

    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "👼🏻 𝐶𝑢́ 𝑝ℎ𝑎́𝑝: /free 𝟶𝟿𝟶𝟿𝟽𝟽𝟾𝟿𝟿𝟾",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    so_dien_thoai = cac_tham_so[0].strip()

    hop_le, thong_diep = xac_thuc_so_voi_nha_mang(so_dien_thoai)
    if not hop_le:
        await gui_phan_hoi(
            message,
            f"👼🏻 {thong_diep}",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    if kiem_tra_so_full(user_id, so_dien_thoai):
        await gui_phan_hoi(
            message,
            f"👼🏻 Số {so_dien_thoai} 𝑑𝑎𝑛𝑔 𝑐ℎ𝑎̣𝑦 𝑓𝑢𝑙𝑙 𝟸𝟺ℎ!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    script = random.choice(SCRIPT_FREE)

    command = f"timeout 180s python3 {script} {so_dien_thoai} 1"
    thanh_cong, pid, _ = chay_tien_trinh_nen_sync(command, timeout=TIMEOUT_NGAN, user_id=user_id)

    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "👼🏻 Lỗi khi khởi động tiến trình!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    noi_dung = (
        f"👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟   :     {lien_ket_nguoi_dung}\n"
        f"🎫 𝑀ã 𝐼𝐷      :     {user_id}\n"
        f"🚀 𝐿𝑒̣̂𝑛ℎ 𝑑𝑎̃ 𝑐ℎ𝑎̣𝑦 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔 !🎯\n"
        f"𝐴𝐸 𝑡𝑒𝑠𝑡 𝑡ℎ𝑢̛̉ 𝑠𝑜̂́ 𝑟𝑜̂̀𝑖 𝑐ℎ𝑜 𝑚𝑖̀𝑛ℎ 𝑥𝑖𝑛 𝑦́ 𝑘𝑖𝑒̂́𝑛 !"
    )

    try:
        keyboard = tao_keyboard_lien_ket_nhom()
        await bot.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{noi_dung}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Lỗi gửi ảnh: {e}")
        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)

    return True

# VIP COMMANDS
@cooldown_nguoi_dung()
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_vip(message: Message):
    if await kiem_tra_lenh_bi_khoa(message, "vip"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    cac_tham_so = trich_xuat_tham_so(message)

    if not cac_tham_so:
        await gui_phan_hoi(
            message,
            "👼🏻 /vip 𝟶𝟿𝟾𝟿𝟸𝟿𝟿𝟿𝟶𝟿...𝑇𝑜̂́𝑖 𝑑𝑎 𝟷𝟶 𝑠𝑜̂́",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    # Xử lý danh sách số trực tiếp (tối đa 10 số)
    if len(cac_tham_so) > 10:
        await gui_phan_hoi(
            message,
            "👼🏻 𝐿𝑒̣̂𝑛ℎ /vip 𝑐ℎ𝑖̉ 𝑐ℎ𝑜 𝑝ℎ𝑒́𝑝 𝑡𝑜̂́𝑖 𝑑𝑎 𝟷𝟶 𝑠𝑜̂́!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    cac_so_hop_le = []
    for so in cac_tham_so[:10]:
        so = so.strip()
        if la_so_dien_thoai_hop_le(so) and not kiem_tra_so_full(user_id, so) and so not in cac_so_hop_le:
            cac_so_hop_le.append(so)

    if not cac_so_hop_le:
        await gui_phan_hoi(
            message,
            "👼🏻 Các số điện thoại đang chạy trong lệnh full 24h hoặc không hợp lệ!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    available_scripts = get_available_scripts(SCRIPT_SMS_DIRECT, 'vip')
    if not available_scripts:
        await gui_phan_hoi(
            message,
            "👼🏻 Không có script VIP nào khả dụng!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    # Chạy nhiều script song song với batch processing
    cac_pid_thanh_cong = []

    # Chia thành batch nhỏ để tránh quá tải
    batch_size = 3
    for i in range(0, len(cac_so_hop_le), batch_size):
        batch = cac_so_hop_le[i:i + batch_size]

        # Chạy đồng thời trong batch
        for j, so in enumerate(batch):
            script_index = (i + j) % len(available_scripts)
            script_duoc_chon = available_scripts[script_index]
            command = f"proxychains4 python3 {script_duoc_chon} {so} 5"
            thanh_cong, pid, _ = chay_tien_trinh_nen_sync(command, timeout=TIMEOUT_NGAN, user_id=user_id)
            if thanh_cong and pid:
                cac_pid_thanh_cong.append(pid)

        # Delay nhỏ giữa các batch để tránh spam
        if i + batch_size < len(cac_so_hop_le):
            await asyncio.sleep(0.1)

    if not cac_pid_thanh_cong:
        await gui_phan_hoi(
            message,
            "👼🏻 Không thể khởi tạo tiến trình nào!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    tieu_de_quyen = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    noi_dung = f"""{tieu_de_quyen}
┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {lien_ket_nguoi_dung}
┃• 🎫 𝑀ã 𝐼𝐷       :      {user_id}
╭━━━━━〔❨✧✧❩〕
 • 𝑁ℎ𝑎̣̂𝑝 𝑇𝑎𝑦        :      {len(cac_so_hop_le)} 𝑠𝑜̂́ 𝐻𝑜̛̣𝑝 𝑙𝑒̣̂
 • 𝑆𝑜̂́ 𝑣𝑜̀𝑛𝑔            :      𝟹𝟶 𝑉𝑜̀𝑛𝑔
 • 𝑉𝑖̣ 𝑡𝑟𝑖́                :       𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒
 • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛         :       {chuoi_gio}
 • 𝑇𝑜𝑑𝑎𝑦              :       {chuoi_ngay}
╰━━━━━〔❨✧𝐕𝐈𝐏✧❩〕"""

    # Gửi ảnh với keyboard
    try:
        keyboard = tao_keyboard_lien_ket_nhom()
        await bot.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{noi_dung}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass

    except Exception as e:
        # Fallback về text nếu không gửi được ảnh
        logger.error(f"Lỗi gửi ảnh: {e}")
        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)

    return True

@cooldown_nguoi_dung()
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_call(message: Message):
    # Kiểm tra lệnh có bị khóa không
    if await kiem_tra_lenh_bi_khoa(message, "call"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    cac_tham_so = trich_xuat_tham_so(message)

    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "👼🏻 𝐶𝑢́ 𝑝ℎ𝑎́𝑝: /call 𝟶𝟿𝟾𝟿𝟸𝟸𝟼𝟿𝟿𝟾",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    so_dien_thoai = cac_tham_so[0].strip()

    hop_le, thong_diep = xac_thuc_so_voi_nha_mang(so_dien_thoai)
    if not hop_le:
        await gui_phan_hoi(
            message,
            f"👼🏻 {thong_diep}",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    if kiem_tra_so_full(user_id, so_dien_thoai):
        await gui_phan_hoi(
            message,
            f"👼🏻 𝑆𝑜̂́ {so_dien_thoai} 𝑑𝑎𝑛𝑔 𝑐ℎ𝑎̣𝑦 𝑓𝑢𝑙𝑙 𝟸𝟺ℎ!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    available_scripts = get_available_scripts(SCRIPT_CALL_DIRECT, 'call')
    if not available_scripts:
        await gui_phan_hoi(
            message,
            "👼🏻 Không có script Call nào khả dụng!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    script = random.choice(available_scripts)
    command = f"python3 {script} {so_dien_thoai} 2"

    thanh_cong, pid, _ = chay_tien_trinh_nen_sync(command, timeout=TIMEOUT_NGAN, user_id=user_id)

    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "👼🏻 Lỗi khi khởi động tiến trình!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    nha_mang = get_carrier(so_dien_thoai)
    tieu_de_quyen = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    noi_dung = f"""{tieu_de_quyen}
┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {lien_ket_nguoi_dung}
┃• 🎫 𝑀ã 𝐼𝐷       :      {user_id}
╭━━━━━〔❨✧✧❩〕
 • 𝑃ℎ𝑜𝑛𝑒 𝑉𝑁       :     {so_dien_thoai}
 • 𝐿𝑎̣̆𝑝 𝑙𝑎̣𝑖             :     𝟿𝟿 𝐿𝑎̂̀𝑛
 • 𝑁ℎ𝑎̀ 𝑚𝑎̣𝑛𝑔       :     {nha_mang}
 • 𝑉𝑖̣ 𝑡𝑟𝑖́                :      𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒
 • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛        :      {chuoi_gio}
 • 𝑇𝑜𝑑𝑎𝑦              :      {chuoi_ngay}
╰━━━━〔❨✧𝐂𝐀𝐋𝐋✧❩〕"""

    # Gửi ảnh với keyboard
    try:
        keyboard = tao_keyboard_lien_ket_nhom()
        await bot.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{noi_dung}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        # Xóa tin nhắn người dùng
        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass

    except Exception as e:
        # Fallback về text nếu không gửi được ảnh
        logger.error(f"Lỗi gửi ảnh: {e}")
        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)

    return True

@cooldown_nguoi_dung()
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_ddos(message: Message):
    # Kiểm tra lệnh có bị khóa không
    if await kiem_tra_lenh_bi_khoa(message, "ddos"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    cac_tham_so = trich_xuat_tham_so(message)

    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "👼🏻 Cú pháp: /ddos [link web]",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    url_muc_tieu = cac_tham_so[0].strip()
    if not any(url_muc_tieu.startswith(proto) for proto in ['http://', 'https://']):
        url_muc_tieu = 'http://' + url_muc_tieu

    script_ddos = "tcp.py"
    thanh_cong, pid, file_log = chay_tien_trinh_nen_sync(
        f"python3 {script_ddos} {url_muc_tieu} 1000",
        timeout=TIMEOUT_TRUNG_BINH
    )

    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "👼🏻 Lỗi khi khởi động lệnh ddos!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    tieu_de_quyen = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = f"""{tieu_de_quyen}
┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {lien_ket_nguoi_dung}
┃• 🎫 𝑀ã 𝐼𝐷       :      {user_id}
╭━━━━━〔❨✧✧❩〕
 • Target       :     {escape_html(url_muc_tieu[:25])}...
 • 𝑆𝑜̂́ vòng          :     Liên tục
 • Power          :     High Performance
 • 𝑉𝑖̣ 𝑡𝑟𝑖́                :      𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒
 • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛        :      {chuoi_gio}
 • 𝑇𝑜𝑑𝑎𝑦              :      {chuoi_ngay}
╰━━━━〔❨✧𝗗𝗗𝗢𝗦✧❩〕"""

    try:
        keyboard = tao_keyboard_lien_ket_nhom()
        await bot.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{noi_dung}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        # Xóa tin nhắn người dùng
        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass

    except Exception as e:
        # Fallback về text nếu không gửi được ảnh
        logger.error(f"Lỗi gửi ảnh: {e}")
        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)

    return True

@cooldown_nguoi_dung()
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_full(message: Message):
    # Kiểm tra lệnh có bị khóa không
    if await kiem_tra_lenh_bi_khoa(message, "full"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    cac_tham_so = trich_xuat_tham_so(message)

    if not cac_tham_so:
        await gui_phan_hoi(
            message,
            "👼🏻 𝐶𝑢́ 𝑝ℎ𝑎́𝑝: /full 𝟶𝟿𝟶𝟿𝟽𝟽𝟾𝟿𝟿𝟾 𝟶𝟿𝟶𝟿𝟽𝟽𝟾𝟿𝟿𝟽...\n𝐶ℎ𝑎̣𝑦 𝑙𝑖𝑒̂𝑛 𝑡𝑢̣𝑐 𝟸𝟺ℎ - 𝑉𝐼𝑃 𝑡𝑜̂́𝑖 𝑑𝑎 𝟹 𝑠𝑜̂́ 𝑚𝑜̂̃𝑖 𝑙𝑎̂̀𝑛 !",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    # Giới hạn số lượng số điện thoại cho VIP (tối đa 3 số)
    if len(cac_tham_so) > 3:
        await gui_phan_hoi(
            message,
            "👼🏻 𝑉𝐼𝑃 𝑐ℎ𝑖̉ 𝑑𝑢̛𝑜̛̣𝑐 𝑝ℎ𝑒́𝑝 𝑛ℎ𝑎̣̂𝑝 𝑡𝑜̂́𝑖 𝑑𝑎 𝟹 𝑆𝑜̂́ 𝑐ℎ𝑜 𝑙𝑒̣̂𝑛ℎ 𝑓𝑢𝑙𝑙!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    cac_so_hop_le = []
    for so in cac_tham_so:
        so = so.strip()
        if la_so_dien_thoai_hop_le(so) and not kiem_tra_so_full(user_id, so) and so not in cac_so_hop_le:
            cac_so_hop_le.append(so)

    if not cac_so_hop_le:
        await gui_phan_hoi(
            message,
            "👼🏻 𝐾ℎ𝑜̂𝑛𝑔 𝑐𝑜́ 𝑆𝑜̂́ 𝑑𝑖𝑒̣̂𝑛 𝑡ℎ𝑜𝑎̣𝑖 ℎ𝑜̛̣𝑝 𝑙𝑒̣̂ 𝑛𝑎̀𝑜!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    # Kiểm tra user đã có số full chưa
    with FULL_LOCK:
        user_full_count = sum(1 for key in FULL_STATUS.keys() if key.startswith(f"{user_id}:"))
        if user_full_count + len(cac_so_hop_le) > 3:
            await gui_phan_hoi(
                message,
                f"👼🏻 𝐵𝑎̣𝑛 𝑑𝑎̃ 𝑐𝑜́ {user_full_count} 𝑠𝑜̂́ 𝑑𝑎𝑛𝑔 𝐹𝑢𝑙𝑙. 𝑉𝐼𝑃 𝑐ℎ𝑖̉ 𝑑𝑢̛𝑜̛̣𝑐 𝑡𝑜̂́𝑖 𝑑𝑎 𝟹 𝑠𝑜̂́!",
                xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
            )
            return False

    # Chạy nhiều số song song
    cac_pid_thanh_cong = []
    cac_so_thanh_cong = []

    for so in cac_so_hop_le:
        # Đặt trạng thái full trước khi chạy
        dat_trang_thai_full(user_id, so)

        command = f"timeout 1200s python3 pro24h.py {so}"
        thanh_cong, pid, _ = chay_tien_trinh_nen_sync(command, timeout=TIMEOUT_MO_RONG, user_id=user_id)

        if thanh_cong and pid:
            cac_pid_thanh_cong.append(pid)
            cac_so_thanh_cong.append(so)
        else:
            xoa_trang_thai_full(user_id, so)

    if not cac_pid_thanh_cong:
        await gui_phan_hoi(
            message,
            "👼🏻 Không thể khởi tạo tiến trình full nào!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    tieu_de_quyen = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    danh_sach_so = ", ".join(cac_so_thanh_cong)

    noi_dung = f"""{tieu_de_quyen}
┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {lien_ket_nguoi_dung}
┃• 🎫 𝑀ã 𝐼𝐷       :      {user_id}
╭━━━━━〔❨✧✧❩〕
 • 𝑃ℎ𝑜𝑛𝑒 𝐵𝑙𝑜𝑐𝑘     :      {len(cac_so_thanh_cong)} 𝑠𝑜̂́ 𝐻𝑜̛̣𝑝 𝑙𝑒̣̂
 • 𝐷𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ        :      {danh_sach_so}
 • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛          :      𝟸𝟺 𝐺𝑖𝑜̛̀ 𝑙𝑖𝑒̂𝑛 𝑡𝑢̣𝑐
 • 𝑇𝑟𝑎̣𝑛𝑔 𝑡ℎ𝑎́𝑖        :       𝐷𝑎𝑛𝑔 𝑔𝑢̛̉𝑖 𝑂𝑇𝑃
 • 𝑉𝑖̣ 𝑡𝑟𝑖́                  :      𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒
 • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛           :      {chuoi_gio}
 • 𝑇𝑜𝑑𝑎𝑦                :      {chuoi_ngay}
 • 📵 𝑈𝑛𝑙𝑜𝑐𝑘         :      /kill 𝐷𝑢̛̀𝑛𝑔 𝑠𝑜̂́
╰━━━〔❨✧𝐅𝐮𝐥𝐥 𝟐𝟒/𝟕✧❩〕"""

    # Gửi ảnh với keyboard
    try:
        keyboard = tao_keyboard_lien_ket_nhom()
        await bot.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{noi_dung}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        # Xóa tin nhắn người dùng
        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass

    except Exception as e:
        # Fallback về text nếu không gửi được ảnh
        logger.error(f"Lỗi gửi ảnh: {e}")
        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)

    return True

@cooldown_nguoi_dung()
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_tiktok(message: Message):
    """Xử lý lệnh TikTok"""
    # Kiểm tra lệnh có bị khóa không
    if await kiem_tra_lenh_bi_khoa(message, "tiktok"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    cac_tham_so = trich_xuat_tham_so(message)

    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "👼🏻 Cú pháp: /tiktok [link video tiktok]",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8,
            co_keyboard=True
        )
        return False

    link_tiktok = cac_tham_so[0].strip()

    if not ("tiktok.com" in link_tiktok or "vm.tiktok.com" in link_tiktok):
        await gui_phan_hoi(
            message,
            "👼🏻 Link TikTok không hợp lệ!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8,
            co_keyboard=True
        )
        return False

    script_tiktok = "tt.py"
    thanh_cong, pid, file_log = chay_tien_trinh_nen_sync(
        f"python3 {script_tiktok} {link_tiktok} 1000",
        timeout=TIMEOUT_MO_RONG
    )

    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "👼🏻 Lỗi khi khởi động lệnh tiktok!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8,
            co_keyboard=True
        )
        return False

    tieu_de_quyen = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = f"""{tieu_de_quyen}
┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {lien_ket_nguoi_dung}
┃• 🎫 𝑀ã 𝐼𝐷       :      {user_id}
╭━━━━━〔❨✧✧❩〕
 • Link          :     {escape_html(link_tiktok[:30])}...
 • Target          :      1000+ views
 • 𝑉𝑖̣ 𝑡𝑟𝑖́        :     𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒
 • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛.      :      {chuoi_gio}
 • 𝑇𝑜𝑑𝑎𝑦             :      {chuoi_ngay}
╰━━━━〔❨✧𝐓𝐢𝐤𝐓𝐨𝐤✧❩〕"""

    # Gửi ảnh với keyboard
    try:
        keyboard = tao_keyboard_lien_ket_nhom()
        await bot.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{noi_dung}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        # Xóa tin nhắn người dùng
        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass

    except Exception as e:
        # Fallback về text nếu không gửi được ảnh
        logger.error(f"Lỗi gửi ảnh: {e}")
        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)

    return True

@cooldown_nguoi_dung()
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_ngl(message: Message):
    """Xử lý lệnh NGL"""
    # Kiểm tra lệnh có bị khóa không
    if await kiem_tra_lenh_bi_khoa(message, "ngl"):
        return False

    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    cac_tham_so = trich_xuat_tham_so(message)

    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "👼🏻 Cú pháp: /ngl [link ngl]",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8,
            co_keyboard=True
        )
        return False

    link_ngl = cac_tham_so[0].strip()

    if not ("ngl.link" in link_ngl):
        await gui_phan_hoi(
            message,
            "👼🏻 Link NGL không hợp lệ!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8,
            co_keyboard=True
        )
        return False

    script_ngl = "spamngl.py"
    thanh_cong, pid, file_log = chay_tien_trinh_nen_sync(
        f"python3 {script_ngl} {link_ngl} 1000",
        timeout=TIMEOUT_MO_RONG
    )

    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "👼🏻 Lỗi khi khởi động lệnh NGL!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8,
            co_keyboard=True
        )
        return False

    tieu_de_quyen = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = f"""{tieu_de_quyen}
┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {lien_ket_nguoi_dung}
┃• 🎫 𝑀ã 𝐼𝐷       :      {user_id}
╭━━━━━〔❨✧✧❩〕
 • Link         :     {escape_html(link_ngl[:30])}...
 • Target           :     1000+ messages
 • 𝑉𝑖̣ 𝑡𝑟𝑖́        :     𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒
 • 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛      :     {chuoi_gio}
 • 𝑇𝑜𝑑𝑎𝑦             :     {chuoi_ngay}
╰━━━━〔❨✧𝐍𝐆𝐋✧❩〕"""

    # Gửi ảnh với keyboard
    try:
        keyboard = tao_keyboard_lien_ket_nhom()
        await bot.send_photo(
            chat_id=message.chat.id,
            photo="https://files.catbox.moe/59n41m.jpeg",
            caption=f"<blockquote>{noi_dung}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

        # Xóa tin nhắn người dùng
        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass

    except Exception as e:
        # Fallback về text nếu không gửi được ảnh
        logger.error(f"Lỗi gửi ảnh: {e}")
        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)

    return True

@cooldown_nguoi_dung()
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_kill_tien_trinh(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id

    pattern = f"lenh.*{user_id}"
    thanh_cong = tat_tien_trinh_dong_bo(pattern)
    
    with FULL_LOCK:
        keys_to_remove = [key for key in FULL_STATUS.keys() if key.startswith(f"{user_id}:")]
        for key in keys_to_remove:
            FULL_STATUS.pop(key, None)

    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    if thanh_cong:
        noi_dung = f"User : {lien_ket_nguoi_dung}\nĐã dừng tất cả tiến trình của bạn!"
    else:
        noi_dung = f"User : {lien_ket_nguoi_dung}\nKhông tìm thấy tiến trình nào để dừng!"

    await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8)
    return True

@chi_nhom
async def xu_ly_checkid(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    ten_day_du = user.full_name or 'Unknown'

    cap_do = lay_cap_do_quyen_nguoi_dung(user_id)
    tieu_de_quyen = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    noi_dung = f"""{tieu_de_quyen}
┃• 👼🏻 𝑀𝑟.𝑈𝑠𝑒𝑟    :      {lien_ket_nguoi_dung}
┃• 🎫 𝑀ã 𝐼𝐷       :      {user_id}
┃• ✨ 𝑄𝑢𝑦𝑒̂̀𝑛      :      {cap_do}"""

    await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)

# ADMIN COMMAND
@cooldown_nguoi_dung()
@chi_nhom
@chi_admin
async def xu_ly_kill_tat_ca_tien_trinh(message: Message):
    if not message.from_user:
        return False
    user = message.from_user

    thanh_cong = tat_tien_trinh_dong_bo("python.*lenh")

    with FULL_LOCK:
        FULL_STATUS.clear()

    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    if thanh_cong:
        noi_dung = f"𝐴𝑑𝑚𝑖𝑛 : {lien_ket_nguoi_dung}\n𝐷𝑎̃ 𝑑𝑢̛̀𝑛𝑔 𝑇𝐴̂́𝑇 𝐶𝐴̉ 𝑡𝑖𝑒̂́𝑛 𝑡𝑟𝑖̀𝑛ℎ 𝑡𝑟𝑜𝑛𝑔 ℎ𝑒̣̂ 𝑡ℎ𝑜̂́𝑛𝑔!"
    else:
        noi_dung = f"𝐴𝑑𝑚𝑖𝑛 : {lien_ket_nguoi_dung}\n𝐾ℎ𝑜̂𝑛𝑔 𝑡𝑖̀𝑚 𝑡ℎ𝑎̂́𝑦 𝑡𝑖𝑒̂́𝑛 𝑡𝑟𝑖̀𝑛ℎ 𝑛𝑎̀𝑜 𝑑𝑒̂̉ 𝑑𝑢̛̀𝑛𝑔!"

    await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=10)
    return True

@cooldown_nguoi_dung()
@chi_nhom
@chi_admin
async def xu_ly_them_vip(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    cac_tham_so = trich_xuat_tham_so(message)

    if len(cac_tham_so) < 1:
        await gui_phan_hoi(
            message,
            "👼🏻 Cú pháp: /themvip USER_ID [TÊN]",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    id_muc_tieu = cac_tham_so[0].strip()
    ten_muc_tieu = " ".join(cac_tham_so[1:]) if len(cac_tham_so) > 1 else "VIP User"

    try:
        them_vip(id_muc_tieu, ten_muc_tieu)
        noi_dung = f"Đã thêm VIP: {id_muc_tieu} - {ten_muc_tieu}"
        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True)
        return True
    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi thêm VIP: {str(e)}",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

@cooldown_nguoi_dung()
@chi_nhom
@chi_admin
async def xu_ly_xoa_vip(message: Message):
    cac_tham_so = trich_xuat_tham_so(message)

    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "👼🏻 Cú pháp: /xoavip USER_ID",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    id_muc_tieu = cac_tham_so[0].strip()

    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM admin WHERE user_id = ? AND role = 'vip'", (id_muc_tieu,))
        so_hang_xoa = cursor.rowcount
        conn.commit()
        conn.close()

        # Xóa cache quyền ngay lập tức để cập nhật nhanh
        quan_ly_quyen_cache.cache.pop(str(id_muc_tieu), None)

        if so_hang_xoa > 0:
            noi_dung = f"Đã xóa VIP: {id_muc_tieu}"
        else:
            noi_dung = f"Không tìm thấy VIP: {id_muc_tieu}"

        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True)
        return True
    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi xóa VIP: {str(e)}",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

@cooldown_nguoi_dung()
@chi_nhom
@chi_admin
async def xu_ly_them_admin(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    cac_tham_so = trich_xuat_tham_so(message)

    if len(cac_tham_so) < 1:
        await gui_phan_hoi(
            message,
            "👼🏻 Cú pháp: /themadmin USER_ID [TÊN]",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    id_muc_tieu = cac_tham_so[0].strip()
    ten_muc_tieu = " ".join(cac_tham_so[1:]) if len(cac_tham_so) > 1 else "Admin User"

    if id_muc_tieu == str(user.id):
        await gui_phan_hoi(
            message,
            "Không thể tự thêm admin cho chính mình!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    try:
        them_admin(id_muc_tieu, ten_muc_tieu)
        noi_dung = f"Đã thêm Admin: {id_muc_tieu} - {ten_muc_tieu}"
        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True)
        return True
    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi thêm Admin: {str(e)}",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

@cooldown_nguoi_dung()
@chi_nhom
@chi_admin
async def xu_ly_xoa_admin(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    cac_tham_so = trich_xuat_tham_so(message)

    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "👼🏻 Cú pháp: /xoaadmin USER_ID",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    id_muc_tieu = cac_tham_so[0].strip()

    if id_muc_tieu == ID_ADMIN_MAC_DINH:
        await gui_phan_hoi(
            message,
            "Không thể xóa Super Admin!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    if id_muc_tieu == str(user.id):
        await gui_phan_hoi(
            message,
            "Không thể tự xóa admin của chính mình!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM admin WHERE user_id = ? AND role = 'admin'", (id_muc_tieu,))
        so_hang_xoa = cursor.rowcount
        conn.commit()
        conn.close()

        # Xóa cache quyền ngay lập tức để cập nhật nhanh
        quan_ly_quyen_cache.cache.pop(str(id_muc_tieu), None)

        if so_hang_xoa > 0:
            noi_dung = f"Đã xóa Admin: {id_muc_tieu}"
        else:
            noi_dung = f"Không tìm thấy Admin: {id_muc_tieu}"

        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True)
        return True
    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi xóa Admin: {str(e)}",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

@chi_nhom
@chi_admin
async def xu_ly_xem_danh_sach_vip(message: Message):
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, name, role FROM admin ORDER BY role, user_id")
        danh_sach = cursor.fetchall()
        conn.close()

        if not danh_sach:
            await gui_phan_hoi(
                message,
                "📋Chưa có VIP/Admin nào trong hệ thống!",
                xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=15
            )
            return False

        noi_dung = "📋 DANH SÁCH VIP & ADMIN:\n\n"

        admin_list = []
        vip_list = []

        for item in danh_sach:
            if item[2] == 'admin':
                admin_list.append(item)
            elif item[2] == 'vip':
                vip_list.append(item)

        if admin_list:
            noi_dung += "👑 ADMIN:\n"
            for i, admin in enumerate(admin_list, 1):
                noi_dung += f"  {i}. {admin[1]} ({admin[0]})\n"
            noi_dung += "\n"

        if vip_list:
            noi_dung += "VIP:\n"
            for i, vip in enumerate(vip_list, 1):
                noi_dung += f"  {i}. {vip[1]} ({vip[0]})\n"

        noi_dung += f"\nTổng: {len(admin_list)} Admin, {len(vip_list)} VIP"

        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True)
        return True

    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi lấy danh sách: {str(e)}",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

@cooldown_nguoi_dung()
@chi_nhom
@chi_admin
async def xu_ly_don_dep_vps(message: Message):
    """Xử lý lệnh dọn dẹp VPS - chỉ thông báo 1 lần"""
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    tieu_de_quyen = lay_tieu_de_quyen(user_id)

    try:
        # Chạy script dọn dẹp VPS không đồng bộ
        chay_tien_trinh_nen_sync("python3 vps.py", timeout=180, user_id=user_id)

        noi_dung = f""" Admin      :     {lien_ket_nguoi_dung}\nDọn dẹp hệ thống thành công !"""

        await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)
        return True

    except FileNotFoundError:
        await gui_phan_hoi(
            message,
            "👼🏻 Không tìm thấy file vps.py!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False
    except Exception as e:
        logger.error(f"Lỗi khi khởi động VPS cleanup: {e}")
        await gui_phan_hoi(
            message,
            f"👼🏻 Lỗi khi khởi động VPS cleanup: {str(e)}",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

@cooldown_nguoi_dung()
@chi_nhom
@chi_admin
async def xu_ly_proxy(message: Message):
    """Xử lý lệnh proxy - chỉ thông báo 1 lần"""
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id

    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()
    tieu_de_quyen = lay_tieu_de_quyen(user_id)

    try:
        # Chạy script proxy không đồng bộ
        thanh_cong, pid, _ = chay_tien_trinh_nen_sync("python3 1.py", timeout=300, user_id=user_id)

        if thanh_cong:
            noi_dung = f"""Admin    :     {lien_ket_nguoi_dung}\nĐang lọc proxy, kết thúc sau 30p nữa !"""


            await gui_phan_hoi(message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True)
            return True
        else:
            await gui_phan_hoi(
                message,
                "👼🏻 Không thể khởi động proxy service!",
                xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
            )
            return False

    except FileNotFoundError:
        await gui_phan_hoi(
            message,
            "👼🏻 Không tìm thấy file 1.py!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False
    except Exception as e:
        logger.error(f"Lỗi khi khởi động proxy: {e}")
        await gui_phan_hoi(
            message,
            f"👼🏻 Lỗi khi khởi động proxy: {str(e)}",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

@chi_nhom
async def xu_ly_random_anh(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id

    # Đọc danh sách ảnh từ file
    danh_sach_anh = doc_file_js("images.js")

    if not danh_sach_anh:
        await gui_phan_hoi(
            message,
            "👼🏻 Không tìm thấy danh sách ảnh!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    # Chọn ảnh random
    anh_random = random.choice(danh_sach_anh)

    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    try:
        await asyncio.wait_for(
            bot.send_photo(
                chat_id=message.chat.id,
                photo=anh_random,
                caption=f"<blockquote>🏓 𝑅𝑎𝑛𝑑𝑜𝑚 𝐴̉𝑛ℎ 𝑐ℎ𝑜 {lien_ket_nguoi_dung}\n"
                       f"⏱️ Thời gian: {chuoi_gio} - {chuoi_ngay}</blockquote>",
                parse_mode=ParseMode.HTML,
            ),
            timeout=30.0
        )

        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass

        return True

    except asyncio.TimeoutError:
        await gui_phan_hoi(
            message,
            "👼🏻 Timeout khi tải ảnh! Thử lại sau.",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False
    except Exception as e:
        logger.error(f"Lỗi gửi ảnh: {e}")
        if "failed to get HTTP URL content" in str(e) and len(danh_sach_anh) > 1:
            anh_backup = random.choice([a for a in danh_sach_anh if a != anh_random])
            try:
                await bot.send_photo(
                    chat_id=message.chat.id,
                    photo=anh_backup,
                    caption=f"<blockquote>🏓 𝑅𝑎𝑛𝑑𝑜𝑚 𝐴̉𝑛ℎ 𝑐ℎ𝑜 {lien_ket_nguoi_dung}\n"
                           f"⏱️ Thời gian: {chuoi_gio} - {chuoi_ngay}</blockquote>",
                    parse_mode=ParseMode.HTML,
                )
                return True
            except Exception:
                pass

        await gui_phan_hoi(
            message,
            "👼🏻 Không thể tải ảnh! URL có thể bị lỗi.",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

@chi_nhom
async def xu_ly_random_video(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    danh_sach_video = doc_file_js("videos.js")
    danh_sach_gif = doc_file_js("video2.js")

    tat_ca_video = danh_sach_video + danh_sach_gif

    if not tat_ca_video:
        await gui_phan_hoi(
            message,
            "👼🏻 Không tìm thấy danh sách video!",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

    # Chọn video random
    video_random = random.choice(tat_ca_video)

    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

    try:
        if video_random.endswith('.gif') or 'giphy' in video_random:
            # Gửi GIF
            await asyncio.wait_for(
                bot.send_animation(
                    chat_id=message.chat.id,
                    animation=video_random,
                    caption=f"<blockquote>🎬 𝑅𝑎𝑛𝑑𝑜𝑚 𝐺𝐼𝐹 𝑐ℎ𝑜 {lien_ket_nguoi_dung}\n"
                           f"⏱️ Thời gian: {chuoi_gio} - {chuoi_ngay}</blockquote>",
                    parse_mode=ParseMode.HTML,
                ),
                timeout=45.0
            )
        else:
            await asyncio.wait_for(
                bot.send_video(
                    chat_id=message.chat.id,
                    video=video_random,
                    caption=f"<blockquote>🎬 𝑅𝑎𝑛𝑑𝑜𝑚 𝑉𝑖𝑑𝑒𝑜 𝑐ℎ𝑜 {lien_ket_nguoi_dung}\n"
                           f"⏱️ Thời gian: {chuoi_gio} - {chuoi_ngay}</blockquote>",
                    parse_mode=ParseMode.HTML,
                ),
                timeout=45.0
            )

        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass

        return True

    except asyncio.TimeoutError:
        await gui_phan_hoi(
            message,
            "👼🏻 Timeout khi tải video! File quá lớn.",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False
    except Exception as e:
        logger.error(f"Lỗi gửi video: {e}")
        if "failed to get HTTP URL content" in str(e) and len(tat_ca_video) > 1:
            video_backup = random.choice([v for v in tat_ca_video if v != video_random])
            try:
                if video_backup.endswith('.gif') or 'giphy' in video_backup:
                    await bot.send_animation(
                        chat_id=message.chat.id,
                        animation=video_backup,
                        caption=f"<blockquote>🎬 Random GIF cho {lien_ket_nguoi_dung}\n"
                               f"⏱️ Thời gian: {chuoi_gio} - {chuoi_ngay}</blockquote>",
                        parse_mode=ParseMode.HTML,
                    )
                else:
                    await bot.send_video(
                        chat_id=message.chat.id,
                        video=video_backup,
                        caption=f"<blockquote>🎬 Random Video cho {lien_ket_nguoi_dung}\n"
                               f"⏱️ Thời gian: {chuoi_gio} - {chuoi_ngay}</blockquote>",
                        parse_mode=ParseMode.HTML,
                    )
                return True
            except Exception:
                pass

        await gui_phan_hoi(
            message,
            "👼🏻 Không thể tải video! URL có thể bị lỗi.",
            xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False



async def xu_ly_tin_nhan_khong_phai_lenh(message: Message):
    try:
        if not message.from_user:
            return
        user_id = message.from_user.id
        is_bot = message.from_user.is_bot or False

        if message.from_user.is_bot:
            return

        if message.chat.id != ID_NHOM_CHO_PHEP:
            if la_admin(user_id):
                return

            try:
                await bot.send_message(
                    chat_id=message.chat.id,
                    text="<blockquote>🏓 𝐵𝑜𝑡 𝑐ℎ𝑖̉ ℎ𝑜𝑎̣𝑡 𝑑𝑜̣̂𝑛𝑔 𝑡𝑟𝑜𝑛𝑔 𝑛ℎ𝑜́𝑚:\n\n"

                         "🚀@attack_vip_cnc 🎯\n\n"

                         "𝑇𝑖𝑛 𝑛ℎ𝑎̆́𝑛 𝑡𝑖𝑒̂́𝑝 𝑡ℎ𝑒𝑜 𝑠𝑒̃ 𝑏𝑖̣ 𝑥𝑜́𝑎 𝑡𝑢̛̣ 𝑑𝑜̣̂𝑛𝑔 !</blockquote>",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass

            try:
                await bot.delete_message(
                    chat_id=message.chat.id,
                    message_id=message.message_id
                )
            except Exception:
                pass
            return
        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id
            )
        except Exception:
            pass
        if message.text and message.text.startswith('/'):
            cac_lenh_hop_le = ['/start', '/vip', '/checkid', '/call', '/sms',
                             '/spam', '/ping', '/full', '/ddos',
                             '/kill', '/killall', '/themvip', '/xoavip',
                             '/themadmin', '/xoaadmin', '/listvip', '/vps', '/prx',
                             '/img', '/vid', '/tiktok', '/ngl']

            phan_lenh = message.text.split()[0]
            if '@' in phan_lenh:
                lenh = phan_lenh.split('@')[0]
            else:
                lenh = phan_lenh

            if lenh not in cac_lenh_hop_le:
                try:
                    phan_hoi = await bot.send_message(
                        chat_id=message.chat.id,
                        text="<blockquote>🏓 𝐶ℎ𝑖̉ 𝑠𝑢̛̉ 𝑑𝑢̣𝑛𝑔 𝑐𝑎́𝑐 𝑙𝑒̣̂𝑛ℎ 𝑑𝑢̛𝑜̛̣𝑐 𝑝ℎ𝑒́𝑝!\n𝐺𝑜̃ /start 𝑑𝑒̂̉ 𝑥𝑒𝑚 𝑑𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ 𝑙𝑒̣̂𝑛ℎ</blockquote>",
                        parse_mode=ParseMode.HTML
                    )
                    asyncio.create_task(tu_dong_xoa_tin_nhan(phan_hoi.chat.id, phan_hoi.message_id, 8))
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Lỗi trong xu_ly_tin_nhan_khong_phai_lenh: {e}")

async def periodic_cleanup():
    cleanup_interval = 3600  # 1 giờ - giảm tần suất cleanup
    process_cleanup_interval = 1800  # 30 phút - cleanup processes cũ
    last_process_cleanup = 0

    while True:
        try:
            await asyncio.sleep(cleanup_interval)

            start_time = time.time()
            logger.info("🧹 Bắt đầu dọn dẹp định kỳ...")

            # 1. Cache cleanup
            try:
                cleanup_old_cache()
                logger.info("Dọn cache hoàn thành")
            except Exception as e:
                logger.error(f"Lỗi dọn cache: {e}")
            try:
                await cleanup_full_status_safe()
            except Exception as e:
                logger.error(f"Lỗi dọn FULL_STATUS: {e}")

            # 3. Cleanup processes cũ (mỗi 30 phút)
            current_time = time.time()
            if current_time - last_process_cleanup > process_cleanup_interval:
                try:
                    await cleanup_old_processes()
                    last_process_cleanup = current_time
                except Exception as e:
                    logger.error(f"Lỗi cleanup processes cũ: {e}")

            # 4. Force garbage collection
            try:
                import gc
                collected = gc.collect()
                if collected > 0:
                    logger.info(f"🗑️ Thu gom {collected} objects")
            except Exception as e:
                logger.error(f"Lỗi garbage collection: {e}")

            # 5. Kiểm tra memory usage
            try:
                import psutil
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                cpu_percent = process.cpu_percent()
                logger.info(f"📊 Memory: {memory_mb:.1f}MB, CPU: {cpu_percent:.1f}%")

                # Cảnh báo nếu memory quá cao
                if memory_mb > 500:
                    logger.warning(f"⚠️ Memory cao: {memory_mb:.1f}MB")

            except ImportError:
                logger.debug("psutil không có, bỏ qua monitor memory")
            except Exception as e:
                logger.error(f"Lỗi check memory: {e}")

            duration = time.time() - start_time
            logger.info(f"Dọn dẹp hoàn thành trong {duration:.1f}s")

        except asyncio.CancelledError:
            logger.info("🛑 Cleanup task bị hủy")
            break
        except Exception as e:
            logger.error(f"Lỗi periodic cleanup: {e}", exc_info=True)

async def cleanup_old_processes():
    """Dọn dẹp processes cũ chạy quá 6 giờ"""
    logger.info("🔍 Kiểm tra processes cũ...")

    old_processes = []
    current_time = time.time()

    try:
        for proc in psutil.process_iter(['pid', 'cmdline', 'create_time', 'name']):
            try:
                proc_info = proc.info
                if not proc_info['cmdline']:
                    continue

                cmdline = ' '.join(proc_info['cmdline'])
                create_time = proc_info.get('create_time', 0)

                # Kiểm tra process Python liên quan
                is_target = (
                    ('python' in proc_info['name'].lower() or 'python' in cmdline.lower()) and
                    any(script in cmdline for script in [
                        'spam_', 'sms_', 'vip_', 'call', 'tcp.py', 'tt.py',
                        'ngl.py', 'pro24h.py', 'vip11122.py', 'master222.py'
                    ])
                )

                if is_target and create_time:
                    age = current_time - create_time
                    if age > 21600:  # 6 giờ
                        old_processes.append({
                            'pid': proc_info['pid'],
                            'age_hours': age / 3600,
                            'cmdline': cmdline[:100]
                        })

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        if old_processes:
            for proc_data in old_processes:
                logger.warning(f"  PID {proc_data['pid']} ({proc_data['age_hours']:.1f}h): {proc_data['cmdline']}")

            killed = tat_tien_trinh_dong_bo("python.*lenh")
            if killed:
                logger.info(f"Đã dọn dẹp processes cũ")
            else:
                logger.warning("⚠️ Không thể dọn dẹp một số processes cũ")
        else:
            logger.info("Không có processes cũ cần dọn dẹp")

    except Exception as e:
        logger.error(f"Lỗi cleanup_old_processes: {e}")

async def cleanup_full_status_safe():
    """Dọn dẹp FULL_STATUS an toàn với batch processing"""
    if 'FULL_STATUS' not in globals() or 'FULL_LOCK' not in globals():
        return

    try:
        current_time = time.time()
        keys_to_remove = []
        with FULL_LOCK:
            keys_to_remove = [k for k, v in FULL_STATUS.items() 
                             if v < current_time - 3600]  # 1 giờ buffer
        if keys_to_remove:
            batch_size = 50
            removed_total = 0

            for i in range(0, len(keys_to_remove), batch_size):
                batch = keys_to_remove[i:i + batch_size]
                with FULL_LOCK:
                    for key in batch:
                        FULL_STATUS.pop(key, None)
                        removed_total += 1

                # Nghỉ giữa các batch
                if i + batch_size < len(keys_to_remove):
                    await asyncio.sleep(0.01)

            logger.info(f"🧹 Đã xóa {removed_total} entries cũ từ FULL_STATUS")

    except Exception as e:
        logger.error(f"Lỗi cleanup FULL_STATUS: {e}")

async def main():
    """Hàm chính với retry mechanism cải tiến"""
    if not MA_TOKEN_BOT or MA_TOKEN_BOT == "YOUR_BOT_TOKEN_HERE":
        logger.error("Token bot không hợp lệ!")
        return

    max_retries = 10
    retry_delay = 2
    cleanup_task = None

    for attempt in range(max_retries):
        try:
            logger.info(f"🚀 Khởi động bot - Lần thử {attempt + 1}/{max_retries}")

            # Khởi tạo database
            try:
                khoi_tao_database()
                khoi_tao_admin_mac_dinh()
                logger.info("Database khởi tạo thành công")
            except Exception as e:
                logger.error(f"Lỗi khởi tạo database: {e}")
                raise

            dp = Dispatcher()
            router = create_router()
            dp.include_router(router)

            bot_info = None
            for connect_attempt in range(3):
                try:
                    logger.info(f"🔌 Thử kết nối lần {connect_attempt + 1}/3...")
                    bot_info = await asyncio.wait_for(bot.get_me(), timeout=30.0)
                    logger.info(f"Bot kết nối thành công: @{bot_info.username}")
                    break
                except (asyncio.TimeoutError, TelegramNetworkError) as e:
                    logger.error(f"Lỗi kết nối lần {connect_attempt + 1}: {e}")
                    if connect_attempt < 2:
                        await asyncio.sleep(5)
                        continue
                    else:
                        raise Exception("Không thể kết nối đến Telegram sau 3 lần thử")

            if not bot_info:
                raise Exception("Không thể lấy thông tin bot")

            # Khởi động cleanup task
            cleanup_task = asyncio.create_task(periodic_cleanup())
            logger.info("🔄 Bắt đầu polling...")
            try:
                await dp.start_polling(
                    bot,
                    drop_pending_updates=True,  # Bỏ qua tin nhắn cũ
                    timeout=20,                 # Timeout hợp lý
                    relax=0.1,                  # Delay ít giữa requests  
                    fast=True,                  # Bật fast mode
                    handle_as_tasks=True,       # Xử lý concurrent
                    allowed_updates=['message', 'callback_query']  # Chỉ nhận cần thiết
                )
            finally:
                # Đảm bảo cleanup task được hủy
                if cleanup_task and not cleanup_task.done():
                    cleanup_task.cancel()
                    try:
                        await cleanup_task
                    except asyncio.CancelledError:
                        pass
                    logger.info(" Cleanup task đã dừng")

            logger.info("Bot chạy thành công và kết thúc bình thường!")
            break

        except KeyboardInterrupt:
            logger.info("⏹️ Bot bị dừng bởi người dùng")
            break

        except (TelegramNetworkError, asyncio.TimeoutError) as e:
            logger.error(f"🌐 Lỗi mạng lần {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                wait_time = min(retry_delay * (attempt + 1), 60)
                logger.info(f"⏳ Chờ {wait_time}s trước khi thử lại...")
                await asyncio.sleep(wait_time)
                continue
            else:
                logger.error("Hết số lần thử kết nối mạng")
                break

        except Exception as e:
            logger.error(f"💥 Lỗi không mong muốn lần {attempt + 1}: {e}", exc_info=True)
            if attempt < max_retries - 1:
                wait_time = min(retry_delay * 2, 30)
                logger.info(f"⏳ Chờ {wait_time}s trước khi restart...")
                await asyncio.sleep(wait_time)
                retry_delay = min(retry_delay * 1.5, 30)
                continue
            else:
                logger.error("Đã hết số lần thử, dừng bot")
                break
        finally:
            # Cleanup nếu có lỗi
            if cleanup_task and not cleanup_task.done():
                cleanup_task.cancel()
                try:
                    await cleanup_task
                except asyncio.CancelledError:
                    pass

def chay_bot():
    import signal
    atexit.register(lambda: tat_tien_trinh_dong_bo("python.*lenh"))

    def signal_handler(signum, frame):
        signal_name = {
            signal.SIGINT: "SIGINT (Ctrl+C)",
            signal.SIGTERM: "SIGTERM (Kill)",
            signal.SIGHUP: "SIGHUP (Hangup)"
        }.get(signum, f"Signal {signum}")

        logger.info(f" Nhận {signal_name}, đang dọn dẹp...")
        try:
            tat_tien_trinh_dong_bo("python.*lenh")
            logger.info("Cleanup hoàn thành")
        except Exception as e:
            logger.error(f"Lỗi cleanup: {e}")
        exit(0)

    for sig in [signal.SIGINT, signal.SIGTERM]:
        if hasattr(signal, sig.name):  # Kiểm tra signal có tồn tại không
            signal.signal(sig, signal_handler)

    # Thêm SIGHUP nếu không phải Windows
    if os.name != 'nt' and hasattr(signal, 'SIGHUP'):
        signal.signal(signal.SIGHUP, signal_handler)

    max_restarts = 10  # Giảm từ 50 xuống 10
    restart_count = 0
    consecutive_failures = 0
    start_time = time.time()

    logger.info("🤖 Khởi động hệ thống bot...")

    while restart_count < max_restarts:
        bot_start_time = time.time()
        try:
            if os.name == 'nt':
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

            asyncio.run(main())
            logger.info(" Bot kết thúc bình thường")
            break

        except KeyboardInterrupt:
            logger.info("⏹️ Bot bị dừng bởi người dùng")
            break

        except Exception as e:
            runtime = time.time() - bot_start_time
            total_runtime = time.time() - start_time

            if runtime > 3600:
                consecutive_failures = 0
                logger.info("🔄 Reset failure count do bot chạy lâu")
            else:
                consecutive_failures += 1

            logger.error(f"💥 Bot crash sau {runtime:.1f}s (tổng: {total_runtime/3600:.1f}h): {e}")
            restart_count += 1

            try:
                tat_tien_trinh_dong_bo("python.*lenh")
            except Exception as cleanup_error:
                logger.error(f"Lỗi cleanup: {cleanup_error}")

            if restart_count < max_restarts:
                base_wait = min(consecutive_failures * 10, 300)  # Tối đa 5 phút
                actual_wait = min(base_wait, 60)  # Giới hạn 1 phút cho lần đầu

                logger.info(f"⏳ Chờ {actual_wait}s trước khi restart (lần {restart_count}/{max_restarts})")
                time.sleep(actual_wait)
            else:
                logger.error("Đã đạt giới hạn restart, dừng bot")
                break

    total_runtime = time.time() - start_time
    logger.info(f"🏁 Bot dừng hoàn toàn sau {total_runtime/3600:.1f} giờ")

if __name__ == "__main__":
    chay_bot()



#7


import telebot
import requests
import urllib.parse
import random
import json
import hashlib
import html
import hmac
import time
import re
from datetime import datetime
from telebot import types
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

TOKEN = "8468137663:AAEPQr8XrMLwWpS5CU1ejXDj6ypMhD-eFV4"
bot = telebot.TeleBot(TOKEN, parse_mode="HTML")

TOMORROW_API_KEY = "mdTWQAInBIDB3mHiDtkwuTlwhVB50rqn"
OPENWEATHER_API_KEY = "e707d13f116e5f7ac80bd21c37883e5e"
WEATHERAPI_KEY = "fe221e3a25734f0297994922240611"
ZING_API_KEY = "X5BM3w8N7MKozC0B85o4KMlzLZKhV00y"
ZING_SECRET_KEY = "acOrvUS15XRW2o9JksiK1KgQ6Vbds8ZW"
ZING_VERSION = "1.11.11"
ZING_URL = "https://zingmp3.vn"

zingmp3_data = {}

bank_codes = {
    "vcb": {
        "bin": "970436",
        "name": "VIETCOMBANK",
        "short_name": "Vietcombank"
    },
    "vietcombank": {
        "bin": "970436",
        "name": "VIETCOMBANK",
        "short_name": "Vietcombank"
    },
    "tcb": {
        "bin": "970407",
        "name": "TECHCOMBANK",
        "short_name": "Techcombank"
    },
    "techcombank": {
        "bin": "970407",
        "name": "TECHCOMBANK",
        "short_name": "Techcombank"
    },
    "mb": {
        "bin": "970422",
        "name": "MB BANK",
        "short_name": "MBBank"
    },
    "mbbank": {
        "bin": "970422",
        "name": "MB BANK",
        "short_name": "MBBank"
    },
    "mb bank": {
        "bin": "970422",
        "name": "MB BANK",
        "short_name": "MBBank"
    },
    "acb": {
        "bin": "970416",
        "name": "ACB",
        "short_name": "ACB"
    },
    "vib": {
        "bin": "970441",
        "name": "VIB",
        "short_name": "VIB"
    },
    "bidv": {
        "bin": "970418",
        "name": "BIDV",
        "short_name": "BIDV"
    },
    "vietinbank": {
        "bin": "970415",
        "name": "VIETINBANK",
        "short_name": "VietinBank"
    },
    "vtb": {
        "bin": "970415",
        "name": "VIETINBANK",
        "short_name": "VietinBank"
    },
    "tpbank": {
        "bin": "970423",
        "name": "TPBANK",
        "short_name": "TPBank"
    },
    "vpbank": {
        "bin": "970432",
        "name": "VPBANK",
        "short_name": "VPBank"
    },
    "agribank": {
        "bin": "970405",
        "name": "AGRIBANK",
        "short_name": "Agribank"
    },
    "sacombank": {
        "bin": "970403",
        "name": "SACOMBANK",
        "short_name": "Sacombank"
    },
    "scb": {
        "bin": "970429",
        "name": "SCB",
        "short_name": "SCB"
    },
    "hdbank": {
        "bin": "970437",
        "name": "HDBANK",
        "short_name": "HDBank"
    },
    "ocb": {
        "bin": "970448",
        "name": "OCB",
        "short_name": "OCB"
    },
    "msb": {
        "bin": "970426",
        "name": "MSB",
        "short_name": "MSB"
    },
    "maritimebank": {
        "bin": "970426",
        "name": "MSB",
        "short_name": "MSB"
    },
    "shb": {
        "bin": "970443",
        "name": "SHB",
        "short_name": "SHB"
    },
    "eximbank": {
        "bin": "970431",
        "name": "EXIMBANK",
        "short_name": "Eximbank"
    },
    "exim": {
        "bin": "970431",
        "name": "EXIMBANK",
        "short_name": "Eximbank"
    },
    "dongabank": {
        "bin": "970406",
        "name": "DONGABANK",
        "short_name": "DongA Bank"
    },
    "dab": {
        "bin": "970406",
        "name": "DONGABANK",
        "short_name": "DongA Bank"
    },
    "pvcombank": {
        "bin": "970412",
        "name": "PVCOMBANK",
        "short_name": "PVcomBank"
    },
    "gpbank": {
        "bin": "970408",
        "name": "GPBANK",
        "short_name": "GPBank"
    },
    "oceanbank": {
        "bin": "970414",
        "name": "OCEANBANK",
        "short_name": "OceanBank"
    },
    "namabank": {
        "bin": "970428",
        "name": "NAMABANK",
        "short_name": "Nam A Bank"
    },
    "ncb": {
        "bin": "970419",
        "name": "NCB",
        "short_name": "NCB"
    },
    "vietabank": {
        "bin": "970427",
        "name": "VIETABANK",
        "short_name": "VietABank"
    },
    "vietbank": {
        "bin": "970433",
        "name": "VIETBANK",
        "short_name": "Vietbank"
    },
    "vrb": {
        "bin": "970421",
        "name": "VRB",
        "short_name": "VRB"
    },
    "wooribank": {
        "bin": "970457",
        "name": "WOORIBANK",
        "short_name": "Woori Bank"
    },
    "uob": {
        "bin": "970458",
        "name": "UOB",
        "short_name": "UOB"
    },
    "standardchartered": {
        "bin": "970410",
        "name": "STANDARD CHARTERED",
        "short_name": "Standard Chartered"
    },
    "publicbank": {
        "bin": "970439",
        "name": "PUBLIC BANK",
        "short_name": "Public Bank"
    },
    "shinhanbank": {
        "bin": "970424",
        "name": "SHINHAN BANK",
        "short_name": "Shinhan Bank"
    },
    "hsbc": {
        "bin": "458761",
        "name": "HSBC",
        "short_name": "HSBC"
    },
    "coop": {
        "bin": "970446",
        "name": "COOPBANK",
        "short_name": "Co-opBank"
    },
    "coopbank": {
        "bin": "970446",
        "name": "COOPBANK",
        "short_name": "Co-opBank"
    },
    "lienvietpostbank": {
        "bin": "970449",
        "name": "LIENVIETPOSTBANK",
        "short_name": "LienVietPostBank"
    },
    "lvb": {
        "bin": "970449",
        "name": "LIENVIETPOSTBANK",
        "short_name": "LienVietPostBank"
    },
    "baovietbank": {
        "bin": "970438",
        "name": "BAOVIETBANK",
        "short_name": "BaoViet Bank"
    },
    "bvb": {
        "bin": "970438",
        "name": "BAOVIETBANK",
        "short_name": "BaoViet Bank"
    }
}

weather_codes = {
    1000: "Quang đãng",
    1100: "Có mây nhẹ",
    1101: "Có mây",
    1102: "Nhiều mây",
    1001: "Âm u",
    2000: "Sương mù",
    2100: "Sương mù nhẹ",
    4000: "Mưa nhỏ",
    4001: "Mưa",
    4200: "Mưa nhẹ",
    4201: "Mưa vừa",
    4202: "Mưa to",
    5000: "Tuyết",
    5001: "Tuyết rơi nhẹ",
    5100: "Mưa tuyết nhẹ",
    6000: "Mưa đá",
    6200: "Mưa đá nhẹ",
    6201: "Mưa đá nặng",
    7000: "Sấm sét",
    7101: "Sấm sét mạnh",
    7102: "Giông bão",
    8000: "Một vài cơn mưa rào"
}

def format_frame(title, content):
    return (
        f"<blockquote>"
        f"╔════════════════════╗\n"
        f"║ <b>{title.upper()}</b>\n"
        f"╠════════════════════╣\n"
        f"{content}\n"
        f"╚════════════════════╝\n"
        f"<i>✨ Powered by Duckiencoder ✨</i>"
        f"</blockquote>"
    )

def get_hash256(string):
    return hashlib.sha256(string.encode()).hexdigest()

def get_hmac512(string, key):
    return hmac.new(key.encode(), string.encode(), hashlib.sha512).hexdigest()

def get_sig(path, params):
    param_string = ''.join(f"{key}={params[key]}" for key in sorted(params.keys()) if key in ["ctime", "id", "type", "page", "count", "version"])
    return get_hmac512(path + get_hash256(param_string), ZING_SECRET_KEY)

def get_cookie():
    try:
        response = requests.get(ZING_URL)
        return response.cookies.get_dict()
    except:
        return {}

def request_zing_mp3(path, params):
    cookies = get_cookie()
    response = requests.get(f"{ZING_URL}{path}", params=params, cookies=cookies)
    return response.json()

def search_music(keyword):
    ctime = str(int(time.time()))
    path = "/api/v2/search"
    params = {
        "q": keyword,
        "type": "song",
        "count": 10,
        "ctime": ctime,
        "version": ZING_VERSION,
        "apiKey": ZING_API_KEY,
        "sig": get_sig(path, {
            "q": keyword,
            "type": "song",
            "count": 10,
            "ctime": ctime,
            "version": ZING_VERSION
        })
    }
    return request_zing_mp3(path, params)

def get_streaming_song(song_id):
    ctime = str(int(time.time()))
    path = "/api/v2/song/get/streaming"
    params = {
        "id": song_id,
        "ctime": ctime,
        "version": ZING_VERSION,
        "apiKey": ZING_API_KEY,
        "sig": get_sig(path, {
            "id": song_id,
            "ctime": ctime,
            "version": ZING_VERSION
        })
    }
    return request_zing_mp3(path, params)

def get_uv_level(index):
    if index <= 2: return "Thấp"
    if index <= 5: return "Trung bình"
    if index <= 7: return "Cao"
    if index <= 10: return "Rất cao"
    return "Nguy hiểm"

def get_wind_direction(degrees):
    directions = ["Bắc", "Đông Bắc", "Đông", "Đông Nam", "Nam", "Tây Nam", "Tây", "Tây Bắc"]
    return directions[round(degrees / 45) % 8]

def get_rain_intensity(intensity):
    if intensity == 0: return "không mưa"
    if intensity < 2.5: return "mưa nhỏ"
    if intensity < 7.6: return "mưa vừa"
    if intensity < 15.2: return "mưa to"
    if intensity < 30.4: return "mưa rất to"
    return "mưa đặc biệt to"

def get_weather_description(code):
    return weather_codes.get(code, "Không rõ")

def get_precipitation_forecast(hourly_data):
    if not isinstance(hourly_data, list): return "Không có dữ liệu dự báo mưa"
    next_24_hours = hourly_data[:24]
    rain_hour = next((h for h in next_24_hours if h.get("values", {}).get("precipitationProbability", 0) > 50), None)
    if not rain_hour:
        if any(h.get("values", {}).get("precipitationProbability", 0) > 30 for h in next_24_hours):
            return "Có thể có mưa nhỏ rải rác"
        return "Dự kiến không mưa"
    try:
        t = datetime.fromisoformat(rain_hour["time"].replace("Z", "+00:00"))
        h = t.hour
        d = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "CN"][t.weekday()]
        p = rain_hour["values"]["precipitationProbability"]
        i = get_rain_intensity(rain_hour["values"].get("rainIntensity", 0))
        sess = "sáng" if 5<=h<12 else "chiều" if 12<=h<18 else "tối" if 18<=h<22 else "đêm"
        return f"Dự báo {i} vào {sess} {d} ({p}%)"
    except:
        return "Không xác định được thời gian mưa"

def generate_vietqr_data(bank_bin, account_number, amount=0, account_name="", add_info=""):
    return f"https://img.vietqr.io/image/{bank_bin}-{account_number}-qr_only.jpg?accountName={urllib.parse.quote(account_name)}&amount={amount}&addInfo={urllib.parse.quote(add_info)}"

def get_bank_info(bank_code):
    return bank_codes.get(bank_code.lower())

def is_valid_qr_url(url):
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200 and "image" in r.headers.get("content-type", "")
    except:
        return False

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    content = (
        "<b> 💻Duckiencoder Only:</b>\n"
        "║ <i>/weather [city]</i> ➣ Dự báo thời tiết VIP\n"
        "║ <i>/qrbank [stk] [bank]</i> ➣ Tạo QR chuyển khoản\n"
        "║ <i>/idfb [link]</i> ➣ Lấy UID Facebook\n"
        "║ <i>/github [user]</i> ➣ Soi thông tin GitHub\n"
        "║ <i>/ip [address]</i> ➣ Truy vết IP Address\n"
        "║ <i>/ask [query]</i> ➣ Trí tuệ nhân tạo AI\n"
        "╟────────────────────╢\n"
        "<b>🎬 MEDIA & ENTERTAINMENT:</b>\n"
        "║ <i>/zingmp3 [song]</i> ➣ Tải nhạc Lossless\n"
        "║ <i>/tiktok [url]</i> ➣ Tải video No-Watermark\n"
        "║ <i>/tt [user]</i> ➣ Phân tích TikTok User\n"
        "║ <i>/anhgaisexy</i> ➣ Random ảnh cực phẩm\n"
        "║ <i>/reggarena</i> ➣ Tạo acc Garena (Hidden Pass)\n"
        "║ <i>/sun</i> ➣ Dự đoán Tài Xỉu Sunwin"
    )
    bot.reply_to(message, format_frame("DUCKIENCODER BOT CONTROL", content))

@bot.message_handler(commands=['github'])
def github_info(message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            bot.reply_to(message, format_frame("TRICK ALERT", "⚠️ <i>Vui lòng nhập username hợp lệ.</i>\nUSAGE: <code>/github [username]</code>"))
            return
        
        res = requests.get(f"https://keyherlyswar.x10.mx/Apidocs/getinfogithub.php?username={args[1].strip()}", timeout=15).json()
        if "user_info" not in res:
            bot.reply_to(message, format_frame("NOT FOUND", "❌ <i>Không tìm thấy dữ liệu người dùng.</i>"))
            return
        
        u = res["user_info"]
        content = (
            f"📛 <b>Name:</b> {u.get('name') or 'N/A'}\n"
            f"🔗 <b>Login:</b> <code>{u.get('login')}</code>\n"
            f"🆔 <b>UID:</b> <code>{u.get('id')}</code>\n"
            f"📍 <b>Loc:</b> {u.get('location') or 'Unknown'}\n"
            f"📦 <b>Repos:</b> {u.get('public_repos')} Public\n"
            f"👥 <b>Subs:</b> {u.get('followers')} Followers\n"
            f"📅 <b>Joined:</b> {u.get('created_at')[:10]}\n"
            f"🌐 <a href='https://github.com/{u.get('login')}'>View Profile</a>"
        )
        bot.send_photo(message.chat.id, res.get("avatar_url", ""), caption=format_frame("GITHUB INTELLIGENCE", content))
    except Exception as e:
        bot.reply_to(message, format_frame("CRITICAL ERROR", f"❌ <i>System Exception:</i> {e}"))

@bot.message_handler(commands=['qrbank'])
def qrbank_command(message):
    try:
        args = message.text[len("/qrbank"):].strip().split()
        if len(args) < 2:
            bot.reply_to(message, format_frame("SYNTAX ERROR", "⚠️ <i>Thiếu thông tin giao dịch.</i>\nUSAGE: <code>/qrbank [stk] [bank] [tiền]</code>"))
            return
        
        acc, bank, amt = args[0], args[1], 0
        name, info = "", ""
        
        if len(args) > 2:
            potential_amt = args[2].replace(',', '').replace('.', '')
            if potential_amt.isdigit():
                amt = int(potential_amt)
                if len(args) > 3: name = args[3]
                if len(args) > 4: info = " ".join(args[4:])
            else:
                name = args[2]
                if len(args) > 3: info = " ".join(args[3:])
            
        b_info = get_bank_info(bank)
        if not b_info:
            bot.reply_to(message, format_frame("BANK ERROR", "❌ <i>Mã ngân hàng không tồn tại.</i>"))
            return
            
        url = generate_vietqr_data(b_info["bin"], acc, amt, name, info)
        content = (
            f"🏦 <b>Bank:</b> {b_info['name']}\n"
            f"🔢 <b>Account:</b> <code>{acc}</code>\n"
        )
        if amt: content += f"💰 <b>Amount:</b> {amt:,} VND\n"
        if name: content += f"👤 <b>Owner:</b> {name}\n"
        if info: content += f"📝 <b>Note:</b> {info}"

        if is_valid_qr_url(url):
            bot.send_photo(message.chat.id, url, caption=format_frame("VIETQR GENERATOR", content))
        else:
            bot.reply_to(message, format_frame("API ERROR", "❌ <i>Không thể khởi tạo mã QR.</i>"))
    except Exception as e:
        bot.reply_to(message, format_frame("TRICK ERROR", f"❌ {e}"))

@bot.message_handler(commands=['weather'])
def weather_command(message):
    try:
        loc = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else random.choice(["Hà Nội", "Hồ Chí Minh", "Đà Nẵng"])
        geo = requests.get(f"https://geocoding-api.open-meteo.com/v1/search?name={loc}&count=1&language=vi&format=json").json()
        
        if not geo.get("results"):
            bot.reply_to(message, format_frame("LOCATION ERROR", "❌ <i>Địa điểm không hợp lệ.</i>"))
            return
            
        lat, lon = geo["results"][0]["latitude"], geo["results"][0]["longitude"]
        name_loc = geo["results"][0]["name"]
        
        tm = requests.get(f"https://api.tomorrow.io/v4/weather/forecast?location={lat},{lon}&apikey={TOMORROW_API_KEY}").json()
        ow = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric&lang=vi").json()
        wa = requests.get(f"http://api.weatherapi.com/v1/forecast.json?key={WEATHERAPI_KEY}&q={lat},{lon}&days=1&aqi=yes&lang=vi").json()
        
        cur, main, wind = ow["weather"][0], ow["main"], ow["wind"]
        
        content = (
            f"📍 <b>Location:</b> {name_loc.upper()}\n"
            f"🌡 <b>Temp:</b> {main['temp']}°C (Feel: {main['feels_like']}°C)\n"
            f"☁️ <b>Sky:</b> {cur['description'].capitalize()}\n"
            f"💧 <b>Humidity:</b> {main['humidity']}%\n"
            f"💨 <b>Wind:</b> {wind['speed']} m/s ({get_wind_direction(wind['deg'])})\n"
            f"☔ <b>Rain:</b> {get_precipitation_forecast(tm['timelines']['hourly'])}\n"
            f"☀️ <b>UV Index:</b> {wa['current']['uv']} ({get_uv_level(wa['current']['uv'])})\n"
            f"👁 <b>Visual:</b> {ow.get('visibility', 0)/1000} km"
        )
        bot.reply_to(message, format_frame("WEATHER REPORT", content))
    except Exception as e:
        bot.reply_to(message, format_frame("API EXCEPTION", f"❌ {e}"))

@bot.message_handler(commands=['idfb'])
def get_facebook_uid(message):
    try:
        if len(message.text.split()) < 2:
            bot.reply_to(message, format_frame("INPUT ERROR", "⚠️ <i>Vui lòng nhập link Facebook.</i>"))
            return
            
        res = requests.get(f"https://keyherlyswar.x10.mx/Apidocs/getuidfb.php?link={urllib.parse.quote(message.text.split()[1])}", timeout=15).json()
        if res.get("status") == "success":
            content = (
                f"✅ <b>Target Found</b>\n"
                f"🔗 <b>Link:</b> {message.text.split()[1]}\n"
                f"🆔 <b>UID:</b> <code>{res['uid']}</code>"
            )
            bot.reply_to(message, format_frame("FACEBOOK TOOL", content))
        else:
            bot.reply_to(message, format_frame("FAILED", "❌ <i>Không thể trích xuất UID.</i>"))
    except Exception as e:
        bot.reply_to(message, format_frame("ERROR", f"❌ {e}"))

@bot.message_handler(commands=['reggarena'])
def reggarena_cmd(message):
    try:
        res = requests.get("https://keyherlyswar.x10.mx/Apidocs/reglq.php", timeout=30).json()
        if res.get("status") and res.get("result"):
            acc = res["result"][0]
            content = (
                f"✅ <b>Registration Successful</b>\n"
                f"👤 <b>User:</b> <code>{acc['account']}</code>\n"
                f"🔑 <b>Pass:</b> <tg-spoiler>{acc['password']}</tg-spoiler>\n"
                f"⚠️ <i>Click vào vùng đen để xem mật khẩu.</i>"
            )
            bot.reply_to(message, format_frame("GARENA CREATOR", content))
        else:
            bot.reply_to(message, format_frame("OUT OF STOCK", "❌ <i>Hệ thống đang bảo trì hoặc hết acc.</i>"))
    except Exception as e:
        bot.reply_to(message, format_frame("CONNECTION ERROR", f"❌ {e}"))

@bot.message_handler(commands=['tiktok'])
def tiktok_down(message):
    try:
        if len(message.text.split()) < 2:
            bot.reply_to(message, format_frame("INPUT ERROR", "⚠️ <i>Vui lòng nhập link video.</i>"))
            return
            
        res = requests.get(f"http://tienich.x10.mx/tiktok.php?url={message.text.split()[1]}", timeout=20).json()
        d = res.get("data", {})
        
        if not d:
            bot.reply_to(message, format_frame("API ERROR", "❌ <i>Không lấy được dữ liệu media.</i>"))
            return

        content = (
            f"📝 <b>Title:</b> {d.get('title')}\n"
            f"👤 <b>Author:</b> {d.get('author', {}).get('nickname')}\n"
            f"🌍 <b>Region:</b> {d.get('region')}\n"
            f"⏱ <b>Duration:</b> {d.get('duration')}s\n"
            f"📊 <b>Stats:</b> {d.get('play_count')} views | {d.get('digg_count')} likes"
        )
        
        caption = format_frame("TIKTOK DOWNLOADER", content)
        
        if d.get("play"):
            if d.get("images"):
                media = [types.InputMediaPhoto(i) for i in d["images"] if i]
                if media: bot.send_media_group(message.chat.id, media)
            bot.send_video(message.chat.id, d["play"], caption=caption)
        elif d.get("images"):
            media = [types.InputMediaPhoto(i) for i in d["images"] if i]
            bot.send_media_group(message.chat.id, media)
            bot.send_message(message.chat.id, caption)
        else:
            bot.reply_to(message, format_frame("MEDIA ERROR", "❌ <i>Không tìm thấy nội dung tải về.</i>"))
    except Exception as e:
        bot.reply_to(message, format_frame("TRICK ERROR", f"❌ {e}"))

@bot.message_handler(commands=['tt'])
def tiktok_info(message):
    try:
        u = message.text.replace("/tt", "").strip()
        if not u:
            bot.reply_to(message, format_frame("INPUT ERROR", "⚠️ <i>Nhập username TikTok cần soi.</i>"))
            return
            
        res = requests.get(f"https://info-tiktok-user.vercel.app/tiktok?input={urllib.parse.quote(u)}", timeout=15).json()
        if not res.get("success"):
            bot.reply_to(message, format_frame("NOT FOUND", "❌ <i>User không tồn tại hoặc bị ẩn.</i>"))
            return
            
        user = res["data"]["userInfo"]["user"]
        stats = res["data"]["userInfo"]["statsV2"]
        
        content = (
            f"👤 <b>Nick:</b> {user.get('nickname')}\n"
            f"🔗 <b>User:</b> @{user.get('uniqueId')}\n"
            f"🆔 <b>ID:</b> <code>{user.get('id')}</code>\n"
            f"📝 <b>Bio:</b> {user.get('signature')}\n"
            f"✅ <b>Verified:</b> {'YES' if user.get('verified') else 'NO'}\n"
            f"👥 <b>Followers:</b> {stats.get('followerCount')}\n"
            f"❤️ <b>Hearts:</b> {stats.get('heartCount')}\n"
            f"🎥 <b>Videos:</b> {stats.get('videoCount')}"
        )
        bot.send_photo(message.chat.id, user.get("avatarLarger", ""), caption=format_frame("TIKTOK STALKER", content))
    except Exception as e:
        bot.reply_to(message, format_frame("ERROR", f"❌ {e}"))

@bot.message_handler(commands=['anhgaisexy'])
def girl_img(message):
    try:
        res = requests.get("https://api.zeidteam.xyz/images/gaisexy", timeout=10).json()
        if res.get("status"):
            bot.send_photo(message.chat.id, res["data"], caption=format_frame("BEAUTY COLLECTION", "😍 <i>Ảnh chất lượng cao được tuyển chọn.</i>"))
        else:
            bot.reply_to(message, format_frame("API ERROR", "❌ <i>Không tải được ảnh.</i>"))
    except Exception as e:
        bot.reply_to(message, format_frame("ERROR", f"❌ {e}"))

@bot.message_handler(commands=['ask'])
def ask_gpt(message):
    try:
        q = message.text.replace("/ask", "").strip()
        if not q:
            bot.reply_to(message, format_frame("SYNTAX ERROR", "⚠️ <i>Vui lòng nhập nội dung câu hỏi.</i>"))
            return
            
        res = requests.get(f"https://api.zeidteam.xyz/ai/chatgpt4?prompt={urllib.parse.quote(q)}", timeout=60).json()
        ans = res.get("response", "AI Server Busy") if res.get("status") else "No Response"
        
        bot.reply_to(message, format_frame("CHATGPT-4 INTELLIGENCE", ans.replace('<','&lt;').replace('>','&gt;')))
    except Exception as e:
        bot.reply_to(message, format_frame("CONNECTION ERROR", f"❌ {e}"))

@bot.message_handler(commands=['sun', 'taixiu'])
def sun_taixiu(message):
    try:
        res = requests.get("https://sunwinsaygex-8616.onrender.com/api/sun", timeout=10).json()
        d = res.get("data", res)
        
        content = (
            f"🆔 <b>Session:</b> {d.get('phien')}\n"
            f"🕒 <b>Time:</b> {d.get('time')}\n"
            f"🎲 <b>Dice:</b> [{d.get('xuc_xac_1')}] - [{d.get('xuc_xac_2')}] - [{d.get('xuc_xac_3')}]\n"
            f"💿 <b>RESULT:</b> <b>{d.get('result', '').upper()} ({d.get('tong')})</b>\n"
            f"📈 <b>Tai:</b> {int(d.get('total_tai',0)):,} VND\n"
            f"📉 <b>Xiu:</b> {int(d.get('total_xiu',0)):,} VND"
        )
        bot.reply_to(message, format_frame("SUNWIN PREDICTOR", content))
    except Exception as e:
        bot.reply_to(message, format_frame("SERVER ERROR", f"❌ {e}"))

@bot.message_handler(commands=['ip'])
def check_ip_info(message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            bot.reply_to(message, format_frame("INPUT ERROR", "⚠️ <i>Vui lòng nhập địa chỉ IP.</i>"))
            return
        
        res = requests.get(f"https://keyherlyswar.x10.mx/Apidocs/checkip.php?ip={args[1].strip()}", timeout=15).json()
        
        if not res.get("success", False):
            bot.reply_to(message, format_frame("NOT FOUND", "❌ <i>IP không tồn tại trong database.</i>"))
            return
        
        content = (
            f"🌐 <b>IP:</b> {res.get('ip')}\n"
            f"📍 <b>Type:</b> {res.get('type')}\n"
            f"🏳️‍🌈 <b>Country:</b> {res.get('country')} {res.get('country_flag')}\n"
            f"🏙 <b>City:</b> {res.get('city')} ({res.get('region')})\n"
            f"📌 <b>Coords:</b> {res.get('latitude')}, {res.get('longitude')}\n"
            f"🏢 <b>ISP:</b> {res.get('isp')}\n"
            f"🕰 <b>Time:</b> {res.get('timezone')}\n"
            f"💰 <b>Curr:</b> {res.get('currency')}"
        )
        bot.send_message(message.chat.id, format_frame("IP TRACER", content))
    except Exception as e:
        bot.reply_to(message, format_frame("ERROR", f"❌ {e}"))

@bot.message_handler(commands=['zingmp3'])
def zingmp3(message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            bot.reply_to(message, format_frame("INPUT ERROR", "⚠️ <i>Nhập tên bài hát cần tìm.</i>"))
            return
        
        keyword = args[1].strip()
        search_result = search_music(keyword)
        
        if not search_result.get('data') or not search_result['data'].get('items'):
            bot.reply_to(message, format_frame("NOT FOUND", "🚫 <i>Không tìm thấy kết quả phù hợp.</i>"))
            return
        
        songs = search_result['data']['items']
        zingmp3_data[message.chat.id] = songs

        content = f"🔎 <b>Keyword:</b> {keyword}\n📊 <b>Results:</b> {len(songs)} songs found.\n<i>Vui lòng chọn bài hát bên dưới:</i>"
        markup = InlineKeyboardMarkup()
        for i, song in enumerate(songs):
            btn = InlineKeyboardButton(text=f"{i+1}. {song['title']} - {song['artistsNames']}", callback_data=f"song_{i}")
            markup.add(btn)
        bot.reply_to(message, format_frame("ZING MP3 SEARCH", content), reply_markup=markup)
    except Exception as e:
        bot.reply_to(message, format_frame("SEARCH ERROR", f"❌ {e}"))

@bot.callback_query_handler(func=lambda call: call.data.startswith("song_"))
def handle_song_selection(call):
    try:
        index = int(call.data.split("_")[1])
        songs = zingmp3_data.get(call.message.chat.id)
        if not songs or index >= len(songs):
            bot.answer_callback_query(call.id, "❌ Session expired.")
            return

        song = songs[index]
        bot.answer_callback_query(call.id, f"🎶 Fetching: {song['title']}")
        streaming_data = get_streaming_song(song["encodeId"])
        
        if streaming_data.get('err') != 0 or not streaming_data.get('data'):
            bot.send_message(call.message.chat.id, format_frame("PREMIUM CONTENT", "🚫 <i>Bài hát yêu cầu VIP hoặc bị chặn.</i>"))
            return

        audio_url = streaming_data['data'].get('320')
        quality = "320kbps (Lossless)"
        if audio_url == "VIP":
            audio_url = streaming_data['data'].get('128')
            quality = "128kbps (Standard)"

        thumbnail_url = song.get('thumbnailM') or song.get('thumbnail')
        if not audio_url:
            bot.send_message(call.message.chat.id, format_frame("LINK ERROR", "🚫 <i>Source nhạc không khả dụng.</i>"))
            return

        content = (
            f"🎶 <b>Track:</b> {song['title']}\n"
            f"👤 <b>Artist:</b> {song['artistsNames']}\n"
            f"🔊 <b>Quality:</b> {quality}\n"
            f"💿 <b>Source:</b> Zing MP3 Official"
        )

        if thumbnail_url:
            bot.send_photo(call.message.chat.id, thumbnail_url, caption=format_frame("MUSIC PLAYER", content))
        bot.send_audio(call.message.chat.id, audio_url, title=song['title'], performer=song['artistsNames'])
    except Exception as e:
        bot.send_message(call.message.chat.id, format_frame("STREAM ERROR", f"❌ {e}"))

print("╔════════════════════════════════════════╗")
print("║    SUPER PREMIUM BOT IS RUNNING...     ║")
print("╚════════════════════════════════════════╝")
bot.infinity_polling()
# === END FILE: ant.py ===

