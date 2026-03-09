import asyncio
import json
import logging
import os
import re
import sqlite3
import uuid
import warnings
from urllib import error as urlerror
from urllib import request as urlrequest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, types
import importlib.util

IS_AIOGRAM_V3 = bool(importlib.util.find_spec("aiogram.filters"))

if IS_AIOGRAM_V3:
    from aiogram.filters import Command, CommandStart, StateFilter
else:
    from aiogram import executor
    from aiogram.dispatcher.filters import Command
    from aiogram.dispatcher.filters.builtin import CommandStart

    class StateFilter:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
if importlib.util.find_spec("aiogram.fsm"):
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.fsm.storage.memory import MemoryStorage
else:
    from aiogram.dispatcher import FSMContext
    from aiogram.dispatcher.filters.state import State, StatesGroup
    from aiogram.contrib.fsm_storage.memory import MemoryStorage

from aiogram.types import CallbackQuery, InlineKeyboardButton
if importlib.util.find_spec("aiogram.utils.keyboard"):
    from aiogram.utils.keyboard import InlineKeyboardBuilder
else:
    class InlineKeyboardBuilder:
        def __init__(self):
            self._rows = []

        def row(self, *buttons):
            self._rows.append(list(buttons))

        def as_markup(self):
            return types.InlineKeyboardMarkup(inline_keyboard=self._rows)

# =================================================================
# [ КОНФИГУРАЦИЯ ]
# =================================================================
TOKEN = os.getenv("BOT_TOKEN", "8708065186:AAG8Eg9Lm1H8KZD0wHY4y1lXeQpqta7O9oE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "8203556349"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003837981813"))
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/karlosinfo")
PRICE_PER_NUMBER = float(os.getenv("PRICE_PER_NUMBER", "4"))
PRIORITY_PRICE_PER_NUMBER = float(os.getenv("PRIORITY_PRICE_PER_NUMBER", "4"))
HOLD_MINUTES = int(os.getenv("HOLD_MINUTES", "20"))
QUEUE_TTL_HOURS = int(os.getenv("QUEUE_TTL_HOURS", "8"))
PRODUCERS_PAGE_SIZE = int(os.getenv("PRODUCERS_PAGE_SIZE", "10"))
REPORTS_PAGE_SIZE = int(os.getenv("REPORTS_PAGE_SIZE", "12"))
DB_NAME = os.getenv("DB_NAME", "titan_v40_final.db")
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / DB_NAME
MENU_PHOTO = os.getenv("MENU_PHOTO", "https://imgur.gg/f/tyvhiWD")
KZ_TZ = ZoneInfo("Europe/Moscow")
REPORT_TIME_SHIFT_HOURS = int(os.getenv("REPORT_TIME_SHIFT_HOURS", "5"))
PAY_DEFAULT_AMOUNT = float(os.getenv("PAY_DEFAULT_AMOUNT", "5"))
PAY_CHECK_ATTEMPTS = int(os.getenv("PAY_CHECK_ATTEMPTS", "10"))
PAY_CHECK_INTERVAL_SECONDS = int(os.getenv("PAY_CHECK_INTERVAL_SECONDS", "10"))
CRYPTO_PAY_API_TOKEN = os.getenv("CRYPTO_PAY_API_TOKEN", "544000:AAgZSOFJD2RNiTGt83utF1cN8FMOygen1hR").strip()
CRYPTO_PAY_TRANSFER_TOKEN = os.getenv("CRYPTO_PAY_TRANSFER_TOKEN", "544000:AAgZSOFJD2RNiTGt83utF1cN8FMOygen1hR").strip()
CRYPTO_PAY_INVOICE_TOKEN = os.getenv("CRYPTO_PAY_INVOICE_TOKEN", "544000:AAgZSOFJD2RNiTGt83utF1cN8FMOygen1hR").strip()
CRYPTO_PAY_API_BASE = "https://pay.crypt.bot/api/"


def _get_token_from_db(key: str) -> str:
    row = db.query(f"SELECT value FROM settings WHERE key='{key}'", fetch="one") if 'db' in globals() else None
    return (row[0] if row and row[0] else "").strip()


def get_crypto_pay_token() -> str:
    return get_crypto_transfer_token()


def get_crypto_transfer_token() -> str:
    # Приоритет: dedicated ENV -> dedicated settings -> common ENV -> common settings.
    # Это позволяет переопределять неактуальный общий ENV токен через /set_crypto_transfer_token без рестарта.
    dedicated_env = (os.getenv("CRYPTO_PAY_TRANSFER_TOKEN", "") or "").strip()
    if dedicated_env:
        return dedicated_env
    dedicated_db = _get_token_from_db("crypto_pay_transfer_token")
    if dedicated_db:
        return dedicated_db
    common_env = (os.getenv("CRYPTO_PAY_API_TOKEN", "") or "").strip()
    if common_env:
        return common_env
    return _get_token_from_db("crypto_pay_api_token")


def get_crypto_invoice_token() -> str:
    # Приоритет: dedicated ENV -> dedicated settings -> common ENV -> common settings.
    # Это позволяет переопределять неактуальный общий ENV токен через /set_crypto_invoice_token без рестарта.
    dedicated_env = (os.getenv("CRYPTO_PAY_INVOICE_TOKEN", "") or "").strip()
    if dedicated_env:
        return dedicated_env
    dedicated_db = _get_token_from_db("crypto_pay_invoice_token")
    if dedicated_db:
        return dedicated_db
    common_env = (os.getenv("CRYPTO_PAY_API_TOKEN", "") or "").strip()
    if common_env:
        return common_env
    return _get_token_from_db("crypto_pay_api_token")


def get_crypto_pay_token_source() -> str:
    return get_crypto_transfer_token_source()


def get_crypto_transfer_token_source() -> str:
    if (os.getenv("CRYPTO_PAY_TRANSFER_TOKEN", "") or "").strip():
        return "env:CRYPTO_PAY_TRANSFER_TOKEN"
    if _get_token_from_db("crypto_pay_transfer_token"):
        return "settings:crypto_pay_transfer_token"
    if (os.getenv("CRYPTO_PAY_API_TOKEN", "") or "").strip():
        return "env:CRYPTO_PAY_API_TOKEN"
    if _get_token_from_db("crypto_pay_api_token"):
        return "settings:crypto_pay_api_token"
    return "none"


def get_crypto_invoice_token_source() -> str:
    if (os.getenv("CRYPTO_PAY_INVOICE_TOKEN", "") or "").strip():
        return "env:CRYPTO_PAY_INVOICE_TOKEN"
    if _get_token_from_db("crypto_pay_invoice_token"):
        return "settings:crypto_pay_invoice_token"
    if (os.getenv("CRYPTO_PAY_API_TOKEN", "") or "").strip():
        return "env:CRYPTO_PAY_API_TOKEN"
    if _get_token_from_db("crypto_pay_api_token"):
        return "settings:crypto_pay_api_token"
    return "none"


def set_crypto_pay_token(token: str):
    db.query("INSERT OR REPLACE INTO settings(key,value) VALUES('crypto_pay_api_token',?)", (token.strip(),))


def set_crypto_transfer_token(token: str):
    db.query("INSERT OR REPLACE INTO settings(key,value) VALUES('crypto_pay_transfer_token',?)", (token.strip(),))


def set_crypto_invoice_token(token: str):
    db.query("INSERT OR REPLACE INTO settings(key,value) VALUES('crypto_pay_invoice_token',?)", (token.strip(),))


def clear_crypto_pay_token():
    db.query("DELETE FROM settings WHERE key='crypto_pay_api_token'")


def clear_crypto_transfer_token():
    db.query("DELETE FROM settings WHERE key='crypto_pay_transfer_token'")


def clear_crypto_invoice_token():
    db.query("DELETE FROM settings WHERE key='crypto_pay_invoice_token'")

def parse_crypto_error_payload(raw: str) -> tuple[str | None, str | None]:
    """Returns (error_text, error_code_as_str)."""
    if not raw:
        return None, None
    try:
        payload = json.loads(raw)
    except Exception:
        return raw[:250], None

    if isinstance(payload, dict):
        err = payload.get("error")
        code = payload.get("error_code")
        if isinstance(err, dict):
            code = err.get("code", code)
            err = err.get("name") or err.get("message") or err
        return (str(err) if err is not None else json.dumps(payload, ensure_ascii=False), str(code) if code is not None else None)
    return json.dumps(payload, ensure_ascii=False), None


def explain_crypto_403(endpoint: str, err_code: str | None, err_text: str | None) -> str:
    endpoint_l = (endpoint or "").lower()
    code = (err_code or "").strip()
    text_l = (err_text or "").lower()

    if "transfer" in endpoint_l or "send" in endpoint_l:
        if code == "1010" or "1010" in text_l:
            return "HTTP 403 / 1010: получатель не активировал переводы в @CryptoBot (нужно нажать Start) или токен выплат невалидный. Проверьте /crypto_diag и токен выплат."
        return "HTTP 403: нет доступа к transfer/send. Проверьте токен выплат (Crypto Pay App token)."

    if "createinvoice" in endpoint_l or "getinvoice" in endpoint_l or "getbalance" in endpoint_l or "getme" in endpoint_l:
        if code == "1010" or "1010" in text_l:
            return "HTTP 403 / 1010: токен не принят API (часто неверный/не тот тип токена). Нужен Crypto Pay App token из @CryptoBot."
        return "HTTP 403: токен не имеет прав для invoice/balance API. Укажите корректный invoice token через /set_crypto_invoice_token или CRYPTO_PAY_INVOICE_TOKEN."

    if code == "1010" or "1010" in text_l:
        return "HTTP 403 / 1010: токен отклонён Crypto Pay API."
    return "HTTP 403: доступ запрещён для этого эндпоинта Crypto Pay API."


warnings.filterwarnings(
    "ignore",
    message=r"You may be able to resolve this warning by setting `model_config\['protected_namespaces'\] = \(\)`.*",
)

logging.basicConfig(level=logging.INFO)
bot = Bot(token=TOKEN)
if IS_AIOGRAM_V3:
    dp = Dispatcher(storage=MemoryStorage())
else:
    dp = Dispatcher(bot, storage=MemoryStorage())

if not IS_AIOGRAM_V3:
    _orig_message_handler = dp.message_handler

    def _message_compat(*filters, **kwargs):
        normalized = []
        for flt in filters:
            # aiogram v3 style: @dp.message(Form.state)
            # aiogram v2 expects: @dp.message_handler(state=Form.state)
            if flt.__class__.__name__ == "State":
                kwargs.setdefault("state", flt)
            else:
                normalized.append(flt)
        return _orig_message_handler(*normalized, **kwargs)

    dp.message = _message_compat
    dp.callback_query = dp.callback_query_handler


class Form(StatesGroup):
    submit_type = State()
    num = State()


class AdminForm(StatesGroup):
    priority_add = State()
    priority_remove = State()
    message_user = State()
    breaks_group = State()
    broadcast = State()
    admin_add = State()
    admin_remove = State()
    set_price = State()
    queue_remove_user = State()
    direct_message = State()
    payout_amount = State()
    app_topup_amount = State()


class SupportForm(StatesGroup):
    request = State()


class CodeForm(StatesGroup):
    waiting_code = State()


class Database:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.create_tables()
        self.ensure_columns()
        self.ensure_settings()

    def create_tables(self):
        with self.conn:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, banned INTEGER DEFAULT 0, priority INTEGER DEFAULT 0, is_admin INTEGER DEFAULT 0)"
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, number TEXT, submit_type TEXT DEFAULT 'code', status TEXT DEFAULT 'waiting', created_at TIMESTAMP, proc_by INTEGER, code_sender_id INTEGER, repeat_requested INTEGER DEFAULT 0, qr_requested INTEGER DEFAULT 0)"
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS sessions (id INTEGER PRIMARY KEY AUTOINCREMENT, number TEXT, user_id INTEGER, start_time TIMESTAMP, end_time TIMESTAMP, status TEXT, paid INTEGER DEFAULT 0, group_id INTEGER, price REAL, proc_by INTEGER, code_sender_id INTEGER, submit_type TEXT DEFAULT 'code')"
            )
            self.conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS submissions (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, number TEXT, submit_type TEXT DEFAULT 'code', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS referrals (inviter_id INTEGER, invited_id INTEGER UNIQUE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS breaks (id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, start_time TIMESTAMP, end_time TIMESTAMP)"
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS payouts (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, number TEXT, submit_type TEXT DEFAULT 'code', amount REAL, payload TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )

    def query(self, sql, params=(), fetch="all"):
        with self.conn:
            cur = self.conn.execute(sql, params)
            if fetch == "one":
                return cur.fetchone()
            return cur.fetchall()

    def ensure_columns(self):
        sess = {r[1] for r in self.query("PRAGMA table_info(sessions)")}
        if "submit_type" not in sess:
            self.query("ALTER TABLE sessions ADD COLUMN submit_type TEXT DEFAULT 'code'")
        if "credited_notified" not in sess:
            self.query("ALTER TABLE sessions ADD COLUMN credited_notified INTEGER DEFAULT 0")

        queue = {r[1] for r in self.query("PRAGMA table_info(queue)")}
        if "submit_type" not in queue:
            self.query("ALTER TABLE queue ADD COLUMN submit_type TEXT DEFAULT 'code'")

        payouts = {r[1] for r in self.query("PRAGMA table_info(payouts)")}
        if "submit_type" not in payouts:
            self.query("ALTER TABLE payouts ADD COLUMN submit_type TEXT DEFAULT 'code'")

    def ensure_settings(self):
        self.query("INSERT OR IGNORE INTO settings(key, value) VALUES('price_per_number', ?)", (str(PRICE_PER_NUMBER),))
        self.query("INSERT OR IGNORE INTO settings(key, value) VALUES('work_enabled', '1')")
        self.query("INSERT OR IGNORE INTO settings(key, value) VALUES('instant_payout_enabled', '1')")
        self.query("INSERT OR IGNORE INTO settings(key, value) VALUES('app_balance_usdt', '0')")
        if CRYPTO_PAY_API_TOKEN:
            self.query("INSERT OR IGNORE INTO settings(key, value) VALUES('crypto_pay_api_token', ?)", (CRYPTO_PAY_API_TOKEN,))
        if CRYPTO_PAY_TRANSFER_TOKEN:
            self.query("INSERT OR IGNORE INTO settings(key, value) VALUES('crypto_pay_transfer_token', ?)", (CRYPTO_PAY_TRANSFER_TOKEN,))
        if CRYPTO_PAY_INVOICE_TOKEN:
            self.query("INSERT OR IGNORE INTO settings(key, value) VALUES('crypto_pay_invoice_token', ?)", (CRYPTO_PAY_INVOICE_TOKEN,))

    def get_price(self, user_id: int | None = None) -> float:
        row = self.query("SELECT value FROM settings WHERE key='price_per_number'", fetch="one")
        try:
            base = float(row[0]) if row else PRICE_PER_NUMBER
        except Exception:
            base = PRICE_PER_NUMBER
        if user_id and is_user_priority(user_id):
            return PRIORITY_PRICE_PER_NUMBER
        return base


async def crypto_bot_send_usdt(user_id: int, amount: float) -> tuple[bool, str, dict | None]:
    """Instant USDT payout via Crypto Bot API.

    Primary path: /transfer.
    Fallback path on transfer failures: /createCheck + DM with redeem link.
    """
    if amount <= 0:
        return False, "Сумма должна быть больше 0.", None
    token = get_crypto_transfer_token()
    if not token:
        return False, "Не задан токен для выплат (CRYPTO_PAY_TRANSFER_TOKEN/CRYPTO_PAY_API_TOKEN).", None

    spend_id = f"pay-{user_id}-{uuid.uuid4().hex[:16]}"
    transfer_payload = {
        "user_id": int(user_id),
        "asset": "USDT",
        "amount": f"{float(amount):.2f}",
        "spend_id": spend_id,
        "comment": "Instant payout",
    }

    def _post(endpoint: str, payload: dict):
        req = urlrequest.Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Crypto-Pay-API-Token": token,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urlrequest.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    last_error = None
    data = None

    # 1) transfer
    try:
        data = await asyncio.to_thread(_post, f"{CRYPTO_PAY_API_BASE}transfer", transfer_payload)
        if data.get("ok"):
            result = data.get("result") or {}
            return True, "Выплата успешно отправлена.", result
        err = data.get("error")
        err_text = err if isinstance(err, str) else json.dumps(err, ensure_ascii=False)
        if "INSUFFICIENT" in err_text.upper():
            return False, "Недостаточно средств на балансе Crypto Bot.", data
        last_error = f"Crypto Bot error: {err_text}"
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
        err_text, err_code = parse_crypto_error_payload(detail)
        last_error = explain_crypto_403("transfer", err_code, err_text or detail) if exc.code == 403 else f"HTTP {exc.code}: {(err_text or detail)[:200]}"
    except Exception as exc:
        last_error = f"Ошибка запроса: {exc}"

    # 2) fallback: createCheck
    check_payload = {
        "asset": "USDT",
        "amount": f"{float(amount):.2f}",
        "description": "Payout fallback check",
    }
    try:
        check_resp = await asyncio.to_thread(_post, f"{CRYPTO_PAY_API_BASE}createCheck", check_payload)
        if not check_resp.get("ok"):
            err = check_resp.get("error")
            err_text = err if isinstance(err, str) else json.dumps(err, ensure_ascii=False)
            return False, f"{last_error} | createCheck error: {err_text}", check_resp

        result = check_resp.get("result") or {}
        check_url = result.get("url") or result.get("pay_url") or result.get("bot_check_url") or result.get("bot_invoice_url")
        if not check_url:
            return False, f"{last_error} | createCheck: ссылка не получена", check_resp

        try:
            await bot.send_message(
                int(user_id),
                (
                    f"💳 Выплата {float(amount):.2f} USDT оформлена чеком.\n"
                    f"Заберите чек: {check_url}"
                ),
            )
        except Exception as send_exc:
            return False, f"{last_error} | чек создан, но не удалось отправить в ЛС: {send_exc}", {"check_url": check_url, **(result or {})}

        return True, "Выплата оформлена чеком и отправлена в ЛС.", {"mode": "check", "check_url": check_url, **(result or {})}
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
        err_text, err_code = parse_crypto_error_payload(detail)
        check_err = explain_crypto_403("createCheck", err_code, err_text or detail) if exc.code == 403 else f"HTTP {exc.code}: {(err_text or detail)[:200]}"
        return False, f"{last_error} | createCheck failed: {check_err}", None
    except Exception as exc:
        return False, f"{last_error} | createCheck exception: {exc}", None


async def crypto_bot_create_check(user_id: int, amount: float, description: str = "Manual payout check") -> tuple[bool, str, dict | None]:
    """Creates Crypto Bot check and sends it to target user in DM."""
    if amount <= 0:
        return False, "Сумма должна быть больше 0.", None
    token = get_crypto_transfer_token()
    if not token:
        return False, "Не задан токен для выплат (CRYPTO_PAY_TRANSFER_TOKEN/CRYPTO_PAY_API_TOKEN).", None

    payload = {
        "asset": "USDT",
        "amount": f"{float(amount):.2f}",
        "description": description,
    }

    def _post():
        req = urlrequest.Request(
            f"{CRYPTO_PAY_API_BASE}createCheck",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Crypto-Pay-API-Token": token,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urlrequest.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    try:
        data = await asyncio.to_thread(_post)
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
        err_text, err_code = parse_crypto_error_payload(detail)
        if exc.code == 403:
            return False, explain_crypto_403("createCheck", err_code, err_text or detail), None
        return False, f"HTTP {exc.code}: {(err_text or detail)[:200]}", None
    except Exception as exc:
        return False, f"Ошибка createCheck: {exc}", None

    if not data.get("ok"):
        err = data.get("error")
        err_text = err if isinstance(err, str) else json.dumps(err, ensure_ascii=False)
        return False, f"Crypto Bot error: {err_text}", data

    result = data.get("result") or {}
    check_url = result.get("url") or result.get("pay_url") or result.get("bot_check_url") or result.get("bot_invoice_url")
    if not check_url:
        return False, "createCheck: ссылка не получена", data

    try:
        await bot.send_message(int(user_id), f"💳 Вам отправлен чек на {float(amount):.2f} USDT.\nЗаберите чек: {check_url}")
    except Exception as exc:
        return False, f"Чек создан, но не отправлен в ЛС: {exc}", {"check_url": check_url, **(result or {})}

    return True, "Чек создан и отправлен в ЛС пользователю.", {"mode": "check_only", "check_url": check_url, **(result or {})}


def is_non_retryable_payout_error(msg: str) -> bool:
    text = (msg or "").lower()
    return (
        "1010" in text
        or "не подключил @cryptobot" in text
        or "нажм" in text and "start" in text
        or "включит прием переводов" in text
    )


async def crypto_bot_get_usdt_balance() -> tuple[bool, float, str]:
    """Returns (ok, usdt_available, message)."""
    token = get_crypto_invoice_token()
    if not token:
        return False, 0.0, "Не задан токен для инвойсов (CRYPTO_PAY_INVOICE_TOKEN/CRYPTO_PAY_API_TOKEN)."

    def _get_balance():
        req = urlrequest.Request(
            f"{CRYPTO_PAY_API_BASE}getBalance",
            headers={"Crypto-Pay-API-Token": token},
            method="GET",
        )
        with urlrequest.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    try:
        data = await asyncio.to_thread(_get_balance)
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
        err_text, err_code = parse_crypto_error_payload(detail)
        if exc.code == 403:
            return False, 0.0, explain_crypto_403("getBalance", err_code, err_text or detail)
        return False, 0.0, f"HTTP {exc.code}: {(err_text or detail)[:200]}"
    except Exception as exc:
        return False, 0.0, f"Ошибка получения баланса: {exc}"

    if not data.get("ok"):
        err = data.get("error")
        err_text = err if isinstance(err, str) else json.dumps(err, ensure_ascii=False)
        return False, 0.0, f"Crypto Bot error: {err_text}"

    result = data.get("result") or []
    usdt = 0.0
    for item in result:
        if str(item.get("currency_code", "")).upper() == "USDT":
            try:
                usdt = float(item.get("available") or item.get("balance") or 0)
            except Exception:
                usdt = 0.0
            break
    return True, usdt, "OK"


async def crypto_bot_create_topup_invoice(amount: float) -> tuple[bool, str, str, int | None]:
    """Returns (ok, pay_url, message, invoice_id)."""
    if amount <= 0:
        return False, "", "Сумма должна быть больше 0.", None
    token = get_crypto_invoice_token()
    if not token:
        return False, "", "Не задан токен для инвойсов (CRYPTO_PAY_INVOICE_TOKEN/CRYPTO_PAY_API_TOKEN).", None

    payload = {
        "asset": "USDT",
        "amount": f"{float(amount):.2f}",
        "description": "Top up app balance",
    }

    def _create():
        req = urlrequest.Request(
            f"{CRYPTO_PAY_API_BASE}createInvoice",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Crypto-Pay-API-Token": token,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urlrequest.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    try:
        data = await asyncio.to_thread(_create)
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
        err_text, err_code = parse_crypto_error_payload(detail)
        if exc.code == 403:
            return False, "", explain_crypto_403("createInvoice", err_code, err_text or detail), None
        pretty = err_text or detail[:200]
        return False, "", f"HTTP {exc.code}: {pretty[:200]}", None
    except Exception as exc:
        return False, "", f"Ошибка создания инвойса: {exc}", None

    if not data.get("ok"):
        err = data.get("error")
        err_text = err if isinstance(err, str) else json.dumps(err, ensure_ascii=False)
        return False, "", f"Crypto Bot error: {err_text}", None

    result = data.get("result") or {}
    pay_url = result.get("pay_url") or result.get("bot_invoice_url") or ""
    if not pay_url:
        return False, "", "Инвойс создан, но ссылка не получена.", None
    invoice_id = result.get("invoice_id") or result.get("id")
    try:
        invoice_id = int(invoice_id) if invoice_id is not None else None
    except Exception:
        invoice_id = None
    return True, pay_url, "OK", invoice_id


async def crypto_bot_check_invoice(invoice_id: int) -> tuple[bool, str, str]:
    """Returns (ok, status, message)."""
    token = get_crypto_invoice_token()
    if not token:
        return False, "", "Не задан токен для инвойсов (CRYPTO_PAY_INVOICE_TOKEN/CRYPTO_PAY_API_TOKEN)."

    def _check():
        req = urlrequest.Request(
            f"{CRYPTO_PAY_API_BASE}getInvoice?invoice_id={int(invoice_id)}",
            headers={"Crypto-Pay-API-Token": token},
            method="GET",
        )
        with urlrequest.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    try:
        data = await asyncio.to_thread(_check)
    except Exception as exc:
        return False, "", f"Ошибка проверки инвойса: {exc}"

    if not data.get("ok"):
        err = data.get("error")
        err_text = err if isinstance(err, str) else json.dumps(err, ensure_ascii=False)
        return False, "", f"Crypto Bot error: {err_text}"

    result = data.get("result") or {}
    if isinstance(result, list):
        result = result[0] if result else {}
    status = str((result or {}).get("status") or "unknown")
    return True, status, "OK"


async def crypto_bot_check_auth() -> tuple[bool, str]:
    """Fast auth check for Crypto Pay token."""
    token = get_crypto_transfer_token()
    if not token:
        return False, "Токен выплат не задан (CRYPTO_PAY_TRANSFER_TOKEN/CRYPTO_PAY_API_TOKEN)."

    def _check():
        req = urlrequest.Request(
            f"{CRYPTO_PAY_API_BASE}getMe",
            headers={"Crypto-Pay-API-Token": token},
            method="GET",
        )
        with urlrequest.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))

    try:
        data = await asyncio.to_thread(_check)
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
        if exc.code == 403:
            return False, "HTTP 403: токен невалидный или это не токен Crypto Pay App. Нужен App token из @CryptoBot (Crypto Pay)."
        return False, f"HTTP {exc.code}: {detail[:200]}"
    except Exception as exc:
        return False, f"Ошибка проверки токена: {exc}"

    if not data.get("ok"):
        err = data.get("error")
        err_text = err if isinstance(err, str) else json.dumps(err, ensure_ascii=False)
        return False, f"Crypto Bot error: {err_text}"

    app = data.get("result") or {}
    app_name = app.get("name") or app.get("app_name") or "Unknown"
    return True, f"OK ({app_name})"


async def crypto_bot_diagnostics_report() -> str:
    """Detailed diagnostics for Crypto Pay integration."""
    transfer_token = get_crypto_transfer_token()
    invoice_token = get_crypto_invoice_token()
    if not transfer_token and not invoice_token:
        return "❌ Не задан ни один токен Crypto Pay. Укажите CRYPTO_PAY_API_TOKEN или отдельные transfer/invoice токены."

    transfer_source = get_crypto_transfer_token_source()
    invoice_source = get_crypto_invoice_token_source()
    transfer_hint = f"{transfer_token[:6]}...{transfer_token[-4:]}" if transfer_token and len(transfer_token) >= 12 else "(нет)"
    invoice_hint = f"{invoice_token[:6]}...{invoice_token[-4:]}" if invoice_token and len(invoice_token) >= 12 else "(нет)"

    def _call(token: str, method: str, endpoint: str, body: dict | None = None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        headers = {"Crypto-Pay-API-Token": token}
        if body is not None:
            headers["Content-Type"] = "application/json"
        req = urlrequest.Request(endpoint, data=data, headers=headers, method=method)
        with urlrequest.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    lines = [
        "<b>🧪 Диагностика Crypto Pay</b>",
        f"Токен выплат: <code>{transfer_hint}</code> (source: {transfer_source})",
        f"Токен инвойсов: <code>{invoice_hint}</code> (source: {invoice_source})",
        "",
    ]
    has_1010_getme = False
    has_1010_balance = False
    has_1010_invoice = False

    # 1) getMe
    if not transfer_token:
        lines.append("❌ getMe: токен выплат не задан")
    else:
        try:
            data = await asyncio.to_thread(_call, transfer_token, "GET", f"{CRYPTO_PAY_API_BASE}getMe", None)
            if data.get("ok"):
                app = data.get("result") or {}
                name = app.get("name") or app.get("app_name") or "Unknown"
                lines.append(f"✅ getMe: OK ({name})")
            else:
                err = data.get("error")
                lines.append(f"❌ getMe: {err}")
        except urlerror.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
            err_text, err_code = parse_crypto_error_payload(detail)
            if exc.code == 403:
                lines.append(f"❌ getMe: {explain_crypto_403('getMe', err_code, err_text or detail)}")
            else:
                lines.append(f"❌ getMe HTTP {exc.code}: {(err_text or detail)[:180]}")
            has_1010_getme = bool((err_code == "1010") or ("1010" in (err_text or detail)))
        except Exception as exc:
            lines.append(f"❌ getMe exception: {exc}")

    # 2) getBalance
    try:
        data = await asyncio.to_thread(_call, invoice_token or transfer_token, "GET", f"{CRYPTO_PAY_API_BASE}getBalance", None)
        if data.get("ok"):
            result = data.get("result") or []
            usdt = 0.0
            for item in result:
                if str(item.get("currency_code", "")).upper() == "USDT":
                    usdt = float(item.get("available") or item.get("balance") or 0)
                    break
            lines.append(f"✅ getBalance: OK (USDT {usdt:.2f})")
        else:
            err = data.get("error")
            lines.append(f"❌ getBalance: {err}")
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
        err_text, err_code = parse_crypto_error_payload(detail)
        if exc.code == 403:
            lines.append(f"❌ getBalance: {explain_crypto_403('getBalance', err_code, err_text or detail)}")
        else:
            lines.append(f"❌ getBalance HTTP {exc.code}: {(err_text or detail)[:180]}")
        has_1010_balance = bool((err_code == "1010") or ("1010" in (err_text or detail)))
    except Exception as exc:
        lines.append(f"❌ getBalance exception: {exc}")

    # 3) createInvoice (small test)
    if not invoice_token:
        lines.append("❌ createInvoice: токен инвойсов не задан")
    else:
        try:
            payload = {"asset": "USDT", "amount": "0.10", "description": "diag"}
            data = await asyncio.to_thread(_call, invoice_token, "POST", f"{CRYPTO_PAY_API_BASE}createInvoice", payload)
            if data.get("ok"):
                lines.append("✅ createInvoice: OK")
            else:
                err = data.get("error")
                lines.append(f"❌ createInvoice: {err}")
        except urlerror.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
            err_text, err_code = parse_crypto_error_payload(detail)
            if exc.code == 403:
                lines.append(f"❌ createInvoice: {explain_crypto_403('createInvoice', err_code, err_text or detail)}")
            else:
                lines.append(f"❌ createInvoice HTTP {exc.code}: {(err_text or detail)[:180]}")
            has_1010_invoice = bool((err_code == "1010") or ("1010" in (err_text or detail)))
        except Exception as exc:
            lines.append(f"❌ createInvoice exception: {exc}")

    lines.append("")
    if has_1010_getme and has_1010_balance and has_1010_invoice:
        lines.append("🔎 Итог: 1010 на getMe/getBalance/createInvoice одновременно — проблема почти наверняка в токене (невалидный/не Crypto Pay App token), а не в том, что пользователь не нажал Start.")
        lines.append("👉 Решение: создайте новый App в @CryptoBot и установите токен через /set_crypto_transfer_token и /set_crypto_invoice_token.")
        lines.append("")
    lines.append("ℹ️ Если только createInvoice падает с 403 — у токена нет прав invoice.")
    lines.append("ℹ️ Если transfer/send падает с 403/1010 — получатель не нажал Start в @CryptoBot.")
    return "\n".join(lines)


def now_kz_naive() -> datetime:
    return datetime.now(KZ_TZ).replace(tzinfo=None)


def parse_dt(value):
    if isinstance(value, datetime):
        return value
    if value is None:
        return now_kz_naive()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return now_kz_naive()


def get_users_count(db_path: Path) -> int:
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            table = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'").fetchone()
            if not table:
                return 0
            row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()
    except Exception:
        return 0


def resolve_db_path() -> Path:
    """Resolve a stable DB path so restarts do not create a new empty DB."""
    env_path = os.getenv("TITAN_DB_PATH")
    if env_path:
        forced = Path(env_path).expanduser().resolve()
        forced.parent.mkdir(parents=True, exist_ok=True)
        return forced

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return DB_PATH


RESOLVED_DB_PATH = resolve_db_path()
db = Database(str(RESOLVED_DB_PATH))


def parse_numbers(text: str):
    raw_numbers = re.findall(r"\+?\d{10,12}", text or "")
    normalized = []
    for raw in raw_numbers:
        digits = re.sub(r"\D", "", raw)
        if len(digits) == 10:
            digits = "7" + digits
        elif len(digits) == 11 and digits.startswith("8"):
            digits = "7" + digits[1:]
        if len(digits) == 11 and digits.startswith("7"):
            normalized.append(f"+{digits}")
    return normalized


def unique_numbers(numbers: list[str]) -> list[str]:
    seen = set()
    unique = []
    for num in numbers:
        if num not in seen:
            seen.add(num)
            unique.append(num)
    return unique


async def check_sub(user_id: int):
    if is_user_admin(user_id):
        return True

    allowed = {"member", "administrator", "creator"}
    explicit_not_sub = False

    targets = [CHANNEL_ID]
    # Поддержка проверки по username канала (на случай неверного CHANNEL_ID/миграции).
    m = re.search(r"t\.me/([A-Za-z0-9_]+)", CHANNEL_URL or "")
    if m:
        targets.append(f"@{m.group(1)}")

    had_api_error = False
    for target in targets:
        try:
            member = await bot.get_chat_member(chat_id=target, user_id=user_id)
            if member.status in allowed:
                return True
            if member.status in {"left", "kicked"}:
                explicit_not_sub = True
        except Exception:
            had_api_error = True
            continue

    if explicit_not_sub:
        return False

    # Fail-open: если API/права бота не позволяют проверить подписку,
    # не блокируем пользователя в бесконечном "подпишитесь".
    if had_api_error:
        return True

    return False


def is_user_admin(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    row = db.query("SELECT is_admin FROM users WHERE user_id=?", (user_id,), fetch="one")
    return bool(row and row[0])


def is_user_banned(user_id: int) -> bool:
    row = db.query("SELECT banned FROM users WHERE user_id=?", (user_id,), fetch="one")
    return bool(row and row[0])


def is_user_priority(user_id: int) -> bool:
    row = db.query("SELECT priority FROM users WHERE user_id=?", (user_id,), fetch="one")
    return bool(row and row[0])


def get_user_price(user_id: int) -> float:
    return db.get_price(user_id)


def get_menu_caption(is_subscribed: bool) -> str:
    if not is_subscribed:
        return "⚠️ Подпишись на канал!"
    cleanup_queue_expired()
    waiting_cnt = db.query("SELECT COUNT(*) FROM queue WHERE status='waiting' AND number IS NOT NULL AND number<>''", fetch="one")[0]
    status_text = "🟢 Работаем" if is_work_enabled() else "🔴 Не работаем"
    return (
        f"<b>Статус:</b> {status_text}\n\n"
        f"<b>Очередь:</b> {waiting_cnt}\n\n"
        "<b>🏠 Главное меню:</b>"
    )


def get_group_binding_key(chat_id: int, thread_id: int | None) -> str:
    return f"gid:{chat_id}:{thread_id or 0}"


def get_topic_mode_key(chat_id: int, thread_id: int | None) -> str:
    return f"mode:{chat_id}:{thread_id or 0}"


def get_topic_mode(chat_id: int, thread_id: int | None) -> str:
    row = db.query("SELECT value FROM settings WHERE key=?", (get_topic_mode_key(chat_id, thread_id),), fetch="one")
    mode = (row[0] if row and row[0] else "MIX").upper()
    return mode if mode in {"SMS", "QR", "MIX"} else "MIX"


def set_topic_mode(chat_id: int, thread_id: int | None, mode: str):
    db.query("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (get_topic_mode_key(chat_id, thread_id), mode.upper()))


def get_thread_id(message_obj) -> int:
    return int(getattr(message_obj, "message_thread_id", 0) or 0)


def get_linked_groups():
    rows = db.query("SELECT key, value FROM settings WHERE key LIKE 'gid:%'")
    groups = []
    for key, value in rows:
        parts = key.split(":")
        if len(parts) < 3:
            continue
        try:
            gid = int(parts[1])
            thread_id = int(parts[2])
        except ValueError:
            continue
        groups.append((gid, thread_id, value or ""))
    groups.sort(key=lambda item: (item[0], item[1]))
    return groups


def is_group_linked(chat_id: int, thread_id: int | None) -> bool:
    tid = int(thread_id or 0)
    return bool(db.query("SELECT 1 FROM settings WHERE key=?", (get_group_binding_key(chat_id, tid),), fetch="one"))


def group_scope_allows(chat_id: int, thread_id: int | None) -> bool:
    # Ограничиваем только если привязки есть именно для текущего чата.
    local_groups = db.query("SELECT key FROM settings WHERE key LIKE ?", (f"gid:{chat_id}:%",))
    if not local_groups:
        return True
    return is_group_linked(chat_id, thread_id)


def set_support_target(chat_id: int, thread_id: int | None, title: str = ""):
    tid = int(thread_id or 0)
    value = f"{chat_id}:{tid}:{title or ''}"
    db.query("INSERT OR REPLACE INTO settings(key, value) VALUES('support_target', ?)", (value,))


def get_support_target() -> tuple[int, int] | None:
    row = db.query("SELECT value FROM settings WHERE key='support_target'", fetch="one")
    if not row or not row[0]:
        return None
    parts = str(row[0]).split(":", 2)
    if len(parts) < 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return None


def set_payout_target(chat_id: int, thread_id: int | None):
    tid = int(thread_id or 0)
    db.query("INSERT OR REPLACE INTO settings(key, value) VALUES('payout_target', ?)", (f"{chat_id}:{tid}",))


def get_payout_target() -> tuple[int, int] | None:
    row = db.query("SELECT value FROM settings WHERE key='payout_target'", fetch="one")
    if not row or not row[0]:
        return None
    parts = str(row[0]).split(":", 1)
    if len(parts) < 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return None


def mask_username(username: str | None, user_id: int) -> str:
    base = (username or f"id{user_id}").strip().lstrip("@")
    if len(base) <= 2:
        return base[0] + "*" if base else f"id{user_id}"
    keep = max(1, len(base) // 2)
    return base[:keep] + ("*" * (len(base) - keep))


async def notify_payout_group(user_id: int, number: str, amount: float, status: str = "выплачено"):
    target = get_payout_target()
    if not target:
        return
    row = db.query("SELECT username FROM users WHERE user_id=?", (user_id,), fetch="one")
    masked = mask_username(row[0] if row else None, user_id)
    text = (
        "<b>💸 Выплата</b>\n"
        f"Пользователь: <code>{masked}</code>\n"
        f"ID: <code>{user_id}</code>\n"
        f"Номер: <code>{number}</code>\n"
        f"Сумма: <b>{float(amount):.2f}$</b>\n"
        f"Статус: <b>{status}</b>"
    )
    chat_id, thread_id = target
    try:
        if thread_id:
            await bot.send_message(chat_id, text, parse_mode="HTML", message_thread_id=thread_id)
        else:
            await bot.send_message(chat_id, text, parse_mode="HTML")
    except Exception:
        pass


def is_work_enabled() -> bool:
    row = db.query("SELECT value FROM settings WHERE key='work_enabled'", fetch="one")
    return (row[0] if row else "1") == "1"


def set_work_enabled(enabled: bool):
    db.query("INSERT OR REPLACE INTO settings(key,value) VALUES('work_enabled',?)", ("1" if enabled else "0",))


def is_instant_payout_enabled() -> bool:
    row = db.query("SELECT value FROM settings WHERE key='instant_payout_enabled'", fetch="one")
    return (row[0] if row else "1") == "1"


def set_instant_payout_enabled(enabled: bool):
    db.query("INSERT OR REPLACE INTO settings(key,value) VALUES('instant_payout_enabled',?)", ("1" if enabled else "0",))


def get_app_balance() -> float:
    row = db.query("SELECT value FROM settings WHERE key='app_balance_usdt'", fetch="one")
    try:
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception:
        return 0.0


def set_app_balance(value: float):
    db.query("INSERT OR REPLACE INTO settings(key,value) VALUES('app_balance_usdt',?)", (f"{float(value):.2f}",))


def get_user_today_stats(uid: int) -> tuple[int, int, int]:
    today = now_kz_naive().strftime("%Y-%m-%d")
    submitted = db.query("SELECT COUNT(*) FROM submissions WHERE user_id=? AND date(created_at)=?", (uid, today), fetch="one")[0]
    vstal = db.query("SELECT COUNT(*) FROM sessions WHERE user_id=? AND date(start_time)=? AND status IN ('vstal','slet','otvyaz')", (uid, today), fetch="one")[0]
    slet = db.query("SELECT COUNT(*) FROM sessions WHERE user_id=? AND date(COALESCE(end_time,start_time))=? AND status='slet'", (uid, today), fetch="one")[0]
    return int(submitted or 0), int(vstal or 0), int(slet or 0)


def parse_break_lines(text: str):
    entries = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or "-" not in line:
            continue
        a, b = [part.strip() for part in line.split("-", 1)]
        try:
            start_time = datetime.strptime(a, "%H:%M")
            end_time = datetime.strptime(b, "%H:%M")
            entries.append((start_time, end_time))
        except ValueError:
            continue
    return entries


def unique_break_rows(rows):
    seen = set()
    unique = []
    for start_time, end_time in rows:
        start_label = parse_dt(start_time).strftime("%H:%M")
        end_label = parse_dt(end_time).strftime("%H:%M")
        key = (start_label, end_label)
        if key in seen:
            continue
        seen.add(key)
        unique.append((start_label, end_label))
    return unique


def get_break_minutes(group_id: int | None, start_time: datetime, end_time: datetime) -> int:
    if not group_id:
        return 0
    rows = db.query(
        "SELECT start_time, end_time FROM breaks WHERE group_id=? AND end_time > ? AND start_time < ?",
        (group_id, start_time.strftime("%Y-%m-%d %H:%M:%S"), end_time.strftime("%Y-%m-%d %H:%M:%S")),
    )
    minutes = 0
    for b_start, b_end in rows:
        b_start_dt = parse_dt(b_start)
        b_end_dt = parse_dt(b_end)
        overlap_start = max(start_time, b_start_dt)
        overlap_end = min(end_time, b_end_dt)
        if overlap_end > overlap_start:
            minutes += int((overlap_end - overlap_start).total_seconds() // 60)
    return minutes


def effective_minutes(group_id: int | None, start_time: datetime, end_time: datetime) -> int:
    total = int((end_time - start_time).total_seconds() // 60)
    return max(0, total - get_break_minutes(group_id, start_time, end_time))


def cleanup_queue_expired():
    cutoff = (now_kz_naive() - timedelta(hours=QUEUE_TTL_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
    db.query("DELETE FROM queue WHERE created_at IS NOT NULL AND created_at < ?", (cutoff,))


def cleanup_paid_reports():
    db.query("DELETE FROM sessions WHERE paid=1")


def cleanup_archives():
    db.query("DELETE FROM submissions")


def get_user_balance(user_id: int):
    row = db.query("SELECT COALESCE(SUM(amount),0) FROM payouts WHERE user_id=?", (user_id,), fetch="one")
    return float(row[0] if row else 0)


def get_office_label_for_group(group_id: int | None) -> str:
    if not group_id:
        return "Офис 1"
    rows = [row for row in get_linked_groups() if row[0] == group_id]
    if rows:
        title = (rows[0][2] or "").strip()
        if title:
            return title
    return "Офис 1"


def can_manage_number(user_id: int, owner_id: int, number: str) -> bool:
    if is_user_admin(user_id):
        return True
    q_row = db.query(
        "SELECT proc_by FROM queue WHERE number=? AND user_id=? AND status='proc' ORDER BY id DESC LIMIT 1",
        (number, owner_id),
        fetch="one",
    )
    if q_row:
        return q_row[0] == user_id
    s_row = db.query(
        "SELECT proc_by FROM sessions WHERE number=? AND user_id=? ORDER BY id DESC LIMIT 1",
        (number, owner_id),
        fetch="one",
    )
    if s_row:
        return s_row[0] == user_id
    return False


def build_producers_page(page: int):
    total = db.query("SELECT COUNT(*) FROM users", fetch="one")[0]
    max_page = max(1, (total + PRODUCERS_PAGE_SIZE - 1) // PRODUCERS_PAGE_SIZE)
    page = max(1, min(page, max_page))
    offset = (page - 1) * PRODUCERS_PAGE_SIZE
    rows = db.query("SELECT user_id, username, banned FROM users ORDER BY user_id DESC LIMIT ? OFFSET ?", (PRODUCERS_PAGE_SIZE, offset))
    return rows, page, max_page


def build_user_queue_view(uid: int):
    rows = db.query("SELECT id, number, submit_type FROM queue WHERE user_id=? AND status='waiting' ORDER BY id ASC", (uid,))
    total_waiting = db.query("SELECT COUNT(*) FROM queue WHERE status='waiting'", fetch="one")[0]
    user_priority = 1 if is_user_priority(uid) else 0
    kb = InlineKeyboardBuilder()
    lines = ["<b>📋 Ваши номера в очереди</b>", f"Всего номеров в общей очереди: <b>{total_waiting}</b>", ""]
    if not rows:
        lines.append("Номеров в очереди нет.")
    else:
        for q_id, number, submit_type in rows:
            q_pos = db.query(
                "SELECT COUNT(*) FROM queue q JOIN users u ON q.user_id=u.user_id WHERE q.status='waiting' AND (u.priority > ? OR (u.priority = ? AND q.id <= ?))",
                (user_priority, user_priority, q_id),
                fetch="one",
            )[0]
            t = "QR" if submit_type == "qr" else "Код"
            lines.append(f"• <code>{number}</code> [{t}] — очередь: {q_pos}")
            kb.row(types.InlineKeyboardButton(text=f"{number} • {t} • {q_pos}", callback_data=f"u_q_del_{q_id}"))
    kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="u_back"))
    return "\n".join(lines), kb.as_markup()


def build_admin_reports_view(page: int):
    today_str = now_kz_naive().strftime("%Y-%m-%d")
    tz_shift = f"{REPORT_TIME_SHIFT_HOURS:+d} hours"
    source_rows = db.query(
        "SELECT u.username, p.user_id, p.number, p.submit_type, p.amount, p.created_at "
        "FROM payouts p LEFT JOIN users u ON p.user_id=u.user_id "
        "WHERE date(datetime(p.created_at, ?))=? ORDER BY p.id DESC",
        (tz_shift, today_str),
    )
    total_paid = db.query("SELECT COALESCE(SUM(amount),0) FROM payouts", fetch="one")[0]

    total_today = sum((row[4] or 0) for row in source_rows)
    total_rows = len(source_rows)
    max_page = max(1, (total_rows + REPORTS_PAGE_SIZE - 1) // REPORTS_PAGE_SIZE)
    page = max(1, min(page, max_page))
    page_rows = source_rows[(page - 1) * REPORTS_PAGE_SIZE : page * REPORTS_PAGE_SIZE]

    lines = [
        f"<b>🧾 Отчёт выплат за сегодня • страница {page}/{max_page}</b>",
        f"<b>💰 Баланс (всё время):</b> {total_paid}$",
        f"<b>💵 За сегодня:</b> {total_today}$",
        "",
        "Пользователь | ID | Номер | Тип | Сумма",
        "",
    ]
    total = 0.0
    if not page_rows:
        lines.append("Пока пусто")
    else:
        for name, u_id, number, submit_type, amount, created_at in page_rows:
            label = f"@{name}" if name else f"ID:{u_id}"
            st = "QR" if submit_type == "qr" else "SMS"
            amt = float(amount or 0)
            total += amt
            lines.append(f"• {label} | <code>{u_id}</code> | <code>{number or '-'}</code> | {st} | {amt}$")
        lines.append("")
        lines.append(f"<b>ИТОГО ПО СТРАНИЦЕ: {total}$</b>")

    kb = InlineKeyboardBuilder()
    nav = []
    if page > 1:
        nav.append(types.InlineKeyboardButton(text="⬅️", callback_data=f"adm_reports_page_{page - 1}"))
    if page < max_page:
        nav.append(types.InlineKeyboardButton(text="➡️", callback_data=f"adm_reports_page_{page + 1}"))
    if nav:
        kb.row(*nav)
    kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="adm_main"))
    return "\n".join(lines), kb.as_markup()


def sms_request_key(user_id: int, number: str) -> str:
    return f"sms_req:{user_id}:{number}"


def sms_done_key(user_id: int, number: str) -> str:
    return f"sms_done:{user_id}:{number}"


def qr_request_key(user_id: int, number: str) -> str:
    return f"qr_req:{user_id}:{number}"


def qr_done_key(user_id: int, number: str) -> str:
    return f"qr_done:{user_id}:{number}"


def resolve_user_id(text: str) -> int | None:
    if not text:
        return None
    value = text.strip()
    if value.startswith("@"):
        row = db.query("SELECT user_id FROM users WHERE username=?", (value[1:],), fetch="one")
        return row[0] if row else None
    if value.isdigit():
        return int(value)
    return None


async def get_main_menu_kb(user_id: int):
    kb = InlineKeyboardBuilder()
    if not await check_sub(user_id):
        kb.row(InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_URL))
        kb.row(InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub"))
        return kb.as_markup()

    kb.row(InlineKeyboardButton(text="☎️ Сдать номер", callback_data="u_yield"))
    kb.row(InlineKeyboardButton(text="💾 Архив", callback_data="archive"), InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats"))
    kb.row(InlineKeyboardButton(text="📋 Общая очередь", callback_data="u_queue_all"), InlineKeyboardButton(text="💻 Техподдержка", callback_data="support"))
    kb.row(InlineKeyboardButton(text="📢 Канал", url=CHANNEL_URL))
    if is_user_admin(user_id):
        kb.row(InlineKeyboardButton(text="⚙️ Админ", callback_data="adm_main"))
    return kb.as_markup()


def back_kb(target: str = "u_back"):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Назад", callback_data=target)
    return kb.as_markup()


async def render_action_message(call: CallbackQuery, text: str, markup=None):
    try:
        await call.message.edit_caption(caption=text, reply_markup=markup, parse_mode="HTML")
        return
    except Exception:
        pass
    try:
        await call.message.edit_text(text=text, reply_markup=markup, parse_mode="HTML")
        return
    except Exception:
        pass


async def render_report_message(call: CallbackQuery, caption_text: str, markup):
    try:
        await call.message.edit_caption(caption=caption_text, reply_markup=markup, parse_mode="HTML")
        return
    except Exception:
        pass
    try:
        await call.message.edit_text(text=caption_text, reply_markup=markup, parse_mode="HTML")
        return
    except Exception:
        pass


async def issue_next_number(chat_id: int, thread_id: int | None, operator_id: int):
    if not is_work_enabled():
        return None
    if not group_scope_allows(chat_id, thread_id):
        return None
    cleanup_queue_expired()
    mode = get_topic_mode(chat_id, thread_id)
    where = "q.status='waiting'"
    params: list = []
    if mode == "SMS":
        where += " AND q.submit_type='code'"
    elif mode == "QR":
        where += " AND q.submit_type='qr'"
    res = db.query(
        f"SELECT q.id, q.user_id, q.number, q.submit_type FROM queue q LEFT JOIN users u ON q.user_id=u.user_id WHERE {where} ORDER BY u.priority DESC, q.id ASC LIMIT 1",
        tuple(params),
        fetch="one",
    )
    if not res:
        return None
    q_id, user_id, number, submit_type = res
    db.query("UPDATE queue SET status='proc', proc_by=?, repeat_requested=0, qr_requested=0 WHERE id=?", (operator_id, q_id))
    return user_id, number, submit_type


@dp.callback_query()
async def cb_handler(call: CallbackQuery, state: FSMContext):
    uid, data = call.from_user.id, call.data

    if data in {"refresh_menu", "u_back", "u_menu"}:
        await state.clear()
        cap = get_menu_caption(await check_sub(uid))
        await render_action_message(call, cap, await get_main_menu_kb(uid))

    elif data == "check_sub":
        cap = get_menu_caption(await check_sub(uid))
        await render_action_message(call, cap, await get_main_menu_kb(uid))

    elif data.startswith("pay_check_"):
        inv_part = data.replace("pay_check_", "", 1)
        if not inv_part.isdigit():
            return await call.answer("Некорректный ID инвойса", show_alert=True)
        inv_id = int(inv_part)
        ok_inv, inv_status, inv_msg = await crypto_bot_check_invoice(inv_id)
        if not ok_inv:
            return await call.answer(f"Ошибка проверки: {inv_msg[:120]}", show_alert=True)
        human = {"paid": "оплачен", "active": "ожидает оплаты", "expired": "истёк"}.get(inv_status, inv_status)
        await call.answer(f"Инвойс {inv_id}: {human}", show_alert=True)

    elif data in {"submit_request", "u_yield"}:
        if not is_work_enabled():
            await call.answer("⛔ Сейчас не работаем. Попробуйте позже.", show_alert=True)
            return
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="🔑 Код", callback_data="u_submit_code"), InlineKeyboardButton(text="🧾 QR", callback_data="u_submit_qr"))
        kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="u_back"))
        await render_action_message(call, "Выберите тип сдачи:", kb.as_markup())

    elif data in {"u_submit_code", "u_submit_qr", "u_yield_confirm"}:
        await state.set_state(Form.num)
        await state.update_data(submit_type="qr" if data.endswith("qr") else "code")
        await call.message.answer("📞 Введите номера (каждый с новой строки или через пробел):")

    elif data == "u_q":
        caption, markup = build_user_queue_view(uid)
        await render_action_message(call, caption, markup)

    elif data.startswith("u_q_del_"):
        q_id = int(data.replace("u_q_del_", ""))
        db.query("DELETE FROM queue WHERE id=? AND user_id=? AND status='waiting'", (q_id, uid))
        caption, markup = build_user_queue_view(uid)
        await render_action_message(call, caption, markup)

    elif data == "u_queue_all":
        cleanup_queue_expired()
        rows = db.query(
            "SELECT id, number, submit_type FROM queue WHERE status='waiting' ORDER BY id ASC LIMIT 80"
        )
        lines = ["<b>📋 Общая очередь</b>", ""]
        kb = InlineKeyboardBuilder()
        if not rows:
            lines.append("Очередь пустая")
        else:
            for qid, number, submit_type in rows:
                t = "QR" if submit_type == "qr" else "SMS"
                lines.append(f"• <code>{number}</code> [{t}]")
                kb.row(types.InlineKeyboardButton(text=f"{number} • {t}", callback_data=f"u_queue_pick_{qid}"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="u_back"))
        await render_action_message(call, "\n".join(lines), kb.as_markup())

    elif data.startswith("u_queue_pick_"):
        qid = int(data.replace("u_queue_pick_", ""))
        row = db.query("SELECT number, submit_type, user_id FROM queue WHERE id=? AND status='waiting'", (qid,), fetch="one")
        if not row:
            return await call.answer("⚠️ Номер уже вышел из очереди", show_alert=False)
        number, submit_type, owner_id = row
        db.query("DELETE FROM queue WHERE id=? AND status='waiting'", (qid,))

        cleanup_queue_expired()
        rows = db.query("SELECT id, number, submit_type FROM queue WHERE status='waiting' ORDER BY id ASC LIMIT 80")
        lines = ["<b>📋 Общая очередь</b>", "", f"✅ Удален номер: <code>{number}</code>", ""]
        kb = InlineKeyboardBuilder()
        if not rows:
            lines.append("Очередь пустая")
        else:
            for next_qid, next_number, next_submit_type in rows:
                t = "QR" if next_submit_type == "qr" else "SMS"
                lines.append(f"• <code>{next_number}</code> [{t}]")
                kb.row(types.InlineKeyboardButton(text=f"{next_number} • {t}", callback_data=f"u_queue_pick_{next_qid}"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="u_back"))
        await render_action_message(call, "\n".join(lines), kb.as_markup())

    elif data in {"archive", "u_my_numbers"}:
        balance = get_user_balance(uid)
        rows = db.query(
            "SELECT number, submit_type, status, price, paid, start_time "
            "FROM sessions WHERE user_id=? AND (status!='vstal' OR credited_notified=1) ORDER BY id DESC LIMIT 300",
            (uid,),
        )
        status_map = {"vstal": "встал", "slet": "слетел", "otvyaz": "бан", "error": "ошибка", "detached": "отвяз"}
        lines = [f"<b>💰 Баланс:</b> <b>{balance}$</b>", "<b>📱 Архив номеров:</b>", "Формат: Номер | Тип | Сумма | Дата/время", ""]
        if not rows:
            lines.append("Пока пусто")
        else:
            for number, submit_type, status, price, paid, dt in rows:
                st = "QR" if submit_type == "qr" else "SMS"
                stat = status_map.get(status or "", status or "-")
                amount = float(price or 0) if int(paid or 0) else 0.0
                lines.append(f"• <code>{number}</code> | {st} | {amount}$ | {dt} ({stat})")
        await render_action_message(call, "\n".join(lines), back_kb())

    elif data == "my_stats":
        submitted, vstal, slet = get_user_today_stats(uid)
        txt = (
            "<b>📊 Ваша статистика за сегодня</b>\n\n"
            f"Сдано номеров: <b>{submitted}</b>\n"
            f"Встало: <b>{vstal}</b>\n"
            f"Слетело: <b>{slet}</b>"
        )
        await render_action_message(call, txt, back_kb())

    elif data == "u_breaks":
        today_label = now_kz_naive().strftime("%Y-%m-%d")
        linked_groups = sorted({gid for gid, _thread, _title in get_linked_groups()})
        lines = ["<b>🪩 Перерывы</b>", ""]
        if not linked_groups:
            lines.append("Активных перерывов нет.")
        else:
            placeholders = ",".join("?" for _ in linked_groups)
            rows = db.query(
                f"SELECT group_id, start_time, end_time FROM breaks WHERE group_id IN ({placeholders}) AND date(start_time)=? ORDER BY group_id ASC, start_time DESC LIMIT 200",
                tuple(linked_groups) + (today_label,),
            )
            if not rows:
                lines.append("Активных перерывов нет.")
            else:
                office_by_gid = {gid: f"Офис {idx}" for idx, gid in enumerate(linked_groups, 1)}
                grouped = {}
                for group_id, start_time, end_time in rows:
                    label = office_by_gid.get(group_id, "Офис 1")
                    grouped.setdefault(label, []).append((start_time, end_time))
                for label, entries in grouped.items():
                    lines.append(f"<b>{label}</b>")
                    for start_label, end_label in unique_break_rows(entries):
                        lines.append(f"• {start_label}–{end_label}")
                    lines.append("")
        await render_action_message(call, "\n".join(lines), back_kb())

    elif data in {"admin_menu", "adm_main"}:
        if not is_user_admin(uid):
            return
        work_label = "🟢 РАБОТАЕМ" if is_work_enabled() else "🔴 НЕ РАБОТАЕМ"
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text=f"Статус: {work_label}", callback_data="adm_toggle_work"))
        kb.row(types.InlineKeyboardButton(text="📣 Сообщение пользователям", callback_data="adm_broadcast"))
        kb.row(types.InlineKeyboardButton(text="✉️ Сообщение по пользователю", callback_data="adm_direct_message"))
        kb.row(types.InlineKeyboardButton(text="👤 Пользователи", callback_data="adm_users"))
        kb.row(types.InlineKeyboardButton(text="💲 Тариф", callback_data="adm_set_price"), types.InlineKeyboardButton(text="💸 Выплаты", callback_data="adm_payouts"))
        kb.row(types.InlineKeyboardButton(text="🏢 Группы", callback_data="adm_groups"), types.InlineKeyboardButton(text="📋 Очередь", callback_data="adm_queue_view"))
        kb.row(types.InlineKeyboardButton(text="🏆 Лидеры", callback_data="adm_leaders"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="u_back"))
        cleanup_queue_expired()
        waiting_cnt = db.query("SELECT COUNT(*) FROM queue WHERE status='waiting' AND number IS NOT NULL AND number<>''", fetch="one")[0]
        await render_action_message(call, f"🛡 <b>Админ-панель</b>\nСтатус: <b>{'РАБОТАЕМ' if is_work_enabled() else 'НЕ РАБОТАЕМ'}</b>\nОчередь: <b>{waiting_cnt}</b>", kb.as_markup())

    elif data == "adm_set_price":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.set_price)
        await call.message.answer("Введите новый прайс за номер (например, 6 или 5.5):")

    elif data == "adm_clear":
        if not is_user_admin(uid):
            return
        db.query("DELETE FROM queue")
        await call.answer("✅ Очередь очищена", show_alert=True)

    elif data == "adm_toggle_work":
        if not is_user_admin(uid):
            return
        set_work_enabled(not is_work_enabled())
        work_label = "🟢 РАБОТАЕМ" if is_work_enabled() else "🔴 НЕ РАБОТАЕМ"
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text=f"Статус: {work_label}", callback_data="adm_toggle_work"))
        kb.row(types.InlineKeyboardButton(text="📣 Сообщение пользователям", callback_data="adm_broadcast"))
        kb.row(types.InlineKeyboardButton(text="✉️ Сообщение по пользователю", callback_data="adm_direct_message"))
        kb.row(types.InlineKeyboardButton(text="👤 Пользователи", callback_data="adm_users"))
        kb.row(types.InlineKeyboardButton(text="💲 Тариф", callback_data="adm_set_price"), types.InlineKeyboardButton(text="💸 Выплаты", callback_data="adm_payouts"))
        kb.row(types.InlineKeyboardButton(text="🏢 Группы", callback_data="adm_groups"), types.InlineKeyboardButton(text="📋 Очередь", callback_data="adm_queue_view"))
        kb.row(types.InlineKeyboardButton(text="🏆 Лидеры", callback_data="adm_leaders"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="u_back"))
        cleanup_queue_expired()
        waiting_cnt = db.query("SELECT COUNT(*) FROM queue WHERE status='waiting' AND number IS NOT NULL AND number<>''", fetch="one")[0]
        await render_action_message(call, f"🛡 <b>Админ-панель</b>\nСтатус: <b>{'РАБОТАЕМ' if is_work_enabled() else 'НЕ РАБОТАЕМ'}</b>\nОчередь: <b>{waiting_cnt}</b>", kb.as_markup())
        await call.answer("✅ Статус обновлён", show_alert=False)

    elif data == "adm_settings":
        if not is_user_admin(uid):
            return
        target = get_support_target()
        support_text = "настроена" if target else "не настроена"
        txt = (
            "<b>⚙️ Настройки</b>\n\n"
            f"Работа бота: <b>{'вкл' if is_work_enabled() else 'выкл'}</b>\n"
            f"Техподдержка: <b>{support_text}</b>\n\n"
            "Команды:\n"
            "• /set — привязка рабочей группы/топика\n"
            "• /set_support — привязка группы/топика техподдержки"
        )
        await render_action_message(call, txt, back_kb("adm_main"))

    elif data == "adm_payouts" or data == "adm_payout_refresh":
        if not is_user_admin(uid):
            return
        total = db.query("SELECT COALESCE(SUM(amount),0) FROM payouts", fetch="one")[0]
        cnt = db.query("SELECT COUNT(*) FROM payouts", fetch="one")[0]
        inst = "🟢 Вкл" if is_instant_payout_enabled() else "🔴 Выкл"
        ok_bal, usdt_bal, bal_msg = await crypto_bot_get_usdt_balance()
        ok_auth, auth_msg = await crypto_bot_check_auth()
        if ok_bal:
            set_app_balance(usdt_bal)
        app_balance = get_app_balance()
        bal_text = f"<b>{usdt_bal:.2f} USDT</b>" if ok_bal else f"<b>ошибка:</b> {bal_msg}"
        auth_text = f"<b>OK</b> ({auth_msg})" if ok_auth else f"<b>ошибка:</b> {auth_msg}"
        txt = (
            "<b>💸 Выплаты</b>\n\n"
            f"Всего выплат: <b>{cnt}</b>\n"
            f"Сумма выплат: <b>{total}$</b>\n"
            f"Моментальная выплата: <b>{inst}</b>\n"
            f"Баланс Crypto Bot (USDT): {bal_text}\n"
            f"Баланс приложения: <b>{app_balance:.2f} USDT</b>\n"
            f"Проверка токена: {auth_text}\n\n"
            "Пополнение: создайте инвойс ниже, оплатите его в @CryptoBot, затем нажмите обновить баланс."
        )
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text=f"⚡ Моментальная выплата: {inst}", callback_data="adm_payout_toggle"))
        kb.row(types.InlineKeyboardButton(text="💳 Create Invoice (пополнение)", callback_data="adm_payout_topup_prompt"))
        kb.row(types.InlineKeyboardButton(text="🔄 Обновить баланс Crypto Bot", callback_data="adm_payout_refresh"))
        kb.row(types.InlineKeyboardButton(text="🧾 Проверить последний инвойс", callback_data="adm_payout_check_invoice"))
        kb.row(types.InlineKeyboardButton(text="🧪 Диагностика Crypto Pay", callback_data="adm_payout_diag"))
        kb.row(types.InlineKeyboardButton(text="💳 Выплатить всем вставшим", callback_data="adm_payout_payall"))
        kb.row(
            types.InlineKeyboardButton(text="⚡ Transfer (мгновенная выплата)", callback_data="adm_payout_user_prompt"),
            types.InlineKeyboardButton(text="🎫 Create Check (выплата)", callback_data="adm_payout_check_user_prompt"),
        )
        kb.row(types.InlineKeyboardButton(text="🧾 Отчёт за день", callback_data="adm_reports"), types.InlineKeyboardButton(text="🧹 Очистить отчёты", callback_data="adm_payout_clear_reports"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="adm_main"))
        await render_action_message(call, txt, kb.as_markup())


    elif data == "adm_payout_topup_prompt":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.app_topup_amount)
        await call.message.answer("Введите сумму пополнения в USDT (например, 50):")

    elif data == "adm_payout_check_invoice":
        if not is_user_admin(uid):
            return
        row = db.query("SELECT value FROM settings WHERE key='last_topup_invoice_id'", fetch="one")
        if not row or not str(row[0]).isdigit():
            return await call.message.answer("ℹ️ Последний инвойс не найден. Сначала создайте пополнение.")
        inv_id = int(row[0])
        ok_inv, inv_status, inv_msg = await crypto_bot_check_invoice(inv_id)
        if not ok_inv:
            return await call.message.answer(f"❌ Ошибка проверки инвойса {inv_id}: {inv_msg}")
        await call.message.answer(f"🧾 Инвойс <code>{inv_id}</code> статус: <b>{inv_status}</b>", parse_mode="HTML")

    elif data == "adm_payout_diag":
        if not is_user_admin(uid):
            return
        report = await crypto_bot_diagnostics_report()
        await call.message.answer(report, parse_mode="HTML")

    elif data == "adm_payout_toggle":
        if not is_user_admin(uid):
            return
        set_instant_payout_enabled(not is_instant_payout_enabled())
        await call.answer("✅ Обновлено", show_alert=False)

    elif data == "adm_payout_user_prompt":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.payout_amount)
        await state.update_data(pay_target_id=None, payout_mode="transfer")
        await call.message.answer("Введите выплату через Transfer в формате: ID сумма (например: 123456789 5)")

    elif data == "adm_payout_check_user_prompt":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.payout_amount)
        await state.update_data(pay_target_id=None, payout_mode="check")
        await call.message.answer("Введите выплату через Create Check в формате: ID сумма (например: 123456789 5)")

    elif data == "adm_payout_payall":
        if not is_user_admin(uid):
            return
        rows = db.query("SELECT user_id, number, submit_type, price FROM sessions WHERE status='vstal' AND paid=0")
        paid = 0
        failed = 0
        for u_id, number, submit_type, price in rows:
            amount = float(price or PRICE_PER_NUMBER)
            ok, msg, result = await crypto_bot_send_usdt(u_id, amount)
            if not ok:
                failed += 1
                logging.warning("Crypto payout failed uid=%s num=%s: %s", u_id, number, msg)
                continue
            payload = json.dumps(result or {}, ensure_ascii=False)
            db.query("INSERT INTO payouts(user_id, number, submit_type, amount, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)", (u_id, number, submit_type or 'code', amount, payload, now_kz_naive().strftime("%Y-%m-%d %H:%M:%S")))
            db.query("UPDATE sessions SET paid=1 WHERE user_id=? AND number=? AND status='vstal'", (u_id, number))
            await notify_payout_group(u_id, number, amount, status="выплачено")
            paid += 1
        await call.answer(f"✅ Выплачено: {paid} | Ошибок: {failed}", show_alert=True)

    elif data == "adm_payout_clear_reports":
        if not is_user_admin(uid):
            return
        db.query("DELETE FROM sessions")
        db.query("DELETE FROM payouts")
        await call.answer("✅ Отчёты очищены", show_alert=True)

    elif data == "adm_queue_view" or data.startswith("adm_queue_page_"):
        if not is_user_admin(uid):
            return
        page = 1
        if data.startswith("adm_queue_page_"):
            try:
                page = int(data.rsplit("_", 1)[1])
            except Exception:
                page = 1
        cleanup_queue_expired()
        total = db.query("SELECT COUNT(*) FROM queue WHERE number IS NOT NULL AND number<>''", fetch="one")[0]
        page_size = 20
        max_page = max(1, (total + page_size - 1) // page_size)
        page = max(1, min(page, max_page))
        offset = (page - 1) * page_size
        rows = db.query("SELECT q.number, q.status, q.submit_type, q.created_at, u.username, q.user_id FROM queue q LEFT JOIN users u ON q.user_id=u.user_id WHERE q.number IS NOT NULL AND q.number<>'' ORDER BY q.id ASC LIMIT ? OFFSET ?", (page_size, offset))
        lines = [f"<b>📋 Очередь ({page}/{max_page})</b>", ""]
        kb = InlineKeyboardBuilder()
        if not rows:
            lines.append("Очередь пустая")
        else:
            for number, status, submit_type, created_at, username, user_id in rows:
                label = f"@{username}" if username else f"ID:{user_id}"
                t = "QR" if submit_type == "qr" else "Код"
                lines.append(f"• <code>{number}</code> [{t}] — {status} — {label}")
        nav = []
        if page > 1:
            nav.append(types.InlineKeyboardButton(text="⬅️", callback_data=f"adm_queue_page_{page-1}"))
        if page < max_page:
            nav.append(types.InlineKeyboardButton(text="➡️", callback_data=f"adm_queue_page_{page+1}"))
        if nav:
            kb.row(*nav)
        kb.row(types.InlineKeyboardButton(text="🧹 Очистить очередь", callback_data="adm_clear"), types.InlineKeyboardButton(text="👤 Удалить по ID", callback_data="adm_queue_remove_user"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="adm_main"))
        await render_action_message(call, "\n".join(lines), kb.as_markup())

    elif data == "adm_queue_remove_user":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.queue_remove_user)
        await call.message.answer("Введите user_id, чью очередь удалить:")

    elif data == "adm_support_hint":
        if not is_user_admin(uid):
            return
        await call.message.answer("ℹ️ Для привязки техподдержки зайдите в нужный чат/топик и отправьте команду: /set_support")

    elif data == "adm_stats":
        if not is_user_admin(uid):
            return
        users = db.query("SELECT COUNT(*) FROM users", fetch="one")[0]
        waiting = db.query("SELECT COUNT(*) FROM queue WHERE status='waiting'", fetch="one")[0]
        paid = db.query("SELECT COUNT(*) FROM sessions WHERE status='slet' AND paid=1", fetch="one")[0]
        paid_amount = db.query("SELECT COALESCE(SUM(price),0) FROM sessions WHERE status='slet' AND paid=1", fetch="one")[0]
        txt = (
            "<b>📊 Статистика</b>\n\n"
            f"Пользователей: <b>{users}</b>\n"
            f"В очереди: <b>{waiting}</b>\n"
            f"Оплаченных слётов: <b>{paid}</b>\n"
            f"Сумма: <b>{paid_amount}$</b>"
        )
        await render_action_message(call, txt, back_kb("adm_main"))

    elif data == "adm_leaders":
        if not is_user_admin(uid):
            return
        tops = db.query(
            "SELECT u.username, u.user_id, COUNT(s.id) as c FROM sessions s JOIN users u ON s.user_id=u.user_id GROUP BY u.user_id ORDER BY c DESC LIMIT 20"
        )
        lines = ["<b>🏆 Лидеры по номерам</b>", ""]
        if not tops:
            lines.append("Пока пусто")
        else:
            for i, (name, u_id, cnt) in enumerate(tops, 1):
                label = f"@{name}" if name else f"ID:{u_id}"
                lines.append(f"{i}. {label} — {cnt}")
        await render_action_message(call, "\n".join(lines), back_kb("adm_main"))

    elif data == "adm_top_vstal":
        if not is_user_admin(uid):
            return
        tops = db.query(
            "SELECT u.username, u.user_id, COUNT(s.id) FROM sessions s JOIN users u ON s.user_id=u.user_id WHERE s.status IN ('vstal','paid','slet') GROUP BY u.user_id ORDER BY COUNT(s.id) DESC LIMIT 30"
        )
        lines = ["<b>✅ Топ встал номеров (ТОП-30)</b>", ""]
        if not tops:
            lines.append("Пока пусто")
        else:
            for i, (name, u_id, cnt) in enumerate(tops, 1):
                label = f"@{name}" if name else f"ID:{u_id}"
                lines.append(f"{i}. {label} — {cnt} шт.")
        await render_action_message(call, "\n".join(lines), back_kb("adm_main"))

    elif data == "adm_reports" or data.startswith("adm_reports_page_"):
        if not is_user_admin(uid):
            return
        page = 1
        if data.startswith("adm_reports_page_"):
            try:
                page = int(data.rsplit("_", 1)[1])
            except ValueError:
                page = 1
        caption_text, markup = build_admin_reports_view(page)
        await render_report_message(call, caption_text, markup)

    elif data == "adm_broadcast":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.broadcast)
        await call.message.answer("Введите текст рассылки для всех пользователей:")

    elif data == "adm_groups":
        if not is_user_admin(uid):
            return
        groups = get_linked_groups()
        kb = InlineKeyboardBuilder()
        lines = ["<b>🏢 Привязанные топики</b>", ""]
        if not groups:
            lines.append("Топики не привязаны.")
        else:
            for gid, thread_id, title in groups:
                label = title or f"Группа {gid}"
                kb.row(types.InlineKeyboardButton(text=f"❌ {label} • топик {thread_id}", callback_data=f"adm_group_unlink_{gid}_{thread_id}"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="adm_main"))
        await render_action_message(call, "\n".join(lines), kb.as_markup())

    elif data == "adm_breaks":
        if not is_user_admin(uid):
            return
        groups = get_linked_groups()
        kb = InlineKeyboardBuilder()
        lines = ["<b>🍽 Перерывы по группам</b>", ""]
        if not groups:
            lines.append("Топики не привязаны.")
        else:
            for gid, _thread_id, title in groups:
                label = title or f"Группа {gid}"
                kb.row(types.InlineKeyboardButton(text=label, callback_data=f"adm_breaks_group_{gid}"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="adm_main"))
        await render_action_message(call, "\n".join(lines), kb.as_markup())

    elif data.startswith("adm_breaks_group_"):
        if not is_user_admin(uid):
            return
        gid = int(data.replace("adm_breaks_group_", ""))
        await state.set_state(AdminForm.breaks_group)
        await state.update_data(break_group_id=gid)
        today_label = now_kz_naive().strftime("%Y-%m-%d")
        rows = db.query("SELECT start_time, end_time FROM breaks WHERE group_id=? AND date(start_time)=? ORDER BY start_time ASC", (gid, today_label))
        group_title = db.query("SELECT value FROM settings WHERE key LIKE ? LIMIT 1", (f"gid:{gid}:%",), fetch="one")
        group_name = group_title[0] if group_title and group_title[0] else f"Группа {gid}"
        lines = [f"<b>🍽 Перерывы {group_name}</b>", "Вводите время строками, например:", "11:00 - 12:00", "14:00 - 14:30", "", "Текущее:"]
        if not rows:
            lines.append("Перерывов нет.")
        else:
            for start_time, end_time in rows:
                lines.append(f"• {parse_dt(start_time).strftime('%H:%M')}–{parse_dt(end_time).strftime('%H:%M')}")
        await render_action_message(call, "\n".join(lines), back_kb("adm_breaks"))

    elif data.startswith("adm_group_unlink_"):
        if not is_user_admin(uid):
            return
        gid_part, thread_part = data.replace("adm_group_unlink_", "").split("_", 1)
        db.query("DELETE FROM settings WHERE key=?", (get_group_binding_key(int(gid_part), int(thread_part)),))
        await call.answer("✅ Топик отвязан", show_alert=True)

    elif data == "adm_users" or data.startswith("adm_users_page_"):
        if not is_user_admin(uid):
            return
        page = 1
        if data.startswith("adm_users_page_"):
            try:
                page = int(data.replace("adm_users_page_", ""))
            except Exception:
                page = 1
        rows, page, max_page = build_producers_page(page)
        kb = InlineKeyboardBuilder()
        lines = [f"<b>👤 Пользователи (страница {page}/{max_page})</b>", ""]
        for user_id, username, banned in rows:
            label = f"@{username}" if username else f"ID:{user_id}"
            status = "🚫" if banned else "✅"
            lines.append(f"{status} {label}")
            kb.row(types.InlineKeyboardButton(text=label, callback_data=f"adm_user_menu_{user_id}_{page}"))
        nav = []
        if page > 1:
            nav.append(types.InlineKeyboardButton(text="⬅️", callback_data=f"adm_users_page_{page-1}"))
        if page < max_page:
            nav.append(types.InlineKeyboardButton(text="➡️", callback_data=f"adm_users_page_{page+1}"))
        if nav:
            kb.row(*nav)
        kb.row(types.InlineKeyboardButton(text="🔎 Найти пользователя", callback_data="adm_user_search"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="adm_main"))
        await render_action_message(call, "\n".join(lines), kb.as_markup())

    elif data == "adm_user_search":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.queue_remove_user)
        await state.update_data(search_mode="user")
        await call.message.answer("Введите @username или ID для поиска пользователя:")

    elif data == "adm_direct_message":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.direct_message)
        await call.message.answer("Введите: @username/ID и текст сообщения через пробел")

    elif data.startswith("adm_user_menu_"):
        if not is_user_admin(uid):
            return
        parts = data.replace("adm_user_menu_", "").split("_")
        target_id = int(parts[0])
        page = int(parts[1]) if len(parts) > 1 else 1
        row = db.query("SELECT username, banned FROM users WHERE user_id=?", (target_id,), fetch="one")
        if not row:
            return
        username, banned = row
        label = f"@{username}" if username else f"ID:{target_id}"
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text="🚫 Бан", callback_data=f"adm_user_ban_{target_id}_{page}"), types.InlineKeyboardButton(text="✅ Разбан", callback_data=f"adm_user_unban_{target_id}_{page}"))
        kb.row(types.InlineKeyboardButton(text="💸 Выплатить", callback_data=f"adm_user_pay_{target_id}_{page}"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data=f"adm_users_page_{page}"))
        await render_action_message(call, f"<b>Пользователь:</b> {label}\nСтатус: {'🚫' if banned else '✅'}", kb.as_markup())

    elif data.startswith("adm_user_ban_"):
        if not is_user_admin(uid):
            return
        parts = data.replace("adm_user_ban_", "").split("_")
        target_id = int(parts[0])
        db.query("UPDATE users SET banned=1 WHERE user_id=?", (target_id,))
        await call.answer("✅ Забанен", show_alert=True)

    elif data.startswith("adm_user_unban_"):
        if not is_user_admin(uid):
            return
        parts = data.replace("adm_user_unban_", "").split("_")
        target_id = int(parts[0])
        db.query("UPDATE users SET banned=0 WHERE user_id=?", (target_id,))
        await call.answer("✅ Разбанен", show_alert=True)

    elif data.startswith("adm_user_pay_"):
        if not is_user_admin(uid):
            return
        parts = data.replace("adm_user_pay_", "").split("_")
        target_id = int(parts[0])
        await state.set_state(AdminForm.payout_amount)
        await state.update_data(pay_target_id=target_id)
        await call.message.answer(f"Введите сумму выплаты для ID {target_id}:")

    elif data == "adm_priorities":
        if not is_user_admin(uid):
            return
        rows = db.query("SELECT user_id, username, priority FROM users WHERE priority=1 ORDER BY user_id DESC LIMIT 30")
        lines = ["<b>⭐️ Приоритеты</b>", ""]
        for user_id, username, priority in rows:
            label = f"@{username}" if username else f"ID:{user_id}"
            lines.append(f"{'⭐️' if priority else '•'} {label}")
        if not rows:
            lines.append("Пока пусто")
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text="Выдать приоритет", callback_data="adm_priority_add"), types.InlineKeyboardButton(text="Снять приоритет", callback_data="adm_priority_remove"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="adm_main"))
        await render_action_message(call, "\n".join(lines), kb.as_markup())

    elif data == "adm_priority_add":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.priority_add)
        await call.message.answer("Введите @username или ID для выдачи приоритета:")

    elif data == "adm_priority_remove":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.priority_remove)
        await call.message.answer("Введите @username или ID для снятия приоритета:")

    elif data == "adm_admins":
        if not is_user_admin(uid):
            return
        rows = db.query("SELECT user_id, username FROM users WHERE is_admin=1 ORDER BY user_id DESC LIMIT 30")
        lines = ["<b>👮 Админы бота</b>", ""] + [f"• {'@'+u if u else 'ID:'+str(i)}" for i, u in rows]
        if not rows:
            lines.append("Список пуст")
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text="➕ Добавить", callback_data="adm_admin_add"), types.InlineKeyboardButton(text="➖ Удалить", callback_data="adm_admin_remove"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="adm_main"))
        await render_action_message(call, "\n".join(lines), kb.as_markup())

    elif data == "adm_admin_add":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.admin_add)
        await call.message.answer("Введите @username или ID для добавления администратора:")

    elif data == "adm_admin_remove":
        if not is_user_admin(uid):
            return
        await state.set_state(AdminForm.admin_remove)
        await call.message.answer("Введите @username или ID для удаления администратора:")

    elif data.startswith("support_reply_"):
        if not is_user_admin(uid):
            return
        target_id = int(data.replace("support_reply_", ""))
        db.query("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (f"support_reply_target:{uid}", str(target_id)))
        try:
            await bot.send_message(uid, "✍️ Напишите ответ пользователю в этом чате (ЛС бота).")
        except Exception:
            pass
        await call.answer("Откройте ЛС бота и отправьте ответ.", show_alert=True)

    elif data == "support":
        await state.set_state(SupportForm.request)
        await call.message.answer("💻 Введите ваш запрос или описание ошибки одним сообщением:")

    elif data == "transfer":
        await render_action_message(call, "🎯 Переводы: автоначисление 4$ сразу при статусе Встал. Для ручной выплаты используйте карточку пользователя в админ-панели.", back_kb())

    elif data == "u_next_num":
        thread_id = get_thread_id(call.message)
        result = await issue_next_number(call.message.chat.id, thread_id, uid)
        if not result:
            await call.answer("Очередь пуста", show_alert=False)
            return
        user_id, number, submit_type = result
        st = "QR" if submit_type == "qr" else "SMS"
        next_kb = InlineKeyboardBuilder()
        if submit_type == "qr":
            next_kb.row(
                types.InlineKeyboardButton(text="🧾 Запросить QR", callback_data=f"qr_{call.message.chat.id}_{thread_id or 0}_{user_id}_{number}"),
                types.InlineKeyboardButton(text="⏭ Скип", callback_data=f"k_{user_id}_{number}"),
            )
            next_kb.row(types.InlineKeyboardButton(text="❌ Номер не встал", callback_data=f"n_{user_id}_{number}"))
        else:
            next_kb.row(
                types.InlineKeyboardButton(text="🔔 Запросить SMS", callback_data=f"rr_{call.message.chat.id}_{thread_id or 0}_{user_id}_{number}"),
                types.InlineKeyboardButton(text="⏭ Скип", callback_data=f"k_{user_id}_{number}"),
            )
            next_kb.row(types.InlineKeyboardButton(text="❌ Номер не встал", callback_data=f"n_{user_id}_{number}"))
        await call.message.answer(
            f"Метод: <b>{st}</b>\nНомер: <code>{number}</code>\nОператор: <code>{uid}</code>\n\nОтправьте фото/код в ответ на это сообщение.",
            parse_mode="HTML",
            reply_markup=next_kb.as_markup(),
        )
        try:
            office_label = get_office_label_for_group(call.message.chat.id)
            total_waiting = db.query("SELECT COUNT(*) FROM queue WHERE status='waiting'", fetch="one")[0]
            await bot.send_message(user_id, f"📨 Ваш номер {number} взяли. {office_label}. Ожидайте кода.\n📋 Текущая общая очередь: {total_waiting}")
        except Exception:
            pass

    elif data.startswith(("rr_", "qr_")):
        parts = data.split("_", 4)
        if len(parts) < 5:
            await call.answer()
            return
        action, chat_id, thread_id, worker_uid, number = parts
        chat_id = int(chat_id)
        thread_id = int(thread_id)
        worker_uid = int(worker_uid)
        flag_col = "repeat_requested" if action == "rr" else "qr_requested"
        row = db.query(f"SELECT {flag_col} FROM queue WHERE number=? AND user_id=? AND status='proc' ORDER BY id DESC LIMIT 1", (number, worker_uid), fetch="one")
        if not row:
            await call.answer("⚠️ Номер уже закрыт", show_alert=False)
            return
        if row[0]:
            await call.answer("⚠️ Уже запрашивали", show_alert=False)
            return
        db.query(f"UPDATE queue SET {flag_col}=1 WHERE number=? AND user_id=? AND status='proc'", (number, worker_uid))
        try:
            if action == "rr":
                db.query("INSERT OR REPLACE INTO settings(key, value) VALUES(?, '1')", (sms_request_key(worker_uid, number),))
                db.query("DELETE FROM settings WHERE key=?", (sms_done_key(worker_uid, number),))
                await bot.send_message(worker_uid, f"🔔 По номеру {number} запросили SMS-код.\nОтправьте код ОДНИМ сообщением в ответ на это сообщение.")
                try:
                    await call.message.edit_reply_markup(reply_markup=call.message.reply_markup)
                    await call.message.answer("✅ SMS была запрошена, ожидайте.")
                except Exception:
                    pass
            else:
                db.query("INSERT OR REPLACE INTO settings(key, value) VALUES(?, '1')", (qr_request_key(worker_uid, number),))
                db.query("DELETE FROM settings WHERE key=?", (qr_done_key(worker_uid, number),))
                await bot.send_message(worker_uid, f"🔔 По номеру {number} запросили повторный QR.\nОтправьте фото QR-кода ОДНИМ сообщением в ответ на это сообщение.")
            await call.answer("✅ Запрос отправлен")
        except Exception:
            await call.answer("⚠️ Не удалось отправить запрос", show_alert=True)

    elif data.startswith(("v_", "s_", "e_", "m_", "k_", "d_", "n_")):
        act, v_uid, v_num = data.split("_", 2)
        v_uid = int(v_uid)
        if not can_manage_number(uid, v_uid, v_num):
            await call.answer("⛔ Нет доступа", show_alert=False)
            return

        if act == "v":
            group_id = call.message.chat.id if call.message and call.message.chat else None
            price = get_user_price(v_uid)
            st_row = db.query("SELECT submit_type, proc_by, code_sender_id FROM queue WHERE number=? AND user_id=? ORDER BY id DESC LIMIT 1", (v_num, v_uid), fetch="one")
            submit_type = st_row[0] if st_row else "code"
            proc_by = st_row[1] if st_row else uid
            code_sender_id = st_row[2] if st_row else uid
            db.query(
                "INSERT INTO sessions (number, user_id, start_time, status, paid, group_id, price, proc_by, code_sender_id, submit_type) VALUES (?, ?, ?, 'vstal', 0, ?, ?, ?, ?, ?)",
                (v_num, v_uid, now_kz_naive().strftime("%Y-%m-%d %H:%M:%S"), group_id, price, proc_by, code_sender_id, submit_type),
            )
            db.query("DELETE FROM queue WHERE number=? AND user_id=?", (v_num, v_uid))
            kb2 = InlineKeyboardBuilder()
            kb2.row(
                types.InlineKeyboardButton(text="🔴 Взлет", callback_data=f"s_{v_uid}_{v_num}"),
                types.InlineKeyboardButton(text="🚫 Бан", callback_data=f"d_{v_uid}_{v_num}"),
            )
            await render_action_message(call, f"✅ {v_num} — <b>Принят</b>\n💸 Начислено: {price}$", kb2.as_markup())

        elif act == "s":
            row = db.query("SELECT id, start_time, submit_type, price, credited_notified FROM sessions WHERE number=? AND user_id=? AND status='vstal' ORDER BY id DESC LIMIT 1", (v_num, v_uid), fetch="one")
            if not row:
                await call.answer("⚠️ Активная сессия уже закрыта", show_alert=False)
                return
            sid, start_time, submit_type, price, credited_notified = row
            now_dt = now_kz_naive()
            elapsed_sec = int((now_dt - parse_dt(start_time)).total_seconds())
            paid_flag = 1 if elapsed_sec >= 180 else 0
            db.query("UPDATE sessions SET status='slet', paid=?, end_time=? WHERE id=?", (paid_flag, now_dt.strftime("%Y-%m-%d %H:%M:%S"), sid))
            if paid_flag:
                amount = float(price or get_user_price(v_uid))
                payout_payload = f"credit uid:{v_uid} amount:{amount:.2f} num:{v_num}"
                payout_status_text = "ожидайте выплату"
                if is_instant_payout_enabled():
                    ok, msg, result = await crypto_bot_send_usdt(v_uid, amount)
                    if ok:
                        payout_payload = json.dumps(result or {}, ensure_ascii=False)
                        payout_status_text = "чек отправлен в бота"
                        await notify_payout_group(v_uid, v_num, amount, status="выплачено")
                    else:
                        payout_payload = f"pending:{msg}"
                existing_payout = db.query("SELECT 1 FROM payouts WHERE user_id=? AND number=? LIMIT 1", (v_uid, v_num), fetch="one")
                if not existing_payout:
                    db.query("INSERT INTO payouts(user_id, number, submit_type, amount, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)", (v_uid, v_num, submit_type or 'code', amount, payout_payload, now_kz_naive().strftime("%Y-%m-%d %H:%M:%S")))
                if not int(credited_notified or 0):
                    try:
                        await bot.send_message(v_uid, f"✅ Номер {v_num} засчитан (прошло больше 3 минут). Вам начислено {amount}$, {payout_status_text}.")
                    except Exception:
                        pass
            else:
                try:
                    await bot.send_message(v_uid, f"⚠️ Номер {v_num} не засчитан: слёт произошёл в течение 3 минут.")
                except Exception:
                    pass
            await render_action_message(call, f"🔴 {v_num} — <b>Слетел</b> ({'засчитан' if paid_flag else 'не засчитан'})")

        elif act == "d":
            sid = db.query("SELECT id FROM sessions WHERE number=? AND user_id=? AND status='vstal' ORDER BY id DESC LIMIT 1", (v_num, v_uid), fetch="one")
            if not sid:
                await call.answer("⚠️ Активная сессия уже закрыта", show_alert=False)
                return
            db.query("UPDATE sessions SET status='otvyaz', paid=0, end_time=? WHERE id=?", (now_kz_naive().strftime("%Y-%m-%d %H:%M:%S"), sid[0]))
            db.query("DELETE FROM payouts WHERE id=(SELECT id FROM payouts WHERE user_id=? AND number=? ORDER BY id DESC LIMIT 1)", (v_uid, v_num))
            await render_action_message(call, f"🚫 {v_num} — <b>Бан/Отвяз</b> (оплата снята)")
            try:
                await bot.send_message(v_uid, f"🚫 По номеру {v_num} бан на аккаунте: номер не засчитан.")
            except Exception:
                pass

        elif act == "e":
            db.query("UPDATE queue SET status='error' WHERE number=? AND user_id=? AND status='proc'", (v_num, v_uid))
            await render_action_message(call, f"⚠️ {v_num} — <b>Ошибка</b>")

        elif act == "m":
            await state.set_state(AdminForm.message_user)
            await state.update_data(target_user_id=v_uid, target_number=v_num)
            await call.message.answer("Введите сообщение пользователю по номеру:")

        elif act == "k":
            active_q = db.query("SELECT id FROM queue WHERE number=? AND user_id=? AND status='proc' ORDER BY id DESC LIMIT 1", (v_num, v_uid), fetch="one")
            if not active_q:
                await call.answer("⚠️ Номер уже не в обработке", show_alert=False)
                return
            db.query("UPDATE queue SET status='waiting', proc_by=NULL, code_sender_id=NULL, repeat_requested=0, qr_requested=0 WHERE id=?", (active_q[0],))
            await render_action_message(call, f"⏭ {v_num} — <b>был возвращен в очередь</b>")
            try:
                await bot.send_message(v_uid, f"⏭ Ваш номер {v_num} был возвращен в очередь.")
            except Exception:
                pass

        elif act == "n":
            active_q = db.query("SELECT id FROM queue WHERE number=? AND user_id=? AND status='proc' ORDER BY id DESC LIMIT 1", (v_num, v_uid), fetch="one")
            if not active_q:
                await call.answer("⚠️ Номер уже не в обработке", show_alert=False)
                return
            db.query("DELETE FROM queue WHERE id=?", (active_q[0],))
            await render_action_message(call, f"❌ {v_num} — <b>номер не встал, удалён из очереди</b>")
            try:
                await bot.send_message(v_uid, f"❌ Ваш номер {v_num} не встал. Сдайте заново.")
            except Exception:
                pass

    await call.answer()


@dp.message(CommandStart())
async def start(message: types.Message):
    db.query("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", (message.from_user.id, message.from_user.username))
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) > 1 and parts[1].startswith("ref_"):
        ref_id_raw = parts[1].replace("ref_", "", 1)
        if ref_id_raw.isdigit() and int(ref_id_raw) != message.from_user.id:
            db.query("INSERT OR IGNORE INTO referrals (inviter_id, invited_id) VALUES (?, ?)", (int(ref_id_raw), message.from_user.id))
    await message.answer_photo(
        photo=MENU_PHOTO,
        caption=get_menu_caption(await check_sub(message.from_user.id)),
        reply_markup=await get_main_menu_kb(message.from_user.id),
        parse_mode="HTML",
    )


@dp.message(Command("set"))
async def set_group(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return await message.answer("⛔ Нет доступа. Команда доступна только админам.")
    if message.chat.type == "private":
        return await message.answer("❌ Введите это в группе!")
    thread_id = get_thread_id(message)
    key = get_group_binding_key(message.chat.id, thread_id)
    gid = db.query("SELECT value FROM settings WHERE key=?", (key,), fetch="one")
    if gid:
        db.query("DELETE FROM settings WHERE key=?", (key,))
        await message.answer("🔓 Привязка снята для этого чата/топика.")
    else:
        title = message.chat.title or "Группа"
        suffix = f" • топик {thread_id}" if thread_id else " • общий чат"
        db.query("INSERT OR REPLACE INTO settings VALUES (?, ?)", (key, f"{title}{suffix}"))
        await message.answer("🔒 Привязка установлена для этого чата/топика.\nДля выдачи номера используйте `номер`.", parse_mode="Markdown")


@dp.message(Command("set_support"))
async def set_support_group(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return await message.answer("⛔ Нет доступа. Команда доступна только админам.")
    if message.chat.type == "private":
        return await message.answer("❌ Введите это в группе!")
    thread_id = get_thread_id(message)
    set_support_target(message.chat.id, thread_id, message.chat.title or "Support")
    if thread_id:
        await message.answer("✅ Канал техподдержки привязан к этому топику.")
    else:
        await message.answer("✅ Канал техподдержки привязан к этому чату.")


@dp.message(Command("set_payout"))
async def set_payout_group(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return await message.answer("⛔ Нет доступа. Команда доступна только админам.")
    if message.chat.type == "private":
        return await message.answer("❌ Введите это в группе!")
    thread_id = get_thread_id(message)
    set_payout_target(message.chat.id, thread_id)
    if thread_id:
        await message.answer("✅ Группа выплат привязана к этому топику.")
    else:
        await message.answer("✅ Группа выплат привязана к этому чату.")


@dp.message(Command("setmaxSMS"))
@dp.message(Command("setmaxQR"))
@dp.message(Command("setmaxMIX"))
async def set_topic_issue_mode(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    if message.chat.type == "private":
        return await message.answer("❌ Введите это в группе!")
    cmd = (message.text or "").split()[0].lower()
    mode = "SMS" if "sms" in cmd else ("QR" if "qr" in cmd else "MIX")
    tid = get_thread_id(message)
    set_topic_mode(message.chat.id, tid, mode)
    await message.answer(f"✅ Режим выдачи для этого чата/топика: {mode}")


@dp.message(Command("infoset"))
async def infoset_cmd(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.answer("Использование: /infoset <номер_топика_из_списка_привязок>")
    idx = int(parts[1])
    groups = get_linked_groups()
    if idx <= 0 or idx > len(groups):
        return await message.answer("0")
    gid, thread_id, title = groups[idx - 1]
    total_sessions = db.query("SELECT COUNT(*) FROM sessions WHERE group_id=?", (gid,), fetch="one")[0]
    vstal_sessions = db.query("SELECT COUNT(*) FROM sessions WHERE group_id=? AND status IN ('vstal','slet','otvyaz')", (gid,), fetch="one")[0]
    waiting_now = db.query("SELECT COUNT(*) FROM queue WHERE status='waiting'", fetch="one")[0]
    txt = (
        f"<b>ℹ️ InfoSet #{idx}</b>\n"
        f"Топик: <b>{title or f'gid:{gid} topic:{thread_id}'}</b>\n"
        f"Всего ушло номеров: <b>{total_sessions}</b>\n"
        f"Всего встало: <b>{vstal_sessions}</b>\n"
        f"Текущая очередь (общая): <b>{waiting_now}</b>"
    )
    await message.answer(txt, parse_mode="HTML")


@dp.message(Command("pay"))
async def pay_cmd(message: types.Message):
    parts = (message.text or "").split(maxsplit=1)
    amount = PAY_DEFAULT_AMOUNT
    if len(parts) > 1:
        try:
            amount = float(parts[1].strip().replace(",", "."))
        except Exception:
            return await message.answer("Использование: /pay <сумма>, например /pay 5")
    if amount <= 0:
        return await message.answer("Сумма должна быть больше 0.")

    ok, pay_url, msg, invoice_id = await crypto_bot_create_topup_invoice(amount)
    if not ok:
        kb = None
        if "1010" in msg:
            kb = types.InlineKeyboardMarkup(
                inline_keyboard=[[types.InlineKeyboardButton(text="Открыть @CryptoBot", url="https://t.me/CryptoBot")]]
            )
        return await message.answer(f"❌ Ошибка создания инвойса: {msg}", reply_markup=kb)

    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="💳 Оплатить", url=pay_url))
    if invoice_id:
        kb.row(types.InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"pay_check_{invoice_id}"))
    await message.answer(
        (
            f"💳 Инвойс создан на <b>{amount:.2f} USDT</b>.\n"
            f"Ссылка на оплату: {pay_url}\n"
            f"ID: <code>{invoice_id if invoice_id else '-'}</code>"
        ),
        parse_mode="HTML",
        reply_markup=kb.as_markup(),
    )

    if invoice_id:
        for _ in range(max(1, PAY_CHECK_ATTEMPTS)):
            await asyncio.sleep(max(1, PAY_CHECK_INTERVAL_SECONDS))
            ok_inv, inv_status, _inv_msg = await crypto_bot_check_invoice(invoice_id)
            if ok_inv and inv_status == "paid":
                await message.answer("✅ Оплата получена! Спасибо.")
                return
        await message.answer("⏳ Оплата пока не подтверждена. Нажмите кнопку 'Проверить оплату' позже.")


@dp.message(Command("set_crypto_token"))
async def set_crypto_token_cmd(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await message.answer("Использование: /set_crypto_token <token>")
    set_crypto_pay_token(parts[1].strip())
    await message.answer("✅ Общий Crypto Pay токен сохранен в settings (fallback для transfer/invoice).")


@dp.message(Command("set_crypto_transfer_token"))
async def set_crypto_transfer_token_cmd(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await message.answer("Использование: /set_crypto_transfer_token <token>")
    set_crypto_transfer_token(parts[1].strip())
    await message.answer("✅ Токен выплат сохранен в settings.")


@dp.message(Command("set_crypto_invoice_token"))
async def set_crypto_invoice_token_cmd(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await message.answer("Использование: /set_crypto_invoice_token <token>")
    set_crypto_invoice_token(parts[1].strip())
    await message.answer("✅ Токен инвойсов сохранен в settings.")


@dp.message(Command("clear_crypto_token"))
async def clear_crypto_token_cmd(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    clear_crypto_pay_token()
    await message.answer("✅ Общий Crypto Pay токен из settings удален. Теперь используются отдельные токены или ENV.")


@dp.message(Command("clear_crypto_transfer_token"))
async def clear_crypto_transfer_token_cmd(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    clear_crypto_transfer_token()
    await message.answer("✅ Токен выплат из settings удален.")


@dp.message(Command("clear_crypto_invoice_token"))
async def clear_crypto_invoice_token_cmd(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    clear_crypto_invoice_token()
    await message.answer("✅ Токен инвойсов из settings удален.")


@dp.message(Command("dbinfo"))
async def db_info(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    users_count = db.query("SELECT COUNT(*) FROM users", fetch="one")[0]
    await message.answer(f"🗄 <b>DB INFO</b>\nПуть: <code>{RESOLVED_DB_PATH}</code>\nПользователей: <b>{users_count}</b>", parse_mode="HTML")


@dp.message(Command("break"))
async def set_break(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    if message.chat.type == "private":
        return await message.answer("❌ Введите это в группе!")
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        return await message.answer("⚠️ Используйте: /break 30, /break 0 или /break 12:00 12:30")
    start = None
    end = None
    if len(parts) == 2 and re.fullmatch(r"-?\d+", parts[1]):
        minutes = int(parts[1])
        if minutes <= 0:
            db.query("DELETE FROM breaks WHERE group_id=?", (message.chat.id,))
            return await message.answer("🧹 Перерывы для группы очищены.")
        start = now_kz_naive()
        end = start + timedelta(minutes=minutes)
    elif len(parts) == 3:
        try:
            now = now_kz_naive()
            start = datetime.strptime(parts[1], "%H:%M").replace(year=now.year, month=now.month, day=now.day)
            end = datetime.strptime(parts[2], "%H:%M").replace(year=now.year, month=now.month, day=now.day)
        except ValueError:
            return await message.answer("⚠️ Формат времени: /break 12:00 12:30")
        if end <= start:
            return await message.answer("⚠️ Конец перерыва должен быть позже начала.")
    else:
        return await message.answer("⚠️ Используйте: /break 30, /break 0 или /break 12:00 12:30")

    start_db = start.strftime("%Y-%m-%d %H:%M:%S")
    end_db = end.strftime("%Y-%m-%d %H:%M:%S")
    exists = db.query("SELECT 1 FROM breaks WHERE group_id=? AND start_time=? AND end_time=?", (message.chat.id, start_db, end_db), fetch="one")
    if exists:
        return await message.answer(f"ℹ️ Такой перерыв уже есть ({start.strftime('%H:%M')}–{end.strftime('%H:%M')}).")
    db.query("INSERT INTO breaks (group_id, start_time, end_time) VALUES (?, ?, ?)", (message.chat.id, start_db, end_db))
    await message.answer(f"🍽 Перерыв добавлен. ({start.strftime('%H:%M')}–{end.strftime('%H:%M')})")


@dp.message(AdminForm.set_price)
async def set_price_handler(message: types.Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    raw = (message.text or "").strip().replace(",", ".")
    try:
        value = float(raw)
    except Exception:
        return await message.answer("❌ Введите число, например: 6")
    if value <= 0:
        return await message.answer("❌ Цена должна быть больше нуля")
    db.query("INSERT OR REPLACE INTO settings(key,value) VALUES('price_per_number',?)", (str(value),))
    await state.clear()
    await message.answer(f"✅ Новый тариф: {value}$")


@dp.message(AdminForm.priority_add)
async def admin_priority_add(message: types.Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    target_id = resolve_user_id(message.text or "")
    if not target_id:
        return await message.answer("❌ Пользователь не найден. Введите @username или ID.")
    db.query("UPDATE users SET priority=1 WHERE user_id=?", (target_id,))
    await state.clear()
    await message.answer("✅ Приоритет выдан.")


@dp.message(AdminForm.priority_remove)
async def admin_priority_remove(message: types.Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    target_id = resolve_user_id(message.text or "")
    if not target_id:
        return await message.answer("❌ Пользователь не найден. Введите @username или ID.")
    db.query("UPDATE users SET priority=0 WHERE user_id=?", (target_id,))
    await state.clear()
    await message.answer("✅ Приоритет снят.")


@dp.message(AdminForm.admin_add)
async def admin_add_handler(message: types.Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    target_id = resolve_user_id(message.text or "")
    if not target_id:
        return await message.answer("❌ Пользователь не найден. Введите @username или ID.")
    db.query("UPDATE users SET is_admin=1 WHERE user_id=?", (target_id,))
    await state.clear()
    await message.answer("✅ Администратор добавлен.")


@dp.message(AdminForm.admin_remove)
async def admin_remove_handler(message: types.Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    target_id = resolve_user_id(message.text or "")
    if not target_id:
        return await message.answer("❌ Пользователь не найден. Введите @username или ID.")
    db.query("UPDATE users SET is_admin=0 WHERE user_id=?", (target_id,))
    await state.clear()
    await message.answer("✅ Администратор удален.")


@dp.message(regexp=r"^\s*\d{3,10}\s*$")
async def sms_code_catcher(message: types.Message):
    # Код от пользователя принимается только ответом на запрос SMS и только один раз.
    code = (message.text or "").strip()
    q = db.query(
        "SELECT id, number, proc_by FROM queue WHERE user_id=? AND status='proc' ORDER BY id DESC LIMIT 1",
        (message.from_user.id,),
        fetch="one",
    )
    if not q:
        return
    _qid, number, _proc = q

    req_row = db.query("SELECT 1 FROM settings WHERE key=?", (sms_request_key(message.from_user.id, number),), fetch="one")
    if not req_row:
        return

    if not message.reply_to_message:
        return await message.answer(f"⚠️ Отправьте код ТОЛЬКО ответом на запрос по номеру {number}.")

    reply_text = (message.reply_to_message.text or message.reply_to_message.caption or "")
    if number not in reply_text:
        return await message.answer(f"⚠️ Ответьте именно на сообщение с запросом кода по номеру {number}.")

    done_row = db.query("SELECT 1 FROM settings WHERE key=?", (sms_done_key(message.from_user.id, number),), fetch="one")
    if done_row:
        return await message.answer("⚠️ Код уже принят. Повторно отправлять не нужно.")

    db.query("INSERT OR REPLACE INTO settings(key, value) VALUES(?, '1')", (sms_done_key(message.from_user.id, number),))

    linked = get_linked_groups()
    if linked:
        gid, thread_id, _title = linked[0]
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text="✅ Встал", callback_data=f"v_{message.from_user.id}_{number}"), types.InlineKeyboardButton(text="🔁 Повтор входа", callback_data=f"k_{message.from_user.id}_{number}"), types.InlineKeyboardButton(text="❌ Не встал", callback_data=f"e_{message.from_user.id}_{number}"))
        txt = f"📩 Код по номеру <code>{number}</code>: <b>{code}</b>"
        try:
            if thread_id:
                await bot.send_message(gid, txt, parse_mode="HTML", message_thread_id=thread_id, reply_markup=kb.as_markup())
            else:
                await bot.send_message(gid, txt, parse_mode="HTML", reply_markup=kb.as_markup())
        except Exception:
            pass
    await message.answer(f"✅ Код по номеру {number} принят. Номер вошёл в работу.")


@dp.message(SupportForm.request)
async def support_request_handler(message: types.Message, state: FSMContext):
    target = get_support_target()
    if not target:
        await state.clear()
        return await message.answer("⚠️ Техподдержка пока не настроена администратором.")

    chat_id, thread_id = target
    text = (message.text or "").strip()
    if not text:
        return await message.answer("⚠️ Отправьте текстом ваш запрос.")

    user = message.from_user
    username = f"@{user.username}" if user.username else "без username"
    payload = (
        "<b>🆘 Новый запрос в техподдержку</b>\n"
        f"Пользователь: {username}\n"
        f"ID: <code>{user.id}</code>\n\n"
        f"<b>Текст:</b>\n{text}"
    )
    try:
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text="✉️ Ответить", callback_data=f"support_reply_{user.id}"))
        if thread_id:
            await bot.send_message(chat_id, payload, parse_mode="HTML", message_thread_id=thread_id, reply_markup=kb.as_markup())
        else:
            await bot.send_message(chat_id, payload, parse_mode="HTML", reply_markup=kb.as_markup())
        await message.answer("✅ Запрос отправлен в техподдержку.")
    except Exception:
        await message.answer("⚠️ Не удалось отправить запрос. Попробуйте позже.")
    await state.clear()


@dp.message()
async def support_reply_fallback(message: types.Message):
    if message.chat.type != "private" or not is_user_admin(message.from_user.id):
        return
    row = db.query("SELECT value FROM settings WHERE key=?", (f"support_reply_target:{message.from_user.id}",), fetch="one")
    if not row or not str(row[0]).isdigit():
        return
    target_id = int(row[0])
    try:
        await bot.send_message(target_id, f"💬 Ответ техподдержки: {message.text}")
        await message.answer("✅ Ответ отправлен пользователю.")
    except Exception:
        await message.answer("⚠️ Не удалось отправить ответ пользователю.")
    db.query("DELETE FROM settings WHERE key=?", (f"support_reply_target:{message.from_user.id}",))


@dp.message(AdminForm.message_user)
async def admin_message_user(message: types.Message, state: FSMContext):
    data = await state.get_data()
    target_id = data.get("target_user_id")
    number = data.get("target_number")
    if not target_id:
        row = db.query("SELECT value FROM settings WHERE key=?", (f"support_reply_target:{message.from_user.id}",), fetch="one")
        if row and str(row[0]).isdigit():
            target_id = int(row[0])
            number = "support"
    if not target_id:
        await state.clear()
        return
    try:
        await bot.send_message(target_id, f"💬 Ответ техподдержки: {message.text}" if number == "support" else f"💬 Сообщение по номеру {number}: {message.text}")
        await message.answer("✅ Сообщение отправлено.")
    except Exception:
        await message.answer("⚠️ Не удалось отправить сообщение.")
    db.query("DELETE FROM settings WHERE key=?", (f"support_reply_target:{message.from_user.id}",))
    await state.clear()


@dp.message(AdminForm.direct_message)
async def admin_direct_message(message: types.Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        await state.clear()
        return
    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=1)
    if len(parts) < 2:
        return await message.answer("⚠️ Формат: @username текст или ID текст")
    target = resolve_user_id(parts[0])
    if not target:
        return await message.answer("❌ Пользователь не найден")
    try:
        await bot.send_message(target, f"💬 Сообщение от администратора:\n{parts[1]}")
        await message.answer("✅ Сообщение отправлено")
    except Exception:
        await message.answer("⚠️ Не удалось отправить сообщение")
    await state.clear()


@dp.message(AdminForm.app_topup_amount)
async def admin_app_topup_invoice(message: types.Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        await state.clear()
        return
    raw = (message.text or "").strip().replace(",", ".")
    try:
        amount = float(raw)
    except Exception:
        return await message.answer("❌ Введите сумму числом")
    if amount <= 0:
        return await message.answer("❌ Сумма должна быть больше 0")

    ok, pay_url, msg, invoice_id = await crypto_bot_create_topup_invoice(amount)
    if not ok:
        await state.clear()
        return await message.answer(f"❌ Не удалось создать инвойс: {msg}\nПополните кошелёк вручную в @CryptoBot (Wallet → Deposit), затем нажмите '🔄 Обновить баланс Crypto Bot'.")

    if invoice_id:
        db.query("INSERT OR REPLACE INTO settings(key,value) VALUES('last_topup_invoice_id',?)", (str(invoice_id),))

    await message.answer(
        "✅ Инвойс создан. Оплатите и затем нажмите '🔄 Обновить баланс Crypto Bot' в меню выплат.\n"
        f"ID инвойса: {invoice_id if invoice_id else '-'}\n"
        f"Ссылка: {pay_url}"
    )
    await state.clear()


@dp.message(AdminForm.payout_amount)
async def admin_user_inline_payout(message: types.Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    target_id = data.get("pay_target_id")
    payout_mode = str(data.get("payout_mode") or "transfer").strip().lower()
    raw = (message.text or "").strip()
    if not target_id:
        parts = raw.split()
        if len(parts) != 2 or (not parts[0].isdigit()):
            return await message.answer("❌ Формат: ID сумма")
        target_id = int(parts[0])
        amount_raw = parts[1]
    else:
        amount_raw = raw
    try:
        amount = float(amount_raw.replace(",", ".").strip())
    except Exception:
        return await message.answer("❌ Введите сумму числом")
    if amount <= 0:
        return await message.answer("❌ Сумма должна быть больше 0")

    if payout_mode == "check":
        ok, msg, result = await crypto_bot_create_check(int(target_id), amount, description="Admin manual check payout")
    else:
        ok, msg, result = await crypto_bot_send_usdt(int(target_id), amount)
    created_at = now_kz_naive().strftime("%Y-%m-%d %H:%M:%S")
    if not ok:
        db.query(
            "INSERT INTO payouts(user_id, number, submit_type, amount, payload, created_at) VALUES (?, 'manual', 'code', ?, ?, ?)",
            (int(target_id), float(amount), f"manual_pending:{msg}", created_at),
        )
        method = "Create Check" if payout_mode == "check" else "Transfer"
        await message.answer(f"⚠️ Выплата ({method}) не отправлена: {msg}\nЗаявка сохранена в отчёт как manual_pending.")
        await state.clear()
        return

    payload = json.dumps(result or {}, ensure_ascii=False)
    db.query(
        "INSERT INTO payouts(user_id, number, submit_type, amount, payload, created_at) VALUES (?, 'manual', 'code', ?, ?, ?)",
        (int(target_id), float(amount), payload, created_at),
    )
    await notify_payout_group(int(target_id), "manual", float(amount), status="выплачено")
    method = "Create Check" if payout_mode == "check" else "Transfer"
    await message.answer(f"✅ Выплата отправлена через {method}: ID {target_id}, сумма {amount}$")
    await state.clear()


@dp.message(AdminForm.broadcast)
async def admin_broadcast(message: types.Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    users = db.query("SELECT user_id FROM users")
    sent = 0
    for (target_uid,) in users:
        try:
            await bot.send_message(target_uid, message.text)
            sent += 1
        except Exception:
            pass
    await state.clear()
    await message.answer(f"✅ Рассылка завершена. Отправлено: {sent}")


@dp.message(AdminForm.queue_remove_user)
async def admin_queue_remove_user_handler(message: types.Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    data = await state.get_data()
    if data.get("search_mode") == "user":
        target_id = resolve_user_id(raw)
        if not target_id:
            return await message.answer("❌ Пользователь не найден")
        row = db.query("SELECT username, banned FROM users WHERE user_id=?", (target_id,), fetch="one")
        username, banned = row if row else (None, 0)
        label = f"@{username}" if username else f"ID:{target_id}"
        kb = InlineKeyboardBuilder()
        kb.row(
            types.InlineKeyboardButton(text="🚫 Бан", callback_data=f"adm_user_ban_{target_id}_1"),
            types.InlineKeyboardButton(text="✅ Разбан", callback_data=f"adm_user_unban_{target_id}_1"),
        )
        await state.clear()
        return await message.answer(f"Найден: {label} | Статус: {'🚫' if banned else '✅'}", reply_markup=kb.as_markup())

    if not raw.isdigit():
        return await message.answer("❌ Введите числовой user_id")
    db.query("DELETE FROM queue WHERE user_id=?", (int(raw),))
    await state.clear()
    await message.answer("✅ Очередь пользователя удалена")


@dp.message(AdminForm.breaks_group)
async def admin_breaks_group(message: types.Message, state: FSMContext):
    data = await state.get_data()
    gid = data.get("break_group_id")
    if not gid:
        await state.clear()
        return
    raw = (message.text or "").strip()
    if re.fullmatch(r"-?\d+", raw) and int(raw) <= 0:
        db.query("DELETE FROM breaks WHERE group_id=?", (gid,))
        await state.clear()
        return await message.answer("🧹 Перерывы для группы очищены.")
    entries = parse_break_lines(message.text or "")
    if not entries:
        return await message.answer("❌ Формат: 11:00 - 12:00 (каждая строка) или 0/минус для очистки.")
    now = now_kz_naive()
    added = 0
    skipped = 0
    for start_time, end_time in entries:
        start_dt = start_time.replace(year=now.year, month=now.month, day=now.day)
        end_dt = end_time.replace(year=now.year, month=now.month, day=now.day)
        if end_dt <= start_dt:
            skipped += 1
            continue
        start_db = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_db = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        exists = db.query("SELECT 1 FROM breaks WHERE group_id=? AND start_time=? AND end_time=?", (gid, start_db, end_db), fetch="one")
        if exists:
            skipped += 1
            continue
        db.query("INSERT INTO breaks (group_id, start_time, end_time) VALUES (?, ?, ?)", (gid, start_db, end_db))
        added += 1
    await state.clear()
    await message.answer(f"✅ Перерывы обработаны. Добавлено: {added}, пропущено: {skipped}.")


@dp.message(Form.num)
async def num_input(message: types.Message, state: FSMContext):
    if is_user_banned(message.from_user.id):
        return await message.answer("🚫 Вы забанены и не можете сдавать номера.")
    data = await state.get_data()
    submit_type = data.get("submit_type", "code")
    numbers = unique_numbers(parse_numbers(message.text or ""))
    if not numbers:
        return await message.answer("❌ Ошибка! Введите 11 цифр в каждой строке или через пробел.")

    user_priority = 1 if is_user_priority(message.from_user.id) else 0
    now = now_kz_naive().strftime("%Y-%m-%d %H:%M:%S")
    for num in numbers:
        db.query("INSERT INTO queue (user_id, number, submit_type, status, created_at) VALUES (?, ?, ?, 'waiting', ?)", (message.from_user.id, num, submit_type, now))
        db.query("INSERT INTO submissions (user_id, number, submit_type, created_at) VALUES (?, ?, ?, ?)", (message.from_user.id, num, submit_type, now))

    last_id = db.query("SELECT MAX(id) FROM queue WHERE user_id=?", (message.from_user.id,), fetch="one")[0]
    q_pos = db.query(
        "SELECT COUNT(*) FROM queue q JOIN users u ON q.user_id=u.user_id WHERE q.status='waiting' AND (u.priority > ? OR (u.priority = ? AND q.id <= ?))",
        (user_priority, user_priority, last_id),
        fetch="one",
    )[0]
    total_waiting = db.query("SELECT COUNT(*) FROM queue WHERE status='waiting'", fetch="one")[0]
    await state.clear()
    t = "QR" if submit_type == "qr" else "Код"
    await message.answer(
        f"✅ Номер(а) добавлены: <b>{len(numbers)}</b> [{t}]\n"
        f"📋 Ваша позиция в очереди: <b>{q_pos}</b>\n"
        f"📋 Всего в очереди: <b>{total_waiting}</b>",
        parse_mode="HTML",
        reply_markup=back_kb("u_menu"),
    )


@dp.message(Command(commands=["submit", "sumbit"]))
async def submit_cmd(message: types.Message, state: FSMContext):
    if is_user_banned(message.from_user.id):
        return await message.answer("🚫 Вы забанены и не можете сдавать номера.")
    if not is_work_enabled():
        return await message.answer("⛔ Сейчас не работаем. Сдача номеров временно отключена.")
    await state.set_state(Form.num)
    await state.update_data(submit_type="code")
    await message.answer("📞 Введите номера (каждый с новой строки или через пробел):")


@dp.message(Command("queue"))
async def queue_cmd(message: types.Message):
    caption, _ = build_user_queue_view(message.from_user.id)
    await message.answer(caption, parse_mode="HTML")


@dp.message(Command("archive"))
async def archive_cmd(message: types.Message):
    uid = message.from_user.id
    balance = get_user_balance(uid)
    rows = db.query(
        "SELECT number, submit_type, status, price, paid, start_time "
        "FROM sessions WHERE user_id=? AND (status!='vstal' OR credited_notified=1) ORDER BY id DESC LIMIT 300",
        (uid,),
    )
    status_map = {"vstal": "встал", "slet": "слетел", "otvyaz": "бан", "error": "ошибка", "detached": "отвяз"}
    lines = [f"<b>💰 Баланс:</b> <b>{balance}$</b>", "<b>📱 Архив номеров:</b>", "Формат: Номер | Тип | Сумма | Дата/время", ""]
    if not rows:
        lines.append("Пока пусто")
    else:
        for number, submit_type, status, price, paid, dt in rows:
            st = "QR" if submit_type == "qr" else "SMS"
            stat = status_map.get(status or "", status or "-")
            amount = float(price or 0) if int(paid or 0) else 0.0
            lines.append(f"• <code>{number}</code> | {st} | {amount}$ | {dt} ({stat})")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("send"))
async def send_cmd(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    await message.answer("ℹ️ Команда /send отключена. Используйте кнопку '💸 Выплатить' в карточке пользователя.")


@dp.message(regexp=r"(?i).*(^|\s)номер([.!?]|\s|$).*")
async def get_num(message: types.Message):
    thread_id = get_thread_id(message)
    if not group_scope_allows(message.chat.id, thread_id):
        return await message.answer("⛔ Этот чат/топик не привязан. Используйте /set здесь.")
    result = await issue_next_number(message.chat.id, thread_id, message.from_user.id)
    if not result:
        return await message.answer("Очередь пуста.")
    user_id, number, submit_type = result
    st = "QR" if submit_type == "qr" else "SMS"
    next_kb = InlineKeyboardBuilder()
    if submit_type == "qr":
        next_kb.row(
            types.InlineKeyboardButton(text="🧾 Запросить QR", callback_data=f"qr_{message.chat.id}_{thread_id or 0}_{user_id}_{number}"),
            types.InlineKeyboardButton(text="⏭ Скип", callback_data=f"k_{user_id}_{number}"),
        )
        next_kb.row(types.InlineKeyboardButton(text="❌ Номер не встал", callback_data=f"n_{user_id}_{number}"))
    else:
        next_kb.row(
            types.InlineKeyboardButton(text="🔔 Запросить SMS", callback_data=f"rr_{message.chat.id}_{thread_id or 0}_{user_id}_{number}"),
            types.InlineKeyboardButton(text="⏭ Скип", callback_data=f"k_{user_id}_{number}"),
        )
        next_kb.row(types.InlineKeyboardButton(text="❌ Номер не встал", callback_data=f"n_{user_id}_{number}"))
    await message.answer(
        f"Метод: <b>{st}</b>\nНомер: <code>{number}</code>\nОператор: <code>{message.from_user.id}</code>\n\nОтправьте фото/код в ответ на это сообщение.",
        parse_mode="HTML",
        reply_markup=next_kb.as_markup(),
    )
    try:
        office_label = get_office_label_for_group(message.chat.id)
        total_waiting = db.query("SELECT COUNT(*) FROM queue WHERE status='waiting'", fetch="one")[0]
        await bot.send_message(user_id, f"📨 Ваш номер {number} взяли. {office_label}. Ожидайте кода.\n📋 Текущая общая очередь: {total_waiting}")
    except Exception:
        pass


@dp.message(Command("num"))
async def get_num_privacy_fallback(message: types.Message):
    # Если в группе включен Privacy Mode, обычный текст "номер" может не доходить до бота.
    return await get_num(message)


async def _process_code_media(message: types.Message):
    thread_id = get_thread_id(message)

    # В ЛС принимаем QR только по явному запросу и только ответом на сообщение.
    if message.chat.type == "private":
        media_id = message.photo[-1].file_id if message.photo else (message.document.file_id if message.document else None)
        if not media_id:
            return

        q = db.query(
            "SELECT id, number FROM queue WHERE user_id=? AND status='proc' ORDER BY id DESC LIMIT 1",
            (message.from_user.id,),
            fetch="one",
        )
        if not q:
            return
        _qid, number = q

        req_row = db.query("SELECT 1 FROM settings WHERE key=?", (qr_request_key(message.from_user.id, number),), fetch="one")
        if not req_row:
            return

        if not message.reply_to_message:
            return await message.answer(f"⚠️ Отправьте QR ТОЛЬКО ответом на запрос по номеру {number}.")

        reply_text = (message.reply_to_message.text or message.reply_to_message.caption or "")
        if number not in reply_text:
            return await message.answer(f"⚠️ Ответьте именно на сообщение с запросом QR по номеру {number}.")

        done_row = db.query("SELECT 1 FROM settings WHERE key=?", (qr_done_key(message.from_user.id, number),), fetch="one")
        if done_row:
            return await message.answer("⚠️ QR уже принят. Повторно отправлять не нужно.")

        db.query("INSERT OR REPLACE INTO settings(key, value) VALUES(?, '1')", (qr_done_key(message.from_user.id, number),))

        linked = get_linked_groups()
        if linked:
            gid, req_thread_id, _title = linked[0]
            kb = InlineKeyboardBuilder()
            kb.row(
                types.InlineKeyboardButton(text="✅ Встал", callback_data=f"v_{message.from_user.id}_{number}"),
                types.InlineKeyboardButton(text="🔁 Повтор входа", callback_data=f"k_{message.from_user.id}_{number}"),
                types.InlineKeyboardButton(text="❌ Не встал", callback_data=f"e_{message.from_user.id}_{number}"),
            )
            txt = f"📩 QR по номеру <code>{number}</code>"
            try:
                if req_thread_id:
                    await bot.send_photo(gid, media_id, caption=txt, parse_mode="HTML", message_thread_id=req_thread_id, reply_markup=kb.as_markup())
                else:
                    await bot.send_photo(gid, media_id, caption=txt, parse_mode="HTML", reply_markup=kb.as_markup())
            except Exception:
                pass

        return await message.answer(f"✅ QR по номеру {number} принят и отправлен оператору.")

    if not group_scope_allows(message.chat.id, thread_id):
        return

    num = None
    worker_id = None
    if message.reply_to_message:
        source_text = message.reply_to_message.text or message.reply_to_message.caption or ""
        match = re.search(r"\+\d{10,15}", source_text)
        if match:
            num = match.group()
            worker = db.query("SELECT user_id FROM queue WHERE number=? AND status='proc' AND proc_by=? ORDER BY id DESC LIMIT 1", (num, message.from_user.id), fetch="one")
            if not worker:
                worker = db.query("SELECT user_id FROM queue WHERE number=? AND status='proc' ORDER BY id DESC LIMIT 1", (num,), fetch="one")
            if worker:
                worker_id = worker[0]

    if not num or not worker_id:
        fallback = db.query("SELECT number, user_id FROM queue WHERE status='proc' AND proc_by=? ORDER BY id DESC LIMIT 1", (message.from_user.id,), fetch="one")
        if not fallback:
            fallback = db.query("SELECT number, user_id FROM queue WHERE status='proc' ORDER BY id DESC LIMIT 1", fetch="one")
        if not fallback:
            return await message.answer("⚠️ Номер не найден в очереди.")
        num, worker_id = fallback

    db.query("UPDATE queue SET code_sender_id=? WHERE number=? AND user_id=? AND status='proc'", (message.from_user.id, num, worker_id))

    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="✅ Встал", callback_data=f"v_{worker_id}_{num}"), types.InlineKeyboardButton(text="⏭ Скип", callback_data=f"k_{worker_id}_{num}"))
    kb.row(types.InlineKeyboardButton(text="⚠️ Ошибка", callback_data=f"e_{worker_id}_{num}"), types.InlineKeyboardButton(text="💬 Сообщение", callback_data=f"m_{worker_id}_{num}"))

    media_id = message.photo[-1].file_id if message.photo else (message.document.file_id if message.document else None)
    if not media_id:
        return await message.answer("⚠️ Не удалось обработать файл.")

    await message.answer_photo(media_id, caption=f"⚙️ Работа: {num}", reply_markup=kb.as_markup())

    try:
        worker_thread_id = message.message_thread_id if message.is_topic_message else 0
        req_kb = InlineKeyboardBuilder()
        req_kb.row(
            types.InlineKeyboardButton(text="🔁 Запросить повторно", callback_data=f"rr_{message.chat.id}_{worker_thread_id}_{worker_id}_{num}"),
            types.InlineKeyboardButton(text="🧾 Запросить QR", callback_data=f"qr_{message.chat.id}_{worker_thread_id}_{worker_id}_{num}"),
        )
        await bot.send_photo(worker_id, media_id, caption=f"📩 По вашему номеру {num} пришел код.", reply_markup=req_kb.as_markup())
    except Exception:
        await message.answer(f"⚠️ Не удалось отправить код владельцу номера {num} в ЛС.")


@dp.message(content_types=["photo"])
async def handle_photo(message: types.Message):
    await _process_code_media(message)


@dp.message(content_types=["document"])
async def handle_document_image(message: types.Message):
    mime = (message.document.mime_type or "") if message.document else ""
    if not mime.startswith("image/"):
        return
    await _process_code_media(message)


async def hold_checker():
    while True:
        await asyncio.sleep(60)


async def credited_checker():
    while True:
        await asyncio.sleep(30)
        threshold = (now_kz_naive() - timedelta(minutes=3)).strftime("%Y-%m-%d %H:%M:%S")
        rows = db.query(
            "SELECT id, user_id, number, submit_type, price FROM sessions WHERE status='vstal' AND credited_notified=0 AND start_time IS NOT NULL AND start_time <= ?",
            (threshold,),
        )
        for sid, user_id, number, submit_type, price in rows:
            amount = float(price or get_user_price(user_id))
            payout_payload = "auto_counted_after_3m"
            payout_status_text = "ожидайте выплату"
            mark_notified = True
            if is_instant_payout_enabled():
                ok, msg, result = await crypto_bot_send_usdt(user_id, amount)
                if ok:
                    payout_payload = json.dumps(result or {}, ensure_ascii=False)
                    payout_status_text = "чек отправлен в бота"
                    await notify_payout_group(user_id, number, amount, status="выплачено")
                else:
                    payout_payload = f"pending:{msg}"
                    if not is_non_retryable_payout_error(msg):
                        # Временная ошибка: оставляем credited_notified=0, чтобы автопроверка повторила выплату.
                        mark_notified = False

            existing_payout = db.query("SELECT 1 FROM payouts WHERE user_id=? AND number=? LIMIT 1", (user_id, number), fetch="one")
            if not existing_payout:
                db.query(
                    "INSERT INTO payouts(user_id, number, submit_type, amount, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (user_id, number, submit_type or 'code', amount, payout_payload, now_kz_naive().strftime("%Y-%m-%d %H:%M:%S")),
                )
            else:
                db.query(
                    "UPDATE payouts SET payload=?, amount=?, submit_type=? WHERE id=(SELECT id FROM payouts WHERE user_id=? AND number=? ORDER BY id DESC LIMIT 1)",
                    (payout_payload, amount, submit_type or 'code', user_id, number),
                )

            db.query("UPDATE sessions SET paid=1, credited_notified=? WHERE id=?", (1 if mark_notified else 0, sid))
            if mark_notified:
                try:
                    await bot.send_message(user_id, f"✅ Номер {number} засчитан (прошло 3 минуты). Начислено {amount}$, {payout_status_text}.")
                except Exception:
                    pass


async def nightly_cleanup():
    while True:
        now = datetime.now(KZ_TZ)
        next_cleanup = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if next_cleanup <= now:
            next_cleanup += timedelta(days=1)
        await asyncio.sleep((next_cleanup - now).total_seconds())
        try:
            cleanup_paid_reports()
            cleanup_archives()
        except Exception:
            pass


async def main():
    asyncio.create_task(hold_checker())
    asyncio.create_task(credited_checker())
    asyncio.create_task(nightly_cleanup())
    if IS_AIOGRAM_V3:
        await dp.start_polling(bot)
    else:
        executor.start_polling(dp, skip_updates=True)


if __name__ == "__main__":
    if IS_AIOGRAM_V3:
        asyncio.run(main())
    else:
        loop = asyncio.get_event_loop()
        loop.create_task(hold_checker())
        loop.create_task(credited_checker())
        loop.create_task(nightly_cleanup())
        executor.start_polling(dp, skip_updates=True)
