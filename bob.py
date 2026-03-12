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

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =================================================================
# [ КОНФИГУРАЦИЯ ]
# =================================================================
TOKEN = os.getenv("BOT_TOKEN", "8725972843:AAF-cEqRMV4oCHekRB3IP_klHRDuE_sYOt0")
ADMIN_ID = int(os.getenv("ADMIN_ID", "8203556349"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003837981813"))
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/carvenmax")
S_URL = os.getenv("S_URL", "https://t.me/carvenwork")
SUPPORT_URL = os.getenv("SUPPORT_URL", S_URL)
PRICE_PER_NUMBER = float(os.getenv("PRICE_PER_NUMBER", "4"))
QUEUE_TTL_HOURS = int(os.getenv("QUEUE_TTL_HOURS", "8"))
PRODUCERS_PAGE_SIZE = int(os.getenv("PRODUCERS_PAGE_SIZE", "10"))
REPORTS_PAGE_SIZE = int(os.getenv("REPORTS_PAGE_SIZE", "12"))
DB_NAME = os.getenv("DB_NAME", "titan_v40_final.db")
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / DB_NAME
MENU_PHOTO = os.getenv("MENU_PHOTO", "https://imgur.gg/f/8Os1TB7")
KZ_TZ = ZoneInfo("Europe/Moscow")
REPORT_TIME_SHIFT_HOURS = int(os.getenv("REPORT_TIME_SHIFT_HOURS", "5"))
PAY_DEFAULT_AMOUNT = float(os.getenv("PAY_DEFAULT_AMOUNT", "5"))
PAY_CHECK_ATTEMPTS = int(os.getenv("PAY_CHECK_ATTEMPTS", "10"))
PAY_CHECK_INTERVAL_SECONDS = int(os.getenv("PAY_CHECK_INTERVAL_SECONDS", "10"))

def _get_token_from_db(key: str) -> str:
    row = db.query(f"SELECT value FROM settings WHERE key='{key}'", fetch="one") if 'db' in globals() else None
    return (row[0] if row and row[0] else "").strip()

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

warnings.filterwarnings(
    "ignore",
    message=r"You may be able to resolve this warning by setting `model_config\['protected_namespaces'\] = \(\)`.*",
)

logging.basicConfig(level=logging.INFO)
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

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

    def get_price(self, user_id: int | None = None) -> float:
        row = self.query("SELECT value FROM settings WHERE key='price_per_number'", fetch="one")
        try:
            base = float(row[0]) if row else PRICE_PER_NUMBER
        except Exception:
            base = PRICE_PER_NUMBER
        if user_id and is_user_priority(user_id):
            return PRIORITY_PRICE_PER_NUMBER
        return base

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
    text = text.replace("\n", " ")
    raw = re.findall(r'\d+', text)

    numbers = []

    for n in raw:
        if len(n) == 11:
            if n.startswith("8"):
                n = "7" + n[1:]
            numbers.append(n)

        elif len(n) == 10:
            numbers.append("7" + n)

    return list(set(numbers))

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

def fmt_money(value: float | int) -> str:
    try:
        amount = float(value)
    except Exception:
        amount = 0.0
    if amount.is_integer():
        return str(int(amount))
    return f"{amount:.2f}".rstrip("0").rstrip(".")

def get_menu_caption(is_subscribed: bool) -> str:
    if not is_subscribed:
        return "⚠️ Подпишись на канал!"
    price = get_user_price(None)
    return (
        "\n"
        f"📌 Ставка за номер: {fmt_money(price)}$\n"
        "💳 Оплата момент!\n\n"
        "<b>🏠 Главное меню:</b>"
    )

async def send_main_menu(chat_id: int, user_id: int):
    await bot.send_photo(
        chat_id,
        photo=MENU_PHOTO,
        caption=get_menu_caption(await check_sub(user_id)),
        reply_markup=await get_main_menu_kb(user_id),
        parse_mode="HTML",
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
    target = (ADMIN_ID, 0)
    row = db.query("SELECT username FROM users WHERE user_id=?", (user_id,), fetch="one")
    masked = mask_username(row[0] if row else None, user_id)
    text = (
        "<b>💸 Выплата</b>\n"
        f"Пользователь: <code>{masked}</code>\n"
        f"ID: <code>{user_id}</code>\n"
        f"Номер: <code>{number}</code>\n"
        f"Сумма: <b>{fmt_money(amount)}$</b>\n"
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
        return float(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0

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
    return

def clear_all_runtime_data():
    db.query("DELETE FROM queue")
    db.query("DELETE FROM sessions")
    db.query("DELETE FROM payouts")

def cleanup_archives():
    # Очищаем именно архивные записи в 00:00 МСК.
    db.query("DELETE FROM sessions WHERE status!='vstal' OR credited_notified=1")

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
    total_waiting = len(rows)
    kb = InlineKeyboardBuilder()
    lines = ["<b>📋 Общая очередь в боте <b>{total_waiting}</b>", ""]
    if not rows:
        lines.append("Номеров в очереди нет.")
    else:
        for q_pos, (q_id, number, submit_type) in enumerate(rows, start=1):
            t = "QR" if submit_type == "qr" else "Код"
            lines.append(f"• <code>{number}</code> [{t}] — очередь: {q_pos}")
            kb.row(types.InlineKeyboardButton(text=f"{number} • {t} • {q_pos}", callback_data=f"u_q_del_{q_id}"))
    kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="u_back"))
    return "\n".join(lines), kb.as_markup()

def build_admin_reports_view(page: int):
    today_str = now_kz_naive().strftime("%Y-%m-%d")
    tz_shift = f"{REPORT_TIME_SHIFT_HOURS:+d} hours"
    source_rows = db.query(
        "SELECT u.username, s.user_id, s.number, s.price, s.start_time "
        "FROM sessions s LEFT JOIN users u ON s.user_id=u.user_id "
        "WHERE s.paid=1 AND s.credited_notified=1 AND date(datetime(s.start_time, ?))=? ORDER BY s.id DESC",
        (tz_shift, today_str),
    )

    total_today = sum((row[3] or 0) for row in source_rows)
    total_rows = len(source_rows)
    max_page = max(1, (total_rows + REPORTS_PAGE_SIZE - 1) // REPORTS_PAGE_SIZE)
    page = max(1, min(page, max_page))
    page_rows = source_rows[(page - 1) * REPORTS_PAGE_SIZE : page * REPORTS_PAGE_SIZE]

    lines = [
        f"<b>🧾 Отчёт за сегодня • страница {page}/{max_page}</b>",
        f"<b>💵 За сегодня:</b> {fmt_money(total_today)}$",
        "",
        "Пользователь | ID | Номер | Сумма",
        "",
    ]
    total = 0
    if not page_rows:
        lines.append("Пока пусто")
    else:
        for name, u_id, number, amount, created_at in page_rows:
            label = f"@{name}" if name else f"ID:{u_id}"
            amt = float(amount or 0)
            total += amt
            lines.append(f"• {label} | <code>{u_id}</code> | <code>{number or '-'}</code> | {fmt_money(amt)}$")
        lines.append("")
        lines.append(f"<b>ИТОГО ПО СТРАНИЦЕ: {fmt_money(total)}$</b>")

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
    kb.row(InlineKeyboardButton(text="💾 Архив", callback_data="archive"), InlineKeyboardButton(text="📋 Очередь", callback_data="u_queue_all"))
    kb.row(InlineKeyboardButton(text="💻 Техподдержка", url=SUPPORT_URL))
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
        try:
            await call.message.delete()
        except Exception:
            pass
        await send_main_menu(call.message.chat.id, uid)

    elif data == "check_sub":
        try:
            await call.message.delete()
        except Exception:
            pass
        await send_main_menu(call.message.chat.id, uid)

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
            "SELECT id, number, submit_type FROM queue WHERE user_id=? AND status='waiting' ORDER BY id ASC LIMIT 80",
            (uid,),
        )
        total_waiting = db.query("SELECT COUNT(*) FROM queue WHERE status='waiting'", fetch="one")[0]
        lines = ["<b>📋 Общая очередь в боте</b>", f"Всего номеров в очереди: <b>{total_waiting}</b>", ""]
        kb = InlineKeyboardBuilder()
        if not rows:
            lines.append("Очередь пустая")
        else:
            for pos, (qid, number, submit_type) in enumerate(rows, start=1):
                t = "QR" if submit_type == "qr" else "SMS"
                lines.append(f"• <code>{number}</code> [{t}] — {pos}")
                kb.row(types.InlineKeyboardButton(text=f"{number} • {t} • {pos}", callback_data=f"u_queue_pick_{qid}"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="u_back"))
        await render_action_message(call, "\n".join(lines), kb.as_markup())

    elif data.startswith("u_queue_pick_"):
        qid = int(data.replace("u_queue_pick_", ""))
        row = db.query("SELECT number, submit_type, user_id FROM queue WHERE id=? AND user_id=? AND status='waiting'", (qid, uid), fetch="one")
        if not row:
            return await call.answer("⚠️ Номер уже вышел из очереди", show_alert=False)
        number, submit_type, _owner_id = row
        db.query("DELETE FROM queue WHERE id=? AND user_id=? AND status='waiting'", (qid, uid))

        cleanup_queue_expired()
        rows = db.query("SELECT id, number, submit_type FROM queue WHERE user_id=? AND status='waiting' ORDER BY id ASC LIMIT 80", (uid,))
        total_waiting = db.query("SELECT COUNT(*) FROM queue WHERE status='waiting'", fetch="one")[0]
        lines = ["<b>📋 Общая очередь в боте</b>", f"Всего номеров в очереди: <b>{total_waiting}</b>", "", f"✅ Удален номер: <code>{number}</code>", ""]
        kb = InlineKeyboardBuilder()
        if not rows:
            lines.append("Очередь пустая")
        else:
            for pos, (next_qid, next_number, next_submit_type) in enumerate(rows, start=1):
                t = "QR" if next_submit_type == "qr" else "SMS"
                lines.append(f"• <code>{next_number}</code> [{t}] — {pos}")
                kb.row(types.InlineKeyboardButton(text=f"{next_number} • {t} • {pos}", callback_data=f"u_queue_pick_{next_qid}"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="u_back"))
        await render_action_message(call, "\n".join(lines), kb.as_markup())

    elif data in {"archive", "u_my_numbers"}:
        balance = get_user_balance(uid)
        rows = db.query(
            "SELECT number, submit_type, status, price, paid, start_time, credited_notified "
            "FROM sessions WHERE user_id=? AND (status='slet' OR status='otvyaz' OR (status='vstal' AND credited_notified=1)) ORDER BY id DESC LIMIT 300",
            (uid,),
        )
        lines = [f"<b>💰 Баланс:</b> <b>{fmt_money(balance)}$</b>", "<b>📱 Архив номеров:</b>", ""]
        if not rows:
            lines.append("Пока пусто")
        else:
            for number, submit_type, status, price, paid, dt, credited_notified in rows:
                st = "QR" if submit_type == "qr" else "SMS"
                if status == "otvyaz":
                    mark = "бан"
                    amount = 0
                elif status == "vstal" and int(credited_notified or 0):
                    mark = "+"
                    amount = float(price or 0)
                else:
                    mark = "-"
                    amount = 0
                lines.append(f"• <code>{number}</code> | {st} | {fmt_money(amount)}$ | {mark}")
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

    elif data in {"admin_menu", "adm_main"}:
        if not is_user_admin(uid):
            return
        work_label = "🟢 Работаем" if is_work_enabled() else "🔴 Не работаем"
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text=f"Статус: {work_label}", callback_data="adm_toggle_work"))
        kb.row(types.InlineKeyboardButton(text="📣 Сообщение пользователям", callback_data="adm_broadcast"))
        kb.row(types.InlineKeyboardButton(text="👤 Пользователи", callback_data="adm_users"))
        kb.row(types.InlineKeyboardButton(text="📊 Статистика", callback_data="adm_stats"))
        kb.row(types.InlineKeyboardButton(text="💲 Тариф", callback_data="adm_set_price"), types.InlineKeyboardButton(text="🧾 Отчёт", callback_data="adm_reports"))
        kb.row(types.InlineKeyboardButton(text="🏢 Группы", callback_data="adm_groups"), types.InlineKeyboardButton(text="📋 Очередь", callback_data="adm_queue_view"))
        kb.row(types.InlineKeyboardButton(text="🧨 Очистка всего", callback_data="adm_clear_all"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="u_back"))
        await render_action_message(call, "🛡 <b>Админ-панель</b>", kb.as_markup())

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

    elif data == "adm_clear_all":
        if not is_user_admin(uid):
            return
        clear_all_runtime_data()
        await call.answer("✅ Очищены отчёты, архивы и очередь", show_alert=True)

    elif data == "adm_toggle_work":
        if not is_user_admin(uid):
            return
        set_work_enabled(not is_work_enabled())
        work_label = "🟢 Работаем" if is_work_enabled() else "🔴 Не работаем"
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text=f"Статус: {work_label}", callback_data="adm_toggle_work"))
        kb.row(types.InlineKeyboardButton(text="📣 Сообщение пользователям", callback_data="adm_broadcast"))
        kb.row(types.InlineKeyboardButton(text="👤 Пользователи", callback_data="adm_users"))
        kb.row(types.InlineKeyboardButton(text="📊 Статистика", callback_data="adm_stats"))
        kb.row(types.InlineKeyboardButton(text="💲 Тариф", callback_data="adm_set_price"), types.InlineKeyboardButton(text="🧾 Отчёт", callback_data="adm_reports"))
        kb.row(types.InlineKeyboardButton(text="🏢 Группы", callback_data="adm_groups"), types.InlineKeyboardButton(text="📋 Очередь", callback_data="adm_queue_view"))
        kb.row(types.InlineKeyboardButton(text="🧨 Очистка всего", callback_data="adm_clear_all"))
        kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="u_back"))
        await render_action_message(call, "🛡 <b>Админ-панель</b>", kb.as_markup())
        await call.answer("✅ Статус обновлён", show_alert=False)

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
                label = f"@{username}" if username else "без username"
                t = "QR" if submit_type == "qr" else "Код"
                lines.append(f"• <code>{number}</code> [{t}] — <code>{user_id}</code> — {label}")
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

    elif data == "adm_stats":
        if not is_user_admin(uid):
            return
        users = db.query("SELECT COUNT(*) FROM users", fetch="one")[0]
        submitted = db.query("SELECT COUNT(*) FROM submissions", fetch="one")[0]
        vstal = db.query("SELECT COUNT(*) FROM sessions WHERE status IN ('vstal','slet','otvyaz')", fetch="one")[0]
        txt = (
            "<b>📊 Статистика</b>\n\n"
            f"Пользователей: <b>{users}</b>\n"
            f"Сданных номеров: <b>{submitted}</b>\n"
            f"Встало номеров: <b>{vstal}</b>"
        )
        await render_action_message(call, txt, back_kb("adm_main"))

    
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
        await call.answer()

    elif data.startswith("adm_payout_req_"):
        if not is_user_admin(uid):
            return
        sid_raw = data.replace("adm_payout_req_", "", 1)
        if not sid_raw.isdigit():
            return await call.answer("Некорректный ID", show_alert=False)
        sid = int(sid_raw)
        row = db.query("SELECT user_id, number, price FROM sessions WHERE id=? LIMIT 1", (sid,), fetch="one")
        if not row:
            return await call.answer("Сессия не найдена", show_alert=False)
        target_uid, number, price = row
        amount = float(price or get_user_price(target_uid))
        await state.set_state(AdminForm.payout_amount)
        await state.update_data(
            payout_sid=sid,
            payout_user_id=target_uid,
            payout_number=number,
            payout_amount=amount,
            payout_src_chat_id=call.message.chat.id if call.message and call.message.chat else None,
            payout_src_message_id=call.message.message_id if call.message else None,
        )
        await call.message.answer(
            f"Отправьте сообщение/чек для пользователя <code>{target_uid}</code> по номеру <code>{number}</code> на сумму <b>{fmt_money(amount)}$</b>.",
            parse_mode="HTML",
        )
        await call.answer()

    elif data == "support":
        await call.message.answer(f"💻 Тех. поддержка: {SUPPORT_URL}")

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
            next_kb.row(types.InlineKeyboardButton(text="⏭ Скип", callback_data=f"k_{user_id}_{number}"))
        else:
            next_kb.row(
                types.InlineKeyboardButton(text="🔔 Запросить КОД", callback_data=f"r_{call.message.chat.id}_{thread_id or 0}_{user_id}_{number}"),
                types.InlineKeyboardButton(text="⏭ Скип", callback_data=f"k_{user_id}_{number}"),
            )
        next_kb.row(types.InlineKeyboardButton(text="⚠️ Ошибка", callback_data=f"e_{user_id}_{number}"))

        issue_text = f"Метод: <b>{st}</b>\nНомер: <code>{number}</code>\nОператор: <code>{uid}</code>"
        if submit_type == "qr":
            issue_text += "\n\nОтправьте QR в ответ на это сообщение."
        await call.message.answer(
            issue_text,
            parse_mode="HTML",
            reply_markup=next_kb.as_markup(),
        )
        try:
            if submit_type == "qr":
                db.query("INSERT OR REPLACE INTO settings(key, value) VALUES(?, '1')", (qr_request_key(user_id, number),))
                db.query("DELETE FROM settings WHERE key=?", (qr_done_key(user_id, number),))
                await bot.send_message(user_id, f"📨 Ваш номер {number} взяли. Ожидайте QR.")
                await bot.send_message(user_id, f"🔔 По номеру {number} отправьте QR в ответ на это сообщение.")
            else:
                await bot.send_message(user_id, f"📨 Ваш номер {number} взяли. Ожидайте кода.")
        except Exception:
            pass

    elif data.startswith(("r_", "rr_", "q_", "qr_")):
        parts = data.split("_", 4)
        if len(parts) < 5:
            await call.answer()
            return
        action, chat_id, thread_id, worker_uid, number = parts
        chat_id = int(chat_id)
        thread_id = int(thread_id)
        worker_uid = int(worker_uid)
        is_repeat_sms = action == "rr"
        is_repeat_qr = action == "qr"
        if action in {"q", "qr"}:
            row = db.query("SELECT qr_requested FROM queue WHERE number=? AND user_id=? AND status='proc' ORDER BY id DESC LIMIT 1", (number, worker_uid), fetch="one")
        else:
            row = db.query("SELECT repeat_requested FROM queue WHERE number=? AND user_id=? AND status='proc' ORDER BY id DESC LIMIT 1", (number, worker_uid), fetch="one")
        if not row:
            await call.answer("⚠️ Номер уже закрыт", show_alert=False)
            return
        if action in {"r", "q"} and row[0]:
            await call.answer("⚠️ Уже запрашивали", show_alert=False)
            return
        if action == "r":
            db.query("UPDATE queue SET repeat_requested=1 WHERE number=? AND user_id=? AND status='proc'", (number, worker_uid))
        elif action == "q":
            db.query("UPDATE queue SET qr_requested=1 WHERE number=? AND user_id=? AND status='proc'", (number, worker_uid))
        try:
            if action in {"r", "rr"}:
                db.query("INSERT OR REPLACE INTO settings(key, value) VALUES(?, '1')", (sms_request_key(worker_uid, number),))
                db.query("DELETE FROM settings WHERE key=?", (sms_done_key(worker_uid, number),))
                if is_repeat_sms:
                    await bot.send_message(worker_uid, f"🔔 Дайте повтор кода на номер {number}.\nОтправьте код в ответ на это сообщение.")
                else:
                    await bot.send_message(worker_uid, f"🔔 По номеру {number} запросили SMS-код.\nОтправьте код ОДНИМ сообщением в ответ на это сообщение.")
                try:
                    sms_kb = InlineKeyboardBuilder()
                    sms_kb.row(types.InlineKeyboardButton(text="⏭ Скип", callback_data=f"k_{worker_uid}_{number}"))
                    await render_action_message(
                        call,
                        f"Метод: <b>SMS</b>\nНомер: <code>{number}</code>\nОператор: <code>{uid}</code>\n\nзапрос на смс отправлен ожидайте смс",
                        sms_kb.as_markup(),
                    )
                except Exception:
                    pass
            else:
                db.query("INSERT OR REPLACE INTO settings(key, value) VALUES(?, '1')", (qr_request_key(worker_uid, number),))
                db.query("DELETE FROM settings WHERE key=?", (qr_done_key(worker_uid, number),))
                if is_repeat_qr:
                    await bot.send_message(worker_uid, f"🔔 Дайте повтор QR на номер {number}.\nОтправьте фото QR в ответ на это сообщение.")
                else:
                    await bot.send_message(worker_uid, f"🔔 По номеру {number} запросили QR-код.\nОтправьте фото QR ОДНИМ сообщением в ответ на это сообщение.")
                try:
                    qr_kb = InlineKeyboardBuilder()
                    qr_kb.row(types.InlineKeyboardButton(text="⏭ Скип", callback_data=f"k_{worker_uid}_{number}"))
                    await render_action_message(
                        call,
                        f"Метод: <b>QR</b>\nНомер: <code>{number}</code>\nОператор: <code>{uid}</code>\n\nзапрос на QR отправлен ожидайте QR",
                        qr_kb.as_markup(),
                    )
                except Exception:
                    pass
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
                types.InlineKeyboardButton(text="🔴 Слет", callback_data=f"s_{v_uid}_{v_num}"),
                types.InlineKeyboardButton(text="🚫 Бан", callback_data=f"d_{v_uid}_{v_num}"),
            )
            await render_action_message(call, f"✅ <b>Номер встал</b>\nНомер: <code>{v_num}</code>", kb2.as_markup())

        elif act == "s":
            row = db.query("SELECT id, start_time, submit_type, price, credited_notified FROM sessions WHERE number=? AND user_id=? AND status='vstal' ORDER BY id DESC LIMIT 1", (v_num, v_uid), fetch="one")
            if not row:
                await call.answer("⚠️ Активная сессия уже закрыта", show_alert=False)
                return
            sid, start_time, submit_type, price, credited_notified = row
            now_dt = now_kz_naive()
            elapsed_sec = int((now_dt - parse_dt(start_time)).total_seconds())
            paid_flag = 1 if elapsed_sec >= 240 else 0
            db.query("UPDATE sessions SET status='slet', paid=?, end_time=? WHERE id=?", (paid_flag, now_dt.strftime("%Y-%m-%d %H:%M:%S"), sid))
            if paid_flag:
                amount = float(price or get_user_price(v_uid))
                existing_payout = db.query("SELECT id FROM payouts WHERE user_id=? AND number=? LIMIT 1", (v_uid, v_num), fetch="one")
                if not existing_payout:
                    db.query(
                        "INSERT INTO payouts(user_id, number, submit_type, amount, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (v_uid, v_num, submit_type or 'code', amount, 'pending_manual', now_kz_naive().strftime("%Y-%m-%d %H:%M:%S")),
                    )
                if not int(credited_notified or 0):
                    try:
                        kb_pay = InlineKeyboardBuilder()
                        kb_pay.row(types.InlineKeyboardButton(text="💸 Выплатить", callback_data=f"adm_payout_req_{sid}"))
                        await bot.send_message(
                            ADMIN_ID,
                            f"✅ Номер успешно засчитан\nНомер: <code>{v_num}</code>\nID: <code>{v_uid}</code>\nК выплате: <b>{fmt_money(amount)}$</b>",
                            parse_mode="HTML",
                            reply_markup=kb_pay.as_markup(),
                        )
                    except Exception:
                        pass
                    try:
                        await bot.send_message(v_uid, f"✅ Номер {v_num} засчитан. Вам начислено {fmt_money(amount)}$.")
                    except Exception:
                        pass
            else:
                try:
                    await bot.send_message(v_uid, f"⚠️ Номер {v_num} не засчитан: слёт произошёл слишком быстро.")
                except Exception:
                    pass
            await render_action_message(call, f"🔴 {v_num} — <b>Слетел</b> ({'засчитан' if paid_flag else 'не засчитан'})")

        elif act == "d":
            sid = db.query("SELECT id FROM sessions WHERE number=? AND user_id=? AND status='vstal' ORDER BY id DESC LIMIT 1", (v_num, v_uid), fetch="one")
            if sid:
                db.query("UPDATE sessions SET status='otvyaz', paid=0, end_time=? WHERE id=?", (now_kz_naive().strftime("%Y-%m-%d %H:%M:%S"), sid[0]))
                db.query("DELETE FROM payouts WHERE id=(SELECT id FROM payouts WHERE user_id=? AND number=? ORDER BY id DESC LIMIT 1)", (v_uid, v_num))
                await render_action_message(call, f"🚫 {v_num} — <b>Бан/Отвяз</b> (оплата снята)")
            else:
                active_q = db.query("SELECT id FROM queue WHERE number=? AND user_id=? AND status='proc' ORDER BY id DESC LIMIT 1", (v_num, v_uid), fetch="one")
                if not active_q:
                    await call.answer("⚠️ Номер уже закрыт", show_alert=False)
                    return
                db.query("DELETE FROM queue WHERE id=?", (active_q[0],))
                await render_action_message(call, f"🚫 {v_num} — <b>Бан</b> (удален из обработки)")
            try:
                await bot.send_message(v_uid, f"🚫 По номеру {v_num} бан на аккаунте, номер не засчитан.")
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
            await render_action_message(call, f"❌ {v_num} — <b>номер не встал и удалён из очереди</b>")
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
    await send_main_menu(message.chat.id, message.from_user.id)

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
        await message.answer("🔒 Привязка установлена для этого чата/топика.\nДля выдачи номера напишите `номер`.", parse_mode="Markdown")

@dp.message(Command("dbinfo"))
async def db_info(message: types.Message):
    if not is_user_admin(message.from_user.id):
        return
    users_count = db.query("SELECT COUNT(*) FROM users", fetch="one")[0]
    await message.answer(f"🗄 <b>DB INFO</b>\nПуть: <code>{RESOLVED_DB_PATH}</code>\nПользователей: <b>{users_count}</b>", parse_mode="HTML")

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
    await message.answer(f"✅ Новый тариф: {fmt_money(value)}$")

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

@dp.message(StateFilter(None), F.text.regexp(r"^\s*\d{3,10}\s*$"))
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
        kb.row(
            types.InlineKeyboardButton(text="✅ Встал", callback_data=f"v_{message.from_user.id}_{number}"),
            types.InlineKeyboardButton(text="🚫 Бан", callback_data=f"d_{message.from_user.id}_{number}"),
        )
        kb.row(
            types.InlineKeyboardButton(text="❌ Номер не встал", callback_data=f"n_{message.from_user.id}_{number}"),
            types.InlineKeyboardButton(text="🔁 Повтор кода", callback_data=f"rr_{gid}_{req_thread_id or 0}_{message.from_user.id}_{number}"),
        )
        txt = f"📩 Код по номеру <code>{number}</code>: <b>{code}</b>"
        try:
            if thread_id:
                await bot.send_message(gid, txt, parse_mode="HTML", message_thread_id=thread_id, reply_markup=kb.as_markup())
            else:
                await bot.send_message(gid, txt, parse_mode="HTML", reply_markup=kb.as_markup())
        except Exception:
            pass
    await message.answer(f"✅ Код по номеру {number} принят. Номер вошёл в работу.")

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

@dp.message(AdminForm.payout_amount)
async def admin_user_inline_payout(message: types.Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    target_uid = data.get("payout_user_id")
    number = data.get("payout_number")
    amount = float(data.get("payout_amount") or 0)
    src_chat_id = data.get("payout_src_chat_id")
    src_message_id = data.get("payout_src_message_id")
    if not target_uid or not number:
        await state.clear()
        return

    text = (message.caption or message.text or "").strip()
    if not text and not message.photo and not message.document:
        return await message.answer("Отправьте текст/чек (можно с фото или файлом).")

    note = f"✅ Выплата по номеру {number} на сумму: {fmt_money(amount)}$\n\n"
    payload_text = note + text if text else note + "Чек отправлен администратором."
    try:
        if message.photo:
            await bot.send_photo(target_uid, message.photo[-1].file_id, caption=payload_text)
        elif message.document:
            await bot.send_document(target_uid, message.document.file_id, caption=payload_text)
        else:
            await bot.send_message(target_uid, payload_text)
        db.query(
            "UPDATE payouts SET payload='paid_by_admin' WHERE id=(SELECT id FROM payouts WHERE user_id=? AND number=? ORDER BY id DESC LIMIT 1)",
            (target_uid, number),
        )
        if src_chat_id and src_message_id:
            try:
                await bot.edit_message_reply_markup(chat_id=int(src_chat_id), message_id=int(src_message_id), reply_markup=None)
            except Exception:
                pass
        await message.answer("✅ Чек отправлен пользователю.")
    except Exception:
        await message.answer("⚠️ Не удалось отправить чек пользователю.")
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

@dp.message(Form.num)
async def num_input(message: types.Message, state: FSMContext):
    if is_user_banned(message.from_user.id):
        return await message.answer("🚫 Вы забанены и не можете сдавать номера.")
    data = await state.get_data()
    submit_type = data.get("submit_type", "code")
    numbers = parse_numbers(message.text or "")
    if not numbers:
        return await message.answer("❌ Ошибка! Введите 11 цифр в каждой строке или через пробел.")

    user_priority = 1 if is_user_priority(message.from_user.id) else 0
    now = now_kz_naive().strftime("%Y-%m-%d %H:%M:%S")
    for num in numbers:
        db.query(
            "INSERT INTO queue (user_id, number, submit_type, status, created_at) VALUES (?, ?, ?, 'waiting', ?)",
            (message.from_user.id, num, submit_type, now)
        )
        db.query(
            "INSERT INTO submissions (user_id, number, submit_type, created_at) VALUES (?, ?, ?, ?)",
            (message.from_user.id, num, submit_type, now)
        )

# ✅ Получаем последний ID, проверяем на None
    last_id = db.query(
        "SELECT MAX(id) FROM queue WHERE user_id=?",
        (message.from_user.id,),
        fetch="one"
    )[0] or 0  # <- добавили or 0, если пустая очередь

# ✅ Получаем позицию в очереди, проверяем на None
    q_pos = db.query(
        "SELECT COUNT(*) FROM queue q JOIN users u ON q.user_id=u.user_id "
        "WHERE q.status='waiting' AND (u.priority > ? OR (u.priority = ? AND q.id <= ?))",
        (user_priority, user_priority, last_id),
        fetch="one"
    )[0] or 0  # <- добавили or 0

# ✅ Добавляем подсчет общего числа в очереди
    total_waiting = db.query(
        "SELECT COUNT(*) FROM queue WHERE status='waiting'",
        fetch="one"
    )[0] or 0  # <- добавили or 0

    await state.clear()
    t = "QR" if submit_type == "qr" else "Код"

# ✅ Сообщение пользователю
    await message.answer(
        f"✅ Номер(а) добавлены: <b>{len(numbers)}</b> [{t}]\n"
        f"📋 Ваша позиция в очереди: <b>{q_pos}</b>\n"
        f"📋 Всего в очереди: <b>{total_waiting}</b>",
        parse_mode="HTML",
        reply_markup=back_kb("u_menu"),  # можно временно убрать для теста
    )

    try:
        username = f"@{message.from_user.username}" if message.from_user.username else "без username"
        preview = ", ".join(numbers[:5])
        if len(numbers) > 5:
            preview += f" и ещё {len(numbers)-5}"
        await bot.send_message(
            ADMIN_ID,
            f"📥 Сдали номер(а)\nПользователь: {username}\nID: <code>{message.from_user.id}</code>\nТип: <b>{t}</b>\nКол-во: <b>{len(numbers)}</b>\nНомера: <code>{preview}</code>",
            parse_mode="HTML",
        )
    except Exception:
        pass

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
        "SELECT number, submit_type, status, price, paid, start_time, credited_notified "
        "FROM sessions WHERE user_id=? AND (status='slet' OR status='otvyaz' OR (status='vstal' AND credited_notified=1)) ORDER BY id DESC LIMIT 300",
        (uid,),
    )
    lines = [f"<b>💰 Баланс:</b> <b>{fmt_money(balance)}$</b>", "<b>📱 Архив номеров:</b>", ""]
    if not rows:
        lines.append("Пока пусто")
    else:
        for number, submit_type, status, price, paid, dt, credited_notified in rows:
            st = "QR" if submit_type == "qr" else "SMS"
            if status == "otvyaz":
                mark = "бан"
                amount = 0
            elif status == "vstal" and int(credited_notified or 0):
                mark = "+"
                amount = float(price or 0)
            else:
                mark = "-"
                amount = 0
            lines.append(f"• <code>{number}</code> | {st} | {fmt_money(amount)}$ | {mark}")
    await message.answer("\n".join(lines), parse_mode="HTML")

@dp.message(F.text.regexp(r"(?i).*(^|\s)номер([.!?]|\s|$).*"))
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
        next_kb.row(types.InlineKeyboardButton(text="⏭ Скип", callback_data=f"k_{user_id}_{number}"))
    else:
        next_kb.row(
            types.InlineKeyboardButton(text="🔔 Запросить КОД", callback_data=f"r_{message.chat.id}_{thread_id or 0}_{user_id}_{number}"),
            types.InlineKeyboardButton(text="⏭ Скип", callback_data=f"k_{user_id}_{number}"),
        )
    next_kb.row(types.InlineKeyboardButton(text="⚠️ Ошибка", callback_data=f"e_{user_id}_{number}"))

    issue_text = f"Метод: <b>{st}</b>\nНомер: <code>{number}</code>\nОператор: <code>{message.from_user.id}</code>"
    if submit_type == "qr":
        issue_text += "\n\nОтправьте QR в ответ на это сообщение."
    await message.answer(
        issue_text,
        parse_mode="HTML",
        reply_markup=next_kb.as_markup(),
    )
    try:
        if submit_type == "qr":
            db.query("INSERT OR REPLACE INTO settings(key, value) VALUES(?, '1')", (qr_request_key(user_id, number),))
            db.query("DELETE FROM settings WHERE key=?", (qr_done_key(user_id, number),))
            await bot.send_message(user_id, f"📨 Ваш номер {number} взяли. Ожидайте QR.")
            await bot.send_message(user_id, f"🔔 По номеру {number} отправьте QR в ответ на это сообщение.")
        else:
            await bot.send_message(user_id, f"📨 Ваш номер {number} взяли. Ожидайте кода.")
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
                types.InlineKeyboardButton(text="🚫 Бан", callback_data=f"d_{message.from_user.id}_{number}"),
            )
            kb.row(
                types.InlineKeyboardButton(text="❌ Номер не встал", callback_data=f"n_{message.from_user.id}_{number}"),
                types.InlineKeyboardButton(text="🔁 Повтор QR", callback_data=f"qr_{gid}_{req_thread_id or 0}_{message.from_user.id}_{number}"),
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

@dp.message(F.photo)
async def handle_photo(message: types.Message):
    await _process_code_media(message)

@dp.message(F.document)
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
        try:
            await asyncio.sleep(30)
            threshold = (now_kz_naive() - timedelta(minutes=4)).strftime("%Y-%m-%d %H:%M:%S")
            rows = db.query(
                "SELECT id, user_id, number, submit_type, price FROM sessions WHERE status='vstal' AND credited_notified=0 AND start_time IS NOT NULL AND start_time <= ?",
                (threshold,),
            )
            for sid, user_id, number, submit_type, price in rows:
                try:
                    amount = float(price) if price is not None else float(get_user_price(user_id))
                except Exception:
                    amount = float(get_user_price(user_id))
                existing_payout = db.query("SELECT id FROM payouts WHERE user_id=? AND number=? LIMIT 1", (user_id, number), fetch="one")
                if not existing_payout:
                    db.query(
                        "INSERT INTO payouts(user_id, number, submit_type, amount, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (user_id, number, submit_type or 'code', amount, 'pending_manual', now_kz_naive().strftime("%Y-%m-%d %H:%M:%S")),
                    )
                db.query("UPDATE sessions SET paid=1, credited_notified=1 WHERE id=?", (sid,))
                try:
                    kb_pay = InlineKeyboardBuilder()
                    kb_pay.row(types.InlineKeyboardButton(text="💸 Выплатить", callback_data=f"adm_payout_req_{sid}"))
                    await bot.send_message(
                        ADMIN_ID,
                        f"✅ Номер успешно засчитан\nНомер: <code>{number}</code>\nID: <code>{user_id}</code>\nК выплате <b>{fmt_money(amount)}$</b>",
                        parse_mode="HTML",
                        reply_markup=kb_pay.as_markup(),
                    )
                except Exception:
                    pass
                try:
                    await bot.send_message(user_id, f"✅ Номер {number} засчитан. Начислено {fmt_money(amount)}$.")
                except Exception:
                    pass
        except Exception:
            logging.exception("credited_checker cycle failed")


async def nightly_cleanup():
    while True:
        now = datetime.now(KZ_TZ)
        next_cleanup = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if next_cleanup <= now:
            next_cleanup += timedelta(days=1)
        await asyncio.sleep((next_cleanup - now).total_seconds())
        try:
            # ВАЖНО: здесь должны быть только вызовы функций,
            # без вложенных `def`, иначе легко поймать IndentationError.
            cleanup_paid_reports()
            cleanup_archives()
        except Exception:
            logging.exception("nightly_cleanup cycle failed")

async def main():
    asyncio.create_task(hold_checker())
    asyncio.create_task(credited_checker())
    asyncio.create_task(nightly_cleanup())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
