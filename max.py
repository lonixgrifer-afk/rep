import json
import os
import re
import sqlite3
import time
import urllib.parse
import urllib.request
from contextlib import closing
from datetime import datetime, timedelta, timezone


# Один файл, без requirements.txt и .env.
# Заполните перед запуском. Можно также передать через переменные окружения.
BOT_TOKEN = os.getenv("BOT_TOKEN", "8680736365:AAGH9QWkNshyIlD8giWHhm93xKR26p7sCiE")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "change-this-password")
DB_PATH = os.getenv("DB_PATH", "bot.db")

# Если список пустой, первый вошедший пользователь автоматически станет админом.
ADMIN_TELEGRAM_IDS = [8949311928]

# Как часто слать автоотчет админам, если автоотчеты включены.
AUTO_REPORT_INTERVAL_SECONDS = 60 * 60

ROLE_OPERATOR = "operator"
ROLE_SUPPLIER = "supplier"

STATUS_AVAILABLE = "available"
STATUS_ASSIGNED = "assigned"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"

WITHDRAWAL_PENDING = "pending"
WITHDRAWAL_DONE = "done"


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(db()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                public_id TEXT NOT NULL UNIQUE,
                telegram_id INTEGER NOT NULL UNIQUE,
                username TEXT,
                role TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0,
                is_blocked INTEGER NOT NULL DEFAULT 0,
                password_check INTEGER NOT NULL DEFAULT 0,
                state TEXT,
                state_data TEXT,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_user_id INTEGER NOT NULL,
                masked_number TEXT NOT NULL,
                volume INTEGER NOT NULL DEFAULT 1,
                remaining INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                assigned_operator_user_id INTEGER,
                assigned_at TEXT,
                completed_at TEXT,
                last_reason TEXT
            );

            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor_user_id INTEGER,
                number_id INTEGER,
                event_type TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                admin_message TEXT,
                created_at TEXT NOT NULL,
                completed_at TEXT
            );
            """
        )
        migrate_schema(conn)
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('auto_reports_enabled', '0')")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('access_password', ?)", (ADMIN_PASSWORD,))
        conn.commit()


def migrate_schema(conn):
    user_cols = {row["name"] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "public_id" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN public_id TEXT")
    if "username" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN username TEXT")
    if "password_check" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN password_check INTEGER NOT NULL DEFAULT 0")
    if "state" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN state TEXT")
    if "state_data" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN state_data TEXT")

    number_cols = {row["name"] for row in conn.execute("PRAGMA table_info(numbers)").fetchall()}
    if "assigned_operator_user_id" not in number_cols:
        conn.execute("ALTER TABLE numbers ADD COLUMN assigned_operator_user_id INTEGER")
    if "last_reason" not in number_cols:
        conn.execute("ALTER TABLE numbers ADD COLUMN last_reason TEXT")
    if "completed_at" not in number_cols:
        conn.execute("ALTER TABLE numbers ADD COLUMN completed_at TEXT")
    if "assigned_client_user_id" in number_cols:
        conn.execute(
            """
            UPDATE numbers
            SET assigned_operator_user_id = COALESCE(assigned_operator_user_id, assigned_client_user_id)
            """
        )
    if "cancel_reason" in number_cols:
        conn.execute("UPDATE numbers SET last_reason = COALESCE(last_reason, cancel_reason)")
    if "fail_reason" in number_cols:
        conn.execute("UPDATE numbers SET last_reason = COALESCE(last_reason, fail_reason)")
    conn.execute("UPDATE users SET role = ? WHERE role IN ('client', 'admin')", (ROLE_SUPPLIER,))
    conn.execute("UPDATE numbers SET status = ? WHERE status = 'confirmed'", (STATUS_ASSIGNED,))
    for row in conn.execute("SELECT id FROM users WHERE public_id IS NULL OR public_id = ''").fetchall():
        conn.execute("UPDATE users SET public_id = ? WHERE id = ?", (public_id(row["id"]), row["id"]))


def api(method, data=None):
    if BOT_TOKEN == "PASTE_BOT_TOKEN_HERE":
        raise RuntimeError("Укажите BOT_TOKEN в начале bot.py или через переменную окружения BOT_TOKEN.")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    body = urllib.parse.urlencode(data or {}).encode("utf-8")
    with urllib.request.urlopen(url, body, timeout=60) as response:
        result = json.loads(response.read().decode("utf-8"))
    if not result.get("ok"):
        raise RuntimeError(result)
    return result["result"]


def send_message(chat_id, text, reply_markup=None, entities=None):
    data = {"chat_id": chat_id, "text": text}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    if entities:
        data["entities"] = json.dumps(entities, ensure_ascii=False)
    return api("sendMessage", data)



def copy_message(chat_id, from_chat_id, message_id, caption=None, reply_markup=None):
    data = {"chat_id": chat_id, "from_chat_id": from_chat_id, "message_id": message_id}
    if caption:
        data["caption"] = caption
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    return api("copyMessage", data)

def answer_callback(callback_id, text=None, alert=False):
    data = {"callback_query_id": callback_id, "show_alert": "true" if alert else "false"}
    if text:
        data["text"] = text
    return api("answerCallbackQuery", data)



def delete_message(chat_id, message_id):
    if not message_id:
        return None
    try:
        return api("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
    except Exception:
        return None


def send_document_bytes(chat_id, filename, content, caption=None, reply_markup=None):
    boundary = f"----bot-boundary-{int(time.time() * 1000)}"
    fields = {"chat_id": str(chat_id)}
    if caption:
        fields["caption"] = caption
    if reply_markup:
        fields["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

    body = bytearray()
    for name, value in fields.items():
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
        body.extend(str(value).encode("utf-8"))
        body.extend(b"\r\n")
    body.extend(f"--{boundary}\r\n".encode("utf-8"))
    body.extend(f'Content-Disposition: form-data; name="document"; filename="{filename}"\r\n'.encode("utf-8"))
    body.extend(b"Content-Type: text/csv; charset=utf-8\r\n\r\n")
    body.extend(content)
    body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode("utf-8"))

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    request = urllib.request.Request(
        url,
        data=bytes(body),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        result = json.loads(response.read().decode("utf-8"))
    if not result.get("ok"):
        raise RuntimeError(result)
    return result["result"]

def role_title(role):
    return {ROLE_OPERATOR: "оператор", ROLE_SUPPLIER: "поставщик"}.get(role, role)


def public_id(user_id):
    return f"U{user_id:06d}"


def user_handle(user):
    if not user:
        return "-"
    username = user["username"] if isinstance(user, sqlite3.Row) else user.get("username")
    return f"@{username}" if username else "без @username"


def normalize_russian_number(text):
    digits = re.sub(r"\D", "", text or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    return None


def parse_russian_numbers(text):
    numbers = []
    bad = []
    for line in (text or "").splitlines():
        raw = line.strip()
        if not raw:
            continue
        number = normalize_russian_number(raw)
        if number:
            numbers.append(number)
        else:
            bad.append(raw)
    return numbers, bad


def looks_like_code(text):
    return bool(re.fullmatch(r"\d{4,10}", (text or "").strip()))


def inline_keyboard(rows):
    return {"inline_keyboard": [[{"text": text, "callback_data": data} for text, data in row] for row in rows]}


def back_row():
    return [("⬅️ Назад", "menu:home")]


def prompt_keyboard(extra_rows=None):
    rows = list(extra_rows or [])
    rows.append(back_row())
    return inline_keyboard(rows)


def send_state_prompt(chat_id, user_id, state, text, data=None, reply_markup=None):
    message = send_message(chat_id, text, reply_markup or prompt_keyboard())
    state_payload = dict(data or {})
    state_payload["prompt_message_id"] = message.get("message_id")
    set_state(user_id, state, state_payload)
    return message


def delete_state_prompt(chat_id, user):
    prompt_message_id = state_data(user).get("prompt_message_id")
    if prompt_message_id:
        delete_message(chat_id, prompt_message_id)


def main_menu_keyboard(user):
    rows = []
    if user["role"] == ROLE_SUPPLIER:
        rows.append([("➕ Добавить номер", "menu:add_number"), ("📦 Моя очередь", "menu:my_queue")])
        rows.append([("💎 Кошелек", "menu:wallet")])
    elif user["role"] == ROLE_OPERATOR:
        rows.append([("📲 Взять номер", "menu:take_number")])
    if user["is_admin"]:
        rows.append([("🛠️ Админ-панель", "menu:admin")])
    return inline_keyboard(rows)


def admin_keyboard():
    return inline_keyboard([
        [("📊 Статистика", "admin:stats"), ("👥 Операторы", "admin:operator_stats")],
        [("📄 Отчет файлом", "admin:report_file"), ("💸 Выводы", "admin:withdrawals")],
        [("🌐 Общая очередь", "admin:global_queue")],
        [("✉️ Написать пользователю", "admin:direct_message")],
        [("🔐 Сменить пароль", "admin:change_password")],
        [("🎧 Выдача оператора", "admin:grant_operator")],
        [("♻️ Сброс очереди", "admin:reset_queue"), ("📣 Рассылка", "admin:broadcast")],
        [("🚫 Блокировка", "admin:block"), ("✅ Разблокировка", "admin:unblock")],
        [("🧨 Очистить базу", "admin:clear_db")],
        [("⬅️ Назад", "menu:home")],
    ])


def supplier_number_keyboard(number_id):
    return inline_keyboard([
        [("🔁 Повтор с причиной", f"supplier:repeat:{number_id}")],
        [("❌ Отменить с причиной", f"supplier:cancel:{number_id}")],
        [("⬅️ Назад", "menu:home")],
    ])


def operator_active_keyboard(number_id):
    return inline_keyboard([
        [("🔁 Повтор сообщения", f"operator:repeat_message:{number_id}")],
        [("✅ Встал", f"operator:done:{number_id}"), ("❌ Не встал", f"operator:failed:{number_id}")],
        [("⏭️ Скипнуть", f"operator:skip:{number_id}")],
        [("⬅️ Назад", "menu:home")],
    ])



def money_text(value):
    value = round(float(value), 2)
    if value.is_integer():
        return f"{int(value)}$"
    return f"{value:.2f}$"


def supplier_balance(conn, user_id):
    done = conn.execute(
        "SELECT COUNT(*) count FROM numbers WHERE supplier_user_id = ? AND status = ?",
        (user_id, STATUS_DONE),
    ).fetchone()["count"]
    withdrawn = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) total FROM withdrawals WHERE user_id = ? AND status IN (?, ?)",
        (user_id, WITHDRAWAL_PENDING, WITHDRAWAL_DONE),
    ).fetchone()["total"]
    return float(done) - float(withdrawn or 0), done


def parse_amount(text):
    normalized = (text or "").strip().replace(",", ".")
    if not re.fullmatch(r"\d+(?:\.\d{1,2})?", normalized):
        return None
    amount = float(normalized)
    return amount if amount > 0 else None

def get_setting(key, default=None):
    with closing(db()) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key, value):
    with closing(db()) as conn:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )
        conn.commit()



def get_access_password():
    return get_setting("access_password", ADMIN_PASSWORD)


def set_access_password(password):
    set_setting("access_password", password)


def get_user(telegram_id):
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()


def get_user_by_handle(value):
    username = (value or "").strip()
    if username.startswith("@"):
        username = username[1:]
    if not username:
        return None
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM users WHERE lower(username) = lower(?)", (username,)).fetchone()


def admin_count():
    with closing(db()) as conn:
        return conn.execute("SELECT COUNT(*) count FROM users WHERE is_admin = 1").fetchone()["count"]


def extract_username(tg_from):
    return (tg_from or {}).get("username")


def create_or_touch_user(telegram_id, username=None):
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()
        if row:
            password_check = 1 if row["is_admin"] or telegram_id in ADMIN_TELEGRAM_IDS else row["password_check"]
            is_admin = 1 if row["is_admin"] or telegram_id in ADMIN_TELEGRAM_IDS else 0
            conn.execute(
                "UPDATE users SET username = COALESCE(?, username), is_admin = ?, password_check = ?, last_seen_at = ? WHERE id = ?",
                (username, is_admin, password_check, now_iso(), row["id"]),
            )
            conn.commit()
            return conn.execute("SELECT * FROM users WHERE id = ?", (row["id"],)).fetchone()

        is_admin = 1 if (telegram_id in ADMIN_TELEGRAM_IDS or (not ADMIN_TELEGRAM_IDS and admin_count() == 0)) else 0
        cur = conn.execute(
            """
            INSERT INTO users (public_id, telegram_id, username, role, is_admin, password_check, created_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("pending", telegram_id, username, ROLE_SUPPLIER, is_admin, 1 if is_admin else 0, now_iso(), now_iso()),
        )
        user_id = cur.lastrowid
        conn.execute("UPDATE users SET public_id = ? WHERE id = ?", (public_id(user_id), user_id))
        conn.commit()
        return conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def set_user_role(user_id, role):
    with closing(db()) as conn:
        conn.execute("UPDATE users SET role = ? WHERE id = ?", (role, user_id))
        conn.commit()
        return conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def set_admin(telegram_id, username=None):
    user = create_or_touch_user(telegram_id, username)
    with closing(db()) as conn:
        conn.execute("UPDATE users SET is_admin = 1, password_check = 1 WHERE id = ?", (user["id"],))
        conn.commit()
        return conn.execute("SELECT * FROM users WHERE id = ?", (user["id"],)).fetchone()


def set_state(user_id, state, data=None):
    with closing(db()) as conn:
        conn.execute(
            "UPDATE users SET state = ?, state_data = ? WHERE id = ?",
            (state, json.dumps(data or {}, ensure_ascii=False), user_id),
        )
        conn.commit()


def clear_state(user_id):
    set_state(user_id, None, {})


def state_data(user):
    try:
        return json.loads(user["state_data"] or "{}")
    except json.JSONDecodeError:
        return {}


def mark_password_ok(user_id):
    with closing(db()) as conn:
        conn.execute("UPDATE users SET password_check = 1 WHERE id = ?", (user_id,))
        conn.commit()


def log_event(actor_user_id, event_type, number_id=None, details=None):
    with closing(db()) as conn:
        conn.execute(
            "INSERT INTO logs (actor_user_id, number_id, event_type, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (actor_user_id, number_id, event_type, details, now_iso()),
        )
        conn.commit()


def show_home(chat_id, user):
    lines = [
        "  Главное меню",
        f"👤 {user_handle(user)}",
        f"🎚️ Роль: {role_title(user['role'])}",
    ]
    
    entities = [
        {"type": "custom_emoji", "offset": 0, "length": 2, "custom_emoji_id": "5244711640343017057"}
    ]

    if user["role"] == ROLE_SUPPLIER:
        with closing(db()) as conn:
            added = conn.execute(
                "SELECT COUNT(*) count FROM numbers WHERE supplier_user_id = ?",
                (user["id"],),
            ).fetchone()["count"]
        lines.append(f"📱 Добавлено номеров: {added}")

    send_message(chat_id, "\n".join(lines), main_menu_keyboard(user), entities=entities)


def build_global_stats():
    with closing(db()) as conn:
        users_total = conn.execute("SELECT COUNT(*) count FROM users").fetchone()["count"]
        suppliers = conn.execute("SELECT COUNT(*) count FROM users WHERE role = ?", (ROLE_SUPPLIER,)).fetchone()["count"]
        operators = conn.execute("SELECT COUNT(*) count FROM users WHERE role = ?", (ROLE_OPERATOR,)).fetchone()["count"]
        blocked = conn.execute("SELECT COUNT(*) count FROM users WHERE is_blocked = 1").fetchone()["count"]
        total = conn.execute("SELECT COUNT(*) count FROM numbers").fetchone()["count"]
        issued = conn.execute("SELECT COUNT(*) count FROM logs WHERE event_type = 'number_taken'").fetchone()["count"]
        done = conn.execute("SELECT COUNT(*) count FROM logs WHERE event_type = 'number_done'").fetchone()["count"]
        failed = conn.execute("SELECT COUNT(*) count FROM logs WHERE event_type = 'number_failed'").fetchone()["count"]
        messages = conn.execute("SELECT COUNT(*) count FROM logs WHERE event_type = 'supplier_message_sent'").fetchone()["count"]
        repeat_messages = conn.execute("SELECT COUNT(*) count FROM logs WHERE event_type = 'repeat_message_requested'").fetchone()["count"]

    lines = [
        "📊 Статистика",
        f"👥 Пользователи: {users_total}",
        f"📦 Поставщики: {suppliers}",
        f"🎧 Операторы: {operators}",
        f"Заблокированы: {blocked}",
        "",
        f"📱 Общее кол-во номеров: {total}",
        f"📤 Выдано операторам: {issued}",
        f"✅ Встали: {done}",
        f"Не встали: {failed}",
        f"📩 Сообщений: {messages}",
        f"🔁 Повторов сообщений: {repeat_messages}",
    ]
    return "\n".join(lines)


def build_recent_numbers():
    with closing(db()) as conn:
        recent = conn.execute(
            """
            SELECT n.id, n.masked_number, n.status,
                   su.username supplier_username, op.username operator_username
            FROM numbers n
            JOIN users su ON su.id = n.supplier_user_id
            LEFT JOIN users op ON op.id = n.assigned_operator_user_id
            WHERE n.status IN (?, ?)
            ORDER BY n.id DESC
            LIMIT 15
            """,
            (STATUS_DONE, STATUS_FAILED),
        ).fetchall()
    lines = ["🧾 Последние номера:"]
    if not recent:
        lines.append("пока пусто")
        return "\n".join(lines)
    for row in recent:
        supplier = f"@{row['supplier_username']}" if row["supplier_username"] else "без @username"
        operator = f"@{row['operator_username']}" if row["operator_username"] else "-"
        if row["status"] == STATUS_DONE:
            result = "встал"
        elif row["status"] == STATUS_FAILED:
            result = "не встал"
        else:
            result = "не встал"
        lines.append(
            f"{row['masked_number']} | юзер {supplier} | взял {operator} | {result}"
        )
    return "\n".join(lines)


def build_auto_report():
    return build_recent_numbers()



def parse_iso_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def msk_time_text(value):
    dt = parse_iso_datetime(value)
    if not dt:
        return "-"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")


def status_title(status):
    if status == STATUS_DONE:
        return "встал"
    if status == STATUS_FAILED:
        return "не встал"
    if status == STATUS_ASSIGNED:
        return "в работе"
    if status == STATUS_AVAILABLE:
        return "в очереди"
    if status == STATUS_CANCELLED:
        return "отменен"
    return status


def parse_report_period(text):
    value = (text or "").strip().lower()
    today = datetime.now(timezone(timedelta(hours=3))).date()
    if value in {"all", "все", "за все", "всё"}:
        return None, None
    if value in {"today", "сегодня"}:
        return today, today
    if value in {"yesterday", "вчера"}:
        day = today - timedelta(days=1)
        return day, day
    if value in {"7", "7d", "7days", "неделя", "7 дней"}:
        return today - timedelta(days=6), today

    parts = re.findall(r"\d{4}-\d{2}-\d{2}", value)
    try:
        if len(parts) == 1:
            day = datetime.strptime(parts[0], "%Y-%m-%d").date()
            return day, day
        if len(parts) >= 2:
            start = datetime.strptime(parts[0], "%Y-%m-%d").date()
            end = datetime.strptime(parts[1], "%Y-%m-%d").date()
            if start > end:
                start, end = end, start
            return start, end
    except ValueError:
        return None, None
    return None, None


def report_filter_keyboard():
    return inline_keyboard([
        [("✅ Встали", "report:status:done"), ("❌ Не встали", "report:status:failed")],
        [("📊 Все", "report:status:all")],
        [back_row()],
    ])


def report_date_keyboard():
    return inline_keyboard([
        [("📅 Сегодня", "report:date:today"), ("📅 Вчера", "report:date:yesterday")],
        [("🗓️ 7 дней", "report:date:7"), ("📚 Все даты", "report:date:all")],
        [back_row()],
    ])


def build_report_csv(status_filter, date_from=None, date_to=None):
    statuses = [STATUS_DONE, STATUS_FAILED]
    if status_filter == "done":
        statuses = [STATUS_DONE]
    elif status_filter == "failed":
        statuses = [STATUS_FAILED]

    placeholders = ",".join("?" for _ in statuses)
    with closing(db()) as conn:
        rows = conn.execute(
            f"""
            SELECT n.masked_number, n.status, n.completed_at,
                   su.username supplier_username, su.public_id supplier_public_id,
                   op.username operator_username, op.public_id operator_public_id
            FROM numbers n
            JOIN users su ON su.id = n.supplier_user_id
            LEFT JOIN users op ON op.id = n.assigned_operator_user_id
            WHERE n.status IN ({placeholders})
            ORDER BY n.completed_at DESC, n.id DESC
            """,
            statuses,
        ).fetchall()

    lines = ["Номер;Кто сдал;Статус;Время МСК сдачи;Кто взял"]
    for row in rows:
        completed = parse_iso_datetime(row["completed_at"])
        if completed:
            local_date = completed.astimezone(timezone(timedelta(hours=3))).date() if completed.tzinfo else completed.date()
            if date_from and local_date < date_from:
                continue
            if date_to and local_date > date_to:
                continue
        elif date_from or date_to:
            continue
        supplier = f"@{row['supplier_username']}" if row["supplier_username"] else row["supplier_public_id"]
        operator = f"@{row['operator_username']}" if row["operator_username"] else (row["operator_public_id"] or "-")
        values = [row["masked_number"], supplier, status_title(row["status"]), msk_time_text(row["completed_at"]), operator]
        escaped = [str(value).replace(";", ",").replace("\n", " ") for value in values]
        lines.append(";".join(escaped))
    return ("\ufeff" + "\n".join(lines) + "\n").encode("utf-8")


def send_report_file(chat_id, admin):
    data = state_data(admin)
    status_filter = data.get("report_status", "all")
    date_from, date_to = parse_report_period(data.get("report_period", "all"))
    content = build_report_csv(status_filter, date_from, date_to)
    period = data.get("report_period", "all")
    filename = f"report_{status_filter}_{period.replace(' ', '_')}.csv"
    send_document_bytes(chat_id, filename, content, "📄 Отчет готов", admin_keyboard())
    clear_state(admin["id"])

def build_operator_stats():
    with closing(db()) as conn:
        rows = conn.execute(
            """
            SELECT
                u.username,
                SUM(CASE WHEN l.event_type = 'number_taken' THEN 1 ELSE 0 END) taken,
                SUM(CASE WHEN l.event_type = 'number_done' THEN 1 ELSE 0 END) done,
                SUM(CASE WHEN l.event_type = 'number_failed' THEN 1 ELSE 0 END) failed,
                SUM(CASE WHEN l.event_type = 'number_skipped' THEN 1 ELSE 0 END) skipped,
                SUM(CASE WHEN l.event_type = 'operator_repeat_requested' THEN 1 ELSE 0 END) repeats,
                SUM(CASE WHEN l.event_type = 'repeat_message_requested' THEN 1 ELSE 0 END) repeat_messages
            FROM users u
            LEFT JOIN logs l ON l.actor_user_id = u.id
            WHERE u.role = ?
            GROUP BY u.id
            ORDER BY taken DESC, done DESC
            LIMIT 30
            """,
            (ROLE_OPERATOR,),
        ).fetchall()
    lines = ["Статистика операторов"]
    if not rows:
        lines.append("Операторов пока нет.")
        return "\n".join(lines)
    for row in rows:
        taken = row["taken"] or 0
        done = row["done"] or 0
        handle = f"@{row['username']}" if row["username"] else "без @username"
        lines.append(
            f"{handle}: взял {taken}, встали {done}, не встали {row['failed'] or 0}, скипы {row['skipped'] or 0}, повторы {row['repeats'] or 0}, повторы сообщений {row['repeat_messages'] or 0}"
        )
    return "\n".join(lines)


def handle_start(chat_id, telegram_id, username=None):
    user = create_or_touch_user(telegram_id, username)
    if user["is_blocked"]:
        send_message(chat_id, "🚫 Доступ заблокирован.")
        return
    if user["password_check"]:
        clear_state(user["id"])
        show_home(chat_id, user)
        return
    
    set_state(user["id"], "login_password")
    
    # Текст с пробелами для эмодзи
    text = "  🔐 Введите пароль доступа."
    # Инструкция для телеграма
    entities = [{"type": "custom_emoji", "offset": 0, "length": 2, "custom_emoji_id": "5244711640343017057"}]
    
    send_message(chat_id, text, entities=entities)



def handle_admin_withdrawal_receipt(chat_id, admin, message):
    if not admin["is_admin"]:
        clear_state(admin["id"])
        send_message(chat_id, "⛔ Нет доступа.")
        return
    data = state_data(admin)
    withdrawal_id = data.get("withdrawal_id")
    with closing(db()) as conn:
        row = conn.execute(
            """
            SELECT w.*, u.telegram_id, u.username
            FROM withdrawals w
            JOIN users u ON u.id = w.user_id
            WHERE w.id = ? AND w.status = ?
            """,
            (withdrawal_id, WITHDRAWAL_PENDING),
        ).fetchone()
        if not row:
            clear_state(admin["id"])
            send_message(chat_id, "⚠️ Заявка на вывод уже обработана или не найдена.", admin_keyboard())
            return
        conn.execute(
            "UPDATE withdrawals SET status = ?, admin_message = ?, completed_at = ? WHERE id = ?",
            (WITHDRAWAL_DONE, "receipt", now_iso(), withdrawal_id),
        )
        conn.commit()
    copy_message(row["telegram_id"], chat_id, message["message_id"], caption=f"💸 Чек/ответ по выводу {money_text(row['amount'])}")
    delete_message(chat_id, message.get("message_id"))
    clear_state(admin["id"])
    log_event(admin["id"], "withdrawal_completed", details=f"withdrawal_id={withdrawal_id};receipt=1")
    send_message(chat_id, f"✅ Чек отправлен пользователю {user_handle(row)}. Заявка убрана из выводов.", admin_keyboard())


def handle_admin_direct_receipt(chat_id, admin, message):
    if not admin["is_admin"]:
        clear_state(admin["id"])
        send_message(chat_id, "⛔ Нет доступа.")
        return
    data = state_data(admin)
    target_user_id = data.get("target_user_id")
    with closing(db()) as conn:
        target = conn.execute("SELECT * FROM users WHERE id = ?", (target_user_id,)).fetchone()
    if not target:
        clear_state(admin["id"])
        send_message(chat_id, "⚠️ Пользователь не найден.", admin_keyboard())
        return
    copy_message(target["telegram_id"], chat_id, message["message_id"], caption="✉️ Сообщение от администратора")
    delete_message(chat_id, message.get("message_id"))
    clear_state(admin["id"])
    log_event(admin["id"], "admin_direct_message", details=f"to={target_user_id};copy=1")
    send_message(chat_id, f"✅ Сообщение отправлено пользователю {user_handle(target)}.", admin_keyboard())


def handle_non_text_message(message):
    chat_id = message["chat"]["id"]
    tg_from = message["from"]
    user = create_or_touch_user(tg_from["id"], extract_username(tg_from))
    if user["is_blocked"] or not user["password_check"]:
        return
    delete_state_prompt(chat_id, user)
    if user["state"] == "admin_withdrawal_message":
        handle_admin_withdrawal_receipt(chat_id, user, message)
    elif user["state"] == "admin_direct_message":
        handle_admin_direct_receipt(chat_id, user, message)

def handle_text(message):
    chat_id = message["chat"]["id"]
    tg_from = message["from"]
    telegram_id = tg_from["id"]
    username = extract_username(tg_from)
    raw_text = message.get("text") or ""
    text = raw_text.strip()
    entities = message.get("entities") or []
    delete_message(chat_id, message.get("message_id"))

    if text == "/start":
        handle_start(chat_id, telegram_id, username)
        return

    if text.startswith("/admin"):
        user = create_or_touch_user(telegram_id, username)
        clear_state(user["id"])
        if user["is_admin"]:
            send_message(chat_id, "🛠️ Админ-панель открывается кнопкой в главном меню.", main_menu_keyboard(user))
        else:
            send_message(chat_id, "⛔ Админ-доступ выдается только по Telegram ID в настройках бота.")
        return

    user = create_or_touch_user(telegram_id, username)
    if user["is_blocked"]:
        send_message(chat_id, "🚫 Доступ заблокирован.")
        return

    if user["state"] == "login_password":
        if text != get_access_password():
            send_message(chat_id, "❌ Пароль неверный.")
            return
        mark_password_ok(user["id"])
        clear_state(user["id"])
        show_home(chat_id, get_user(telegram_id))
        return

    if not user["password_check"]:
        send_message(chat_id, "🔐 Сначала войдите по паролю через /start.")
        return

    delete_state_prompt(chat_id, user)

    if user["state"] == "add_number":
        data = state_data(user)
        numbers, bad = parse_russian_numbers(text)
        if not numbers:
            prompt = send_message(
                chat_id,
                "➕ Отправьте российские номера списком, каждый с новой строки.\nПример:\n+79991234567\n89997654321",
                inline_keyboard([back_row()]),
            )
            set_state(user["id"], "add_number", {"prompt_message_id": prompt.get("message_id")})
            return
        with closing(db()) as conn:
            for number in numbers:
                conn.execute(
                    """
                    INSERT INTO numbers (supplier_user_id, masked_number, volume, remaining, status, created_at)
                    VALUES (?, ?, 1, 1, ?, ?)
                    """,
                    (user["id"], number, STATUS_AVAILABLE, now_iso()),
                )
            conn.commit()
        clear_state(user["id"])
        log_event(user["id"], "numbers_added", details=f"count={len(numbers)}")
        extra = f"\nНе добавлены: {', '.join(bad)}" if bad else ""
        send_message(chat_id, f"✅ Добавлено номеров: {len(numbers)}.{extra}", main_menu_keyboard(user))
        return


    if user["state"] == "supplier_message":
        save_supplier_message(chat_id, user, text)
        return

    if user["state"] in {"cancel_reason", "supplier_repeat_reason", "operator_repeat_reason", "fail_reason"}:
        save_reason(chat_id, user, text)
        return

    if user["state"] == "withdraw_amount":
        handle_withdraw_amount(chat_id, user, text)
        return

    if user["state"] == "admin_withdrawal_message":
        handle_admin_withdrawal_message(chat_id, user, text)
        return

    if user["state"] == "admin_report_date":
        data = state_data(user)
        data["report_period"] = text
        set_state(user["id"], "admin_report_date", data)
        send_report_file(chat_id, get_user(telegram_id))
        return

    if user["state"] == "admin_direct_message":
        handle_admin_direct_message(chat_id, user, raw_text, entities)
        return

    if user["state"] == "admin_change_password":
        handle_admin_change_password(chat_id, user, text)
        return

    if user["state"] in {"grant_operator", "block_user", "unblock_user"}:
        handle_admin_text_state(chat_id, user, text)
        return

    if user["state"] == "broadcast":
        handle_broadcast_text(chat_id, user, raw_text, entities)
        return

    text = "  👇 Используйте inline-кнопки ниже."
    entities = [{"type": "custom_emoji", "offset": 0, "length": 2, "custom_emoji_id": "5244711640343017057"}]
    
    send_message(chat_id, text, main_menu_keyboard(user), entities=entities)



def handle_withdraw_amount(chat_id, user, text):
    if user["role"] != ROLE_SUPPLIER:
        clear_state(user["id"])
        send_message(chat_id, "⛔ Вывод доступен только поставщикам.", main_menu_keyboard(user))
        return
    amount = parse_amount(text)
    if amount is None:
        send_state_prompt(chat_id, user["id"], "withdraw_amount", "💸 Введите сумму вывода числом, например: 1 или 2.50")
        return
    with closing(db()) as conn:
        balance, _ = supplier_balance(conn, user["id"])
        if amount > balance:
            send_message(chat_id, f"⚠️ Недостаточно средств. Доступно: {money_text(balance)}.")
            return
        conn.execute(
            "INSERT INTO withdrawals (user_id, amount, status, created_at) VALUES (?, ?, ?, ?)",
            (user["id"], amount, WITHDRAWAL_PENDING, now_iso()),
        )
        conn.commit()
    clear_state(user["id"])
    log_event(user["id"], "withdrawal_requested", details=f"amount={amount}")
    send_message(chat_id, f"💸 Заявка на вывод {money_text(amount)} создана. ⏳ Ожидайте вывода.", main_menu_keyboard(user))


def handle_admin_withdrawal_message(chat_id, admin, text):
    if not admin["is_admin"]:
        clear_state(admin["id"])
        send_message(chat_id, "⛔ Нет доступа.")
        return
    data = state_data(admin)
    withdrawal_id = data.get("withdrawal_id")
    with closing(db()) as conn:
        row = conn.execute(
            """
            SELECT w.*, u.telegram_id, u.username
            FROM withdrawals w
            JOIN users u ON u.id = w.user_id
            WHERE w.id = ? AND w.status = ?
            """,
            (withdrawal_id, WITHDRAWAL_PENDING),
        ).fetchone()
        if not row:
            clear_state(admin["id"])
            send_message(chat_id, "⚠️ Заявка на вывод уже обработана или не найдена.", admin_keyboard())
            return
        conn.execute(
            "UPDATE withdrawals SET status = ?, admin_message = ?, completed_at = ? WHERE id = ?",
            (WITHDRAWAL_DONE, text, now_iso(), withdrawal_id),
        )
        conn.commit()
    clear_state(admin["id"])
    log_event(admin["id"], "withdrawal_completed", details=f"withdrawal_id={withdrawal_id}")
    send_message(row["telegram_id"], f"💸 Ответ по выводу {money_text(row['amount'])}:\n{text}")
    send_message(chat_id, f"✅ Сообщение отправлено пользователю {user_handle(row)}. Заявка убрана из выводов.", admin_keyboard())

def save_reason(chat_id, user, text):
    data = state_data(user)
    number_id = data.get("number_id")
    reason = text[:500]
    state = user["state"]

    if state == "cancel_reason":
        status = STATUS_CANCELLED
        event = "number_cancelled"
        message = f"Номер #{number_id} отменен."
    elif state == "fail_reason":
        status = STATUS_FAILED
        event = "number_failed"
        message = f"Номер #{number_id}: отказ сохранен."
    elif state == "supplier_repeat_reason":
        status = None
        event = "supplier_repeat_requested"
        message = f"🔁 Повтор по номеру #{number_id} сохранен."
    else:
        status = None
        event = "operator_repeat_requested"
        message = f"🔁 Запрос повтора по номеру #{number_id} сохранен."

    with closing(db()) as conn:
        if status:
            conn.execute(
                "UPDATE numbers SET status = ?, last_reason = ?, completed_at = ? WHERE id = ?",
                (status, reason, now_iso(), number_id),
            )
        else:
            conn.execute("UPDATE numbers SET last_reason = ? WHERE id = ?", (reason, number_id))
        row = conn.execute("SELECT supplier_user_id, assigned_operator_user_id FROM numbers WHERE id = ?", (number_id,)).fetchone()
        supplier = conn.execute("SELECT telegram_id FROM users WHERE id = ?", (row["supplier_user_id"],)).fetchone() if row else None
        operator = conn.execute("SELECT telegram_id FROM users WHERE id = ?", (row["assigned_operator_user_id"],)).fetchone() if row and row["assigned_operator_user_id"] else None
        conn.commit()

    clear_state(user["id"])
    log_event(user["id"], event, number_id, reason)
    send_message(chat_id, f"{message}\nПричина: {reason}", main_menu_keyboard(user))
    if state == "operator_repeat_reason" and supplier:
        send_message(supplier["telegram_id"], f"🔁 По номеру #{number_id} оператор запросил повтор.\nПричина: {reason}", supplier_number_keyboard(number_id))
    if state == "supplier_repeat_reason" and operator:
        send_message(operator["telegram_id"], f"🔁 По номеру #{number_id} поставщик запросил повтор.\nПричина: {reason}", operator_active_keyboard(number_id))
    if state == "fail_reason" and supplier:
        send_message(supplier["telegram_id"], f"По номеру #{number_id}: отказ.\nПричина: {reason}")


def save_supplier_message(chat_id, user, text):
    data = state_data(user)
    number_id = data.get("number_id")
    message = text.strip()
    if not message:
        send_state_prompt(chat_id, user["id"], "supplier_message", "Введите непустое сообщение для оператора.", {"number_id": number_id})
        return

    with closing(db()) as conn:
        row = conn.execute(
            "SELECT supplier_user_id, assigned_operator_user_id FROM numbers WHERE id = ?",
            (number_id,),
        ).fetchone()
        if not row or row["supplier_user_id"] != user["id"] or not row["assigned_operator_user_id"]:
            clear_state(user["id"])
            send_message(chat_id, "Номер не найден или уже не активен.", main_menu_keyboard(user))
            return
        operator = conn.execute("SELECT telegram_id FROM users WHERE id = ?", (row["assigned_operator_user_id"],)).fetchone()

    clear_state(user["id"])
    log_event(user["id"], "supplier_message_sent", number_id, "sent")
    send_message(chat_id, f"Сообщение по номеру #{number_id} отправлено оператору.", main_menu_keyboard(user))
    if operator:
        send_message(
            operator["telegram_id"],
            f"Сообщение по номеру #{number_id}:\n{message}",
            operator_active_keyboard(number_id),
        )


def handle_admin_text_state(chat_id, admin, text):
    if not admin["is_admin"]:
        send_message(chat_id, "⛔ Нет доступа.")
        return
    target = get_user_by_handle(text)
    if not target:
        clear_state(admin["id"])
        send_message(chat_id, "Пользователь не найден. Укажите @username.", admin_keyboard())
        return

    if admin["state"] == "grant_operator":
        set_user_role(target["id"], ROLE_OPERATOR)
        action_text = f"🎧 Пользователю {user_handle(target)} выдана роль оператора."
    elif admin["state"] == "block_user":
        with closing(db()) as conn:
            conn.execute("UPDATE users SET is_blocked = 1 WHERE id = ?", (target["id"],))
            conn.commit()
        action_text = f"Пользователь {user_handle(target)} заблокирован."
    else:
        with closing(db()) as conn:
            conn.execute("UPDATE users SET is_blocked = 0 WHERE id = ?", (target["id"],))
            conn.commit()
        action_text = f"✅ Пользователь {user_handle(target)} разблокирован."

    log_event(admin["id"], admin["state"], details=user_handle(target))
    clear_state(admin["id"])
    send_message(chat_id, action_text, admin_keyboard())


def handle_broadcast_text(chat_id, admin, text, entities=None):
    if not admin["is_admin"]:
        send_message(chat_id, "⛔ Нет доступа.")
        return
    sent = 0
    with closing(db()) as conn:
        rows = conn.execute("SELECT telegram_id FROM users WHERE is_blocked = 0 AND password_check = 1").fetchall()
    for row in rows:
        try:
            send_message(row["telegram_id"], text, entities=entities)
            sent += 1
        except Exception:
            pass
    clear_state(admin["id"])
    log_event(admin["id"], "broadcast", details=f"sent={sent}")
    send_message(chat_id, f"📣 Рассылка отправлена: {sent}.", admin_keyboard())


def handle_callback(callback):
    callback_id = callback["id"]
    chat_id = callback["message"]["chat"]["id"]
    tg_from = callback["from"]
    telegram_id = tg_from["id"]
    data = callback["data"]
    delete_message(chat_id, callback["message"]["message_id"])
    user = create_or_touch_user(telegram_id, extract_username(tg_from))
    if user["is_blocked"] or not user["password_check"]:
        answer_callback(callback_id, "Сначала войдите по паролю.", True)
        return

    if data.startswith("menu:"):
        handle_menu_callback(callback_id, chat_id, user, data)
        return
    if data.startswith("supplier:"):
        handle_supplier_callback(callback_id, chat_id, user, data)
        return
    if data.startswith("operator:"):
        handle_operator_callback(callback_id, chat_id, user, data)
        return
    if data.startswith("admin:"):
        handle_admin_callback(callback_id, chat_id, user, data)
        return
    if data.startswith("queue:"):
        handle_queue_callback(callback_id, chat_id, user, data)
        return
    if data.startswith("withdrawal:"):
        handle_withdrawal_callback(callback_id, chat_id, user, data)
        return
    if data.startswith("report:"):
        handle_report_callback(callback_id, chat_id, user, data)
        return
    if data.startswith("usermsg:"):
        handle_user_message_callback(callback_id, chat_id, user, data)
        return
    if data.startswith("db:"):
        handle_db_callback(callback_id, chat_id, user, data)
        return
    answer_callback(callback_id)


def handle_menu_callback(callback_id, chat_id, user, data):
    action = data.split(":", 1)[1]
    if action == "home":
        clear_state(user["id"])
        show_home(chat_id, get_user(user["telegram_id"]) or user)
    elif action == "profile":
        show_profile(chat_id, user)
    elif action == "wallet":
        show_wallet(chat_id, user)
    elif action == "withdraw":
        withdraw_start(chat_id, user)
    elif action == "add_number":
        add_number_start(chat_id, user)
    elif action == "my_numbers":
        show_my_numbers(chat_id, user)
    elif action == "my_queue":
        show_my_queue(chat_id, user)
    elif action == "take_number":
        take_number(chat_id, user)
    elif action == "admin":
        if user["is_admin"]:
            send_message(chat_id, "🛠️ Админ-панель", admin_keyboard())
        else:
            send_message(chat_id, "⛔ Нет доступа.", main_menu_keyboard(user))
    answer_callback(callback_id)


def show_profile(chat_id, user):
    with closing(db()) as conn:
        added = conn.execute("SELECT COUNT(*) count FROM numbers WHERE supplier_user_id = ?", (user["id"],)).fetchone()["count"]
        taken = conn.execute("SELECT COUNT(*) count FROM logs WHERE actor_user_id = ? AND event_type = 'number_taken'", (user["id"],)).fetchone()["count"]
        done = conn.execute("SELECT COUNT(*) count FROM logs WHERE actor_user_id = ? AND event_type = 'number_done'", (user["id"],)).fetchone()["count"]
        failed = conn.execute("SELECT COUNT(*) count FROM logs WHERE actor_user_id = ? AND event_type = 'number_failed'", (user["id"],)).fetchone()["count"]
    lines = [
        "👤 Профиль",
        f"👤 Username: {user_handle(user)}",
        f"🎚️ Роль: {role_title(user['role'])}",
    ]
    if user["role"] == ROLE_SUPPLIER:
        lines.append(f"📱 Добавлено номеров: {added}")
    if user["role"] == ROLE_OPERATOR:
        lines.extend([
            f"📲 Взято номеров: {taken}",
            f"✅ Встали: {done}",
            f"❌ Не встали: {failed}",
        ])
    text = "\n".join(lines)
    send_message(chat_id, text, inline_keyboard([back_row()]))


def show_wallet(chat_id, user):
    if user["role"] != ROLE_SUPPLIER:
        send_message(chat_id, "⛔ Кошелек доступен только поставщикам.", main_menu_keyboard(user))
        return
    with closing(db()) as conn:
        balance, supplier_done = supplier_balance(conn, user["id"])
    lines = [
        "💎 Кошелек",
        f"✅ Встало номеров: {supplier_done}",
        f"💰 Мой баланс: {money_text(balance)}",
    ]
    send_message(chat_id, "\n".join(lines), inline_keyboard([[('💸 Вывод', 'menu:withdraw')], back_row()]))


def withdraw_start(chat_id, user):
    if user["role"] != ROLE_SUPPLIER:
        send_message(chat_id, "⛔ Вывод доступен только поставщикам.", main_menu_keyboard(user))
        return
    with closing(db()) as conn:
        balance, _ = supplier_balance(conn, user["id"])
    if balance <= 0:
        send_message(chat_id, "💰 Баланс пока 0$. Вывод станет доступен после вставших номеров.", inline_keyboard([back_row()]))
        return
    send_state_prompt(chat_id, user["id"], "withdraw_amount", f"💸 Введите сумму вывода. Доступно: {money_text(balance)}.")


def number_button_rows(rows, prefix):
    keyboard_rows = []
    for row in rows:
        keyboard_rows.append([(f"📱 {row['masked_number']}", f"{prefix}:{row['id']}")])
    keyboard_rows.append(back_row())
    return keyboard_rows


def show_global_queue(chat_id):
    with closing(db()) as conn:
        total = conn.execute(
            "SELECT COUNT(*) count FROM numbers WHERE status = ? AND remaining > 0",
            (STATUS_AVAILABLE,),
        ).fetchone()["count"]
        rows = conn.execute(
            """
            SELECT id, masked_number
            FROM numbers
            WHERE status = ? AND remaining > 0
            ORDER BY created_at ASC
            LIMIT 50
            """,
            (STATUS_AVAILABLE,),
        ).fetchall()
    if not rows:
        send_message(chat_id, "🌐 Общая очередь пуста. 📭", inline_keyboard([back_row()]))
        return
    send_message(
        chat_id,
        f"🌐 Общая очередь в боте\n📦 Всего номеров в очереди: {total}\n👇 Нажмите номер, чтобы убрать его из очереди.",
        inline_keyboard(number_button_rows(rows, "queue:clear")),
    )


def show_withdrawals(chat_id):
    with closing(db()) as conn:
        rows = conn.execute(
            """
            SELECT w.id, w.amount, u.username, u.public_id
            FROM withdrawals w
            JOIN users u ON u.id = w.user_id
            WHERE w.status = ?
            ORDER BY w.created_at ASC
            LIMIT 50
            """,
            (WITHDRAWAL_PENDING,),
        ).fetchall()
    if not rows:
        send_message(chat_id, "💸 Ожидающих выводов нет. ✅", admin_keyboard())
        return
    buttons = []
    for row in rows:
        handle = f"@{row['username']}" if row["username"] else row["public_id"]
        buttons.append([(f"💸 {handle} — {money_text(row['amount'])}", f"withdrawal:select:{row['id']}")])
    buttons.append(back_row())
    send_message(chat_id, "💸 Выводы на обработке:\n👇 Выберите заявку.", inline_keyboard(buttons))


def show_user_picker(chat_id, callback_prefix, title):
    with closing(db()) as conn:
        rows = conn.execute(
            """
            SELECT id, username, public_id, role
            FROM users
            WHERE is_blocked = 0
            ORDER BY id DESC
            LIMIT 50
            """
        ).fetchall()
    if not rows:
        send_message(chat_id, "👤 Пользователей пока нет.", admin_keyboard())
        return
    buttons = []
    for row in rows:
        handle = f"@{row['username']}" if row["username"] else row["public_id"]
        buttons.append([(f"👤 {handle} ({role_title(row['role'])})", f"{callback_prefix}:select:{row['id']}")])
    buttons.append(back_row())
    send_message(chat_id, title, inline_keyboard(buttons))


def handle_admin_direct_message(chat_id, admin, text, entities=None):
    if not admin["is_admin"]:
        clear_state(admin["id"])
        send_message(chat_id, "⛔ Нет доступа.")
        return
    data = state_data(admin)
    target_user_id = data.get("target_user_id")
    with closing(db()) as conn:
        target = conn.execute("SELECT * FROM users WHERE id = ?", (target_user_id,)).fetchone()
    if not target:
        clear_state(admin["id"])
        send_message(chat_id, "⚠️ Пользователь не найден.", admin_keyboard())
        return
    prefix = "Сообщение от администратора:\n"
    shifted_entities = []
    for entity in entities or []:
        shifted = dict(entity)
        shifted["offset"] = shifted.get("offset", 0) + len(prefix)
        shifted_entities.append(shifted)
    send_message(target["telegram_id"], prefix + text, entities=shifted_entities)
    clear_state(admin["id"])
    log_event(admin["id"], "admin_direct_message", details=f"to={target_user_id}")
    send_message(chat_id, f"✅ Сообщение отправлено пользователю {user_handle(target)}.", admin_keyboard())



def handle_admin_change_password(chat_id, admin, text):
    if not admin["is_admin"]:
        clear_state(admin["id"])
        send_message(chat_id, "⛔ Нет доступа.")
        return
    new_password = (text or "").strip()
    if len(new_password) < 4:
        send_state_prompt(chat_id, admin["id"], "admin_change_password", "⚠️ Пароль должен быть минимум 4 символа. Введите другой пароль.")
        return
    set_access_password(new_password)
    clear_state(admin["id"])
    log_event(admin["id"], "admin_change_password", details="changed")
    send_message(chat_id, "🔐 Пароль доступа к боту изменен.", admin_keyboard())

def clear_database():
    with closing(db()) as conn:
        for table in ("numbers", "logs", "withdrawals", "users", "settings"):
            conn.execute(f"DELETE FROM {table}")
        conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('numbers', 'logs', 'withdrawals', 'users')")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('auto_reports_enabled', '0')")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('access_password', ?)", (ADMIN_PASSWORD,))
        conn.commit()

def add_number_start(chat_id, user):
    if user["role"] != ROLE_SUPPLIER:
        send_message(chat_id, "Добавлять номера может только поставщик.", main_menu_keyboard(user))
        return
    prompt = send_message(
        chat_id,
        "➕ Отправьте российские номера списком, каждый с новой строки.\nПример:\n+79991234567\n89997654321",
        inline_keyboard([back_row()]),
    )
    set_state(user["id"], "add_number", {"prompt_message_id": prompt.get("message_id")})


def show_my_numbers(chat_id, user):
    if user["role"] != ROLE_SUPPLIER:
        send_message(chat_id, "Раздел доступен поставщикам.", main_menu_keyboard(user))
        return
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT * FROM numbers WHERE supplier_user_id = ? ORDER BY id DESC LIMIT 10",
            (user["id"],),
        ).fetchall()
    if not rows:
        send_message(chat_id, "Пока номеров нет.", inline_keyboard([back_row()]))
        return
    send_message(chat_id, "📱 Последние ваши номера:", inline_keyboard([back_row()]))
    for row in rows:
        send_message(
            chat_id,
            f"#{row['id']} {row['masked_number']}\nСтатус: {row['status']}\nОсталось: {row['remaining']} из {row['volume']}\nПричина: {row['last_reason'] or '-'}",
            supplier_number_keyboard(row["id"]),
        )


def show_my_queue(chat_id, user):
    if user["role"] == ROLE_SUPPLIER:
        with closing(db()) as conn:
            total = conn.execute(
                "SELECT COUNT(*) count FROM numbers WHERE supplier_user_id = ? AND status = ? AND remaining > 0",
                (user["id"], STATUS_AVAILABLE),
            ).fetchone()["count"]
            rows = conn.execute(
                """
                SELECT id, masked_number
                FROM numbers
                WHERE supplier_user_id = ? AND status = ? AND remaining > 0
                ORDER BY created_at ASC
                LIMIT 50
                """,
                (user["id"], STATUS_AVAILABLE),
            ).fetchall()
        if not rows:
            send_message(chat_id, "📦 В вашей очереди нет доступных номеров. 📭", inline_keyboard([back_row()]))
            return
        send_message(
            chat_id,
            f"📦 Моя очередь\n🔢 Всего номеров в очереди: {total}\n👇 Нажмите номер, чтобы убрать его из очереди.",
            inline_keyboard(number_button_rows(rows, "queue:clear")),
        )
        return

    if user["role"] == ROLE_OPERATOR:
        show_active_number(chat_id, user)
        return

    send_message(chat_id, "📦 Очередь доступна поставщикам и операторам.", main_menu_keyboard(user))


def take_number(chat_id, user):
    if user["role"] != ROLE_OPERATOR:
        send_message(chat_id, "Брать номера может только оператор.", main_menu_keyboard(user))
        return
    with closing(db()) as conn:
        active = conn.execute(
            """
            SELECT * FROM numbers
            WHERE assigned_operator_user_id = ? AND status = ?
            ORDER BY assigned_at DESC LIMIT 1
            """,
            (user["id"], STATUS_ASSIGNED),
        ).fetchone()
        if active:
            send_message(chat_id, "У вас уже есть активный номер.", operator_active_keyboard(active["id"]))
            return
        row = conn.execute(
            """
            SELECT * FROM numbers
            WHERE status = ? AND remaining > 0
            ORDER BY created_at ASC LIMIT 1
            """,
            (STATUS_AVAILABLE,),
        ).fetchone()
        if not row:
            send_message(chat_id, "Сейчас свободных номеров нет.", main_menu_keyboard(user))
            return
        conn.execute(
            "UPDATE numbers SET status = ?, assigned_operator_user_id = ?, assigned_at = ? WHERE id = ?",
            (STATUS_ASSIGNED, user["id"], now_iso(), row["id"]),
        )
        supplier = conn.execute("SELECT id, telegram_id FROM users WHERE id = ?", (row["supplier_user_id"],)).fetchone()
        conn.commit()

    log_event(user["id"], "number_taken", row["id"])
    send_message(chat_id, f"📲 Номер #{row['id']} взят.\nНомер: {row['masked_number']}\nОжидайте сообщение от поставщика.", operator_active_keyboard(row["id"]))
    if supplier:
        send_state_prompt(
            supplier["telegram_id"],
            supplier["id"],
            "supplier_message",
            f"📩 Ваш номер #{row['id']} взяли.\nВведите сообщение для оператора.",
            {"number_id": row["id"]},
            supplier_number_keyboard(row["id"]),
        )


def show_active_number(chat_id, user):
    with closing(db()) as conn:
        row = conn.execute(
            """
            SELECT * FROM numbers
            WHERE assigned_operator_user_id = ? AND status = ?
            ORDER BY assigned_at DESC LIMIT 1
            """,
            (user["id"], STATUS_ASSIGNED),
        ).fetchone()
    if not row:
        send_message(chat_id, "Активного номера нет.", main_menu_keyboard(user))
        return
    send_message(chat_id, f"📦 Активный номер #{row['id']}\nНомер: {row['masked_number']}\nСтатус: {row['status']}", operator_active_keyboard(row["id"]))


def handle_supplier_callback(callback_id, chat_id, user, data):
    if user["role"] != ROLE_SUPPLIER:
        answer_callback(callback_id, "⛔ Нет доступа.", True)
        return
    _, action, raw_id = data.split(":")
    number_id = int(raw_id)
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM numbers WHERE id = ?", (number_id,)).fetchone()
        if not row or row["supplier_user_id"] != user["id"]:
            answer_callback(callback_id, "Нет доступа к номеру.", True)
            return
        if action == "repeat":
            send_state_prompt(chat_id, user["id"], "supplier_repeat_reason", "🔁 Укажите причину повтора.", {"number_id": number_id})
        elif action == "cancel":
            send_state_prompt(chat_id, user["id"], "cancel_reason", "❌ Укажите причину отмены.", {"number_id": number_id})
    answer_callback(callback_id)


def handle_operator_callback(callback_id, chat_id, user, data):
    if user["role"] != ROLE_OPERATOR:
        answer_callback(callback_id, "⛔ Нет доступа.", True)
        return
    _, action, raw_id = data.split(":")
    number_id = int(raw_id)
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM numbers WHERE id = ?", (number_id,)).fetchone()
        if not row or row["assigned_operator_user_id"] != user["id"]:
            answer_callback(callback_id, "Нет доступа к номеру.", True)
            return
        supplier = conn.execute("SELECT id, telegram_id FROM users WHERE id = ?", (row["supplier_user_id"],)).fetchone()
        if action == "done":
            conn.execute(
                """
                UPDATE numbers
                SET status = ?, remaining = 0, completed_at = ?
                WHERE id = ?
                """,
                (STATUS_DONE, now_iso(), number_id),
            )
            conn.commit()
            log_event(user["id"], "number_done", number_id)
            send_message(chat_id, f"✅ Номер #{number_id}: успех сохранен.", main_menu_keyboard(user))
            if supplier:
                send_message(supplier["telegram_id"], f"✅ По номеру #{number_id}: успех.")
        elif action == "repeat_message":
            conn.commit()
            log_event(user["id"], "repeat_message_requested", number_id)
            send_message(chat_id, f"🔁 По номеру #{number_id} запрошен повтор сообщения.", operator_active_keyboard(number_id))
            if supplier:
                send_state_prompt(
                    supplier["telegram_id"],
                    supplier["id"],
                    "supplier_message",
                    f"🔁 Оператор запросил повтор сообщения по номеру #{number_id}.\nВведите новое сообщение.",
                    {"number_id": number_id},
                    supplier_number_keyboard(number_id),
                )
        elif action == "repeat":
            send_state_prompt(chat_id, user["id"], "operator_repeat_reason", "🔁 Укажите причину повтора: неверный формат, не пришел код или другая причина.", {"number_id": number_id})
        elif action == "skip":
            conn.execute(
                "UPDATE numbers SET status = ?, assigned_operator_user_id = NULL, assigned_at = NULL WHERE id = ?",
                (STATUS_AVAILABLE, number_id),
            )
            conn.commit()
            log_event(user["id"], "number_skipped", number_id)
            send_message(chat_id, f"⏭️ Номер #{number_id} возвращен в очередь.", main_menu_keyboard(user))
        elif action == "failed":
            send_state_prompt(chat_id, user["id"], "fail_reason", "❌ Укажите причину, почему не встал.", {"number_id": number_id})
    answer_callback(callback_id)



def handle_queue_callback(callback_id, chat_id, user, data):
    parts = data.split(":")
    if len(parts) != 3 or parts[1] != "clear":
        answer_callback(callback_id)
        return
    number_id = int(parts[2])
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM numbers WHERE id = ?", (number_id,)).fetchone()
        if not row or row["status"] != STATUS_AVAILABLE:
            answer_callback(callback_id, "Номер уже не в очереди.", True)
            return
        if not user["is_admin"] and row["supplier_user_id"] != user["id"]:
            answer_callback(callback_id, "Нет доступа к номеру.", True)
            return
        conn.execute(
            "UPDATE numbers SET status = ?, remaining = 0, completed_at = ?, last_reason = ? WHERE id = ?",
            (STATUS_CANCELLED, now_iso(), "Убран из очереди", number_id),
        )
        conn.commit()
    log_event(user["id"], "queue_number_removed", number_id, "removed_from_queue")
    answer_callback(callback_id, "Номер убран из очереди.")
    send_message(chat_id, f"🧹 Номер {row['masked_number']} убран из очереди.", main_menu_keyboard(user) if not user["is_admin"] else admin_keyboard())


def handle_withdrawal_callback(callback_id, chat_id, user, data):
    if not user["is_admin"]:
        answer_callback(callback_id, "⛔ Нет доступа.", True)
        return
    parts = data.split(":")
    if len(parts) != 3 or parts[1] != "select":
        answer_callback(callback_id)
        return
    withdrawal_id = int(parts[2])
    with closing(db()) as conn:
        row = conn.execute(
            """
            SELECT w.*, u.username, u.public_id
            FROM withdrawals w
            JOIN users u ON u.id = w.user_id
            WHERE w.id = ? AND w.status = ?
            """,
            (withdrawal_id, WITHDRAWAL_PENDING),
        ).fetchone()
    if not row:
        answer_callback(callback_id, "Заявка уже обработана.", True)
        return
    handle = f"@{row['username']}" if row["username"] else row["public_id"]
    send_state_prompt(chat_id, user["id"], "admin_withdrawal_message", f"💸 Вывод {money_text(row['amount'])} для {handle}.\n✍️ Введите сообщение или чек, который нужно отправить пользователю.", {"withdrawal_id": withdrawal_id})
    answer_callback(callback_id)


def handle_report_callback(callback_id, chat_id, user, data):
    if not user["is_admin"]:
        answer_callback(callback_id, "⛔ Нет доступа.", True)
        return
    parts = data.split(":")
    if len(parts) < 3:
        answer_callback(callback_id)
        return
    if parts[1] == "status":
        status_filter = parts[2]
        set_state(user["id"], "admin_report_date", {"report_status": status_filter})
        send_message(
            chat_id,
            "📅 Выберите дату для отчета или отправьте текстом:\n"
            "• today / сегодня\n"
            "• yesterday / вчера\n"
            "• 7 дней\n"
            "• all / все\n"
            "• 2026-05-29\n"
            "• 2026-05-01 2026-05-29",
            report_date_keyboard(),
        )
    elif parts[1] == "date":
        data_state = state_data(user)
        data_state["report_period"] = parts[2]
        set_state(user["id"], "admin_report_date", data_state)
        send_report_file(chat_id, get_user(user["telegram_id"]))
    answer_callback(callback_id)


def handle_user_message_callback(callback_id, chat_id, user, data):
    if not user["is_admin"]:
        answer_callback(callback_id, "⛔ Нет доступа.", True)
        return
    parts = data.split(":")
    if len(parts) != 3 or parts[1] != "select":
        answer_callback(callback_id)
        return
    target_user_id = int(parts[2])
    with closing(db()) as conn:
        target = conn.execute("SELECT * FROM users WHERE id = ?", (target_user_id,)).fetchone()
    if not target:
        answer_callback(callback_id, "Пользователь не найден.", True)
        return
    send_state_prompt(chat_id, user["id"], "admin_direct_message", f"✉️ Введите сообщение для пользователя {user_handle(target)}.", {"target_user_id": target_user_id})
    answer_callback(callback_id)


def handle_db_callback(callback_id, chat_id, user, data):
    if not user["is_admin"]:
        answer_callback(callback_id, "⛔ Нет доступа.", True)
        return
    if data == "db:clear_cancel":
        send_message(chat_id, "✅ Очистка базы отменена.", admin_keyboard())
        answer_callback(callback_id)
        return
    if data == "db:clear_confirm":
        clear_database()
        send_message(chat_id, "🧨 База очищена: пользователи, номера, логи и выводы удалены.", None)
        answer_callback(callback_id)
        return
    answer_callback(callback_id)

def handle_admin_callback(callback_id, chat_id, user, data):
    if not user["is_admin"]:
        answer_callback(callback_id, "⛔ Нет доступа.", True)
        return
    action = data.split(":")[1]
    if action == "stats":
        send_message(chat_id, build_global_stats(), admin_keyboard())
    elif action == "operator_stats":
        send_message(chat_id, build_operator_stats(), admin_keyboard())
    elif action == "auto_report":
        send_message(chat_id, build_auto_report(), admin_keyboard())
    elif action == "report_file":
        set_state(user["id"], "admin_report_date", {"report_status": "all", "report_period": "all"})
        send_report_file(chat_id, get_user(user["telegram_id"]))
    elif action == "withdrawals":
        show_withdrawals(chat_id)
    elif action == "global_queue":
        show_global_queue(chat_id)
    elif action == "direct_message":
        show_user_picker(chat_id, "usermsg", "✉️ Выберите пользователя, которому написать:")
    elif action == "change_password":
        send_state_prompt(chat_id, user["id"], "admin_change_password", "🔐 Введите новый пароль доступа к боту.")
    elif action == "clear_db":
        send_message(
            chat_id,
            "🧨 Точно очистить всю базу? Будут удалены пользователи, номера, логи и выводы.",
            inline_keyboard([[("✅ Да, очистить", "db:clear_confirm")], [("❌ Отмена", "db:clear_cancel")]]),
        )
    elif action == "grant_operator":
        send_state_prompt(chat_id, user["id"], "grant_operator", "🎧 Введите @username пользователя, которому выдать роль оператора.")
    elif action == "reset_queue":
        with closing(db()) as conn:
            conn.execute(
                """
                UPDATE numbers
                SET status = ?, assigned_operator_user_id = NULL, assigned_at = NULL
                WHERE status = ?
                """,
                (STATUS_AVAILABLE, STATUS_ASSIGNED),
            )
            conn.commit()
        log_event(user["id"], "queue_reset")
        send_message(chat_id, "♻️ Очередь сброшена: активные заявки возвращены в доступные.", admin_keyboard())
    elif action == "broadcast":
        send_state_prompt(chat_id, user["id"], "broadcast", "📣 Введите текст рассылки. Можно использовать Telegram Premium emoji.")
    elif action == "block":
        send_state_prompt(chat_id, user["id"], "block_user", "🚫 Введите @username пользователя для блокировки.")
    elif action == "unblock":
        send_state_prompt(chat_id, user["id"], "unblock_user", "✅ Введите @username пользователя для разблокировки.")
    answer_callback(callback_id)


def send_auto_reports_if_needed(state):
    # Автоотчеты отключены: отчет отправляется только по кнопке «📄 Отчет файлом».
    return


def poll():
    init_db()
    offset = 0
    runtime_state = {"last_auto_report_at": time.time()}
    print("Bot started.")
    while True:
        try:
            updates = api("getUpdates", {"offset": offset, "timeout": 50})
            for update in updates:
                offset = update["update_id"] + 1
                if "message" in update and "text" in update["message"]:
                    handle_text(update["message"])
                elif "message" in update:
                    handle_non_text_message(update["message"])
                elif "callback_query" in update:
                    handle_callback(update["callback_query"])
            # Автоотчеты намеренно не отправляем: отчет доступен только по кнопке «📄 Отчет файлом».
        except KeyboardInterrupt:
            print("Bot stopped.")
            break
        except Exception as exc:
            print(f"Error: {exc}")
            time.sleep(3)


if __name__ == "__main__":
    poll()
