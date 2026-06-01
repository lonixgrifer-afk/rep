import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import closing 
from datetime import datetime, timedelta, timezone


# Один файл, без requirements.txt и .env.
# Заполните перед запуском. Можно также передать через переменные окружения.
BOT_TOKEN = os.getenv("BOT_TOKEN", "8854265866:AAGMNw0kEatEuk-DZWbdVUSu98kL-hulQ-g")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "change-this-password")
DB_PATH = os.getenv("DB_PATH", "bot.db")

DROP_GROUP_CHAT_ID = int(os.getenv("DROP_GROUP_CHAT_ID", "0") or 0)
DROP_GROUP_THREAD_ID = int(os.getenv("DROP_GROUP_THREAD_ID", "0") or 0)
OPERATOR_GROUP_CHAT_ID = int(os.getenv("OPERATOR_GROUP_CHAT_ID", "0") or 0)
OPERATOR_GROUP_THREAD_ID = int(os.getenv("OPERATOR_GROUP_THREAD_ID", "0") or 0)

# JSON-словарь для премиум-эмодзи в inline-кнопках актуального Bot API.
# Ключ — callback_data кнопки или ее текст, значение — custom_emoji_id.
# Пример: BUTTON_CUSTOM_EMOJI_IDS='{"menu:admin":"5368324170671202286","Назад":"5368324170671202286"}'
BUTTON_CUSTOM_EMOJI_IDS_JSON = os.getenv("BUTTON_CUSTOM_EMOJI_IDS", "{}")

# Премиум-эмодзи для всех кнопок «Назад».
BACK_BUTTON_CUSTOM_EMOJI_ID = os.getenv("BACK_BUTTON_CUSTOM_EMOJI_ID", "5427242965829457646")

# Необязательно: JSON-словарь стилей кнопок Bot API: danger, success или primary.
BUTTON_STYLES_JSON = os.getenv("BUTTON_STYLES", "{}")

# Если список пустой, первый вошедший пользователь автоматически станет админом.
ADMIN_TELEGRAM_IDS = [8684253040]

# Отдельные Telegram ID, которым разрешена команда /give в группе операторов.
# Формат переменной окружения GIVE_TELEGRAM_IDS: "123,456". Если список пустой, /give доступна админам.
def parse_id_list(value):
    ids = []
    for part in str(value or "").replace(";", ",").split(","):
        part = part.strip()
        if part.lstrip("-").isdigit():
            ids.append(int(part))
    return ids

GIVE_TELEGRAM_IDS = parse_id_list(os.getenv("GIVE_TELEGRAM_IDS", "8949311928"))
ANONYMOUS_ADMIN_TELEGRAM_ID = 1087968824

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


class TelegramAPIError(RuntimeError):
    def __init__(self, method, status_code=None, description=None, response=None):
        self.method = method
        self.status_code = status_code
        self.description = description or str(response)
        self.response = response
        super().__init__(f"Telegram API {method} failed: {self.description}")


def is_bad_request(exc):
    return isinstance(exc, TelegramAPIError) and exc.status_code == 400


def is_conflict_error(exc):
    return isinstance(exc, TelegramAPIError) and exc.status_code == 409


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
                last_reason TEXT,
                source_chat_id INTEGER,
                source_thread_id INTEGER,
                source_message_id INTEGER,
                operator_chat_id INTEGER,
                operator_thread_id INTEGER,
                operator_message_id INTEGER,
                pending_operator_user_id INTEGER
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

            CREATE TABLE IF NOT EXISTS group_members (
                chat_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL DEFAULT 0,
                telegram_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, thread_id, telegram_id)
            );

            CREATE TABLE IF NOT EXISTS operator_groups (
                chat_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL DEFAULT 0,
                bound_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, thread_id)
            );
            """
        )
        migrate_schema(conn)
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('auto_reports_enabled', '0')")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('price_per_number', '1')")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('work_enabled', '0')")
        if DROP_GROUP_CHAT_ID:
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('drop_group_chat_id', ?)", (DROP_GROUP_CHAT_ID,))
        if DROP_GROUP_THREAD_ID:
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('drop_group_thread_id', ?)", (DROP_GROUP_THREAD_ID,))
        if OPERATOR_GROUP_CHAT_ID:
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('operator_group_chat_id', ?)", (OPERATOR_GROUP_CHAT_ID,))
        if OPERATOR_GROUP_THREAD_ID:
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('operator_group_thread_id', ?)", (OPERATOR_GROUP_THREAD_ID,))
        conn.commit()


def migrate_schema(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS group_members (
            chat_id INTEGER NOT NULL,
            thread_id INTEGER NOT NULL DEFAULT 0,
            telegram_id INTEGER NOT NULL,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            last_seen_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, thread_id, telegram_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS operator_groups (
            chat_id INTEGER NOT NULL,
            thread_id INTEGER NOT NULL DEFAULT 0,
            bound_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, thread_id)
        )
        """
    )
    legacy_operator_chat_id = configured_operator_chat_id()
    if legacy_operator_chat_id:
        conn.execute(
            "INSERT OR IGNORE INTO operator_groups (chat_id, thread_id, bound_at) VALUES (?, ?, ?)",
            (legacy_operator_chat_id, configured_operator_thread_id(), now_iso()),
        )

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
    for column in (
        "source_chat_id",
        "source_thread_id",
        "source_message_id",
        "operator_chat_id",
        "operator_thread_id",
        "operator_message_id",
        "pending_operator_user_id",
    ):
        if column not in number_cols:
            conn.execute(f"ALTER TABLE numbers ADD COLUMN {column} INTEGER")
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
    try:
        with urllib.request.urlopen(url, body, timeout=60) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            result = json.loads(raw)
        except json.JSONDecodeError:
            result = {"description": raw or str(exc)}
        raise TelegramAPIError(method, exc.code, result.get("description"), result) from exc
    if not result.get("ok"):
        raise TelegramAPIError(method, description=result.get("description"), response=result)
    return result["result"]


def strip_inline_keyboard_extras(reply_markup):
    if not reply_markup or "inline_keyboard" not in reply_markup:
        return reply_markup
    return {
        "inline_keyboard": [
            [
                {key: value for key, value in button.items() if key not in {"icon_custom_emoji_id", "style"}}
                for button in row
            ]
            for row in reply_markup["inline_keyboard"]
        ]
    }


def send_message(chat_id, text, reply_markup=None, entities=None, message_thread_id=None):
    if entities is None:
        text, entities = premiumize_text(text)
    data = {"chat_id": chat_id, "text": text}
    if message_thread_id:
        data["message_thread_id"] = message_thread_id
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    if entities:
        data["entities"] = json.dumps(entities, ensure_ascii=False)
    try:
        return api("sendMessage", data)
    except TelegramAPIError as exc:
        if not is_bad_request(exc):
            raise
        description = (exc.description or "").lower()
        if "chat not found" in description:
            return None
        retry_data = dict(data)
        if "message thread" in description or "message_thread_id" in description or "thread not found" in description:
            retry_data.pop("message_thread_id", None)
        if "icon_custom_emoji_id" in description or "style" in description or "reply markup" in description:
            clean_markup = strip_inline_keyboard_extras(reply_markup)
            if clean_markup:
                retry_data["reply_markup"] = json.dumps(clean_markup, ensure_ascii=False)
        if "document_invalid" in description or "entity" in description or "entities" in description:
            retry_data.pop("entities", None)
        if retry_data == data:
            raise
        try:
            return api("sendMessage", retry_data)
        except TelegramAPIError as retry_exc:
            retry_description = (retry_exc.description or "").lower()
            if is_bad_request(retry_exc) and "chat not found" in retry_description:
                return None
            raise



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
    try:
        return api("answerCallbackQuery", data)
    except TelegramAPIError as exc:
        if is_bad_request(exc):
            return None
        raise



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
        caption, caption_entities = premiumize_text(caption)
        fields["caption"] = caption
        if caption_entities:
            fields["caption_entities"] = json.dumps(caption_entities, ensure_ascii=False)
    if reply_markup:
        fields["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

    def perform_send(send_fields):
        body = bytearray()
        for name, value in send_fields.items():
            body.extend(f"--{boundary}\r\n".encode("utf-8"))
            body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
            body.extend(str(value).encode("utf-8"))
            body.extend(b"\r\n")
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(f'Content-Disposition: form-data; name="document"; filename="{filename}"\r\n'.encode("utf-8"))
        body.extend(b"Content-Type: text/csv; charset=utf-8\r\n\r\n")
        body.extend(content or b"\xef\xbb\xbf\n")
        body.extend(b"\r\n")
        body.extend(f"--{boundary}--\r\n".encode("utf-8"))

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
        request = urllib.request.Request(
            url,
            data=bytes(body),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                result = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                result = json.loads(raw)
            except json.JSONDecodeError:
                result = {"description": raw or str(exc)}
            raise TelegramAPIError("sendDocument", exc.code, result.get("description"), result) from exc
        if not result.get("ok"):
            raise TelegramAPIError("sendDocument", description=result.get("description"), response=result)
        return result["result"]

    try:
        return perform_send(fields)
    except TelegramAPIError as exc:
        if not is_bad_request(exc):
            raise
        description = (exc.description or "").lower()
        if "chat not found" in description:
            return None
        retry_fields = dict(fields)
        if "document_invalid" in description or "entity" in description or "entities" in description:
            retry_fields.pop("caption_entities", None)
        if "reply markup" in description or "icon_custom_emoji_id" in description or "style" in description:
            clean_markup = strip_inline_keyboard_extras(reply_markup)
            if clean_markup:
                retry_fields["reply_markup"] = json.dumps(clean_markup, ensure_ascii=False)
        if retry_fields == fields:
            raise
        try:
            return perform_send(retry_fields)
        except TelegramAPIError as retry_exc:
            retry_description = (retry_exc.description or "").lower()
            if is_bad_request(retry_exc) and "chat not found" in retry_description:
                return None
            raise

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


def parse_json_object(value):
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


DEFAULT_BUTTON_CUSTOM_EMOJI_IDS = {
    "menu:add_number": "5427191301667856308",
    "menu:wallet": "5426879920833863274",
    "menu:my_queue": "5426906459436780975",
    "menu:withdraw": "5427268774287943522",
    "menu:admin": "5427080431382076756",
    "menu:take_number": "5426893972238379207",
    "work:take": "5427191301667856308",
    "work:next": "5427304070329177027",
    "work:done": "5426881849274179100",
    "work:failed": "5426844457288897234",
    "work:repeat_code": "5426940471282816637",
    "operator:repeat_message": "5426940471282816637",
    "operator:done": "5426881849274179100",
    "operator:failed": "5426844457288897234",
    "operator:skip": "5427304070329177027",
    "supplier:repeat": "5427165523274144649",
    "supplier:cancel": "5426940471282816637",
    "admin:stats": "5426842013452506664",
    "admin:operator_stats": "5426842013452506664",
    "admin:report_file": "5427067988861819649",
    "admin:withdrawals": "5427268774287943522",
    "admin:global_queue": "5426906459436780975",
    "admin:direct_message": "5427304070329177027",
    "admin:change_password": "5429421102659049990",
    "admin:change_price": "5426993329445311067",
    "admin:grant_operator": "5427055821219469820",
    "admin:reset_queue": "5427221362143960850",
    "admin:broadcast": "5427181187019874230",
    "admin:block": "5427193629540128339",
    "admin:unblock": "5426980427363556942",
    "admin:start_work": "5427181187019874230",
    "admin:stop_work": "5426949632448043251",
    "broadcast:operators": "5426875956579051157",
    "broadcast:suppliers": "5427304070329177027",
    "work:my_queue": "5426906459436780975",
}
BUTTON_CUSTOM_EMOJI_IDS = {
    **DEFAULT_BUTTON_CUSTOM_EMOJI_IDS,
    **parse_json_object(BUTTON_CUSTOM_EMOJI_IDS_JSON),
}
BUTTON_STYLES = parse_json_object(BUTTON_STYLES_JSON)


TEXT_CUSTOM_EMOJI_RULES = [
    ("Группа операторов отвязана.", "📭", "5427193629540128339"),
    ("Группа операторов привязана.", "📱", "5426881849274179100"),
    ("Группа дропов привязана. Теперь номера можно отправлять сюда.", "📱", "5426881849274179100"),
    ("Введите свой номер для сдачи.", "🚫", "5429421102659049990"),
    ("Группа дропов отвязана.", "📭", "5427193629540128339"),
    ("Ворк закончен.", "🧹", "5426844457288897234"),
    ("Статистика", "📊", "5426842013452506664"),
    ("Меню", "📋", "5427195553685479167"),
    ("Куда отправить рассылку?", "📣", "5427181187019874230"),
    ("номера, работаем!", "📦", "5426875956579051157"),
    ("ворк закончен.", "📭", "5427193629540128339"),
    ("Работа закончена.", "📭", "5427193629540128339"),
    ("Добавлено номеров:", "📱", "5426881849274179100"),
    ("Моя очередь", "📦", "5426906459436780975"),
    ("Всего номеров в очереди:", "🔢", "5426875956579051157"),
    ("Нажмите номер, чтобы убрать его из очереди.", "👇", "5427293277076366247"),
    ("убран из очереди.", "🧹", "5426844457288897234"),
    ("Отправьте российские номера списком", "➕", "5427191301667856308"),
    ("В вашей очереди нет доступных номеров.", "📦", "5427193629540128339"),
    ("Кошелек", "💎", "5426879920833863274"),
    ("Встало номеров:", "✅", "5426993329445311067"),
    ("Мой баланс:", "💰", "5427195553685479167"),
    ("Баланс пока 0$", "💰", "5427396897457345890"),
    ("Админ-панель", "🛠", "5426980427363556942"),
    ("Прайс за номер:", "💵", "5426952574500640167"),
    ("Главное меню:", "✨", "5426906459436780975"),
    ("Ваш номер", "📩", "5427244675226442797"),
    ("Повтор с причиной", "🔁", "5427165523274144649"),
    ("Отменить с причиной", "❌", "5426940471282816637"),
    ("Общая очередь в боте", "🌐", "5427181187019874230"),
    ("Введите новый пароль доступа", "🔐", "5427228397300387633"),
    ("Очередь сброшена", "♻️", "5426949632448043251"),
    ("Введите текст рассылки", "📣", "5427368000917381732"),
    ("Введите @username пользователя для разблокировки", "✅", "5427212948303028717"),
    ("Введите @username пользователя для блокировки", "🚫", "5426842013452506664"),
    ("Текущий прайс:", "💵", "5427391193740779498"),
    ("Введите @username пользователя, которому выдать роль оператора", "🎧", "542702811438544649"),
    ("Выберите пользователя, которому написать:", "✉️", "5429463124619072437"),
    ("Пользователи:", "👥", "5427125644502801684"),
    ("Заблокированы:", "🚫", "5429421102659049990"),
    ("Встали:", "✅", "5427304070329177027"),
    ("Не встали:", "❌", "5427103057269793539"),
    ("Повторов кода:", "🔁", "5427009405625902094"),
    ("Статистика операторов", "📊", "5427184451000806450"),
    ("Операторов пока нет", "🎧", "5426919293126861502"),
    ("выдана роль оператора", "🎧", "5427122176311681284"),
    ("Отчет готов", "📄", "5427244675226442797"),
    ("Ожидающих выводов нет", "💸", "5427376742512128795"),
    ("Выводы на обработке:", "💸", "5427311746200234273"),
    ("Выберите заявку", "👇", "5427116744865809706"),
    ("Вывод", "💸", "5426830740693026040"),
    ("Введите текст или чек", "✍️", "5426998632616895318"),
    ("Ваш вывод", "💸", "5427341851218576402"),
    ("Сообщение отправлено пользователю", "✅", "5427334710156942470"),
    ("он встал.", "✅", "5426980427363556942"),
    ("не встал.", "❌", "5426844457288897234"),
    ("Повторный код нужен по номеру", "🔁", "5427092753643249034"),
    ("запрошен повтор кода", "🔁", "5427092753643249034"),
    ("Рассылка по операторам отправлена.", "📣", "5427181187019874230"),
    ("Рассылка по поставщикам отправлена.", "📣", "5427181187019874230"),
    ("По номеру", "✅", "5427038101662991630"),
    ("Зачислено", "💰", "5427142415174408197"),
    ("Введите сообщение для пользователя", "✉️", "5427218386121830403"),
    ("Пароль доступа к боту изменен", "🔐", "5427246473121544465"),
    ("Прайс изменен", "💵", "5426895995221973641"),
    ("Рассылка отправлена", "📣", "5427092120612812297"),
    ("заблокирован", "🚫", "5427170138402924177"),
    ("разблокирован", "✅", "5427315516091234833"),
    ("Сейчас свободных номеров нет", "📭", "5427193629540128339"),
    ("Взять номер", "📲", "5427191301667856308"),
    ("Номер #", "📲", "5427350258407480293"),
    ("Повтор кода", "🔁", "5427242137683936643"),
    ("Скипнуть", "⏭️", "5427183658234842113"),
    ("Код по номеру", "🔢", "5427350258407480293"),
    ("Укажите причину, почему не встал", "❌", "5427251268395021237"),
    ("возвращен в очередь", "⏭️", "5427103057269793539"),
]


def utf16_len(value):
    return len(value.encode("utf-16-le")) // 2


def premiumize_text(text):
    if not text:
        return text, None
    entities = []
    occupied_offsets = set()
    rebuilt_lines = []
    utf16_base = 0
    for line in str(text).split("\n"):
        new_line = line
        for phrase, marker, emoji_id in TEXT_CUSTOM_EMOJI_RULES:
            if phrase not in new_line:
                continue
            marker_index = new_line.find(marker)
            if marker_index < 0:
                new_line = f"{marker} {new_line}"
                marker_index = 0
            offset = utf16_base + utf16_len(new_line[:marker_index])
            if offset not in occupied_offsets:
                entities.append({
                    "type": "custom_emoji",
                    "offset": offset,
                    "length": utf16_len(marker),
                    "custom_emoji_id": emoji_id,
                })
                occupied_offsets.add(offset)
            break
        rebuilt_lines.append(new_line)
        utf16_base += utf16_len(new_line) + 1
    return "\n".join(rebuilt_lines), entities or None


def button_extra_value(mapping, text, callback_data):
    if callback_data in mapping:
        return mapping[callback_data]
    for key, value in mapping.items():
        if callback_data.startswith(f"{key}:"):
            return value
    return mapping.get(text)


def inline_button(button):
    if isinstance(button, dict):
        result = dict(button)
        if "text" in result:
            result["text"] = str(result["text"])
        if "callback_data" in result:
            result["callback_data"] = str(result["callback_data"])
        return result

    if len(button) == 2:
        text, callback_data = button
        options = {}
    elif len(button) == 3 and isinstance(button[2], dict):
        text, callback_data, options = button
    elif len(button) == 3:
        text, callback_data, icon_custom_emoji_id = button
        options = {"icon_custom_emoji_id": icon_custom_emoji_id}
    elif len(button) == 4:
        text, callback_data, icon_custom_emoji_id, style = button
        options = {"icon_custom_emoji_id": icon_custom_emoji_id, "style": style}
    else:
        raise ValueError(
            "Inline button must be a dict or a tuple: "
            "(text, callback_data[, options/custom_emoji_id[, style]])"
        )

    text = str(text)
    callback_data = str(callback_data)
    result = {"text": text, "callback_data": callback_data}

    icon_custom_emoji_id = options.get("icon_custom_emoji_id") or button_extra_value(
        BUTTON_CUSTOM_EMOJI_IDS, text, callback_data
    )
    if icon_custom_emoji_id:
        result["icon_custom_emoji_id"] = str(icon_custom_emoji_id)

    style = options.get("style") or button_extra_value(BUTTON_STYLES, text, callback_data)
    if style:
        result["style"] = str(style)

    return result


def inline_keyboard(rows):
    return {"inline_keyboard": [[inline_button(button) for button in row] for row in rows]}


def back_row(target="menu:home"):
    return [("Назад", target, {"icon_custom_emoji_id": BACK_BUTTON_CUSTOM_EMOJI_ID})]


def admin_back_row():
    return back_row("admin:panel")


def prompt_keyboard(extra_rows=None, back_to="menu:home"):
    rows = list(extra_rows or [])
    rows.append(back_row(back_to))
    return inline_keyboard(rows)


def send_state_prompt(chat_id, user_id, state, text, data=None, reply_markup=None, back_to="menu:home"):
    message = send_message(chat_id, text, reply_markup or prompt_keyboard(back_to=back_to))
    state_payload = dict(data or {})
    state_payload["prompt_message_id"] = message.get("message_id")
    set_state(user_id, state, state_payload)
    return message


def delete_state_prompt(chat_id, user):
    prompt_message_id = state_data(user).get("prompt_message_id")
    if prompt_message_id:
        delete_message(chat_id, prompt_message_id)


def main_menu_keyboard(user):
    return inline_keyboard([])


def admin_keyboard():
    return inline_keyboard([
        [("Статистика", "admin:stats")],
        [("Отчет файлом", "admin:report_file")],
        [("Рассылка", "admin:broadcast")],
        [("Начать ворк", "admin:start_work"), ("Закончить ворк", "admin:stop_work")],
        back_row(),
    ])


def broadcast_target_keyboard():
    return inline_keyboard([
        [("По операторам", "broadcast:operators")],
        [("По поставщикам", "broadcast:suppliers")],
        admin_back_row(),
    ])


def supplier_number_keyboard(number_id):
    return inline_keyboard([
        [("Повтор с причиной", f"supplier:repeat:{number_id}")],
        [("Отменить с причиной", f"supplier:cancel:{number_id}")],
        back_row(),
    ])


def operator_active_keyboard(number_id):
    return inline_keyboard([
        [("Повторный код", f"operator:repeat_message:{number_id}")],
        [("Встал", f"operator:done:{number_id}"), ("Не встал", f"operator:failed:{number_id}")],
        [("След номер", f"operator:skip:{number_id}")],
        back_row(),
    ])


def work_menu_keyboard():
    return inline_keyboard([
        [("Взять номер", "work:take_next:0", {"icon_custom_emoji_id": "5427191301667856308"})],
        [("След номер", "work:next:0", {"icon_custom_emoji_id": "5427304070329177027"})],
        [
            ("Встал", "work:done:0", {"icon_custom_emoji_id": "5426881849274179100"}),
            ("Не встал", "work:failed:0", {"icon_custom_emoji_id": "5426844457288897234"}),
        ],
        [("Повторный код", "work:repeat_code:0", {"icon_custom_emoji_id": "5426940471282816637"})],
    ])


def work_number_keyboard(number_id):
    return inline_keyboard([[("Взять номер", f"work:take:{number_id}")]])


def work_active_keyboard(number_id):
    return inline_keyboard([
        [("След номер", f"work:next:{number_id}")],
        [("Встал", f"work:done:{number_id}"), ("Не встал", f"work:failed:{number_id}")],
        [("Повторный код", f"work:repeat_code:{number_id}")],
    ])


def work_approve_keyboard(number_id, operator_user_id):
    return inline_keyboard([
        [("Взять номер", f"work:approve:{number_id}:{operator_user_id}")],
        [("Не брать", f"work:reject:{number_id}:{operator_user_id}")],
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
    earned = float(done) * get_price_per_number()
    return earned - float(withdrawn or 0), done


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




def get_price_per_number():
    try:
        price = float(get_setting("price_per_number", "1") or "1")
    except (TypeError, ValueError):
        return 1.0
    return price if price > 0 else 1.0


def set_price_per_number(price):
    set_setting("price_per_number", str(round(float(price), 2)))

def get_access_password():
    return get_setting("access_password", ADMIN_PASSWORD)


def set_access_password(password):
    set_setting("access_password", password)


def bool_setting(key, default=False):
    value = get_setting(key, "1" if default else "0")
    return str(value).lower() in {"1", "true", "yes", "on"}


def set_bool_setting(key, value):
    set_setting(key, "1" if value else "0")


def configured_drop_chat_id():
    try:
        return int(get_setting("drop_group_chat_id", DROP_GROUP_CHAT_ID) or 0)
    except (TypeError, ValueError):
        return 0


def configured_operator_chat_id():
    try:
        return int(get_setting("operator_group_chat_id", OPERATOR_GROUP_CHAT_ID) or 0)
    except (TypeError, ValueError):
        return 0


def configured_drop_thread_id():
    try:
        return int(get_setting("drop_group_thread_id", DROP_GROUP_THREAD_ID) or 0)
    except (TypeError, ValueError):
        return 0


def configured_operator_thread_id():
    try:
        return int(get_setting("operator_group_thread_id", OPERATOR_GROUP_THREAD_ID) or 0)
    except (TypeError, ValueError):
        return 0


def operator_group_rows():
    with closing(db()) as conn:
        rows = conn.execute("SELECT chat_id, thread_id FROM operator_groups ORDER BY bound_at ASC").fetchall()
    if rows:
        return rows
    chat_id = configured_operator_chat_id()
    if not chat_id:
        return []
    return [{"chat_id": chat_id, "thread_id": configured_operator_thread_id()}]


def operator_group_count():
    return len(operator_group_rows())


def add_operator_group(chat_id, thread_id):
    with closing(db()) as conn:
        conn.execute(
            """
            INSERT INTO operator_groups (chat_id, thread_id, bound_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id, thread_id) DO UPDATE SET bound_at = excluded.bound_at
            """,
            (chat_id, thread_id, now_iso()),
        )
        conn.commit()


def remove_operator_group(chat_id, thread_id):
    with closing(db()) as conn:
        conn.execute("DELETE FROM operator_groups WHERE chat_id = ? AND thread_id = ?", (chat_id, thread_id))
        conn.commit()


def is_operator_group_bound(chat_id, thread_id=0):
    for row in operator_group_rows():
        if row["chat_id"] == chat_id and (not row["thread_id"] or row["thread_id"] == thread_id):
            return True
    return False


def message_operator_group_target(message):
    return message["chat"]["id"], int(message.get("message_thread_id") or 0)


def same_topic(message, chat_id, thread_id):
    if not chat_id or message["chat"]["id"] != chat_id:
        return False
    return not thread_id or int(message.get("message_thread_id") or 0) == thread_id


def work_enabled():
    return bool_setting("work_enabled", False)


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
            password_check = 1
            is_admin = 1 if telegram_id in ADMIN_TELEGRAM_IDS else 0
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
            ("pending", telegram_id, username, ROLE_SUPPLIER, is_admin, 1, now_iso(), now_iso()),
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
    if user["is_admin"]:
        send_message(chat_id, "Админ-панель", admin_keyboard())
        return
    send_message(chat_id, "(бот для админов)", None)


def build_global_stats():
    with closing(db()) as conn:
        submitted = conn.execute("SELECT COUNT(*) count FROM numbers").fetchone()["count"]
    supplier_bindings = 1 if configured_drop_chat_id() else 0
    operator_bindings = operator_group_count()
    lines = [
        "📊 Статистика",
        f"Кол-во поставщиков (привязок): {supplier_bindings}",
        f"Кол-во операторов (привязок): {operator_bindings}",
        f"Сдано номеров: {submitted}",
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
        admin_back_row(),
    ])


def report_date_keyboard():
    return inline_keyboard([
        [("📅 Сегодня", "report:date:today"), ("📅 Вчера", "report:date:yesterday")],
        [("🗓️ 7 дней", "report:date:7"), ("📚 Все даты", "report:date:all")],
        admin_back_row(),
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
    send_document_bytes(chat_id, filename, content, "Отчет готов", admin_keyboard())
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
            f"{handle}: взял {taken}, встали {done}, не встали {row['failed'] or 0}, скипы {row['skipped'] or 0}, повторы {row['repeats'] or 0}, повторы кода {row['repeat_messages'] or 0}"
        )
    return "\n".join(lines)


def handle_start(chat_id, telegram_id, username=None):
    user = create_or_touch_user(telegram_id, username)
    if user["is_blocked"]:
        send_message(chat_id, "🚫 Доступ заблокирован.")
        return
    mark_password_ok(user["id"])
    clear_state(user["id"])
    show_home(chat_id, get_user(telegram_id) or user)



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
    copy_message(row["telegram_id"], chat_id, message["message_id"], caption=f"💸 Ваш вывод {money_text(row['amount'])}")
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


def is_real_telegram_user(tg_from):
    return bool(tg_from and tg_from.get("id") and tg_from.get("id") != ANONYMOUS_ADMIN_TELEGRAM_ID)


def remember_group_member(message):
    tg_from = message.get("from") or {}
    if not is_real_telegram_user(tg_from):
        return
    chat_id = message["chat"]["id"]
    thread_id = int(message.get("message_thread_id") or 0)
    with closing(db()) as conn:
        conn.execute(
            """
            INSERT INTO group_members (chat_id, thread_id, telegram_id, username, first_name, last_name, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, thread_id, telegram_id) DO UPDATE SET
                username = COALESCE(excluded.username, group_members.username),
                first_name = COALESCE(excluded.first_name, group_members.first_name),
                last_name = COALESCE(excluded.last_name, group_members.last_name),
                last_seen_at = excluded.last_seen_at
            """,
            (
                chat_id,
                thread_id,
                tg_from["id"],
                extract_username(tg_from),
                tg_from.get("first_name"),
                tg_from.get("last_name"),
                now_iso(),
            ),
        )
        conn.commit()


def can_use_give_command(message, user=None):
    tg_from = message.get("from") or {}
    telegram_id = tg_from.get("id")
    sender_chat_id = (message.get("sender_chat") or {}).get("id")
    allowed_ids = GIVE_TELEGRAM_IDS or ADMIN_TELEGRAM_IDS
    if telegram_id in allowed_ids:
        return True
    if user and user["is_admin"]:
        return True
    # Сообщение от имени группы появляется у анонимных администраторов Telegram.
    return telegram_id == ANONYMOUS_ADMIN_TELEGRAM_ID and sender_chat_id == message["chat"]["id"]


def group_member_label(row):
    if row["username"]:
        return f"@{row['username']}"
    name = " ".join(part for part in (row["first_name"], row["last_name"]) if part)
    return name or f"id {row['telegram_id']}"


def known_operator_group_members(chat_id, thread_id=None):
    with closing(db()) as conn:
        if thread_id is None:
            return conn.execute(
                """
                SELECT telegram_id, username, first_name, last_name, last_seen_at
                FROM group_members
                WHERE chat_id = ?
                ORDER BY username IS NULL, lower(username), telegram_id
                """,
                (chat_id,),
            ).fetchall()
        return conn.execute(
            """
            SELECT telegram_id, username, first_name, last_name, last_seen_at
            FROM group_members
            WHERE chat_id = ? AND thread_id = ?
            ORDER BY username IS NULL, lower(username), telegram_id
            """,
            (chat_id, thread_id),
        ).fetchall()


def give_member_list_text(rows):
    if not rows:
        return "Пока нет сохраненных пользователей из группы /op."
    lines = ["👥 Юзернеймы из группы /op:"]
    for row in rows[:100]:
        lines.append(f"• {group_member_label(row)}")
    return "\n".join(lines)


def known_operator_group_members_for_groups(groups):
    seen = set()
    members = []
    for group in groups:
        thread_id = group["thread_id"] if group["thread_id"] else None
        for row in known_operator_group_members(group["chat_id"], thread_id):
            if row["telegram_id"] in seen:
                continue
            seen.add(row["telegram_id"])
            members.append(row)
    return members


def find_give_target(message, argument):
    reply_from = (message.get("reply_to_message") or {}).get("from") or {}
    if not argument and is_real_telegram_user(reply_from):
        return create_or_touch_user(reply_from["id"], extract_username(reply_from))

    value = (argument or "").strip()
    if not value:
        return None
    if value.startswith("@"):
        return get_user_by_handle(value)
    if value.upper().startswith("U") and value[1:].isdigit():
        with closing(db()) as conn:
            return conn.execute("SELECT * FROM users WHERE public_id = ?", (value.upper(),)).fetchone()
    if value.lstrip("-").isdigit():
        telegram_id = int(value)
        username = None
        with closing(db()) as conn:
            member = conn.execute(
                """
                SELECT username
                FROM group_members
                WHERE chat_id = ? AND thread_id = ? AND telegram_id = ?
                """,
                (message["chat"]["id"], int(message.get("message_thread_id") or 0), telegram_id),
            ).fetchone()
            if member:
                username = member["username"]
        return create_or_touch_user(telegram_id, username)
    return None


def handle_give_command(message, issuer=None):
    chat_id = message["chat"]["id"]
    thread_id = int(message.get("message_thread_id") or 0)
    if not is_operator_group_bound(chat_id, thread_id):
        send_message(chat_id, "⚠️ /give работает только в группе операторов, привязанной командой /op.", message_thread_id=thread_id or None)
        return True
    if not can_use_give_command(message, issuer):
        send_message(chat_id, "⛔ /give доступна только админу или отдельным ID из GIVE_TELEGRAM_IDS.", message_thread_id=thread_id or None)
        return True

    text = (message.get("text") or "").strip()
    parts = text.split(maxsplit=1)
    argument = parts[1].strip() if len(parts) > 1 else ""
    target = find_give_target(message, argument)
    if target:
        target = set_user_role(target["id"], ROLE_OPERATOR)
        log_event(issuer["id"] if issuer else target["id"], "give_operator", details=user_handle(target))
        send_message(
            chat_id,
            f"🎧 {user_handle(target)} назначен оператором. Можно брать номера в этой группе.",
            message_thread_id=thread_id or None,
        )
        return True

    rows = known_operator_group_members(chat_id, thread_id)
    if not rows:
        send_message(
            chat_id,
            "Пока нет сохраненных пользователей из группы /op.",
            message_thread_id=thread_id or None,
        )
        return True
    send_message(chat_id, give_member_list_text(rows), message_thread_id=thread_id or None)
    return True


def handle_private_give_command(chat_id, user):
    if not can_use_give_command({"chat": {"id": chat_id}, "from": {"id": user["telegram_id"]}}, user):
        send_message(chat_id, "⛔ /give доступна только админу или отдельным ID из GIVE_TELEGRAM_IDS.")
        return True
    groups = operator_group_rows()
    if not groups:
        send_message(chat_id, "Группа операторов еще не привязана командой /op.")
        return True
    rows = known_operator_group_members_for_groups(groups)
    send_message(chat_id, give_member_list_text(rows))
    return True


def work_thread_id_for_message(message):
    return int(message.get("message_thread_id") or 0) or None


def bind_drop_group(message):
    chat_id = message["chat"]["id"]
    thread_id = int(message.get("message_thread_id") or 0)
    if configured_drop_chat_id() == chat_id and configured_drop_thread_id() == thread_id:
        set_setting("drop_group_chat_id", 0)
        set_setting("drop_group_thread_id", 0)
        send_message(chat_id, "Группа дропов отвязана.", message_thread_id=thread_id or None)
        return True
    set_setting("drop_group_chat_id", chat_id)
    set_setting("drop_group_thread_id", thread_id)
    send_message(chat_id, "Группа дропов привязана. Теперь номера можно отправлять сюда.", message_thread_id=thread_id or None)
    return True


def bind_operator_group(message):
    chat_id = message["chat"]["id"]
    thread_id = int(message.get("message_thread_id") or 0)
    if is_operator_group_bound(chat_id, thread_id):
        remove_operator_group(chat_id, thread_id)
        if configured_operator_chat_id() == chat_id and configured_operator_thread_id() == thread_id:
            remaining = operator_group_rows()
            if remaining:
                set_setting("operator_group_chat_id", remaining[-1]["chat_id"])
                set_setting("operator_group_thread_id", remaining[-1]["thread_id"])
            else:
                set_setting("operator_group_chat_id", 0)
                set_setting("operator_group_thread_id", 0)
        send_message(chat_id, "Группа операторов отвязана.", message_thread_id=thread_id or None)
        return True
    add_operator_group(chat_id, thread_id)
    set_setting("operator_group_chat_id", chat_id)
    set_setting("operator_group_thread_id", thread_id)
    send_message(chat_id, f"Группа операторов привязана. Всего привязок: {operator_group_count()}.", work_menu_keyboard(), message_thread_id=thread_id or None)
    return True


def operator_group_send(text, reply_markup=None, chat_id=None, thread_id=None):
    chat_id = chat_id or configured_operator_chat_id()
    if not chat_id:
        return None
    if thread_id is None:
        thread_id = configured_operator_thread_id()
    return send_message(chat_id, text, reply_markup, message_thread_id=thread_id or None)


def drop_group_send(text, reply_markup=None):
    chat_id = configured_drop_chat_id()
    if not chat_id:
        return None
    return send_message(chat_id, text, reply_markup, message_thread_id=configured_drop_thread_id() or None)




def supplier_queue_keyboard(supplier_user_id):
    return inline_keyboard([[("Моя очередь", f"work:my_queue:{supplier_user_id}", {"icon_custom_emoji_id": "5426906459436780975"})]])


def supplier_queue_lines(supplier_user_id):
    with closing(db()) as conn:
        rows = conn.execute(
            """
            SELECT id, supplier_user_id, masked_number
            FROM numbers
            WHERE status = ? AND remaining > 0
            ORDER BY created_at ASC, id ASC
            """,
            (STATUS_AVAILABLE,),
        ).fetchall()
    lines = []
    for index, row in enumerate(rows, start=1):
        if row["supplier_user_id"] == supplier_user_id:
            lines.append(f"{row['masked_number']} - очередь {index}")
    return lines


def supplier_queue_text(supplier_user_id):
    lines = supplier_queue_lines(supplier_user_id)
    if not lines:
        return "В вашей очереди нет доступных номеров."
    return "\n".join(lines)


def publish_number_to_operator_group(number_id, target_chat_id=None, target_thread_id=None):
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM numbers WHERE id = ?", (number_id,)).fetchone()
    if not row:
        return None
    message = operator_group_send(
        f"Номер #{row['id']} в очереди.\nНомер: {row['masked_number']}",
        work_number_keyboard(row["id"]),
        target_chat_id,
        target_thread_id,
    )
    if message:
        with closing(db()) as conn:
            conn.execute(
                "UPDATE numbers SET operator_chat_id = ?, operator_thread_id = ?, operator_message_id = ? WHERE id = ?",
                (target_chat_id or configured_operator_chat_id(), target_thread_id if target_thread_id is not None else configured_operator_thread_id(), message.get("message_id"), number_id),
            )
            conn.commit()
    return message


def register_drop_numbers(message, supplier):
    text = message.get("text") or ""
    numbers, bad = parse_russian_numbers(text)
    if not numbers:
        return False
    chat_id = message["chat"]["id"]
    thread_id = int(message.get("message_thread_id") or 0)
    with closing(db()) as conn:
        inserted = []
        for number in numbers:
            cur = conn.execute(
                """
                INSERT INTO numbers (
                    supplier_user_id, masked_number, volume, remaining, status, created_at,
                    source_chat_id, source_thread_id, source_message_id
                )
                VALUES (?, ?, 1, 1, ?, ?, ?, ?, ?)
                """,
                (supplier["id"], number, STATUS_AVAILABLE, now_iso(), chat_id, thread_id, message.get("message_id")),
            )
            inserted.append(cur.lastrowid)
        conn.commit()
    log_event(supplier["id"], "work_numbers_added", details=f"count={len(inserted)}")
    extra = f"\nНе добавлены: {', '.join(bad)}" if bad else ""
    queue_lines = supplier_queue_lines(supplier["id"])
    details = "\n" + "\n".join(queue_lines) if queue_lines else ""
    send_message(
        chat_id,
        f"📱 Добавлено номеров: {len(inserted)}{details}{extra}",
        supplier_queue_keyboard(supplier["id"]),
        message_thread_id=work_thread_id_for_message(message),
    )
    return True


def forward_code_to_operator_group(message):
    if not message.get("reply_to_message"):
        return False
    text = (message.get("text") or "").strip()
    if not text:
        return False
    reply_id = message["reply_to_message"].get("message_id")
    with closing(db()) as conn:
        row = conn.execute(
            """
            SELECT * FROM numbers
            WHERE source_chat_id = ? AND source_message_id = ? AND status = ?
            ORDER BY id DESC LIMIT 1
            """,
            (message["chat"]["id"], reply_id, STATUS_ASSIGNED),
        ).fetchone()
    if not row:
        return False
    operator_group_send(f"Код по номеру {row['masked_number']}: {text}", work_active_keyboard(row["id"]))
    return True


def forward_code_to_drop_group(message):
    if not message.get("reply_to_message"):
        return False
    text = (message.get("text") or "").strip()
    if not text:
        return False
    reply_id = message["reply_to_message"].get("message_id")
    with closing(db()) as conn:
        row = conn.execute(
            """
            SELECT * FROM numbers
            WHERE operator_chat_id = ? AND operator_message_id = ? AND status = ?
            ORDER BY id DESC LIMIT 1
            """,
            (message["chat"]["id"], reply_id, STATUS_ASSIGNED),
        ).fetchone()
    if not row:
        return False
    send_message(
        row["source_chat_id"],
        f"Код по номеру {row['masked_number']}: {text}",
        message_thread_id=row["source_thread_id"] or None,
    )
    return True


def handle_work_group_text(message):
    chat_id = message["chat"]["id"]
    if chat_id > 0:
        return False
    text = (message.get("text") or "").strip()
    tg_from = message.get("from") or {}
    remember_group_member(message)
    issuer = create_or_touch_user(tg_from.get("id"), extract_username(tg_from)) if is_real_telegram_user(tg_from) else None
    if text.startswith("/set"):
        return bind_drop_group(message)
    if text.startswith("/op"):
        return bind_operator_group(message)
    if text.startswith("/give"):
        return handle_give_command(message, issuer)

    drop_chat_id = configured_drop_chat_id()
    if not issuer:
        return False
    user = issuer
    in_drop_topic = same_topic(message, drop_chat_id, configured_drop_thread_id())
    in_operator_topic = is_operator_group_bound(chat_id, int(message.get("message_thread_id") or 0))
    if not work_enabled():
        return in_drop_topic or in_operator_topic
    if in_drop_topic:
        if forward_code_to_operator_group(message) or register_drop_numbers(message, user):
            return True
        send_message(
            chat_id,
            "Введите свой номер для сдачи.",
            message_thread_id=work_thread_id_for_message(message),
        )
        return True
    if in_operator_topic:
        user = set_user_role(user["id"], ROLE_OPERATOR)
        forward_code_to_drop_group(message)
        return True
    return False


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

    if handle_work_group_text(message):
        return
    if chat_id < 0:
        return

    delete_message(chat_id, message.get("message_id"))

    if text.startswith("/start"):
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

    if text.startswith("/give"):
        clear_state(user["id"])
        handle_private_give_command(chat_id, user)
        return

    if user["state"] != "supplier_message":
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
        save_supplier_message(chat_id, user, text, message)
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

    if user["state"] == "admin_change_price":
        handle_admin_change_price(chat_id, user, text)
        return

    if user["state"] in {"grant_operator", "block_user", "unblock_user"}:
        handle_admin_text_state(chat_id, user, text)
        return

    if user["state"] == "broadcast":
        handle_broadcast_text(chat_id, user, raw_text, entities)
        return

    clear_state(user["id"])
    show_home(chat_id, get_user(telegram_id) or user)



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
    send_message(row["telegram_id"], f"💸 Ваш вывод {money_text(row['amount'])}:\n{text}")
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


def save_supplier_message(chat_id, user, text, message_obj):
    data = state_data(user)
    number_id = data.get("number_id")
    prompt_message_id = data.get("prompt_message_id")
    reply_message_id = (message_obj.get("reply_to_message") or {}).get("message_id")
    if prompt_message_id and reply_message_id != prompt_message_id:
        delete_message(chat_id, data.get("error_message_id"))
        error = send_message(
            chat_id,
            "⚠️ Ошибка: введите код ответом на запрос кода.",
            prompt_keyboard(),
        )
        data["error_message_id"] = error.get("message_id")
        set_state(user["id"], "supplier_message", data)
        return

    message = text.strip()
    if not message:
        send_state_prompt(chat_id, user["id"], "supplier_message", "Введите непустой код для оператора.", {"number_id": number_id})
        return

    with closing(db()) as conn:
        row = conn.execute(
            "SELECT supplier_user_id, assigned_operator_user_id, masked_number FROM numbers WHERE id = ?",
            (number_id,),
        ).fetchone()
        if not row or row["supplier_user_id"] != user["id"] or not row["assigned_operator_user_id"]:
            clear_state(user["id"])
            send_message(chat_id, "Номер не найден или уже не активен.", main_menu_keyboard(user))
            return
        operator = conn.execute("SELECT telegram_id FROM users WHERE id = ?", (row["assigned_operator_user_id"],)).fetchone()

    delete_message(chat_id, prompt_message_id)
    delete_message(chat_id, data.get("error_message_id"))
    clear_state(user["id"])
    log_event(user["id"], "supplier_message_sent", number_id, "sent")
    send_message(chat_id, f"Код по номеру {row['masked_number']} отправлен оператору.", main_menu_keyboard(user))
    if operator:
        send_message(
            operator["telegram_id"],
            f"Код по номеру {row['masked_number']}:\n{message}",
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
    target = state_data(admin).get("broadcast_target")
    if target == "operators":
        target_chat_id = configured_operator_chat_id()
        target_thread_id = configured_operator_thread_id()
        target_title = "операторам"
    else:
        target_chat_id = configured_drop_chat_id()
        target_thread_id = configured_drop_thread_id()
        target_title = "поставщикам"
    sent = 0
    if target_chat_id:
        send_message(target_chat_id, text, entities=entities, message_thread_id=target_thread_id or None)
        sent = 1
    clear_state(admin["id"])
    log_event(admin["id"], "broadcast", details=f"target={target};sent={sent}")
    if sent:
        send_message(chat_id, f"Рассылка по {target_title} отправлена.", admin_keyboard())
    else:
        send_message(chat_id, f"📣 Группа для рассылки по {target_title} не привязана.", admin_keyboard())


def send_next_available_to_operator_group(operator_user=None, exclude_number_id=None, target_chat_id=None, target_thread_id=None):
    with closing(db()) as conn:
        row = conn.execute(
            """
            SELECT * FROM numbers
            WHERE status = ? AND remaining > 0 AND (? IS NULL OR id != ?)
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            """,
            (STATUS_AVAILABLE, exclude_number_id, exclude_number_id),
        ).fetchone()
        if not row:
            operator_group_send("Сейчас свободных номеров нет.", work_menu_keyboard(), target_chat_id, target_thread_id)
            return None
        if operator_user:
            conn.execute(
                """
                UPDATE numbers
                SET status = ?, assigned_operator_user_id = ?, assigned_at = ?, pending_operator_user_id = NULL
                WHERE id = ?
                """,
                (STATUS_ASSIGNED, operator_user["id"], now_iso(), row["id"]),
            )
            conn.commit()
            log_event(operator_user["id"], "number_taken", row["id"])
            message = operator_group_send(
                f"Номер #{row['id']} взят.\nНомер: {row['masked_number']}\nОжидайте код от поставщика.",
                work_active_keyboard(row["id"]),
                target_chat_id,
                target_thread_id,
            )
            if message:
                conn.execute(
                    "UPDATE numbers SET operator_chat_id = ?, operator_thread_id = ?, operator_message_id = ? WHERE id = ?",
                    (target_chat_id or configured_operator_chat_id(), target_thread_id if target_thread_id is not None else configured_operator_thread_id(), message.get("message_id"), row["id"]),
                )
                conn.commit()
            prompt = send_message(
                row["source_chat_id"] or configured_drop_chat_id(),
                f"Ваш номер {row['masked_number']} взяли. Введите код для оператора ответом на это сообщение.",
                message_thread_id=row["source_thread_id"] or configured_drop_thread_id() or None,
            )
            if prompt:
                conn.execute(
                    "UPDATE numbers SET source_chat_id = ?, source_thread_id = ?, source_message_id = ? WHERE id = ?",
                    (
                        row["source_chat_id"] or configured_drop_chat_id(),
                        row["source_thread_id"] or configured_drop_thread_id(),
                        prompt.get("message_id"),
                        row["id"],
                    ),
                )
                conn.commit()
            return message
    return publish_number_to_operator_group(row["id"], target_chat_id, target_thread_id)


def handle_work_callback(callback_id, callback, user, data):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    chat_id = callback["message"]["chat"]["id"]
    target_chat_id = chat_id
    target_thread_id = int(callback["message"].get("message_thread_id") or 0)
    if is_operator_group_bound(target_chat_id, target_thread_id):
        user = set_user_role(user["id"], ROLE_OPERATOR)
    if action not in {"my_queue"} and not work_enabled():
        answer_callback(callback_id, "Ворк закончен.", True)
        return
    if action == "my_queue":
        supplier_user_id = int(parts[2]) if len(parts) > 2 else user["id"]
        if supplier_user_id != user["id"]:
            answer_callback(callback_id, "Это очередь другого поставщика.", True)
            return
        send_message(
            chat_id,
            supplier_queue_text(user["id"]),
            supplier_queue_keyboard(user["id"]),
            message_thread_id=callback["message"].get("message_thread_id") or configured_drop_thread_id() or None,
        )
        answer_callback(callback_id)
        return
    if action in {"next", "take_next"}:
        number_id = int(parts[2]) if len(parts) > 2 else 0
        if action == "next" and number_id:
            with closing(db()) as conn:
                row = conn.execute("SELECT * FROM numbers WHERE id = ?", (number_id,)).fetchone()
                if row and row["assigned_operator_user_id"] == user["id"] and row["status"] == STATUS_ASSIGNED:
                    conn.execute(
                        "UPDATE numbers SET status = ?, assigned_operator_user_id = NULL, assigned_at = NULL WHERE id = ?",
                        (STATUS_AVAILABLE, number_id),
                    )
                    conn.commit()
                    log_event(user["id"], "number_skipped", number_id)
        send_next_available_to_operator_group(user, number_id or None, target_chat_id, target_thread_id)
        answer_callback(callback_id)
        return

    if len(parts) < 3:
        answer_callback(callback_id)
        return
    number_id = int(parts[2])
    if number_id == 0:
        send_next_available_to_operator_group(user, target_chat_id=target_chat_id, target_thread_id=target_thread_id)
        answer_callback(callback_id, "Сначала возьмите номер.")
        return
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM numbers WHERE id = ?", (number_id,)).fetchone()
        if not row:
            answer_callback(callback_id, "Номер не найден.", True)
            return
        if action == "take":
            if row["status"] != STATUS_AVAILABLE:
                answer_callback(callback_id, "Номер уже не свободен.", True)
                return
            conn.execute(
                """
                UPDATE numbers
                SET status = ?, assigned_operator_user_id = ?, assigned_at = ?, pending_operator_user_id = NULL
                WHERE id = ?
                """,
                (STATUS_ASSIGNED, user["id"], now_iso(), number_id),
            )
            conn.commit()
            log_event(user["id"], "number_taken", number_id)
            message = operator_group_send(
                f"Номер #{number_id} взят.\nНомер: {row['masked_number']}\nОжидайте код от поставщика.",
                work_active_keyboard(number_id),
                target_chat_id,
                target_thread_id,
            )
            if message:
                conn.execute(
                    "UPDATE numbers SET operator_chat_id = ?, operator_thread_id = ?, operator_message_id = ? WHERE id = ?",
                    (target_chat_id or configured_operator_chat_id(), target_thread_id if target_thread_id is not None else configured_operator_thread_id(), message.get("message_id"), number_id),
                )
                conn.commit()
            prompt = send_message(
                row["source_chat_id"] or configured_drop_chat_id(),
                f"Ваш номер {row['masked_number']} взяли. Введите код для оператора ответом на это сообщение.",
                message_thread_id=row["source_thread_id"] or configured_drop_thread_id() or None,
            )
            if prompt:
                conn.execute(
                    "UPDATE numbers SET source_chat_id = ?, source_thread_id = ?, source_message_id = ? WHERE id = ?",
                    (
                        row["source_chat_id"] or configured_drop_chat_id(),
                        row["source_thread_id"] or configured_drop_thread_id(),
                        prompt.get("message_id"),
                        number_id,
                    ),
                )
                conn.commit()
            answer_callback(callback_id, "Номер выдан.")
            return
        if action in {"approve", "reject"}:
            operator_user_id = int(parts[3]) if len(parts) > 3 else row["pending_operator_user_id"]
            if row["supplier_user_id"] != user["id"]:
                answer_callback(callback_id, "Подтвердить может только тот, кто выдал номер.", True)
                return
            operator = conn.execute("SELECT * FROM users WHERE id = ?", (operator_user_id,)).fetchone()
            if action == "reject":
                conn.execute("UPDATE numbers SET pending_operator_user_id = NULL WHERE id = ?", (number_id,))
                conn.commit()
                operator_group_send(f"Номер {row['masked_number']} не выдан. Возьмите другой номер.", work_menu_keyboard(), target_chat_id, target_thread_id)
                answer_callback(callback_id, "Отказано.")
                return
            conn.execute(
                """
                UPDATE numbers
                SET status = ?, assigned_operator_user_id = ?, assigned_at = ?, pending_operator_user_id = NULL
                WHERE id = ?
                """,
                (STATUS_ASSIGNED, operator_user_id, now_iso(), number_id),
            )
            conn.commit()
            log_event(operator_user_id, "number_taken", number_id)
            message = operator_group_send(
                f"Номер #{number_id} взят.\nНомер: {row['masked_number']}\nОжидайте код от поставщика.",
                work_active_keyboard(number_id),
                target_chat_id,
                target_thread_id,
            )
            if message:
                conn.execute(
                    "UPDATE numbers SET operator_chat_id = ?, operator_thread_id = ?, operator_message_id = ? WHERE id = ?",
                    (target_chat_id or configured_operator_chat_id(), target_thread_id if target_thread_id is not None else configured_operator_thread_id(), message.get("message_id"), number_id),
                )
                conn.commit()
            prompt = send_message(
                row["source_chat_id"] or configured_drop_chat_id(),
                f"Ваш номер {row['masked_number']} взяли. Введите код для оператора ответом на это сообщение.",
                message_thread_id=row["source_thread_id"] or configured_drop_thread_id() or None,
            )
            if prompt:
                conn.execute(
                    "UPDATE numbers SET source_chat_id = ?, source_thread_id = ?, source_message_id = ? WHERE id = ?",
                    (
                        row["source_chat_id"] or configured_drop_chat_id(),
                        row["source_thread_id"] or configured_drop_thread_id(),
                        prompt.get("message_id"),
                        number_id,
                    ),
                )
                conn.commit()
            answer_callback(callback_id, "Номер выдан.")
            return

        if row["assigned_operator_user_id"] != user["id"]:
            answer_callback(callback_id, "Этот номер закреплен за другим оператором.", True)
            return
        supplier = conn.execute("SELECT * FROM users WHERE id = ?", (row["supplier_user_id"],)).fetchone()
        source_chat_id = row["source_chat_id"] or (supplier["telegram_id"] if supplier else configured_drop_chat_id())
        source_thread_id = row["source_thread_id"] or configured_drop_thread_id() or None
        if action == "done":
            conn.execute(
                "UPDATE numbers SET status = ?, remaining = 0, completed_at = ? WHERE id = ?",
                (STATUS_DONE, now_iso(), number_id),
            )
            conn.commit()
            log_event(user["id"], "number_done", number_id)
            send_message(chat_id, f"По номеру {row['masked_number']} он встал.", work_menu_keyboard(), message_thread_id=target_thread_id or None)
            send_message(source_chat_id, f"По номеру {row['masked_number']} он встал.", message_thread_id=source_thread_id)
        elif action == "failed":
            conn.execute(
                "UPDATE numbers SET status = ?, remaining = 0, completed_at = ?, last_reason = ? WHERE id = ?",
                (STATUS_FAILED, now_iso(), "Не встал", number_id),
            )
            conn.commit()
            log_event(user["id"], "number_failed", number_id)
            send_message(chat_id, f"По номеру {row['masked_number']} не встал.", work_menu_keyboard(), message_thread_id=target_thread_id or None)
            send_message(source_chat_id, f"По номеру {row['masked_number']} не встал.", message_thread_id=source_thread_id)
        elif action == "repeat_code":
            log_event(user["id"], "repeat_message_requested", number_id)
            send_message(chat_id, f"По номеру {row['masked_number']} запрошен повтор кода.", work_active_keyboard(number_id), message_thread_id=target_thread_id or None)
            prompt = send_message(source_chat_id, f"Повторный код нужен по номеру, введите код в ответ на это сообщение {row['masked_number']}.", message_thread_id=source_thread_id)
            if prompt:
                conn.execute(
                    "UPDATE numbers SET source_chat_id = ?, source_thread_id = ?, source_message_id = ? WHERE id = ?",
                    (source_chat_id, source_thread_id or 0, prompt.get("message_id"), number_id),
                )
                conn.commit()
    answer_callback(callback_id)


def handle_callback(callback):
    callback_id = callback["id"]
    chat_id = callback["message"]["chat"]["id"]
    tg_from = callback["from"]
    telegram_id = tg_from["id"]
    data = callback["data"]
    user = create_or_touch_user(telegram_id, extract_username(tg_from))
    if user["is_blocked"]:
        answer_callback(callback_id, "Доступ заблокирован.", True)
        return
    if data.startswith("work:"):
        handle_work_callback(callback_id, callback, user, data)
        return
    delete_message(chat_id, callback["message"]["message_id"])

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
    if data.startswith("broadcast:"):
        handle_broadcast_callback(callback_id, chat_id, user, data)
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
            send_message(chat_id, "Админ-панель", admin_keyboard())
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
    send_message(chat_id, "\n".join(lines), inline_keyboard([[('Вывод', 'menu:withdraw')], back_row()]))


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


def number_button_rows(rows, prefix, back_to="menu:home"):
    keyboard_rows = []
    for row in rows:
        keyboard_rows.append([(f"📱 {row['masked_number']}", f"{prefix}:{row['id']}")])
    keyboard_rows.append(back_row(back_to))
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
        send_message(chat_id, "🌐 Общая очередь пуста. 📭", inline_keyboard([admin_back_row()]))
        return
    send_message(
        chat_id,
        f"🌐 Общая очередь в боте\n📦 Всего номеров в очереди: {total}\n👇 Нажмите номер, чтобы убрать его из очереди.",
        inline_keyboard(number_button_rows(rows, "queue:clear", "admin:panel")),
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
    buttons.append(admin_back_row())
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
    buttons.append(admin_back_row())
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
        send_state_prompt(chat_id, admin["id"], "admin_change_password", "⚠️ Пароль должен быть минимум 4 символа. Введите другой пароль.", back_to="admin:panel")
        return
    set_access_password(new_password)
    clear_state(admin["id"])
    log_event(admin["id"], "admin_change_password", details="changed")
    send_message(chat_id, "🔐 Пароль доступа к боту изменен.", admin_keyboard())



def handle_admin_change_price(chat_id, admin, text):
    if not admin["is_admin"]:
        clear_state(admin["id"])
        send_message(chat_id, "⛔ Нет доступа.")
        return
    price = parse_amount(text)
    if price is None:
        send_state_prompt(chat_id, admin["id"], "admin_change_price", "⚠️ Введите прайс числом, например: 1 или 2.50", back_to="admin:panel")
        return
    set_price_per_number(price)
    clear_state(admin["id"])
    log_event(admin["id"], "admin_change_price", details=f"price={price}")
    send_message(chat_id, f"💵 Прайс изменен: {money_text(price)} за номер.", admin_keyboard())

def clear_database():
    with closing(db()) as conn:
        for table in ("numbers", "logs", "withdrawals", "users", "settings"):
            conn.execute(f"DELETE FROM {table}")
        conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('numbers', 'logs', 'withdrawals', 'users')")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('auto_reports_enabled', '0')")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('price_per_number', '1')")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('work_enabled', '0')")
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
    send_message(chat_id, f"📲 Номер #{row['id']} взят.\nНомер: {row['masked_number']}\nОжидайте код от поставщика.", operator_active_keyboard(row["id"]))
    if supplier:
        send_state_prompt(
            supplier["telegram_id"],
            supplier["id"],
            "supplier_message",
            f"📩 Ваш номер {row['masked_number']} взяли.\nВведите код для оператора ответом на это сообщение.",
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
            send_message(chat_id, f"По номеру {row['masked_number']} он встал.", main_menu_keyboard(user))
            if supplier:
                send_message(supplier["telegram_id"], f"По номеру {row['masked_number']} он встал.")
        elif action == "repeat_message":
            conn.commit()
            log_event(user["id"], "repeat_message_requested", number_id)
            send_message(chat_id, f"🔁 По номеру {row['masked_number']} запрошен повтор кода.", operator_active_keyboard(number_id))
            if supplier:
                send_state_prompt(
                    supplier["telegram_id"],
                    supplier["id"],
                    "supplier_message",
                    f"Повторный код нужен по номеру, введите код в ответ на это сообщение {row['masked_number']}.",
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
    send_state_prompt(chat_id, user["id"], "admin_withdrawal_message", f"💸 Вывод {money_text(row['amount'])} для {handle}.\n✍️ Введите текст или чек, который нужно отправить пользователю.", {"withdrawal_id": withdrawal_id}, back_to="admin:panel")
    answer_callback(callback_id)




def handle_broadcast_callback(callback_id, chat_id, user, data):
    if not user["is_admin"]:
        answer_callback(callback_id, "⛔ Нет доступа.", True)
        return
    target = data.split(":", 1)[1]
    if target not in {"operators", "suppliers"}:
        answer_callback(callback_id)
        return
    title = "операторов" if target == "operators" else "поставщиков"
    send_state_prompt(
        chat_id,
        user["id"],
        "broadcast",
        f"📣 Введите текст рассылки для группы {title}. Можно использовать Telegram Premium emoji.",
        {"broadcast_target": target},
        back_to="admin:panel",
    )
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
    send_state_prompt(chat_id, user["id"], "admin_direct_message", f"✉️ Введите сообщение для пользователя {user_handle(target)}.", {"target_user_id": target_user_id}, back_to="admin:panel")
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
    if action == "panel":
        clear_state(user["id"])
        send_message(chat_id, "Админ-панель", admin_keyboard())
    elif action == "start_work":
        set_bool_setting("work_enabled", True)
        drop_chat_id = configured_drop_chat_id() or chat_id
        if not configured_drop_chat_id():
            set_setting("drop_group_chat_id", drop_chat_id)
        send_message(drop_chat_id, "номера, работаем!", message_thread_id=configured_drop_thread_id() or None)
        send_message(chat_id, "Админ-панель", admin_keyboard())
    elif action == "stop_work":
        set_bool_setting("work_enabled", False)
        drop_chat_id = configured_drop_chat_id() or chat_id
        send_message(drop_chat_id, "ворк закончен.", message_thread_id=configured_drop_thread_id() or None)
        if configured_operator_chat_id():
            operator_group_send("Работа закончена.")
        send_message(chat_id, "Ворк закончен.", admin_keyboard())
    elif action == "stats":
        send_message(chat_id, build_global_stats(), admin_keyboard())
    elif action == "report_file":
        set_state(user["id"], "admin_report_date", {"report_status": "all", "report_period": "all"})
        send_report_file(chat_id, get_user(user["telegram_id"]))
    elif action == "broadcast":
        send_message(chat_id, "📣 Куда отправить рассылку?", broadcast_target_keyboard())
    answer_callback(callback_id)


def send_auto_reports_if_needed(state):
    # Автоотчеты отключены: отчет отправляется только по кнопке «📄 Отчет файлом».
    return


def poll():
    init_db()
    offset = 0
    runtime_state = {"last_auto_report_at": time.time()}
    try:
        api("deleteWebhook", {"drop_pending_updates": "false"})
    except TelegramAPIError as exc:
        print(f"deleteWebhook warning: {exc.description}")
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
        except TelegramAPIError as exc:
            if is_conflict_error(exc):
                print("Error 409: запущен другой экземпляр бота или активен webhook. Остановите второй процесс/деплой; бот повторит попытку.")
                time.sleep(10)
            elif is_bad_request(exc):
                print(f"Error 400 in {exc.method}: {exc.description}")
                time.sleep(3)
            else:
                print(f"Telegram API error in {exc.method}: {exc.description}")
                time.sleep(3)
        except Exception as exc:
            print(f"Error: {exc}")
            time.sleep(3)


if __name__ == "__main__":
    poll()
