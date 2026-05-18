import json
import os
import urllib.request
import zipfile
import urllib.parse
import asyncio
from datetime import datetime
from io import BytesIO
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

# Конфигурация
# Настройки в самом верху maxx.py
BOT_TOKEN = "8967607425:AAGPblsB4gnTStoxHCYuVqPED-eE3JvyNys"
CRYPTO_PAY_TOKEN = "583752:AAitno5sv2mSdC8rdRzuQXdyXnCGyAKqvWy"  # <--- Прямо внутри кавычек
CRYPTO_PAY_API = "https://pay.crypt.bot/api"
BASE_URL = "https://web.max.ru"
SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True)
USERS_FILE = Path("users.json")
EVENTS_FILE = Path("events.jsonl")
INVOICES_FILE = Path("invoices.json")

MIN_QR_BALANCE = 0.1
REFERRAL_BONUS = 0.2
QR_PRICE = 0.1

ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "8779583069").split(",") if x.strip().isdigit()}

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
            # Добавляем User-Agent, чтобы Cloudflare думал, что запрос идет от обычного браузера
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
    users = load_users()
    key = str(chat_id)
    if key not in users:
        users[key] = {"balance": 0.0, "referrer": None, "referrals": 0, "has_recharged": False}
    user = users[key]
    balance = float(user.get("balance", 0.0))
    if balance < QR_PRICE: return False, balance
    user["balance"] = round(balance - QR_PRICE, 2)
    users[key] = user
    save_users(users)
    log_event("qr_charged", {"chat_id": chat_id, "amount": QR_PRICE})
    return True, user["balance"]

def is_admin(chat_id: int) -> bool: return chat_id in ADMIN_IDS
def session_path(chat_id: int) -> Path: return SESSIONS_DIR / f"session_{chat_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}.json"
def chat_sessions(chat_id: int) -> list[Path]: return sorted(SESSIONS_DIR.glob(f"session_{chat_id}_*.json"))

# --- Меню ---
def main_menu(chat_id: int) -> InlineKeyboardMarkup:
    balance = float(get_user(chat_id).get("balance", 0.0))
    
    # Базовая сетка для всех пользователей
    rows = [
        # Первый ряд: Получить QR (слева) и Мои сессии (справа)
        [
            InlineKeyboardButton("📲 Получить QR", callback_data="qr:get"),
            InlineKeyboardButton("🗂 Мои сессии", callback_data="session:list")
        ],
        # Второй ряд: Пополнить баланс (слева) и Рефералка (справа)
        [
            InlineKeyboardButton(f"💳 Пополнить баланс", callback_data="balance:menu"),
            InlineKeyboardButton("👥 Рефералка", callback_data="ref:menu")
        ]
    ]
    
    # Третий ряд: Динамически добавляется В САМЫЙ НИЗ только для админов
    if is_admin(chat_id):
        rows.append([InlineKeyboardButton("🛠 Админ-панель", callback_data="admin:menu")])
        
    return InlineKeyboardMarkup(rows)

def admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("📊 Статистика за сегодня", callback_data="admin:stats")], [InlineKeyboardButton("📣 Рассылка", callback_data="admin:broadcast")], [InlineKeyboardButton("💸 Выдать баланс", callback_data="admin:give_balance")], [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])

def session_menu(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("📄 Список сессий", callback_data="session:show_list")], [InlineKeyboardButton("📦 Выгрузить все сессии", callback_data="session:export_all")], [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])

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

async def run_qr_process(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(viewport={"width": 400, "height": 600})
        page = await ctx.new_page()
        try:
            await page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(2)
            await context.bot.send_photo(chat_id=chat_id, photo=await capture_qr_image(page), caption="✅ QR готов! Сканируй для входа.")
            await wait_success_login(page)
            spath = session_path(chat_id)
            await ctx.storage_state(path=str(spath))
            log_event("token_created", {"chat_id": chat_id, "session_file": spath.name})
            await context.bot.send_message(chat_id=chat_id, text="🎉 Авторизация успешна! Сессия сохранена.", reply_markup=main_menu(chat_id))
        except Exception:
            await context.bot.send_message(chat_id=chat_id, text="❌ Ошибка или время истекло.")
        finally:
            await browser.close()

# --- Команды Telegram ---
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user = get_user(chat_id)
    
    # Сохраняем username пользователя, если он есть
    if update.effective_user and update.effective_user.username:
        user["username"] = update.effective_user.username
        update_user(chat_id, user)
        
    # Проверяем реферальный код
    if context.args and context.args[0].startswith("ref_"):
        try:
            ref_id = int(context.args[0].replace("ref_", ""))
            if ref_id != chat_id and not user.get("referrer"):
                user["referrer"] = ref_id
                update_user(chat_id, user)
                await update.message.reply_text("✅ Реферальный код применен.")
        except ValueError: pass

    # Вытаскиваем баланс для текста сообщения
    balance = float(user.get("balance", 0.0))

    # Красивое сообщение с балансом при старте
    welcome_text = (
        f"💳 **Ваш баланс:** `${balance:.2f}`\n\n"
        f"👇 Выберите нужное действие в меню ниже:"
    )

    await update.message.reply_text(
        text=welcome_text, 
        parse_mode="Markdown", 
        reply_markup=main_menu(chat_id)
    )

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_chat.id):
        await update.message.reply_text("⛔ Нет доступа.")
        return
    await update.message.reply_text("🛠 Админ-панель:", reply_markup=admin_menu())

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()
    
    # --- ЛОГИКА ДЛЯ ОБЫЧНЫХ ПОЛЬЗОВАТЕЛЕЙ (Ввод своей суммы) ---
    user_mode = context.user_data.get("user_mode")
    if user_mode == "enter_balance":
        # Заменяем запятую на точку, если юзер ввел например "0,5"
        text_clean = text.replace(",", ".")
        try:
            amount = float(text_clean)
            if amount <= 0:
                await update.message.reply_text("❌ Сумма должна быть больше нуля. Введите корректное число:")
                return
            # Округляем до 2 знаков после запятой
            amount = round(amount, 2)
        except ValueError:
            await update.message.reply_text("❌ Непонятная сумма. Введите число (например: 0.5 или 10):")
            return
        
        # Сбрасываем режим ожидания ввода
        context.user_data.pop("user_mode", None)
        
        # Создаем инвойс в Crypto Bot на введенную сумму
        ok, inv = create_invoice(chat_id, amount)
        if not ok:
            await update.message.reply_text(
                f"❌ Не удалось создать счет: {inv.get('error', 'unknown')}", 
                reply_markup=main_menu(chat_id)
            )
            return
            
        pay_url = inv.get("pay_url") or inv.get("bot_invoice_url") or inv.get("mini_app_invoice_url", "")
        kb = [
            [InlineKeyboardButton("💳 Перейти к оплате", url=pay_url)], 
            [InlineKeyboardButton("⬅️ Главное меню", callback_data="back_to_main")]
        ]
        await update.message.reply_text(
            f"🚀 Ссылка на оплату **${amount:.2f}** успешно создана!\n\n"
            f"Баланс пополнится автоматически сразу после оплаты.", 
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    # --- ЛОГИКА АДМИН-ПАНЕЛИ (Оставляем как было) ---
    if not is_admin(chat_id): 
        return
        
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
        
    elif admin_mode == "give_balance":
        parts = text.split()
        if len(parts) != 2:
            await update.message.reply_text("Формат: <username> <сумма>")
            return
        uid, u = get_user_by_username(parts[0])
        if uid is None:
            await update.message.reply_text("❌ Пользователь не найден.")
            return
        try: amount = float(parts[1])
        except ValueError:
            await update.message.reply_text("❌ Неверная сумма.")
            return
        user, _ = add_balance(uid, amount, source="admin")
        context.user_data.pop("admin_mode", None)
        await update.message.reply_text(f"✅ Выдано ${amount:.2f} @{u.get('username','')}. Баланс: ${float(user.get('balance',0)):.2f}")

# --- Роутер инлайн кнопок ---
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = query.message.chat_id
    data = query.data
    await query.answer()

    if data == "qr:get":
        balance = float(get_user(chat_id).get("balance", 0.0))
        if balance < MIN_QR_BALANCE:
            await query.message.reply_text(f"❌ Недостаточно средств. Нужно минимум ${MIN_QR_BALANCE:.2f}. Баланс: ${balance:.2f}", reply_markup=main_menu(chat_id))
            return
        ok, new_balance = charge_for_qr(chat_id)
        if not ok:
            await query.message.reply_text(f"❌ Недостаточно средств. Баланс: ${new_balance:.2f}", reply_markup=main_menu(chat_id))
            return
        await query.message.reply_text(f"✅ Списано ${QR_PRICE:.2f}. Остаток: ${new_balance:.2f}\n🚀 Запускаю получение QR...")
        context.application.create_task(run_qr_process(chat_id, context))

    elif data == "balance:menu":
        # Переводим пользователя в статус ожидания ввода суммы
        context.user_data["user_mode"] = "enter_balance"
        
        kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]]
        await query.edit_message_text(
            f"💳 Ваш текущий баланс: ${float(get_user(chat_id).get('balance', 0.0)):.2f}\n\n"
            f"✍️ **Введите сумму в чат**:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    elif data.startswith("balance:create:"):
        amount = float(data.split(":")[-1])
        ok, inv = create_invoice(chat_id, amount)
        if not ok:
            await query.edit_message_text(f"❌ Не удалось создать счет: {inv.get('error','unknown')}", reply_markup=main_menu(chat_id))
            return
        pay_url = inv.get("pay_url") or inv.get("bot_invoice_url") or inv.get("mini_app_invoice_url", "")
        kb = [[InlineKeyboardButton("💳 Перейти к оплате", url=pay_url)], [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]]
        await query.edit_message_text(f"🚀 Ссылка на оплату ${amount:.2f} создана.\n\nБаланс пополнится автоматически сразу после оплаты. Можете не закрывать это меню.", reply_markup=InlineKeyboardMarkup(kb))

    elif data == "ref:menu":
        uname = (await context.bot.get_me()).username
        u = get_user(chat_id)
        await query.edit_message_text(f"👥 Реферальная программа:\n• За каждого кто зайдет по ссылке будет засчитанно: ${REFERRAL_BONUS:.2f} \n• Успешные: {int(u.get('referrals',0))}\n\nСсылка:\nhttps://t.me/{uname}?start=ref_{chat_id}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]]))

    elif data == "session:list":
        await query.edit_message_text("🗂 Мои сессии:", reply_markup=session_menu(chat_id))
    elif data == "session:show_list":
        sessions = chat_sessions(chat_id)
        if not sessions:
            await query.edit_message_text("Список сессий пуст.", reply_markup=session_menu(chat_id))
            return
        kb = [[InlineKeyboardButton(f"📁 Сессия №{i}", callback_data=f"sess_manage:{s.name}")] for i, s in enumerate(sessions, 1)]
        kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="session:list")])
        await query.edit_message_text("Выберите сессию:", reply_markup=InlineKeyboardMarkup(kb))
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
        await query.edit_message_text(f"Управление: {fn}", reply_markup=InlineKeyboardMarkup(kb))
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
            await query.edit_message_text(f"✅ Сессия {fn} удалена.", reply_markup=session_menu(chat_id))
        else:
            await query.edit_message_text("❌ Сессия уже удалена или не существует.", reply_markup=session_menu(chat_id))

    elif data == "admin:menu":
        if not is_admin(chat_id): return
        await query.edit_message_text("🛠 Админ-панель:", reply_markup=admin_menu())
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
        await query.edit_message_text(f"📊 Статистика: \n• Людей: {st['users_count']}\n• Пополнения: ${st['topups_sum']:.2f}\n• Токенов: {st['tokens_count']}", reply_markup=admin_menu())
    elif data == "admin:broadcast":
        if not is_admin(chat_id): return
        context.user_data["admin_mode"] = "broadcast"
        await query.message.reply_text("Введите текст для рассылки.")
    elif data == "admin:give_balance":
        if not is_admin(chat_id): return
        context.user_data["admin_mode"] = "give_balance"
        await query.message.reply_text("Введите: <username> <сумма>")
    elif data == "back_to_main":
        context.user_data.pop("user_mode", None)  # Сбрасываем режим ввода
        await query.edit_message_text("Выберите действие:", reply_markup=main_menu(chat_id))


# --- ФОНОВАЯ ЗАДАЧА ПРОВЕРКИ ОПЛАТЫ (Через Поллинг API) ---
async def check_invoices_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    invoices = load_invoices()
    # Фильтруем только те счета, которые созданы, но еще не зачислены
    active_ids = [k for k, v in invoices.items() if not v.get("credited")]
    
    if not active_ids:
        return

    # Запрашиваем статусы пачкой (максимум по 100 шт за раз, согласно лимитам API)
    # Передаем id счетов через запятую
    resp = crypto_api_call("getInvoices", {"invoice_ids": ",".join(active_ids)})
    if not resp.get("ok"):
        return

    items = resp.get("result", {}).get("items", [])
    updated = False

    for item in items:
        status = item.get("status")
        invoice_id = str(item.get("invoice_id"))
        
        if status == "paid" and invoice_id in invoices:
            local = invoices[invoice_id]
            if local.get("credited"):
                continue
                
            chat_id = int(local["chat_id"])
            amount = float(local["amount"])

            # Начисляем средства
            user, ref_chat_id = add_balance(chat_id, amount, source="cryptopay_polling")
            local["credited"] = True
            invoices[invoice_id] = local
            updated = True

            # Уведомляем пользователя
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"💳 **Баланс успешно пополнен!**\n\nЗачислено: `${amount:.2f}`\nТекущий баланс: `${float(user.get('balance', 0.0)):.2f}`",
                    parse_mode="Markdown",
                    reply_markup=main_menu(chat_id)
                )
                # Если это первая оплата и есть реферер — платим ему бонус
                if ref_chat_id:
                    ref_user = get_user(int(ref_chat_id))
                    await context.bot.send_message(
                        chat_id=int(ref_chat_id),
                        text=f"👥 **Реферальный бонус!**\n\nВаш реферал пополнил баланс. Вам начислено `${REFERRAL_BONUS:.2f}`\nВаш баланс: `${float(ref_user.get('balance', 0.0)):.2f}`",
                        parse_mode="Markdown"
                    )
            except Exception as e:
                print(f"Не удалось отправить уведомление для {chat_id}: {e}")

    if updated:
        save_invoices(invoices)


def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Регистрация стандартных хэндлеров
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    # Настройка фонового джоба (Проверка раз в 7 секунд)
    job_queue = app.job_queue
    job_queue.run_repeating(check_invoices_job, interval=7, first=5)

    print("Бот запущен в режиме Polling...")
    app.run_polling()


if __name__ == "__main__":
    main()
