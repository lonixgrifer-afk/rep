import json
import asyncio
import os
from datetime import datetime
from pathlib import Path
from io import BytesIO

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)
from playwright.async_api import async_playwright

BOT_TOKEN = "8967607425:AAGPblsB4gnTStoxHCYuVqPED-eE3JvyNys"
BASE_URL = "https://web.max.ru"
SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True)

def session_path(chat_id: int) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    return SESSIONS_DIR / f"session_{chat_id}_{ts}.json"

def chat_sessions(chat_id: int) -> list[Path]:
    return sorted(SESSIONS_DIR.glob(f"session_{chat_id}_*.json"))

def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📲 Получить QR", callback_data="qr:get")],
        [InlineKeyboardButton("🗂 Мои сессии", callback_data="session:list")],
    ])

def get_js_console_code_raw(file_path: Path) -> str:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        storage = data['origins'][0]['localStorage']
        device_id = next(item['value'] for item in storage if item['name'] == '__oneme_device_id')
        auth_data = next(item['value'] for item in storage if item['name'] == '__oneme_auth')
        auth_ready = auth_data.replace("'", "\\'")
        
        return (
            f"sessionStorage.clear();\n"
            f"localStorage.clear();\n"
            f"localStorage.setItem('__oneme_device_id', '{device_id}');\n"
            f"localStorage.setItem('__oneme_auth', '{auth_ready}');\n"
            f"localStorage.setItem('__oneme_locale', 'ru');\n"
            f"localStorage.setItem('__oneme_theme', '{{\"colorScheme\":\"system\",\"colorTheme\":\"space\"}}');\n"
            f"window.location.reload();"
        )
    except: return ""

async def capture_qr_image(page) -> bytes:
    selectors = ["canvas", 'img[src*="qr"]', 'div[class*="qr"]', 'svg']
    for selector in selectors:
        try:
            handle = await page.wait_for_selector(selector, timeout=10000, state="visible")
            if handle: return await handle.screenshot(type="png")
        except: continue
    return await page.screenshot(type="png", clip={'x': 0, 'y': 0, 'width': 500, 'height': 500})

async def wait_success_login(page) -> None:
    await page.wait_for_function(
        """() => {
          const text = document.body ? document.body.innerText.toLowerCase() : "";
          return !(text.includes("qr") || text.includes("сканируйте") || text.includes("войдите"));
        }""", timeout=180000
    )

async def run_qr_process(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(viewport={'width': 400, 'height': 600})
        page = await ctx.new_page()
        try:
            await page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(2) 
            qr_bytes = await capture_qr_image(page)
            
            await context.bot.send_photo(
                chat_id=chat_id, 
                photo=qr_bytes, 
                caption="✅ QR готов! Сканируй для входа."
            )
            
            await wait_success_login(page)
            
            spath = session_path(chat_id)
            await ctx.storage_state(path=str(spath))
            await context.bot.send_message(
                chat_id=chat_id, 
                text=f"🎉 Авторизация успешна! Сессия сохранена.",
                reply_markup=main_menu()
            )
        except Exception as e:
            await context.bot.send_message(chat_id=chat_id, text="❌ Ошибка или время истекло.")
        finally:
            await browser.close()

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Выберите действие:", reply_markup=main_menu())

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = query.message.chat_id
    data = query.data
    await query.answer()

    if data == "qr:get":
        await query.message.reply_text("🚀 Запускаю получение QR...")
        asyncio.create_task(run_qr_process(chat_id, context))

    elif data == "session:list":
        sessions = chat_sessions(chat_id)
        if not sessions:
            await query.edit_message_text("Список сессий пуст.", reply_markup=main_menu())
            return
        
        keyboard = []
        for i, s in enumerate(sessions, start=1):
            # Отображаем время создания для удобства
            time_mark = s.name.split('_')[2] 
            keyboard.append([InlineKeyboardButton(f"📁 Сессия №{i} ({time_mark})", callback_data=f"sess_manage:{s.name}")])
        
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])
        await query.edit_message_text("Выберите сессию для управления:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("sess_manage:"):
        filename = data.split(":")[1]
        keyboard = [
            [InlineKeyboardButton("📜 Получить скрипт", callback_data=f"sess_get:{filename}")],
            [InlineKeyboardButton("🗑 Удалить сессию", callback_data=f"sess_del:{filename}")],
            [InlineKeyboardButton("⬅️ Назад к списку", callback_data="session:list")]
        ]
        await query.edit_message_text(f"Управление сессией: {filename}", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("sess_get:"):
        filename = data.split(":")[1]
        target = SESSIONS_DIR / filename
        if target.exists():
            js_code = get_js_console_code_raw(target)
            bio = BytesIO(js_code.encode('utf-8'))
            bio.name = "login.txt"
            await query.message.reply_document(document=bio, caption="Инструкция внутри файла.")
        else:
            await query.message.reply_text("Ошибка: файл не найден.")

    elif data.startswith("sess_del:"):
        filename = data.split(":")[1]
        target = SESSIONS_DIR / filename
        if target.exists():
            os.remove(target)
            await query.edit_message_text(f"✅ Сессия {filename} удалена.", reply_markup=main_menu())
        else:
            await query.edit_message_text("❌ Сессия уже удалена или не существует.", reply_markup=main_menu())

    elif data == "back_to_main":
        await query.edit_message_text("Выберите действие:", reply_markup=main_menu())

def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CallbackQueryHandler(callback_router))
    print("Бот запущен...")
    app.run_polling()

if __name__ == "__main__":
    main()
