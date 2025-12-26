import os
import logging
import asyncio
from aiohttp import web
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

# Настройка логов - это поможет нам увидеть ошибки подключения
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
TOKEN = "8431935487:AAFBSEtd1uU6h2rAf7vwlNKLguZYSNtuIXE"
DATABASE_URL = "postgresql://postgres:Prokopenko_772@db.kkfckmmbdvohwujpxofc.supabase.co:5432/postgres"

bot = Bot(token=TOKEN, parse_mode='HTML')
dp = Dispatcher(bot)

# Класс для работы с базой (упрощенный)
class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(
                DATABASE_URL,
                ssl="require",
                statement_cache_size=0 # Важно для Supabase
            )
            logger.info("✅ УСПЕХ: База данных Supabase подключена!")
            
            # Создаем тестовую таблицу, если её нет
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS test_users (
                        id SERIAL PRIMARY KEY,
                        telegram_id BIGINT UNIQUE NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
        except Exception as e:
            logger.error(f"❌ ОШИБКА подключения к базе: {e}")
            raise e

db = Database()

# --- Кнопки интерфейса как в оригинале ---
def get_main_menu():
    markup = ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row(KeyboardButton("🎮 Заказать игру"), KeyboardButton("📅 Консультация"))
    markup.row(KeyboardButton("💰 Личный кабинет"), KeyboardButton("📞 Поддержка"))
    return markup

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    # Пробуем записать пользователя в базу для теста
    try:
        async with db.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO test_users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING",
                message.from_user.id
            )
        
        await message.answer(
            f"<b>Привет, {message.from_user.full_name}!</b>\n\n"
            "Это тестовый запуск бота. Если ты видишь это сообщение и кнопки внизу, "
            "значит подключение к базе данных прошло успешно! ✅",
            reply_markup=get_main_menu()
        )
    except Exception as e:
        await message.answer(f"Ошибка при работе с базой: {e}")

# --- Поддержка Render ---
async def handle(request):
    return web.Response(text="Bot is alive")

async def on_startup(_):
    # Сначала база
    await db.connect()
    # Потом веб-сервер для Render
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", 10000)))
    asyncio.create_task(site.start())

if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup)