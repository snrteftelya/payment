import os
import asyncio
from aiohttp import web
import logging
import json
from datetime import datetime
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import select
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

Base = declarative_base()

from sqlalchemy import Column, Integer, String, Boolean, BigInteger, JSON, DateTime, Index


class TributePurchase(Base):
    __tablename__ = 'tribute_purchases'

    id = Column(Integer, primary_key=True, autoincrement=True)
    purchase_id = Column(String, unique=True, nullable=False)
    user_id = Column(BigInteger, nullable=False)
    telegram_user_id = Column(BigInteger, nullable=False)
    product_id = Column(String, nullable=False)
    product_name = Column(String, nullable=False)
    amount = Column(Integer, nullable=False)
    currency = Column(String, default='stars')
    status = Column(String, default='completed')
    service_activated = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)  # Добавь эту строку
    activated_at = Column(DateTime, nullable=True)
    raw_data = Column(JSON, nullable=True)

    __table_args__ = (
        Index('ix_tribute_purchases_user_id', 'user_id'),
        Index('ix_tribute_purchases_telegram_user_id', 'telegram_user_id'),
        Index('ix_tribute_purchases_purchase_id', 'purchase_id'),
    )


class PaymentWebhookHandler:
    def __init__(self, bot: Bot, session_maker):
        self.bot = bot
        self.session_maker = session_maker

    async def handle_webhook(self, request):
        """Обработчик всех вебхуков от Tribute.tg"""
        try:
            client_ip = request.remote
            logger.info(f"📨 Входящий запрос от {client_ip}")

            data = await request.json()
            logger.info(f"📊 Получен вебхук: {json.dumps(data, indent=2, ensure_ascii=False)}")

            event_type = self.determine_event_type(data)
            logger.info(f"🎯 Тип события: {event_type}")

            if event_type == 'new_digital_product':
                await self.handle_new_digital_product(data)
            elif event_type == 'payment.completed':
                await self.handle_payment_completed(data)
            else:
                logger.info(f"🔍 Пропускаем событие: {event_type}")

            return web.Response(text='OK', status=200)

        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON: {e}")
            return web.Response(text='OK', status=200)
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка: {e}")
            return web.Response(text='OK', status=200)

    def determine_event_type(self, data):
        """Определяет тип события на основе структуры данных"""
        if 'name' in data:
            return data.get('name')
        elif 'type' in data:
            return data.get('type')
        elif 'event' in data:
            return data.get('event')
        else:
            return 'unknown'

    async def handle_new_digital_product(self, data):
        """Обработка покупки цифрового товара в звездах"""
        try:
            payload = data.get('payload', {})
            purchase_id = payload.get('purchase_id')
            telegram_user_id = payload.get('telegram_user_id')
            product_name = payload.get('product_name')
            amount = payload.get('amount', 0)

            if not purchase_id:
                logger.error("❌ Нет purchase_id в данных")
                return

            logger.info(f"💰 НОВАЯ ПОКУПКА: {product_name} для пользователя {telegram_user_id}")

            await self.save_tribute_purchase(data)
            await self.send_payment_notification(telegram_user_id, product_name, amount, purchase_id)

            logger.info(f"✅ Покупка {purchase_id} полностью обработана")

        except Exception as e:
            logger.error(f"❌ Ошибка обработки цифрового товара: {e}")

    async def save_tribute_purchase(self, data):
        """Сохраняем информацию о покупке в базу"""
        try:
            payload = data.get('payload', {})

            async with self.session_maker() as session:
                existing_purchase = await session.execute(
                    select(TributePurchase).where(TributePurchase.purchase_id == str(payload.get('purchase_id')))
                )
                if existing_purchase.scalar_one_or_none():
                    logger.info(f"⚠️ Покупка {payload.get('purchase_id')} уже обработана")
                    return

                purchase = TributePurchase(
                    purchase_id=str(payload.get('purchase_id')),
                    user_id=payload.get('trb_user_id'),
                    telegram_user_id=payload.get('telegram_user_id'),
                    product_id=str(payload.get('product_id')),
                    product_name=payload.get('product_name'),
                    amount=payload.get('amount', 0),
                    currency=payload.get('currency', 'stars'),
                    raw_data=data
                )

                session.add(purchase)
                await session.commit()

                logger.info(f"💾 Покупка {payload.get('purchase_id')} сохранена в базу")

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения покупки в базу: {e}")
            raise

    async def send_payment_notification(self, user_id: int, product_name: str, amount: int, purchase_id: str):
        """Отправка уведомления пользователю"""
        try:
            message = f"cпасибо за оплату)\n\nнажми на \"🔓 Разблокировать сейчас\" выше, чтобы активировать {product_name} 🫶"
            await self.bot.send_message(user_id, message)
            logger.info(f"💬 Уведомление отправлено пользователю {user_id}")

        except Exception as e:
            logger.error(f"❌ Ошибка отправки уведомления пользователю {user_id}: {e}")
            raise

    async def handle_payment_completed(self, data):
        """Обработка завершенного платежа"""
        try:
            payment_data = data.get('data', {})
            payment_id = payment_data.get('id')

            if not payment_id:
                return

            logger.info(f"💰 ОБЫЧНЫЙ ПЛАТЕЖ: {payment_id}")

        except Exception as e:
            logger.error(f"❌ Ошибка обработки платежа: {e}")


async def health_check(request):
    return web.Response(
        text='🚀 Tribute.tg Webhook Server - ALL SYSTEMS GO!\n\n'
             'Endpoints:\n'
             '- GET  /health\n'
             '- POST /webhook/tribute\n\n'
             'Статус: ✅ РАБОТАЕТ',
        status=200
    )


async def create_app():
    """Создание приложения"""
    bot_token = os.getenv('BOT_TOKEN')
    if not bot_token:
        logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: BOT_TOKEN не установлен!")
        raise ValueError("BOT_TOKEN environment variable is required")

    # Инициализация бота
    logger.info("🤖 Инициализация бота...")
    bot = Bot(
        token=bot_token,
        default=DefaultBotProperties(parse_mode='HTML')
    )
    database_url = os.getenv('DATABASE_URL')

    # Инициализация базы данных PostgreSQL
    logger.info("🗄️ Инициализация PostgreSQL...")
    logger.info(f"🔗 Подключение к: {database_url}")

    try:
        engine = create_async_engine(
            database_url,
            echo=True,
            pool_pre_ping=True,  # Проверяет соединение перед использованием
            pool_recycle=1800,   # Пересоздает соединения каждые 30 минут
            pool_size=10,        # Размер пула (можно настроить под нагрузку)
            max_overflow=20      # Доп. соединения при пиковой нагрузке
        )
        # Тестируем подключение
        async with engine.begin() as conn:
            await conn.execute(select(1))
            logger.info("✅ Подключение к PostgreSQL успешно")

        session_maker = async_sessionmaker(engine, expire_on_commit=False)

        # Создаем таблицы
        logger.info("📊 Создание таблиц...")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("✅ Таблицы созданы успешно")

    except Exception as e:
        logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
        raise

    handler = PaymentWebhookHandler(bot, session_maker)

    app = web.Application()
    app.router.add_post('/webhook/tribute', handler.handle_webhook)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)

    logger.info("✅ Приложение успешно создано")
    return app


async def main():
    """Основная функция"""
    try:
        app = await create_app()

        port = int(os.getenv('PORT', 8080))
        host = 'localhost'

        logger.info(f"🚀 Запуск сервера на {host}:{port}")

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()

        logger.info(f"🌐 Payment Service успешно запущен!")
        logger.info(f"📊 Health check: http://{host}:{port}/health")
        logger.info(f"🔔 Webhook endpoint: http://{host}:{port}/webhook/tribute")
        logger.info("🛑 Для остановки нажмите Ctrl+C")

        while True:
            await asyncio.sleep(3600)

    except OSError as e:
        if e.errno == 48:
            logger.error(f"❌ Порт {port} занят. Попробуйте другой порт:")
            logger.info("   export PORT=8081")
        else:
            logger.error(f"❌ Ошибка OS: {e}")
        raise
    except KeyboardInterrupt:
        logger.info("🛑 Остановка сервера по запросу пользователя")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise
    finally:
        if 'runner' in locals():
            await runner.cleanup()
        logger.info("👋 Сервер остановлен")


if __name__ == '__main__':
    print("🚀 ЗАПУСК TRIBUTE.TG PAYMENT SERVICE")
    print("=" * 50)

    bot_token = os.getenv('BOT_TOKEN')
    database_url = os.getenv('DATABASE_URL')

    if not bot_token:
        print("❌ BOT_TOKEN: НЕ УСТАНОВЛЕН")
        print("💡 export BOT_TOKEN='ваш_токен_бота'")
        exit(1)
    else:
        print("✅ BOT_TOKEN: УСТАНОВЛЕН")

    print(f"✅ DATABASE_URL: {database_url}")
    print("=" * 50)
    print("ЗАПУСК СЕРВЕРА...")

    asyncio.run(main())