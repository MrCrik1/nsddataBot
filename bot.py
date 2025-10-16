import requests
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from bs4 import BeautifulSoup
import sqlite3
import asyncio
import os
from datetime import datetime, timedelta
import re
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NSDMonitor:
    def __init__(self):
        self.setup_database()
        self.base_url = "https://nsddata.ru"
        
    def setup_database(self):
        """Создаем базу данных для отслеживания новостей"""
        self.conn = sqlite3.connect('nsd_news.db', check_same_thread=False)
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tracked_news (
                id INTEGER PRIMARY KEY,
                news_id TEXT UNIQUE,
                isin TEXT,
                title TEXT,
                event_type TEXT,
                payment_amount TEXT,
                news_url TEXT,
                published_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()
    
    def add_isin_tracking(self, isin_code, user_id):
        """Добавляем ISIN для отслеживания конкретным пользователем"""
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_isins (
                user_id INTEGER,
                isin TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, isin)
            )
        ''')
        cursor.execute(
            'INSERT OR IGNORE INTO user_isins (user_id, isin) VALUES (?, ?)',
            (user_id, isin_code.upper())
        )
        self.conn.commit()
        return cursor.rowcount > 0
    
    def get_user_isins(self, user_id):
        """Получаем список ISIN пользователя"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT isin FROM user_isins WHERE user_id = ?', (user_id,))
        return [row[0] for row in cursor.fetchall()]
    
    def is_news_tracked(self, news_id):
        """Проверяем, есть ли новость в базе"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM tracked_news WHERE news_id = ?', (news_id,))
        return cursor.fetchone() is not None
    
    def save_news(self, news_data):
        """Сохраняем новость в базу"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO tracked_news 
            (news_id, isin, title, event_type, payment_amount, news_url, published_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            news_data['news_id'],
            news_data['isin'],
            news_data['title'],
            news_data['event_type'],
            news_data['payment_amount'],
            news_data['news_url'],
            news_data['published_date']
        ))
        self.conn.commit()
        return cursor.rowcount > 0
    
    def parse_news_page(self, html_content):
        """Парсим страницу с новостью и извлекаем структурированные данные"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Извлекаем заголовок
        title_element = soup.find('h1')
        title = title_element.get_text().strip() if title_element else "Неизвестно"
        
        # Ищем ISIN в тексте (формат RU000A106SE5)
        isin_match = re.search(r'RU[0-9A-Z]{10}', title)
        isin = isin_match.group(0) if isin_match else None
        
        # Определяем тип события
        event_type = "Неизвестно"
        if 'выплата купонного дохода' in title.lower():
            event_type = "Выплата купонного дохода"
        elif 'погашение' in title.lower():
            event_type = "Погашение"
        elif 'оферта' in title.lower():
            event_type = "Оферта"
        
        # Ищем размер выплаты
        payment_amount = None
        payment_match = re.search(r'(\d+[.,]\d+)\s*руб', html_content, re.IGNORECASE)
        if payment_match:
            payment_amount = payment_match.group(1) + " руб."
        
        # Ищем дату публикации
        date_element = soup.find('time') or soup.find('div', class_=re.compile('date'))
        published_date = date_element.get_text().strip() if date_element else datetime.now().strftime("%d.%m.%Y")
        
        return {
            'title': title,
            'isin': isin,
            'event_type': event_type,
            'payment_amount': payment_amount,
            'published_date': published_date
        }
    
    def get_recent_news(self):
        """Получаем последние новости с главной страницы nsddata.ru"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(f"{self.base_url}/ru/news", headers=headers, timeout=10)
            response.encoding = 'utf-8'
            
            if response.status_code != 200:
                logger.error(f"Ошибка доступа к сайту: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем ссылки на новости (нужно адаптировать под структуру сайта)
            news_links = []
            
            # Вариант 1: Ищем по классам/тегам (нужно исследовать сайт)
            news_elements = soup.find_all('a', href=re.compile(r'/ru/news/view/'))
            
            for element in news_elements[:10]:  # Проверяем последние 10 новостей
                href = element.get('href')
                if href and '/ru/news/view/' in href:
                    full_url = f"{self.base_url}{href}" if href.startswith('/') else href
                    news_id = href.split('/')[-1] if '/' in href else href
                    
                    # Парсим каждую новость для получения деталей
                    news_details = self.parse_news_page(response.text)  # Упрощенно
                    
                    news_data = {
                        'news_id': news_id,
                        'news_url': full_url,
                        'title': element.get_text(strip=True) or "Без названия",
                        'isin': news_details.get('isin'),
                        'event_type': news_details.get('event_type'),
                        'payment_amount': news_details.get('payment_amount'),
                        'published_date': news_details.get('published_date')
                    }
                    
                    news_links.append(news_data)
            
            return news_links
            
        except Exception as e:
            logger.error(f"Ошибка при получении новостей: {e}")
            return []
    
    def check_new_news(self):
        """Проверяем новые новости и возвращаем непрочитанные"""
        recent_news = self.get_recent_news()
        new_news = []
        
        for news in recent_news:
            if not self.is_news_tracked(news['news_id']):
                if self.save_news(news):
                    new_news.append(news)
                    logger.info(f"Новая новость: {news['title']}")
        
        return new_news

# Создаем экземпляр монитора
nsd_monitor = NSDMonitor()

# Команды бота
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(
        f"👋 Привет, {user.first_name}!\n\n"
        "Я бот для отслеживания новостей по облигациям на nsddata.ru\n\n"
        "📋 **Команды:**\n"
        "/add RU000A106SE5 - добавить ISIN для отслеживания\n"
        "/list - показать ваши ISIN\n"
        "/check - проверить новости вручную\n"
        "/last - показать последние новости\n\n"
        "🔔 Бот автоматически уведомит о новых событиях по вашим ISIN!"
    )

async def add_isin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Укажите ISIN код: /add RU000A106SE5")
        return
    
    isin_code = context.args[0].upper().strip()
    user_id = update.effective_user.id
    
    # Валидация ISIN
    if not re.match(r'^RU[0-9A-Z]{10}$', isin_code):
        await update.message.reply_text("❌ Неверный формат ISIN. Пример: RU000A106SE5")
        return
    
    if nsd_monitor.add_isin_tracking(isin_code, user_id):
        await update.message.reply_text(f"✅ ISIN `{isin_code}` добавлен для отслеживания", parse_mode='Markdown')
    else:
        await update.message.reply_text(f"⚠️ ISIN `{isin_code}` уже отслеживается", parse_mode='Markdown')

async def list_isins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_isins = nsd_monitor.get_user_isins(user_id)
    
    if not user_isins:
        await update.message.reply_text("📭 У вас нет отслеживаемых ISIN кодов")
        return
    
    message = "📋 **Ваши ISIN коды:**\n\n" + "\n".join(f"• `{isin}`" for isin in user_isins)
    await update.message.reply_text(message, parse_mode='Markdown')

async def manual_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_isins = nsd_monitor.get_user_isins(user_id)
    
    if not user_isins:
        await update.message.reply_text("❌ Сначала добавьте ISIN коды для отслеживания")
        return
    
    await update.message.reply_text("🔍 Проверяю последние новости...")
    
    new_news = nsd_monitor.check_new_news()
    relevant_news = [news for news in new_news if news.get('isin') in user_isins]
    
    if relevant_news:
        for news in relevant_news:
            message = format_news_message(news)
            await update.message.reply_text(message, parse_mode='Markdown')
    else:
        await update.message.reply_text("✅ Новых событий по вашим ISIN не обнаружено")

async def show_last_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает последние новости (для тестирования)"""
    recent_news = nsd_monitor.get_recent_news()[:5]  # Последние 5 новостей
    
    if not recent_news:
        await update.message.reply_text("❌ Не удалось получить новости")
        return
    
    message = "📰 **Последние новости:**\n\n"
    for news in recent_news:
        message += f"• {news['title'][:50]}...\n"
        if news.get('isin'):
            message += f"  ISIN: `{news['isin']}`\n"
        message += "\n"
    
    await update.message.reply_text(message, parse_mode='Markdown')

def format_news_message(news):
    """Форматирует сообщение о новости в красивом виде"""
    message = "👔 **НОВОЕ СОБЫТИЕ**\n\n"
    
    if news.get('isin'):
        message += f"`{news['isin']}`\n\n"
    
    if news.get('published_date'):
        message += f"🗓 *{news['published_date']}*\n"
    
    if news.get('event_type'):
        message += f"📋 *{news['event_type']}*\n\n"
    
    message += f"*{news['title']}*\n"
    
    if news.get('payment_amount'):
        message += f"\n💰 *Размер выплаты:* {news['payment_amount']}"
    
    if news.get('news_url'):
        message += f"\n\n🔗 [Подробнее]({news['news_url']})"
    
    return message

# Фоновая задача для автоматической проверки
async def scheduled_news_check(context: ContextTypes.DEFAULT_TYPE):
    """Автоматическая проверка новостей каждые 10 минут"""
    logger.info("🔍 Автоматическая проверка новостей...")
    
    try:
        new_news = nsd_monitor.check_new_news()
        
        if new_news:
            # Для каждого пользователя фильтруем релевантные новости
            cursor = nsd_monitor.conn.cursor()
            cursor.execute('SELECT DISTINCT user_id FROM user_isins')
            users = cursor.fetchall()
            
            for (user_id,) in users:
                user_isins = nsd_monitor.get_user_isins(user_id)
                relevant_news = [news for news in new_news if news.get('isin') in user_isins]
                
                for news in relevant_news:
                    message = format_news_message(news)
                    try:
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=message,
                            parse_mode='Markdown'
                        )
                        # Задержка между сообщениями чтобы не спамить
                        await asyncio.sleep(1)
                    except Exception as e:
                        logger.error(f"Ошибка отправки пользователю {user_id}: {e}")
    
    except Exception as e:
        logger.error(f"Ошибка в scheduled_news_check: {e}")

def main():
    BOT_TOKEN = os.environ.get('BOT_TOKEN')
    
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN не установлен!")
        return
    
    # Создаем приложение
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add", add_isin))
    application.add_handler(CommandHandler("list", list_isins))
    application.add_handler(CommandHandler("check", manual_check))
    application.add_handler(CommandHandler("last", show_last_news))
    
    # Запускаем периодическую проверку (каждые 10 минут)
    job_queue = application.job_queue
    job_queue.run_repeating(scheduled_news_check, interval=600, first=10)  # 600 сек = 10 мин
    
    # Запускаем бота
    logger.info("🤖 Бот запущен!")
    application.run_polling()

if __name__ == "__main__":
    main()