"""
Keep-alive скрипт для предотвращения засыпания бесплатного Render сервера.
Запускается на внешнем сервисе (cron-job.org, UptimeRobot, etc.)

Или используй бесплатные сервисы мониторинга:
1. https://uptimerobot.com - пингует каждые 5 минут бесплатно
2. https://cron-job.org - выполняет HTTP запросы по расписанию
3. https://betterstack.com/uptime - бесплатный мониторинг

Настрой пинг на твой URL: https://rasswet-gifts.onrender.com/health
"""
import requests
import time
import os

WEBSITE_URL = os.getenv('WEBSITE_URL', 'https://rasswet-gifts.onrender.com')

def ping():
    """Пингует сервер для предотвращения сна."""
    try:
        response = requests.get(f"{WEBSITE_URL}/health", timeout=30)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Ping: {response.status_code}")
        return response.status_code == 200
    except Exception as e:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Error: {e}")
        return False

if __name__ == '__main__':
    # Для локального тестирования
    while True:
        ping()
        time.sleep(300)  # каждые 5 минут
