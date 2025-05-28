import os
import time
import random
import requests
from datetime import datetime
from flask import Flask, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup

app = Flask(__name__)

# Конфигурация (значения по умолчанию для вашего проекта)
SPREADSHEET_URL = os.getenv('SPREADSHEET_URL', "https://docs.google.com/spreadsheets/d/1jWVGxmQRgDp3ionhDY09aC2nhOs4DCzeS07umo5Zcig/edit")
SHEET_NAME = os.getenv('SHEET_NAME', "rss_feed_wf4_6")
WORKSHEET_NAME = os.getenv('WORKSHEET_NAME', "relevant")
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 20))

def auth_google():
    """Аутентификация с вашим сервисным аккаунтом"""
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        'service_account.json',
        ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    )
    return gspread.authorize(creds)

@app.route('/run', methods=['POST'])
def parse_sheet():
    try:
        gc = auth_google()
        sheet = gc.open_by_url(SPREADSHEET_URL).worksheet(WORKSHEET_NAME)
        records = sheet.get_all_records()

        processed = 0
        for i, row in enumerate(records[:BATCH_SIZE]):
            if not row.get('relevants'):
                url = row.get('link', '')
                if url.startswith('http'):
                    content = parse_article(url)
                    if content:
                        sheet.update_cell(i+2, 3, content)  # Колонка C = content
                        sheet.update_cell(i+2, 6, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))  # Колонка F = date_update
                        processed += 1
                        time.sleep(random.uniform(5, 12))

        return jsonify({
            "status": "success",
            "processed": processed,
            "project": "savvy-kit-457018-b4"
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "project": "savvy-kit-457018-b4"
        }), 500

def parse_article(url):
    """Парсинг контента статьи"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=30)
        soup = BeautifulSoup(response.text, 'html.parser')
        return '\n\n'.join(p.get_text(strip=True) for p in soup.find_all('p'))
    except Exception as e:
        print(f"Ошибка парсинга {url}: {e}")
        return None

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
