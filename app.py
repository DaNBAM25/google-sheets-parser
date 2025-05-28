from fastapi import FastAPI
import uvicorn

app = FastAPI()

@app.get("/run")
def run_script():
    # @title Текст заголовка по умолчанию
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from datetime import datetime
from google.colab import auth
from google.auth import default
import gspread
from google.colab import drive
import pickle
import os
import numpy as np

# Настройки
BATCH_SIZE = 20  # Количество строк для обработки за один раз
MIN_DELAY = 5    # Минимальная задержка между запросами (секунды)
MAX_DELAY = 12   # Максимальная задержка между запросами (секунды)
RESUME_FILE = '/content/progress.pkl'  # Файл для сохранения прогресса

# Аутентификация в Google
auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)
drive.mount('/content/drive')

def is_relevants_empty(value):
    """Проверяет, считается ли поле relevants пустым"""
    if pd.isna(value) or value is None:
        return True
    if isinstance(value, str) and value.strip() == '':
        return True
    return False

# Функция парсинга статьи
def parse_article_content(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        article_text = ''
        text_blocks = soup.find_all('p', class_=False)
        for block in text_blocks:
            article_text += block.get_text(strip=True) + '\n\n'
        
        return article_text.strip()
    
    except Exception as e:
        print(f"Ошибка при парсинге {url}: {str(e)}")
        return None

# Загрузка прогресса
def load_progress():
    if os.path.exists(RESUME_FILE):
        with open(RESUME_FILE, 'rb') as f:
            return pickle.load(f)
    return {'last_processed': -1, 'total_processed': 0}

# Сохранение прогресса
def save_progress(last_processed, total_processed):
    with open(RESUME_FILE, 'wb') as f:
        pickle.dump({'last_processed': last_processed, 'total_processed': total_processed}, f)

# Основная функция
def main():
    # Открываем таблицу
    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1jWVGxmQRgDp3ionhDY09aC2nhOs4DCzeS07umo5Zcig/edit?usp=sharing"
    sheet_name = "rss_feed_wf4_6"
    worksheet_name = "relevant"

    try:
        spreadsheet = gc.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet(worksheet_name)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        
        # Загружаем прогресс
        progress = load_progress()
        last_processed = progress['last_processed']
        total_processed = progress['total_processed']
        
        # Фильтруем только строки с пустым relevants
        df_to_process = df[df['relevants'].apply(is_relevants_empty)]
        print(f"Всего строк в таблице: {len(df)}, из них для обработки: {len(df_to_process)}")
        
        # Получаем индексы отфильтрованных строк в исходном DataFrame
        original_indices = df_to_process.index.tolist()
        
        print(f"Продолжаем с позиции {last_processed + 1}, ранее обработано: {total_processed}")
        
        # Обрабатываем порциями
        start_idx = max(0, last_processed + 1)
        for batch_start in range(start_idx, len(df_to_process), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(df_to_process))
            print(f"\nОбработка строк {batch_start + 2}-{batch_end + 1}...")
            
            batch_updates = []
            for i in range(batch_start, batch_end):
                # Получаем индекс в исходном DataFrame
                original_i = original_indices[i]
                row = df_to_process.iloc[i]
                
                if pd.isna(row['link']) or not row['link'].startswith('http'):
                    continue
                
                print(f"Строка {original_i + 2}: {row['title'][:50]}...")
                content = parse_article_content(row['link'])
                
                if content:
                    # Формируем обновление для этой строки
                    update = {
                        'content': content,
                        'date_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    # Сохраняем индекс исходного DataFrame
                    batch_updates.append((original_i, update))
                    total_processed += 1
                    
                    # Случайная задержка
                    delay = random.uniform(MIN_DELAY, MAX_DELAY)
                    time.sleep(delay)
            
            # Применяем все обновления батча
            if batch_updates:
                for original_i, update in batch_updates:
                    # Обновляем DataFrame
                    for key, value in update.items():
                        df.at[original_i, key] = value
                
                # Обновляем Google Sheet
                print("Обновление таблицы...")
                # Обновляем только обработанные строки (оптимизация)
                for original_i, update in batch_updates:
                    row_num = original_i + 2  # +1 для заголовка, +1 для 0-based индекса
                    # Обновляем только content и date_update
                    worksheet.update(f'C{row_num}', [[update['content']]])
                    worksheet.update(f'F{row_num}', [[update['date_update']]])
                
                # Сохраняем прогресс
                last_processed = batch_end - 1
                save_progress(last_processed, total_processed)
                print(f"Прогресс сохранен. Обработано: {total_processed}")
            
            # Пауза между батчами
            time.sleep(2)
        
        print("\nГотово! Все строки с пустым relevants обработаны.")
        if os.path.exists(RESUME_FILE):
            os.remove(RESUME_FILE)
    
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        print("Прогресс сохранен. Можно продолжить позже.")

if __name__ == "__main__":
    main()
# В конце ноутбука:
import time
time.sleep(30)  # 5 минут
exit()  # Закрывает сессию
    result = {"status": "success", "message": "Hello from Binder!"}
    return result

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
