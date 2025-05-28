import os
import time
import random
import pickle
import requests
import pandas as pd
from datetime import datetime
from flask import Flask, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# ������������
BATCH_SIZE = 20
SPREADSHEET_URL = os.getenv('SPREADSHEET_URL')  # ������ �� ���������� ���������
SHEET_NAME = os.getenv('SHEET_NAME', "rss_feed_wf4_6")
WORKSHEET_NAME = os.getenv('WORKSHEET_NAME', "relevant")

def auth_google():
    """�������������� � Google Sheets"""
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        'service_account.json',
        ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    )
    return gspread.authorize(creds)

@app.route('/run', methods=['POST'])  # POST ��� n8n
def run_parser():
    try:
        gc = auth_google()
        sheet = gc.open_by_url(SPREADSHEET_URL).worksheet(WORKSHEET_NAME)
        records = sheet.get_all_records()
        
        # ���� ������ ��������� ������
        for i, row in enumerate(records[:BATCH_SIZE]):  # ������������ ������ BATCH_SIZE �����
            if not row.get('relevants'):
                content = parse_article_content(row['link'])
                if content:
                    sheet.update_cell(i+2, 3, content)  # ������� C
                    sheet.update_cell(i+2, 6, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))  # ������� F
                    time.sleep(random.uniform(5, 12))  # �������� ����� ���������
        
        return jsonify({"status": "success", "processed": BATCH_SIZE})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))  # Cloud Run ���������� ���� 8080