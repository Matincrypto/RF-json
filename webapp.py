# webapp.py
from flask import Flask, jsonify
import json
import os

# نام فایل JSON که توسط اسکریپت اصلی تولید می‌شود
JSON_FILE = 'signals.json'

app = Flask(__name__)

@app.route('/signals', methods=['GET'])
def get_signals():
    """
    این تابع محتوای فایل signals.json را می‌خواند و نمایش می‌دهد.
    """
    # <<< CHANGE: تغییر منطق این بخش شروع می‌شود
    # بررسی اینکه آیا فایل وجود دارد یا نه
    if not os.path.exists(JSON_FILE):
        # اگر فایل وجود نداشت، به جای خطا، یک لیست خالی برگردان
        return jsonify([])
    # <<< CHANGE: تغییر منطق این بخش تمام می‌شود
    
    try:
        # اگر فایل خالی بود، باز هم یک لیست خالی برگردان
        if os.path.getsize(JSON_FILE) == 0:
            return jsonify([])
            
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # استفاده از jsonify برای ارسال پاسخ صحیح به صورت JSON
        return jsonify(data)
    except Exception as e:
        # اگر در خواندن فایل مشکلی پیش آمد، یک پیام خطا در فرمت JSON برگردان
        error_message = {"error": "Could not process signal file", "details": str(e)}
        return jsonify(error_message), 500

if __name__ == '__main__':
    # برنامه روی پورت 8080 اجرا می‌شود
    app.run(host='0.0.0.0', port=8080, debug=False)