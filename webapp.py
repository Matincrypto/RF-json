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
    این تابع محتوای فایل signals.json را می‌خواند
    و فقط آخرین سیگنال برای هر نماد (symbol) را نمایش می‌دهد.
    """
    if not os.path.exists(JSON_FILE) or os.path.getsize(JSON_FILE) == 0:
        return jsonify([])

    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            all_signals = json.load(f)

        # <<< CHANGE: منطق جدید برای پیدا کردن آخرین سیگنال هر ارز شروع می‌شود
        latest_signals = {}
        for signal in all_signals:
            # هر سیگنال جدید برای یک ارز، جایگزین سیگنال قبلی آن در دیکشنری می‌شود
            symbol = signal.get("symbol")
            if symbol:
                latest_signals[symbol] = signal
        
        # دیکشنری را به یک لیست تبدیل می‌کنیم تا خروجی JSON یک لیست باشد
        final_list = list(latest_signals.values())
        # <<< CHANGE: منطق جدید تمام می‌شود

        return jsonify(final_list)
        
    except Exception as e:
        error_message = {"error": "Could not process signal file", "details": str(e)}
        return jsonify(error_message), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)