# webapp.py (Final Version)
from flask import Flask, jsonify
import json
import os

JSON_FILE = 'signals.json'
app = Flask(__name__)

@app.route('/signals', methods=['GET'])
def get_signals():
    """
    Reads and displays the content of signals.json, which contains only the latest signal.
    """
    if not os.path.exists(JSON_FILE) or os.path.getsize(JSON_FILE) == 0:
        # Returns an empty object if no signal is available
        return jsonify({})

    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            latest_signal = json.load(f)
        return jsonify(latest_signal)
        
    except Exception as e:
        error_message = {"error": "Could not process signal file", "details": str(e)}
        return jsonify(error_message), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)