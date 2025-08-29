import requests
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta, UTC
import numpy as np
import warnings
import logging
import configparser
import sys
import json # <<< CHANGE: ماژول json برای کار با فایل جیسون اضافه شد
import os   # <<< CHANGE: ماژول os برای بررسی وجود فایل اضافه شد

# --- 1. تنظیمات اولیه لاگ و کانفیگ ---

warnings.simplefilter(action='ignore', category=FutureWarning)

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler('bot_activity.log', mode='a', encoding='utf-8')
file_handler.setFormatter(log_formatter)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(log_formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')

# --- 2. بارگذاری متغیرها از فایل کانفیگ ---
try:
    # <<< CHANGE: بخش تلگرام حذف شد چون دیگر استفاده نمی‌شود
    SYMBOLS_TO_ANALYZE = [symbol.strip() for symbol in config.get('analysis', 'symbols').split(',')]
    RESOLUTION_TO_ANALYZE = config.getint('analysis', 'resolution')
    CANDLE_COUNT = config.getint('analysis', 'candle_count')
    DB_FILE = config.get('database', 'db_file')
    JSON_OUTPUT_FILE = 'signals.json' # <<< CHANGE: نام فایل خروجی جیسون
except (configparser.NoSectionError, configparser.NoOptionError) as e:
    logger.error(f"Error reading config.ini file: {e}")
    exit()

# --- تنظیمات ثابت اندیکاتور Range Filter ---
RF_SETTINGS = {
    'f_type': "Type 1", 'mov_src': "Close", 'rng_qty': 2.618,
    'rng_scale': "Average Change", 'rng_per': 14, 'smooth_range': True,
    'smooth_per': 27, 'av_vals': True, 'av_samples': 2
}

# --- 3. توابع اصلی برنامه ---

# <<< CHANGE: تابع send_telegram_signal به طور کامل حذف شد

# <<< CHANGE: تابع جدید برای ذخیره سیگنال در فایل JSON
def save_signal_to_json(signal_data):
    """
    این تابع یک سیگنال جدید را به فایل JSON اضافه می‌کند.
    اگر فایل وجود نداشته باشد، آن را ایجاد می‌کند.
    """
    signals = []
    # اگر فایل وجود داشت و خالی نبود، محتوای آن را بخوان
    if os.path.exists(JSON_OUTPUT_FILE) and os.path.getsize(JSON_OUTPUT_FILE) > 0:
        try:
            with open(JSON_OUTPUT_FILE, 'r', encoding='utf-8') as f:
                signals = json.load(f)
            # اطمینان حاصل کنید که محتوای خوانده شده یک لیست است
            if not isinstance(signals, list):
                logger.warning(f"{JSON_OUTPUT_FILE} does not contain a list. Initializing a new list.")
                signals = []
        except json.JSONDecodeError:
            logger.error(f"Could not decode JSON from {JSON_OUTPUT_FILE}. Starting with an empty list.")
            signals = []
            
    # سیگنال جدید را به لیست اضافه کن
    signals.append(signal_data)
    
    # لیست آپدیت شده را در فایل بنویس
    try:
        with open(JSON_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(signals, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ New signal successfully saved to {JSON_OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"❌ Error saving signal to {JSON_OUTPUT_FILE}: {e}")


def convert_resolution_to_period(resolution_minutes):
    if resolution_minutes < 60: return f"{resolution_minutes}min"
    elif resolution_minutes >= 60: return f"{resolution_minutes // 60}hour"
    return None

def fetch_coinex_data(market, resolution, limit):
    url = "https://api.coinex.com/v2/spot/kline"
    period = convert_resolution_to_period(resolution)
    if not period: return None
    params = {'market': market, 'period': period, 'limit': limit}
    logger.info(f"Fetching data for {market} | Timeframe: {period}...")
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get('code') != 0 or not data.get('data'): return None
        df = pd.DataFrame(data['data'])
        df = df[['created_at', 'open', 'high', 'low', 'close', 'volume']]
        df.rename(columns={'created_at': 'timestamp'}, inplace=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        for col in ['open', 'high', 'low', 'close', 'volume']: df[col] = pd.to_numeric(df[col])
        df.set_index('timestamp', inplace=True)
        return df.sort_index()
    except Exception as e:
        logger.error(f"--> Error fetching data for {market}: {e}")
        return None

def calculate_range_filter(df, settings):
    # این تابع بدون تغییر باقی می‌ماند
    h_val = df['high'] if settings['mov_src'] == 'Wicks' else df['close']
    l_val = df['low'] if settings['mov_src'] == 'Wicks' else df['close']
    avg_price = (h_val + l_val) / 2
    ac = abs(avg_price - avg_price.shift(1)).ewm(span=settings['rng_per'], adjust=False).mean()
    rng = settings['rng_qty'] * ac
    if settings['smooth_range']: rng = rng.ewm(span=settings['rng_per'], adjust=False).mean()
    filt_raw = pd.Series(np.nan, index=df.index)
    filt_raw.iloc[0] = avg_price.iloc[0]
    for i in range(1, len(df)):
        prev_filt = filt_raw.iloc[i-1]
        r = rng.iloc[i]
        if h_val.iloc[i] - r > prev_filt: filt_raw.iloc[i] = h_val.iloc[i] - r
        elif l_val.iloc[i] + r < prev_filt: filt_raw.iloc[i] = l_val.iloc[i] + r
        else: filt_raw.iloc[i] = prev_filt
    if settings['av_vals']:
        condition = (filt_raw != filt_raw.shift(1))
        alpha = 2 / (settings['av_samples'] + 1)
        ema_values = pd.Series(np.nan, index=filt_raw.index)
        last_ema = np.nan
        for i in range(len(filt_raw)):
            if condition.iloc[i]:
                if np.isnan(last_ema): last_ema = filt_raw.iloc[i]
                else: last_ema = (filt_raw.iloc[i] - last_ema) * alpha + last_ema
            ema_values.iloc[i] = last_ema
        filt = ema_values.ffill()
    else: filt = filt_raw
    df['filter'] = filt
    df['fdir'] = 0
    df.loc[df['filter'] > df['filter'].shift(1), 'fdir'] = 1
    df.loc[df['filter'] < df['filter'].shift(1), 'fdir'] = -1
    df['fdir'] = df['fdir'].replace(0, method='ffill').fillna(0).astype(int)
    upward = (df['fdir'] == 1)
    downward = (df['fdir'] == -1)
    long_cond = (df['close'] > df['filter']) & upward
    short_cond = (df['close'] < df['filter']) & downward
    cond_ini = pd.Series(0, index=df.index)
    for i in range(1, len(df)):
        if long_cond.iloc[i]: cond_ini.iloc[i] = 1
        elif short_cond.iloc[i]: cond_ini.iloc[i] = -1
        else: cond_ini.iloc[i] = cond_ini.iloc[i-1]
    long_condition = long_cond & (cond_ini.shift(1) == -1)
    short_condition = short_cond & (cond_ini.shift(1) == 1)
    df['signal'] = "NO_SIGNAL"
    df.loc[long_condition, 'signal'] = "BUY"
    df.loc[short_condition, 'signal'] = "SELL"
    return df

def store_in_db(df, table_name):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=True)
        logger.info(f"--> Results saved to table '{table_name}'.")
    except Exception as e:
        logger.error(f"--> Error saving to database: {e}")

# --- 4. حلقه اصلی برنامه ---
if __name__ == "__main__":
    last_sent_signal_type = {}
    
    logger.info("🚀 Starting the analysis bot...")
    
    while True:
        try:
            # از همان منطق زمان‌بندی بهبودیافته استفاده می‌کنیم
            now_utc = datetime.now(UTC)
            resolution = RESOLUTION_TO_ANALYZE
            minutes_to_next_candle = resolution - (now_utc.minute % resolution)
            next_run_time = (now_utc + timedelta(minutes=minutes_to_next_candle)).replace(second=1, microsecond=0)
            sleep_duration = (next_run_time - now_utc).total_seconds()

            if sleep_duration > 0:
                logger.info(f"Analysis cycle finished. Sleeping for {sleep_duration/60:.2f} minutes until {next_run_time.strftime('%H:%M:%S')} UTC...")
                time.sleep(sleep_duration)

            logger.info(f"===== Starting new analysis cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
            
            for symbol in SYMBOLS_TO_ANALYZE:
                ohlc_df = fetch_coinex_data(symbol, RESOLUTION_TO_ANALYZE, CANDLE_COUNT)
                
                if ohlc_df is not None and len(ohlc_df) > 50:
                    analysis_df = calculate_range_filter(ohlc_df, RF_SETTINGS)
                    table_name = f"{symbol}_{RESOLUTION_TO_ANALYZE}m_analysis"
                    store_in_db(analysis_df, table_name)
                    closed_candle = analysis_df.iloc[-2]
                    new_signal = closed_candle['signal']
                    
                    logger.info(f"Analysis for {symbol}: The closed candle at {closed_candle.name.strftime('%H:%M')} shows signal: {new_signal}")

                    if new_signal in ["BUY", "SELL"]:
                        key = f"{symbol}_{RESOLUTION_TO_ANALYZE}m"
                        last_signal = last_sent_signal_type.get(key)
                        
                        if new_signal != last_signal:
                            # <<< CHANGE: به جای ساختن پیام تلگرام، یک دیکشنری برای JSON می‌سازیم
                            signal_payload = {
                                "symbol": symbol,
                                "timeframe_minutes": RESOLUTION_TO_ANALYZE,
                                "signal_type": new_signal,
                                "price": closed_candle['close'],
                                "signal_time_utc": closed_candle.name.strftime('%Y-%m-%d %H:%M:%S')
                            }
                            # <<< CHANGE: تابع جدید را برای ذخیره سیگنال فراخوانی می‌کنیم
                            save_signal_to_json(signal_payload)
                            last_sent_signal_type[key] = new_signal
                        else:
                            logger.info(f"A repeat signal ({new_signal}) was found for {symbol}. No new entry will be saved.")

        except KeyboardInterrupt:
            logger.info("Bot stopped manually by the user.")
            break
        except Exception as e:
            logger.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
            # <<< CHANGE: پیام خطای تلگرام حذف شد
            time.sleep(300)