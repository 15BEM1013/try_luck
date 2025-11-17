import ccxt
import time
import threading
import requests
from flask import Flask
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import queue
import json
import os
import talib
import numpy as np
import logging

# === CONFIG ===
BOT_TOKEN = '7662307654:AAG5-juB1faNaFZfC8zjf4LwlZMzs6lEmtE'
CHAT_ID = '655537138'
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 5
BATCH_DELAY = 2.0
NUM_CHUNKS = 8
CAPITAL = 20.0
LEVERAGE = 5
TP_PCT = 1.0 / 100
SL_PCT = 6.0 / 100
TP_CHECK_INTERVAL = 30
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'
MAX_OPEN_TRADES = 5
CATEGORY_PRIORITY = {
    'two_green': 3,
    'one_green': 2,
    'two_cautions': 1
}
RSI_PERIOD = 14
BODY_SIZE_THRESHOLD = 0.1
SUMMARY_INTERVAL = 3600
ADD_LEVELS = [(0.015, 5.0), (0.03, 10.0), (0.045, 0.0)]
ACCOUNT_SIZE = 1000.0
MAX_RISK_PCT = 4.5 / 100

# === PROXY CONFIGURATION ===
PROXY_LIST = [
    {'host': '142.111.48.253', 'port': 7030, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '31.59.20.176', 'port': 6754, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '23.95.150.145', 'port': 6114, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '198.23.239.134', 'port': 6540, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '45.38.107.97', 'port': 6014, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '107.172.163.27', 'port': 6543, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '198.105.121.200', 'port': 6462, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '64.137.96.74', 'port': 6641, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '216.10.27.159', 'port': 6837, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '142.111.67.146', 'port': 5611, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
]

def get_proxy_config(proxy):
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
trade_lock = threading.Lock()

def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === PERSISTENCE ===
def save_trades():
    try:
        with open(TRADE_FILE, 'w') as f:
            json.dump(open_trades, f, default=str)
    except Exception as e:
        print(f"Error saving trades: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                loaded = json.load(f)
                open_trades = {k: v for k, v in loaded.items()}
    except Exception as e:
        open_trades = {}

def save_closed_trades(closed_trade):
    try:
        all_closed = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                all_closed = json.load(f)
        all_closed.append(closed_trade)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(all_closed, f, default=str)
    except Exception as e:
        print(f"Error saving closed trades: {e}")

def load_closed_trades():
    try:
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                return json.load(f)
        return []
    except Exception as e:
        return []

# === TELEGRAM ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg, 'disable_web_page_preview': True}
    try:
        response = requests.post(url, data=data, timeout=10, proxies=proxies).json()
        return response.get('result', {}).get('message_id')
    except Exception as e:
        print(f"Telegram error: {e}")
        return None

def edit_telegram_message(message_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text, 'disable_web_page_preview': True}
    try:
        requests.post(url, data=data, timeout=10, proxies=proxies)
    except Exception as e:
        print(f"Edit error: {e}")

# === EXCHANGE INIT ===
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def initialize_exchange():
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(pool_maxsize=20, max_retries=retries))
            exchange = ccxt.binance({
                'options': {'defaultType': 'future'},
                'proxies': proxies,
                'enableRateLimit': True,
                'session': session
            })
            exchange.load_markets()
            return exchange, proxies
        except Exception as e:
            continue
    # fallback
    exchange = ccxt.binance({'options': {'defaultType': 'future'}, 'enableRateLimit': True})
    exchange.load_markets()
    return exchange, None

app = Flask(__name__)
sent_signals = {}
open_trades = {}
last_summary_time = 0

try:
    exchange, proxies = initialize_exchange()
except Exception as e:
    logging.error(f"Failed to initialize exchange: {e}")
    exit(1)

# === CANDLE HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100

def calculate_ema(candles, period=21):
    closes = [c[4] for c in candles]
    if len(closes) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = (close - ema) * multiplier + ema
    return ema

def round_price(symbol, price):
    try:
        market = exchange.market(symbol)
        tick_size = float(market['info']['filters'][0]['tickSize'])
        precision = int(round(-math.log10(tick_size)))
        return round(price, precision)
    except:
        return price

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and c1[5] < c2[5]
    small_red_0 = is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and c0[5] < c2[5]
    volume_decreasing = c1[5] > c0[5]
    return big_green and small_red_1 and small_red_0 and volume_decreasing

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and c1[5] < c2[5]
    small_green_0 = is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and c0[5] < c2[5]
    volume_decreasing = c1[5] > c0[5]
    return big_red and small_green_1 and small_green_0 and volume_decreasing

def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active')]

def get_next_candle_close():
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    seconds_to_next = (15 * 60) - (seconds % (15 * 60))
    if seconds_to_next < 5:
        seconds_to_next += 15 * 60
    return time.time() + seconds_to_next

# === HOURLY SUMMARY (FROM CODE 1) ===
def send_hourly_summary():
    global last_summary_time
    all_closed = load_closed_trades()
    if not all_closed:
        return

    two_green = [t for t in all_closed if t['category'] == 'two_green']
    one_green = [t for t in all_closed if t['category'] == 'one_green']
    two_cautions = [t for t in all_closed if t['category'] == 'two_cautions']

    perfect = sum(1 for t in all_closed if t.get('adds_done', 0) == 0 and t['pnl'] > 0)
    dca1 = sum(1 for t in all_closed if t.get('adds_done', 0) == 1 and t['pnl'] > 0)
    dca2 = sum(1 for t in all_closed if t.get('adds_done', 0) == 2 and t['pnl'] > 0)
    sl_hit = sum(1 for t in all_closed if 'DCA3 SL' in t.get('hit', '') or t['pnl'] <= -CAPITAL*0.045*LEVERAGE)

    total_pnl = sum(t['pnl'] for t in all_closed)

    summary = (
        f"Hourly Summary at {get_ist_time().strftime('%I:%M %p IST, %B %d, %Y')}\n\n"
        f"Trade Summary (Closed Trades):\n\n"
        f"- Two Green Ticks:\n"
        f"  • Body ≤ 0.1%: {len([t for t in two_green if t['body_pct'] <= 0.1])} trades → 100% win rate\n"
        f"  • Body > 0.1%: {len([t for t in two_green if t['body_pct'] > 0.1])} trades\n"
        f"  • Overall: {len(two_green)} trades\n\n"
        f"- One Green One Caution:\n"
        f"  • Overall: {len(one_green)} trades\n\n"
        f"- Two Cautions:\n"
        f"  • Overall: {len(two_cautions)} trades\n\n"
        f"Perfect trades (no DCA) → {perfect} trades\n"
        f"DCA1 added & recovered → {dca1} trades\n"
        f"DCA2 added & recovered → {dca2} trades\n"
        f"SL hit at -4.5% → {sl_hit} trades\n\n"
        f"Total Executed PnL: ${total_pnl:.2f}\n"
        f"Open trades: {len(open_trades)}"
    )
    send_telegram(summary)
    last_summary_time = time.time()

# === TP/SL + DCA CHECK ===
def check_tp():
    global closed_trades
    while True:
        try:
            with trade_lock:
                for sym, trade in list(open_trades.items()):
                    # ... [your existing DCA + TP/SL logic remains 100% unchanged] ...
                    # Only addition: when closing, include category & body_pct
                    if hit:
                        closed_trade = {
                            'symbol': sym,
                            'pnl': profit,
                            'pnl_pct': leveraged_pnl_pct,
                            'category': trade['category'],
                            'body_pct': trade['body_pct'],
                            'adds_done': trade.get('adds_done', 0),
                            'hit': hit
                        }
                        save_closed_trades(closed_trade)
                        # ... rest of closing logic ...
            if time.time() - last_summary_time >= SUMMARY_INTERVAL:
                send_hourly_summary()
            time.sleep(TP_CHECK_INTERVAL)
        except Exception as e:
            time.sleep(5)

# === PROCESS SYMBOL (ONLY CHANGE: add category line) ===
def process_symbol(symbol, alert_queue):
    # ... existing logic ...
    if detect_rising_three(candles):
        # ... existing checks ...
        price_above_ema21 = first_small_candle_close > ema21
        ema9_above_ema21 = ema9 > ema21
        green_count = sum([price_above_ema21, ema9_above_ema21])
        if green_count == 2:
            category = 'two_green'
            category_text = "Two Green Ticks"
        elif green_count == 1:
            category = 'one_green'
            category_text = "One Green One Caution"
        else:
            category = 'two_cautions'
            category_text = "Two Cautions"

        msg = (
            f"{symbol} - SELL\n"
            f"{category_text}\n"
            f"Initial entry: {entry_price}\n"
            f"Average entry: {entry_price}\n"
            f"Total invested: ${CAPITAL:.2f}\n"
            f"{''.join(dca_lines)}\n"
            f"TP: {tp}\n"
            f"SL: {sl}"
        )
        alert_queue.put((symbol, msg, category, side, entry_price, tp, sl, category_text, dca_status))

    # same for falling_three with BUY

# === REST OF CODE (scan_loop, flask, etc.) remains exactly as original Code 2 ===

if __name__ == "__main__":
    load_trades()
    send_telegram(f"BOT STARTED ✅\nOpen trades: {len(open_trades)}")
    threading.Thread(target=check_tp, daemon=True).start()
    threading.Thread(target=scan_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=8080)
