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
SL_PCT = 4.5 / 100
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
RSI_OVERBOUGHT = 80
RSI_OVERSOLD = 30
BODY_SIZE_THRESHOLD = 0.1
SUMMARY_INTERVAL = 3600
ACCOUNT_SIZE = 1000.0
MAX_RISK_PCT = 4.5 / 100

# === PROXY CONFIGURATION ===
PROXY_LIST = [
    {'host': '142.111.48.253', 'port': 7030, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '31.59.20.176', 'port': 6754, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '23.95.150.145', 'port': 6114, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '198.23.239.134', 'port': 6540, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '107.172.163.27', 'port': 6543, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '198.105.121.200','port': 6462, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '64.137.96.74', 'port': 6641, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '84.247.60.125', 'port': 6095, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '216.10.27.159', 'port': 6837, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '142.111.67.146', 'port': 5611, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
]

def get_proxy_config(proxy):
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
trade_lock = threading.Lock()

# === TIME ZONE ===
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === PERSISTENCE ===
def save_trades():
    with trade_lock:
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
                open_trades = json.load(f)
    except Exception as e:
        print(f"Error loading trades: {e}")
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

# === TELEGRAM ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg}
    try:
        response = requests.post(url, data=data, timeout=5, proxies=proxies).json()
        return response.get('result', {}).get('message_id')
    except Exception as e:
        print(f"Telegram error: {e}")
        return None

def edit_telegram_message(message_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text}
    try:
        requests.post(url, data=data, timeout=5, proxies=proxies)
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
            logging.info(f"Connected via proxy: {proxy['host']}:{proxy['port']}")
            return exchange, proxies
        except Exception as e:
            logging.error(f"Proxy failed {proxy['host']}:{proxy['port']}: {e}")
    # Fallback
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

# === HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100

def analyze_first_small_candle(candle, pattern_type):
    body = body_pct(candle)
    upper_wick = (candle[2] - max(candle[1], candle[4])) / candle[1] * 100
    lower_wick = (min(candle[1], candle[4]) - candle[3]) / candle[1] * 100
    wick_ratio = upper_wick / lower_wick if lower_wick != 0 else float('inf')
    wick_ratio_reverse = lower_wick / upper_wick if upper_wick != 0 else float('inf')

    if pattern_type == 'rising':
        if wick_ratio >= 2.5 and body < 0.1:
            return {'text': f"Selling pressure\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
        elif wick_ratio_reverse >= 2.5 and body < 0.1:
            return {'text': f"Buying pressure\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
        elif body >= 0.1:
            if wick_ratio_reverse >= 2.5:
                return {'text': f"Buying pressure\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
            elif wick_ratio >= 2.5:
                return {'text': f"Selling pressure\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
            else:
                return {'text': f"Neutral\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}
        else:
            return {'text': f"Neutral\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}
    else:  # falling
        # same logic as above but reversed
        if wick_ratio_reverse >= 2.5 and body < 0.1:
            return {'text': f"Buying pressure\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
        elif wick_ratio >= 2.5 and body < 0.1:
            return {'text': f"Selling pressure\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
        elif body >= 0.1:
            if wick_ratio_reverse >= 2.5:
                return {'text': f"Buying pressure\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
            elif wick_ratio >= 2.5:
                return {'text': f"Selling pressure\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
            else:
                return {'text': f"Neutral\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}
        else:
            return {'text': f"Neutral\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}

def calculate_ema(candles, period=21):
    closes = [c[4] for c in candles]
    if len(closes) < period: return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = (close - ema) * multiplier + ema
    return ema

def calculate_rsi(candles, period=14):
    closes = np.array([c[4] for c in candles])
    if len(closes) < period: return None
    return talib.RSI(closes, timeperiod=period)[-1]

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
    small_red_1 = is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    small_red_0 = is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and c0[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    return big_green and small_red_1 and small_red_0

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    small_green_0 = is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and c0[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    return big_red and small_green_1 and small_green_0

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

# === TP/SL CHECK (NO DCA) ===
def check_tp():
    while True:
        try:
            with trade_lock:
                for sym, trade in list(open_trades.items()):
                    try:
                        ticker = exchange.fetch_ticker(sym)
                        price = round_price(sym, ticker['last'])

                        hit = None
                        hit_price = price

                        if trade['side'] == 'buy':
                            if price >= trade['tp']:
                                hit = "TP hit"
                            elif price <= trade['sl']:
                                hit = "SL hit"
                        else:
                            if price <= trade['tp']:
                                hit = "TP hit"
                            elif price >= trade['sl']:
                                hit = "SL hit"

                        if hit:
                            pnl_pct = (price - trade['entry']) / trade['entry'] * 100 if trade['side'] == 'buy' else (trade['entry'] - price) / trade['entry'] * 100
                            leveraged_pnl = pnl_pct * LEVERAGE
                            profit = CAPITAL * leveraged_pnl / 100

                            # Keep confidence level in final message
                            signal_strength = {
                                'two_green': "HIGH CONFIDENCE (2 Green)",
                                'one_green': "MID CONFIDENCE (1 Green + 1 Caution)",
                                'two_cautions': "LOW CONFIDENCE (2 Cautions)"
                            }[trade['category']]

                            final_msg = (
                                f"{sym} - {'BUY' if trade['side']=='buy' else 'SELL'} {signal_strength}\n"
                                f"• Price vs EMA21: {trade['ema_status']['price_ema21']}\n"
                                f"• EMA9 vs EMA21: {trade['ema_status']['ema9_ema21']}\n"
                                f"Entry: {trade['entry']}\n"
                                f"TP: {trade['tp']}\n"
                                f"SL: {trade['sl']}\n"
                                f"Exit: {hit_price}\n"
                                f"Profit: {leveraged_pnl:+.2f}% (${profit:+.2f})\n"
                                f"Result: {hit}"
                            )

                            edit_telegram_message(trade['msg_id'], final_msg)

                            closed_trade = {
                                'symbol': sym,
                                'pnl': profit,
                                'pnl_pct': leveraged_pnl,
                                'category': trade['category'],
                                'hit': hit,
                                'side': trade['side']
                            }
                            save_closed_trades(closed_trade)
                            del open_trades[sym]
                            save_trades()
                    except Exception as e:
                        logging.error(f"Error checking {sym}: {e}")
            time.sleep(TP_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"TP check loop error: {e}")
            time.sleep(10)

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=30)
        if len(candles) < 25: return

        ema21 = calculate_ema(candles, 21)
        ema9 = calculate_ema(candles, 9)
        if ema21 is None or ema9 is None: return

        signal_time = candles[-2][0]
        entry_price = round_price(symbol, candles[-2][4])

        if detect_rising_three(candles):
            analysis = analyze_first_small_candle(candles[-3], 'rising')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'rising')) == signal_time: return
            sent_signals[(symbol, 'rising')] = signal_time

            price_above_ema21 = candles[-3][4] > ema21
            ema9_above_ema21 = ema9 > ema21
            ema_status = {'price_ema21': 'Green' if price_above_ema21 else 'Caution', 'ema9_ema21': 'Green' if ema9_above_ema21 else 'Caution'}
            green_count = sum(1 for v in ema_status.values() if v == 'Green')
            category = 'two_green' if green_count == 2 else 'one_green' if green_count == 1 else 'two_cautions'

            tp = round_price(symbol, entry_price * (1 - TP_PCT))
            sl = round_price(symbol, entry_price * (1 + SL_PCT))

            signal_strength = {
                'two_green': "HIGH CONFIDENCE (2 Green)",
                'one_green': "MID CONFIDENCE (1 Green + 1 Caution)",
                'two_cautions': "LOW CONFIDENCE (2 Cautions)"
            }[category]

            msg = (
                f"{symbol} - SELL {signal_strength}\n"
                f"• Price vs EMA21: {ema_status['price_ema21']}\n"
                f"• EMA9 vs EMA21: {ema_status['ema9_ema21']}\n"
                f"Entry: {entry_price}\n"
                f"TP: {tp}\n"
                f"SL: {sl}\n"
                f"Capital: ${CAPITAL}"
            )

            alert_queue.put((symbol, msg, 'sell', entry_price, tp, sl, ema_status, category))

        elif detect_falling_three(candles):
            analysis = analyze_first_small_candle(candles[-3], 'falling')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'falling')) == signal_time: return
            sent_signals[(symbol, 'falling')] = signal_time

            price_below_ema21 = candles[-3][4] < ema21
            ema9_below_ema21 = ema9 < ema21
            ema_status = {'price_ema21': 'Green' if price_below_ema21 else 'Caution', 'ema9_ema21': 'Green' if ema9_below_ema21 else 'Caution'}
            green_count = sum(1 for v in ema_status.values() if v == 'Green')
            category = 'two_green' if green_count == 2 else 'one_green' if green_count == 1 else 'two_cautions'

            tp = round_price(symbol, entry_price * (1 + TP_PCT))
            sl = round_price(symbol, entry_price * (1 - SL_PCT))

            signal_strength = {
                'two_green': "HIGH CONFIDENCE (2 Green)",
                'one_green': "MID CONFIDENCE (1 Green + 1 Caution)",
                'two_cautions': "LOW CONFIDENCE (2 Cautions)"
            }[category]

            msg = (
                f"{symbol} - BUY {signal_strength}\n"
                f"• Price vs EMA21: {ema_status['price_ema21']}\n"
                f"• EMA9 vs EMA21: {ema_status['ema9_ema21']}\n"
                f"Entry: {entry_price}\n"
                f"TP: {tp}\n"
                f"SL: {sl}\n"
                f"Capital: ${CAPITAL}"
            )

            alert_queue.put((symbol, msg, 'buy', entry_price, tp, sl, ema_status, category))

    except Exception as e:
        logging.error(f"Error processing {symbol}: {e}")

# === SCAN LOOP ===
def scan_loop():
    load_trades()
    symbols = get_symbols()
    chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
    chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

    alert_queue = queue.Queue()

    def alert_handler():
        while True:
            try:
                symbol, msg, side, entry, tp, sl, ema_status, category = alert_queue.get(timeout=1)
                with trade_lock:
                    if len(open_trades) < MAX_OPEN_TRADES or CATEGORY_PRIORITY[category] > min((CATEGORY_PRIORITY[t['category']] for t in open_trades.values()), default=0):
                        # Replace lowest priority if full
                        if len(open_trades) >= MAX_OPEN_TRADES:
                            lowest = min(open_trades.items(), key=lambda x: CATEGORY_PRIORITY[x[1]['category']])
                            edit_telegram_message(lowest[1]['msg_id'], f"{lowest[0]} - CANCELED (Higher priority signal)")
                            del open_trades[lowest[0]]

                        mid = send_telegram(msg)
                        if mid:
                            open_trades[symbol] = {
                                'side': side,
                                'entry': entry,
                                'tp': tp,
                                'sl': sl,
                                'msg_id': mid,
                                'ema_status': ema_status,
                                'category': category
                            }
                            save_trades()
                alert_queue.task_done()
            except queue.Empty:
                time.sleep(0.1)

    threading.Thread(target=alert_handler, daemon=True).start()
    threading.Thread(target=check_tp, daemon=True).start()

    while True:
        time.sleep(max(0, get_next_candle_close() - time.time()))
        for i, chunk in enumerate(chunks):
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
                for symbol in chunk:
                    exec.submit(process_symbol, symbol, alert_queue)
            if i < len(chunks)-1:
                time.sleep(BATCH_DELAY)

@app.route('/')
def home():
    return "Rising & Falling Three Bot (No DCA + Confidence Preserved) - Running!"

def run_bot():
    load_trades()
    send_telegram(f"BOT STARTED\nOpen trades: {len(open_trades)}")
    threading.Thread(target=scan_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=8080)

if __name__ == "__main__":
    run_bot()
