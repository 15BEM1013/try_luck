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
import traceback
import psutil
import random

# === ANTI-BAN CONFIG ===
BOT_TOKEN = '7662307654:AAG5-juB1faNaFZfC8zjf4LwlZMzs6lEmtE'
CHAT_ID = '655537138'
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0

# 🚨 ANTI-BAN SETTINGS
MAX_WORKERS = 1
BATCH_DELAY = 15.0
NUM_CHUNKS = 20
REQUEST_DELAY = 1.5
MAX_REQS_PER_MIN = 10
SYMBOLS_PER_BATCH = 10
PROXY_ROTATION = True
BACKOFF_MULTIPLIER = 2

CAPITAL = 20.0
LEVERAGE = 5
TP_PCT = 1.0 / 100
SL_PCT = 6.0 / 100
TP_CHECK_INTERVAL = 60
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
ADD_LEVELS = [(0.015, 5.0), (0.03, 10.0), (0.045, 15.0)]
ACCOUNT_SIZE = 1000.0
MAX_RISK_PCT = 4.5 / 100

# === PROXY POOL ===
PROXY_LIST = []

# === RATE LIMITER ===
class RateLimiter:
    def __init__(self, max_requests=10, window=60):
        self.max_requests = max_requests
        self.window = window
        self.requests = []
    
    def wait_if_needed(self):
        now = time.time()
        self.requests = [t for t in self.requests if now - t < self.window]
        if len(self.requests) >= self.max_requests:
            sleep_time = self.window - (now - self.requests[0])
            time.sleep(sleep_time + random.uniform(0.1, 0.5))
        self.requests.append(now)

rate_limiter = RateLimiter(MAX_REQS_PER_MIN, 60)

# === PROXY MANAGER ===
current_proxy_index = 0
def get_next_proxy():
    global current_proxy_index
    if not PROXY_LIST:
        return None
    proxy = PROXY_LIST[current_proxy_index % len(PROXY_LIST)]
    current_proxy_index += 1
    return get_proxy_config(proxy)

def get_proxy_config(proxy):
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

# === CONFIGURE LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === THREAD LOCK ===
trade_lock = threading.Lock()

# === TIME ZONE HELPER ===
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === TRADE PERSISTENCE ===
last_save_time = 0
open_trades = {}
closed_trades = []
sent_signals = {}
last_summary_time = 0
active_threads = {'scan_loop': False, 'send_alerts': False, 'check_tp': False}

def save_trades():
    global last_save_time
    current_time = time.time()
    if current_time - last_save_time >= 300:
        try:
            with trade_lock:
                with open(TRADE_FILE, 'w') as f:
                    json.dump(open_trades, f, default=str)
            last_save_time = current_time
        except Exception as e:
            logging.error(f"Error saving trades: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                open_trades = {k: v for k, v in json.load(f).items()}
    except Exception as e:
        logging.error(f"Error loading trades: {e}")
        open_trades = {}

def save_closed_trades(closed_trade):
    try:
        all_closed_trades = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                all_closed_trades = json.load(f)
        all_closed_trades.append(closed_trade)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(all_closed_trades, f, default=str)
        print(f"Closed trade saved to {CLOSED_TRADE_FILE}")
    except Exception as e:
        print(f"Error saving closed trades: {e}")

def load_closed_trades():
    try:
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                return json.load(f)
        return []
    except Exception as e:
        print(f"Error loading closed trades: {e}")
        return []

# === TELEGRAM ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg}
    try:
        response = requests.post(url, data=data, timeout=10)
        return response.json().get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram error: {e}")
        return None

def edit_telegram_message(message_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text}
    try:
        requests.post(url, data=data, timeout=10)
        print(f"Telegram updated: {new_text}")
    except Exception as e:
        print(f"Edit error: {e}")

# === ENHANCED EXCHANGE INIT ===
exchange = None
def initialize_exchange():
    global exchange
    if PROXY_ROTATION and PROXY_LIST:
        for i, proxy in enumerate(PROXY_LIST):
            try:
                proxies = get_proxy_config(proxy)
                exchange = ccxt.binance({
                    'options': {'defaultType': 'future'},
                    'proxies': proxies,
                    'enableRateLimit': True,
                    'rateLimit': 1200,
                    'timeout': 30000,
                })
                exchange.load_markets()
                logging.info(f"✅ Connected with proxy {i+1}: {proxy['host']}")
                return exchange, proxies
            except Exception as e:
                logging.warning(f"Proxy {i+1} failed: {e}")
                time.sleep(2)
    
    logging.info("🔄 Using direct connection with max safety")
    exchange = ccxt.binance({
        'options': {'defaultType': 'future'},
        'enableRateLimit': True,
        'rateLimit': 1200,
        'timeout': 30000,
        'sandbox': False,
    })
    exchange.load_markets()
    return exchange, None

# === SAFE FETCH ===
def safe_fetch_ohlcv(exchange, symbol, timeframe, limit, max_retries=3):
    rate_limiter.wait_if_needed()
    for attempt in range(max_retries):
        try:
            candles = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            time.sleep(REQUEST_DELAY + random.uniform(0, 0.5))
            return candles
        except ccxt.RateLimitExceeded:
            wait_time = 60 * (2 ** attempt)
            logging.warning(f"Rate limited on {symbol}, waiting {wait_time}s")
            time.sleep(wait_time)
        except ccxt.NetworkError as e:
            logging.warning(f"Network error on {symbol} (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Fetch error on {symbol}: {e}")
            time.sleep(1)
    return None

def safe_fetch_ticker(exchange, symbol, max_retries=3):
    rate_limiter.wait_if_needed()
    for attempt in range(max_retries):
        try:
            ticker = exchange.fetch_ticker(symbol)
            time.sleep(REQUEST_DELAY)
            return ticker
        except ccxt.RateLimitExceeded:
            time.sleep(60 * (2 ** attempt))
        except Exception as e:
            logging.warning(f"Ticker error on {symbol}: {e}")
            time.sleep(2 ** attempt)
    return None

# === CANDLE HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100
def lower_wick_pct(c):
    if is_bearish(c) and (c[1] - c[4]) != 0:
        return (c[1] - c[3]) / (c[1] - c[4]) * 100
    return 0

def upper_wick_pct(c):
    if is_bullish(c) and (c[4] - c[1]) != 0:
        return (c[2] - c[4]) / (c[4] - c[1]) * 100
    elif is_bearish(c) and (c[1] - c[4]) != 0:
        return (c[2] - c[1]) / (c[1] - c[4]) * 100
    return 0

def analyze_first_small_candle(candle, pattern_type):
    body = body_pct(candle)
    upper_wick = (candle[2] - max(candle[1], candle[4])) / candle[1] * 100
    lower_wick = (min(candle[1], candle[4]) - candle[3]) / candle[1] * 100
    wick_ratio = upper_wick / lower_wick if lower_wick != 0 else float('inf')
    wick_ratio_reverse = lower_wick / upper_wick if upper_wick != 0 else float('inf')

    if pattern_type == 'rising':
        if wick_ratio >= 2.5 and body < 0.1:
            return {'text': f"Selling pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
        elif wick_ratio_reverse >= 2.5 and body < 0.1:
            return {'text': f"Buying pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
        elif body >= 0.1:
            if wick_ratio_reverse >= 2.5:
                return {'text': f"Buying pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
            elif wick_ratio >= 2.5:
                return {'text': f"Selling pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
            else:
                return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}
        else:
            return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}
    elif pattern_type == 'falling':
        if wick_ratio_reverse >= 2.5 and body < 0.1:
            return {'text': f"Buying pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
        elif wick_ratio >= 2.5 and body < 0.1:
            return {'text': f"Selling pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
        elif body >= 0.1:
            if wick_ratio_reverse >= 2.5:
                return {'text': f"Buying pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
            elif wick_ratio >= 2.5:
                return {'text': f"Selling pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
            else:
                return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}
        else:
            return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}

# === EMA & RSI ===
def calculate_ema(candles, period=21):
    closes = [c[4] for c in candles]
    if len(closes) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = (close - ema) * multiplier + ema
    return ema

def calculate_rsi(candles, period=14):
    closes = np.array([c[4] for c in candles])
    if len(closes) < period:
        return None
    return talib.RSI(closes, timeperiod=period)[-1]

# === PRICE ROUNDING ===
def round_price(symbol, price):
    try:
        market = exchange.market(symbol)
        tick_size = float(market['info']['filters'][0]['tickSize'])
        precision = int(round(-math.log10(tick_size)))
        return round(price, precision)
    except Exception as e:
        print(f"Error rounding price for {symbol}: {e}")
        return price

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    if len(candles) < 4: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = (
        is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT and
        c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3
    )
    small_red_0 = (
        is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and
        lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT and
        c0[4] > c2[3] + (c2[2] - c2[3]) * 0.3
    )
    return big_green and small_red_1 and small_red_0

def detect_falling_three(candles):
    if len(candles) < 4: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = (
        is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3
    )
    small_green_0 = (
        is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and
        c0[4] < c2[2] - (c2[2] - c2[3]) * 0.3
    )
    return big_red and small_green_1 and small_green_0

# === SYMBOLS ===
def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active') and markets[s].get('info', {}).get('status') == 'TRADING']

# === CANDLE CLOSE ===
def get_next_candle_close():
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    seconds_to_next = (15 * 60) - (seconds % (15 * 60))
    if seconds_to_next < 5:
        seconds_to_next += 15 * 60
    return time.time() + seconds_to_next

# === TP AND SL CHECK ===
def check_tp():
    active_threads['check_tp'] = True
    try:
        global closed_trades
        while True:
            try:
                with trade_lock:
                    for sym, trade in list(open_trades.items()):
                        try:
                            hit = ""
                            current_price = None
                            dca_messages = trade.get('dca_messages', [])

                            ticker = safe_fetch_ticker(exchange, sym)
                            current_price = round_price(sym, ticker['last']) if ticker else None
                            
                            if current_price:
                                initial_entry = trade['initial_entry']
                                adds_done = trade.get('adds_done', 0)
                                total_invested = trade.get('total_invested', CAPITAL)
                                average_entry = trade.get('average_entry', initial_entry)
                                quantity = trade.get('quantity', total_invested / initial_entry)

                                # DCA Logic
                                for i, (against_pct, add_amount) in enumerate(ADD_LEVELS):
                                    if adds_done > i: continue
                                    dca_triggered = False
                                    add_price = None
                                    if trade['side'] == 'buy' and current_price <= initial_entry * (1 - against_pct):
                                        add_price = current_price
                                        dca_triggered = True
                                        dca_message = f"${add_amount:.1f} @ {add_price}"
                                    elif trade['side'] == 'sell' and current_price >= initial_entry * (1 + against_pct):
                                        add_price = current_price
                                        dca_triggered = True
                                        dca_message = f"${add_amount:.1f} @ {add_price}"
                                    if dca_triggered:
                                        add_quantity = add_amount / add_price
                                        total_quantity = quantity + add_quantity
                                        total_invested += add_amount
                                        average_entry = (quantity * average_entry + add_quantity * add_price) / total_quantity
                                        new_tp = round_price(sym, average_entry * (1 + TP_PCT) if trade['side'] == 'buy' else average_entry * (1 - TP_PCT))
                                        new_sl = round_price(sym, average_entry * (1 - SL_PCT) if trade['side'] == 'buy' else average_entry * (1 + SL_PCT))
                                        trade['adds_done'] = i + 1
                                        trade['total_invested'] = total_invested
                                        trade['average_entry'] = round_price(sym, average_entry)
                                        trade['quantity'] = total_quantity
                                        trade['tp'] = new_tp
                                        trade['sl'] = new_sl
                                        trade['last_update_time'] = int(time.time() * 1000)
                                        trade['dca_status'][i] = "🟣"
                                        dca_messages.append(dca_message)
                                        trade['dca_messages'] = dca_messages
                                        
                                        dca_lines = []
                                        for j, (against_pct, _) in enumerate(ADD_LEVELS):
                                            dca_price = round_price(sym, initial_entry * (1 - against_pct) if trade['side'] == 'buy' else initial_entry * (1 + against_pct))
                                            dca_tp = round_price(sym, dca_price * (1 + TP_PCT) if trade['side'] == 'buy' else dca_price * (1 - TP_PCT))
                                            dca_lines.append(f"DCA {j+1} {dca_price} tp-{dca_tp} ({trade['dca_status'][j]})")
                                        new_msg = (
                                            f"{sym} - {'BUY' if trade['side'] == 'buy' else 'SELL'}\n"
                                            f"Initial entry: {trade['initial_entry']}\n"
                                            f"Average entry: {trade['average_entry']}\n"
                                            f"Total invested: ${trade['total_invested']:.2f}\n"
                                            f"{'\n'.join(dca_lines)}\n"
                                            f"DCA Added: {', '.join(dca_messages)}\n"
                                            f"TP: {trade['tp']}\n"
                                            f"SL: {trade['sl']}"
                                        )
                                        trade['msg'] = new_msg
                                        edit_telegram_message(trade['msg_id'], new_msg)
                                        save_trades()
                                        break

                            # TP/SL Check
                            if not hit and current_price:
                                if trade['side'] == 'buy':
                                    if current_price >= trade['tp']:
                                        hit = "TP hit"
                                        hit_price = current_price
                                    elif current_price <= trade['sl']:
                                        hit = "SL hit"
                                        hit_price = current_price
                                else:
                                    if current_price <= trade['tp']:
                                        hit = "TP hit"
                                        hit_price = current_price
                                    elif current_price >= trade['sl']:
                                        hit = "SL hit"
                                        hit_price = current_price

                            if hit:
                                total_quantity = trade.get('quantity', total_invested / initial_entry)
                                if trade['side'] == 'buy':
                                    pnl = (hit_price - trade['average_entry']) / trade['average_entry'] * 100
                                else:
                                    pnl = (trade['average_entry'] - hit_price) / trade['average_entry'] * 100
                                leveraged_pnl_pct = pnl * LEVERAGE
                                profit = trade['total_invested'] * leveraged_pnl_pct / 100
                                
                                closed_trade = {
                                    'symbol': sym,
                                    'pnl': profit,
                                    'pnl_pct': leveraged_pnl_pct,
                                    'category': trade['category'],
                                    'ema_status': trade['ema_status'],
                                    'pressure_status': trade['pressure_status'],
                                    'hit': hit,
                                    'body_pct': trade['body_pct'],
                                    'adds_done': trade['adds_done'],
                                    'total_invested': trade['total_invested'],
                                    'dca_messages': trade.get('dca_messages', [])
                                }
                                closed_trades.append(closed_trade)
                                save_closed_trades(closed_trade)
                                
                                dca_lines = []
                                for j, (against_pct, _) in enumerate(ADD_LEVELS):
                                    dca_price = round_price(sym, trade['initial_entry'] * (1 - against_pct) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + against_pct))
                                    dca_tp = round_price(sym, dca_price * (1 + TP_PCT) if trade['side'] == 'buy' else dca_price * (1 - TP_PCT))
                                    dca_lines.append(f"DCA {j+1} {dca_price} tp-{dca_tp} ({trade['dca_status'][j]})")
                                new_msg = (
                                    f"{sym} - {'BUY' if trade['side'] == 'buy' else 'SELL'}\n"
                                    f"Initial entry: {trade['initial_entry']}\n"
                                    f"Average entry: {trade['average_entry']}\n"
                                    f"Total invested: ${trade['total_invested']:.2f}\n"
                                    f"{'\n'.join(dca_lines)}\n"
                                    f"DCA Added: {', '.join(dca_messages) if dca_messages else 'None'}\n"
                                    f"TP: {trade['tp']}\n"
                                    f"SL: {trade['sl']}\n"
                                    f"Exit: {hit_price}\n"
                                    f"Profit: {leveraged_pnl_pct:.2f}% (${profit:.2f})"
                                )
                                trade['msg'] = new_msg
                                trade['hit'] = hit
                                edit_telegram_message(trade['msg_id'], new_msg)
                                del open_trades[sym]
                                save_trades()
                        except Exception as e:
                            logging.error(f"TP/SL/DCA check error on {sym}: {e}")
                time.sleep(TP_CHECK_INTERVAL)
            except Exception as e:
                logging.error(f"TP/SL/DCA loop error: {e}")
                time.sleep(5)
    except Exception as e:
        active_threads['check_tp'] = False
        error_msg = f"🚨 TP/SL Check Thread Crashed at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S %Z')}\nError: {str(e)}"
        logging.error(error_msg)
        send_telegram(error_msg)

# === PROCESS SYMBOL ===
def clean_sent_signals():
    current_time = int(time.time() * 1000)
    for key in list(sent_signals.keys()):
        if current_time - sent_signals[key] > 24 * 3600 * 1000:
            del sent_signals[key]

def process_symbol(symbol, alert_queue):
    try:
        candles = safe_fetch_ohlcv(exchange, symbol, TIMEFRAME, 30)
        if not candles or len(candles) < 25:
            return
        
        if candles[-1][0] > candles[-2][0]:
            time.sleep(2)
            return

        ema21 = calculate_ema(candles, period=21)
        ema9 = calculate_ema(candles, period=9)
        rsi = calculate_rsi(candles, period=RSI_PERIOD)
        if ema21 is None or ema9 is None or rsi is None:
            return

        signal_time = candles[-2][0]
        first_small_candle_close = round_price(symbol, candles[-3][4])
        second_small_candle_close = round_price(symbol, candles[-2][4])

        if detect_rising_three(candles):
            first_candle_analysis = analyze_first_small_candle(candles[-3], 'rising')
            if first_candle_analysis['body_pct'] > BODY_SIZE_THRESHOLD:
                return
            if sent_signals.get((symbol, 'rising')) == signal_time:
                return
            sent_signals[(symbol, 'rising')] = signal_time
            price_above_ema21 = first_small_candle_close > ema21
            ema9_above_ema21 = ema9 > ema21
            ema_status = {
                'price_ema21': '✅' if price_above_ema21 else '⚠️',
                'ema9_ema21': '✅' if ema9_above_ema21 else '⚠️'
            }
            green_count = sum(1 for v in ema_status.values() if v == '✅')
            category = 'two_green' if green_count == 2 else 'one_green' if green_count == 1 else 'two_cautions'
            side = 'sell'
            entry_price = second_small_candle_close
            tp = round_price(symbol, entry_price * (1 - TP_PCT))
            sl = round_price(symbol, entry_price * (1 + SL_PCT))
            pattern = 'rising'
            dca_lines = []
            dca_status = {i: "Pending" for i in range(len(ADD_LEVELS))}
            for i, (against_pct, _) in enumerate(ADD_LEVELS):
                dca_price = round_price(symbol, entry_price * (1 + against_pct))
                dca_tp = round_price(symbol, dca_price * (1 - TP_PCT))
                dca_lines.append(f"DCA {i+1} {dca_price} tp-{dca_tp} (Pending)")
            msg = (
                f"{symbol} - SELL\n"
                f"Initial entry: {entry_price}\n"
                f"Average entry: {entry_price}\n"
                f"Total invested: ${CAPITAL:.2f}\n"
                f"{'\n'.join(dca_lines)}\n"
                f"DCA Added: None\n"
                f"TP: {tp}\n"
                f"SL: {sl}"
            )
            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], pattern, dca_status, sl))

        elif detect_falling_three(candles):
            first_candle_analysis = analyze_first_small_candle(candles[-3], 'falling')
            if first_candle_analysis['body_pct'] > BODY_SIZE_THRESHOLD:
                return
            if sent_signals.get((symbol, 'falling')) == signal_time:
                return
            sent_signals[(symbol, 'falling')] = signal_time
            price_below_ema21 = first_small_candle_close < ema21
            ema9_below_ema21 = ema9 < ema21
            ema_status = {
                'price_ema21': '✅' if price_below_ema21 else '⚠️',
                'ema9_ema21': '✅' if ema9_below_ema21 else '⚠️'
            }
            green_count = sum(1 for v in ema_status.values() if v == '✅')
            category = 'two_green' if green_count == 2 else 'one_green' if green_count == 1 else 'two_cautions'
            side = 'buy'
            entry_price = second_small_candle_close
            tp = round_price(symbol, entry_price * (1 + TP_PCT))
            sl = round_price(symbol, entry_price * (1 - SL_PCT))
            pattern = 'falling'
            dca_lines = []
            dca_status = {i: "Pending" for i in range(len(ADD_LEVELS))}
            for i, (against_pct, _) in enumerate(ADD_LEVELS):
                dca_price = round_price(symbol, entry_price * (1 - against_pct))
                dca_tp = round_price(symbol, dca_price * (1 + TP_PCT))
                dca_lines.append(f"DCA {i+1} {dca_price} tp-{dca_tp} (Pending)")
            msg = (
                f"{symbol} - BUY\n"
                f"Initial entry: {entry_price}\n"
                f"Average entry: {entry_price}\n"
                f"Total invested: ${CAPITAL:.2f}\n"
                f"{'\n'.join(dca_lines)}\n"
                f"DCA Added: None\n"
                f"TP: {tp}\n"
                f"SL: {sl}"
            )
            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], pattern, dca_status, sl))

    except Exception as e:
        logging.error(f"Process error {symbol}: {e}")
        time.sleep(2)

# === BATCH PROCESSING ===
def process_batch(symbols, alert_queue):
    for symbol in symbols:
        process_symbol(symbol, alert_queue)
        time.sleep(BATCH_DELAY / len(symbols))

# === SCAN LOOP ===
def scan_loop():
    active_threads['scan_loop'] = True
    try:
        global closed_trades, last_summary_time
        load_trades()
        symbols = get_symbols()
        logging.info(f"🔍 Scanning {len(symbols)} symbols SAFELY...")
        
        batch_size = SYMBOLS_PER_BATCH
        batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        
        alert_queue = queue.Queue()

        def send_alerts():
            active_threads['send_alerts'] = True
            try:
                while True:
                    try:
                        item = alert_queue.get(timeout=0.1)
                        symbol, msg, ema_status, category, side, entry_price, tp, first_candle_analysis, pressure_status, body_pct, pattern, dca_status, sl = item
                        with trade_lock:
                            if len(open_trades) < MAX_OPEN_TRADES:
                                mid = send_telegram(msg)
                                if mid and symbol not in open_trades:
                                    trade = {
                                        'side': side,
                                        'entry': entry_price,
                                        'tp': tp,
                                        'sl': sl,
                                        'msg': msg,
                                        'msg_id': mid,
                                        'ema_status': ema_status,
                                        'category': category,
                                        'first_candle_analysis': first_candle_analysis,
                                        'pressure_status': pressure_status,
                                        'body_pct': body_pct,
                                        'entry_time': int(time.time() * 1000),
                                        'last_update_time': int(time.time() * 1000),
                                        'pattern': pattern,
                                        'adds_done': 0,
                                        'average_entry': entry_price,
                                        'total_invested': CAPITAL,
                                        'initial_entry': entry_price,
                                        'quantity': CAPITAL / entry_price,
                                        'dca_messages': [],
                                        'dca_status': dca_status
                                    }
                                    open_trades[symbol] = trade
                                    save_trades()
                                    logging.info(f"New trade opened for {symbol}")
                            else:
                                lowest_priority = min(
                                    (CATEGORY_PRIORITY[trade['category']] for trade in open_trades.values()),
                                    default=0
                                )
                                if CATEGORY_PRIORITY[category] > lowest_priority:
                                    for sym, trade in list(open_trades.items()):
                                        if CATEGORY_PRIORITY[trade['category']] == lowest_priority:
                                            edit_telegram_message(
                                                trade['msg_id'],
                                                f"{sym} - Trade canceled for higher-priority signal."
                                            )
                                            del open_trades[sym]
                                            save_trades()
                                            mid = send_telegram(msg)
                                            if mid and symbol not in open_trades:
                                                trade = {
                                                    'side': side,
                                                    'entry': entry_price,
                                                    'tp': tp,
                                                    'sl': sl,
                                                    'msg': msg,
                                                    'msg_id': mid,
                                                    'ema_status': ema_status,
                                                    'category': category,
                                                    'first_candle_analysis': first_candle_analysis,
                                                    'pressure_status': pressure_status,
                                                    'body_pct': body_pct,
                                                    'entry_time': int(time.time() * 1000),
                                                    'last_update_time': int(time.time() * 1000),
                                                    'pattern': pattern,
                                                    'adds_done': 0,
                                                    'average_entry': entry_price,
                                                    'total_invested': CAPITAL,
                                                    'initial_entry': entry_price,
                                                    'quantity': CAPITAL / entry_price,
                                                    'dca_messages': [],
                                                    'dca_status': dca_status
                                                }
                                                open_trades[symbol] = trade
                                                save_trades()
                                            break
                        alert_queue.task_done()
                    except queue.Empty:
                        time.sleep(1)
                    except Exception as e:
                        logging.error(f"Alert thread error: {e}")
                        time.sleep(1)
            except Exception as e:
                active_threads['send_alerts'] = False
                send_telegram(f"🚨 Send Alerts Thread Crashed: {str(e)}")

        threading.Thread(target=send_alerts, daemon=True).start()
        threading.Thread(target=check_tp, daemon=True).start()

        while True:
            clean_sent_signals()
            next_close = get_next_candle_close()
            wait_time = max(0, next_close - time.time())
            logging.info(f"⏳ Waiting {wait_time:.1f}s for candle...")
            time.sleep(wait_time)

            for i, batch in enumerate(batches):
                logging.info(f"Batch {i+1}/{len(batches)} ({len(batch)} symbols)")
                process_batch(batch, alert_queue)
                if i < len(batches) - 1:
                    time.sleep(BATCH_DELAY * 2)

            logging.info("✅ Scan complete")
            num_open = len(open_trades)
            logging.info(f"📊 Number of open trades: {num_open}")

            current_time = time.time()
            if current_time - last_summary_time >= SUMMARY_INTERVAL:
                all_closed_trades = load_closed_trades()
                # ... (summary logic - same as original)
                summary_msg = f"📊 Hourly Summary\nTotal PnL: ${sum(t.get('pnl', 0) for t in all_closed_trades):.2f}\nOpen Trades: {num_open}"
                send_telegram(summary_msg)
                last_summary_time = current_time
    except Exception as e:
        active_threads['scan_loop'] = False
        send_telegram(f"🚨 Scan Loop Crashed: {str(e)}")

# === FLASK APP ===
app = Flask(__name__)  # ✅ FIXED: MOVED HERE!

@app.route('/')
def home():
    return "✅ Rising & Falling Three Pattern Bot is Live!"

@app.route('/health')
def health():
    thread_status = {name: active_threads.get(name, False) for name in ['scan_loop', 'send_alerts', 'check_tp']}
    memory_usage = psutil.Process().memory_info().rss / 1024 / 1024
    return {
        "status": "running" if all(thread_status.values()) else "degraded",
        "threads": thread_status,
        "open_trades": len(open_trades),
        "memory_mb": f"{memory_usage:.2f}",
        "timestamp": get_ist_time().strftime("%Y-%m-%d %H:%M:%S %Z")
    }

# === RUN ===
def monitor_threads():
    while True:
        for thread_name in ['scan_loop', 'send_alerts', 'check_tp']:
            if not active_threads.get(thread_name, False):
                error_msg = f"🚨 {thread_name.replace('_', ' ').title()} Thread Stopped\nRestarting..."
                logging.error(error_msg)
                send_telegram(error_msg)
                if thread_name == 'scan_loop':
                    threading.Thread(target=scan_loop, daemon=True).start()
                elif thread_name == 'check_tp':
                    threading.Thread(target=check_tp, daemon=True).start()
        time.sleep(60)

def run_bot():
    global last_summary_time
    initialize_exchange()
    threading.Thread(target=monitor_threads, daemon=True).start()
    while True:
        try:
            load_trades()
            num_open = len(open_trades)
            last_summary_time = time.time()
            startup_msg = f"BOT STARTED\nNumber of open trades: {num_open}"
            send_telegram(startup_msg)
            threading.Thread(target=scan_loop, daemon=True).start()
            app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
        except Exception as e:
            error_msg = f"🚨 BOT CRASHED\nError: {str(e)}\nRestarting..."
            logging.error(error_msg)
            send_telegram(error_msg)
            time.sleep(5)

if __name__ == "__main__":
    run_bot()
