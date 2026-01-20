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
RSI_OVERBOUGHT = 80
RSI_OVERSOLD = 30
BODY_SIZE_THRESHOLD = 0.1
SUMMARY_INTERVAL = 3600
ADD_LEVELS = [(0.015, 5.0), (0.03, 10.0), (0.045, 0.0)]
ACCOUNT_SIZE = 1000.0
MAX_RISK_PCT = 4.5 / 100

# === PROXY CONFIGURATION ===
PROXY_LIST = [
    {'host': '142.111.48.253',  'port': 7030, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '31.59.20.176',    'port': 6754, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '23.95.150.145',   'port': 6114, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '198.23.239.134',  'port': 6540, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '45.38.107.97',    'port': 6014, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '107.172.163.27',  'port': 6543, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '64.137.96.74',    'port': 6641, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '216.10.27.159',   'port': 6837, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '142.111.67.146',  'port': 5611, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '142.147.128.93',  'port': 6593, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
]

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
def save_trades():
    try:
        with open(TRADE_FILE, 'w') as f:
            json.dump(open_trades, f, default=str)
        print(f"Trades saved to {TRADE_FILE}")
    except Exception as e:
        print(f"Error saving trades: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                loaded = json.load(f)
                open_trades = {k: v for k, v in loaded.items()}
            print(f"Loaded {len(open_trades)} trades from {TRADE_FILE}")
    except Exception as e:
        print(f"Error loading trades: {e}")
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
        response = requests.post(url, data=data, timeout=5, proxies=proxies).json()
        print(f"Telegram sent: {msg}")
        return response.get('result', {}).get('message_id')
    except Exception as e:
        print(f"Telegram error: {e}")
        return None

def edit_telegram_message(message_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text}
    try:
        requests.post(url, data=data, timeout=5, proxies=proxies)
        print(f"Telegram updated: {new_text}")
    except Exception as e:
        print(f"Edit error: {e}")

# === INIT ===
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def initialize_exchange():
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            logging.info(f"Trying proxy: {proxy['host']}:{proxy['port']}")
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
            logging.info(f"Successfully connected using proxy: {proxy['host']}:{proxy['port']}")
            return exchange, proxies
        except Exception as e:
            logging.error(f"Failed to connect with proxy {proxy['host']}:{proxy['port']}: {e}")
            continue
    logging.error("All proxies failed. Falling back to direct connection.")
    try:
        exchange = ccxt.binance({
            'options': {'defaultType': 'future'},
            'enableRateLimit': True
        })
        exchange.load_markets()
        logging.info("Successfully connected using direct connection.")
        return exchange, None
    except Exception as e:
        logging.error(f"Direct connection failed: {e}")
        raise Exception("All proxies and direct connection failed.")

app = Flask(__name__)

sent_signals = {}
open_trades = {}
closed_trades = []
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
    elif pattern_type == 'falling':
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

# === EMA ===
def calculate_ema(candles, period=21):
    closes = [c[4] for c in candles]
    if len(closes) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = (close - ema) * multiplier + ema
    return ema

# === RSI ===
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
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = (
        is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT and
        c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    )
    small_red_0 = (
        is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and
        lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT and
        c0[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    )
    volume_decreasing = c1[5] > c0[5]
    return big_green and small_red_1 and small_red_0 and volume_decreasing

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = (
        is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    )
    small_green_0 = (
        is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and
        c0[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    )
    volume_decreasing = c1[5] > c0[5]
    return big_red and small_green_1 and small_green_0 and volume_decreasing

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

# === TP AND SL CHECK AND DCA ===
def check_tp():
    global closed_trades
    while True:
        try:
            with trade_lock:
                for sym, trade in list(open_trades.items()):
                    try:
                        hit = ""
                        pnl = 0
                        hit_price = None
                        current_price = None
                        dca_messages = trade.get('dca_messages', [])

                        ticker = exchange.fetch_ticker(sym)
                        current_price = round_price(sym, ticker['last'])
                        if current_price:
                            initial_entry = trade['initial_entry']
                            adds_done = trade.get('adds_done', 0)
                            total_invested = trade.get('total_invested', CAPITAL)
                            average_entry = trade.get('average_entry', initial_entry)
                            quantity = trade.get('quantity', total_invested / initial_entry)

                            for i, (against_pct, add_amount) in enumerate(ADD_LEVELS[:2]):
                                if adds_done > i:
                                    continue
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
                                    trade['dca_status'][i] = "Added"
                                    dca_messages.append(dca_message)
                                    trade['dca_messages'] = dca_messages
                                    logging.info(f"Added ${add_amount} to {sym} at {add_price}, new avg entry: {average_entry}, new TP: {new_tp}, new SL: {new_sl}, total invested: {total_invested}")
                                    dca_lines = []
                                    for j, (against_pct, _) in enumerate(ADD_LEVELS):
                                        if j < 2:
                                            dca_price = round_price(sym, initial_entry * (1 - against_pct) if trade['side'] == 'buy' else initial_entry * (1 + against_pct))
                                            dca_tp = round_price(sym, dca_price * (1 + TP_PCT) if trade['side'] == 'buy' else dca_price * (1 - TP_PCT))
                                            dca_lines.append(f"DCA {j+1} {dca_price} tp-{dca_tp} ({trade['dca_status'][j]})")
                                        else:
                                            dca_price = round_price(sym, initial_entry * (1 - against_pct) if trade['side'] == 'buy' else initial_entry * (1 + against_pct))
                                            dca_lines.append(f"DCA3/SL {dca_price} ({trade['dca_status'][j]})")
                                    ema_status = trade['ema_status']
                                    price_ema21_emoji = "✅" if ema_status['price_ema21'] == 'Green' else "⚠️"
                                    ema9_ema21_emoji = "✅" if ema_status['ema9_ema21'] == 'Green' else "⚠️"
                                    green_count = sum(1 for v in ema_status.values() if v == 'Green')
                                    if green_count == 2:
                                        category_text = "2 green"
                                    elif green_count == 1:
                                        category_text = "1 green 1 cautious"
                                    else:
                                        category_text = "2 cautious"
                                    new_msg = (
                                        f"{sym} - {trade['pattern'].upper()} {'BUY' if trade['side'] == 'buy' else 'SELL'} PATTERN\n"
                                        f"Above 21 ema - {price_ema21_emoji}\n"
                                        f"ema 9 above 21 - {ema9_ema21_emoji}\n"
                                        f"Category: {category_text}\n"
                                        f"First small candle: {trade['pressure_status']}\n"
                                        f"{trade['first_candle_analysis'].replace('\n', '\n')}\n"
                                        f"entry - {trade['average_entry']}\n"
                                        f"tp - {trade['tp']}\n"
                                        f"sl - {trade['sl']}\n"
                                        f"Total invested: ${trade['total_invested']:.2f}\n"
                                        f"{'\n'.join(dca_lines)}\n"
                                        f"DCA Added: {', '.join(dca_messages)}\n"
                                    )
                                    trade['msg'] = new_msg
                                    edit_telegram_message(trade['msg_id'], new_msg)
                                    save_trades()
                                    break

                            if adds_done < 2:
                                against_pct, _ = ADD_LEVELS[2]
                                dca3_sl_triggered = False
                                dca3_price = None
                                if trade['side'] == 'buy' and current_price <= initial_entry * (1 - against_pct):
                                    dca3_price = current_price
                                    dca3_sl_triggered = True
                                elif trade['side'] == 'sell' and current_price >= initial_entry * (1 + against_pct):
                                    dca3_price = current_price
                                    dca3_sl_triggered = True
                                if dca3_sl_triggered:
                                    hit = "DCA3 SL hit"
                                    hit_price = dca3_price
                                    trade['dca_status'][2] = "SL Hit"
                                    logging.info(f"DCA3 SL triggered for {sym} at {dca3_price}")

                        if not hit and trade.get('adds_done', 0) == 0 and trade.get('last_update_time'):
                            candles_1m = exchange.fetch_ohlcv(sym, '1m', since=trade['last_update_time'], limit=2880)
                            for c in candles_1m:
                                high = c[2]
                                low = c[3]
                                if trade['side'] == 'buy':
                                    if high >= trade['tp']:
                                        hit = "TP hit"
                                        hit_price = high
                                        break
                                else:
                                    if low <= trade['tp']:
                                        hit = "TP hit"
                                        hit_price = low
                                        break
                        if not hit and current_price:
                            if trade['side'] == 'buy':
                                if current_price >= trade['tp']:
                                    hit = "TP hit"
                                    hit_price = current_price
                            else:
                                if current_price <= trade['tp']:
                                    hit = "TP hit"
                                    hit_price = current_price

                        if not hit and current_price:
                            if trade['side'] == 'buy':
                                if current_price <= trade['sl']:
                                    hit = "SL hit"
                                    hit_price = current_price
                            else:
                                if current_price >= trade['sl']:
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
                            logging.info(f"{hit} for {sym}: {hit}, Leveraged PnL: {leveraged_pnl_pct:.2f}% at price {hit_price}")
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
                                if j < 2:
                                    dca_price = round_price(sym, trade['initial_entry'] * (1 - against_pct) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + against_pct))
                                    dca_tp = round_price(sym, dca_price * (1 + TP_PCT) if trade['side'] == 'buy' else dca_price * (1 - TP_PCT))
                                    dca_lines.append(f"DCA {j+1} {dca_price} tp-{dca_tp} ({trade['dca_status'][j]})")
                                else:
                                    dca_price = round_price(sym, trade['initial_entry'] * (1 - against_pct) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + against_pct))
                                    dca_lines.append(f"DCA3/SL {dca_price} ({trade['dca_status'][j]})")
                            ema_status = trade['ema_status']
                            price_ema21_emoji = "✅" if ema_status['price_ema21'] == 'Green' else "⚠️"
                            ema9_ema21_emoji = "✅" if ema_status['ema9_ema21'] == 'Green' else "⚠️"
                            green_count = sum(1 for v in ema_status.values() if v == 'Green')
                            if green_count == 2:
                                category_text = "2 green"
                            elif green_count == 1:
                                category_text = "1 green 1 cautious"
                            else:
                                category_text = "2 cautious"
                            new_msg = (
                                f"{sym} - {trade['pattern'].upper()} {'BUY' if trade['side'] == 'buy' else 'SELL'} PATTERN\n"
                                f"Above 21 ema - {price_ema21_emoji}\n"
                                f"ema 9 above 21 - {ema9_ema21_emoji}\n"
                                f"Category: {category_text}\n"
                                f"First small candle: {trade['pressure_status']}\n"
                                f"{trade['first_candle_analysis'].replace('\n', '\n')}\n"
                                f"entry - {trade['average_entry']}\n"
                                f"tp - {trade['tp']}\n"
                                f"sl - {trade['sl']}\n"
                                f"Total invested: ${trade['total_invested']:.2f}\n"
                                f"{'\n'.join(dca_lines)}\n"
                                f"DCA Added: {', '.join(dca_messages) if dca_messages else 'None'}\n"
                                f"Profit/Loss: {leveraged_pnl_pct:.2f}% (${profit:.2f})\n"
                                f"{hit}"
                            )
                            trade['msg'] = new_msg
                            trade['hit'] = hit
                            edit_telegram_message(trade['msg_id'], new_msg)
                            del open_trades[sym]
                            save_trades()
                            logging.info(f"Trade closed for {sym}")
                    except Exception as e:
                        logging.error(f"TP/SL/DCA check error on {sym}: {e}")
            time.sleep(TP_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"TP/SL/DCA loop error: {e}")
            time.sleep(5)

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        for attempt in range(3):
            try:
                candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=30)
                if len(candles) < 25:
                    return
                if attempt < 2 and candles[-1][0] > candles[-2][0]:
                    break
                time.sleep(0.5)
            except ccxt.NetworkError as e:
                print(f"Network error on {symbol}: {e}")
                time.sleep(2 ** attempt)
                continue

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
                'price_ema21': 'Green' if price_above_ema21 else 'Caution',
                'ema9_ema21': 'Green' if ema9_above_ema21 else 'Caution'
            }
            green_count = sum(1 for v in ema_status.values() if v == 'Green')
            if green_count == 2:
                category = 'two_green'
                category_text = "2 green"
            elif green_count == 1:
                category = 'one_green'
                category_text = "1 green 1 cautious"
            else:
                category = 'two_cautions'
                category_text = "2 cautious"
            side = 'sell'
            entry_price = second_small_candle_close
            tp = round_price(symbol, entry_price * (1 - TP_PCT))
            sl = round_price(symbol, entry_price * (1 + SL_PCT))
            pattern = 'rising'
            dca_lines = []
            dca_status = {0: "Pending", 1: "Pending", 2: "Pending"}
            for i, (against_pct, _) in enumerate(ADD_LEVELS):
                if i < 2:
                    dca_price = round_price(symbol, entry_price * (1 + against_pct))
                    dca_tp = round_price(symbol, dca_price * (1 - TP_PCT))
                    dca_lines.append(f"DCA {i+1} {dca_price} tp-{dca_tp} (Pending)")
                else:
                    dca_price = round_price(symbol, entry_price * (1 + against_pct))
                    dca_lines.append(f"DCA3/SL {dca_price} (Pending)")
            price_ema21_emoji = "✅" if ema_status['price_ema21'] == 'Green' else "⚠️"
            ema9_ema21_emoji = "✅" if ema_status['ema9_ema21'] == 'Green' else "⚠️"
            msg = (
                f"{symbol} - {pattern.upper()} SELL PATTERN\n"
                f"Above 21 ema - {price_ema21_emoji}\n"
                f"ema 9 above 21 - {ema9_ema21_emoji}\n"
                f"Category: {category_text}\n"
                f"First small candle: {first_candle_analysis['status']}\n"
                f"{first_candle_analysis['text'].replace('\n', '\n')}\n"
                f"entry - {entry_price}\n"
                f"tp - {tp}\n"
                f"sl - {sl}\n"
                f"Total invested: ${CAPITAL:.2f}\n"
                f"{'\n'.join(dca_lines)}\n"
                f"DCA Added: None\n"
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
                'price_ema21': 'Green' if price_below_ema21 else 'Caution',
                'ema9_ema21': 'Green' if ema9_below_ema21 else 'Caution'
            }
            green_count = sum(1 for v in ema_status.values() if v == 'Green')
            if green_count == 2:
                category = 'two_green'
                category_text = "2 green"
            elif green_count == 1:
                category = 'one_green'
                category_text = "1 green 1 cautious"
            else:
                category = 'two_cautions'
                category_text = "2 cautious"
            side = 'buy'
            entry_price = second_small_candle_close
            tp = round_price(symbol, entry_price * (1 + TP_PCT))
            sl = round_price(symbol, entry_price * (1 - SL_PCT))
            pattern = 'falling'
            dca_lines = []
            dca_status = {0: "Pending", 1: "Pending", 2: "Pending"}
            for i, (against_pct, _) in enumerate(ADD_LEVELS):
                if i < 2:
                    dca_price = round_price(symbol, entry_price * (1 - against_pct))
                    dca_tp = round_price(symbol, dca_price * (1 + TP_PCT))
                    dca_lines.append(f"DCA {i+1} {dca_price} tp-{dca_tp} (Pending)")
                else:
                    dca_price = round_price(symbol, entry_price * (1 - against_pct))
                    dca_lines.append(f"DCA3/SL {dca_price} (Pending)")
            price_ema21_emoji = "✅" if ema_status['price_ema21'] == 'Green' else "⚠️"
            ema9_ema21_emoji = "✅" if ema_status['ema9_ema21'] == 'Green' else "⚠️"
            msg = (
                f"{symbol} - {pattern.upper()} BUY PATTERN\n"
                f"Above 21 ema - {price_ema21_emoji}\n"
                f"ema 9 above 21 - {ema9_ema21_emoji}\n"
                f"Category: {category_text}\n"
                f"First small candle: {first_candle_analysis['status']}\n"
                f"{first_candle_analysis['text'].replace('\n', '\n')}\n"
                f"entry - {entry_price}\n"
                f"tp - {tp}\n"
                f"sl - {sl}\n"
                f"Total invested: ${CAPITAL:.2f}\n"
                f"{'\n'.join(dca_lines)}\n"
                f"DCA Added: None\n"
            )
            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], pattern, dca_status, sl))

    except ccxt.RateLimitExceeded:
        time.sleep(5)
    except Exception as e:
        logging.error(f"Error on {symbol}: {e}")

# === PROCESS BATCH ===
def process_batch(symbols, alert_queue):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {executor.submit(process_symbol, symbol, alert_queue): symbol for symbol in symbols}
        for future in as_completed(future_to_symbol):
            future.result()

# === SCAN LOOP ===
def scan_loop():
    global closed_trades, last_summary_time
    load_trades()
    symbols = get_symbols()
    print(f"Scanning {len(symbols)} Binance Futures symbols...")
    alert_queue = queue.Queue()

    chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
    symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

    def send_alerts():
        while True:
            try:
                symbol, msg, ema_status, category, side, entry_price, tp, first_candle_analysis, pressure_status, body_pct, pattern, dca_status, sl = alert_queue.get(timeout=1)
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
                        lowest_priority = min((CATEGORY_PRIORITY[trade['category']] for trade in open_trades.values()), default=0)
                        if CATEGORY_PRIORITY[category] > lowest_priority:
                            for sym, trade in list(open_trades.items()):
                                if CATEGORY_PRIORITY[trade['category']] == lowest_priority:
                                    edit_telegram_message(trade['msg_id'], f"{sym} - Trade canceled for higher-priority signal.")
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
                                        logging.info(f"Replaced trade with higher priority for {symbol}")
                                    break
                    alert_queue.task_done()
            except queue.Empty:
                with trade_lock:
                    for sym, trade in list(open_trades.items()):
                        if 'hit' in trade:
                            edit_telegram_message(trade['msg_id'], trade['msg'])
                            logging.info(f"Safety net update for {sym}: {trade['hit']}")
                time.sleep(1)
                continue
            except Exception as e:
                logging.error(f"Alert thread error: {e}")
                time.sleep(1)

    threading.Thread(target=send_alerts, daemon=True).start()
    threading.Thread(target=check_tp, daemon=True).start()

    while True:
        next_close = get_next_candle_close()
        wait_time = max(0, next_close - time.time())
        print(f"Waiting {wait_time:.1f} seconds for next 15m candle close...")
        time.sleep(wait_time)

        for i, chunk in enumerate(symbol_chunks):
            print(f"Processing batch {i+1}/{NUM_CHUNKS}...")
            process_batch(chunk, alert_queue)
            if i < NUM_CHUNKS - 1:
                time.sleep(BATCH_DELAY)

        print("Scan complete.")
        num_open = len(open_trades)
        print(f"Number of open trades: {num_open}")

        current_time = time.time()
        if current_time - last_summary_time >= SUMMARY_INTERVAL:
            all_closed_trades = load_closed_trades()
            # ... [summary logic unchanged] ...
            last_summary_time = current_time
            closed_trades = []

# === FLASK ===
@app.route('/')
def home():
    return "Rising & Falling Three Pattern Bot is Live!"

# === RUN ===
def run_bot():
    global last_summary_time
    load_trades()
    num_open = len(open_trades)
    last_summary_time = time.time()
    startup_msg = f"BOT STARTED\nNumber of open trades: {num_open}"
    send_telegram(startup_msg)
    threading.Thread(target=scan_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=8080)

if __name__ == "__main__":
    run_bot()
