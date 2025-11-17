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

# ================================== CONFIG ==================================
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
SUMMARY_INTERVAL = 3600  # 1 hour

# DCA Levels: (against %, add amount)
ADD_LEVELS = [(0.015, 5.0), (0.03, 10.0), (0.045, 0.0)]  # Last one is DCA3/SL

# ============================= PROXY CONFIG ================================
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

# ============================= LOGGING & GLOBALS =============================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
trade_lock = threading.Lock()
open_trades = {}
sent_signals = {}
proxies = None  # Will be set globally after connection
last_summary_time = 0

# ============================= TIME HELPERS =============================
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# ============================= PERSISTENCE =============================
def save_trades():
    with trade_lock:
        try:
            with open(TRADE_FILE, 'w') as f:
                json.dump(open_trades, f, default=str)
        except Exception as e:
            logging.error(f"Error saving open trades: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                open_trades = json.load(f)
            logging.info(f"Loaded {len(open_trades)} open trades")
    except Exception as e:
        logging.error(f"Error loading open trades: {e}")
        open_trades = {}

def save_closed_trades(closed_trade):
    try:
        trades = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                trades = json.load(f)
        trades.append(closed_trade)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(trades, f, default=str)
    except Exception as e:
        logging.error(f"Error saving closed trade: {e}")

def load_closed_trades():
    try:
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                return json.load(f)
        return []
    except:
        return []

# ============================= TELEGRAM =============================
def send_telegram(msg):
    global proxies
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg, 'disable_web_page_preview': True}
    try:
        response = requests.post(url, data=data, timeout=10, proxies=proxies or {}).json()
        return response.get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram send error: {e}")
        return None

def edit_telegram_message(message_id, new_text):
    global proxies
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text, 'disable_web_page_preview': True}
    try:
        requests.post(url, data=data, timeout=10, proxies=proxies or {})
    except Exception as e:
        logging.error(f"Telegram edit error: {e}")

# ============================= EXCHANGE INIT =============================
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def initialize_exchange():
    global proxies
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            logging.info(f"Trying proxy: {proxy['host']}:{proxy['port']}")
            session = requests.Session()
            retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(pool_maxsize=20, max_retries=retry))
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
            logging.error(f"Proxy failed {proxy['host']}:{proxy['port']} → {e}")
            continue

    logging.warning("All proxies failed. Trying direct connection...")
    try:
        exchange = ccxt.binance({
            'options': {'defaultType': 'future'},
            'enableRateLimit': True
        })
        exchange.load_markets()
        proxies = None
        logging.info("Connected directly (no proxy)")
        return exchange, None
    except Exception as e:
        logging.error(f"Direct connection failed: {e}")
        raise

exchange, proxies = initialize_exchange()

# ============================= CANDLE HELPERS =============================
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100

def analyze_first_small_candle(candle, pattern_type):
    open_p, high, low, close = candle[1], candle[2], candle[3], candle[4]
    body = abs(close - open_p)
    upper_wick = high - max(open_p, close)
    lower_wick = min(open_p, close) - low
    total = body + upper_wick + lower_wick
    if total == 0: total = 0.0001
    upper_pct = upper_wick / total * 100
    lower_pct = lower_wick / total * 100
    body_pct_val = body / candle[1] * 100

    if body_pct_val > BODY_SIZE_THRESHOLD:
        return {'text': 'Body too big', 'status': 'invalid', 'body_pct': body_pct_val}

    if upper_pct / lower_pct >= 2.5:
        pressure = "Selling pressure" if pattern_type == 'rising' else "Buying pressure"
        status = 'selling_pressure' if pattern_type == 'rising' else 'buying_pressure'
    elif lower_pct / upper_pct >= 2.5:
        pressure = "Buying pressure" if pattern_type == 'rising' else "Selling pressure"
        status = 'buying_pressure' if pattern_type == 'rising' else 'selling_pressure'
    else:
        pressure = "Neutral"
        status = 'neutral'

    return {
        'text': f"{pressure}\nUpper: {upper_pct:.1f}%\nLower: {lower_pct:.1f}%\nBody: {body_pct_val:.2f}%",
        'status': status,
        'body_pct': body_pct_val
    }

def calculate_ema(candles, period=21):
    closes = [c[4] for c in candles]
    if len(closes) < period: return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
    return ema

def calculate_rsi(candles, period=14):
    closes = np.array([c[4] for c in candles])
    if len(closes) < period + 1: return None
    return talib.RSI(closes, timeperiod=period)[-1]

def round_price(symbol, price):
    try:
        market = exchange.market(symbol)
        tick_size = float(market['info']['filters'][2]['tickSize'])
        precision = int(round(-math.log10(tick_size)))
        return round(price, precision)
    except:
        return round(price, 8)

# ============================= PATTERN DETECTION =============================
def detect_rising_three(candles):
    if len(candles) < 5: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-7:-2]) / 5
    return (is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol and
            all(is_bearish(c) and body_pct(c) <= MAX_SMALL_BODY_PCT and c[4] > c2[3] for c in [c1, c0]) and
            c1[5] > c0[5])

def detect_falling_three(candles):
    if len(candles) < 5: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-7:-2]) / 5
    return (is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol and
            all(is_bullish(c) and body_pct(c) <= MAX_SMALL_BODY_PCT and c[4] < c2[2] for c in [c1, c0]) and
            c1[5] > c0[5])

def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s]['active']]

def get_next_candle_close():
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    seconds_to_next = (15 * 60) - (seconds % (15 * 60))
    if seconds_to_next <= 10:
        seconds_to_next += 15 * 60
    return time.time() + seconds_to_next

# ============================= TP/SL/DCA MONITOR =============================
def check_tp():
    global last_summary_time
    while True:
        try:
            with trade_lock:
                for sym, trade in list(open_trades.items()):
                    try:
                        ticker = exchange.fetch_ticker(sym)
                        price = ticker['last']
                        price = round_price(sym, price)

                        initial_entry = trade['initial_entry']
                        avg_entry = trade['average_entry']
                        adds_done = trade.get('adds_done', 0)
                        total_invested = trade.get('total_invested', CAPITAL)
                        quantity = trade.get('quantity', CAPITAL / initial_entry)
                        side = trade['side']

                        # DCA Logic (now based on current avg entry)
                        for i, (pct, amount) in enumerate(ADD_LEVELS[:2]):
                            if adds_done > i:
                                continue
                            trigger_price = avg_entry * (1 + pct) if side == 'sell' else avg_entry * (1 - pct)
                            if (side == 'buy' and price <= trigger_price) or (side == 'sell' and price >= trigger_price):
                                add_qty = amount / price
                                new_qty = quantity + add_qty
                                new_avg = (quantity * avg_entry + add_qty * price) / new_qty
                                new_tp = round_price(sym, new_avg * (1 + TP_PCT) if side == 'buy' else new_avg * (1 - TP_PCT))
                                new_sl = round_price(sym, new_avg * (1 - SL_PCT) if side == 'buy' else new_avg * (1 + SL_PCT))

                                trade.update({
                                    'adds_done': i + 1,
                                    'quantity': new_qty,
                                    'average_entry': new_avg,
                                    'total_invested': total_invested + amount,
                                    'tp': new_tp,
                                    'sl': new_sl,
                                    'dca_messages': trade.get('dca_messages', []) + [f"${amount} @ {price}"],
                                    'dca_status': trade.get('dca_status', ["Pending"]*3)
                                })
                                trade['dca_status'][i] = "Added"
                                save_trades()
                                update_trade_message(sym, trade)
                                break

                        # Check DCA3/SL
                        if adds_done < 2:
                            pct, _ = ADD_LEVELS[2]
                            trigger = avg_entry * (1 + pct) if side == 'sell' else avg_entry * (1 - pct)
                            if (side == 'buy' and price <= trigger) or (side == 'sell' and price >= trigger):
                                close_trade(sym, trade, "DCA3/SL Hit", price)

                        # TP/SL Check
                        tp_hit = (side == 'buy' and price >= trade['tp']) or (side == 'sell' and price <= trade['tp'])
                        sl_hit = (side == 'buy' and price <= trade['sl']) or (side == 'sell' and price >= trade['sl'])

                        if tp_hit or sl_hit:
                            hit_type = "TP Hit" if tp_hit else "SL Hit"
                            close_trade(sym, trade, hit_type, price)

                    except Exception as e:
                        logging.error(f"Error monitoring {sym}: {e}")
            time.sleep(TP_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"TP monitor crash: {e}")
            time.sleep(10)

def close_trade(sym, trade, hit_type, exit_price):
    pnl_pct = ((exit_price - trade['average_entry']) / trade['average_entry'] * 100 * (1 if trade['side'] == 'buy' else -1))
    leveraged_pnl = pnl_pct * LEVERAGE
    profit = trade['total_invested'] * leveraged_pnl / 100

    closed = {
        'symbol': sym,
        'pnl': round(profit, 2),
        'pnl_pct': round(leveraged_pnl, 2),
        'category': trade['category'],
        'hit': hit_type,
        'total_invested': trade['total_invested'],
        'timestamp': get_ist_time().strftime('%Y-%m-%d %H:%M')
    }
    save_closed_trades(closed)

    final_msg = f"{trade['msg']}\n\nExit: {exit_price}\n{hit_type}\nProfit: {leveraged_pnl:+.2f}% (${profit:+.2f})"
    edit_telegram_message(trade['msg_id'], final_msg)
    del open_trades[sym]
    save_trades()

def update_trade_message(sym, trade):
    lines = [f"{sym} - {'BUY' if trade['side']=='buy' else 'SELL'}"]
    lines += [f"Initial: {trade['initial_entry']}", f"Avg Entry: {trade['average_entry']:.6f}"]
    lines += [f"Invested: ${trade['total_invested']:.2f}"]
    for i, (pct, amt) in enumerate(ADD_LEVELS):
        price = trade['initial_entry'] * (1 - pct) if trade['side']=='buy' else trade['initial_entry'] * (1 + pct)
        price = round_price(sym, price)
        status = trade.get('dca_status', ['Pending']*3)[i]
        if i < 2:
            lines.append(f"DCA {i+1}: {price} ({status})")
        else:
            lines.append(f"DCA3/SL: {price} ({status})")
    lines += [f"TP: {trade['tp']}", f"SL: {trade['sl']}"]
    if trade.get('dca_messages'):
        lines.append(f"Added: {', '.join(trade['dca_messages'])}")
    new_msg = "\n".join(lines)
    trade['msg'] = new_msg
    edit_telegram_message(trade['msg_id'], new_msg)

# ============================= MAIN SCAN =============================
def process_symbol(symbol, alert_queue):
    try:
        candles = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=30)
        if len(candles) < 25: return

        ema9 = calculate_ema(candles, 9)
        ema21 = calculate_ema(candles, 21)
        if not ema9 or not ema21: return

        close_prev = candles[-2][4]
        signal_time = candles[-2][0]

        if detect_rising_three(candles):
            analysis = analyze_first_small_candle(candles[-3], 'rising')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'rising')) == signal_time: return
            sent_signals[(symbol, 'rising')] = signal_time

            price_above_ema21 = close_prev > ema21
            ema9_above_ema21 = ema9 > ema21
            greens = sum([price_above_ema21, ema9_above_ema21])
            category = ['two_cautions', 'one_green', 'two_green'][greens]

            entry = round_price(symbol, close_prev)
            tp = round_price(symbol, entry * (1 - TP_PCT))
            sl = round_price(symbol, entry * (1 + SL_PCT))

            msg = f"{symbol} - SELL\nEntry: {entry}\nTP: {tp}\nSL: {sl}\nCategory: {category.replace('_', ' ').title()}\n{analysis['text']}"
            alert_queue.put((CATEGORY_PRIORITY[category], symbol, msg, 'sell', entry, tp, sl, category, analysis['status']))

        elif detect_falling_three(candles):
            analysis = analyze_first_small_candle(candles[-3], 'falling')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'falling')) == signal_time: return
            sent_signals[(symbol, 'falling')] = signal_time

            price_below_ema21 = close_prev < ema21
            ema9_below_ema21 = ema9 < ema21
            greens = sum([price_below_ema21, ema9_below_ema21])
            category = ['two_cautions', 'one_green', 'two_green'][greens]

            entry = round_price(symbol, close_prev)
            tp = round_price(symbol, entry * (1 + TP_PCT))
            sl = round_price(symbol, entry * (1 - SL_PCT))

            msg = f"{symbol} - BUY\nEntry: {entry}\nTP: {tp}\nSL: {sl}\nCategory: {category.replace('_', ' ').title()}\n{analysis['text']}"
            alert_queue.put((CATEGORY_PRIORITY[category], symbol, msg, 'buy', entry, tp, sl, category, analysis['status']))

    except Exception as e:
        if "rate limit" not in str(e).lower():
            logging.error(f"Error processing {symbol}: {e}")

# ============================= ALERT HANDLER =============================
def alert_handler(alert_queue):
    while True:
        try:
            priority, sym, msg, side, entry, tp, sl, category, pressure = alert_queue.get(timeout=2)
            with trade_lock:
                if len(open_trades) < MAX_OPEN_TRADES or priority > min(CATEGORY_PRIORITY[t['category']] for t in open_trades.values()):
                    if len(open_trades) == MAX_OPEN_TRADES:
                        worst = min(open_trades.items(), key=lambda x: CATEGORY_PRIORITY[x[1]['category']])
                        edit_telegram_message(worst[1]['msg_id'], f"{worst[0]} - CANCELED (Higher priority signal)")
                        del open_trades[worst[0]]

                    mid = send_telegram(msg)
                    if mid:
                        open_trades[sym] = {
                            'side': side, 'initial_entry': entry, 'average_entry': entry,
                            'tp': tp, 'sl': sl, 'msg': msg, 'msg_id': mid, 'category': category,
                            'pressure_status': pressure, 'total_invested': CAPITAL, 'quantity': CAPITAL/entry,
                            'adds_done': 0, 'dca_messages': [], 'dca_status': ["Pending"]*3
                        }
                        save_trades()
            alert_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Alert handler error: {e}")

# ============================= MAIN LOOP =============================
app = Flask(__name__)

@app.route('/')
def home():
    return "Rising & Falling Three Bot is Running | " + get_ist_time().strftime('%d %b %Y %I:%M %p')

def run_bot():
    global last_summary_time
    load_trades()
    send_telegram(f"BOT STARTED\nOpen Trades: {len(open_trades)}\nTime: {get_ist_time().strftime('%d %b %Y %I:%M %p')}")
    last_summary_time = time.time()

    alert_queue = queue.PriorityQueue()
    threading.Thread(target=alert_handler, args=(alert_queue,), daemon=True).start()
    threading.Thread(target=check_tp, daemon=True).start()

    symbols = get_symbols()
    chunks = [symbols[i::NUM_CHUNKS] for i in range(NUM_CHUNKS)]

    while True:
        next_close = get_next_candle_close()
        sleep_time = max(0, next_close - time.time())
        print(f"Next 15m candle in {sleep_time:.0f}s...")
        time.sleep(sleep_time + 5)

        for i, chunk in enumerate(chunks):
            print(f"Scanning chunk {i+1}/{len(chunks)}...")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
                for sym in chunk:
                    exec.submit(process_symbol, sym, alert_queue)
            time.sleep(BATCH_DELAY)

        # PnL Summary
        if time.time() - last_summary_time >= SUMMARY_INTERVAL:
            trades = load_closed_trades()
            if trades:
                total_pnl = sum(t['pnl'] for t in trades)
                wins = sum(1 for t in trades if t['pnl'] > 0)
                winrate = wins/len(trades)*100 if trades else 0
                summary = (
                    f"PNL SUMMARY\n"
                    f"Date: {get_ist_time().strftime('%d %b %Y %I:%M %p')}\n"
                    f"Closed Trades: {len(trades)}\n"
                    f"Win Rate: {winrate:.1f}% ({wins}W/{len(trades)-wins}L)\n"
                    f"Total Profit: ${total_pnl:+.2f}\n"
                    f"Open Trades: {len(open_trades)}"
                )
                send_telegram(summary)
            last_summary_time = time.time()

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    app.run(host='0.0.0.0', port=8080)
