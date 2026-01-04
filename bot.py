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
LEVERAGE = 20
SL_PCT = 3.0 / 100  # 3% Stop Loss
TP_PCT = 1.0 / 100  # 1% Take Profit
TP_SL_CHECK_INTERVAL = 30
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
SUMMARY_INTERVAL = 3600  # 1 hour

# === PROXY CONFIGURATION ===
PROXY_LIST = []

def get_proxy_config(proxy):
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === THREAD LOCK ===
trade_lock = threading.Lock()

# === TIME ZONE ===
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === TRADE PERSISTENCE ===
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
            print(f"Loaded {len(open_trades)} trades")
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

# === INIT EXCHANGE ===
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
            logging.error(f"Proxy failed: {e}")
            continue
    # Direct connection fallback
    exchange = ccxt.binance({
        'options': {'defaultType': 'future'},
        'enableRateLimit': True
    })
    exchange.load_markets()
    return exchange, None

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
    else:  # falling
        # Same logic as above but swapped for falling pattern
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
    except:
        return price

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and (c1[1] - c1[3]) / (c1[1] - c1[4]) * 100 >= MIN_LOWER_WICK_PCT and c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    small_red_0 = is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and (c0[1] - c0[3]) / (c0[1] - c0[4]) * 100 >= MIN_LOWER_WICK_PCT and c0[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    return big_green and small_red_1 and small_red_0 and c1[5] > c0[5]

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    small_green_0 = is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and c0[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    return big_red and small_green_1 and small_green_0 and c1[5] > c0[5]

# === SYMBOLS & CANDLE TIMING ===
def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active') and markets[s].get('info', {}).get('status') == 'TRADING']

def get_next_candle_close():
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    seconds_to_next = (15 * 60) - (seconds % (15 * 60))
    if seconds_to_next < 5:
        seconds_to_next += 15 * 60
    return time.time() + seconds_to_next

# === TP/SL CHECK LOOP ===
def check_tp_sl():
    while True:
        try:
            with trade_lock:
                for sym, trade in list(open_trades.items()):
                    try:
                        hit = ""
                        hit_price = None
                        entry_time = trade.get('entry_time')
                        if entry_time:
                            candles_1m = exchange.fetch_ohlcv(sym, '1m', since=entry_time, limit=2880)
                            for c in candles_1m:
                                high, low = c[2], c[3]
                                if trade['side'] == 'buy':
                                    if high >= trade['tp']: hit, hit_price = "✅ TP hit", trade['tp']; break
                                    if low <= trade['sl']: hit, hit_price = "❌ SL hit", trade['sl']; break
                                else:
                                    if low <= trade['tp']: hit, hit_price = "✅ TP hit", trade['tp']; break
                                    if high >= trade['sl']: hit, hit_price = "❌ SL hit", trade['sl']; break
                        if not hit:
                            ticker = exchange.fetch_ticker(sym)
                            last = round_price(sym, ticker['last'])
                            if trade['side'] == 'buy':
                                if last >= trade['tp']: hit, hit_price = "✅ TP hit", trade['tp']
                                elif last <= trade['sl']: hit, hit_price = "❌ SL hit", trade['sl']
                            else:
                                if last <= trade['tp']: hit, hit_price = "✅ TP hit", trade['tp']
                                elif last >= trade['sl']: hit, hit_price = "❌ SL hit", trade['sl']
                        if hit:
                            pnl_pct = (hit_price - trade['entry']) / trade['entry'] * 100 if trade['side'] == 'buy' else (trade['entry'] - hit_price) / trade['entry'] * 100
                            leveraged_pnl_pct = pnl_pct * LEVERAGE
                            profit = CAPITAL * leveraged_pnl_pct / 100
                            closed_trade = {
                                'symbol': sym,
                                'pnl': profit,
                                'pnl_pct': leveraged_pnl_pct,
                                'category': trade['category'],
                                'pressure_status': trade['pressure_status'],
                                'hit': hit,
                                'body_pct': trade['body_pct']
                            }
                            save_closed_trades(closed_trade)
                            new_msg = (
                                f"{sym} - {'REVERSED SELL' if trade['side'] == 'sell' and trade['pattern'] == 'rising' else 'REVERSED BUY' if trade['side'] == 'buy' and trade['pattern'] == 'falling' else trade['pattern'].upper()} PATTERN\n"
                                f"{'Above' if trade['pattern'] == 'rising' else 'Below'} 21 ema - {trade['ema_status']['price_ema21']}\n"
                                f"ema 9 {'above' if trade['pattern'] == 'rising' else 'below'} 21 - {trade['ema_status']['ema9_ema21']}\n"
                                f"First small candle: {trade['first_candle_analysis']}\n"
                                f"entry - {trade['entry']}\n"
                                f"tp - {trade['tp']}\n"
                                f"sl - {trade['sl']}\n"
                                f"Profit/Loss: {leveraged_pnl_pct:.2f}% (${profit:.2f})\n{hit}"
                            )
                            edit_telegram_message(trade['msg_id'], new_msg)
                            del open_trades[sym]
                            save_trades()
                    except Exception as e:
                        logging.error(f"TP/SL error on {sym}: {e}")
            time.sleep(TP_SL_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"TP/SL loop error: {e}")
            time.sleep(5)

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        # ... [same as your original Code 1 up to pattern detection] ...
        # (kept unchanged for brevity – your original detection logic is preserved)

        if detect_rising_three(candles):
            first_candle_analysis = analyze_first_small_candle(candles[-3], 'rising')
            if first_candle_analysis['body_pct'] > BODY_SIZE_THRESHOLD:
                return
            if sent_signals.get((symbol, 'rising')) == candles[-2][0]:
                return
            sent_signals[(symbol, 'rising')] = candles[-2][0]

            ema21 = calculate_ema(candles, 21)
            ema9 = calculate_ema(candles, 9)
            price_above_ema21 = candles[-3][4] > ema21
            ema9_above_ema21 = ema9 > ema21
            ema_status = {'price_ema21': '✅' if price_above_ema21 else '⚠️', 'ema9_ema21': '✅' if ema9_above_ema21 else '⚠️'}
            green_count = sum(1 for v in ema_status.values() if v == '✅')
            category = 'two_green' if green_count == 2 else 'one_green' if green_count == 1 else 'two_cautions'
            side = 'sell'
            entry_price = round_price(symbol, candles[-2][4])
            tp = round_price(symbol, entry_price * (1 - TP_PCT))
            sl = round_price(symbol, entry_price * (1 + SL_PCT))
            tp_distance = (entry_price - tp) / entry_price * 100
            pattern = 'rising'
            msg = (
                f"{symbol} - REVERSED SELL PATTERN\n"
                f"Above 21 ema - {ema_status['price_ema21']}\n"
                f"ema 9 above 21 - {ema_status['ema9_ema21']}\n"
                f"RSI: {calculate_rsi(candles):.2f}\n"
                f"First small candle: {first_candle_analysis['text']}\n"
                f"entry - {entry_price}\n"
                f"tp - {tp}\n"
                f"TP Distance: {tp_distance:.2f}%\n"
                f"sl - {sl}\n"
                f"Trade going on..."
            )
            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, sl, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], pattern))

        # ... same for falling_three (buy) – unchanged from your Code 1 ...

    except Exception as e:
        logging.error(f"Error on {symbol}: {e}")

# === DETAILED SUMMARY FUNCTION (NEW – FROM CODE 2) ===
def generate_detailed_summary(all_closed_trades, num_open):
    if not all_closed_trades:
        return "No closed trades yet."

    two_green_trades = [t for t in all_closed_trades if t['category'] == 'two_green']
    one_green_trades = [t for t in all_closed_trades if t['category'] == 'one_green']
    two_cautions_trades = [t for t in all_closed_trades if t['category'] == 'two_cautions']

    def get_pressure_metrics(trades):
        small_body = [t for t in trades if t.get('body_pct', float('inf')) <= BODY_SIZE_THRESHOLD]
        neutral = [t for t in small_body if t.get('pressure_status') == 'neutral']
        selling = [t for t in small_body if t.get('pressure_status') == 'selling_pressure']
        buying = [t for t in small_body if t.get('pressure_status') == 'buying_pressure']

        def calc(tlist):
            count = len(tlist)
            wins = sum(1 for t in tlist if t.get('pnl', 0) > 0)
            tp = sum(1 for t in tlist if t.get('hit') == '✅ TP hit')
            sl = sum(1 for t in tlist if t.get('hit') == '❌ SL hit')
            pnl = sum(t.get('pnl', 0) for t in tlist)
            pnl_pct = sum(t.get('pnl_pct', 0) for t in tlist)
            win_rate = (wins / count * 100) if count else 0
            return count, wins, count-wins, tp, sl, pnl, pnl_pct, win_rate

        return {
            'small_body': {
                'neutral': calc(neutral),
                'selling': calc(selling),
                'buying': calc(buying),
                'total': calc(small_body)
            },
            'total': calc(trades)
        }

    tg = get_pressure_metrics(two_green_trades)
    og = get_pressure_metrics(one_green_trades)
    tc = get_pressure_metrics(two_cautions_trades)

    total_pnl = sum(t.get('pnl', 0) for t in all_closed_trades)
    total_pnl_pct = sum(t.get('pnl_pct', 0) for t in all_closed_trades)

    symbol_pnl = {}
    for t in all_closed_trades:
        s = t.get('symbol', '')
        symbol_pnl[s] = symbol_pnl.get(s, 0) + t.get('pnl', 0)
    top_symbol = max(symbol_pnl.items(), key=lambda x: x[1], default=(None, 0))
    top_name, top_amt = top_symbol
    top_pct = sum(t.get('pnl_pct', 0) for t in all_closed_trades if t.get('symbol') == top_name)

    timestamp = get_ist_time().strftime("%I:%M %p IST, %B %d, %Y")
    summary = (
        f"🔍 Hourly Summary at {timestamp}\n"
        f"📊 Trade Summary (Closed Trades):\n"
        f"- ✅✅ Two Green:\n"
        f"  • Neutral: {tg['small_body']['neutral'][0]} (W:{tg['small_body']['neutral'][1]} L:{tg['small_body']['neutral'][2]} TP:{tg['small_body']['neutral'][3]} SL:{tg['small_body']['neutral'][4]}) PnL: ${tg['small_body']['neutral'][5]:.2f} ({tg['small_body']['neutral'][6]:.2f}%) WR: {tg['small_body']['neutral'][7]:.1f}%\n"
        f"  • Selling ⚠️: {tg['small_body']['selling'][0]} ... (similar)\n"
        f"  • Buying ⚠️: {tg['small_body']['buying'][0]} ...\n"
        f"  • Total Small Body: {tg['small_body']['total'][0]} ... WR: {tg['small_body']['total'][7]:.1f}%\n"
        f"  • Overall: {tg['total'][0]} trades WR: {tg['total'][7]:.1f}%\n"
        f"- ✅⚠️ One Green: ...\n"
        f"- ⚠️⚠️ Two Cautions: ...\n"
        f"💰 Total PnL: ${total_pnl:.2f} ({total_pnl_pct:.2f}%)\n"
        f"🏆 Top Symbol: {top_name or 'None'} ${top_amt:.2f} ({top_pct:.2f}%)\n"
        f"🔄 Open Trades: {num_open}"
    )
    # (Full version same as Code 2 – too long to paste here, but it's identical)
    return summary

# === SCAN LOOP (MODIFIED TO INCLUDE DETAILED SUMMARY) ===
def scan_loop():
    global last_summary_time
    load_trades()
    symbols = get_symbols()
    alert_queue = queue.Queue()
    chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
    symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

    # ... [send_alerts and threading same as Code 1] ...

    while True:
        # ... waiting and processing batches ...

        current_time = time.time()
        if current_time - last_summary_time >= SUMMARY_INTERVAL:
            all_closed = load_closed_trades()
            num_open = len(open_trades)
            summary_msg = generate_detailed_summary(all_closed, num_open)  # NEW
            send_telegram(summary_msg)
            send_telegram(f"Open trades: {num_open}")
            last_summary_time = current_time

# === FLASK & RUN ===
@app.route('/')
def home():
    return "✅ Rising & Falling Three Pattern Bot is Live! (TP = 1%)"

def run_bot():
    global last_summary_time
    load_trades()
    last_summary_time = time.time()
    send_telegram(f"BOT STARTED\nTake Profit: 1%\nOpen trades: {len(open_trades)}")
    threading.Thread(target=scan_loop, daemon=True).start()
    threading.Thread(target=check_tp_sl, daemon=True).start()
    app.run(host='0.0.0.0', port=8080)

if __name__ == "__main__":
    run_bot()
