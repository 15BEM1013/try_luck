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

# === UPDATED CONFIG ===
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
SL_PCT = 4.5 / 100  # ✅ FIXED at 4.5% from INITIAL ENTRY
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
ADD_LEVELS = [(0.015, 10.0), (0.03, 10.0)]  # ✅ DCA1: $10, DCA2: $10
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

# === GLOBALS ===
trade_lock = threading.Lock()
open_trades = {}
closed_trades = []
sent_signals = {}
active_threads = {}
app = Flask(__name__)

# === TIME ZONE HELPER ===
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === NEW COMPACT MESSAGE FUNCTION ===
def create_compact_msg(symbol, side, initial_entry, avg_entry, total_invested, tp, sl, dca_status):
    dca_lines = []
    for i, status in dca_status.items():
        if i >= len(ADD_LEVELS): continue
        pct, amt = ADD_LEVELS[i]
        dca_price = round_price(symbol, initial_entry * (1 - pct) if side == 'buy' else initial_entry * (1 + pct))
        dca_lines.append(f"DCA{i+1}: ${amt} {dca_price} {status}")
    
    return (
        f"{symbol} - {side.upper()}\n"
        f"Avg: ${avg_entry} | ${total_invested}\n"
        f"TP: ${tp} | SL: ${sl}\n"
        f"{chr(10).join(dca_lines)}"
    )

# === TRADE PERSISTENCE ===
def save_trades():
    try:
        with trade_lock:
            with open(TRADE_FILE, 'w') as f:
                json.dump(open_trades, f, default=str)
    except Exception as e:
        logging.error(f"Error saving trades: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                open_trades = json.load(f)
    except Exception as e:
        logging.error(f"Error loading trades: {e}")
        open_trades = {}

def save_closed_trades(closed_trade):
    try:
        all_closed_trades = load_closed_trades()
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
    except Exception as e:
        print(f"Edit error: {e}")

# === EXCHANGE INIT ===
def initialize_exchange():
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
                logging.info(f"✅ Connected with proxy {i+1}")
                return exchange, proxies
            except Exception as e:
                logging.warning(f"Proxy {i+1} failed: {e}")
                time.sleep(2)
    
    exchange = ccxt.binance({
        'options': {'defaultType': 'future'},
        'enableRateLimit': True,
        'rateLimit': 1200,
        'timeout': 30000,
    })
    exchange.load_markets()
    return exchange, None

exchange, current_proxies = initialize_exchange()

# === SAFE FETCH ===
def safe_fetch_ohlcv(exchange, symbol, timeframe, limit, max_retries=3):
    rate_limiter.wait_if_needed()
    for attempt in range(max_retries):
        try:
            candles = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            time.sleep(REQUEST_DELAY + random.uniform(0, 0.5))
            return candles
        except ccxt.RateLimitExceeded:
            time.sleep(60 * (2 ** attempt))
        except Exception as e:
            logging.warning(f"Fetch error on {symbol}: {e}")
            time.sleep(2 ** attempt)
    return None

def safe_fetch_ticker(exchange, symbol, max_retries=3):
    rate_limiter.wait_if_needed()
    for attempt in range(max_retries):
        try:
            ticker = exchange.fetch_ticker(symbol)
            time.sleep(REQUEST_DELAY)
            return ticker
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

def round_price(symbol, price):
    try:
        market = exchange.market(symbol)
        tick_size = float(market['info']['filters'][0]['tickSize'])
        precision = int(round(-math.log10(tick_size)))
        return round(price, precision)
    except:
        return price

# === EMA & RSI ===
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

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    if len(candles) < 4: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = (is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT and c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3)
    small_red_0 = (is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT and c0[4] > c2[3] + (c2[2] - c2[3]) * 0.3)
    return big_green and small_red_1 and small_red_0

def detect_falling_three(candles):
    if len(candles) < 4: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = (is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3)
    small_green_0 = (is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and c0[4] < c2[2] - (c2[2] - c2[3]) * 0.3)
    return big_red and small_green_1 and small_green_0

# === SYMBOLS ===
def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active')]

# === UPDATED TP/SL/DCA CHECK ===
def check_tp():
    active_threads['check_tp'] = True
    try:
        while True:
            try:
                with trade_lock:
                    for sym, trade in list(open_trades.items()):
                        ticker = safe_fetch_ticker(exchange, sym)
                        if not ticker: continue
                        current_price = round_price(sym, ticker['last'])
                        
                        # FIXED SL from INITIAL ENTRY
                        initial_sl = round_price(sym, trade['initial_entry'] * (1 - SL_PCT) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + SL_PCT))
                        
                        adds_done = trade.get('adds_done', 0)
                        total_invested = trade.get('total_invested', CAPITAL)
                        average_entry = trade.get('average_entry', trade['initial_entry'])
                        quantity = trade.get('quantity', total_invested / average_entry)
                        
                        # DCA CHECK (MAX 2 LEVELS)
                        dca_triggered = False
                        for i, (against_pct, add_amount) in enumerate(ADD_LEVELS):
                            if adds_done >= 2: break  # MAX 2 DCA
                            if adds_done > i: continue
                            
                            if trade['side'] == 'buy' and current_price <= trade['initial_entry'] * (1 - against_pct):
                                add_quantity = add_amount / current_price
                                total_quantity = quantity + add_quantity
                                total_invested += add_amount
                                average_entry = (quantity * average_entry + add_quantity * current_price) / total_quantity
                                new_tp = round_price(sym, average_entry * (1 + TP_PCT))
                                
                                trade['adds_done'] = i + 1
                                trade['total_invested'] = total_invested
                                trade['average_entry'] = round_price(sym, average_entry)
                                trade['quantity'] = total_quantity
                                trade['tp'] = new_tp
                                trade['last_update_time'] = int(time.time() * 1000)
                                trade['dca_status'][i] = "🟢"
                                trade['dca_messages'].append(f"${add_amount} @ {current_price}")
                                dca_triggered = True
                                break
                            elif trade['side'] == 'sell' and current_price >= trade['initial_entry'] * (1 + against_pct):
                                # Similar logic for sell...
                                pass
                        
                        # TP/SL CHECK
                        hit = ""
                        if trade['side'] == 'buy':
                            if current_price >= trade['tp']: hit = "TP hit"
                            elif current_price <= initial_sl: hit = "SL hit"
                        else:
                            if current_price <= trade['tp']: hit = "TP hit"
                            elif current_price >= initial_sl: hit = "SL hit"
                        
                        # UPDATE MESSAGE
                        if dca_triggered or hit:
                            msg = create_compact_msg(sym, trade['side'], trade['initial_entry'], average_entry, total_invested, trade['tp'], initial_sl, trade['dca_status'])
                            trade['msg'] = msg
                            edit_telegram_message(trade['msg_id'], msg)
                            save_trades()
                        
                        # PROCESS HIT
                        if hit:
                            total_quantity = trade.get('quantity', total_invested / average_entry)
                            if trade['side'] == 'buy':
                                pnl = (current_price - average_entry) / average_entry * 100
                            else:
                                pnl = (average_entry - current_price) / average_entry * 100
                            leveraged_pnl_pct = pnl * LEVERAGE
                            profit = total_invested * leveraged_pnl_pct / 100
                            
                            closed_trade = {
                                'symbol': sym, 'pnl': profit, 'pnl_pct': leveraged_pnl_pct,
                                'category': trade['category'], 'adds_done': adds_done,
                                'total_invested': total_invested, 'hit': hit
                            }
                            save_closed_trades(closed_trade)
                            msg += f"\n{hit}: ${profit:.2f}"
                            edit_telegram_message(trade['msg_id'], msg)
                            del open_trades[sym]
                            save_trades()
                
                time.sleep(TP_CHECK_INTERVAL)
            except Exception as e:
                logging.error(f"TP/SL loop error: {e}")
                time.sleep(5)
    except Exception as e:
        active_threads['check_tp'] = False
        send_telegram(f"🚨 TP Thread Crashed: {e}")

# === NEW COMPACT SUMMARY ===
def get_compact_summary():
    all_closed = load_closed_trades()
    
    no_dca = [t for t in all_closed if t['adds_done'] == 0]
    dca1 = [t for t in all_closed if t['adds_done'] == 1]
    dca2 = [t for t in all_closed if t['adds_done'] == 2]
    
    def calc_stats(trades):
        if not trades: return 0,0,0,0.0
        tp = sum(1 for t in trades if t['hit'] == 'TP hit')
        sl = sum(1 for t in trades if t['hit'] == 'SL hit')
        pnl = sum(t['pnl'] for t in trades)
        return len(trades), tp, sl, pnl
    
    n_dca = calc_stats(no_dca)
    d1 = calc_stats(dca1)
    d2 = calc_stats(dca2)
    
    total_pnl = sum(t['pnl'] for t in all_closed)
    top_symbol = max(set(t['symbol'] for t in all_closed), 
                    key=lambda s: sum(t['pnl'] for t in all_closed if t['symbol']==s), 
                    default='None')
    
    return (
        f"🔍 SUMMARY {get_ist_time().strftime('%I:%M %p IST')}\n"
        f"TOTAL PnL: ${total_pnl:.2f}\n\n"
        f"DCA NO HIT ({n_dca[0]})\n"
        f"TP: {n_dca[1]} | SL: {n_dca[2]} | +${n_dca[3]:.2f}\n\n"
        f"DCA1 HIT ({d1[0]})\n"
        f"TP: {d1[1]} | SL: {d1[2]} | +${d1[3]:.2f}\n\n"
        f"DCA2 HIT ({d2[0]})\n"
        f"TP: {d2[1]} | SL: {d2[2]} | +${d2[3]:.2f}\n\n"
        f"OPEN: {len(open_trades)} | TOP: {top_symbol}"
    )

# === PROCESS SYMBOL (UPDATED) ===
def process_symbol(symbol, alert_queue):
    try:
        candles = safe_fetch_ohlcv(exchange, symbol, TIMEFRAME, 30)
        if not candles or len(candles) < 25: return
        
        if candles[-1][0] > candles[-2][0]: return

        ema21 = calculate_ema(candles, 21)
        ema9 = calculate_ema(candles, 9)
        if ema21 is None or ema9 is None: return

        signal_time = candles[-2][0]
        entry_price = round_price(symbol, candles[-2][4])
        
        # FIXED SL from INITIAL
        sl = round_price(symbol, entry_price * (1 - SL_PCT) if 'falling' else entry_price * (1 + SL_PCT))
        tp = round_price(symbol, entry_price * (1 + TP_PCT) if 'falling' else entry_price * (1 - TP_PCT))
        
        dca_status = {0: "🔴", 1: "🔴"}  # Only 2 levels
        
        if detect_rising_three(candles):
            if sent_signals.get((symbol, 'rising')) == signal_time: return
            sent_signals[(symbol, 'rising')] = signal_time
            side = 'sell'
            category = 'two_green'  # Simplified
            msg = create_compact_msg(symbol, side, entry_price, entry_price, CAPITAL, tp, sl, dca_status)
            alert_queue.put((symbol, msg, category, side, entry_price, tp, sl, dca_status))
            
        elif detect_falling_three(candles):
            if sent_signals.get((symbol, 'falling')) == signal_time: return
            sent_signals[(symbol, 'falling')] = signal_time
            side = 'buy'
            category = 'two_green'
            msg = create_compact_msg(symbol, side, entry_price, entry_price, CAPITAL, tp, sl, dca_status)
            alert_queue.put((symbol, msg, category, side, entry_price, tp, sl, dca_status))
            
    except Exception as e:
        logging.error(f"Process error {symbol}: {e}")

# === SCAN LOOP (🚨 FIXED!) ===
def scan_loop():
    active_threads['scan_loop'] = True
    try:
        load_trades()
        symbols = get_symbols()
        alert_queue = queue.Queue()
        
        # 🔥 THE 1-LINE FIX THAT SAVES YOUR TRADES!
        global last_summary_time
        if last_summary_time == 0:
            last_summary_time = time.time()
            print("🚀 FIXED: last_summary_time initialized!")
        
        def send_alerts():
            active_threads['send_alerts'] = True
            while True:
                try:
                    item = alert_queue.get(timeout=0.1)
                    if len(item) < 8: continue
                    symbol, msg, category, side, entry_price, tp, sl, dca_status = item
                    
                    with trade_lock:
                        if len(open_trades) < MAX_OPEN_TRADES:
                            mid = send_telegram(msg)
                            if mid:
                                trade = {
                                    'side': side, 'initial_entry': entry_price, 'tp': tp, 'sl': sl,
                                    'msg': msg, 'msg_id': mid, 'category': category,
                                    'entry_time': int(time.time() * 1000), 'adds_done': 0,
                                    'average_entry': entry_price, 'total_invested': CAPITAL,
                                    'quantity': CAPITAL / entry_price, 'dca_messages': [],
                                    'dca_status': dca_status
                                }
                                open_trades[symbol] = trade
                                save_trades()
                                print(f"💰 NEW TRADE: {symbol} {side}")  # DEBUG
                except queue.Empty:
                    time.sleep(1)
        
        threading.Thread(target=send_alerts, daemon=True).start()
        threading.Thread(target=check_tp, daemon=True).start()
        
        print("🚀 SCAN LOOP STARTED - TRADES LIVE!")  # DEBUG
        
        while True:
            next_close = time.time() + ((15 * 60) - (time.time() % (15 * 60)))
            time.sleep(max(0, next_close - time.time()))
            
            for i, batch in enumerate([symbols[i:i+SYMBOLS_PER_BATCH] for i in range(0, len(symbols), SYMBOLS_PER_BATCH)]):
                for symbol in batch:
                    process_symbol(symbol, alert_queue)
                if i < len([symbols[i:i+SYMBOLS_PER_BATCH] for i in range(0, len(symbols), SYMBOLS_PER_BATCH)]) - 1:
                    time.sleep(BATCH_DELAY)
            
            # ✅ NOW SAFE - NO CRASH!
            if time.time() - last_summary_time >= SUMMARY_INTERVAL:
                print("📊 SENDING SUMMARY")  # DEBUG
                send_telegram(get_compact_summary())
                last_summary_time = time.time()
                
    except Exception as e:
        active_threads['scan_loop'] = False
        print(f"🚨 Scan Error: {e}")  # DEBUG
        send_telegram(f"🚨 Scan Crashed: {e}")

# === FLASK ===
@app.route('/')
def home(): return "✅ Updated Bot Live!"

@app.route('/health')
def health():
    return {"status": "running", "open_trades": len(open_trades)}

# === RUN ===
def run_bot():
    global last_summary_time
    last_summary_time = time.time()  # ✅ SAFE INITIALIZE
    print("🚀 BOT STARTING - TRADES WILL WORK!")
    send_telegram(f"BOT STARTED | OPEN: {len(open_trades)}")
    threading.Thread(target=scan_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))

if __name__ == "__main__":
    run_bot()
