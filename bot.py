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

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === THREAD LOCK ===
trade_lock = threading.Lock()

# === TIME ZONE ===
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
            continue
    # Fallback to direct
    exchange = ccxt.binance({
        'options': {'defaultType': 'future'},
        'enableRateLimit': True
    })
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

def analyze_first_small_candle(candle, pattern_type):
    body = body_pct(candle)
    upper_wick = (candle[2] - max(candle[1], candle[4])) / candle[1] * 100
    lower_wick = (min(candle[1], candle[4]) - candle[3]) / candle[1] * 100
    wick_ratio = upper_wick / lower_wick if lower_wick != 0 else float('inf')
    wick_ratio_reverse = lower_wick / upper_wick if upper_wick != 0 else float('inf')

    if pattern_type == 'rising':
        if wick_ratio >= 2.5 and body < 0.1:
            pressure = "Selling pressure"
        elif wick_ratio_reverse >= 2.5 and body < 0.1:
            pressure = "Buying pressure"
        elif body >= 0.1:
            if wick_ratio_reverse >= 2.5:
                pressure = "Buying pressure"
            elif wick_ratio >= 2.5:
                pressure = "Selling pressure"
            else:
                pressure = "Neutral"
        else:
            pressure = "Neutral"
    else:  # falling
        if wick_ratio_reverse >= 2.5 and body < 0.1:
            pressure = "Buying pressure"
        elif wick_ratio >= 2.5 and body < 0.1:
            pressure = "Selling pressure"
        elif body >= 0.1:
            if wick_ratio_reverse >= 2.5:
                pressure = "Buying pressure"
            elif wick_ratio >= 2.5:
                pressure = "Selling pressure"
            else:
                pressure = "Neutral"
        else:
            pressure = "Neutral"

    wick_body_line = f"U: {upper_wick:.2f}% L: {lower_wick:.2f}% Body: {body:.2f}%"
    full_text = f"{pressure}\n{wick_body_line}"

    return {'text': full_text, 'main': pressure, 'wick_body': wick_body_line, 'status': pressure.lower().replace(' ', '_'), 'body_pct': body}

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

# === SYMBOLS ===
def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active')]

# === NEXT CANDLE ===
def get_next_candle_close():
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    seconds_to_next = (15 * 60) - (seconds % (15 * 60))
    if seconds_to_next < 5:
        seconds_to_next += 15 * 60
    return time.time() + seconds_to_next

# === TP/SL + DCA CHECK ===
def check_tp():
    global closed_trades
    while True:
        try:
            with trade_lock:
                for sym, trade in list(open_trades.items()):
                    try:
                        ticker = exchange.fetch_ticker(sym)
                        current_price = round_price(sym, ticker['last'])
                        initial_entry = trade['initial_entry']
                        adds_done = trade.get('adds_done', 0)
                        total_invested = trade.get('total_invested', CAPITAL)
                        average_entry = trade.get('average_entry', initial_entry)
                        dca_messages = trade.get('dca_messages', [])

                        hit = ""
                        hit_price = None

                        # DCA Logic
                        for i, (against_pct, add_amount) in enumerate(ADD_LEVELS[:2]):
                            if adds_done > i:
                                continue
                            trigger_price = initial_entry * (1 + against_pct) if trade['side'] == 'sell' else initial_entry * (1 - against_pct)
                            if (trade['side'] == 'sell' and current_price >= trigger_price) or (trade['side'] == 'buy' and current_price <= trigger_price):
                                add_price = current_price
                                add_quantity = add_amount / add_price
                                total_quantity = trade['quantity'] + add_quantity
                                total_invested += add_amount
                                average_entry = (trade['quantity'] * trade['average_entry'] + add_quantity * add_price) / total_quantity
                                new_tp = round_price(sym, average_entry * (1 - TP_PCT) if trade['side'] == 'sell' else average_entry * (1 + TP_PCT))
                                new_sl = round_price(sym, average_entry * (1 + SL_PCT) if trade['side'] == 'sell' else average_entry * (1 - SL_PCT))

                                trade.update({
                                    'adds_done': i + 1,
                                    'total_invested': total_invested,
                                    'average_entry': average_entry,
                                    'quantity': total_quantity,
                                    'tp': new_tp,
                                    'sl': new_sl,
                                    'dca_status': trade['dca_status'].copy(),
                                    'dca_messages': dca_messages + [f"${add_amount:.1f} @ {add_price}"]
                                })
                                trade['dca_status'][i] = "Added"

                                # Update message
                                new_msg = build_trade_message(sym, trade)
                                edit_telegram_message(trade['msg_id'], new_msg)
                                trade['msg'] = new_msg
                                save_trades()
                                break

                        # DCA3/SL Trigger
                        if adds_done < 2:
                            against_pct = ADD_LEVELS[2][0]
                            trigger_price = initial_entry * (1 + against_pct) if trade['side'] == 'sell' else initial_entry * (1 - against_pct)
                            if (trade['side'] == 'sell' and current_price >= trigger_price) or (trade['side'] == 'buy' and current_price <= trigger_price):
                                hit = "DCA3 SL hit"
                                hit_price = current_price
                                trade['dca_status'][2] = "SL Hit"

                        # TP/SL Check
                        if not hit:
                            if (trade['side'] == 'buy' and current_price >= trade['tp']) or (trade['side'] == 'sell' and current_price <= trade['tp']):
                                hit = "TP hit"
                                hit_price = current_price
                            elif (trade['side'] == 'buy' and current_price <= trade['sl']) or (trade['side'] == 'sell' and current_price >= trade['sl']):
                                hit = "SL hit"
                                hit_price = current_price

                        if hit:
                            pnl_pct = (hit_price - average_entry) / average_entry * 100 if trade['side'] == 'buy' else (average_entry - hit_price) / average_entry * 100
                            leveraged_pnl_pct = pnl_pct * LEVERAGE
                            profit = total_invested * leveraged_pnl_pct / 100

                            closed_trade = {
                                'symbol': sym,
                                'pnl': profit,
                                'pnl_pct': leveraged_pnl_pct,
                                'category': trade['category'],
                                'pressure_status': trade['pressure_status'],
                                'hit': hit,
                                'adds_done': adds_done,
                                'total_invested': total_invested
                            }
                            save_closed_trades(closed_trade)

                            final_msg = build_trade_message(sym, trade, hit=hit, hit_price=hit_price, profit=profit, pnl_pct=leveraged_pnl_pct)
                            edit_telegram_message(trade['msg_id'], final_msg)
                            del open_trades[sym]
                            save_trades()

                    except Exception as e:
                        logging.error(f"Check error {sym}: {e}")
            time.sleep(TP_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"Check loop error: {e}")
            time.sleep(5)

def build_trade_message(sym, trade, hit=None, hit_price=None, profit=None, pnl_pct=None):
    side_text = 'BUY' if trade['side'] == 'buy' else 'SELL'
    ema_line = f"{'Below' if trade['side'] == 'buy' else 'Above'} 21 ema - {'✅' if trade['ema_status']['price_ema21'] == 'Green' else '⚠️'} ema 9 {'below' if trade['side'] == 'buy' else 'above'} 21 - {'✅' if trade['ema_status']['ema9_ema21'] == 'Green' else '⚠️'}"

    pressure_main = trade['first_candle_analysis']['main']
    wick_body = trade['first_candle_analysis'].get('wick_body', '')

    dca_added = 'None' if not trade.get('dca_messages') else ', '.join(trade.get('dca_messages', []))

    dca_lines = []
    for j, (against_pct, _) in enumerate(ADD_LEVELS):
        status = trade['dca_status'].get(j, "Pending")
        if j < 2:
            price = round_price(sym, trade['initial_entry'] * (1 - against_pct) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + against_pct))
            tp_price = round_price(sym, price * (1 + TP_PCT) if trade['side'] == 'buy' else price * (1 - TP_PCT))
            dca_lines.append(f"DCA {j+1} {price} tp-{tp_price} ({status})")
        else:
            price = round_price(sym, trade['initial_entry'] * (1 - against_pct) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + against_pct))
            dca_lines.append(f"SL {price} ({status})")

    base = (
        f"{sym} - {side_text}\n"
        f"{ema_line}\n"
        f"First small candle: {pressure_main}\n"
        f"{wick_body}\n"
        f"Initial entry: {trade['initial_entry']} Average entry: {trade['average_entry']} Total invested: ${trade['total_invested']:.2f}\n"
        f"{' '.join(dca_lines)}\n"
        f"DCA Added: {dca_added}\n"
        f"TP: {trade['tp']} SL: {trade['sl']}"
    )

    if hit:
        base += f"\nExit: {hit_price}\nProfit: {pnl_pct:.2f}% (${profit:.2f})\n{hit}"

    return base

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=30)
        if len(candles) < 25:
            return

        ema21 = calculate_ema(candles, 21)
        ema9 = calculate_ema(candles, 9)
        if ema21 is None or ema9 is None:
            return

        signal_time = candles[-2][0]
        entry_price = round_price(symbol, candles[-2][4])

        if detect_rising_three(candles):
            analysis = analyze_first_small_candle(candles[-3], 'rising')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD:
                return
            if sent_signals.get((symbol, 'rising')) == signal_time:
                return
            sent_signals[(symbol, 'rising')] = signal_time

            price_above = candles[-3][4] > ema21
            ema9_above = ema9 > ema21
            ema_status = {'price_ema21': 'Green' if price_above else 'Caution', 'ema9_ema21': 'Green' if ema9_above else 'Caution'}
            green_count = sum(1 for v in ema_status.values() if v == 'Green')
            category = 'two_green' if green_count == 2 else 'one_green' if green_count == 1 else 'two_cautions'

            side = 'sell'
            tp = round_price(symbol, entry_price * (1 - TP_PCT))
            sl = round_price(symbol, entry_price * (1 + SL_PCT))

            dca_lines = []
            dca_status = {0: "Pending", 1: "Pending", 2: "Pending"}
            for i, (pct, _) in enumerate(ADD_LEVELS):
                if i < 2:
                    dca_p = round_price(symbol, entry_price * (1 + pct))
                    dca_tp = round_price(symbol, dca_p * (1 - TP_PCT))
                    dca_lines.append(f"DCA {i+1} {dca_p} tp-{dca_tp} (Pending)")
                else:
                    dca_p = round_price(symbol, entry_price * (1 + pct))
                    dca_lines.append(f"SL {dca_p} (Pending)")

            msg = build_trade_message(symbol, {
                'side': side, 'initial_entry': entry_price, 'average_entry': entry_price, 'total_invested': CAPITAL,
                'tp': tp, 'sl': sl, 'ema_status': ema_status, 'first_candle_analysis': analysis,
                'pressure_status': analysis['status'], 'category': category, 'dca_status': dca_status
            })

            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, analysis['text'], analysis['status'], analysis['body_pct'], dca_status))

        elif detect_falling_three(candles):
            analysis = analyze_first_small_candle(candles[-3], 'falling')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD:
                return
            if sent_signals.get((symbol, 'falling')) == signal_time:
                return
            sent_signals[(symbol, 'falling')] = signal_time

            price_below = candles[-3][4] < ema21
            ema9_below = ema9 < ema21
            ema_status = {'price_ema21': 'Green' if price_below else 'Caution', 'ema9_ema21': 'Green' if ema9_below else 'Caution'}
            green_count = sum(1 for v in ema_status.values() if v == 'Green')
            category = 'two_green' if green_count == 2 else 'one_green' if green_count == 1 else 'two_cautions'

            side = 'buy'
            tp = round_price(symbol, entry_price * (1 + TP_PCT))
            sl = round_price(symbol, entry_price * (1 - SL_PCT))

            dca_lines = []
            dca_status = {0: "Pending", 1: "Pending", 2: "Pending"}
            for i, (pct, _) in enumerate(ADD_LEVELS):
                if i < 2:
                    dca_p = round_price(symbol, entry_price * (1 - pct))
                    dca_tp = round_price(symbol, dca_p * (1 + TP_PCT))
                    dca_lines.append(f"DCA {i+1} {dca_p} tp-{dca_tp} (Pending)")
                else:
                    dca_p = round_price(symbol, entry_price * (1 - pct))
                    dca_lines.append(f"SL {dca_p} (Pending)")

            msg = build_trade_message(symbol, {
                'side': side, 'initial_entry': entry_price, 'average_entry': entry_price, 'total_invested': CAPITAL,
                'tp': tp, 'sl': sl, 'ema_status': ema_status, 'first_candle_analysis': analysis,
                'pressure_status': analysis['status'], 'category': category, 'dca_status': dca_status
            })

            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, analysis['text'], analysis['status'], analysis['body_pct'], dca_status))

    except Exception as e:
        logging.error(f"Error {symbol}: {e}")

# === BATCH & SCAN ===
def process_batch(symbols, alert_queue):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_symbol, s, alert_queue): s for s in symbols}
        for f in as_completed(futures):
            f.result()

def scan_loop():
    global last_summary_time
    load_trades()
    symbols = get_symbols()
    chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
    chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

    alert_queue = queue.Queue()

    def send_alerts():
        while True:
            try:
                item = alert_queue.get(timeout=1)
                symbol, msg, ema_status, category, side, entry_price, tp, _, pressure_status, body_pct, dca_status = item
                with trade_lock:
                    if len(open_trades) >= MAX_OPEN_TRADES:
                        # Priority replacement logic
                        lowest = min((CATEGORY_PRIORITY[t['category']] for t in open_trades.values()), default=0)
                        if CATEGORY_PRIORITY[category] <= lowest:
                            alert_queue.task_done()
                            continue
                        for s, t in list(open_trades.items()):
                            if CATEGORY_PRIORITY[t['category']] == lowest:
                                edit_telegram_message(t['msg_id'], f"{s} - Trade canceled for higher-priority signal.")
                                del open_trades[s]
                                save_trades()
                                break

                    mid = send_telegram(msg)
                    if mid:
                        open_trades[symbol] = {
                            'side': side,
                            'initial_entry': entry_price,
                            'average_entry': entry_price,
                            'total_invested': CAPITAL,
                            'quantity': CAPITAL / entry_price,
                            'tp': tp,
                            'sl': round_price(symbol, entry_price * (1 + SL_PCT) if side == 'sell' else entry_price * (1 - SL_PCT)),
                            'msg': msg,
                            'msg_id': mid,
                            'ema_status': ema_status,
                            'category': category,
                            'first_candle_analysis': analysis,
                            'pressure_status': pressure_status,
                            'adds_done': 0,
                            'dca_messages': [],
                            'dca_status': dca_status
                        }
                        save_trades()
                alert_queue.task_done()
            except queue.Empty:
                time.sleep(1)

    threading.Thread(target=send_alerts, daemon=True).start()
    threading.Thread(target=check_tp, daemon=True).start()

    while True:
        time.sleep(max(0, get_next_candle_close() - time.time()))
        for i, chunk in enumerate(chunks):
            process_batch(chunk, alert_queue)
            if i < len(chunks) - 1:
                time.sleep(BATCH_DELAY)

        num_open = len(open_trades)
        current_time = time.time()
        if current_time - last_summary_time >= SUMMARY_INTERVAL:
            all_closed_trades = load_closed_trades()

            def get_metrics(trades):
                neutral = [t for t in trades if t.get('pressure_status') == 'neutral']
                selling = [t for t in trades if t.get('pressure_status') == 'selling_pressure']
                buying = [t for t in trades if t.get('pressure_status') == 'buying_pressure']

                def calc(trade_list):
                    count = len(trade_list)
                    if count == 0:
                        return 0,0,0,0,0,0.0,0.0,0.0,(0,0,0,0)
                    wins = sum(1 for t in trade_list if t.get('pnl',0) > 0)
                    tp = sum(1 for t in trade_list if 'TP hit' in t.get('hit',''))
                    sl = sum(1 for t in trade_list if 'SL hit' in t.get('hit','') or 'DCA3' in t.get('hit',''))
                    pnl = sum(t.get('pnl',0) for t in trade_list)
                    pnl_pct = sum(t.get('pnl_pct',0) for t in trade_list)
                    win_rate = wins / count * 100
                    adds = [t.get('adds_done',0) for t in trade_list]
                    no_dca = adds.count(0)
                    dca1 = adds.count(1)
                    dca2 = adds.count(2)
                    dca3 = count - (no_dca + dca1 + dca2)
                    return count,wins,count-wins,tp,sl,pnl,pnl_pct,win_rate,(no_dca,dca1,dca2,dca3)

                n = calc(neutral)
                s = calc(selling)
                b = calc(buying)
                total_count = n[0]+s[0]+b[0]
                total_w = n[1]+s[1]+b[1]
                total_pnl = n[5]+s[5]+b[5]
                total_pct = n[6]+s[6]+b[6]
                total_wr = total_w / total_count * 100 if total_count else 0
                total_dca = (n[8][0]+s[8][0]+b[8][0], n[8][1]+s[8][1]+b[8][1], n[8][2]+s[8][2]+b[8][2], n[8][3]+s[8][3]+b[8][3])
                return {'neutral': n, 'selling': s, 'buying': b, 'total': (total_count,total_w,total_count-total_w,n[3]+s[3]+b[3],n[4]+s[4]+b[4],total_pnl,total_pct,total_wr,total_dca)}

            tg = get_metrics([t for t in all_closed_trades if t['category'] == 'two_green'])
            og = get_metrics([t for t in all_closed_trades if t['category'] == 'one_green'])
            tc = get_metrics([t for t in all_closed_trades if t['category'] == 'two_cautions'])

            total_pnl = sum(t.get('pnl',0) for t in all_closed_trades)
            total_pnl_pct = sum(t.get('pnl_pct',0) for t in all_closed_trades)

            top_symbol = "None"
            top_pnl = 0
            top_pct = 0
            if all_closed_trades:
                sym_pnl = {}
                for t in all_closed_trades:
                    sym_pnl[t['symbol']] = sym_pnl.get(t['symbol'],0) + t.get('pnl',0)
                if sym_pnl:
                    top_symbol = max(sym_pnl, key=sym_pnl.get)
                    top_pnl = sym_pnl[top_symbol]
                    top_pct = sum(t.get('pnl_pct',0) for t in all_closed_trades if t['symbol'] == top_symbol)

            timestamp = get_ist_time().strftime("%I:%M %p IST, %B %d, %Y")

            def subcat(name, stats, indent=" • "):
                c,w,l,tp,sl,pnl,pct,wr,dca = stats
                no,d1,d2,d3 = dca
                return (
                    f"{indent}{name}: {c} trade{'s' if c!=1 else ''} (W:{w} L:{l} | TP:{tp} SL:{sl})\n"
                    f"   - No DCA: {no} | DCA1: {d1} | DCA2: {d2} | DCA3/SL: {d3}\n"
                    f"   → +${pnl:.2f} (+{pct:.1f}%)"
                )

            summary_msg = (
                f"🔍 Hourly Summary at {timestamp}\n\n"
                f"📊 Closed Trades Summary:\n\n"
                f"✅✅ Two Green Ticks ({tg['total'][0]} trades)\n"
                f"{subcat('Neutral', tg['neutral'])}\n"
                f"{subcat('Selling Pressure', tg['selling'])}\n"
                f"{subcat('Buying Pressure', tg['buying'])}\n"
                f" → Total: +${tg['total'][5]:.2f} (+{tg['total'][6]:.1f}%) | Win Rate: {tg['total'][7]:.1f}%\n\n"
                f"✅⚠️ One Green One Caution ({og['total'][0]} trades)\n"
                f"{subcat('Neutral', og['neutral'])}\n"
                f"{subcat('Selling Pressure', og['selling'])}\n"
                f"{subcat('Buying Pressure', og['buying'])}\n"
                f" → Total: +${og['total'][5]:.2f} (+{og['total'][6]:.1f}%) | Win Rate: {og['total'][7]:.1f}%\n\n"
                f"⚠️⚠️ Two Cautions ({tc['total'][0]} trades)\n"
                f"{subcat('Neutral', tc['neutral'])}\n"
                f"{subcat('Selling Pressure', tc['selling'])}\n"
                f"{subcat('Buying Pressure', tc['buying'])}\n"
                f" → Total: +${tc['total'][5]:.2f} (+{tc['total'][6]:.1f}%) | Win Rate: {tc['total'][7]:.1f}%\n\n"
                f"💰 Total PnL: +${total_pnl:.2f} (+{total_pnl_pct:.1f}%)\n"
                f"🏆 Top Symbol: {top_symbol} → +${top_pnl:.2f} (+{top_pct:.1f}%)\n"
                f"🔄 Open Trades: {num_open}"
            )

            send_telegram(summary_msg)
            last_summary_time = current_time

@app.route('/')
def home():
    return "Bot is running!"

def run_bot():
    load_trades()
    send_telegram(f"BOT STARTED\nOpen trades: {len(open_trades)}")
    threading.Thread(target=scan_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=8080)

if __name__ == "__main__":
    run_bot()
