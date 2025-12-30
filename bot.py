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
BODY_SIZE_THRESHOLD = 0.1
SUMMARY_INTERVAL = 3600  # 1 hour
ADD_LEVELS = [(0.015, 5.0), (0.03, 10.0), (0.045, 0.0)]  # 2 DCA + final SL

# === PROXY LIST ===
PROXY_LIST = [
    {'host': '142.111.48.253', 'port': 7030, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '31.59.20.176',   'port': 6754, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '23.95.150.145',  'port': 6114, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '198.23.239.134', 'port': 6540, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '45.38.107.97',   'port': 6014, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '107.172.163.27', 'port': 6543, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '64.137.96.74',   'port': 6641, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '216.10.27.159',  'port': 6837, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '142.111.67.146', 'port': 5611, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '142.147.128.93', 'port': 6593, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
]

def get_proxy_config(proxy):
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

# === LOGGING & THREADING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
trade_lock = threading.Lock()

# === TIME ===
def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

# === PERSISTENCE ===
def save_trades():
    with trade_lock:
        try:
            with open(TRADE_FILE, 'w') as f:
                json.dump(open_trades, f, default=str)
        except Exception as e:
            logging.error(f"Save trades error: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                open_trades = json.load(f)
    except Exception as e:
        logging.error(f"Load trades error: {e}")
        open_trades = {}

def save_closed_trades(trade):
    try:
        trades = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                trades = json.load(f)
        trades.append(trade)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(trades, f, default=str)
    except Exception as e:
        logging.error(f"Save closed error: {e}")

def load_closed_trades():
    try:
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                return json.load(f)
        return []
    except Exception as e:
        logging.error(f"Load closed error: {e}")
        return []

# === TELEGRAM ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg}
    try:
        resp = requests.post(url, data=data, timeout=10, proxies=proxies if 'proxies' in globals() else None)
        return resp.json().get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram send error: {e}")
        return None

def edit_telegram_message(msg_id, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': msg_id, 'text': text}
    try:
        requests.post(url, data=data, timeout=10, proxies=proxies if 'proxies' in globals() else None)
    except Exception as e:
        logging.error(f"Telegram edit error: {e}")

# === EXCHANGE INIT ===
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def initialize_exchange():
    global proxies
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            session = requests.Session()
            retry = Retry(total=3, backoff_factor=1)
            session.mount('https://', HTTPAdapter(max_retries=retry))
            ex = ccxt.binance({
                'options': {'defaultType': 'future'},
                'proxies': proxies,
                'enableRateLimit': True,
                'session': session
            })
            ex.load_markets()
            logging.info(f"Connected via proxy {proxy['host']}")
            return ex
        except Exception as e:
            logging.warning(f"Proxy failed: {e}")
    # Fallback
    ex = ccxt.binance({'options': {'defaultType': 'future'}, 'enableRateLimit': True})
    ex.load_markets()
    proxies = None
    logging.info("Connected directly")
    return ex

app = Flask(__name__)
exchange = initialize_exchange()
sent_signals = {}
open_trades = {}
last_summary_time = 0

# === CANDLE & ANALYSIS ===
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100

def analyze_first_small_candle(candle, pattern_type):
    body = body_pct(candle)
    upper = (candle[2] - max(candle[1], candle[4])) / candle[1] * 100
    lower = (min(candle[1], candle[4]) - candle[3]) / candle[1] * 100
    u_ratio = upper / lower if lower else float('inf')
    l_ratio = lower / upper if upper else float('inf')

    if pattern_type == 'rising':
        if u_ratio >= 2.5 and body < 0.1:
            pressure = "Selling pressure"
        elif l_ratio >= 2.5 and body < 0.1:
            pressure = "Buying pressure"
        else:
            pressure = "Neutral"
    else:
        if l_ratio >= 2.5 and body < 0.1:
            pressure = "Buying pressure"
        elif u_ratio >= 2.5 and body < 0.1:
            pressure = "Selling pressure"
        else:
            pressure = "Neutral"

    wick_line = f"U: {upper:.2f}% L: {lower:.2f}% Body: {body:.2f}%"
    return {
        'main': pressure,
        'wick_body': wick_line,
        'status': pressure.lower().replace(' ', '_'),
        'body_pct': body
    }

def calculate_ema(candles, period):
    closes = [c[4] for c in candles]
    if len(closes) < period: return None
    mul = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for p in closes[period:]:
        ema = (p - ema) * mul + ema
    return ema

def round_price(symbol, price):
    try:
        info = exchange.market(symbol)['info']['filters']
        tick = float([f for f in info if f['filterType'] == 'PRICE_FILTER'][0]['tickSize'])
        prec = int(round(-math.log10(tick)))
        return round(price, prec)
    except:
        return price

# === PATTERNS ===
def detect_rising_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    if c2[4] <= c2[1]: return False
    if body_pct(c2) < MIN_BIG_BODY_PCT: return False
    if not (body_pct(c1) <= MAX_SMALL_BODY_PCT and body_pct(c0) <= MAX_SMALL_BODY_PCT): return False
    return True

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    if c2[4] >= c2[1]: return False
    if body_pct(c2) < MIN_BIG_BODY_PCT: return False
    if not (body_pct(c1) <= MAX_SMALL_BODY_PCT and body_pct(c0) <= MAX_SMALL_BODY_PCT): return False
    return True

def get_symbols():
    return [s for s in exchange.markets if 'USDT' in s and exchange.markets[s]['future'] and exchange.markets[s]['active']]

def get_next_candle_close():
    now = get_ist_time()
    secs = now.minute * 60 + now.second
    to_next = (15 * 60) - (secs % (15 * 60))
    if to_next < 10: to_next += 15 * 60
    return time.time() + to_next

# === MESSAGE BUILDER ===
def build_trade_message(sym, trade, hit=None, hit_price=None, profit=None, pnl_pct=None):
    side = 'BUY' if trade['side'] == 'buy' else 'SELL'
    ema_line = (
        f"{'Below' if trade['side'] == 'buy' else 'Above'} 21 ema - "
        f"{'✅' if trade['ema_status']['price_ema21'] == 'Green' else '⚠️'} "
        f"ema 9 {'below' if trade['side'] == 'buy' else 'above'} 21 - "
        f"{'✅' if trade['ema_status']['ema9_ema21'] == 'Green' else '⚠️'}"
    )
    pressure = trade['first_candle_analysis']['main']
    wicks = trade['first_candle_analysis']['wick_body']
    dca_added = 'None' if not trade.get('dca_messages') else ', '.join(trade['dca_messages'])

    lines = []
    for i, (pct, _) in enumerate(ADD_LEVELS):
        status = trade['dca_status'].get(i, "Pending")
        if i < 2:
            p = round_price(sym, trade['initial_entry'] * (1 - pct) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + pct))
            tp_p = round_price(sym, p * (1 + TP_PCT) if trade['side'] == 'buy' else p * (1 - TP_PCT))
            lines.append(f"DCA {i+1} {p} tp-{tp_p} ({status})")
        else:
            p = round_price(sym, trade['initial_entry'] * (1 - pct) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + pct))
            lines.append(f"SL {p} ({status})")

    msg = (
        f"{sym} - {side}\n"
        f"{ema_line}\n"
        f"First small candle: {pressure}\n"
        f"{wicks}\n"
        f"Initial entry: {trade['initial_entry']} Average entry: {trade['average_entry']} Total invested: ${trade['total_invested']:.2f}\n"
        f"{' '.join(lines)}\n"
        f"DCA Added: {dca_added}\n"
        f"TP: {trade['tp']} SL: {trade['sl']}"
    )
    if hit:
        msg += f"\nExit: {hit_price}\nProfit: {pnl_pct:.2f}% (${profit:.2f})\n{hit}"
    return msg

# === TP/SL & DCA CHECKER ===
def check_tp_sl_dca():
    while True:
        time.sleep(TP_CHECK_INTERVAL)
        with trade_lock:
            for sym, trade in list(open_trades.items()):
                try:
                    ticker = exchange.fetch_ticker(sym)
                    price = round_price(sym, ticker['last'])

                    # DCA Triggers
                    added = False
                    for i, (pct, amt) in enumerate(ADD_LEVELS[:2]):
                        if trade.get('adds_done', 0) > i: continue
                        trigger = trade['initial_entry'] * (1 - pct) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + pct)
                        if (trade['side'] == 'buy' and price <= trigger) or (trade['side'] == 'sell' and price >= trigger):
                            new_qty = trade['quantity'] + amt / price
                            new_avg = (trade['quantity'] * trade['average_entry'] + (amt / price) * price) / new_qty
                            new_tp = round_price(sym, new_avg * (1 + TP_PCT) if trade['side'] == 'buy' else new_avg * (1 - TP_PCT))
                            new_sl = round_price(sym, new_avg * (1 - SL_PCT) if trade['side'] == 'buy' else new_avg * (1 + SL_PCT))

                            trade.update({
                                'adds_done': i + 1,
                                'average_entry': new_avg,
                                'total_invested': trade['total_invested'] + amt,
                                'quantity': new_qty,
                                'tp': new_tp,
                                'sl': new_sl,
                                'dca_messages': trade.get('dca_messages', []) + [f"${amt:.1f} @ {price}"],
                                'dca_status': trade['dca_status'].copy()
                            })
                            trade['dca_status'][i] = "Added"
                            edit_telegram_message(trade['msg_id'], build_trade_message(sym, trade))
                            save_trades()
                            added = True
                            break
                    if added: continue

                    # Final SL (DCA3)
                    if trade.get('adds_done', 0) < 2:
                        pct = ADD_LEVELS[2][0]
                        trigger = trade['initial_entry'] * (1 - pct) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + pct)
                        if (trade['side'] == 'buy' and price <= trigger) or (trade['side'] == 'sell' and price >= trigger):
                            hit = "DCA3 SL hit"
                            hit_price = price
                            trade['dca_status'][2] = "SL Hit"
                        else:
                            hit = hit_price = None
                    else:
                        hit = hit_price = None

                    # TP / SL
                    if not hit:
                        if (trade['side'] == 'buy' and price >= trade['tp']) or (trade['side'] == 'sell' and price <= trade['tp']):
                            hit = "TP hit"
                            hit_price = price
                        elif (trade['side'] == 'buy' and price <= trade['sl']) or (trade['side'] == 'sell' and price >= trade['sl']):
                            hit = "SL hit"
                            hit_price = price

                    if hit:
                        pnl_pct_val = (hit_price - trade['average_entry']) / trade['average_entry'] * 100 if trade['side'] == 'buy' else (trade['average_entry'] - hit_price) / trade['average_entry'] * 100
                        leveraged = pnl_pct_val * LEVERAGE
                        profit = trade['total_invested'] * leveraged / 100

                        closed = {
                            'symbol': sym,
                            'pnl': profit,
                            'pnl_pct': leveraged,
                            'category': trade['category'],
                            'pressure_status': trade['pressure_status'],
                            'hit': hit,
                            'adds_done': trade.get('adds_done', 0)
                        }
                        save_closed_trades(closed)

                        final_msg = build_trade_message(sym, trade, hit, hit_price, profit, leveraged)
                        edit_telegram_message(trade['msg_id'], final_msg)
                        del open_trades[sym]
                        save_trades()

                except Exception as e:
                    logging.error(f"Check error {sym}: {e}")

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        candles = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=50)
        if len(candles) < 30: return

        ema21 = calculate_ema(candles, 21)
        ema9 = calculate_ema(candles, 9)
        if not ema21 or not ema9: return

        close_prev = candles[-3][4]
        entry_price = round_price(symbol, candles[-2][4])

        if detect_rising_three(candles):
            analysis = analyze_first_small_candle(candles[-3], 'rising')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'rising')) == candles[-2][0]: return
            sent_signals[(symbol, 'rising')] = candles[-2][0]

            price_ok = close_prev > ema21
            ema_ok = ema9 > ema21
            ema_status = {'price_ema21': 'Green' if price_ok else 'Caution', 'ema9_ema21': 'Green' if ema_ok else 'Caution'}
            category = 'two_green' if price_ok and ema_ok else 'one_green' if price_ok or ema_ok else 'two_cautions'

            side = 'sell'
            tp = round_price(symbol, entry_price * (1 - TP_PCT))
            sl = round_price(symbol, entry_price * (1 + SL_PCT))

            dca_status = {0: "Pending", 1: "Pending", 2: "Pending"}
            trade_data = {
                'side': side, 'initial_entry': entry_price, 'average_entry': entry_price,
                'total_invested': CAPITAL, 'tp': tp, 'sl': sl, 'ema_status': ema_status,
                'first_candle_analysis': analysis, 'pressure_status': analysis['status'],
                'category': category, 'dca_status': dca_status
            }
            msg = build_trade_message(symbol, trade_data)

            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, analysis, dca_status))

        elif detect_falling_three(candles):
            analysis = analyze_first_small_candle(candles[-3], 'falling')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'falling')) == candles[-2][0]: return
            sent_signals[(symbol, 'falling')] = candles[-2][0]

            price_ok = close_prev < ema21
            ema_ok = ema9 < ema21
            ema_status = {'price_ema21': 'Green' if price_ok else 'Caution', 'ema9_ema21': 'Green' if ema_ok else 'Caution'}
            category = 'two_green' if price_ok and ema_ok else 'one_green' if price_ok or ema_ok else 'two_cautions'

            side = 'buy'
            tp = round_price(symbol, entry_price * (1 + TP_PCT))
            sl = round_price(symbol, entry_price * (1 - SL_PCT))

            dca_status = {0: "Pending", 1: "Pending", 2: "Pending"}
            trade_data = {
                'side': side, 'initial_entry': entry_price, 'average_entry': entry_price,
                'total_invested': CAPITAL, 'tp': tp, 'sl': sl, 'ema_status': ema_status,
                'first_candle_analysis': analysis, 'pressure_status': analysis['status'],
                'category': category, 'dca_status': dca_status
            }
            msg = build_trade_message(symbol, trade_data)

            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, analysis, dca_status))

    except Exception as e:
        logging.error(f"Process {symbol} error: {e}")

# === SCAN LOOP ===
def scan_loop():
    global last_summary_time
    load_trades()
    symbols = get_symbols()
    chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
    chunks = [symbols[i:i+chunk_size] for i in range(0, len(symbols), chunk_size)]

    alert_queue = queue.Queue()

    def send_alerts():
        while True:
            try:
                symbol, msg, ema_status, category, side, entry_price, tp, analysis, dca_status = alert_queue.get(timeout=1)
                with trade_lock:
                    if len(open_trades) >= MAX_OPEN_TRADES:
                        lowest = min((CATEGORY_PRIORITY.get(t['category'], 0) for t in open_trades.values()), default=0)
                        if CATEGORY_PRIORITY.get(category, 0) <= lowest:
                            alert_queue.task_done()
                            continue
                        for s in list(open_trades):
                            if CATEGORY_PRIORITY.get(open_trades[s]['category'], 0) == lowest:
                                edit_telegram_message(open_trades[s]['msg_id'], f"{s} - Canceled for higher priority")
                                del open_trades[s]
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
                            'sl': round_price(symbol, entry_price * (1 - SL_PCT) if side == 'buy' else entry_price * (1 + SL_PCT)),
                            'msg': msg,
                            'msg_id': mid,
                            'ema_status': ema_status,
                            'category': category,
                            'first_candle_analysis': analysis,
                            'pressure_status': analysis['status'],
                            'adds_done': 0,
                            'dca_messages': [],
                            'dca_status': dca_status
                        }
                        save_trades()
                alert_queue.task_done()
            except queue.Empty:
                time.sleep(0.5)
            except Exception as e:
                logging.error(f"Send alerts error: {e}")

    threading.Thread(target=send_alerts, daemon=True).start()
    threading.Thread(target=check_tp_sl_dca, daemon=True).start()

    while True:
        time.sleep(max(0, get_next_candle_close() - time.time()))
        for chunk in chunks:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
                for sym in chunk:
                    exec.submit(process_symbol, sym, alert_queue)
            time.sleep(BATCH_DELAY)

        num_open = len(open_trades)
        now = time.time()
        if now - last_summary_time >= SUMMARY_INTERVAL:
            # === HOURLY SUMMARY ===
            closed = load_closed_trades()

            def metrics(trades):
                def calc(group):
                    if not group: return 0,0,0,0,0,0.0,0.0,0.0,(0,0,0,0)
                    wins = sum(1 for t in group if t.get('pnl',0) > 0)
                    tp = sum(1 for t in group if 'TP hit' in t.get('hit',''))
                    sl = sum(1 for t in group if 'SL hit' in t.get('hit','') or 'DCA3' in t.get('hit',''))
                    pnl = sum(t.get('pnl',0) for t in group)
                    pct = sum(t.get('pnl_pct',0) for t in group)
                    adds = [t.get('adds_done',0) for t in group]
                    dca = (adds.count(0), adds.count(1), adds.count(2), len(group) - sum(adds.count(i) for i in range(3)))
                    return len(group), wins, len(group)-wins, tp, sl, pnl, pct, wins/len(group)*100 if group else 0, dca

                neutral = [t for t in trades if t.get('pressure_status') == 'neutral']
                selling = [t for t in trades if t.get('pressure_status') == 'selling_pressure']
                buying = [t for t in trades if t.get('pressure_status') == 'buying_pressure']
                return {'neutral': calc(neutral), 'selling': calc(selling), 'buying': calc(buying)}

            tg = metrics([t for t in closed if t['category'] == 'two_green'])
            og = metrics([t for t in closed if t['category'] == 'one_green'])
            tc = metrics([t for t in closed if t['category'] == 'two_cautions'])

            total_pnl = sum(t.get('pnl',0) for t in closed)
            total_pct = sum(t.get('pnl_pct',0) for t in closed)

            top_sym = "None"
            top_val = 0
            if closed:
                sym_pnl = {}
                for t in closed:
                    sym_pnl[t['symbol']] = sym_pnl.get(t['symbol'],0) + t.get('pnl',0)
                if sym_pnl:
                    top_sym = max(sym_pnl, key=sym_pnl.get)
                    top_val = sym_pnl[top_sym]

            ts = get_ist_time().strftime("%I:%M %p IST, %B %d, %Y")

            def sub(name, data):
                c,w,l,tp,sl,pnl,pct,wr,dca = data
                no,d1,d2,d3 = dca
                return (
                    f" • {name}: {c} trade{'s' if c>1 else ''} (W:{w} L:{l} | TP:{tp} SL:{sl})\n"
                    f"   - No DCA: {no} | DCA1: {d1} | DCA2: {d2} | DCA3/SL: {d3}\n"
                    f"   → +${pnl:.2f} (+{pct:.1f}%)"
                )

            summary = (
                f"🔍 Hourly Summary at {ts}\n\n"
                f"✅✅ Two Green Ticks ({tg['neutral'][0]+tg['selling'][0]+tg['buying'][0]} trades)\n"
                f"{sub('Neutral', tg['neutral'])}\n"
                f"{sub('Selling Pressure', tg['selling'])}\n"
                f"{sub('Buying Pressure', tg['buying'])}\n\n"
                f"✅⚠️ One Green One Caution ({og['neutral'][0]+og['selling'][0]+og['buying'][0]} trades)\n"
                f"{sub('Neutral', og['neutral'])}\n"
                f"{sub('Selling Pressure', og['selling'])}\n"
                f"{sub('Buying Pressure', og['buying'])}\n\n"
                f"⚠️⚠️ Two Cautions ({tc['neutral'][0]+tc['selling'][0]+tc['buying'][0]} trades)\n"
                f"{sub('Neutral', tc['neutral'])}\n"
                f"{sub('Selling Pressure', tc['selling'])}\n"
                f"{sub('Buying Pressure', tc['buying'])}\n\n"
                f"💰 Total PnL: +${total_pnl:.2f} (+{total_pct:.1f}%)\n"
                f"🏆 Top Symbol: {top_sym} → +${top_val:.2f}\n"
                f"🔄 Open Trades: {num_open}"
            )
            send_telegram(summary)
            last_summary_time = now

@app.route('/')
def home():
    return "Bot is running!"

def run_bot():
    load_trades()
    send_telegram(f"BOT STARTED\nOpen trades: {len(open_trades)}")
    threading.Thread(target=scan_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))

if __name__ == "__main__":
    run_bot()
