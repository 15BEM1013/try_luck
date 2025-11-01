import ccxt.pro as ccxt  # Use pro for WS
import asyncio
import time
import threading
import requests
from flask import Flask
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor
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
    {'host': '142.111.48.253', 'port': 7030, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '31.59.20.176', 'port': 6754, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '23.95.150.145', 'port': 6114, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '198.23.239.134', 'port': 6540, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '45.38.107.97', 'port': 6014, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '107.172.163.27', 'port': 6543, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '64.137.96.74', 'port': 6641, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '216.10.27.159', 'port': 6837, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '142.111.67.146', 'port': 5611, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '142.147.128.93', 'port': 6593, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
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
# === CREATE EXCHANGE INSTANCES WITH PROXY ROTATION ===
async def create_exchanges():
    exchanges = []
    for proxy in PROXY_LIST:
        try:
            proxies_config = get_proxy_config(proxy)
            logging.info(f"Creating exchange for proxy: {proxy['host']}:{proxy['port']}")
            exchange = ccxt.binance({
                'options': {'defaultType': 'future'},
                'proxies': proxies_config,
                'enableRateLimit': True,
                'asyncio_loop': asyncio.get_event_loop(),
                'rateLimit': 1200,  # Increase rate limit wait
            })
            await exchange.load_markets()
            exchanges.append((exchange, proxies_config))
            logging.info(f"Successfully created exchange for proxy: {proxy['host']}:{proxy['port']}")
        except Exception as e:
            logging.error(f"Failed to create exchange with proxy {proxy['host']}:{proxy['port']}: {e}")
            continue
    if not exchanges:
        logging.error("No proxies worked. Trying direct connection.")
        try:
            exchange = ccxt.binance({
                'options': {'defaultType': 'future'},
                'enableRateLimit': True,
                'asyncio_loop': asyncio.get_event_loop(),
                'rateLimit': 1200,
            })
            await exchange.load_markets()
            exchanges.append((exchange, None))
        except Exception as e:
            logging.error(f"Direct connection failed: {e}")
            raise Exception("All connections failed.")
    return exchanges

# Global exchanges list
exchanges = None
proxies = None  # For Telegram, use first proxy
app = Flask(__name__)
sent_signals = {}
open_trades = {}
closed_trades = []
last_summary_time = 0
exchange = None  # Main exchange for WS, fallback to first
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
def round_price(markets, symbol, price):
    try:
        market = markets[symbol]
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
async def get_symbols(exch):
    markets = await exch.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active') and markets[s].get('info', {}).get('status') == 'TRADING']
# === ASYNC TP/SL/DCA CHECK ===
async def check_tp_single(exch, markets, sym, ticker):
    global closed_trades
    try:
        current_price = round_price(markets, sym, ticker['last'])
        if not current_price:
            return
        hit = ""
        pnl = 0
        hit_price = None
        dca_messages = open_trades[sym].get('dca_messages', [])
        initial_entry = open_trades[sym]['initial_entry']
        adds_done = open_trades[sym].get('adds_done', 0)
        total_invested = open_trades[sym].get('total_invested', CAPITAL)
        average_entry = open_trades[sym].get('average_entry', initial_entry)
        quantity = open_trades[sym].get('quantity', total_invested / initial_entry)
        for i, (against_pct, add_amount) in enumerate(ADD_LEVELS[:2]):
            if adds_done > i:
                continue
            dca_triggered = False
            add_price = None
            if open_trades[sym]['side'] == 'buy' and current_price <= initial_entry * (1 - against_pct):
                add_price = current_price
                dca_triggered = True
                dca_message = f"${add_amount:.1f} @ {add_price}"
            elif open_trades[sym]['side'] == 'sell' and current_price >= initial_entry * (1 + against_pct):
                add_price = current_price
                dca_triggered = True
                dca_message = f"${add_amount:.1f} @ {add_price}"
            if dca_triggered:
                add_quantity = add_amount / add_price
                total_quantity = quantity + add_quantity
                total_invested += add_amount
                average_entry = (quantity * average_entry + add_quantity * add_price) / total_quantity
                new_tp = round_price(markets, sym, average_entry * (1 + TP_PCT) if open_trades[sym]['side'] == 'buy' else average_entry * (1 - TP_PCT))
                new_sl = round_price(markets, sym, average_entry * (1 - SL_PCT) if open_trades[sym]['side'] == 'buy' else average_entry * (1 + SL_PCT))
                open_trades[sym]['adds_done'] = i + 1
                open_trades[sym]['total_invested'] = total_invested
                open_trades[sym]['average_entry'] = round_price(markets, sym, average_entry)
                open_trades[sym]['quantity'] = total_quantity
                open_trades[sym]['tp'] = new_tp
                open_trades[sym]['sl'] = new_sl
                open_trades[sym]['last_update_time'] = int(time.time() * 1000)
                open_trades[sym]['dca_status'][i] = "Added"
                dca_messages.append(dca_message)
                open_trades[sym]['dca_messages'] = dca_messages
                logging.info(f"Added ${add_amount} to {sym} at {add_price}, new avg entry: {average_entry}, new TP: {new_tp}, new SL: {new_sl}, total invested: {total_invested}")
                dca_lines = []
                for j, (against_pct_j, _) in enumerate(ADD_LEVELS):
                    if j < 2:
                        dca_price = round_price(markets, sym, initial_entry * (1 - against_pct_j) if open_trades[sym]['side'] == 'buy' else initial_entry * (1 + against_pct_j))
                        dca_tp = round_price(markets, sym, dca_price * (1 + TP_PCT) if open_trades[sym]['side'] == 'buy' else dca_price * (1 - TP_PCT))
                        dca_lines.append(f"DCA {j+1} {dca_price} tp-{dca_tp} ({open_trades[sym]['dca_status'][j]})")
                    else:
                        dca_price = round_price(markets, sym, initial_entry * (1 - against_pct_j) if open_trades[sym]['side'] == 'buy' else initial_entry * (1 + against_pct_j))
                        dca_lines.append(f"DCA3/SL {dca_price} ({open_trades[sym]['dca_status'][j]})")
                new_msg = (
                    f"{sym} - {'BUY' if open_trades[sym]['side'] == 'buy' else 'SELL'}\n"
                    f"Initial entry: {open_trades[sym]['initial_entry']}\n"
                    f"Average entry: {open_trades[sym]['average_entry']}\n"
                    f"Total invested: ${open_trades[sym]['total_invested']:.2f}\n"
                    f"{'\n'.join(dca_lines)}\n"
                    f"DCA Added: {', '.join(dca_messages)}\n"
                    f"TP: {open_trades[sym]['tp']}\n"
                    f"SL: {open_trades[sym]['sl']}"
                )
                open_trades[sym]['msg'] = new_msg
                edit_telegram_message(open_trades[sym]['msg_id'], new_msg)
                save_trades()
                return  # Break after DCA to avoid multiple in one check
        if adds_done < 2:
            against_pct, _ = ADD_LEVELS[2]
            dca3_sl_triggered = False
            dca3_price = None
            if open_trades[sym]['side'] == 'buy' and current_price <= initial_entry * (1 - against_pct):
                dca3_price = current_price
                dca3_sl_triggered = True
            elif open_trades[sym]['side'] == 'sell' and current_price >= initial_entry * (1 + against_pct):
                dca3_price = current_price
                dca3_sl_triggered = True
            if dca3_sl_triggered:
                hit = "DCA3 SL hit"
                hit_price = dca3_price
                open_trades[sym]['dca_status'][2] = "SL Hit"
                logging.info(f"DCA3 SL triggered for {sym} at {dca3_price}")
        if not hit and current_price:
            if open_trades[sym]['side'] == 'buy':
                if current_price >= open_trades[sym]['tp']:
                    hit = "TP hit"
                    hit_price = current_price
            else:
                if current_price <= open_trades[sym]['tp']:
                    hit = "TP hit"
                    hit_price = current_price
        if not hit and current_price:
            if open_trades[sym]['side'] == 'buy':
                if current_price <= open_trades[sym]['sl']:
                    hit = "SL hit"
                    hit_price = current_price
            else:
                if current_price >= open_trades[sym]['sl']:
                    hit = "SL hit"
                    hit_price = current_price
        if hit:
            total_quantity = open_trades[sym].get('quantity', total_invested / initial_entry)
            if open_trades[sym]['side'] == 'buy':
                pnl = (hit_price - open_trades[sym]['average_entry']) / open_trades[sym]['average_entry'] * 100
            else:
                pnl = (open_trades[sym]['average_entry'] - hit_price) / open_trades[sym]['average_entry'] * 100
            leveraged_pnl_pct = pnl * LEVERAGE
            profit = open_trades[sym]['total_invested'] * leveraged_pnl_pct / 100
            logging.info(f"{hit} for {sym}: {hit}, Leveraged PnL: {leveraged_pnl_pct:.2f}% at price {hit_price}")
            closed_trade = {
                'symbol': sym,
                'pnl': profit,
                'pnl_pct': leveraged_pnl_pct,
                'category': open_trades[sym]['category'],
                'ema_status': open_trades[sym]['ema_status'],
                'pressure_status': open_trades[sym]['pressure_status'],
                'hit': hit,
                'body_pct': open_trades[sym]['body_pct'],
                'adds_done': open_trades[sym]['adds_done'],
                'total_invested': open_trades[sym]['total_invested'],
                'dca_messages': open_trades[sym].get('dca_messages', [])
            }
            closed_trades.append(closed_trade)
            save_closed_trades(closed_trade)
            dca_lines = []
            for j, (against_pct_j, _) in enumerate(ADD_LEVELS):
                if j < 2:
                    dca_price = round_price(markets, sym, open_trades[sym]['initial_entry'] * (1 - against_pct_j) if open_trades[sym]['side'] == 'buy' else open_trades[sym]['initial_entry'] * (1 + against_pct_j))
                    dca_tp = round_price(markets, sym, dca_price * (1 + TP_PCT) if open_trades[sym]['side'] == 'buy' else dca_price * (1 - TP_PCT))
                    dca_lines.append(f"DCA {j+1} {dca_price} tp-{dca_tp} ({open_trades[sym]['dca_status'][j]})")
                else:
                    dca_price = round_price(markets, sym, open_trades[sym]['initial_entry'] * (1 - against_pct_j) if open_trades[sym]['side'] == 'buy' else open_trades[sym]['initial_entry'] * (1 + against_pct_j))
                    dca_lines.append(f"DCA3/SL {dca_price} ({open_trades[sym]['dca_status'][j]})")
            new_msg = (
                f"{sym} - {'BUY' if open_trades[sym]['side'] == 'buy' else 'SELL'}\n"
                f"Initial entry: {open_trades[sym]['initial_entry']}\n"
                f"Average entry: {open_trades[sym]['average_entry']}\n"
                f"Total invested: ${open_trades[sym]['total_invested']:.2f}\n"
                f"{'\n'.join(dca_lines)}\n"
                f"DCA Added: {', '.join(dca_messages) if dca_messages else 'None'}\n"
                f"TP: {open_trades[sym]['tp']}\n"
                f"SL: {open_trades[sym]['sl']}\n"
                f"Exit: {hit_price}\n"
                f"Profit: {leveraged_pnl_pct:.2f}% (${profit:.2f})"
            )
            open_trades[sym]['msg'] = new_msg
            open_trades[sym]['hit'] = hit
            edit_telegram_message(open_trades[sym]['msg_id'], new_msg)
            del open_trades[sym]
            save_trades()
            logging.info(f"Trade closed for {sym}")
    except Exception as e:
        logging.error(f"TP/SL/DCA check error on {sym}: {e}")
async def watch_tp_sl_dca(main_exch, markets):
    global closed_trades
    ticker_tasks = {}
    while True:
        try:
            with trade_lock:
                # Unsubscribe closed trades
                for sym in list(ticker_tasks):
                    if sym not in open_trades:
                        ticker_tasks[sym].cancel()
                        del ticker_tasks[sym]
                # Subscribe new open trades
                for sym in open_trades:
                    if sym not in ticker_tasks:
                        task = asyncio.create_task(watch_single_ticker(main_exch, sym, markets))
                        ticker_tasks[sym] = task
            await asyncio.sleep(TP_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"TP/SL/DCA loop error: {e}")
            await asyncio.sleep(5)
async def watch_single_ticker(main_exch, sym, markets):
    while sym in open_trades:
        try:
            ticker = await main_exch.watch_ticker(sym)
            await check_tp_single(main_exch, markets, sym, ticker)
            if sym not in open_trades:
                break
        except Exception as e:
            logging.error(f"Ticker WS error for {sym}: {e}")
            await asyncio.sleep(1)
# === PROCESS SYMBOL WS ===
async def process_symbol_ws(markets, symbol, historical_candles, alert_queue):
    try:
        if len(historical_candles) < 25:
            return
        ema21 = calculate_ema(historical_candles, period=21)
        ema9 = calculate_ema(historical_candles, period=9)
        rsi = calculate_rsi(historical_candles, period=RSI_PERIOD)
        if ema21 is None or ema9 is None or rsi is None:
            return
        signal_time = historical_candles[-2][0]
        first_small_candle_close = round_price(markets, symbol, historical_candles[-3][4])
        second_small_candle_close = round_price(markets, symbol, historical_candles[-2][4])
        if detect_rising_three(historical_candles):
            first_candle_analysis = analyze_first_small_candle(historical_candles[-3], 'rising')
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
            elif green_count == 1:
                category = 'one_green'
            else:
                category = 'two_cautions'
            side = 'sell'
            entry_price = second_small_candle_close
            tp = round_price(markets, symbol, entry_price * (1 - TP_PCT))
            sl = round_price(markets, symbol, entry_price * (1 + SL_PCT))
            pattern = 'rising'
            dca_lines = []
            dca_status = {0: "Pending", 1: "Pending", 2: "Pending"}
            for i, (against_pct, _) in enumerate(ADD_LEVELS):
                if i < 2:
                    dca_price = round_price(markets, symbol, entry_price * (1 + against_pct))
                    dca_tp = round_price(markets, symbol, dca_price * (1 - TP_PCT))
                    dca_lines.append(f"DCA {i+1} {dca_price} tp-{dca_tp} (Pending)")
                else:
                    dca_price = round_price(markets, symbol, entry_price * (1 + against_pct))
                    dca_lines.append(f"DCA3/SL {dca_price} (Pending)")
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
            await alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], pattern, dca_status, sl))
        elif detect_falling_three(historical_candles):
            first_candle_analysis = analyze_first_small_candle(historical_candles[-3], 'falling')
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
            elif green_count == 1:
                category = 'one_green'
            else:
                category = 'two_cautions'
            side = 'buy'
            entry_price = second_small_candle_close
            tp = round_price(markets, symbol, entry_price * (1 + TP_PCT))
            sl = round_price(markets, symbol, entry_price * (1 - SL_PCT))
            pattern = 'falling'
            dca_lines = []
            dca_status = {0: "Pending", 1: "Pending", 2: "Pending"}
            for i, (against_pct, _) in enumerate(ADD_LEVELS):
                if i < 2:
                    dca_price = round_price(markets, symbol, entry_price * (1 - against_pct))
                    dca_tp = round_price(markets, symbol, dca_price * (1 + TP_PCT))
                    dca_lines.append(f"DCA {i+1} {dca_price} tp-{dca_tp} (Pending)")
                else:
                    dca_price = round_price(markets, symbol, entry_price * (1 - against_pct))
                    dca_lines.append(f"DCA3/SL {dca_price} (Pending)")
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
            await alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], pattern, dca_status, sl))
    except Exception as e:
        logging.error(f"WS process error on {symbol}: {e}")
# === WS KLINE STREAMING ===
async def stream_klines_batch(main_exch, markets, symbols_batch, alert_queue, historical_cache):
    tasks = []
    for symbol in symbols_batch:
        task = asyncio.create_task(watch_single_kline(main_exch, markets, symbol, alert_queue, historical_cache))
        tasks.append(task)
    await asyncio.gather(*tasks, return_exceptions=True)
async def watch_single_kline(main_exch, markets, symbol, alert_queue, historical_cache):
    while True:
        try:
            ohlcv = await main_exch.watch_ohlcv(symbol, '15m')
            if ohlcv and len(ohlcv) > 0:
                latest_candle = ohlcv[-1]
                if len(latest_candle) >= 7 and latest_candle[6]:  # Check length and is_closed
                    if symbol not in historical_cache or len(historical_cache[symbol]) < 25:
                        # Fallback fetch if not loaded or insufficient
                        try:
                            candles = await main_exch.fetch_ohlcv(symbol, '15m', limit=30)
                            if len(candles) >= 25:
                                historical_cache[symbol] = candles
                        except Exception as e:
                            logging.warning(f"Failed fallback fetch for {symbol}: {e}")
                            await asyncio.sleep(60)  # Wait 1 min before retry
                            continue
                    historical = historical_cache.get(symbol, [])
                    if len(historical) > 0 and historical[-1][0] < latest_candle[0]:
                        historical.append(latest_candle)
                        historical = historical[-30:]
                        historical_cache[symbol] = historical
                        await process_symbol_ws(markets, symbol, historical, alert_queue)
        except ccxt.NetworkError:
            logging.error(f"WS kline disconnect for {symbol}, reconnecting...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"Kline WS error for {symbol}: {e}")
            await asyncio.sleep(1)
# === MAIN ASYNC SCAN LOOP ===
async def scan_loop_async(exchanges_list, main_exch, main_markets):
    global closed_trades, last_summary_time
    load_trades()
    # Get symbols from main exchange
    symbols = await get_symbols(main_exch)
    # Limit to top 100 by volume or something, but for now, all, but load slowly
    print(f"Scanning {len(symbols)} Binance Futures symbols via WS...")

    # Initial historical load with proxy rotation and delays
    historical_cache = {}
    # Distribute symbols across exchanges
    symbols_per_exch = math.ceil(len(symbols) / len(exchanges_list))
    for idx, (exch, _) in enumerate(exchanges_list):
        batch_symbols = symbols[idx * symbols_per_exch : (idx + 1) * symbols_per_exch]
        if batch_symbols:
            await load_historical_batch(exch, batch_symbols, historical_cache)

    alert_queue = asyncio.Queue()

    # Batch symbols for WS (20 per batch to reduce load)
    batch_size = 20
    symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
    stream_tasks = [stream_klines_batch(main_exch, main_markets, batch, alert_queue, historical_cache) for batch in symbol_batches]

    # Async alert sender
    async def send_alerts_async():
        while True:
            try:
                item = await alert_queue.get()
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
            except Exception as e:
                logging.error(f"Alert thread error: {e}")
                await asyncio.sleep(1)

    # Start tasks
    asyncio.create_task(send_alerts_async())
    asyncio.create_task(watch_tp_sl_dca(main_exch, main_markets))
    await asyncio.gather(*stream_tasks)  # Run forever

    # Summary logic
    current_time = time.time()
    if current_time - last_summary_time >= SUMMARY_INTERVAL:
        all_closed_trades = load_closed_trades()
        # Add your summary logic here if needed
        last_summary_time = current_time
        closed_trades = []

async def load_historical_batch(exch, batch_symbols, historical_cache):
    """Load historical for a batch with delays"""
    semaphore = asyncio.Semaphore(2)  # Very low concurrency
    async def load_single(s):
        async with semaphore:
            for attempt in range(3):
                try:
                    candles = await exch.fetch_ohlcv(s, '15m', limit=30)
                    if len(candles) >= 25:
                        historical_cache[s] = candles
                        logging.info(f"Loaded historical for {s}")
                    break
                except ccxt.RateLimitExceeded:
                    wait_time = 2 ** attempt * 10  # Exponential backoff
                    logging.warning(f"Rate limit for {s}, waiting {wait_time}s")
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    logging.error(f"Load error for {s}: {e}")
                    break
            await asyncio.sleep(1)  # Delay between fetches

    tasks = [load_single(s) for s in batch_symbols]
    await asyncio.gather(*tasks, return_exceptions=True)

# === FLASK ===
@app.route('/')
def home():
    return "Rising & Falling Three Pattern Bot is Live! (WS Mode)"
# === RUN ===
async def run_bot_async():
    global exchanges, proxies, exchange, last_summary_time
    load_trades()
    num_open = len(open_trades)
    last_summary_time = time.time()
    startup_msg = f"BOT STARTED (WS Mode)\nNumber of open trades: {num_open}"
    send_telegram(startup_msg)
    try:
        exchanges_list = await create_exchanges()
        # Use first for main WS and Telegram
        exchange, proxies = exchanges_list[0]
        main_markets = await exchange.load_markets()
        await scan_loop_async(exchanges_list, exchange, main_markets)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        for exch, _ in exchanges_list:
            await exch.close()

def run_bot():
    # Run async bot in thread, Flask in main
    bot_thread = threading.Thread(target=lambda: asyncio.run(run_bot_async()))
    bot_thread.start()
    app.run(host='0.0.0.0', port=8080)

if __name__ == "__main__":
    run_bot()
