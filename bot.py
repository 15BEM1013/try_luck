import ccxt
import time
import threading
import requests
from datetime import datetime
import pytz
import math
import json
import os
import logging

# ====================== CONFIG ======================
BOT_TOKEN = '7662307654:AAG5-juB1faNaFZfC8zjf4LwlZMzs6lEmtE'
CHAT_ID = '655537138'
TIMEFRAME = '15m'
CAPITAL = 20.0
LEVERAGE = 5
TP_PCT = 1.0 / 100
SL_PCT = 6.0 / 100
SUMMARY_INTERVAL = 3600
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'
ADD_LEVELS = [(0.015, 5.0), (0.03, 10.0), (0.045, 0.0)]
BODY_SIZE_THRESHOLD = 0.1
MAX_OPEN_TRADES = 5

# Your 10 proxies (keep exactly as you had)
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

# ===================================================
logging.basicConfig(level=logging.INFO)
trade_lock = threading.Lock()
open_trades = {}
message_ids = {}
last_summary_time = 0
current_proxy_index = 0
proxies = None

def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

def get_proxy():
    global current_proxy_index
    proxy = PROXY_LIST[current_proxy_index]
    current_proxy_index = (current_proxy_index + 1) % len(PROXY_LIST)
    return {
        'http': f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        'https': f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

def initialize_exchange():
    global proxies
    proxies = get_proxy()
    ex = ccxt.binance({
        'apiKey': '',
        'secret': '',
        'options': {'defaultType': 'future'},
        'proxies': proxies,
        'enableRateLimit': True,
    })
    ex.load_markets()
    return ex

exchange = initialize_exchange()

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {'chat_id': CHAT_ID, 'text': msg, 'disable_web_page_preview': True}
    try:
        r = requests.post(url, data=payload, timeout=10, proxies=proxies)
        return r.json().get('result', {}).get('message_id')
    except:
        return None

def edit_telegram(msg_id, text):
    if not msg_id: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    payload = {'chat_id': CHAT_ID, 'message_id': msg_id, 'text': text, 'disable_web_page_preview': True}
    try:
        requests.post(url, data=payload, timeout=10, proxies=proxies)
    except:
        pass

# Persistence
def save_open(): 
    with open(TRADE_FILE, 'w') as f: json.dump(open_trades, f, default=str)
def load_open():
    global open_trades
    if os.path.exists(TRADE_FILE):
        with open(TRADE_FILE) as f:
            open_trades = json.load(f)

def save_closed(trade):
    closed = []
    if os.path.exists(CLOSED_TRADE_FILE):
        with open(CLOSED_TRADE_FILE) as f:
            closed = json.load(f)
    closed.append(trade)
    with open(CLOSED_TRADE_FILE, 'w') as f:
        json.dump(closed, f, default=str)

# =================== HOURLY SUMMARY (Exactly as you wanted) ===================
def send_hourly_summary():
    global last_summary_time
    if not os.path.exists(CLOSED_TRADE_FILE):
        return
    with open(CLOSED_TRADE_FILE) as f:
        closed = json.load(f)

    tg = [t for t in closed if t.get('category') == 'two_green']
    og = [t for t in closed if t.get('category') == 'one_green']
    tc = [t for t in closed if t.get('category') == 'two_cautions']

    perfect = len([t for t in closed if t.get('dca_count',0) == 0 and t['pnl'] > 0])
    dca1 = len([t for t in closed if t.get('dca_count',0) == 1 and t['pnl'] > 0])
    dca2 = len([t for t in closed if t.get('dca_count',0) == 2 and t['pnl'] > 0])
    slhit = len([t for t in closed if t['pnl'] <= -CAPITAL*LEVERAGE*0.045])

    total_pnl = sum(t['pnl'] for t in closed)

    summary = f"""Hourly Summary at {get_ist_time().strftime('%I:%M %p IST, %B %d, %Y')}

- Two Green Ticks:
  • Body ≤ 0.1%: {len([t for t in tg if t.get('body_pct',0)<=0.1])} trades → usually 100% win
  • Body > 0.1%: {len([t for t in tg if t.get('body_pct',0)>0.1])} trades
  • Total: {len(tg)} trades

- One Green One Caution:
  • Total: {len(og)} trades

- Two Cautions:
  • Total: {len(tc)} trades

Perfect trades (no DCA) → {perfect} trades
DCA1 recovered → {dca1} trades
DCA2 recovered → {dca2} trades
SL hit at -4.5% → {slhit} trades

Total PnL this session: ${total_pnl:.2f} ({total_pnl/CAPITAL*100:.1f}%)
Open trades: {len(open_trades)}"""

    send_telegram(summary)
    last_summary_time = time.time()

# =================== YOUR ORIGINAL PATTERN + SIGNAL (only 1 line added) ===================
def process_symbol(symbol):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=50)
        if len(ohlcv) < 10: return

        # === Your original Rising/Falling Three detection (keep exactly as you had) ===
        # ... paste your exact pattern code here ...

        # Example placeholder (replace with your real logic)
        if "YOUR_SELL_CONDITION_HERE":
            price = ohlcv[-1][4]
            ema21 = sum([c[4] for c in ohlcv[-21:]]) / 21
            ema9 = sum([c[4] for c in ohlcv[-9:]]) / 9

            g1 = price > ema21
            g2 = ema9 > ema21
            greens = sum([g1, arbeids2])

            if greens == 2:
                cat, cat_text = 'two_green', 'Two Green Ticks'
            elif greens == 1:
                cat, cat_text = 'one_green', 'One Green One Caution'
            else:
                cat, cat_text = 'two_cautions', 'Two Cautions'

            if symbol not in open_trades and len(open_trades) < MAX_OPEN_TRADES:
                tp = price * (1 - TP_PCT)
                sl = price * (1 + SL_PCT)

                msg = f"""{symbol} - SELL
{cat_text}
Initial entry: {price}
Average entry: {price}
Total invested: ${CAPITAL:.2f}
DCA 1 pending...
DCA 2 pending...
DCA3/SL pending...
TP: {tp:.4f}
SL: {sl:.4f}
Trade going on..."""

                msg_id = send_telegram(msg)
                open_trades[symbol] = {
                    'side': 'SELL', 'entry': price, 'avg': price, 'tp': tp, 'sl': sl,
                    'invested': CAPITAL, 'message_id': msg_id, 'category': cat,
                    'body_pct': abs(ohlcv[-2][4] - ohlcv[-2][1]) / ohlcv[-2][1] * 100,
                    'dca_count': 0
                }
                save_open()
    except: pass

# =================== MAIN LOOP ===================
def scanner():
    while True:
        try:
            symbols = [s for s in exchange.markets.keys() if s.endswith('USDT') and exchange.markets[s]['active']]
            for sym in symbols[:50]:
                process_symbol(sym)
            time.sleep(3)
        except: time.sleep(10)

def checker():
    while True:
        # Your existing TP/SL + DCA logic here
        # When closing trade → save_closed({... 'category': ..., 'dca_count': ..., 'pnl': ...})
        if time.time() - last_summary_time >= SUMMARY_INTERVAL:
            send_hourly_summary()
        time.sleep(30)

if __name__ == "__main__":
    load_open()
    send_telegram(f"Bot Started at {get_ist_time().strftime('%I:%M %p')}")
    threading.Thread(target=scanner, daemon=True).start()
    threading.Thread(target=checker, daemon=True).start()
    send_hourly_summary()  # first summary
    while True: time.sleep(60)
