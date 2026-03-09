import ccxt.async_support as ccxt
import asyncio
import aiohttp
import time
import json
import os
import logging
from dotenv import load_dotenv
from datetime import datetime
import pytz
import math

# Load .env only if it exists (local dev)
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    logging.info("Loaded local .env file")
else:
    logging.info("No local .env found – using system environment variables")

# === CONFIG ===
BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID          = os.getenv("TELEGRAM_CHAT_ID")
TIMEFRAME        = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0
BATCH_DELAY      = 2.0
NUM_CHUNKS       = 8

CAPITAL_INITIAL   = 10.0
CAPITAL_DCA       = 20.0
MAX_MARGIN_PER_TRADE = 30.0

LEVERAGE         = 5
TP_INITIAL_PCT   = 1.0 / 100
TP_AFTER_DCA_PCT = 0.5 / 100
DCA_TRIGGER_PCT  = 2.0 / 100

TP_CHECK_INTERVAL = 8

MAX_OPEN_TRADES  = 5
TRADE_FILE       = '/app/data/open_trades.json'        # Render persistent disk
CLOSED_TRADE_FILE= '/app/data/closed_trades.json'

API_KEY    = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET')
if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set in environment variables")

PROXY_LIST = []  # ← Add real proxies here later (highly recommended)

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

trade_lock = asyncio.Lock()

def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

# === TRADE PERSISTENCE ===
def save_trades():
    try:
        with open(TRADE_FILE, 'w') as f:
            json.dump(open_trades, f, default=str)
        logging.info(f"Trades saved ({len(open_trades)} open)")
    except Exception as e:
        logging.error(f"Save trades error: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                open_trades = json.load(f)
            logging.info(f"Loaded {len(open_trades)} open trades")
        else:
            open_trades = {}
    except Exception as e:
        logging.error(f"Load trades error: {e}")
        open_trades = {}

def save_closed_trade(closed):
    try:
        closed_list = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                closed_list = json.load(f)
        closed_list.append(closed)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(closed_list, f, default=str)
    except Exception as e:
        logging.error(f"Save closed trade error: {e}")

# === TELEGRAM ===
async def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data={
                'chat_id': CHAT_ID,
                'text': msg,
                'parse_mode': 'Markdown'
            }, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                r = await resp.json()
                return r.get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram send error: {e}")
        return None

async def edit_telegram_message(mid, new_text):
    if not mid:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data={
                'chat_id': CHAT_ID,
                'message_id': mid,
                'text': new_text,
                'parse_mode': 'Markdown'
            }, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                await resp.release()
    except Exception as e:
        logging.error(f"Telegram edit error: {e}")

# === EXCHANGE ===
async def initialize_exchange():
    max_retries = 5
    base_delay = 30  # seconds

    for attempt in range(max_retries):
        # Try proxies first
        for proxy in PROXY_LIST:
            try:
                proxies = {
                    'http': f"http://{proxy.get('username')}:{proxy.get('password')}@{proxy['host']}:{proxy['port']}",
                    'https': f"http://{proxy.get('username')}:{proxy.get('password')}@{proxy['host']}:{proxy['port']}"
                }
                ex = ccxt.binance({
                    'apiKey': API_KEY, 'secret': API_SECRET,
                    'options': {'defaultType': 'future', 'marginMode': 'isolated'},
                    'proxies': proxies,
                    'enableRateLimit': True,
                    'rateLimit': 500,  # conservative
                })
                await ex.load_markets()
                logging.info(f"Connected via proxy {proxy.get('host')}")
                return ex
            except ccxt.DDoSProtection as e:
                wait = base_delay * (2 ** attempt)
                logging.warning(f"Binance ban (proxy): {e}. Waiting {wait}s...")
                await asyncio.sleep(wait)
            except Exception as e:
                logging.warning(f"Proxy {proxy.get('host')} failed: {e}")

        # Fallback: direct
        try:
            ex = ccxt.binance({
                'apiKey': API_KEY, 'secret': API_SECRET,
                'options': {'defaultType': 'future', 'marginMode': 'isolated'},
                'enableRateLimit': True,
                'rateLimit': 500,
            })
            await ex.load_markets()
            logging.info("Connected directly")
            return ex
        except ccxt.DDoSProtection as e:
            wait = base_delay * (2 ** attempt)
            logging.warning(f"Binance ban (direct): {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)
        except Exception as e:
            logging.warning(f"Direct connection failed (attempt {attempt+1}): {e}")
            await asyncio.sleep(base_delay * (attempt + 1))

    raise RuntimeError("Failed to initialize exchange after all retries")

exchange = None
sent_signals = {}
open_trades = {}

# === CANDLE HELPERS === (unchanged - keeping short)
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100 if c[1] != 0 else 0

def lower_wick_pct(c):
    if is_bearish(c) and (c[1] - c[4]) != 0:
        return (c[1] - c[3]) / (c[1] - c[4]) * 100
    return 0

def round_price(symbol, price):
    try:
        m = exchange.market(symbol)
        tick = float(m['info']['filters'][0]['tickSize'])
        prec = int(round(-math.log10(tick)))
        return round(price, prec)
    except:
        return price

def round_amount(symbol, amt):
    try:
        return exchange.amount_to_precision(symbol, amt)
    except:
        return amt

# === PATTERN DETECTION === (unchanged)
def detect_rising_three(candles):
    if len(candles) < 6: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol
    small_red_1 = is_bearish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT and lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT
    small_red_0 = is_bearish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT and lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT
    return big_green and small_red_1 and small_red_0

def detect_falling_three(candles):
    if len(candles) < 6: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol
    small_green_1 = is_bullish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT and lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT
    small_green_0 = is_bullish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT and lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT
    return big_red and small_green_1 and small_green_0

# === SYMBOLS ===
def get_symbols(markets):
    return [s for s in markets if 'USDT' in s and markets[s].get('swap') and markets[s].get('active', True)]

async def prepare_symbol(symbol):
    try:
        await exchange.set_margin_mode('isolated', symbol)
        await exchange.set_leverage(LEVERAGE, symbol)
        logging.info(f"Prepared {symbol}: isolated + {LEVERAGE}x")
    except Exception as e:
        logging.warning(f"Prepare {symbol} failed: {e}")

# === NEXT CANDLE ===
def get_next_candle_close():
    now = get_ist_time()
    secs = now.minute * 60 + now.second
    secs_to = (15 * 60) - (secs % (15 * 60))
    if secs_to < 10:
        secs_to += 15 * 60
    return time.time() + secs_to

# === HELPERS ===
def get_avg_entry_and_total(trade):
    total_pos = 0.0
    weighted = 0.0
    for e in trade['entries']:
        weighted += e['price'] * e['amount']
        total_pos += e['amount']
    if total_pos == 0:
        return 0.0, 0.0
    return weighted / total_pos, total_pos

# === MONITOR TP + DCA === (unchanged for brevity - add your original code here)

# === PROCESS SYMBOL === (unchanged - add your original code)

# === BATCH & SCAN LOOP === (add heartbeat)
async def scan_loop(symbols):
    first_run = True
    while True:
        if first_run:
            logging.info("Performing immediate first scan on startup")
            # Your batch scanning code here...
            # (paste your original chunk processing logic)
            first_run = False
            await asyncio.sleep(60)
            continue

        wait_until = get_next_candle_close()
        sleep_sec = max(0, wait_until - time.time())
        logging.info(f"Next 15m close in ~{sleep_sec//60} min")
        await asyncio.sleep(sleep_sec)

        # Your batch scanning...
        # ...

# === MAIN ===
async def main():
    global exchange
    exchange = None
    try:
        exchange = await initialize_exchange()
        markets = exchange.markets
        symbols = get_symbols(markets)

        load_trades()
        logging.info(f"Scanning {len(symbols)} USDT perpetuals")

        startup = (
            f"Bot restarted @ {get_ist_time().strftime('%Y-%m-%d %H:%M IST')}\n"
            f"Open positions: {len(open_trades)}\n"
            f"Max trades: {MAX_OPEN_TRADES} | Lev: {LEVERAGE}x\n"
            f"Initial: ${CAPITAL_INITIAL} → DCA +${CAPITAL_DCA} (max ${MAX_MARGIN_PER_TRADE})\n"
            f"TP: {TP_INITIAL_PCT*100:.1f}% → {TP_AFTER_DCA_PCT*100:.1f}% after DCA • **No SL**"
        )
        await send_telegram(startup)

        tasks = [
            asyncio.create_task(scan_loop(symbols)),
            asyncio.create_task(monitor_tp_and_dca()),
            asyncio.create_task(daily_summary()),
        ]

        # Heartbeat to keep Render happy
        heartbeat_task = asyncio.create_task(heartbeat())

        await asyncio.gather(*tasks, heartbeat_task)

    except Exception as e:
        logging.error(f"Main loop error: {e}", exc_info=True)
    finally:
        if exchange is not None:
            try:
                await exchange.close()
                logging.info("Exchange closed cleanly")
            except Exception as e:
                logging.warning(f"Failed to close exchange: {e}")

async def heartbeat():
    start = time.time()
    while True:
        await asyncio.sleep(45)
        elapsed = time.time() - start
        logging.info(f"Worker alive | uptime: {elapsed//60:.0f} min")

if __name__ == "__main__":
    asyncio.run(main())
