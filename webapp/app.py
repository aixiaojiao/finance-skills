"""
个股看板 · Stock Dashboard — 单文件 Flask + yfinance Web 应用

启动:  webapp/.venv/bin/python webapp/app.py  →  http://localhost:8000

页面1「个股看板」: 概览 / 估值 / 期权 / 多股对比 四个子标签 + 自选股
页面2「市场热力图」: 个股树状热力图(按板块分组)+ 板块 ETF 热力图

后端接口与对应 finance-skills 技能:
  /api/quote /api/history          基础行情 + MA            yfinance / sepa-strategy
  /api/sepa                        SEPA 趋势模板评分卡       sepa-strategy
  /api/earnings                    财报日 + beat/miss        earnings-preview
  /api/news                        新闻流                    yfinance
  /api/liquidity                   流动性评分                stock-liquidity
  /api/market                      大盘情绪/技术/环境         sepa-strategy/market-environment
  /api/quotes                      自选股批量行情            yfinance
  /api/compare                     多股归一化对比 + 相关性    stock-correlation
  /api/valuation                   DCF + 相对估值 + 预期趋势  company-valuation / estimate-analysis
  /api/options/expiries /chain     期权链                    options-payoff
  /api/heatmap                     全市场 + 板块热力图        (市场总览)

数据来自 Yahoo Finance(yfinance),非实时、有延迟,仅供研究/学习,不构成投资建议。
"""

import math
import os
import time
import json
import sqlite3
import threading
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, jsonify, request, Response, g
import yfinance as yf
import pandas as pd
import numpy as np

app = Flask(__name__)

# ============================ SQLite 持久化 ============================
# 持仓 / 自选 / 预警 / 设置 落盘,跨设备。路径可用 DASHBOARD_DB 覆盖。

DB_PATH = os.environ.get("DASHBOARD_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "dashboard.db"))


def get_db():
    db = getattr(g, "_db", None)
    if db is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        db = g._db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
    return db


@app.teardown_appcontext
def _close_db(exc):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
    CREATE TABLE IF NOT EXISTS positions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL, shares REAL NOT NULL,
        entry REAL NOT NULL, stop REAL, target REAL,
        opened_at TEXT, status TEXT DEFAULT 'open',
        exit_price REAL, closed_at TEXT, note TEXT
    );
    -- 交易明细:买卖流水台账。建仓=buy(记成本价,无盈亏);平仓/减仓=sell(记成交价+已实现盈亏)
    CREATE TABLE IF NOT EXISTS trades(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        position_id INTEGER, ticker TEXT NOT NULL,
        action TEXT NOT NULL DEFAULT 'sell',
        shares REAL NOT NULL, price REAL NOT NULL, pl REAL,
        at TEXT, note TEXT
    );
    CREATE TABLE IF NOT EXISTS alerts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL, kind TEXT NOT NULL,
        level REAL, note TEXT, active INTEGER DEFAULT 1, created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS watchlist(
        ticker TEXT PRIMARY KEY, added_at TEXT, sort INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY, value TEXT
    );
    -- 日线 K 线持久化缓存:重启不丢、跨 worker 共享、降 yfinance 压力
    CREATE TABLE IF NOT EXISTS bars(
        ticker TEXT NOT NULL, date TEXT NOT NULL,
        open REAL, high REAL, low REAL, close REAL, adj_close REAL, volume REAL,
        PRIMARY KEY(ticker, date)
    );
    -- 每个标的的缓存元信息:上次拉取时间(epoch)、已存最早/最晚日期
    CREATE TABLE IF NOT EXISTS bars_meta(
        ticker TEXT PRIMARY KEY, last_fetch TEXT, first_date TEXT, last_date TEXT
    );
    -- 用户自定义板块(任务4):热力图按此分组;首次为空时由内置默认种子填充
    CREATE TABLE IF NOT EXISTS user_sectors(
        name TEXT PRIMARY KEY, etf TEXT, sort INTEGER DEFAULT 0
    );
    -- 板块成分股:同一股票可归属多个板块(联合主键)
    CREATE TABLE IF NOT EXISTS user_sector_members(
        sector TEXT NOT NULL, ticker TEXT NOT NULL,
        PRIMARY KEY(sector, ticker)
    );
    -- 告警推送去重:key=alert:<id> / pos:<id>:stop / pos:<id>:target,记录已推送时间;
    -- 触发时推一次并落 key,条件回落(不再触发)时删 key 以便重新武装。
    CREATE TABLE IF NOT EXISTS notify_state(
        key TEXT PRIMARY KEY, fired_at TEXT
    );
    -- 个股研究笔记:在个股界面随手记录想法,系统自动记时间;按标的归档。
    CREATE TABLE IF NOT EXISTS stock_notes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL, body TEXT NOT NULL, created_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_stock_notes_ticker ON stock_notes(ticker, id DESC);
    -- 每日复盘:每天留存一份组合快照(持仓/交易流水/敞口等),外加用户手写复盘 review。
    CREATE TABLE IF NOT EXISTS snapshots(
        date TEXT PRIMARY KEY, payload TEXT, review TEXT, created_at TEXT
    );
    """)
    # 迁移:持仓支持「手填现价」(期权/港股等无实时行情的标的)——非空即视为手动定价,不拉 yfinance
    pos_cols = [r[1] for r in con.execute("PRAGMA table_info(positions)").fetchall()]
    if "manual_price" not in pos_cols:
        con.execute("ALTER TABLE positions ADD COLUMN manual_price REAL")
    # 迁移:为旧库 watchlist 补 sort 列,并按 added_at 回填初始顺序
    wl_cols = [r[1] for r in con.execute("PRAGMA table_info(watchlist)").fetchall()]
    if "sort" not in wl_cols:
        con.execute("ALTER TABLE watchlist ADD COLUMN sort INTEGER DEFAULT 0")
        for i, r in enumerate(con.execute("SELECT ticker FROM watchlist ORDER BY added_at, ticker").fetchall()):
            con.execute("UPDATE watchlist SET sort=? WHERE ticker=?", (i, r[0]))
    # 迁移:旧版 trades(仅卖出台账,列为 entry/exit_price/closed_at)→ 统一买卖流水(action/price/at)
    tr_cols = [r[1] for r in con.execute("PRAGMA table_info(trades)").fetchall()]
    if "action" not in tr_cols:
        con.execute("ALTER TABLE trades RENAME TO trades_old")
        con.execute("""CREATE TABLE trades(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id INTEGER, ticker TEXT NOT NULL,
            action TEXT NOT NULL DEFAULT 'sell',
            shares REAL NOT NULL, price REAL NOT NULL, pl REAL,
            at TEXT, note TEXT)""")
        con.execute("""INSERT INTO trades(id, position_id, ticker, action, shares, price, pl, at, note)
            SELECT id, position_id, ticker, 'sell', shares, exit_price, pl, closed_at, note FROM trades_old""")
        con.execute("DROP TABLE trades_old")
    # 回填卖出:旧库里已平仓的持仓补一条 sell 流水(幂等)
    con.execute("""INSERT INTO trades(position_id, ticker, action, shares, price, pl, at, note)
        SELECT id, ticker, 'sell', shares, exit_price, (exit_price-entry)*shares, closed_at, note
        FROM positions
        WHERE status='closed' AND exit_price IS NOT NULL
          AND id NOT IN (SELECT position_id FROM trades WHERE action='sell' AND position_id IS NOT NULL)""")
    # 回填买入:已有持仓(建仓时未记流水的)补一条 buy 流水(幂等),让交易明细完整
    con.execute("""INSERT INTO trades(position_id, ticker, action, shares, price, pl, at, note)
        SELECT id, ticker, 'buy', shares, entry, NULL, opened_at, note
        FROM positions
        WHERE id NOT IN (SELECT position_id FROM trades WHERE action='buy' AND position_id IS NOT NULL)""")
    # 自选股默认值(仅首次为空时)
    cur = con.execute("SELECT COUNT(*) c FROM watchlist").fetchone()
    if cur[0] == 0:
        con.executemany("INSERT INTO watchlist(ticker, added_at, sort) VALUES(?, ?, ?)",
                        [("AAPL", "", 0), ("NVDA", "", 1), ("TSLA", "", 2)])
    con.commit()
    con.close()


init_db()


# ============================ 通用工具 ============================

def _safe(v):
    try:
        if v is None:
            return None
        if hasattr(v, "item"):
            v = v.item()
        if isinstance(v, float) and math.isnan(v):
            return None
        return v
    except Exception:
        return None


def _num(v):
    v = _safe(v)
    if v is None:
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


_CACHE = {}

def _is_empty(v):
    if v is None:
        return True
    if isinstance(v, (pd.DataFrame, pd.Series)):
        return v.empty
    if isinstance(v, (list, tuple, dict, str)):
        return len(v) == 0
    return False

def cached(ttl, keep_empty=True):
    """内存 TTL 缓存。keep_empty=False 时不缓存空/失败结果(避免把限流返回的空值缓存住),
    且本次取到空时回退到上一次的有效缓存(过期也好过空)。"""
    def deco(fn):
        def wrap(*args):
            key = (fn.__name__, args)
            now = time.time()
            hit = _CACHE.get(key)
            if hit and now - hit[0] < ttl:
                return hit[1]
            val = fn(*args)
            if keep_empty or not _is_empty(val):
                _CACHE[key] = (now, val)
                return val
            return hit[1] if hit else val
        return wrap
    return deco


def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    return 100 - 100 / (1 + gain / loss)


@cached(90, keep_empty=False)
def get_info(ticker):
    return yf.Ticker(ticker).info or {}


@cached(120, keep_empty=False)
def _yf_history(ticker, period, interval="1d"):
    # 直接拉 yfinance(只缓存非空结果);限流/失败时退避重试一次。
    for attempt in range(2):
        try:
            df = yf.Ticker(ticker).history(period=period, interval=interval, auto_adjust=False)
        except Exception:
            df = None
        if df is not None and not df.empty:
            return df
        if attempt == 0:
            time.sleep(0.5)  # 多数 429 是瞬时的
    return pd.DataFrame()  # 仍为空:decorator 不缓存、自动回退上次有效缓存


# ============================ 日线 K 线持久化缓存 ============================
# 设计:日线(interval=1d)走 SQLite——先查库,缺/过期才拉 yfinance 并增量 upsert。
#   · 触发式:任何被访问的标的,其日线自动落库,后续从库读(带新鲜度判断)。
#   · 日内只增量拉 "1mo"(而非每次重拉 max),重启不丢、跨进程共享、大幅降 yfinance 压力。
#   · 收盘后由后台线程刷新「自选股 ∪ 持仓」当日 K 线;其他标的触发式更新。
# 非日线(intraday)不落库,仍走 _yf_history 内存缓存。

_BARS_LOCK = threading.Lock()
FAST_TTL = 90            # 距上次拉取 90s 内直接读库,不碰网络
# period 字符串 → 回看天数(None=全部);留足余量供 MA200 在左缘计算
_PERIOD_DAYS = {"5d": 8, "1mo": 35, "3mo": 100, "6mo": 190, "1y": 370,
                "2y": 740, "5y": 1830, "10y": 3660, "ytd": 370, "max": None}


def _cache_db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def _period_start(period):
    days = _PERIOD_DAYS.get(period, 740)
    if days is None:
        return None
    return (date.today() - timedelta(days=days)).isoformat()


def last_expected_session():
    """最近一个『已收盘完成』的美股交易日(YYYY-MM-DD)。仅按周末/收盘时间判断,不含节假日历
    (单用户研究工具可接受;配合拉取冷却避免节假日空拉硬磕)。"""
    et = datetime.now(ZoneInfo("America/New_York"))
    d = et.date()
    if et.hour < 16 or (et.hour == 16 and et.minute < 10):
        d = d - timedelta(days=1)            # 今日尚未收盘定稿 → 用前一交易日
    while d.weekday() >= 5:                   # 跨过周六/周日
        d = d - timedelta(days=1)
    return d.isoformat()


def _market_open():
    et = datetime.now(ZoneInfo("America/New_York"))
    if et.weekday() >= 5:
        return False
    m = et.hour * 60 + et.minute
    return 9 * 60 + 30 <= m <= 16 * 60


def _read_bars_meta(ticker):
    con = _cache_db()
    try:
        r = con.execute("SELECT last_fetch, first_date, last_date FROM bars_meta WHERE ticker=?",
                        (ticker,)).fetchone()
    finally:
        con.close()
    if not r:
        return None
    try:
        lf = float(r[0] or 0)
    except (TypeError, ValueError):
        lf = 0.0
    return {"last_fetch": lf, "first_date": r[1], "last_date": r[2]}


def _touch_bars_fetch(ticker):
    # 即使本次拉取为空(限流/失败)也更新 last_fetch,以 FAST_TTL 作冷却,避免反复硬磕。
    with _BARS_LOCK:
        con = _cache_db()
        try:
            con.execute("""INSERT INTO bars_meta(ticker, last_fetch, first_date, last_date)
                VALUES(?, ?, NULL, NULL)
                ON CONFLICT(ticker) DO UPDATE SET last_fetch=excluded.last_fetch""",
                        (ticker, str(time.time())))
            con.commit()
        finally:
            con.close()


def _store_bars(ticker, df):
    """把 yfinance 日线 df upsert 进库,并刷新 meta。返回写入行数。"""
    rows = []
    for idx, r in df.iterrows():
        rows.append((ticker, idx.strftime("%Y-%m-%d"),
                     _num(r.get("Open")), _num(r.get("High")), _num(r.get("Low")),
                     _num(r.get("Close")), _num(r.get("Adj Close")), _num(r.get("Volume"))))
    if not rows:
        return 0
    with _BARS_LOCK:
        con = _cache_db()
        try:
            con.executemany("""INSERT INTO bars(ticker, date, open, high, low, close, adj_close, volume)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(ticker, date) DO UPDATE SET
                  open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close,
                  adj_close=excluded.adj_close, volume=excluded.volume""", rows)
            mm = con.execute("SELECT MIN(date), MAX(date) FROM bars WHERE ticker=?", (ticker,)).fetchone()
            con.execute("""INSERT INTO bars_meta(ticker, last_fetch, first_date, last_date)
                VALUES(?,?,?,?)
                ON CONFLICT(ticker) DO UPDATE SET
                  last_fetch=excluded.last_fetch, first_date=excluded.first_date, last_date=excluded.last_date""",
                        (ticker, str(time.time()), mm[0], mm[1]))
            con.commit()
        finally:
            con.close()
    return len(rows)


def _load_bars_df(ticker, start_date):
    con = _cache_db()
    try:
        if start_date:
            cur = con.execute("""SELECT date, open, high, low, close, adj_close, volume
                FROM bars WHERE ticker=? AND date>=? ORDER BY date""", (ticker, start_date))
        else:
            cur = con.execute("""SELECT date, open, high, low, close, adj_close, volume
                FROM bars WHERE ticker=? ORDER BY date""", (ticker,))
        rows = cur.fetchall()
    finally:
        con.close()
    if not rows:
        return pd.DataFrame()
    idx = pd.to_datetime([r[0] for r in rows])
    return pd.DataFrame({
        "Open": [r[1] for r in rows], "High": [r[2] for r in rows], "Low": [r[3] for r in rows],
        "Close": [r[4] for r in rows], "Adj Close": [r[5] for r in rows], "Volume": [r[6] for r in rows],
    }, index=idx)


def _history_daily(ticker, period):
    need_start = _period_start(period)
    meta = _read_bars_meta(ticker)
    now = time.time()
    refetch = None
    if meta is None or not meta["last_date"]:
        refetch = period                                  # 首次:拉整段
    else:
        need_older = need_start is not None and meta["first_date"] and meta["first_date"] > need_start
        if need_older:
            refetch = period                              # 需要更早历史 → 重拉整段补齐
        elif now - meta["last_fetch"] > FAST_TTL:
            if meta["last_date"] < last_expected_session() or _market_open():
                refetch = "1mo"                           # 缺最新交易日 / 盘中 → 增量拉
    if refetch:
        df = _yf_history(ticker, refetch, "1d")
        if df is not None and not df.empty:
            _store_bars(ticker, df)
        else:
            _touch_bars_fetch(ticker)                     # 失败也置冷却,避免硬磕
    out = _load_bars_df(ticker, need_start)
    if out is None or out.empty:
        out = _yf_history(ticker, period, "1d")           # 库里仍空(首拉失败)→ 兜底直拉
    return out


def get_history_df(ticker, period, interval="1d"):
    """日线走持久化缓存;intraday 直接拉。任何缓存异常都回退直拉,绝不让缓存层拖垮接口。"""
    if interval != "1d":
        return _yf_history(ticker, period, interval)
    try:
        return _history_daily(ticker, period)
    except Exception:
        return _yf_history(ticker, period, interval)


# ---- 收盘后批量刷新:自选股 ∪ 持仓(重点标的);其他标的触发式更新 ----

def tracked_tickers():
    con = _cache_db()
    try:
        wl = [r[0] for r in con.execute("SELECT ticker FROM watchlist")]
        pos = [r[0] for r in con.execute("SELECT DISTINCT ticker FROM positions WHERE status='open'")]
    finally:
        con.close()
    return sorted({t.strip().upper() for t in (wl + pos) if t and t.strip()})


def refresh_tracked_bars():
    """增量刷新所有重点标的当日 K 线。返回 {ticker: 写入行数}。"""
    res = {}
    for t in tracked_tickers():
        try:
            df = _yf_history(t, "1mo", "1d")
            res[t] = _store_bars(t, df) if (df is not None and not df.empty) else 0
        except Exception:
            res[t] = -1
        time.sleep(0.3)   # 对 yfinance 温柔些,避免限流
    return res


# ============================ 告警 Telegram 推送(个人持仓/组合) ============================
# 投递复用首尔本机的 tv-relay:把告警文本 POST 到 TG_RELAY_WEBHOOK(形如
#   http://127.0.0.1:8080/tv/<SECRET>/webapp),由中转转 Telegram —— webapp 本身不存 bot token。
# 全局开关 settings.telegramPushEnabled(默认 '0' 关闭);关闭或未配置 webhook 则完全不推。
# 盘中后台每 ~3 分钟轮询「自选/持仓告警」,新触发的推一次(notify_state 去重),条件回落后重新武装。

import urllib.request as _urlreq  # noqa: E402

ALERT_PUSH_INTERVAL = 180  # 盘中轮询间隔(秒)


def _tg_relay_webhook():
    return os.environ.get("TG_RELAY_WEBHOOK", "").strip()


def _tg_send(text):
    """把一条文本经本机 tv-relay 转发到 Telegram。返回 (ok, info)。未配置则不发。"""
    url = _tg_relay_webhook()
    if not url:
        return False, "TG_RELAY_WEBHOOK 未配置"
    try:
        data = json.dumps({"text": text, "parse_mode": "HTML"}).encode("utf-8")
        # 带 UA:Cloudflare 默认拦 Python-urllib 的 UA(403),需浏览器前缀 UA 才放行
        req = _urlreq.Request(url, data=data, headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; finance-dashboard/1.0)",
        })
        with _urlreq.urlopen(req, timeout=10) as resp:
            return True, resp.status
    except Exception as e:
        return False, str(e)[:200]


def _push_enabled():
    con = _cache_db()
    try:
        r = con.execute("SELECT value FROM settings WHERE key='telegramPushEnabled'").fetchone()
    finally:
        con.close()
    return bool(r) and str(r[0]) == "1"


def _fired_keys():
    con = _cache_db()
    try:
        return {r[0] for r in con.execute("SELECT key FROM notify_state")}
    finally:
        con.close()


def _mark_fired(key):
    con = _cache_db()
    try:
        con.execute("INSERT INTO notify_state(key, fired_at) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET fired_at=excluded.fired_at",
                    (key, time.strftime("%Y-%m-%d %H:%M:%S")))
        con.commit()
    finally:
        con.close()


def _clear_fired(keys):
    if not keys:
        return
    con = _cache_db()
    try:
        con.executemany("DELETE FROM notify_state WHERE key=?", [(k,) for k in keys])
        con.commit()
    finally:
        con.close()


def _alert_text(kind, level):
    return {"above": f"≥ {level}", "below": f"≤ {level}", "stop": f"触止损 ≤ {level}",
            "break_ma20": "跌破 20MA"}.get(kind, kind)


def _collect_alert_events():
    """汇总「自选股手动告警 + 持仓止损/目标」的触发事件。返回 [{key, ticker, text, triggered}]。
    个人化:只覆盖与你的告警/持仓绑定的条件,市场技术形态交给 TradingView/Pine。"""
    con = _cache_db()
    try:
        alerts = [dict(zip(("id", "ticker", "kind", "level"), r)) for r in con.execute(
            "SELECT id, ticker, kind, level FROM alerts WHERE active=1")]
        positions = [dict(zip(("id", "ticker", "stop", "target", "entry"), r)) for r in con.execute(
            "SELECT id, ticker, stop, target, entry FROM positions WHERE status='open'")]
    finally:
        con.close()
    tickers = {a["ticker"] for a in alerts} | {p["ticker"] for p in positions}
    if not tickers:
        return []
    live = _live_prices(list(tickers))
    events = []
    for a in alerts:
        lp = live.get(a["ticker"], {})
        price, ma20 = lp.get("price"), lp.get("ma20")
        if price is None:
            continue
        lv = a["level"]
        trig = False
        if a["kind"] == "above" and lv is not None:
            trig = price >= lv
        elif a["kind"] in ("below", "stop") and lv is not None:
            trig = price <= lv
        elif a["kind"] == "break_ma20" and ma20:
            trig = price < ma20
        events.append({"key": f"alert:{a['id']}", "ticker": a["ticker"], "triggered": trig,
                       "text": f"🔔 <b>{a['ticker']}</b> {_alert_text(a['kind'], lv)} 已触发(现价 {round(price, 2)})"})
    for p in positions:
        lp = live.get(p["ticker"], {})
        price = lp.get("price")
        if price is None:
            continue
        if p["stop"]:
            events.append({"key": f"pos:{p['id']}:stop", "ticker": p["ticker"], "triggered": price <= p["stop"],
                           "text": f"🛑 <b>{p['ticker']}</b> 持仓触止损 {p['stop']}(现价 {round(price, 2)},成本 {p['entry']})"})
        if p["target"]:
            events.append({"key": f"pos:{p['id']}:target", "ticker": p["ticker"], "triggered": price >= p["target"],
                           "text": f"🎯 <b>{p['ticker']}</b> 持仓到目标价 {p['target']}(现价 {round(price, 2)},成本 {p['entry']})"})
    return events


def _alert_check_and_push():
    """评估并推送。仅在开关开启且 webhook 已配置时动作。返回推送条数。"""
    if not _push_enabled() or not _tg_relay_webhook():
        return 0
    try:
        events = _collect_alert_events()
    except Exception:
        return 0
    fired = _fired_keys()
    sent = 0
    rearm = []
    for ev in events:
        if ev["triggered"]:
            if ev["key"] not in fired:
                ok, _ = _tg_send(ev["text"])
                if ok:
                    _mark_fired(ev["key"])
                    sent += 1
        elif ev["key"] in fired:
            rearm.append(ev["key"])          # 条件回落 → 删除,以便下次再触发可重新推送
    _clear_fired(rearm)
    return sent


# ============================ 每日盘后总结(Telegram 日报) ============================
# 收盘后约 1 小时(美东 17:00)推一份:大盘 + 板块 + 自选股 SEPA 概览。
# 复用 tv-relay → Telegram 的同一通道(_tg_send)。开关:settings.dailyReportEnabled(默认开)。

def _daily_report_enabled():
    con = _cache_db()
    try:
        r = con.execute("SELECT value FROM settings WHERE key='dailyReportEnabled'").fetchone()
    finally:
        con.close()
    return (r is None) or str(r[0]) != "0"     # 用户已明确要日报:默认开,显式设 '0' 才关


def _money(v):
    return "—" if v is None else f"{v:,.2f}"


def _signed_pct(v):
    return "—" if v is None else f"{v:+.2f}%"


def _chg_mark(v):
    return "⬜" if v is None else ("🟢" if v > 0 else ("🔴" if v < 0 else "⬜"))


def build_daily_report():
    """组装盘后总结的 HTML 文本(Telegram parse_mode=HTML)。"""
    sess = last_expected_session()
    L = [f"📊 <b>盘后总结 · {sess}</b>(美东收盘)"]

    # —— 大盘 ——
    try:
        m = _market_overview()
    except Exception:
        m = None
    if m:
        L += ["", "<b>🌎 大盘</b>"]

        def idx_line(name, x):
            if not x:
                return f"{name} —"
            vs200 = "↑200日线" if x.get("above200") else ("↓200日线" if x.get("above200") is False else "")
            s = f"{name} {_money(x.get('price'))} {_signed_pct(x.get('changePct'))}"
            if vs200:
                s += f" · {vs200}"
            if x.get("rsi") is not None:
                s += f" · RSI {x['rsi']:.0f}"
            return s
        L.append(idx_line("标普500", m.get("spx")))
        L.append(idx_line("纳指", m.get("ndx")))
        tail = f"环境 {m.get('environment', '—')} · 情绪 {m.get('sentimentLabel', '—')}"
        if m.get("sentiment") is not None:
            tail += f"({m['sentiment']})"
        if m.get("vix") is not None:
            tail = f"VIX {m['vix']:.1f} · " + tail
        L.append(tail)

    # —— 板块 ——
    try:
        hm = compute_heatmap()
    except Exception:
        hm = None
    etfs = [e for e in (hm or {}).get("sectorEtfs", []) if e.get("change") is not None] if hm else []
    if etfs:
        etfs.sort(key=lambda e: -e["change"])

        def sec_str(e):
            return f"{e['sector'].split(' ')[0]}{e['etf']} {_signed_pct(e['change'])}"
        L += ["", "<b>🧩 板块(当日 ETF)</b>"]
        L.append("强 " + " · ".join(sec_str(e) for e in etfs[:3]))
        L.append("弱 " + " · ".join(sec_str(e) for e in list(reversed(etfs[-3:]))))

    # —— 自选股(逐只 SEPA 概览)——
    con = _cache_db()
    try:
        wl = [r[0] for r in con.execute("SELECT ticker FROM watchlist ORDER BY sort, added_at, ticker")]
    finally:
        con.close()
    if wl:
        L += ["", "<b>⭐ 自选股</b>"]
        for tk in wl:
            s = _compute_sepa(tk)
            if "error" in s:
                L.append(f"{_chg_mark(None)} <b>{tk}</b> — {s['error']}")
                continue
            verdict = (s.get("verdict") or "").split(" · ")[-1]
            L.append(f"{_chg_mark(s.get('changePct'))} <b>{tk}</b> {_money(s.get('price'))} "
                     f"{_signed_pct(s.get('changePct'))} · {s.get('stage', '')} · "
                     f"模板{s.get('passed')}/{s.get('total')} · {verdict}")

    L += ["", "<i>数据 yfinance(延迟);SEPA 趋势模板评分,仅供参考,非投资建议。</i>"]
    return "\n".join(L)


def send_daily_report():
    """组装并经 tv-relay 推送盘后总结。返回 (ok, info, text)。"""
    text = build_daily_report()
    ok, info = _tg_send(text)
    return ok, info, text


def _daily_report_loop():
    # 每个交易日美东 17:00(收盘后 1 小时)推送一次盘后总结。
    while True:
        et = datetime.now(ZoneInfo("America/New_York"))
        target = et.replace(hour=17, minute=0, second=0, microsecond=0)
        if target <= et:
            target += timedelta(days=1)
        time.sleep(max(60, (target - et).total_seconds()))
        try:
            now_et = datetime.now(ZoneInfo("America/New_York"))
            key = f"report:{now_et.date().isoformat()}"        # 每个交易日只发一次(防 worker 重启重发)
            if (now_et.weekday() < 5 and _tg_relay_webhook() and _daily_report_enabled()
                    and key not in _fired_keys()):
                ok, _info, _text = send_daily_report()
                if ok:
                    _mark_fired(key)
        except Exception:
            pass
        # 每日组合快照(本地留档,供「每日复盘」回查;与 Telegram 推送开关无关)
        try:
            now_et = datetime.now(ZoneInfo("America/New_York"))
            snap_key = f"snapshot:{now_et.date().isoformat()}"
            if now_et.weekday() < 5 and snap_key not in _fired_keys():
                take_portfolio_snapshot(now_et.date().isoformat())
                _mark_fired(snap_key)
        except Exception:
            pass


def _alert_push_loop():
    # 盘中每 ALERT_PUSH_INTERVAL 秒检查一次(开关关闭/盘后则空转)。
    while True:
        time.sleep(ALERT_PUSH_INTERVAL)
        try:
            if _market_open():
                _alert_check_and_push()
        except Exception:
            pass


def _scheduler_loop():
    # 每天 22:10 UTC(美股 EDT 收盘 20:00 / EST 21:00 UTC 之后)刷新重点标的当日 K 线。
    while True:
        now = datetime.now(timezone.utc)
        target = now.replace(hour=22, minute=10, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        time.sleep(max(60, (target - now).total_seconds()))
        try:
            if datetime.now(timezone.utc).weekday() < 5:   # 仅工作日(美股交易日近似)
                refresh_tracked_bars()
        except Exception:
            pass


def start_scheduler():
    if os.environ.get("ENABLE_SCHEDULER", "1") != "1":
        return
    if getattr(start_scheduler, "_started", False):
        return
    start_scheduler._started = True
    threading.Thread(target=_scheduler_loop, name="kline-scheduler", daemon=True).start()
    threading.Thread(target=_alert_push_loop, name="alert-push", daemon=True).start()
    threading.Thread(target=_daily_report_loop, name="daily-report", daemon=True).start()


# ---- 以下接口此前每次请求都现拉 yfinance,统一加缓存(keep_empty=False:不缓存失败值) ----
@cached(900, keep_empty=False)
def get_earnings_dates(ticker):
    try:
        ed = yf.Ticker(ticker).get_earnings_dates(limit=16)
    except Exception:
        return None
    return ed if (ed is not None and not ed.empty) else None

@cached(900, keep_empty=False)
def get_calendar(ticker):
    try:
        return yf.Ticker(ticker).calendar or {}
    except Exception:
        return {}

@cached(600, keep_empty=False)
def get_news_raw(ticker):
    try:
        return yf.Ticker(ticker).news or []
    except Exception:
        return []

@cached(900, keep_empty=False)
def get_option_expiries(ticker):
    try:
        return list(yf.Ticker(ticker).options or [])
    except Exception:
        return []

@cached(300, keep_empty=False)
def get_option_chain(ticker, expiry):
    # 返回 yfinance 的 Options(namedtuple,含 calls/puts DataFrame);失败抛出由调用方兜底
    return yf.Ticker(ticker).option_chain(expiry)

@cached(60, keep_empty=False)
def get_quotes_download(tickers_key):
    # tickers_key:逗号拼接的有序代码串,保证缓存键稳定
    tickers = tickers_key.split(",")
    return yf.download(tickers, period="5d", auto_adjust=False, progress=False, group_by="ticker")

@cached(1800, keep_empty=False)
def get_financials(ticker):
    # 财报报表(利润表/现金流/营收预期),用于 DCF;最重的调用,缓存 30 分钟
    tk = yf.Ticker(ticker)
    try:
        inc = tk.income_stmt
    except Exception:
        inc = None
    if inc is None or inc.empty:
        return {}  # 空不缓存,下次重试
    out = {"inc": inc}
    for k, getter in (("cf", lambda: tk.cashflow), ("rev_est", lambda: tk.revenue_estimate),
                      ("earn_est", lambda: tk.earnings_estimate), ("eps_trend", lambda: tk.eps_trend)):
        try:
            out[k] = getter()
        except Exception:
            out[k] = None
    return out


def _fin_fallback(ticker):
    """从利润表(income_stmt)补算被 .info 漏掉的基本面字段。

    yfinance 的 .info(quoteSummary)被限流时会回一个「非空但残缺」的 dict,
    缺 trailingEps / profitMargins / grossMargins —— 残缺响应还会被 get_info 缓存住,
    于是个股界面这些指标显示「—」(MU 复现)。这里用已为 DCF 拉取并缓存 30min 的
    利润表做兜底,取最近一个财年口径,把缺失项补齐。返回值会覆盖到 None 的项上。"""
    out = {"eps": None, "revenue": None, "profitMargin": None, "grossMargin": None}
    try:
        inc = get_financials(ticker).get("inc")
        if inc is None or inc.empty:
            return out
        col = inc.columns[0]  # 最近一期(列按时间倒序)

        def row(name):
            try:
                return _num(inc.loc[name, col]) if name in inc.index else None
            except Exception:
                return None

        rev = row("Total Revenue") or row("Operating Revenue")
        net = row("Net Income") or row("Net Income Common Stockholders")
        gross = row("Gross Profit")
        out["revenue"] = rev
        out["eps"] = row("Diluted EPS") or row("Basic EPS")
        if rev:
            if net is not None:
                out["profitMargin"] = net / rev
            if gross is not None:
                out["grossMargin"] = gross / rev
    except Exception:
        pass
    return out


# ============================ API: 报价 + 基本面 ============================

@app.route("/api/quote")
def api_quote():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    try:
        info = get_info(ticker)
    except Exception as e:
        return jsonify({"error": f"获取 {ticker} 失败: {e}"}), 502
    if not info or not (info.get("shortName") or info.get("longName")):
        return jsonify({"error": f"未找到股票 {ticker},请检查代码"}), 404

    price = _num(info.get("currentPrice") or info.get("regularMarketPrice"))
    prev = _num(info.get("regularMarketPreviousClose") or info.get("previousClose"))
    change = change_pct = None
    if price is not None and prev:
        change = price - prev
        change_pct = change / prev * 100

    financials = {
        "marketCap": _num(info.get("marketCap")), "trailingPE": _num(info.get("trailingPE")),
        "forwardPE": _num(info.get("forwardPE")), "priceToBook": _num(info.get("priceToBook")),
        "eps": _num(info.get("trailingEps")), "revenue": _num(info.get("totalRevenue")),
        "profitMargin": _num(info.get("profitMargins")), "grossMargin": _num(info.get("grossMargins")),
        "dividendYield": _num(info.get("dividendYield")), "beta": _num(info.get("beta")),
        "fiftyTwoWeekHigh": _num(info.get("fiftyTwoWeekHigh")), "fiftyTwoWeekLow": _num(info.get("fiftyTwoWeekLow")),
    }
    # .info 被限流时常漏掉 eps/利润率/营收(MU 复现)→ 用利润表兜底补齐缺失项
    if any(financials[k] is None for k in ("eps", "revenue", "profitMargin", "grossMargin")):
        fb = _fin_fallback(ticker)
        for k, v in fb.items():
            if financials.get(k) is None and v is not None:
                financials[k] = v

    return jsonify({
        "ticker": ticker,
        "name": _safe(info.get("longName") or info.get("shortName")),
        "currency": _safe(info.get("currency")) or "USD",
        "exchange": _safe(info.get("fullExchangeName") or info.get("exchange")),
        "sector": _safe(info.get("sector")), "industry": _safe(info.get("industry")),
        "price": price, "prevClose": prev, "change": change, "changePct": change_pct,
        "dayHigh": _num(info.get("dayHigh")), "dayLow": _num(info.get("dayLow")),
        "open": _num(info.get("open") or info.get("regularMarketOpen")),
        "volume": _num(info.get("volume") or info.get("regularMarketVolume")),
        "financials": financials,
        "analyst": {
            "targetMean": _num(info.get("targetMeanPrice")), "targetHigh": _num(info.get("targetHighPrice")),
            "targetLow": _num(info.get("targetLowPrice")), "recommendation": _safe(info.get("recommendationKey")),
            "numAnalysts": _num(info.get("numberOfAnalystOpinions")),
        },
    })


# ============================ API: K线 + 均线 ============================

MA_WINDOWS = [5, 10, 20, 50, 200]

# 时间周期 = 单根 K 线代表的时长(timeframe),而非「看多长历史」。本系统只看趋势,
# 不做盘中实时(实时报警交由 TradingView),故最小周期为 4h,不提供 4h 以下日内档。
# 每档配:(yfinance interval, 拉取窗口, 显示根数, 重采样规则)
#   · 4h Yahoo 不原生支持 → 拉 1h(Yahoo 1h 上限 730 天,已实测)后端重采样合成。
#   · 1d 走 SQLite 缓存路径(get_history_df interval=1d);其余走 _yf_history。
INTERVAL_CFG = {
    "4h":  {"yf": "60m", "fetch": "730d", "bars": 360, "resample": "4h",  "intraday": True},
    "1d":  {"yf": "1d",  "fetch": "2y",   "bars": 252, "resample": None,  "intraday": False},
    "1w":  {"yf": "1wk", "fetch": "max",  "bars": 260, "resample": None,  "intraday": False},
}
DEFAULT_INTERVAL = "1d"


def _resample_ohlc(df, rule):
    """把更细周期的 OHLCV 重采样成 rule(如 '4h')。"""
    agg = {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    cols = [c for c in agg if c in df.columns]
    # origin="start":桶从首根 K(美股为 09:30 开盘)起算;24h 可被 4h 整除 → 每日对齐到 09:30/13:30。
    out = df[cols].resample(rule, label="left", closed="left", origin="start").agg(
        {c: agg[c] for c in cols}).dropna(subset=["Open"])
    return out


def _bar_time(idx, intraday):
    """K 线时间字段:日线/周线用 YYYY-MM-DD(business day);日内用 UNIX 秒。
    日内把交易所(美东)墙钟时间当作 UTC 输出,使 lightweight-charts 直接显示美东时分。"""
    if not intraday:
        return idx.strftime("%Y-%m-%d")
    ts = idx
    if ts.tzinfo is not None:
        ts = ts.tz_convert(ZoneInfo("America/New_York")).tz_localize(None)
    return int(ts.tz_localize(ZoneInfo("UTC")).timestamp())


@app.route("/api/history")
def api_history():
    ticker = (request.args.get("ticker") or "").strip().upper()
    period = request.args.get("period", DEFAULT_INTERVAL)
    cfg = INTERVAL_CFG.get(period) or INTERVAL_CFG[DEFAULT_INTERVAL]
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    try:
        df = get_history_df(ticker, cfg["fetch"], cfg["yf"]).copy()
    except Exception as e:
        return jsonify({"error": f"获取历史失败: {e}"}), 502
    if df is None or df.empty:
        return jsonify({"error": "无历史数据"}), 404

    if cfg["resample"]:
        df = _resample_ohlc(df, cfg["resample"])
        if df.empty:
            return jsonify({"error": "无历史数据"}), 404

    intraday = cfg["intraday"]
    for w in MA_WINDOWS:
        df[f"ma{w}"] = df["Close"].rolling(w).mean()
    df = df.tail(cfg["bars"])

    candles, volumes = [], []
    ma_series = {str(w): [] for w in MA_WINDOWS}
    for idx, row in df.iterrows():
        ts = _bar_time(idx, intraday)
        o, h, l, c = _num(row.get("Open")), _num(row.get("High")), _num(row.get("Low")), _num(row.get("Close"))
        if None in (o, h, l, c):
            continue
        candles.append({"time": ts, "open": o, "high": h, "low": l, "close": c})
        vol = _num(row.get("Volume")) or 0
        volumes.append({"time": ts, "value": vol,
                        "color": "rgba(38,166,154,0.5)" if c >= o else "rgba(239,83,80,0.5)"})
        for w in MA_WINDOWS:
            mv = _num(row.get(f"ma{w}"))
            if mv is not None:
                ma_series[str(w)].append({"time": ts, "value": mv})
    return jsonify({"ticker": ticker, "candles": candles, "volumes": volumes,
                    "ma": ma_series, "interval": period, "intraday": intraday})


# ============================ API: SEPA ============================

@cached(300)
def _benchmark_close():
    # 标普500 收盘序列,^GSPC 取不到时回退到 SPY ETF(更不易被限流)
    for sym in ("^GSPC", "SPY"):
        try:
            df = get_history_df(sym, "2y")
            if df is not None and not df.empty and len(df) >= 60:
                return df["Close"].dropna()
        except Exception:
            continue
    return None


def _compute_sepa(ticker):
    """SEPA 趋势模板分析,返回结果 dict;失败返回带 error/_status 的 dict。供 API 与日报复用。"""
    try:
        df = get_history_df(ticker, "2y").copy()
        info = get_info(ticker)
    except Exception as e:
        return {"error": f"获取失败: {e}", "_status": 502}
    if df is None or df.empty or len(df) < 60:
        return {"error": "历史数据不足,无法做 SEPA 分析", "_status": 404}

    close = df["Close"]
    price = _num(close.iloc[-1])
    prev = _num(close.iloc[-2]) if len(close) >= 2 else None
    change_pct = ((price / prev - 1) * 100) if (price and prev) else None
    ma50 = _num(close.rolling(50).mean().iloc[-1])
    ma150 = _num(close.rolling(150).mean().iloc[-1]) if len(df) >= 150 else None
    ma200 = _num(close.rolling(200).mean().iloc[-1]) if len(df) >= 200 else None
    ma200_1mo = _num(close.rolling(200).mean().iloc[-22]) if len(df) >= 222 else None
    win = min(len(df), 252)
    hi52 = _num(df["High"].tail(win).max())
    lo52 = _num(df["Low"].tail(win).min())

    # 相对强度:基准与个股取同一回看窗口(优先 252 日,不足则用可用最长窗口,≥60 日才有意义)
    rs_pass = rs_val = None
    rs_lookback = None
    bench = _benchmark_close()
    if bench is None:
        rs_value_str = "基准(标普500)暂不可用,稍后重试"
    else:
        lookback = min(len(close), len(bench), 252)
        if lookback < 60:
            rs_value_str = f"历史仅 {len(close)} 日(上市未满3个月),暂不算 RS"
        else:
            rs_lookback = lookback
            stock_ret = price / _num(close.iloc[-lookback]) - 1
            bench_ret = _num(bench.iloc[-1]) / _num(bench.iloc[-lookback]) - 1
            if stock_ret is not None and bench_ret is not None:
                rs_val = (stock_ret - bench_ret) * 100
                rs_pass = stock_ret > bench_ret
                wlabel = "1年" if lookback >= 252 else f"{lookback}日"
                rs_value_str = f"相对标普 {rs_val:+.1f}pp({wlabel})"
            else:
                rs_value_str = "数据不足"

    def pct(a, b):
        return None if (a is None or b is None or b == 0) else (a / b - 1) * 100
    pct_above_low = pct(price, lo52)
    pct_from_high = pct(price, hi52)

    conds = []
    def cond(no, name, ok, val):
        conds.append({"no": no, "name": name, "pass": bool(ok) if ok is not None else None, "value": val})
    cond(1, "价格 > 150MA 且 > 200MA",
         (ma150 and ma200 and price > ma150 and price > ma200),
         f"价 {price:.2f} / 150MA {ma150:.2f} / 200MA {ma200:.2f}" if ma150 and ma200 else "数据不足")
    cond(2, "150MA > 200MA", (ma150 and ma200 and ma150 > ma200),
         f"{ma150:.2f} vs {ma200:.2f}" if ma150 and ma200 else "数据不足")
    cond(3, "200MA 近 1 个月向上", (ma200 and ma200_1mo and ma200 > ma200_1mo),
         f"现 {ma200:.2f} vs 1月前 {ma200_1mo:.2f}" if ma200 and ma200_1mo else "数据不足")
    cond(4, "50MA > 150MA 且 > 200MA", (ma50 and ma150 and ma200 and ma50 > ma150 and ma50 > ma200),
         f"50MA {ma50:.2f}" if ma50 else "数据不足")
    cond(5, "价格 > 50MA", (ma50 and price > ma50),
         f"价 {price:.2f} / 50MA {ma50:.2f}" if ma50 else "数据不足")
    cond(6, "高于 52周低点 ≥ 30%", (pct_above_low is not None and pct_above_low >= 30),
         f"+{pct_above_low:.1f}%" if pct_above_low is not None else "—")
    cond(7, "距 52周高点 ≤ 25%", (pct_from_high is not None and pct_from_high >= -25),
         f"{pct_from_high:.1f}%" if pct_from_high is not None else "—")
    cond(8, "相对强度 RS 跑赢大盘(代理)", rs_pass, rs_value_str)

    passed = sum(1 for c in conds if c["pass"] is True)

    stage = "Stage 1 · 筑底"
    if ma200 is not None:
        below_all = (ma50 and price < ma50) and price < ma200 and (ma50 and ma50 < ma200)
        stack_up = (ma50 and ma150 and ma200 and price > ma50 > ma150 > ma200)
        near_200 = abs(pct(price, ma200) or 99) < 8
        if stack_up and (ma200_1mo is None or ma200 >= ma200_1mo):
            stage = "Stage 2 · 上升(可买区)"
        elif below_all:
            stage = "Stage 4 · 下降(回避)"
        elif price > ma200 and not stack_up:
            stage = "Stage 3 · 做头(减仓)"
        elif near_200:
            stage = "Stage 1 · 筑底"

    eps_growth = _num(info.get("earningsQuarterlyGrowth"))
    fgrade = "?" if eps_growth is None else ("A" if eps_growth > 0.30 else "B" if eps_growth >= 0.15 else "C" if eps_growth >= 0 else "D")

    if "Stage 2" in stage and passed == 8 and fgrade in ("A", "B"):
        verdict, vclass = "Strong Buy Setup · 强势候选", "buy"
    elif passed >= 6 and "Stage 4" not in stage:
        verdict, vclass = "Watch List · 观察", "hold"
    else:
        verdict, vclass = "Pass · 暂不符合", "sell"

    return {"ticker": ticker, "price": price, "changePct": change_pct, "stage": stage, "conditions": conds,
            "passed": passed, "total": 8, "fundamentalGrade": fgrade,
            "epsGrowth": eps_growth, "revGrowth": _num(info.get("revenueGrowth")),
            "verdict": verdict, "verdictClass": vclass,
            "rsNote": f"RS 为相对标普500 涨幅代理({('回看'+str(rs_lookback)+'日') if rs_lookback else '窗口自适应'}),非全市场百分位排名"}


@app.route("/api/sepa")
def api_sepa():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    res = _compute_sepa(ticker)
    if "error" in res:
        return jsonify({"error": res["error"]}), res.get("_status", 502)
    return jsonify(res)


# ============================ API: 财报 ============================

@app.route("/api/earnings")
def api_earnings():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    upcoming, history = None, []
    ed = get_earnings_dates(ticker)
    if ed is not None and not ed.empty:
        cols = {c.lower(): c for c in ed.columns}
        est_col = next((cols[k] for k in cols if "estimate" in k), None)
        rep_col = next((cols[k] for k in cols if "reported" in k), None)
        sur_col = next((cols[k] for k in cols if "surprise" in k), None)
        for idx, row in ed.iterrows():
            est = _num(row.get(est_col)) if est_col else None
            rep = _num(row.get(rep_col)) if rep_col else None
            sur = _num(row.get(sur_col)) if sur_col else None
            date_str = idx.strftime("%Y-%m-%d")
            if rep is None and upcoming is None:
                upcoming = {"date": date_str, "estimate": est}
            elif rep is not None:
                history.append({"date": date_str, "estimate": est, "reported": rep,
                                "surprisePct": sur, "beat": (est is not None and rep >= est)})
        history = history[:6]
    if upcoming is None:
        try:
            cal = get_calendar(ticker)
            if isinstance(cal, dict) and cal.get("Earnings Date"):
                ds = cal["Earnings Date"]
                d0 = ds[0] if isinstance(ds, (list, tuple)) else ds
                upcoming = {"date": str(d0)[:10], "estimate": _num(cal.get("Earnings Average"))}
        except Exception:
            pass
    try:
        info = get_info(ticker)
    except Exception:
        info = {}
    return jsonify({"ticker": ticker, "upcoming": upcoming, "history": history,
                    "epsForward": _num(info.get("forwardEps")), "epsTrailing": _num(info.get("trailingEps")),
                    "revenueGrowth": _num(info.get("revenueGrowth")), "earningsGrowth": _num(info.get("earningsQuarterlyGrowth"))})


# ============================ API: 新闻 ============================

@app.route("/api/news")
def api_news():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    raw = get_news_raw(ticker)
    items = []
    for it in raw[:12]:
        c = it.get("content") if isinstance(it, dict) and "content" in it else it
        if not isinstance(c, dict):
            continue
        title = c.get("title")
        pub = c["provider"].get("displayName") if isinstance(c.get("provider"), dict) else c.get("publisher")
        link = None
        if isinstance(c.get("canonicalUrl"), dict):
            link = c["canonicalUrl"].get("url")
        if not link and isinstance(c.get("clickThroughUrl"), dict):
            link = c["clickThroughUrl"].get("url")
        link = link or c.get("link")
        ts = c.get("pubDate") or c.get("providerPublishTime") or c.get("displayTime")
        if isinstance(ts, (int, float)):
            ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))
        elif isinstance(ts, str):
            ts = ts.replace("T", " ").replace("Z", "")[:16]
        if title:
            items.append({"title": title, "publisher": pub, "link": link, "time": ts})
    return jsonify({"ticker": ticker, "news": items})


# ============================ API: 流动性 ============================

@app.route("/api/liquidity")
def api_liquidity():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    try:
        info = get_info(ticker)
    except Exception as e:
        return jsonify({"error": f"获取失败: {e}"}), 502
    price = _num(info.get("currentPrice") or info.get("regularMarketPrice"))
    adtv = _num(info.get("averageVolume") or info.get("averageDailyVolume10Day"))
    bid, ask = _num(info.get("bid")), _num(info.get("ask"))
    shares = _num(info.get("sharesOutstanding"))
    dollar_vol = price * adtv if price and adtv else None
    spread_bps = None
    if bid and ask and ask > 0 and bid > 0:
        mid = (bid + ask) / 2
        if mid > 0:
            spread_bps = (ask - bid) / mid * 10000
    turnover = (adtv / shares * 100) if adtv and shares else None
    if dollar_vol is None:
        grade, gdesc = "?", "数据不足"
    elif dollar_vol >= 1e9:
        grade, gdesc = "A", "极佳 · 可大额进出"
    elif dollar_vol >= 1e8:
        grade, gdesc = "B", "良好 · 一般机构可承载"
    elif dollar_vol >= 1e7:
        grade, gdesc = "C", "中等 · 注意冲击成本"
    else:
        grade, gdesc = "D", "偏低 · 滑点风险高"
    return jsonify({"ticker": ticker, "adtv": adtv, "dollarVol": dollar_vol, "bid": bid, "ask": ask,
                    "spreadBps": spread_bps, "turnover": turnover, "grade": grade, "gradeDesc": gdesc})


# ============================ API: 大盘 ============================

def _index_snapshot(symbol):
    try:
        df = get_history_df(symbol, "2y")
    except Exception:
        df = None
    if df is None or df.empty:
        return None
    c = df["Close"].dropna()
    price = _num(c.iloc[-1])
    prev = _num(c.iloc[-2]) if len(c) >= 2 else None
    ma50 = _num(c.rolling(50).mean().iloc[-1]) if len(c) >= 50 else None
    ma200 = _num(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else None
    return {"symbol": symbol, "price": price, "changePct": ((price / prev - 1) * 100) if price and prev else None,
            "ma50": ma50, "ma200": ma200,
            "above50": (price > ma50) if (price and ma50) else None,
            "above200": (price > ma200) if (price and ma200) else None,
            "pctVs200": ((price / ma200 - 1) * 100) if (price and ma200) else None,
            "rsi": _num(rsi(c).iloc[-1]) if len(c) >= 15 else None}


def _market_overview():
    spx = _index_snapshot("^GSPC")
    ndx = _index_snapshot("^IXIC")
    vix = None
    try:
        vdf = get_history_df("^VIX", "1mo")
        if vdf is not None and not vdf.empty:
            vix = _num(vdf["Close"].dropna().iloc[-1])
    except Exception:
        pass
    a200 = [x["above200"] for x in (spx, ndx) if x and x["above200"] is not None]
    env, env_class = "Choppy · 震荡", "hold"
    if a200:
        if all(a200):
            env, env_class = "Bull · 多头", "buy"
        elif not any(a200):
            env, env_class = "Bear · 空头", "sell"
    parts = []
    if vix is not None:
        parts.append(max(0, min(100, (40 - vix) / (40 - 12) * 100)))
    if spx and spx["pctVs200"] is not None:
        parts.append(max(0, min(100, (spx["pctVs200"] + 10) / 20 * 100)))
    if spx and spx["rsi"] is not None:
        parts.append(max(0, min(100, spx["rsi"])))
    sentiment = round(sum(parts) / len(parts)) if parts else None
    slabel = "—" if sentiment is None else ("极度恐慌" if sentiment < 25 else "恐慌" if sentiment < 45 else "中性" if sentiment <= 55 else "贪婪" if sentiment <= 75 else "极度贪婪")
    return {"spx": spx, "ndx": ndx, "vix": vix, "environment": env, "environmentClass": env_class,
            "sentiment": sentiment, "sentimentLabel": slabel,
            "note": "情绪为 VIX/指数趋势/RSI 合成代理"}


@app.route("/api/market")
def api_market():
    return jsonify(_market_overview())


# ============================ API: 自选股批量行情 ============================

@app.route("/api/quotes")
def api_quotes():
    raw = (request.args.get("tickers") or "").strip().upper()
    tickers = [t for t in raw.replace(" ", ",").split(",") if t]
    if not tickers:
        return jsonify({"quotes": []})
    out = []
    try:
        data = get_quotes_download(",".join(sorted(set(tickers))))
    except Exception:
        data = None
    for t in tickers:
        price = prev = None
        try:
            sub = data[t] if (data is not None and t in data.columns.get_level_values(0)) else None
            if sub is not None:
                c = sub["Close"].dropna()
                if len(c) >= 1:
                    price = _num(c.iloc[-1])
                if len(c) >= 2:
                    prev = _num(c.iloc[-2])
        except Exception:
            pass
        chg = ((price / prev - 1) * 100) if price and prev else None
        out.append({"ticker": t, "price": price, "changePct": chg})
    return jsonify({"quotes": out})


# ============================ API: 多股对比(stock-correlation) ============================

@app.route("/api/compare")
def api_compare():
    raw = (request.args.get("tickers") or "").strip().upper()
    period = request.args.get("period", "1y")
    tickers = [t for t in dict.fromkeys(raw.replace(" ", ",").split(",")) if t][:8]
    if len(tickers) < 2:
        return jsonify({"error": "至少需要 2 只股票"}), 400
    try:
        data = yf.download(tickers, period=period, auto_adjust=True, progress=False, group_by="column")
        closes = data["Close"] if "Close" in data else data
    except Exception as e:
        return jsonify({"error": f"下载失败: {e}"}), 502
    if isinstance(closes, pd.Series):
        closes = closes.to_frame()
    closes = closes.dropna(axis=1, how="all").dropna()
    if closes.empty or closes.shape[1] < 2:
        return jsonify({"error": "有效数据不足"}), 404

    cols = list(closes.columns)
    series = {}
    for t in cols:
        base = closes[t].iloc[0]
        series[t] = [{"time": idx.strftime("%Y-%m-%d"), "value": round(closes[t].loc[idx] / base * 100, 2)}
                     for idx in closes.index]

    rets = np.log(closes / closes.shift(1)).dropna()
    corr = rets.corr()
    matrix = [[round(_num(corr.loc[a, b]) or 0, 2) for b in cols] for a in cols]

    base_t = cols[0]
    stats = []
    for t in cols:
        total_ret = (closes[t].iloc[-1] / closes[t].iloc[0] - 1) * 100
        vol = rets[t].std() * math.sqrt(252) * 100
        beta = None
        if t != base_t:
            cov = rets[[t, base_t]].cov()
            denom = _num(cov.loc[base_t, base_t])
            if denom:
                beta = _num(cov.loc[t, base_t]) / denom
        stats.append({"ticker": t, "totalReturn": round(total_ret, 1),
                      "annVol": round(vol, 1), "beta": round(beta, 2) if beta is not None else None,
                      "corrToBase": round(_num(corr.loc[t, base_t]) or 0, 2)})
    return jsonify({"tickers": cols, "base": base_t, "series": series,
                    "matrix": matrix, "stats": stats, "observations": len(rets), "period": period})


# ============================ API: 估值(company-valuation + estimate-analysis) ============================

def _compute_dcf(ticker, info):
    try:
        fin = get_financials(ticker)
        inc = fin.get("inc")
        cf = fin.get("cf")
        if inc is None or inc.empty or "Total Revenue" not in inc.index:
            return None
        rev_row = inc.loc["Total Revenue"].dropna().astype(float)
        rev = rev_row[::-1]  # 旧 -> 新
        if len(rev) < 2:
            return None
        hist_cagr = (rev.iloc[-1] / rev.iloc[0]) ** (1 / (len(rev) - 1)) - 1
        y1 = hist_cagr
        try:
            re = fin.get("rev_est")
            if re is not None and "+1y" in re.index:
                g = _num(re.loc["+1y", "growth"])
                if g is not None:
                    y1 = g
        except Exception:
            pass
        y1 = max(min(y1, 0.40), -0.10)
        g_term = 0.025
        path = np.linspace(y1, g_term + 0.01, 5)

        def med_ratio(num_row, denom=rev_row, default=None):
            try:
                return float((num_row / denom).dropna().iloc[:3].median())
            except Exception:
                return default
        if "Operating Income" not in inc.index:
            return None
        ebit_margin = med_ratio(inc.loc["Operating Income"].astype(float))
        if ebit_margin is None:
            return None
        da_pct = med_ratio(cf.loc["Depreciation And Amortization"].abs().astype(float), default=0.03) if (cf is not None and "Depreciation And Amortization" in cf.index) else 0.03
        capex_pct = med_ratio(cf.loc["Capital Expenditure"].abs().astype(float), default=0.04) if (cf is not None and "Capital Expenditure" in cf.index) else 0.04
        nwc_pct = med_ratio(cf.loc["Change In Working Capital"].abs().astype(float), default=0.01) if (cf is not None and "Change In Working Capital" in cf.index) else 0.01
        tax = 0.21

        mcap = _num(info.get("marketCap"))
        shares = _num(info.get("sharesOutstanding"))
        debt = _num(info.get("totalDebt")) or 0
        cash = _num(info.get("totalCash")) or 0
        beta = _num(info.get("beta")) or 1.0
        if not mcap or not shares:
            return None

        rf = 0.045
        try:
            tnx = get_history_df("^TNX", "5d")
            if tnx is not None and not tnx.empty:
                rf = float(tnx["Close"].dropna().iloc[-1]) / 100
        except Exception:
            pass
        erp, kd = 0.055, 0.055
        ke = rf + beta * erp
        e_v = mcap / (mcap + debt)
        wacc = e_v * ke + (1 - e_v) * kd * (1 - tax)
        if wacc <= g_term:
            wacc = g_term + 0.02

        rev_t = float(rev.iloc[-1])
        fcff = []
        for g in path:
            rev_t *= (1 + g)
            nopat = rev_t * ebit_margin * (1 - tax)
            fcff.append(nopat + rev_t * da_pct - rev_t * capex_pct - rev_t * nwc_pct)
        tv = fcff[-1] * (1 + g_term) / (wacc - g_term)
        pv_fcff = sum(f / (1 + wacc) ** (i + 1) for i, f in enumerate(fcff))
        pv_tv = tv / (1 + wacc) ** 5
        ev = pv_fcff + pv_tv
        implied = (ev + cash - debt) / shares
        if implied <= 0 or not math.isfinite(implied):
            return None
        return {"implied": round(implied, 2), "wacc": round(wacc * 100, 2), "termGrowth": round(g_term * 100, 1),
                "rf": round(rf * 100, 2), "ebitMargin": round(ebit_margin * 100, 1),
                "y1Growth": round(y1 * 100, 1), "tvWeight": round(pv_tv / ev * 100, 0)}
    except Exception:
        return None


@app.route("/api/valuation")
def api_valuation():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    try:
        info = get_info(ticker)
    except Exception as e:
        return jsonify({"error": f"获取失败: {e}"}), 502
    price = _num(info.get("currentPrice") or info.get("regularMarketPrice"))

    dcf = _compute_dcf(ticker, info)

    # 相对估值锚:forwardPE × forwardEPS(若有)
    rel = None
    fpe, feps = _num(info.get("forwardPE")), _num(info.get("forwardEps"))
    tpe, teps = _num(info.get("trailingPE")), _num(info.get("trailingEps"))
    if feps and fpe and feps > 0:
        rel = round(feps * fpe, 2)  # = forward price implied by current fwd PE(近似锚)
    # 分析师目标
    tgt = _num(info.get("targetMeanPrice"))

    methods = []
    if dcf:
        methods.append(("DCF 内在价值", dcf["implied"]))
    if tgt:
        methods.append(("分析师平均目标价", tgt))
    if rel:
        methods.append(("远期PE隐含价", rel))
    blended = round(float(np.mean([v for _, v in methods])), 2) if methods else None

    upside = ((blended / price - 1) * 100) if (blended and price) else None
    if upside is None:
        verdict, vclass = "数据不足", "hold"
    elif upside >= 15:
        verdict, vclass = "低估 · Undervalued", "buy"
    elif upside <= -15:
        verdict, vclass = "高估 · Overvalued", "sell"
    else:
        verdict, vclass = "合理 · Fairly valued", "hold"

    # 预期趋势 estimate-analysis
    estimates = []
    revisions = []
    try:
        ee = get_financials(ticker).get("earn_est")
        if ee is not None and not ee.empty:
            label = {"0q": "本季", "+1q": "下季", "0y": "本年", "+1y": "明年"}
            for p in ee.index:
                estimates.append({"period": label.get(p, p), "avg": _num(ee.loc[p, "avg"]),
                                  "low": _num(ee.loc[p, "low"]), "high": _num(ee.loc[p, "high"]),
                                  "growth": _num(ee.loc[p, "growth"]),
                                  "numAnalysts": _num(ee.loc[p, "numberOfAnalysts"])})
    except Exception:
        pass
    try:
        et = get_financials(ticker).get("eps_trend")
        if et is not None and not et.empty:
            label = {"0q": "本季", "+1q": "下季", "0y": "本年", "+1y": "明年"}
            for p in et.index:
                cur = _num(et.loc[p, "current"])
                ago = _num(et.loc[p, "90daysAgo"])
                trend = None
                if cur is not None and ago is not None:
                    if cur > ago * 1.002:
                        trend = "up"
                    elif cur < ago * 0.998:
                        trend = "down"
                    else:
                        trend = "flat"
                revisions.append({"period": label.get(p, p), "current": cur, "ago90": ago, "trend": trend})
    except Exception:
        pass

    return jsonify({"ticker": ticker, "price": price, "dcf": dcf, "relative": rel, "target": tgt,
                    "methods": [{"name": n, "value": v} for n, v in methods], "blended": blended,
                    "upside": round(upside, 1) if upside is not None else None,
                    "verdict": verdict, "verdictClass": vclass,
                    "estimates": estimates, "revisions": revisions,
                    "trailingPE": tpe, "forwardPE": fpe, "trailingEps": teps, "forwardEps": feps,
                    "note": "DCF 为简化 5 年 FCFF 模型,默认参数(ERP 5.5%, 永续 2.5%),仅供参考"})


# ============================ API: 期权(options-payoff) ============================

@app.route("/api/options/expiries")
def api_option_expiries():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    exps = get_option_expiries(ticker)
    if not exps:
        return jsonify({"ticker": ticker, "expiries": [], "spot": _num(get_info(ticker).get("currentPrice"))})
    price = _num(get_info(ticker).get("currentPrice") or get_info(ticker).get("regularMarketPrice"))
    return jsonify({"ticker": ticker, "expiries": exps, "spot": price})


@app.route("/api/options/chain")
def api_option_chain():
    ticker = (request.args.get("ticker") or "").strip().upper()
    expiry = request.args.get("expiry")
    if not ticker or not expiry:
        return jsonify({"error": "缺少 ticker 或 expiry"}), 400
    try:
        oc = get_option_chain(ticker, expiry)
    except Exception as e:
        return jsonify({"error": f"获取期权链失败: {e}"}), 502
    spot = _num(get_info(ticker).get("currentPrice") or get_info(ticker).get("regularMarketPrice"))

    def pack(dfo):
        rows = []
        for _, r in dfo.iterrows():
            rows.append({"strike": _num(r.get("strike")), "last": _num(r.get("lastPrice")),
                         "bid": _num(r.get("bid")), "ask": _num(r.get("ask")),
                         "iv": _num(r.get("impliedVolatility")), "volume": _num(r.get("volume")),
                         "oi": _num(r.get("openInterest")), "itm": bool(r.get("inTheMoney"))})
        return rows
    calls, puts = pack(oc.calls), pack(oc.puts)
    # 只取 ATM 上下各 ~12 档,减小体积
    if spot:
        def near(rows):
            rows = [r for r in rows if r["strike"] is not None]
            rows.sort(key=lambda r: abs(r["strike"] - spot))
            return sorted(rows[:24], key=lambda r: r["strike"])
        calls, puts = near(calls), near(puts)
    return jsonify({"ticker": ticker, "expiry": expiry, "spot": spot, "calls": calls, "puts": puts})


# ============================ API: 期权墙(Max Pain / OI 墙 / GEX) ============================

def _norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def _bs_gamma(S, K, T, r, sigma):
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + sigma * sigma / 2) * T) / (sigma * math.sqrt(T))
    return _norm_pdf(d1) / (S * sigma * math.sqrt(T))


@cached(300)
def _risk_free():
    try:
        tnx = get_history_df("^TNX", "5d")
        if tnx is not None and not tnx.empty:
            return float(tnx["Close"].dropna().iloc[-1]) / 100
    except Exception:
        pass
    return 0.045


@app.route("/api/options/walls")
def api_option_walls():
    ticker = (request.args.get("ticker") or "").strip().upper()
    expiry = request.args.get("expiry")
    if not ticker or not expiry:
        return jsonify({"error": "缺少 ticker 或 expiry"}), 400
    try:
        oc = get_option_chain(ticker, expiry)
    except Exception as e:
        return jsonify({"error": f"获取期权链失败: {e}"}), 502
    spot = _num(get_info(ticker).get("currentPrice") or get_info(ticker).get("regularMarketPrice"))
    if not spot:
        return jsonify({"error": "无法获取现价"}), 502

    # 剩余到期(年)
    try:
        exp_t = time.mktime(time.strptime(expiry, "%Y-%m-%d"))
        days = max(0.5, (exp_t - time.time()) / 86400)
    except Exception:
        days = 7
    T = days / 365.0
    r = _risk_free()

    calls = oc.calls.fillna(0)
    puts = oc.puts.fillna(0)

    # 按行权价聚合 OI / Volume / IV
    def by_strike(df):
        m = {}
        for _, row in df.iterrows():
            k = _num(row.get("strike"))
            if k is None:
                continue
            m[k] = {"oi": _num(row.get("openInterest")) or 0, "vol": _num(row.get("volume")) or 0,
                    "iv": _num(row.get("impliedVolatility")) or 0}
        return m
    cmap, pmap = by_strike(calls), by_strike(puts)
    strikes = sorted(set(cmap) | set(pmap))
    if not strikes:
        return jsonify({"error": "无有效行权价"}), 404

    oi_call = sum(v["oi"] for v in cmap.values())
    oi_put = sum(v["oi"] for v in pmap.values())
    vol_call = sum(v["vol"] for v in cmap.values())
    vol_put = sum(v["vol"] for v in pmap.values())
    total_oi = oi_call + oi_put
    total_vol = vol_call + vol_put

    # 墙/MaxPain/GEX 的权重依据:正常用未平仓量 OI;但当 OI 缺失或远小于成交量
    # (如 OI 隔夜未更新、新上市合约、近月 OI=0 但成交活跃)时,改用 Volume 作代理,
    # 否则会出现「明明有大量成交却看不到墙」。NVDA 近月即此情形。
    use_vol = total_oi < 0.1 * total_vol
    basis = "volume" if use_vol else "oi"
    wfield = "vol" if use_vol else "oi"
    def w(m, k):
        return m.get(k, {}).get(wfield, 0)

    # Max Pain:使期权买方总收益(=卖方赔付)最小的结算价
    # 候选结算价限定在现价 ±40% 区间:避免远端稀疏持仓把痛点拉到离谱位置(如 spot=1132 却报 70)
    band = [k for k in strikes if 0.6 * spot <= k <= 1.4 * spot] or strikes
    def payout(S):
        tot = 0.0
        for k in strikes:
            tot += w(cmap, k) * max(S - k, 0)
            tot += w(pmap, k) * max(k - S, 0)
        return tot
    max_pain = min(band, key=payout)

    # 墙(按所选依据:OI 或成交量)
    call_walls = sorted([{"strike": k, "oi": w(cmap, k)} for k in cmap if w(cmap, k) > 0],
                        key=lambda x: -x["oi"])[:6]
    put_walls = sorted([{"strike": k, "oi": w(pmap, k)} for k in pmap if w(pmap, k) > 0],
                       key=lambda x: -x["oi"])[:6]

    # GEX:每档 (权重_call·γ - 权重_put·γ)·S²·1%·100(权重缺 OI 时用成交量代理 → gamma flow)
    gex_by_strike = []
    for k in strikes:
        civ = cmap.get(k, {}).get("iv", 0)
        piv = pmap.get(k, {}).get("iv", 0)
        gc = _bs_gamma(spot, k, T, r, civ) if civ > 0 else 0
        gp = _bs_gamma(spot, k, T, r, piv) if piv > 0 else 0
        gex = (w(cmap, k) * gc - w(pmap, k) * gp) * spot * spot * 0.01 * 100
        gex_by_strike.append({"strike": k, "gex": gex})
    net_gex = sum(x["gex"] for x in gex_by_strike)

    # Gamma Flip:净 gamma 随假设现价 S 变化、由负转正的价位(零伽马)
    def gex_at(S):
        tot = 0.0
        for k in strikes:
            civ = cmap.get(k, {}).get("iv", 0)
            piv = pmap.get(k, {}).get("iv", 0)
            gc = _bs_gamma(S, k, T, r, civ) if civ > 0 else 0
            gp = _bs_gamma(S, k, T, r, piv) if piv > 0 else 0
            tot += (w(cmap, k) * gc - w(pmap, k) * gp)
        return tot * S * S
    gamma_flip = None
    lo_s, hi_s = spot * 0.6, spot * 1.4
    steps = 80
    prev_s = lo_s
    prev_v = gex_at(prev_s)
    best = None
    for i in range(1, steps + 1):
        s = lo_s + (hi_s - lo_s) * i / steps
        v = gex_at(s)
        if prev_v == 0 or (prev_v < 0 < v) or (prev_v > 0 > v):
            cross = prev_s if v == prev_v else prev_s + (s - prev_s) * (0 - prev_v) / (v - prev_v)
            if best is None or abs(cross - spot) < abs(best - spot):
                best = cross
        prev_s, prev_v = s, v
    if best is not None:
        gamma_flip = round(best, 2)

    # 画图档位:取「权重最大的若干档(即墙)」∪「现价附近若干档」,确保墙一定出现在图里
    def wt_of(k):
        return w(cmap, k) + w(pmap, k)
    top = sorted(strikes, key=lambda k: -wt_of(k))[:28]
    near_spot = sorted(strikes, key=lambda k: abs(k - spot))[:18]
    sel = sorted(set(top) | set(near_spot))
    dist = [{"strike": k, "call": w(cmap, k), "put": w(pmap, k)} for k in sel]
    sel_set = set(sel)
    gex_dist = [x for x in gex_by_strike if x["strike"] in sel_set]

    note = ("本到期日 OI 缺失/远小于成交量,墙·MaxPain·GEX 改用『成交量』作代理(更反映当日新建仓)。"
            if use_vol else
            "墙·MaxPain·GEX 基于未平仓量 OI。") + "GEX 用 BS gamma 估算,Gamma Flip 为净GEX过零点近似。"

    return jsonify({
        "ticker": ticker, "expiry": expiry, "spot": spot, "daysToExpiry": round(days, 1),
        "basis": basis,
        "maxPain": max_pain, "maxPainVsSpot": round((max_pain / spot - 1) * 100, 2),
        "callWalls": call_walls, "putWalls": put_walls,
        "netGex": net_gex, "gammaFlip": gamma_flip,
        "pcRatioOI": round(oi_put / oi_call, 2) if oi_call else None,
        "pcRatioVol": round(vol_put / vol_call, 2) if vol_call else None,
        "totalOI": total_oi, "totalVol": total_vol,
        "oiDist": dist, "gexDist": gex_dist,
        "note": note,
    })


# ============================ API: 市场热力图 ============================

SECTOR_ETFS = {
    "科技 Technology": "XLK", "通信 Comm. Services": "XLC", "可选消费 Cons. Disc.": "XLY",
    "必需消费 Cons. Staples": "XLP", "金融 Financials": "XLF", "医疗 Health Care": "XLV",
    "工业 Industrials": "XLI", "能源 Energy": "XLE", "原材料 Materials": "XLB",
    "公用事业 Utilities": "XLU", "房地产 Real Estate": "XLRE",
}

HEATMAP_UNIVERSE = {
    "科技 Technology": ["AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "CRM", "ADBE", "AMD", "CSCO", "ACN", "QCOM", "TXN", "INTC", "IBM", "NOW"],
    "通信 Comm. Services": ["GOOGL", "META", "NFLX", "DIS", "TMUS", "VZ", "T", "CMCSA"],
    "可选消费 Cons. Disc.": ["AMZN", "TSLA", "HD", "MCD", "NKE", "SBUX", "LOW", "BKNG"],
    "必需消费 Cons. Staples": ["WMT", "PG", "KO", "PEP", "COST", "MDLZ", "PM"],
    "金融 Financials": ["BRK-B", "JPM", "V", "MA", "BAC", "WFC", "GS", "MS", "AXP", "SPGI"],
    "医疗 Health Care": ["LLY", "UNH", "JNJ", "MRK", "ABBV", "PFE", "TMO", "ABT", "DHR"],
    "工业 Industrials": ["GE", "CAT", "HON", "UNP", "BA", "RTX", "UPS", "DE"],
    "能源 Energy": ["XOM", "CVX", "COP", "SLB", "EOG"],
    "原材料 Materials": ["LIN", "SHW", "FCX", "NEM"],
    "公用事业 Utilities": ["NEE", "DUK", "SO"],
    "房地产 Real Estate": ["PLD", "AMT", "EQIX"],
}


# ---- 板块配置(任务4):用户可自定义板块与成分股,同股可归多板块。----
# 首次为空时用内置默认作种子;之后完全由 DB 驱动。compute_heatmap 据此分组。

def _seed_sectors_if_empty(con):
    n = con.execute("SELECT COUNT(*) FROM user_sectors").fetchone()[0]
    if n:
        return
    for i, (sector, tickers) in enumerate(HEATMAP_UNIVERSE.items()):
        con.execute("INSERT OR IGNORE INTO user_sectors(name, etf, sort) VALUES(?,?,?)",
                    (sector, SECTOR_ETFS.get(sector), i))
        for t in tickers:
            con.execute("INSERT OR IGNORE INTO user_sector_members(sector, ticker) VALUES(?,?)",
                        (sector, t.strip().upper()))
    con.commit()


def get_sector_config():
    """返回 [{name, etf, tickers:[...]}, ...],按 sort 排序。空则用内置默认种子初始化。"""
    con = _cache_db()
    try:
        _seed_sectors_if_empty(con)
        secs = con.execute("SELECT name, etf FROM user_sectors ORDER BY sort, name").fetchall()
        out = []
        for name, etf in secs:
            members = [r[0] for r in con.execute(
                "SELECT ticker FROM user_sector_members WHERE sector=? ORDER BY ticker", (name,))]
            out.append({"name": name, "etf": (etf or None), "tickers": members})
    finally:
        con.close()
    return out


def save_sector_config(sectors):
    """整体覆盖保存。sectors=[{name, etf, tickers:[...]}]。保存后失效热力图缓存。"""
    con = _cache_db()
    try:
        con.execute("DELETE FROM user_sectors")
        con.execute("DELETE FROM user_sector_members")
        for i, s in enumerate(sectors):
            name = (s.get("name") or "").strip()
            if not name:
                continue
            etf = (s.get("etf") or "").strip().upper() or None
            con.execute("INSERT OR REPLACE INTO user_sectors(name, etf, sort) VALUES(?,?,?)", (name, etf, i))
            seen = set()
            for t in (s.get("tickers") or []):
                t = (t or "").strip().upper()
                if t and t not in seen:
                    seen.add(t)
                    con.execute("INSERT OR IGNORE INTO user_sector_members(sector, ticker) VALUES(?,?)", (name, t))
        con.commit()
    finally:
        con.close()
    _invalidate_heatmap_cache()


# ---- 热力图持久化缓存(任务1):盘后用缓存不重拉,盘中按需/定时刷新 ----
HEATMAP_OPEN_TTL = 300        # 盘中:>5 分钟才允许自动重拉(force 例外)
_HEATMAP_MEM = {"data": None, "epoch": 0.0}


def _read_heatmap_cache():
    """读持久化热力图(settings 里存 JSON)。返回 (data|None, epoch)。"""
    if _HEATMAP_MEM["data"] is not None:
        return _HEATMAP_MEM["data"], _HEATMAP_MEM["epoch"]
    con = _cache_db()
    try:
        rows = dict(con.execute(
            "SELECT key, value FROM settings WHERE key IN('heatmap_json','heatmap_epoch')").fetchall())
    finally:
        con.close()
    raw = rows.get("heatmap_json")
    if not raw:
        return None, 0.0
    try:
        data = json.loads(raw)
        epoch = float(rows.get("heatmap_epoch") or 0)
    except (ValueError, TypeError):
        return None, 0.0
    _HEATMAP_MEM["data"], _HEATMAP_MEM["epoch"] = data, epoch
    return data, epoch


def _write_heatmap_cache(data, epoch):
    _HEATMAP_MEM["data"], _HEATMAP_MEM["epoch"] = data, epoch
    con = _cache_db()
    try:
        con.execute("INSERT INTO settings(key,value) VALUES('heatmap_json',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (json.dumps(data),))
        con.execute("INSERT INTO settings(key,value) VALUES('heatmap_epoch',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(epoch),))
        con.commit()
    finally:
        con.close()


def _invalidate_heatmap_cache():
    _HEATMAP_MEM["data"], _HEATMAP_MEM["epoch"] = None, 0.0
    con = _cache_db()
    try:
        con.execute("DELETE FROM settings WHERE key IN('heatmap_json','heatmap_epoch')")
        con.commit()
    finally:
        con.close()


def _heatmap_needs_refetch(epoch, force):
    """盘后:有缓存且已是收盘后数据 → 永不重拉(force 也不);无缓存 → 必拉一次。
    盘中:force 立即拉;否则距上次>TTL 才拉。"""
    if epoch <= 0:
        return True                                   # 从未缓存:必须拉一次
    if _market_open():
        return force or (time.time() - epoch > HEATMAP_OPEN_TTL)
    # 盘后:若缓存是在『最近一次收盘』之后抓的,即为最终收盘数据,直接复用,不受 force 影响
    et = datetime.now(ZoneInfo("America/New_York"))
    close_dt = datetime.combine(date.fromisoformat(last_expected_session()),
                                datetime.min.time(), tzinfo=ZoneInfo("America/New_York")) \
        .replace(hour=16, minute=10)
    return epoch < close_dt.timestamp()               # 缓存早于上次收盘 → 补拉一次最终数据


@cached(21600, keep_empty=False)
def _mcaps_cached(tickers_key):
    """市值变化慢,整体缓存 6 小时,避免每次热力图都打 ~100 次 fast_info(降 yahoo 压力)。"""
    tickers = tickers_key.split(",")
    with ThreadPoolExecutor(max_workers=16) as ex:
        return dict(zip(tickers, ex.map(_mcap, tickers)))


def _mcap(tk):
    try:
        fi = yf.Ticker(tk).fast_info
        mc = getattr(fi, "market_cap", None)
        if mc:
            return float(mc)
        lp, sh = getattr(fi, "last_price", None), getattr(fi, "shares", None)
        if lp and sh:
            return float(lp) * float(sh)
    except Exception:
        pass
    return None


def _compute_heatmap_fresh():
    """实际抓取并计算热力图(用用户板块配置)。仅在需要刷新时调用。"""
    config = get_sector_config()
    all_t = sorted({t for s in config for t in s["tickers"]})
    if not all_t:
        return {"sectors": [], "sectorEtfs": [], "asof": time.strftime("%Y-%m-%d %H:%M"),
                "note": "未配置任何板块/成分股"}
    try:
        data = yf.download(all_t, period="5d", auto_adjust=False, progress=False, group_by="ticker")
    except Exception:
        data = None

    def change_of(t):
        try:
            c = data[t]["Close"].dropna()
            vol = data[t]["Volume"].dropna()
            if len(c) >= 2:
                chg = (c.iloc[-1] / c.iloc[-2] - 1) * 100
                dvol = float(c.iloc[-1]) * float(vol.iloc[-1]) if len(vol) else None
                return _num(chg), _num(c.iloc[-1]), dvol
        except Exception:
            pass
        return None, None, None

    # 全市场只算一次每只股票的涨跌/价格/市值,再按板块复用(同股可归多板块)
    mcaps = _mcaps_cached(",".join(all_t)) or {}
    stock_info = {}
    for t in all_t:
        chg, last, dvol = change_of(t)
        if chg is None:
            continue
        size = mcaps.get(t) or dvol or 1e9
        stock_info[t] = {"ticker": t, "change": round(chg, 2), "price": last, "size": round(size / 1e9, 2)}

    sectors = []
    for s in config:
        children = [dict(stock_info[t]) for t in s["tickers"] if t in stock_info]
        if not children:
            continue
        tot = sum(c["size"] for c in children)
        wavg = sum(c["change"] * c["size"] for c in children) / tot if tot else 0
        children.sort(key=lambda c: -c["size"])
        sectors.append({"sector": s["name"], "etf": s["etf"],
                        "change": round(wavg, 2), "size": round(tot, 1), "stocks": children})

    # 板块 ETF 行情(仅对配置里指定了 ETF 的板块)
    etf_list = sorted({s["etf"] for s in config if s["etf"]})
    etf_perf = {}
    if etf_list:
        try:
            edata = yf.download(etf_list, period="5d", auto_adjust=False, progress=False, group_by="ticker")
            for e in etf_list:
                try:
                    c = edata[e]["Close"].dropna()
                    if len(c) >= 2:
                        etf_perf[e] = round((c.iloc[-1] / c.iloc[-2] - 1) * 100, 2)
                except Exception:
                    pass
        except Exception:
            pass
    sector_etfs = [{"sector": s["name"], "etf": s["etf"], "change": etf_perf.get(s["etf"])}
                   for s in config if s["etf"]]
    sector_etfs.sort(key=lambda x: (x["change"] is None, -(x["change"] or 0)))

    return {"sectors": sectors, "sectorEtfs": sector_etfs,
            "asof": time.strftime("%Y-%m-%d %H:%M"), "note": "面积≈市值(十亿美元),颜色=当日涨跌幅"}


def compute_heatmap(force=False):
    """市场时段感知缓存:盘后用持久化缓存不重拉;盘中按需(force)/超 TTL 才重算。"""
    data, epoch = _read_heatmap_cache()
    if not _heatmap_needs_refetch(epoch, force) and data is not None:
        out = dict(data)
        out["cached"] = True
        out["marketOpen"] = _market_open()
        return out
    fresh = _compute_heatmap_fresh()
    # 仅当抓到有效板块数据才落库(避免把限流空结果缓存住);否则回退旧缓存
    if fresh.get("sectors"):
        _write_heatmap_cache(fresh, time.time())
    elif data is not None:
        out = dict(data)
        out["cached"] = True
        out["stale"] = True
        out["marketOpen"] = _market_open()
        return out
    fresh["cached"] = False
    fresh["marketOpen"] = _market_open()
    return fresh


@app.route("/api/heatmap")
def api_heatmap():
    force = request.args.get("force") in ("1", "true", "yes")
    return jsonify(compute_heatmap(force=force))


@app.route("/api/heatmap/sectors", methods=["GET", "POST"])
def api_heatmap_sectors():
    """GET: 返回当前板块配置 + 内置默认(供前端「恢复默认」)。POST: 整体覆盖保存。"""
    if request.method == "GET":
        defaults = [{"name": s, "etf": SECTOR_ETFS.get(s), "tickers": list(ts)}
                    for s, ts in HEATMAP_UNIVERSE.items()]
        return jsonify({"sectors": get_sector_config(), "defaults": defaults})
    d = request.get_json(force=True, silent=True) or {}
    sectors = d.get("sectors")
    if not isinstance(sectors, list):
        return jsonify({"error": "需要 sectors 数组"}), 400
    save_sector_config(sectors)
    return jsonify({"ok": True, "sectors": get_sector_config()})


# ============================ API: 持仓 / 交易记录 ============================

def _live_prices(tickers):
    """批量取最新价 + 前收,复用 yf.download。"""
    out = {}
    tickers = [t for t in tickers if t]
    if not tickers:
        return out
    try:
        data = yf.download(list(set(tickers)), period="5d", auto_adjust=False, progress=False, group_by="ticker")
    except Exception:
        data = None
    for t in set(tickers):
        try:
            sub = data[t] if (data is not None and t in data.columns.get_level_values(0)) else None
            c = sub["Close"].dropna() if sub is not None else None
            out[t] = {"price": _num(c.iloc[-1]) if c is not None and len(c) else None,
                      "ma20": _num(c.rolling(20).mean().iloc[-1]) if c is not None and len(c) >= 20 else None}
        except Exception:
            out[t] = {"price": None, "ma20": None}
    return out


@app.route("/api/positions", methods=["GET", "POST"])
def api_positions():
    db = get_db()
    if request.method == "POST":
        d = request.get_json(force=True, silent=True) or {}
        tk = (d.get("ticker") or "").strip().upper()
        if not tk or not d.get("shares") or not d.get("entry"):
            return jsonify({"error": "ticker / shares / entry 必填"}), 400
        today = time.strftime("%Y-%m-%d")
        add_shares, add_entry = float(d["shares"]), float(d["entry"])
        mp = _num(d.get("manual_price"))
        # 加仓合并:同标的已有未平仓仓位则按加权成本并入,不再新建一行
        existing = db.execute("SELECT * FROM positions WHERE ticker=? AND status='open' ORDER BY id LIMIT 1",
                              (tk,)).fetchone()
        if existing:
            new_shares = existing["shares"] + add_shares
            new_entry = (existing["shares"] * existing["entry"] + add_shares * add_entry) / new_shares
            qn = f"{add_shares:g}" if float(add_shares).is_integer() else f"{add_shares}"
            tn = f"{new_shares:g}" if float(new_shares).is_integer() else f"{new_shares}"
            tag = f"[加仓 {today} +{qn}@{add_entry:g}|共{tn}股 均价{round(new_entry, 4):g}]"
            new_note = ((existing["note"] + " ") if existing["note"] else "") + tag
            new_mp = mp if mp is not None else existing["manual_price"]   # 提供新现价则更新,否则保留
            db.execute("UPDATE positions SET shares=?, entry=?, note=?, manual_price=? WHERE id=?",
                       (new_shares, new_entry, new_note, new_mp, existing["id"]))
            pos_id = existing["id"]
        else:
            cur = db.execute("INSERT INTO positions(ticker,shares,entry,stop,target,opened_at,status,note,manual_price) VALUES(?,?,?,?,?,?, 'open', ?, ?)",
                             (tk, add_shares, add_entry, _num(d.get("stop")), _num(d.get("target")),
                              today, d.get("note") or "", mp))
            pos_id = cur.lastrowid
        db.execute("INSERT INTO trades(position_id,ticker,action,shares,price,pl,at,note) VALUES(?,?,'buy',?,?,NULL,?,?)",
                   (pos_id, tk, add_shares, add_entry, today, d.get("note") or ""))
        db.commit()
        return jsonify({"ok": True, "merged": bool(existing)})
    # GET: 带实时盈亏
    return jsonify(_portfolio_state(db))


def _portfolio_state(db):
    """组合实时状态:持仓(含派生值)+ 汇总 + 买卖流水。供 /api/positions 与每日快照复用。
    db 需 row_factory=sqlite3.Row。不依赖 flask 请求上下文,后台线程亦可调用。"""
    rows = [dict(r) for r in db.execute("SELECT * FROM positions ORDER BY id DESC").fetchall()]
    open_rows = [r for r in rows if r["status"] == "open"]
    # 仅对「自动行情」的持仓拉 yfinance;手填现价的(期权/港股)跳过
    live = _live_prices([r["ticker"] for r in open_rows if r.get("manual_price") is None])
    arow = db.execute("SELECT value FROM settings WHERE key='accountValue'").fetchone()
    acct = float(arow["value"]) if arow and arow["value"] else None
    total_mv = total_cost = total_pl = total_risk = 0.0
    for r in rows:
        manual = r.get("manual_price") is not None
        r["manual"] = manual
        if r["status"] != "open":
            lp, price = {}, r.get("exit_price")
        elif manual:
            lp, price = {}, r.get("manual_price")
        else:
            lp = live.get(r["ticker"], {})
            price = lp.get("price")
        r["price"] = price
        r["ma20"] = lp.get("ma20")
        cost = r["shares"] * r["entry"]
        r["cost"] = cost
        if price is not None:
            mv = r["shares"] * price
            r["marketValue"] = mv
            r["pl"] = mv - cost
            r["plPct"] = (price / r["entry"] - 1) * 100
            # 距止损:现价回落到止损的跌幅(负数=还要跌多少才触发止损),遵循绿涨红跌
            r["toStopPct"] = ((r["stop"] / price - 1) * 100) if r["stop"] else None
            # 风险敞口($):从现价回落到止损会亏多少(现价−止损)×股数;止损≥成本则为锁定的利润回吐
            r["riskDollar"] = ((price - r["stop"]) * r["shares"]) if r["stop"] else None
            if r["stop"] and r["status"] == "open":
                rps = r["entry"] - r["stop"]
                if rps > 0:                                   # 止损低于成本:正常 R 倍数
                    r["rMultiple"] = (price - r["entry"]) / rps
                else:                                         # 止损≥成本:已锁定利润,R 无意义
                    r["lockedPct"] = (r["stop"] / r["entry"] - 1) * 100
        if r["status"] == "open":
            total_cost += cost                                  # 成本不依赖现价,务必全量累加
            total_mv += mv if price is not None else cost        # 缺现价时回退用成本,避免漏算市值
            if price is not None:
                total_pl += mv - cost
                if r["stop"]:
                    total_risk += max(0.0, (price - r["stop"]) * r["shares"])
    summary = {"openCount": len(open_rows), "totalMarketValue": total_mv, "totalCost": total_cost,
               "totalPL": total_pl, "totalPLPct": (total_pl / total_cost * 100) if total_cost else None,
               "totalRisk": total_risk, "account": acct,
               "investedPct": (total_cost / acct * 100) if acct else None,   # 已投入成本占账户
               "marketPct": (total_mv / acct * 100) if acct else None,        # 持仓市值占账户=仓位占账户
               "riskPct": (total_risk / acct * 100) if acct else None}
    # 倒序:最新交易在最上面;同一天「买入」沉到当天底部(它是当天最早的动作),其余按 id 倒序
    trades = [dict(r) for r in db.execute(
        "SELECT * FROM trades ORDER BY at DESC, (action='buy') ASC, id DESC").fetchall()]
    return {"positions": rows, "summary": summary, "trades": trades}


@app.route("/api/positions/<int:pid>", methods=["PUT", "DELETE"])
def api_position_one(pid):
    db = get_db()
    if request.method == "DELETE":
        db.execute("DELETE FROM positions WHERE id=?", (pid,))
        db.commit()
        return jsonify({"ok": True})
    d = request.get_json(force=True, silent=True) or {}
    if d.get("action") == "close":
        row = db.execute("SELECT ticker, shares, entry, note FROM positions WHERE id=?", (pid,)).fetchone()
        if row is None:
            return jsonify({"error": "持仓不存在"}), 404
        px = _num(d.get("exit_price"))
        if px is None:
            return jsonify({"error": "平仓价格必填"}), 400
        cur_shares = row["shares"]
        qty = _num(d.get("shares"))
        if qty is None or qty <= 0 or qty >= cur_shares:
            qty = cur_shares                                   # 默认/超额视为清仓
        today = time.strftime("%Y-%m-%d")
        pl = (px - row["entry"]) * qty
        sell_cur = db.execute("INSERT INTO trades(position_id,ticker,action,shares,price,pl,at,note) VALUES(?,?,'sell',?,?,?,?,?)",
                              (pid, row["ticker"], qty, px, pl, today, d.get("note") or ""))
        if qty >= cur_shares:                                  # 清仓
            db.execute("UPDATE positions SET status='closed', exit_price=?, closed_at=?, shares=? WHERE id=?",
                       (px, today, qty, pid))
            # 清仓:累计该标的所有卖出的已实现盈亏,写进本笔流水备注,便于复盘看总盈利
            total = db.execute("SELECT COALESCE(SUM(pl),0) FROM trades WHERE position_id=? AND action='sell'",
                               (pid,)).fetchone()[0] or 0.0
            sgn = "+" if total >= 0 else ""
            base = (d.get("note") or "").strip()
            clr = f"[清仓·累计已实现 {sgn}{round(total, 2):g}]"
            db.execute("UPDATE trades SET note=? WHERE id=?",
                       ((base + " " + clr) if base else clr, sell_cur.lastrowid))
        else:                                                  # 部分平仓:减仓并在条目里注明现有仓位
            remain = cur_shares - qty
            qn = (f"{qty:g}") if float(qty).is_integer() else f"{qty}"
            rn = (f"{remain:g}") if float(remain).is_integer() else f"{remain}"
            sign = "+" if pl >= 0 else ""
            tag = f"[减仓 {today} 卖{qn}@{px:g} {sign}{round(pl, 2):g}|剩{rn}股]"
            new_note = ((row["note"] + " ") if row["note"] else "") + tag
            db.execute("UPDATE positions SET shares=?, note=? WHERE id=?", (remain, new_note, pid))
        db.commit()
        return jsonify({"ok": True})
    else:
        for field in ("shares", "entry", "stop", "target", "note", "manual_price"):
            if field in d:
                db.execute(f"UPDATE positions SET {field}=? WHERE id=?", (_num(d[field]) if field != "note" else d[field], pid))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/trades/<int:tid>", methods=["DELETE"])
def api_trade_one(tid):
    db = get_db()
    db.execute("DELETE FROM trades WHERE id=?", (tid,))
    db.commit()
    return jsonify({"ok": True})


# ============================ 每日复盘:组合快照 ============================

def take_portfolio_snapshot(date_str):
    """留存当日组合快照(JSON)。同一天再次调用覆盖 payload,但保留已写的 review。可在后台线程调用。"""
    con = _cache_db()
    con.row_factory = sqlite3.Row
    try:
        payload = json.dumps(_portfolio_state(con), ensure_ascii=False,
                             default=lambda o: float(o) if hasattr(o, "__float__") else None)
        con.execute("INSERT INTO snapshots(date,payload,created_at) VALUES(?,?,?) "
                    "ON CONFLICT(date) DO UPDATE SET payload=excluded.payload, created_at=excluded.created_at",
                    (date_str, payload, time.strftime("%Y-%m-%d %H:%M:%S")))
        con.commit()
    finally:
        con.close()
    return date_str


@app.route("/api/snapshots", methods=["GET"])
def api_snapshots():
    rows = get_db().execute(
        "SELECT date, created_at, (review IS NOT NULL AND TRIM(review)<>'') AS hr "
        "FROM snapshots ORDER BY date DESC").fetchall()
    return jsonify({"snapshots": [
        {"date": r["date"], "created_at": r["created_at"], "hasReview": bool(r["hr"])} for r in rows]})


@app.route("/api/snapshots/take", methods=["POST"])
def api_snapshot_take():
    d = datetime.now(ZoneInfo("America/New_York")).date().isoformat()   # 以美东交易日为快照日期
    take_portfolio_snapshot(d)
    return jsonify({"ok": True, "date": d})


@app.route("/api/snapshots/<date>", methods=["GET"])
def api_snapshot_one(date):
    r = get_db().execute("SELECT date, payload, review, created_at FROM snapshots WHERE date=?", (date,)).fetchone()
    if r is None:
        return jsonify({"error": "无该日快照"}), 404
    return jsonify({"date": r["date"], "createdAt": r["created_at"], "review": r["review"] or "",
                    "state": json.loads(r["payload"]) if r["payload"] else None})


@app.route("/api/snapshots/<date>/review", methods=["POST"])
def api_snapshot_review(date):
    db = get_db()
    d = request.get_json(force=True, silent=True) or {}
    db.execute("INSERT INTO snapshots(date,review,created_at) VALUES(?,?,?) "      # 允许给无快照日期先写复盘
               "ON CONFLICT(date) DO UPDATE SET review=excluded.review",
               (date, d.get("review", ""), time.strftime("%Y-%m-%d %H:%M:%S")))
    db.commit()
    return jsonify({"ok": True})


# ============================ API: 自选股 / 设置 / 预警(持久化) ============================

@app.route("/api/watchlist", methods=["GET", "POST", "DELETE"])
def api_watchlist():
    db = get_db()
    if request.method == "GET":
        rows = db.execute("SELECT ticker FROM watchlist ORDER BY sort, added_at, ticker").fetchall()
        return jsonify({"watchlist": [r["ticker"] for r in rows]})
    tk = ((request.get_json(force=True, silent=True) or {}).get("ticker") or request.args.get("ticker") or "").strip().upper()
    if not tk:
        return jsonify({"error": "缺少 ticker"}), 400
    if request.method == "POST":
        # 新加入的排到末尾
        db.execute("INSERT OR IGNORE INTO watchlist(ticker, added_at, sort) VALUES(?, ?, COALESCE((SELECT MAX(sort)+1 FROM watchlist), 0))",
                   (tk, time.strftime("%Y-%m-%d %H:%M:%S")))
    else:
        db.execute("DELETE FROM watchlist WHERE ticker=?", (tk,))
    db.commit()
    rows = db.execute("SELECT ticker FROM watchlist ORDER BY sort, added_at, ticker").fetchall()
    return jsonify({"watchlist": [r["ticker"] for r in rows]})


@app.route("/api/watchlist/reorder", methods=["POST"])
def api_watchlist_reorder():
    """按传入的 ticker 顺序重排自选股(拖拽排序)。body: {order: [TICKER, ...]}"""
    db = get_db()
    order = (request.get_json(force=True, silent=True) or {}).get("order") or []
    for i, tk in enumerate(order):
        db.execute("UPDATE watchlist SET sort=? WHERE ticker=?", (i, (tk or "").strip().upper()))
    db.commit()
    rows = db.execute("SELECT ticker FROM watchlist ORDER BY sort, added_at, ticker").fetchall()
    return jsonify({"watchlist": [r["ticker"] for r in rows]})


@app.route("/api/notes", methods=["GET", "POST", "DELETE"])
def api_notes():
    """个股研究笔记。GET ?ticker= 列出某只标的的笔记(新→旧);
    POST {ticker, body} 新增一条并自动记当前时间;DELETE ?id= 删除一条。"""
    db = get_db()
    if request.method == "GET":
        tk = (request.args.get("ticker") or "").strip().upper()
        if not tk:
            return jsonify({"notes": []})
        rows = db.execute("SELECT id, ticker, body, created_at FROM stock_notes WHERE ticker=? ORDER BY id DESC", (tk,)).fetchall()
        return jsonify({"notes": [dict(r) for r in rows]})
    if request.method == "POST":
        d = request.get_json(force=True, silent=True) or {}
        tk = (d.get("ticker") or "").strip().upper()
        body = (d.get("body") or "").strip()
        if not tk or not body:
            return jsonify({"error": "ticker / body 必填"}), 400
        cur = db.execute("INSERT INTO stock_notes(ticker, body, created_at) VALUES(?,?,?)",
                         (tk, body, time.strftime("%Y-%m-%d %H:%M:%S")))
        db.commit()
        row = db.execute("SELECT id, ticker, body, created_at FROM stock_notes WHERE id=?", (cur.lastrowid,)).fetchone()
        return jsonify({"note": dict(row)})
    # DELETE
    nid = request.args.get("id")
    if nid:
        db.execute("DELETE FROM stock_notes WHERE id=?", (nid,))
        db.commit()
    return jsonify({"ok": True})


def _get_setting(key):
    row = get_db().execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


@app.route("/api/settings", methods=["GET", "POST"])
def api_settings():
    db = get_db()
    if request.method == "GET":
        rows = db.execute("SELECT key, value FROM settings").fetchall()
        return jsonify({r["key"]: r["value"] for r in rows})
    d = request.get_json(force=True, silent=True) or {}
    for k, v in d.items():
        db.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, str(v)))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/alerts", methods=["GET", "POST", "DELETE"])
def api_alerts():
    db = get_db()
    if request.method == "POST":
        d = request.get_json(force=True, silent=True) or {}
        tk = (d.get("ticker") or "").strip().upper()
        if not tk or not d.get("kind"):
            return jsonify({"error": "ticker / kind 必填"}), 400
        db.execute("INSERT INTO alerts(ticker,kind,level,note,active,created_at) VALUES(?,?,?,?,1,?)",
                   (tk, d["kind"], _num(d.get("level")), d.get("note") or "", time.strftime("%Y-%m-%d %H:%M")))
        db.commit()
        return jsonify({"ok": True})
    if request.method == "DELETE":
        aid = request.args.get("id")
        if aid:
            db.execute("DELETE FROM alerts WHERE id=?", (aid,))
            db.commit()
        return jsonify({"ok": True})
    # GET: 评估触发状态
    rows = [dict(r) for r in db.execute("SELECT * FROM alerts WHERE active=1 ORDER BY id DESC").fetchall()]
    live = _live_prices([r["ticker"] for r in rows])
    for r in rows:
        lp = live.get(r["ticker"], {})
        price, ma20 = lp.get("price"), lp.get("ma20")
        r["price"] = price
        triggered = False
        if price is not None:
            if r["kind"] == "above" and r["level"]:
                triggered = price >= r["level"]
            elif r["kind"] in ("below", "stop") and r["level"]:
                triggered = price <= r["level"]
            elif r["kind"] == "break_ma20" and ma20:
                triggered = price < ma20
        r["triggered"] = triggered
    return jsonify({"alerts": rows})


@app.route("/api/notify/status")
def api_notify_status():
    """前端用:Telegram 推送是否开启 + webhook 是否已配置(决定开关可用性与提示)。"""
    return jsonify({"enabled": _push_enabled(), "configured": bool(_tg_relay_webhook())})


@app.route("/api/notify/test", methods=["POST"])
def api_notify_test():
    """发送一条测试推送(验证 webhook 链路);不受开关影响,但需已配置 webhook。"""
    if not _tg_relay_webhook():
        return jsonify({"ok": False, "error": "服务器未配置 TG_RELAY_WEBHOOK(联系部署方设置)"}), 400
    ok, info = _tg_send("✅ 个股看板测试推送 · Telegram 链路正常")
    return jsonify({"ok": ok, "info": str(info)})


@app.route("/api/report/daily/preview")
def api_report_preview():
    """生成盘后总结文本但不推送(用于预览/确认格式)。"""
    return jsonify({"text": build_daily_report()})


@app.route("/api/report/daily/send", methods=["POST"])
def api_report_send():
    """立即组装并推送盘后总结(手动触发;需已配置 webhook,不受日报开关限制)。"""
    if not _tg_relay_webhook():
        return jsonify({"ok": False, "error": "服务器未配置 TG_RELAY_WEBHOOK"}), 400
    ok, info, text = send_daily_report()
    return jsonify({"ok": ok, "info": str(info), "text": text})


@app.route("/api/cache/refresh", methods=["POST"])
def api_cache_refresh():
    """手动触发 K 线缓存增量更新。?ticker=AAPL 更新单只;无参则刷新自选股∪持仓。"""
    t = (request.args.get("ticker") or "").strip().upper()
    if t:
        try:
            df = _yf_history(t, "1mo", "1d")
            n = _store_bars(t, df) if (df is not None and not df.empty) else 0
        except Exception as e:
            return jsonify({"error": str(e)}), 502
        return jsonify({"ticker": t, "updated": n})
    return jsonify({"refreshed": refresh_tracked_bars()})


@app.route("/api/cache/status")
def api_cache_status():
    """缓存概览:每个标的已存的日线区间与上次拉取时间。"""
    con = _cache_db()
    try:
        rows = con.execute("""SELECT m.ticker, m.first_date, m.last_date, m.last_fetch,
            (SELECT COUNT(*) FROM bars b WHERE b.ticker=m.ticker) AS bars
            FROM bars_meta m ORDER BY m.ticker""").fetchall()
    finally:
        con.close()
    out = []
    for r in rows:
        try:
            lf = datetime.fromtimestamp(float(r[3] or 0)).strftime("%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError, OSError):
            lf = None
        out.append({"ticker": r[0], "first_date": r[1], "last_date": r[2],
                    "bars": r[4], "last_fetch": lf})
    return jsonify({"tracked": tracked_tickers(), "cached": out})


# ============================ 前端 ============================

@app.route("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>个股看板 · Stock Dashboard</title>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
  :root{--bg:#0d1117;--panel:#161b22;--panel2:#1c2230;--border:#21262d;--text:#e6edf3;--muted:#8b949e;--green:#26a69a;--red:#ef5350;--accent:#58a6ff;--yellow:#f6c343}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif}
  a{color:var(--accent);text-decoration:none;cursor:pointer} a:hover{text-decoration:underline}
  .marketbar{display:flex;align-items:center;gap:14px;padding:8px 24px;background:#0a0d12;border-bottom:1px solid var(--border);flex-wrap:wrap;font-size:13px}
  .mb-item{display:flex;gap:6px;align-items:baseline}.mb-item .lbl{color:var(--muted)}
  .badge{padding:3px 10px;border-radius:6px;font-weight:600;font-size:12px}
  .buy{background:rgba(38,166,154,.15);color:var(--green)}.sell{background:rgba(239,83,80,.15);color:var(--red)}.hold{background:rgba(246,195,67,.15);color:var(--yellow)}
  .gauge{display:flex;align-items:center;gap:8px}
  .gauge .bar{width:120px;height:8px;border-radius:4px;background:linear-gradient(90deg,#ef5350,#f6c343,#26a69a);position:relative}
  .gauge .dot{position:absolute;top:-3px;width:14px;height:14px;border-radius:50%;background:#fff;border:2px solid #0a0d12;transform:translateX(-50%)}
  h1{font-size:18px;margin:0;font-weight:600}
  /* ---- 全局左侧栏布局 ---- */
  .app{display:flex;align-items:flex-start}
  .sidebar{flex:0 0 212px;position:sticky;top:0;align-self:stretch;min-height:calc(100vh - 41px);max-height:calc(100vh - 41px);overflow-y:auto;background:#0a0d12;border-right:1px solid var(--border);padding:14px 12px;display:flex;flex-direction:column;gap:14px}
  .sidebar .brand{font-size:18px;font-weight:700;padding:2px 4px 6px}
  .sidenav{display:flex;flex-direction:column;gap:6px}
  .sidenav button{background:transparent;border:1px solid var(--border);color:var(--muted);padding:10px 14px;border-radius:8px;cursor:pointer;font-size:14px;font-weight:500;text-align:left}
  .sidenav button:hover{color:var(--text);border-color:var(--accent)}
  .sidenav button.active{background:var(--accent);color:#fff;border-color:var(--accent)}
  .search{display:flex;gap:6px}
  .search input{background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px 10px;border-radius:8px;font-size:14px;width:100%;min-width:0;text-transform:uppercase}
  .search button{background:var(--accent);border:none;color:#fff;padding:8px 12px;border-radius:8px;cursor:pointer;font-size:14px;white-space:nowrap}
  .side-watch{display:flex;flex-direction:column;gap:6px;min-height:0}
  .side-watch-hd{display:flex;align-items:center;justify-content:space-between}
  .side-watch-hd .lbl{color:var(--muted);font-size:12px;font-weight:600}
  .wl-sort{background:none;border:1px solid var(--border);color:var(--muted);font-size:11px;padding:2px 7px;border-radius:6px;cursor:pointer}
  .wl-sort:hover{border-color:var(--accent);color:var(--text)}
  .watchlist{display:flex;flex-direction:column;gap:5px;overflow-y:auto}
  .wchip{display:flex;gap:6px;align-items:center;justify-content:space-between;background:var(--panel);border:1px solid var(--border);padding:7px 10px;border-radius:8px;cursor:pointer;white-space:nowrap;font-size:13px;touch-action:none;user-select:none}
  .wchip:hover{border-color:var(--accent)}
  .wchip.dragging{opacity:.7;border-color:var(--accent);box-shadow:0 4px 14px rgba(0,0,0,.45)}
  .wchip .wtk{font-weight:600}
  .wchip .wq{display:flex;align-items:center;gap:5px;font-size:12px}
  .wchip .wprice{color:var(--text)}
  .wchip .x{color:var(--muted);font-size:11px;padding-left:4px}.wchip .x:hover{color:var(--red)}
  .notes-box{display:flex;flex-direction:column;gap:8px}
  .note-input{width:100%;box-sizing:border-box;background:var(--panel);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:9px 11px;font:inherit;font-size:13px;resize:vertical}
  .note-input:focus{outline:none;border-color:var(--accent)}
  .note-actions{display:flex;justify-content:flex-end}
  .note-save{background:var(--accent);border:none;color:#fff;padding:6px 16px;border-radius:7px;cursor:pointer;font-size:13px}
  .note-save:hover{opacity:.9}
  .note-item{background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:9px 11px;display:flex;flex-direction:column;gap:4px}
  .note-item .note-meta{display:flex;justify-content:space-between;align-items:center;color:var(--muted);font-size:11px}
  .note-item .note-body{white-space:pre-wrap;word-break:break-word;font-size:13px;line-height:1.5}
  .note-item .note-del{color:var(--muted);cursor:pointer;font-size:11px}.note-item .note-del:hover{color:var(--red)}
  .notes-empty{color:var(--muted);font-size:12px;padding:4px 0}
  .content{flex:1 1 auto;min-width:0;padding:20px 24px;max-width:1520px}
  @media(max-width:880px){
    .app{flex-direction:column}
    .sidebar{flex-basis:auto;width:100%;position:static;min-height:0;max-height:none;border-right:none;border-bottom:1px solid var(--border)}
    .sidenav{flex-direction:row;flex-wrap:wrap}
    .watchlist{flex-direction:row;flex-wrap:wrap}
    .wchip{justify-content:flex-start}
  }
  .tabs{display:flex;gap:4px;border-bottom:1px solid var(--border);margin-bottom:18px;flex-wrap:wrap}
  .tabs button{background:transparent;border:none;border-bottom:2px solid transparent;color:var(--muted);padding:10px 16px;cursor:pointer;font-size:14px}
  .tabs button.active{color:var(--text);border-bottom-color:var(--accent)}
  .chips{display:flex;gap:6px;flex-wrap:wrap}.chip{background:var(--panel);border:1px solid var(--border);padding:5px 10px;border-radius:16px;cursor:pointer;font-size:12px;color:var(--muted)}.chip:hover{color:var(--text);border-color:var(--accent)}
  .quote-head{display:flex;align-items:baseline;gap:14px;flex-wrap:wrap;margin-bottom:4px}.quote-head .name{font-size:22px;font-weight:600}.quote-head .tk{color:var(--muted);font-size:14px}
  .star{cursor:pointer;font-size:20px}
  .price-row{display:flex;align-items:baseline;gap:14px;margin-bottom:12px}.price{font-size:34px;font-weight:700}.chg{font-size:16px;font-weight:600}
  .meta{color:var(--muted);font-size:13px;margin-bottom:14px}
  .controls{display:flex;gap:6px;margin:10px 0;flex-wrap:wrap;align-items:center}
  .controls button{background:var(--panel);border:1px solid var(--border);color:var(--muted);padding:5px 12px;border-radius:6px;cursor:pointer;font-size:12px}.controls button.active{background:var(--accent);color:#fff;border-color:var(--accent)}
  .ma-toggles{display:flex;gap:10px;margin-left:8px;flex-wrap:wrap;font-size:12px}.ma-toggles label{display:flex;align-items:center;gap:4px;cursor:pointer;color:var(--muted)}
  #chart{width:100%;height:440px;border:1px solid var(--border);border-radius:10px;overflow:hidden}
  .section-title{font-size:15px;font-weight:600;margin:30px 0 10px;display:flex;align-items:center;gap:8px}.section-title .tag{font-size:11px;color:var(--muted);font-weight:400;background:var(--panel);padding:2px 8px;border-radius:10px}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:12px}
  .card{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px}.card .k{color:var(--muted);font-size:12px;margin-bottom:6px}.card .v{font-size:18px;font-weight:600}
  table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:8px 10px;border-bottom:1px solid var(--border)}th{color:var(--muted);font-weight:500}
  .green{color:var(--green)}.red{color:var(--red)}.muted{color:var(--muted)}
  .pass{color:var(--green);font-weight:600}.fail{color:var(--red);font-weight:600}.unk{color:var(--muted)}
  .sepa-head{display:flex;gap:14px;flex-wrap:wrap;align-items:center;margin-bottom:12px}
  .scorebig{font-size:26px;font-weight:700}.gradechip{font-size:20px;font-weight:700;padding:4px 14px;border-radius:8px}
  .gA{background:rgba(38,166,154,.18);color:var(--green)}.gB{background:rgba(88,166,255,.18);color:var(--accent)}.gC{background:rgba(246,195,67,.18);color:var(--yellow)}.gD{background:rgba(239,83,80,.18);color:var(--red)}.gq{background:var(--panel2);color:var(--muted)}
  .news-item{padding:10px 0;border-bottom:1px solid var(--border)}.news-item .t{font-size:14px}.news-item .m{font-size:12px;color:var(--muted);margin-top:3px}
  .loading{color:var(--muted);padding:30px;text-align:center}.error{color:var(--red);padding:20px;background:rgba(239,83,80,.1);border-radius:8px}
  .small{font-size:11px;color:var(--muted);margin-top:6px}
  .two-col{display:grid;grid-template-columns:1fr 1fr;gap:24px}@media(max-width:880px){.two-col{grid-template-columns:1fr}}
  .disclaimer{color:var(--muted);font-size:11px;margin-top:36px;text-align:center}
  #alertBanner:not(:empty){padding:8px 24px;background:rgba(239,83,80,.12);border-bottom:1px solid var(--red)}
  #alertBanner .ab{color:var(--red);font-size:13px;font-weight:600;margin-right:14px}
  .hidden{display:none}
  /* 对比 */
  #cmpChart{width:100%;height:420px;border:1px solid var(--border);border-radius:10px;overflow:hidden}
  .cmpbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:14px}
  .cmpbar input{background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px 12px;border-radius:8px;font-size:13px;width:320px;text-transform:uppercase}
  .heat-cell{text-align:center;font-weight:600}
  /* 期权 */
  .legpick td{cursor:pointer}.legpick tr:hover{background:var(--panel2)}
  #payoff{width:100%;height:360px;border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-top:12px}
  .leg-tag{display:inline-flex;gap:6px;align-items:center;background:var(--panel);border:1px solid var(--border);padding:4px 10px;border-radius:8px;font-size:12px;margin:3px}
  /* 热力图 */
  #treemap,#sectreemap{width:100%;height:640px;border:1px solid var(--border);border-radius:10px}
  .sectorgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:8px;margin-top:12px}
  .stile{border-radius:10px;padding:14px;color:#fff;cursor:default}
  .stile .s1{font-size:13px;opacity:.9}.stile .s2{font-size:22px;font-weight:700;margin-top:4px}.stile .s3{font-size:11px;opacity:.8}
  .pform{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:14px;max-width:940px;margin-bottom:18px}
  .pform label{display:block;color:var(--muted);font-size:12px;margin-bottom:6px}
  .pform .row{display:flex;gap:6px}
  .pform input,.pform select{background:var(--panel);border:1px solid var(--border);color:var(--text);padding:9px 11px;border-radius:8px;font-size:14px;width:100%}
  .pform select{width:auto;flex:0 0 auto}
  .pform .hint{font-size:11px;color:var(--muted);margin-top:4px;min-height:14px}
  .bindbox{display:flex;gap:14px;flex-wrap:wrap;align-items:center;margin:8px 0 14px}
  .shares-big{font-size:38px;font-weight:800}
  .ov-grid{display:flex;gap:22px;align-items:flex-start}
  .ov-main{flex:1 1 auto;min-width:0}
  .ov-side{flex:0 0 360px}
  .sizer-panel{position:sticky;top:16px;background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:16px 18px}
  .sizer-panel .pform{grid-template-columns:1fr 1fr;max-width:none;gap:10px;margin-bottom:12px}
  .sizer-panel .pform>div:first-child{grid-column:1/-1}
  .sizer-panel .grid{grid-template-columns:1fr 1fr;gap:8px}
  .sizer-panel table{font-size:11px;max-width:100%!important}
  .sizer-panel .shares-big{font-size:30px}
  @media(max-width:1080px){.ov-grid{flex-direction:column}.ov-side{flex-basis:auto;width:100%}.sizer-panel{position:static}.sizer-panel .pform{grid-template-columns:repeat(auto-fill,minmax(180px,1fr))}}
  .chart-wrap{position:relative}
  .ohlc-legend{position:absolute;top:8px;left:10px;z-index:3;pointer-events:none;font-size:12px;color:var(--muted);background:rgba(22,27,34,.55);padding:2px 8px;border-radius:6px;white-space:nowrap}
  .ohlc-legend b{font-weight:600}
  .ma-stops-row{grid-column:1/-1;margin-top:-4px}
  .ma-stops{display:flex;flex-wrap:wrap;gap:5px;align-items:center;margin-top:6px}
  .ma-pill{background:var(--bg);border:1px solid var(--border);color:var(--text);padding:3px 8px;border-radius:6px;cursor:pointer;font-size:11px}
  .ma-pill:hover{border-color:var(--accent);color:var(--accent)}
  #sub-edit{background:var(--panel);border:1px solid var(--border);color:var(--muted)}#sub-edit:hover{border-color:var(--accent);color:var(--text)}
  #sector-editor{border:1px solid var(--border);border-radius:10px;padding:16px;margin-bottom:14px;background:var(--panel2)}
  .sec-edit-row{display:grid;grid-template-columns:1fr 150px auto;gap:8px;align-items:center;background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:8px}
  .sec-edit-row .se-tk{grid-column:1/-1}
  .sec-edit-row input,.sec-edit-row textarea{background:var(--bg);border:1px solid var(--border);color:var(--text);padding:7px 9px;border-radius:6px;font-size:13px;width:100%}
  .sec-edit-row textarea{min-height:46px;resize:vertical;text-transform:uppercase;font-family:inherit}
  .capbar{background:linear-gradient(135deg,var(--panel2),var(--panel));border:1px solid var(--border);border-radius:12px;padding:16px 20px;margin-bottom:18px;display:flex;flex-direction:column;gap:10px;max-width:640px}
  .capbar .caprow{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
  .capbar .caplabel{color:var(--muted);font-size:14px;font-weight:600;margin-right:4px}
  .capbar .capcur{font-size:24px;font-weight:700;color:var(--muted)}
  .capbar input{background:var(--bg);border:1px solid var(--border);color:var(--text);font-size:26px;font-weight:800;padding:5px 12px;border-radius:8px;width:200px}
  .capbar .capmeta{display:flex;gap:20px;flex-wrap:wrap;font-size:13px;color:var(--muted)}
  .subtabs{display:flex;gap:6px;margin:14px 0}
  .subtabs button{background:var(--panel);border:1px solid var(--border);color:var(--muted);padding:6px 14px;border-radius:8px;cursor:pointer;font-size:13px}.subtabs button.active{background:var(--accent);color:#fff;border-color:var(--accent)}
</style>
</head>
<body>

<div class="marketbar" id="marketbar"><span class="muted">大盘加载中…</span></div>
<div id="alertBanner"></div>

<div class="app">
 <!-- 全局左侧栏:品牌 + 导航 + 搜索 + 自选股 -->
 <aside class="sidebar">
   <div class="brand">📈 看板</div>
   <nav class="sidenav">
     <button id="nav-stock" class="active" onclick="switchPage('stock')">📊 个股看板</button>
     <button id="nav-positions" onclick="switchPage('positions')">💼 持仓</button>
     <button id="nav-review" onclick="switchPage('review')">📓 每日复盘</button>
     <button id="nav-heatmap" onclick="switchPage('heatmap')">🔥 市场热力图</button>
   </nav>
   <div class="search" id="stockSearch">
     <input id="tickerInput" placeholder="代码 如 AAPL" value="AAPL" />
     <button onclick="loadTicker()">查询</button>
   </div>
   <div class="side-watch">
     <div class="side-watch-hd"><span class="lbl">自选股</span><button class="wl-sort" onclick="sortWatchByChange()" title="按当日涨跌幅降序排列">涨跌幅 ↓</button></div>
     <div class="watchlist" id="watchlist"></div>
   </div>
 </aside>

 <main class="content">
  <!-- 个股看板页 -->
  <div id="page-stock">
    <div class="tabs">
      <button id="tab-overview" class="active" onclick="switchTab('overview')">概览</button>
      <button id="tab-valuation" onclick="switchTab('valuation')">估值</button>
      <button id="tab-options" onclick="switchTab('options')">期权墙</button>
      <button id="tab-compare" onclick="switchTab('compare')">多股对比</button>
    </div>
    <div id="tabc-overview"><div class="loading">加载中…</div></div>
    <div id="tabc-valuation" class="hidden"></div>
    <div id="tabc-options" class="hidden"></div>
    <div id="tabc-compare" class="hidden"></div>
  </div>

  <!-- 持仓页 -->
  <div id="page-positions" class="hidden"><div class="loading">加载持仓…</div></div>

  <!-- 每日复盘页 -->
  <div id="page-review" class="hidden"><div class="loading">加载复盘…</div></div>

  <!-- 市场热力图页 -->
  <div id="page-heatmap" class="hidden">
    <div class="subtabs">
      <button id="sub-stocks" class="active" onclick="switchHeat('stocks')">个股热力图</button>
      <button id="sub-sectors" onclick="switchHeat('sectors')">板块热力图</button>
      <button id="sub-edit" onclick="toggleSectorEditor()">⚙ 编辑板块</button>
      <span id="heat-status" class="muted" style="margin-left:auto;font-size:12px;align-self:center"></span>
      <button class="muted" style="cursor:pointer" onclick="loadHeatmap(true)">↻ 刷新</button>
    </div>
    <div id="sector-editor" class="hidden"></div>
    <div id="heat-stocks"><div class="loading">热力图加载中(首次约 10-20 秒)…</div></div>
    <div id="heat-sectors" class="hidden"></div>
    <div class="small" id="heat-note"></div>
  </div>

  <div class="disclaimer">
    数据来源 Yahoo Finance(yfinance),非实时、有延迟,仅供研究与学习,不构成投资建议。<br>
    SEPA 基于 Minervini 趋势模板;估值为简化 DCF;RS/情绪/热力图面积为代理算法。
  </div>
 </main>
</div>

<script>
const POPULAR=["AAPL","TSLA","NVDA","MSFT","GOOGL","AMZN","META","AMD"];
const MA_COLORS={"5":"#f6c343","10":"#ff9f40","20":"#58a6ff","50":"#a78bfa","200":"#e6edf3"};
const MA_DEFAULT_ON={"5":false,"10":false,"20":true,"50":true,"200":true};
let chart,candleSeries,volSeries,maSeries={},curTicker="AAPL",curPeriod="1d",curTab="overview",curPage="stock";
let loadedTabs={};
let watchlist=[];

const fmtNum=(n,d=2)=>n==null?"—":Number(n).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const fmtBig=n=>{if(n==null)return"—";const a=Math.abs(n);if(a>=1e12)return(n/1e12).toFixed(2)+"T";if(a>=1e9)return(n/1e9).toFixed(2)+"B";if(a>=1e6)return(n/1e6).toFixed(2)+"M";if(a>=1e3)return(n/1e3).toFixed(2)+"K";return fmtNum(n)};
const fmtPct=n=>n==null?"—":(n>=0?"+":"")+n.toFixed(2)+"%";
const j=async u=>{const r=await fetch(u);return r.json();};
const heatColor=c=>{if(c==null)return"#30363d";const x=Math.max(-3,Math.min(3,c))/3;if(x>=0){const g=Math.round(60+x*100);return`rgb(${Math.round(40-x*10)},${100+Math.round(x*66)},${Math.round(74+x*20)})`;}const r=-x;return`rgb(${120+Math.round(r*119)},${Math.round(60-r*30)},${Math.round(70-r*30)})`;};

// ---------- 页面/标签切换 ----------
function switchPage(p){
  curPage=p;
  try{localStorage.setItem("curPage",p);}catch(e){}   // 记住当前页,刷新后恢复
  ["stock","positions","review","heatmap"].forEach(x=>{
    document.getElementById("page-"+x).classList.toggle("hidden",p!==x);
    document.getElementById("nav-"+x).classList.toggle("active",p===x);
  });
  if(p==="heatmap" && !window._heatLoaded) loadHeatmap();
  if(p==="positions") loadPositions();
  if(p==="review") loadReview();
}
function switchTab(t){
  curTab=t;
  ["overview","valuation","options","compare"].forEach(x=>{
    document.getElementById("tab-"+x).classList.toggle("active",x===t);
    document.getElementById("tabc-"+x).classList.toggle("hidden",x!==t);
  });
  if(t==="valuation"&&loadedTabs.valuation!==curTicker)loadValuation();
  if(t==="options"&&loadedTabs.options!==curTicker)loadOptions();
  if(t==="compare"&&!loadedTabs.compare)loadCompare();
}

// ---------- 大盘条 ----------
async function loadMarket(){
  try{
    const m=await j("/api/market");
    const idx=(name,o)=>!o?"":`<div class="mb-item"><span class="lbl">${name}</span><b>${fmtNum(o.price)}</b><span class="${(o.changePct||0)>=0?'green':'red'}">${fmtPct(o.changePct)}</span></div>`;
    const vixCls=m.vix==null?"":(m.vix>=25?"red":(m.vix<=16?"green":"hold"));
    document.getElementById("marketbar").innerHTML=
      idx("标普500",m.spx)+idx("纳指",m.ndx)+
      `<div class="mb-item"><span class="lbl">VIX</span><b class="${vixCls}">${fmtNum(m.vix)}</b></div>`+
      `<div class="mb-item"><span class="lbl">环境</span><span class="badge ${m.environmentClass}">${m.environment}</span></div>`+
      `<div class="gauge"><span class="lbl">情绪 ${m.sentiment??"—"} · ${m.sentimentLabel}</span><div class="bar"><div class="dot" style="left:${m.sentiment??50}%"></div></div></div>`;
  }catch(e){document.getElementById("marketbar").innerHTML='<span class="muted">大盘数据获取失败</span>';}
}

// ---------- 设置(后端 SQLite,localStorage 兜底) ----------
let settings={};
async function loadSettings(){try{settings=await j("/api/settings")||{};}catch(e){settings={};}}
function getSetting(k,def){if(settings[k]!=null&&settings[k]!=="")return settings[k];const ls=localStorage.getItem(k);return ls!=null?ls:def;}
function setSetting(k,v){settings[k]=String(v);localStorage.setItem(k,v);fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({[k]:String(v)})}).catch(()=>{});}

// ---------- 自选股(后端持久化) ----------
function inWatch(t){return watchlist.includes(t.toUpperCase());}
async function loadWatch(){try{const d=await j("/api/watchlist");watchlist=d.watchlist||[];}catch(e){}renderWatch();}
async function toggleWatch(t){t=t.toUpperCase();const method=inWatch(t)?"DELETE":"POST";try{const d=await(await fetch("/api/watchlist?ticker="+t,{method})).json();watchlist=d.watchlist||watchlist;}catch(e){}renderWatch();const s=document.getElementById("starBtn");if(s)s.textContent=inWatch(curTicker)?"★":"☆";}
async function renderWatch(){
  const el=document.getElementById("watchlist");if(!el)return;
  if(!watchlist.length){el.innerHTML='<span class="lbl" style="font-size:12px;padding:4px">空 · 搜索后点 ☆ 加入</span>';return;}
  el.innerHTML=watchlist.map(t=>`<span class="wchip" id="w-${t}" onclick="wchipClick('${t}')"><span class="wtk">${t}</span><span class="muted">…</span></span>`).join("");
  attachWatchDnD();
  const q=await j("/api/quotes?tickers="+watchlist.join(","));
  window._wq={};
  q.quotes.forEach(x=>{window._wq[x.ticker]=x;const c=document.getElementById("w-"+x.ticker);if(c)c.innerHTML=`<span class="wtk">${x.ticker}</span><span class="wq"><span class="wprice">${fmtNum(x.price)}</span><span class="${(x.changePct||0)>=0?'green':'red'}">${fmtPct(x.changePct)}</span><span class="x" onclick="event.stopPropagation();toggleWatch('${x.ticker}')">✕</span></span>`;});
}
// 按当日涨跌幅降序重排自选股(无行情数据的排末尾),并持久化新顺序
async function sortWatchByChange(){
  if(!watchlist.length)return;
  let q=window._wq;
  if(!q){try{const d=await j("/api/quotes?tickers="+watchlist.join(","));q={};(d.quotes||[]).forEach(x=>q[x.ticker]=x);window._wq=q;}catch(e){return;}}
  const chg=t=>{const v=q[t]&&q[t].changePct;return v==null?-Infinity:v;};
  watchlist=[...watchlist].sort((a,b)=>chg(b)-chg(a));
  renderWatch();persistWatchOrder(watchlist);
}
// 点击自选股加载;若刚结束一次拖拽则吞掉这次点击
function wchipClick(t){if(window._wSuppressClick){window._wSuppressClick=false;return;}loadTicker(t);}
// 长按自选股方块进入拖拽,上下移动改变顺序,松手立即落定。
// move/up 监听挂在 document 上(只注册一次),拖拽中重排 DOM 不会丢失 pointerup。
let _wdrag=null;
function attachWatchDnD(){
  const cont=document.getElementById("watchlist");if(!cont)return;
  cont.querySelectorAll(".wchip").forEach(el=>{
    el.addEventListener("pointerdown",e=>{
      if(e.target.classList&&e.target.classList.contains("x"))return;   // 删除按钮不拖
      window._wSuppressClick=false;
      _wdrag={el,active:false,startY:e.clientY,startX:e.clientX,
        timer:setTimeout(()=>{if(_wdrag){_wdrag.active=true;el.classList.add("dragging");}},350)};
    });
  });
}
function _wdragMove(e){
  if(!_wdrag)return;
  if(!_wdrag.active){ // 长按未触发前移动过大 → 视作滚动/点击,取消
    if(Math.abs(e.clientY-_wdrag.startY)>8||Math.abs(e.clientX-_wdrag.startX)>8){clearTimeout(_wdrag.timer);_wdrag=null;}
    return;}
  e.preventDefault();
  const cont=document.getElementById("watchlist");if(!cont)return;
  const chips=[...cont.querySelectorAll(".wchip")];
  const after=chips.find(c=>c!==_wdrag.el&&e.clientY<c.getBoundingClientRect().top+c.getBoundingClientRect().height/2);
  if(after){if(after!==_wdrag.el.nextSibling)cont.insertBefore(_wdrag.el,after);}
  else if(cont.lastElementChild!==_wdrag.el)cont.appendChild(_wdrag.el);
}
function _wdragEnd(){
  if(!_wdrag)return;clearTimeout(_wdrag.timer);
  if(_wdrag.active){
    _wdrag.el.classList.remove("dragging");
    const cont=document.getElementById("watchlist");
    const order=[...cont.querySelectorAll(".wchip")].map(c=>c.id.slice(2));
    if(order.join()!==watchlist.join()){watchlist=order;persistWatchOrder(order);}
    window._wSuppressClick=true;   // 阻止松手后紧跟的 click 触发加载
  }
  _wdrag=null;
}
document.addEventListener("pointermove",_wdragMove,{passive:false});
document.addEventListener("pointerup",_wdragEnd);
document.addEventListener("pointercancel",_wdragEnd);
async function persistWatchOrder(order){try{await fetch("/api/watchlist/reorder",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({order})});}catch(e){}}

// ---------- 个股笔记 ----------
function _noteEsc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}
async function loadNotes(){
  const list=document.getElementById("notesList");if(!list)return;
  let d;try{d=await j("/api/notes?ticker="+encodeURIComponent(curTicker));}catch(e){list.innerHTML='<div class="notes-empty">加载失败</div>';return;}
  renderNotes(d.notes||[]);
}
function renderNotes(notes){
  const list=document.getElementById("notesList");if(!list)return;
  if(!notes.length){list.innerHTML='<div class="notes-empty">还没有笔记,写点什么吧。</div>';return;}
  list.innerHTML=notes.map(n=>`<div class="note-item"><div class="note-meta"><span>🕒 ${n.created_at||""}</span><span class="note-del" onclick="deleteNote(${n.id})">删除</span></div><div class="note-body">${_noteEsc(n.body)}</div></div>`).join("");
}
async function saveNote(){
  const ta=document.getElementById("noteInput");if(!ta)return;
  const body=ta.value.trim();if(!body)return;
  const tk=curTicker;
  try{
    const r=await(await fetch("/api/notes",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({ticker:tk,body})})).json();
    if(r.error){alert(r.error);return;}
    ta.value="";
    if(tk===curTicker)loadNotes();
  }catch(e){alert("保存失败");}
}
async function deleteNote(id){
  if(!confirm("删除这条笔记?"))return;
  try{await fetch("/api/notes?id="+id,{method:"DELETE"});loadNotes();}catch(e){}
}
// Ctrl/⌘+Enter 在笔记框内快捷保存
document.addEventListener("keydown",e=>{
  if((e.ctrlKey||e.metaKey)&&e.key==="Enter"&&e.target&&e.target.id==="noteInput"){e.preventDefault();saveNote();}
});

// ---------- 主入口 ----------
async function loadTicker(t){
  if(curPage!=="stock")switchPage("stock");
  curTicker=t?t:document.getElementById("tickerInput").value.trim().toUpperCase();
  if(!curTicker)return;
  document.getElementById("tickerInput").value=curTicker;
  loadedTabs={};
  switchTab("overview");
  document.getElementById("tabc-overview").innerHTML='<div class="loading">加载 '+curTicker+' …</div>';
  const q=await j("/api/quote?ticker="+encodeURIComponent(curTicker));
  if(q.error){document.getElementById("tabc-overview").innerHTML='<div class="error">'+q.error+'</div>';return;}
  window._curPrice=q.price;
  renderOverview(q);
  loadChart(curPeriod);loadSepa();loadEarnings();loadLiquidity();loadNews();loadDecision();loadAlerts();loadNotes();
}
function card(k,v){return `<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`;}

// ---------- 概览 ----------
function renderOverview(q){
  const up=(q.change||0)>=0,cls=up?"green":"red",f=q.financials,a=q.analyst;
  const recClass=!a.recommendation?"hold":(/buy/.test(a.recommendation)?"buy":(/sell|underperform/.test(a.recommendation)?"sell":"hold"));
  const maToggles=Object.keys(MA_COLORS).map(w=>`<label><input type="checkbox" ${MA_DEFAULT_ON[w]?"checked":""} onchange="toggleMA('${w}',this.checked)"><span style="color:${MA_COLORS[w]}">MA${w}</span></label>`).join("");
  document.getElementById("tabc-overview").innerHTML=`
   <div class="quote-head"><span class="name">${q.name||q.ticker}</span><span class="tk">${q.ticker} · ${q.exchange||""} · ${q.currency}</span>
     <span class="star" id="starBtn" onclick="toggleWatch(curTicker)">${inWatch(q.ticker)?"★":"☆"}</span></div>
   <div class="price-row"><span class="price">${fmtNum(q.price)}</span><span class="chg ${cls}">${up?"▲":"▼"} ${fmtNum(q.change)} (${fmtPct(q.changePct)})</span></div>
   <div class="meta">${q.sector||""}${q.industry?" · "+q.industry:""} &nbsp;|&nbsp; 开 ${fmtNum(q.open)} · 高 ${fmtNum(q.dayHigh)} · 低 ${fmtNum(q.dayLow)} · 量 ${fmtBig(q.volume)}</div>
   <div id="decisionCard" style="margin-bottom:16px"></div>
   <div class="ov-grid">
    <div class="ov-main">
     <div class="controls">${["4h","1d","1w"].map(p=>`<button class="${p===curPeriod?'active':''}" onclick="loadChart('${p}')">${p}</button>`).join("")}<span class="ma-toggles">${maToggles}<label style="margin-left:6px"><input type="checkbox" id="wallOverlay" onchange="toggleOptionWall(this.checked)"><span style="color:#f6c343">期权墙</span></label></span></div>
     <div class="chart-wrap"><div id="chart"></div><div id="ohlcLegend" class="ohlc-legend"></div></div>
     <div class="section-title">SEPA 趋势模板分析 <span class="tag">skill: sepa-strategy</span></div>
     <div id="sepa"><div class="loading">分析中…</div></div>
     <div class="two-col">
       <div><div class="section-title">关键财务指标</div><div class="grid">
         ${card("市值",fmtBig(f.marketCap))}${card("市盈率 TTM",fmtNum(f.trailingPE))}${card("预期PE",fmtNum(f.forwardPE))}${card("市净率",fmtNum(f.priceToBook))}
         ${card("EPS",fmtNum(f.eps))}${card("营收TTM",fmtBig(f.revenue))}${card("净利率",f.profitMargin!=null?fmtNum(f.profitMargin*100)+"%":"—")}${card("毛利率",f.grossMargin!=null?fmtNum(f.grossMargin*100)+"%":"—")}
         ${card("Beta",fmtNum(f.beta))}${card("股息率",f.dividendYield!=null?fmtNum(f.dividendYield)+"%":"—")}${card("52周高",fmtNum(f.fiftyTwoWeekHigh))}${card("52周低",fmtNum(f.fiftyTwoWeekLow))}</div></div>
       <div><div class="section-title">分析师评级</div><div class="grid">
         <div class="card"><div class="k">综合评级</div><div class="v"><span class="badge ${recClass}">${a.recommendation||"无"}</span></div></div>
         ${card("平均目标价",fmtNum(a.targetMean))}${card("最高/最低",fmtNum(a.targetHigh)+" / "+fmtNum(a.targetLow))}${card("分析师数",a.numAnalysts!=null?a.numAnalysts:"—")}${card("目标空间",(a.targetMean&&q.price)?fmtPct((a.targetMean/q.price-1)*100):"—")}</div>
         <div class="section-title" style="margin-top:24px">流动性 <span class="tag">skill: stock-liquidity</span></div><div id="liquidity"><div class="loading">分析中…</div></div></div>
     </div>
     <div class="section-title">财报日 / 业绩 <span class="tag">skill: earnings-preview</span></div><div id="earnings"><div class="loading">加载中…</div></div>
     <div class="section-title">价格 / 止损预警 <span class="tag">到价 · 跌破20MA · 止损</span></div><div id="alertsPanel"></div>
     <div class="section-title">重要消息 / 新闻</div><div id="news"><div class="loading">加载中…</div></div>
     <div class="section-title">我的笔记</div>
     <div class="notes-box">
       <textarea id="noteInput" class="note-input" rows="3" placeholder="记点关于 ${q.ticker} 的想法…(Ctrl+Enter 保存)"></textarea>
       <div class="note-actions"><button class="note-save" onclick="saveNote()">保存笔记</button></div>
       <div id="notesList"><div class="loading">加载中…</div></div>
     </div>
    </div>
    <aside class="ov-side"><div id="sizerPanel" class="sizer-panel"></div></aside>
   </div>`;
  initChart();
  renderSizerPanel();
}
// 概览页右侧仓位计算面板
function renderSizerPanel(){
  const el=document.getElementById("sizerPanel");if(!el)return;
  const entry=window._curPrice?window._curPrice.toFixed(2):"";
  window._ovStopAuto=true;                  // 新标的:止损回到「自动取最近均线」
  el.innerHTML=`<div class="section-title" style="margin-top:0;font-size:15px">仓位计算 · 能买多少股 <span class="tag">position-sizing</span></div>
   <div class="muted" style="font-size:12px;margin-bottom:10px">买入价默认现价;止损默认取<b>最靠近且低于现价的均线</b>,也可点下方 MA 快捷设。算<b>同时满足仓位上限与风险上限</b>的最大可买股数。</div>
   ${sizerForm("ov",{entry,maStops:true})}`;
  applyOvDefaultStop();                      // _maLast 还没好时先用 -8% 兜底,loadChart 完成后会再校正
}
// 现有均线中「低于现价且最靠近现价」的那条(默认止损)。无则返回 null。
function nearestMaBelow(price){
  const m=window._maLast||{};let best=null;
  ["10","20","50","200"].forEach(w=>{const v=m[w];if(isFinite(v)&&v<price&&(best===null||v>best))best=v;});
  return best;
}
// 仅当用户未手动改过止损时,把概览仓位计算的默认止损设为最近均线(兜底 -8%)。
function applyOvDefaultStop(){
  if(!window._ovStopAuto)return;
  const price=window._curPrice;if(!price)return;
  const e=document.getElementById("ovStop");if(!e)return;
  const s=nearestMaBelow(price);
  e.value=(s!=null?s:price*0.92).toFixed(2);
  calcSize("ov");renderMaStopBtns("ov");
}
function onStopInput(p){if(p==="ov")window._ovStopAuto=false;calcSize(p);}
function setStop(p,val){const e=document.getElementById(p+"Stop");if(e&&isFinite(val)){if(p==="ov")window._ovStopAuto=false;e.value=Number(val).toFixed(2);calcSize(p);renderMaStopBtns(p);}}
function renderMaStopBtns(p){
  const el=document.getElementById(p+"MaBtns");if(!el)return;
  const m=window._maLast||{};
  const entryEl=document.getElementById(p+"Entry"),entry=entryEl?parseFloat(entryEl.value):NaN;
  const pill=(label,val)=>(val&&isFinite(val))?`<button class="ma-pill" onclick="setStop('${p}',${val})">${label} ${fmtNum(val)}</button>`:"";
  el.innerHTML=`<span class="muted" style="font-size:11px">快捷止损:</span>${pill("MA10",m["10"])}${pill("MA20",m["20"])}${pill("MA50",m["50"])}${pill("MA200",m["200"])}${entry>0?`<button class="ma-pill" onclick="setStop('${p}',${entry*0.92})">-8%</button>`:""}`;
}

function initChart(){
  const el=document.getElementById("chart");if(!el)return;
  chart=LightweightCharts.createChart(el,{layout:{background:{color:"#161b22"},textColor:"#8b949e"},grid:{vertLines:{color:"#21262d"},horzLines:{color:"#21262d"}},rightPriceScale:{borderColor:"#21262d"},timeScale:{borderColor:"#21262d",timeVisible:true,secondsVisible:false},crosshair:{mode:0},width:el.clientWidth,height:440});
  candleSeries=chart.addCandlestickSeries({upColor:"#26a69a",downColor:"#ef5350",borderVisible:false,wickUpColor:"#26a69a",wickDownColor:"#ef5350"});
  maSeries={};Object.keys(MA_COLORS).forEach(w=>{maSeries[w]=chart.addLineSeries({color:MA_COLORS[w],lineWidth:w==="200"?2:1,priceLineVisible:false,lastValueVisible:false,visible:MA_DEFAULT_ON[w]});});
  volSeries=chart.addHistogramSeries({priceFormat:{type:"volume"},priceScaleId:""});volSeries.priceScale().applyOptions({scaleMargins:{top:0.85,bottom:0}});
  // 左上角 OHLC 图例:悬停某根 K 显示其开/高/低/收;未悬停时回落到最新一根
  chart.subscribeCrosshairMove(param=>{
    let bar=null;
    if(param&&param.time&&param.seriesData){const b=param.seriesData.get(candleSeries);if(b)bar=b;}
    if(!bar&&window._ohlcData&&window._ohlcData.length)bar=window._ohlcData[window._ohlcData.length-1];
    renderOhlc(bar);
  });
  window.addEventListener("resize",()=>{if(chart)chart.applyOptions({width:el.clientWidth});});
}
function renderOhlc(bar){
  const el=document.getElementById("ohlcLegend");if(!el)return;
  if(!bar){el.innerHTML="";return;}
  const col=bar.close>=bar.open?"#26a69a":"#ef5350";
  el.innerHTML=`开<b style="color:${col}">${fmtNum(bar.open)}</b>　高<b style="color:${col}">${fmtNum(bar.high)}</b>　低<b style="color:${col}">${fmtNum(bar.low)}</b>　收<b style="color:${col}">${fmtNum(bar.close)}</b>`;
}
function toggleMA(w,on){if(maSeries[w])maSeries[w].applyOptions({visible:on});}
let wallLines=[];
async function toggleOptionWall(on){
  wallLines.forEach(l=>{try{candleSeries.removePriceLine(l);}catch(e){}});wallLines=[];
  if(!on||!candleSeries)return;
  let d=window._walls;
  if(!d||d.ticker!==curTicker){
    const e=await j("/api/options/expiries?ticker="+encodeURIComponent(curTicker));
    if(e.error||!e.expiries||!e.expiries.length){const cb=document.getElementById("wallOverlay");if(cb)cb.checked=false;return;}
    const exp=pickMonthlyExpiry(e.expiries.slice(0,16));
    d=await j(`/api/options/walls?ticker=${encodeURIComponent(curTicker)}&expiry=${exp}`);
    if(d.error)return;window._walls=d;
  }
  const add=(price,color,title)=>{if(price==null)return;wallLines.push(candleSeries.createPriceLine({price,color,lineWidth:1,lineStyle:2,axisLabelVisible:true,title}));};
  add(d.maxPain,"#f6c343","Max Pain");
  if(d.callWalls&&d.callWalls[0])add(d.callWalls[0].strike,"#ef5350","Call墙(压力)");
  if(d.putWalls&&d.putWalls[0])add(d.putWalls[0].strike,"#26a69a","Put墙(支撑)");
  add(d.gammaFlip,"#58a6ff","Gamma Flip");
}
async function loadChart(period){
  curPeriod=period;
  document.querySelectorAll("#tabc-overview .controls>button").forEach(b=>b.classList.toggle("active",b.textContent===period));
  if(!chart)initChart();
  const d=await j(`/api/history?ticker=${encodeURIComponent(curTicker)}&period=${period}`);
  if(d.error||!d.candles)return;
  candleSeries.setData(d.candles);volSeries.setData(d.volumes);
  window._ohlcData=d.candles;renderOhlc(d.candles[d.candles.length-1]);   // 默认显示最新一根 OHLC
  Object.keys(MA_COLORS).forEach(w=>{if(maSeries[w]&&d.ma&&d.ma[w])maSeries[w].setData(d.ma[w]);});
  // 记录各 MA 最新值,供仓位计算的快捷止损用
  window._maLast={};
  Object.keys(MA_COLORS).forEach(w=>{const arr=d.ma&&d.ma[w];if(arr&&arr.length)window._maLast[w]=arr[arr.length-1].value;});
  renderMaStopBtns("ov");
  applyOvDefaultStop();   // 均线就绪/切周期后,若用户未改过止损则取最近均线为默认
  chart.timeScale().fitContent();
}

async function loadSepa(){
  const el=document.getElementById("sepa");if(!el)return;
  const s=await j("/api/sepa?ticker="+encodeURIComponent(curTicker));
  if(s.error){el.innerHTML='<div class="muted">'+s.error+'</div>';return;}
  const stageCls=/Stage 2/.test(s.stage)?"buy":(/Stage 4/.test(s.stage)?"sell":"hold");
  const rows=s.conditions.map(c=>{const st=c.pass===true?'<span class="pass">✓ 通过</span>':(c.pass===false?'<span class="fail">✗ 不满足</span>':'<span class="unk">? 未知</span>');return `<tr><td class="muted">${c.no}</td><td>${c.name}</td><td>${st}</td><td class="muted">${c.value}</td></tr>`;}).join("");
  const g=s.fundamentalGrade,gcls={"A":"gA","B":"gB","C":"gC","D":"gD"}[g]||"gq";
  el.innerHTML=`<div class="sepa-head"><span class="badge ${s.verdictClass}" style="font-size:14px">${s.verdict}</span><span class="badge ${stageCls}">${s.stage}</span>
    <span class="muted">趋势模板 <span class="scorebig ${s.passed===s.total?'green':(s.passed>=6?'':'red')}">${s.passed}/${s.total}</span></span>
    <span class="muted">基本面 <span class="gradechip ${gcls}">${g}</span></span>${s.epsGrowth!=null?`<span class="muted">季度EPS同比 ${fmtPct(s.epsGrowth*100)}</span>`:""}</div>
    <table><thead><tr><th>#</th><th>条件</th><th>结果</th><th>实际值</th></tr></thead><tbody>${rows}</tbody></table><div class="small">${s.rsNote}</div>`;
}

// ---------- 一键决策卡(纯 SEPA 驱动)----------
async function loadDecision(){
  const el=document.getElementById("decisionCard");if(!el)return;
  const tk=curTicker;
  el.innerHTML='<div class="card"><div class="muted">SEPA 决策分析中…</div></div>';
  const sepa=await j("/api/sepa?ticker="+tk).catch(()=>null);
  if(tk!==curTicker)return; // 期间切换了
  if(!sepa||sepa.error){el.innerHTML=`<div class="card"><div class="muted">${(sepa&&sepa.error)||'SEPA 数据不足,暂无决策'}</div></div>`;return;}
  const vclass=sepa.verdictClass||"hold";
  // 结论直接取 SEPA 自身评分;下方理由展开阶段 / 模板分 / 基本面
  const reasons=[
    {k:"阶段",v:sepa.stage,c:/Stage 2/.test(sepa.stage)?"buy":(/Stage 4/.test(sepa.stage)?"sell":"hold")},
    {k:"趋势模板",v:`${sepa.passed}/${sepa.total} 条件通过`,c:sepa.passed>=7?"buy":(sepa.passed<=3?"sell":"hold")},
    {k:"基本面",v:`季度EPS评级 ${sepa.fundamentalGrade}`+(sepa.epsGrowth!=null?` · 同比 ${fmtPct(sepa.epsGrowth*100)}`:""),c:/[AB]/.test(sepa.fundamentalGrade)?"buy":(sepa.fundamentalGrade==="D"?"sell":"hold")},
  ];
  // 建议仓位(用已存设置;止损沿用 SEPA 经典 -8%)
  const acct=parseFloat(getSetting("accountValue","100000"))||0;
  const riskUnit=getSetting("riskUnit","%"),riskVal=parseFloat(getSetting("riskVal","1"))||1;
  const posUnit=getSetting("posUnit","%"),posVal=parseFloat(getSetting("posVal","25"))||25;
  let sizeNote="";
  if(sepa.price){const entry=sepa.price,stop=entry*0.92;const rps=entry-stop;
    const riskD=riskUnit==="%"?acct*riskVal/100:riskVal,posD=posUnit==="%"?acct*posVal/100:posVal;
    const shares=Math.max(0,Math.min(Math.floor(riskD/rps),Math.floor(posD/entry)));
    sizeNote=`按你的设置(账户$${fmtBig(acct)}、风险${riskVal}${riskUnit}、仓位${posVal}${posUnit}、止损-8%)建议约 <b>${shares.toLocaleString()}</b> 股(投入$${fmtBig(shares*entry)})`;}
  el.innerHTML=`<div class="card" style="border-left:4px solid ${vclass==='buy'?'var(--green)':vclass==='sell'?'var(--red)':'var(--yellow)'}">
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:8px">
      <span style="font-size:13px;color:var(--muted)">一键决策 · SEPA</span>
      <span class="badge ${vclass}" style="font-size:15px">${sepa.verdict}</span>
      <span class="muted" style="font-size:12px">模板 ${sepa.passed}/${sepa.total}</span></div>
    <div style="display:flex;gap:18px;flex-wrap:wrap">${reasons.map(r=>`<div style="font-size:13px"><span class="badge ${r.c}" style="font-size:11px">${r.k}</span> <span class="muted">${r.v}</span></div>`).join("")}</div>
    ${sizeNote?`<div class="small" style="margin-top:8px">${sizeNote} · <a onclick="document.getElementById('sizerPanel')&&document.getElementById('sizerPanel').scrollIntoView({behavior:'smooth',block:'center'})">看右侧仓位计算 →</a></div>`:""}
    <div class="small">基于 SEPA 趋势模板评分,仅供参考,不构成投资建议。</div></div>`;
}
async function loadEarnings(){
  const el=document.getElementById("earnings");if(!el)return;
  const e=await j("/api/earnings?ticker="+encodeURIComponent(curTicker));
  if(e.error){el.innerHTML='<div class="muted">'+e.error+'</div>';return;}
  let head="";
  if(e.upcoming){const days=Math.round((new Date(e.upcoming.date)-new Date())/864e5);
    head=`<div class="grid" style="margin-bottom:14px">${card("下次财报日",e.upcoming.date+(isFinite(days)?` <span class="muted" style="font-size:12px">(${days>=0?days+"天后":"约"})</span>`:""))}${card("预期EPS",fmtNum(e.upcoming.estimate))}${card("远期EPS",fmtNum(e.epsForward))}${card("营收同比",e.revenueGrowth!=null?fmtPct(e.revenueGrowth*100):"—")}</div>`;
  }else head='<div class="muted" style="margin-bottom:10px">暂无下次财报日数据</div>';
  let hist="";
  if(e.history&&e.history.length)hist=`<table><thead><tr><th>财报日</th><th>预期EPS</th><th>实际EPS</th><th>意外%</th><th>结果</th></tr></thead><tbody>`+e.history.map(h=>`<tr><td>${h.date}</td><td>${fmtNum(h.estimate)}</td><td>${fmtNum(h.reported)}</td><td class="${(h.surprisePct||0)>=0?'green':'red'}">${h.surprisePct!=null?fmtPct(h.surprisePct):"—"}</td><td>${h.beat?'<span class="pass">Beat</span>':'<span class="fail">Miss</span>'}</td></tr>`).join("")+`</tbody></table>`;
  el.innerHTML=head+hist;
}
async function loadLiquidity(){
  const el=document.getElementById("liquidity");if(!el)return;
  const l=await j("/api/liquidity?ticker="+encodeURIComponent(curTicker));
  if(l.error){el.innerHTML='<div class="muted">'+l.error+'</div>';return;}
  const gcls={"A":"gA","B":"gB","C":"gC","D":"gD"}[l.grade]||"gq";
  el.innerHTML=`<div class="grid"><div class="card"><div class="k">流动性评级</div><div class="v"><span class="gradechip ${gcls}">${l.grade}</span> <span class="muted" style="font-size:12px">${l.gradeDesc}</span></div></div>${card("日均成交量",fmtBig(l.adtv))}${card("美元成交额",l.dollarVol!=null?"$"+fmtBig(l.dollarVol):"—")}${card("买卖价差",l.spreadBps!=null?fmtNum(l.spreadBps,1)+" bps":"—")}${card("换手率",l.turnover!=null?fmtNum(l.turnover)+"%":"—")}</div>`;
}
async function loadNews(){
  const el=document.getElementById("news");if(!el)return;
  const n=await j("/api/news?ticker="+encodeURIComponent(curTicker));
  if(n.error||!n.news||!n.news.length){el.innerHTML='<div class="muted">暂无新闻</div>';return;}
  el.innerHTML=n.news.map(it=>`<div class="news-item"><div class="t">${it.link?`<a href="${it.link}" target="_blank" rel="noopener">${it.title}</a>`:it.title}</div><div class="m">${it.publisher||""}${it.time?" · "+it.time:""}</div></div>`).join("");
}

// ---------- 预警 ----------
let allAlerts=[];
async function loadAlerts(){
  try{const d=await j("/api/alerts");allAlerts=d.alerts||[];}catch(e){allAlerts=[];}
  try{window._notify=await j("/api/notify/status");}catch(e){window._notify={enabled:false,configured:false};}
  renderAlertBanner();renderAlertsPanel();
}
async function toggleTgPush(on){
  setSetting("telegramPushEnabled",on?"1":"0");
  window._notify=window._notify||{};window._notify.enabled=on;
  renderAlertsPanel();
}
async function testTgPush(){
  const r=await(await fetch("/api/notify/test",{method:"POST"})).json();
  alert(r.ok?"已发送测试推送,去 Telegram 看一眼":("测试失败: "+(r.error||r.info||"")));
}
function renderAlertBanner(){
  const el=document.getElementById("alertBanner");if(!el)return;
  const trig=allAlerts.filter(a=>a.triggered);
  el.innerHTML=trig.length?trig.map(a=>`<span class="ab">⚠ ${a.ticker} ${alertText(a)} 已触发(现价 ${fmtNum(a.price)})</span>`).join(""):"";
}
function alertText(a){return a.kind==="above"?`≥ ${fmtNum(a.level)}`:a.kind==="below"?`≤ ${fmtNum(a.level)}`:a.kind==="stop"?`止损 ${fmtNum(a.level)}`:a.kind==="break_ma20"?"跌破20MA":a.kind;}
function renderAlertsPanel(){
  const el=document.getElementById("alertsPanel");if(!el)return;
  const mine=allAlerts.filter(a=>a.ticker===curTicker);
  const rows=mine.map(a=>`<span class="leg-tag">${alertText(a)} ${a.triggered?'<span class="red">●触发</span>':'<span class="muted">待触发</span>'} <a style="color:var(--red)" onclick="delAlert(${a.id})">✕</a></span>`).join("");
  const n=window._notify||{enabled:false,configured:false};
  const pushBar=`<div class="cmpbar" style="margin-bottom:8px;align-items:center">
    <label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-weight:600">
      <input type="checkbox" ${n.enabled?"checked":""} ${n.configured?"":"disabled"} onchange="toggleTgPush(this.checked)">
      🔔 Telegram 推送(全局)</label>
    <span class="muted" style="font-size:12px">${n.configured?(n.enabled?"已开启 · 盘中自动推送你的预警与持仓止损/目标(关→不推送)":"已关闭 · 不会推送任何消息"):"服务器未配置推送通道(TG_RELAY_WEBHOOK)"}</span>
    ${n.configured?'<button class="search" style="margin:0" onclick="testTgPush()">测试推送</button>':""}</div>`;
  el.innerHTML=pushBar+`<div class="cmpbar">
    <select id="alKind" style="background:var(--panel);color:var(--text);border:1px solid var(--border);padding:7px;border-radius:8px"><option value="above">价格 ≥</option><option value="below">价格 ≤</option><option value="stop">触及止损 ≤</option><option value="break_ma20">跌破20MA</option></select>
    <input id="alLevel" type="number" placeholder="价格" style="width:100px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:8px">
    <button class="search" style="margin:0" onclick="addAlert()">为 ${curTicker} 添加预警</button>
    <span style="margin-left:8px">${rows||'<span class="muted">该股暂无预警</span>'}</span></div>`;
}
async function addAlert(){
  const kind=document.getElementById("alKind").value;const level=parseFloat(document.getElementById("alLevel").value);
  if(kind!=="break_ma20"&&!level){alert("请输入价格");return;}
  await fetch("/api/alerts",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({ticker:curTicker,kind,level:level||null})});
  loadAlerts();
}
async function delAlert(id){await fetch("/api/alerts?id="+id,{method:"DELETE"});loadAlerts();}

// ---------- 仓位计算器(能买多少股,前缀化复用) ----------
function sizerForm(p,opts){
  opts=opts||{};
  const acct=getSetting("accountValue","100000");
  const posUnit=getSetting("posUnit","%"),riskUnit=getSetting("riskUnit","%");
  const posVal=getSetting("posVal","25"),riskVal=getSetting("riskVal","1");
  const unitSel=(suf,u)=>`<select id="${p}${suf}" onchange="calcSize('${p}')"><option ${u==="%"?"selected":""}>%</option><option ${u==="$"?"selected":""}>$</option></select>`;
  const tickerRow=opts.withTicker?`<div><label>股票代码(可选)</label><div class="row"><input id="${p}Ticker" placeholder="如 AAPL" value="${opts.ticker||''}" style="text-transform:uppercase" onkeydown="if(event.key==='Enter')sizerFetchPrice('${p}')"><button class="search" style="margin:0" onclick="sizerFetchPrice('${p}')">取现价</button></div><div class="hint" id="${p}hTk">填代码点「取现价」自动带入买入价</div></div>`:"";
  return `
   <div class="pform">
     ${tickerRow}
     <div><label>总资产 ($)</label><div class="row"><input id="${p}Account" type="number" value="${acct}" oninput="calcSize('${p}')"></div><div class="hint">默认取自持仓页总资金,可临时改(不回写)</div></div>
     <div><label>买入价 ($)</label><div class="row"><input id="${p}Entry" type="number" value="${opts.entry||''}" oninput="calcSize('${p}')"></div><div class="hint" id="${p}hEntry"></div></div>
     <div><label>止损价 ($)</label><div class="row"><input id="${p}Stop" type="number" value="${opts.stop||''}" oninput="onStopInput('${p}')"></div><div class="hint" id="${p}hStop"></div></div>
     ${opts.maStops?`<div class="ma-stops-row"><div class="ma-stops" id="${p}MaBtns"></div></div>`:""}
     <div><label>① 总买入仓位上限</label><div class="row"><input id="${p}MaxPos" type="number" value="${posVal}" oninput="calcSize('${p}')">${unitSel("MaxPosUnit",posUnit)}</div><div class="hint" id="${p}hPos"></div></div>
     <div><label>② 总风险上限(最多亏)</label><div class="row"><input id="${p}MaxRisk" type="number" value="${riskVal}" oninput="calcSize('${p}')">${unitSel("MaxRiskUnit",riskUnit)}</div><div class="hint" id="${p}hRisk"></div></div>
   </div>
   <div id="${p}Result"></div>`;
}
async function sizerFetchPrice(p){
  const el=document.getElementById(p+"Ticker");if(!el)return;
  const tk=el.value.trim().toUpperCase();if(!tk)return;
  const h=document.getElementById(p+"hTk");if(h)h.textContent="读取现价…";
  try{
    const q=await j("/api/quote?ticker="+encodeURIComponent(tk));
    if(q&&q.price){
      document.getElementById(p+"Entry").value=q.price.toFixed(2);
      document.getElementById(p+"Stop").value=(q.price*0.92).toFixed(2);
      if(h)h.innerHTML=`${tk} 现价 <b>$${fmtNum(q.price)}</b> 已带入(止损默认 -8%,可改)`;
      calcSize(p);
    }else if(h)h.textContent="未取到现价,请手动填买入价";
  }catch(e){if(h)h.textContent="取价失败,请手动填买入价";}
}
function calcSize(p){
  const num=suf=>parseFloat(document.getElementById(p+suf).value);
  const account=num("Account"),entry=num("Entry"),stop=num("Stop");
  const maxPosIn=num("MaxPos"),maxRiskIn=num("MaxRisk");
  const posUnit=document.getElementById(p+"MaxPosUnit").value,riskUnit=document.getElementById(p+"MaxRiskUnit").value;
  // 记忆(总资金以「持仓页顶部」为唯一真相,这里仅取默认值,不回写;其余为仓位计算偏好)
  setSetting("posUnit",posUnit);setSetting("riskUnit",riskUnit);
  if(maxPosIn>=0)setSetting("posVal",maxPosIn);if(maxRiskIn>=0)setSetting("riskVal",maxRiskIn);
  const tkEl=document.getElementById(p+"Ticker");
  const ticker=tkEl?tkEl.value.trim().toUpperCase():(curTicker||"");
  window["_lastCalc_"+p]={entry,stop,shares:0,ticker};

  const res=document.getElementById(p+"Result");
  const setHint=(suf,t)=>{const e=document.getElementById(p+suf);if(e)e.textContent=t;};
  // 派生金额
  const maxPosDollar=posUnit==="%"?(account*maxPosIn/100):maxPosIn;
  const maxRiskDollar=riskUnit==="%"?(account*maxRiskIn/100):maxRiskIn;
  setHint("hPos",isFinite(maxPosDollar)?"= $"+fmtBig(maxPosDollar)+(posUnit==="$"&&account>0?` · 占 ${(maxPosDollar/account*100).toFixed(1)}%`:""):"");
  setHint("hRisk",isFinite(maxRiskDollar)?"= $"+fmtBig(maxRiskDollar)+(riskUnit==="$"&&account>0?` · 占 ${(maxRiskDollar/account*100).toFixed(2)}%`:""):"");
  setHint("hStop", (entry>0&&stop>0)?`止损距离 ${((entry-stop)/entry*100).toFixed(2)}%`:"");

  if(!(account>0&&entry>0&&stop>0&&maxPosIn>=0&&maxRiskIn>=0)){res.innerHTML='<div class="muted">请完整填写各项(均为正数)。</div>';return;}
  if(stop>=entry){res.innerHTML='<div class="error">止损价必须低于买入价。</div>';return;}

  const riskPerShare=entry-stop;
  const sharesByRisk=Math.floor(maxRiskDollar/riskPerShare);
  const sharesByPos=Math.floor(maxPosDollar/entry);
  const shares=Math.max(0,Math.min(sharesByRisk,sharesByPos));
  const binding=sharesByRisk<=sharesByPos?"风险上限":"仓位上限";
  const bindClass=sharesByRisk<=sharesByPos?"sell":"hold";
  window["_lastCalc_"+p]={entry,stop,shares,ticker};

  const capital=shares*entry;
  const riskDollar=shares*riskPerShare;
  const t1=entry*1.08,t2=entry*1.15;          // SEPA: +8% 卖一半, +15% 再卖25%
  const R=riskPerShare,rr1=(t1-entry)/R,rr2=(t2-entry)/R;

  if(shares<=0){
    res.innerHTML=`<div class="error">在当前条件下可买股数为 0 —— 风险上限或仓位上限太小,或止损距离太宽(每股风险 $${fmtNum(riskPerShare)})。</div>`;return;
  }
  res.innerHTML=`
   <div class="bindbox">
     <div><div class="muted" style="font-size:12px">建议最大买入</div><div class="shares-big">${shares.toLocaleString()} <span style="font-size:18px;font-weight:600">股</span></div></div>
     <span class="badge ${bindClass}">受「${binding}」约束</span>
     <button class="search" style="margin:0" onclick="addSizerRecord('${p}')">＋ 记入持仓</button>
   </div>
   <div class="grid">
     ${card("投入资金",`$${fmtBig(capital)}`)}
     ${card("占总资产",`${(capital/account*100).toFixed(1)}%`)}
     ${card("实际风险金额",`$${fmtBig(riskDollar)}`)}
     ${card("占总资产(风险)",`${(riskDollar/account*100).toFixed(2)}%`)}
     ${card("每股风险",`$${fmtNum(riskPerShare)}`)}
     ${card("止损距离",`${((entry-stop)/entry*100).toFixed(2)}%`)}
   </div>
   <div class="section-title" style="font-size:14px">两个约束的候选股数(取较小值)</div>
   <table style="max-width:560px"><thead><tr><th>约束</th><th>可买股数</th><th>对应金额</th><th></th></tr></thead><tbody>
     <tr><td>① 按总仓位上限</td><td>${sharesByPos.toLocaleString()}</td><td class="muted">$${fmtBig(sharesByPos*entry)}</td><td>${binding==="仓位上限"?'<span class="pass">← 生效</span>':''}</td></tr>
     <tr><td>② 按总风险上限</td><td>${sharesByRisk.toLocaleString()}</td><td class="muted">最多亏 $${fmtBig(sharesByRisk*riskPerShare)}</td><td>${binding==="风险上限"?'<span class="pass">← 生效</span>':''}</td></tr>
   </tbody></table>
   <div class="section-title" style="font-size:14px">盈亏目标参考(SEPA)</div>
   <table style="max-width:620px"><thead><tr><th>价位</th><th>价格</th><th>距现</th><th>盈亏比 R/R</th><th>动作</th></tr></thead><tbody>
     <tr><td>止损</td><td class="red">$${fmtNum(stop)}</td><td class="red">-${((entry-stop)/entry*100).toFixed(1)}%</td><td>-1R</td><td class="muted">触及立即离场</td></tr>
     <tr><td>目标1</td><td class="green">$${fmtNum(t1)}</td><td class="green">+8%</td><td>${rr1.toFixed(2)}:1</td><td class="muted">卖一半,止损上移保本</td></tr>
     <tr><td>目标2</td><td class="green">$${fmtNum(t2)}</td><td class="green">+15%</td><td>${rr2.toFixed(2)}:1</td><td class="muted">再卖25%,余下跟踪20MA</td></tr>
   </tbody></table>
   <div class="small">公式:股数 = min( 总风险额÷每股风险 , 总仓位额÷买入价 )。SEPA 建议单笔风险 0.5–2%、盈亏比≥2:1、止损 7–8% 内。本工具仅为计算,不构成投资建议。</div>`;
}
async function addSizerRecord(p){
  const c=window["_lastCalc_"+p];
  if(!c||!c.shares){alert("请先得到有效的可买股数");return;}
  const tk=(c.ticker||"").trim().toUpperCase();
  if(!tk){alert("请先填写股票代码,再记入持仓");return;}
  await fetch("/api/positions",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({ticker:tk,shares:c.shares,entry:c.entry,stop:c.stop,target:(c.entry*1.15).toFixed(2)})});
  alert(`已记入持仓:${tk} ${c.shares} 股 @ ${fmtNum(c.entry)}`);
  if(document.getElementById("posTrack"))loadTrack();
}

// ---------- 持仓页 ----------
function loadPositions(){
  const el=document.getElementById("page-positions");
  el.innerHTML=`
    <div class="section-title" style="margin-top:6px">我的持仓 · 跟踪 <span class="tag">SQLite 持久化</span></div>
    <div class="muted" style="font-size:13px;margin-bottom:14px">「能买多少股」计算器在<b>个股看板 → 概览页 K 线右侧</b>,算好可一键记入这里。</div>
    <div id="posTrack"><div class="loading">加载持仓…</div></div>`;
  loadTrack();
}
async function loadTrack(){
  const el=document.getElementById("posTrack");
  const d=await j("/api/positions");
  const s=d.summary||{};
  const open=d.positions.filter(p=>p.status==="open"),trades=d.trades||[];
  const acct=getSetting("accountValue","100000");
  const freeCash=(s.totalMarketValue!=null&&acct)?(parseFloat(acct)-s.totalMarketValue):null;
  const capBar=`<div class="capbar">
    <div class="caprow"><span class="caplabel">总资金量</span><span class="capcur">$</span>
      <input id="acctInput" type="number" value="${acct}" onkeydown="if(event.key==='Enter')saveAccount()">
      <button class="search" style="margin:0" onclick="saveAccount()">更新</button></div>
    <div class="capmeta">
      <span>已投入 <b>$${fmtBig(s.totalCost||0)}</b> (${s.investedPct!=null?s.investedPct.toFixed(1):'0'}%)</span>
      <span>可用现金 <b>${freeCash!=null?'$'+fmtBig(freeCash):'—'}</b></span>
      <span>浮盈亏 <b class="${(s.totalPL||0)>=0?'green':'red'}">${s.totalPL!=null?(s.totalPL>=0?'+':'')+'$'+fmtBig(Math.abs(s.totalPL)):'—'}</b></span>
    </div>
    <div class="muted" style="font-size:11px">仓位计算默认从这里取值;修改后下方比例与「概览页仓位计算」同步。盈亏按 yfinance 延迟现价对开仓价实时计算。</div>
  </div>`;
  const sumCards=`<div class="grid" style="margin-bottom:18px">
    ${card("持仓数",s.openCount||0)}
    ${card("总市值",s.totalMarketValue!=null?"$"+fmtBig(s.totalMarketValue):"—")}
    ${card("总成本",s.totalCost!=null?"$"+fmtBig(s.totalCost):"—")}
    <div class="card"><div class="k">总浮盈亏</div><div class="v ${(s.totalPL||0)>=0?'green':'red'}">${s.totalPL!=null?(s.totalPL>=0?"+":"")+"$"+fmtBig(Math.abs(s.totalPL)):"—"} ${s.totalPLPct!=null?`(${fmtPct(s.totalPLPct)})`:""}</div></div>
    ${card("仓位占账户",s.marketPct!=null?s.marketPct.toFixed(1)+"%":"—")}
    <div class="card"><div class="k">组合风险敞口</div><div class="v ${(s.riskPct||0)>6?'red':''}">${s.totalRisk!=null?"$"+fmtBig(s.totalRisk):"—"} ${s.riskPct!=null?`(${s.riskPct.toFixed(2)}%)`:""}</div></div>
  </div>`;
  window._openPos=open;
  const openRows=open.map(p=>{
    const plc=(p.pl||0)>=0?"green":"red";
    // 止损≥成本=锁利(绿),否则=亏损风险(红);遵循绿赚红亏
    const locked=(p.stop!=null&&p.entry!=null&&p.stop>=p.entry);
    const stopc=p.stop==null?"":(locked?"green":"red");
    const riskCell=p.riskDollar!=null
      ? `<span class="${stopc}" title="(现价−止损)×股数;${locked?'止损已在成本之上,这是会回吐的利润':'触及止损将亏损此金额'}">${p.riskDollar>=0?"":"-"}$${fmtBig(Math.abs(p.riskDollar))}</span>`
      : '<span class="red" title="未设止损,下行风险不封顶">未设止损</span>';
    return `<tr id="posrow-${p.id}">
      <td><b class="wchip" style="cursor:pointer;padding:2px 6px" onclick="loadTicker('${p.ticker}')">${p.ticker}</b></td>
      <td>${fmtNum(p.shares,0)}</td><td>${fmtNum(p.entry)}</td><td>${fmtNum(p.price)}${p.manual?' <span class="tag" title="手填现价,不走实时行情(期权/港股等)">手填</span>':''}</td>
      <td class="${plc}">${p.pl!=null?(p.pl>=0?"+":"")+fmtBig(p.pl):"—"} ${p.plPct!=null?`(${fmtPct(p.plPct)})`:""}</td>
      <td>${fmtNum(p.stop)}</td><td class="${stopc}" title="现价到止损还要跌多少;止损在成本之上则锁利(绿)">${p.toStopPct!=null?fmtPct(p.toStopPct):"—"}</td>
      <td>${riskCell}</td>
      <td class="muted">${p.note||""}</td>
      <td><a onclick="editPosition(${p.id})">改</a> · <a onclick="closePosition(${p.id},${p.price||0})">平仓</a> · <a style="color:var(--red)" onclick="delPosition(${p.id})">删</a></td>
    </tr>`;}).join("");
  window._allTrades=trades;window._tradePage=0;
  el.innerHTML=`
    ${capBar}
    ${sumCards}
    <div class="cmpbar"><b>手动添加:</b>
      <input id="npTicker" placeholder="代码" style="width:90px;text-transform:uppercase;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:8px">
      <input id="npShares" placeholder="股数" type="number" style="width:90px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:8px">
      <input id="npEntry" placeholder="买入价" type="number" style="width:90px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:8px">
      <input id="npStop" placeholder="止损价" type="number" style="width:90px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:8px">
      <input id="npPrice" placeholder="现价(手填,选填)" type="number" title="期权/港股等无实时行情时手填现价;留空则走 yfinance" style="width:130px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:8px">
      <button class="search" style="margin:0" onclick="addPositionManual()">添加</button></div>
    ${open.length?`<table><thead><tr><th>代码</th><th>股数</th><th>成本</th><th>现价</th><th>浮盈亏</th><th>止损</th><th>距止损</th><th>风险敞口</th><th>备注</th><th>操作</th></tr></thead><tbody>${openRows}</tbody></table>`:'<div class="muted">暂无持仓。可用上方①计算器算好后一键记入,或这里手动添加。</div>'}
    <div id="tradeBox"></div>
    <div class="small">组合风险敞口 = Σ(现价−止损)×股数,占账户比例即「组合热度」,SEPA 建议总热度别过高。现价为 yfinance 延迟数据。</div>`;
  renderTrades();
}
const TRADES_PER_PAGE=20;
function renderTrades(){
  const box=document.getElementById("tradeBox");if(!box)return;
  const all=window._allTrades||[];
  if(!all.length){box.innerHTML="";return;}
  const pages=Math.ceil(all.length/TRADES_PER_PAGE);
  let pg=window._tradePage||0;if(pg<0)pg=0;if(pg>=pages)pg=pages-1;window._tradePage=pg;
  const rows=all.slice(pg*TRADES_PER_PAGE,pg*TRADES_PER_PAGE+TRADES_PER_PAGE).map(t=>{
    const isBuy=t.action==="buy";
    const plc=(t.pl||0)>=0?"green":"red";
    return `<tr class="muted"><td>${t.at||""}</td><td>${t.ticker}</td>
      <td>${isBuy?"买入":"卖出"}</td><td>${fmtNum(t.shares,0)}</td><td>${fmtNum(t.price)}</td>
      <td class="${isBuy?'muted':plc}">${(!isBuy&&t.pl!=null)?(t.pl>=0?"+":"")+fmtBig(t.pl):"—"}</td>
      <td class="muted">${t.note||""}</td>
      <td><a style="color:var(--red)" onclick="delTrade(${t.id})">删</a></td></tr>`;}).join("");
  const nav=pages>1?`<div class="cmpbar" style="justify-content:flex-end;gap:10px;margin-top:8px">
      <button class="search" style="margin:0;opacity:${pg<=0?0.4:1}" ${pg<=0?"disabled":""} onclick="changeTradePage(-1)">← 上一页</button>
      <span class="muted" style="align-self:center">第 ${pg+1}/${pages} 页 · 共 ${all.length} 条</span>
      <button class="search" style="margin:0;opacity:${pg>=pages-1?0.4:1}" ${pg>=pages-1?"disabled":""} onclick="changeTradePage(1)">下一页 →</button></div>`:"";
  box.innerHTML=`<div class="section-title" style="font-size:14px">交易明细(买卖流水) <span class="muted" style="font-size:12px">最新在前</span></div>
    <table><thead><tr><th>日期</th><th>代码</th><th>方向</th><th>股数</th><th>价格</th><th>已实现盈亏</th><th>备注</th><th></th></tr></thead><tbody>${rows}</tbody></table>${nav}`;
}
function changeTradePage(delta){window._tradePage=(window._tradePage||0)+delta;renderTrades();}
async function saveAccount(){
  const v=parseFloat(document.getElementById("acctInput").value);
  if(!(v>0)){alert("请输入正数总资金量");return;}
  settings.accountValue=String(v);localStorage.setItem("accountValue",v);
  // 先确保后端写入,再重算(investedPct/riskPct 后端依赖该值)
  try{await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({accountValue:String(v)})});}catch(e){}
  loadTrack();
}
async function addPositionManual(){
  const tk=document.getElementById("npTicker").value.trim().toUpperCase();
  const shares=parseFloat(document.getElementById("npShares").value),entry=parseFloat(document.getElementById("npEntry").value),stop=parseFloat(document.getElementById("npStop").value),mp=parseFloat(document.getElementById("npPrice").value);
  if(!tk||!shares||!entry){alert("代码/股数/买入价必填");return;}
  await fetch("/api/positions",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({ticker:tk,shares,entry,stop:stop||null,manual_price:mp>0?mp:null})});
  loadTrack();
}
// 行内平仓表单:输入平仓价 + 平仓数量(默认全部);数量<持仓为部分平仓(减仓)
function closePosition(id,price){
  const p=(window._openPos||[]).find(x=>x.id===id);if(!p)return;
  const tr=document.getElementById("posrow-"+id);if(!tr)return;
  const ist='type="number" style="width:80px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:5px;border-radius:6px"';
  tr.innerHTML=`<td><b>${p.ticker}</b></td>
    <td><input id="cp-shares-${id}" ${ist} value="${p.shares??""}" max="${p.shares??""}" placeholder="平仓数量"></td>
    <td class="muted">成本 ${fmtNum(p.entry)}</td>
    <td><input id="cp-price-${id}" ${ist} value="${price?price.toFixed(2):""}" placeholder="平仓价"></td>
    <td colspan="4" class="muted">持仓 ${fmtNum(p.shares,0)} 股;数量小于持仓即部分减仓,会保留剩余仓位并注明</td>
    <td><input id="cp-note-${id}" value="" style="width:110px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:5px;border-radius:6px" placeholder="备注"></td>
    <td><a onclick="confirmClose(${id})">确认平仓</a> · <a onclick="loadTrack()">取消</a></td>`;
  const f=document.getElementById("cp-price-"+id);if(f){f.focus();}
}
async function confirmClose(id){
  const v=s=>{const e=document.getElementById("cp-"+s+"-"+id);return e?e.value.trim():"";};
  const px=parseFloat(v("price"));
  if(!(px>0)){alert("请输入有效平仓价格");return;}
  const qtyRaw=v("shares"),qty=qtyRaw===""?null:parseFloat(qtyRaw);
  if(qtyRaw!==""&&!(qty>0)){alert("平仓数量必须为正数");return;}
  await fetch("/api/positions/"+id,{method:"PUT",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({action:"close",exit_price:px,shares:qty,note:v("note")})});
  loadTrack();
}
async function delPosition(id){
  if(!confirm("确认删除该记录?"))return;
  await fetch("/api/positions/"+id,{method:"DELETE"});loadTrack();
}
async function delTrade(id){
  if(!confirm("确认删除该交易记录?(不影响当前持仓)"))return;
  await fetch("/api/trades/"+id,{method:"DELETE"});loadTrack();
}
// 行内编辑持仓:股数 / 成本 / 止损 / 目标 / 备注
function editPosition(id){
  const p=(window._openPos||[]).find(x=>x.id===id);if(!p)return;
  const tr=document.getElementById("posrow-"+id);if(!tr)return;
  const ist='type="number" style="width:72px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:5px;border-radius:6px"';
  const esc=s=>String(s==null?"":s).replace(/"/g,"&quot;");
  tr.innerHTML=`<td><b>${p.ticker}</b></td>
    <td><input id="ep-shares-${id}" ${ist} value="${p.shares??""}"></td>
    <td><input id="ep-entry-${id}" ${ist} value="${p.entry??""}"></td>
    <td><input id="ep-mprice-${id}" ${ist} value="${p.manual_price??""}" placeholder="${p.manual?'手填现价':'留空=自动'}" title="填入则用手填现价(期权/港股);留空则走实时行情"></td>
    <td class="muted">—</td>
    <td><input id="ep-stop-${id}" ${ist} value="${p.stop??""}" placeholder="止损"></td>
    <td colspan="2"><input id="ep-target-${id}" ${ist} value="${p.target??""}" placeholder="目标价"></td>
    <td><input id="ep-note-${id}" value="${esc(p.note)}" style="width:120px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:5px;border-radius:6px" placeholder="备注"></td>
    <td><a onclick="savePosition(${id})">存</a> · <a onclick="loadTrack()">取消</a></td>`;
}
async function savePosition(id){
  const v=s=>{const e=document.getElementById("ep-"+s+"-"+id);return e?e.value.trim():"";};
  const numOrNull=x=>x===""?null:parseFloat(x);
  const shares=parseFloat(v("shares")),entry=parseFloat(v("entry"));
  if(!(shares>0)||!(entry>0)){alert("股数/买入价必须为正数");return;}
  // 止损可高于成本(移动止损锁利),不做 stop<entry 限制
  const body={shares,entry,stop:numOrNull(v("stop")),target:numOrNull(v("target")),note:v("note"),manual_price:numOrNull(v("mprice"))};
  await fetch("/api/positions/"+id,{method:"PUT",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
  loadTrack();
}

// ---------- 每日复盘 ----------
let _reviewDate=null;
async function loadReview(){
  const el=document.getElementById("page-review");
  el.innerHTML='<div class="loading">加载复盘…</div>';
  const d=await j("/api/snapshots");
  const snaps=d.snapshots||[];
  const opts=snaps.map(s=>`<option value="${s.date}">${s.date}${s.hasReview?' ✍':''}</option>`).join("");
  el.innerHTML=`
    <div class="section-title" style="margin-top:6px">每日复盘 <span class="tag">每个交易日 17:00(美东)自动留存快照</span></div>
    <div class="muted" style="font-size:13px;margin-bottom:12px">每天自动留存一份组合快照(持仓、交易流水、盈亏、风险敞口),并可写下自己的复盘,日后回查、提升交易能力。</div>
    <div class="cmpbar">
      <span class="muted">选择日期</span>
      <select id="revDate" onchange="showSnapshot(this.value)" style="background:var(--panel);color:var(--text);border:1px solid var(--border);padding:7px 10px;border-radius:8px">${opts||'<option value="">暂无快照</option>'}</select>
      <button class="search" style="margin:0" onclick="takeSnapshotNow()">📸 立即生成今日快照</button>
    </div>
    <div id="revBody">${snaps.length?'<div class="loading">加载快照…</div>':'<div class="muted">还没有快照。点「立即生成今日快照」手动创建一份;之后每个交易日 17:00(美东)会自动留存。</div>'}</div>`;
  if(snaps.length)showSnapshot(snaps[0].date);
}
async function showSnapshot(date){
  if(!date)return;
  _reviewDate=date;
  const dd=document.getElementById("revDate");if(dd)dd.value=date;
  const body=document.getElementById("revBody");
  body.innerHTML='<div class="loading">加载快照…</div>';
  const d=await j("/api/snapshots/"+encodeURIComponent(date));
  if(d.error){body.innerHTML='<div class="error">'+d.error+'</div>';return;}
  const st=d.state||{},s=st.summary||{};
  const pos=(st.positions||[]).filter(p=>p.status==='open');
  const trades=(st.trades||[]).filter(t=>t.at===date);
  const cards=`<div class="grid" style="margin-bottom:14px">
    ${card("账户总值",s.account!=null?'$'+fmtBig(s.account):'—')}
    ${card("持仓市值",s.totalMarketValue!=null?'$'+fmtBig(s.totalMarketValue):'—')}
    ${card("总成本",s.totalCost!=null?'$'+fmtBig(s.totalCost):'—')}
    <div class="card"><div class="k">总浮盈亏</div><div class="v ${(s.totalPL||0)>=0?'green':'red'}">${s.totalPL!=null?(s.totalPL>=0?'+':'')+'$'+fmtBig(Math.abs(s.totalPL)):'—'} ${s.totalPLPct!=null?'('+fmtPct(s.totalPLPct)+')':''}</div></div>
    ${card("仓位占账户",s.marketPct!=null?s.marketPct.toFixed(1)+'%':'—')}
    <div class="card"><div class="k">组合风险敞口</div><div class="v">${s.totalRisk!=null?'$'+fmtBig(s.totalRisk):'—'} ${s.riskPct!=null?'('+s.riskPct.toFixed(2)+'%)':''}</div></div>
  </div>`;
  const posRows=pos.map(p=>`<tr><td>${p.ticker}${p.manual?' <span class="tag">手填</span>':''}</td><td>${fmtNum(p.shares,0)}</td><td>${fmtNum(p.entry)}</td><td>${fmtNum(p.price)}</td>
    <td class="${(p.pl||0)>=0?'green':'red'}">${p.pl!=null?(p.pl>=0?'+':'')+fmtBig(p.pl):'—'} ${p.plPct!=null?'('+fmtPct(p.plPct)+')':''}</td>
    <td>${fmtNum(p.stop)}</td><td>${p.toStopPct!=null?fmtPct(p.toStopPct):'—'}</td>
    <td>${p.riskDollar!=null?'$'+fmtBig(p.riskDollar):'—'}</td><td class="muted">${p.note||''}</td></tr>`).join("");
  const trRows=trades.map(t=>`<tr class="muted"><td>${t.ticker}</td><td>${t.action==='buy'?'买入':'卖出'}</td><td>${fmtNum(t.shares,0)}</td><td>${fmtNum(t.price)}</td><td class="${t.action==='buy'?'muted':((t.pl||0)>=0?'green':'red')}">${(t.action!=='buy'&&t.pl!=null)?(t.pl>=0?'+':'')+fmtBig(t.pl):'—'}</td><td class="muted">${t.note||''}</td></tr>`).join("");
  const revEsc=(d.review||"").replace(/&/g,"&amp;").replace(/</g,"&lt;");
  body.innerHTML=`
    <div class="muted" style="font-size:12px;margin-bottom:10px">快照生成时间:${d.createdAt||'—'}</div>
    ${cards}
    <div class="section-title" style="font-size:14px">当日持仓</div>
    ${pos.length?`<table><thead><tr><th>代码</th><th>股数</th><th>成本</th><th>现价</th><th>浮盈亏</th><th>止损</th><th>距止损</th><th>风险敞口</th><th>备注</th></tr></thead><tbody>${posRows}</tbody></table>`:'<div class="muted">当日无持仓</div>'}
    <div class="section-title" style="font-size:14px">当日交易</div>
    ${trades.length?`<table><thead><tr><th>代码</th><th>方向</th><th>股数</th><th>价格</th><th>已实现盈亏</th><th>备注</th></tr></thead><tbody>${trRows}</tbody></table>`:'<div class="muted">当日无交易</div>'}
    <div class="section-title" style="font-size:14px">我的复盘</div>
    <textarea id="revText" placeholder="写下今天的复盘:做对/做错了什么、情绪与纪律、明天的改进点……" style="width:100%;min-height:200px;box-sizing:border-box;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:12px;border-radius:10px;font-size:14px;line-height:1.7;resize:vertical">${revEsc}</textarea>
    <div style="margin-top:10px"><button class="search" style="margin:0" onclick="saveReview()">保存复盘</button> <span id="revSaved" class="muted" style="margin-left:10px"></span></div>`;
}
async function saveReview(){
  if(!_reviewDate){alert("请先选择日期");return;}
  const txt=document.getElementById("revText").value;
  await fetch("/api/snapshots/"+encodeURIComponent(_reviewDate)+"/review",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({review:txt})});
  const el=document.getElementById("revSaved");if(el){el.textContent="已保存 ✓";setTimeout(()=>{if(el)el.textContent="";},2000);}
}
async function takeSnapshotNow(){
  const r=await fetch("/api/snapshots/take",{method:"POST"});let d={};try{d=await r.json();}catch(e){}
  await loadReview();
  if(d.date)showSnapshot(d.date);
}

// ---------- 估值 ----------
async function loadValuation(){
  const el=document.getElementById("tabc-valuation");loadedTabs.valuation=curTicker;
  el.innerHTML='<div class="loading">估值计算中…</div>';
  const v=await j("/api/valuation?ticker="+encodeURIComponent(curTicker));
  if(v.error){el.innerHTML='<div class="error">'+v.error+'</div>';return;}
  const methodCards=v.methods.map(m=>card(m.name,fmtNum(m.value))).join("");
  const dcfBlock=v.dcf?`<div class="grid" style="margin-top:10px">${card("WACC",v.dcf.wacc+"%")}${card("永续增速",v.dcf.termGrowth+"%")}${card("无风险利率",v.dcf.rf+"%")}${card("EBIT利润率",v.dcf.ebitMargin+"%")}${card("首年营收增速",v.dcf.y1Growth+"%")}${card("终值占比",v.dcf.tvWeight+"%")}</div>`:'<div class="muted">该公司财务结构不适用简化 DCF(如金融/REIT/亏损),已用分析师目标与远期PE 估值</div>';
  const estRows=(v.estimates||[]).map(e=>`<tr><td>${e.period}</td><td>${fmtNum(e.avg)}</td><td class="muted">${fmtNum(e.low)} ~ ${fmtNum(e.high)}</td><td class="${(e.growth||0)>=0?'green':'red'}">${e.growth!=null?fmtPct(e.growth*100):"—"}</td><td class="muted">${e.numAnalysts??"—"}</td></tr>`).join("");
  const revRows=(v.revisions||[]).map(r=>{const ar=r.trend==="up"?'<span class="pass">↑ 上修</span>':(r.trend==="down"?'<span class="fail">↓ 下修</span>':'<span class="muted">→ 持平</span>');return `<tr><td>${r.period}</td><td>${fmtNum(r.current)}</td><td class="muted">${fmtNum(r.ago90)}</td><td>${ar}</td></tr>`;}).join("");
  el.innerHTML=`
    <div class="section-title">估值三角定位 <span class="tag">skill: company-valuation</span></div>
    <div class="sepa-head"><span class="badge ${v.verdictClass}" style="font-size:14px">${v.verdict}</span>
      <span class="muted">现价 <b>${fmtNum(v.price)}</b></span><span class="muted">→ 合理价 <b>${fmtNum(v.blended)}</b></span>
      <span class="muted">空间 <b class="${(v.upside||0)>=0?'green':'red'}">${fmtPct(v.upside)}</b></span></div>
    <div class="grid">${methodCards}</div>
    <div class="section-title" style="font-size:14px">DCF 假设</div>${dcfBlock}
    <div class="section-title">分析师预期 <span class="tag">skill: estimate-analysis</span></div>
    <table><thead><tr><th>周期</th><th>预期均值</th><th>区间</th><th>同比增速</th><th>分析师数</th></tr></thead><tbody>${estRows||'<tr><td colspan=5 class="muted">无预期数据</td></tr>'}</tbody></table>
    <div class="section-title" style="font-size:14px">EPS 预期修正(当前 vs 90天前)</div>
    <table><thead><tr><th>周期</th><th>当前预期</th><th>90天前</th><th>修正方向</th></tr></thead><tbody>${revRows||'<tr><td colspan=4 class="muted">无修正数据</td></tr>'}</tbody></table>
    <div class="small">${v.note}</div>`;
}

// ---------- 期权墙(对股价的影响) ----------
async function loadOptions(){
  const el=document.getElementById("tabc-options");loadedTabs.options=curTicker;
  el.innerHTML='<div class="loading">加载期权到期日…</div>';
  const e=await j("/api/options/expiries?ticker="+encodeURIComponent(curTicker));
  if(e.error||!e.expiries||!e.expiries.length){el.innerHTML='<div class="muted">该标的无期权数据</div>';return;}
  const exps=e.expiries.slice(0,16);
  const def=pickMonthlyExpiry(exps);
  el.innerHTML=`<div class="section-title">期权墙 · 对股价的影响 <span class="tag">Max Pain / OI 墙 / GEX</span></div>
    <div class="muted" style="font-size:13px;margin-bottom:12px">期权未平仓量(OI)与做市商 Gamma 敞口会牵引股价:Max Pain 是到期吸引位,大 Call OI=上方压力墙,大 Put OI=下方支撑墙,正 GEX 倾向钉价/抑波动、负 GEX 放大波动。</div>
    <div class="cmpbar"><span class="muted">到期日</span><select id="wallExpiry" onchange="loadWalls()" style="background:var(--panel);color:var(--text);border:1px solid var(--border);padding:7px 10px;border-radius:8px">${exps.map(x=>`<option ${x===def?"selected":""}>${x}</option>`).join("")}</select>
    <span class="muted">现价 <b>${fmtNum(e.spot)}</b></span></div>
    <div id="wallSummary"></div>
    <div class="two-col" style="margin-top:8px"><div><div class="section-title" style="font-size:14px">持仓量/成交量 分布(墙)</div><div id="oiChart" style="height:420px;border:1px solid var(--border);border-radius:10px"></div></div>
    <div><div class="section-title" style="font-size:14px">Gamma 敞口 GEX 剖面</div><div id="gexChart" style="height:420px;border:1px solid var(--border);border-radius:10px"></div></div></div>
    <div class="small" id="wallNote"></div>`;
  loadWalls();
}
function pickMonthlyExpiry(exps){ // 选 ~2-5 周后的到期,信息量更足
  const today=new Date();
  for(const x of exps){const d=(new Date(x)-today)/864e5;if(d>=14&&d<=45)return x;}
  return exps[Math.min(2,exps.length-1)];
}
async function loadWalls(){
  const exp=document.getElementById("wallExpiry").value;
  document.getElementById("wallSummary").innerHTML='<div class="loading">计算期权墙…</div>';
  const d=await j(`/api/options/walls?ticker=${encodeURIComponent(curTicker)}&expiry=${exp}`);
  if(d.error){document.getElementById("wallSummary").innerHTML='<div class="muted">'+d.error+'</div>';return;}
  if(!d.totalOI && !d.totalVol){
    document.getElementById("wallSummary").innerHTML='<div class="muted">该到期日暂无有效期权数据(OI 与成交量均为空)— 可能是新上市/流动性低,或 Yahoo 该到期数据缺失。换个到期日(优先月度第三个周五)再试。</div>';
    ['oiChart','gexChart'].forEach(id=>{const el=document.getElementById(id);if(el)echarts.getInstanceByDom(el)?.clear();});
    document.getElementById("wallNote").textContent=d.note||"";
    return;
  }
  window._walls=d;
  window._wallBasis=d.basis;
  const isVol=d.basis==="volume";
  const basisWord=isVol?"成交量":"未平仓量(OI)";
  const gexPos=d.netGex>=0;
  const gexLabel=gexPos?"正 GEX · 倾向钉价/抑制波动":"负 GEX · 放大波动/助涨助跌";
  const pcActive=isVol?d.pcRatioVol:d.pcRatioOI;
  const pcBias=pcActive==null?"":(pcActive<0.7?"偏看涨":(pcActive>1.0?"偏看跌":"中性"));
  const basisBanner=isVol
    ?`<div class="error" style="background:rgba(246,195,67,.12);color:var(--yellow);margin-bottom:10px;font-size:13px">⚠ 本到期日<b>未平仓量(OI)缺失</b>(OI 隔夜更新滞后/近月合约常见),墙·MaxPain·GEX 已自动改用<b>成交量(Volume)</b>作代理 — 更反映当日新建仓,但非持仓累计。</div>`
    :"";
  document.getElementById("wallSummary").innerHTML=`${basisBanner}<div class="grid">
    <div class="card"><div class="k">Max Pain 最大痛点 <span class="muted" style="font-size:10px">(${basisWord})</span></div><div class="v">$${fmtNum(d.maxPain)} <span class="${d.maxPainVsSpot>=0?'green':'red'}" style="font-size:13px">(${fmtPct(d.maxPainVsSpot)})</span></div></div>
    <div class="card"><div class="k">净 Gamma 敞口</div><div class="v ${gexPos?'green':'red'}" style="font-size:15px">${gexLabel}</div></div>
    ${card("Gamma Flip 翻转点",d.gammaFlip!=null?"$"+fmtNum(d.gammaFlip):"—")}
    <div class="card"><div class="k">Put/Call 比率(${isVol?'成交量':'OI'})</div><div class="v">${pcActive??"—"} <span class="muted" style="font-size:12px">${pcBias}</span></div></div>
    ${card("总"+basisWord,fmtBig(isVol?d.totalVol:d.totalOI))}
    ${card("到期天数",d.daysToExpiry+" 天")}
  </div>`;
  document.getElementById("wallNote").textContent=d.note;
  drawOIChart(d);drawGexChart(d);
}
function drawOIChart(d){
  const el=document.getElementById("oiChart");if(!el)return;echarts.getInstanceByDom(el)?.dispose();
  const ks=d.oiDist.map(x=>x.strike);
  const suf=d.basis==="volume"?"Vol":"OI";
  const ch=echarts.init(el,'dark');
  ch.setOption({backgroundColor:'#161b22',grid:{left:55,right:20,top:30,bottom:30},
    legend:{data:['Call '+suf,'Put '+suf],textStyle:{color:'#8b949e'},top:4},
    tooltip:{trigger:'axis',axisPointer:{type:'shadow'}},
    xAxis:{type:'value',axisLabel:{color:'#8b949e'},splitLine:{lineStyle:{color:'#21262d'}}},
    yAxis:{type:'category',data:ks,axisLabel:{color:'#8b949e'},inverse:false},
    series:[
      {name:'Call '+suf,type:'bar',stack:'x',data:d.oiDist.map(x=>x.call),itemStyle:{color:'rgba(239,83,80,.75)'}},
      {name:'Put '+suf,type:'bar',stack:'y',data:d.oiDist.map(x=>-x.put),itemStyle:{color:'rgba(38,166,154,.75)'},
       markLine:{symbol:'none',silent:true,data:[
         {yAxis:nearestIdx(ks,d.spot),lineStyle:{color:'#f6c343',type:'dashed'},label:{formatter:'现价',color:'#f6c343'}},
         {yAxis:nearestIdx(ks,d.maxPain),lineStyle:{color:'#58a6ff',type:'dotted'},label:{formatter:'MaxPain',color:'#58a6ff'}}]}}
    ]});
}
function drawGexChart(d){
  const el=document.getElementById("gexChart");if(!el)return;echarts.getInstanceByDom(el)?.dispose();
  const ks=d.gexDist.map(x=>x.strike);
  const ch=echarts.init(el,'dark');
  ch.setOption({backgroundColor:'#161b22',grid:{left:55,right:20,top:20,bottom:30},
    tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:p=>`行权价 ${p[0].axisValue}<br/>GEX ${(p[0].data/1e6).toFixed(1)}M`},
    xAxis:{type:'value',axisLabel:{color:'#8b949e',formatter:v=>(v/1e6).toFixed(0)+'M'},splitLine:{lineStyle:{color:'#21262d'}}},
    yAxis:{type:'category',data:ks,axisLabel:{color:'#8b949e'}},
    series:[{type:'bar',data:d.gexDist.map(x=>x.gex),itemStyle:{color:p=>p.data>=0?'rgba(38,166,154,.8)':'rgba(239,83,80,.8)'},
      markLine:{symbol:'none',silent:true,data:[{yAxis:nearestIdx(ks,d.spot),lineStyle:{color:'#f6c343',type:'dashed'},label:{formatter:'现价',color:'#f6c343'}}]}}]});
}
const nearestIdx=(arr,v)=>{if(v==null)return -1;let bi=0,bd=1e18;arr.forEach((x,i)=>{const dd=Math.abs(x-v);if(dd<bd){bd=dd;bi=i;}});return bi;};

// ---------- 多股对比 ----------
let cmpChart=null;
function loadCompare(){
  loadedTabs.compare=true;
  const el=document.getElementById("tabc-compare");
  const def=[curTicker,...watchlist.filter(t=>t!==curTicker)].slice(0,5).join(",");
  el.innerHTML=`<div class="section-title">多股归一化对比 + 相关性 <span class="tag">skill: stock-correlation</span></div>
    <div class="cmpbar"><input id="cmpInput" value="${def}" placeholder="逗号分隔,如 AAPL,MSFT,NVDA"><select id="cmpPeriod" style="background:var(--panel);color:var(--text);border:1px solid var(--border);padding:8px;border-radius:8px"><option>6mo</option><option selected>1y</option><option>2y</option></select><button class="search" style="margin:0" onclick="runCompare()">对比</button></div>
    <div id="cmpChart"></div><div id="cmpStats"></div>`;
  runCompare();
}
async function runCompare(){
  const tickers=document.getElementById("cmpInput").value,period=document.getElementById("cmpPeriod").value;
  document.getElementById("cmpStats").innerHTML='<div class="loading">计算中…</div>';
  const d=await j(`/api/compare?tickers=${encodeURIComponent(tickers)}&period=${period}`);
  if(d.error){document.getElementById("cmpStats").innerHTML='<div class="error">'+d.error+'</div>';return;}
  const el=document.getElementById("cmpChart");if(cmpChart)cmpChart.remove?.();
  cmpChart=LightweightCharts.createChart(el,{layout:{background:{color:"#161b22"},textColor:"#8b949e"},grid:{vertLines:{color:"#21262d"},horzLines:{color:"#21262d"}},rightPriceScale:{borderColor:"#21262d"},timeScale:{borderColor:"#21262d"},width:el.clientWidth,height:420});
  const palette=["#58a6ff","#26a69a","#f6c343","#ef5350","#a78bfa","#ff9f40","#e6edf3","#56d364"];
  d.tickers.forEach((t,i)=>{const s=cmpChart.addLineSeries({color:palette[i%palette.length],lineWidth:2,title:t,priceLineVisible:false});s.setData(d.series[t]);});
  cmpChart.timeScale().fitContent();
  const statRows=d.stats.map(s=>`<tr><td><b>${s.ticker}</b></td><td class="${s.totalReturn>=0?'green':'red'}">${fmtPct(s.totalReturn)}</td><td>${fmtNum(s.annVol)}%</td><td>${s.beta!=null?fmtNum(s.beta):"基准"}</td><td>${fmtNum(s.corrToBase)}</td></tr>`).join("");
  const hdr=d.tickers.map(t=>`<th>${t}</th>`).join("");
  const matrixRows=d.tickers.map((t,i)=>`<tr><td><b>${t}</b></td>${d.matrix[i].map(v=>`<td class="heat-cell" style="background:${heatCorr(v)}">${v.toFixed(2)}</td>`).join("")}</tr>`).join("");
  document.getElementById("cmpStats").innerHTML=`
    <div class="section-title" style="font-size:14px">区间表现(基准 ${d.base},${d.observations} 交易日)</div>
    <table><thead><tr><th>代码</th><th>区间涨幅</th><th>年化波动</th><th>Beta(对基准)</th><th>相关性(对基准)</th></tr></thead><tbody>${statRows}</tbody></table>
    <div class="section-title" style="font-size:14px">相关性矩阵</div>
    <table><thead><tr><th></th>${hdr}</tr></thead><tbody>${matrixRows}</tbody></table>
    <div class="small">归一化=区间起点 rebase 至 100。相关性基于日对数收益。相关≠因果,历史相关不保证未来。</div>`;
}
const heatCorr=v=>{const x=Math.max(-1,Math.min(1,v));if(x>=0)return`rgba(38,166,154,${0.12+x*0.5})`;return`rgba(239,83,80,${0.12+(-x)*0.5})`;};

// ---------- 市场热力图 ----------
let curHeat="stocks";
function switchHeat(h){curHeat=h;document.getElementById("sub-stocks").classList.toggle("active",h==="stocks");document.getElementById("sub-sectors").classList.toggle("active",h==="sectors");document.getElementById("heat-stocks").classList.toggle("hidden",h!=="stocks");document.getElementById("heat-sectors").classList.toggle("hidden",h!=="sectors");if(!window._heatData)return;if(h==="stocks")setTimeout(renderTreemap,50);else setTimeout(renderSectorTreemap,50);}
async function loadHeatmap(force){
  window._heatLoaded=true;
  if(force)window._heatData=null;
  if(window._heatData&&!force){renderTreemap();renderSectors();return;}
  if(!window._heatData)document.getElementById("heat-stocks").innerHTML='<div class="loading">热力图加载中(首次约 10-20 秒,抓取近百只个股)…</div>';
  const d=await j("/api/heatmap"+(force?"?force=1":""));
  if(d.error){document.getElementById("heat-stocks").innerHTML='<div class="error">'+d.error+'</div>';return;}
  window._heatData=d;
  document.getElementById("heat-stocks").innerHTML='<div id="treemap"></div>';
  document.getElementById("heat-note").textContent=`更新于 ${d.asof} · ${d.note}`;
  updateHeatStatus(d);
  renderTreemap();renderSectors();
  setupHeatAutoRefresh(d);
}
function updateHeatStatus(d){
  const el=document.getElementById("heat-status");if(!el)return;
  const open=d.marketOpen?'<span class="green">● 开盘中</span>':'<span class="muted">○ 已收盘</span>';
  const src=d.cached?(d.stale?'缓存(数据源暂不可用)':'缓存'):'实时抓取';
  el.innerHTML=`${open} · ${d.asof} · ${src}`;
}
let _heatTimer=null;
function setupHeatAutoRefresh(d){
  if(_heatTimer)clearInterval(_heatTimer);
  if(!d.marketOpen)return;                              // 盘后用缓存,不自动刷新
  _heatTimer=setInterval(()=>{
    if(curPage!=="heatmap"||document.hidden)return;     // 仅在热力图页且标签可见时刷新
    loadHeatmap(true);
  },300000);                                            // 盘中每 5 分钟(与后端 TTL 对齐,温柔对待 yahoo)
}
// ---------- 板块编辑器(任务4) ----------
function toggleSectorEditor(){
  const el=document.getElementById("sector-editor");if(!el)return;
  if(!el.classList.contains("hidden")){el.classList.add("hidden");return;}
  el.classList.remove("hidden");loadSectorEditor();
}
async function loadSectorEditor(){
  const el=document.getElementById("sector-editor");el.innerHTML='<div class="loading">加载板块配置…</div>';
  const d=await j("/api/heatmap/sectors");
  window._sectorCfg=(d.sectors||[]).map(s=>({name:s.name,etf:s.etf||"",tickers:(s.tickers||[]).join(", ")}));
  window._sectorDefaults=d.defaults||[];
  renderSectorEditor();
}
const escAttr=s=>String(s||"").replace(/&/g,"&amp;").replace(/"/g,"&quot;").replace(/</g,"&lt;");
function renderSectorEditor(){
  const el=document.getElementById("sector-editor");if(!el)return;
  const rows=window._sectorCfg.map((s,i)=>`
    <div class="sec-edit-row">
      <input class="se-name" placeholder="板块名" value="${escAttr(s.name)}" oninput="window._sectorCfg[${i}].name=this.value">
      <input class="se-etf" placeholder="ETF(可选)" value="${escAttr(s.etf)}" oninput="window._sectorCfg[${i}].etf=this.value">
      <button class="ma-pill" style="color:var(--red)" onclick="delSector(${i})">删除板块</button>
      <textarea class="se-tk" placeholder="成分股,逗号或空格分隔 如 AAPL, MSFT, NVDA" oninput="window._sectorCfg[${i}].tickers=this.value">${escAttr(s.tickers)}</textarea>
    </div>`).join("");
  el.innerHTML=`
    <div class="muted" style="font-size:13px;margin-bottom:10px">自定义板块:可增删板块、编辑成分股与对标 ETF。<b>同一只股票可填进多个板块</b>。保存后热力图按你的配置重算。</div>
    ${rows||'<div class="muted">暂无板块,点下方「添加板块」。</div>'}
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:12px;align-items:center">
      <button class="search" style="margin:0" onclick="addSector()">＋ 添加板块</button>
      <button class="search" style="margin:0;background:var(--green)" onclick="saveSectors()">✓ 保存并刷新热力图</button>
      <button class="ma-pill" onclick="restoreDefaultSectors()">恢复内置默认</button>
    </div>`;
}
function addSector(){window._sectorCfg.push({name:"新板块",etf:"",tickers:""});renderSectorEditor();}
function delSector(i){window._sectorCfg.splice(i,1);renderSectorEditor();}
function restoreDefaultSectors(){
  if(!confirm("用内置默认覆盖当前配置?(保存后才会生效)"))return;
  window._sectorCfg=(window._sectorDefaults||[]).map(s=>({name:s.name,etf:s.etf||"",tickers:(s.tickers||[]).join(", ")}));
  renderSectorEditor();
}
async function saveSectors(){
  const payload={sectors:window._sectorCfg.map(s=>({name:(s.name||"").trim(),etf:(s.etf||"").trim(),
    tickers:(s.tickers||"").split(/[\s,]+/).map(t=>t.trim().toUpperCase()).filter(Boolean)})).filter(s=>s.name&&s.tickers.length)};
  if(!payload.sectors.length){alert("至少配置一个含成分股的板块");return;}
  const el=document.getElementById("sector-editor");el.innerHTML='<div class="loading">保存并重算热力图(约 10-20 秒)…</div>';
  try{await fetch("/api/heatmap/sectors",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});}
  catch(e){alert("保存失败");}
  await loadHeatmap(true);
  loadSectorEditor();
}
function renderTreemap(){
  const d=window._heatData;if(!d)return;const el=document.getElementById("treemap");if(!el)return;
  const data=d.sectors.map(s=>({name:s.sector,value:s.size,children:s.stocks.map(st=>({name:st.ticker,value:st.size,change:st.change,itemStyle:{color:heatColor(st.change)}}))}));
  echarts.getInstanceByDom(el)?.dispose();
  const ch=echarts.init(el,'dark');
  ch.setOption({backgroundColor:'#161b22',tooltip:{formatter:p=>{const c=p.data.change;return `<b>${p.name}</b>${c!=null?'<br/>涨跌 '+fmtPct(c)+'<br/>市值 ~$'+fmtNum(p.value,1)+'B':''}`;}},
    series:[{type:'treemap',roam:false,nodeClick:false,breadcrumb:{show:false},width:'100%',height:'100%',
      levels:[{itemStyle:{borderColor:'#0d1117',borderWidth:3,gapWidth:3}},{itemStyle:{borderColor:'#0d1117',borderWidth:1,gapWidth:1},upperLabel:{show:true,height:22,color:'#e6edf3',fontWeight:600,fontSize:12}}],
      label:{show:true,formatter:p=>p.data.change!=null?`${p.name}\n${(p.data.change>=0?'+':'')+p.data.change.toFixed(1)}%`:p.name,color:'#fff',fontSize:11,fontWeight:600},
      data:data}]});
  window.addEventListener("resize",()=>ch.resize());
}
function renderSectors(){
  const d=window._heatData;if(!d)return;
  document.getElementById("heat-sectors").innerHTML='<div id="sectreemap"></div>';
  renderSectorTreemap();
}
function renderSectorTreemap(){
  const d=window._heatData;if(!d)return;const el=document.getElementById("sectreemap");if(!el)return;
  const etfMap={};(d.sectorEtfs||[]).forEach(s=>etfMap[s.sector]={etf:s.etf,change:s.change});
  const data=d.sectors.map(s=>({name:s.sector,value:s.size,change:s.change,n:s.stocks.length,
    etf:(etfMap[s.sector]||{}).etf,etfChg:(etfMap[s.sector]||{}).change,itemStyle:{color:heatColor(s.change)}}));
  echarts.getInstanceByDom(el)?.dispose();
  const ch=echarts.init(el,'dark');
  ch.setOption({backgroundColor:'#161b22',
    tooltip:{formatter:p=>`<b>${p.name}</b><br/>市值加权 ${fmtPct(p.data.change)}<br/>总市值 ~$${fmtNum(p.value,0)}B · ${p.data.n}只`+(p.data.etf?`<br/>ETF ${p.data.etf} ${fmtPct(p.data.etfChg)}`:'')},
    series:[{type:'treemap',roam:false,nodeClick:false,breadcrumb:{show:false},width:'100%',height:'100%',
      itemStyle:{borderColor:'#0d1117',borderWidth:2,gapWidth:2},
      label:{show:true,formatter:p=>p.data.change!=null?`${p.name}\n${(p.data.change>=0?'+':'')+p.data.change.toFixed(2)}%`:p.name,color:'#fff',fontSize:13,fontWeight:600},
      data:data}]});
  window.addEventListener("resize",()=>ch.resize());
}

// ---------- 启动 ----------
document.getElementById("tickerInput").addEventListener("keydown",e=>{if(e.key==="Enter")loadTicker();});
loadMarket();loadSettings();loadWatch().then(()=>{
  loadTicker(watchlist[0]||"AAPL");                       // 预载个股看板数据(内部会切到 stock 页)
  let sp;try{sp=localStorage.getItem("curPage");}catch(e){}
  if(sp&&sp!=="stock")switchPage(sp);                     // 刷新前在别的页 → 恢复到那一页
});
setInterval(loadAlerts,60000);
</script>
</body>
</html>"""


# 启动收盘后 K 线刷新的后台线程(gunicorn 导入模块时即生效;ENABLE_SCHEDULER=0 可禁用)。
start_scheduler()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"个股看板启动中(开发服务器) → http://localhost:{port}")
    app.run(host=os.environ.get("HOST", "0.0.0.0"), port=port, debug=False)
