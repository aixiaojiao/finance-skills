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
    CREATE TABLE IF NOT EXISTS alerts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL, kind TEXT NOT NULL,
        level REAL, note TEXT, active INTEGER DEFAULT 1, created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS watchlist(
        ticker TEXT PRIMARY KEY, added_at TEXT
    );
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY, value TEXT
    );
    """)
    # 自选股默认值(仅首次为空时)
    cur = con.execute("SELECT COUNT(*) c FROM watchlist").fetchone()
    if cur[0] == 0:
        con.executemany("INSERT INTO watchlist(ticker, added_at) VALUES(?, ?)",
                        [("AAPL", ""), ("NVDA", ""), ("TSLA", "")])
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

def cached(ttl):
    def deco(fn):
        def wrap(*args):
            key = (fn.__name__, args)
            now = time.time()
            hit = _CACHE.get(key)
            if hit and now - hit[0] < ttl:
                return hit[1]
            val = fn(*args)
            _CACHE[key] = (now, val)
            return val
        return wrap
    return deco


def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    return 100 - 100 / (1 + gain / loss)


@cached(90)
def get_info(ticker):
    return yf.Ticker(ticker).info or {}


@cached(120)
def get_history_df(ticker, period, interval="1d"):
    df = yf.Ticker(ticker).history(period=period, interval=interval, auto_adjust=False)
    return df if df is not None else pd.DataFrame()


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
        "financials": {
            "marketCap": _num(info.get("marketCap")), "trailingPE": _num(info.get("trailingPE")),
            "forwardPE": _num(info.get("forwardPE")), "priceToBook": _num(info.get("priceToBook")),
            "eps": _num(info.get("trailingEps")), "revenue": _num(info.get("totalRevenue")),
            "profitMargin": _num(info.get("profitMargins")), "grossMargin": _num(info.get("grossMargins")),
            "dividendYield": _num(info.get("dividendYield")), "beta": _num(info.get("beta")),
            "fiftyTwoWeekHigh": _num(info.get("fiftyTwoWeekHigh")), "fiftyTwoWeekLow": _num(info.get("fiftyTwoWeekLow")),
        },
        "analyst": {
            "targetMean": _num(info.get("targetMeanPrice")), "targetHigh": _num(info.get("targetHighPrice")),
            "targetLow": _num(info.get("targetLowPrice")), "recommendation": _safe(info.get("recommendationKey")),
            "numAnalysts": _num(info.get("numberOfAnalystOpinions")),
        },
    })


# ============================ API: K线 + 均线 ============================

FETCH_MAP = {"1mo": "1y", "3mo": "2y", "6mo": "2y", "1y": "2y", "2y": "5y", "5y": "max"}
DISPLAY_BARS = {"1mo": 22, "3mo": 66, "6mo": 126, "1y": 252, "2y": 504, "5y": 1300}
MA_WINDOWS = [5, 10, 20, 50, 200]


@app.route("/api/history")
def api_history():
    ticker = (request.args.get("ticker") or "").strip().upper()
    period = request.args.get("period", "6mo")
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    try:
        df = get_history_df(ticker, FETCH_MAP.get(period, "2y")).copy()
    except Exception as e:
        return jsonify({"error": f"获取历史失败: {e}"}), 502
    if df is None or df.empty:
        return jsonify({"error": "无历史数据"}), 404

    for w in MA_WINDOWS:
        df[f"ma{w}"] = df["Close"].rolling(w).mean()
    df = df.tail(DISPLAY_BARS.get(period, 126))

    candles, volumes = [], []
    ma_series = {str(w): [] for w in MA_WINDOWS}
    for idx, row in df.iterrows():
        ts = idx.strftime("%Y-%m-%d")
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
    return jsonify({"ticker": ticker, "candles": candles, "volumes": volumes, "ma": ma_series})


# ============================ API: SEPA ============================

@cached(300)
def _spy_return_252():
    try:
        df = get_history_df("^GSPC", "2y")
        if df is None or df.empty or len(df) < 252:
            return None
        c = df["Close"].dropna()
        return c.iloc[-1] / c.iloc[-252] - 1
    except Exception:
        return None


@app.route("/api/sepa")
def api_sepa():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    try:
        df = get_history_df(ticker, "2y").copy()
        info = get_info(ticker)
    except Exception as e:
        return jsonify({"error": f"获取失败: {e}"}), 502
    if df is None or df.empty or len(df) < 60:
        return jsonify({"error": "历史数据不足,无法做 SEPA 分析"}), 404

    close = df["Close"]
    price = _num(close.iloc[-1])
    ma50 = _num(close.rolling(50).mean().iloc[-1])
    ma150 = _num(close.rolling(150).mean().iloc[-1]) if len(df) >= 150 else None
    ma200 = _num(close.rolling(200).mean().iloc[-1]) if len(df) >= 200 else None
    ma200_1mo = _num(close.rolling(200).mean().iloc[-22]) if len(df) >= 222 else None
    win = min(len(df), 252)
    hi52 = _num(df["High"].tail(win).max())
    lo52 = _num(df["Low"].tail(win).min())

    rs_pass = rs_val = None
    if len(df) >= 252:
        stock_ret = price / _num(close.iloc[-252]) - 1
        spy_ret = _spy_return_252()
        if spy_ret is not None:
            rs_val = (stock_ret - spy_ret) * 100
            rs_pass = stock_ret > spy_ret

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
    cond(8, "相对强度 RS 跑赢大盘(代理)", rs_pass,
         f"相对 S&P500 {rs_val:+.1f}pp" if rs_val is not None else "数据不足")

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

    return jsonify({"ticker": ticker, "price": price, "stage": stage, "conditions": conds,
                    "passed": passed, "total": 8, "fundamentalGrade": fgrade,
                    "epsGrowth": eps_growth, "revGrowth": _num(info.get("revenueGrowth")),
                    "verdict": verdict, "verdictClass": vclass,
                    "rsNote": "RS 为相对 S&P500 涨幅代理,非全市场百分位排名"})


# ============================ API: 财报 ============================

@app.route("/api/earnings")
def api_earnings():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    t = yf.Ticker(ticker)
    upcoming, history = None, []
    try:
        ed = t.get_earnings_dates(limit=16)
    except Exception:
        ed = None
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
            cal = t.calendar
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
    try:
        raw = yf.Ticker(ticker).news or []
    except Exception:
        raw = []
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


@app.route("/api/market")
def api_market():
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
    return jsonify({"spx": spx, "ndx": ndx, "vix": vix, "environment": env, "environmentClass": env_class,
                    "sentiment": sentiment, "sentimentLabel": slabel,
                    "note": "情绪为 VIX/指数趋势/RSI 合成代理"})


# ============================ API: 自选股批量行情 ============================

@app.route("/api/quotes")
def api_quotes():
    raw = (request.args.get("tickers") or "").strip().upper()
    tickers = [t for t in raw.replace(" ", ",").split(",") if t]
    if not tickers:
        return jsonify({"quotes": []})
    out = []
    try:
        data = yf.download(tickers, period="5d", auto_adjust=False, progress=False, group_by="ticker")
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

def _compute_dcf(t, info):
    try:
        inc = t.income_stmt
        cf = t.cashflow
        if inc is None or inc.empty or "Total Revenue" not in inc.index:
            return None
        rev_row = inc.loc["Total Revenue"].dropna().astype(float)
        rev = rev_row[::-1]  # 旧 -> 新
        if len(rev) < 2:
            return None
        hist_cagr = (rev.iloc[-1] / rev.iloc[0]) ** (1 / (len(rev) - 1)) - 1
        y1 = hist_cagr
        try:
            re = t.revenue_estimate
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
    t = yf.Ticker(ticker)
    try:
        info = get_info(ticker)
    except Exception as e:
        return jsonify({"error": f"获取失败: {e}"}), 502
    price = _num(info.get("currentPrice") or info.get("regularMarketPrice"))

    dcf = _compute_dcf(t, info)

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
        ee = t.earnings_estimate
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
        et = t.eps_trend
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
    try:
        exps = list(yf.Ticker(ticker).options or [])
    except Exception as e:
        return jsonify({"error": f"获取失败: {e}"}), 502
    price = _num(get_info(ticker).get("currentPrice") or get_info(ticker).get("regularMarketPrice"))
    return jsonify({"ticker": ticker, "expiries": exps, "spot": price})


@app.route("/api/options/chain")
def api_option_chain():
    ticker = (request.args.get("ticker") or "").strip().upper()
    expiry = request.args.get("expiry")
    if not ticker or not expiry:
        return jsonify({"error": "缺少 ticker 或 expiry"}), 400
    try:
        oc = yf.Ticker(ticker).option_chain(expiry)
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
        oc = yf.Ticker(ticker).option_chain(expiry)
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

    # Max Pain:使期权买方总收益(=卖方赔付)最小的结算价
    def payout(S):
        tot = 0.0
        for k in strikes:
            tot += (cmap.get(k, {}).get("oi", 0)) * max(S - k, 0)
            tot += (pmap.get(k, {}).get("oi", 0)) * max(k - S, 0)
        return tot
    max_pain = min(strikes, key=payout)

    # OI 墙
    call_walls = sorted([{"strike": k, "oi": cmap[k]["oi"]} for k in cmap if cmap[k]["oi"] > 0],
                        key=lambda x: -x["oi"])[:6]
    put_walls = sorted([{"strike": k, "oi": pmap[k]["oi"]} for k in pmap if pmap[k]["oi"] > 0],
                       key=lambda x: -x["oi"])[:6]

    # GEX:每档 (OI_call·γ - OI_put·γ)·S²·1%·100(在当前现价下的剖面)
    gex_by_strike = []
    for k in strikes:
        civ = cmap.get(k, {}).get("iv", 0)
        piv = pmap.get(k, {}).get("iv", 0)
        gc = _bs_gamma(spot, k, T, r, civ) if civ > 0 else 0
        gp = _bs_gamma(spot, k, T, r, piv) if piv > 0 else 0
        gex = (cmap.get(k, {}).get("oi", 0) * gc - pmap.get(k, {}).get("oi", 0) * gp) * spot * spot * 0.01 * 100
        gex_by_strike.append({"strike": k, "gex": gex})
    net_gex = sum(x["gex"] for x in gex_by_strike)

    # Gamma Flip:净做市商 gamma 随假设现价 S 变化、由负转正的价位(零伽马)
    def gex_at(S):
        tot = 0.0
        for k in strikes:
            civ = cmap.get(k, {}).get("iv", 0)
            piv = pmap.get(k, {}).get("iv", 0)
            gc = _bs_gamma(S, k, T, r, civ) if civ > 0 else 0
            gp = _bs_gamma(S, k, T, r, piv) if piv > 0 else 0
            tot += (cmap.get(k, {}).get("oi", 0) * gc - pmap.get(k, {}).get("oi", 0) * gp)
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
            # 线性插值交叉点
            cross = prev_s if v == prev_v else prev_s + (s - prev_s) * (0 - prev_v) / (v - prev_v)
            if best is None or abs(cross - spot) < abs(best - spot):
                best = cross
        prev_s, prev_v = s, v
    if best is not None:
        gamma_flip = round(best, 2)

    oi_call = sum(v["oi"] for v in cmap.values())
    oi_put = sum(v["oi"] for v in pmap.values())
    vol_call = sum(v["vol"] for v in cmap.values())
    vol_put = sum(v["vol"] for v in pmap.values())

    # 给前端画图用的 OI 分布(限制档数,ATM 附近)
    near = sorted(strikes, key=lambda k: abs(k - spot))[:40]
    near = sorted(near)
    oi_dist = [{"strike": k, "call": cmap.get(k, {}).get("oi", 0), "put": pmap.get(k, {}).get("oi", 0)} for k in near]
    gex_dist = [x for x in gex_by_strike if x["strike"] in set(near)]

    return jsonify({
        "ticker": ticker, "expiry": expiry, "spot": spot, "daysToExpiry": round(days, 1),
        "maxPain": max_pain, "maxPainVsSpot": round((max_pain / spot - 1) * 100, 2),
        "callWalls": call_walls, "putWalls": put_walls,
        "netGex": net_gex, "gammaFlip": gamma_flip,
        "pcRatioOI": round(oi_put / oi_call, 2) if oi_call else None,
        "pcRatioVol": round(vol_put / vol_call, 2) if vol_call else None,
        "oiDist": oi_dist, "gexDist": gex_dist,
        "note": "GEX 用 BS gamma(IV/剩余到期/无风险利率)估算,Gamma Flip 为累计净GEX过零点近似",
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


@cached(600)
def compute_heatmap():
    all_t = [t for lst in HEATMAP_UNIVERSE.values() for t in lst]
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

    # 并行取市值
    with ThreadPoolExecutor(max_workers=16) as ex:
        mcaps = dict(zip(all_t, ex.map(_mcap, all_t)))

    sectors = []
    for sector, tickers in HEATMAP_UNIVERSE.items():
        children = []
        for t in tickers:
            chg, last, dvol = change_of(t)
            if chg is None:
                continue
            size = mcaps.get(t) or dvol or 1e9
            children.append({"ticker": t, "change": round(chg, 2), "price": last,
                             "size": round(size / 1e9, 2)})  # 单位:十亿
        if children:
            tot = sum(c["size"] for c in children)
            wavg = sum(c["change"] * c["size"] for c in children) / tot if tot else 0
            children.sort(key=lambda c: -c["size"])
            sectors.append({"sector": sector, "etf": SECTOR_ETFS.get(sector),
                            "change": round(wavg, 2), "size": round(tot, 1), "stocks": children})

    # 板块 ETF 行情
    etf_list = list(SECTOR_ETFS.values())
    etf_perf = {}
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
    sector_etfs = [{"sector": s, "etf": e, "change": etf_perf.get(e)} for s, e in SECTOR_ETFS.items()]
    sector_etfs.sort(key=lambda x: (x["change"] is None, -(x["change"] or 0)))

    return {"sectors": sectors, "sectorEtfs": sector_etfs,
            "asof": time.strftime("%Y-%m-%d %H:%M"), "note": "面积≈市值(十亿美元),颜色=当日涨跌幅"}


@app.route("/api/heatmap")
def api_heatmap():
    return jsonify(compute_heatmap())


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
        db.execute("INSERT INTO positions(ticker,shares,entry,stop,target,opened_at,status,note) VALUES(?,?,?,?,?,?, 'open', ?)",
                   (tk, float(d["shares"]), float(d["entry"]), _num(d.get("stop")), _num(d.get("target")),
                    time.strftime("%Y-%m-%d"), d.get("note") or ""))
        db.commit()
        return jsonify({"ok": True})
    # GET: 带实时盈亏
    rows = [dict(r) for r in db.execute("SELECT * FROM positions ORDER BY id DESC").fetchall()]
    open_rows = [r for r in rows if r["status"] == "open"]
    live = _live_prices([r["ticker"] for r in open_rows])
    acct = _get_setting("accountValue")
    acct = float(acct) if acct else None
    total_mv = total_cost = total_pl = total_risk = 0.0
    for r in rows:
        lp = live.get(r["ticker"], {})
        price = lp.get("price") if r["status"] == "open" else r.get("exit_price")
        r["price"] = price
        r["ma20"] = lp.get("ma20")
        cost = r["shares"] * r["entry"]
        r["cost"] = cost
        if price is not None:
            mv = r["shares"] * price
            r["marketValue"] = mv
            r["pl"] = mv - cost
            r["plPct"] = (price / r["entry"] - 1) * 100
            r["toStopPct"] = ((price / r["stop"] - 1) * 100) if r["stop"] else None
            if r["stop"] and r["status"] == "open":
                rps = r["entry"] - r["stop"]
                r["rMultiple"] = (price - r["entry"]) / rps if rps else None
            if r["status"] == "open":
                total_mv += mv
                total_cost += cost
                total_pl += mv - cost
                if r["stop"]:
                    total_risk += max(0.0, (price - r["stop"]) * r["shares"])
    summary = {"openCount": len(open_rows), "totalMarketValue": total_mv, "totalCost": total_cost,
               "totalPL": total_pl, "totalPLPct": (total_pl / total_cost * 100) if total_cost else None,
               "totalRisk": total_risk, "account": acct,
               "investedPct": (total_cost / acct * 100) if acct else None,
               "riskPct": (total_risk / acct * 100) if acct else None}
    return jsonify({"positions": rows, "summary": summary})


@app.route("/api/positions/<int:pid>", methods=["PUT", "DELETE"])
def api_position_one(pid):
    db = get_db()
    if request.method == "DELETE":
        db.execute("DELETE FROM positions WHERE id=?", (pid,))
        db.commit()
        return jsonify({"ok": True})
    d = request.get_json(force=True, silent=True) or {}
    if d.get("action") == "close":
        db.execute("UPDATE positions SET status='closed', exit_price=?, closed_at=? WHERE id=?",
                   (_num(d.get("exit_price")), time.strftime("%Y-%m-%d"), pid))
    else:
        for field in ("stop", "target", "note"):
            if field in d:
                db.execute(f"UPDATE positions SET {field}=? WHERE id=?", (_num(d[field]) if field != "note" else d[field], pid))
    db.commit()
    return jsonify({"ok": True})


# ============================ API: 自选股 / 设置 / 预警(持久化) ============================

@app.route("/api/watchlist", methods=["GET", "POST", "DELETE"])
def api_watchlist():
    db = get_db()
    if request.method == "GET":
        rows = db.execute("SELECT ticker FROM watchlist ORDER BY added_at, ticker").fetchall()
        return jsonify({"watchlist": [r["ticker"] for r in rows]})
    tk = ((request.get_json(force=True, silent=True) or {}).get("ticker") or request.args.get("ticker") or "").strip().upper()
    if not tk:
        return jsonify({"error": "缺少 ticker"}), 400
    if request.method == "POST":
        db.execute("INSERT OR IGNORE INTO watchlist(ticker, added_at) VALUES(?, ?)", (tk, time.strftime("%Y-%m-%d %H:%M:%S")))
    else:
        db.execute("DELETE FROM watchlist WHERE ticker=?", (tk,))
    db.commit()
    rows = db.execute("SELECT ticker FROM watchlist ORDER BY added_at, ticker").fetchall()
    return jsonify({"watchlist": [r["ticker"] for r in rows]})


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
  a{color:var(--accent);text-decoration:none} a:hover{text-decoration:underline}
  .marketbar{display:flex;align-items:center;gap:14px;padding:8px 24px;background:#0a0d12;border-bottom:1px solid var(--border);flex-wrap:wrap;font-size:13px}
  .mb-item{display:flex;gap:6px;align-items:baseline}.mb-item .lbl{color:var(--muted)}
  .badge{padding:3px 10px;border-radius:6px;font-weight:600;font-size:12px}
  .buy{background:rgba(38,166,154,.15);color:var(--green)}.sell{background:rgba(239,83,80,.15);color:var(--red)}.hold{background:rgba(246,195,67,.15);color:var(--yellow)}
  .gauge{display:flex;align-items:center;gap:8px}
  .gauge .bar{width:120px;height:8px;border-radius:4px;background:linear-gradient(90deg,#ef5350,#f6c343,#26a69a);position:relative}
  .gauge .dot{position:absolute;top:-3px;width:14px;height:14px;border-radius:50%;background:#fff;border:2px solid #0a0d12;transform:translateX(-50%)}
  header{padding:12px 24px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:16px;flex-wrap:wrap}
  h1{font-size:18px;margin:0;font-weight:600}
  .topnav{display:flex;gap:6px}
  .topnav button{background:transparent;border:1px solid var(--border);color:var(--muted);padding:7px 16px;border-radius:8px;cursor:pointer;font-size:14px;font-weight:500}
  .topnav button.active{background:var(--accent);color:#fff;border-color:var(--accent)}
  .search{display:flex;gap:8px;margin-left:auto}
  .search input{background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px 12px;border-radius:8px;font-size:14px;width:140px;text-transform:uppercase}
  .search button{background:var(--accent);border:none;color:#fff;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:14px}
  .watchstrip{display:flex;gap:8px;padding:8px 24px;background:#0a0d12;border-bottom:1px solid var(--border);overflow-x:auto;align-items:center}
  .watchstrip .lbl{color:var(--muted);font-size:12px;white-space:nowrap}
  .wchip{display:flex;gap:6px;align-items:center;background:var(--panel);border:1px solid var(--border);padding:5px 10px;border-radius:8px;cursor:pointer;white-space:nowrap;font-size:13px}
  .wchip .x{color:var(--muted);font-size:11px;padding-left:2px}.wchip .x:hover{color:var(--red)}
  main{padding:20px 24px;max-width:1340px;margin:0 auto}
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
  .subtabs{display:flex;gap:6px;margin:14px 0}
  .subtabs button{background:var(--panel);border:1px solid var(--border);color:var(--muted);padding:6px 14px;border-radius:8px;cursor:pointer;font-size:13px}.subtabs button.active{background:var(--accent);color:#fff;border-color:var(--accent)}
</style>
</head>
<body>

<div class="marketbar" id="marketbar"><span class="muted">大盘加载中…</span></div>

<header>
  <h1>📈 看板</h1>
  <div class="topnav">
    <button id="nav-stock" class="active" onclick="switchPage('stock')">个股看板</button>
    <button id="nav-positions" onclick="switchPage('positions')">持仓</button>
    <button id="nav-heatmap" onclick="switchPage('heatmap')">市场热力图</button>
  </div>
  <div class="search" id="stockSearch">
    <input id="tickerInput" placeholder="代码 如 AAPL" value="AAPL" />
    <button onclick="loadTicker()">查询</button>
  </div>
</header>

<div class="watchstrip" id="watchstrip"><span class="lbl">自选股</span></div>
<div id="alertBanner"></div>

<main>
  <!-- 个股看板页 -->
  <div id="page-stock">
    <div class="tabs">
      <button id="tab-overview" class="active" onclick="switchTab('overview')">概览</button>
      <button id="tab-position" onclick="switchTab('position')">仓位计算</button>
      <button id="tab-valuation" onclick="switchTab('valuation')">估值</button>
      <button id="tab-options" onclick="switchTab('options')">期权墙</button>
      <button id="tab-compare" onclick="switchTab('compare')">多股对比</button>
    </div>
    <div id="tabc-overview"><div class="loading">加载中…</div></div>
    <div id="tabc-position" class="hidden"></div>
    <div id="tabc-valuation" class="hidden"></div>
    <div id="tabc-options" class="hidden"></div>
    <div id="tabc-compare" class="hidden"></div>
  </div>

  <!-- 持仓页 -->
  <div id="page-positions" class="hidden"><div class="loading">加载持仓…</div></div>

  <!-- 市场热力图页 -->
  <div id="page-heatmap" class="hidden">
    <div class="subtabs">
      <button id="sub-stocks" class="active" onclick="switchHeat('stocks')">个股热力图</button>
      <button id="sub-sectors" onclick="switchHeat('sectors')">板块热力图</button>
      <button class="muted" style="margin-left:auto;cursor:pointer" onclick="loadHeatmap(true)">↻ 刷新</button>
    </div>
    <div id="heat-stocks"><div class="loading">热力图加载中(首次约 10-20 秒)…</div></div>
    <div id="heat-sectors" class="hidden"></div>
    <div class="small" id="heat-note"></div>
  </div>

  <div class="disclaimer">
    数据来源 Yahoo Finance(yfinance),非实时、有延迟,仅供研究与学习,不构成投资建议。<br>
    SEPA 基于 Minervini 趋势模板;估值为简化 DCF;RS/情绪/热力图面积为代理算法。
  </div>
</main>

<script>
const POPULAR=["AAPL","TSLA","NVDA","MSFT","GOOGL","AMZN","META","AMD"];
const MA_COLORS={"5":"#f6c343","10":"#ff9f40","20":"#58a6ff","50":"#a78bfa","200":"#e6edf3"};
const MA_DEFAULT_ON={"5":false,"10":false,"20":true,"50":true,"200":true};
let chart,candleSeries,volSeries,maSeries={},curTicker="AAPL",curPeriod="6mo",curTab="overview",curPage="stock";
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
  ["stock","positions","heatmap"].forEach(x=>{
    document.getElementById("page-"+x).classList.toggle("hidden",p!==x);
    document.getElementById("nav-"+x).classList.toggle("active",p===x);
  });
  document.getElementById("stockSearch").style.display=p==="stock"?"flex":"none";
  if(p==="heatmap" && !window._heatLoaded) loadHeatmap();
  if(p==="positions") loadPositions();
}
function switchTab(t){
  curTab=t;
  ["overview","position","valuation","options","compare"].forEach(x=>{
    document.getElementById("tab-"+x).classList.toggle("active",x===t);
    document.getElementById("tabc-"+x).classList.toggle("hidden",x!==t);
  });
  if(t==="position"&&loadedTabs.position!==curTicker)loadPosition();
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
  const el=document.getElementById("watchstrip");
  el.innerHTML='<span class="lbl">自选股</span>'+watchlist.map(t=>`<span class="wchip" id="w-${t}" onclick="loadTicker('${t}')">${t} <span class="muted">…</span><span class="x" onclick="event.stopPropagation();toggleWatch('${t}')">✕</span></span>`).join("")||'<span class="lbl">自选股(空)</span>';
  if(!watchlist.length)return;
  const q=await j("/api/quotes?tickers="+watchlist.join(","));
  q.quotes.forEach(x=>{const c=document.getElementById("w-"+x.ticker);if(c)c.innerHTML=`${x.ticker} <span class="${(x.changePct||0)>=0?'green':'red'}">${fmtPct(x.changePct)}</span><span class="x" onclick="event.stopPropagation();toggleWatch('${x.ticker}')">✕</span>`;});
}

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
  loadChart(curPeriod);loadSepa();loadEarnings();loadLiquidity();loadNews();loadDecision();loadAlerts();
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
   <div class="controls">${["1mo","3mo","6mo","1y","2y","5y"].map(p=>`<button class="${p===curPeriod?'active':''}" onclick="loadChart('${p}')">${p}</button>`).join("")}<span class="ma-toggles">${maToggles}<label style="margin-left:6px"><input type="checkbox" id="wallOverlay" onchange="toggleOptionWall(this.checked)"><span style="color:#f6c343">期权墙</span></label></span></div>
   <div id="chart"></div>
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
   <div class="section-title">重要消息 / 新闻</div><div id="news"><div class="loading">加载中…</div></div>`;
  initChart();
}

function initChart(){
  const el=document.getElementById("chart");if(!el)return;
  chart=LightweightCharts.createChart(el,{layout:{background:{color:"#161b22"},textColor:"#8b949e"},grid:{vertLines:{color:"#21262d"},horzLines:{color:"#21262d"}},rightPriceScale:{borderColor:"#21262d"},timeScale:{borderColor:"#21262d"},crosshair:{mode:0},width:el.clientWidth,height:440});
  candleSeries=chart.addCandlestickSeries({upColor:"#26a69a",downColor:"#ef5350",borderVisible:false,wickUpColor:"#26a69a",wickDownColor:"#ef5350"});
  maSeries={};Object.keys(MA_COLORS).forEach(w=>{maSeries[w]=chart.addLineSeries({color:MA_COLORS[w],lineWidth:w==="200"?2:1,priceLineVisible:false,lastValueVisible:false,visible:MA_DEFAULT_ON[w]});});
  volSeries=chart.addHistogramSeries({priceFormat:{type:"volume"},priceScaleId:""});volSeries.priceScale().applyOptions({scaleMargins:{top:0.85,bottom:0}});
  window.addEventListener("resize",()=>{if(chart)chart.applyOptions({width:el.clientWidth});});
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
  Object.keys(MA_COLORS).forEach(w=>{if(maSeries[w]&&d.ma&&d.ma[w])maSeries[w].setData(d.ma[w]);});
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

// ---------- 一键决策卡 ----------
async function loadDecision(){
  const el=document.getElementById("decisionCard");if(!el)return;
  const tk=curTicker;
  el.innerHTML='<div class="card"><div class="muted">综合决策分析中…</div></div>';
  // 并行拉三块;期权墙需先取到期日
  const wallsP=(async()=>{try{const e=await j("/api/options/expiries?ticker="+tk);if(e.error||!e.expiries||!e.expiries.length)return null;const exp=pickMonthlyExpiry(e.expiries.slice(0,16));return await j(`/api/options/walls?ticker=${tk}&expiry=${exp}`);}catch(_){return null;}})();
  const [sepa,val,walls]=await Promise.all([j("/api/sepa?ticker="+tk).catch(()=>null),j("/api/valuation?ticker="+tk).catch(()=>null),wallsP]);
  if(tk!==curTicker)return; // 期间切换了
  let score=0;const reasons=[];
  // SEPA
  if(sepa&&!sepa.error){let sc=0;if(/Stage 2/.test(sepa.stage)&&sepa.passed>=7)sc=1;else if(/Stage 4/.test(sepa.stage)||sepa.passed<=3)sc=-1;score+=sc;
    reasons.push({k:"SEPA 趋势",v:`${sepa.stage} · 模板 ${sepa.passed}/${sepa.total} · 基本面 ${sepa.fundamentalGrade}`,c:sc>0?"buy":(sc<0?"sell":"hold")});}
  // 估值
  if(val&&!val.error&&val.upside!=null){let vc=0;if(val.upside>=15)vc=1;else if(val.upside<=-15)vc=-1;score+=vc;
    reasons.push({k:"估值",v:`${val.verdict} · 现价${fmtNum(val.price)}→合理${fmtNum(val.blended)} (${fmtPct(val.upside)})`,c:vc>0?"buy":(vc<0?"sell":"hold")});}
  // 期权墙(轻权重:P/C 持仓偏向 + GEX 体制)
  if(walls&&!walls.error){let oc=0;if(walls.pcRatioOI!=null){if(walls.pcRatioOI<0.7)oc=1;else if(walls.pcRatioOI>1.1)oc=-1;}score+=oc;
    const mp=walls.maxPainVsSpot;const gex=walls.netGex>=0?"正GEX(抑波)":"负GEX(放大波动)";
    reasons.push({k:"期权墙",v:`MaxPain ${fmtNum(walls.maxPain)}(${fmtPct(mp)}) · P/C ${walls.pcRatioOI??"—"} · ${gex}`,c:oc>0?"buy":(oc<0?"sell":"hold")});}
  // 建议仓位(用已存设置)
  const acct=parseFloat(getSetting("accountValue","100000"))||0;
  const riskUnit=getSetting("riskUnit","%"),riskVal=parseFloat(getSetting("riskVal","1"))||1;
  const posUnit=getSetting("posUnit","%"),posVal=parseFloat(getSetting("posVal","25"))||25;
  let sizeNote="";
  if(sepa&&sepa.price){const entry=sepa.price,stop=entry*0.92;const rps=entry-stop;
    const riskD=riskUnit==="%"?acct*riskVal/100:riskVal,posD=posUnit==="%"?acct*posVal/100:posVal;
    const shares=Math.max(0,Math.min(Math.floor(riskD/rps),Math.floor(posD/entry)));
    sizeNote=`按你的设置(账户$${fmtBig(acct)}、风险${riskVal}${riskUnit}、仓位${posVal}${posUnit}、止损-8%)建议约 <b>${shares.toLocaleString()}</b> 股(投入$${fmtBig(shares*entry)})`;}
  let verdict,vclass;
  if(score>=2){verdict="倾向买入 · 多个信号共振";vclass="buy";}
  else if(score<=-2){verdict="倾向回避 · 信号偏空";vclass="sell";}
  else{verdict="观察 · 信号不一致或不足";vclass="hold";}
  el.innerHTML=`<div class="card" style="border-left:4px solid ${vclass==='buy'?'var(--green)':vclass==='sell'?'var(--red)':'var(--yellow)'}">
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:8px">
      <span style="font-size:13px;color:var(--muted)">一键决策</span>
      <span class="badge ${vclass}" style="font-size:15px">${verdict}</span>
      <span class="muted" style="font-size:12px">综合分 ${score>0?'+':''}${score}</span></div>
    <div style="display:flex;gap:18px;flex-wrap:wrap">${reasons.map(r=>`<div style="font-size:13px"><span class="badge ${r.c}" style="font-size:11px">${r.k}</span> <span class="muted">${r.v}</span></div>`).join("")}</div>
    ${sizeNote?`<div class="small" style="margin-top:8px">${sizeNote} · <a onclick="switchTab('position')">去仓位计算</a></div>`:""}
    <div class="small">综合 SEPA/估值/期权墙的启发式打分,仅供参考,不构成投资建议。</div></div>`;
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
  renderAlertBanner();renderAlertsPanel();
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
  el.innerHTML=`<div class="cmpbar">
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

// ---------- 仓位计算 ----------
function loadPosition(){
  loadedTabs.position=curTicker;
  const el=document.getElementById("tabc-position");
  const acct=getSetting("accountValue","100000");
  const entry=window._curPrice?window._curPrice.toFixed(2):"";
  const stop=window._curPrice?(window._curPrice*0.92).toFixed(2):"";  // 默认 -8%
  const posUnit=getSetting("posUnit","%");
  const riskUnit=getSetting("riskUnit","%");
  const posVal=getSetting("posVal","25");
  const riskVal=getSetting("riskVal","1");
  const unitSel=(id,u)=>`<select id="${id}" onchange="calcPosition()"><option ${u==="%"?"selected":""}>%</option><option ${u==="$"?"selected":""}>$</option></select>`;
  el.innerHTML=`
   <div class="section-title">仓位计算器 <span class="tag">skill: sepa-strategy/position-sizing</span></div>
   <div class="muted" style="font-size:13px;margin-bottom:14px">输入买入价、止损价、总资产,以及两个上限(总买入仓位 / 总风险),自动算出<b>同时满足全部条件</b>的最大可买股数。已带入 ${curTicker} 现价,可手动改。</div>
   <div class="pform">
     <div><label>总资产 ($)</label><div class="row"><input id="posAccount" type="number" value="${acct}" oninput="calcPosition()"></div><div class="hint">会自动记住</div></div>
     <div><label>买入价 ($)</label><div class="row"><input id="posEntry" type="number" value="${entry}" oninput="calcPosition()"></div><div class="hint" id="hEntry"></div></div>
     <div><label>止损价 ($)</label><div class="row"><input id="posStop" type="number" value="${stop}" oninput="calcPosition()"></div><div class="hint" id="hStop"></div></div>
     <div><label>① 总买入仓位上限</label><div class="row"><input id="posMaxPos" type="number" value="${posVal}" oninput="calcPosition()">${unitSel("posMaxPosUnit",posUnit)}</div><div class="hint" id="hPos"></div></div>
     <div><label>② 总风险上限(最多亏)</label><div class="row"><input id="posMaxRisk" type="number" value="${riskVal}" oninput="calcPosition()">${unitSel("posMaxRiskUnit",riskUnit)}</div><div class="hint" id="hRisk"></div></div>
   </div>
   <div id="posResult"></div>`;
  calcPosition();
}
function calcPosition(){
  const num=id=>parseFloat(document.getElementById(id).value);
  const account=num("posAccount"),entry=num("posEntry"),stop=num("posStop");
  const maxPosIn=num("posMaxPos"),maxRiskIn=num("posMaxRisk");
  const posUnit=document.getElementById("posMaxPosUnit").value,riskUnit=document.getElementById("posMaxRiskUnit").value;
  // 记忆
  if(account>0)setSetting("accountValue",account);
  setSetting("posUnit",posUnit);setSetting("riskUnit",riskUnit);
  if(maxPosIn>=0)setSetting("posVal",maxPosIn);if(maxRiskIn>=0)setSetting("riskVal",maxRiskIn);
  window._lastCalc={entry,stop,shares:0};

  const res=document.getElementById("posResult");
  const setHint=(id,t)=>{const e=document.getElementById(id);if(e)e.textContent=t;};
  // 派生金额
  const maxPosDollar=posUnit==="%"?(account*maxPosIn/100):maxPosIn;
  const maxRiskDollar=riskUnit==="%"?(account*maxRiskIn/100):maxRiskIn;
  setHint("hPos",isFinite(maxPosDollar)?"= $"+fmtBig(maxPosDollar)+(posUnit==="$"&&account>0?` · 占 ${(maxPosDollar/account*100).toFixed(1)}%`:""):"");
  setHint("hRisk",isFinite(maxRiskDollar)?"= $"+fmtBig(maxRiskDollar)+(riskUnit==="$"&&account>0?` · 占 ${(maxRiskDollar/account*100).toFixed(2)}%`:""):"");
  setHint("hEntry","");setHint("hStop", (entry>0&&stop>0)?`止损距离 ${((entry-stop)/entry*100).toFixed(2)}%`:"");

  if(!(account>0&&entry>0&&stop>0&&maxPosIn>=0&&maxRiskIn>=0)){res.innerHTML='<div class="muted">请完整填写各项(均为正数)。</div>';return;}
  if(stop>=entry){res.innerHTML='<div class="error">止损价必须低于买入价。</div>';return;}

  const riskPerShare=entry-stop;
  const sharesByRisk=Math.floor(maxRiskDollar/riskPerShare);
  const sharesByPos=Math.floor(maxPosDollar/entry);
  const shares=Math.max(0,Math.min(sharesByRisk,sharesByPos));
  const binding=sharesByRisk<=sharesByPos?"风险上限":"仓位上限";
  const bindClass=sharesByRisk<=sharesByPos?"sell":"hold";
  window._lastCalc={entry,stop,shares};

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
     <button class="search" style="margin:0" onclick="addPositionFromCalc()">＋ 记入持仓</button>
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
async function addPositionFromCalc(){
  const c=window._lastCalc;
  if(!c||!c.shares){alert("请先得到有效的可买股数");return;}
  await fetch("/api/positions",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({ticker:curTicker,shares:c.shares,entry:c.entry,stop:c.stop,target:(c.entry*1.15).toFixed(2)})});
  alert(`已记入持仓:${curTicker} ${c.shares} 股 @ ${fmtNum(c.entry)}`);
}

// ---------- 持仓页 ----------
async function loadPositions(){
  const el=document.getElementById("page-positions");
  el.innerHTML='<div class="loading">加载持仓…</div>';
  const d=await j("/api/positions");
  const s=d.summary||{};
  const open=d.positions.filter(p=>p.status==="open"),closed=d.positions.filter(p=>p.status==="closed");
  const sumCards=`<div class="grid" style="margin-bottom:18px">
    ${card("持仓数",s.openCount||0)}
    ${card("总市值",s.totalMarketValue!=null?"$"+fmtBig(s.totalMarketValue):"—")}
    ${card("总成本",s.totalCost!=null?"$"+fmtBig(s.totalCost):"—")}
    <div class="card"><div class="k">总浮盈亏</div><div class="v ${(s.totalPL||0)>=0?'green':'red'}">${s.totalPL!=null?(s.totalPL>=0?"+":"")+"$"+fmtBig(Math.abs(s.totalPL)):"—"} ${s.totalPLPct!=null?`(${fmtPct(s.totalPLPct)})`:""}</div></div>
    ${card("仓位占账户",s.investedPct!=null?s.investedPct.toFixed(1)+"%":"—")}
    <div class="card"><div class="k">组合风险敞口</div><div class="v ${(s.riskPct||0)>6?'red':''}">${s.totalRisk!=null?"$"+fmtBig(s.totalRisk):"—"} ${s.riskPct!=null?`(${s.riskPct.toFixed(2)}%)`:""}</div></div>
  </div>`;
  const openRows=open.map(p=>{
    const plc=(p.pl||0)>=0?"green":"red";
    const stopc=p.toStopPct!=null&&p.toStopPct<5?"red":"";
    return `<tr>
      <td><b class="wchip" style="cursor:pointer;padding:2px 6px" onclick="loadTicker('${p.ticker}')">${p.ticker}</b></td>
      <td>${fmtNum(p.shares,0)}</td><td>${fmtNum(p.entry)}</td><td>${fmtNum(p.price)}</td>
      <td class="${plc}">${p.pl!=null?(p.pl>=0?"+":"")+fmtBig(p.pl):"—"} ${p.plPct!=null?`(${fmtPct(p.plPct)})`:""}</td>
      <td>${fmtNum(p.stop)}</td><td class="${stopc}">${p.toStopPct!=null?fmtPct(p.toStopPct):"—"}</td>
      <td>${p.rMultiple!=null?p.rMultiple.toFixed(2)+"R":"—"}</td>
      <td class="muted">${p.note||""}</td>
      <td><a onclick="closePosition(${p.id},${p.price||0})">平仓</a> · <a style="color:var(--red)" onclick="delPosition(${p.id})">删</a></td>
    </tr>`;}).join("");
  const closedRows=closed.map(p=>{
    const pl=(p.exit_price!=null)?(p.exit_price-p.entry)*p.shares:null;
    return `<tr class="muted"><td>${p.ticker}</td><td>${fmtNum(p.shares,0)}</td><td>${fmtNum(p.entry)}</td><td>${fmtNum(p.exit_price)}</td>
      <td class="${(pl||0)>=0?'green':'red'}">${pl!=null?(pl>=0?"+":"")+fmtBig(pl):"—"}</td><td>${p.opened_at||""}→${p.closed_at||""}</td>
      <td><a style="color:var(--red)" onclick="delPosition(${p.id})">删</a></td></tr>`;}).join("");
  el.innerHTML=`
    <div class="section-title" style="margin-top:6px">我的持仓 <span class="tag">SQLite 持久化</span></div>
    ${sumCards}
    <div class="cmpbar"><b>手动添加:</b>
      <input id="npTicker" placeholder="代码" style="width:90px;text-transform:uppercase;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:8px">
      <input id="npShares" placeholder="股数" type="number" style="width:90px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:8px">
      <input id="npEntry" placeholder="买入价" type="number" style="width:90px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:8px">
      <input id="npStop" placeholder="止损价" type="number" style="width:90px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:8px">
      <button class="search" style="margin:0" onclick="addPositionManual()">添加</button></div>
    ${open.length?`<table><thead><tr><th>代码</th><th>股数</th><th>成本</th><th>现价</th><th>浮盈亏</th><th>止损</th><th>距止损</th><th>R</th><th>备注</th><th>操作</th></tr></thead><tbody>${openRows}</tbody></table>`:'<div class="muted">暂无持仓。可在「仓位计算」算好后一键记入,或上方手动添加。</div>'}
    ${closed.length?`<div class="section-title" style="font-size:14px">已平仓 / 交易记录</div><table><thead><tr><th>代码</th><th>股数</th><th>成本</th><th>平仓价</th><th>盈亏</th><th>持有</th><th></th></tr></thead><tbody>${closedRows}</tbody></table>`:""}
    <div class="small">组合风险敞口 = Σ(现价−止损)×股数,占账户比例即「组合热度」,SEPA 建议总热度别过高。现价为 yfinance 延迟数据。</div>`;
}
async function addPositionManual(){
  const tk=document.getElementById("npTicker").value.trim().toUpperCase();
  const shares=parseFloat(document.getElementById("npShares").value),entry=parseFloat(document.getElementById("npEntry").value),stop=parseFloat(document.getElementById("npStop").value);
  if(!tk||!shares||!entry){alert("代码/股数/买入价必填");return;}
  await fetch("/api/positions",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({ticker:tk,shares,entry,stop:stop||null})});
  loadPositions();
}
async function closePosition(id,price){
  const px=prompt("平仓价格",price?price.toFixed(2):"");
  if(px===null)return;
  await fetch("/api/positions/"+id,{method:"PUT",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"close",exit_price:parseFloat(px)})});
  loadPositions();
}
async function delPosition(id){
  if(!confirm("确认删除该记录?"))return;
  await fetch("/api/positions/"+id,{method:"DELETE"});loadPositions();
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
    <div class="two-col" style="margin-top:8px"><div><div class="section-title" style="font-size:14px">未平仓量 OI 分布(墙)</div><div id="oiChart" style="height:420px;border:1px solid var(--border);border-radius:10px"></div></div>
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
  window._walls=d;
  const gexPos=d.netGex>=0;
  const gexLabel=gexPos?"正 GEX · 倾向钉价/抑制波动":"负 GEX · 放大波动/助涨助跌";
  const pcBias=d.pcRatioOI==null?"":(d.pcRatioOI<0.7?"偏看涨":(d.pcRatioOI>1.0?"偏看跌":"中性"));
  document.getElementById("wallSummary").innerHTML=`<div class="grid">
    <div class="card"><div class="k">Max Pain 最大痛点</div><div class="v">$${fmtNum(d.maxPain)} <span class="${d.maxPainVsSpot>=0?'green':'red'}" style="font-size:13px">(${fmtPct(d.maxPainVsSpot)})</span></div></div>
    <div class="card"><div class="k">净 Gamma 敞口</div><div class="v ${gexPos?'green':'red'}" style="font-size:15px">${gexLabel}</div></div>
    ${card("Gamma Flip 翻转点",d.gammaFlip!=null?"$"+fmtNum(d.gammaFlip):"—")}
    <div class="card"><div class="k">Put/Call 比率(OI)</div><div class="v">${d.pcRatioOI??"—"} <span class="muted" style="font-size:12px">${pcBias}</span></div></div>
    ${card("Put/Call(成交量)",d.pcRatioVol??"—")}
    ${card("到期天数",d.daysToExpiry+" 天")}
  </div>`;
  document.getElementById("wallNote").textContent=d.note;
  drawOIChart(d);drawGexChart(d);
}
function drawOIChart(d){
  const el=document.getElementById("oiChart");if(!el)return;echarts.getInstanceByDom(el)?.dispose();
  const ks=d.oiDist.map(x=>x.strike);
  const ch=echarts.init(el,'dark');
  ch.setOption({backgroundColor:'#161b22',grid:{left:55,right:20,top:30,bottom:30},
    legend:{data:['Call OI','Put OI'],textStyle:{color:'#8b949e'},top:4},
    tooltip:{trigger:'axis',axisPointer:{type:'shadow'}},
    xAxis:{type:'value',axisLabel:{color:'#8b949e'},splitLine:{lineStyle:{color:'#21262d'}}},
    yAxis:{type:'category',data:ks,axisLabel:{color:'#8b949e'},inverse:false},
    series:[
      {name:'Call OI',type:'bar',stack:'x',data:d.oiDist.map(x=>x.call),itemStyle:{color:'rgba(239,83,80,.75)'}},
      {name:'Put OI',type:'bar',stack:'y',data:d.oiDist.map(x=>-x.put),itemStyle:{color:'rgba(38,166,154,.75)'},
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
  if(window._heatData){renderTreemap();renderSectors();return;}
  document.getElementById("heat-stocks").innerHTML='<div class="loading">热力图加载中(首次约 10-20 秒,抓取近百只个股)…</div>';
  const d=await j("/api/heatmap");
  if(d.error){document.getElementById("heat-stocks").innerHTML='<div class="error">'+d.error+'</div>';return;}
  window._heatData=d;
  document.getElementById("heat-stocks").innerHTML='<div id="treemap"></div>';
  document.getElementById("heat-note").textContent=`更新于 ${d.asof} · ${d.note}`;
  renderTreemap();renderSectors();
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
loadMarket();loadSettings();loadWatch();loadTicker("AAPL");
setInterval(loadAlerts,60000);
</script>
</body>
</html>"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"个股看板启动中(开发服务器) → http://localhost:{port}")
    app.run(host=os.environ.get("HOST", "0.0.0.0"), port=port, debug=False)
