"""
个股看板 · Stock Dashboard — 单文件 Flask + yfinance Web 应用

本地启动:
    webapp/.venv/bin/python webapp/app.py
浏览器打开 http://localhost:8000

功能:
- 实时报价 + K线(蜡烛+成交量)+ MA5/10/20/50/200 叠加
- SEPA 趋势模板评分卡(Minervini 8 条件 + 四阶段判定 + 结论)   [skill: sepa-strategy]
- 财报日 + 预期EPS + 历史 beat/miss                           [skill: earnings-preview]
- 重要消息 / 新闻流                                           (yfinance news)
- 大盘情绪指标(VIX 恐慌/贪婪代理)+ 大盘技术指标 + 市场环境    [skill: sepa-strategy/market-environment]
- 流动性评分(ADTV / 美元成交额 / 价差)                       [skill: stock-liquidity]
- 关键财务指标 + 分析师评级 / 目标价

数据来自 Yahoo Finance(yfinance),非实时、有延迟,仅供研究/学习,不构成投资建议。
配套 finance-skills 仓库的 market-analysis 个股分析技能。
"""

import math
import time
from flask import Flask, jsonify, request, Response
import yfinance as yf
import pandas as pd

app = Flask(__name__)


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


# 简单的内存 TTL 缓存,避免重复打 Yahoo
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
    rs = gain / loss
    out = 100 - 100 / (1 + rs)
    return out


# ============================ 数据获取(带缓存) ============================

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

    data = {
        "ticker": ticker,
        "name": _safe(info.get("longName") or info.get("shortName")),
        "currency": _safe(info.get("currency")) or "USD",
        "exchange": _safe(info.get("fullExchangeName") or info.get("exchange")),
        "sector": _safe(info.get("sector")),
        "industry": _safe(info.get("industry")),
        "price": price, "prevClose": prev, "change": change, "changePct": change_pct,
        "dayHigh": _num(info.get("dayHigh")), "dayLow": _num(info.get("dayLow")),
        "open": _num(info.get("open") or info.get("regularMarketOpen")),
        "volume": _num(info.get("volume") or info.get("regularMarketVolume")),
        "financials": {
            "marketCap": _num(info.get("marketCap")),
            "trailingPE": _num(info.get("trailingPE")),
            "forwardPE": _num(info.get("forwardPE")),
            "priceToBook": _num(info.get("priceToBook")),
            "eps": _num(info.get("trailingEps")),
            "revenue": _num(info.get("totalRevenue")),
            "profitMargin": _num(info.get("profitMargins")),
            "grossMargin": _num(info.get("grossMargins")),
            "dividendYield": _num(info.get("dividendYield")),
            "beta": _num(info.get("beta")),
            "fiftyTwoWeekHigh": _num(info.get("fiftyTwoWeekHigh")),
            "fiftyTwoWeekLow": _num(info.get("fiftyTwoWeekLow")),
        },
        "analyst": {
            "targetMean": _num(info.get("targetMeanPrice")),
            "targetHigh": _num(info.get("targetHighPrice")),
            "targetLow": _num(info.get("targetLowPrice")),
            "recommendation": _safe(info.get("recommendationKey")),
            "numAnalysts": _num(info.get("numberOfAnalystOpinions")),
        },
    }
    return jsonify(data)


# ============================ API: K线 + 均线 ============================

# 显示窗口 -> 实际抓取区间(留足 200MA 预热)
FETCH_MAP = {"1mo": "1y", "3mo": "2y", "6mo": "2y", "1y": "2y", "2y": "5y", "5y": "max"}
DISPLAY_BARS = {"1mo": 22, "3mo": 66, "6mo": 126, "1y": 252, "2y": 504, "5y": 1300}
MA_WINDOWS = [5, 10, 20, 50, 200]


@app.route("/api/history")
def api_history():
    ticker = (request.args.get("ticker") or "").strip().upper()
    period = request.args.get("period", "6mo")
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    fetch_period = FETCH_MAP.get(period, "2y")
    try:
        df = get_history_df(ticker, fetch_period).copy()
    except Exception as e:
        return jsonify({"error": f"获取历史失败: {e}"}), 502
    if df is None or df.empty:
        return jsonify({"error": "无历史数据"}), 404

    for w in MA_WINDOWS:
        df[f"ma{w}"] = df["Close"].rolling(w).mean()

    n = DISPLAY_BARS.get(period, 126)
    df = df.tail(n)

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


# ============================ API: SEPA 趋势模板 ============================

@cached(300)
def _spy_return_252():
    """S&P500 近 252 交易日收益率,用于 RS 代理。"""
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
    ma200_series = close.rolling(200).mean()
    ma200_1mo = _num(ma200_series.iloc[-22]) if len(df) >= 222 else None

    win = min(len(df), 252)
    hi52 = _num(df["High"].tail(win).max())
    lo52 = _num(df["Low"].tail(win).min())

    # RS 代理:个股 vs S&P500 近 12 个月相对涨幅
    rs_pass = None
    rs_val = None
    if len(df) >= 252:
        stock_ret = price / _num(close.iloc[-252]) - 1
        spy_ret = _spy_return_252()
        if spy_ret is not None:
            rs_val = (stock_ret - spy_ret) * 100  # 相对跑赢百分点
            rs_pass = stock_ret > spy_ret

    def pct(a, b):
        if a is None or b is None or b == 0:
            return None
        return (a / b - 1) * 100

    pct_above_low = pct(price, lo52)
    pct_from_high = pct(price, hi52)  # 负数 = 低于高点

    conds = []
    def cond(no, name, ok, val):
        conds.append({"no": no, "name": name, "pass": bool(ok) if ok is not None else None, "value": val})

    cond(1, "价格 > 150MA 且 > 200MA",
         (ma150 is not None and ma200 is not None and price > ma150 and price > ma200),
         f"价 {price:.2f} / 150MA {ma150:.2f} / 200MA {ma200:.2f}" if ma150 and ma200 else "数据不足")
    cond(2, "150MA > 200MA",
         (ma150 is not None and ma200 is not None and ma150 > ma200),
         f"{ma150:.2f} vs {ma200:.2f}" if ma150 and ma200 else "数据不足")
    cond(3, "200MA 近 1 个月向上",
         (ma200 is not None and ma200_1mo is not None and ma200 > ma200_1mo),
         f"现 {ma200:.2f} vs 1月前 {ma200_1mo:.2f}" if ma200 and ma200_1mo else "数据不足")
    cond(4, "50MA > 150MA 且 > 200MA",
         (ma50 is not None and ma150 is not None and ma200 is not None and ma50 > ma150 and ma50 > ma200),
         f"50MA {ma50:.2f}" if ma50 else "数据不足")
    cond(5, "价格 > 50MA",
         (ma50 is not None and price > ma50),
         f"价 {price:.2f} / 50MA {ma50:.2f}" if ma50 else "数据不足")
    cond(6, "高于 52周低点 ≥ 30%",
         (pct_above_low is not None and pct_above_low >= 30),
         f"+{pct_above_low:.1f}%" if pct_above_low is not None else "—")
    cond(7, "距 52周高点 ≤ 25%",
         (pct_from_high is not None and pct_from_high >= -25),
         f"{pct_from_high:.1f}%" if pct_from_high is not None else "—")
    cond(8, "相对强度 RS 跑赢大盘(代理)",
         rs_pass,
         (f"相对 S&P500 {rs_val:+.1f}pp" if rs_val is not None else "数据不足"))

    passed = sum(1 for c in conds if c["pass"] is True)
    total = len(conds)

    # 四阶段判定(启发式)
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

    # 基本面评级(用 info 季度同比)
    eps_growth = _num(info.get("earningsQuarterlyGrowth"))
    rev_growth = _num(info.get("revenueGrowth"))
    if eps_growth is None:
        fgrade = "?"
    elif eps_growth > 0.30:
        fgrade = "A"
    elif eps_growth >= 0.15:
        fgrade = "B"
    elif eps_growth >= 0:
        fgrade = "C"
    else:
        fgrade = "D"

    # 综合结论
    if "Stage 2" in stage and passed == total and fgrade in ("A", "B"):
        verdict, vclass = "Strong Buy Setup · 强势候选", "buy"
    elif passed >= 6 and "Stage 4" not in stage:
        verdict, vclass = "Watch List · 观察", "hold"
    else:
        verdict, vclass = "Pass · 暂不符合", "sell"

    return jsonify({
        "ticker": ticker, "price": price, "stage": stage,
        "conditions": conds, "passed": passed, "total": total,
        "fundamentalGrade": fgrade,
        "epsGrowth": eps_growth, "revGrowth": rev_growth,
        "verdict": verdict, "verdictClass": vclass,
        "rsNote": "RS 为相对 S&P500 涨幅代理,非全市场百分位排名",
    })


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

    # 兜底:用 calendar 取下次财报日
    if upcoming is None:
        try:
            cal = t.calendar
            if isinstance(cal, dict):
                ds = cal.get("Earnings Date")
                if ds:
                    d0 = ds[0] if isinstance(ds, (list, tuple)) else ds
                    upcoming = {"date": str(d0)[:10], "estimate": _num(cal.get("Earnings Average"))}
        except Exception:
            pass

    info = {}
    try:
        info = get_info(ticker)
    except Exception:
        pass

    return jsonify({
        "ticker": ticker,
        "upcoming": upcoming,
        "history": history,
        "epsForward": _num(info.get("forwardEps")),
        "epsTrailing": _num(info.get("trailingEps")),
        "revenueGrowth": _num(info.get("revenueGrowth")),
        "earningsGrowth": _num(info.get("earningsQuarterlyGrowth")),
    })


# ============================ API: 新闻 ============================

@app.route("/api/news")
def api_news():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400
    items = []
    try:
        raw = yf.Ticker(ticker).news or []
    except Exception:
        raw = []
    for it in raw[:12]:
        # 兼容新旧两种结构
        c = it.get("content") if isinstance(it, dict) and "content" in it else it
        if not isinstance(c, dict):
            continue
        title = c.get("title")
        pub = None
        if isinstance(c.get("provider"), dict):
            pub = c["provider"].get("displayName")
        pub = pub or c.get("publisher")
        link = None
        if isinstance(c.get("canonicalUrl"), dict):
            link = c["canonicalUrl"].get("url")
        link = link or (c.get("clickThroughUrl", {}) or {}).get("url") if isinstance(c.get("clickThroughUrl"), dict) else link
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
    bid = _num(info.get("bid"))
    ask = _num(info.get("ask"))
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

    return jsonify({
        "ticker": ticker, "adtv": adtv, "dollarVol": dollar_vol,
        "bid": bid, "ask": ask, "spreadBps": spread_bps,
        "turnover": turnover, "grade": grade, "gradeDesc": gdesc,
    })


# ============================ API: 大盘(情绪 + 技术 + 环境) ============================

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
    chg_pct = ((price / prev - 1) * 100) if price and prev else None
    ma50 = _num(c.rolling(50).mean().iloc[-1]) if len(c) >= 50 else None
    ma200 = _num(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else None
    r = _num(rsi(c).iloc[-1]) if len(c) >= 15 else None
    return {
        "symbol": symbol, "price": price, "changePct": chg_pct,
        "ma50": ma50, "ma200": ma200,
        "above50": (price > ma50) if (price and ma50) else None,
        "above200": (price > ma200) if (price and ma200) else None,
        "pctVs200": ((price / ma200 - 1) * 100) if (price and ma200) else None,
        "rsi": r,
    }


@app.route("/api/market")
def api_market():
    spx = _index_snapshot("^GSPC")
    ndx = _index_snapshot("^IXIC")
    vix_df = None
    try:
        vix_df = get_history_df("^VIX", "1mo")
    except Exception:
        pass
    vix = None
    if vix_df is not None and not vix_df.empty:
        vix = _num(vix_df["Close"].dropna().iloc[-1])

    # 市场环境(Bull / Choppy / Bear)— 依据 sepa market-environment 规则
    a200 = [x["above200"] for x in (spx, ndx) if x and x["above200"] is not None]
    env, env_class = "Choppy · 震荡", "hold"
    if a200:
        if all(a200):
            env, env_class = "Bull · 多头", "buy"
        elif not any(a200):
            env, env_class = "Bear · 空头", "sell"

    # 情绪指标(0-100,越高越贪婪)— VIX + 指数相对 200MA + RSI 的合成代理
    parts = []
    if vix is not None:
        parts.append(max(0, min(100, (40 - vix) / (40 - 12) * 100)))   # 低VIX=贪婪
    if spx and spx["pctVs200"] is not None:
        parts.append(max(0, min(100, (spx["pctVs200"] + 10) / 20 * 100)))  # 高于200MA=贪婪
    if spx and spx["rsi"] is not None:
        parts.append(max(0, min(100, spx["rsi"])))
    sentiment = round(sum(parts) / len(parts)) if parts else None
    if sentiment is None:
        slabel = "—"
    elif sentiment < 25:
        slabel = "极度恐慌"
    elif sentiment < 45:
        slabel = "恐慌"
    elif sentiment <= 55:
        slabel = "中性"
    elif sentiment <= 75:
        slabel = "贪婪"
    else:
        slabel = "极度贪婪"

    return jsonify({
        "spx": spx, "ndx": ndx, "vix": vix,
        "environment": env, "environmentClass": env_class,
        "sentiment": sentiment, "sentimentLabel": slabel,
        "note": "情绪为 VIX/指数趋势/RSI 合成代理,非 CNN 官方恐惧贪婪指数",
    })


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
<style>
  :root{--bg:#0d1117;--panel:#161b22;--panel2:#1c2230;--border:#21262d;--text:#e6edf3;--muted:#8b949e;--green:#26a69a;--red:#ef5350;--accent:#58a6ff;--yellow:#f6c343}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif}
  a{color:var(--accent);text-decoration:none} a:hover{text-decoration:underline}
  /* 顶部大盘条 */
  .marketbar{display:flex;align-items:center;gap:14px;padding:8px 24px;background:#0a0d12;border-bottom:1px solid var(--border);flex-wrap:wrap;font-size:13px}
  .mb-item{display:flex;gap:6px;align-items:baseline}
  .mb-item .lbl{color:var(--muted)}
  .badge{padding:3px 10px;border-radius:6px;font-weight:600;font-size:12px}
  .buy{background:rgba(38,166,154,.15);color:var(--green)} .sell{background:rgba(239,83,80,.15);color:var(--red)} .hold{background:rgba(246,195,67,.15);color:var(--yellow)}
  .gauge{display:flex;align-items:center;gap:8px}
  .gauge .bar{width:120px;height:8px;border-radius:4px;background:linear-gradient(90deg,#ef5350,#f6c343,#26a69a);position:relative}
  .gauge .dot{position:absolute;top:-3px;width:14px;height:14px;border-radius:50%;background:#fff;border:2px solid #0a0d12;transform:translateX(-50%)}
  header{padding:14px 24px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:16px;flex-wrap:wrap}
  h1{font-size:18px;margin:0;font-weight:600}
  .chips{display:flex;gap:6px;flex-wrap:wrap}
  .chip{background:var(--panel);border:1px solid var(--border);padding:5px 10px;border-radius:16px;cursor:pointer;font-size:12px;color:var(--muted)}
  .chip:hover{color:var(--text);border-color:var(--accent)}
  .search{display:flex;gap:8px;margin-left:auto}
  .search input{background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px 12px;border-radius:8px;font-size:14px;width:150px;text-transform:uppercase}
  .search button{background:var(--accent);border:none;color:#fff;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:14px;font-weight:500}
  main{padding:20px 24px;max-width:1320px;margin:0 auto}
  .quote-head{display:flex;align-items:baseline;gap:16px;flex-wrap:wrap;margin-bottom:4px}
  .quote-head .name{font-size:22px;font-weight:600}
  .quote-head .tk{color:var(--muted);font-size:14px}
  .price-row{display:flex;align-items:baseline;gap:14px;margin-bottom:12px}
  .price{font-size:34px;font-weight:700}
  .chg{font-size:16px;font-weight:600}
  .meta{color:var(--muted);font-size:13px;margin-bottom:14px}
  .controls{display:flex;gap:6px;margin:10px 0;flex-wrap:wrap;align-items:center}
  .controls button{background:var(--panel);border:1px solid var(--border);color:var(--muted);padding:5px 12px;border-radius:6px;cursor:pointer;font-size:12px}
  .controls button.active{background:var(--accent);color:#fff;border-color:var(--accent)}
  .ma-toggles{display:flex;gap:10px;margin-left:8px;flex-wrap:wrap;font-size:12px}
  .ma-toggles label{display:flex;align-items:center;gap:4px;cursor:pointer;color:var(--muted)}
  #chart{width:100%;height:440px;border:1px solid var(--border);border-radius:10px;overflow:hidden}
  .section-title{font-size:15px;font-weight:600;margin:30px 0 10px;display:flex;align-items:center;gap:8px}
  .section-title .tag{font-size:11px;color:var(--muted);font-weight:400;background:var(--panel);padding:2px 8px;border-radius:10px}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:12px}
  .card{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px}
  .card .k{color:var(--muted);font-size:12px;margin-bottom:6px}
  .card .v{font-size:18px;font-weight:600}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{text-align:left;padding:8px 10px;border-bottom:1px solid var(--border)}
  th{color:var(--muted);font-weight:500}
  .green{color:var(--green)} .red{color:var(--red)} .muted{color:var(--muted)}
  .pass{color:var(--green);font-weight:600} .fail{color:var(--red);font-weight:600} .unk{color:var(--muted)}
  .sepa-head{display:flex;gap:14px;flex-wrap:wrap;align-items:center;margin-bottom:12px}
  .scorebig{font-size:26px;font-weight:700}
  .gradechip{font-size:20px;font-weight:700;padding:4px 14px;border-radius:8px}
  .gA{background:rgba(38,166,154,.18);color:var(--green)} .gB{background:rgba(88,166,255,.18);color:var(--accent)}
  .gC{background:rgba(246,195,67,.18);color:var(--yellow)} .gD{background:rgba(239,83,80,.18);color:var(--red)} .gq{background:var(--panel2);color:var(--muted)}
  .news-item{padding:10px 0;border-bottom:1px solid var(--border)}
  .news-item .t{font-size:14px}
  .news-item .m{font-size:12px;color:var(--muted);margin-top:3px}
  .loading{color:var(--muted);padding:30px;text-align:center}
  .error{color:var(--red);padding:20px;background:rgba(239,83,80,.1);border-radius:8px}
  .small{font-size:11px;color:var(--muted);margin-top:6px}
  .disclaimer{color:var(--muted);font-size:11px;margin-top:36px;text-align:center}
  .two-col{display:grid;grid-template-columns:1fr 1fr;gap:24px}
  @media(max-width:880px){.two-col{grid-template-columns:1fr}}
</style>
</head>
<body>

<div class="marketbar" id="marketbar"><span class="muted">大盘加载中…</span></div>

<header>
  <h1>📈 个股看板</h1>
  <div class="chips" id="chips"></div>
  <div class="search">
    <input id="tickerInput" placeholder="代码 如 AAPL" value="AAPL" />
    <button onclick="loadTicker()">查询</button>
  </div>
</header>

<main>
  <div id="content"><div class="loading">加载中…</div></div>
  <div class="disclaimer">
    数据来源 Yahoo Finance(yfinance),非实时报价、有延迟,仅供研究与学习,不构成投资建议。<br>
    SEPA 分析基于 Mark Minervini 趋势模板;RS 与情绪指标为代理算法。
  </div>
</main>

<script>
const POPULAR = ["AAPL","TSLA","NVDA","MSFT","GOOGL","AMZN","META","AMD","PLTR","COIN"];
const MA_COLORS = {"5":"#f6c343","10":"#ff9f40","20":"#58a6ff","50":"#a78bfa","200":"#e6edf3"};
const MA_DEFAULT_ON = {"5":false,"10":false,"20":true,"50":true,"200":true};
let chart, candleSeries, volSeries, maSeries = {}, curTicker = "AAPL", curPeriod = "6mo";

const fmtNum=(n,d=2)=>n==null?"—":Number(n).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const fmtBig=n=>{if(n==null)return"—";const a=Math.abs(n);if(a>=1e12)return(n/1e12).toFixed(2)+"T";if(a>=1e9)return(n/1e9).toFixed(2)+"B";if(a>=1e6)return(n/1e6).toFixed(2)+"M";if(a>=1e3)return(n/1e3).toFixed(2)+"K";return fmtNum(n)};
const fmtPct=n=>n==null?"—":(n>=0?"+":"")+n.toFixed(2)+"%";
const j=async u=>{const r=await fetch(u);return r.json();};

// ---------- 大盘条 ----------
async function loadMarket(){
  try{
    const m=await j("/api/market");
    const idx=(name,o)=>!o?"":`<div class="mb-item"><span class="lbl">${name}</span><b>${fmtNum(o.price)}</b><span class="${(o.changePct||0)>=0?'green':'red'}">${fmtPct(o.changePct)}</span></div>`;
    const vixCls=m.vix==null?"":(m.vix>=25?"red":(m.vix<=16?"green":"hold"));
    const dot=m.sentiment==null?50:m.sentiment;
    document.getElementById("marketbar").innerHTML=
      idx("标普500",m.spx)+idx("纳指",m.ndx)+
      `<div class="mb-item"><span class="lbl">VIX</span><b class="${vixCls}">${fmtNum(m.vix)}</b></div>`+
      `<div class="mb-item"><span class="lbl">环境</span><span class="badge ${m.environmentClass}">${m.environment}</span></div>`+
      `<div class="gauge"><span class="lbl">情绪 ${m.sentiment??"—"} · ${m.sentimentLabel}</span><div class="bar"><div class="dot" style="left:${dot}%"></div></div></div>`;
  }catch(e){ document.getElementById("marketbar").innerHTML='<span class="muted">大盘数据获取失败</span>'; }
}

// ---------- 主入口 ----------
async function loadTicker(t){
  curTicker = t ? t : document.getElementById("tickerInput").value.trim().toUpperCase();
  if(!curTicker) return;
  if(t) document.getElementById("tickerInput").value=t;
  document.getElementById("content").innerHTML='<div class="loading">加载 '+curTicker+' …</div>';
  const q=await j("/api/quote?ticker="+encodeURIComponent(curTicker));
  if(q.error){document.getElementById("content").innerHTML='<div class="error">'+q.error+'</div>';return;}
  renderShell(q);
  loadChart(curPeriod);
  // 并行加载各深度板块
  loadSepa(); loadEarnings(); loadLiquidity(); loadNews();
}

function card(k,v){return `<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`;}

function renderShell(q){
  const up=(q.change||0)>=0, cls=up?"green":"red", f=q.financials, a=q.analyst;
  const recClass=!a.recommendation?"hold":(/buy/.test(a.recommendation)?"buy":(/sell|underperform/.test(a.recommendation)?"sell":"hold"));
  const maToggles=Object.keys(MA_COLORS).map(w=>`<label><input type="checkbox" ${MA_DEFAULT_ON[w]?"checked":""} onchange="toggleMA('${w}',this.checked)"><span style="color:${MA_COLORS[w]}">MA${w}</span></label>`).join("");
  document.getElementById("content").innerHTML=`
   <div class="quote-head"><span class="name">${q.name||q.ticker}</span><span class="tk">${q.ticker} · ${q.exchange||""} · ${q.currency}</span></div>
   <div class="price-row"><span class="price">${fmtNum(q.price)}</span><span class="chg ${cls}">${up?"▲":"▼"} ${fmtNum(q.change)} (${fmtPct(q.changePct)})</span></div>
   <div class="meta">${q.sector||""}${q.industry?" · "+q.industry:""} &nbsp;|&nbsp; 开 ${fmtNum(q.open)} · 高 ${fmtNum(q.dayHigh)} · 低 ${fmtNum(q.dayLow)} · 量 ${fmtBig(q.volume)}</div>

   <div class="controls">
     ${["1mo","3mo","6mo","1y","2y","5y"].map(p=>`<button class="${p===curPeriod?'active':''}" onclick="loadChart('${p}')">${p}</button>`).join("")}
     <span class="ma-toggles">${maToggles}</span>
   </div>
   <div id="chart"></div>

   <div class="section-title">SEPA 趋势模板分析 <span class="tag">skill: sepa-strategy · Minervini</span></div>
   <div id="sepa"><div class="loading">分析中…</div></div>

   <div class="two-col">
     <div>
       <div class="section-title">关键财务指标</div>
       <div class="grid">
         ${card("市值",fmtBig(f.marketCap))}${card("市盈率 TTM",fmtNum(f.trailingPE))}${card("预期PE",fmtNum(f.forwardPE))}
         ${card("市净率",fmtNum(f.priceToBook))}${card("EPS",fmtNum(f.eps))}${card("营收TTM",fmtBig(f.revenue))}
         ${card("净利率",f.profitMargin!=null?fmtNum(f.profitMargin*100)+"%":"—")}${card("毛利率",f.grossMargin!=null?fmtNum(f.grossMargin*100)+"%":"—")}
         ${card("Beta",fmtNum(f.beta))}${card("股息率",f.dividendYield!=null?fmtNum(f.dividendYield)+"%":"—")}
         ${card("52周高",fmtNum(f.fiftyTwoWeekHigh))}${card("52周低",fmtNum(f.fiftyTwoWeekLow))}
       </div>
     </div>
     <div>
       <div class="section-title">分析师评级</div>
       <div class="grid">
         <div class="card"><div class="k">综合评级</div><div class="v"><span class="badge ${recClass}">${a.recommendation||"无"}</span></div></div>
         ${card("平均目标价",fmtNum(a.targetMean))}${card("最高/最低",fmtNum(a.targetHigh)+" / "+fmtNum(a.targetLow))}
         ${card("分析师数",a.numAnalysts!=null?a.numAnalysts:"—")}
         ${card("目标空间",(a.targetMean&&q.price)?fmtPct((a.targetMean/q.price-1)*100):"—")}
       </div>
       <div class="section-title" style="margin-top:24px">流动性 <span class="tag">skill: stock-liquidity</span></div>
       <div id="liquidity"><div class="loading">分析中…</div></div>
     </div>
   </div>

   <div class="section-title">财报日 / 业绩 <span class="tag">skill: earnings-preview</span></div>
   <div id="earnings"><div class="loading">加载中…</div></div>

   <div class="section-title">重要消息 / 新闻</div>
   <div id="news"><div class="loading">加载中…</div></div>
  `;
  initChart();
}

// ---------- 图表 ----------
function initChart(){
  const el=document.getElementById("chart");
  chart=LightweightCharts.createChart(el,{
    layout:{background:{color:"#161b22"},textColor:"#8b949e"},
    grid:{vertLines:{color:"#21262d"},horzLines:{color:"#21262d"}},
    rightPriceScale:{borderColor:"#21262d"},timeScale:{borderColor:"#21262d"},
    crosshair:{mode:0},width:el.clientWidth,height:440,
  });
  candleSeries=chart.addCandlestickSeries({upColor:"#26a69a",downColor:"#ef5350",borderVisible:false,wickUpColor:"#26a69a",wickDownColor:"#ef5350"});
  maSeries={};
  Object.keys(MA_COLORS).forEach(w=>{
    maSeries[w]=chart.addLineSeries({color:MA_COLORS[w],lineWidth:w==="200"?2:1,priceLineVisible:false,lastValueVisible:false,visible:MA_DEFAULT_ON[w]});
  });
  volSeries=chart.addHistogramSeries({priceFormat:{type:"volume"},priceScaleId:""});
  volSeries.priceScale().applyOptions({scaleMargins:{top:0.85,bottom:0}});
  window.addEventListener("resize",()=>{if(chart)chart.applyOptions({width:el.clientWidth});});
}
function toggleMA(w,on){ if(maSeries[w]) maSeries[w].applyOptions({visible:on}); }
async function loadChart(period){
  curPeriod=period;
  document.querySelectorAll(".controls > button").forEach(b=>b.classList.toggle("active",b.textContent===period));
  if(!chart) initChart();
  const d=await j(`/api/history?ticker=${encodeURIComponent(curTicker)}&period=${period}`);
  if(d.error||!d.candles)return;
  candleSeries.setData(d.candles);
  volSeries.setData(d.volumes);
  Object.keys(MA_COLORS).forEach(w=>{ if(maSeries[w]&&d.ma&&d.ma[w]) maSeries[w].setData(d.ma[w]); });
  chart.timeScale().fitContent();
}

// ---------- SEPA ----------
async function loadSepa(){
  const el=document.getElementById("sepa"); if(!el)return;
  const s=await j("/api/sepa?ticker="+encodeURIComponent(curTicker));
  if(s.error){el.innerHTML='<div class="muted">'+s.error+'</div>';return;}
  const stageCls=/Stage 2/.test(s.stage)?"buy":(/Stage 4/.test(s.stage)?"sell":"hold");
  const rows=s.conditions.map(c=>{
    const st=c.pass===true?'<span class="pass">✓ 通过</span>':(c.pass===false?'<span class="fail">✗ 不满足</span>':'<span class="unk">? 未知</span>');
    return `<tr><td class="muted">${c.no}</td><td>${c.name}</td><td>${st}</td><td class="muted">${c.value}</td></tr>`;
  }).join("");
  const g=s.fundamentalGrade, gcls={"A":"gA","B":"gB","C":"gC","D":"gD"}[g]||"gq";
  el.innerHTML=`
    <div class="sepa-head">
      <span class="badge ${s.verdictClass}" style="font-size:14px">${s.verdict}</span>
      <span class="badge ${stageCls}">${s.stage}</span>
      <span class="muted">趋势模板 <span class="scorebig ${s.passed===s.total?'green':(s.passed>=6?'':'red')}">${s.passed}/${s.total}</span></span>
      <span class="muted">基本面 <span class="gradechip ${gcls}">${g}</span></span>
      ${s.epsGrowth!=null?`<span class="muted">季度EPS同比 ${fmtPct(s.epsGrowth*100)}</span>`:""}
    </div>
    <table><thead><tr><th>#</th><th>条件</th><th>结果</th><th>实际值</th></tr></thead><tbody>${rows}</tbody></table>
    <div class="small">${s.rsNote}</div>`;
}

// ---------- 财报 ----------
async function loadEarnings(){
  const el=document.getElementById("earnings"); if(!el)return;
  const e=await j("/api/earnings?ticker="+encodeURIComponent(curTicker));
  if(e.error){el.innerHTML='<div class="muted">'+e.error+'</div>';return;}
  let head="";
  if(e.upcoming){
    const days=Math.round((new Date(e.upcoming.date)-new Date())/864e5);
    head=`<div class="grid" style="margin-bottom:14px">
      ${card("下次财报日",e.upcoming.date+(isFinite(days)?` <span class="muted" style="font-size:12px">(${days>=0?days+"天后":"约"})</span>`:""))}
      ${card("预期EPS",fmtNum(e.upcoming.estimate))}
      ${card("预期EPS(forward)",fmtNum(e.epsForward))}
      ${card("营收同比",e.revenueGrowth!=null?fmtPct(e.revenueGrowth*100):"—")}
    </div>`;
  } else head='<div class="muted" style="margin-bottom:10px">暂无下次财报日数据</div>';
  let hist="";
  if(e.history&&e.history.length){
    hist=`<table><thead><tr><th>财报日</th><th>预期EPS</th><th>实际EPS</th><th>意外%</th><th>结果</th></tr></thead><tbody>`+
      e.history.map(h=>`<tr><td>${h.date}</td><td>${fmtNum(h.estimate)}</td><td>${fmtNum(h.reported)}</td>
        <td class="${(h.surprisePct||0)>=0?'green':'red'}">${h.surprisePct!=null?fmtPct(h.surprisePct):"—"}</td>
        <td>${h.beat?'<span class="pass">Beat</span>':'<span class="fail">Miss</span>'}</td></tr>`).join("")+
      `</tbody></table>`;
  }
  el.innerHTML=head+hist;
}

// ---------- 流动性 ----------
async function loadLiquidity(){
  const el=document.getElementById("liquidity"); if(!el)return;
  const l=await j("/api/liquidity?ticker="+encodeURIComponent(curTicker));
  if(l.error){el.innerHTML='<div class="muted">'+l.error+'</div>';return;}
  const gcls={"A":"gA","B":"gB","C":"gC","D":"gD"}[l.grade]||"gq";
  el.innerHTML=`<div class="grid">
    <div class="card"><div class="k">流动性评级</div><div class="v"><span class="gradechip ${gcls}">${l.grade}</span> <span class="muted" style="font-size:12px">${l.gradeDesc}</span></div></div>
    ${card("日均成交量",fmtBig(l.adtv))}${card("美元成交额",l.dollarVol!=null?"$"+fmtBig(l.dollarVol):"—")}
    ${card("买卖价差",l.spreadBps!=null?fmtNum(l.spreadBps,1)+" bps":"—")}${card("换手率",l.turnover!=null?fmtNum(l.turnover)+"%":"—")}
  </div>`;
}

// ---------- 新闻 ----------
async function loadNews(){
  const el=document.getElementById("news"); if(!el)return;
  const n=await j("/api/news?ticker="+encodeURIComponent(curTicker));
  if(n.error||!n.news||!n.news.length){el.innerHTML='<div class="muted">暂无新闻</div>';return;}
  el.innerHTML=n.news.map(it=>`<div class="news-item">
    <div class="t">${it.link?`<a href="${it.link}" target="_blank" rel="noopener">${it.title}</a>`:it.title}</div>
    <div class="m">${it.publisher||""}${it.time?" · "+it.time:""}</div></div>`).join("");
}

// ---------- 启动 ----------
document.getElementById("chips").innerHTML=POPULAR.map(t=>`<span class="chip" onclick="loadTicker('${t}')">${t}</span>`).join("");
document.getElementById("tickerInput").addEventListener("keydown",e=>{if(e.key==="Enter")loadTicker();});
loadMarket();
loadTicker("AAPL");
</script>
</body>
</html>"""


if __name__ == "__main__":
    print("个股看板启动中 → http://localhost:8000")
    app.run(host="0.0.0.0", port=8000, debug=False)
