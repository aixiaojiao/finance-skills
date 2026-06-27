"""
个股看板 (Stock Dashboard) — 单文件 Flask + yfinance Web 应用

本地启动:
    webapp/.venv/bin/python webapp/app.py
然后浏览器打开 http://localhost:8000

数据来自 Yahoo Finance (通过 yfinance),仅供研究/学习用途,非实时报价,可能有延迟。
配套 finance-skills 仓库的 market-analysis 个股分析能力。
"""

import math
from flask import Flask, jsonify, request, Response
import yfinance as yf

app = Flask(__name__)


# ----------------------------- 工具函数 -----------------------------

def _safe(v):
    """把 NaN / numpy 类型转成可 JSON 序列化的值。"""
    try:
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        # numpy 标量
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
        return float(v)
    except (TypeError, ValueError):
        return None


# ----------------------------- API: 报价 + 基本面 -----------------------------

@app.route("/api/quote")
def api_quote():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400

    try:
        t = yf.Ticker(ticker)
        info = t.info or {}
    except Exception as e:
        return jsonify({"error": f"获取 {ticker} 失败: {e}"}), 502

    # 没有名字基本说明 ticker 无效
    if not info or not (info.get("shortName") or info.get("longName")):
        return jsonify({"error": f"未找到股票 {ticker},请检查代码"}), 404

    price = _num(info.get("currentPrice") or info.get("regularMarketPrice"))
    prev = _num(info.get("regularMarketPreviousClose") or info.get("previousClose"))
    change = change_pct = None
    if price is not None and prev:
        change = price - prev
        change_pct = change / prev * 100

    # 分析师评级 / 目标价
    analyst = {
        "targetMean": _num(info.get("targetMeanPrice")),
        "targetHigh": _num(info.get("targetHighPrice")),
        "targetLow": _num(info.get("targetLowPrice")),
        "recommendation": _safe(info.get("recommendationKey")),
        "numAnalysts": _num(info.get("numberOfAnalystOpinions")),
    }

    data = {
        "ticker": ticker,
        "name": _safe(info.get("longName") or info.get("shortName")),
        "currency": _safe(info.get("currency")) or "USD",
        "exchange": _safe(info.get("fullExchangeName") or info.get("exchange")),
        "sector": _safe(info.get("sector")),
        "industry": _safe(info.get("industry")),
        "price": price,
        "prevClose": prev,
        "change": change,
        "changePct": change_pct,
        "dayHigh": _num(info.get("dayHigh")),
        "dayLow": _num(info.get("dayLow")),
        "open": _num(info.get("open") or info.get("regularMarketOpen")),
        "volume": _num(info.get("volume") or info.get("regularMarketVolume")),
        # 关键财务指标
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
        "analyst": analyst,
    }
    return jsonify(data)


# ----------------------------- API: K线历史 -----------------------------

@app.route("/api/history")
def api_history():
    ticker = (request.args.get("ticker") or "").strip().upper()
    period = request.args.get("period", "6mo")
    interval = request.args.get("interval", "1d")
    if not ticker:
        return jsonify({"error": "缺少 ticker 参数"}), 400

    try:
        t = yf.Ticker(ticker)
        hist = t.history(period=period, interval=interval, auto_adjust=False)
    except Exception as e:
        return jsonify({"error": f"获取历史失败: {e}"}), 502

    if hist is None or hist.empty:
        return jsonify({"error": "无历史数据"}), 404

    candles = []
    volumes = []
    for idx, row in hist.iterrows():
        # lightweight-charts 需要 time 为 yyyy-mm-dd 或 unix 秒
        ts = idx.strftime("%Y-%m-%d") if interval.endswith("d") or interval.endswith("wk") or interval.endswith("mo") else int(idx.timestamp())
        o, h, l, c = _num(row.get("Open")), _num(row.get("High")), _num(row.get("Low")), _num(row.get("Close"))
        if None in (o, h, l, c):
            continue
        candles.append({"time": ts, "open": o, "high": h, "low": l, "close": c})
        vol = _num(row.get("Volume")) or 0
        volumes.append({"time": ts, "value": vol, "color": "rgba(38,166,154,0.5)" if c >= o else "rgba(239,83,80,0.5)"})

    return jsonify({"ticker": ticker, "candles": candles, "volumes": volumes})


# ----------------------------- 前端页面 -----------------------------

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
  :root{--bg:#0d1117;--panel:#161b22;--border:#21262d;--text:#e6edf3;--muted:#8b949e;--green:#26a69a;--red:#ef5350;--accent:#58a6ff}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif}
  header{padding:16px 24px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:16px;flex-wrap:wrap}
  h1{font-size:18px;margin:0;font-weight:600}
  .search{display:flex;gap:8px;margin-left:auto}
  .search input{background:var(--panel);border:1px solid var(--border);color:var(--text);padding:8px 12px;border-radius:8px;font-size:14px;width:160px;text-transform:uppercase}
  .search button{background:var(--accent);border:none;color:#fff;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:14px;font-weight:500}
  .search button:hover{opacity:.9}
  .chips{display:flex;gap:6px;flex-wrap:wrap}
  .chip{background:var(--panel);border:1px solid var(--border);padding:5px 10px;border-radius:16px;cursor:pointer;font-size:12px;color:var(--muted)}
  .chip:hover{color:var(--text);border-color:var(--accent)}
  main{padding:20px 24px;max-width:1280px;margin:0 auto}
  .quote-head{display:flex;align-items:baseline;gap:16px;flex-wrap:wrap;margin-bottom:4px}
  .quote-head .name{font-size:22px;font-weight:600}
  .quote-head .tk{color:var(--muted);font-size:14px}
  .price-row{display:flex;align-items:baseline;gap:14px;margin-bottom:16px}
  .price{font-size:34px;font-weight:700}
  .chg{font-size:16px;font-weight:600}
  .meta{color:var(--muted);font-size:13px;margin-bottom:16px}
  .controls{display:flex;gap:6px;margin:12px 0}
  .controls button{background:var(--panel);border:1px solid var(--border);color:var(--muted);padding:5px 12px;border-radius:6px;cursor:pointer;font-size:12px}
  .controls button.active{background:var(--accent);color:#fff;border-color:var(--accent)}
  #chart{width:100%;height:420px;border:1px solid var(--border);border-radius:10px;overflow:hidden}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px;margin-top:20px}
  .card{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px}
  .card .k{color:var(--muted);font-size:12px;margin-bottom:6px}
  .card .v{font-size:18px;font-weight:600}
  .section-title{font-size:15px;font-weight:600;margin:28px 0 4px}
  .analyst{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px;margin-top:12px}
  .rec{display:inline-block;padding:4px 12px;border-radius:6px;font-weight:600;text-transform:uppercase;font-size:13px}
  .green{color:var(--green)} .red{color:var(--red)}
  .recbuy{background:rgba(38,166,154,.15);color:var(--green)}
  .recsell{background:rgba(239,83,80,.15);color:var(--red)}
  .rechold{background:rgba(139,148,158,.15);color:var(--muted)}
  .loading{color:var(--muted);padding:40px;text-align:center}
  .error{color:var(--red);padding:20px;background:rgba(239,83,80,.1);border-radius:8px}
  .disclaimer{color:var(--muted);font-size:11px;margin-top:30px;text-align:center}
</style>
</head>
<body>
<header>
  <h1>📈 个股看板</h1>
  <div class="chips" id="chips"></div>
  <div class="search">
    <input id="tickerInput" placeholder="输入代码 如 AAPL" value="AAPL" />
    <button onclick="loadTicker()">查询</button>
  </div>
</header>
<main>
  <div id="content"><div class="loading">加载中…</div></div>
  <div class="disclaimer">数据来源 Yahoo Finance(yfinance),非实时报价,可能延迟,仅供研究与学习用途。</div>
</main>

<script>
const POPULAR = ["AAPL","TSLA","NVDA","MSFT","GOOGL","AMZN","META","AMD"];
let chart, candleSeries, volSeries, curTicker = "AAPL", curPeriod = "6mo";

function fmtNum(n, d=2){ if(n==null) return "—"; return Number(n).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d}); }
function fmtBig(n){ if(n==null) return "—"; const a=Math.abs(n);
  if(a>=1e12) return (n/1e12).toFixed(2)+"T"; if(a>=1e9) return (n/1e9).toFixed(2)+"B";
  if(a>=1e6) return (n/1e6).toFixed(2)+"M"; if(a>=1e3) return (n/1e3).toFixed(2)+"K"; return fmtNum(n); }
function fmtPct(n){ if(n==null) return "—"; return (n>=0?"+":"")+n.toFixed(2)+"%"; }

function renderChips(){
  document.getElementById("chips").innerHTML = POPULAR.map(t=>`<span class="chip" onclick="loadTicker('${t}')">${t}</span>`).join("");
}

async function loadTicker(t){
  if(t){ curTicker = t; document.getElementById("tickerInput").value = t; }
  else { curTicker = document.getElementById("tickerInput").value.trim().toUpperCase(); }
  if(!curTicker) return;
  document.getElementById("content").innerHTML = '<div class="loading">加载 '+curTicker+' …</div>';
  try{
    const q = await fetch("/api/quote?ticker="+encodeURIComponent(curTicker)).then(r=>r.json());
    if(q.error){ document.getElementById("content").innerHTML = '<div class="error">'+q.error+'</div>'; return; }
    renderQuote(q);
    await loadChart(curPeriod);
  }catch(e){
    document.getElementById("content").innerHTML = '<div class="error">请求失败: '+e+'</div>';
  }
}

function renderQuote(q){
  const up = (q.change||0) >= 0;
  const cls = up ? "green" : "red";
  const f = q.financials, a = q.analyst;
  const recClass = !a.recommendation ? "rechold" : (/buy/.test(a.recommendation)?"recbuy":(/sell|underperform/.test(a.recommendation)?"recsell":"rechold"));

  let html = `
   <div class="quote-head">
     <span class="name">${q.name||q.ticker}</span>
     <span class="tk">${q.ticker} · ${q.exchange||""} · ${q.currency}</span>
   </div>
   <div class="price-row">
     <span class="price">${fmtNum(q.price)}</span>
     <span class="chg ${cls}">${up?"▲":"▼"} ${fmtNum(q.change)} (${fmtPct(q.changePct)})</span>
   </div>
   <div class="meta">${q.sector||""}${q.industry?" · "+q.industry:""} &nbsp;|&nbsp; 开 ${fmtNum(q.open)} · 高 ${fmtNum(q.dayHigh)} · 低 ${fmtNum(q.dayLow)} · 量 ${fmtBig(q.volume)}</div>

   <div class="controls">
     ${["1mo","3mo","6mo","1y","2y","5y"].map(p=>`<button class="${p===curPeriod?'active':''}" onclick="loadChart('${p}')">${p}</button>`).join("")}
   </div>
   <div id="chart"></div>

   <div class="section-title">关键财务指标</div>
   <div class="grid">
     ${card("市值", fmtBig(f.marketCap))}
     ${card("市盈率 (TTM)", fmtNum(f.trailingPE))}
     ${card("预期市盈率", fmtNum(f.forwardPE))}
     ${card("市净率", fmtNum(f.priceToBook))}
     ${card("每股收益 EPS", fmtNum(f.eps))}
     ${card("营收 (TTM)", fmtBig(f.revenue))}
     ${card("净利率", f.profitMargin!=null?fmtNum(f.profitMargin*100)+"%":"—")}
     ${card("毛利率", f.grossMargin!=null?fmtNum(f.grossMargin*100)+"%":"—")}
     ${card("股息率", f.dividendYield!=null?fmtNum(f.dividendYield)+"%":"—")}
     ${card("Beta", fmtNum(f.beta))}
     ${card("52周最高", fmtNum(f.fiftyTwoWeekHigh))}
     ${card("52周最低", fmtNum(f.fiftyTwoWeekLow))}
   </div>

   <div class="section-title">分析师评级</div>
   <div class="analyst">
     <div class="card"><div class="k">综合评级</div><div class="v"><span class="rec ${recClass}">${a.recommendation||"无数据"}</span></div></div>
     ${card("平均目标价", fmtNum(a.targetMean))}
     ${card("最高目标价", fmtNum(a.targetHigh))}
     ${card("最低目标价", fmtNum(a.targetLow))}
     ${card("分析师人数", a.numAnalysts!=null?a.numAnalysts:"—")}
     ${card("目标价空间", (a.targetMean&&q.price)?fmtPct((a.targetMean/q.price-1)*100):"—")}
   </div>`;
  document.getElementById("content").innerHTML = html;
  initChart();
}

function card(k,v){ return `<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`; }

function initChart(){
  const el = document.getElementById("chart");
  chart = LightweightCharts.createChart(el, {
    layout:{background:{color:"#161b22"},textColor:"#8b949e"},
    grid:{vertLines:{color:"#21262d"},horzLines:{color:"#21262d"}},
    rightPriceScale:{borderColor:"#21262d"},
    timeScale:{borderColor:"#21262d"},
    crosshair:{mode:0},
    width: el.clientWidth, height: 420,
  });
  candleSeries = chart.addCandlestickSeries({upColor:"#26a69a",downColor:"#ef5350",borderVisible:false,wickUpColor:"#26a69a",wickDownColor:"#ef5350"});
  volSeries = chart.addHistogramSeries({priceFormat:{type:"volume"},priceScaleId:""});
  volSeries.priceScale().applyOptions({scaleMargins:{top:0.82,bottom:0}});
  window.addEventListener("resize",()=>{ if(chart) chart.applyOptions({width:el.clientWidth}); });
}

async function loadChart(period){
  curPeriod = period;
  document.querySelectorAll(".controls button").forEach(b=>b.classList.toggle("active", b.textContent===period));
  if(!chart) initChart();
  const d = await fetch(`/api/history?ticker=${encodeURIComponent(curTicker)}&period=${period}`).then(r=>r.json());
  if(d.error || !d.candles){ return; }
  candleSeries.setData(d.candles);
  volSeries.setData(d.volumes);
  chart.timeScale().fitContent();
}

renderChips();
loadTicker("AAPL");
document.getElementById("tickerInput").addEventListener("keydown",e=>{ if(e.key==="Enter") loadTicker(); });
</script>
</body>
</html>"""


if __name__ == "__main__":
    print("个股看板启动中 → http://localhost:8000")
    app.run(host="0.0.0.0", port=8000, debug=False)
