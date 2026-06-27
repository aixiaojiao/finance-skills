# 个股看板 · Stock Dashboard

一个单文件 Flask + yfinance 的美股看板,双页面 SPA。前端用 [TradingView lightweight-charts](https://github.com/tradingview/lightweight-charts) 画 K 线、[ECharts](https://echarts.apache.org/) 画热力图与期权收益曲线,暗色主题。

## 页面1 · 个股看板

四个子标签 + 顶部自选股条(localStorage 持久化,点 ☆ 加自选):

**概览**
- **K线 + MA5/10/20/50/200** 叠加(可勾选开关)+ 成交量
- **SEPA 趋势模板**评分卡:Minervini 8 条件 + 四阶段判定 + 基本面评级 + 综合结论 — `sepa-strategy`
- **财报日 / 业绩**:下次财报日、预期 EPS、历史 beat/miss — `earnings-preview`
- **重要消息 / 新闻流**(yfinance news)
- **流动性评分**:日均成交量、美元成交额、买卖价差、换手率 — `stock-liquidity`
- 关键财务指标 + 分析师评级 / 目标价

**估值** — `company-valuation` + `estimate-analysis`
- DCF 内在价值(简化 5 年 FCFF + WACC + 永续)+ 分析师目标 + 远期PE,三角定位出合理价与上下空间,判定低估/合理/高估
- 分析师预期表(各周期均值/区间/增速)+ EPS 预期修正方向(当前 vs 90天前)

**期权** — `options-payoff`
- 期权链(按到期日,ATM 上下各 12 档,含 IV / OI)
- 点击 Call/Put 加为「腿」,实时绘制**到期收益曲线**(支持多腿组合、买卖翻转),标注最大盈亏/现价

**多股对比** — `stock-correlation`
- 多股归一化走势叠加(rebase 100)+ 区间涨幅 / 年化波动 / Beta + **相关性矩阵热力**

## 页面2 · 市场热力图(与个股看板平行的独立页签)

- **个股热力图**:近百只标普大盘股按板块分组的树状图(treemap),面积≈市值,颜色=当日涨跌幅(finviz 风格)
- **板块热力图**:11 个 SPDR 板块 ETF(XL*)当日表现 + 板块市值加权聚合

顶部常驻**大盘条**:标普500 / 纳指 / VIX + 市场环境(Bull/Choppy/Bear)+ 情绪指标 — `sepa-strategy/market-environment`

> 说明:SEPA 的 RS 用「12个月相对 S&P500 涨幅」做代理;大盘情绪为 VIX/指数趋势/RSI 合成代理(非 CNN 官方指数);热力图面积优先取市值、缺失时回退成交额;DCF 为简化模型,金融/REIT/亏损公司自动回退到分析师目标+远期PE。

## 来源 / Attribution

本看板是个人 fork [`aixiaojiao/finance-skills`](https://github.com/aixiaojiao/finance-skills) 新增的部分。
源项目 / Upstream:[**himself65/finance-skills**](https://github.com/himself65/finance-skills) —— 个股分析逻辑参考其 `market-analysis` 技能。

## 本地运行

```bash
# 首次:创建虚拟环境并装依赖(lxml 用于解析财报 beat/miss 历史)
python3 -m venv webapp/.venv
webapp/.venv/bin/pip install flask yfinance lxml

# 启动
webapp/.venv/bin/python webapp/app.py
# 打开 http://localhost:8000
```

## 接口

| 路由 | 说明 |
|---|---|
| `/` | 前端页面(双页面 SPA) |
| `/api/quote?ticker=AAPL` | 报价 + 基本面 + 分析师评级 |
| `/api/history?ticker=AAPL&period=6mo` | K线 OHLC + 成交量 + MA5/10/20/50/200 |
| `/api/sepa?ticker=AAPL` | SEPA 趋势模板评分卡 |
| `/api/earnings?ticker=AAPL` | 财报日 + beat/miss 历史 |
| `/api/news?ticker=AAPL` | 新闻流 |
| `/api/liquidity?ticker=AAPL` | 流动性评分 |
| `/api/market` | 大盘情绪 / 技术 / 环境 |
| `/api/quotes?tickers=AAPL,NVDA` | 自选股批量行情 |
| `/api/compare?tickers=AAPL,MSFT&period=1y` | 多股归一化对比 + 相关性矩阵 |
| `/api/valuation?ticker=AAPL` | DCF + 相对估值 + 预期趋势 |
| `/api/options/expiries?ticker=AAPL` | 期权到期日列表 |
| `/api/options/chain?ticker=AAPL&expiry=YYYY-MM-DD` | 期权链 |
| `/api/heatmap` | 全市场个股 + 板块热力图 |

## 部署到个人网站

`app.py` 用 Flask 开发服务器运行(`app.run`)。部署到生产时建议:

```bash
# 用 gunicorn 跑(需先 pip install gunicorn)
gunicorn -w 2 -b 0.0.0.0:8000 'app:app'
```

再用 Nginx/Caddy 反代到你的域名即可。也可容器化部署(Dockerfile 可按需补充)。

> ⚠️ 数据来自 Yahoo Finance(yfinance),非实时、有延迟,仅供研究学习,不构成投资建议。
