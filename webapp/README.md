# 个股看板 · Stock Dashboard

一个单文件 Flask + yfinance 的美股个股看板。前端用 [TradingView lightweight-charts](https://github.com/tradingview/lightweight-charts) 绘制 K 线,暗色主题。

## 功能

- **K线 + MA5/10/20/50/200** 叠加(可勾选开关)+ 成交量
- **SEPA 趋势模板**评分卡:Minervini 8 条件 + 四阶段判定 + 基本面评级 + 综合结论 — 对应 `sepa-strategy` 技能
- **财报日 / 业绩**:下次财报日、预期 EPS、历史 beat/miss 记录 — 对应 `earnings-preview` 技能
- **重要消息 / 新闻流**(yfinance news)
- **大盘条**:标普500 / 纳指 / VIX + 市场环境(Bull/Choppy/Bear)+ 情绪指标(代理) — 对应 `sepa-strategy/market-environment`
- **流动性评分**:日均成交量、美元成交额、买卖价差、换手率 — 对应 `stock-liquidity` 技能
- **关键财务指标 + 分析师评级 / 目标价**

> 说明:SEPA 的 RS 相对强度用「12个月相对 S&P500 涨幅」做代理;大盘情绪为 VIX/指数趋势/RSI 合成代理,非 CNN 官方恐惧贪婪指数。

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
| `/` | 前端页面 |
| `/api/quote?ticker=AAPL` | 报价 + 基本面 + 分析师评级 |
| `/api/history?ticker=AAPL&period=6mo` | K线 OHLC + 成交量 |

## 部署到个人网站

`app.py` 用 Flask 开发服务器运行(`app.run`)。部署到生产时建议:

```bash
# 用 gunicorn 跑(需先 pip install gunicorn)
gunicorn -w 2 -b 0.0.0.0:8000 'app:app'
```

再用 Nginx/Caddy 反代到你的域名即可。也可容器化部署(Dockerfile 可按需补充)。

> ⚠️ 数据来自 Yahoo Finance(yfinance),非实时、有延迟,仅供研究学习,不构成投资建议。
