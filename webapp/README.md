# 个股看板 · Stock Dashboard

一个单文件 Flask + yfinance 的美股个股看板:实时报价、K线图(蜡烛+成交量)、关键财务指标、分析师评级与目标价,支持搜索任意股票代码。

前端用 [TradingView lightweight-charts](https://github.com/tradingview/lightweight-charts) 绘制 K 线,暗色主题。

## 来源 / Attribution

本看板是个人 fork [`aixiaojiao/finance-skills`](https://github.com/aixiaojiao/finance-skills) 新增的部分。
源项目 / Upstream:[**himself65/finance-skills**](https://github.com/himself65/finance-skills) —— 个股分析逻辑参考其 `market-analysis` 技能。

## 本地运行

```bash
# 首次:创建虚拟环境并装依赖
python3 -m venv webapp/.venv
webapp/.venv/bin/pip install flask yfinance

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
