# 个股看板 · Stock Dashboard

一个 Flask + yfinance 的个人炒股决策仪表盘,三页面 SPA。前端用 [TradingView lightweight-charts](https://github.com/tradingview/lightweight-charts) 画 K 线、[ECharts](https://echarts.apache.org/) 画热力图/期权墙/GEX,暗色主题。持仓/自选/预警/设置存在服务端 **SQLite**。

## 页面1 · 个股看板

五个子标签 + 顶部自选股条(后端持久化,点 ☆ 加自选)+ 触发预警 banner:

**概览**
- 顶部 **一键决策卡**:聚合 SEPA + 估值 + 期权墙 → 买入/观察/回避 + 建议仓位
- **K线 + MA5/10/20/50/200** 叠加 + 成交量 +(可选)**期权墙位叠加**(Max Pain / 压力墙 / 支撑墙 / Gamma Flip)
- **SEPA 趋势模板**评分卡:8 条件 + 四阶段 + 基本面评级 + 结论 — `sepa-strategy`
- **财报日 / 业绩**:下次财报日、预期 EPS、历史 beat/miss — `earnings-preview`
- **流动性评分** — `stock-liquidity`、关键财务、分析师评级、新闻流
- **价格 / 止损预警**:到价、跌破 20MA、触及止损(后端实时评估,触发上 banner)

**仓位计算** — `sepa-strategy/position-sizing`
- 输入买入价/止损价/总资产 + 总仓位上限 + 总风险上限(% 或 $),算同时满足全部条件的最大股数;一键「记入持仓」

**估值** — `company-valuation` + `estimate-analysis`
- DCF + 分析师目标 + 远期PE 三角定位合理价与上下空间;分析师预期表 + EPS 预期修正方向

**期权墙** — Max Pain / OI 墙 / GEX(对股价的影响,非交易期权)
- Max Pain 最大痛点、Call/Put OI 支撑压力墙、**净 GEX + Gamma Flip 翻转点**、Put/Call 比率
- OI 分布柱状图 + GEX 剖面图;关键墙位可叠加到概览 K 线

**多股对比** — `stock-correlation`
- 归一化走势叠加 + 区间涨幅 / 年化波动 / Beta + **相关性矩阵热力**

## 页面2 · 持仓(SQLite 持久化)

- 持仓表:实时浮盈亏、距止损、R 倍数;汇总:总市值/浮盈亏、仓位占比、**组合风险敞口(热度)**
- 手动添加 / 平仓 / 删除;已平仓沉淀为交易记录

## 页面3 · 市场热力图(独立页签)

- **个股热力图**:近百只标普大盘股按板块分组 treemap,面积≈市值,颜色=当日涨跌
- **板块热力图**:11 板块 treemap(同风格,面积=板块市值,色=市值加权涨跌),ETF 数据在悬浮提示

顶部常驻**大盘条**:标普500 / 纳指 / VIX + 市场环境(Bull/Choppy/Bear)+ 情绪 — `sepa-strategy/market-environment`

> 说明:SEPA 的 RS、大盘情绪、GEX/Gamma Flip、热力图面积均为代理/估算;DCF 为简化模型,金融/REIT/亏损公司自动回退到分析师目标+远期PE。

## 部署

生产部署到服务器 + Cloudflare 域名(Tunnel + Access 零信任):见 **[`deploy/README-deploy.md`](deploy/README-deploy.md)**。本地用浏览器 localStorage 之外的数据均存服务端 SQLite(`DASHBOARD_DB`,默认 `webapp/data/dashboard.db`)。

## 数据缓存(K线持久化)

日线 K 线落库到同一个 SQLite(`bars` / `bars_meta` 表),**重启不丢、跨请求共享、大幅降 yfinance 压力**:

- **触发式**:任何被访问的标的,其日线自动落库;后续请求直接读库(实测二次命中 ~8ms vs 首拉 ~2s)。距上次拉取 `FAST_TTL`(90s)内不碰网络;过期或盘中只增量拉 `1mo` 而非重拉整段。
- **收盘后自动刷新**:应用内后台线程每天 **22:10 UTC**(美股 EDT/EST 收盘后)增量刷新「**自选股 ∪ 持仓**」当日 K 线;其他标的触发式更新。
- **手动触发**:`POST /api/cache/refresh`;或容器内 `docker exec finance-dash python cache_update.py`。
- **持仓/自选/预警/设置** 一直就在同一个 SQLite,本来就持久化;备份照旧只需拷 `dashboard.db` 一个文件。

> 调度线程假设**单 worker**(生产 Dockerfile 已是 `-w 1`)。多 worker 部署时,其余 worker 请设 `ENABLE_SCHEDULER=0`,避免重复刷新。

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
| `/api/cache/refresh` (POST) | 手动触发 K线缓存增量更新(`?ticker=AAPL` 单只;无参刷新自选股∪持仓) |
| `/api/cache/status` | 缓存概览(每个标的已存日线区间、行数、上次拉取时间) |

## 部署到个人网站

`app.py` 用 Flask 开发服务器运行(`app.run`)。部署到生产时建议:

```bash
# 用 gunicorn 跑(需先 pip install gunicorn)
gunicorn -w 2 -b 0.0.0.0:8000 'app:app'
```

再用 Nginx/Caddy 反代到你的域名即可。也可容器化部署(Dockerfile 可按需补充)。

> ⚠️ 数据来自 Yahoo Finance(yfinance),非实时、有延迟,仅供研究学习,不构成投资建议。
