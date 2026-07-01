# 个股看板 · Stock Dashboard

![version](https://img.shields.io/badge/version-1.0.1-blue)

一个 Flask + yfinance 的个人炒股决策仪表盘,**四页面** SPA。前端用 [TradingView lightweight-charts](https://github.com/tradingview/lightweight-charts) 画 K 线、[ECharts](https://echarts.apache.org/) 画热力图/期权墙/GEX,暗色主题。持仓/交易流水/复盘/自选/预警/设置存在服务端 **SQLite**。

> 当前版本 **V1.0.1**。完整变更见 [`CHANGELOG.md`](CHANGELOG.md)。按[语义化版本](https://semver.org/lang/zh-CN/)管理,git 标签为 `webapp-v<版本>`。

## 页面1 · 个股看板

五个子标签 + **左侧栏自选股**(后端持久化,点 ☆ 加自选,所有页面常驻;窄屏自动横排)+ 触发预警 banner:

**概览**
- 顶部 **一键决策卡**:聚合 SEPA + 估值 + 期权墙 → 买入/观察/回避 + 建议仓位
- **K线 + MA5/10/20/50/200** 叠加 + 成交量 +(可选)**期权墙位叠加**(Max Pain / 压力墙 / 支撑墙 / Gamma Flip)
  - **时间周期 = 单根 K 线代表的时长**:`4h / 1d / 1w`(默认 1d)。本系统只看趋势,不做盘中实时(实时报警交由 TradingView),故最小周期为 4h;`4h` 由 1h 后端重采样合成(Yahoo 不原生提供,1h 数据上限 730 天)
- **SEPA 趋势模板**评分卡:8 条件 + 四阶段 + 基本面评级 + 结论 — `sepa-strategy`
- **财报日 / 业绩**:下次财报日、预期 EPS、历史 beat/miss — `earnings-preview`
- **流动性评分** — `stock-liquidity`、关键财务、分析师评级、新闻流
- **价格 / 止损预警**:到价、跌破 20MA、触及止损(后端实时评估,触发上 banner)

**仓位计算** — `sepa-strategy/position-sizing`
- 输入买入价/止损价/总资产 + 总仓位上限 + 总风险上限(% 或 $),算同时满足全部条件的最大股数
- **买入股数可手改**(默认最大值),投入资金/风险金额实时重算;改任一条件即重置回最大值
- 一键「记入持仓」,按实际填入的股数记录

**估值** — `company-valuation` + `estimate-analysis`
- DCF + 分析师目标 + 远期PE 三角定位合理价与上下空间;分析师预期表 + EPS 预期修正方向

**期权墙** — Max Pain / OI 墙 / GEX(对股价的影响,非交易期权)
- Max Pain 最大痛点、Call/Put OI 支撑压力墙、**净 GEX + Gamma Flip 翻转点**、Put/Call 比率
- OI 分布柱状图 + GEX 剖面图;关键墙位可叠加到概览 K 线

**多股对比** — `stock-correlation`
- 归一化走势叠加 + 区间涨幅 / 年化波动 / Beta + **相关性矩阵热力**

## 页面2 · 持仓(SQLite 持久化)

- 顶部 **总资金量**:可编辑、持久化,作为唯一真相;显示已投入/可用现金/浮盈亏;仓位计算默认从此取值
- 持仓表:实时浮盈亏、**距止损(回落跌幅,绿赚红亏)**、**风险敞口($ = (现价−止损)×股数)**、备注
- 汇总:总市值/浮盈亏、**仓位占账户(按市值)**、**组合风险敞口(热度)**
- **行内编辑**(股数/成本/现价/止损/目标/备注);只有一笔买入的仓位编辑后会同步修正其交易明细
- **加仓自动合并**到同标的仓位(加权平均成本),不再重复建行
- **手填现价**:期权/港股等无实时行情的标的可自填现价,不走 yfinance
- **空头支持**:负股数表示卖出开仓(如备兑 call),盈亏方向与平仓均正确
- **平仓**:行内输入平仓价 + 数量,支持部分平仓(保留剩余并注明)与清仓(记累计已实现盈亏)

### 交易明细(买卖流水台账)
- 每笔买入/卖出独立记录,带精确时间戳,**严格按成交先后倒序**(最新在最上);分页,每页 20 条,可单条删除

## 页面3 · 每日复盘

- **「今天」实时显示**当前组合(改总资金量/价格/交易即时反映)
- 每个交易日 **17:00(美东)自动冻结**当日存档(持仓、交易、盈亏、敞口);历史日期为只读快照
- 每日可写**复盘文字**并持久化,供日后回查、提升交易能力;另有「立即冻结/更新今日存档」按钮

## 页面4 · 资金曲线 · 收益跟踪

- **每日快照(8点→8点窗口)**:美东 20:00 自动锁定当日总资金量 + 收盘持仓市值/成本;在持仓页更新总资金量时即时落当日快照
- **收益口径**:累计/区间收益率按每日收益率连乘的**时间加权**计算,**净入金(出入金)不计入收益**;当日收益 = 今值 − 昨值 − 当日净入金
- **echarts 双轴曲线**:总资金量 + 累计收益率,出入金标记点;区间切换(全部 / 近30 / 近7天)
- 支持**手动补录 / 编辑 / 删除**历史快照
- 独立 `/api/equity` 路由与「每日复盘」(`/api/snapshots`)分离,互不冲突

## 页面5 · 市场热力图(独立页签)

- **个股热力图**:按板块分组 treemap,面积≈市值,颜色=当日涨跌
- **板块热力图**:板块 treemap(同风格,面积=板块市值,色=市值加权涨跌),ETF 数据在悬浮提示
- **⚙ 自定义板块**:可增删板块、编辑成分股与对标 ETF,**同股可归多板块**;内置 11 板块为默认可恢复
- **缓存策略**:盘后一律用缓存(零网络);盘中点 ↻ 或每 5 分钟自动增量刷新,温柔对待 Yahoo 限流

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
| `/api/history?ticker=AAPL&period=1d` | K线 OHLC + 成交量 + MA5/10/20/50/200(`period` 为单根 K 时长:`4h/1d/1w`) |
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
| `/api/positions` (GET/POST) | 持仓列表(实时盈亏/敞口 + 汇总 + 买卖流水);POST 建仓/加仓(同标的合并),记买入流水 |
| `/api/positions/<id>` (PUT/DELETE) | 编辑/平仓(`action=close`,支持部分与空头)/删除;单买入仓位编辑同步流水 |
| `/api/trades/<id>` (DELETE) | 删除一条交易流水 |
| `/api/snapshots` (GET) | 复盘存档日期列表 + 美东 today |
| `/api/snapshots/take` (POST) | 立即冻结/更新今日存档 |
| `/api/snapshots/<date>` (GET) | 某日冻结快照(持仓/交易/汇总)+ 复盘文字 |
| `/api/snapshots/<date>/review` (POST) | 保存某日复盘文字 |
| `/api/notes?ticker=AAPL` (GET/POST/DELETE) | 个股研究笔记 |
| `/api/heatmap` | 全市场个股 + 板块热力图(`?force=1` 盘中强制刷新;盘后恒用缓存) |
| `/api/heatmap/sectors` (GET/POST) | 读取/覆盖自定义板块配置(GET 含内置默认) |
| `/api/notify/status` | Telegram 推送开关状态 + 是否已配置推送通道 |
| `/api/notify/test` (POST) | 发送一条测试推送(验证链路) |
| `/api/cache/refresh` (POST) | 手动触发 K线缓存增量更新(`?ticker=AAPL` 单只;无参刷新自选股∪持仓) |
| `/api/cache/status` | 缓存概览(每个标的已存日线区间、行数、上次拉取时间) |

## 部署到个人网站

`app.py` 用 Flask 开发服务器运行(`app.run`)。部署到生产时建议:

```bash
# 用 gunicorn 跑(需先 pip install gunicorn)
gunicorn -w 2 -b 0.0.0.0:8000 'app:app'
```

再用 Nginx/Caddy 反代到你的域名即可。也可容器化部署(Dockerfile 可按需补充)。

## Telegram 告警推送(个人持仓/组合)

webapp 的预警(到价/触止损/跌破20MA)+ **持仓止损/目标** 可推送到 Telegram,**复用 tv-relay 中转**(webapp 不存 bot token)。
盘中后台每 ~3 分钟轮询、触发推一次(去重,条件回落后重新武装);**网页有全局开关,默认关,关→完全不推**。
分工:个人持仓/组合告警走这里;市场技术形态(突破/急拉)走 TradingView/Pine。

**激活(部署方设置一个环境变量即可)**:给 finance-dash 容器设 `TG_RELAY_WEBHOOK` 指向中转的 webapp 端点:
```bash
# 推荐用公网 URL(容器走正常 egress,无需改 docker 网络;secret 从中转 env 取)
-e TG_RELAY_WEBHOOK="https://tv-relay.traderjiao.com/tv/<TV_RELAY_SECRET>/webapp"
```
未设置该变量时,推送功能不可用(网页开关置灰)。设置后在网页打开开关即生效。

## 文档

- [`ROADMAP.md`](ROADMAP.md) — 已完成改动 + 未来功能(机器人进出场/回测、交易日志/画线、Telegram、TradingView、净值曲线)+ 版面重设计建议
- [`docs/options-walls.md`](docs/options-walls.md) — 期权墙(Max Pain / OI 墙 / GEX / Gamma Flip)算法与准确性说明

> ⚠️ 数据来自 Yahoo Finance(yfinance),非实时、有延迟,仅供研究学习,不构成投资建议。
