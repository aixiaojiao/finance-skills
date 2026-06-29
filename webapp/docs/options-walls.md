# 期权墙(Options Walls)算法说明

> 看板「个股看板 → 期权墙」标签 / K 线右侧「期权墙」叠加。
> 对应后端:`app.py` 的 `/api/options/walls`(`api_option_walls`)。

## 数据源

| 项 | 来源 | 说明 |
|---|---|---|
| 期权链(OI / Volume / IV / Strike) | `yfinance` → `Ticker.option_chain(expiry)` | Yahoo 转发的期权链;缓存 300s |
| 现价 spot | `info.currentPrice / regularMarketPrice` | yfinance,缓存 90s |
| 无风险利率 r | `^TNX`(美 10 年国债收益率)/100 | 缓存 300s,失败回退 4.5% |
| 剩余到期 T | `(到期日 - 今天) / 365`(自然日) | 至少 0.5 天 |

## 各指标算法

### 1. Max Pain(最大痛点)
对每个候选结算价 `S`,计算全体未平仓合约的内在价值赔付:

```
payout(S) = Σ_k [ callOI_k · max(S - k, 0) + putOI_k · max(k - S, 0) ]
max_pain = argmin_S payout(S)        # 在所有行权价里取赔付最小者
```

含义:期权卖方(做市商)总赔付最小、买方总收益最小的价位 —— 理论上的"到期吸引位"。
**性质**:算法标准、正确。但它是基于"所有 OI 持有到期、纯内在价值结算"的**理论**位,不是预测。

### 2. OI 墙(支撑 / 压力)
call / put 各取未平仓量最大的前 6 档行权价:
- 大 **Call OI** → 上方**压力墙**(做市商在该价位附近卖出 gamma,倾向压制突破)。
- 大 **Put OI** → 下方**支撑墙**。

### 3. GEX(Gamma Exposure,净 Gamma 敞口)
每个行权价的 Black-Scholes gamma:

```
γ(S,K,T,r,σ) = N'(d1) / (S·σ·√T),  d1 = [ln(S/K) + (r + σ²/2)T] / (σ√T)
```

每档敞口与净敞口:

```
GEX_k   = (callOI_k · γ_call_k − putOI_k · γ_put_k) · S² · 0.01 · 100
net_GEX = Σ_k GEX_k
```

- `S²·0.01` ≈ 现价 ±1% 时,每单位 gamma 带来的对冲股数变化;`·100` 为每张合约 100 股。
- **符号约定**(关键假设):本实现假设做市商 **long call / short put**,故 `call − put`。
  这是业界常见的"naive"约定,但**并非唯一** —— 部分平台假设相反,符号会反号。
- `net_GEX > 0` → 正 GEX,做市商对冲方向**抑制波动 / 倾向钉价**;`< 0` → 负 GEX,**放大波动 / 助涨助跌**。

### 4. Gamma Flip(零伽马翻转点)
把假设现价 `S` 在 `[0.6·spot, 1.4·spot]` 区间扫 80 步,计算各点净 GEX,
找其**由负转正(过零)**的价位,线性插值取最靠近现价的交叉点。各档 IV 在扫描时固定不重算(合理简化)。

### 5. Put/Call 比率
```
P/C(OI)  = Σ putOI  / Σ callOI
P/C(Vol) = Σ putVol / Σ callVol
```
偏低(<0.7)看涨情绪、偏高(>1.0)看跌情绪。

## 准确性评估(必读的 3 个硬限制)

1. **OI 是隔夜数据**:OCC 每天收盘后才结算未平仓量,Yahoo 次日早上才更新。
   → Max Pain / OI 墙 反映的是**前一交易日**的持仓,盘中不会变。
2. **行情延迟**:Yahoo 期权报价约延迟 15 分钟,盘中 volume 是延迟值。
3. **GEX 是带单边假设的代理**:真实做市商净 gamma 方向无法从公开数据确知,
   这里给的是行业常用的近似,**方向性参考**可以,别当精确值。

**结论**:Max Pain / OI 墙 / P/C 是教科书标准、可信;GEX / Gamma Flip 是带假设的启发式估算
(代码 `note` 字段已注明)。要做到券商级精度,需换付费数据源(CBOE / ORATS / Polygon)。
