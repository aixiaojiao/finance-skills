# 部署手册 — ECS + Cloudflare Tunnel + Access

把个股决策仪表盘部署到 ECS(首尔),通过 Cloudflare 域名访问,并用 Cloudflare Access 做零信任登录。应用只监听本机回环,**不开放任何公网入站端口**,由 Cloudflare Tunnel 主动连出。

## 架构

```
浏览器 → Cloudflare(TLS + Access 登录) → Tunnel → ECS 上 127.0.0.1:8799(Docker/gunicorn)
```

---

## ✅ 当前进度(首尔 ECS 上已为你完成)

- 已 `git clone` fork 到 `~/finance-skills`
- 已 **Docker 构建并运行**容器 `finance-dash`(`--restart unless-stopped`,开机自启)
- 端口**只绑 `127.0.0.1:8799`**,公网无法直连(已用 `ss` 验证仅 loopback)
- yfinance 在服务器实测正常,数据库挂载在 `~/finance-dash-data`
- `cloudflared` 已安装

所以下面的「第 1 步:跑应用」**已经做完**,你回来后从「第 2 步 Cloudflare Tunnel」开始即可。

> **实际线上状态(2026-06-29 更新)**:本应用对外域名为 `stock.traderjiao.com`,走 **token 托管(远程管理)隧道 `stock-seoul`**,在 ECS 上以**独立** systemd 服务 `cloudflared-stock.service` 运行(回源 `127.0.0.1:8799`),与服务器上已有的 `cloudflared.service`(tv-relay)并存、互不覆盖。Access 策略限 `xnch6384@gmail.com`。
> 下面「第 2 步」描述的是 `cloudflared tunnel login/create` **本地管理隧道**的手动备选流程;实际部署用的是 token 托管方式。**多隧道并存时严禁** `sudo cloudflared service install <token>`(会覆盖默认 `cloudflared.service`),应改建独立服务。
> 端口分配见服务器上的 `~/SERVER-PORTS.md`。

## 🚀 你回来后只需做这几步(Cloudflare,约 10 分钟)

你的域名已托管在 Cloudflare、只差一个子域名(Tunnel 会自动建 DNS,无需手动加子域名记录)。SSH 登到服务器后:

```bash
# 1) 浏览器授权(会打印一个 URL,在你电脑浏览器打开、选择你的域名)
cloudflared tunnel login

# 2) 建隧道(记下打印出的 TUNNEL_ID)
cloudflared tunnel create finance-dash

# 3) 写配置:把样例复制过去,填 TUNNEL_ID / 凭证路径 / 你的子域名
cp ~/finance-skills/webapp/deploy/cloudflared-config.example.yml ~/.cloudflared/config.yml
nano ~/.cloudflared/config.yml      # 改 hostname 为 dash.你的域名

# 4) 自动建 DNS 路由(这一步会在 Cloudflare 自动创建 dash 子域名的 CNAME)
cloudflared tunnel route dns finance-dash dash.你的域名

# 5) 装成系统服务、开机自启
sudo cloudflared service install
sudo systemctl status cloudflared
```

然后做 **Cloudflare Access**(见下面第 3 节)限定只有你能登录,完成。

如果服务器上的应用要更新到最新代码:
```bash
cd ~/finance-skills && git pull
cd webapp && docker build -t finance-dash . && docker rm -f finance-dash && \
docker run -d --name finance-dash --restart unless-stopped \
  --env-file ~/finance-dash.env \
  -p 127.0.0.1:8799:8000 -v ~/finance-dash-data:/data finance-dash
```
> **`--env-file ~/finance-dash.env` 必带**:内含 `TG_RELAY_WEBHOOK`(每日盘后总结 / 告警推送经 tv-relay 转 Telegram 用)。该文件由 tv-relay 的 `TV_RELAY_SECRET` 派生而来(`https://tv-relay.traderjiao.com/tv/<SECRET>/webapp`),权限 600,**不入库**。漏带这个参数会导致推送通道未配置(`configured:false`),日报与告警都发不出去。
备份数据(持仓/自选/预警):直接拷 `~/finance-dash-data/dashboard.db`。

---

## 0. 前置验证(必做)

ECS 上确认能访问 Yahoo Finance(首尔区域正常):
```bash
python3 -c "import yfinance as yf; print(yf.Ticker('AAPL').fast_info['lastPrice'])"
```
能在数秒内返回价格即可。

## 1. 跑应用(两选一)

仓库假设克隆在 `~/finance-skills`。

### 方式 A:Docker(推荐)
```bash
cd ~/finance-skills/webapp
docker build -t finance-dash .
docker run -d --name finance-dash --restart unless-stopped \
  --env-file ~/finance-dash.env \
  -p 127.0.0.1:8799:8000 \
  -v ~/finance-dash-data:/data \
  finance-dash
```
- `--env-file ~/finance-dash.env` 提供 `TG_RELAY_WEBHOOK`(经 tv-relay 转 Telegram;见上文说明)。
- `-p 127.0.0.1:8799:8000` 只绑回环,公网无法直连。
- 数据库持久化在 `~/finance-dash-data`。
- 更新:`git pull && docker build -t finance-dash . && docker rm -f finance-dash && <上面的 run 命令>`。

### 方式 B:systemd + gunicorn
```bash
cd ~/finance-skills/webapp
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
# 改 service 里的用户/路径(默认 /home/ubuntu/...)后:
sudo cp deploy/finance-dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now finance-dashboard
systemctl status finance-dashboard      # 确认 running
curl -s localhost:8799/api/market | head -c 100   # 自测
```

## 2. Cloudflare Tunnel

前提:域名已托管在 Cloudflare(NS 已指向 Cloudflare)。

```bash
cloudflared tunnel login                       # 浏览器授权选择域名
cloudflared tunnel create finance-dash         # 生成 TUNNEL_ID 和凭证 json
cp ~/finance-skills/webapp/deploy/cloudflared-config.example.yml ~/.cloudflared/config.yml
# 编辑 ~/.cloudflared/config.yml:填入 TUNNEL_ID、凭证路径、你的子域名(如 dash.你的域名)
cloudflared tunnel route dns finance-dash dash.你的域名   # 自动建 CNAME
sudo cloudflared service install               # 开机自启
sudo systemctl status cloudflared
```

## 3. Cloudflare Access(零信任登录)

Cloudflare 控制台 → **Zero Trust** → **Access** → **Applications** → Add an application → **Self-hosted**:
- Application domain:`dash.你的域名`
- Policy:Action = **Allow**,Include = **Emails** = 你的邮箱(或 Google 登录)。

保存后,所有访问 `https://dash.你的域名` 都会先跳转 Cloudflare 登录(邮箱 OTP / Google),只有你能进。

## 4. 验证

```bash
# 公网直连应用端口应失败(确认未裸露)
curl --max-time 5 http://<ECS公网IP>:8799/    # 期望:超时/拒绝

# 域名访问应先要求 Cloudflare 登录
```
浏览器打开 `https://dash.你的域名` → Cloudflare 登录 → 进入仪表盘,各功能正常。

## 备注

- **多 worker 缓存独立**:gunicorn 2 worker 各自有进程内缓存,数据稍有不同步可接受;要强一致可后续上 Redis。
- **数据库**:SQLite(WAL)。Docker 用 `/data` 卷,systemd 用 `webapp/data/`。定期备份该文件即可保住持仓/自选/预警。
- **安全**:不在应用层存敏感凭证;鉴权交给 Cloudflare Access;ECS 安全组无需为本应用开 80/443。
- **ECS 安全组**:Tunnel 只需出站 443,通常默认放行;无需额外入站规则。
