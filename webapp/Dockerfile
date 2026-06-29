FROM python:3.12-slim

WORKDIR /app

# 依赖先装,利用缓存
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 应用代码
COPY app.py wsgi.py cache_update.py ./

# 数据库放在挂载卷,持久化
ENV DASHBOARD_DB=/data/dashboard.db
VOLUME ["/data"]

EXPOSE 8000

# 单进程 + 多线程:进程内缓存全局共享(否则多 worker 各存一份、命中率减半),
# 多线程支撑前端并行打多个 API(yfinance 是网络 I/O,会释放 GIL)。timeout 放宽(热力图首次~20s)。
CMD ["gunicorn", "-w", "1", "--threads", "8", "-k", "gthread", "-b", "0.0.0.0:8000", "--timeout", "120", "wsgi:app"]
