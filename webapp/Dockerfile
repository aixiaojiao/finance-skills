FROM python:3.12-slim

WORKDIR /app

# 依赖先装,利用缓存
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 应用代码
COPY app.py wsgi.py ./

# 数据库放在挂载卷,持久化
ENV DASHBOARD_DB=/data/dashboard.db
VOLUME ["/data"]

EXPOSE 8000

# yfinance 部分请求较慢(热力图首次~20s),timeout 放宽
CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:8000", "--timeout", "120", "wsgi:app"]
