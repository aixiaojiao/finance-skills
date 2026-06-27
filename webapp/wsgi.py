"""WSGI 入口,供 gunicorn 使用:  gunicorn -w 2 -b 127.0.0.1:8000 wsgi:app

app.py 在导入时已调用 init_db(),所以这里直接导出 app 即可。
"""
from app import app  # noqa: F401
