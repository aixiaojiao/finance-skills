#!/usr/bin/env python3
"""手动/定时触发 K 线缓存增量更新(自选股 ∪ 持仓)。

应用内已有后台线程每日收盘后自动刷新;本脚本是手动/备份触发方式,例如:
    docker exec finance-dash python cache_update.py
或宿主机 cron 调用。导入时禁用应用内调度器,避免起重复线程。
"""
import os

os.environ.setdefault("ENABLE_SCHEDULER", "0")

import app  # noqa: E402  (设置完环境变量后再导入)


def main():
    res = app.refresh_tracked_bars()
    ok = sum(1 for v in res.values() if v and v > 0)
    print(f"refreshed {ok}/{len(res)} tickers")
    for t, n in sorted(res.items()):
        print(f"  {t}: {n}")


if __name__ == "__main__":
    main()
