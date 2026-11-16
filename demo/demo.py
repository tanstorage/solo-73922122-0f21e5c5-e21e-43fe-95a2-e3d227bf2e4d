#!/usr/bin/env python3
"""端到端演示：全量中跨表更正、源库断连、采集进程重启。

前置条件：
    docker compose up -d --build
然后运行：
    python3 demo/demo.py

演示步骤：
  1. 全量同步进行中，源库在同一事务里跨表更正 stations+readings，
     验证全量期间源库可写、副本未开放(409)、衔接后更正一致生效；
  2. 流式阶段反复跨表更正，轮询副本验证两表修改原子可见（不出现半提交）；
  3. docker compose stop db：状态显示断连、已发布副本仍可查询；
     重新启动 db 后自动续传，新写入继续同步；
  4. docker compose restart collector：从持久化位置接续，无漏收、无重复生效；
  5. 全量进行中重启 collector：未完成不开放副本，自动重建后最终一致。

仅使用 Python 标准库；通过 `docker compose exec db psql` 操作源库。
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = "http://localhost:8000"
FAILS = []


# ----------------------------------------------------------------- helpers
def api(path, method="GET"):
    req = urllib.request.Request(API + path, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read() or b"{}")
        except Exception:
            body = {}
        return e.code, body
    except Exception:
        return 0, {}


def compose(*args, check=True):
    return subprocess.run(
        ["docker", "compose", *args], capture_output=True, text=True, check=check
    )


def psql(sql):
    r = compose(
        "exec", "-T", "db", "psql", "-U", "postgres", "-d", "appdb",
        "-v", "ON_ERROR_STOP=1", "-At", "-c", sql,
    )
    return r.stdout.strip()


def status():
    return api("/api/status")[1]


def phase():
    return status().get("phase")


def wait_for(desc, cond, timeout=90, interval=0.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if cond():
                return True
        except Exception:
            pass
        time.sleep(interval)
    print(f"  !! 超时: {desc}")
    return False


def get_station(sid):
    code, body = api("/api/stations")
    if code != 200:
        return None
    for r in body["rows"]:
        if r["id"] == sid:
            return r["correction"]
    return None


def get_reading(rid):
    code, body = api("/api/readings?limit=1000")
    if code != 200:
        return None
    for r in body["rows"]:
        if r["id"] == rid:
            return r["value"]
    return None


def get_pair(sid, rid):
    """stations[sid] 与 readings[rid] 的同一次原子读取（单个 SQLite 读事务）。"""
    code, body = api("/api/replica/dump")
    if code != 200:
        return None, None
    s = next((r["correction"] for r in body["stations"] if r["id"] == sid), None)
    r = next((r["value"] for r in body["readings"] if r["id"] == rid), None)
    return s, r


def compare_full():
    src_s = psql("SELECT id || ':' || correction FROM stations ORDER BY id").splitlines()
    src_r = psql(
        "SELECT id || ':' || station_id || ':' || value FROM readings ORDER BY id"
    ).splitlines()
    code, dump = api("/api/replica/dump")
    if code != 200:
        return False
    rep_s = [f"{r['id']}:{r['correction']}" for r in dump["stations"]]
    rep_r = [f"{r['id']}:{r['station_id']}:{r['value']}" for r in dump["readings"]]
    return src_s == rep_s and src_r == rep_r


def check(name, cond, extra=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name} {extra}")
    if not cond:
        FAILS.append(name)


# -------------------------------------------------------------------- demo
def main():
    print("== 0. 重置采集器（清空副本与复制槽） ==")
    api("/api/sync/reset", "POST")

    print("== 1. 全量同步进行中注入跨表更正 ==")
    api("/api/sync/start", "POST")
    wait_for("进入全量阶段", lambda: phase() == "snapshot", 30)
    code, _ = api("/api/stations")
    check("全量未完成时副本不开放 (HTTP 409)", code == 409, f"(got {code})")
    # 源库在全量期间仍可写；同一事务修改两表（readings.id=202 的 station_id=3）
    psql("BEGIN; "
         "UPDATE stations SET correction = 777 WHERE id = 3; "
         "UPDATE readings  SET value = 777      WHERE id = 202; "
         "COMMIT;")
    print("  已在全量期间提交跨表更正 stations(3)=777, readings(202)=777")
    check("全量完成进入流式", wait_for("进入流式阶段", lambda: phase() == "streaming", 180))
    check(
        "全量期间的跨表更正已生效",
        wait_for("更正到达副本",
                 lambda: get_station(3) == 777 and get_reading(202) == 777, 30),
    )
    check("副本与源完全一致", wait_for("全量后一致", compare_full, 30))

    print("== 2. 流式阶段跨表修改的原子可见性 ==")
    mixed = 0
    for i in range(1, 11):
        marker = 900000 + i
        psql(f"BEGIN; "
             f"UPDATE stations SET correction = {marker} WHERE id = 7; "
             f"UPDATE readings  SET value = {marker}      WHERE id = 207; "
             f"COMMIT;")
        deadline = time.time() + 10
        while time.time() < deadline:
            s, r = get_pair(7, 207)  # 同一读事务内的原子观察
            if s is None or r is None:
                break
            if (s == marker) != (r == marker):
                mixed += 1
            if s == marker and r == marker:
                break
            time.sleep(0.01)
    check("未观察到跨表半提交状态", mixed == 0, f"(观察到 {mixed} 次不一致)")

    print("== 3. 源库断连与恢复 ==")
    compose("stop", "db")
    check("状态显示断连",
          wait_for("connected=false", lambda: status().get("connected") is False, 60))
    code, body = api("/api/stations")
    check("断连时已发布副本仍可查询", code == 200 and body.get("count", 0) > 0,
          f"(HTTP {code}, {body.get('count', 0)} 行)")
    compose("start", "db")
    check("自动恢复流式", wait_for("恢复 streaming", lambda: phase() == "streaming", 120))
    psql("INSERT INTO stations VALUES (900, 42)")
    check("重连后新写入继续同步",
          wait_for("stations(900)=42", lambda: get_station(900) == 42, 30))

    print("== 4. 采集进程重启（流式位置接续） ==")
    psql("BEGIN; "
         "UPDATE stations SET correction = -1 WHERE id = 10; "
         "DELETE FROM readings WHERE id = 400; "
         "INSERT INTO readings VALUES (950, 10, 321); "
         "COMMIT;")
    compose("restart", "collector")
    check("重启后恢复流式",
          wait_for("恢复 streaming", lambda: phase() == "streaming", 180))
    check(
        "重启前已提交事务无漏收",
        wait_for("重启前事务生效",
                 lambda: get_station(10) == -1
                 and get_reading(400) is None
                 and get_reading(950) == 321, 60),
    )
    check("副本与源完全一致（无重复生效）", wait_for("重启后一致", compare_full, 60))

    print("== 5. 全量进行中重启采集进程（中断重建） ==")
    api("/api/sync/reset", "POST")
    api("/api/sync/start", "POST")
    wait_for("再次进入全量", lambda: phase() == "snapshot", 60)
    time.sleep(2)  # 让全量拷贝进行一段
    compose("restart", "collector")
    check("重建期间副本未开放",
          wait_for("published=false",
                   lambda: status().get("published") is False, 90))
    check("自动重建并进入流式",
          wait_for("恢复 streaming", lambda: phase() == "streaming", 240))
    check("重建后副本与源一致", wait_for("最终一致", compare_full, 90))

    print()
    if FAILS:
        print(f"演示结束：{len(FAILS)} 项未通过 -> {FAILS}")
        sys.exit(1)
    print("演示结束：全部通过。")


if __name__ == "__main__":
    main()
