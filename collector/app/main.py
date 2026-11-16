"""FastAPI web app: start sync, watch progress, query the local replica."""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from . import config, store
from .worker import STATUS, WORKER

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("collector.main")


@asynccontextmanager
async def lifespan(_app):
    store.init_db()
    # Resume automatically after a process restart if sync was ever started:
    # an unfinished snapshot is rebuilt, a finished one resumes streaming from
    # the persisted LSN.
    if store.get_meta("sync_requested") == "1":
        log.info("resuming sync from persisted state")
        WORKER.start()
    yield
    WORKER.stop()


app = FastAPI(title="Offline QC replica collector", lifespan=lifespan)


@app.get("/api/status")
def api_status():
    st = STATUS.snapshot()
    st.update(
        published=store.published(),
        sync_requested=store.get_meta("sync_requested") == "1",
        last_applied_lsn=store.get_meta("last_applied_lsn"),
        slot_name=config.SLOT_NAME,
        worker_running=WORKER.running(),
        snapshot_row_delay_ms=config.SNAPSHOT_ROW_DELAY_MS,
    )
    return st


@app.post("/api/sync/start")
def api_sync_start():
    store.set_meta("sync_requested", "1")
    ok, msg = WORKER.start()
    return {"ok": ok, "message": msg}


@app.post("/api/sync/reset")
def api_sync_reset():
    try:
        WORKER.reset_local()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"ok": True, "message": "replica cleared; start sync to rebuild"}


def _require_published():
    if not store.published():
        raise HTTPException(
            status_code=409,
            detail="replica not published yet (initial snapshot incomplete)",
        )


@app.get("/api/stations")
def api_stations():
    _require_published()
    rows = store.fetch_stations()
    return {
        "count": len(rows),
        "rows": [{"id": r[0], "correction": r[1]} for r in rows],
    }


@app.get("/api/readings")
def api_readings(
    station_id: int | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=5000),
):
    _require_published()
    rows = store.fetch_readings(station_id, limit)
    return {
        "count": len(rows),
        "rows": [
            {"id": r[0], "station_id": r[1], "value": r[2]} for r in rows
        ],
    }


@app.get("/api/replica/dump")
def api_replica_dump():
    """Both tables in one read transaction (consistent cross-table view)."""
    _require_published()
    stations, readings = store.fetch_dump()
    return {
        "stations": [{"id": r[0], "correction": r[1]} for r in stations],
        "readings": [
            {"id": r[0], "station_id": r[1], "value": r[2]} for r in readings
        ],
    }


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML_PAGE


HTML_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>离线质控数据副本采集</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 24px; color: #222; }
  h1 { font-size: 20px; }
  .card { border: 1px solid #ddd; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
  .badge { display: inline-block; padding: 2px 10px; border-radius: 12px; color: #fff; font-size: 13px; }
  .idle { background: #888; } .snapshot { background: #d9a000; }
  .streaming { background: #1a7f37; } .disconnected { background: #b02a37; }
  .error { background: #6f1d1b; }
  .dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; }
  .on { background: #1a7f37; } .off { background: #b02a37; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th, td { border: 1px solid #ddd; padding: 4px 8px; text-align: left; }
  th { background: #f5f5f5; }
  button { padding: 6px 14px; margin-right: 8px; border-radius: 6px; border: 1px solid #999;
           background: #fff; cursor: pointer; }
  button:hover { background: #eee; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  .kv { font-family: ui-monospace, monospace; font-size: 13px; }
  .kv b { display: inline-block; width: 150px; font-weight: 600; }
  .bar { height: 14px; background: #eee; border-radius: 7px; overflow: hidden; margin-top: 6px; }
  .bar > div { height: 100%; background: #d9a000; width: 0%; transition: width .3s; }
  .warn { color: #b02a37; font-weight: 600; }
  .muted { color: #777; font-size: 12px; }
</style>
</head>
<body>
<h1>离线质控数据副本采集器</h1>

<div class="card">
  <div>
    <span class="dot" id="connDot"></span><span id="connText">…</span>
    &nbsp;&nbsp;阶段: <span class="badge idle" id="phase">…</span>
    &nbsp;&nbsp;<span id="msg" class="muted"></span>
  </div>
  <div class="bar" id="barWrap" style="display:none"><div id="bar"></div></div>
  <div class="kv" style="margin-top:10px">
    <div><b>副本已发布</b><span id="published">…</span></div>
    <div><b>全量进度</b><span id="prog">…</span></div>
    <div><b>已应用事务/变更</b><span id="applied">…</span></div>
    <div><b>已持久化 LSN</b><span id="lsn">…</span></div>
    <div><b>最近源事务时间</b><span id="lastCommit">…</span></div>
    <div><b>复制槽</b><span id="slot">…</span></div>
  </div>
  <div style="margin-top:12px">
    <button onclick="startSync()">开始同步</button>
    <button onclick="resetSync()">重置并清空副本</button>
    <span class="muted" id="hint"></span>
  </div>
</div>

<div class="card" id="closedNote" style="display:none">
  <span class="warn">副本未发布：初始全量同步未完成，查询已关闭。</span>
</div>
<div class="card" id="staleNote" style="display:none">
  <span class="warn">源库断连：以下为最后一次成功同步的已发布副本。</span>
</div>

<div class="grid">
  <div class="card">
    <b>stations 副本</b> <span class="muted" id="stCount"></span>
    <div style="max-height:300px; overflow:auto; margin-top:8px">
      <table><thead><tr><th>id</th><th>correction</th></tr></thead>
      <tbody id="stationsBody"></tbody></table>
    </div>
  </div>
  <div class="card">
    <b>readings 副本</b> <span class="muted" id="rdCount"></span>
    <div style="max-height:300px; overflow:auto; margin-top:8px">
      <table><thead><tr><th>id</th><th>station_id</th><th>value</th></tr></thead>
      <tbody id="readingsBody"></tbody></table>
    </div>
  </div>
</div>

<script>
async function j(url, method) {
  const r = await fetch(url, {method: method || 'GET'});
  if (!r.ok) throw new Error(url + ' -> ' + r.status);
  return r.json();
}
async function poll() {
  try {
    const s = await j('/api/status');
    const ph = document.getElementById('phase');
    ph.textContent = s.phase; ph.className = 'badge ' + s.phase;
    document.getElementById('connDot').className = 'dot ' + (s.connected ? 'on' : 'off');
    document.getElementById('connText').textContent = s.connected ? '源库已连接' : '源库未连接';
    document.getElementById('msg').textContent = s.message || '';
    document.getElementById('published').textContent = s.published ? '是' : '否';
    document.getElementById('prog').textContent =
      s.phase === 'snapshot' ? (s.rows_copied + ' / ' + s.total_rows + ' 行')
                             : (s.published ? '完成' : '未开始');
    document.getElementById('applied').textContent = s.applied_txns + ' / ' + s.applied_changes;
    document.getElementById('lsn').textContent = s.last_applied_lsn || '…';
    document.getElementById('lastCommit').textContent = s.last_commit_at || '…';
    document.getElementById('slot').textContent = s.slot_name;
    document.getElementById('barWrap').style.display = s.phase === 'snapshot' ? 'block' : 'none';
    if (s.total_rows > 0)
      document.getElementById('bar').style.width = (100 * s.rows_copied / s.total_rows) + '%';
    document.getElementById('closedNote').style.display = s.published ? 'none' : 'block';
    document.getElementById('staleNote').style.display = (!s.connected && s.published) ? 'block' : 'none';
    document.getElementById('hint').textContent =
      s.worker_running ? '' : (s.sync_requested ? '工作线程未运行' : '尚未启动同步');
  } catch (e) { /* ignore transient errors */ }
}
async function loadTables() {
  let s;
  try { s = await j('/api/status'); } catch (e) { return; }
  if (!s.published) return;
  try {
    const st = await j('/api/stations');
    document.getElementById('stCount').textContent = '(' + st.count + ' 行)';
    document.getElementById('stationsBody').innerHTML =
      st.rows.map(r => `<tr><td>${r.id}</td><td>${r.correction}</td></tr>`).join('');
    const rd = await j('/api/readings?limit=1000');
    document.getElementById('rdCount').textContent = '(' + rd.count + ' 行)';
    document.getElementById('readingsBody').innerHTML =
      rd.rows.map(r => `<tr><td>${r.id}</td><td>${r.station_id}</td><td>${r.value}</td></tr>`).join('');
  } catch (e) { /* replica closed mid-refresh */ }
}
async function startSync() { await j('/api/sync/start', 'POST'); poll(); }
async function resetSync() {
  if (confirm('确定要清空本地副本与同步状态并重新开始吗？')) { await j('/api/sync/reset', 'POST'); poll(); }
}
setInterval(poll, 1000); setInterval(loadTables, 2000);
poll(); loadTables();
</script>
</body>
</html>
"""
