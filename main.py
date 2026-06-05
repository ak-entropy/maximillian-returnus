"""
Maximillian Returnus — Polymarket 5-minute BTC Up/Down paper-trading harness.

Strategy (paper / DRY RUN by default):
  - Watch the live order book of the current 5-minute "BTC Up or Down" market.
  - If UP midpoint <= LOW_THRESH within the first 4 minutes  -> simulate BUY YES (UP)
  - If UP midpoint >= HIGH_THRESH within the first 4 minutes -> simulate BUY NO  (DOWN)
  - First touch wins; one entry per market. Hold to resolution.
  - Read Polymarket's resolved outcome -> mark Win / Loss, record hypothetical P&L.
  - Auto-advance to the next market (markets are deterministic on the UTC clock).

Markets are discovered deterministically:
  window_ts = floor(now_utc / 300) * 300
  slug      = "btc-updown-5m-{window_ts}"

NOTE: This is a PAPER harness. It places NO real orders. Live execution can be
added later (reuse the Maximus FOK _place_with_retry pattern) once the data
confirms an edge.
"""

import asyncio
import contextlib
import json
import logging
import os
import sqlite3
import time
from collections import deque
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("MAX_DB_PATH", os.path.join(BASE_DIR, "data", "maximillian.db"))
DASHBOARD_PATH = os.path.join(BASE_DIR, "dashboard.html")
AUTH_TOKEN = os.environ.get("MAX_AUTH_TOKEN", "CHANGE_ME_SET_MAX_AUTH_TOKEN")

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

WINDOW_SECS = 300          # 5-minute markets
ENTRY_CUTOFF_SECS = 240    # "within the first 4 minutes"
RESOLVE_DELAY_SECS = 8     # wait this long after close before asking for resolution
VOID_AFTER_SECS = 1800     # give up resolving a trade after 30 min -> VOID

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("maximillian")

# --------------------------------------------------------------------------- #
# Runtime state (resets on restart; trades persist in SQLite)
# --------------------------------------------------------------------------- #
ST = {
    "running": False,
    "started_at": None,
    "last_poll": None,
    "poll_count": 0,
    "asset": "btc",
    "settings": {
        "low_thresh": 0.30,     # UP mid <= this  -> BUY YES (UP)
        "high_thresh": 0.70,    # UP mid >= this  -> BUY NO  (DOWN)
        "stake": 1.00,          # paper stake per trade ($)
        "poll_interval": 5.0,   # seconds between polls
        "entry_cutoff": ENTRY_CUTOFF_SECS,
    },
    "current": {},              # snapshot of the active market window
    "windows_observed": 0,
    "errors": deque(maxlen=50),
}

# entry tracking for the in-flight window: {window_ts: trade_id} so we never double-enter
_entered_windows = {}
_known_window = None
_log_journal = deque(maxlen=500)
_ws_clients: set[WebSocket] = set()
_http: httpx.AsyncClient | None = None
_poller_task: asyncio.Task | None = None


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                window_ts       INTEGER NOT NULL,
                slug            TEXT NOT NULL,
                asset           TEXT NOT NULL,
                side            TEXT NOT NULL,        -- YES or NO
                bought_outcome  TEXT NOT NULL,        -- UP or DOWN
                trigger_up_mid  REAL,
                fill_price      REAL,
                shares          REAL,
                stake           REAL,
                entry_iso       TEXT,
                status          TEXT NOT NULL,        -- PENDING / WIN / LOSS / VOID
                resolved_outcome TEXT,                -- UP / DOWN
                payout          REAL,
                pnl             REAL,
                resolved_iso    TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON trades(status)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_window ON trades(window_ts, asset)")
        conn.commit()


# --------------------------------------------------------------------------- #
# Journal / broadcast
# --------------------------------------------------------------------------- #
def push_log(msg: str, level: str = "info"):
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "level": level, "msg": msg}
    _log_journal.append(entry)
    log.info("[journal] %s", msg)
    asyncio.create_task(_broadcast({"type": "log", "entry": entry}))


async def _broadcast(payload: dict):
    if not _ws_clients:
        return
    dead = []
    data = json.dumps(payload, default=str)
    for ws in list(_ws_clients):
        try:
            await ws.send_text(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)


# --------------------------------------------------------------------------- #
# Polymarket helpers (deterministic discovery + CLOB pricing)
# --------------------------------------------------------------------------- #
def current_window_ts(now: float | None = None) -> int:
    now = now if now is not None else time.time()
    return int(now - (now % WINDOW_SECS))


def slug_for(window_ts: int, asset: str) -> str:
    return f"{asset}-updown-5m-{window_ts}"


async def fetch_event(slug: str) -> dict | None:
    """Return {up_token, down_token, market, closed, resolved_outcome} or None."""
    try:
        r = await _http.get(f"{GAMMA}/events", params={"slug": slug})
        r.raise_for_status()
        events = r.json()
    except Exception as e:
        ST["errors"].append(f"fetch_event {slug}: {e}")
        return None
    if not events:
        return None
    event = events[0]
    markets = event.get("markets") or []
    if not markets:
        return None
    m = markets[0]

    # outcomes & clobTokenIds arrive as JSON-encoded strings, aligned by index
    outcomes = m.get("outcomes")
    token_ids = m.get("clobTokenIds")
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)
    if isinstance(token_ids, str):
        token_ids = json.loads(token_ids)
    if not outcomes or not token_ids or len(outcomes) != len(token_ids):
        return None

    up_token = down_token = None
    for name, tid in zip(outcomes, token_ids):
        if str(name).strip().lower() == "up":
            up_token = tid
        elif str(name).strip().lower() == "down":
            down_token = tid
    if not up_token or not down_token:
        # fall back to positional [Up, Down]
        up_token, down_token = token_ids[0], token_ids[1]

    closed = bool(m.get("closed"))
    resolved_outcome = None
    prices = m.get("outcomePrices")
    if isinstance(prices, str):
        with contextlib.suppress(Exception):
            prices = json.loads(prices)
    if closed and isinstance(prices, list) and len(prices) == len(outcomes):
        for name, p in zip(outcomes, prices):
            with contextlib.suppress(Exception):
                if float(p) >= 0.99:
                    resolved_outcome = "UP" if str(name).strip().lower() == "up" else "DOWN"
    return {
        "up_token": up_token,
        "down_token": down_token,
        "closed": closed,
        "resolved_outcome": resolved_outcome,
    }


async def fetch_midpoint(token_id: str) -> float | None:
    try:
        r = await _http.get(f"{CLOB}/midpoint", params={"token_id": token_id})
        r.raise_for_status()
        return float(r.json()["mid"])
    except Exception as e:
        ST["errors"].append(f"midpoint: {e}")
        return None


async def fetch_buy_price(token_id: str) -> float | None:
    """Best ask = what you'd pay to BUY this token."""
    try:
        r = await _http.get(f"{CLOB}/price", params={"token_id": token_id, "side": "BUY"})
        r.raise_for_status()
        return float(r.json()["price"])
    except Exception as e:
        ST["errors"].append(f"buy_price: {e}")
        return None


# --------------------------------------------------------------------------- #
# Trade recording
# --------------------------------------------------------------------------- #
def record_entry(window_ts, slug, asset, side, outcome, trigger_mid, fill_price, stake):
    shares = round(stake / fill_price, 4) if fill_price and fill_price > 0 else 0.0
    entry_iso = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        try:
            cur = conn.execute(
                """INSERT INTO trades
                   (window_ts, slug, asset, side, bought_outcome, trigger_up_mid,
                    fill_price, shares, stake, entry_iso, status)
                   VALUES (?,?,?,?,?,?,?,?,?,?,'PENDING')""",
                (window_ts, slug, asset, side, outcome, trigger_mid,
                 fill_price, shares, stake, entry_iso),
            )
            conn.commit()
            return cur.lastrowid
        except sqlite3.IntegrityError:
            return None  # already traded this window


def resolve_trade(trade_id, winner):
    with db() as conn:
        row = conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
        if not row:
            return
        win = (row["bought_outcome"] == winner)
        payout = row["shares"] if win else 0.0
        pnl = round(payout - row["stake"], 4)
        conn.execute(
            """UPDATE trades SET status=?, resolved_outcome=?, payout=?, pnl=?, resolved_iso=?
               WHERE id=?""",
            ("WIN" if win else "LOSS", winner, round(payout, 4), pnl,
             datetime.now(timezone.utc).isoformat(), trade_id),
        )
        conn.commit()
    return win


def void_trade(trade_id):
    with db() as conn:
        conn.execute("UPDATE trades SET status='VOID', resolved_iso=? WHERE id=?",
                     (datetime.now(timezone.utc).isoformat(), trade_id))
        conn.commit()


# --------------------------------------------------------------------------- #
# Core poll
# --------------------------------------------------------------------------- #
async def poll_once():
    global _known_window
    ST["last_poll"] = datetime.now(timezone.utc).isoformat()
    ST["poll_count"] += 1
    asset = ST["asset"]
    s = ST["settings"]
    now = time.time()
    wts = current_window_ts(now)
    seconds_into = now - wts
    slug = slug_for(wts, asset)

    # new window?
    if wts != _known_window:
        _known_window = wts
        ST["windows_observed"] += 1
        push_log(f"New window {slug} — {datetime.fromtimestamp(wts, timezone.utc).strftime('%H:%M:%S')} UTC")

    ev = await fetch_event(slug)
    cur = {
        "window_ts": wts,
        "slug": slug,
        "asset": asset,
        "seconds_into": round(seconds_into, 1),
        "seconds_left": round(WINDOW_SECS - seconds_into, 1),
        "found": ev is not None,
    }

    if ev:
        up_mid = await fetch_midpoint(ev["up_token"])
        cur["up_mid"] = up_mid
        cur["down_mid"] = round(1 - up_mid, 4) if up_mid is not None else None

        already = wts in _entered_windows
        cur["entered"] = already

        if (not already and up_mid is not None and seconds_into <= s["entry_cutoff"]):
            side = outcome = None
            if up_mid <= s["low_thresh"]:
                side, outcome, buy_token = "YES", "UP", ev["up_token"]
            elif up_mid >= s["high_thresh"]:
                side, outcome, buy_token = "NO", "DOWN", ev["down_token"]

            if side:
                fill = await fetch_buy_price(buy_token)
                if fill is None or fill <= 0:
                    fill = up_mid if outcome == "UP" else round(1 - up_mid, 4)
                tid = record_entry(wts, slug, asset, side, outcome,
                                   round(up_mid, 4), round(fill, 4), s["stake"])
                if tid:
                    _entered_windows[wts] = tid
                    cur["entered"] = True
                    push_log(
                        f"ENTRY {slug}: UP mid {up_mid:.2f} -> BUY {side} ({outcome}) "
                        f"@ {fill:.2f}  [t+{seconds_into:.0f}s]  PAPER",
                        level="entry",
                    )

    ST["current"] = cur
    await resolve_pending()
    await _broadcast({"type": "state", "current": cur, "stats": compute_stats()})


async def resolve_pending():
    now = time.time()
    with db() as conn:
        rows = conn.execute("SELECT * FROM trades WHERE status='PENDING'").fetchall()
    for row in rows:
        close_ts = row["window_ts"] + WINDOW_SECS
        if now < close_ts + RESOLVE_DELAY_SECS:
            continue
        ev = await fetch_event(row["slug"])
        winner = ev.get("resolved_outcome") if ev else None
        if winner in ("UP", "DOWN"):
            win = resolve_trade(row["id"], winner)
            push_log(
                f"RESOLVED {row['slug']}: winner {winner} -> "
                f"{'WIN ✅' if win else 'LOSS ❌'} (bought {row['bought_outcome']})",
                level="win" if win else "loss",
            )
            _entered_windows.pop(row["window_ts"], None)
        elif now > close_ts + VOID_AFTER_SECS:
            void_trade(row["id"])
            push_log(f"VOID {row['slug']}: no resolution after 30m", level="loss")
            _entered_windows.pop(row["window_ts"], None)


def compute_stats():
    with db() as conn:
        rows = conn.execute("SELECT * FROM trades").fetchall()
    wins = sum(1 for r in rows if r["status"] == "WIN")
    losses = sum(1 for r in rows if r["status"] == "LOSS")
    pending = sum(1 for r in rows if r["status"] == "PENDING")
    void = sum(1 for r in rows if r["status"] == "VOID")
    settled = wins + losses
    pnl = round(sum(r["pnl"] or 0 for r in rows if r["status"] in ("WIN", "LOSS")), 4)
    staked = round(sum(r["stake"] or 0 for r in rows if r["status"] in ("WIN", "LOSS")), 4)
    return {
        "total": len(rows),
        "wins": wins,
        "losses": losses,
        "pending": pending,
        "void": void,
        "win_rate": round(100 * wins / settled, 1) if settled else None,
        "pnl": pnl,
        "roi": round(100 * pnl / staked, 1) if staked else None,
        "windows_observed": ST["windows_observed"],
    }


# --------------------------------------------------------------------------- #
# Poller loop
# --------------------------------------------------------------------------- #
async def poller():
    push_log("Poller started")
    while ST["running"]:
        try:
            await poll_once()
        except Exception as e:
            ST["errors"].append(f"poll: {e}")
            log.exception("poll error")
        await asyncio.sleep(ST["settings"]["poll_interval"])
    push_log("Poller stopped")


# --------------------------------------------------------------------------- #
# FastAPI app
# --------------------------------------------------------------------------- #
app = FastAPI(title="Maximillian Returnus")


def require_auth(authorization: str | None):
    if authorization != f"Bearer {AUTH_TOKEN}":
        raise HTTPException(status_code=401, detail="bad token")


@app.on_event("startup")
async def _startup():
    global _http
    init_db()
    _http = httpx.AsyncClient(timeout=10.0, headers={"User-Agent": "maximillian/1.0"})
    log.info("Maximillian ready. Auth token set: %s", AUTH_TOKEN != "CHANGE_ME_SET_MAX_AUTH_TOKEN")


@app.on_event("shutdown")
async def _shutdown():
    ST["running"] = False
    if _http:
        await _http.aclose()


@app.get("/health")
async def health():
    return {
        "running": ST["running"],
        "mode": "PAPER",
        "asset": ST["asset"],
        "last_poll": ST["last_poll"],
        "poll_count": ST["poll_count"],
        "current": ST.get("current", {}),
        "stats": compute_stats(),
        "settings": ST["settings"],
    }


@app.post("/start")
async def start(authorization: str | None = Header(None)):
    require_auth(authorization)
    global _poller_task
    if ST["running"]:
        return {"running": True, "note": "already running"}
    ST["running"] = True
    ST["started_at"] = datetime.now(timezone.utc).isoformat()
    _poller_task = asyncio.create_task(poller())
    return {"running": True}


@app.post("/stop")
async def stop(authorization: str | None = Header(None)):
    require_auth(authorization)
    ST["running"] = False
    return {"running": False}


@app.post("/tick")
async def tick(authorization: str | None = Header(None)):
    """Manual single poll (does not require the loop to be running)."""
    require_auth(authorization)
    await poll_once()
    return {"current": ST.get("current", {}), "stats": compute_stats()}


@app.get("/state")
async def state():
    return {"current": ST.get("current", {}), "stats": compute_stats(),
            "running": ST["running"], "settings": ST["settings"]}


@app.get("/trades")
async def trades(limit: int = 200):
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM trades ORDER BY window_ts DESC LIMIT ?", (limit,)
        ).fetchall()
    return JSONResponse([dict(r) for r in rows])


@app.get("/stats")
async def stats():
    return compute_stats()


@app.get("/journal")
async def journal():
    return list(_log_journal)


@app.post("/settings")
async def update_settings(payload: dict, authorization: str | None = Header(None)):
    require_auth(authorization)
    s = ST["settings"]
    for k in ("low_thresh", "high_thresh", "stake", "poll_interval", "entry_cutoff"):
        if k in payload:
            s[k] = float(payload[k])
    if "asset" in payload:
        ST["asset"] = str(payload["asset"]).lower()
    push_log(f"Settings updated: {s} asset={ST['asset']}")
    return {"settings": s, "asset": ST["asset"]}


@app.post("/reset")
async def reset(authorization: str | None = Header(None)):
    """Wipe the trade history (paper data only)."""
    require_auth(authorization)
    with db() as conn:
        conn.execute("DELETE FROM trades")
        conn.commit()
    _entered_windows.clear()
    ST["windows_observed"] = 0
    push_log("Trade history reset", level="warn")
    return {"ok": True}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.add(ws)
    with contextlib.suppress(Exception):
        await ws.send_text(json.dumps({
            "type": "state", "current": ST.get("current", {}),
            "stats": compute_stats(), "running": ST["running"],
        }))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(ws)


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    try:
        with open(DASHBOARD_PATH, encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>Maximillian Returnus</h1><p>dashboard.html not found</p>")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8002, reload=False)
