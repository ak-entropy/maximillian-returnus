# Maximillian Returnus

Paper-trading harness for Polymarket **5-minute BTC Up/Down** markets. Watches the
live order book, applies a contrarian rule, reads Polymarket's own resolution, and
logs hypothetical Win/Loss + P&L. **Places no real orders.**

## Strategy
- UP midpoint **≤ 0.30** within the first 4 min → simulate **BUY YES (UP)**
- UP midpoint **≥ 0.70** within the first 4 min → simulate **BUY NO (DOWN)**
- First touch only, one entry per market, held to resolution.
- Markets auto-advance — they're deterministic on the UTC clock
  (`slug = btc-updown-5m-{floor(now/300)*300}`), so there's nothing to "find."

## How it differs from Maximus
- **Internal asyncio poller** (default 5 s), not an external 60 s cron — a 5-min
  window needs fine resolution to catch a 30/70 touch. `/tick`, `/start`, `/stop`
  still exist for manual control.
- Port **8002** (Maximus is 8001). Separate service, separate DB.

## Endpoints
| Method | Path | Notes |
|---|---|---|
| GET | `/` | dashboard |
| GET | `/health` `/state` `/trades` `/stats` `/journal` | read-only |
| POST | `/start` `/stop` `/tick` `/settings` `/reset` | require `Authorization: Bearer $MAX_AUTH_TOKEN` |
| WS | `/ws` | live state + journal |

## Local run
```bash
pip install -r requirements.txt
export MAX_AUTH_TOKEN="$(openssl rand -hex 32)"
python3 -m uvicorn main:app --host 0.0.0.0 --port 8002
# open http://localhost:8002 , paste the token, hit Start
```

## Deploy on the Hetzner box (mirrors your Maximus flow)
```bash
# one-time
mkdir -p /root/maximillian && cd /root/maximillian
# (pull your repo here, or rsync the app/ folder)
cp .env.example .env && nano .env          # set MAX_AUTH_TOKEN
pip install -r requirements.txt
cp maximillian.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now maximillian
# health
curl -s http://localhost:8002/health | python3 -m json.tool
```

## Going live later
When the paper numbers justify it, wire `record_entry()` to a real order call using
the Maximus FOK `_place_with_retry` / `_do_place` pattern (price rounded to whole
cents → ÷100, `log.error` not `push_log` inside any executor). Keep a DRY/LIVE flag
exactly like S4.

## A caveat worth tracking
The two biggest threats to a contrarian 30/70 rule in these markets are **spread**
(you pay the ask, not the mid — the dashboard shows trigger-mid vs fill so you can
see the gap) and the possibility that 30%/70% prices are simply *correct*. The whole
point of running this in paper first is to find out which. Not financial advice.
