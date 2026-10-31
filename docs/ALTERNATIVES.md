# ALTERNATIVES: what was rejected and why

Decisions made during this work are in [DECISIONS.md](DECISIONS.md). The entries below cover choices that were already in the code. Their original reasons were not written down, so the reasoning here is inferred from the code and should be confirmed by the owner.

| Choice in the code | Rejected option | Why this one (inferred) | What it gave up | Revisit if |
|---|---|---|---|---|
| Browser polls every 100 ms | Push every update with `socketio.emit` | Polling is simple, and a page that reconnects catches up on its own | Resends every image about 10 times a second per screen | Bandwidth or CPU on a Pi becomes a limit |
| `assign()` at `OFFSET_END` | `subscribe()` with a group | A live display wants only what is happening now, with no rebalancing | Reads partition 0 only, so only the first camera at a point is shown, and misses anything sent while down | Every camera at a point should feed one display |
| Image server builds the rows | Fetch images in this app | Face data stays on the CCTV side | Each message needs a network call | Image server latency hurts the display |
| In-memory state | Redis or a database | Nothing needs to survive a restart | No history, and one process only | Several workers or history are needed |
| TCP port probes | Service health endpoints | Works with every service unchanged | An open port does not prove a working service | A false healthy causes a real outage |
| One Python process with threads | Separate consumer and web services | One container, easy on a Pi | One crash takes down both | The watchdog is no longer enough |
| Flask development server | A production WSGI server | Simplest way to serve one kiosk | Not built for production load | More than a few screens connect |
