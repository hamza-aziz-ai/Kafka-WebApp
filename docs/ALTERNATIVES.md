# ALTERNATIVES: what was rejected and why

Decisions made during this work are in [DECISIONS.md](DECISIONS.md). The entries below cover choices that were already in the code. Their original reasons were not written down, so the reasoning here is inferred from the code and should be confirmed by the owner.

| Choice in the code | Rejected option | Why this one (inferred) | What it gave up | Revisit if |
|---|---|---|---|---|
| Browser polls every 100 ms | Push every update with `socketio.emit` | Polling is simple, and a page that reconnects catches up on its own | Resends every image about 10 times a second per screen | Bandwidth or CPU on a Pi becomes a limit |
| `assign()` at `OFFSET_END` | `subscribe()` with a group | A live display wants only what is happening now, with no rebalancing | Misses anything sent while down. See D-006 for the choice to read every partition | Displays must not miss messages |
| Image server builds the rows | Fetch images in this app | Face data stays on the CCTV side | Each message needs a network call | Image server latency hurts the display |
| In-memory state | Redis or a database | Nothing needs to survive a restart | No history, and one process only | Several workers or history are needed |
| The producer's readiness endpoint (D-021) | TCP port probes, used until `v0.7.5` | The producer knows whether recognition and attendance writes work, and a port does not | Depends on one CCTV endpoint, so an older CCTV release reads as unreachable | The endpoint itself proves unreliable |
| One Python process with threads | Separate consumer and web services | One container, easy on a Pi | One crash takes down both | The watchdog is no longer enough |
| Flask development server | A production WSGI server | Simplest way to serve one kiosk | Not built for production load | More than a few screens connect |
