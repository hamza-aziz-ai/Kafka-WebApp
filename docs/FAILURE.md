# FAILURE: where it breaks and how you would notice

Checked against the same code as [FLOW.md](FLOW.md). None of these have been run against a real CCTV host, except the hung image server, which was simulated with a local socket for PR #7. Use this as the simulation checklist: for each row, cause the failure, then record what really happened and how long recovery took.

## Dependencies

| Dependency | Failure | What happens | How you notice |
|---|---|---|---|
| Server network | Down | After 10 failed passes, a red `Network Issue` message. Consuming stops | On screen, in the log |
| Any service port | Closed | After 10 failed passes, a red `Temporarily Down` message. Recovery waits 2 minutes 5 seconds before consuming | On screen, in the log |
| Kafka | Port open but failing | The consumer reports an error, `message_queue` clears, and the screen goes blank after 15 seconds | Log only |
| Kafka | Slow | Rows appear late | Nobody is told |
| Kafka | Topic missing or wrong `KAFKA_TOPIC` | The screen stays blank, and the check repeats every 60 seconds | Log only |
| Settings | `KAFKA_PARTITIONS` names a partition that does not exist | A warning is logged at every check, and it is picked up when it appears | Log only |
| Settings | `IP_ADDRESS` not set | The default `127.0.0.1` is used. In a container that is the container itself, so a red `Network Issue` panel shows | On screen, in the log |
| Settings | `KAFKA_PARTITIONS` is not whole numbers | The app stops at startup with a clear error | The container exits |
| Camera | Lighting or stream problem | The CCTV producer sends an `ERROR` control message, shown as an amber chip until that camera's `SUCCESS` | On screen, in the strip |
| Camera | Added while running | Its partition is picked up within 60 seconds, and anything it sent before then is not shown | Log: `Reading <topic> partitions [...]` |
| Image server | Unknown person ID or missing image | A placeholder JPEG shows under the ID, with no error | A person spotting the placeholder |
| Image server | Hung, port open | Each message waits 5 seconds, then the update is dropped. The screen goes blank and the consumer falls behind | Log only |
| Image server | Non-200 or bad JSON | The update is dropped and logged | Log only |
| Image server | Wrong `IMAGE_SERVER_ADDRESS` | Fails as above, or shows another client's faces if it points at a live server | Log, or a person spotting a wrong face |
| CDN | Unreachable at page load | The page draws but never updates | The screen shows no rows or status |
| Browser | Crashes or loses the socket | A status pushed in that moment is lost, because statuses are not stored | The kiosk `Restart=on-abort` covers only aborts |
| Consumer thread | Dies | The watchdog restarts it within 10 seconds | Log |
| Consumer thread | Stuck in a call with no timeout | Display freezes. The watchdog only sees dead threads | Nobody is told |
| Disk | Full | Logging can fail. Logs are not strictly capped across months | Host monitoring only |

## Limits, first expected first

1. **Throughput.** Each pass makes up to 7 TCP connections with the default list before it polls for one message, then one blocking POST. On a slow link, sustained bursts will lag. No limit has been measured.
2. **Screens.** Every open page polls 10 times a second and receives all images each time, so traffic grows with each screen and image size.
3. **Queue size.** The display shows 4 people. In a busy moment, earlier rows are pushed off the screen before anyone reads them.
4. **Cameras per screen.** Every camera at a point now shares one consumer loop and 4 rows, so limits 1 and 3 arrive sooner as cameras are added. No limit has been measured.
5. **Timing.** `session.timeout.ms` and the other group settings have no effect with `assign()`, so do not tune them to fix lag.

## Known behaviour worth checking in simulation

- Anything published during a services outage or the 2 minute restart wait is never shown, because the consumer is rebuilt at `OFFSET_END`.
- A listed port that never opens stops the display for good: no consumer is created until every entry answers, so the screen stays on the red panel. A display in the CCTV stack hit this with 19001.
- `PROJECT_SERVICES` entries that use a service name need DNS. A name that does not resolve counts as down, and a slow lookup slows every pass, because the 0.5 second timeout does not cover it.
- Camera status lines are lost on a display restart or a rebuilt consumer, and they return with the next `ERROR`: within 30 seconds for an inaccessible camera, and at the next evaluation for low light.
- A `SUCCESS` missed during an outage is harmless, because the rebuilt consumer empties the strip and only a live problem re-sends `ERROR`.
- A page that loads during an outage shows no status, because statuses are pushed once and not stored.
- After a Kafka reconnect, messages published during the gap are never shown, because the new consumer starts at `OFFSET_END`.
- `ping_server` never closes its sockets and relies on the garbage collector. CPython reclaims them at once, but watch the file handle count on a long run.

## How you would find out today

There are no alerts. You find out from the screen or from `logs/`. The Docker health check proves only that port 9090 accepts connections. Before a real user, the display needs a way to raise an alarm when a dependency has been down for a while.
