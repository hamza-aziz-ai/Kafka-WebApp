# FAILURE: where it breaks and how you would notice

Checked against the same code as [FLOW.md](FLOW.md). Most rows were simulated on 2026-11-03 against `v0.7.0` on a private stack: a Kafka broker with topic auto-create off (like CCTV's), a stub image and name server that can switch faults, a producer, and headless Chrome logging the screen every 500 ms. The results and times are in [Simulation results](#simulation-results-2026-11-03). None have been run against a real CCTV host. Use this as the simulation checklist: for each row, cause the failure, then record what really happened and how long recovery took.

## Dependencies

| Dependency | Failure | What happens | How you notice |
|---|---|---|---|
| Server network | Down | After 10 failed passes, at least 5 seconds, a red `Network Issue` message. Consuming stops | On screen, in the log |
| Any service port | Closed | After 10 failed passes, at least 5 seconds, a red `Temporarily Down` message. Recovery waits 2 minutes 5 seconds before consuming | On screen, in the log |
| Kafka | Port open but failing | The partition check fails every 10 seconds, nothing is read, and the screen stays on the idle face guide | Log only |
| Kafka | Slow | Rows appear late | Nobody is told |
| Kafka | Topic missing or wrong `KAFKA_TOPIC` | The screen stays blank, and the check repeats every 60 seconds | Log only |
| Settings | `KAFKA_PARTITIONS` names a partition that does not exist | A warning is logged at every check, and it is picked up when it appears | Log only |
| Settings | `IP_ADDRESS` not set | The default `127.0.0.1` is used. In a container that is the container itself, so a red `Network Issue` panel shows | On screen, in the log |
| Settings | `KAFKA_PARTITIONS` is not whole numbers | The app stops at startup with a clear error | The container exits |
| Camera | Lighting or stream problem | The CCTV producer sends an `ERROR` control message, scrolled in the marquee as an amber chip until that camera's `SUCCESS`, or 60 seconds after the last repeat | On screen, in the strip |
| Camera | Added while running | Its partition is picked up within 60 seconds, and anything it sent before then is not shown | Log: `Reading <topic> partitions [...]` |
| Image server | Unknown person ID or missing image | A placeholder JPEG, or a coloured silhouette once the server sends `null`, shows under the ID, with no error | A person spotting the placeholder |
| Image server | Hung, port open | Each message waits 5 seconds, then the update is dropped. The screen keeps the last rows, and the consumer falls behind | Log only |
| Image server | Non-200 or bad JSON | The update is dropped and logged | Log only |
| Image server | Wrong `IMAGE_SERVER_ADDRESS` | Fails as above, or shows another client's faces if it points at a live server | Log, or a person spotting a wrong face |
| CDN | Unreachable at page load | The page script stops at its first line, so the content area stays empty and never updates | The screen shows no rows or status |
| Browser | Crashes or loses the socket | While the socket is down, the page shows red `Network Issue`. On reconnect or reload, it gets the screen as the server has it, including any status with the time it has left | The kiosk `Restart=on-abort` covers only aborts |
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
- Camera status lines are lost on a display restart, and return with the next `ERROR`. Through an outage they are kept and hidden, and they return the moment the restart wait ends. Simulated on 2026-11-03: both chips returned with the rows, and a camera that stopped repeating dropped off 59 seconds later. The 60 seconds hold for both repeats on CCTV `main`, confirmed by the CCTV session on 2026-11-03: a dark camera every 30 seconds (`LIGHTING_ALERT_SECONDS`), and an inaccessible camera about every 21 seconds.
- A `SUCCESS` missed during an outage leaves its amber chip for up to 60 seconds after the wait, until it drops off unrepeated.
- A page that loads during an outage showed the idle face guide before `v0.7.3`, because statuses were pushed once and not stored. The owner hit this during a CCTV deploy on 2026-11-03. From `v0.7.3` the server keeps the status in force and sends it to every page that connects. A page that loads during the 2 minute restart wait shows the full 2 minute bar from when it loaded.
- After a Kafka reconnect, messages published during the gap are never shown, because the new consumer starts at `OFFSET_END`.
- `ping_server` never closes its sockets and relies on the garbage collector. CPython reclaims them at once, but watch the file handle count on a long run.

## Simulation results (2026-11-03)

| Scenario | What happened | Time |
|---|---|---|
| Normal message | Card with name on screen | Within 1 s of producing |
| Camera `ERROR` | Amber chip in the marquee | 0.8 s |
| Display restart with a chip showing | Chip gone until the next `ERROR` | About 6 s after the restart |
| Image server 500 or bad JSON | Update dropped and logged, old rows stay. The skipped people appear on the next good reply, because the queue keeps them | Immediate |
| Image server sends `null` | Coloured silhouettes, with names | Immediate |
| Image server hung | 5 messages took 20 s, 5 s each, while the screen kept the old rows. The 5th message, after the server recovered, showed at once | 5 s per message |
| Kafka frozen (`docker pause`), which is also the host check here | Red `Network Issue` | 16 s. Recovered 0.4 s after unpause, with no 2 minute wait |
| Display cut off its network | Logged `Network Issue` after 3.3 s and recovered at once on reconnect. The browser lost its socket too, so it saw neither status, because statuses are not stored | 3.3 s |
| Image server stopped, a listed service | Red `Temporarily Down`. About 2.2 s per pass, because a stopped container's name no longer resolves | 22.5 s |
| Image server back | `Getting Started` after 2.6 s, then consuming 2 minutes 5 seconds later. None of the 4 messages sent during the outage or the wait were ever shown | 2 min 7.6 s in all |
| Camera added (2 to 3 partitions) | Picked up at the next 60 second check. A message from it before then was never shown | 10 s here, up to 60 s |
| Wrong `KAFKA_TOPIC`, topic missing | `UNKNOWN_TOPIC_OR_PART` logged every 60 s, nothing on screen | Log only |
| `KAFKA_PARTITIONS=7`, not in the topic | `Partitions [7] are not in maingate_entry yet` logged every 60 s | Log only |
| `KAFKA_PARTITIONS=0,x` | Exits with code 1 and a clear message | At start |
| `IP_ADDRESS` not set | `Network Issue` | 3 s |
| A listed port that never opens (refuses) | On `v0.7.0`: red `Temporarily Down` within 2 s, then the loop spun at 90% of a CPU core on the display and 19% on Kafka from constant connections, with false `Network Issue` flaps when the Kafka check failed under that load. Fixed in `v0.7.1` by a 0.5 second pause after each failed pass: the red panel came after 5 s, at 0.15% CPU, with no flaps | 5 s from `v0.7.1` |
| A listed name that does not resolve | Red `Temporarily Down`, about 3 s per DNS lookup, at 0.1% CPU | 28 s |
| Kafka port open but silent | Partition check fails every 10 s, idle screen, nothing on screen | Log only |
| Page opened during an outage | On `v0.7.2`: idle face guide, not the red panel, until the next status push. Fixed in `v0.7.3`: frames recorded from the first moment show the red panel at once with no idle frame, and a page opened 45 s into the restart wait shows the bar at 39% | Immediate from `v0.7.3` |
| CDN blocked | Page script fails at its first line, content area empty | Permanent |
| 300 messages in 0.5 s | All 300 posted to the image server in 3.4 s, about 88 a second against a local stub. Sampling every 500 ms saw only 20 of the 300 people on screen, so in a burst most rows are never readable | 3.4 s |
| Bandwidth, 4 cards with 25 KB photos | About 343 KB a second per open screen, 10 frames a second | Steady |
| Long run, about 25 minutes and 330 messages | 14 open file handles at the start and end, about 51 MB of memory, 1.5% to 1.9% CPU | Steady |

Not simulated, because they need a code fault or the host: the consumer thread dying, a call stuck with no timeout, a full disk, and a browser crash.

## How you would find out today

There are no alerts. You find out from the screen or from `logs/`. The Docker health check proves only that port 9090 accepts connections. Before a real user, the display needs a way to raise an alarm when a dependency has been down for a while.
