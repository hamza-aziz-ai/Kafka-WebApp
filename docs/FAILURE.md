# FAILURE: where it breaks and how you would notice

Checked against the same code as [FLOW.md](FLOW.md). Most rows were simulated on 2026-11-03 against `v0.7.0` on a private stack: a Kafka broker with topic auto-create off (like CCTV's), a stub image and name server that can switch faults, a producer, and headless Chrome logging the screen every 500 ms. The results and times are in [Simulation results](#simulation-results-2026-11-03). None have been run against a real CCTV host. Use this as the simulation checklist: for each row, cause the failure, then record what really happened and how long recovery took.

## Dependencies

| Dependency | Failure | What happens | How you notice |
|---|---|---|---|
| Readiness endpoint | No answer, refused or timing out | After 3 failed checks, about 6 seconds, a red `Cannot Reach the Facial Recognition Attendance System` panel | On screen, in the log |
| Readiness endpoint | Answers `unready` | After 3 checks, a red `Temporarily Down` panel. The reasons are logged | On screen, in the log |
| Readiness endpoint | Answers `starting` | Green `Getting Started. Please Wait` until it is ready, then `Has Been Started` for 5 seconds | On screen |
| Readiness endpoint | Missing, as on a CCTV release before D-051 | Every check fails, so the display stays on `Cannot Reach` | On screen |
| Readiness endpoint | Health thread stuck or dead | After 7.5 seconds with no result, `Cannot Reach` | On screen, in the log |
| Kafka | All brokers down | `Cannot Reach` as soon as librdkafka reports it, about 5 seconds after the broker stopped | On screen, in the log |
| Kafka | Port open but failing | After 3 failed metadata checks, `Cannot Reach` | On screen, in the log |
| Kafka | Slow | Rows appear late | Nobody is told |
| Kafka | Topic missing or wrong `KAFKA_TOPIC` | After 3 failed checks, `Cannot Reach`, and the check repeats every 2 seconds | On screen, in the log |
| Settings | `KAFKA_PARTITIONS` names a partition that does not exist | A warning is logged at every check, and it is picked up when it appears | Log only |
| Settings | `IP_ADDRESS` not set | The default `127.0.0.1` is used. In a container that is the container itself, so `Cannot Reach` shows | On screen, in the log |
| Settings | `HEALTH_URL` not an `http` or `https` URL | The app stops at startup with a clear error | The container exits |
| Settings | `KAFKA_PARTITIONS` is not whole numbers | The app stops at startup with a clear error | The container exits |
| Camera | Lighting or stream problem | The CCTV producer sends an `ERROR` control message, scrolled in the marquee as an amber chip until that camera's `SUCCESS`, or 180 seconds after the last repeat | On screen, in the strip |
| Camera | Added while running | Its partition is picked up within 60 seconds, and anything it sent before then is not shown | Log: `Reading <topic> partitions [...]` |
| Image server | Unknown person ID or missing image | A placeholder JPEG, or a coloured silhouette once the server sends `null`, shows under the ID, with no error | A person spotting the placeholder |
| Image server | Hung, port open | Each message waits 5 seconds, then shows the current IDs with silhouettes, and the consumer falls behind | Log, and silhouettes on screen |
| Image server | Down, non-200 or bad JSON | The current IDs show with silhouettes and no names, never the rows from before | Silhouettes on screen, in the log |
| Image server | Wrong `IMAGE_SERVER_ADDRESS` | Fails as above, or shows another client's faces if it points at a live server | Log, or a person spotting a wrong face |
| CDN | Unreachable at page load | The page script stops at its first line, so the content area stays empty and never updates | The screen shows no rows or status |
| Browser | Crashes or loses the socket | While the socket is down, the page shows red `Network Issue`, its own wording, because the page cannot tell the display server from the network. On reconnect or reload, it gets the screen as the server has it, including any status with the time it has left | The kiosk `Restart=on-abort` covers only aborts |
| Consumer thread | Dies | The watchdog restarts it within 10 seconds | Log |
| Consumer thread | Stuck in a call with no timeout | Display freezes. The watchdog only sees dead threads | Nobody is told |
| Disk | Full | Logging can fail. Logs are not strictly capped across months | Host monitoring only |

## Limits, first expected first

1. **Throughput.** Each pass polls for one message, then makes two blocking POSTs, for images and names. Readiness is one GET every 2 seconds in its own thread. On a slow link, sustained bursts will lag. No limit has been measured.
2. **Screens.** Every open page polls 10 times a second and receives all images each time, so traffic grows with each screen and image size.
3. **Queue size.** The display shows 4 people. In a busy moment, earlier rows are pushed off the screen before anyone reads them.
4. **Cameras per screen.** Every camera at a point now shares one consumer loop and 4 rows, so limits 1 and 3 arrive sooner as cameras are added. No limit has been measured.
5. **Timing.** `session.timeout.ms` and the other group settings have no effect with `assign()`, so do not tune them to fix lag.

## Known behaviour worth checking in simulation

- A person recognised while a panel shows is never shown, on purpose: it would come back as a stale row, for attendance that may not have been kept.
- The 3-in-a-row confirmation means an outage the producer reports shows about 6 seconds late, and the rows or idle screen stay up until then.
- Camera status lines are lost on a display restart, and return with the next `ERROR`. Through an outage they are still read and kept hidden, and they return with the rows. `CAMERA_ERROR_SECONDS` (180) assumes an inaccessible camera's CPU reopen takes under about 2 minutes, because its `ERROR` comes after a 30 second wait, a GPU probe of up to 30 seconds and that reopen, which has no fixed limit on the CCTV side.
- A `SUCCESS` missed while Kafka was out of reach leaves its amber chip for up to 180 seconds, until it drops off unrepeated.
- Through the proxy, `HEALTH_URL` needs a certificate the display trusts. An untrusted one reads as `Cannot Reach`.
- A page that loads during an outage showed the idle face guide before `v0.7.3`, because statuses were pushed once and not stored. The owner hit this during a CCTV deploy on 2026-11-03. From `v0.7.3` the server keeps the status in force and sends it to every page that connects. A page that loads during the 2 minute restart wait shows the full 2 minute bar from when it loaded.
- After a Kafka reconnect, messages published during the gap are never shown, because the new consumer starts at `OFFSET_END`.

## Simulation results (2026-11-06, readiness endpoint)

Simulated before release on a private stack: Kafka, the stub image and name server, a stub `/health` that can be switched, a producer, and headless Chrome logging the screen every 500 ms. The owner watched the second run live.

| Scenario | What happened | Time |
|---|---|---|
| Starting at display start | Green `Getting Started`, then `Has Been Started` 6 s after the stub said ready, then rows | 6 s |
| Unready | Red `Temporarily Down`, chips hidden, reasons logged | 5 s |
| Starting after unready | Green `Getting Started` | Within 1 s |
| Health check hangs past its timeout | Red `Cannot Reach` | 7 s |
| Health container stopped | First run: no panel at all, because a 3 second DNS failure overran the 2 second beat, a negative sleep raised, and the health thread died. Fixed before release, with a test, and the stale guard added. Second run: red `Cannot Reach` | 8 s |
| Image server stopped | The current people as silhouettes, with no names and no old rows. Photos and names returned with the server | Immediate |
| Kafka stopped | With 3 checks: the idle screen for 6 s, then `Cannot Reach` after 18 s. With the owner's choice of an immediate panel on all brokers down: the rows stayed until `Cannot Reach`, with no idle frame | 5 s |
| Kafka back | `Has Been Started`, then rows and chips | 5 s |
| Page refreshed during an outage | The same panel at once | Immediate |

## Simulation results (2026-11-03)

These ran against `v0.7.0`, before the readiness endpoint, so the port checks and the 2 minute wait in them are gone.

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
