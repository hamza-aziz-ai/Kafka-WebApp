# DECISIONS

One entry per meaningful decision, newest on top, with the rejected options. Rejected options with their revisit conditions are also collected in [ALTERNATIVES.md](ALTERNATIVES.md).

## D-012: Rename `DGX_IP_ADDRESS` to `SERVER_IP_ADDRESS`

Date: 2026-10-31. Model: Claude Opus 5.5. Chosen by the owner.
Context: The name tied the code to one kind of hardware, and the repo should stay general.
Options: 1) `SERVER_IP_ADDRESS` 2) keep the name.
Chosen: 1. It is an internal name only, so the `IP_ADDRESS` environment variable stays the same and no deployment changes.
Why: Any machine can run these services.
Trade-off: None for deployments. The owner also decided not to rewrite git history, so old commits keep the old name.
Revisit if: Never expected.

## D-011: Default `IP_ADDRESS` to `127.0.0.1`

Date: 2026-10-31. Model: Claude Opus 5.5. Chosen by the owner.
Context: The old default looked like a real site address, and the owner wants the repo kept general.
Options: 1) `127.0.0.1` 2) require `IP_ADDRESS`, and stop at startup without it 3) keep the old default.
Chosen: 1.
Why: It reveals nothing about any site, and it suits a display on the same machine as the CCTV stack.
Trade-off: A display that relied on the old default now shows `Network Issue` until `IP_ADDRESS` is set. In a container `127.0.0.1` is the container itself, so Docker runs must set it.
Revisit if: A missing `IP_ADDRESS` causes a real outage, and then require it.

## D-010: Use the Face-Dataset-Collector light theme, with no logos

Date: 2026-10-31. Model: Claude Opus 5.5. Chosen by the owner from a design comparing the app's dark capture theme and light staff theme.
Context: The old page was a blue gradient with stacked rows and third-party logos, and the display should look like part of one product with the collector app.
Options: 1) light staff theme 2) dark capture theme 3) keep the old page.
Chosen: 1, with person cards in a 2 by 2 grid, no logos, and IBM Plex Sans bundled.
Why: The owner picked it, and it matches the collector's main screens. Removing logos fits keeping the repo general. Bundling the font keeps a kiosk's look the same with no internet.
Trade-off: Dark would read better in a dim space, so check the contrast at a real gate. The three font files add about 600 KB to the image.
Revisit if: The light theme is hard to read at a gate in real conditions.

## D-009: Turn on Kafka SASL by setting `KAFKA_USERNAME`

Date: 2026-10-31. Model: Claude Opus 5.5. Design from the CCTV session, for decision D-036, step 3c.
Context: Step 3c requires a login on Kafka, and without one the display loses access.
Options: 1) SASL when `KAFKA_USERNAME` is set 2) a separate on/off setting 3) ship it only at cutover.
Chosen: 1, with `SCRAM-SHA-512`, and `KAFKA_SECURITY_PROTOCOL` for a later `SASL_SSL`.
Why: The image can ship before step 3c, and each display switches by setting its variables at cutover.
Trade-off: `SASL_PLAINTEXT` authenticates without encrypting, so Kafka traffic stays readable on the network.
Revisit if: The broker moves to `SASL_SSL`, which may need a CA setting too.

## D-008: Add `PROJECT_SERVICES` for health checks, and change the default ports

Date: 2026-10-31. Model: Claude Opus 5.5. Requested by the CCTV session with the owner's approval, for CCTV PR #62.
Context: CCTV PR #62 stops publishing the database controller (20001) and feedback controller (20012) on the host, so a port check on `IP_ADDRESS` would show a failure forever.
Options: 1) a new variable taking `host:port` or a bare port 2) `host:port` syntax in `PROJECT_SERVICES_PORT` 3) drop those two checks everywhere.
Chosen: 1, and the default list becomes `6385,19000,19530,9092,9090,20013`. It drops 20001 and 20012, adds 9090, the reporting API, as proof that the app stack is up, and drops Redis (6381) and Face Liveness (6386, 6970, 6971), because the CCTV side confirmed that nothing starts Redis and liveness is off. It also drops 19001, because one InsightFace container now serves on 19000 only, and a listed port that never opens stops the display reading Kafka.
Why: The old image runs `int(p)` on `PROJECT_SERVICES_PORT`, so it would crash on `host:port`, and CCTV's compose sets the variable before the new image is pinned. Old images ignore a new name.
Trade-off: Two variables do a similar job. A display using the defaults no longer checks the two controllers, a host running liveness or a second InsightFace container must add its ports through `PROJECT_SERVICES`, and a service name adds a DNS lookup to each probe, which the 0.5 second timeout does not cover.
Revisit if: No deployment uses `PROJECT_SERVICES_PORT` any more, so it can go.

## D-007: Show camera problems in a separate status strip

Date: 2026-10-31. Model: Claude Opus 5.5. Recommended by the CCTV session.
Context: With every camera on one display, one camera's `ERROR` in the queue hid every other camera's rows, because the page shows a control message alone. Clearing it on the next person made it vanish in a second at a busy gate, so nobody would fix the camera.
Options: 1) a per-camera strip, separate from the rows 2) clear the error on the next person 3) keep the error in the queue.
Chosen: 1, under the header, above the person cards, because four rows fill the main area and a strip under them overlapped the last row.
Why: Person rows and camera health stop competing for the same space, and an error stays until that camera recovers.
Trade-off: The strip is in memory and is emptied when the consumer is rebuilt, so it relies on the producer re-sending `ERROR`, which the CCTV side confirmed it does. More than about 3 lines at once run into the banner below.
Revisit if: The producer stops re-sending, or a point has enough cameras to fill the strip.

## D-006: Read every partition by default, with `KAFKA_PARTITIONS` to narrow it

Date: 2026-10-31. Model: Claude Opus 5.5. Default confirmed by the owner on 2026-10-31.
Context: Each camera has its own partition, and the display read partition 0 only, so it showed the first camera at a point. The owner cannot predict whether future clients want one screen per entry point or per camera.
Options: 1) every partition, with an optional list 2) keep partition 0 3) `subscribe()` with one group per display.
Chosen: 1, with a check every 60 seconds using `incremental_assign`.
Why: It works for both layouts. `subscribe()` would split partitions between displays that share a group.
Trade-off: Messages on a new camera's partition are missed until the next check, so for up to 60 seconds, because new partitions start at `OFFSET_END`. Each check blocks the consumer briefly, for up to 5 seconds if the broker is failing.
Revisit if: Losing a new camera's first minute matters, or the check slows a busy display.

## D-005: Write the docs against the merged state of open PRs

Date: 2026-10-31. Model: Claude Opus 5.5.
Context: The owner wanted docs without waiting for GitHub Actions, and PRs #5, #6 and #7 were blocked by the CI quota.
Options: 1) wait for the merges 2) document `main` as it is 3) document the merged state in a docs-only PR.
Chosen: 3.
Why: Option 1 stalls until CI returns, and option 2 goes stale within a day. A merged test tree let every fact be checked.
Trade-off: Until the three PRs merge, the docs describe code that is not on `main` yet. Each detailed doc states this at the top.
Revisit if: One of the three PRs changes or is dropped, because the docs must then change too.

## D-004: Delete the `client_log` handler (PR #6)

Date: 2026-10-31. Model: Claude Opus 5.5.
Context: Any client could append unlimited text to `client_console.log`.
Options: 1) delete it 2) add a size limit and auth 3) leave it.
Chosen: 1.
Why: The page never emits `client_log` and nothing else refers to it, so limits would protect code nobody uses.
Trade-off: There is no way to collect browser console output. Nothing used it.
Revisit if: Kiosk browser errors need collecting, and then it needs auth and a size limit.

## D-003: 5 second timeout on image server calls (PR #7)

Date: 2026-10-31. Model: Claude Opus 5.5.
Context: A hung image server blocked the consumer thread forever, and neither the port probe nor the watchdog could detect it.
Options: 1) fixed `timeout` on `requests.post` 2) post from a separate worker thread 3) make the watchdog detect stuck threads.
Chosen: 1, with `IMAGE_SERVER_TIMEOUT = 5` in `parameters.py`.
Why: It is one argument, and the existing `except` already logs and carries on.
Trade-off: A hang costs up to 5 seconds per message, so the consumer falls behind. 5 seconds is an assumption: the CCTV side expects tens of milliseconds (up to 4 disk reads and JPEG encodes) but has not measured it.
Revisit if: Simulation measures real latency near 5 seconds, or falling behind during a hang matters.

## D-002: Make `IMAGE_SERVER_ADDRESS` configurable (PR #5)

Date: 2026-10-31. Model: Claude Opus 5.5. Requested by the CCTV session, decision D-036, step 2.
Context: On a central host, each client's image server has its own port, and a display on the fixed `:20013` would show another client's faces.
Options: 1) environment override with today's value as default 2) separate host and port settings 3) keep the fixed address.
Chosen: 1.
Why: A full URL also fits a service name such as `http://display-image-server:20013/post_images`, and existing deployments keep working.
Trade-off: A variable that is set but empty does not fall back to the default. The CCTV compose always sets a literal.
Revisit if: A deployment needs empty to mean default.

## D-001: Derive the consumer group from the topic (PR #5)

Date: 2026-10-31. Model: Claude Opus 5.5. Requested by the CCTV session, decision D-036, step 1.
Context: Kafka access will be granted by `<client>.` prefix, covering topics and groups, and a fixed group sat outside every prefix.
Options: 1) `<topic>.display` 2) a separate group variable 3) keep the fixed name.
Chosen: 1.
Why: It needs no extra setting and cannot drift from the topic. The rename loses nothing, because the consumer uses `assign()` at `OFFSET_END` and never reads committed offsets.
Trade-off: Two displays on the same topic share a group. That is harmless, because `assign()` skips group balancing.
Revisit if: The consumer moves to `subscribe()`.
