# DECISIONS

One entry per meaningful decision, newest on top, with the rejected options. Rejected options with their revisit conditions are also collected in [ALTERNATIVES.md](ALTERNATIVES.md).

## D-007: A person message removes a control message from the queue

Date: 2026-10-31. Model: Claude Opus 5.5.
Context: With every camera on one display, one camera's `ERROR` stayed in the queue. The page shows a control message alone, so it hid the rows of every other camera until 3 more people pushed it out or 15 idle seconds passed.
Options: 1) drop control messages from the queue when a person message arrives 2) show control messages beside the rows 3) keep them out of the queue.
Chosen: 1.
Why: It is the smallest change that keeps recognition visible, and the page already handles the rest.
Trade-off: A camera error can be replaced by the next person on any camera, so it may show only briefly at a busy point.
Revisit if: Camera errors need to stay on screen alongside the rows, which needs a page change.

## D-006: Read every partition by default, with `KAFKA_PARTITIONS` to narrow it

Date: 2026-10-31. Model: Claude Opus 5.5. Default agreed between the owner and the CCTV session, relayed by that session.
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
