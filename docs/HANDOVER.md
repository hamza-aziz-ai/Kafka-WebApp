# HANDOVER

Read this first and update it last, every session. Newest entry goes on top. Then read [CONSTRAINTS.md](CONSTRAINTS.md).

## Latest session: 2026-11-02 (model: Claude Opus 5.5, `claude-opus-5-5`)

Done:
- Released `v0.3.1` (`95692b4`): PR #12 makes the page connect its socket to its own origin. The page hardcoded `http://`, so behind the CCTV stack's HTTPS proxy the browser blocked the socket and the display got no live updates. Tested in real headless Chrome over plain HTTP and through a throwaway Caddy proxy.
- Released `v0.4.0` (`d917cf0`): PR #13, at the owner's request, moves camera chips into a one-line marquee at the bottom of the screen (D-013). Tested in real headless Chrome with 12 failed cameras and 4 rows at 1920x1080 and 1024x600. The owner saw it running and liked it.
- The CCTV session deployed `v0.4.0` and reported the marquee scrolling real camera alerts and a live recognition reaching the signed-in HTTPS page through its real proxy, so the socket fix holds there.
- Released `v0.4.1` (`4891e0a`): PR #14 fixes the squeezed name. With one or two people, the single grid row took the full height, the portrait grew to about 729 px and the name fell to one letter per line. The grid now always has two rows, and the portrait and name are capped by width too. The card CSS was unchanged from `v0.3.0`, so this predates the marquee. All 16 landscape cases were clean in real headless Chrome.
- Sent the CCTV session all three releases with their digests.

- The CCTV session deployed `v0.4.1` in CCTV `v2.9.1` and reported a live recognition on the signed-in HTTPS page: one person, about 315 px portrait, the name on one line, clear of the header and marquee. `ROLLBACK.md`'s last known good is now `v0.4.1`.

Next:
- The first Next item below is done. The other items there still stand.

Risks added:
- Marquee limits are in D-013: a list change restarts the scroll, and a 5 second green chip may be only partly seen. It has not been run on real Raspberry Pi hardware.
- On a 1080x1920 portrait screen the name column is 120 px, IDs wrap more, and the `Just now` tag wraps to two lines. Text stays inside the card. No portrait kiosk is known to exist.
- One card sits top-left and leaves the rest of the screen empty. The owner saw this and approved it.

## Session: 2026-11-01 (model: Claude Opus 5.5, `claude-opus-5-5`)

Done:
- Released `v0.2.0` (`0c6df18`): PRs #4 to #9 merged on their first CI run. It holds the consumer group per topic, the `IMAGE_SERVER_ADDRESS` override, the `client_log` removal, the image server timeout, the company logo removal and these docs.
- Released `v0.3.0` (`c1f4d7f`): PR #10 merged on its first CI run. It reads every camera with `KAFKA_PARTITIONS` to narrow it, adds the per-camera status strip, `PROJECT_SERVICES` with new default ports, SASL login, the light theme redesign with no logos, `IP_ADDRESS` defaulting to `127.0.0.1`, and the `SERVER_IP_ADDRESS` rename.
- Sent the CCTV session the `v0.3.0` tag to pin in its compose file. The owner deleted the old `feat/display-all-partitions` branch.
- The merge plan, the rehearsal script and the test tools are kept outside the repo, in `Kafka-WebApp-merge-plan` beside it.
- New owner rule: Markdown-only chores go straight to `main` with a `docs:` subject and `[skip ci]`, which costs no Actions minutes. Everything else keeps a PR, and a PR's commits never carry `[skip ci]`, because automerge would stall.
- New owner rule: every image is tagged, past, present and future, and nobody handles `<none>` by hand.

In progress:
- The CCTV stack now runs `v0.3.0`, and its container is healthy. The CCTV session has not yet reported what the display shows, so `ROLLBACK.md` stays on `v0.1.2`.
- The CCTV session is automating removal of the `<none>` digest rows after each pull, including `kafka-webapp`, whose row returns on the next digest pull. Not confirmed until it survives a real deploy. Only remove a digest name while a running container uses the image: with no container, `docker rmi` deletes the whole image.

Broken or risky (found by reading the code, not reproduced or fixed yet):
- `cors_allowed_origins="*"` lets any origin connect.
- Anyone who can reach port 9090 can see the person IDs and face images on the display, because the page and the socket have no auth.
- Kafka consumers are dropped with `kafka_consumer = None` and never closed, on poll failure and on every outage path.
- Every consumer loop opens up to 7 TCP connections with the default list before it polls for one message, which is likely the first throughput limit.
- `socket.io.js` loads from `cdnjs.cloudflare.com`, so a kiosk with no internet gets no live updates.
- The app runs on the Flask development server.
- There are no automated tests of app behaviour. CI runs lint, gitleaks, an image build and an import smoke test only.
- Old commits and published images `v0.1.0` to `v0.1.2` still hold company names, logos and a work email. The owner decided not to rewrite history, so these stay.

Assumptions:
- Unverified: the 5 second timeout. The CCTV session expects tens of milliseconds but has not measured it.
- Verified by the CCTV session against `src/multicam_stream_producer.py`: `ERROR` is re-sent every `IP_CAM_REINIT_WAIT_DURATION` (30 seconds) while a camera is down (lines 314-317), and on every evaluation while it stays dark (around line 226). `SUCCESS` is sent once per recovery. So emptying `camera_status` on a consumer rebuild is safe.
- Verified: the Kafka and `/post_images` contract, by the CCTV session against its code on 2026-10-31.

Next:
- Get the CCTV session's report of what the live display shows on `v0.3.0`, then set `ROLLBACK.md`'s last known good release to `v0.3.0`.
- Add `python test_partitions.py` to CI, which needs the owner's approval.
- Run the simulation in `FAILURE.md` and record real results and recovery times, including the 5 second timeout and the light theme's contrast at a gate.
- Owner reviews `CONSTRAINTS.md`.

Do not touch:
- See [CONSTRAINTS.md](CONSTRAINTS.md).

Read first: `ARCHITECTURE.md`, `CONSTRAINTS.md`.

## Session: 2026-10-31 (model: Claude Opus 5.5, `claude-opus-5-5`)

Done:
- Created `docs/` following the Defend What You Ship field guide: `CONSTRAINTS.md`, `FLOW.md`, `ARCHITECTURE.md`, `DATA.md`, `DECISIONS.md`, `ALTERNATIVES.md`, `THREAT.md`, `FAILURE.md`, `TESTING.md`, `ROLLBACK.md`. `defense/` briefs are not started, because they belong in the owner's own words.
- Rebuilt `README.md` with System Design and Solution Architecture, and moved the Raspberry Pi setup to `SETUP-RASPBERRY-PI.md`.
- Added `docs` to `.dockerignore`, because the existing `*.md` rule only matches the repo root and `COPY . .` would have put these files in the published image.
- Confirmed the display contract with the CCTV session and recorded it in `ARCHITECTURE.md`.
