# HANDOVER

Read this first and update it last, every session. Newest entry goes on top. Then read [CONSTRAINTS.md](CONSTRAINTS.md).

## Latest session: 2026-11-03 (model: Claude Opus 5.5, `claude-opus-5-5`)

Done:
- `v0.4.2` is the last known good release, approved by the owner after the CCTV session checked long names on the owner's display.
- Recorded in the contract with CCTV that its sign-in and `/api/live` pages load our `static/fonts/` files, and `test_partitions.py` checks that those files are served.
- The owner decided the display shows the person's name under the ID (D-015). CCTV added `POST /post_names` in its PR #104, for its `v2.12.0` release. This side is built and tested: 8 stub cases, plus real headless Chrome at 2000x1125, 1920x1080, 1024x600 and 1080x1920 with 1 to 4 cards.
- New owner rule: save Actions minutes across all three repos (D-016). CI now bills 3 jobs per PR instead of 5, and a release tests the amd64 image it ships instead of running CI again. `test_partitions.py` now runs in CI. Checked locally with actionlint, and by building the image and running both CI steps in it. The owner chose to ship the names feature and these CI changes in one `feat:` PR.

Next:
- Done: PR #17 merged on its first CI run, which billed 3 jobs. The new `release.yml` published `v0.5.0` (digest `sha256:e2bee2a5...`, amd64 and arm64), with jobs billed about 4 minutes instead of about 11. The `automerge` run sent the CCTV dispatch without a warning, and CCTV's `submodules` run passed.
- Send the CCTV session the `v0.5.0` digest, wait for its live report with real names, then set `ROLLBACK.md`'s last known good to `v0.5.0`.

Risks added:
- A hung name server adds up to 5 seconds per message to the consumer loop, although the faces show first. Names are new personal data on a page with no auth. CCTV publishes 9090 on 127.0.0.1 behind its sign-in proxy (CCTV D-040), and a Raspberry Pi display is the open case.
- The release no longer re-runs lint, gitleaks or the arm64 smoke test on the merged commit (D-016).

## Session: 2026-11-02 (model: Claude Opus 5.5, `claude-opus-5-5`)

Done:
- Released `v0.3.1` (`95692b4`): PR #12 makes the page connect its socket to its own origin. The page hardcoded `http://`, so behind the CCTV stack's HTTPS proxy the browser blocked the socket and the display got no live updates. Tested in real headless Chrome over plain HTTP and through a throwaway Caddy proxy.
- Released `v0.4.0` (`d917cf0`): PR #13, at the owner's request, moves camera chips into a one-line marquee at the bottom of the screen (D-013). Tested in real headless Chrome with 12 failed cameras and 4 rows at 1920x1080 and 1024x600. The owner saw it running and liked it.
- The CCTV session deployed `v0.4.0` and reported the marquee scrolling real camera alerts and a live recognition reaching the signed-in HTTPS page through its real proxy, so the socket fix holds there.
- Released `v0.4.1` (`4891e0a`): PR #14 fixes the squeezed name. With one or two people, the single grid row took the full height, the portrait grew to about 729 px and the name fell to one letter per line. The grid now always has two rows, and the portrait and name are capped by width too. The card CSS was unchanged from `v0.3.0`, so this predates the marquee. All 16 landscape cases were clean in real headless Chrome.
- Sent the CCTV session all three releases with their digests.
- The CCTV session deployed `v0.4.1` in CCTV `v2.9.1` and reported a live recognition on the signed-in HTTPS page: one person, about 315 px portrait, the name on one line, clear of the header and marquee. `ROLLBACK.md`'s last known good is now `v0.4.1`.
- Released `v0.4.2` (`0e6623a`): PR #15, which the owner asked for through the CCTV session, stops a name breaking mid-word, such as `hamza_aziz` showing as "hamza_azi" and "z". The page shrinks each name to fit one line, down to half size, then wraps only after an underscore (D-014). Tested in real headless Chrome at 2000x1125, 1920x1080, 1024x600 and 1080x1920 with 1 to 4 cards, before and after release. Sent the CCTV session the tag and digest `sha256:a5864d54...`.
- Fixed `ROLLBACK.md` after a code review: its `docker run` set only `IP_ADDRESS`, which would drop the Kafka login, topic and image server. It now carries the old container's variables over and says the CCTV stack rolls back by changing its compose pin.
- Merged PR #16 (`e8b6d97`, `ci:`, no release), which the owner asked for through the CCTV session: after each merge, `automerge.yml` sends the CCTV repo a `submodule-main` dispatch so its submodule pin follows `main`. The owner has saved `SUBMODULES_TOKEN`. PR #16's own merge ran the older `automerge.yml`, so the next merge is the first to send it.

Next:
- The first Next item below is done. The other items there still stand.
- Done 2026-11-03: CCTV `v2.9.3` runs `v0.4.2` on the laptop. The CCTV session checked dummy long names on the owner's display, and the owner approved `v0.4.2` as last known good, so `ROLLBACK.md` now says `v0.4.2`.
- On the next merge, check that the `automerge` run sends the CCTV dispatch without a warning.

Risks added:
- Marquee limits are in D-013: a list change restarts the scroll, and a 5 second green chip may be only partly seen. It has not been run on real Raspberry Pi hardware.
- On a 1080x1920 portrait screen the name column is 120 px, IDs wrap more, and the `Just now` tag wraps to two lines. Text stays inside the card. No portrait kiosk is known to exist.
- One card sits top-left and leaves the rest of the screen empty. The owner saw this and approved it.
- D-014 limits: names in one grid can show at different sizes, and on a 1080x1920 portrait screen, or with an ID that has no underscores, a long part still breaks mid-word.

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
