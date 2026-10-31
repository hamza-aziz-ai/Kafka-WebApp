# HANDOVER

Read this first and update it last, every session. Newest entry goes on top. Then read [CONSTRAINTS.md](CONSTRAINTS.md).

## Latest session: 2026-10-31 (model: Claude Opus 5.5, `claude-opus-5-5`)

Done:
- Created `docs/` following the Defend What You Ship field guide: `CONSTRAINTS.md`, `FLOW.md`, `ARCHITECTURE.md`, `DATA.md`, `DECISIONS.md`, `ALTERNATIVES.md`, `THREAT.md`, `FAILURE.md`, `TESTING.md`, `ROLLBACK.md`. `defense/` briefs are not started, because they belong in the owner's own words.
- Rebuilt `README.md` with System Design and Solution Architecture, and moved the Raspberry Pi setup to `SETUP-RASPBERRY-PI.md`.
- Added `docs` to `.dockerignore`, because the existing `*.md` rule only matches the repo root and `COPY . .` would have put these files in the published image.
- Confirmed the display contract with the CCTV session and recorded it in `ARCHITECTURE.md`.

In progress:
- Open PRs, all waiting for CI. GitHub Actions is blocked until the quota resets on 2026-11-01 at 00:00 UTC (05:30 IST). The last release is `v0.1.2`. Green CI merges each PR and deletes its branch automatically.
- PR #5: `group.id` becomes `<topic>.display`, and `IMAGE_SERVER_ADDRESS` can be set from the environment (CCTV decision D-036, steps 1 and 2).
- PR #6: removes the `client_log` handler. Before the fix, an anonymous client could write to `client_console.log`.
- PR #7: 5 second timeout on image server calls. Before the fix, a hung server blocked the consumer thread.
- PR #8: removes the company logo, so the repo stays general. The redesign removes the last two logos.
- PR #9: the docs. The detailed docs describe the code with #5, #6 and #7 merged.
- Branch `feat/display-all-partitions`, with no PR yet: reads every partition, adds `KAFKA_PARTITIONS`, adds the per-camera status strip, adds `PROJECT_SERVICES` with new default ports for CCTV PR #62, and adds SASL login for CCTV decision D-036, step 3c, and redesigns the page in the Face-Dataset-Collector light theme with no logos. It is stacked on #5 to #9, so opening a PR now would merge all of them as one. Tested with `test_partitions.py` and on a private broker.

Broken or risky (found by reading the code, not reproduced or fixed yet):
- `cors_allowed_origins="*"` lets any origin connect.
- Anyone who can reach port 9090 can see the person IDs and face images on the display, because the page and the socket have no auth.
- Kafka consumers are dropped with `kafka_consumer = None` and never closed, on poll failure and on every outage path.
- Every consumer loop opens up to 7 TCP connections with the default list before it polls for one message, which is likely the first throughput limit.
- `socket.io.js` loads from `cdnjs.cloudflare.com`, so a kiosk with no internet gets no live updates.
- The app runs on the Flask development server.
- There are no automated tests of app behaviour. CI runs lint, gitleaks, an image build and an import smoke test only.
- Old commits and published images `v0.1.0` to `v0.1.2` still hold company names, logos and a work email. Removing them needs a history rewrite, which is the owner's decision.

Assumptions:
- Unverified: the 5 second timeout. The CCTV session expects tens of milliseconds but has not measured it.
- Verified by the CCTV session against `src/multicam_stream_producer.py`: `ERROR` is re-sent every `IP_CAM_REINIT_WAIT_DURATION` (30 seconds) while a camera is down (lines 314-317), and on every evaluation while it stays dark (around line 226). `SUCCESS` is sent once per recovery. So emptying `camera_status` on a consumer rebuild is safe.
- Verified: the Kafka and `/post_images` contract, by the CCTV session against its code on 2026-10-31.

Next:
- After 05:30 IST on 2026-11-01, rerun CI on PRs #5, #6 and #7 (`gh run rerun 41190876365`, `41192493079`, `41192556848`) and on PR #8 and the docs PR. Marking a PR ready does not start CI, and `automerge.yml` only sweeps when a CI run finishes. PR #4 is not a draft and will automerge if its rerun passes.
- After #5 to #9 merge: rebase the commits at the tip of `feat/display-all-partitions` on `main` with `git rebase --onto origin/main origin/docs/system-design`, dropping the local merge commits, then open its PR.
- Add `python test_partitions.py` to CI, which needs the owner's approval.
- Run the simulation in `FAILURE.md` and record real results and recovery times.
- Owner reviews `CONSTRAINTS.md`.

Do not touch:
- See [CONSTRAINTS.md](CONSTRAINTS.md).

Read first: `ARCHITECTURE.md`, `CONSTRAINTS.md`.
