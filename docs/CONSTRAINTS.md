# CONSTRAINTS

Hard rules for any AI working in this repo. Stop and ask the owner before breaking one. Drafted on 2026-10-31 by Claude Opus 5.5 from the Defend What You Ship field guide and a read of the code; the owner has not reviewed it yet.

This project has no customers yet. The owner is simulating real-world conditions before going to market, so that timelines promised later can be met. Work to Lane B (customer-facing) standards anyway, because a rehearsal only counts if it matches the real thing: the display is built to show person IDs and face images at an attendance point, so correctness and safety come before speed.

## Never

- Never commit secrets, keys, tokens or `.env` contents. `SECRET_KEY` and `KAFKA_PASSWORD` come from the environment only.
- Never read or edit `.env` or other secrets files without asking.
- Never run production, deploy or release commands, or touch a live Kafka topic or the CCTV server, without asking.
- Never force push or rewrite history.
- Never disable or weaken CI checks, gitleaks or the image smoke test.
- Never write person IDs or face images to logs or to any new storage. This repo only passes them to the display.

## Always

- Always send the message queue to the image server with `json=`, because FastAPI rejects a body without the JSON `Content-Type` with a 422 (`post_message_queue_and_get_images`).
- Always keep `.github/workflows/` actions pinned to a commit SHA and the `Dockerfile` base image pinned to a digest.
- Always keep the container running as the non-root `app` user.
- Always use Conventional Commit PR titles, because they set the release version: `feat:` gives a minor release, `fix:` and `perf:` a patch, and `type!:` a major.
- Always make one behaviour change per request.
- Always render message text with `textContent`, never `innerHTML`, because person IDs come from outside this app (`templates/index.html`).

## Only with the owner's approval

- New dependencies. Add them to `Pipfile` with `~=`, run `pipenv lock`, copy the exact versions from `Pipfile.lock` into `requirements.txt`, and log why they are needed and who maintains them. Local tools go in `requirements-dev.txt`, never in the image.
- Changes to `Dockerfile`, `.dockerignore` or `.github/workflows/`.
- New network calls or external hosts.
- Adding or changing auth, CORS or the port the app listens on.
- Changes to the default ports, topic or addresses in `parameters.py`, because deployed displays and the CCTV services depend on them.
- Deleting files.

## Coupled code

- The "Getting Started" status text is duplicated. `_consume_messages` emits it and `templates/index.html:192` matches it exactly to hold the message for 2 minutes, so change both together.
- Kafka messages starting with `ERROR ` or `SUCCESS ` are camera control messages that go to `camera_status` (`update_camera_status`). This app shows what the CCTV system sends, so the [contract](ARCHITECTURE.md#contract-with-the-cctv-system) is shared: check with that project before changing it, and tell it about anything here that changes what is displayed.

## Keep it general

- Never add a real company, client or site name, logo or branding to this repo, including third-party vendor logos. Use `<client>` and `<location>_<point>` style placeholders, and generic examples.
