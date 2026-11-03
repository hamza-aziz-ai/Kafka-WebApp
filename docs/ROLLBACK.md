# ROLLBACK: the way back out

**Last known good release:** `v0.4.2`, commit `0e6623a`. Update this line after each release proves itself on a real display.

## Roll back a release

Images carry version tags only, so go back by running the previous tag.

On the CCTV stack, the display runs from the CCTV compose file. Change its `kafka-webapp` pin (`name:tag@sha256:...`) to the previous release and redeploy through the CCTV session. Do not use `docker run` there.

On a standalone display, keep its settings. A new container started with only `IP_ADDRESS` loses the Kafka login, topic, partitions, services and image server, and on a shared host the default image server can show another client's faces. Save the old container's variables first, then start the previous tag with the same variables, port and network (`docker inspect <container>` shows them):

```bash
docker inspect <container> --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -vE '^(PATH|LANG|GPG_KEY|PYTHON_[A-Z0-9_]*)=' > display.env
docker pull ghcr.io/hamza-aziz-ai/kafka-webapp:v0.4.2
docker stop <container>
docker rm <container>
docker run -d --name <container> --restart unless-stopped -p 9090:9090 --env-file display.env ghcr.io/hamza-aziz-ai/kafka-webapp:v0.4.2
rm display.env
```

`display.env` holds `KAFKA_PASSWORD` and `SECRET_KEY` when they are set, so keep it off shared folders and delete it afterwards.

Removing the container loses its `logs/`, because the image has no volume for them, so copy them out first if you need them.

Check it afterwards: the page loads, a recognition shows a person ID and face, and the log has no new errors.

## Undo a merged change

Open a PR that reverts the squash commit, using a `fix:` title so that a new release is cut. Never rewrite `main`.

## Change by change

| Change | To undo | Data impact |
|---|---|---|
| Consumer group rename (PR #5) | Revert | None. The old group's offsets were never read |
| `IMAGE_SERVER_ADDRESS` override (PR #5) | Revert, and remove the variable from deployments | None. The CCTV in-stack display depends on it, so tell that session first |
| `client_log` removal (PR #6) | Revert | None. This also reopens the disk write risk |
| `PROJECT_SERVICES` and new default ports | Revert. Older images ignore `PROJECT_SERVICES`, and with CCTV PR #62 their default list would fail on 20001 and 20012, so set `PROJECT_SERVICES_PORT` | None |
| Default `IP_ADDRESS` of `127.0.0.1` | Set `IP_ADDRESS` on the display, with no deploy needed | None |
| Read every partition | Set `KAFKA_PARTITIONS=0` to go back to the first camera without a deploy, or revert | None |
| Image server timeout (PR #7) | Revert, or raise `IMAGE_SERVER_TIMEOUT` in `parameters.py` if the 5 seconds is the problem. It is a constant, not a variable, so it needs a release | None |
| Same-origin socket (PR #12) | Do not go below `v0.3.1` on an HTTPS display: older pages get no live updates behind a proxy | None |
| Name fit (PR #15) | Revert. Older pages break a long name mid-word | None |

This app has no database and no migrations, so a rollback never touches stored data. Topic names and the `/post_images` contract are shared with the CCTV system, so check with it before rolling back anything that touches them.
