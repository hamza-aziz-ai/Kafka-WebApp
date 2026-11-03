# TESTING: how changes are verified

A change is done only when the checks below pass and their real output is pasted in the PR. Saying tests pass is not enough.

## CI

These run on every PR and on any push to `main` without `[skip ci]`. The `ci` job must pass for automerge. GitHub bills each job rounded up to a whole minute, and the account's minutes are shared across repos, so the quick checks share the `ci` job instead of each taking a job of their own.

| Job | Proves | Pass looks like |
|---|---|---|
| `image` (amd64, arm64) | The image builds, `pip check` passes, and settings load from the environment. On amd64, `test_partitions.py` also runs inside the image | The smoke test exits 0, and each check prints `PASS` |
| `ci` | `ruff` with `E9,F63,F7,F82` (syntax errors and undefined names only), gitleaks finds no secrets in history, and both image jobs passed | `All checks passed!`, no leaks found, and the job passes |

A release does not run CI again. It builds the amd64 image of the merged commit, runs `pip check` and `test_partitions.py` in it, then pushes that build with arm64 added. CI does not start the app against real services.

## Local checks

Run from the repo root with the project's dependencies installed (`pipenv install`). Set `IP_ADDRESS` to `192.0.2.1`, a reserved address that reaches nothing, so that a check cannot touch a real host. Do not use `127.0.0.1`, because a CCTV stack may be running on the same machine. Importing `app` starts its threads, so end with `os._exit`.

Partition selection and the message queue:

```bash
python test_partitions.py
```

Each check prints `PASS`. CI and the release run this file inside the amd64 image. `assign_new_partitions` was also run against a private `cp-kafka` broker: reading every partition, `KAFKA_PARTITIONS`, two displays in one group, and a partition added while running. The login was run against a private SCRAM broker too: the right password reads, and a wrong password or no login is refused. Never run such a test against a live broker.

Settings:

```bash
IP_ADDRESS=192.0.2.1 KAFKA_TOPIC=acme.maingate_entry python -c "import os, app; assert app.kafka_conf['group.id'] == 'acme.maingate_entry.display'; print('PASS', flush=True); os._exit(0)"
IP_ADDRESS=192.0.2.1 python -c "import os, app; assert app.kafka_conf['group.id'] == 'maingate_entry.display'; print('PASS', flush=True); os._exit(0)"
IP_ADDRESS=192.0.2.1 python -c "import os, app; assert app.IMAGE_SERVER_ADDRESS == 'http://192.0.2.1:20013/post_images'; print('PASS', flush=True); os._exit(0)"
IP_ADDRESS=192.0.2.1 IMAGE_SERVER_ADDRESS=http://display-image-server:20013/post_images python -c "import os, app; assert app.IMAGE_SERVER_ADDRESS == 'http://display-image-server:20013/post_images'; print('PASS', flush=True); os._exit(0)"
```

Each prints `PASS`.

No anonymous write to disk:

```python
import os, app
c = app.socketio.test_client(app.app)
c.emit('client_log', {'message': 'x'})
assert not os.path.exists('client_console.log')
print('PASS', flush=True); os._exit(0)
```

A hung image server cannot freeze the display:

```python
import os, socket, threading, time, app
srv = socket.socket(); srv.bind(('127.0.0.1', 0)); srv.listen(5)
app.IMAGE_SERVER_ADDRESS = f'http://127.0.0.1:{srv.getsockname()[1]}/post_images'  # accepts, never answers
t = threading.Thread(target=app.post_message_queue_and_get_images, args=({'messages': ['x']},), daemon=True)
t.start(); t.join(12)
assert not t.is_alive()
print('PASS', flush=True); os._exit(0)
```

It should return after about 5 seconds and log `Read timed out`.

## Manual path

There is no automated end-to-end test, so walk this by hand against a CCTV host before a release that changes behaviour:

1. Start the app with `IP_ADDRESS` set, and open `http://<host>:9090`.
2. Stand in front of each camera at the point in turn. Your person ID and face appear for each, and they clear after about 15 seconds of no activity.
3. Show one person, then four, including a long ID with underscores, such as `maingate_visitor_0000123456789_01`. The portrait stays small and the name stays beside it. A short name such as `hamza_aziz` stays on one line, shrinking if it must, and a long one breaks only after an underscore. A person registered with a name shows it in smaller text under the ID, and a person without one shows the ID alone.
4. Block one camera. An amber chip for it scrolls along the bottom of the screen and stays, while the other cameras' cards keep working. Restore it, and a green chip shows for 5 seconds.
5. Stop one service on the CCTV host. A red `Temporarily Down` panel appears.
6. Start it again. A green `Getting Started` panel with a progress bar shows for 2 minutes, and recognition then resumes.
7. Unplug the network. A red `Network Issue` panel appears. Reconnect, and it recovers.

Use [FAILURE.md](FAILURE.md) for the wider simulation.
