"""Checks partition, queue, camera status, readiness, screen and Kafka login logic without a broker. Run: python test_partitions.py"""
import json
import os
import subprocess
import sys
import threading
import time

os.environ["IP_ADDRESS"] = "192.0.2.1"  # reserved, reaches nothing, so app's threads cannot touch a real Kafka
os.environ.pop("KAFKA_PARTITIONS", None)

# app starts its threads on import, for flask run on a Raspberry Pi; hold them back, so each check drives one pass itself
_start = threading.Thread.start
threading.Thread.start = lambda self: None
import app
threading.Thread.start = _start
import parameters
from types import SimpleNamespace


class FakeConsumer:
    def __init__(self, partitions, error=None):
        self.partitions, self.error, self.assigned = partitions, error, []

    def list_topics(self, topic, timeout):
        return SimpleNamespace(topics={topic: SimpleNamespace(error=self.error, partitions=dict.fromkeys(self.partitions))})

    def incremental_assign(self, partitions):
        self.assigned.append([(p.partition, p.offset) for p in partitions])


class FakeKafka(FakeConsumer):
    """A consumer for consume_once: messages to hand out one per poll, and a broker that can go away"""
    def __init__(self, partitions=(0,), messages=()):
        super().__init__(list(partitions))
        self.messages, self.down = list(messages), False

    def list_topics(self, topic, timeout):
        if self.down:
            raise app.requests.ConnectionError("broker down")  # Any exception; confluent_kafka raises KafkaException
        return super().list_topics(topic, timeout)

    def poll(self, timeout):
        value = self.messages.pop(0) if self.messages else None
        return None if value is None else SimpleNamespace(error=lambda: None, value=lambda: value.encode())

    def close(self):
        pass


def reset_screen(system=None, kafka=None):
    app.system_state, app.health_streak, app.health_seen = system, 0, time.monotonic()
    app.kafka_state, app.kafka_failures, app.kafka_suspect = kafka, 0, False
    app.shown = app.CONNECTING
    app.display_status = {"level": app.CONNECTING[0], "text": app.CONNECTING[1], "until": None}
    for items in (app.message_queue, app.messages_to_display, app.names_to_display, app.assigned_partitions):
        items.clear()
    app.next_partition_check = 0
    app.last_update_time = time.time()


def screen():
    return app.current_state()["message"]


def check_parse():
    parse = parameters._parse_partitions
    assert parse("") == [] and parse("  ") == []
    assert parse("0") == [0] and parse("3, 0,3") == [0, 3]
    for bad in ("a", "-1", "1.5", "0,", ",0", "0,,3", "²"):
        try:
            parse(bad)
        except SystemExit as e:
            assert "KAFKA_PARTITIONS" in str(e) and repr(bad) in str(e)
        else:
            raise AssertionError(f"accepted {bad!r}")


def check_assign():
    end = app.OFFSET_END

    # Default: every partition, then only partitions added later
    app.KAFKA_PARTITIONS = []
    app.assigned_partitions.clear()
    consumer = FakeConsumer([0, 1])
    app.assign_new_partitions(consumer)
    consumer.partitions = [0, 1, 2]
    app.assign_new_partitions(consumer)
    app.assign_new_partitions(consumer)
    assert consumer.assigned == [[(0, end), (1, end)], [(2, end)]], consumer.assigned

    # KAFKA_PARTITIONS: only those, and a missing one is picked up when it appears
    app.KAFKA_PARTITIONS = [0, 3]
    app.assigned_partitions.clear()
    consumer = FakeConsumer([0, 1])
    app.assign_new_partitions(consumer)
    consumer.partitions = [0, 1, 2, 3]
    app.assign_new_partitions(consumer)
    assert consumer.assigned == [[(0, end)], [(3, end)]], consumer.assigned

    # Topic error: nothing assigned
    app.assigned_partitions.clear()
    consumer = FakeConsumer([], error="UNKNOWN_TOPIC_OR_PART")
    app.assign_new_partitions(consumer)
    assert consumer.assigned == []


def check_queue():
    app.message_queue = []
    for message in ("a", "b", "a", "c", "d", "e"):
        app.update_message_queue(message)
    assert app.message_queue == ["e", "d", "c", "b"], app.message_queue


def check_camera_status():
    app.camera_status.clear()
    app.update_camera_status("ERROR gate_a_cam2: Camera stream from gate_a_cam2 is not accessible.")
    app.update_camera_status("ERROR gate_a_cam1: Inadequate lighting condition detected at gate_a_cam1.")
    assert app.current_camera_status() == [
        {"level": "ERROR", "text": "Inadequate lighting condition detected at gate_a_cam1."},
        {"level": "ERROR", "text": "Camera stream from gate_a_cam2 is not accessible."},
    ], app.current_camera_status()

    # A SUCCESS replaces only its own camera's error, then expires
    app.CAMERA_SUCCESS_SECONDS = 0.2
    app.update_camera_status("SUCCESS gate_a_cam2: Camera gate_a_cam2 reconnection successful")
    assert app.current_camera_status()[1] == {"level": "SUCCESS", "text": "Camera gate_a_cam2 reconnection successful"}
    time.sleep(0.3)
    assert [status["level"] for status in app.current_camera_status()] == ["ERROR"]

    # A message with no ": " still shows
    app.camera_status.clear()
    app.update_camera_status("ERROR gate_a_cam3")
    assert app.current_camera_status() == [{"level": "ERROR", "text": "gate_a_cam3"}]

    # An ERROR lasts while the producer repeats it, and drops off when it stops, as when its SUCCESS was missed in an outage
    original = app.CAMERA_ERROR_SECONDS
    app.CAMERA_ERROR_SECONDS = 0.3
    try:
        app.update_camera_status("ERROR gate_a_cam4: Camera stream from gate_a_cam4 is not accessible.")
        time.sleep(0.2)
        app.update_camera_status("ERROR gate_a_cam4: Camera stream from gate_a_cam4 is not accessible.")
        time.sleep(0.2)
        assert any(s["text"].startswith("Camera stream from gate_a_cam4") for s in app.current_camera_status()), "a repeat must renew it"
        time.sleep(0.2)
        assert not any(s["text"].startswith("Camera stream from gate_a_cam4") for s in app.current_camera_status()), "it must drop off"
    finally:
        app.CAMERA_ERROR_SECONDS = original


def check_startup_rejects_bad_value():
    env = {**os.environ, "KAFKA_PARTITIONS": "0,x"}
    result = subprocess.run([sys.executable, "-c", "import parameters"], env=env, capture_output=True, text=True)
    assert result.returncode != 0 and "KAFKA_PARTITIONS must be" in result.stderr, result


def check_default_address():
    env = {key: value for key, value in os.environ.items() if key != "IP_ADDRESS"}
    code = "import parameters as p; print(p.SERVER_IP_ADDRESS)"
    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert result.stdout.strip() == "127.0.0.1", result


def security_conf(**env):
    full_env = {key: value for key, value in os.environ.items() if not key.startswith("KAFKA_")}
    code = "import json, parameters as p; print(json.dumps(p.KAFKA_SECURITY_CONF))"
    return subprocess.run([sys.executable, "-c", code], env={**full_env, **env}, capture_output=True, text=True)


def check_page():
    response = app.app.test_client().get("/")
    page = response.get_data(as_text=True)
    assert response.status_code == 200
    assert parameters.DISPLAY_LOCATION in page
    assert "static/img" not in page and "nvidia" not in page.lower()
    assert page.index('id="content"') < page.index('id="camera-status"')
    for font in ("regular", "medium", "semibold"):
        assert f"fonts/ibm_plex_sans_{font}.ttf" in page
        assert app.app.test_client().get(f"/static/fonts/ibm_plex_sans_{font}.ttf").status_code == 200
    assert "Please Wait for 2 Minutes" not in page, "starting has no fixed length any more"


def check_kafka_login():
    assert json.loads(security_conf().stdout) == {"security.protocol": "PLAINTEXT"}
    assert json.loads(security_conf(KAFKA_USERNAME="acme_display", KAFKA_PASSWORD="x").stdout) == {
        "security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "SCRAM-SHA-512", "sasl.username": "acme_display", "sasl.password": "x"}
    assert json.loads(security_conf(KAFKA_USERNAME="a", KAFKA_PASSWORD="x", KAFKA_SECURITY_PROTOCOL="SASL_SSL").stdout)["security.protocol"] == "SASL_SSL"
    missing_password = security_conf(KAFKA_USERNAME="a")
    assert missing_password.returncode != 0 and "KAFKA_PASSWORD must be set" in missing_password.stderr
    assert app.kafka_conf["security.protocol"] == "PLAINTEXT"


def check_names():
    urls = []

    def names_for(reply):
        def post(url, json, timeout):
            urls.append(url)
            if isinstance(reply, Exception):
                raise reply
            return reply
        original = app.requests.post
        app.requests.post, app.names_to_display = post, {"stale": "x"}
        try:
            app.post_message_queue_and_get_names({"messages": ["a", "b", "c"]})
        finally:
            app.requests.post = original
        return app.names_to_display

    assert names_for(Reply(200, {"a": "Ann Lee", "b": None, "c": ""})) == {"a": "Ann Lee"}
    # An older CCTV release (404), a failing server, a bad body or a hang all leave the ID alone on screen
    for bad in (Reply(404), Reply(500), Reply(200, ["a"]), Reply(200, {"a": 5}), Reply(200, ValueError("bad")), app.requests.Timeout("t")):
        assert names_for(bad) == {}, bad
    assert set(urls) == {parameters.NAME_SERVER_ADDRESS} and parameters.NAME_SERVER_ADDRESS.endswith(":20013/post_names"), urls


def check_names_cleared_when_idle():
    # A name must not outlive its card: the consumer thread clears the rows after STATUS_THRESHOLD idle seconds
    reset_screen("ready", True)
    app.shown, app.display_status = None, None
    app.kafka_consumer, app.next_partition_check = FakeKafka(), float("inf")
    app.names_to_display.update({"a": "Ann Lee"})
    app.last_update_time = 0
    app.consume_once()
    app.kafka_consumer = None
    assert app.names_to_display == {}, app.names_to_display


def check_page_loading_during_outage():
    def first_message():
        client = app.socketio.test_client(app.app)
        received = client.get_received()
        client.disconnect()
        data = next(event["args"][0] for event in received if event["name"] == "new_message")
        return data["message"], data["status_seconds"]

    app.camera_status.clear()
    app.update_camera_status("ERROR gate_a_cam2: Camera stream from gate_a_cam2 is not accessible.")
    app.push_status(*app.UNREACHABLE)
    assert first_message() == ([{"ERROR": app.UNREACHABLE[1]}], None)
    assert app.current_state()["camera_status"] == [], "camera chips must hide behind a system status"
    # Every poll repeats it too, so the page mirrors the server and never falls back to the idle screen
    client = app.socketio.test_client(app.app)
    client.get_received()
    client.emit("check_updates")
    polled = [e["args"][0]["message"] for e in client.get_received() if e["name"] == "new_message"]
    client.disconnect()
    assert polled and polled[-1] == [{"ERROR": app.UNREACHABLE[1]}], polled
    # A green status resumes with the time it has left, then ends
    app.push_status("SUCCESS", app.RESUMED, seconds=60)
    message, seconds = first_message()
    assert message == [{"SUCCESS": app.RESUMED}] and 58 < seconds <= 60, (message, seconds)
    # The page itself carries the state, so its first frame is never the idle screen
    page = app.app.test_client().get("/").get_data(as_text=True)
    assert app.RESUMED in page and "showMessage({" in page
    app.display_status["until"] = time.time() - 1
    assert first_message() == (app.messages_to_display, None)
    assert app.current_state()["camera_status"] == [{"level": "ERROR", "text": "Camera stream from gate_a_cam2 is not accessible."}]
    app.camera_status.clear()


def check_health_url():
    def health_url(value):
        env = {**os.environ, "HEALTH_URL": value}
        return subprocess.run([sys.executable, "-c", "import parameters as p; print(p.HEALTH_URL)"], env=env, capture_output=True, text=True)
    assert health_url("https://site.example/health").stdout.strip() == "https://site.example/health"
    assert health_url("").stdout.strip() == "http://192.0.2.1:20010/health", "defaults to IP_ADDRESS"
    for bad in ("site.example/health", "ftp://site.example/health", "http://"):
        result = health_url(bad)
        assert result.returncode != 0 and "HEALTH_URL must be" in result.stderr, (bad, result)


class Reply:
    """A requests response, closed by its with block"""
    def __init__(self, status, body=None):
        self.status_code, self.body, self.closed = status, body, False

    def json(self):
        if isinstance(self.body, Exception):
            raise self.body
        return self.body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise app.requests.HTTPError(str(self.status_code))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.closed = True


def answering(reply):
    def call(url, timeout, json=None):
        if isinstance(reply, Exception):
            raise reply
        return reply
    return call


def check_read_health():
    def health_for(reply):
        original, app.requests.get = app.requests.get, answering(reply)
        try:
            return app.read_health()
        finally:
            app.requests.get = original

    ready = Reply(200, {"status": "ready", "reasons": []})
    assert health_for(ready) == ("ready", []) and ready.closed, "the response must be closed"
    assert health_for(Reply(503, {"status": "starting", "reasons": ["not_started"]})) == ("starting", ["not_started"])
    assert health_for(Reply(503, {"status": "unready", "reasons": ["milvus_unavailable", "Bad Code\n", 7]})) == ("unready", ["milvus_unavailable"])
    # Answers that are not a clean ready are down, never ready
    for odd in (Reply(200, {"status": "starting"}), Reply(200, ValueError("html")), Reply(404), Reply(503, ["x"]), Reply(500, {"status": "ready"})):
        assert health_for(odd)[0] in ("starting", "unready"), odd.body
    for gone in (app.requests.ConnectionError("refused"), app.requests.Timeout("slow")):
        assert health_for(gone) == ("unreachable", []), gone
    assert parameters.HEALTH_TIMEOUT < parameters.HEALTH_INTERVAL, "a check must end before the next is due"


def check_health_debounce():
    reset_screen()
    for state in ("unready", "unready"):
        app.record_health(state)
    assert app.system_state is None, "two failures are not a verdict"
    app.record_health("unreachable")
    assert app.system_state == "unreachable"
    app.record_health("starting")
    assert app.system_state == "starting", "once down, the panel follows each answer"
    # Flapping never recovers: it takes 3 readies in a row
    for state in ("ready", "ready", "unready", "ready", "ready"):
        app.record_health(state)
    assert app.system_state == "unready", app.system_state
    app.record_health("ready")
    assert app.system_state == "ready"
    # And while ready, a blip of one or two failures changes nothing
    for state in ("unreachable", "starting", "ready", "unready", "unready"):
        app.record_health(state)
    assert app.system_state == "ready"
    app.record_health("unready")
    assert app.system_state == "unready"


def check_health_thread_survives_slow_checks():
    # A check can overrun its slot, as a failed DNS lookup does in about 3 s, which the request timeout does not cover.
    # The thread must keep going: in v0.8.0 before release, a negative sleep raised and ended it, leaving the screen on ready
    calls = []

    def slow_read():
        calls.append(time.monotonic())
        if len(calls) > 6:
            raise SystemExit  # Not an Exception, so it ends the thread and the check
        time.sleep(0.15)
        return "unreachable", []

    reset_screen()
    original = app.read_health, app.HEALTH_INTERVAL
    app.read_health, app.HEALTH_INTERVAL = slow_read, 0.1
    try:
        thread = threading.Thread(target=app.watch_health, daemon=True)
        thread.start()
        thread.join(5)
    finally:
        app.read_health, app.HEALTH_INTERVAL = original
    assert len(calls) == 7, f"the thread died after {len(calls)} checks"
    assert app.system_state == "unreachable"

    # And if it stops recording for any reason, the screen does not go on saying ready
    reset_screen("ready", True)
    app.check_health_stale()
    assert app.system_state == "ready"
    app.health_seen = time.monotonic() - parameters.HEALTH_STALE - 1
    app.check_health_stale()
    assert app.system_state == "unreachable" and app.wanted_panel() == app.UNREACHABLE
    app.record_health("ready")
    assert app.system_state == "unreachable", "a late answer still needs 3 readies in a row"


def check_kafka_gate():
    reset_screen()
    consumer = FakeKafka(partitions=[0, 1])
    assert app.check_kafka(consumer) is True and app.kafka_state is True
    consumer.down = True
    for _ in range(2):
        app.check_kafka(consumer)
    assert app.kafka_state is True, "two failed checks are not a verdict"
    app.check_kafka(consumer)
    assert app.kafka_state is False
    # A topic it cannot read, or one with none of the wanted partitions, is no usable assignment either
    for consumer in (FakeConsumer([], error="UNKNOWN_TOPIC_OR_PART"), FakeConsumer([])):
        reset_screen()
        for _ in range(3):
            assert app.check_kafka(consumer) is False
        assert app.kafka_state is False
    # All brokers down is already librdkafka's verdict after its retries: the panel goes up at once, and one good check ends it
    reset_screen("ready", True)
    broker_down = SimpleNamespace(code=lambda: app.KafkaError._ALL_BROKERS_DOWN, fatal=lambda: False)
    app.on_kafka_error(SimpleNamespace(code=lambda: app.KafkaError._TRANSPORT, fatal=lambda: False))
    assert app.kafka_state is True and not app.kafka_suspect, "one broker connection dropping is no outage"
    app.on_kafka_error(broker_down)
    assert app.kafka_state is False and app.kafka_suspect and app.wanted_panel() == app.UNREACHABLE
    assert app.check_kafka(FakeKafka()) is True and app.wanted_panel() is None

    # Coming back renews each camera error, since nothing could be repeated while Kafka was out of reach
    app.camera_status.clear()
    app.update_camera_status("ERROR gate_a_cam2: Camera stream from gate_a_cam2 is not accessible.")
    app.camera_status["gate_a_cam2"]["expires"] = time.time() + 1
    app.kafka_state = False
    app.check_kafka(FakeKafka())
    assert app.camera_status["gate_a_cam2"]["expires"] > time.time() + parameters.CAMERA_ERROR_SECONDS - 5
    app.camera_status.clear()


def check_screen_follows_readiness():
    sent = []

    def post(url, json, timeout):
        sent.append(list(json["messages"]))
        raise app.requests.ConnectionError("image server down")

    original_post, app.requests.post = app.requests.post, post
    try:
        reset_screen()
        app.camera_status.clear()
        app.kafka_consumer = FakeKafka(messages=["EMP-1"])
        app.consume_once()
        assert app.kafka_state is True and screen() == [{"INFO": app.CONNECTING[1]}], "no readiness verdict yet"
        assert sent == [], "a recognition behind a panel is not shown later"

        app.system_state = "ready"
        app.kafka_consumer.messages = ["EMP-2"]
        app.consume_once()
        assert app.shown is None and app.display_status is None
        # The image and name servers are down, so the row shows with a silhouette, not the rows from before
        assert screen() == [{"EMP-2": None}] and app.names_to_display == {}, screen()

        # The producer restarts: Getting Started at once, camera chips still read, and no 2 minute hold
        app.system_state = "starting"
        app.kafka_consumer.messages = ["ERROR gate_a_cam2: Camera stream from gate_a_cam2 is not accessible.", "EMP-3"]
        began = time.monotonic()
        for _ in range(3):
            app.consume_once()
        assert time.monotonic() - began < 2, "a pass must not wait on readiness"
        assert screen() == [{"SUCCESS": app.STARTING[1]}] and app.current_state()["camera_status"] == []
        assert "gate_a_cam2" in app.camera_status and sent[-1] == ["EMP-2"], "camera read, recognition held back"

        app.system_state = "unready"
        app.consume_once()
        assert screen() == [{"ERROR": app.DOWN[1]}]
        app.system_state = "unreachable"
        app.consume_once()
        assert screen() == [{"ERROR": app.UNREACHABLE[1]}], "nothing answered, and the panel does not guess why"

        app.system_state = "ready"
        app.consume_once()
        assert screen() == [{"SUCCESS": app.RESUMED}] and app.current_state()["status_seconds"] <= 5
        app.display_status["until"] = time.time() - 1
        assert screen() == [] and app.current_state()["camera_status"][0]["level"] == "ERROR", "the chips return with the rows"

        # Ready, but the display's own Kafka path is gone: no rows, and recovery needs a usable assignment again
        app.kafka_consumer.down = True
        app.kafka_suspect = True
        for _ in range(3):
            app.consume_once()
            app.next_partition_check = 0
        assert screen() == [{"ERROR": app.UNREACHABLE[1]}]
        app.kafka_consumer.down = False
        app.consume_once()
        assert screen() == [{"SUCCESS": app.RESUMED}]
    finally:
        app.requests.post = original_post
        app.kafka_consumer = None
        app.camera_status.clear()


def check_image_fallback():
    original = app.requests.post
    try:
        for reply in (Reply(200, [{"a": "Zm9v"}, {"b": None}]), Reply(500), Reply(200, {"a": "x"}), Reply(200, ValueError("html")), app.requests.Timeout("t")):
            app.requests.post = answering(reply)
            app.messages_to_display = [{"stale": "old"}]
            app.post_message_queue_and_get_images({"messages": ["a", "b"]})
            good = isinstance(reply, Reply) and reply.status_code == 200 and isinstance(reply.body, list)
            assert app.messages_to_display == (reply.body if good else [{"a": None}, {"b": None}]), (reply, app.messages_to_display)
    finally:
        app.requests.post = original


def check_camera_error_outlasts_slow_reopen():
    # An inaccessible camera's ERROR comes back after a 30 s wait, a GPU probe of up to 30 s and a CPU reopen, here 60 s.
    # Scaled 1:100, the chip must stay up the whole time the fault lasts
    scale = 100
    cycle = (30 + 30 + 60) / scale
    original = app.CAMERA_ERROR_SECONDS
    app.CAMERA_ERROR_SECONDS = parameters.CAMERA_ERROR_SECONDS / scale
    app.camera_status.clear()
    try:
        began = time.monotonic()
        next_repeat = began
        while time.monotonic() - began < 3 * cycle:
            if time.monotonic() >= next_repeat:
                app.update_camera_status("ERROR gate_a_cam5: Camera stream from gate_a_cam5 is not accessible.")
                next_repeat += cycle
            assert app.current_camera_status(), f"the chip vanished {time.monotonic() - began:.2f} s in, while the fault lasted"
            time.sleep(0.02)
    finally:
        app.CAMERA_ERROR_SECONDS = original
        app.camera_status.clear()


if __name__ == "__main__":
    for check in (check_parse, check_assign, check_queue, check_camera_status, check_startup_rejects_bad_value, check_kafka_login, check_page, check_default_address, check_names, check_names_cleared_when_idle, check_page_loading_during_outage,
                  check_health_url, check_read_health, check_health_debounce, check_health_thread_survives_slow_checks, check_kafka_gate, check_screen_follows_readiness, check_image_fallback, check_camera_error_outlasts_slow_reopen):
        check()
        print("PASS", check.__name__, flush=True)
    os._exit(0)  # app's daemon threads would otherwise keep logging until exit
