"""Checks partition, queue, camera status, health check and Kafka login logic without a broker. Run: python test_partitions.py"""
import json
import os
import pathlib
import subprocess
import sys
import time

os.environ["IP_ADDRESS"] = "192.0.2.1"  # reserved, reaches nothing, so app's threads cannot touch a real Kafka
os.environ.pop("KAFKA_PARTITIONS", None)

import app
import parameters
from types import SimpleNamespace


class FakeConsumer:
    def __init__(self, partitions, error=None):
        self.partitions, self.error, self.assigned = partitions, error, []

    def list_topics(self, topic, timeout):
        return SimpleNamespace(topics={topic: SimpleNamespace(error=self.error, partitions=dict.fromkeys(self.partitions))})

    def incremental_assign(self, partitions):
        self.assigned.append([(p.partition, p.offset) for p in partitions])


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

    # Errors never expire, and a message with no ": " still shows
    app.camera_status.clear()
    app.update_camera_status("ERROR gate_a_cam3")
    assert app.current_camera_status() == [{"level": "ERROR", "text": "gate_a_cam3"}]


def check_startup_rejects_bad_value():
    env = {**os.environ, "KAFKA_PARTITIONS": "0,x"}
    result = subprocess.run([sys.executable, "-c", "import parameters"], env=env, capture_output=True, text=True)
    assert result.returncode != 0 and "KAFKA_PARTITIONS must be" in result.stderr, result


def run_parameters(**env):
    full_env = {key: value for key, value in os.environ.items() if key not in ("PROJECT_SERVICES", "PROJECT_SERVICES_PORT")}
    code = "import parameters as p; print(p.PROJECT_SERVICES)"
    return subprocess.run([sys.executable, "-c", code], env={**full_env, "IP_ADDRESS": "198.51.100.7", **env}, capture_output=True, text=True)


def check_default_address():
    env = {key: value for key, value in os.environ.items() if key != "IP_ADDRESS"}
    code = "import parameters as p; print(p.SERVER_IP_ADDRESS)"
    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert result.stdout.strip() == "127.0.0.1", result


def check_project_services():
    ip = "198.51.100.7"
    bare_and_named = run_parameters(PROJECT_SERVICES="6385, 9092,db-controller:20001,10.0.0.5:20013")
    assert bare_and_named.stdout.strip() == str([(ip, 6385), (ip, 9092), ("db-controller", 20001), ("10.0.0.5", 20013)]), bare_and_named

    for bad in ("abc", "6385,", "host:", ":20001", "a:b:20001", "0", "70000", "host name:20001"):
        result = run_parameters(PROJECT_SERVICES=bad)
        assert result.returncode != 0 and "PROJECT_SERVICES entries must be" in result.stderr, (bad, result)

    # Unset or blank falls back to PROJECT_SERVICES_PORT, then to the defaults
    assert run_parameters(PROJECT_SERVICES_PORT="1,2").stdout.strip() == str([(ip, 1), (ip, 2)])
    assert run_parameters(PROJECT_SERVICES="  ", PROJECT_SERVICES_PORT="1").stdout.strip() == str([(ip, 1)])
    defaults = parameters.DEFAULT_PROJECT_SERVICES_PORT
    assert defaults == [6385, 19000, 19530, 9092, 9090, 20013], defaults
    assert str([(ip, port) for port in defaults]) == run_parameters().stdout.strip()


def check_probes_use_each_host():
    probed = []
    original_services, original_ping = app.PROJECT_SERVICES, app.ping_server
    app.PROJECT_SERVICES = [("192.0.2.1", 9092), ("db-controller", 20001), ("image-server", 20013)]
    app.ping_server = lambda host, port: probed.append((host, port)) or port != 20001
    try:
        assert app.ping_project_services() is False
    finally:
        app.PROJECT_SERVICES, app.ping_server = original_services, original_ping
    assert probed == [("192.0.2.1", 9092), ("db-controller", 20001)], probed


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
    # app.py and the page must agree on this text, or the 2 minute hold stops working
    start = "Facial Recognition Attendance System is Getting Started. Please Wait for 2 Minutes"
    assert f'"{start}"' in page and f'"{start}"' in pathlib.Path("app.py").read_text(encoding="utf-8")


def check_kafka_login():
    assert json.loads(security_conf().stdout) == {"security.protocol": "PLAINTEXT"}
    assert json.loads(security_conf(KAFKA_USERNAME="acme_display", KAFKA_PASSWORD="x").stdout) == {
        "security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "SCRAM-SHA-512", "sasl.username": "acme_display", "sasl.password": "x"}
    assert json.loads(security_conf(KAFKA_USERNAME="a", KAFKA_PASSWORD="x", KAFKA_SECURITY_PROTOCOL="SASL_SSL").stdout)["security.protocol"] == "SASL_SSL"
    missing_password = security_conf(KAFKA_USERNAME="a")
    assert missing_password.returncode != 0 and "KAFKA_PASSWORD must be set" in missing_password.stderr
    assert app.kafka_conf["security.protocol"] == "PLAINTEXT"


if __name__ == "__main__":
    for check in (check_parse, check_assign, check_queue, check_camera_status, check_startup_rejects_bad_value, check_project_services, check_probes_use_each_host, check_kafka_login, check_page, check_default_address):
        check()
        print("PASS", check.__name__, flush=True)
    os._exit(0)  # app's daemon threads would otherwise keep logging until exit
