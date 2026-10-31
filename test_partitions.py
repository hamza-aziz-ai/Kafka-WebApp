"""Checks the partition and queue logic without a broker. Run: python test_partitions.py"""
import os
import subprocess
import sys

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

    app.update_message_queue("ERROR cctv-camera-2: Camera stream is not accessible.")
    assert app.message_queue == ["ERROR cctv-camera-2: Camera stream is not accessible."]

    # Another camera's person replaces the control message, or the page would hide the row
    app.update_message_queue("f")
    assert app.message_queue == ["f"], app.message_queue


def check_startup_rejects_bad_value():
    env = {**os.environ, "KAFKA_PARTITIONS": "0,x"}
    result = subprocess.run([sys.executable, "-c", "import parameters"], env=env, capture_output=True, text=True)
    assert result.returncode != 0 and "KAFKA_PARTITIONS must be" in result.stderr, result


if __name__ == "__main__":
    for check in (check_parse, check_assign, check_queue, check_startup_rejects_bad_value):
        check()
        print("PASS", check.__name__, flush=True)
    os._exit(0)  # app's daemon threads would otherwise keep logging until exit
