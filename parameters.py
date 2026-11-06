import os
from urllib.parse import urlsplit

# Project Services
SERVER_IP_ADDRESS = os.getenv("IP_ADDRESS", "127.0.0.1")

# Readiness of the attendance system: the producer answers 200 {"status": "ready"} or 503 with starting or unready and its reasons.
# Outside the client's stack, as on a Raspberry Pi, point it at https://<site host>/health through the proxy
HEALTH_URL = os.getenv("HEALTH_URL") or f"http://{SERVER_IP_ADDRESS}:20010/health"
if urlsplit(HEALTH_URL).scheme not in ("http", "https") or not urlsplit(HEALTH_URL).netloc:
    raise SystemExit(f"HEALTH_URL must be an http or https URL, got {HEALTH_URL!r}")

# Seconds between readiness checks, and the most one may take, so a check is done before the next is due
HEALTH_INTERVAL = 2
HEALTH_TIMEOUT = 1.5

# Results in a row that change the screen between ready and down, for readiness and for the display's own Kafka checks
HEALTH_STREAK = 3

# Seconds with no readiness result, as when a check hangs where its timeout does not reach, before the system counts as unreachable
HEALTH_STALE = HEALTH_STREAK * HEALTH_INTERVAL + HEALTH_TIMEOUT

# Kafka IP and Port
CONFLUENT_KAFKA_IP_ADDRESS = SERVER_IP_ADDRESS
CONFLUENT_KAFKA_PORT = 9092

# Kafka login: setting KAFKA_USERNAME turns on SASL, so a display can run this image before the broker requires it
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
if KAFKA_USERNAME and not KAFKA_PASSWORD:
    raise SystemExit("KAFKA_PASSWORD must be set when KAFKA_USERNAME is set")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL") or ("SASL_PLAINTEXT" if KAFKA_USERNAME else "PLAINTEXT")
KAFKA_SECURITY_CONF = {"security.protocol": KAFKA_SECURITY_PROTOCOL}
if KAFKA_USERNAME:
    KAFKA_SECURITY_CONF |= {"sasl.mechanism": "SCRAM-SHA-512", "sasl.username": KAFKA_USERNAME, "sasl.password": KAFKA_PASSWORD}

# Kafka topic for messages
CONFLUENT_KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "maingate_entry")

def _parse_partitions(value: str) -> list[int]:
    if not value.strip():
        return []
    entries = [entry.strip() for entry in value.split(",")]
    if not all(entry.isascii() and entry.isdigit() for entry in entries):
        raise SystemExit(f"KAFKA_PARTITIONS must be comma-separated partition numbers (camera number - 1), got {value!r}")
    return sorted({int(entry) for entry in entries})

# Partitions to show, for one screen per camera (partition = camera number - 1). Empty means every partition of the topic
KAFKA_PARTITIONS = _parse_partitions(os.getenv("KAFKA_PARTITIONS", ""))

# Seconds between checks for partitions added with a new camera
KAFKA_PARTITION_CHECK_INTERVAL = 60

# Shown on the screen: the topic without its client prefix, so acme.maingate_entry reads Maingate Entry
DISPLAY_LOCATION = CONFLUENT_KAFKA_TOPIC.rpartition(".")[2].replace("_", " ").title()

# The group lives under the topic's client prefix, so a client's Kafka permissions cover it
CONFLUENT_KAFKA_GROUP = f"{CONFLUENT_KAFKA_TOPIC}.display"

# Number of names to be displayed at once
DISPLAY_LIST_SIZE = 4

# Message Retention time on Screen
STATUS_THRESHOLD = 15

# Image Server Address
# Overridable because on a central host each client's image server has its own port; a fixed :20013 would show another client's faces
IMAGE_SERVER_ADDRESS = os.getenv("IMAGE_SERVER_ADDRESS", f"http://{SERVER_IP_ADDRESS}:20013/post_images")
# Names come from the same server; older CCTV releases answer 404, and the page then shows the ID alone
NAME_SERVER_ADDRESS = IMAGE_SERVER_ADDRESS.rsplit("/", 1)[0] + "/post_names"

# Seconds a camera's SUCCESS text stays in the status strip
CAMERA_SUCCESS_SECONDS = 5

# Seconds a camera's ERROR stays without a repeat, so a chip whose SUCCESS was missed, as during an outage, drops off. The producer
# repeats an inaccessible camera's ERROR after each failed reopen: a 30 second wait, a GPU probe of up to 30 seconds, then a CPU
# fallback with no fixed limit. This covers a 60 second CPU reopen with a minute to spare, and test_partitions.py checks it
CAMERA_ERROR_SECONDS = 180

# Seconds to wait on the image server, so a hung server cannot freeze the consumer thread
IMAGE_SERVER_TIMEOUT = 5

# Logs directory path
LOGS_FOLDER = "logs"
