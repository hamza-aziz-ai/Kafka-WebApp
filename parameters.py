import os

# Project Services
DGX_IP_ADDRESS = os.getenv("IP_ADDRESS", "192.168.12.1")

# Port that shows the host is reachable
HOST_CHECK_PORT = int(os.getenv("HOST_CHECK_PORT", "22"))

DEFAULT_PROJECT_SERVICES_PORT = [
    6385, 19000, 19001,     # InsightFace
    19530,                  # Milvus Vector Database
    9092,                   # Kafka Broker
    6381,                   # Redis Database
    20001,                  # Database-Controller
    20012,                  # Kafka-Message-Controller
    20013,                  # Display-Image-Server
    6386, 6970, 6971        # Face Liveness Detection
]
# Comma-separated override, for a host that does not run every service above
PROJECT_SERVICES_PORT = [int(p) for p in os.getenv("PROJECT_SERVICES_PORT", "").split(",") if p] or DEFAULT_PROJECT_SERVICES_PORT

# Kafka IP and Port
CONFLUENT_KAFKA_IP_ADDRESS = DGX_IP_ADDRESS
CONFLUENT_KAFKA_PORT = 9092

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

# The group lives under the topic's client prefix, so a client's Kafka permissions cover it
CONFLUENT_KAFKA_GROUP = f"{CONFLUENT_KAFKA_TOPIC}.display"

# Number of names to be displayed at once
DISPLAY_LIST_SIZE = 4

# Message Retention time on Screen
STATUS_THRESHOLD = 15

# Image Server Address
# Overridable because on a central host each client's image server has its own port; a fixed :20013 would show another client's faces
IMAGE_SERVER_ADDRESS = os.getenv("IMAGE_SERVER_ADDRESS", f"http://{DGX_IP_ADDRESS}:20013/post_images")

# Seconds a camera's SUCCESS text stays in the status strip
CAMERA_SUCCESS_SECONDS = 5

# Number of times to check for network failure
NETWORK_FAIL_CHECK = 10

# Seconds to wait on the image server, so a hung server cannot freeze the consumer thread
IMAGE_SERVER_TIMEOUT = 5

# Logs directory path
LOGS_FOLDER = "logs"
