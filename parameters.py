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
CONFLUENT_KAFKA_PARTITION = 0

# Number of names to be displayed at once
DISPLAY_LIST_SIZE = 4

# Message Retention time on Screen
STATUS_THRESHOLD = 15

# Image Server Address
IMAGE_SERVER_ADDRESS = f"http://{DGX_IP_ADDRESS}:20013/post_images"

# Number of times to check for network failure
NETWORK_FAIL_CHECK = 10

# Logs directory path
LOGS_FOLDER = "logs"
