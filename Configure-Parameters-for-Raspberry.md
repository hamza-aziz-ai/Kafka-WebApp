# Congigure Raspberry Pi Kafka-WebApp Parameters

## The Structure of Flask Application

```tree
Kafka-WebApp
   ├── static/
   │   └── fonts/
   ├── templates/
   │   └── index.html
   ├── app.py
   ├── custom_logging.py
   ├── parameters.py
   └── requirements.txt
```

## The `parameters.py` file

The [`parameters.py`](Kafka-WebApp/parameters.py) file contains important information to setup your flask application. You need to change these as per your requiremnets.

```python
# Project Services
SERVER_IP_ADDRESS <IP address of the server>    # e.g., "10.0.0.5"

# Readiness of the attendance system, through the site's proxy
HEALTH_URL = "https://<site host>/health"

# Kafka IP and Port
CONFLUENT_KAFKA_IP_ADDRESS = SERVER_IP_ADDRESS
CONFLUENT_KAFKA_PORT = 9092

# Kafka topic for messages
CONFLUENT_KAFKA_TOPIC = "feedback_messages_topic_for_<Replace with Camera-Name>"    # e.g., "feedback_messages_topic_for_library-gate"

# Number of names to be displayed at once
DISPLAY_LIST_SIZE = 4

# Message Retention time on Screen
STATUS_THRESHOLD = 15

# Image Server Address
IMAGE_SERVER_ADDRESS = f"http://{SERVER_IP_ADDRESS}:20013/post_images"

# Logs directory path
LOGS_FOLDER = "logs"
```
