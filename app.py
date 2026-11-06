import os
import re
import secrets
import threading
import time
import requests
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_END
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from datetime import datetime
from custom_logging import feedback_client_logger
from parameters import CONFLUENT_KAFKA_IP_ADDRESS, \
    CONFLUENT_KAFKA_PORT, \
    CONFLUENT_KAFKA_TOPIC, \
    KAFKA_PARTITIONS, \
    KAFKA_PARTITION_CHECK_INTERVAL, \
    CAMERA_SUCCESS_SECONDS, \
    CAMERA_ERROR_SECONDS, \
    DISPLAY_LOCATION, \
    KAFKA_SECURITY_CONF, \
    CONFLUENT_KAFKA_GROUP, \
    DISPLAY_LIST_SIZE, \
    IMAGE_SERVER_ADDRESS, \
    NAME_SERVER_ADDRESS, \
    STATUS_THRESHOLD, \
    HEALTH_URL, \
    HEALTH_INTERVAL, \
    HEALTH_TIMEOUT, \
    HEALTH_STREAK, \
    HEALTH_STALE, \
    IMAGE_SERVER_TIMEOUT

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY') or secrets.token_hex(32)
socketio = SocketIO(app, cors_allowed_origins="*")

kafka_conf = {
    'bootstrap.servers': f'{CONFLUENT_KAFKA_IP_ADDRESS}:{CONFLUENT_KAFKA_PORT}',
    'group.id': CONFLUENT_KAFKA_GROUP,
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'heartbeat.interval.ms': 2000,
    'max.poll.interval.ms': 300000,
    **KAFKA_SECURITY_CONF
}

# The panels that replace the rows. Each says no more than the checks know
CONNECTING = ("INFO", "Connecting to the Facial Recognition Attendance System")
UNREACHABLE = ("ERROR", "Cannot Reach the Facial Recognition Attendance System")  # Nothing answered: the network or the services, unknown which
STARTING = ("SUCCESS", "Facial Recognition Attendance System is Getting Started. Please Wait")
DOWN = ("ERROR", "Facial Recognition System Temporarily Down for Maintenance or Due to an Unexpected Issue")
RESUMED = "Facial Recognition Attendance System Has Been Started. Please Resume Attendance"

message_queue = []
messages_to_display = []
display_status = {"level": CONNECTING[0], "text": CONNECTING[1], "until": None}  # The last status: {level, text, until}; until is None while it lasts until changed. Until the first checks finish, nothing is known
names_to_display = {}  # Person ID to name, for the IDs last sent to the name server. Memory only, never logged, and cleared with the rows
last_update_time = time.time()
shown = CONNECTING  # The panel the consumer thread last put up, or None while the rows show

# Readiness from the producer's /health, written by the health thread: None until the first verdict, then ready, starting, unready or unreachable
system_state = None
health_streak = 0  # Results in a row that agree: positive for ready, negative for not
health_seen = time.monotonic()  # When the health thread last recorded a result

# The display's own path to Kafka, which readiness cannot see: None until the first check, True while it holds partitions, False after failed checks
kafka_state = None
kafka_failures = 0
kafka_suspect = False  # Set by a Kafka error, so the next pass checks at once instead of a minute later

kafka_consumer = None
next_partition_check = 0
assigned_partitions = set()
camera_status = {}

CONTROL_PREFIXES = ("ERROR ", "SUCCESS ")
REASON = re.compile(r"[a-z0-9_]{1,64}")  # Readiness reasons are short codes; anything else is dropped, not logged

# Statuses are pushed once, so keep the last one with its end time: a page that loads or reconnects resumes it, with the time it has left
def push_status(level, text, seconds=None):
    global display_status
    display_status = {"level": level, "text": text, "until": None if seconds is None else time.time() + seconds}
    socketio.emit('new_message', current_state())  # The whole screen, so the camera chips go in the same frame as the panel comes

def status_in_force():
    status = display_status
    if status and (status["until"] is None or status["until"] > time.time()):
        return status
    return None

# Everything a page needs to draw the screen as it is now, sent on connect and on every poll. A status in force replaces the rows
def current_state():
    status = status_in_force()
    message = [{status["level"]: status["text"]}] if status else messages_to_display
    seconds = round(status["until"] - time.time(), 1) if status and status["until"] else None
    # A system status supersedes the camera chips: no camera can be checked while the system itself is down. They stay in
    # camera_status, so they return when the status ends
    cameras = [] if status else current_camera_status()
    return {'message': message, 'status_seconds': seconds, 'names': names_to_display, 'camera_status': cameras}

# One readiness check: ready only on 200 with status ready. Any other answer is starting or unready, and no answer is unreachable
def read_health():
    try:
        with requests.get(HEALTH_URL, timeout=HEALTH_TIMEOUT) as response:
            status_code = response.status_code
            try:
                body = response.json()
            except ValueError:
                body = None
    except requests.RequestException:
        return "unreachable", []
    body = body if isinstance(body, dict) else {}
    if status_code == 200 and body.get("status") == "ready":
        return "ready", []
    reasons = body.get("reasons")
    reasons = [r for r in reasons if isinstance(r, str) and REASON.fullmatch(r)] if isinstance(reasons, list) else []
    return ("starting" if body.get("status") == "starting" else "unready"), reasons

# Debounced: it takes HEALTH_STREAK results in a row to go from ready to down or back, so one slow check cannot flash a panel.
# Once down, the panel follows each failed result, so Getting Started shows as soon as the producer says so
def record_health(state, reasons=()):
    global system_state, health_streak
    ready = state == "ready"
    health_streak = max(health_streak, 0) + 1 if ready else min(health_streak, 0) - 1
    if abs(health_streak) < HEALTH_STREAK and (ready or system_state in (None, "ready")):
        return
    if state != system_state:
        log = feedback_client_logger.info if ready else feedback_client_logger.error
        log(f"Attendance system is {state}" + (f": {', '.join(reasons)}" if reasons else ""))
    system_state = state

def watch_health():
    global health_seen
    next_check = time.monotonic()
    while True:
        try:
            record_health(*read_health())
            health_seen = time.monotonic()
        except Exception:
            feedback_client_logger.exception("Readiness check failed")
        now = time.monotonic()  # Read once: a second read could pass next_check, and a negative sleep raises and ends the thread
        next_check = max(next_check + HEALTH_INTERVAL, now)  # A fixed beat, not one that drifts by each check's length
        time.sleep(next_check - now)

# No verdict for this long means the health thread is stuck or gone, and the screen must not go on saying ready
def check_health_stale():
    global system_state, health_streak
    if system_state != "unreachable" and time.monotonic() - health_seen > HEALTH_STALE:
        feedback_client_logger.error("No readiness result, treating the attendance system as unreachable")
        system_state, health_streak = "unreachable", 0

def post_message_queue_and_get_images(data):
    global messages_to_display
    try:
        with requests.post(IMAGE_SERVER_ADDRESS, json=data, timeout=IMAGE_SERVER_TIMEOUT) as response:  # json= sets Content-Type; FastAPI rejects a body without it (422)
            response.raise_for_status()
            rows = response.json()
        if not isinstance(rows, list):
            raise ValueError("not a list")
        messages_to_display = rows
    except Exception as e:
        feedback_client_logger.error(f"Image server error: {type(e).__name__}")
        # The current IDs with silhouettes: the person who just walked past must not see the rows from before
        messages_to_display = [{person_id: None} for person_id in data["messages"]]

# Best effort, after the images, so a missing or failing name server never holds back a face
def post_message_queue_and_get_names(data):
    global names_to_display
    try:
        with requests.post(NAME_SERVER_ADDRESS, json=data, timeout=IMAGE_SERVER_TIMEOUT) as response:
            if response.status_code == 404:  # An older CCTV release with no /post_names
                names_to_display = {}
                return
            response.raise_for_status()
            names = response.json()
        names_to_display = {i: n for i, n in names.items() if isinstance(n, str) and n} if isinstance(names, dict) else {}
    except Exception as e:
        names_to_display = {}
        feedback_client_logger.error(f"Name server error: {type(e).__name__}")  # Type only: the message could carry a name

# Returns False when the topic cannot be read, so the caller counts a failed check
def assign_new_partitions(consumer):
    # Keep assign(), not subscribe(): displays share a group, and subscribe() would split the partitions between them
    topic = consumer.list_topics(CONFLUENT_KAFKA_TOPIC, timeout=5).topics[CONFLUENT_KAFKA_TOPIC]
    if topic.error:
        feedback_client_logger.error(f"Kafka topic {CONFLUENT_KAFKA_TOPIC}: {topic.error}")
        return False

    available = set(topic.partitions)
    wanted = set(KAFKA_PARTITIONS) or available
    if wanted - available:
        feedback_client_logger.warning(f"Partitions {sorted(wanted - available)} are not in {CONFLUENT_KAFKA_TOPIC} yet")

    new_partitions = sorted((wanted & available) - assigned_partitions)
    if new_partitions:
        consumer.incremental_assign([TopicPartition(CONFLUENT_KAFKA_TOPIC, partition, OFFSET_END) for partition in new_partitions])
        assigned_partitions.update(new_partitions)
        feedback_client_logger.info(f"Reading {CONFLUENT_KAFKA_TOPIC} partitions {sorted(assigned_partitions)}")
    return True

# A metadata request and the assignment it allows. Neither creating a consumer nor polling one proves the broker answers
def check_kafka(consumer):
    global kafka_state, kafka_failures, kafka_suspect
    kafka_suspect = False
    try:
        usable = assign_new_partitions(consumer) and bool(assigned_partitions)
    except Exception as e:
        feedback_client_logger.error(f"Kafka check failed: {e}")
        usable = False
    if usable:
        if kafka_state is not True:
            # Nothing was read while Kafka was out of reach, so no ERROR could be repeated: give each camera error a fresh life,
            # so the chips return now and any whose camera recovered in the meantime drop off unrepeated
            for status in camera_status.values():
                if status["level"] == "ERROR":
                    status["expires"] = time.time() + CAMERA_ERROR_SECONDS
        kafka_state, kafka_failures = True, 0
    else:
        kafka_failures += 1
        if kafka_failures >= HEALTH_STREAK and kafka_state is not False:
            kafka_state = False
            feedback_client_logger.error("Cannot read Kafka")
    return usable

# librdkafka reports these only after it has tried every broker, so the panel goes up at once, without waiting for 3 failed
# checks, in which the idle screen could show while nothing can be read. Coming back still needs a check that succeeds
def on_kafka_error(error):
    global kafka_suspect, kafka_state
    if error.code() in (KafkaError._ALL_BROKERS_DOWN, KafkaError._AUTHENTICATION) or error.fatal():
        if not kafka_suspect:
            feedback_client_logger.error(f"Kafka error: {error}")
        kafka_suspect = True
        if kafka_state is not False:
            kafka_state = False
            feedback_client_logger.error("Cannot read Kafka")

def wanted_panel():
    if system_state is None or (system_state == "ready" and kafka_state is None):
        return CONNECTING
    if system_state == "ready":
        return None if kafka_state else UNREACHABLE
    return {"unreachable": UNREACHABLE, "starting": STARTING}.get(system_state, DOWN)

# Puts up the panel the checks call for, once, when it changes. Rows from before a panel never come back after it
def update_screen():
    global shown, display_status
    panel = wanted_panel()
    if panel == shown:
        return
    previous, shown = shown, panel
    message_queue.clear()
    messages_to_display.clear()
    names_to_display.clear()
    if panel:
        push_status(*panel)
        feedback_client_logger.info(panel[1])
    elif previous == CONNECTING:
        display_status = None  # The first checks passed, so the startup panel gives way to the rows
        socketio.emit('new_message', current_state())
    else:
        push_status('SUCCESS', RESUMED, seconds=5)
        feedback_client_logger.info(RESUMED)

def update_message_queue(message):
    if message not in message_queue:
        if len(message_queue) >= DISPLAY_LIST_SIZE:
            message_queue.pop()
        message_queue.insert(0, message)

def update_camera_status(message):
    # Format: ERROR <camera>: <text>. Kept apart from the rows, so a camera error hides no person. It lasts while the producer
    # repeats it, so it survives an outage and returns when the panel goes
    level, _, rest = message.partition(" ")
    camera, _, text = rest.partition(": ")
    expires = time.time() + (CAMERA_ERROR_SECONDS if level == "ERROR" else CAMERA_SUCCESS_SECONDS)
    camera_status[camera] = {"level": level, "text": text or rest, "expires": expires}

def current_camera_status():
    now = time.time()
    return [{"level": status["level"], "text": status["text"]}
            for _, status in sorted(list(camera_status.items()), key=lambda item: item[0])
            if status["expires"] > now]

def close_consumer():
    global kafka_consumer
    consumer, kafka_consumer = kafka_consumer, None
    try:
        consumer.close()
    except Exception:
        pass

# One pass of the consumer thread. Nothing in it waits on readiness, so camera messages keep coming while the system is down
def consume_once():
    global kafka_consumer, next_partition_check, last_update_time, kafka_suspect

    if kafka_consumer is None:
        try:
            kafka_consumer = Consumer({**kafka_conf, 'error_cb': on_kafka_error})
            assigned_partitions.clear()
            next_partition_check = 0
            feedback_client_logger.info("Created a fresh Kafka consumer")
        except Exception:
            feedback_client_logger.exception("Failed to initialize Kafka Consumer")
            kafka_consumer = None
            time.sleep(5)
            return

    if kafka_suspect or time.monotonic() >= next_partition_check:
        usable = check_kafka(kafka_consumer)
        next_partition_check = time.monotonic() + (KAFKA_PARTITION_CHECK_INTERVAL if usable else HEALTH_INTERVAL)

    check_health_stale()
    update_screen()

    try:
        message_from_kafka = kafka_consumer.poll(0.2)
    except Exception:
        feedback_client_logger.exception("Kafka polling failed")
        close_consumer()
        return

    if message_from_kafka is None:
        pass
    elif message_from_kafka.error():
        message_queue.clear()
        kafka_suspect = True
        feedback_client_logger.error(f"Kafka error: {message_from_kafka.error()}")
    else:
        try:
            decoded_kafka_message = message_from_kafka.value().decode()
            if decoded_kafka_message.startswith(CONTROL_PREFIXES):
                update_camera_status(decoded_kafka_message)
            elif shown is None:
                # Only while the rows show: a person read behind a panel would come back as a stale row, for attendance that may not be kept
                update_message_queue(decoded_kafka_message)
                data = {"messages": message_queue}
                post_message_queue_and_get_images(data)
                post_message_queue_and_get_names(data)
                last_update_time = time.time()
        except Exception:
            feedback_client_logger.exception("Error processing Kafka message")

    if time.time() - last_update_time >= STATUS_THRESHOLD:
        message_queue.clear()
        messages_to_display.clear()
        names_to_display.clear()
        socketio.emit('new_message', current_state())  # The whole state, so clearing the rows cannot hide a status in force
        last_update_time = time.time()

def consume_messages():
    try:
        while True:
            consume_once()
    except Exception as e:
        feedback_client_logger.exception("Consumer thread crashed")

# Watchdog to restart the thread if it crashes
def monitor_consumer_thread(thread_ref):
    while True:
        if not thread_ref.is_alive():
            feedback_client_logger.error("Kafka consumer thread died. Restarting...")
            new_thread = threading.Thread(target=consume_messages)
            new_thread.daemon = True
            new_thread.start()
            thread_ref = new_thread
        time.sleep(10)

health_thread = threading.Thread(target=watch_health, daemon=True)
health_thread.start()

kafka_consumer_thread = threading.Thread(target=consume_messages)
kafka_consumer_thread.daemon = True
kafka_consumer_thread.start()

watchdog_thread = threading.Thread(target=monitor_consumer_thread, args=(kafka_consumer_thread,))
watchdog_thread.daemon = True
watchdog_thread.start()

@socketio.on('connect')
def handle_connect():
    emit('new_message', current_state())

@socketio.on('check_updates')
def handle_check_updates():
    emit('new_message', current_state())

@app.route('/')
def index():
    # Drawn into the page, so its first frame is the real screen, never the idle face guide while the socket connects
    return render_template('index.html', location=DISPLAY_LOCATION, state=current_state())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9090)
