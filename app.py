import os
import secrets
import socket
import threading
import time
import requests
from confluent_kafka import Consumer, TopicPartition, OFFSET_END
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from datetime import datetime
from custom_logging import feedback_client_logger
from parameters import CONFLUENT_KAFKA_IP_ADDRESS, \
    CONFLUENT_KAFKA_PORT, \
    CONFLUENT_KAFKA_TOPIC, \
    CONFLUENT_KAFKA_PARTITION, \
    DISPLAY_LIST_SIZE, \
    IMAGE_SERVER_ADDRESS, \
    STATUS_THRESHOLD, \
    DGX_IP_ADDRESS, \
    HOST_CHECK_PORT, \
    NETWORK_FAIL_CHECK, \
    PROJECT_SERVICES_PORT

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY') or secrets.token_hex(32)
socketio = SocketIO(app, cors_allowed_origins="*")

kafka_conf = {
    'bootstrap.servers': f'{CONFLUENT_KAFKA_IP_ADDRESS}:{CONFLUENT_KAFKA_PORT}',
    'group.id': 'face_recognition_feedback_message_consumer_group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'heartbeat.interval.ms': 2000,
    'max.poll.interval.ms': 300000
}

message_queue = []
messages_to_display = []
last_update_time = time.time()
connection_failure_message_sent = False
project_services_down_message_sent = False
successful_reconnection_message_sent = not connection_failure_message_sent
project_restart_message_sent = not project_services_down_message_sent

kafka_consumer = None

def ping_server(ip: str, port: int):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.5)
        sock.connect((ip, port))
        return True
    except socket.error:
        return False

def ping_project_services() -> bool:
    return all(ping_server(DGX_IP_ADDRESS, port) for port in PROJECT_SERVICES_PORT)

def post_message_queue_and_get_images(data):
    global messages_to_display
    try:
        response = requests.post(IMAGE_SERVER_ADDRESS, json=data)  # json= sets Content-Type; FastAPI rejects a body without it (422)
        if response.status_code == 200:
            messages_to_display = response.json()
        else:
            feedback_client_logger.error(f"Image server error: {response.status_code}, {response.reason}")
    except Exception as e:
        feedback_client_logger.exception(f"Exception in image post: {e}")

def _consume_messages():
    global message_queue, last_update_time, messages_to_display
    global connection_failure_message_sent, project_services_down_message_sent
    global successful_reconnection_message_sent, project_restart_message_sent, kafka_consumer

    server_ping_fail = 0
    services_ping_fail = 0

    while True:
        is_dgx_reachable = ping_server(DGX_IP_ADDRESS, HOST_CHECK_PORT)
        are_services_running = ping_project_services()

        if is_dgx_reachable:
            server_ping_fail = 0
            if not successful_reconnection_message_sent:
                message_queue.clear()
                messages_to_display.clear()
                socketio.emit('new_message', {'message': [{'SUCCESS': "🛜 Network Reconnection Successful 🛜"}]})
                successful_reconnection_message_sent = True
                connection_failure_message_sent = False
                feedback_client_logger.info("Network Reconnection Successful")

            if are_services_running:
                services_ping_fail = 0
                if not project_restart_message_sent:
                    message_queue.clear()
                    messages_to_display.clear()
                    socketio.emit('new_message', {'message': [{'SUCCESS': "✅ Facial Recognition Attendance System is Getting Started. Please Wait for 2 Minutes ✅"}]})
                    feedback_client_logger.info("Facial Recognition Attendance System is Getting Started. Please Wait for 2 Minutes")
                    time.sleep(2 * 60)

                    socketio.emit('new_message', {'message': [{'SUCCESS': "Facial Recognition Attendance System Has Been Started. Please Resume Attendance"}]})
                    feedback_client_logger.info("Facial Recognition Attendance System Has Been Started. Please Resume Attendance")
                    project_restart_message_sent = True
                    project_services_down_message_sent = False
                    time.sleep(5)

                if kafka_consumer is None:
                    try:
                        kafka_consumer = Consumer(kafka_conf)
                        topic_partition = TopicPartition(CONFLUENT_KAFKA_TOPIC, CONFLUENT_KAFKA_PARTITION, OFFSET_END)
                        kafka_consumer.assign([topic_partition])
                        feedback_client_logger.info("Created a fresh Kafka consumer")
                    except Exception as e:
                        feedback_client_logger.exception("Failed to initialize Kafka Consumer")
                        kafka_consumer = None
                        time.sleep(5)
                        continue

                try:
                    message_from_kafka = kafka_consumer.poll(0.2)
                except Exception as e:
                    feedback_client_logger.exception("Kafka polling failed")
                    kafka_consumer = None
                    continue

                if message_from_kafka is None:
                    pass
                    # feedback_client_logger.debug("No message received from Kafka")
                elif message_from_kafka.error():
                    message_queue.clear()
                    feedback_client_logger.error(f"Kafka error: {message_from_kafka.error()}")
                else:
                    try:
                        # key = message_from_kafka.key().decode()
                        decoded_kafka_message = message_from_kafka.value().decode()
                        # timestamp_type, timestamp_milliseconds = message_from_kafka.timestamp()
                        # timestamp = datetime.fromtimestamp(timestamp_milliseconds / 1000.0).strftime("%d-%m-%Y %I:%M:%S.%f %p")
                        # feedback_client_logger.debug(f"[KAFKA] Key = {key}, Message = {decoded_kafka_message}, Timestamp = {timestamp}")

                        if decoded_kafka_message.startswith(("ERROR", "SUCCESS")):
                            message_queue = []
                            message_queue.insert(0, decoded_kafka_message)
                        else:
                            if decoded_kafka_message not in message_queue:
                                if len(message_queue) >= DISPLAY_LIST_SIZE:
                                    message_queue.pop()
                                message_queue.insert(0, decoded_kafka_message)

                        # print(message_queue)
                        data = {"messages": message_queue}
                        post_message_queue_and_get_images(data)
                        # message_queue.clear()
                        last_update_time = time.time()
                    except Exception as e:
                        feedback_client_logger.exception("Error processing Kafka message")
            else:
                services_ping_fail += 1
                if services_ping_fail >= NETWORK_FAIL_CHECK and not project_services_down_message_sent:
                    message_queue.clear()
                    messages_to_display.clear()
                    socketio.emit('new_message', {'message': [{'ERROR': "⚠️ Facial Recognition System Temporarily Down for Maintenance or Due to an Unexpected Issue ⚠️"}]})
                    project_services_down_message_sent = True
                    project_restart_message_sent = False
                    feedback_client_logger.error("Facial Recognition System Temporarily Down for Maintenance or Due to an Unexpected Issue")
                    kafka_consumer = None
                    services_ping_fail = 0
        else:
            server_ping_fail += 1
            if server_ping_fail >= NETWORK_FAIL_CHECK and not connection_failure_message_sent:
                message_queue.clear()
                messages_to_display.clear()
                socketio.emit('new_message', {'message': [{'ERROR': "⚠️ Network Issue: Unable to Connect to the Server ⚠️"}]})
                connection_failure_message_sent = True
                successful_reconnection_message_sent = False
                project_services_down_message_sent = False
                feedback_client_logger.error("Network Issue: Unable to Connect to the Server")
                kafka_consumer = None
                server_ping_fail = 0

        if time.time() - last_update_time >= STATUS_THRESHOLD:
            message_queue.clear()
            messages_to_display.clear()
            socketio.emit('new_message', {'message': messages_to_display})
            last_update_time = time.time()

def consume_messages():
    try:
        _consume_messages()
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

kafka_consumer_thread = threading.Thread(target=consume_messages)
kafka_consumer_thread.daemon = True
kafka_consumer_thread.start()

watchdog_thread = threading.Thread(target=monitor_consumer_thread, args=(kafka_consumer_thread,))
watchdog_thread.daemon = True
watchdog_thread.start()

@socketio.on('connect')
def handle_connect():
    emit('new_message', {'message': messages_to_display})

@socketio.on('check_updates')
def handle_check_updates():
    emit('new_message', {'message': messages_to_display})

@app.route('/')
def index():
    return render_template('index.html', message=messages_to_display)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9090)
