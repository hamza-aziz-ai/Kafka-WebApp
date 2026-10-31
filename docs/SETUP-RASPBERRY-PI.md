# First Time Raspberry Pi Configuration as a Recognition Message Displayer

> This is the setup for running on a Raspberry Pi without Docker. Docker is the current way to run the app: see the [quick start](../README.md#quick-start). The `parameters.py` block below predates the environment settings, so check it against [`parameters.py`](../parameters.py) itself.

## 1. Installing OS

Click the following link to get started with your Raspberry Pi:

<https://www.raspberrypi.com/documentation/computers/getting-started.html>

Additionally, you can walk through the following link to do your custom configurations:

<https://www.raspberrypi.com/documentation/computers/configuration.html>

> NOTE: You must give the following specified ***username*** and ***hostname*** while OS installation for the successful setup of your Raspberry Pi. If not given so, you may encounter several issues.

- **username:** *raspberry*
- **hostname:** *raspberry*

## 2. Enabling Kafka Consumer's Flask Application Service

### The Structure of Flask Application

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

To get the `Kafka-WebApp` onto the Raspberry Pi, you can clone the repository from GitHub.

```bash
# Clone GitHub Repository
git clone https://github.com/hamza-aziz-ai/Kafka-WebApp.git

# Change directpry to Kafka-WebApp
cd Kafka-WebApp
```

### The `parameters.py` file

The [`parameters.py`](../parameters.py) file contains important information to setup your flask application. You need to change these as per your requiremnets.

```python
# Project Services
DGX_IP_ADDRESS = <IP Address of DGX>    # e.g., "10.0.0.5"

PROJECT_SERVICES_PORT = [
    6385, 19000, 19001,     # InsightFace
    19530,                  # Milvus Vector database
    9092,                   # Kafka Broker
    6381,                   # Redis Database
    20001,                  # Database-Controller
    20012,                  # Kafka-Message-Controller
    20013,                  # Display-Image-Server
    6386, 6970, 6971        # Face Liveness Detection
]

# Kafka IP and Port
CONFLUENT_KAFKA_IP_ADDRESS = DGX_IP_ADDRESS
CONFLUENT_KAFKA_PORT = 9092

# Kafka topic for messages
CONFLUENT_KAFKA_TOPIC = "feedback_messages_topic_for_<Replace with Camera-Name>"    # e.g., "feedback_messages_topic_for_library-gate"

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
```

Once you made these, the next step is install the requirements required by the `Kafka-WebApp`

### The `requirements.txt` file

The [`requirements.txt`](../requirements.txt) file contains the packages/libraries required for the **Kafka-WebApp**

You can install these requirements by running the following command on the Raspberry Pi terminal

```bash
# Create and Activate a Python Virtual Environment
python3 -m venv venv
source venv/bin/activate
```

```bash
# Upgrade pip and install the requirements
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

Once you made these changes you are ready to setup your flask application service.

### The *Flask Application Service*

#### Step - 1) Create `flask-application-startup.sh`

This file is responsible for starting the flask application.

First, copy the below contents.

```bash
#!/bin/bash

# Change directory to Kafka Web Application
cd /home/raspberry/Kafka-WebApp

# Activate the virtual environment
source venv/bin/activate

# Start the application at a specified port and open it to all incoming networks
python3 -m flask --app app.py run --host 0.0.0.0 --port 9090
```

Now, create a `flask-application-startup.sh` file by running the following command and paste the copied contents into it.

```bash
sudo nano /usr/local/bin/flask-application-startup.sh
```

#### Step - 2) Create `flask-application-startup.service`

This file is responsible for creating a service for our flask application which will help in automatically start the application on system reboot.

Copy the below contents.

```service
[Unit]
Description=Kafka Web Application

[Service]
ExecStart=/usr/local/bin/flask-application-startup.sh

[Install]
WantedBy=multi-user.target
```

Now, create a `flask-application-startup.service` file by running the following command and paste the copied contents into it.

```bash
sudo nano /etc/systemd/system/flask-application-startup.service
```

### Step - 3) Start and Enable the Service

Reload the daemon

```bash
sudo systemctl daemon-reload
```

Start the service

```bash
sudo systemctl start flask-application-startup.service
```

Check the Status of the Service

```bash
sudo systemctl status flask-application-startup.service
```

Enable the service to automatically restart on system reboot

```bash
sudo systemctl enable flask-application-startup.service
```

## 3. The KIOSK Mode

### Step - 1) Install the Chromium Browser and Other Packages

```bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install chromium-browser unclutter xdotool
```

### Step - 2) Create `kiosk-startup.sh`

This file is responsible for starting the Chromium Browser in KIOSK Mode.

Copy the below contents.

```bash
#!/bin/bash

xset s noblank
xset s off
xset -dpms

unclutter -idle 0.5 -root &

sed -i 's/"exited_cleanly":false/"exited_cleanly":true/' /home/raspberry/.config/chromium/Default/Preferences
sed -i 's/"exit_type":"Crashed"/"exit_type":"Normal"/' /home/raspberry/.config/chromium/Default/Preferences

/usr/bin/chromium-browser --noerrdialogs --disable-infobars --disable-application-cache --use-gl=egl --enable-gpu-rasterization --enable-native-gpu-memory-buffers --disable-software-rasterizer --kiosk http://localhost:9090 &  # Adjust the URL

while true; do
    xdotool keydown ctrl+Tab; xdotool keyup ctrl+Tab;
    sleep 30
done
```

Now, create a `kiosk-startup.sh` file by running the following command and paste the copied contents into it.

```bash
sudo nano /usr/local/bin/kiosk-startup.sh
```

### Step - 3) Create `kiosk-startup.service`

This file is responsible for creating a service for our kiosk startup which will help in automatically start the kiosk on system reboot.

Copy the below contents.

```service
[Unit]
Description=Kiosk Mode Flask Application
Wants=graphical.target
After=graphical.target

[Service]
Environment=DISPLAY=:0.0
Environment=XAUTHORITY=/home/raspberry/.Xauthority
Type=simple
ExecStart=/bin/bash /usr/local/bin/kiosk-startup.sh
Restart=on-abort
User=raspberry
Group=raspberry

[Install]
WantedBy=graphical.target
```

Now, create a `kiosk-startup.service` file by running the following command and paste the copied contents into it.

```bash
sudo nano /etc/systemd/system/kiosk-startup.service
```

### Step - 4) Start and Enable the Service

Reload the daemon

```bash
sudo systemctl daemon-reload
```

Start the service

```bash
sudo systemctl start kiosk-startup.service
```

Check the Status of the Service

```bash
sudo systemctl status kiosk-startup.service
```

Enable the service to automatically restart on system reboot

```bash
sudo systemctl enable kiosk-startup.service
```

## 4. Reboot the System

To make these settings effect on the system, we will bw needing to restart the system. You can do this by running the following command in your terminal.

```bash
sudo reboot -h now
```

