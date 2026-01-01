import paho.mqtt.client as mqtt
import time
import re
import taos
from taos import connect

# MQTT broker settings
BROKER_ADDRESS = "localhost"
BROKER_PORT = 1883
TOPICS = [
    "factory/oee/performance",
    "factory/equipment/cnc/1/temperature",
    "factory/oee/quality"
]

# TDengine connection settings
TDENGINE_HOST = "localhost"
TDENGINE_PORT = 6030
TDENGINE_USER = "root"
TDENGINE_PASSWORD = "taosdata"
TDENGINE_DB = "datadb"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        for topic in TOPICS:
            client.subscribe(topic)
            print(f"Subscribed to topic: {topic}")
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    timestamp = int(time.time() * 1000)  # milliseconds since epoch
    payload_str = msg.payload.decode()
    # Extract numerical value from payload string
    match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", payload_str)
    if match:
        value = float(match.group(0))
    else:
        print("No numerical value found in payload")
        return

    tag = msg.topic
    table_name = tag.replace('/', '_')

    try:
        # Connect to TDengine
        conn = connect(host=TDENGINE_HOST, user=TDENGINE_USER, password=TDENGINE_PASSWORD, database=TDENGINE_DB, port=TDENGINE_PORT)
        cursor = conn.cursor()

        # Insert data into TDengine table
        sql = f"INSERT INTO {table_name} USING factorydata TAGS ('{tag}') VALUES ({timestamp}, {value})"
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Inserted data into TDengine: timestamp={timestamp}, tag={tag}, value={value}")
    except Exception as e:
        print(f"Failed to insert data into TDengine: {e}")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()
