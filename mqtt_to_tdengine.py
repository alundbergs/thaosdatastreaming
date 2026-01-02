import paho.mqtt.client as mqtt
import time
import re
import requests
from requests.auth import HTTPBasicAuth

# MQTT broker settings
BROKER_ADDRESS = "localhost"
BROKER_PORT = 1883
TOPICS = [
    "factory/oee/performance",
    "factory/equipment/cnc/1/temperature",
    "factory/oee/quality"
]

# TDengine REST API settings
TDENGINE_REST_URL = "http://localhost:6041/rest/sql/datadb"  # RESTful API URL with database name included
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

def insert_data_rest(sql):
    try:
        headers = {
            "Content-Type": "text/plain"
        }
        print(f"Sending SQL to TDengine REST API: {sql}")
        response = requests.post(
            TDENGINE_REST_URL,
            data=sql,
            headers=headers,
            auth=HTTPBasicAuth(TDENGINE_USER, TDENGINE_PASSWORD)
        )
        print(f"REST API response status: {response.status_code}, response text: {response.text}")
        if response.status_code == 200:
            print(f"Successfully inserted data with SQL: {sql}")
        else:
            print(f"Failed to insert data. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Exception during REST API call: {e}")

import json

def on_message(client, userdata, msg):
    timestamp = int(time.time() * 1000)  # milliseconds since epoch
    payload_str = msg.payload.decode()
    try:
        payload_json = json.loads(payload_str)
        value = float(payload_json.get("value"))
        print(value)
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        print(f"Failed to parse JSON or extract 'value': {e}")
        return

    tag = msg.topic
    table_name = tag.replace('/', '_')

    # Construct SQL insert statement for REST API without database name (since it's in URL)
    sql = f"INSERT INTO {table_name} USING factorydata TAGS ('{tag}') VALUES ({timestamp}, {value})"
    insert_data_rest(sql)

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()
