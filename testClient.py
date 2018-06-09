import json
import logging
import random
import time
import paho.mqtt.client as mqtt
from LoMqttConf import *


log_level = 'INFO'
logging.basicConfig(level=getattr(logging, log_level), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

Connected = False


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info('Connected to MQTT broker')
        global Connected
        Connected = True
    else:
        logger.error('Connected with result code ' + str(rc))


def on_message(client, userdata, msg):
    msg_payload = json.loads(msg.payload)
    logger.info('Message: {} received on topic: {}'.format(msg_payload, msg.topic))
    if msg.topic == 'dev/cmd':
        response = {
            "res": {
                "received_cmd": True,
                },
            "cid": msg_payload['cid']
            }
        logger.info('Sending cmd confirmation: {}'.format(json.dumps(response)))
        client.publish(topic='dev/cmd/res', payload=json.dumps(response))


client = mqtt.Client('urn:lo:nsid:python:001')
client.username_pw_set(username="json+device", password=LO_USER_APIK)
client.on_connect = on_connect
client.on_message = on_message
client.connect(LO_MQTT_IP, LO_MQTT_PORT, 10)
client.loop_start()


while Connected is not True:  # Wait for connection
    time.sleep(0.1)

client.subscribe('dev/cmd')

try:
    n = 0
    while True:
        time.sleep(1)
        if n > 20:
            msg = {
                "s": "urn:lo:nsid:python:01!temperature",
                "m": "Python_emulator_01",
                "v": {
                    "temp": round(random.random() * 100),
                    "hum": round(random.random() * 100)
                },
                "t": ["City.MOW", "Python", "Sensor.Type.Humidity"]
            }
            logger.info('Sending msg: {}'.format(json.dumps(msg)))
            client.publish(topic='dev/data', payload=json.dumps(msg))
            n = 0
        n = n + 1

except KeyboardInterrupt:
    logger.info('Exiting the script. Keyboard interrupt recieved')
    client.disconnect()
    client.loop_stop()
