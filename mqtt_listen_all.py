# mqtt_listen_all.py
import paho.mqtt.client as mqtt

HOST = "automaatio.cloud.shiftr.io"
PORT = 1883
USER = "automaatio"
PASS = "Z0od2PZF65jbtcXu"
TOPIC = "#"   # kaikki topicit

def on_connect(client, userdata, flags, rc):
    print("Connected to broker rc=", rc)
    client.subscribe(TOPIC, qos=1)
    print("Subscribed to", TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8', errors='replace')
    except:
        payload = str(msg.payload)
    print("MSG:", msg.topic, payload)

client = mqtt.Client()
client.username_pw_set(USER, PASS)
client.on_connect = on_connect
client.on_message = on_message
client.connect(HOST, PORT, keepalive=60)
client.loop_forever()
