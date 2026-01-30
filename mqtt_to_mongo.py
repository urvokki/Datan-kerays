# mqtt_to_mongo.py
import os
import json
import time
import signal
from datetime import datetime
from pathlib import Path

import logging
from dotenv import load_dotenv
from pymongo import MongoClient, errors
import paho.mqtt.client as mqtt

# --- config & logging ---
load_dotenv()  # lukee .env tiedoston

MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASS = os.getenv("MQTT_PASS")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "aiotgarage/+/+/presence")

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "presence_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "presence")

FAILED_QUEUE_FILE = Path("failed_queue.jsonl")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mqtt-to-mongo")

# validate required vars
if not MQTT_HOST or not MONGO_URI:
    log.error("Puuttuva konfiguraatio. Tarkista .env (MQTT_HOST ja MONGO_URI pakollisia).")
    raise SystemExit(1)

# --- MongoDB yhteys ---
try:
    mongo_client = MongoClient(MONGO_URI)
    # kevyt yhteyden tarkistus
    mongo_client.admin.command("ping")
    db = mongo_client[MONGO_DB]
    coll = db[MONGO_COLLECTION]
    log.info("Yhdistetty MongoDB:hen (%s.%s)", MONGO_DB, MONGO_COLLECTION)
except Exception as e:
    log.exception("MongoDB-yhteys epäonnistui: %s", e)
    raise SystemExit(1)

# --- MQTT callbacks ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("MQTT yhdistetty, tila: rc=%s. Tilataan topic: %s", rc, MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC, qos=1)
    else:
        log.error("MQTT connect epäonnistui, rc=%s", rc)

def enqueue_failed(record: dict):
    # Lisää epäonnistunut viesti tiedostoon (later retry)
    try:
        with FAILED_QUEUE_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        log.exception("failed_queue append epäonnistui")

def insert_record(record: dict):
    try:
        coll.insert_one(record)
        log.info("Tallennettu dokumentti: %s", record.get("topic", "<no-topic>"))
    except errors.PyMongoError:
        log.exception("Mongo insert epäonnistui, puskuroidaan tiedostoon")
        enqueue_failed(record)

def on_message(client, userdata, msg):
    print("MQTT-viesti vastaanotettu:", msg.topic, msg.payload)

    payload_raw = msg.payload.decode("utf-8", errors="replace")
    try:
        payload = json.loads(payload_raw)
    except Exception:
        payload = {"raw_payload": payload_raw}

    record = {
        "topic": msg.topic,
        "payload": payload,
        "_received_at": datetime.utcnow().isoformat() + "Z"
    }
    insert_record(record)


# --- MQTT client setup ---
mqtt_client = mqtt.Client()
if MQTT_USER:
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# try connecting
def start_mqtt():
    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
        log.info("MQTT loop started")
    except Exception:
        log.exception("MQTT connect failed")
        raise

# --- graceful shutdown ---
def stop_all(signum=None, frame=None):
    log.info("Suljetaan yhteydet...")
    try:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    except Exception:
        pass
    try:
        mongo_client.close()
    except Exception:
        pass
    log.info("Valmis. Lopetetaan.")
    raise SystemExit(0)

signal.signal(signal.SIGINT, stop_all)
signal.signal(signal.SIGTERM, stop_all)

if __name__ == "__main__":
    start_mqtt()
    # pääsilmukka: pidetään prosessi elossa
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_all()
