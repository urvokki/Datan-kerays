# mqtt_to_mongo.py
import os
import json
import time
import signal
import re
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
    log.info("Yhdistetty MongoDB:hen (connection OK)")
except Exception as e:
    log.exception("MongoDB-yhteys epäonnistui: %s", e)
    raise SystemExit(1)

# --- util ---
VALID_NAME_RE = re.compile(r'^[A-Za-z0-9_-]{1,64}$')

def enqueue_failed(record: dict):
    try:
        with FAILED_QUEUE_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        log.exception("failed_queue append epäonnistui")

# --- MQTT callbacks ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("MQTT yhdistetty, tila: rc=%s. Tilataan topic: %s", rc, MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC, qos=1)
    else:
        log.error("MQTT connect epäonnistui, rc=%s", rc)

def on_message(client, userdata, msg):
    # DEBUG: näytä että viesti tuli (ja topic)
    print("MQTT-viesti vastaanotettu:", msg.topic, msg.payload)

    payload_raw = msg.payload.decode("utf-8", errors="replace")
    try:
        payload = json.loads(payload_raw)
    except Exception:
        payload = {"raw_payload": payload_raw}

    # Normalisoi kentänimi "person count" -> "person_count"
    if isinstance(payload, dict):
        if "person count" in payload and "person_count" not in payload:
            try:
                payload["person_count"] = int(payload.pop("person count"))
            except Exception:
                payload["person_count"] = payload.pop("person count")
        # alias id -> sensor_id
        if "id" in payload and "sensor_id" not in payload:
            payload["sensor_id"] = payload.get("id")

    # db/collection valinta viestistä tai ympäristöasetuksesta
    db_name = payload.get("db_name") if isinstance(payload, dict) else None
    coll_name = payload.get("coll_name") if isinstance(payload, dict) else None

    if not db_name or not isinstance(db_name, str) or not VALID_NAME_RE.match(db_name):
        db_name = MONGO_DB
    if not coll_name or not isinstance(coll_name, str) or not VALID_NAME_RE.match(coll_name):
        coll_name = MONGO_COLLECTION

    # Lisää DateTime jos puuttuu (ihmisluettava)
    if isinstance(payload, dict) and "DateTime" not in payload:
        payload["DateTime"] = datetime.utcnow().strftime("%d %b %Y %H:%M:%S")

    document = payload.copy() if isinstance(payload, dict) else {"raw_payload": payload}
    document["_received_at"] = datetime.utcnow().isoformat() + "Z"
    document["topic"] = msg.topic

    # Yksi talletuskutsu — ei tuplaa
    try:
        db = mongo_client[db_name]
        coll = db[coll_name]
        coll.insert_one(document)
        log.info("Tallennettu dokumentti: %s.%s (topic=%s)", db_name, coll_name, msg.topic)
    except Exception as e:
        log.exception("Mongo insert epäonnistui: %s", e)
        enqueue_failed({"db": db_name, "coll": coll_name, "document": document})

# --- MQTT client setup ---
mqtt_client = mqtt.Client()
if MQTT_USER:
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

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
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_all()
