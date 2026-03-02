# mqtt_to_mongo.py
# Updated: uses timezone-aware UTC for _received_at and also stores local iso string.
# Accepts payloads in JSON or simple key/value text, normalizes fields and stores only allowed collections.
# Configuration via .env:
#   MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASS, MQTT_TOPIC
#   MONGO_URI, MONGO_DB (default), MONGO_COLLECTION (default)
#   SENSOR_TZ (optional, default "Europe/Helsinki")

import os
import re
import json
import time
import signal
import logging
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient, errors
import paho.mqtt.client as mqtt
from dateutil import parser, tz

# --- load config ---
load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASS = os.getenv("MQTT_PASS")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "automaatio")  # subscribe to sensor topic root (change if needed)

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "presence_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "presence")

# Sensor timezone for parsing naive sensor DateTime strings
SENSOR_TZNAME = os.getenv("SENSOR_TZ", "Europe/Helsinki")
DEFAULT_SENSOR_TZ = tz.gettz(SENSOR_TZNAME)

FAILED_QUEUE_FILE = Path("failed_queue.jsonl")

# --- logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mqtt-to-mongo")

# --- validation regex ---
VALID_NAME_RE = re.compile(r'^[A-Za-z0-9_-]{1,64}$')

# --- sanity checks ---
if not MQTT_HOST or not MONGO_URI:
    log.error("Puuttuva konfiguraatio. Varmista .env: MQTT_HOST ja MONGO_URI asetetut.")
    raise SystemExit(1)

# --- MongoDB client setup ---
try:
    mongo_client = MongoClient(MONGO_URI)
    mongo_client.admin.command("ping")
    log.info("Yhdistetty MongoDB:hen (connection OK)")
except Exception as e:
    log.exception("MongoDB-yhteys epäonnistui: %s", e)
    raise SystemExit(1)

# --- helper functions for parsing and normalization ---

def enqueue_failed(record: dict):
    try:
        with FAILED_QUEUE_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        log.exception("failed_queue append epäonnistui")

def try_int(s):
    try:
        return int(float(str(s)))
    except Exception:
        return s

def try_float(s):
    try:
        return float(str(s))
    except Exception:
        return s

def try_number_if_possible(s):
    # attempt int -> float -> keep string
    try:
        iv = int(s)
        return iv
    except Exception:
        pass
    try:
        fv = float(s)
        return fv
    except Exception:
        return s

KV_KEY_RE = re.compile(r'^[A-Za-z0-9 _-]+$')

def parse_kv_text(text: str) -> dict:
    """
    Parse simple key/value text blocks into a dict.
    Accepts formats:
      Key\nValue
      Key: Value
      Key Value
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip() != ""]
    i = 0
    out = {}
    while i < len(lines):
        line = lines[i]
        # Key: Value or Key = Value
        if ':' in line or '=' in line:
            sep = ':' if ':' in line else '='
            k, v = map(str.strip, line.split(sep, 1))
            out[k] = v.strip('"').strip("'")
            i += 1
            continue
        # Key Value on same line
        parts = line.split(maxsplit=1)
        if len(parts) == 2 and not parts[0].isdigit():
            k, v = parts[0].strip(), parts[1].strip()
            out[k] = v.strip('"').strip("'")
            i += 1
            continue
        # Key on its own line, next line is value
        if i + 1 < len(lines):
            next_line = lines[i + 1]
            # if next line looks like a value (numeric or quoted) or not like a key -> take it
            if next_line.startswith('"') or next_line.startswith("'") or re.match(r'^[-+]?[0-9]*\.?[0-9]+$', next_line) or not KV_KEY_RE.match(next_line):
                out[line] = next_line.strip().strip('"').strip("'")
                i += 2
                continue
        # fallback: treat whole line as key with empty value
        out[line] = ""
        i += 1
    return out

def normalize_parsed(d: dict) -> dict:
    """
    Normalize parsed key-value pairs into desired fields and types.
    """
    out = {}
    for k, v in d.items():
        key = k.strip()
        lk = key.lower()
        if lk in ("pcount", "p_count", "person count", "person_count"):
            out["person_count"] = try_int(v)
        elif lk in ("id",):
            out["sensor_id"] = v
        elif lk in ("sensor_id",):
            out["sensor_id"] = v
        elif lk in ("db_name", "dbname"):
            out["db_name"] = v
        elif lk in ("coll_name", "collname"):
            out["coll_name"] = v
        elif lk in ("t", "temp", "temperature"):
            out.setdefault("DateTime", v)
            out["temperature"] = try_float(v)
        elif lk in ("h", "humidity"):
            out["humidity"] = try_float(v)
        elif lk in ("dp", "dewpoint"):
            out["dew_point"] = try_float(v)
        elif lk in ("co2",):
            out["co2"] = try_int(v)
        elif lk in ("time", "time_", "Time", "Time:"):
            out.setdefault("DateTime", v)
        else:
            safe = key.replace(" ", "_")
            out[safe] = try_number_if_possible(v)
    return out

def parse_sensor_datetime(raw):
    """
    Parse sensor-provided DateTime into tz-aware UTC datetime.
    Accepts epoch (digit string) or various human formats.
    Returns datetime in UTC or None.
    """
    if raw is None:
        return None
    s = str(raw).strip().strip('"').strip("'")
    if s.isdigit():
        try:
            return datetime.fromtimestamp(int(s), tz=timezone.utc)
        except Exception:
            return None
    try:
        dt = parser.parse(s)
    except Exception:
        return None
    if dt.tzinfo is None:
        # assume sensor local tz if none provided
        dt = dt.replace(tzinfo=DEFAULT_SENSOR_TZ)
    try:
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

# --- MQTT callbacks and logic ---

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("MQTT yhdistetty, tila: rc=%s. Tilataan topic: %s", rc, MQTT_TOPIC)
        try:
            client.subscribe(MQTT_TOPIC, qos=1)
        except Exception:
            log.exception("Subscribe epäonnistui")
    else:
        log.error("MQTT connect epäonnistui, rc=%s", rc)

def on_message(client, userdata, msg):
    """
    Accept only messages that target data_ml.p_count or data_ml.p_count_2.
    Parse JSON or key/value text, normalize fields, parse datetimes.
    """
    log.debug("MQTT-viesti vastaanotettu: %s", msg.topic)
    payload_raw = msg.payload.decode("utf-8", errors="replace").strip()
    if not payload_raw:
        log.debug("Tyhjä payload topicista %s — ohitetaan", msg.topic)
        return

    parsed = None

    # Try JSON first
    try:
        parsed = json.loads(payload_raw)
        if not isinstance(parsed, dict):
            parsed = {"raw_payload": parsed}
    except Exception:
        # Try key/value text parsing
        try:
            kv = parse_kv_text(payload_raw)
            parsed = normalize_parsed(kv)
        except Exception as e:
            log.debug("Payload ei tunnistettu, tallennetaan raw? Ohitetaan. Err: %s", e)
            return

    if not isinstance(parsed, dict):
        log.debug("Parsed ei dict — ohitetaan")
        return

    # require db_name and coll_name to be present and valid
    db_name = parsed.get("db_name")
    coll_name = parsed.get("coll_name")
    if not (isinstance(db_name, str) and isinstance(coll_name, str) and VALID_NAME_RE.match(db_name) and VALID_NAME_RE.match(coll_name)):
        # If absent or invalid, ignore message (user requested filtering)
        log.debug("Puuttuva/virheellinen db_name/coll_name — ohitetaan. Keys=%s", list(parsed.keys()))
        return

    # Accept only specific target collections
    if not (db_name == "data_ml" and coll_name in ("p_count", "p_count_2")):
        log.debug("Ei sallittu kohde db/coll: %s.%s — ohitetaan", db_name, coll_name)
        return

    # Normalize fields: convert "person count" -> person_count, id -> sensor_id
    if "person count" in parsed and "person_count" not in parsed:
        try:
            parsed["person_count"] = int(parsed.pop("person count"))
        except Exception:
            parsed["person_count"] = parsed.pop("person count")
    if "id" in parsed and "sensor_id" not in parsed:
        parsed["sensor_id"] = parsed.get("id")

    # Parse sensor DateTime into UTC
    sensor_raw = parsed.get("DateTime") or parsed.get("Time") or parsed.get("time")
    if sensor_raw:
        sensor_dt = parse_sensor_datetime(sensor_raw)
        if sensor_dt:
            parsed["_sensor_datetime_utc"] = sensor_dt  # datetime object (UTC)
            parsed["DateTime_parsed"] = sensor_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build document, include received times
    document = parsed.copy()
    now_utc = datetime.now(timezone.utc)  # timezone-aware UTC datetime
    document["_received_at"] = now_utc     # PyMongo will store as BSON datetime
    # store local iso string for convenient viewing (does not replace UTC)
    local_tz = tz.gettz(SENSOR_TZNAME)
    if local_tz:
        document["_received_at_local"] = now_utc.astimezone(local_tz).isoformat()
    else:
        document["_received_at_local"] = now_utc.isoformat()

    document["topic"] = msg.topic

    # Insert one (single) time
    try:
        db = mongo_client[db_name]
        coll = db[coll_name]
        coll.insert_one(document)
        log.info("Tallennettu dokumentti: %s.%s (sensor=%s, count=%s)", db_name, coll_name,
                 document.get("sensor_id"), document.get("person_count"))
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
