// index.js
require('dotenv').config();
const mqtt = require('mqtt');
const { MongoClient } = require('mongodb');

const MQTT_URL = process.env.MQTT_URL;     // esim. mqtt://automaatio.cloud.shiftr.io:1883
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;
const MQTT_TOPIC = process.env.MQTT_TOPIC || 'automaatio/#';

const MONGO_URI = process.env.MONGO_URI;   // Atlas connection string (korvaa käyttäjä/salasana)
const MONGO_OPTIONS = { useNewUrlParser: true, useUnifiedTopology: true };

const VALID_NAME_RE = /^[A-Za-z0-9_-]{1,64}$/;

let mongoClient;
let mqttClient;

async function start() {
  // 1) Yhdistä MongoDB Atlas -asiakkaaseen
  mongoClient = new MongoClient(MONGO_URI, MONGO_OPTIONS);
  await mongoClient.connect();
  console.log('Connected to MongoDB Atlas');

  // 2) Yhdistä MQTT-brokeriin
  mqttClient = mqtt.connect(MQTT_URL, {
    username: MQTT_USER,
    password: MQTT_PASS,
    reconnectPeriod: 5000
  });

  mqttClient.on('connect', () => {
    console.log('Connected to MQTT');
    mqttClient.subscribe(MQTT_TOPIC, { qos: 1 }, (err) => {
      if (err) console.error('Subscribe error', err);
      else console.log('Subscribed to', MQTT_TOPIC);
    });
  });

  mqttClient.on('message', async (topic, message) => {
    let obj;
    try {
      const txt = message.toString('utf8');
      obj = JSON.parse(txt);
    } catch (e) {
      // Jos viesti ei ole JSON, tallenna raakatiedot kenttään raw_payload
      obj = { raw_payload: message.toString('utf8') };
    }

    // Validoi tai aseta oletus DB/collection-nimille
    const dbname = (obj.db_name && VALID_NAME_RE.test(obj.db_name)) ? obj.db_name : 'presence_db';
    const collName = (obj.coll_name && VALID_NAME_RE.test(obj.coll_name)) ? obj.coll_name : 'presence';

    // Lisää ISO-aikaleima (koneystävällinen)
    obj._received_at = new Date().toISOString();

    try {
      const db = mongoClient.db(dbname);
      const coll = db.collection(collName);
      await coll.insertOne(obj);
      console.log('Inserted into', `${dbname}.${collName}`);
    } catch (e) {
      console.error('Mongo insert failed:', e.message);
      // Jos haluat: lisää lokitiedostoon tai puskurin myöhempiä lähetyksiä varten.
    }
  });

  mqttClient.on('error', (err) => {
    console.error('MQTT error:', err.message);
  });

  process.on('SIGINT', async () => {
    console.log('Shutting down...');
    try { if (mqttClient) mqttClient.end(true); } catch(e) {}
    try { if (mongoClient) await mongoClient.close(); } catch(e) {}
    process.exit(0);
  });
}

start().catch(err => {
  console.error('Startup failed:', err.message);
  process.exit(1);
});
