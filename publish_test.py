# publish_test.py
import paho.mqtt.publish as publish
publish.single("aiotgarage/lab/room1/presence",
               payload='{"sensor_id":"pir_01","count":1}',
               hostname="automaatio.cloud.shiftr.io",
               port=1883,
               auth={"username":"automaatio","password":"Z0od2PZF65jbtcXu"})
