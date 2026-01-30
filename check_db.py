# check_db.py
from dotenv import load_dotenv
load_dotenv()

from pymongo import MongoClient
import os, json

uri = os.getenv("MONGO_URI")
if not uri:
    print("MONGO_URI ei löytynyt .env:stä. Tarkista tiedosto ja polku.")
else:
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # Yritetään pingata palvelinta
        client.admin.command("ping")
        print("Yhteys MongoDB:hen OK")
        dbname = os.getenv("MONGO_DB", "presence_db")
        db = client[dbname]
        cols = db.list_collection_names()
        print("Tietokannat:", client.list_database_names())
        print(f"Kokoelmat tietokannassa '{dbname}':", cols)
        if "presence" in cols:
            coll = db["presence"]
            docs = list(coll.find().sort([("_received_at", -1)]).limit(5))
            print("Uusimmat dokumentit (enintään 5):")
            for d in docs:
                d["_id"] = str(d["_id"])
                print(json.dumps(d, ensure_ascii=False))
        else:
            print("Kokoelmaa 'presence' ei löytynyt tässä tietokannassa.")
    except Exception as e:
        print("Yhteyden testaus epäonnistui:", repr(e))
