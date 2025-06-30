#!/usr/bin/env python3
"""
bombers_bot.py

Publica (o simula) las intervenciones más relevantes de Bombers
priorizando fase “actiu”, nº dotacions y tipo d'incendi.

Requisitos:
    requests  geopy  tweepy>=4.0.0  pyproj
"""

#!/usr/bin/env python3
import os, json, logging, requests, tweepy
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from geopy.geocoders import Nominatim
from pyproj import Transformer

LAYER_URL = ("https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/"
             "ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0")
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "3"))
IS_TEST_MODE  = os.getenv("IS_TEST_MODE", "true").lower() == "true"
API_KEY       = os.getenv("ARCGIS_API_KEY", "")
MAPA_OFICIAL  = "https://interior.gencat.cat/ca/arees_dactuacio/bombers/actuacions-de-bombers/"

STATE_FILE = Path("state.json")
GEOCODER   = Nominatim(user_agent="bombers_bot")
TRANSFORM  = Transformer.from_crs(25831, 4326, always_xy=True)

TW_KEYS = {
    "ck": os.getenv("TW_CONSUMER_KEY"),
    "cs": os.getenv("TW_CONSUMER_SECRET"),
    "at": os.getenv("TW_ACCESS_TOKEN"),
    "as": os.getenv("TW_ACCESS_SECRET"),
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# ---------- estado ----------
def load_state(): return json.loads(STATE_FILE.read_text())["last_id"] if STATE_FILE.exists() else -1
def save_state(i): STATE_FILE.write_text(json.dumps({"last_id": i}))

# ---------- ArcGIS ----------
def fetch_features(limit=100):
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": (
            "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2"
        ),
        "orderByFields": "ACT_DAT_ACTUACIO DESC",  # ← corrección
        "resultRecordCount": limit,
        "returnGeometry": "true",
        "cacheHint": "true",
    }
    if API_KEY:
        params["token"] = API_KEY
    r = requests.get(f"{LAYER_URL}/query", params=params, timeout=15)
    data = r.json()
    if "error" in data:
        logging.error("ArcGIS error %s: %s", data["error"]["code"], data["error"]["message"])
        return []
    return data.get("features", [])

# ---------- utilidades ----------
def tipo_val(a):
    d = (a.get("TAL_DESC_ALARMA1","")+" "+a.get("TAL_DESC_ALARMA2","")).lower()
    return 1 if "forestal" in d or "vegetació" in d else (2 if "agrí" in d else 3)
def classify(a): return {1:"forestal",2:"agrícola",3:"urbà"}[tipo_val(a)]
def utm_to_latlon(x,y): lon,lat=TRANSFORM.transform(x,y); return lat,lon
def place(a, geom):
    if geom:
        lat,lon=utm_to_latlon(geom["x"],geom["y"])
        try:
            loc=GEOCODER.reverse((lat,lon),exactly_one=True,timeout=8,language="ca")
            if loc: return loc.address.split(",")[0]
        except Exception: pass
    return "ubicació desconeguda"
def cuerpo(a,p):
    hora=datetime.fromtimestamp(a["ACT_DAT_ACTUACIO"]/1000,tz=timezone.utc)\
         .astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    return (f"🔥 Incendi {classify(a)} a {p}\n"
            f"🕒 {hora}  |  🚒 {a['ACT_NUM_VEH']} dotacions treballant\n{MAPA_OFICIAL}")
def enviar(txt,api):
    print("TUIT SIMULADO:\n"+txt+"\n") if IS_TEST_MODE else api.update_status(txt)

# ---------- main ----------
def main():
    api=None
    if not IS_TEST_MODE and all(TW_KEYS.values()):
        auth=tweepy.OAuth1UserHandler(TW_KEYS["ck"],TW_KEYS["cs"],TW_KEYS["at"],TW_KEYS["as"])
        api=tweepy.API(auth)

    last_id=load_state()
    feats=fetch_features()
    if not feats: return

    # candidatos activos y con dotacions
    candidatos=[f for f in feats if (
        f["attributes"]["ACT_NUM_VEH"]>=MIN_DOTACIONS and
        f["attributes"]["COM_FASE"].lower() in ("","actiu") and
        f["attributes"]["ESRI_OID"]>last_id)]
    candidatos.sort(key=lambda f:(-f["attributes"]["ACT_NUM_VEH"],
                                  tipo_val(f["attributes"]),
                                  -f["attributes"]["ACT_DAT_ACTUACIO"]))

    tweets=[]
    if candidatos:
        tweets.append(candidatos[0])
        # segundo tweet (distinto ID) si hay otro
        for f in candidatos[1:]:
            if f["attributes"]["ESRI_OID"]!=tweets[0]["attributes"]["ESRI_OID"]:
                tweets.append(f); break
    else:
        # fallback: solo el más reciente global
        first_new=next((f for f in feats if f["attributes"]["ESRI_OID"]>last_id), None)
        if first_new: tweets.append(first_new)

    max_id=last_id
    for f in tweets:
        a=f["attributes"]; p=place(a,f.get("geometry"))
        enviar(cuerpo(a,p),api)
        max_id=max(max_id,a["ESRI_OID"])

    save_state(max_id)

if __name__=="__main__":
    main()

