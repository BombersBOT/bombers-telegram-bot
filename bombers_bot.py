#!/usr/bin/env python3
"""
bombers_bot.py

Publica (o simula) las intervenciones de Bombers priorizando:
1) fase “actiu” (o sin fase) 2) nº dotacions 3) tipo (forestal > agrícola > urbà).

Requisitos:
    requests    geopy    tweepy>=4.0.0    pyproj
"""

import os, json, logging, requests, tweepy
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from geopy.geocoders import Nominatim
from pyproj import Transformer

# ---------------- CONFIG ------------------------------------------------
LAYER_URL = ("https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/"
             "ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0")
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "3"))     # mínimo dotacions
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

# ---------------- ESTADO -----------------------------------------------
def load_state() -> int:
    return json.loads(STATE_FILE.read_text()).get("last_id", -1) if STATE_FILE.exists() else -1

def save_state(last_id: int):
    STATE_FILE.write_text(json.dumps({"last_id": last_id}))

# ---------------- CONSULTA ARCGIS --------------------------------------
def fetch_features(limit=100):
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": (
            "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2,MUN_NOM_MUNICIPI,VIAL_NOM_CARRER"
        ),
        "orderByFields": "ACT_DAT_ACTUACIO DESC",  # espacio, no %20
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

# ---------------- UTILIDADES -------------------------------------------
def tipo_val(a):
    d = (a.get("TAL_DESC_ALARMA1","")+" "+a.get("TAL_DESC_ALARMA2","")).lower()
    return 1 if "forestal" in d or "vegetació" in d else (2 if "agrí" in d else 3)

def classify(a): return {1:"forestal", 2:"agrícola", 3:"urbà"}[tipo_val(a)]

def utm_to_latlon(x, y):
    lon, lat = TRANSFORM.transform(x, y)
    return lat, lon

def get_street_from_coords(geom):
    if geom:
        lat, lon = utm_to_latlon(geom["x"], geom["y"])
        try:
            loc = GEOCODER.reverse((lat, lon), exactly_one=True, timeout=8, language="ca")
            if loc and loc.address:
                # Attempt to extract street name (first part of address before a comma or number)
                parts = loc.address.split(',')[0].strip().split(' ')
                street_name = []
                for part in parts:
                    if any(char.isdigit() for char in part):
                        break
                    street_name.append(part)
                return " ".join(street_name) if street_name else ""
        except Exception:
            pass
    return ""

def format_intervention(a, geom):
    municipio = a.get("MUN_NOM_MUNICIPI", "desconegut")
    calle = get_street_from_coords(geom)

    hora = datetime.fromtimestamp(a["ACT_DAT_ACTUACIO"]/1000, tz=timezone.utc)\
               .astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    
    location_str = f"{calle}, {municipio}" if calle else municipio
    
    return (f"🔥 Incendi {classify(a)} a {location_str}\n"
            f"🕒 {hora} | 🚒 {a['ACT_NUM_VEH']} dotacions treballant")

def send(text, api):
    if IS_TEST_MODE:
        print("TUIT SIMULADO:\n" + text + "\n")
    else:
        api.update_status(text)

# ---------------- MAIN --------------------------------------------------
def main():
    # Twitter API (solo producción)
    api = None
    if not IS_TEST_MODE and all(TW_KEYS.values()):
        auth = tweepy.OAuth1UserHandler(TW_KEYS["ck"], TW_KEYS["cs"], TW_KEYS["at"], TW_KEYS["as"])
        api = tweepy.API(auth)

    last_id = load_state()
    feats = fetch_features()
    if not feats:
        logging.info("ArcGIS devolvió 0 features.")
        return

    # candidatos activos con dotacions >= mínimo
    candidatos = [
        f for f in feats
        if f["attributes"]["ACT_NUM_VEH"] >= MIN_DOTACIONS
           and (str(f["attributes"].get("COM_FASE") or "")).lower() in ("", "actiu")
           and f["attributes"]["ESRI_OID"] > last_id
    ]
    candidatos.sort(
        key=lambda f: (
            -f["attributes"]["ACT_NUM_VEH"],
            tipo_val(f["attributes"]),
            -f["attributes"]["ACT_DAT_ACTUACIO"]
        )
    )

    intervenciones_para_tweet = []

    if candidatos:
        intervenciones_para_tweet.append({"title": "Incendi més rellevant", "feature": candidatos[0]})
        # posible segunda intervención
        for f in candidatos[1:]:
            if f["attributes"]["ESRI_OID"] != intervenciones_para_tweet[0]["feature"]["attributes"]["ESRI_OID"]:
                intervenciones_para_tweet.append({"title": "Actuació més recent", "feature": f})
                break
    else:
        # fallback: la intervención más reciente no procesada
        first_new = next((f for f in feats if f["attributes"]["ESRI_OID"] > last_id), None)
        if first_new:
            intervenciones_para_tweet.append({"title": "Actuació més recent", "feature": first_new})

    if not intervenciones_para_tweet:
        logging.info("No hay intervenciones nuevas para tuitear.")
        return

    tweet_parts = []
    max_id = last_id

    for item in intervenciones_para_tweet:
        title = item["title"]
        feature = item["feature"]
        a = feature["attributes"]
        geom = feature.get("geometry")
        
        formatted_interv = format_intervention(a, geom)
        tweet_parts.append(f"• {title}:\n{formatted_interv}")
        max_id = max(max_id, a["ESRI_OID"])
    
    final_tweet_text = "\n\n".join(tweet_parts) + f"\n\n{MAPA_OFICIAL}"
    send(final_tweet_text, api)
    
    save_state(max_id)

if __name__ == "__main__":
    main()


