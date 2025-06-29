#!/usr/bin/env python3
"""
bombers_bot.py  –  consulta 5 capas de ArcGIS (PRE/PRO, vistas y vw)
y publica (o simula) un tuit con la intervención más reciente.

Dependencias:
    requests  geopy  tweepy>=4.0.0  pyproj
"""

import os, json, logging, itertools
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from geopy.geocoders import Nominatim
from pyproj import Transformer
import tweepy

# ------------------ CAPAS ------------------------------------------------
CAPAS = [
    "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/ACTUACIONS_URGENTS_online_PRE_visualització/FeatureServer/0",
    "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/ACTUACIONS_URGENTS_online_PRE_VW/FeatureServer/0",
    "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0",
    "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/ACTUACIONS_URGENTS_online_PRO_view/FeatureServer/0",
    "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/ACTUACIONS_URGENTS_online_PRO_VW/FeatureServer/0",
]

MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "5"))
IS_TEST_MODE  = os.getenv("IS_TEST_MODE", "true").lower() == "true"
GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot")

STATE_FILE = Path("state.json")

TW_CONSUMER_KEY    = os.getenv("TW_CONSUMER_KEY")
TW_CONSUMER_SECRET = os.getenv("TW_CONSUMER_SECRET")
TW_ACCESS_TOKEN    = os.getenv("TW_ACCESS_TOKEN")
TW_ACCESS_SECRET   = os.getenv("TW_ACCESS_SECRET")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# ------------------ ESTADO ----------------------------------------------
def load_state():
    return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {"last_id": 0}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state))
    logging.info("Estado guardado: last_id=%s", state["last_id"])

# ------------------ TRANSFORMADOR UTM -----------------------------------
transformer = Transformer.from_crs(25831, 4326, always_xy=True)

# ------------------ CONSULTA CAPA ---------------------------------------
def fetch_latest_from_layer(url):
    params = {
        "where": "ACT_NUM_VEH > 0",
        "outFields": (
            "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2"
        ),
        "orderByFields": "ACT_DAT_ACTUACIO desc",
        "f": "json",
        "resultRecordCount": "1",
        "returnGeometry": "true",
    }
    try:
        r = requests.get(f"{url}/query", params=params, timeout=15)
        r.raise_for_status()
        feats = r.json().get("features", [])
        if feats:
            feats[0]["layer_url"] = url  # marca de procedencia
            return feats[0]
    except Exception as e:
        logging.warning("Error capa %s → %s", url.split('/')[-4], e)
    return None

def get_most_recent_feature():
    feats = filter(None, (fetch_latest_from_layer(u) for u in CAPAS))
    return max(feats, key=lambda f: f["attributes"]["ACT_DAT_ACTUACIO"], default=None)

# ------------------ UTILIDADES ------------------------------------------
def classify_incident(attrs):
    desc = (attrs.get("TAL_DESC_ALARMA1","")+" "+attrs.get("TAL_DESC_ALARMA2","")).lower()
    if "urbà" in desc or "urbano" in desc or "vegetació urbana" in desc:
        return "urbà"
    if "agrícola" in desc or "agricola" in desc:
        return "agrícola"
    return "forestal" if ("forestal" in desc or "vegetació" in desc) else "forestal"

geocoder = Nominatim(user_agent=GEOCODER_USER_AGENT)

def utm_to_latlon(x, y):
    lon, lat = transformer.transform(x, y)
    return lat, lon

def reverse_geocode(lat, lon):
    try:
        loc = geocoder.reverse((lat, lon), exactly_one=True, timeout=10, language="ca")
        if loc:
            a = loc.raw["address"]
            road = a.get("road") or a.get("pedestrian")
            num  = a.get("house_number")
            town = a.get("town") or a.get("village") or a.get("municipality")
            if road:
                return f"{road} {num or ''}, {town}".strip(", ")
            return town or a.get("county") or f"{lat:.3f},{lon:.3f}"
    except Exception as e:
        logging.warning("Reverse geocode error: %s", e)
    return f"{lat:.3f},{lon:.3f}"

def format_tweet(attrs, place, kind):
    hora = datetime.fromtimestamp(attrs["ACT_DAT_ACTUACIO"]/1000, tz=timezone.utc)\
                   .astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    dot  = attrs["ACT_NUM_VEH"]
    url  = "https://experience.arcgis.com/experience/f6172fd2d6974bc0a8c51e3a6bc2a735"
    return (f"🔥 Incendi {kind} a {place}\n"
            f"🕒 {hora}  |  🚒 {dot} dotacions treballant\n{url}")

def tweet(text, api):
    if IS_TEST_MODE:
        print("TUIT SIMULADO:\n" + text)
    else:
        api.update_status(text)

# ------------------ MAIN ------------------------------------------------
def main():
    api = None
    if not IS_TEST_MODE and all([TW_CONSUMER_KEY,TW_CONSUMER_SECRET,TW_ACCESS_TOKEN,TW_ACCESS_SECRET]):
        auth = tweepy.OAuth1UserHandler(TW_CONSUMER_KEY,TW_CONSUMER_SECRET,
                                        TW_ACCESS_TOKEN,TW_ACCESS_SECRET)
        api = tweepy.API(auth)

    last_id = load_state()["last_id"]

    feat = get_most_recent_feature()
    if not feat:
        logging.info("Ninguna capa devolvió intervenciones.")
        return

    attrs   = feat["attributes"]
    obj_id  = attrs["ESRI_OID"]

    if obj_id <= last_id:
        logging.info("Intervención %s ya procesada.", obj_id)
        return

    lat, lon = utm_to_latlon(feat["geometry"]["x"], feat["geometry"]["y"])
    place    = reverse_geocode(lat, lon)
    kind     = classify_incident(attrs)
    capa     = feat["layer_url"].split('/')[-4]
    logging.info("Intervención de capa: %s", capa)

    if attrs["ACT_NUM_VEH"] < MIN_DOTACIONS:
        print("PREVISUALIZACIÓN (dotacions < min):\n" +
              format_tweet(attrs, place, kind))
        return

    tweet(format_tweet(attrs, place, kind), api)
    save_state({"last_id": obj_id})

if __name__ == "__main__":
    main()
