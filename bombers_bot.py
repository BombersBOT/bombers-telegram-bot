"""
bombers_bot.py

Bot que consulta la capa de ArcGIS de los Bombers de la Generalitat y publica
en Twitter (X) nuevas actuaciones relevantes (incendios con muchas dotaciones).

Dependencias: tweepy, requests, geopy
"""

import os
import json
import requests
import logging
from datetime import datetime, timezone
from pathlib import Path
from geopy.geocoders import Nominatim
import tweepy

# ----------------------------------------------------------------------
# CONFIGURACIÓN
# ----------------------------------------------------------------------
LAYER_URL = os.getenv(
    "ARCGIS_LAYER_URL",
    "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/"
    "ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0"
)

MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "5"))
STATE_FILE = Path("state.json")

TW_CONSUMER_KEY = os.getenv("TW_CONSUMER_KEY")
TW_CONSUMER_SECRET = os.getenv("TW_CONSUMER_SECRET")
TW_ACCESS_TOKEN = os.getenv("TW_ACCESS_TOKEN")
TW_ACCESS_SECRET = os.getenv("TW_ACCESS_SECRET")

GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot")
IS_TEST_MODE = os.getenv("IS_TEST_MODE", "true").lower() == "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ----------------------------------------------------------------------
# UTILIDADES DE ESTADO
# ----------------------------------------------------------------------
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_id": 0}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state))
    logging.info(f"Estado guardado: last_id = {state.get('last_id')}")

# ----------------------------------------------------------------------
# CONSULTA ARCGIS: solo 1 registro (el más reciente)
# ----------------------------------------------------------------------
def query_latest_feature():
    url = f"{LAYER_URL}/query"
    params = {
        "where": "1=1",
        "outFields": "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO",
        "orderByFields": "ACT_DAT_ACTUACIO desc",
        "f": "json",
        "resultRecordCount": "1",          # <‑‑ solo el más reciente
        "returnGeometry": "true",
        "cacheHint": "true"
    }
    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    feats = data.get("features", [])
    return feats[0] if feats else None

# ----------------------------------------------------------------------
# FILTRO, GEOCODING Y FORMATO DEL TUIT
# ----------------------------------------------------------------------
def looks_relevant(attrs):
    return attrs.get("ACT_NUM_VEH", 0) >= MIN_DOTACIONS

geocoder = Nominatim(user_agent=GEOCODER_USER_AGENT)

def reverse_geocode(lat, lon):
    try:
        loc = geocoder.reverse((lat, lon), exactly_one=True, timeout=10)
        if not loc:
            return f"{lat:.3f}, {lon:.3f}"
        adr = loc.raw.get("address", {})
        town = adr.get("town") or adr.get("village") or adr.get("municipality")
        county = adr.get("county") or adr.get("state_district")
        return f"{town or county}, {adr.get('state', '')}".strip(", ")
    except Exception as e:
        logging.warning(f"Reverse geocode error: {e}")
        return f"{lat:.3f}, {lon:.3f}"

def format_tweet(attrs, place):
    dt = datetime.utcfromtimestamp(attrs["ACT_DAT_ACTUACIO"] / 1000)\
                  .replace(tzinfo=timezone.utc).astimezone()
    hora = dt.strftime("%H:%M")
    dot = attrs.get("ACT_NUM_VEH", "?")
    mapa = "https://experience.arcgis.com/experience/f6172fd2d6974bc0a8c51e3a6bc2a735"
    return (
        f"🔥 Incendi forestal important a {place}\n"
        f"🕒 {hora}  |  🚒 {dot} dotacions treballant\n"
        f"{mapa}"
    )

def tweet(text, api):
    if IS_TEST_MODE:
        logging.info("SIMULACIÓN — Este sería el tuit:\n" + text)
    else:
        api.update_status(text)

# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
def main():
    # Autenticación Twitter (si se va a publicar realmente)
    api = None
    if not IS_TEST_MODE:
        if not all([TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET]):
            logging.error("Faltan claves API de Twitter en variables de entorno.")
            return
        auth = tweepy.OAuth1UserHandler(
            TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET
        )
        api = tweepy.API(auth)

    stat





