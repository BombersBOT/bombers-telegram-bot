#!/usr/bin/env python3
"""
#!/usr/bin/env python3
"""
bombers_bot.py

Consulta la capa ArcGIS de Bombers de la Generalitat y publica (o simula)
un tuit con la última intervención relevante.

Dependencias:
    requests   geopy   tweepy   pyproj
"""

import os
import json
import requests
import logging
from datetime import datetime, timezone
from pathlib import Path
from geopy.geocoders import Nominatim
from pyproj import Transformer
import tweepy

# ---------------- CONFIGURACIÓN ---------------------------------------
LAYER_URL = os.getenv(
    "ARCGIS_LAYER_URL",
    "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/"
    "ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0"
)

MIN_DOTACIONS     = int(os.getenv("MIN_DOTACIONS", "5"))
IS_TEST_MODE      = os.getenv("IS_TEST_MODE", "true").lower() == "true"
GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot")
STATE_FILE        = Path("state.json")

TW_CONSUMER_KEY    = os.getenv("TW_CONSUMER_KEY")
TW_CONSUMER_SECRET = os.getenv("TW_CONSUMER_SECRET")
TW_ACCESS_TOKEN    = os.getenv("TW_ACCESS_TOKEN")
TW_ACCESS_SECRET   = os.getenv("TW_ACCESS_SECRET")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# ---------------- ESTADO ----------------------------------------------
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_id": 0}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state))
    logging.info(f"Estado guardado: last_id = {state['last_id']}")

# ---------------- TRANSFORMADOR DE COORDENADAS ------------------------
# De EPSG:25831 (UTM 31N, ETRS89) → EPSG:4326 (WGS‑84 lat/lon)
transformer = Transformer.from_crs("EPSG:25831", "EPSG:4326", always_xy=True)

# ---------------- CONSULTA ARCGIS (1 intervención) --------------------
def query_latest_feature():
    url = f"{LAYER_URL}/query"
    params = {
        "where": "1=1",
        "outFields": "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO",
        "orderByFields": "ACT_DAT_ACTUACIO desc",
        "f": "json",
        "resultRecordCount": "1",
        "returnGeometry": "true",
        "cacheHint": "true"
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    feats = r.json().get("features", [])
    return feats[0] if feats else None

# ---------------- UTILIDADES ------------------------------------------
def looks_relevant(attrs):
    return attrs.get("ACT_NUM_VEH", 0) >= MIN_DOTACIONS

geocoder = Nominatim(user_agent=GEOCODER_USER_AGENT)

def reverse_geocode(lat, lon):
    try:
        loc = geocoder.reverse((lat, lon), exactly_one=True, timeout=10)
        if not loc:
            return f"{lat:.3f}, {lon:.3f}"
        adr = loc.raw.get("address", {})
        town   = adr.get("town") or adr.get("village") or adr.get("municipality")
        county = adr.get("county") or adr.get("state_district")
        return f"{town or county}, {adr.get('state', '')}".strip(", ")
    except Exception as e:
        logging.warning(f"Reverse geocode error: {e}")
        return f"{lat:.3f}, {lon:.3f}"

def format_tweet(attrs, place):
    dt = datetime.utcfromtimestamp(attrs["ACT_DAT_ACTUACIO"] / 1000)\
                  .replace(tzinfo=timezone.utc).astimezone()
    hora = dt.strftime("%H:%M")
    dot  = attrs.get("ACT_NUM_VEH", "?")
    mapa = "https://experience.arcgis.com/experience/f6172fd2d6974bc0a8c51e3a6bc2a735"
    return (f"🔥 Incendi forestal important a {place}\n"
            f"🕒 {hora}  |  🚒 {dot} dotacions treballant\n"
            f"{mapa}")

def tweet(text, api):
    if IS_TEST_MODE:
        print("TUIT SIMULADO:\n" + text)
    else:
        api.update_status(text)

# ---------------- MAIN ------------------------------------------------
def main():
    # Twitter solo si se va a publicar
    api = None
    if not IS_TEST_MODE:
        if not all([TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET]):
            logging.error("Faltan claves API de Twitter.")
            return
        auth = tweepy.OAuth1UserHandler(
            TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET
        )
        api = tweepy.API(auth)

    state   = load_state()
    last_id = state["last_id"]

    feat = query_latest_feature()
    if not feat:
        logging.info("No se encontraron intervenciones.")
        return

    attrs  = feat["attributes"]
    obj_id = attrs["ESRI_OID"]

    if obj_id <= last_id:
        logging.info("La intervención más reciente ya se procesó.")
        return

    if not looks_relevant(attrs):
        logging.info(
            f"Intervención {obj_id} con {attrs.get('ACT_NUM_VEH', 0)} dotacions; "
            f"mínimo {MIN_DOTACIONS}. No se tuitea."
        )
        geom = feat["geometry"]
        lon, lat = transformer.transform(geom["x"], geom["y"])
        preview  = format_tweet(attrs, reverse_geocode(lat, lon))
        print("PREVISUALIZACIÓN (no se publica):\n" + preview)
        return

    geom = feat["geometry"]
    lon, lat = transformer.transform(geom["x"], geom["y"])
    texto = format_tweet(attrs, reverse_geocode(lat, lon))

    tweet(texto, api)
    save_state({"last_id": obj_id})

if __name__ == "__main__":
    main()
