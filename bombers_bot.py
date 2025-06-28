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

# Configuración variables entorno y constantes
LAYER_URL = os.getenv("ARCGIS_LAYER_URL",
                      "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0")
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "5"))
STATE_FILE = Path("state.json")

TW_CONSUMER_KEY = os.getenv("TW_CONSUMER_KEY")
TW_CONSUMER_SECRET = os.getenv("TW_CONSUMER_SECRET")
TW_ACCESS_TOKEN = os.getenv("TW_ACCESS_TOKEN")
TW_ACCESS_SECRET = os.getenv("TW_ACCESS_SECRET")

GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot")
IS_TEST_MODE = os.getenv("IS_TEST_MODE", "true").lower() == "true"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_id": 0}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state))
    logging.info(f"Estado guardado: last_id = {state.get('last_id')}")

def query_arcgis():
    url = f"{LAYER_URL}/query"
    params = {
        "where": "1=1",
        "outFields": "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO",
        "orderByFields": "ACT_DAT_ACTUACIO desc",
        "f": "json",
        "resultOffset": "0",
        "resultRecordCount": "100",
        "returnGeometry": "true",
        "cacheHint": "true"
    }
    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    features = data.get("features", [])
    logging.info(f"Número de intervenciones consultadas: {len(features)}")
    return features

def looks_relevant(attrs):
    return attrs.get("ACT_NUM_VEH", 0) >= MIN_DOTACIONS

def reverse_geocode(lat, lon, geocoder):
    try:
        location = geocoder.reverse((lat, lon), exactly_one=True, timeout=10)
        if location is None:
            return f"{lat:.3f}, {lon:.3f}"
        parts = location.raw.get("address", {})
        town = parts.get("town") or parts.get("village") or parts.get("municipality")
        county = parts.get("county") or parts.get("state_district")
        return f"{town or county}, {parts.get('state', '')}".strip(", ")
    except Exception as e:
        logging.warning(f"Reverse geocode error: {e}")
        return f"{lat:.3f}, {lon:.3f}"

def format_tweet(attrs, place):
    # ACT_DAT_ACTUACIO es un timestamp en milisegundos desde epoch UTC
    dt = datetime.utcfromtimestamp(attrs["ACT_DAT_ACTUACIO"] / 1000).replace(tzinfo=timezone.utc).astimezone()
    hora = dt.strftime("%H:%M")
    dot = attrs.get("ACT_NUM_VEH", "?")
    mapa_url = "https://experience.arcgis.com/experience/f6172fd2d6974bc0a8c51e3a6bc2a735"
    texto = (
        f"🔥 Incendi forestal important a {place}\n"
        f"🕒 {hora}  |  🚒 {dot} dotacions treballant\n"
        f"{mapa_url}"
    )
    return texto

def tweet(text, api):
    if IS_TEST_MODE:
        print("SIMULACIÓN — Publicaría este tuit:")
        print(text)
    else:
        api.update_status(text)

def main():
    if not all([TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET]):
        logging.error("Faltan claves API de Twitter en variables de entorno.")
        return

    geocoder = Nominatim(user_agent=GEOCODER_USER_AGENT)
    auth = tweepy.OAuth1UserHandler(TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET)
    api = tweepy.API(auth)

    state = load_state()
    last_id = state.get("last_id", 0)

    try:
        features = query_arcgis()
    except Exception as e:
        logging.error(f"Error consultando ArcGIS: {e}")
        return

    logging.info(f"Modo test: {IS_TEST_MODE}")
    logging.info(f"Last processed id: {last_id}")

    for feat in features:
        attrs = feat["attributes"]
        obj_id = attrs["ESRI_OID"]
        if obj_id <= last_id:
            continue
        if not looks_relevant(attrs):
            continue
        geom = feat.get("geometry")
        if geom is None:
            place = "Ubicación desconocida"
        else:
            lat = geom["y"]
            lon = geom["x"]
            place = reverse_geocode(lat, lon, geocoder)
        texto = format_tweet(attrs, place)

        try:
            tweet(texto, api)
            logging.info(f"Tuit enviado: {texto.replace(chr(10), ' | ')}")
            last_id = max(last_id, obj_id)
        except Exception as e:
            logging.error(f"Error enviando tuit {obj_id}: {e}")

    save_state({"last_id": last_id})

if __name__ == "__main__":
    main()




