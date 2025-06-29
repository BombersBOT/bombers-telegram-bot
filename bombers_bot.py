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
import pytz

# Configuración desde variables de entorno
LAYER_URL = os.getenv(
    "ARCGIS_LAYER_URL",
    "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0"
)
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
    params = {
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "ACT_DAT_ACTUACIO desc",
        "f": "json",
        "resultRecordCount": 100,
    }
    url = f"{LAYER_URL}/query"
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json().get("features", [])


def classify_incident(attrs) -> str:
    """
    Devuelve 'forestal', 'urbà' o 'agrícola' basado en la descripción
    (prioriza vegetació urbana sobre forestal).
    """
    desc1 = attrs.get("TAL_DESC_ALARMA1", "") or ""
    desc2 = attrs.get("TAL_DESC_ALARMA2", "") or ""
    desc = (desc1 + " " + desc2).lower().strip()

    logging.info(f"Descripción alarma1: '{desc1}'")
    logging.info(f"Descripción alarma2: '{desc2}'")
    logging.info(f"Descripción combinada: '{desc}'")

    if "vegetació" in desc and "urbana" in desc:
        return "urbà"
    if "vegetación" in desc and "urbana" in desc:
        return "urbà"
    if "urbà" in desc or "urbano" in desc:
        return "urbà"
    if "agrícola" in desc or "agricola" in desc:
        return "agrícola"
    if "forestal" in desc:
        return "forestal"
    if "vegetació" in desc or "vegetacion" in desc:
        return "forestal"

    logging.warning("No se pudo clasificar bien la intervención. Se asigna forestal por defecto.")
    return "forestal"


def looks_relevant(attrs):
    return (attrs.get("ACT_NUM_VEH", 0) or 0) >= MIN_DOTACIONS


def reverse_geocode(lat, lon, geocoder):
    try:
        location = geocoder.reverse((lat, lon), exactly_one=True, timeout=10)
        if location is None:
            return f"{lat:.3f}, {lon:.3f}"
        addr = location.raw.get("address", {})
        # Tratar de obtener la calle más precisa posible
        road = addr.get("road") or addr.get("pedestrian") or addr.get("footway") or ""
        house_number = addr.get("house_number") or ""
        town = addr.get("town") or addr.get("village") or addr.get("municipality") or addr.get("city") or ""
        county = addr.get("county") or ""
        state = addr.get("state") or ""
        parts = []
        if road:
            parts.append(road)
        if house_number:
            parts[-1] += f", {house_number}" if parts else house_number
        if town:
            parts.append(town)
        elif county:
            parts.append(county)
        if state:
            parts.append(state)
        return ", ".join(parts).strip(", ")
    except Exception as e:
        logging.warning(f"Reverse geocode error: {e}")
        return f"{lat:.3f}, {lon:.3f}"


def format_tweet(attrs, place):
    # Convertir fecha a zona horaria de Madrid
    dt_utc = datetime.utcfromtimestamp(attrs["ACT_DAT_ACTUACIO"] / 1000).replace(tzinfo=timezone.utc)
    madrid_tz = pytz.timezone("Europe/Madrid")
    dt_madrid = dt_utc.astimezone(madrid_tz)
    hora = dt_madrid.strftime("%H:%M")

    tipo = classify_incident(attrs)
    dot = attrs.get("ACT_NUM_VEH", "?")
    mapa_url = "https://experience.arcgis.com/experience/f6172fd2d6974bc0a8c51e3a6bc2a735"

    texto = (f"🔥 Incendi {tipo} important a {place}\n"
             f"🕒 {hora}  |  🚒 {dot} dotacions treballant\n"
             f"{mapa_url}")
    return texto


def tweet(text, api):
    if IS_TEST_MODE:
        print("PREVISUALIZACIÓN (no se publica):")
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

    logging.info(f"Número de intervenciones consultadas: {len(features)}")
    logging.info(f"Modo test: {IS_TEST_MODE}")
    logging.info(f"Last processed id: {last_id}")

    for feat in features:
        obj_id = feat["attributes"]["OBJECTID"]
        if obj_id <= last_id:
            continue
        if not looks_relevant(feat["attributes"]):
            logging.info(f"Intervención {obj_id} con {feat['attributes'].get('ACT_NUM_VEH',0)} dotacions (<{MIN_DOTACIONS}). No se tuitea.")
            continue

        lat = feat["geometry"]["y"]
        lon = feat["geometry"]["x"]
        place = reverse_geocode(lat, lon, geocoder)
        texto = format_tweet(feat["attributes"], place)

        try:
            tweet(texto, api)
            logging.info(f"Tuit enviado: {texto.replace(chr(10), ' | ')}")
            last_id = max(last_id, obj_id)
        except Exception as e:
            logging.error(f"Error enviando tuit {obj_id}: {e}")

    save_state({"last_id": last_id})


if __name__ == "__main__":
    main()
