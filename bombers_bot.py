#!/usr/bin/env python3
"""
bombers_bot.py

Consulta la capa ArcGIS y publica un tuit con la intervención más reciente
y otra relevante en fase 'actiu' o sin fase.

Dependencias (requirements.txt):
    requests
    geopy
    tweepy>=4.0.0
    pyproj
"""

import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from geopy.geocoders import Nominatim
from pyproj import Transformer
import tweepy

# ---------------- CONFIG ------------------------------------------------
LAYER_URL = os.getenv(
    "ARCGIS_LAYER_URL",
    "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/"
    "ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0"
)
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "5"))
IS_TEST_MODE = os.getenv("IS_TEST_MODE", "true").lower() == "true"
GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot")

STATE_FILE = Path("state.json")

TW_CONSUMER_KEY = os.getenv("TW_CONSUMER_KEY")
TW_CONSUMER_SECRET = os.getenv("TW_CONSUMER_SECRET")
TW_ACCESS_TOKEN = os.getenv("TW_ACCESS_TOKEN")
TW_ACCESS_SECRET = os.getenv("TW_ACCESS_SECRET")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# --------------- ESTADO -------------------------------------------------
def load_state():
    return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {"last_id": -1}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state))
    logging.info("Estado guardado: last_id=%s", state["last_id"])

# --------------- TRANSFORMADOR UTM ➜ WGS‑84 -----------------------------
transformer = Transformer.from_crs(25831, 4326, always_xy=True)

# --------------- ARC­GIS QUERY -----------------------------------------
def query_features():
    """
    Devuelve todas las intervenciones recientes ordenadas por fecha descendente.
    """
    url = f"{LAYER_URL}/query"
    params = {
        "where": "1=1",
        "outFields": (
            "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2,MUN_NOM"
        ),
        "orderByFields": "ACT_DAT_ACTUACIO desc",
        "f": "json",
        "resultRecordCount": "50",
        "returnGeometry": "true",
        "cacheHint": "true",
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    feats = r.json().get("features", [])
    return feats

# --------------- UTILIDADES --------------------------------------------
def looks_relevant(attrs):
    return attrs.get("ACT_NUM_VEH", 0) >= MIN_DOTACIONS

def classify_incident(attrs) -> str:
    """Devuelve forestal / urbà / agrícola (por defecto forestal)."""
    desc = (attrs.get("TAL_DESC_ALARMA1", "") + " " +
            attrs.get("TAL_DESC_ALARMA2", "")).lower()

    if "urbà" in desc or "urbano" in desc:
        return "urbà"
    if "agrícola" in desc or "agricola" in desc:
        return "agrícola"
    if "forestal" in desc or "vegetació" in desc or "vegetacion" in desc:
        return "forestal"
    return "forestal"  # fallback

geocoder = Nominatim(user_agent=GEOCODER_USER_AGENT)

def utm_to_latlon(x, y):
    lon, lat = transformer.transform(x, y)  # always_xy
    return lat, lon

def reverse_geocode(lat, lon):
    """
    Intenta obtener calle y municipio vía geocoder. Si falla, devolver None.
    """
    try:
        loc = geocoder.reverse((lat, lon),
                               exactly_one=True,
                               timeout=10,
                               language="ca")

        if loc:
            adr = loc.raw.get("address", {})
            house = adr.get("house_number")
            road = (adr.get("road") or adr.get("pedestrian") or adr.get("footway")
                    or adr.get("cycleway") or adr.get("path"))
            town = adr.get("town") or adr.get("village") or adr.get("municipality")
            county = adr.get("county") or adr.get("state_district")

            if road:
                if house:
                    return f"{road} {house}, {town or county}"
                return f"{road}, {town or county}"
            if town or county:
                return f"{town or county}"
    except Exception as e:
        logging.warning("Reverse geocode error: %s", e)
    return None

def format_tweet(attrs, place, incident_type):
    dt_utc = datetime.utcfromtimestamp(attrs["ACT_DAT_ACTUACIO"] / 1000).replace(tzinfo=timezone.utc)
    hora_local = dt_utc.astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    dot = attrs.get("ACT_NUM_VEH", "?")
    mapa_url = "https://experience.arcgis.com/experience/f6172fd2d6974bc0a8c51e3a6bc2a735"

    return (f"🔥 Incendi {incident_type} a {place}\n"
            f"🕒 {hora_local}  |  🚒 {dot} dotacions treballant\n"
            f"{mapa_url}")

def tweet(text, api):
    if IS_TEST_MODE:
        print("TUIT SIMULADO:\n" + text)
    else:
        api.update_status(text)

# --------------- MAIN --------------------------------------------------
def main():
    api = None
    if not IS_TEST_MODE:
        if not all([TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET]):
            logging.error("Faltan credenciales de Twitter.")
            return
        auth = tweepy.OAuth1UserHandler(
            TW_CONSUMER_KEY, TW_CONSUMER_SECRET,
            TW_ACCESS_TOKEN, TW_ACCESS_SECRET
        )
        api = tweepy.API(auth)

    state = load_state()
    last_id = state["last_id"]
    logging.info(f"Último ESRI_OID procesado: {last_id}")

    feats = query_features()
    logging.info(f"Recibidas {len(feats)} intervenciones desde ArcGIS.")

    # Buscar la intervención más reciente
    most_recent = None
    for f in feats:
        obj_id = f["attributes"]["ESRI_OID"]
        if obj_id > last_id:
            most_recent = f
            break

    if not most_recent:
        logging.info("No hay intervenciones nuevas.")
        return

    # Preparar datos para el tweet de la más reciente
    attrs = most_recent["attributes"]
    geom = most_recent.get("geometry", {})
    lat, lon = utm_to_latlon(geom.get("x", 0), geom.get("y", 0))
    place = reverse_geocode(lat, lon) or attrs.get("MUN_NOM") or "ubicació desconeguda"
    incident_type = classify_incident(attrs)

    tweets_to_post = []

    tweets_to_post.append(format_tweet(attrs, place, incident_type))

    # Buscar otra intervención con dotaciones >= MIN_DOTACIONS y en fase 'actiu' o sin fase
    for f in feats:
        attrs_f = f["attributes"]
        obj_id_f = attrs_f["ESRI_OID"]
        if obj_id_f <= last_id or obj_id_f == attrs["ESRI_OID"]:
            continue
        dot = attrs_f.get("ACT_NUM_VEH", 0)
        fase = attrs_f.get("COM_FASE", "") or ""
        if dot >= MIN_DOTACIONS and (fase.lower() == "actiu" or fase == ""):
            geom_f = f.get("geometry", {})
            lat_f, lon_f = utm_to_latlon(geom_f.get("x", 0), geom_f.get("y", 0))
            place_f = reverse_geocode(lat_f, lon_f) or attrs_f.get("MUN_NOM") or "ubicació desconeguda"
            incident_type_f = classify_incident(attrs_f)
            tweets_to_post.append(format_tweet(attrs_f, place_f, incident_type_f))
            break

    if not tweets_to_post:
        logging.info("No hay intervenciones nuevas y relevantes.")
        return

    for t in tweets_to_post:
        tweet(t, api)

    # Guardar el último ESRI_OID procesado (el mayor de los dos)
    max_id = max(f["attributes"]["ESRI_OID"] for f in feats)
    save_state({"last_id": max_id})

if __name__ == "__main__":
    main()
