#!/usr/bin/env python3
"""
bombers_bot.py

Consulta la capa ArcGIS “ACTUACIONS URGENTS online PRO” de Bombers
y publica (o simula) tuits con todas las intervenciones actuales para depuración.

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
API_KEY = os.getenv("ARCGIS_API_KEY")  # tu token API aquí

STATE_FILE = Path("state.json")

TW_CONSUMER_KEY = os.getenv("TW_CONSUMER_KEY")
TW_CONSUMER_SECRET = os.getenv("TW_CONSUMER_SECRET")
TW_ACCESS_TOKEN = os.getenv("TW_ACCESS_TOKEN")
TW_ACCESS_SECRET = os.getenv("TW_ACCESS_SECRET")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# --------------- ESTADO -------------------------------------------------
def load_state():
    return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {"last_id": 0}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state))
    logging.info("Estado guardado: last_id=%s", state["last_id"])

# --------------- TRANSFORMADOR UTM ➜ WGS‑84 -----------------------------
transformer = Transformer.from_crs(25831, 4326, always_xy=True)

# --------------- ARC­GIS QUERY -----------------------------------------
def query_features():
    url = f"{LAYER_URL}/query"
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,TAL_DESC_ALARMA1,TAL_DESC_ALARMA2",
        "orderByFields": "ACT_DAT_ACTUACIO DESC",
        "resultRecordCount": 100,
        "returnGeometry": "true",
    }
    if API_KEY:
        params["token"] = API_KEY

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        feats = data.get("features", [])
        logging.info(f"Recibidas {len(feats)} intervenciones desde ArcGIS.")
        for i, feat in enumerate(feats[:5]):
            attrs = feat["attributes"]
            logging.info(f"Intervención {i+1}: ESRI_OID={attrs.get('ESRI_OID')}, "
                         f"dotaciones={attrs.get('ACT_NUM_VEH')}, "
                         f"alarma1={attrs.get('TAL_DESC_ALARMA1')}")
        return feats
    except Exception as e:
        logging.error(f"Error en consulta ArcGIS: {e}")
        return []

# --------------- UTILIDADES --------------------------------------------
def classify_incident(attrs) -> str:
    desc = (attrs.get("TAL_DESC_ALARMA1", "") + " " + attrs.get("TAL_DESC_ALARMA2", "")).lower()
    if "urbà" in desc or "urbano" in desc:
        return "urbà"
    if "agrícola" in desc or "agricola" in desc:
        return "agrícola"
    if "forestal" in desc or "vegetació" in desc or "vegetacion" in desc:
        return "forestal"
    return "forestal"

geocoder = Nominatim(user_agent=GEOCODER_USER_AGENT)

def utm_to_latlon(x, y):
    lon, lat = transformer.transform(x, y)
    return lat, lon

def reverse_geocode(lat, lon):
    try:
        loc = geocoder.reverse((lat, lon), exactly_one=True, timeout=10, language="ca")
        if loc:
            adr = loc.raw.get("address", {})
            house = adr.get("house_number")
            road = (adr.get("road") or adr.get("pedestrian") or adr.get("footway") or adr.get("cycleway") or adr.get("path"))
            town = adr.get("town") or adr.get("village") or adr.get("municipality")
            county = adr.get("county") or adr.get("state_district")
            if road:
                if house:
                    return f"{road} {house}, {town or county}"
                return f"{road}, {town or county}"
            return f"{town or county}, {adr.get('state', '')}".strip(", ")
    except Exception as e:
        logging.warning(f"Reverse geocode error: {e}")
    return f"{lat:.3f}, {lon:.3f}"

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
    # No filtro por estado para depuración: procesa todo
    # state = load_state()
    # last_id = state["last_id"]
    last_id = -1
    logging.info(f"Último ESRI_OID procesado (simulado): {last_id}")

    api = None
    if not IS_TEST_MODE:
        if not all([TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET]):
            logging.error("Faltan credenciales de Twitter.")
            return
        auth = tweepy.OAuth1UserHandler(TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET)
        api = tweepy.API(auth)

    feats = query_features()
    if not feats:
        logging.info("No hay intervenciones recibidas.")
        return

    new_relevant_found = False
    for feat in feats:
        attrs = feat["attributes"]
        obj_id = attrs["ESRI_OID"]
        dotacions = attrs.get("ACT_NUM_VEH", 0)

        logging.info(f"Procesando intervención ESRI_OID={obj_id}, dotaciones={dotacions}")

        # Ignoro filtro last_id para depurar todo
        # if obj_id <= last_id:
        #     logging.info(f"Intervención {obj_id} ya procesada (según estado).")
        #     continue

        geom = feat.get("geometry")
        if geom:
            lat, lon = utm_to_latlon(geom["x"], geom["y"])
        else:
            lat, lon = None, None

        place = reverse_geocode(lat, lon) if lat and lon else "ubicació desconeguda"
        incident_type = classify_incident(attrs)

        if dotacions < MIN_DOTACIONS:
            logging.info(f"Intervención {obj_id} con dotacions={dotacions} < mínimo {MIN_DOTACIONS}, previsualizando:")
            print(format_tweet(attrs, place, incident_type))
            continue

        new_relevant_found = True
        texto = format_tweet(attrs, place, incident_type)
        tweet(texto, api)
        # save_state({"last_id": obj_id})  # No guardo estado para depurar

    if not new_relevant_found:
        logging.info("No hay intervenciones nuevas y relevantes.")

if __name__ == "__main__":
    main()
