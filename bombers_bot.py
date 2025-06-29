#!/usr/bin/env python3
"""
bombers_bot.py

Consulta la capa ArcGIS de Bombers y publica (o simula) un tuit
con la última intervención relevante (mínimo 3 dotaciones), mostrando
la más reciente y, si existe, otra con 3+ dotaciones en fase "actiu" o sin fase.

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
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "3"))  # Bajado a 3
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
    Devuelve intervenciones recientes ordenadas por fecha descendente.
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
    logging.info("Consulta URL: %s", r.url)
    r.raise_for_status()
    data = r.json()
    logging.info("Respuesta keys: %s", list(data.keys()))
    feats = data.get("features", [])
    logging.info("Número de features recibidos: %d", len(feats))
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
    lon, lat = transformer.transform(x, y)  # always_xy=True
    return lat, lon

def reverse_geocode(lat, lon):
    """
    Devuelve:
      • calle + nº + municipio
      • calle + municipio
      • municipio + provincia
      • lat,lon si no hay datos
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
            return f"{town or county}, {adr.get('state', '')}".strip(", ")
    except Exception as e:
        logging.warning("Reverse geocode error: %s", e)

    return None

def format_place(attrs, lat, lon):
    place = reverse_geocode(lat, lon)
    if not place:
        # Usa el campo municipio si no pudo obtener la dirección precisa
        place = attrs.get("MUN_NOM", "ubicació desconeguda")
    return place

def format_tweet(attrs, place, incident_type):
    dt_utc = datetime.utcfromtimestamp(attrs["ACT_DAT_ACTUACIO"] / 1000)\
                      .replace(tzinfo=timezone.utc)
    hora_local = dt_utc.astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    dot = attrs.get("ACT_NUM_VEH", "?")
    mapa_url = ("https://experience.arcgis.com/experience/"
                "f6172fd2d6974bc0a8c51e3a6bc2a735")

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
    # Autenticación Twitter si es producción
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
    logging.info("Último ESRI_OID procesado: %s", last_id)

    feats = query_features()
    if not feats:
        logging.info("No se encontraron intervenciones.")
        return

    # Ordenamos por ACT_DAT_ACTUACIO descendente (por si acaso)
    feats.sort(key=lambda f: f["attributes"]["ACT_DAT_ACTUACIO"], reverse=True)

    # Seleccionamos la más reciente (sin filtro mínimo dotaciones)
    latest_feat = feats[0]
    latest_attrs = latest_feat["attributes"]

    # Buscamos otra con dotaciones >= MIN_DOTACIONS y fase actiu o sin fase,
    # diferente a la más reciente y que no esté ya procesada
    candidate_feat = None
    for feat in feats[1:]:
        attrs = feat["attributes"]
        if attrs["ESRI_OID"] <= last_id:
            continue
        if looks_relevant(attrs):
            fase = attrs.get("COM_FASE", "").lower()
            if fase == "" or "actiu" in fase:
                candidate_feat = feat
                break

    # Procesar solo la más reciente y la candidata si cumplen condiciones
    to_process = []

    if latest_attrs["ESRI_OID"] > last_id:
        to_process.append(latest_feat)

    if candidate_feat and candidate_feat["attributes"]["ESRI_OID"] > last_id:
        to_process.append(candidate_feat)

    if not to_process:
        logging.info("No hay intervenciones nuevas y relevantes.")
        return

    for feat in to_process:
        attrs = feat["attributes"]
        obj_id = attrs["ESRI_OID"]
        dot = attrs.get("ACT_NUM_VEH", 0)
        geom = feat.get("geometry")
        lat, lon = (None, None)
        if geom:
            lat, lon = utm_to_latlon(geom["x"], geom["y"])
        place = format_place(attrs, lat, lon)
        incident_type = classify_incident(attrs)

        texto = format_tweet(attrs, place, incident_type)
        tweet(texto, api)
        save_state({"last_id": obj_id})

if __name__ == "__main__":
    main()
