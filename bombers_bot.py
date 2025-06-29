#!/usr/bin/env python3
"""
bombers_bot.py

Consulta la capa ArcGIS “ACTUACIONS URGENTS online PRO” de Bombers
y publica (o simula) un tuit con la última intervención relevante.

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
MIN_DOTACIONS   = int(os.getenv("MIN_DOTACIONS", "3"))  # Bajado a 3 unidades
IS_TEST_MODE    = os.getenv("IS_TEST_MODE", "true").lower() == "true"
GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot")

STATE_FILE = Path("state.json")

TW_CONSUMER_KEY    = os.getenv("TW_CONSUMER_KEY")
TW_CONSUMER_SECRET = os.getenv("TW_CONSUMER_SECRET")
TW_ACCESS_TOKEN    = os.getenv("TW_ACCESS_TOKEN")
TW_ACCESS_SECRET   = os.getenv("TW_ACCESS_SECRET")

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
        "orderByFields": "ACT_DAT_ACTUACIO desc",  # Espacio correcto para desc
        "f": "json",
        "resultRecordCount": "50",
        "returnGeometry": "true",
        "cacheHint": "true",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        logging.info("Consulta URL: %s", r.url)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logging.error("Error en la petición o parseo JSON: %s", e)
        return []

    if "error" in data:
        logging.error("Error en respuesta ArcGIS: %s", data["error"])
        return []

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
    if "agrícola" in desc or "agrícola" in desc or "agricola" in desc:
        return "agrícola"
    # palabras clave vegetació forestal
    if "forestal" in desc or "vegetació" in desc or "vegetacion" in desc:
        return "forestal"
    return "forestal"  # fallback

geocoder = Nominatim(user_agent=GEOCODER_USER_AGENT)

def utm_to_latlon(x, y):
    lon, lat = transformer.transform(x, y)  # always_xy
    return lat, lon

def reverse_geocode(lat, lon, fallback_municipio=None):
    """
    Devuelve:
      • calle + nº + municipio
      • calle + municipio
      • municipio + provincia
      • lat,lon si no hay datos
    En caso de no encontrar municipio, usa fallback_municipio si se proporciona.
    """
    try:
        loc = geocoder.reverse((lat, lon),
                               exactly_one=True,
                               timeout=10,
                               language="ca")

        if loc:
            adr = loc.raw.get("address", {})
            house = adr.get("house_number")
            road  = (adr.get("road") or adr.get("pedestrian") or adr.get("footway")
                     or adr.get("cycleway") or adr.get("path"))
            town  = adr.get("town") or adr.get("village") or adr.get("municipality") or fallback_municipio
            county = adr.get("county") or adr.get("state_district")

            if road:
                if house:
                    return f"{road} {house}, {town or county}"
                return f"{road}, {town or county}"
            return f"{town or county}, {adr.get('state', '')}".strip(", ")

    except Exception as e:
        logging.warning("Reverse geocode error: %s", e)

    return f"{lat:.3f}, {lon:.3f}"

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
    if not feats:
        logging.info("No se encontraron intervenciones.")
        return

    # Filtrar solo intervenciones nuevas y ordenarlas por ACT_DAT_ACTUACIO descendente
    feats_nuevas = [f for f in feats if f["attributes"]["ESRI_OID"] > last_id]
    feats_nuevas.sort(key=lambda f: f["attributes"]["ACT_DAT_ACTUACIO"], reverse=True)

    if not feats_nuevas:
        logging.info("No hay intervenciones nuevas.")
        return

    # Tomar la intervención más reciente
    principal = feats_nuevas[0]
    attrs_p = principal["attributes"]
    geom_p = principal.get("geometry")
    lat_p, lon_p = (None, None)
    if geom_p:
        lat_p, lon_p = utm_to_latlon(geom_p["x"], geom_p["y"])
    municipio_p = attrs_p.get("MUN_NOM")
    place_p = reverse_geocode(lat_p, lon_p, fallback_municipio=municipio_p) if lat_p and lon_p else municipio_p or "ubicació desconeguda"
    incident_type_p = classify_incident(attrs_p)

    texto = format_tweet(attrs_p, place_p, incident_type_p)
    tweet(texto, api)

    # Buscar otra intervención con dotaciones >= MIN_DOTACIONS y fase activa o sin fase
    secundaria = None
    for f in feats_nuevas[1:]:
        a = f["attributes"]
        fase = a.get("COM_FASE", "").lower()
        if looks_relevant(a) and (fase == "" or fase == "actiu"):
            secundaria = f
            break

    if secundaria:
        attrs_s = secundaria["attributes"]
        geom_s = secundaria.get("geometry")
        lat_s, lon_s = (None, None)
        if geom_s:
            lat_s, lon_s = utm_to_latlon(geom_s["x"], geom_s["y"])
        municipio_s = attrs_s.get("MUN_NOM")
        place_s = reverse_geocode(lat_s, lon_s, fallback_municipio=municipio_s) if lat_s and lon_s else municipio_s or "ubicació desconeguda"
        incident_type_s = classify_incident(attrs_s)

        texto_s = format_tweet(attrs_s, place_s, incident_type_s)
        tweet(texto_s, api)

    # Guardar último ESRI_OID procesado (el máximo entre las dos)
    max_id = max(attrs_p["ESRI_OID"], secundaria["attributes"]["ESRI_OID"] if secundaria else last_id)
    save_state({"last_id": max_id})

if __name__ == "__main__":
    main()
