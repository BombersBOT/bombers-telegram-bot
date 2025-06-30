#!/usr/bin/env python3
"""
bombers_bot.py

Consulta la capa ArcGIS “ACTUACIONS URGENTS online PRO” de Bombers
y publica (o simula) tuits con las intervenciones más recientes.

Dependencias:
    requests, geopy, tweepy, pyproj
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
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "3"))
IS_TEST_MODE = os.getenv("IS_TEST_MODE", "true").lower() == "true"
GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot")
API_KEY = os.getenv("ARCGIS_API_KEY")

STATE_FILE = Path("state.json")

TW_CONSUMER_KEY = os.getenv("TW_CONSUMER_KEY")
TW_CONSUMER_SECRET = os.getenv("TW_CONSUMER_SECRET")
TW_ACCESS_TOKEN = os.getenv("TW_ACCESS_TOKEN")
TW_ACCESS_SECRET = os.getenv("TW_ACCESS_SECRET")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# --------------- TRANSFORMADOR UTM ➜ WGS‑84 -----------------------------
transformer = Transformer.from_crs(25831, 4326, always_xy=True)

# --------------- GEOCODING ----------------------------------------------
GEOCODER = Nominatim(user_agent=GEOCODER_USER_AGENT)

def utm_to_latlon(x, y):
    lon, lat = transformer.transform(x, y)
    return lat, lon

def place_from_geom(a, geom):
    nom_municipi = a.get("MUN_NOM", "").title()
    if geom:
        lat, lon = utm_to_latlon(geom["x"], geom["y"])
        try:
            loc = GEOCODER.reverse((lat, lon), exactly_one=True, timeout=8, language="ca")
            if loc:
                adr = loc.raw.get("address", {})
                road = adr.get("road") or adr.get("pedestrian") or adr.get("footway") or adr.get("path")
                town = adr.get("town") or adr.get("village") or adr.get("municipality") or adr.get("city")
                county = adr.get("county") or adr.get("state_district")

                if road and (town or county):
                    return f"{road}, {town or county}"
                elif road and nom_municipi:
                    return f"{road}, {nom_municipi}"
                elif town or county:
                    return f"{town or county}"
                elif nom_municipi:
                    return nom_municipi
        except Exception as e:
            logging.warning(f"Reverse geocode error: {e}")
    return nom_municipi or "ubicació desconeguda"

# --------------- FORMATO Y PUBLICACIÓN ---------------------------------
def format_tweet(a, lloc, tipus):
    hora = datetime.utcfromtimestamp(a["ACT_DAT_ACTUACIO"] / 1000).replace(
        tzinfo=timezone.utc).astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    n = a.get("ACT_NUM_VEH", "?")
    url = "https://interior.gencat.cat/ca/arees_dactuacio/bombers/actuacions-de-bombers/"
    return f"🔥 Incendi {tipus} a {lloc}\n🕒 {hora}  |  🚒 {n} dotacions treballant\n{url}"

def classify(a):
    t1 = (a.get("TAL_DESC_ALARMA1") or "").lower()
    t2 = (a.get("TAL_DESC_ALARMA2") or "").lower()
    txt = t1 + " " + t2
    if "urbà" in txt or "urbano" in txt:
        return "urbà"
    if "agrícola" in txt or "agricola" in txt:
        return "agrícola"
    if "forestal" in txt or "vegetació" in txt or "vegetacion" in txt:
        return "forestal"
    return "forestal"

def tweet(text, api):
    if IS_TEST_MODE:
        print("TUIT SIMULADO:\n" + text)
    else:
        api.update_status(text)

# --------------- CONSULTA A ARCGIS --------------------------------------
def fetch_features():
    url = f"{LAYER_URL}/query"
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": (
            "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2,MUN_NOM"
        ),
        "orderByFields": "ACT_DAT_ACTUACIO DESC",
        "resultRecordCount": 50,
        "returnGeometry": "true",
        "cacheHint": "true"
    }
    if API_KEY:
        params["token"] = API_KEY

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise Exception(f"ArcGIS error {data['error'].get('code')}: {data['error'].get('message')}")
    return data.get("features", [])

# --------------- MAIN --------------------------------------------------
def main():
    last_id = -1
    logging.info("Último ESRI_OID procesado: %s", last_id)

    api = None
    if not IS_TEST_MODE:
        if not all([TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET]):
            logging.error("Faltan credenciales de Twitter.")
            return
        auth = tweepy.OAuth1UserHandler(TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET)
        api = tweepy.API(auth)

    try:
        feats = fetch_features()
    except Exception as e:
        logging.error(f"Error al consultar ArcGIS: {e}")
        return

    logging.info("Consulta URL: %s/query?...", LAYER_URL)
    logging.info("Número de features recibidos: %s", len(feats))
    if not feats:
        logging.info("No se encontraron intervenciones.")
        return

    candidatos = [
        f for f in feats
        if f["attributes"].get("COM_FASE", "").lower() in ("", "actiu")
    ]

    if not candidatos:
        logging.info("No hay intervenciones en fase activa o sin fase.")
        return

    tuit = None
    prioritaria = None
    for f in candidatos:
        a = f["attributes"]
        if a["ACT_NUM_VEH"] >= MIN_DOTACIONS:
            prioritaria = f
            break

    if prioritaria:
        a = prioritaria["attributes"]
        lloc = place_from_geom(a, prioritaria.get("geometry"))
        tipus = classify(a)
        tuit = format_tweet(a, lloc, tipus)
    else:
        a = candidatos[0]["attributes"]
        lloc = place_from_geom(a, candidatos[0].get("geometry"))
        tipus = classify(a)
        tuit = format_tweet(a, lloc, tipus)

    tweet(tuit, api)

if __name__ == "__main__":
    main()
