#!/usr/bin/env python3
"""
bombers_bot.py

Consulta la capa ArcGIS de Bombers de la Generalitat
y publica un tuit conjunto con:
- L’actuació més recent
- L’incendi actiu més rellevant (≥ MIN_DOTACIONS)

Dependencias: requests, geopy, tweepy, pyproj
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

TW_CONSUMER_KEY = os.getenv("TW_CONSUMER_KEY")
TW_CONSUMER_SECRET = os.getenv("TW_CONSUMER_SECRET")
TW_ACCESS_TOKEN = os.getenv("TW_ACCESS_TOKEN")
TW_ACCESS_SECRET = os.getenv("TW_ACCESS_SECRET")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

transformer = Transformer.from_crs(25831, 4326, always_xy=True)
geocoder = Nominatim(user_agent=GEOCODER_USER_AGENT)

def utm_to_latlon(x, y):
    lon, lat = transformer.transform(x, y)
    return lat, lon

def place_from_geom(a, geom):
    nom_municipi = a.get("MUN_NOM", "").title()
    if geom:
        lat, lon = utm_to_latlon(geom["x"], geom["y"])
        try:
            loc = geocoder.reverse((lat, lon), exactly_one=True, timeout=8, language="ca")
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

def hora_str(epoch_ms):
    dt = datetime.utcfromtimestamp(epoch_ms / 1000).replace(tzinfo=timezone.utc)
    return dt.astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")

def format_intervencio(a, lloc, tipus):
    hora = hora_str(a["ACT_DAT_ACTUACIO"])
    n = a.get("ACT_NUM_VEH", "?")
    return f"🔥 Incendi {tipus} a {lloc}\n🕒 {hora}  |  🚒 {n} dotacions treballant"

def format_tweet(combinat1, combinat2=None):
    url = "https://interior.gencat.cat/ca/arees_dactuacio/bombers/actuacions-de-bombers/"
    text = f"🔥 Incendi actiu més rellevant:\n{combinat1}"
    if combinat2:
        text += f"\n\n🔥 Actuació més recent:\n{combinat2}"
    text += f"\n{url}"
    return text

def tweet(text, api):
    if IS_TEST_MODE:
        print("TUIT SIMULAT:\n" + text)
    else:
        api.update_status(text)

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

def main():
    logging.info("Consultando intervenciones...")
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

    activos = [f for f in feats if f["attributes"].get("COM_FASE", "").lower() in ("", "actiu")]
    if not activos:
        logging.info("No hay intervenciones activas.")
        return

    rellevant = next((f for f in activos if f["attributes"].get("ACT_NUM_VEH", 0) >= MIN_DOTACIONS), None)
    recent = activos[0]  # primera por orden DESC

    a1 = rellevant or recent
    g1 = a1.get("geometry")
    txt1 = format_intervencio(a1["attributes"], place_from_geom(a1["attributes"], g1), classify(a1["attributes"]))

    txt2 = None
    if rellevant and recent and rellevant != recent:
        a2 = recent
        g2 = a2.get("geometry")
        txt2 = format_intervencio(a2["attributes"], place_from_geom(a2["attributes"], g2), classify(a2["attributes"]))

    text = format_tweet(txt1, txt2)
    tweet(text, api)

if __name__ == "__main__":
    main()

