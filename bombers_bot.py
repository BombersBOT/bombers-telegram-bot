#!/usr/bin/env python3
"""
bombers_bot.py

Consulta la capa ArcGIS “ACTUACIONS URGENTS online PRO” de Bombers
y publica (o simula) tuits con intervenciones relevantes.

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
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "3"))  # Cambiado a 3 según petición
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

# --------------- ARC­GIS QUERY -------------------------------------------
def query_features():
    url = f"{LAYER_URL}/query"
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": (
            "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2,MUNICIPI_DPX"
        ),
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
        if "error" in data:
            logging.error(f"Error en respuesta ArcGIS: {data['error']}")
            return []
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

# --------------- UTILIDADES ----------------------------------------------
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

def place_from_geom(attrs, geom):
    nom_municipi = attrs.get("MUNICIPI_DPX", "").title()
    if geom:
        lat, lon = utm_to_latlon(geom["x"], geom["y"])
        try:
            loc = geocoder.reverse((lat, lon), exactly_one=True, timeout=8, language="ca")
            if loc:
                adr = loc.raw.get("address", {})
                road = adr.get("road") or adr.get("pedestrian") or adr.get("footway") or adr.get("path")
                town = adr.get("town") or adr.get("village") or adr.get("municipality") or adr.get("city")
                county = adr.get("county") or adr.get("state_district")

                # Si hay calle y municipio detectado por geolocalización:
                if road and (town or county):
                    return f"{road}, {town or county}"
                # Si hay calle pero no municipio detectado, usar municipio del ArcGIS
                elif road and nom_municipi:
                    return f"{road}, {nom_municipi}"
                # Si no hay calle pero municipio detectado:
                elif town or county:
                    return f"{town or county}"
                # Si no hay nada detectado, usar municipio ArcGIS
                elif nom_municipi:
                    return nom_municipi
        except Exception as e:
            logging.warning(f"Reverse geocode error: {e}")

    # Si no hay geom o no se pudo geolocalizar:
    return nom_municipi or "ubicació desconeguda"

def format_tweet(attrs, place, incident_type):
    dt_utc = datetime.utcfromtimestamp(attrs["ACT_DAT_ACTUACIO"] / 1000).replace(tzinfo=timezone.utc)
    hora_local = dt_utc.astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    dot = attrs.get("ACT_NUM_VEH", "?")
    mapa_url = "https://interior.gencat.cat/ca/arees_dactuacio/bombers/actuacions-de-bombers/"
    return (f"🔥 Incendi {incident_type} a {place}\n"
            f"🕒 {hora_local}  |  🚒 {dot} dotacions treballant\n"
            f"{mapa_url}")

def tweet(text, api):
    if IS_TEST_MODE:
        print("TUIT SIMULADO:\n" + text)
    else:
        api.update_status(text)

# --------------- MAIN ----------------------------------------------------
def main():
    logging.info("Consultando intervenciones...")
    state = load_state()
    last_id = state.get("last_id", -1)
    logging.info(f"Último ESRI_OID procesado: {last_id}")

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

    # Filtrar nuevas intervenciones, fase activa o sin fase
    candidatos = [
        f for f in feats
        if f["attributes"].get("ESRI_OID", 0) > last_id
        and f["attributes"].get("COM_FASE", "") and f["attributes"]["COM_FASE"].lower() in ("", "actiu")
    ]

    if not candidatos:
        logging.info("No hay intervenciones nuevas y relevantes.")
        return

    # Intervención más reciente (ordenado por ACT_DAT_ACTUACIO descendente)
    candidatos.sort(key=lambda f: f["attributes"]["ACT_DAT_ACTUACIO"], reverse=True)
    mas_reciente = candidatos[0]

    # Buscar otra con dotaciones >= MIN_DOTACIONS en fase actiu o sin fase, diferente de la más reciente
    con_dotacions = [f for f in candidatos if f["attributes"].get("ACT_NUM_VEH", 0) >= MIN_DOTACIONS and f != mas_reciente]
    con_dotacions = sorted(con_dotacions, key=lambda f: f["attributes"]["ACT_DAT_ACTUACIO"], reverse=True)
    mas_relevante = con_dotacions[0] if con_dotacions else None

    # Formatear texto único para tweet con ambas intervenciones
    tweet_text = ""

    attrs = mas_reciente["attributes"]
    geom = mas_reciente.get("geometry")
    place = place_from_geom(attrs, geom)
    incident_type = classify_incident(attrs)
    tweet_text += f"🆕 Actuació més recent:\n{format_tweet(attrs, place, incident_type)}\n\n"

    if mas_relevante:
        attrs = mas_relevante["attributes"]
        geom = mas_relevante.get("geometry")
        place = place_from_geom(attrs, geom)
        incident_type = classify_incident(attrs)
        tweet_text += f"🔥 Incendi actiu més rellevant:\n{format_tweet(attrs, place, incident_type)}"

    if not IS_TEST_MODE:
        tweet(tweet_text, api)
        save_state({"last_id": mas_reciente["attributes"]["ESRI_OID"]})
    else:
        print("TUIT SIMULADO:\n" + tweet_text)

if __name__ == "__main__":
    main()

