#!/usr/bin/env python3
"""
bombers_bot.py

Publica (o simula) las intervenciones de Bombers priorizando:
1) fase “actiu” (o sin fase)
2) nº dotacions
3) tipo (forestal > agrícola > urbà).

Requisitos:
    requests   geopy   tweepy>=4.0.0   pyproj
"""

import os
import json
import logging
import requests
import tweepy
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from geopy.geocoders import Nominatim
from pyproj import Transformer

# ---------------- CONFIG ------------------------------------------------
LAYER_URL = (
    "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/"
    "ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0"
)
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "3"))  # mínimo dotacions
IS_TEST_MODE = os.getenv("IS_TEST_MODE", "true").lower() == "true"
API_KEY = os.getenv("ARCGIS_API_KEY", "")
MAPA_OFICIAL = "https://interior.gencat.cat/ca/arees_dactuacio/bombers/actuacions-de-bombers/"

STATE_FILE = Path("state.json")
GEOCODER = Nominatim(user_agent="bombers_bot")
TRANSFORM = Transformer.from_crs(25831, 4326, always_xy=True)

TW_KEYS = {
    "ck": os.getenv("TW_CONSUMER_KEY"),
    "cs": os.getenv("TW_CONSUMER_SECRET"),
    "at": os.getenv("TW_ACCESS_TOKEN"),
    "as": os.getenv("TW_ACCESS_SECRET"),
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------- ESTADO -----------------------------------------------
def load_state() -> int:
    return json.loads(STATE_FILE.read_text()).get("last_id", -1) if STATE_FILE.exists() else -1

def save_state(last_id: int):
    STATE_FILE.write_text(json.dumps({"last_id": last_id}))

# ---------------- CONSULTA ARCGIS --------------------------------------
def fetch_features():
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": (
            "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2,MUN_NOM"
        ),
        "orderByFields": "ACT_DAT_ACTUACIO DESC",  # espacio entre campo y DESC
        "returnGeometry": "true",
        "cacheHint": "true",
    }
    if API_KEY:
        params["token"] = API_KEY
    try:
        r = requests.get(f"{LAYER_URL}/query", params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            logging.error("ArcGIS error %s: %s", data["error"]["code"], data["error"]["message"])
            return []
        feats = data.get("features", [])
        logging.info(f"Recibidas {len(feats)} intervenciones desde ArcGIS.")
        return feats
    except Exception as e:
        logging.error(f"Error en consulta ArcGIS: {e}")
        return []

# ---------------- UTILIDADES -------------------------------------------
def tipo_val(a):
    d = (a.get("TAL_DESC_ALARMA1", "") + " " + a.get("TAL_DESC_ALARMA2", "")).lower()
    if "forestal" in d or "vegetació" in d:
        return 1
    if "agrí" in d or "agricola" in d:
        return 2
    return 3

def classify(a):
    return {1: "forestal", 2: "agrícola", 3: "urbà"}[tipo_val(a)]

def utm_to_latlon(x, y):
    lon, lat = TRANSFORM.transform(x, y)
    return lat, lon

def place_from_geom_or_field(a, geom):
    # Primero intenta geocodificar con coordenadas
    if geom:
        lat, lon = utm_to_latlon(geom["x"], geom["y"])
        try:
            loc = GEOCODER.reverse((lat, lon), exactly_one=True, timeout=8, language="ca")
            if loc:
                adr = loc.raw.get("address", {})
                road = adr.get("road") or adr.get("pedestrian") or adr.get("footway") or adr.get("cycleway") or adr.get("path")
                house = adr.get("house_number")
                town = adr.get("town") or adr.get("village") or adr.get("municipality")
                county = adr.get("county") or adr.get("state_district")
                # Construimos dirección, incluyendo calle si hay
                if road:
                    street_part = f"{road} {house}" if house else road
                    if town or county:
                        return f"{street_part}, {town or county}"
                    else:
                        return street_part
                elif town or county:
                    return f"{town or county}"
        except Exception as e:
            logging.warning(f"Reverse geocode error: {e}")
    # Si no pudo geocodificar, usa el municipio del campo MUN_NOM si existe
    mun_nom = a.get("MUN_NOM")
    if mun_nom:
        return mun_nom
    return "ubicació desconeguda"

def tweet_body(a, place, title):
    hora = datetime.fromtimestamp(a["ACT_DAT_ACTUACIO"] / 1000, tz=timezone.utc)\
           .astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    return (f"{title}\n"
            f"🔥 Incendi {classify(a)} a {place}\n"
            f"🕒 {hora}  |  🚒 {a['ACT_NUM_VEH']} dotacions treballant\n"
            f"{MAPA_OFICIAL}")

def send(text, api):
    if IS_TEST_MODE:
        print("TUIT SIMULADO:\n" + text + "\n")
    else:
        api.update_status(text)

# ---------------- MAIN --------------------------------------------------
def main():
    # Twitter API (solo producción)
    api = None
    if not IS_TEST_MODE and all(TW_KEYS.values()):
        auth = tweepy.OAuth1UserHandler(TW_KEYS["ck"], TW_KEYS["cs"], TW_KEYS["at"], TW_KEYS["as"])
        api = tweepy.API(auth)

    last_id = load_state()
    logging.info(f"Último ESRI_OID procesado: {last_id}")

    feats = fetch_features()
    if not feats:
        logging.info("ArcGIS devolvió 0 features.")
        return

    # Filtrar intervenciones nuevas
    new_feats = [f for f in feats if f["attributes"]["ESRI_OID"] > last_id]
    if not new_feats:
        logging.info("No hay intervenciones nuevas para tuitear.")
        return

    # Candidatos en fase actiu o sin fase con dotacions >= mínimo
    candidatos = [
        f for f in new_feats
        if (str(f["attributes"].get("COM_FASE") or "").lower() in ("", "actiu"))
        and f["attributes"]["ACT_NUM_VEH"] >= MIN_DOTACIONS
    ]

    candidatos.sort(
        key=lambda f: (
            -f["attributes"]["ACT_NUM_VEH"],
            tipo_val(f["attributes"]),
            -f["attributes"]["ACT_DAT_ACTUACIO"],
        )
    )

    tweets = []

    if candidatos:
        tweets.append(candidatos[0])  # Incendi actiu més rellevant

        # Busca otra intervención distinta que tenga más de 3 dotacions si existe
        for c in candidatos[1:]:
            if c["attributes"]["ACT_NUM_VEH"] > 3 and c["attributes"]["ESRI_OID"] != candidatos[0]["attributes"]["ESRI_OID"]:
                tweets.append(c)
                break
    else:
        # Si no hay candidatas con fase actiu o dotacions suficientes,
        # publica la intervención más reciente nueva (aunque no cumpla dotacions)
        most_recent = sorted(new_feats, key=lambda f: -f["attributes"]["ACT_DAT_ACTUACIO"])
        tweets.append(most_recent[0])

    max_id = last_id
    textos = []
    for idx, f in enumerate(tweets):
        a = f["attributes"]
        place = place_from_geom_or_field(a, f.get("geometry"))
        title = "🔥 Incendi actiu més rellevant" if idx == 0 and len(tweets) > 1 else "🔥 Actuació més recent"
        textos.append(tweet_body(a, place, title))
        max_id = max(max_id, a["ESRI_OID"])

    # Unir tweets en uno solo, separados por línea en blanco para Twitter
    tweet_text = "\n\n".join(textos)

    send(tweet_text, api)
    save_state(max_id)

if __name__ == "__main__":
    main()



