#!/usr/bin/env python3
"""
bombers_bot.py

Publica (o simula) las intervenciones más relevantes de Bombers
priorizando fase “actiu”, nº dotacions y tipo d'incendi.

Requisitos:
    requests  geopy  tweepy>=4.0.0  pyproj
"""

import os, json, logging, requests, tweepy
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from geopy.geocoders import Nominatim
from pyproj import Transformer

# ---------- CONFIG -----------------------------------------------------
LAYER_URL = (
    "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/"
    "ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0"
)
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "3"))     # mínimo unidades
IS_TEST_MODE  = os.getenv("IS_TEST_MODE", "true").lower() == "true"
API_KEY       = os.getenv("ARCGIS_API_KEY", "")
MAPA_OFICIAL  = "https://interior.gencat.cat/ca/arees_dactuacio/bombers/actuacions-de-bombers/"

STATE_FILE = Path("state.json")
GEOCODER = Nominatim(user_agent="bombers_bot")
TRANSFORM = Transformer.from_crs(25831, 4326, always_xy=True)

TW_KEYS = {
    "ck": os.getenv("TW_CONSUMER_KEY"),
    "cs": os.getenv("TW_CONSUMER_SECRET"),
    "at": os.getenv("TW_ACCESS_TOKEN"),
    "as": os.getenv("TW_ACCESS_SECRET"),
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)s  %(message)s")

# ---------- ESTADO -----------------------------------------------------
def load_state() -> int:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text()).get("last_id", -1)
    return -1

def save_state(last_id: int):
    STATE_FILE.write_text(json.dumps({"last_id": last_id}))

# ---------- CONSULTA ARCGIS -------------------------------------------
def fetch_features(limit=100):
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": (
            "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2"
        ),
        "orderByFields": "ACT_DAT_ACTUACIO%20desc",
        "resultRecordCount": limit,
        "returnGeometry": "true",
        "cacheHint": "true",
    }
    if API_KEY:
        params["token"] = API_KEY

    r = requests.get(f"{LAYER_URL}/query", params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        logging.error("ArcGIS error %s: %s", data["error"]["code"], data["error"]["message"])
        return []
    feats = data.get("features", [])
    logging.info("ArcGIS devolvió %s intervenciones", len(feats))
    return feats

# ---------- UTILIDADES -------------------------------------------------
def tipo_val(attrs):
    desc = (attrs.get("TAL_DESC_ALARMA1","")+" "+attrs.get("TAL_DESC_ALARMA2","")).lower()
    if "forestal" in desc or "vegetació" in desc or "vegetacion" in desc:
        return 1
    if "agrí" in desc:
        return 2
    return 3  # urbà u otros

def classify_incident(attrs):
    return {1:"forestal", 2:"agrícola", 3:"urbà"}[tipo_val(attrs)]

def utm_to_latlon(x, y):
    lon, lat = TRANSFORM.transform(x, y)
    return lat, lon

def place_from_geom(attrs, geom):
    if geom:
        lat, lon = utm_to_latlon(geom["x"], geom["y"])
        try:
            loc = GEOCODER.reverse((lat, lon), exactly_one=True, timeout=8, language="ca")
            if loc:
                return loc.address.split(",")[0]
        except Exception:
            pass
    return "ubicació desconeguda"

def tweet_body(attrs, place):
    hora = datetime.fromtimestamp(attrs["ACT_DAT_ACTUACIO"]/1000, tz=timezone.utc)\
           .astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    return (f"🔥 Incendi {classify_incident(attrs)} a {place}\n"
            f"🕒 {hora}  |  🚒 {attrs['ACT_NUM_VEH']} dotacions treballant\n"
            f"{MAPA_OFICIAL}")

def send(text, api):
    if IS_TEST_MODE:
        print("TUIT SIMULADO:\n" + text + "\n")
    else:
        api.update_status(text)

# ---------- MAIN -------------------------------------------------------
def main():
    # Twitter API (solo prod)
    api = None
    if not IS_TEST_MODE and all(TW_KEYS.values()):
        auth = tweepy.OAuth1UserHandler(TW_KEYS["ck"], TW_KEYS["cs"],
                                        TW_KEYS["at"], TW_KEYS["as"])
        api = tweepy.API(auth)

    last_id = load_state()
    feats   = fetch_features()
    if not feats:
        return

    # --- filtrar candidatos (fase actiu o sin fase + dotacions ≥ mínimo) ---
    actius = [
        f for f in feats
        if (f["attributes"]["ACT_NUM_VEH"] >= MIN_DOTACIONS and
            f["attributes"]["COM_FASE"].lower() in ("", "actiu"))
    ]

    # ordenar por prioridad
    actius.sort(
        key=lambda f: (
            -f["attributes"]["ACT_NUM_VEH"],  # más dotacions
            tipo_val(f["attributes"]),        # forestal < agrícola < urbà
            -f["attributes"]["ACT_DAT_ACTUACIO"]
        )
    )

    tweets = []

    # Selección principal (si existe); si no, la más reciente global
    if actius:
        tweets.append(actius[0])
    else:
        tweets.append(feats[0])  # fallback: intervención más reciente

    # Segunda intervención (si hay otra de distinta OID)
    for f in actius[1:]:
        if f["attributes"]["ESRI_OID"] != tweets[0]["attributes"]["ESRI_OID"]:
            tweets.append(f)
            break
    # máximo 2 tweets
    tweets = tweets[:2]

    # Publicación
    max_id = last_id
    for ft in tweets:
        a = ft["attributes"]
        place = place_from_geom(a, ft.get("geometry"))
        send(tweet_body(a, place), api)
        max_id = max(max_id, a["ESRI_OID"])

    save_state(max_id)

if __name__ == "__main__":
    main()

