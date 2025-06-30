#!/usr/bin/env python3
"""
bombers_bot.py

Publica (o simula) las intervenciones de Bombers priorizando:
1) fase “actiu” (o sin fase) 2) nº dotacions 3) tipo (forestal > agrícola > urbà).

Requisitos:
    requests    geopy    tweepy>=4.0.0    pyproj
"""

import os, json, logging, requests, tweepy
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from geopy.geocoders import Nominatim
from pyproj import Transformer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------- CONFIG ------------------------------------------------
LAYER_URL = ("https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/"
             "ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0")
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "3"))     # mínimo dotacions
IS_TEST_MODE  = os.getenv("IS_TEST_MODE", "true").lower() == "true"
API_KEY       = os.getenv("ARCGIS_API_KEY", "")
MAPA_OFICIAL  = "https://interior.gencat.cat/ca/arees_dactuacio/bombers/actuacions-de-bombers/"

STATE_FILE = Path("state.json")
GEOCODER   = Nominatim(user_agent="bombers_bot")
TRANSFORM  = Transformer.from_crs(25831, 4326, always_xy=True)

TW_KEYS = {
    "ck": os.getenv("TW_CONSUMER_KEY"),
    "cs": os.getenv("TW_CONSUMER_SECRET"),
    "at": os.getenv("TW_ACCESS_TOKEN"),
    "as": os.getenv("TW_ACCESS_SECRET"),
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# Configuración de reintentos para requests
retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session = requests.Session()
session.mount('https://', HTTPAdapter(max_retries=retries))


# ---------------- ESTADO -----------------------------------------------
def load_state() -> int:
    return json.loads(STATE_FILE.read_text()).get("last_id", -1) if STATE_FILE.exists() else -1

def save_state(last_id: int):
    STATE_FILE.write_text(json.dumps({"last_id": last_id}))

# ---------------- CONSULTA ARCGIS --------------------------------------
def fetch_features(limit=100):
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": (
            "ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2,MUN_NOM_MUNICIPI" # Incluimos MUN_NOM_MUNICIPI
        ),
        "orderByFields": "ACT_DAT_ACTUACIO DESC",
        "resultRecordCount": limit,
        "returnGeometry": "true",
        "cacheHint": "true",
    }
    if API_KEY:
        params["token"] = API_KEY
    
    try:
        r = session.get(f"{LAYER_URL}/query", params=params, timeout=30)
        r.raise_for_status()
    except requests.exceptions.Timeout:
        logging.error("Error de timeout al consultar ArcGIS. El servidor no respondió a tiempo.")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Error de conexión al consultar ArcGIS: {e}")
        return []

    data = r.json()
    if "error" in data:
        logging.error("ArcGIS error %s: %s", data["error"]["code"], data["error"]["message"])
        return []
    return data.get("features", [])

# ---------------- UTILIDADES -------------------------------------------
def tipo_val(a):
    d = (a.get("TAL_DESC_ALARMA1","")+" "+a.get("TAL_DESC_ALARMA2","")).lower()
    return 1 if "forestal" in d or "vegetació" in d else (2 if "agrí" in d else 3)

def classify(a): return {1:"forestal", 2:"agrícola", 3:"urbà"}[tipo_val(a)]

def utm_to_latlon(x, y):
    lon, lat = TRANSFORM.transform(x, y)
    return lat, lon

def get_street_from_coords(geom):
    """
    Intenta obtener solo el nombre de la calle (o lo más cercano) de las coordenadas.
    """
    if geom:
        lat, lon = utm_to_latlon(geom["x"], geom["y"])
        try:
            loc = GEOCODER.reverse((lat, lon), exactly_one=True, timeout=15, language="ca") 
            if loc and loc.address:
                parts = loc.address.split(',')[0].strip().split(' ')
                street_name_parts = []
                for part in parts:
                    if any(char.isdigit() for char in part):
                        break 
                    street_name_parts.append(part)
                return " ".join(street_name_parts) if street_name_parts else ""
        except Exception as e:
            logging.debug(f"Error al geocodificar para la calle: {e}")
            pass
    return ""


def format_intervention(a, geom):
    # Obtener el municipio directamente de los atributos de ArcGIS
    municipio = a.get("MUN_NOM_MUNICIPI", "ubicació desconeguda")
    
    # Obtener la calle de la geocodificación
    calle = get_street_from_coords(geom)

    hora = datetime.fromtimestamp(a["ACT_DAT_ACTUACIO"]/1000, tz=timezone.utc)\
               .astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    
    location_str = ""
    if calle:
        # Si tenemos calle, la combinamos con el municipio (que siempre deberíamos tener de ArcGIS)
        location_str = f"{calle}, {municipio}"
    else:
        # Si no pudimos obtener la calle, usamos solo el municipio
        location_str = municipio
    
    # Si al final la ubicación sigue siendo 'desconocida', es que ni ArcGIS ni geocodificador lo dieron
    if location_str.lower() == "ubicació desconeguda" and not calle:
        location_str = "ubicació desconeguda"

    return (f"🔥 Incendi {classify(a)} a {location_str}\n"
            f"🕒 {hora} | 🚒 {a['ACT_NUM_VEH']} dotacions treballant")

def send(text, api):
    if IS_TEST_MODE:
        print("TUIT SIMULADO:\n" + text + "\n")
    else:
        api.update_status(text)

# ---------------- MAIN --------------------------------------------------
def main():
    api = None
    if not IS_TEST_MODE and all(TW_KEYS.values()):
        auth = tweepy.OAuth1UserHandler(TW_KEYS["ck"], TW_KEYS["cs"], TW_KEYS["at"], TW_KEYS["as"])
        api = tweepy.API(auth)

    last_id = load_state()
    feats = fetch_features()
    if not feats:
        logging.info("ArcGIS devolvió 0 features.")
        return

    # Candidatos activos con dotaciones >= mínimo y más recientes que la última ID procesada
    candidatos_activos = [
        f for f in feats
        if f["attributes"]["ACT_NUM_VEH"] >= MIN_DOTACIONS
           and (str(f["attributes"].get("COM_FASE") or "")).lower() in ("", "actiu")
           and f["attributes"]["ESRI_OID"] > last_id
    ]
    
    # La intervención más reciente de todas las nuevas (sin importar dotaciones o fase)
    first_new_overall = next((f for f in feats if f["attributes"]["ESRI_OID"] > last_id), None)

    intervenciones_para_tweet = []

    # 1. Identificar la actuación más reciente (de todas las nuevas)
    most_recent_feature = None
    if first_new_overall:
        most_recent_feature = first_new_overall
        intervenciones_para_tweet.append({"title": "Actuació més recent", "feature": most_recent_feature})

    # 2. Identificar la actuación más relevante (cumpliendo criterios de dotaciones/fase)
    # y que NO sea la misma que la más reciente si ya la incluimos
    most_relevant_feature = None
    if candidatos_activos:
        # Ordenamos los candidatos activos por relevancia (dotaciones, tipo, fecha)
        candidatos_activos.sort(
            key=lambda f: (
                -f["attributes"]["ACT_NUM_VEH"],
                tipo_val(f["attributes"]),
                -f["attributes"]["ACT_DAT_ACTUACIO"]
            )
        )
        # La primera de esta lista es la más relevante
        potential_relevant = candidatos_activos[0]

        # Solo la añadimos si no es la misma que la "más reciente"
        if most_recent_feature is None or potential_relevant["attributes"]["ESRI_OID"] != most_recent_feature["attributes"]["ESRI_OID"]:
            most_relevant_feature = potential_relevant
            intervenciones_para_tweet.append({"title": "Incendi més rellevant", "feature": most_relevant_feature})
    
    # Si tenemos dos intervenciones, aseguramos el orden: la más reciente primero, luego la más relevante.
    # Necesitamos una forma robusta de ordenar si ambas existen.
    # El orden en `intervenciones_para_tweet` ya debería reflejarlo si `most_recent_feature` se añade primero.
    # Si solo hay una, simplemente se añade.

    # Si por alguna razón la "más relevante" se añade antes, o queremos forzar el orden final:
    # reordenar si ambas están presentes
    if len(intervenciones_para_tweet) == 2:
        # Aseguramos que "Actuació més recent" vaya primero y "Incendi més rellevant" segundo
        if intervenciones_para_tweet[0]["title"] == "Incendi més rellevant":
            intervenciones_para_tweet.reverse() # Invertir el orden si la relevante está primera

    if not intervenciones_para_tweet:
        logging.info("No hay intervenciones nuevas para tuitear.")
        return

    tweet_parts = []
    max_id = last_id

    for item in intervenciones_para_tweet:
        title = item["title"]
        feature = item["feature"]
        a = feature["attributes"]
        geom = feature.get("geometry")
        
        formatted_interv = format_intervention(a, geom)
        tweet_parts.append(f"• {title}:\n{formatted_interv}")
        max_id = max(max_id, a["ESRI_OID"])
    
    final_tweet_text = "\n\n".join(tweet_parts) + f"\n\n{MAPA_OFICIAL}"
    send(final_tweet_text, api)
    
    save_state(max_id)

if __name__ == "__main__":
    main()
