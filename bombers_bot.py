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
        # ¡IMPORTANTE! Hemos reducido los outFields al mínimo + MUN_NOM_MUNICIPI
        # para probar si el problema es la cantidad de campos solicitados.
        "outFields": (
            "ESRI_OID,ACT_NUM_VEH,COM_FASE,ACT_DAT_ACTUACIO,MUN_NOM_MUNICIPI" 
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
        # Aquí verificamos si el error 400 es por el parámetro de consulta.
        logging.error("ArcGIS error %s: %s", data["error"]["code"], data["error"]["message"])
        # Si el error persiste con MUN_NOM_MUNICIPI, volvemos a la consulta sin él
        # y registramos que no pudimos obtener el municipio directamente.
        if data["error"]["code"] == 400 and "Invalid query parameters" in data["error"]["message"]:
            logging.warning("No se pudo obtener MUN_NOM_MUNICIPI directamente de ArcGIS. Intentando sin él.")
            params["outFields"] = ("ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
                                   "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2")
            try:
                r = session.get(f"{LAYER_URL}/query", params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                if "error" in data: # Doble chequeo por si hay otro error
                    logging.error("ArcGIS fallback error %s: %s", data["error"]["code"], data["error"]["message"])
                    return []
                # Añadimos un marcador para saber que el municipio no vino de ArcGIS
                for feature in data.get("features", []):
                    feature["attributes"]["MUN_NOM_MUNICIPI_FROM_ARCGIS"] = False
                return data.get("features", [])
            except requests.exceptions.RequestException as e:
                logging.error(f"Error de conexión en fallback de ArcGIS: {e}")
                return []

        return [] # Si es otro tipo de error de ArcGIS, simplemente retornamos vacío
    
    # Marcador para saber que el municipio SÍ vino de ArcGIS
    for feature in data.get("features", []):
        feature["attributes"]["MUN_NOM_MUNICIPI_FROM_ARCGIS"] = True
    return data.get("features", [])

# ---------------- UTILIDADES -------------------------------------------
def tipo_val(a):
    d = (a.get("TAL_DESC_ALARMA1","")+" "+a.get("TAL_DESC_ALARMA2","")).lower()
    return 1 if "forestal" in d or "vegetació" in d else (2 if "agrí" in d else 3)

def classify(a): return {1:"forestal", 2:"agrícola", 3:"urbà"}[tipo_val(a)]

def utm_to_latlon(x, y):
    lon, lat = TRANSFORM.transform(x, y)
    return lat, lon

def get_address_components_from_coords(geom):
    """
    Obtiene la dirección completa de las coordenadas y la parsea en componentes.
    Devuelve un diccionario con 'street', 'municipality', 'full_address'.
    """
    street = ""
    municipality = ""
    full_address = "ubicació desconeguda"

    if geom:
        lat, lon = utm_to_latlon(geom["x"], geom["y"])
        try:
            loc = GEOCODER.reverse((lat, lon), exactly_one=True, timeout=15, language="ca")
            if loc and loc.address:
                full_address = loc.address
                # Intentar extraer componentes más específicos de Nominatim si están disponibles
                address_parts = loc.raw.get('address', {})
                street = address_parts.get('road', '')
                if not street: # A veces 'road' no está, buscar en otras propiedades comunes de calle
                    street = address_parts.get('building', '') or address_parts.get('amenity', '')
                
                municipality = address_parts.get('city', '') or \
                               address_parts.get('town', '') or \
                               address_parts.get('village', '')
                
                # Fallback para municipio si no se encuentra directamente
                if not municipality:
                    parts = [p.strip() for p in full_address.split(',')]
                    for p in reversed(parts): # Buscar desde el final
                        if not any(char.isdigit() for char in p) and len(p) > 2 and p.lower() not in ["catalunya", "españa"]:
                            municipality = p
                            break

        except Exception as e:
            logging.debug(f"Error al geocodificar: {e}")
            pass
    
    return {"street": street, "municipality": municipality, "full_address": full_address}


def format_intervention(a, geom):
    # Intentar obtener el municipio de ArcGIS
    municipio_arcgis = a.get("MUN_NOM_MUNICIPI")
    
    # Bandera para saber si el municipio vino de ArcGIS o no (establecida en fetch_features)
    municipio_from_arcgis_success = a.get("MUN_NOM_MUNICIPI_FROM_ARCGIS", False)

    calle = ""
    municipio_final = "ubicació desconeguda"

    if municipio_arcgis and municipio_from_arcgis_success:
        municipio_final = municipio_arcgis # Usar el de ArcGIS si lo tenemos y se obtuvo con éxito
    
    # Siempre intentamos obtener la calle y un municipio de la geocodificación
    # Esto es útil si el municipio de ArcGIS falla o como respaldo para la calle
    address_components = get_address_components_from_coords(geom)
    calle = address_components["street"]

    # Si el municipio de ArcGIS falló o no estaba disponible, usamos el de la geocodificación
    if municipio_final == "ubicació desconeguda" or not municipio_arcgis:
        municipio_final = address_components["municipality"] if address_components["municipality"] else "ubicació desconeguda"


    hora = datetime.fromtimestamp(a["ACT_DAT_ACTUACIO"]/1000, tz=timezone.utc)\
               .astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    
    location_str = ""
    if calle and municipio_final != "ubicació desconeguda":
        location_str = f"{calle}, {municipio_final}"
    elif municipio_final != "ubicació desconeguda":
        location_str = municipio_final
    elif calle: # Si solo tenemos calle y el municipio es "desconocido"
        location_str = calle
    else:
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
    most_recent_feature = next((f for f in feats if f["attributes"]["ESRI_OID"] > last_id), None)

    intervenciones_para_tweet = []

    if most_recent_feature:
        intervenciones_para_tweet.append({"title": "Actuació més recent", "feature": most_recent_feature})

    # Identificar la actuación más relevante (cumpliendo criterios de dotaciones/fase)
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
        potential_relevant = candidatos_activos[0]

        # Solo la añadimos si no es la misma que la "más reciente"
        if most_recent_feature is None or potential_relevant["attributes"]["ESRI_OID"] != most_recent_feature["attributes"]["ESRI_OID"]:
            most_relevant_feature = potential_relevant
            intervenciones_para_tweet.append({"title": "Incendi més rellevant", "feature": most_relevant_feature})
    
    # Asegurar el orden final: "Actuació més recent" siempre primero si ambas existen
    if len(intervenciones_para_tweet) == 2:
        if intervenciones_para_tweet[0]["title"] == "Incendi més rellevant":
            intervenciones_para_tweet.reverse() 

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
