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
# IS_TEST_MODE se establece a True por defecto (simulación)
# Para publicar en real, la variable de entorno IS_TEST_MODE debe ser "false"
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
retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
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
            "ESRI_OID,ACT_NUM_VEH,COM_FASE,ACT_DAT_ACTUACIO,"
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
        logging.error("Timeout al consultar ArcGIS. Servidor no respondió a tiempo.")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Error de conexión al consultar ArcGIS: {e}")
        # Si hay un error general de request, intentamos el fallback por si es el MUN_NOM_MUNICIPI
        if "400" in str(e) and "Invalid query parameters" in str(e):
             logging.warning("Error 400 al obtener MUN_NOM_MUNICIPI. Intentando sin él.")
        else:
             logging.warning("Error de ArcGIS, pero no el esperado con MUN_NOM_MUNICIPI. Reintentando consulta básica.")
        
        # Fallback si falla la consulta con MUN_NOM_MUNICIPI
        params["outFields"] = ("ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
                               "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2")
        try:
            r = session.get(f"{LAYER_URL}/query", params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                logging.error("Fallback ArcGIS error %s: %s", data["error"]["code"], data["error"]["message"])
                return []
            # Añadimos un marcador para saber que el municipio no vino de ArcGIS
            for feature in data.get("features", []):
                feature["attributes"]["_municipio_from_arcgis_success"] = False
            return data.get("features", [])
        except requests.exceptions.RequestException as e_fallback:
            logging.error(f"Error en fallback de ArcGIS: {e_fallback}")
            return []

    data = r.json()
    if "error" in data:
        logging.error("ArcGIS error %s: %s", data["error"]["code"], data["error"]["message"])
        # Si el error es específico de parámetros y veníamos de una consulta con MUN_NOM_MUNICIPI,
        # intentamos el fallback aquí también, si no se hizo antes.
        if data["error"]["code"] == 400 and "Invalid query parameters" in data["error"]["message"]:
            logging.warning("Error 400 al obtener MUN_NOM_MUNICIPI. Intentando sin él.")
            params["outFields"] = ("ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
                                   "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2")
            try:
                r = session.get(f"{LAYER_URL}/query", params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                if "error" in data:
                    logging.error("Fallback ArcGIS error %s: %s", data["error"]["code"], data["error"]["message"])
                    return []
                for feature in data.get("features", []):
                    feature["attributes"]["_municipio_from_arcgis_success"] = False
                return data.get("features", [])
            except requests.exceptions.RequestException as e_fallback:
                logging.error(f"Error en fallback de ArcGIS: {e_fallback}")
                return []
        return []
    
    for feature in data.get("features", []):
        feature["attributes"]["_municipio_from_arcgis_success"] = True
    return data.get("features", [])


# ---------------- UTILIDADES -------------------------------------------
def tipo_val(a):
    d = (a.get("TAL_DESC_ALARMA1","")+" "+a.get("TAL_DESC_ALARMA2","")).lower()
    # MODIFICACIÓN AQUI: Priorizar "agrícola" sobre "vegetación"
    if "agrí" in d:         # Detectar "agrícola" (por "agrí" en "agrícola", "agrícoles", etc.)
        return 2            # Tipo: agrícola
    elif "forestal" in d or "vegetació" in d: # Luego, si es "forestal" o "vegetación" (que no sea agrícola)
        return 1            # Tipo: forestal
    else:                   # Si no encaja en las anteriores
        return 3            # Tipo: urbano

def classify(a):
    return {1: "forestal", 2: "agrícola", 3: "urbà"}[tipo_val(a)]

def utm_to_latlon(x, y):
    lon, lat = TRANSFORM.transform(x, y)
    return lat, lon

def get_address_components_from_coords(geom):
    """
    Obtiene la dirección completa de las coordenadas y la parsea en componentes.
    Devuelve un diccionario con 'street', 'municipality'.
    """
    street = ""
    municipality = ""
    
    if geom:
        lat, lon = utm_to_latlon(geom["x"], geom["y"])
        try:
            loc = GEOCODER.reverse((lat, lon), exactly_one=True, timeout=15, language="ca")
            if loc and loc.raw: # Acceder a .raw para un mejor parsing
                address_parts = loc.raw.get('address', {})
                # Priorizar campos específicos para calle y municipio
                street = address_parts.get('road', '') or address_parts.get('building', '') or address_parts.get('amenity', '')
                municipality = address_parts.get('city', '') or \
                               address_parts.get('town', '') or \
                               address_parts.get('village', '') or \
                               address_parts.get('county', '') # County a veces puede ser un municipio más amplio

                # Si el municipio sigue vacío, intentar de la dirección completa
                if not municipality and loc.address:
                    parts = [p.strip() for p in loc.address.split(',')]
                    # Buscar desde el final, evitando números y términos genéricos
                    for p in reversed(parts):
                        if not any(char.isdigit() for char in p) and len(p) > 2 and p.lower() not in ["catalunya", "españa"]:
                            municipality = p
                            break

        except Exception as e:
            logging.debug(f"Error al geocodificar: {e}")
            pass
    
    return {"street": street, "municipality": municipality}


def format_intervention(a, geom):
    # Intentar obtener el municipio de ArcGIS
    municipio_arcgis = a.get("MUN_NOM_MUNICIPI")
    _municipio_from_arcgis_success = a.get("_municipio_from_arcgis_success", False)

    calle_geocoded = ""
    municipio_geocoded = ""
    
    # Siempre obtenemos los componentes de la geocodificación como respaldo/para la calle
    address_components = get_address_components_from_coords(geom)
    calle_geocoded = address_components["street"]
    municipio_geocoded = address_components["municipality"]

    municipio_final = "ubicació desconeguda"
    
    # Lógica de prioridad para el municipio: ArcGIS > Geocodificador > "desconegut"
    if _municipio_from_arcgis_success and municipio_arcgis:
        municipio_final = municipio_arcgis
    elif municipio_geocoded:
        municipio_final = municipio_geocoded
    
    # La calle siempre viene de la geocodificación
    calle_final = calle_geocoded if calle_geocoded else ""

    hora = datetime.fromtimestamp(a["ACT_DAT_ACTUACIO"]/1000, tz=timezone.utc)\
               .astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    
    location_str = ""
    if calle_final and municipio_final != "ubicació desconeguda":
        location_str = f"{calle_final}, {municipio_final}"
    elif municipio_final != "ubicació desconeguda":
        location_str = municipio_final
    elif calle_final: # Si solo tenemos calle (y el municipio es desconocido)
        location_str = calle_final
    else:
        location_str = "ubicació desconeguda"

    # Texto principal de la intervención
    intervention_text = (f"🔥 {classify(a)} a {location_str}\n"
                         f"🕒 {hora} | 🚒 {a['ACT_NUM_VEH']} dot.")
    
    return intervention_text

def send(text, api):
    if IS_TEST_MODE:
        print("TUIT SIMULADO:\n" + text + "\n")
    else:
        api.update_status(text)

# ---------------- MAIN --------------------------------------------------
def main():
    api = None
    # Solo intenta autenticarse si NO estamos en modo de prueba y las claves están presentes
    if not IS_TEST_MODE and all(TW_KEYS.values()):
        try:
            auth = tweepy.OAuth1UserHandler(TW_KEYS["ck"], TW_KEYS["cs"], TW_KEYS["at"], TW_KEYS["as"])
            api = tweepy.API(auth)
            # Verificar credenciales para detectar errores tempranamente
            api.verify_credentials()
            logging.info("Autenticación con Twitter exitosa.")
        except tweepy.TweepyException as e:
            logging.error(f"Error de autenticación con Twitter: {e}. Asegúrate de que las claves son correctas y la API está accesible.")
            # Si la autenticación falla, salimos para evitar intentar publicar
            return


    last_id = load_state()
    feats = fetch_features()
    if not feats:
        logging.info("ArcGIS devolvió 0 features.")
        return

    # Filtra solo las nuevas intervenciones (por ESRI_OID)
    new_feats = [f for f in feats if f["attributes"]["ESRI_OID"] > last_id]

    # La intervención más reciente de todas las nuevas
    most_recent_feature = None
    if new_feats:
        # Ordenar por fecha de actuación para encontrar la más reciente entre todas las nuevas
        new_feats.sort(key=lambda f: f["attributes"]["ACT_DAT_ACTUACIO"], reverse=True)
        most_recent_feature = new_feats[0]

    # Candidatos activos con dotaciones >= mínimo y de las nuevas intervenciones
    candidatos_activos = [
        f for f in new_feats # Buscar solo entre las nuevas
        if f["attributes"]["ACT_NUM_VEH"] >= MIN_DOTACIONS
           and (str(f["attributes"].get("COM_FASE") or "")).lower() in ("", "actiu")
    ]
    
    intervenciones_para_tweet = []

    if most_recent_feature:
        intervenciones_para_tweet.append({"title": "Act. més recent", "feature": most_recent_feature}) # Más conciso

    # Identificar la actuación más relevante (cumpliendo criterios de dotaciones/fase)
    # y que NO sea la misma que la más reciente si ya la incluimos
    if candidatos_activos:
        candidatos_activos.sort(
            key=lambda f: (
                -f["attributes"]["ACT_NUM_VEH"],
                tipo_val(f["attributes"]),
                -f["attributes"]["ACT_DAT_ACTUACIO"]
            )
        )
        potential_relevant = candidatos_activos[0]

        # Solo la añadimos si es diferente de la "más reciente" ya incluida
        if most_recent_feature is None or potential_relevant["attributes"]["ESRI_OID"] != most_recent_feature["attributes"]["ESRI_OID"]:
            intervenciones_para_tweet.append({"title": "Inc. més rellevant", "feature": potential_relevant}) # Más conciso
    
    # Asegurar el orden final: "Act. més recent" siempre primero si ambas existen
    if len(intervenciones_para_tweet) == 2:
        if intervenciones_para_tweet[0]["title"] == "Inc. més rellevant":
            intervenciones_para_tweet.reverse() 

    if not intervenciones_para_tweet:
        logging.info("No hay intervenciones nuevas para tuitear.")
        return

    tweet_parts = []
    max_id = last_id

    for item in intervenciones_para_tweet:
        title_text = item["title"]
        feature = item["feature"]
        a = feature["attributes"]
        geom = feature.get("geometry")
        
        formatted_interv = format_intervention(a, geom)
        tweet_parts.append(f"• {title_text}:\n{formatted_interv}")
        max_id = max(max_id, a["ESRI_OID"])
    
    final_tweet_text = "\n\n".join(tweet_parts) + f"\n\nFuente: {MAPA_OFICIAL}"
    
    # Solo enviar el tweet si no estamos en modo de prueba
    send(final_tweet_text, api)
    
    save_state(max_id)

if __name__ == "__main__":
    main()
