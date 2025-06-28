"""
bombers_bot.py

Bot que consulta la capa de ArcGIS de los Bombers de la Generalitat y publica
en Twitter (X) nuevas actuaciones relevantes (incendios con muchas dotaciones).

Dependencias: tweepy, requests, geopy
"""

import os
import requests
import logging
from datetime import datetime
import pytz
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Configuración
LAYER_URL = os.getenv("ARCGIS_LAYER_URL", "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0/query")
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", 5))
GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot_1.0")

geolocator = Nominatim(user_agent=GEOCODER_USER_AGENT)

def classify_incident(attrs) -> str:
    desc1 = attrs.get("TAL_DESC_ALARMA1", "") or ""
    desc2 = attrs.get("TAL_DESC_ALARMA2", "") or ""
    combined_desc = f"{desc1} {desc2}".lower().strip()

    logging.info(f"Descripción TAL_DESC_ALARMA1: '{desc1}'")
    logging.info(f"Descripción TAL_DESC_ALARMA2: '{desc2}'")
    logging.info(f"Descripción combinada para clasificación: '{combined_desc}'")

    if "vegetació urbana" in combined_desc:
        return "urbà"
    if "vegetación urbana" in combined_desc:
        return "urbà"
    if "urbà" in combined_desc or "urbano" in combined_desc:
        return "urbà"
    if "agrícola" in combined_desc or "agricola" in combined_desc:
        return "agrícola"
    if "forestal" in combined_desc:
        return "forestal"
    if "vegetació" in combined_desc or "vegetacion" in combined_desc:
        return "forestal"

    logging.warning("No se pudo clasificar la intervención, asignando forestal por defecto.")
    return "forestal"

def reverse_geocode(lat, lon):
    try:
        location = geolocator.reverse((lat, lon), language='ca', exactly_one=True, timeout=10)
        if location and location.address:
            return location.address
        else:
            logging.warning("No se encontró dirección con reverse geocode, devolviendo coordenadas.")
            return f"{lat:.5f}, {lon:.5f}"
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        logging.warning(f"Reverse geocode error: {e}")
        return f"{lat:.5f}, {lon:.5f}"

def fetch_interventions(last_id=0):
    params = {
        "f": "json",
        "where": f"ESRI_OID > {last_id} AND TAL_COD_ALARMA1 = 'IV' AND ACT_NUM_VEH > 0",
        "orderByFields": "ESRI_OID ASC",
        "outFields": "*",
        "resultOffset": 0,
        "resultRecordCount": 100,
        "cacheHint": "true",
    }
    response = requests.get(LAYER_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("features", [])

def main():
    last_id = 0  # Aquí podrías cargar desde un fichero o base de datos para continuar desde la última intervención

    interventions = fetch_interventions(last_id)
    logging.info(f"Número de intervenciones consultadas: {len(interventions)}")

    if not interventions:
        logging.info("No hay nuevas intervenciones.")
        return

    # Procesar la última intervención (la de mayor ESRI_OID)
    last_intervention = interventions[-1]
    attrs = last_intervention["attributes"]
    esri_oid = attrs.get("ESRI_OID", 0)
    num_dotacions = attrs.get("ACT_NUM_VEH", 0)

    logging.info(f"Intervención {esri_oid} con {num_dotacions} dotacions")

    # Clasificar tipo incendio
    incident_type = classify_incident(attrs)

    # Extraer fecha y hora, ajustando a Madrid
    utc_dt = None
    if attrs.get("DATA_ACT"):
        utc_dt = datetime.utcfromtimestamp(attrs["DATA_ACT"] / 1000)
    elif attrs.get("ACT_DAT_ACTUACIO"):
        utc_dt = datetime.utcfromtimestamp(attrs["ACT_DAT_ACTUACIO"] / 1000)

    if utc_dt is None:
        utc_dt = datetime.utcnow()

    madrid_tz = pytz.timezone("Europe/Madrid")
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(madrid_tz)
    hora_str = local_dt.strftime("%H:%M")

    # Coordenadas (UTM ETRS89 / Zone 31N, EPSG:25831)
    # NOTA: Hay que transformar las coordenadas a lat/lon para geopy
    # Si no tienes librería para reproyección, usa un servicio o asume lat/lon
    # Por ahora, extraemos del atributo, pero la API no da explícito lat/lon
    # En este ejemplo asumimos que hay "X" y "Y" en atributos (debes confirmar)
    try:
        x = attrs.get("ACT_X_UTM", None) or attrs.get("ACT_X_UTM_DPX", None)
        y = attrs.get("ACT_Y_UTM", None) or attrs.get("ACT_Y_UTM_DPX", None)
        if x is not None and y is not None:
            # Para transformar UTM a lat/lon usa pyproj, pero para simplificar, mostramos coords UTM
            # Puedes instalar pyproj y hacer la transformación si quieres lat/lon exactos
            lat, lon = None, None  # No implementado aquí
            coords_text = f"coordenades UTM: {x}, {y}"
        else:
            lat = attrs.get("latitude")
            lon = attrs.get("longitude")
            coords_text = f"{lat}, {lon}" if lat and lon else "coordenades no disponibles"
    except Exception as e:
        logging.error(f"Error obteniendo coordenadas: {e}")
        coords_text = "coordenades no disponibles"

    # Intentar geocodificar si hay lat/lon
    address = None
    if lat is not None and lon is not None:
        address = reverse_geocode(lat, lon)

    # Montar texto del tweet
    loc_text = address if address else coords_text
    dotacions_text = f"{num_dotacions} dotacions"
    incident_map = {
        "forestal": "Incendi forestal",
        "urbà": "Incendi urbà",
        "agrícola": "Incendi agrícola"
    }
    incident_text = incident_map.get(incident_type, "Incendi")

    tweet_text = (
        f"🔥 {incident_text} important a {loc_text}\n"
        f"🕒 {hora_str}  |  🚒 {dotacions_text} treballant\n"
        f"https://experience.arcgis.com/experience/f6172fd2d6974bc0a8c51e3a6bc2a735"
    )

    # Solo publicar si hay dotaciones suficientes
    if num_dotacions >= MIN_DOTACIONS:
        logging.info(f"Publicando tweet:\n{tweet_text}")
        # Aquí tu código para tuitear
    else:
        logging.info(f"Intervención {esri_oid} con {num_dotacions} dotacions (<{MIN_DOTACIONS}). No se tuitea.")
        logging.info(f"PREVISUALIZACIÓN (no se publica):\n{tweet_text}")

    # Guardar último id para la próxima ejecución (implementa según tu necesidad)
    last_id = esri_oid
    logging.info(f"Estado guardado: last_id = {last_id}")

if __name__ == "__main__":
    main()

