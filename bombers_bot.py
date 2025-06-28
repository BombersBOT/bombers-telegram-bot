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
import tweepy

# Configuración logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Variables de entorno
MODE_TEST = os.getenv("MODE_TEST", "True").lower() == "true"
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "5"))
GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot_1.0")

# Twitter credentials
TW_CONSUMER_KEY = os.getenv("TW_CONSUMER_KEY")
TW_CONSUMER_SECRET = os.getenv("TW_CONSUMER_SECRET")
TW_ACCESS_TOKEN = os.getenv("TW_ACCESS_TOKEN")
TW_ACCESS_SECRET = os.getenv("TW_ACCESS_SECRET")

# URL de la capa ArcGIS
ARCGIS_LAYER_URL = os.getenv("ARCGIS_LAYER_URL")  # Ej: https://.../FeatureServer/0

if not ARCGIS_LAYER_URL:
    logging.error("Falta configurar ARCGIS_LAYER_URL")
    exit(1)

# Inicializar geocoder
geolocator = Nominatim(user_agent=GEOCODER_USER_AGENT)

# Inicializar Twitter cliente (solo si no es modo test)
if not MODE_TEST:
    auth = tweepy.OAuth1UserHandler(
        TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET
    )
    twitter_api = tweepy.API(auth)

logging.info(f"Modo test: {MODE_TEST}")
logging.info(f"Mínimo dotacions requerido para tuitear: {MIN_DOTACIONS}")

def reverse_geocode(lat, lon):
    try:
        location = geolocator.reverse((lat, lon), language="ca")
        if location and location.address:
            return location.address
    except Exception as e:
        logging.warning(f"Error en reverse geocode: {e}")
    return None

def get_fire_type(desc):
    desc = desc.lower()
    if "urbana" in desc:
        return "urbà"
    elif "agrícola" in desc or "agrícola" in desc:
        return "agrícola"
    elif "vegetació" in desc or "forestal" in desc:
        return "forestal"
    else:
        return "desconegut"

def get_latest_interventions():
    query_url = f"{ARCGIS_LAYER_URL}/query"
    params = {
        "where": "TAL_COD_ALARMA1 = 'IV' AND ACT_NUM_VEH > 0",
        "outFields": "ACT_NUM_ACTUACIO,ACT_DAT_ACTUACIO,TAL_DESC_ALARMA1,MUNICIPI_SIG,ACT_X_UTM,ACT_Y_UTM,ACT_URGENT,ACT_NUM_VEH",
        "orderByFields": "ACT_NUM_ACTUACIO DESC",
        "f": "json",
        "resultRecordCount": 5,
    }
    resp = requests.get(query_url, params=params)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", [])
    logging.info(f"Número de intervenciones consultadas: {len(features)}")
    return features

def utm_to_latlon(x, y, zone=31, northern_hemisphere=True):
    # Para UTM zona 31N (Catalunya). Usa pyproj si disponible
    try:
        import pyproj
        proj_utm = pyproj.Proj(proj="utm", zone=zone, ellps="WGS84", south=not northern_hemisphere)
        lon, lat = proj_utm(x, y, inverse=True)
        return lat, lon
    except ImportError:
        logging.warning("pyproj no está instalado, se usarán coordenadas UTM sin convertir")
        return y, x  # Simple fallback, no exacto

def main():
    features = get_latest_interventions()
    if not features:
        logging.info("No hay intervenciones recientes.")
        return

    intervencion = features[0]["attributes"]
    id_interv = intervencion.get("ACT_NUM_ACTUACIO")
    fecha_utc = intervencion.get("ACT_DAT_ACTUACIO")
    desc_alarma = intervencion.get("TAL_DESC_ALARMA1", "")
    municipio = intervencion.get("MUNICIPI_SIG", "")
    x_utm = intervencion.get("ACT_X_UTM")
    y_utm = intervencion.get("ACT_Y_UTM")
    urgent = intervencion.get("ACT_URGENT")
    dotacions = intervencion.get("ACT_NUM_VEH", 0)

    logging.info(f"Intervención {id_interv} con {dotacions} dotacions.")

    # Convertir fecha UTC a Madrid
    if fecha_utc:
        # El campo fecha_utc está en milisegundos desde epoch
        try:
            fecha_ts = int(fecha_utc) / 1000
            dt_utc = datetime.utcfromtimestamp(fecha_ts).replace(tzinfo=pytz.utc)
            madrid_tz = pytz.timezone("Europe/Madrid")
            dt_madrid = dt_utc.astimezone(madrid_tz)
            hora_madrid = dt_madrid.strftime("%H:%M")
        except Exception as e:
            logging.warning(f"Error convirtiendo fecha: {e}")
            hora_madrid = "Desconocida"
    else:
        hora_madrid = "Desconocida"

    # Convertir coordenadas UTM a lat/lon
    if x_utm is not None and y_utm is not None:
        lat, lon = utm_to_latlon(x_utm, y_utm)
    else:
        lat, lon = None, None

    if lat is not None and lon is not None:
        direccion = reverse_geocode(lat, lon)
    else:
        direccion = None

    tipo_incendi = get_fire_type(desc_alarma)

    # Construir texto
    loc_text = direccion if direccion else municipio if municipio else f"{lat},{lon}" if lat and lon else "ubicació desconeguda"

    tweet_text = (
        f"🔥 Incendi {tipo_incendi} important a {loc_text}\n"
        f"🕒 {hora_madrid}  |  🚒 {dotacions} dotacions treballant\n"
        f"https://experience.arcgis.com/experience/f6172fd2d6974bc0a8c51e3a6bc2a735"
    )

    # Filtro mínimo dotacions (solo si no es test)
    if not MODE_TEST and dotacions < MIN_DOTACIONS:
        logging.info(f"Intervención {id_interv} con {dotacions} dotacions (<{MIN_DOTACIONS}). No se tuitea.")
        logging.info("PREVISUALIZACIÓN (no se publica):\n" + tweet_text)
        return

    # Mostrar previsualización
    logging.info("PREVISUALIZACIÓN (o tuiteo real si no es test):\n" + tweet_text)

    # Si no es modo test, tuitear
    if not MODE_TEST:
        try:
            twitter_api.update_status(tweet_text)
            logging.info("Tweet enviado correctamente.")
        except Exception as e:
            logging.error(f"Error al enviar tweet: {e}")

if __name__ == "__main__":
    main()
