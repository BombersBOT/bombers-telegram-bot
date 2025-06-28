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
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import pytz

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Parámetros
ARCGIS_URL = "https://services.arcgis.com/f6172fd2d6974bc0/arcgis/rest/services/ACTUACIONS_URGENTS_ONLINE_PRO/FeatureServer/0/query"
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "5"))
GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot_1.0")
MODE_TEST = os.getenv("MODE_TEST", "True").lower() == "true"

# Inicializamos geocoder
geolocator = Nominatim(user_agent=GEOCODER_USER_AGENT)

def get_latest_interventions():
    params = {
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "OBJECTID DESC",
        "resultRecordCount": 1,
        "f": "json"
    }
    try:
        response = requests.get(ARCGIS_URL, params=params)
        response.raise_for_status()
        data = response.json()
        features = data.get("features", [])
        return features
    except Exception as e:
        logging.error(f"Error consultando ArcGIS: {e}")
        return []

def reverse_geocode(lat, lon):
    try:
        location = geolocator.reverse((lat, lon), exactly_one=True, language="ca")
        if location and location.address:
            # Devolvemos la dirección más precisa posible
            return location.address
        else:
            return None
    except GeocoderTimedOut:
        logging.warning("Geocoder timed out")
        return None
    except Exception as e:
        logging.warning(f"Reverse geocode error: {e}")
        return None

def get_fire_type(desc):
    desc_lower = desc.lower()
    if "urbana" in desc_lower or "vegetació urbana" in desc_lower:
        return "urbà"
    elif "agrícola" in desc_lower or "agricola" in desc_lower:
        return "agrícola"
    elif "vegetació" in desc_lower or "forestal" in desc_lower:
        return "forestal"
    else:
        return "desconegut"

def main():
    features = get_latest_interventions()
    if not features:
        logging.info("No hay intervenciones recientes.")
        return

    intervencion = features[0]["attributes"]
    objectid = intervencion.get("OBJECTID")
    dotacions = intervencion.get("ACT_NUM_VEH", 0)
    desc_alarma = intervencion.get("TAL_DESC_ALARMA1", "")
    municipi = intervencion.get("MUNICIPI_DPX", "")
    x_coord = intervencion.get("ACT_X_UTM_DPX")
    y_coord = intervencion.get("ACT_Y_UTM_DPX")
    data_hora = intervencion.get("ACT_DAT_ACTUACIO")

    logging.info(f"Intervención {objectid} con {dotacions} dotacions.")
    logging.info(f"Campo TAL_DESC_ALARMA1: {desc_alarma}")

    tipo_incendi = get_fire_type(desc_alarma)
    logging.info(f"Tipo de incendio detectado: {tipo_incendi}")

    # Convertir fecha a hora Madrid
    if data_hora:
        dt_utc = datetime.utcfromtimestamp(data_hora / 1000)
        madrid_tz = pytz.timezone("Europe/Madrid")
        dt_madrid = pytz.utc.localize(dt_utc).astimezone(madrid_tz)
        hora_str = dt_madrid.strftime("%H:%M")
    else:
        hora_str = "hora desconeguda"

    # Geocodificación inversa para obtener dirección
    direccion = None
    if x_coord and y_coord:
        # Convertir UTM a lat/lon si es necesario o asumir que están en lat/lon
        # Aquí suponemos que son coordenadas UTM (ETRS89 / UTM zone 31N - EPSG:25831)
        # Necesitamos convertir a lat/lon
        try:
            import pyproj
            proj_utm = pyproj.Proj("epsg:25831")
            proj_latlon = pyproj.Proj(proj="latlong", datum="WGS84")
            lon, lat = pyproj.transform(proj_utm, proj_latlon, x_coord, y_coord)
            direccion = reverse_geocode(lat, lon)
        except ImportError:
            logging.warning("pyproj no instalado, se usará coordenadas sin convertir")
            direccion = reverse_geocode(y_coord, x_coord)
        except Exception as e:
            logging.warning(f"Error en conversión coordenadas: {e}")
            direccion = reverse_geocode(y_coord, x_coord)
    else:
        logging.warning("No hay coordenadas para geocodificar")

    # Construcción texto para tweet
    lugar = direccion or municipi or "ubicació desconeguda"
    dotacions_text = f"{dotacions} dotacions" if dotacions else "sense dotacions"
    tweet = (
        f"🔥 Incendi {tipo_incendi} important a {lugar}\n"
        f"🕒 {hora_str}  |  🚒 {dotacions_text} treballant\n"
        "https://experience.arcgis.com/experience/f6172fd2d6974bc0a8c51e3a6bc2a735"
    )

    if MODE_TEST:
        logging.info("Modo test activo, no se publica el tweet.")
        logging.info("PREVISUALIZACIÓN (no se publica):")
        print(tweet)
    else:
        if dotacions >= MIN_DOTACIONS:
            # Aquí iría el código para publicar el tweet
            logging.info("Publicando tweet:")
            print(tweet)
        else:
            logging.info(f"Intervención {objectid} con {dotacions} dotacions (<{MIN_DOTACIONS}). No se tuitea.")
            logging.info("PREVISUALIZACIÓN (no se publica):")
            print(tweet)

if __name__ == "__main__":
    main()
