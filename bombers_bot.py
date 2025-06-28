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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Configuración
LAYER_URL = os.getenv("ARCGIS_LAYER_URL", "https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/ACTUACIONS_URGENTS_online_PRO/FeatureServer/0/query")
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "5"))
GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot_1.0")
MODE_TEST = True  # Cambiar a False para publicar tweets

# Inicializar geolocalizador
geolocator = Nominatim(user_agent=GEOCODER_USER_AGENT)

def reverse_geocode(lat, lon):
    try:
        location = geolocator.reverse((lat, lon), language="ca", timeout=10)
        if location and location.address:
            return location.address
        else:
            return None
    except GeocoderTimedOut:
        logging.warning("Reverse geocode timeout. Intentando de nuevo...")
        return reverse_geocode(lat, lon)
    except Exception as e:
        logging.warning(f"Reverse geocode error: {e}")
        return None

def fetch_interventions():
    params = {
        "where": "TAL_COD_ALARMA1 = 'IV' AND ACT_NUM_VEH > 0",
        "outFields": "*",
        "orderByFields": "ACT_DAT_ACTUACIO desc",
        "f": "json",
        "resultRecordCount": 1
    }
    response = requests.get(LAYER_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("features", [])

def main():
    interventions = fetch_interventions()
    logging.info(f"Número de intervenciones consultadas: {len(interventions)}")
    
    if not interventions:
        logging.info("No hay intervenciones nuevas.")
        return
    
    latest = interventions[0]
    attributes = latest["attributes"]
    
    act_id = attributes.get("ESRI_OID", "desconocido")
    dotacions = attributes.get("ACT_NUM_VEH", 0)
    
    logging.info(f"Intervención {act_id} con {dotacions} dotacions.")
    
    if dotacions < MIN_DOTACIONS:
        logging.info(f"La intervención {act_id} tiene {dotacions} dotacions (<{MIN_DOTACIONS}). No se tuitea.")
        return
    
    # Determinar tipo de incendio
    fire_text = attributes.get("TAL_DESC_ALARMA1", "").lower()
    if "forestal" in fire_text:
        fire_type = "incendi forestal"
    elif "urbana" in fire_text or "urbà" in fire_text:
        fire_type = "incendi urbà"
    elif "agrícola" in fire_text or "agricola" in fire_text:
        fire_type = "incendi agrícola"
    else:
        fire_type = "incendi"
    
    logging.info(f"Tipo de incendio detectado: {fire_type}")
    
    # Obtener ubicación
    x_utm = attributes.get("ACT_X_UTM_DPX")
    y_utm = attributes.get("ACT_Y_UTM_DPX")
    
    # Nota: si tienes coordenadas en UTM, hay que convertirlas a lat/lon.
    # El sistema es EPSG:25831 (ETRS89 / UTM zone 31N).
    # Para la simplificación, se asume que ACT_X_UTM_DPX es 'easting' y ACT_Y_UTM_DPX es 'northing'.
    # Usamos pyproj para convertir:
    from pyproj import Transformer
    transformer = Transformer.from_crs("epsg:25831", "epsg:4326", always_xy=True)
    if x_utm is None or y_utm is None:
        # Si no hay coordenadas UTM, no se puede geolocalizar bien
        location_str = "ubicació desconeguda"
        lat, lon = None, None
    else:
        lon, lat = transformer.transform(x_utm, y_utm)
        address = reverse_geocode(lat, lon)
        location_str = address if address else f"{lat:.5f}, {lon:.5f}"
    
    # Obtener fecha y hora en hora Madrid
    dt_utc = attributes.get("ACT_DAT_ACTUACIO")
    if dt_utc:
        dt = datetime.utcfromtimestamp(dt_utc / 1000)
        madrid_tz = pytz.timezone("Europe/Madrid")
        dt_madrid = dt.replace(tzinfo=pytz.utc).astimezone(madrid_tz)
        hora_str = dt_madrid.strftime("%H:%M")
    else:
        hora_str = "hora desconeguda"
    
    # Construir tweet
    tweet = (
        f"🔥 {fire_type} important a {location_str}\n"
        f"🕒 {hora_str}  |  🚒 {dotacions} dotacions treballant\n"
        f"https://experience.arcgis.com/experience/f6172fd2d6974bc0a8c51e3a6bc2a735"
    )
    
    logging.info("PREVISUALIZACIÓN (no se publica):")
    logging.info(tweet)
    
    if not MODE_TEST:
        # Aquí iría el código para publicar el tweet
        pass

if __name__ == "__main__":
    main()


