"""
bombers_bot.py

Bot que consulta la capa de ArcGIS de los Bombers de la Generalitat y publica
en Twitter (X) nuevas actuaciones relevantes (incendios con muchas dotaciones).

Dependencias: tweepy, requests, geopy
"""

import os
import logging
from datetime import datetime
import pytz
import requests
from geopy.geocoders import Nominatim

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "5"))
MODE_TEST = os.getenv("MODE_TEST", "True").lower() == "true"  # True o False como string

logging.info(f"Modo test: {MODE_TEST}")
logging.info(f"Mínimo dotaciones para tuitear: {MIN_DOTACIONS}")

def consultar_intervenciones():
    # Aquí tu código para consultar intervenciones a ArcGIS
    # Devuelve lista de intervenciones de ejemplo para demo
    return [{
        "ACT_NUM_ACTUACIO": "225724",
        "ACT_NUM_VEH": 1,
        "COM_FASE": "Incendi vegetació urbana",
        "DATA_ACT": 1698497220000,  # timestamp ms UTC
        "LONG": 1.2345,
        "LAT": 41.5678
    }]

def obtener_direccion(lat, lon):
    geolocator = Nominatim(user_agent="bombers_bot_1.0")
    try:
        location = geolocator.reverse((lat, lon), exactly_one=True, language="ca")
        if location and location.address:
            return location.address
        else:
            return None
    except Exception as e:
        logging.warning(f"Reverse geocode error: {e}")
        return None

def formatear_hora_esp(timestamp_ms):
    utc_dt = datetime.utcfromtimestamp(timestamp_ms / 1000.0)
    madrid_tz = pytz.timezone("Europe/Madrid")
    madrid_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(madrid_tz)
    return madrid_dt.strftime("%H:%M")

def clasificar_incendi(com_fase):
    com_fase = com_fase.lower()
    if "urbana" in com_fase:
        return "urbà"
    elif "agrícola" in com_fase or "agricola" in com_fase:
        return "agrícola"
    elif "forestal" in com_fase or "vegetació" in com_fase or "vegetació forestal" in com_fase:
        return "forestal"
    else:
        return "desconegut"

def main():
    intervenciones = consultar_intervenciones()
    logging.info(f"Número de intervenciones consultadas: {len(intervenciones)}")

    for intervencion in intervenciones:
        act_id = intervencion["ACT_NUM_ACTUACIO"]
        dotacions = intervencion.get("ACT_NUM_VEH", 0)
        com_fase = intervencion.get("COM_FASE", "")
        timestamp_ms = intervencion.get("DATA_ACT")
        lat = intervencion.get("LAT")
        lon = intervencion.get("LONG")

        logging.info(f"Intervención {act_id} con {dotacions} dotacions. Tipo: {com_fase}")

        if not MODE_TEST and dotacions < MIN_DOTACIONS:
            logging.info(f"Intervención {act_id} con {dotacions} dotacions (<{MIN_DOTACIONS}). No se tuitea.")
            continue  # Salta esta intervención

        hora_local = formatear_hora_esp(timestamp_ms) if timestamp_ms else "hora desconeguda"

        direccion = obtener_direccion(lat, lon)
        if direccion:
            lugar = direccion
        else:
            lugar = f"{lat:.5f}, {lon:.5f}"

        tipo_incendi = clasificar_incendi(com_fase)

        tweet = (
            f"🔥 Incendi {tipo_incendi} important a {lugar}\n"
            f"🕒 {hora_local}  |  🚒 {dotacions} dotacions treballant\n"
            "https://experience.arcgis.com/experience/f6172fd2d6974bc0a8c51e3a6bc2a735"
        )

        logging.info("PREVISUALIZACIÓN (no se publica):")
        logging.info(tweet)

if __name__ == "__main__":
    main()

