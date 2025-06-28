import requests
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Usa tu user agent (puedes usar la variable de entorno o hardcodear)
USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "bombers_bot_test/1.0")
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", 5))

def reverse_geocode(lat, lon):
    geolocator = Nominatim(user_agent=USER_AGENT)
    try:
        location = geolocator.reverse((lat, lon), language="ca")
        if location:
            # Retorna la dirección completa o la más cercana posible
            return location.address
        return None
    except GeocoderTimedOut:
        return None

def main():
    URL = "https://servei-incendis.maps.arcgis.com/sharing/rest/content/items/4c7a3ebfa72f43298644ec9a0e9d3ca2/data"
    try:
        r = requests.get(URL)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print("Error al descargar datos:", e)
        return

    intervenciones = data.get("intervencions", [])
    if not intervenciones:
        print("No hay intervenciones.")
        return

    ultima = intervenciones[-1]  # La última intervención

    lat = ultima.get("latitud")
    lon = ultima.get("longitud")
    tipus = ultima.get("tipus", "").lower()
    dotacions = int(ultima.get("dotacions", 0))
    hora = ultima.get("hora", "")

    # Obtener dirección inversa
    direccion = reverse_geocode(lat, lon) or "coordenadas desconocidas"

    # Determinar tipo incendio más exacto
    if "vegetació urbana" in tipus or "vegetacio urbana" in tipus:
        tipo_texto = "incendi urbà"
    elif "forestal" in tipus:
        tipo_texto = "incendi forestal"
    elif "agrícola" in tipus or "agricola" in tipus:
        tipo_texto = "incendi agrícola"
    else:
        tipo_texto = "incendi"

    print(f"Última intervención:")
    print(f"ID: {ultima.get('id')}")
    print(f"Tipo: {tipo_texto}")
    print(f"Dotacions: {dotacions}")
    print(f"Hora: {hora}")
    print(f"Ubicación: {direccion}")

if __name__ == "__main__":
    main()
