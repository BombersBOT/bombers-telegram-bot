#!/usr/bin/env python3

"""

bombers_bot.py



Publica (o simula) las intervenciones de Bombers priorizando:

1) fase “actiu” (o sin fase) 2) nº dotacions 3) tipo (forestal > agrícola > urbà).



Requisitos:

    requests    geopy    tweepy>=4.0.0    pyproj

"""



import os, json, logging, requests

from datetime import datetime, timezone

from pathlib import Path

from zoneinfo import ZoneInfo

from geopy.geocoders import Nominatim

from pyproj import Transformer

from requests.adapters import HTTPAdapter

from urllib3.util.retry import Retry

# No es necesario importar tweepy si solo usaremos Telegram o la simulación

# import tweepy 





# --- CONFIG ---

LAYER_URL = ("https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/"

             "ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0")

MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "3"))     # mínimo dotacions



IS_TEST_MODE  = os.getenv("IS_TEST_MODE", "true").lower() == "true" 



API_KEY       = os.getenv("ARCGIS_API_KEY", "") # Para ArcGIS

MAPA_OFICIAL  = "https://interior.gencat.cat/ca/arees_dactuacio/bombers/actuacions-de-bombers/"



STATE_FILE = Path("state.json")

GEOCODER   = Nominatim(user_agent="bombers_bot")

TRANSFORM  = Transformer.from_crs(25831, 4326, always_xy=True)



# Credenciales de X (Twitter) - Se mantienen para compatibilidad, pero no se usan para publicar

TW_KEYS = {

    "ck": os.getenv("TW_CONSUMER_KEY"),

    "cs": os.getenv("TW_CONSUMER_SECRET"),

    "at": os.getenv("TW_ACCESS_TOKEN"),

    "as": os.getenv("TW_ACCESS_SECRET"),

}



# --- Credenciales de Telegram ---

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- FIN Credenciales Telegram ---



logging.basicConfig(level=logging.INFO,

                    format="%(asctime)s %(levelname)s %(message)s")



# Configuración de reintentos para requests (para ArcGIS y Nominatim)

retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])

session = requests.Session()

session.mount('https://', HTTPAdapter(max_retries=retries))





# --- ESTADO ---

def load_state() -> int:

    return json.loads(STATE_FILE.read_text()).get("last_id", -1) if STATE_FILE.exists() else -1



def save_state(last_id: int):

    STATE_FILE.write_text(json.dumps({"last_id": last_id}))



# --- CONSULTA ARCGIS (SIMPLIFICADA) ---

def fetch_features(limit=100):

    params = {

        "f": "json",

        "where": "1=1",

        # Volvemos a los outFields que sabemos que funcionan bien, sin MUN_NOM_MUNICIPI

        "outFields": (

            "ESRI_OID,ACT_NUM_VEH,COM_FASE,ACT_DAT_ACTUACIO,"

            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2" 

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

        r.raise_for_status() # Lanza un error si la respuesta HTTP no es 2xx

    except requests.exceptions.Timeout:

        logging.error("Timeout al consultar ArcGIS. Servidor no respondió a tiempo.")

        return []

    except requests.exceptions.RequestException as e:

        # Capturamos cualquier error de la petición a ArcGIS aquí y logueamos.

        # Ya no hay lógica de fallback doble dentro de esta función, simplificando.

        logging.error(f"Error al consultar ArcGIS: {e}")

        return []



    data = r.json()

    if "error" in data:

        logging.error("ArcGIS devolvió un error en los datos: %s", data["error"]["message"])

        return []

    

    # Ya no añadimos "_municipio_from_arcgis_success" porque el municipio siempre vendrá de Nominatim

    return data.get("features", [])





# --- UTILIDADES ---

def tipo_val(a):

    d = (a.get("TAL_DESC_ALARMA1","")+" "+a.get("TAL_DESC_ALARMA2","")).lower()

    

    # Prioridad: Urbà/Urbana > Agrícola > Forestal/Vegetació > Urbà (por defecto)

    if "urbà" in d or "urbana" in d:

        return 3 # Esto es "urbà"

    elif "agrí" in d:

        return 2 # Esto es "agrícola"

    elif "forestal" in d or "vegetació" in d:

        return 1 # Esto es "forestal"

    else:

        return 3 # Asumir urbano por defecto si no hay mejor clasificación



def classify(a):

    return {1: "forestal", 2: "agrícola", 3: "urbà"}[tipo_val(a)]



def utm_to_latlon(x, y):

    lon, lat = TRANSFORM.transform(x, y)

    return lat, lon



def get_address_components_from_coords(geom):

    """

    Obtiene la dirección de las coordenadas y la parsea en componentes.

    Devuelve un diccionario con 'street', 'municipality'.

    """

    street = ""

    municipality = ""

    

    if geom:

        lat, lon = utm_to_latlon(geom["x"], geom["y"])

        try:

            loc = GEOCODER.reverse((lat, lon), exactly_one=True, timeout=15, language="ca")

            if loc and loc.raw:

                address_parts = loc.raw.get('address', {})

                street = address_parts.get('road', '') or address_parts.get('building', '') or address_parts.get('amenity', '')

                municipality = address_parts.get('city', '') or \

                               address_parts.get('town', '') or \

                               address_parts.get('village', '') or \

                               address_parts.get('county', '')



                if not municipality and loc.address:

                    parts = [p.strip() for p in loc.address.split(',')]

                    for p in reversed(parts):

                        if not any(char.isdigit() for char in p) and len(p) > 2 and p.lower() not in ["catalunya", "españa"]:

                            municipality = p

                            break



        except Exception as e:

            logging.debug(f"Error al geocodificar: {e}")

            pass

    

    return {"street": street, "municipality": municipality}





def format_intervention(a, geom):

    # La ubicación (calle y municipio) siempre vendrá de la geocodificación

    address_components = get_address_components_from_coords(geom)

    calle_final = address_components["street"] if address_components["street"] else ""

    municipio_final = address_components["municipality"] if address_components["municipality"] else "ubicació desconeguda"

    

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



    # Formato para el texto de la intervención (usando HTML para Telegram)

    intervention_text = (f"🔥 <b>{classify(a).capitalize()}</b> a {location_str}\n"

                         f"🕒 {hora} | 🚒 {a['ACT_NUM_VEH']} dot.")

    

    return intervention_text



# --- Funciones de envío ---

def send_telegram_message(text):

    """Envía un mensaje al canal/grupo de Telegram."""

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:

        logging.warning("Variables de entorno de Telegram (TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID) no configuradas. No se enviará mensaje a Telegram.")

        return



    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {

        "chat_id": TELEGRAM_CHAT_ID,

        "text": text,

        "parse_mode": "HTML", # Crucial para que las negritas y enlaces funcionen

        "disable_web_page_preview": True # Evita previsualizar el enlace al mapa

    }

    try:

        response = requests.post(telegram_url, json=payload, timeout=10)

        response.raise_for_status() # Lanza un error si la respuesta HTTP no es 2xx

        logging.info("Notificación enviada a Telegram exitosamente.")

    except requests.exceptions.RequestException as e:

        logging.error(f"Error al enviar notificación a Telegram: {e}")



def send(text, api=None): # api es un argumento heredado, pero ya no se usa para publicar en X

    """

    Gestiona el envío del mensaje. Solo envía a Telegram.

    La lógica de X (Twitter) se mantiene en modo de simulación o inactiva debido a las restricciones.

    """

    if IS_TEST_MODE:

        logging.info("MODO DE PRUEBA (X): No se publicará en Twitter. Simulando en consola.")

        logging.info("TUIT SIMULADO:\n" + text + "\n")

    # Lógica para X (Twitter) - Solo para loguear si se intentara publicar

    # Aunque IS_TEST_MODE esté en "false" y api exista, ya no llamamos api.update_status

    # para evitar el error 403 y la dependencia de tweepy.

    else:

         logging.info("El bot está configurado para modo real, pero la publicación en X (Twitter) está deshabilitada/restringida.")

         logging.info("Texto que se intentaría publicar en X:\n" + text + "\n")





    # --- Envío a Telegram (SIEMPRE se intenta si está configurado) ---

    send_telegram_message(text)

    # --- Fin Envío a Telegram ---



# --- MAIN ---

def main():

    # Cargar el estado de los incidentes procesados

    last_id = load_state()



    # No es necesario autenticarse con tweepy si solo se publica en Telegram.

    # El objeto 'api' para tweepy ya no se creará aquí, simplificando el main.



    feats = fetch_features()

    if not feats:

        logging.info("ArcGIS devolvió 0 features.")

        return



    # Filtra solo las nuevas intervenciones (por ESRI_OID)

    # y también las que tienen DATA_AVIS para poder ordenar

    new_feats = [f for f in feats if f["attributes"].get("ESRI_OID") and f["attributes"]["ESRI_OID"] > last_id]



    most_recent_feature = None

    if new_feats:

        new_feats.sort(key=lambda f: f["attributes"].get("ACT_DAT_ACTUACIO", 0), reverse=True)

        most_recent_feature = new_feats[0]



    candidatos_activos = [

        f for f in new_feats

        if f["attributes"].get("ACT_NUM_VEH", 0) >= MIN_DOTACIONS

           and (str(f["attributes"].get("COM_FASE") or "")).lower() in ("", "actiu")

    ]

    

    intervenciones_para_notificar = [] # Se usará para construir el mensaje de Telegram



    if most_recent_feature:

        intervenciones_para_notificar.append({"title": "Act. més recent", "feature": most_recent_feature})



    if candidatos_activos:

        candidatos_activos.sort(

            key=lambda f: (

                -f["attributes"].get("ACT_NUM_VEH", 0),

                tipo_val(f["attributes"]),

                -f["attributes"].get("ACT_DAT_ACTUACIO", 0)

            )

        )

        potential_relevant = candidatos_activos[0]



        if most_recent_feature is None or potential_relevant["attributes"]["ESRI_OID"] != most_recent_feature["attributes"]["ESRI_OID"]:

            intervenciones_para_notificar.append({"title": "Inc. més rellevant", "feature": potential_relevant})

    

    if len(intervenciones_para_notificar) == 2:

        if intervenciones_para_notificar[0]["title"] == "Inc. més rellevant":

            intervenciones_para_notificar.reverse() 



    if not intervenciones_para_notificar:

        logging.info("No hay intervenciones nuevas para notificar.")

        return



    telegram_message_parts = []

    max_id_to_save = last_id # Variable para el ID máximo que se guardará



    for item in intervenciones_para_notificar:

        title_text = item["title"]

        feature = item["feature"]

        a = feature["attributes"]

        geom = feature.get("geometry")

        

        formatted_interv = format_intervention(a, geom)

        telegram_message_parts.append(f"• <b>{title_text}</b>:\n{formatted_interv}")

        

        current_object_id = a.get("ESRI_OID")

        if current_object_id:

             max_id_to_save = max(max_id_to_save, current_object_id) # Actualizar el ID máximo

             # Nota: PROCESSED_INCIDENTS se usaría si queremos historial, pero para el state.json

             # solo necesitamos el último ID más alto.





    final_telegram_text = "\n\n".join(telegram_message_parts) + f"\n\nFuente: <a href='{MAPA_OFICIAL}'>Mapa Oficial Bombers</a>"

    

    # Enviar el mensaje a Telegram. El segundo argumento es api, que será None.

    send(final_telegram_text, None) 

    

    # Guardar el ID de la última actuación procesada para no repetirla

    save_state(max_id_to_save)





if __name__ == "__main__":

    main()

    
