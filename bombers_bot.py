#!/usr/bin/env python3
"""
bombers_bot.py

Publica (o simula) las intervenciones de Bombers priorizando:
1) fase “actiu” (o sin fase) 2) nº dotacions 3) tipo (forestal > agrícola > urbà).

Ahora integra IA (Gemini) para interpretar datos y buscar actualizaciones.

Requisitos:
    requests    geopy    pyproj    google-generativeai    google-api-python-client
"""

import os, json, logging, requests
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from geopy.geocoders import Nominatim
from pyproj import Transformer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time 

# Importar las librerías de Google
import google.generativeai as genai 
from googleapiclient.discovery import build # Para la API de Google Custom Search

# --- CONFIG ---
LAYER_URL = ("https://services7.arcgis.com/ZCqVt1fRXwwK6GF4/arcgis/rest/services/"
             "ACTUACIONS_URGENTS_online_PRO_AMB_FASE_VIEW/FeatureServer/0")
MIN_DOTACIONS = int(os.getenv("MIN_DOTACIONS", "3"))     # mínimo dotacions

IS_TEST_MODE  = os.getenv("IS_TEST_MODE", "true").lower() == "true" 

API_KEY       = os.getenv("ARCGIS_API_KEY", "") # Para ArcGIS
MAPA_OFICIAL  = "https://interior.gencat.cat/ca/arees_dactuacio/bombers/actuacions-de-bombers/"

STATE_FILE = Path("state.json")
GEOCODER   = Nominatim(user_agent="bombers_bot")
TRANSFORM  = Transformer.from_crs(25831, 4326, always_xy=True)

# Credenciales de X (Twitter) - Se mantienen, pero no se usan para publicar
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

# --- Configuración de Gemini ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
gemini_model = None 

if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel(
            'models/gemini-1.5-flash', 
            generation_config={"temperature": 0.2}
        )
        logging.info("API de Gemini configurada y modelo 'models/gemini-1.5-flash' inicializado.")
    except Exception as e:
        logging.error(f"ERROR: No se pudo inicializar el modelo 'models/gemini-1.5-flash'. Asegúrate de que la clave API es correcta y el modelo accesible para tu proyecto. Detalle: {e}")
        gemini_model = None 
else:
    logging.warning("GEMINI_API_KEY no configurada. Las funciones de IA no estarán disponibles.")
    gemini_model = None 

# --- Configuración de Google Custom Search ---
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID") # <-- NUEVA VARIABLE DE ENTORNO
Google Search_service = None
if GOOGLE_CSE_ID and GEMINI_API_KEY: # Reusa la GEMINI_API_KEY para la búsqueda si es la misma
    try:
        Google Search_service = build("customsearch", "v1", developerKey=GEMINI_API_KEY)
        logging.info("Servicio de Google Custom Search inicializado.")
    except Exception as e:
        logging.error(f"ERROR: No se pudo inicializar el servicio de Google Custom Search. Detalle: {e}")
        Google Search_service = None
else:
    logging.warning("GOOGLE_CSE_ID o GEMINI_API_KEY no configurados. La búsqueda en Google no estará disponible.")

# --- FIN Configuración de Google Custom Search ---

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

# --- CONSULTA ARCGIS ---
def fetch_features(limit=100):
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": (
            "ESRI_OID,ACT_NUM_VEH,COM_FASE,ACT_DAT_ACTUACIO,"
            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2,MUN_NOM_MUNICIPI,"
            "COM_NOM_COMARCA,PRO_NOM_PROVINCIA" 
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
        if "400" in str(e) and "Invalid query parameters" in str(e):
            logging.warning("Error 400 al obtener campos de ubicación de ArcGIS. Intentando sin ellos.")
            params["outFields"] = ("ESRI_OID,ACT_NUM_VEH,COM_FASE,ACT_DAT_ACTUACIO,"
                                   "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2")
            try:
                r = session.get(f"{LAYER_URL}/query", params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                for feature in data.get("features", []):
                    feature["attributes"]["_full_location_from_arcgis_success"] = False 
                return data.get("features", [])
            except requests.exceptions.RequestException as e_fallback:
                logging.error(f"Error en fallback de ArcGIS: {e_fallback}")
                return []
        return []

    data = r.json()
    if "error" in data:
        logging.error("ArcGIS devolvió un error en los datos: %s", data["error"]["message"])
        if data["error"]["code"] == 400 and "Invalid query parameters" in data["error"]["message"]:
            logging.warning("Error 400 al obtener campos de ubicación de ArcGIS. Intentando sin ellos. (Post-JSON parse)")
            params["outFields"] = ("ESRI_OID,ACT_NUM_VEH,COM_FASE,ACT_DAT_ACTUACIO,"
                                   "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2")
            try:
                r = session.get(f"{LAYER_URL}/query", params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                for feature in data.get("features", []):
                    feature["attributes"]["_full_location_from_arcgis_success"] = False
                return data.get("features", [])
            except requests.exceptions.RequestException as e_fallback:
                logging.error(f"Error en fallback de ArcGIS: {e_fallback}")
                return []
        return []
    
    for feature in data.get("features", []):
        feature["attributes"]["_full_location_from_arcgis_success"] = True
    return data.get("features", [])

# --- UTILIDADES ---
def tipo_val(a):
    d = (a.get("TAL_DESC_ALARMA1","")+" "+a.get("TAL_DESC_ALARMA2","")).lower()
    
    if "urbà" in d or "urbana" in d:
        return 3
    elif "agrí" in d:
        return 2
    elif "forestal" in d or "vegetació" in d:
        return 1
    else:
        return 3

def classify(a):
    return {1: "forestal", 2: "agrícola", 3: "urbà"}[tipo_val(a)]

def utm_to_latlon(x, y):
    lon, lat = TRANSFORM.transform(x, y)
    return lat, lon

def get_address_components_from_coords(geom):
    street = ""
    municipality = ""
    comarca = "" 
    provincia = ""
    
    if geom and geom["x"] and geom["y"]:
        lat, lon = utm_to_latlon(geom["x"], geom["y"])
        try:
            loc = GEOCODER.reverse((lat, lon), exactly_one=True, timeout=15, language="ca")
            if loc and loc.raw:
                address_parts = loc.raw.get('address', {})
                street = address_parts.get('road', '') or address_parts.get('building', '') or address_parts.get('amenity', '')
                municipality = address_parts.get('city', '') or \
                               address_parts.get('town', '') or \
                               address_parts.get('village', '')
                comarca = address_parts.get('county', '') 
                provincia = address_parts.get('state', '') 

                if not municipality and loc.address:
                    parts = [p.strip() for p in loc.address.split(',')]
                    for p in reversed(parts):
                        if not any(char.isdigit() for char in p) and len(p) > 2:
                            if "provincia" in p.lower() and not provincia: provincia = p 
                            elif not municipio: municipio = p 
                            
        except Exception as e:
            logging.debug(f"Error al geocodificar: {e}")
            pass
    
    return {"street": street, "municipality": municipality, "comarca": comarca, "provincia": provincia}


def perform_google_cse_search(query_string):
    """Realiza una búsqueda usando la API de Google Custom Search y devuelve los resultados."""
    if not Google Search_service:
        logging.warning("Servicio de Google Custom Search no inicializado. No se puede realizar la búsqueda.")
        return []

    try:
        # Pide los primeros 5 resultados, ajusta según necesidad
        res = Google Search_service.cse().list(q=query_string, cx=GOOGLE_CSE_ID, num=5).execute()
        
        search_results_text = []
        if 'items' in res:
            for item in res['items']:
                search_results_text.append(f"Título: {item.get('title', 'N/A')}\nURL: {item.get('link', 'N/A')}\nSnippet: {item.get('snippet', 'N/A')}\n---")
        
        logging.info(f"Búsqueda CSE para '{query_string}' obtuvo {len(res.get('items',[]))} resultados.")
        return search_results_text
    except Exception as e:
        logging.error(f"Error al llamar a Google Custom Search API para '{query_string}': {e}")
        return []


def format_intervention_with_gemini(feature):
    """
    Formatea la intervención usando los datos de ArcGIS, Gemini para interpretación,
    y Google Custom Search para la búsqueda.
    """
    a = feature["attributes"]
    geom = feature.get("geometry")

    # --- 1. Obtener Ubicación Completa (priorizando ArcGIS, luego geocodificación) ---
    municipio_arcgis = a.get("MUN_NOM_MUNICIPI")
    comarca_arcgis = a.get("COM_NOM_COMARCA") 
    provincia_arcgis = a.get("PRO_NOM_PROVINCIA") 
    _full_location_from_arcgis_success = a.get("_full_location_from_arcgis_success", False)

    calle_geocoded_data = get_address_components_from_coords(geom) 
    calle_final = calle_geocoded_data["street"] if calle_geocoded_data["street"] else ""
    municipio_geocoded = calle_geocoded_data["municipality"] if calle_geocoded_data["municipality"] else ""
    comarca_geocoded = calle_geocoded_data["comarca"] if calle_geocoded_data["comarca"] else ""
    provincia_geocoded = calle_geocoded_data["provincia"] if calle_geocoded_data["provincia"] else ""


    location_parts = []
    if calle_final: 
        location_parts.append(calle_final)
    
    # Intenta usar la ubicación completa de ArcGIS si está disponible y es fiable
    if _full_location_from_arcgis_success: 
        if municipio_arcgis and municipio_arcgis not in location_parts:
            location_parts.append(municipio_arcgis)
        if comarca_arcgis and comarca_arcgis not in location_parts:
            location_parts.append(comarca_arcgis)
        if provincia_arcgis and provincia_arcgis not in location_parts:
            location_parts.append(provincia_arcgis)
    else: # Si ArcGIS no dio la ubicación completa o falló, usar geocodificación como fallback
        if municipio_geocoded and municipio_geocoded not in location_parts:
            location_parts.append(municipio_geocoded)
        if comarca_geocoded and comarca_geocoded not in location_parts:
            location_parts.append(comarca_geocoded)
        if provincia_geocoded and provincia_geocoded not in location_parts:
            location_parts.append(provincia_geocoded)
    
    location_str = ", ".join(location_parts) if location_parts else "ubicació desconeguda"

    # Hora y Fecha del aviso
    timestamp_ms = a.get("ACT_DAT_ACTUACIO", 0)
    hora_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)\
                      .astimezone(ZoneInfo("Europe/Madrid"))
    
    hora_str = hora_dt.strftime("%H:%M")
    fecha_str = hora_dt.strftime("%d/%m/%Y") 

    # --- 2. Preparar datos para Gemini ---
    incident_data_for_gemini = {
        "tipo_alarma1": a.get("TAL_DESC_ALARMA1"),
        "tipo_alarma2": a.get("TAL_DESC_ALARMA2"),
        "dotaciones": a.get("ACT_NUM_VEH"),
        "fase": a.get("COM_FASE"),
        "ubicacion_completa": location_str, 
        "municipio": municipio_arcgis if _full_location_from_arcgis_success else municipio_geocoded,
        "comarca": comarca_arcgis if _full_location_from_arcgis_success else comarca_geocoded,
        "provincia": provincia_arcgis if _full_location_from_arcgis_success else provincia_geocoded,
        "hora": hora_str,
        "fecha": fecha_str,
        "tipo_clasificado": classify(a) 
    }

    gemini_interpretation = "No disponible (IA no configurada o error)."
    gemini_relevance = 0
    gemini_search_summary = "No se encontraron actualizaciones relevantes en Google." # Texto por defecto
    search_keywords = [] 

    if gemini_model: 
        try:
            # --- 3. Llamada a Gemini para interpretación ---
            # Prompt para interpretación: Genera resumen, relevancia y palabras clave
            prompt_interpret = f"""Analiza el siguiente incidente de Bombers y proporciona un resumen descriptivo e informativo.
            Datos del incidente:
            - Tipo alarma principal de Bombers: {incident_data_for_gemini['tipo_alarma1']}
            - Tipo alarma secundaria de Bombers: {incident_data_for_gemini['tipo_alarma2']}
            - Clasificación general del bot: {incident_data_for_gemini['tipo_clasificado']}
            - Dotaciones movilizadas: {incident_data_for_gemini['dotaciones']}
            - Fase actual del incidente: {incident_data_for_gemini['fase']}
            - Ubicación exacta: {incident_data_for_gemini['ubicacion_completa']}
            - Municipio: {incident_data_for_gemini['municipio']}
            - Comarca: {incident_data_for_gemini['comarca']}
            - Provincia: {incident_data_for_gemini['provincia']}
            - Fecha y Hora del aviso: {incident_data_for_gemini['fecha']} a las {incident_data_for_gemini['hora']}

            Proporciona un resumen conciso y muy descriptivo del incidente (entre 3 y 5 frases).
            Asegúrate de fusionar el tipo de alarma principal, secundaria y la clasificación general de forma natural y sin redundancias.
            Confirma la ubicación geográfica exacta (municipio, comarca, provincia) si se ha proporcionado, evitando errores.
            Estima su relevancia en una escala del 1 (muy bajo) al 10 (muy alto) para la población.
            Sugiere 2-3 palabras clave o hashtags (ej. #Incendio[Municipio], #Incendio[Tipo]) para buscar actualizaciones en Google. Asegúrate de que las palabras clave geográficas sean exactas y no infieras provincias si no se han dado.
            Formato de salida (JSON, solo el objeto JSON, sin envolver en bloques de código ni texto adicional):
            {{
              "resumen": "Aquí el resumen detallado del incidente, integrando los tipos de alarma y confirmando la ubicación geográfica de forma precisa.",
              "relevancia": "N",
              "palabras_clave_busqueda": ["palabra1", "palabra2", "palabra3"]
            }}
            """
            
            response_interpret = gemini_model.generate_content(prompt_interpret)
            try:
                response_text_cleaned = response_interpret.text.strip()
                if response_text_cleaned.startswith('```json') and response_text_cleaned.endswith('```'):
                    response_text_cleaned = response_text_cleaned[len('```json'):-len('```')].strip()
                
                parsed_interpret = json.loads(response_text_cleaned) 
                gemini_interpretation = parsed_interpret.get("resumen", "Error al interpretar.")
                gemini_relevance = int(parsed_interpret.get("relevancia", 0))
                search_keywords = parsed_interpret.get("palabras_clave_busqueda", [])
            except json.JSONDecodeError:
                logging.warning(f"Respuesta de Gemini no es JSON válido (después de limpieza) para interpretación: '{response_interpret.text}'. Puede que Gemini no haya seguido el formato o el JSON sea inválido.")
                gemini_interpretation = "Resumen no disponible (Gemini no devolvió JSON válido)."
                gemini_relevance = 0
                search_keywords = []

            logging.info(f"Gemini interpretación: Relevancia={gemini_relevance}, Resumen={gemini_interpretation}")

            # --- 4. Realizar búsqueda en Google Custom Search API y luego resumir con Gemini ---
            if gemini_relevance >= 7 and Google Search_service: # Solo buscar si es relevante y el servicio CSE está activo
                
                # Construir la query para Google Custom Search - Más amplia y flexible
                cse_query_parts = []
                if incident_data_for_gemini['municipio']: cse_query_parts.append(incident_data_for_gemini['municipio'])
                if incident_data_for_gemini['comarca']: cse_query_parts.append(incident_data_for_gemini['comarca'])
                if incident_data_for_gemini['provincia']: cse_query_parts.append(incident_data_for_gemini['provincia'])
                
                base_cse_query = "incendio " + " ".join(cse_query_parts) if cse_query_parts else "incendio"
                # Añadimos palabras clave generales y específicas
                cse_query = f"{base_cse_query} {' '.join(search_keywords)} noticia actual" # Añadir "noticia actual" para balancear

                logging.info(f"Realizando búsqueda externa en Google CSE para: '{cse_query}'")
                search_results = perform_google_cse_search(cse_query) # Llama a la nueva función de búsqueda CSE

                if search_results:
                    # Si hay resultados, pedir a Gemini que los resuma
                    results_text = "\n".join(search_results)
                    prompt_search_summary = f"""Has realizado la siguiente búsqueda en Google: '{cse_query}'.
                    Aquí están los resultados crudos de la búsqueda:
                    ---
                    {results_text}
                    ---
                    Basándote SÓLO en estos resultados proporcionados, resume las noticias más relevantes (aproximadamente 50-100 palabras) sobre el incidente de Bombers. Enfócate en el estado actual, el impacto y si hay personas afectadas. Si los resultados no contienen información relevante o específica sobre el incidente, indica 'No se encontraron actualizaciones relevantes en Google'. No generes información que no esté en los resultados.
                    """
                    try:
                        summary_response = gemini_model.generate_content(prompt_search_summary)
                        gemini_search_summary = summary_response.text.strip()
                        # Si Gemini sigue dando "no encontrados" basados en los resultados,
                        # mantenemos nuestro mensaje por defecto si es muy genérico.
                        if "no se encontraron actualizaciones relevantes" in gemini_search_summary.lower() or \
                           "no se encontró información" in gemini_search_summary.lower() or \
                           "no puedo encontrar" in gemini_search_summary.lower() or \
                           "no se encontraron resultados" in gemini_search_summary.lower():
                            gemini_search_summary = "No se encontraron actualizaciones relevantes en Google."
                        else:
                            logging.info(f"Gemini (resumen CSE): {gemini_search_summary}")
                    except Exception as summary_e:
                        logging.error(f"Error al pedir resumen de búsqueda a Gemini: {summary_e}")
                        gemini_search_summary = "Error al resumir búsqueda con IA."
                else:
                    logging.info(f"Google CSE no devolvió resultados para: '{cse_query}'")
                    gemini_search_summary = "No se encontraron actualizaciones relevantes en Google."
            else:
                logging.info("Búsqueda no realizada: Relevancia IA < 7 o servicio CSE no activo.")
                gemini_search_summary = "No se encontraron actualizaciones relevantes en Google." # Mantiene default

        except Exception as e: 
            logging.error(f"Error general al interactuar con la API de Gemini: {e}")
            gemini_interpretation = f"Error al interpretar con IA: {e}"
            gemini_search_summary = "Búsqueda con IA fallida."
    else: 
        logging.warning("gemini_model no está disponible. Saltando interacciones con la IA.")


    # --- 5. Construir el mensaje final para Telegram (HTML) ---
    telegram_message = (
        f"🚨 <b>AVÍS BOMBERS</b> | {location_str} 🚨\n\n"
        f"<b>Fecha:</b> {fecha_str} | <b>Hora:</b> {hora_str} | <b>Dotaciones:</b> {a.get('ACT_NUM_VEH')} | <b>Fase:</b> {a.get('COM_FASE', 'Desconeguda')}\n"
        f"<b>Relevancia IA:</b> {gemini_relevance}/10\n\n"
        f"<i>Resumen IA:</i> {gemini_interpretation}\n\n" 
    )
    
    if gemini_relevance >= 7 and gemini_search_summary and "no se encontraron actualizaciones relevantes" not in gemini_search_summary:
         telegram_message += f"<i>Actualizaciones IA (Google):</i> {gemini_search_summary}\n\n"
    
    telegram_message += f"🌐 <a href='{MAPA_OFICIAL}'>Mapa Oficial Bombers</a>"

    return telegram_message, gemini_relevance, timestamp_ms 

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
        "parse_mode": "HTML", 
        "disable_web_page_preview": True 
    }
    try:
        response = requests.post(telegram_url, json=payload, timeout=10)
        response.raise_for_status() 
        logging.info("Notificación enviada a Telegram exitosamente.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al enviar notificación a Telegram: {e}")

def send(text, api=None): 
    """
    Gestiona el envío del mensaje. Solo envía a Telegram.
    La lógica de X (Twitter) se mantiene en modo de simulación o inactiva debido a las restricciones.
    """
    if IS_TEST_MODE:
        logging.info("MODO DE PRUEBA (X): No se publicará en Twitter. Simulando en consola.")
        logging.info("TUIT SIMULADO:\n" + text + "\n")
    else: 
         logging.info("La publicación en X (Twitter) está deshabilitada/restringida para este bot.")
         logging.info("Texto que se intentaría publicar en X:\n" + text + "\n")


    # --- Envío a Telegram (SIEMPRE se intenta si está configurado) ---
    send_telegram_message(text)
    # --- Fin Envío a Telegram ---

# --- MAIN ---
def main():
    # Cargar el estado de los incidentes procesados
    last_id = load_state()

    global gemini_model 
    if GEMINI_API_KEY:
        try:
            if gemini_model is None: 
                logging.error("gemini_model no se inicializó globalmente. Intentando re-inicializar y listar modelos.")
                gemini_model = genai.GenerativeModel(
                    'models/gemini-1.5-flash',
                    generation_config={"temperature": 0.2}
                )
                logging.info("Modelo 'models/gemini-1.5-flash' re-inicializado con éxito en main().")

            if gemini_model:
                found_target_model = False
                available_models_for_gc = [] 
                
                for m in genai.list_models():
                    if 'generateContent' in m.supported_generation_methods:
                        available_models_for_gc.append(m.name)
                        if m.name == 'models/gemini-1.5-flash': 
                            found_target_model = True
                
                if not found_target_model:
                    logging.warning(f"El modelo objetivo 'models/gemini-1.5-flash' NO está en la lista de modelos disponibles para generateContent.")
                    logging.warning(f"Modelos compatibles: {', '.join(available_models_for_gc) if available_models_for_gc else 'Ninguno'}")
                    logging.warning("Se continuará sin este modelo si no se pudo inicializar.")
                    gemini_model = None 
                
                logging.info("Listado de modelos de Gemini completado.")
            else:
                logging.warning("gemini_model sigue siendo None después de la inicialización y/o re-inicialización. No se listarán modelos.")

        except Exception as e:
            logging.error(f"Error crítico en el bloque de diagnóstico/inicialización de Gemini en main(): {e}.")
            gemini_model = None 
    else:
        logging.warning("GEMINI_API_KEY no configurada. Saltando verificación de modelos Gemini y operaciones de IA.")
        gemini_model = None 


    feats = fetch_features()
    if not feats:
        logging.info("ArcGIS devolvió 0 features.")
        return

    new_feats = [f for f in feats if f["attributes"].get("ESRI_OID") and f["attributes"]["ESRI_OID"] > last_id]

    if not new_feats:
        logging.info("No hay intervenciones nuevas para procesar.")
        return

    # --- INICIO: Lógica de priorización y envío ---
    all_processed_new_feats = []

    # 1. Procesar todas las nuevas actuaciones con Gemini para obtener su relevancia y mensaje
    for feature in new_feats:
        current_object_id = feature["attributes"].get("ESRI_OID")
        
        telegram_message, relevance, timestamp = format_intervention_with_gemini(feature)
        
        all_processed_new_feats.append({
            "object_id": current_object_id,
            "message": telegram_message,
            "relevance": relevance,
            "timestamp": timestamp
        })
        
        time.sleep(0.5) 

    # 2. Ordenar las actuaciones procesadas para seleccionar
    all_processed_new_feats.sort(key=lambda x: x["timestamp"], reverse=True)

    messages_to_send_final = []
    sent_object_ids = set() 

    MAX_TOTAL_MESSAGES_PER_RUN = 3 
    MIN_RELEVANCE_FOR_IMPORTANT = 7 

    # --- Añadir la actuación NUEVA más reciente (si existe) ---
    if all_processed_new_feats:
        most_recent_incident = all_processed_new_feats[0]
        messages_to_send_final.append(most_recent_incident["message"])
        sent_object_ids.add(most_recent_incident["object_id"])
        logging.info(f"Añadida la actuación nueva más reciente (ID: {most_recent_incident['object_id']}) a la cola de envío.")

    # --- Añadir actuaciones importantes (relevancia >= MIN_RELEVANCE_FOR_IMPORTANT) ---
    important_incidents_filtered = [
        inc for inc in all_processed_new_feats 
        if inc["relevance"] >= MIN_RELEVANCE_FOR_IMPORTANT and inc["object_id"] not in sent_object_ids
    ]

    important_incidents_filtered.sort(key=lambda x: (x["relevance"], x["timestamp"]), reverse=True) 

    current_messages_count = len(messages_to_send_final)

    for incident in important_incidents_filtered:
        if current_messages_count < MAX_TOTAL_MESSAGES_PER_RUN:
            messages_to_send_final.append(incident["message"])
            sent_object_ids.add(incident["object_id"])
            logging.info(f"Añadida actuación importante (ID: {incident['object_id']}, Relevancia: {incident['relevance']}) a la cola de envío.")
            current_messages_count += 1
        else:
            break 

    if not messages_to_send_final:
        logging.info("No hay actuaciones nuevas que cumplan los criterios para ser enviadas.")
        
    # --- Envío de los mensajes finales ---
    for message_content in messages_to_send_final:
        send(message_content, None)
        time.sleep(1) 

    # --- FIN: Lógica de priorización y envío ---

    # Actualizar last_id con el ID más alto de TODAS las nuevas actuaciones procesadas
    max_id_to_save = last_id
    if new_feats: 
        max_id_to_save = max(max_id_to_save, max(f["attributes"].get("ESRI_OID", 0) for f in new_feats if f["attributes"].get("ESRI_OID")))
    
    save_state(max_id_to_save)


if __name__ == "__main__":
    main()
