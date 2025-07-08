#!/usr/bin/env python3
"""
bombers_bot.py

Publica (o simula) las intervenciones de Bombers priorizando:
1) fase “actiu” (o sin fase) 2) nº dotacions 3) tipo (forestal > agrícola > urbà).

Ahora integra IA (Gemini) para interpretar datos y buscar actualizaciones.

Requisitos:
    requests    geopy    pyproj    google-generativeai
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

# Importar la librería de Gemini
import google.generativeai as genai 

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

# --- FIN Configuración de Gemini ---

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
            "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2,MUN_NOM_MUNICIPI" 
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
            logging.warning("Error 400 al obtener MUN_NOM_MUNICIPI. Intentando sin él.")
            params["outFields"] = ("ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
                                   "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2")
            try:
                r = session.get(f"{LAYER_URL}/query", params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                for feature in data.get("features", []):
                    feature["attributes"]["_municipio_from_arcgis_success"] = False
                return data.get("features", [])
            except requests.exceptions.RequestException as e_fallback:
                logging.error(f"Error en fallback de ArcGIS: {e_fallback}")
                return []
        return []

    data = r.json()
    if "error" in data:
        logging.error("ArcGIS devolvió un error en los datos: %s", data["error"]["message"])
        if data["error"]["code"] == 400 and "Invalid query parameters" in data["error"]["message"]:
            logging.warning("Error 400 al obtener MUN_NOM_MUNICIPI. Intentando sin él. (Post-JSON parse)")
            params["outFields"] = ("ACT_NUM_VEH,COM_FASE,ESRI_OID,ACT_DAT_ACTUACIO,"
                                   "TAL_DESC_ALARMA1,TAL_DESC_ALARMA2")
            try:
                r = session.get(f"{LAYER_URL}/query", params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
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
    
    if geom and geom["x"] and geom["y"]:
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


def format_intervention_with_gemini(feature):
    """
    Formatea la intervención usando los datos de ArcGIS y Gemini para interpretación y búsqueda.
    Devuelve el mensaje de Telegram, la relevancia y las palabras clave.
    """
    a = feature["attributes"]
    geom = feature.get("geometry")

    # --- 1. Obtener Ubicación (Calle y Municipio) ---
    municipio_arcgis = a.get("MUN_NOM_MUNICIPI")
    _municipio_from_arcgis_success = a.get("_municipio_from_arcgis_success", False)

    address_components = get_address_components_from_coords(geom)
    calle_final = address_components["street"] if address_components["street"] else ""
    municipio_geocoded = address_components["municipality"] if address_components["municipality"] else ""

    municipio_final = "ubicació desconeguda"
    if _municipio_from_arcgis_success and municipio_arcgis:
        municipio_final = municipio_arcgis
    elif municipio_geocoded:
        municipio_final = municipio_geocoded
    
    location_str = ""
    if calle_final and municipio_final != "ubicació desconeguda":
        location_str = f"{calle_final}, {municipio_final}"
    elif municipio_final != "ubicació desconeguda":
        location_str = municipio_final
    elif calle_final:
        location_str = calle_final
    else:
        location_str = "ubicació desconeguda"

    hora = datetime.fromtimestamp(a["ACT_DAT_ACTUACIO"]/1000, tz=timezone.utc)\
               .astimezone(ZoneInfo("Europe/Madrid")).strftime("%H:%M")
    
    # --- 2. Preparar datos para Gemini ---
    incident_data_for_gemini = {
        "tipo_alarma1": a.get("TAL_DESC_ALARMA1"),
        "tipo_alarma2": a.get("TAL_DESC_ALARMA2"),
        "dotaciones": a.get("ACT_NUM_VEH"),
        "fase": a.get("COM_FASE"),
        "ubicacion_geo": location_str,
        "hora": hora,
        "tipo_clasificado": classify(a) 
    }

    gemini_interpretation = "No disponible (IA no configurada o error)."
    gemini_relevance = 0
    gemini_search_summary = "No se encontraron actualizaciones en Google (IA no configurada o no relevante)."
    search_keywords = [] 

    if gemini_model: 
        try:
            # --- 3. Llamada a Gemini para interpretación ---
            prompt_interpret = f"""Analiza el siguiente incidente de Bombers:
            Tipo alarma principal: {incident_data_for_gemini['tipo_alarma1']}
            Tipo alarma secundaria: {incident_data_for_gemini['tipo_alarma2']}
            Dotaciones: {incident_data_for_gemini['dotaciones']}
            Fase: {incident_data_for_gemini['fase']}
            Ubicación: {incident_data_for_gemini['ubicacion_geo']}
            Hora: {incident_data_for_gemini['hora']}
            Tipo (clasificación básica): {incident_data_for_gemini['tipo_clasificado']}

            Proporciona un resumen conciso y descriptivo del incidente.
            Estima su relevancia en una escala del 1 (muy bajo) al 10 (muy alto) para la población.
            Sugiere 2-3 palabras clave o hashtags (ej. #Incendio[Municipio]) para buscar actualizaciones en Google.
            Formato de salida (JSON, solo el objeto JSON, sin envolver en bloques de código ni texto adicional):
            {{
              "resumen": "Aquí el resumen del incidente.",
              "relevancia": "N",
              "palabras_clave_busqueda": ["palabra1", "palabra2"]
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

            # --- 4. Llamada a Gemini para búsqueda (si es relevante) ---
            if gemini_relevance >= 7 and search_keywords: 
                query = f"incendio {location_str} {' '.join(search_keywords)} últimas noticias"
                logging.info(f"Realizando búsqueda con Gemini para: {query}")
                
                try:
                    search_response = gemini_model.generate_content(
                        f"Resume muy concisamente (no más de 3 frases) noticias y actualizaciones sobre: '{query}'.", 
                        tools=[] 
                    )
                    
                    if search_response and search_response.text:
                         gemini_search_summary = search_response.text.strip()
                         if "no se encontraron resultados" in gemini_search_summary.lower() or "no puedo encontrar" in gemini_search_summary.lower():
                            gemini_search_summary = "No se encontraron actualizaciones relevantes en Google."
                         else:
                             logging.info(f"Gemini búsqueda: {gemini_search_summary}")
                    else:
                        gemini_search_summary = "Búsqueda con IA fallida o sin resultados."
                except Exception as search_e:
                    logging.error(f"Error al realizar búsqueda con Gemini: {search_e}")
                    gemini_search_summary = "Búsqueda con IA fallida."
            
        except Exception as e: 
            logging.error(f"Error general al interactuar con la API de Gemini: {e}")
            gemini_interpretation = f"Error al interpretar con IA: {e}"
            gemini_search_summary = "Búsqueda con IA fallida."
    else: # Si gemini_model es None (API_KEY no configurada o inicialización fallida)
        logging.warning("gemini_model no está disponible. Saltando interacciones con la IA.")


    # --- 5. Construir el mensaje final para Telegram (HTML) ---
    telegram_message = (
        f"🚨 <b>AVÍS BOMBERS</b> | {location_str} 🚨\n\n"
        f"<b>Tipus:</b> {classify(a).capitalize()} ({a.get('TAL_DESC_ALARMA1', '')} {a.get('TAL_DESC_ALARMA2', '')})\n"
        f"<b>Hora:</b> {hora} | <b>Dotacions:</b> {a.get('ACT_NUM_VEH')} | <b>Fase:</b> {a.get('COM_FASE', 'Desconeguda')}\n"
        f"<b>Relevancia IA:</b> {gemini_relevance}/10\n\n"
        f"<i>Resumen IA:</i> {gemini_interpretation}\n\n"
    )
    
    if gemini_relevance >= 7 and gemini_search_summary and "no se encontraron actualizaciones relevantes" not in gemini_search_summary:
         telegram_message += f"<i>Actualizaciones IA (Google):</i> {gemini_search_summary}\n\n"
    
    telegram_message += f"🌐 <a href='{MAPA_OFICIAL}'>Mapa Oficial Bombers</a>"

    return telegram_message, gemini_relevance # Devolver también la relevancia

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

    # --- INICIO BLOQUE DE DIAGNÓSTICO Y ASIGNACIÓN DE MODELO GEMINI ---
    global gemini_model 
    if GEMINI_API_KEY:
        try:
            # Si gemini_model ya se inicializó arriba sin error, no intentamos de nuevo.
            # Solo listamos modelos si gemini_model ya es un objeto válido.
            if gemini_model is None: # Si falló en la inicialización global, intentamos de nuevo aquí con más logs.
                logging.error("gemini_model no se inicializó globalmente. Intentando re-inicializar y listar modelos.")
                gemini_model = genai.GenerativeModel(
                    'models/gemini-1.5-flash',
                    generation_config={"temperature": 0.2}
                )
                logging.info("Modelo 'models/gemini-1.5-flash' re-inicializado con éxito en main().")

            # Ahora que gemini_model_ ha sido (re)inicializado, listamos modelos si es válido.
            if gemini_model:
                logging.info("Intentando listar modelos de Gemini disponibles para confirmación...")
                found_target_model = False
                available_models_for_gc = [] 
                
                for m in genai.list_models():
                    if 'generateContent' in m.supported_generation_methods:
                        available_models_for_gc.append(m.name)
                        logging.info(f"Modelo disponible para generateContent: {m.name}")
                        if m.name == 'models/gemini-1.5-flash': 
                            found_target_model = True
                    else:
                        logging.info(f"Modelo no soportado para generateContent: {m.name}")
                
                if not found_target_model:
                    logging.warning(f"El modelo objetivo 'models/gemini-1.5-flash' NO está en la lista de modelos disponibles para generateContent.")
                    logging.warning(f"Modelos compatibles: {', '.join(available_models_for_gc) if available_models_for_gc else 'Ninguno'}")
                    logging.warning("Se continuará sin este modelo si no se pudo inicializar.")
                    # Si no encontramos el modelo objetivo, nos aseguramos de que gemini_model sea None
                    # para que format_intervention_with_gemini lo sepa.
                    gemini_model = None 
                
                logging.info("Listado de modelos de Gemini completado.")
            else:
                logging.warning("gemini_model sigue siendo None después de la inicialización y/o re-inicialización. No se listarán modelos.")

        except Exception as e: # Captura errores durante el listado o re-inicialización en main()
            logging.error(f"Error crítico en el bloque de diagnóstico/inicialización de Gemini en main(): {e}.")
            gemini_model = None # Asegurarse de que el modelo es None si falla el diagnóstico
    else:
        logging.warning("GEMINI_API_KEY no configurada. Saltando verificación de modelos Gemini y operaciones de IA.")
        gemini_model = None 
    # --- FIN BLOQUE DE DIAGNÓSTICO Y ASIGNACIÓN DE MODELO GEMINI ---


    feats = fetch_features()
    if not feats:
        logging.info("ArcGIS devolvió 0 features.")
        return

    new_feats = [f for f in feats if f["attributes"].get("ESRI_OID") and f["attributes"]["ESRI_OID"] > last_id]

    if not new_feats:
        logging.info("No hay intervenciones nuevas para procesar.")
        return

    max_id_to_save = last_id 
    
    # --- INICIO: Lógica de priorización y envío ---
    processed_incidents = []

    # Procesar todas las nuevas actuaciones con Gemini para obtener su relevancia
    for feature in new_feats:
        current_object_id = feature["attributes"].get("ESRI_OID")
        
        # format_intervention_with_gemini ahora devuelve el mensaje Y la relevancia
        telegram_message, relevance = format_intervention_with_gemini(feature)
        
        processed_incidents.append({
            "feature": feature,
            "message": telegram_message,
            "relevance": relevance,
            "object_id": current_object_id,
            "timestamp": feature["attributes"].get("ACT_DAT_ACTUACIO", 0) 
        })
        
        # Actualizar el max_id_to_save independientemente de si se va a enviar o no
        if current_object_id:
             max_id_to_save = max(max_id_to_save, current_object_id)
        
        time.sleep(0.5) 

    # --- Lógica de selección de mensajes a enviar ---
    messages_to_send_final = []
    sent_object_ids = set() # Para evitar duplicados

    # 1. Identificar y añadir la actuación NUEVA más reciente
    most_recent_incident_processed = None
    if processed_incidents:
        # Asegurarse de que processed_incidents está ordenado por timestamp descendente
        processed_incidents.sort(key=lambda x: x["timestamp"], reverse=True)
        most_recent_incident_processed = processed_incidents[0]
    
    if most_recent_incident_processed:
        messages_to_send_final.append(most_recent_incident_processed["message"])
        sent_object_ids.add(most_recent_incident_processed["object_id"])
        logging.info(f"Añadida la actuación nueva más reciente (ID: {most_recent_incident_processed['object_id']}) a la cola de envío.")

    # 2. Filtrar y añadir actuaciones importantes (relevancia >= 7), excluyendo la ya enviada
    important_incidents_filtered = [
        inc for inc in processed_incidents 
        if inc["relevance"] >= 7 and inc["object_id"] not in sent_object_ids
    ]

    # Ordenar las importantes por relevancia (descendente) y luego por fecha (descendente)
    important_incidents_filtered.sort(key=lambda x: (x["relevance"], x["timestamp"]), reverse=True)

    # Limitar el número total de mensajes a enviar (ej. 3 mensajes en total: 1 reciente + 2 importantes)
    MAX_TOTAL_MESSAGES_PER_RUN = 3
    current_messages_sent_count = len(messages_to_send_final)

    for incident in important_incidents_filtered:
        if current_messages_sent_count < MAX_TOTAL_MESSAGES_PER_RUN:
            messages_to_send_final.append(incident["message"])
            sent_object_ids.add(incident["object_id"])
            logging.info(f"Añadida actuación importante (ID: {incident['object_id']}, Relevancia: {incident['relevance']}) a la cola de envío.")
            current_messages_sent_count += 1
        else:
            break # Si ya alcanzamos el límite, no añadir más

    if not messages_to_send_final:
        logging.info("No hay actuaciones nuevas que superen los criterios para ser enviadas.")
        
    # --- Envío de los mensajes finales ---
    for message_content in messages_to_send_final:
        send(message_content, None)
        time.sleep(1) # Pausa entre envíos de mensajes a Telegram

    # --- FIN: Lógica de priorización y envío ---
    
    save_state(max_id_to_save)


if __name__ == "__main__":
    main()
