import json
import threading
import socket
import time

# ==========================================
# CONFIGURACIÓN DE RED Y PERSISTENCIA
# ==========================================
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

MI_NOMBRE = config["mi_nombre"]
NODOS = config["nodos"]
PORT = 5000
ID_NUMERICO = int(MI_NOMBRE.replace("vm", ""))

DB_LOCK = threading.Lock()
DB_FILE = "db.json"

# Variables de estado globales y dinámicas
MAESTRO_ACTUAL = "vm1" 
NODOS_ACTIVOS = ["vm1", "vm2", "vm3", "vm4"]
EN_ELECCION = False

# Variables específicas para Ricart-Agrawala (Exclusión Mutua)
lamport_clock = 0
reclamando_seccion_critica = False
respuestas_ok_recibidas = 0
peticiones_diferidas = []  # Almacena conexiones de nodos bloqueados temporalmente
mutex_lock = threading.Lock()

# Variables específicas para 2PC (Consenso)
db_temporal_preparada = None

def cargar_db():
    with DB_LOCK:
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {"ingenieros": [], "usuarios": [], "dispositivos": [], "tickets": []}

def guardar_db(data):
    with DB_LOCK:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

# ==========================================
# CLIENTE: EMISIÓN DE MENSAJES
# ==========================================

def enviar_mensaje(host, port, mensaje_dict, timeout=1.0):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        s.send(json.dumps(mensaje_dict).encode("utf-8"))
        respuesta = s.recv(4096).decode("utf-8")
        s.close()
        return json.loads(respuesta)
    except:
        return {"status": "ERROR", "error": "Inalcanzable"}

# ==========================================
# IMPLEMENTACIÓN DE CONSENSO REAL: 2PC
# ==========================================

def ejecutar_consenso_2pc(nueva_db):
    """Protocolo de Compromiso en Dos Fases (Two-Phase Commit)"""
    global NODOS_ACTIVOS
    nodos_a_votar = [n for n in NODOS if n["host"] in NODOS_ACTIVOS and n["host"] != MI_NOMBRE]
    
    # Fase 1: Preparación (Votación)
    votos_favorables = True
    conexiones_vivas = []
    
    for nodo in nodos_a_votar:
        res = enviar_mensaje(nodo["host"], nodo["port"], {
            "tipo": "PREPARE_2PC", 
            "payload": {"db": nueva_db}
        }, timeout=1.5)
        
        if res.get("status") == "VOTE_COMMIT":
            conexiones_vivas.append(nodo)
        else:
            votos_favorables = False
            break

    # Fase 2: Decisión Global
    if votos_favorables:
        # Mi propio commit local
        guardar_db(nueva_db)
        # Commit distribuido
        for nodo in conexiones_vivas:
            enviar_mensaje(nodo["host"], nodo["port"], {"tipo": "GLOBAL_COMMIT_2PC"})
        return True
    else:
        # Abortar transacciones distribuidas
        for nodo in nodos_a_votar:
            enviar_mensaje(nodo["host"], nodo["port"], {"tipo": "GLOBAL_ABORT_2PC"})
        return False

# ==========================================
# SERVIDOR: RESPUESTAS DINÁMICAS
# ==========================================

def despachar_peticion(conn, addr):
    global MAESTRO_ACTUAL, EN_ELECCION, NODOS_ACTIVOS
    global lamport_clock, reclamando_seccion_critica, respuestas_ok_recibidas, peticiones_diferidas
    global db_temporal_preparada
    
    try:
        data = conn.recv(4096).decode("utf-8")
        if not data: return
        
        peticion = json.loads(data)
        tipo = peticion.get("tipo")
        payload = peticion.get("payload")
        
        respuesta = {"status": "ACK"}

        if tipo == "PING":
            respuesta = {"status": "ALIVE", "maestro": MAESTRO_ACTUAL}

        # --- MANEJADORES DE CONSENSO 2PC ---
        elif tipo == "PREPARE_2PC":
            db_temporal_preparada = payload["db"]
            respuesta = {"status": "VOTE_COMMIT"}

        elif tipo == "GLOBAL_COMMIT_2PC":
            if db_temporal_preparada:
                guardar_db(db_temporal_preparada)
                db_temporal_preparada = None
            respuesta = {"status": "ACK"}

        elif tipo == "GLOBAL_ABORT_2PC":
            db_temporal_preparada = None
            respuesta = {"status": "ACK"}

        # --- MANEJADORES DE EXCLUSIÓN MUTUA RICART-AGRAWALA ---
        elif tipo == "MUTEX_REQUEST":
            req_clock = payload["clock"]
            req_nodo = payload["nodo"]
            
            with mutex_lock:
                lamport_clock = max(lamport_clock, req_clock) + 1
                
                # Regla de decisión de Ricart-Agrawala
                id_req = int(req_nodo.replace("vm", ""))
                yo_compitiendo = reclamando_seccion_critica
                
                if yo_compitiendo and (req_clock > lamport_clock or (req_clock == lamport_clock and id_req > ID_NUMERICO)):
                    # Mi petición tiene prioridad, difiero la respuesta guardando la conexión
                    peticiones_diferidas.append(conn)
                    return # No cerramos la conexión ni mandamos mensaje aún
                else:
                    # No estoy compitiendo o su prioridad es mayor, respondo OK de inmediato
                    conn.send(json.dumps({"status": "MUTEX_OK"}).encode("utf-8"))
                    return

        # --- PROCESAMIENTO CENTRALIZADO EN MAESTRO ---
        elif tipo == "SOLICITAR_TICKET_NUEVO":
            respuesta = procesar_ticket_en_maestro(payload) if MI_NOMBRE == MAESTRO_ACTUAL else {"status": "ERROR", "error": "No soy el maestro"}

        elif tipo == "SOLICITAR_ALTA_DISPOSITIVO":
            respuesta = procesar_dispositivo_en_maestro(payload) if MI_NOMBRE == MAESTRO_ACTUAL else {"status": "ERROR", "error": "No soy el maestro"}

        elif tipo == "SOLICITAR_CIERRE_TICKET":
            respuesta = procesar_cierre_en_maestro(payload) if MI_NOMBRE == MAESTRO_ACTUAL else {"status": "ERROR", "error": "No soy el maestro"}

        elif tipo == "SOLICITAR_ALTA_USUARIO":
            respuesta = procesar_usuario_en_maestro(payload) if MI_NOMBRE == MAESTRO_ACTUAL else {"status": "ERROR", "error": "No soy el maestro"}

        elif tipo == "SOLICITAR_ALTA_INGENIERO":
            respuesta = procesar_ingeniero_en_maestro(payload) if MI_NOMBRE == MAESTRO_ACTUAL else {"status": "ERROR", "error": "No soy el maestro"}

        elif tipo == "ELECTION":
            if ID_NUMERICO > payload["id_emisor"]:
                respuesta = {"status": "OK"}
                if not EN_ELECCION:
                    threading.Thread(target=iniciar_eleccion, daemon=True).start()

        elif tipo == "COORDINATOR":
            MAESTRO_ACTUAL = payload["maestro"]
            EN_ELECCION = False
            respuesta = {"status": "ACK"}

        conn.send(json.dumps(respuesta).encode("utf-8"))
    except:
        pass
    finally:
        try: conn.close()
        except: pass

def servidor_escucha():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("0.0.0.0", PORT))
        s.listen()
    except:
        return
    while True:
        try:
            conn, addr = s.accept()
            threading.Thread(target=despachar_peticion, args=(conn, addr), daemon=True).start()
        except: break

# ==========================================
# SOLICITUD DE ENTRADA A SECCIÓN CRÍTICA LOCAL
# ==========================================

def solicitar_acceso_mutex_global():
    global lamport_clock, reclamando_seccion_critica, NODOS_ACTIVOS
    reclamando_seccion_critica = True
    lamport_clock += 1
    
    nodos_a_solicitar = [n for n in NODOS if n["host"] in NODOS_ACTIVOS and n["host"] != MI_NOMBRE]
    oks_necesarios = len(nodos_a_solicitar)
    oks_obtenidos = 0
    
    for nodo in nodos_a_solicitar:
        res = enviar_mensaje(nodo["host"], nodo["port"], {
            "tipo": "MUTEX_REQUEST",
            "payload": {"clock": lamport_clock, "nodo": MI_NOMBRE}
        }, timeout=1.0)
        if res.get("status") == "MUTEX_OK":
            oks_obtenidos += 1
            
    return oks_obtenidos >= oks_necesarios

def liberar_mutex_global():
    global reclamando_seccion_critica, peticiones_diferidas
    reclamando_seccion_critica = False
    # Responder a todos los nodos que dejamos en espera por prioridad inferior
    for conn_diferida in peticiones_diferidas:
        try:
            conn_diferida.send(json.dumps({"status": "MUTEX_OK"}).encode("utf-8"))
            conn_diferida.close()
        except: pass
    peticiones_diferidas.clear()

# ==========================================
# LÓGICA DEL MAESTRO & ACTUALIZACIONES (CONSENSO 2PC)
# ==========================================

def procesar_usuario_en_maestro(payload):
    db = cargar_db()
    id_u, nombre = payload["id_usuario"], payload["nombre"]

    if any(u["id"] == id_u for u in db["usuarios"]):
        return {"status": "ERROR", "error": f"El Usuario con ID {id_u} ya existe."}

    db["usuarios"].append({"id": id_u, "nombre": nombre})
    
    if ejecutar_consenso_2pc(db):
        return {"status": "SUCCESS"}
    return {"status": "ERROR", "error": "Fallo en el consenso distribuidora 2PC"}

def procesar_ingeniero_en_maestro(payload):
    db = cargar_db()
    id_i, nombre, sucursal = payload["id_ingeniero"], payload["nombre"], payload["sucursal"]

    if any(i["id"] == id_i for i in db["ingenieros"]):
        return {"status": "ERROR", "error": f"El Ingeniero con ID {id_i} ya existe."}

    db["ingenieros"].append({
        "id": id_i, 
        "nombre": nombre, 
        "sucursal": sucursal, 
        "tickets_asignados": 0
    })
    
    if ejecutar_consenso_2pc(db):
        return {"status": "SUCCESS"}
    return {"status": "ERROR", "error": "Fallo en el consenso distribuidora 2PC"}

def procesar_ticket_en_maestro(payload):
    db = cargar_db()
    id_u, id_d, sucursal_origen = payload["id_usuario"], payload["id_dispositivo"], payload["sucursal"]

    if not any(u["id"] == id_u for u in db["usuarios"]) or not any(d["id"] == id_d for d in db["dispositivos"]):
        return {"status": "ERROR", "error": "Usuario o Dispositivo inexistente."}

    candidatos = [i for i in db["ingenieros"] if i["sucursal"] == sucursal_origen and i["sucursal"] in NODOS_ACTIVOS] or [i for i in db["ingenieros"] if i["sucursal"] in NODOS_ACTIVOS]
    if not candidatos: return {"status": "ERROR", "error": "No hay ingenieros activos en la red mundial."}
        
    ingeniero = min(candidatos, key=lambda x: x["tickets_asignados"])
    num_ticket = f"TK{len(db['tickets']) + 1:03d}"
    folio = f"{id_u}+{ingeniero['id']}+{sucursal_origen}+{num_ticket}"
    
    id_ing_asignado = ingeniero["id"]
    db["tickets"].append({
        "folio": folio, "id_usuario": id_u, "id_ingeniero": id_ing_asignado,
        "id_dispositivo": id_d, "sucursal": sucursal_origen, "estado": "ABIERTO"
    })
    
    for i in db["ingenieros"]:
        if i["id"] == id_ing_asignado: i["tickets_asignados"] += 1; break

    if ejecutar_consenso_2pc(db):
        return {"status": "SUCCESS", "folio": folio, "ingeniero": ingeniero["nombre"]}
    return {"status": "ERROR", "error": "Fallo en el consenso distribuidora 2PC"}

def procesar_dispositivo_en_maestro(payload):
    db = cargar_db()
    id_d, tipo_d = payload["id_dispositivo"], payload["tipo"]

    if any(d["id"] == id_d for d in db["dispositivos"]):
        return {"status": "ERROR", "error": "El dispositivo ya existe."}

    conteo_sucursales = {n["host"]: 0 for n in NODOS if n["host"] in NODOS_ACTIVOS}
    for d in db["dispositivos"]:
        suc = d["sucursal_asignada"]
        if suc in conteo_sucursales: conteo_sucursales[suc] += 1

    sucursal_elegida = min(conteo_sucursales, key=conteo_sucursales.get) if conteo_sucursales else MI_NOMBRE
    db["dispositivos"].append({"id": id_d, "tipo": tipo_d, "sucursal_asignada": sucursal_elegida})

    if ejecutar_consenso_2pc(db):
        return {"status": "SUCCESS", "sucursal_asignada": sucursal_elegida}
    return {"status": "ERROR", "error": "Fallo en el consenso distribuidora 2PC"}

def procesar_cierre_en_maestro(payload):
    db = cargar_db()
    id_ticket_buscar = payload["id_ticket"]

    ticket = next((t for t in db["tickets"] if t["folio"].split("+")[-1] == id_ticket_buscar), None)
    if not ticket: return {"status": "ERROR", "error": "ID de Ticket no encontrado."}
    if ticket["estado"] == "CERRADO": return {"status": "ERROR", "error": "El ticket ya está CERRADO."}

    ticket["estado"] = "CERRADO"
    id_ing = ticket["id_ingeniero"]
    for i in db["ingenieros"]:
        if i["id"] == id_ing and i["tickets_asignados"] > 0:
            i["tickets_asignados"] -= 1; break

    if ejecutar_consenso_2pc(db):
        return {"status": "SUCCESS"}
    return {"status": "ERROR", "error": "Fallo en el consenso distribuidora 2PC"}

def ejecutar_redistribucion_por_falla(nodo_muerto):
    if MI_NOMBRE != MAESTRO_ACTUAL: return
    db = cargar_db()
    print(f"\n[F5 - REDISTRIBUCIÓN] Limpiando pendientes de la sucursal caída: {nodo_muerto}")
    tickets_afectados = [t for t in db["tickets"] if t["sucursal"] == nodo_muerto and t["estado"] == "ABIERTO"]
    sobrevivientes = [n for n in NODOS_ACTIVOS if n != nodo_muerto]
    if not sobrevivientes: return

    for idx, tk in enumerate(tickets_afectados):
        nueva_sucursal = sobrevivientes[idx % len(sobrevivientes)]
        candidatos = [i for i in db["ingenieros"] if i["sucursal"] == nueva_sucursal]
        if candidatos:
            ing_elegido = min(candidatos, key=lambda x: x["tickets_asignados"])
            old_ing = tk["id_ingeniero"]
            for i in db["ingenieros"]:
                if i["id"] == old_ing and i["tickets_asignados"] > 0: i["tickets_asignados"] -= 1
                if i["id"] == ing_elegido["id"]: i["tickets_asignados"] += 1
            
            id_ticket_corto = tk["folio"].split("+")[-1]
            nuevo_folio = f"{tk['id_usuario']}+{ing_elegido['id']}+{nueva_sucursal}+{id_ticket_corto}"
            tk["sucursal"] = nueva_sucursal
            tk["id_ingeniero"] = ing_elegido["id"]
            tk["folio"] = nuevo_folio

    for d in db["dispositivos"]:
        if d["sucursal_asignada"] == nodo_muerto:
            nueva_suc = sobrevivientes[len(db["dispositivos"]) % len(sobrevivientes)]
            d["sucursal_asignada"] = nueva_suc

    # Ejecutar 2PC para forzar la reestructuración por falla en todos los nodos
    ejecutar_consenso_2pc(db)

# ==========================================
# MOTOR DE ALTA DISPONIBILIDAD (BULLY ALGORITHM)
# ==========================================

def hilo_heartbeat():
    global NODOS_ACTIVOS, MAESTRO_ACTUAL, EN_ELECCION
    print("[INFO] Sistema de monitoreo en espera (15s)...")
    time.sleep(15)
    print("[INFO] Monitoreo de alta disponibilidad ACTIVO.")

    while True:
        time.sleep(2.5)
        if EN_ELECCION: continue

        lista_vivos_ahora = []
        maestro_respondio = False

        for nodo in NODOS:
            res = enviar_mensaje(nodo["host"], nodo["port"], {"tipo": "PING"}, timeout=0.5)
            if res.get("status") == "ALIVE":
                lista_vivos_ahora.append(nodo["host"])
                if nodo["host"] == MAESTRO_ACTUAL: maestro_respondio = True
            else:
                if MI_NOMBRE == MAESTRO_ACTUAL and nodo["host"] in NODOS_ACTIVOS:
                    NODOS_ACTIVOS.remove(nodo["host"])
                    ejecutar_redistribucion_por_falla(nodo["host"])

        NODOS_ACTIVOS = lista_vivos_ahora
        if MI_NOMBRE != MAESTRO_ACTUAL and not maestro_respondio:
            print(f"\n[!] Líder {MAESTRO_ACTUAL} caído. Convocando elecciones...")
            iniciar_eleccion()

def iniciar_eleccion():
    global EN_ELECCION, MAESTRO_ACTUAL
    EN_ELECCION = True
    nodos_mayores = [n for n in NODOS if int(n["host"].replace("vm", "")) > ID_NUMERICO and n["host"] in NODOS_ACTIVOS]
    if not nodos_mayores:
        proclamarse_maestro()
        return

    recibi_ok = False
    for n in nodos_mayores:
        res = enviar_mensaje(n["host"], n["port"], {"tipo": "ELECTION", "payload": {"id_emisor": ID_NUMERICO}}, timeout=0.8)
        if res.get("status") == "OK":
            recibi_ok = True; break 
            
    if not recibi_ok: proclamarse_maestro()

def proclamarse_maestro():
    global MAESTRO_ACTUAL, EN_ELECCION
    MAESTRO_ACTUAL = MI_NOMBRE
    EN_ELECCION = False
    print(f"\n[LOG] Gane las elecciones. Nuevo lider: {MAESTRO_ACTUAL}.")
    for nodo in NODOS:
        if nodo["host"] != MI_NOMBRE and nodo["host"] in NODOS_ACTIVOS:
            enviar_mensaje(nodo["host"], nodo["port"], {"tipo": "COORDINATOR", "payload": {"maestro": MI_NOMBRE}}, timeout=0.5)

# ==========================================
# VISTAS DEL MENÚ Y CONSULTAS
# ==========================================

def submenu_consultas():
    while True:
        print("\n--- SUBMENÚ DE CONSULTAS DISTRIBUIDAS ---")
        print("1. Ver lista de Ingenieros")
        print("2. Ver lista de Usuarios")
        print("3. Ver lista de Dispositivos por Sucursal")
        print("4. Ver Tickets Abiertos")
        print("5. Regresar al Menú Principal")
        op = input("Selecciona una opción: ")
        db = cargar_db()
        if op == "1":
            for i in db["ingenieros"]: print(f"ID: {i['id']} | Nombre: {i['nombre']} | Sucursal: {i['sucursal']} | Tickets Activos: {i['tickets_asignados']}")
        elif op == "2":
            for u in db["usuarios"]: print(f"ID: {u['id']} | Nombre: {u['nombre']}")
        elif op == "3":
            for d in db["dispositivos"]: print(f"ID: {d['id']} | Tipo: {d['tipo']} | Sucursal Asignada: {d['sucursal_asignada']}")
        elif op == "4":
            abiertos = [t for t in db["tickets"] if t["estado"] == "ABIERTO"]
            if not abiertos: print("No hay tickets abiertos.")
            for t in abiertos: print(f"Folio Global: {t['folio']} | Estado: {t['estado']}")
        elif op == "5": break

def menu():
    global MAESTRO_ACTUAL
    threading.Thread(target=servidor_escucha, daemon=True).start()
    time.sleep(0.5)
    
    for n in NODOS:
        if n["host"] != MI_NOMBRE:
            res = enviar_mensaje(n["host"], n["port"], {"tipo": "PING"}, timeout=0.5)
            if res.get("status") == "ALIVE":
                MAESTRO_ACTUAL = res["maestro"]
                break

    threading.Thread(target=hilo_heartbeat, daemon=True).start()

    while True:
        try:
            maestro_info = next(n for n in NODOS if n["host"] == MAESTRO_ACTUAL)
        except StopIteration:
            print("Esperando estabilización de red..."); time.sleep(1); continue

        print(f"\n--- SUCURSAL: {MI_NOMBRE} ---")
        print(f"Líder Coordinador Actual: {MAESTRO_ACTUAL}")
        print(f"Nodos Activos en Red: {NODOS_ACTIVOS}")
        print("1. Menú de Consultas Específicas")
        print("2. Registrar Nuevo Usuario [ACTUALIZAR TABLA]")
        print("3. Registrar Nuevo Ingeniero [ACTUALIZAR TABLA]")
        print("4. Levantar Ticket de Soporte [RICART-AGRAWALA MUTEX]")
        print("5. Agregar Dispositivo [INGENIEROS - EQUITATIVO]")
        print("6. Cerrar Ticket de Soporte [INGENIEROS]")
        print("7. Salir")
        
        opcion = input("Selecciona una opción: ")
        
        if opcion == "1":
            submenu_consultas()
            
        elif opcion == "2":
            id_u = input("Asigna ID al nuevo Usuario [ej: USR04]: ").strip().upper()
            nombre_u = input("Nombre completo del Usuario: ").strip()
            if not id_u or not nombre_u: continue

            peticion = {"tipo": "SOLICITAR_ALTA_USUARIO", "payload": {"id_usuario": id_u, "nombre": nombre_u}}
            res = enviar_mensaje(maestro_info["host"], maestro_info["port"], peticion)
            print(f"\n[OK] Usuario registrado por consenso global 2PC." if res.get("status") == "SUCCESS" else f"\n[ERROR] {res.get('error')}")

        elif opcion == "3":
            id_i = input("Asigna ID al nuevo Ingeniero [ej: ING05]: ").strip().upper()
            nombre_i = input("Nombre completo del Ingeniero: ").strip()
            suc_i = input(f"Sucursal base para este ingeniero (vm1 a vm4) [Dejar vacío para usar {MI_NOMBRE}]: ").strip().lower()
            if not suc_i: suc_i = MI_NOMBRE
            if not id_i or not nombre_i: continue

            peticion = {"tipo": "SOLICITAR_ALTA_INGENIERO", "payload": {"id_ingeniero": id_i, "nombre": nombre_i, "sucursal": suc_i}}
            res = enviar_mensaje(maestro_info["host"], maestro_info["port"], peticion)
            print(f"\n[OK] Ingeniero registrado por consenso global 2PC en {suc_i}!" if res.get("status") == "SUCCESS" else f"\n[ERROR] {res.get('error')}")

        elif opcion == "4":
            id_u = input("ID Usuario [ej: USR01]: ").strip().upper()
            id_d = input("ID Dispositivo [ej: DEV01]: ").strip().upper()
            if not id_u or not id_d: continue
            
            print("\n[MUTEX] Solicitando entrada a Sección Crítica Distribuida (Ricart-Agrawala)...")
            # Adquisición de la sección crítica real distribuida
            if solicitar_acceso_mutex_global():
                print("[MUTEX] Acceso concedido a la Sección Crítica. Procesando ticket...")
                res = enviar_mensaje(maestro_info["host"], maestro_info["port"], {
                    "tipo": "SOLICITAR_TICKET_NUEVO", 
                    "payload": {"id_usuario": id_u, "id_dispositivo": id_d, "sucursal": MI_NOMBRE}
                })
                liberar_mutex_global() # Salida de sección crítica distribuidora
                print(f"\n[OK] Ticket Creado! Folio: {res['folio']}\nAsignado a: {res['ingeniero']}" if res.get("status") == "SUCCESS" else f"\n[ERROR] {res.get('error')}")
            else:
                print("\n[ERROR] Acceso denegado o colisión en la exclusión mutua distribuidora.")
            
        elif opcion == "5":
            id_d = input("ID del dispositivo [ej: DEV03]: ").strip().upper()
            tipo_d = input("Tipo [ej: Impresora]: ").strip()
            if not id_d or not tipo_d: continue
            res = enviar_mensaje(maestro_info["host"], maestro_info["port"], {"tipo": "SOLICITAR_ALTA_DISPOSITIVO", "payload": {"id_dispositivo": id_d, "tipo": tipo_d}})
            print(f"\n[OK] Distribuido a la sucursal: {res['sucursal_asignada']}" if res.get("status") == "SUCCESS" else f"\n[ERROR] {res.get('error')}")
            
        elif opcion == "6":
            id_tk = input("Introduce el ID corto del Ticket a cerrar [ej: TK001]: ").strip().upper()
            if not id_tk: continue
            res = enviar_mensaje(maestro_info["host"], maestro_info["port"], {"tipo": "SOLICITAR_CIERRE_TICKET", "payload": {"id_ticket": id_tk}})
            print("\n[OK] El ticket ha sido cerrado globalmente por 2PC." if res.get("status") == "SUCCESS" else f"\n[ERROR] {res.get('error')}")
            
        elif opcion == "7": 
            break

if __name__ == "__main__":
    menu()