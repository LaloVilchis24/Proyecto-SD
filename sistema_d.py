import json
import threading
import socket
import time

# ==========================================
# 🔹 CONFIGURACIÓN DE RED Y PERSISTENCIA
# ==========================================
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

MI_NOMBRE = config["mi_nombre"] # "vm1", "vm2", etc.
NODOS = config["nodos"]
PORT = 5000

# Extraer ID numérico para el algoritmo del Matón (Bully) -> "vm1" de ahí saca el 1
ID_NUMERICO = int(MI_NOMBRE.replace("vm", ""))

DB_LOCK = threading.Lock()
DB_FILE = "db.json"

# Variables dinámicas del estado distribuido
MAESTRO_ACTUAL = "vm1" 
NODOS_ACTIVOS = ["vm1", "vm2", "vm3", "vm4"]
EN_ELECCION = False

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
# 🔹 CLIENTE: ENVÍO DE MENSAJES Y DIFUSIÓN
# ==========================================

def enviar_mensaje(host, port, mensaje_dict):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1.5) # Timeout corto para detectar caídas rápido
        s.connect((host, port))
        s.send(json.dumps(mensaje_dict).encode("utf-8"))
        respuesta = s.recv(4096).decode("utf-8")
        s.close()
        return json.loads(respuesta)
    except:
        return {"status": "ERROR", "error": "Nodo inalcanzable"}

def difundir_consenso(mensaje_dict):
    for nodo in NODOS:
        if nodo["host"] in NODOS_ACTIVOS:
            enviar_mensaje(nodo["host"], nodo["port"], mensaje_dict)

# ==========================================
# 🔹 SERVIDOR: PROCESAMIENTO DE PETICIONES
# ==========================================

def despachar_peticion(conn, addr):
    global MAESTRO_ACTUAL, EN_ELECCION
    try:
        data = conn.recv(4096).decode("utf-8")
        if not data: return
        
        peticion = json.loads(data)
        tipo = peticion.get("tipo")
        payload = peticion.get("payload")
        
        respuesta = {"status": "ACK"}

        # PING DE MONITOREO
        if tipo == "PING":
            respuesta = {"status": "ALIVE"}

        # REPLICACIÓN (Consenso)
        elif tipo == "REPLICAR_COMMIT":
            guardar_db(payload["db"])
            respuesta = {"status": "ACK"}

        # ENRUTAMIENTO AL MAESTRO
        elif tipo == "SOLICITAR_TICKET_NUEVO":
            respuesta = procesar_ticket_en_maestro(payload) if MI_NOMBRE == MAESTRO_ACTUAL else {"status": "ERROR", "error": "No soy el maestro"}

        elif tipo == "SOLICITAR_ALTA_DISPOSITIVO":
            respuesta = procesar_dispositivo_en_maestro(payload) if MI_NOMBRE == MAESTRO_ACTUAL else {"status": "ERROR", "error": "No soy el maestro"}

        elif tipo == "SOLICITAR_CIERRE_TICKET":
            respuesta = procesar_cierre_en_maestro(payload) if MI_NOMBRE == MAESTRO_ACTUAL else {"status": "ERROR", "error": "No soy el maestro"}

        # ALGORITMO BULLY (ELECCIÓN)
        elif tipo == "ELECTION":
            # Si el que me contacta tiene menor ID que yo, le digo "OK" para frenarlo y tomo el control
            if ID_NUMERICO > payload["id_emisor"]:
                respuesta = {"status": "OK"}
                if not EN_ELECCION:
                    threading.Thread(target=iniciar_eleccion, daemon=True).start()

        elif tipo == "COORDINATOR":
            MAESTRO_ACTUAL = payload["maestro"]
            EN_ELECCION = False
            print(f"\n[ALERTA] Nuevo jefe en la red: {MAESTRO_ACTUAL}. Elección finalizada.")
            respuesta = {"status": "ACK"}

        conn.send(json.dumps(respuesta).encode("utf-8"))
    except Exception as e:
        pass
    finally:
        conn.close()

def servidor_escucha():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("0.0.0.0", PORT))
        s.listen()
    except Exception as e:
        return
    while True:
        try:
            conn, addr = s.accept()
            threading.Thread(target=despachar_peticion, args=(conn, addr), daemon=True).start()
        except: break

# ==========================================
# 🔹 LÓGICA ATÓMICA EN EL NODO MAESTRO
# ==========================================

def procesar_ticket_en_maestro(payload):
    db = cargar_db()
    id_u, id_d, sucursal_origen = payload["id_usuario"], payload["id_dispositivo"], payload["sucursal"]

    if not any(u["id"] == id_u for u in db["usuarios"]) or not any(d["id"] == id_d for d in db["dispositivos"]):
        return {"status": "ERROR", "error": "Usuario o Dispositivo inexistente."}

    candidatos = [i for i in db["ingenieros"] if i["sucursal"] == sucursal_origen] or db["ingenieros"]
    if not candidatos: return {"status": "ERROR", "error": "No hay ingenieros."}
        
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

    difundir_consenso({"tipo": "REPLICAR_COMMIT", "payload": {"db": db}})
    return {"status": "SUCCESS", "folio": folio, "ingeniero": ingeniero["nombre"]}

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

    difundir_consenso({"tipo": "REPLICAR_COMMIT", "payload": {"db": db}})
    return {"status": "SUCCESS", "sucursal_asignada": sucursal_elegida}

def procesar_cierre_en_maestro(payload):
    db = cargar_db()
    id_ticket_buscar = payload["id_ticket"] # Viene como "TK001"

    # 🔹 CAMBIO SOLICITADO: Buscar si algún folio termina con el ID proporcionado
    ticket = next((t for t in db["tickets"] if t["folio"].split("+")[-1] == id_ticket_buscar), None)
    
    if not ticket:
        return {"status": "ERROR", "error": f"No se encontró ningún ticket con el ID {id_ticket_buscar}."}
    if ticket["estado"] == "CERRADO":
        return {"status": "ERROR", "error": "Este ticket ya está CERRADO."}

    ticket["estado"] = "CERRADO"
    id_ing = ticket["id_ingeniero"]
    for i in db["ingenieros"]:
        if i["id"] == id_ing and i["tickets_asignados"] > 0:
            i["tickets_asignados"] -= 1
            break

    difundir_consenso({"tipo": "REPLICAR_COMMIT", "payload": {"db": db}})
    return {"status": "SUCCESS"}

# ==========================================
# 🔹 DETECCIÓN DE FALLAS Y ALGORITMO BULLY
# ==========================================

def hilo_heartbeat():
    """Vigila de forma asíncrona la salud del Maestro."""
    global NODOS_ACTIVOS, EN_ELECCION
    while True:
        time.sleep(3)
        if EN_ELECCION: continue

        if MI_NOMBRE != MAESTRO_ACTUAL:
            # Conseguir los datos de red del maestro actual
            try:
                maestro_info = next(n for n in NODOS if n["host"] == MAESTRO_ACTUAL)
                res = enviar_mensaje(maestro_info["host"], maestro_info["port"], {"tipo": "PING"})
                if res.get("status") == "ERROR":
                    print(f"\n[!] El Maestro ({MAESTRO_ACTUAL}) no responde. Convocando a elecciones...")
                    iniciar_eleccion()
            except StopIteration:
                pass

def iniciar_eleccion():
    global EN_ELECCION, MAESTRO_ACTUAL
    EN_ELECCION = True
    print("[ELECCIÓN] Ejecutando Algoritmo del Matón (Bully)...")
    
    # Buscar nodos con un ID mayor al mío
    nodos_mayores = [n for n in NODOS if int(n["host"].replace("vm", "")) > ID_NUMERICO]
    
    if not nodos_mayores:
        # No hay nadie más alto vivo, yo soy el nuevo jefe
        proclamarse_maestro()
        return

    recibi_ok = False
    for n in nodos_mayores:
        res = enviar_mensaje(n["host"], n["port"], {"tipo": "ELECTION", "payload": {"id_emisor": ID_NUMERICO}})
        if res.get("status") == "OK":
            recibi_ok = True
            break # Un nodo mayor tomó el control, yo me quedo esperando pacientemente
            
    if not recibi_ok:
        # Si nadie mayor respondió, yo me quedo el puesto
        proclamarse_maestro()

def proclamarse_maestro():
    global MAESTRO_ACTUAL, EN_ELECCION
    MAESTRO_ACTUAL = MI_NOMBRE
    EN_ELECCION = False
    print(f"\n👑 [LOG] ¡Gané las elecciones! Ahora yo ({MI_NOMBRE}) soy el nuevo Maestro.")
    # Avisar a toda la red del nuevo orden mundial
    for nodo in NODOS:
        if nodo["host"] != MI_NOMBRE:
            enviar_mensaje(nodo["host"], nodo["port"], {"tipo": "COORDINATOR", "payload": {"maestro": MI_NOMBRE}})

# ==========================================
# 🔹 VISTAS DEL MENÚ
# ==========================================

def submenu_consultas():
    while True:
        print("\n--- 🔍 SUBMENÚ DE CONSULTAS DISTRIBUIDAS ---")
        print("1. Ver lista de Ingenieros")
        print("2. Ver lista de Usuarios")
        print("3. Ver lista de Dispositivos por Sucursal")
        print("4. Ver Tickets Abiertos")
        print("5. Regresar al Menú Principal")
        op = input("Selecciona una opción: ")
        db = cargar_db()
        if op == "1":
            for i in db["ingenieros"]: print(f"ID: {i['id']} | Nombre: {i['nombre']} | Sucursal: {i['sucursal']} | Tickets: {i['tickets_asignados']}")
        elif op == "2":
            for u in db["usuarios"]: print(f"ID: {u['id']} | Nombre: {u['nombre']}")
        elif op == "3":
            for d in db["dispositivos"]: print(f"ID: {d['id']} | Tipo: {d['tipo']} | Sucursal: {d['sucursal_asignada']}")
        elif op == "4":
            abiertos = [t for t in db["tickets"] if t["estado"] == "ABIERTO"]
            if not abiertos: print("No hay tickets abiertos.")
            for t in abiertos: print(f"Folio Global: {t['folio']} | Estado: {t['estado']}")
        elif op == "5": break

def menu():
    threading.Thread(target=servidor_escucha, daemon=True).start()
    threading.Thread(target=hilo_heartbeat, daemon=True).start()
    time.sleep(0.5)

    while True:
        try:
            maestro_info = next(n for n in NODOS if n["host"] == MAESTRO_ACTUAL)
        except StopIteration:
            print("Error en el mapeo del maestro."); break

        print(f"\n--- SUCURSAL: {MI_NOMBRE} ---")
        print(f"Líder Coordinador Actual: {MAESTRO_ACTUAL}")
        print("1. Menú de Consultas")
        print("2. Levantar Ticket de Soporte [USUARIOS]")
        print("3. Agregar Dispositivo [INGENIEROS]")
        print("4. Cerrar Ticket de Soporte [INGENIEROS]")
        print("5. Salir")
        
        opcion = input("Selecciona una opción: ")
        
        if opcion == "1":
            submenu_consultas()
        elif opcion == "2":
            id_u = input("ID Usuario [ej: USR01]: ").strip().upper()
            id_d = input("ID Dispositivo [ej: DEV01]: ").strip().upper()
            if not id_u or not id_d: continue
            res = enviar_mensaje(maestro_info["host"], maestro_info["port"], {"tipo": "SOLICITAR_TICKET_NUEVO", "payload": {"id_usuario": id_u, "id_dispositivo": id_d, "sucursal": MI_NOMBRE}})
            print(f"\n✅ Ticket Creado! Folio: {res['folio']}" if res.get("status") == "SUCCESS" else f"\n❌ Error: {res.get('error')}")
        elif opcion == "3":
            id_d = input("ID del dispositivo [ej: DEV03]: ").strip().upper()
            tipo_d = input("Tipo [ej: Impresora]: ").strip()
            if not id_d or not tipo_d: continue
            res = enviar_mensaje(maestro_info["host"], maestro_info["port"], {"tipo": "SOLICITAR_ALTA_DISPOSITIVO", "payload": {"id_dispositivo": id_d, "tipo": tipo_d}})
            print(f"\n✅ Distribuido a la sucursal: {res['sucursal_asignada']}" if res.get("status") == "SUCCESS" else f"\n❌ Error: {res.get('error')}")
        elif opcion == "4":
            # 🔹 INTERFAZ MEJORADA: Ahora solo pide el código corto del ticket
            id_tk = input("Introduce el ID corto del Ticket a cerrar [ej: TK001]: ").strip().upper()
            if not id_tk: continue
            res = enviar_mensaje(maestro_info["host"], maestro_info["port"], {"tipo": "SOLICITAR_CIERRE_TICKET", "payload": {"id_ticket": id_tk}})
            print("\n✅ ¡El ticket ha sido cerrado globalmente!" if res.get("status") == "SUCCESS" else f"\n❌ Error: {res.get('error')}")
        elif opcion == "5": break

if __name__ == "__main__":
    menu()