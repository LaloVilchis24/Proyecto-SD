import json
import threading
import socket
import time

# ==========================================
# 🔹 CONFIGURACIÓN DE RED Y PERSISTENCIA
# ==========================================
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

MI_NOMBRE = config["mi_nombre"]
NODOS = config["nodos"]
PORT = 5000

DB_LOCK = threading.Lock()
DB_FILE = "db.json"
MAESTRO_ACTUAL = "vm1"  # Por diseño inicial, vm1 arranca como maestro

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
    """Envía un diccionario JSON a un nodo específico y espera respuesta."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect((host, port))
        s.send(json.dumps(mensaje_dict).encode("utf-8"))
        
        respuesta = s.recv(4096).decode("utf-8")
        s.close()
        return json.loads(respuesta)
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

def difundir_consenso(mensaje_dict):
    """El Maestro envía la orden de replicación a TODOS los nodos de la red."""
    for nodo in NODOS:
        # Enviamos a todos, incluso a nosotros mismos para mantener homogeneidad
        res = enviar_mensaje(nodo["host"], nodo["port"], mensaje_dict)
        if res.get("status") == "ERROR":
            print(f"[CONSENSO] Nodo {nodo['host']} no respondió a la replicación.")

# ==========================================
# 🔹 SERVIDOR: ESCUCHA Y PROCESAMIENTO
# ==========================================

def despachar_peticion(conn, addr):
    """Manejador de solicitudes entrantes por red."""
    try:
        data = conn.recv(4096).decode("utf-8")
        if not data:
            return
        
        peticion = json.loads(data)
        tipo = peticion.get("tipo")
        payload = peticion.get("payload")
        
        respuesta = {"status": "REJECT", "info": "Petición no reconocida"}

        # Rol Esclavo/Todos: Acatar la replicación ordenada por el maestro
        if tipo == "REPLICAR_COMMIT":
            guardar_db(payload["db"])
            print(f"\n[REPLICACIÓN] Base de datos sincronizada por orden del Maestro.")
            respuesta = {"status": "ACK"}

        # Rol Maestro: Recibir solicitudes de nuevos tickets
        elif tipo == "SOLICITAR_TICKET_NUEVO":
            if MI_NOMBRE != MAESTRO_ACTUAL:
                respuesta = {"status": "ERROR", "error": "No soy el nodo maestro"}
            else:
                # Procesar atómicamente en el maestro (Exclusión Mutua)
                respuesta = procesar_ticket_en_maestro(payload)

        conn.send(json.dumps(respuesta).encode("utf-8"))
    except Exception as e:
        print(f"Error en servidor: {e}")
    finally:
        conn.close()

def servidor_escucha():
    """Hilo encargado de mantener el puerto abierto."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("0.0.0.0", PORT))
        s.listen()
        print(f"[*] Nodo {MI_NOMBRE} escuchando en el puerto {PORT}...")
    except Exception as e:
        print(f"Error crítico al abrir el puerto: {e}")
        return

    while True:
        try:
            conn, addr = s.accept()
            # Despachar cada petición en un hilo nuevo para no congelar la red
            threading.Thread(target=despachar_peticion, args=(conn, addr), daemon=True).start()
        except:
            break

# ==========================================
# 🔹 LÓGICA CORE EN EL MAESTRO
# ==========================================

def procesar_ticket_en_maestro(payload):
    """El maestro ejecuta la lógica de asignación y esparce el resultado."""
    db = cargar_db()
    id_u = payload["id_usuario"]
    id_d = payload["id_dispositivo"]
    sucursal_origen = payload["sucursal"]

    # Algoritmo de asignación óptimo (Menos cargado)
    ingenieros_locales = [i for i in db["ingenieros"] if i["sucursal"] == sucursal_origen]
    candidatos = ingenieros_locales if ingenieros_locales else db["ingenieros"]
    
    if not candidatos:
        return {"status": "ERROR", "error": "No hay ingenieros en el sistema"}
        
    ingeniero = min(candidatos, key=lambda x: x["tickets_asignados"])
    
    # Generar folio oficial
    num_ticket = f"TK{len(db['tickets']) + 1:03d}"
    folio = f"{id_u}+{ingeniero['id']}+{sucursal_origen}+{num_ticket}"
    
    nuevo_ticket = {
        "folio": folio,
        "id_usuario": id_u,
        "id_ingeniero": ingeniero["id"],
        "id_dispositivo": id_d,
        "sucursal": sucursal_origen,
        "estado": "ABIERTO"
    }
    
    # Impactar cambios en la estructura de datos del maestro
    ingeniero["tickets_asignados"] += 1
    db["tickets"].append(nuevo_ticket)
    
    # CONSENSO / REPLICACIÓN: Obligar a todos los nodos a adoptar este nuevo estado
    msg_consenso = {"tipo": "REPLICAR_COMMIT", "payload": {"db": db}}
    difundir_consenso(msg_consenso)
    
    return {"status": "SUCCESS", "folio": folio, "ingeniero": ingeniero["nombre"]}

# ==========================================
# 🔹 INTERFAZ DE USUARIO CON RED
# ==========================================

def menu():
    # Arrancar el servidor en un hilo secundario daemon
    threading.Thread(target=servidor_escucha, daemon=True).start()
    time.sleep(0.5) # Pausa para dejar que el puerto se abra limpio

    while True:
        print(f"\n--- SUCURSAL COOPERATIVA: {MI_NOMBRE} ---")
        print(f"Nodo Maestro Actual: {MAESTRO_ACTUAL}")
        print("1. Consultar Base de Datos Local Replicada")
        print("2. Levantar Ticket de Soporte (Vía Red/Maestro)")
        print("3. Salir")
        
        opcion = input("Selecciona una opción: ")
        
        if opcion == "1":
            db = cargar_db()
            print("\n================ ESTADO DE LA DB REPLICADA ================")
            print(json.dumps(db, indent=2, ensure_ascii=False))
            print("============================================================\n")
            
        elif opcion == "2":
            id_u = input("ID Usuario: ").strip().upper()
            id_d = input("ID Dispositivo: ").strip().upper()
            
            # Preparar la transacción remota
            peticion = {
                "tipo": "SOLICITAR_TICKET_NUEVO",
                "payload": {"id_usuario": id_u, "id_dispositivo": id_d, "sucursal": MI_NOMBRE}
            }
            
            # Buscar la dirección del maestro en la configuración
            maestro_info = next(n for n in NODOS if n["host"] == MAESTRO_ACTUAL)
            print(f"[RED] Enviando solicitud al nodo maestro ({MAESTRO_ACTUAL})...")
            
            # Enviar la petición al maestro
            respuesta = enviar_mensaje(maestro_info["host"], maestro_info["port"], peticion)
            
            if respuesta.get("status") == "SUCCESS":
                print(f"\n✅ Ticket creado globalmente por el maestro!")
                print(f"Folio: {respuesta['folio']}")
                print(f"Asignado a: {respuesta['ingeniero']}")
            else:
                print(f"\n❌ Error al crear el ticket: {respuesta.get('error', 'Desconocido')}")
                
        elif opcion == "3":
            break

if __name__ == "__main__":
    menu()