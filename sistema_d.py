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
MAESTRO_ACTUAL = "vm1" 

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
        s.settimeout(2.0)
        s.connect((host, port))
        s.send(json.dumps(mensaje_dict).encode("utf-8"))
        respuesta = s.recv(4096).decode("utf-8")
        s.close()
        return json.loads(respuesta)
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

def difundir_consenso(mensaje_dict):
    for nodo in NODOS:
        res = enviar_mensaje(nodo["host"], nodo["port"], mensaje_dict)
        if res.get("status") == "ERROR":
            print(f"[CONSENSO] Nodo {nodo['host']} no respondió a la replicación.")

# ==========================================
# 🔹 SERVIDOR: PROCESAMIENTO EN RED
# ==========================================

def despachar_peticion(conn, addr):
    try:
        data = conn.recv(4096).decode("utf-8")
        if not data: return
        
        peticion = json.loads(data)
        tipo = peticion.get("tipo")
        payload = peticion.get("payload")
        
        respuesta = {"status": "REJECT", "info": "Petición no reconocida"}

        # REPLICACIÓN GLOBAL (Consenso)
        if tipo == "REPLICAR_COMMIT":
            guardar_db(payload["db"])
            print(f"\n[REPLICACIÓN] Base de datos sincronizada por orden del Maestro.")
            respuesta = {"status": "ACK"}

        # ENRUTAMIENTO AL MAESTRO (Exclusión mutua y balanceo)
        elif tipo == "SOLICITAR_TICKET_NUEVO":
            if MI_NOMBRE != MAESTRO_ACTUAL:
                respuesta = {"status": "ERROR", "error": "No soy el nodo maestro"}
            else:
                respuesta = procesar_ticket_en_maestro(payload)

        elif tipo == "SOLICITAR_ALTA_DISPOSITIVO":
            if MI_NOMBRE != MAESTRO_ACTUAL:
                respuesta = {"status": "ERROR", "error": "No soy el nodo maestro"}
            else:
                respuesta = procesar_dispositivo_en_maestro(payload)

        elif tipo == "SOLICITAR_CIERRE_TICKET":
            if MI_NOMBRE != MAESTRO_ACTUAL:
                respuesta = {"status": "ERROR", "error": "No soy el nodo maestro"}
            else:
                respuesta = procesar_cierre_en_maestro(payload)

        conn.send(json.dumps(respuesta).encode("utf-8"))
    except Exception as e:
        print(f"Error en servidor: {e}")
    finally:
        conn.close()

def servidor_escucha():
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
            threading.Thread(target=despachar_peticion, args=(conn, addr), daemon=True).start()
        except: break

# ==========================================
# 🔹 LÓGICA ATÓMICA EN EL NODO MAESTRO
# ==========================================

def procesar_ticket_en_maestro(payload):
    db = cargar_db()
    id_u = payload["id_usuario"]
    id_d = payload["id_dispositivo"]
    sucursal_origen = payload["sucursal"]

    # Validar existencia de usuario y dispositivo antes de asignar
    usuario_existe = any(u["id"] == id_u for u in db["usuarios"])
    dispositivo_existe = any(d["id"] == id_d for d in db["dispositivos"])

    if not usuario_existe:
        return {"status": "ERROR", "error": f"El usuario {id_u} no existe en la base de datos."}
    if not dispositivo_existe:
        return {"status": "ERROR", "error": f"El dispositivo {id_d} no está registrado."}

    # Algoritmo de asignación óptimo (Exclusión mutua centralizada)
    ingenieros_locales = [i for i in db["ingenieros"] if i["sucursal"] == sucursal_origen]
    candidatos = ingenieros_locales if ingenieros_locales else db["ingenieros"]
    
    if not candidatos:
        return {"status": "ERROR", "error": "No hay ingenieros configurados en el sistema"}
        
    ingeniero = min(candidatos, key=lambda x: x["tickets_asignados"])
    
    num_ticket = f"TK{len(db['tickets']) + 1:03d}"
    folio = f"{id_u}+{ingeniero['id']}+{sucursal_origen}+{num_ticket}"
    
    # 🔹 CORRECCIÓN AQUÍ: Guardamos el ID por separado y creamos el ticket limpio
    id_ing_asignado = ingeniero["id"]

    nuevo_ticket = {
        "folio": folio,
        "id_usuario": id_u,
        "id_ingeniero": id_ing_asignado,
        "id_dispositivo": id_d,
        "sucursal": sucursal_origen,
        "estado": "ABIERTO"
    }
    
    # Modificar estado interno del maestro
    for i in db["ingenieros"]:
        if i["id"] == id_ing_asignado:
            i["tickets_asignados"] += 1
            break

    db["tickets"].append(nuevo_ticket)
    
    # Consenso
    difundir_consenso({"tipo": "REPLICAR_COMMIT", "payload": {"db": db}})
    return {"status": "SUCCESS", "folio": folio, "ingeniero": ingeniero["nombre"]}

def procesar_dispositivo_en_maestro(payload):
    db = cargar_db()
    id_d = payload["id_dispositivo"]
    tipo_d = payload["tipo"]

    if any(d["id"] == id_d for d in db["dispositivos"]):
        return {"status": "ERROR", "error": f"El dispositivo {id_d} ya existe."}

    # 🔹 ALGORITMO: Distribución Equitativa de Dispositivos (Balanceo de Carga)
    # Contamos cuántos dispositivos tiene asignada cada sucursal actualmente
    conteo_sucursales = {"vm1": 0, "vm2": 0, "vm3": 0, "vm4": 0}
    for d in db["dispositivos"]:
        suc = d["sucursal_asignada"]
        if suc in conteo_sucursales:
            conteo_sucursales[suc] += 1

    # Elegimos la sucursal con el número mínimo de dispositivos
    sucursal_elegida = min(conteo_sucursales, key=conteo_sucursales.get)

    nuevo_dispositivo = {
        "id": id_d,
        "tipo": tipo_d,
        "sucursal_asignada": sucursal_elegida
    }
    db["dispositivos"].append(nuevo_dispositivo)

    # Consenso
    difundir_consenso({"tipo": "REPLICAR_COMMIT", "payload": {"db": db}})
    return {"status": "SUCCESS", "sucursal_asignada": sucursal_elegida}

def procesar_cierre_en_maestro(payload):
    db = cargar_db()
    folio_buscar = payload["folio"]

    ticket = next((t for t in db["tickets"] if t["folio"] == folio_buscar), None)
    if not ticket:
        return {"status": "ERROR", "error": "El folio de ticket especificado no existe."}
    
    if ticket["estado"] == "CERRADO":
        return {"status": "ERROR", "error": "Este ticket ya se encuentra CERRADO."}

    # Modificar el estado del ticket y liberar la carga del ingeniero asignado
    ticket["estado"] = "CERRADO"
    id_ing = ticket["id_ingeniero"]

    for i in db["ingenieros"]:
        if i["id"] == id_ing:
            if i["tickets_asignados"] > 0:
                i["tickets_asignados"] -= 1
            break

    # Consenso
    difundir_consenso({"tipo": "REPLICAR_COMMIT", "payload": {"db": db}})
    return {"status": "SUCCESS"}

# ==========================================
# 🔹 INTERFAZ DE USUARIO CON RED
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
            print("\n--- INGENIEROS ---")
            for i in db["ingenieros"]:
                print(f"ID: {i['id']} | Nombre: {i['nombre']} | Sucursal: {i['sucursal']} | Tickets Activos: {i['tickets_asignados']}")
        elif op == "2":
            print("\n--- USUARIOS ---")
            for u in db["usuarios"]:
                print(f"ID: {u['id']} | Nombre: {u['nombre']}")
        elif op == "3":
            print("\n--- DISPOSITIVOS DISTRIBUIDOS ---")
            for d in db["dispositivos"]:
                print(f"ID: {d['id']} | Tipo: {d['tipo']} | Sucursal Asignada: {d['sucursal_asignada']}")
        elif op == "4":
            print("\n--- TICKETS ABIERTOS ---")
            abiertos = [t for t in db["tickets"] if t["estado"] == "ABIERTO"]
            if not abiertos: print("No hay tickets pendientes.")
            for t in abiertos:
                print(f"Folio: {t['folio']} | Dispositivo: {t['id_dispositivo']} | Estado: {t['estado']}")
        elif op == "5":
            break

def menu():
    threading.Thread(target=servidor_escucha, daemon=True).start()
    time.sleep(0.5)

    try:
        maestro_info = next(n for n in NODOS if n["host"] == MAESTRO_ACTUAL)
    except StopIteration:
        print("Error crítico en archivo config.json")
        return

    while True:
        print(f"\n--- SUCURSAL COOPERATIVA: {MI_NOMBRE} ---")
        print(f"Nodo Maestro Actual: {MAESTRO_ACTUAL}")
        print("1. Menú de Consultas Específicas")
        print("2. Levantar Ticket de Soporte [USUARIOS]")
        print("3. Agregar Dispositivo [INGENIEROS]")
        print("4. Cerrar Ticket de Soporte [INGENIEROS]")
        print("5. Salir")
        
        opcion = input("Selecciona una opción: ")
        
        if opcion == "1":
            submenu_consultas()
            
        elif opcion == "2":
            id_u = input("ID Usuario [Ejemplo: USR01]: ").strip().upper()
            id_d = input("ID Dispositivo [Ejemplo: DEV01]: ").strip().upper()
            if not id_u or not id_d: continue
            
            peticion = {"tipo": "SOLICITAR_TICKET_NUEVO", "payload": {"id_usuario": id_u, "id_dispositivo": id_d, "sucursal": MI_NOMBRE}}
            respuesta = enviar_mensaje(maestro_info["host"], maestro_info["port"], peticion)
            
            if respuesta.get("status") == "SUCCESS":
                print(f"\n✅ Ticket Creado!\nFolio: {respuesta['folio']}\nAsignado a: {respuesta['ingeniero']}")
            else:
                print(f"\n❌ Error: {respuesta.get('error')}")
                
        elif opcion == "3":
            id_d = input("Asigna ID al nuevo dispositivo [Ejemplo: DEV03]: ").strip().upper()
            tipo_d = input("Tipo de dispositivo [Ejemplo: Impresora HP]: ").strip()
            if not id_d or not tipo_d: continue

            peticion = {"tipo": "SOLICITAR_ALTA_DISPOSITIVO", "payload": {"id_dispositivo": id_d, "tipo": tipo_d}}
            respuesta = enviar_mensaje(maestro_info["host"], maestro_info["port"], peticion)

            if respuesta.get("status") == "SUCCESS":
                print(f"\n✅ Dispositivo Registrado y distribuido equitativamente a la sucursal: {respuesta['sucursal_asignada']}")
            else:
                print(f"\n❌ Error: {respuesta.get('error')}")

        elif opcion == "4":
            folio = input("Introduce el Folio Completo del Ticket a cerrar: ").strip()
            if not folio: continue

            peticion = {"tipo": "SOLICITAR_CIERRE_TICKET", "payload": {"folio": folio}}
            respuesta = enviar_mensaje(maestro_info["host"], maestro_info["port"], peticion)

            if respuesta.get("status") == "SUCCESS":
                print(f"\n✅ ¡El ticket ha sido cerrado con éxito globalmente!")
            else:
                print(f"\n❌ Error: {respuesta.get('error')}")
                
        elif opcion == "5":
            break

if __name__ == "__main__":
    menu()