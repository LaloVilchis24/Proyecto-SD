import json
import threading
import os

# Cerrojo para exclusión mutua local (evita que hilos choquen al escribir el archivo)
DB_LOCK = threading.Lock()
DB_FILE = "db.json"

# ==========================================
# 🔹 CAPA DE PERSISTENCIA (MANEJO DEL JSON)
# ==========================================

def cargar_db():
    """Lee de forma segura el archivo JSON local."""
    with DB_LOCK:
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"[ALERTA] Archivo {DB_FILE} no encontrado. Creando estructura básica vacía.")
            return {"ingenieros": [], "usuarios": [], "dispositivos": [], "tickets": []}

def guardar_db(data):
    """Escribe de forma segura el nuevo estado en el archivo JSON local."""
    with DB_LOCK:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

# ==========================================
# 🔹 LÓGICA DE NEGOCIO (REQUISITOS DEL SISTEMA)
# ==========================================

def obtener_ingeniero_menos_cargado(db, sucursal_origen):
    """
    Busca ingenieros en la sucursal de origen. Si no hay ninguno disponible,
    busca en todo el sistema. Retorna al que tenga menos tickets asignados.
    """
    # Filtrar por sucursal actual
    ingenieros_locales = [i for i in db["ingenieros"] if i["sucursal"] == sucursal_origen]
    
    # Si la sucursal está vacía o sin personal en ese momento, busca globalmente
    candidatos = ingenieros_locales if ingenieros_locales else db["ingenieros"]
    
    if not candidatos:
        return None
        
    # Obtener el que tenga menor carga (exclusión mutua lógica)
    return min(candidatos, key=lambda x: x["tickets_asignados"])

def generar_folio_ticket(id_usuario, id_ingeniero, sucursal, id_ticket):
    """Genera el identificador único con el formato formal requerido."""
    # Estructura: IDUSUARIO+IDINGENIERO+SUCURSAL+IDTICKET
    return f"{id_usuario}+{id_ingeniero}+{sucursal}+{id_ticket}"

# ==========================================
# 🔹 CAPA DE CONSULTAS
# ==========================================

def consultar_todo():
    """Muestra el estado completo del JSON en la consola de forma legible."""
    db = cargar_db()
    print("\n================ ESTADO DE LA DB LOCAL ================")
    print(json.dumps(db, indent=2, ensure_ascii=False))
    print("========================================================\n")

def consultar_tickets_ingeniero(id_ingeniero):
    """Muestra los tickets pendientes de un ingeniero en específico."""
    db = cargar_db()
    tickets_filtrados = [t for t in db["tickets"] if t["id_ingeniero"] == id_ingeniero and t["estado"] == "ABIERTO"]
    
    print(f"\n--- Tickets ABIERTOS para el Ingeniero: {id_ingeniero} ---")
    if not tickets_filtrados:
        print("Sin pendientes asignados.")
        return
        
    for t in tickets_filtrados:
        print(f"• Folio: {t['folio']} | Dispositivo: {t['id_dispositivo']} | Sucursal: {t['sucursal']}")

# ==========================================
# 🔹 INTERFAZ DE USUARIO LOCAL (MENÚ)
# ==========================================

def menu():
    # Detectar el nombre de la máquina simulada de forma local o poner por defecto vm1
    mi_sucursal = "vm1" 
    
    while True:
        print(f"\n--- MENÚ SUCURSAL LOCAL ({mi_sucursal}) ---")
        print("1. Consultar Base de Datos Completa")
        print("2. Consultar Tickets de un Ingeniero")
        print("3. Simulador Local: Crear Ticket (Prueba de Algoritmo)")
        print("4. Salir")
        
        opcion = input("Selecciona una opción: ")
        
        if opcion == "1":
            consultar_todo()
            
        elif opcion == "2":
            id_ing = input("Introduce el ID del Ingeniero (ej. ING01): ").strip().upper()
            consultar_tickets_ingeniero(id_ing)
            
        elif opcion == "3":
            db = cargar_db()
            id_u = input("ID Usuario (ej. USR01): ").strip().upper()
            id_d = input("ID Dispositivo (ej. DEV01): ").strip().upper()
            
            # Buscar el ingeniero ideal usando nuestro algoritmo
            ingeniero = obtener_ingeniero_menos_cargado(db, mi_sucursal)
            
            if not ingeniero:
                print("Error: No hay ingenieros registrados en el sistema.")
                continue
                
            # Calcular número correlativo de ticket
            num_ticket = f"TK{len(db['tickets']) + 1:03d}"
            
            # Generar el Folio Oficial
            folio = generar_folio_ticket(id_u, ingeniero["id"], mi_sucursal, num_ticket)
            
            # Construir estructura del ticket
            nuevo_ticket = {
                "folio": folio,
                "id_usuario": id_u,
                "id_ingeniero": ingeniero["id"],
                "id_dispositivo": id_d,
                "sucursal": mi_sucursal,
                "estado": "ABIERTO"
            }
            
            # Actualizar datos locales de prueba
            ingeniero["tickets_asignados"] += 1
            db["tickets"].append(nuevo_ticket)
            guardar_db(db)
            
            print(f"\n✅ Ticket creado exitosamente local de prueba!")
            print(f"Folio generado: {folio}")
            print(f"Asignado a: {ingeniero['nombre']} ({ingeniero['id']})")
            
        elif opcion == "4":
            print("Saliendo del programa...")
            break
        else:
            print("Opción inválida.")

if __name__ == "__main__":
    menu()