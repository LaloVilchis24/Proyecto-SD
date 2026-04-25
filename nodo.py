import socket
import threading
import json
from datetime import datetime

PORT = 5000

# 🔹 cargar configuración
with open("config.json") as f:
    config = json.load(f)

MI_NOMBRE = config["mi_nombre"]
NODOS = [(n["host"], n["port"]) for n in config["nodos"]]

def guardar(msg):
    with open("chat.txt", "a") as f:
        f.write(msg + "\n")

def recibir():
    try:
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", PORT))
        s.listen()
        print(f"{MI_NOMBRE} escuchando en puerto {PORT}...")
    except Exception as e:
        print("Error al iniciar servidor:", e)
        return

    while True:
        try:
            conn, addr = s.accept()
            mensaje = conn.recv(1024).decode()

            print("\nRecibido:", mensaje)
            guardar(mensaje)

            conn.send("RECIBIDO".encode())
            conn.close()
        except Exception as e:
            print("Error al recibir:", e)

def enviar():
    while True:
        msg = input("Mensaje: ")
        timestamp = datetime.now().strftime("%H:%M:%S")
        mensaje = f"[{MI_NOMBRE} {timestamp}] {msg}"

        for host, port in NODOS:
            if host == MI_NOMBRE:
                continue

            try:
                s = socket.socket()
                s.connect((host, port))
                s.send(mensaje.encode())

                resp = s.recv(1024).decode()
                print(f"ACK de {host}:", resp)

                s.close()
            except Exception as e:
                print(f"No se pudo conectar a {host}: {e}")

threading.Thread(target=recibir, daemon=True).start()
threading.Thread(target=enviar, daemon=True).start()

while True:
    pass