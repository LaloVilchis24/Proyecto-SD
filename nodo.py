import socket
import threading
import json
from datetime import datetime
import os

PORT = 5000

N_HOST = socket.gethostname()

with open("config.json") as f:
    config = json.load(f)

NODOS = [(n["host"], n["port"]) for n in config["nodos"]]

# Guardado de Mensajes
def guardar(msg):
    with open("chat.txt", "a") as f:
        f.write(msg + "\n")

# Servidor
def recibir():
    s = socket.socket()
    s.bind(("0.0.0.0", PORT))
    s.listen()

    print(f"{N_HOST} escuchando...")

    while True:
        conn, addr = s.accept()
        mensaje = conn.recv(1024).decode()

        print("\nRecibido:", mensaje)
        guardar(mensaje)

        conn.send("RECIBIDO".encode())
        conn.close()

# Cliente
def enviar():
    while True:
        msg = input("Mensaje: ")
        timestamp = datetime.now().strftime("%H:%M:%S")
        mensaje = f"[{N_HOST}{timestamp}] {msg}"

        for host, port in NODOS:
            if host == N_HOST:
                continue
            
            try:
                s = socket.socket()
                s.connect((host,port))
                s.send(mensaje.encode())
                resp = s.recv(1024).decode()
                print(f"ACK de {N_HOST}:", resp)
                s.close()   
            except:
                print(f"No se pudo conectar a {host}")

# Paralelismo
threading.Thread(target=recibir).start()
threading.Thread(target=enviar).start()