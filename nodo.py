import socket
import threading
from datetime import datetime

HOST = "0.0.0.0"
PORT = 5000
NODOS = [
    ("192.168.209.129", 5000), #vm1
    ("192.168.209.130", 5000) #vm2
]

def guardar(msg):
    with open("chat.txt", "a") as f:
        f.write(msg + "\n")

# Servidor
def recibir():
    s = socket.socket()
    s.bind((HOST, PORT))
    s.listen()

    print("Escuchando...")

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
        mensaje = f"[{timestamp}] {msg}"

        for nodo in NODOS:
            try:
                s = socket.socket()
                s.connect(nodo)
                s.send(mensaje.encode())

                resp = s.recv(1024).decode()
                print("ACK:", resp)

                s.close()
            except:
                pass

# Paralelismo
threading.Thread(target=recibir).start()
threading.Thread(target=enviar).start()