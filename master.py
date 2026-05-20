import json
import os
import queue
import socket
import threading
import time

MASTER_HOST = os.getenv("MASTER_HOST", "0.0.0.0")
MASTER_PORT = int(os.getenv("MASTER_PORT", "5000"))
SERVER_UUID = os.getenv("MASTER_UUID", "Master A")
ACCEPT_TIMEOUT = float(os.getenv("ACCEPT_TIMEOUT", "1"))
SOCKET_TIMEOUT = float(os.getenv("SOCKET_TIMEOUT", "3"))
DEFAULT_TASK_USERS = os.getenv("TASK_USERS", "Michel,Julia")
HEARTBEAT_TTL = float(os.getenv("HEARTBEAT_TTL", "15"))


class MasterServer:
    def __init__(self, host=MASTER_HOST, port=MASTER_PORT, server_uuid=SERVER_UUID):
        self.host = host
        self.port = port
        self.server_uuid = server_uuid
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.settimeout(ACCEPT_TIMEOUT)
        self.stop_event = threading.Event()
        self.client_threads = []
        self.thread_lock = threading.Lock()
        self.task_queue = queue.Queue()
        self._load_initial_tasks()

    def log(self, message):
        print(f"[MASTER {self.server_uuid}] {message}")

    def _load_initial_tasks(self):
        for raw_user in DEFAULT_TASK_USERS.split(","):
            user_name = raw_user.strip()
            if user_name:
                self.task_queue.put({"TASK": "QUERY", "USER": user_name})

    def start(self):
        self.server.bind((self.host, self.port))
        self.server.listen()
        self.log(f"Servidor escutando em {self.host}:{self.port}")

        try:
            while not self.stop_event.is_set():
                try:
                    conn, addr = self.server.accept()
                except socket.timeout:
                    continue

                conn.settimeout(SOCKET_TIMEOUT)
                self.log(f"Worker conectado: {addr[0]}:{addr[1]}")
                thread = threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True)
                thread.start()
                with self.thread_lock:
                    self.client_threads.append(thread)
        except KeyboardInterrupt:
            self.log("Encerrando master...")
        finally:
            self.stop_event.set()
            self.server.close()

    def handle_client(self, conn, addr):
        buffer = ""
        session = {
            "worker_uuid": None,
            "origin_server_uuid": None,
            "last_task": None,
            "heartbeat_ok": False,
            "last_heartbeat": 0.0,
        }

        try:
            while not self.stop_event.is_set():
                try:
                    chunk = conn.recv(1024)
                    if not chunk:
                        self.log(f"Worker desconectado: {addr[0]}:{addr[1]}")
                        break

                    buffer += chunk.decode("utf-8")
                    while "\n" in buffer:
                        raw_message, buffer = buffer.split("\n", 1)
                        message = raw_message.strip()
                        if not message:
                            continue
                        try:
                            self.process_message(conn, addr, message, session)
                        except ValueError as exc:
                            self.log(f"Payload inválido de {addr[0]}:{addr[1]}: {exc}")
                except socket.timeout:
                    continue
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError) as exc:
            self.log(f"Falha de comunicação com {addr[0]}:{addr[1]}: {exc}")
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def process_message(self, conn, addr, message, session):
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            self.log(f"JSON inválido recebido de {addr[0]}:{addr[1]}: {message}")
            return

        self.log(f"Recebido de {addr[0]}:{addr[1]}: {payload}")

        if self.is_heartbeat(payload):
            self.handle_heartbeat(conn, payload, session)
            return

        if self.is_presentation(payload):
            self.handle_presentation(conn, payload, session)
            return

        if self.is_status_report(payload):
            self.handle_status_report(conn, payload, session)
            return

        self.log(f"Payload ignorado por não seguir o protocolo da sprint 2: {payload}")

    def is_presentation(self, payload):
        return payload.get("WORKER") == "ALIVE"

    def is_heartbeat(self, payload):
        return str(payload.get("TASK", "")).upper() == "HEARTBEAT"

    def is_status_report(self, payload):
        return "STATUS" in payload and "TASK" in payload and "WORKER_UUID" in payload

    def handle_heartbeat(self, conn, payload, session):
        server_uuid = payload.get("SERVER_UUID")
        if not server_uuid:
            raise ValueError("HEARTBEAT sem SERVER_UUID.")
        if server_uuid != self.server_uuid:
            raise ValueError(
                f"HEARTBEAT destinado a outro servidor: {server_uuid}."
            )

        session["heartbeat_ok"] = True
        session["last_heartbeat"] = time.monotonic()

        response = {
            "SERVER_UUID": self.server_uuid,
            "TASK": "HEARTBEAT",
            "RESPONSE": "ALIVE",
        }
        self.send_json(conn, response)
        self.log(f"Heartbeat respondido: {response}")

    def heartbeat_is_valid(self, session):
        if not session.get("heartbeat_ok"):
            return False
        return (time.monotonic() - session.get("last_heartbeat", 0.0)) <= HEARTBEAT_TTL

    def handle_presentation(self, conn, payload, session):
        if not self.heartbeat_is_valid(session):
            raise ValueError("Task queue exige heartbeat válido antes da apresentação do worker.")

        worker_uuid = payload.get("WORKER_UUID")
        if not worker_uuid:
            raise ValueError("Apresentação sem WORKER_UUID.")

        origin_server_uuid = payload.get("SERVER_UUID")
        session["worker_uuid"] = worker_uuid
        session["origin_server_uuid"] = origin_server_uuid
        session["last_task"] = None

        if origin_server_uuid:
            self.log(f"Worker emprestado apresentado: {worker_uuid} (origem: {origin_server_uuid})")
        else:
            self.log(f"Worker local apresentado: {worker_uuid}")

        try:
            task_payload = self.task_queue.get_nowait()
            session["last_task"] = task_payload
        except queue.Empty:
            task_payload = {"TASK": "NO_TASK"}

        self.send_json(conn, task_payload)
        self.log(f"Tarefa enviada para {worker_uuid}: {task_payload}")

    def handle_status_report(self, conn, payload, session):
        if not self.heartbeat_is_valid(session):
            raise ValueError("Status recebido sem heartbeat válido na sessão.")

        worker_uuid = payload.get("WORKER_UUID")
        task_name = str(payload.get("TASK", "")).upper()
        status_value = str(payload.get("STATUS", "")).upper()

        if not worker_uuid or not task_name or not status_value:
            raise ValueError("Reporte de status sem campos obrigatórios.")
        if task_name != "QUERY":
            raise ValueError(f"TASK inválida no reporte de status: {payload}")
        if status_value not in {"OK", "NOK"}:
            raise ValueError(f"STATUS inválido no reporte de status: {payload}")
        if session.get("worker_uuid") != worker_uuid:
            raise ValueError(f"WORKER_UUID divergente na sessão: {payload}")

        task_payload = session.get("last_task") or {"TASK": task_name, "USER": "desconhecido"}
        task_user = task_payload.get("USER", "desconhecido")
        origin_server_uuid = session.get("origin_server_uuid")
        worker_origin = f"emprestado de {origin_server_uuid}" if origin_server_uuid else "local"

        self.log(
            f"Tarefa concluída por worker {worker_uuid} ({worker_origin}) para o usuário {task_user}: {status_value}"
        )

        ack_payload = {
            "STATUS": "ACK",
            "WORKER_UUID": worker_uuid,
        }
        self.send_json(conn, ack_payload)
        self.log(f"ACK enviado para {worker_uuid}: {ack_payload}")
        session["last_task"] = None

    def send_json(self, conn, payload):
        message = (json.dumps(payload) + "\n").encode("utf-8")
        total_sent = 0

        while total_sent < len(message):
            sent = conn.send(message[total_sent:])
            if sent == 0:
                raise ConnectionResetError("Socket fechado durante o envio.")
            total_sent += sent


def main():
    MasterServer().start()


if __name__ == "__main__":
    main()
