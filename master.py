import json
import math
import os
import queue
import socket
import threading
import time
import uuid
from datetime import datetime

MASTER_HOST = os.getenv("MASTER_HOST", "0.0.0.0")
MASTER_PORT = int(os.getenv("MASTER_PORT", "5090"))
SERVER_UUID = os.getenv("MASTER_UUID", "Master A")
ACCEPT_TIMEOUT = float(os.getenv("ACCEPT_TIMEOUT", "1"))
SOCKET_TIMEOUT = float(os.getenv("SOCKET_TIMEOUT", "3"))
DEFAULT_TASK_USERS = os.getenv("TASK_USERS", "Michel,Julia")
HEARTBEAT_TTL = float(os.getenv("HEARTBEAT_TTL", "15"))
MASTER_RESPONSE_TIMEOUT = float(os.getenv("MASTER_RESPONSE_TIMEOUT", "5"))
LOAD_MONITOR_INTERVAL = float(os.getenv("LOAD_MONITOR_INTERVAL", "1"))
LOAD_CAPACITY = int(os.getenv("LOAD_CAPACITY", "1"))
RELEASE_THRESHOLD = int(os.getenv("RELEASE_THRESHOLD", "0"))
WORKER_LOAD_UNIT = int(os.getenv("WORKER_LOAD_UNIT", "1"))
NEIGHBOR_MASTERS = os.getenv("NEIGHBOR_MASTERS", "")


def normalize_host(host):
    try:
        return socket.gethostbyname(host)
    except OSError:
        return host


def normalize_address(address):
    host, port_text = address.rsplit(":", 1)
    return f"{normalize_host(host)}:{int(port_text)}"


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
        self.thread_lock = threading.RLock()
        self.task_queue = queue.Queue()
        self.worker_sessions = {}
        self.borrowed_workers = {}
        self.lent_workers = {}
        self.redirect_reservations = {}
        self.release_pending = set()
        self.neighbor_directory = self._parse_neighbor_directory(NEIGHBOR_MASTERS)
        self.neighbor_connections = {}
        self.neighbor_locks = {}
        self.help_in_progress = False
        self._load_initial_tasks()
        self.monitor_thread = threading.Thread(target=self.monitor_load, daemon=True)

    def log(self, message):
        print(f"[MASTER {self.server_uuid}] {message}")

    def log_m2m(self, direction, peer_id, message_type, request_id):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log(
            f"{timestamp} {direction} M2M peer={peer_id} type={message_type} request_id={request_id}"
        )

    def _load_initial_tasks(self):
        for raw_user in DEFAULT_TASK_USERS.split(","):
            user_name = raw_user.strip()
            if user_name:
                self.task_queue.put({"TASK": "QUERY", "USER": user_name})

    def _parse_neighbor_directory(self, raw_neighbors):
        directory = {}
        for chunk in raw_neighbors.split(","):
            item = chunk.strip()
            if not item or "@" not in item or ":" not in item:
                continue

            master_id, address = item.split("@", 1)
            host, port_str = address.rsplit(":", 1)
            try:
                normalized_host = normalize_host(host.strip())
                normalized_port = int(port_str.strip())
                directory[master_id.strip()] = {
                    "host": normalized_host,
                    "port": normalized_port,
                    "address": f"{normalized_host}:{normalized_port}",
                }
            except ValueError:
                continue
        return directory

    def start(self):
        self.server.bind((self.host, self.port))
        self.server.listen()
        self.log(f"Servidor escutando em {self.host}:{self.port}")
        self.monitor_thread.start()

        try:
            while not self.stop_event.is_set():
                try:
                    conn, addr = self.server.accept()
                except socket.timeout:
                    continue

                conn.settimeout(SOCKET_TIMEOUT)
                self.log(f"Conexão recebida: {addr[0]}:{addr[1]}")
                thread = threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True)
                thread.start()
                with self.thread_lock:
                    self.client_threads.append(thread)
        except KeyboardInterrupt:
            self.log("Encerrando master...")
        finally:
            self.stop_event.set()
            self.server.close()
            self._close_all_neighbor_connections()

    def monitor_load(self):
        while not self.stop_event.is_set():
            try:
                current_load = self.task_queue.qsize()
                if current_load > LOAD_CAPACITY:
                    self.request_help_if_needed(current_load)
                elif current_load <= RELEASE_THRESHOLD:
                    self.schedule_borrowed_worker_release()
            except Exception as exc:
                self.log(f"Falha no monitor de carga: {exc}")
            time.sleep(LOAD_MONITOR_INTERVAL)

    def request_help_if_needed(self, current_load):
        if not self.neighbor_directory:
            return

        with self.thread_lock:
            if self.help_in_progress:
                return
            self.help_in_progress = True

        try:
            workers_needed = max(1, math.ceil((current_load - LOAD_CAPACITY) / max(1, WORKER_LOAD_UNIT)))
            for neighbor_id in self.neighbor_directory:
                if workers_needed <= 0:
                    break

                response = self.send_help_request(neighbor_id, current_load, workers_needed)
                if not response:
                    continue

                if response.get("type") == "response_accepted":
                    workers_needed -= int(response.get("payload", {}).get("workers_offered", 0))
        finally:
            with self.thread_lock:
                self.help_in_progress = False

    def send_help_request(self, neighbor_id, current_load, workers_needed):
        request_id = str(uuid.uuid4())
        payload = {
            "type": "request_help",
            "request_id": request_id,
            "payload": {
                "master_id": self.server_uuid,
                "current_load": current_load,
                "capacity": LOAD_CAPACITY,
                "workers_needed": workers_needed,
            },
        }

        try:
            return self.send_master_message(neighbor_id, payload, expect_response=True)
        except socket.timeout:
            self.log_m2m("timeout", neighbor_id, "request_help", request_id)
            return None
        except OSError as exc:
            self.log(f"Falha ao negociar com {neighbor_id}: {exc}")
            return None

    def schedule_borrowed_worker_release(self):
        with self.thread_lock:
            for worker_id in self.borrowed_workers:
                self.release_pending.add(worker_id)

    def handle_client(self, conn, addr):
        buffer = ""
        session = {
            "role": "unknown",
            "worker_uuid": None,
            "origin_server_uuid": None,
            "original_master_address": None,
            "last_task": None,
            "heartbeat_ok": False,
            "last_heartbeat": 0.0,
            "state": "idle",
            "conn": conn,
            "addr": addr,
        }

        try:
            while not self.stop_event.is_set():
                try:
                    chunk = conn.recv(1024)
                    if not chunk:
                        self.log(f"Conexão encerrada: {addr[0]}:{addr[1]}")
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
            self.unregister_worker_session(session)
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

        message_type = payload.get("type")
        if isinstance(message_type, str):
            self.process_typed_message(conn, payload, session)
            return

        if self.is_heartbeat(payload):
            self.handle_heartbeat(conn, payload, session)
            return

        if self.is_presentation(payload):
            self.handle_presentation(conn, payload, session)
            return

        if self.is_status_report(payload):
            self.handle_status_report(conn, payload, session)
            return

        self.log(f"Payload ignorado por não seguir o protocolo conhecido: {payload}")

    def process_typed_message(self, conn, payload, session):
        message_type = payload.get("type")
        request_id = payload.get("request_id", "")

        if message_type == "request_help":
            session["role"] = "master"
            self.log_m2m("recebido", payload.get("payload", {}).get("master_id", "desconhecido"), message_type, request_id)
            self.handle_request_help(conn, payload)
            return

        if message_type == "notify_worker_returned":
            session["role"] = "master"
            self.log_m2m("recebido", "vizinho", message_type, request_id)
            self.handle_notify_worker_returned(payload)
            return

        if message_type == "register_temporary_worker":
            session["role"] = "worker"
            self.handle_register_temporary_worker(payload, session)
            return

        self.log(f"Mensagem com type desconhecido ignorada: {payload}")

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
            raise ValueError(f"HEARTBEAT destinado a outro servidor: {server_uuid}.")

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

    def handle_register_temporary_worker(self, payload, session):
        request_id = payload.get("request_id", "")
        register_payload = payload.get("payload", {})
        worker_id = register_payload.get("worker_id")
        original_master_address = register_payload.get("original_master_address")

        if not worker_id or not original_master_address:
            raise ValueError("register_temporary_worker sem campos obrigatórios.")

        original_master_id = self.find_neighbor_id_by_address(original_master_address)
        session["worker_uuid"] = worker_id
        session["original_master_address"] = original_master_address
        session["origin_server_uuid"] = original_master_id
        session["state"] = "idle"

        with self.thread_lock:
            self.borrowed_workers[worker_id] = {
                "original_master_address": original_master_address,
                "original_master_id": original_master_id,
            }

        self.log_m2m("recebido", original_master_id or original_master_address, "register_temporary_worker", request_id)
        self.log(f"Worker temporário registrado: {worker_id} (origem: {original_master_address})")
        self.log_worker_state()

    def handle_presentation(self, conn, payload, session):
        if not self.heartbeat_is_valid(session):
            raise ValueError("Task queue exige heartbeat válido antes da apresentação do worker.")

        worker_uuid = payload.get("WORKER_UUID")
        if not worker_uuid:
            raise ValueError("Apresentação sem WORKER_UUID.")

        origin_server_uuid = payload.get("SERVER_UUID") or session.get("origin_server_uuid")
        borrowed = worker_uuid in self.borrowed_workers or bool(origin_server_uuid)

        session["worker_uuid"] = worker_uuid
        session["origin_server_uuid"] = origin_server_uuid
        session["state"] = "idle"
        session["conn"] = conn
        session["addr"] = session.get("addr") or conn.getpeername()

        with self.thread_lock:
            self.worker_sessions[worker_uuid] = session

        if borrowed:
            self.log(f"Worker emprestado apresentado: {worker_uuid} (origem: {origin_server_uuid})")
        else:
            self.log(f"Worker local apresentado: {worker_uuid}")

        if borrowed and worker_uuid in self.release_pending:
            self.send_release_command(conn, worker_uuid)
            return

        if not borrowed and worker_uuid in self.redirect_reservations:
            self.send_redirect_command(conn, worker_uuid)
            return

        try:
            task_payload = self.task_queue.get_nowait()
            session["last_task"] = task_payload
            session["state"] = "busy"
        except queue.Empty:
            task_payload = {"TASK": "NO_TASK"}
            session["last_task"] = None

        self.send_json(conn, task_payload)
        self.log(f"Tarefa enviada para {worker_uuid}: {task_payload}")
        self.log_worker_state()

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
        session["state"] = "idle"
        self.log_worker_state()

    def handle_request_help(self, conn, payload):
        request_id = payload.get("request_id")
        body = payload.get("payload", {})
        requester_id = body.get("master_id")
        current_load = body.get("current_load")
        capacity = body.get("capacity")
        workers_needed = body.get("workers_needed")

        if not request_id or requester_id is None or current_load is None or capacity is None or workers_needed is None:
            raise ValueError("request_help sem campos obrigatórios.")

        if self.task_queue.qsize() > LOAD_CAPACITY:
            response = self.build_rejected_response(request_id, "high_load")
            self.send_json(conn, response)
            self.log_m2m("enviado", requester_id, response["type"], request_id)
            return

        available_workers = self.select_idle_local_workers(int(workers_needed))
        if not available_workers:
            response = self.build_rejected_response(request_id, "no_workers_available")
            self.send_json(conn, response)
            self.log_m2m("enviado", requester_id, response["type"], request_id)
            return

        neighbor_data = self.neighbor_directory.get(requester_id)
        if not neighbor_data:
            response = self.build_rejected_response(request_id, "refused")
            self.send_json(conn, response)
            self.log_m2m("enviado", requester_id, response["type"], request_id)
            return

        with self.thread_lock:
            for worker_id in available_workers:
                self.redirect_reservations[worker_id] = {
                    "target_master_id": requester_id,
                    "target_address": neighbor_data["address"],
                }

        response = {
            "type": "response_accepted",
            "request_id": request_id,
            "payload": {
                "workers_offered": len(available_workers),
                "worker_details": [
                    {"id": worker_id, "address": self.format_worker_address(worker_id)}
                    for worker_id in available_workers
                ],
            },
        }
        self.send_json(conn, response)
        self.log_m2m("enviado", requester_id, response["type"], request_id)
        self.log_worker_state()

    def handle_notify_worker_returned(self, payload):
        request_id = payload.get("request_id")
        body = payload.get("payload", {})
        worker_id = body.get("worker_id")

        if not request_id or not worker_id:
            raise ValueError("notify_worker_returned sem campos obrigatórios.")

        with self.thread_lock:
            self.lent_workers.pop(worker_id, None)
            self.redirect_reservations.pop(worker_id, None)

        self.log(f"Master vizinho notificou devolução do worker {worker_id}")
        self.log_worker_state()

    def select_idle_local_workers(self, workers_needed):
        available = []
        with self.thread_lock:
            for worker_id, session in self.worker_sessions.items():
                if session.get("origin_server_uuid"):
                    continue
                if session.get("state") != "idle":
                    continue
                if worker_id in self.redirect_reservations or worker_id in self.lent_workers:
                    continue
                available.append(worker_id)
                if len(available) >= workers_needed:
                    break
        return available

    def send_redirect_command(self, conn, worker_id):
        with self.thread_lock:
            reservation = self.redirect_reservations.pop(worker_id, None)
            if not reservation:
                return
            self.lent_workers[worker_id] = reservation

        payload = {
            "type": "command_redirect",
            "request_id": str(uuid.uuid4()),
            "payload": {
                "new_master_address": reservation["target_address"],
                "new_master_uuid": reservation["target_master_id"],
            },
        }
        self.send_json(conn, payload)
        self.log(f"command_redirect enviado para {worker_id}: {reservation['target_address']}")
        self.log_worker_state()

    def send_release_command(self, conn, worker_id):
        with self.thread_lock:
            borrowed_info = self.borrowed_workers.get(worker_id)
        if not borrowed_info:
            return

        payload = {
            "type": "command_release",
            "request_id": str(uuid.uuid4()),
            "payload": {
                "original_master_address": borrowed_info["original_master_address"],
                "original_master_uuid": borrowed_info.get("original_master_id"),
            },
        }
        self.send_json(conn, payload)
        self.log(f"command_release enviado para {worker_id}: {borrowed_info['original_master_address']}")
        self.notify_worker_returned(worker_id, borrowed_info)

    def notify_worker_returned(self, worker_id, borrowed_info):
        original_master_id = borrowed_info.get("original_master_id")
        if original_master_id:
            payload = {
                "type": "notify_worker_returned",
                "request_id": str(uuid.uuid4()),
                "payload": {
                    "worker_id": worker_id,
                },
            }
            try:
                self.send_master_message(original_master_id, payload, expect_response=False)
            except OSError as exc:
                self.log(f"Falha ao notificar devolução do worker {worker_id}: {exc}")

        with self.thread_lock:
            self.release_pending.discard(worker_id)
            self.borrowed_workers.pop(worker_id, None)
            self.worker_sessions.pop(worker_id, None)

        self.log(f"notify_worker_returned processado para {worker_id}")
        self.log_worker_state()

    def unregister_worker_session(self, session):
        worker_id = session.get("worker_uuid")
        if not worker_id:
            return

        with self.thread_lock:
            current = self.worker_sessions.get(worker_id)
            if current is session:
                self.worker_sessions.pop(worker_id, None)

            if worker_id in self.borrowed_workers:
                self.release_pending.add(worker_id)

        self.log_worker_state()

    def build_rejected_response(self, request_id, reason):
        return {
            "type": "response_rejected",
            "request_id": request_id,
            "payload": {
                "reason": reason,
            },
        }

    def format_worker_address(self, worker_id):
        with self.thread_lock:
            session = self.worker_sessions.get(worker_id)
        if not session:
            return "unknown"
        addr = session.get("addr")
        if not addr:
            return "unknown"
        return f"{addr[0]}:{addr[1]}"

    def find_neighbor_id_by_address(self, address):
        normalized_address = normalize_address(address)
        for master_id, data in self.neighbor_directory.items():
            if data["address"] == normalized_address:
                return master_id
        return None

    def log_worker_state(self):
        with self.thread_lock:
            local_workers = sum(1 for session in self.worker_sessions.values() if not session.get("origin_server_uuid"))
            borrowed_workers = len(self.borrowed_workers)
        self.log(f"Estado da farm: locais={local_workers} emprestados={borrowed_workers}")

    def _close_all_neighbor_connections(self):
        with self.thread_lock:
            neighbor_ids = list(self.neighbor_connections)
        for neighbor_id in neighbor_ids:
            self.close_neighbor_connection(neighbor_id)

    def close_neighbor_connection(self, neighbor_id):
        with self.thread_lock:
            info = self.neighbor_connections.pop(neighbor_id, None)
        if not info:
            return
        try:
            info["sock"].close()
        except OSError:
            pass

    def get_neighbor_lock(self, neighbor_id):
        with self.thread_lock:
            lock = self.neighbor_locks.get(neighbor_id)
            if lock is None:
                lock = threading.Lock()
                self.neighbor_locks[neighbor_id] = lock
            return lock

    def get_neighbor_connection(self, neighbor_id):
        with self.thread_lock:
            info = self.neighbor_connections.get(neighbor_id)
            if info:
                return info

        neighbor = self.neighbor_directory.get(neighbor_id)
        if not neighbor:
            raise OSError(f"Master vizinho desconhecido: {neighbor_id}")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(MASTER_RESPONSE_TIMEOUT)
        sock.connect((neighbor["host"], neighbor["port"]))
        sock.settimeout(SOCKET_TIMEOUT)

        info = {"sock": sock, "buffer": ""}
        with self.thread_lock:
            self.neighbor_connections[neighbor_id] = info
        return info

    def send_master_message(self, neighbor_id, payload, expect_response):
        lock = self.get_neighbor_lock(neighbor_id)
        with lock:
            info = self.get_neighbor_connection(neighbor_id)
            sock = info["sock"]
            request_id = payload.get("request_id", "")
            message_type = payload.get("type", "")

            try:
                self.send_json(sock, payload)
                self.log_m2m("enviado", neighbor_id, message_type, request_id)

                if not expect_response:
                    return None

                deadline = time.monotonic() + MASTER_RESPONSE_TIMEOUT
                while time.monotonic() < deadline:
                    sock.settimeout(max(0.1, deadline - time.monotonic()))
                    response, info["buffer"] = self.recv_json(sock, info["buffer"])
                    if response.get("request_id") != request_id:
                        self.log(f"Resposta M2M ignorada por request_id divergente: {response}")
                        continue

                    self.log_m2m("recebido", neighbor_id, response.get("type", ""), request_id)
                    return response

                raise socket.timeout("Timeout aguardando resposta M2M")
            except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError):
                self.close_neighbor_connection(neighbor_id)
                raise
            finally:
                try:
                    sock.settimeout(SOCKET_TIMEOUT)
                except OSError:
                    pass

    def send_json(self, conn, payload):
        message = (json.dumps(payload) + "\n").encode("utf-8")
        total_sent = 0

        while total_sent < len(message):
            sent = conn.send(message[total_sent:])
            if sent == 0:
                raise ConnectionResetError("Socket fechado durante o envio.")
            total_sent += sent

    def recv_json(self, conn, buffer):
        while "\n" not in buffer:
            chunk = conn.recv(1024)
            if not chunk:
                raise ConnectionResetError("Conexão encerrada durante a leitura.")
            buffer += chunk.decode("utf-8")

        raw_message, buffer = buffer.split("\n", 1)
        message = raw_message.strip()
        if not message:
            return self.recv_json(conn, buffer)
        return json.loads(message), buffer


def main():
    MasterServer().start()


if __name__ == "__main__":
    main()
