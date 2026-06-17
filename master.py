import json
import math
import os
import queue
import socket
import ssl
import threading
import time
import uuid
from datetime import datetime

try:
    import psutil
except ImportError:
    psutil = None

try:
    import certifi
except ImportError:
    certifi = None

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
WARN_CPU_PERCENT = float(os.getenv("WARN_CPU_PERCENT", "80"))
WARN_MEMORY_PERCENT = float(os.getenv("WARN_MEMORY_PERCENT", "80"))
SUPERVISOR_INTERVAL_SECONDS = float(os.getenv("SUPERVISOR_INTERVAL_SECONDS", "10"))
TCP_SOCKET_HOST = os.getenv("TCP_SOCKET_HOST", "nuted-ia.dev")
TCP_SOCKET_PORT = int(os.getenv("TCP_SOCKET_PORT", "443"))
TCP_SOCKET_TLS = os.getenv("TCP_SOCKET_TLS", "True").lower() in {"1", "true", "yes", "on"}
TCP_SOCKET_SNI = os.getenv("TCP_SOCKET_SNI", "nuted-ia.dev")


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
        self.start_time = time.time()
        self.tasks_completed_count = 0
        self.tasks_failed_count = 0
        self.workers_failed_count = 0
        self.neighbor_health = {}
        self.host_name = socket.gethostname()
        self._load_initial_tasks()
        self.monitor_thread = threading.Thread(target=self.monitor_load, daemon=True)
        self.supervisor_thread = threading.Thread(target=self.supervisor_report_loop, daemon=True)
        if psutil:
            # Warm-up avoids a misleading 0.0 on the first cpu_percent call.
            psutil.cpu_percent(interval=None)

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
        # Sprint 03 - Tarefa 02/03: ativar monitor de carga para negociação M2M.
        self.monitor_thread.start()
        self.supervisor_thread.start()

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
            if session.get("worker_uuid"):
                with self.thread_lock:
                    self.workers_failed_count += 1
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
            peer_id = payload.get("payload", {}).get("master_id", "desconhecido")
            self.log_m2m("recebido", peer_id, message_type, request_id)
            self.update_neighbor_health(peer_id, "online")
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

        with self.thread_lock:
            if status_value == "OK":
                self.tasks_completed_count += 1
            else:
                self.tasks_failed_count += 1

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

        try:
            request_uuid = uuid.UUID(str(request_id), version=4)
            if str(request_uuid) != str(request_id):
                raise ValueError
        except ValueError as exc:
            raise ValueError("request_help com request_id inválido (UUID v4 obrigatório).") from exc

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
                self.update_neighbor_health(neighbor_id, "online")

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
                    self.update_neighbor_health(neighbor_id, "online")
                    return response

                raise socket.timeout("Timeout aguardando resposta M2M")
            except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError):
                self.update_neighbor_health(neighbor_id, "offline")
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

    def update_neighbor_health(self, neighbor_id, status):
        if not neighbor_id:
            return
        with self.thread_lock:
            self.neighbor_health[neighbor_id] = {
                "status": status,
                "last_heartbeat": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            }

    def collect_system_metrics(self):
        uptime_seconds = max(0, int(time.time() - self.start_time))
        try:
            load_1m, load_5m, _ = os.getloadavg()
        except (AttributeError, OSError):
            load_1m, load_5m = 0.0, 0.0

        cpu_count_logical = psutil.cpu_count(logical=True) if psutil else os.cpu_count() or 1
        cpu_count_physical = psutil.cpu_count(logical=False) if psutil else None
        cpu_usage_percent = psutil.cpu_percent(interval=None) if psutil else min(100.0, (load_1m / max(1, cpu_count_logical)) * 100.0)

        if psutil:
            vm = psutil.virtual_memory()
            memory_total_mb = round(vm.total / (1024 * 1024), 2)
            memory_available_mb = round(vm.available / (1024 * 1024), 2)
            memory_percent_used = float(vm.percent)
            memory_used = round((vm.total - vm.available) / (1024 * 1024), 2)
            disk = psutil.disk_usage("/")
            disk_total_gb = round(disk.total / (1024 * 1024 * 1024), 2)
            disk_free_gb = round(disk.free / (1024 * 1024 * 1024), 2)
            disk_percent_used = float(disk.percent)
        else:
            memory_total_mb = 0.0
            memory_available_mb = 0.0
            memory_percent_used = 0.0
            memory_used = 0.0
            disk_total_gb = 0.0
            disk_free_gb = 0.0
            disk_percent_used = 0.0

        return {
            "uptime_seconds": uptime_seconds,
            "load_average_1m": round(load_1m, 2),
            "load_average_5m": round(load_5m, 2),
            "cpu": {
                "usage_percent": round(cpu_usage_percent, 2),
                "count_logical": int(cpu_count_logical or 0),
                "count_physical": int(cpu_count_physical) if cpu_count_physical else 0,
            },
            "memory": {
                "total_mb": memory_total_mb,
                "available_mb": memory_available_mb,
                "percent_used": memory_percent_used,
            },
            "memory_used": memory_used,
            "disk": {
                "total_gb": disk_total_gb,
                "free_gb": disk_free_gb,
                "percent_used": disk_percent_used,
            },
        }

    def collect_farm_state_metrics(self):
        now_mono = time.monotonic()
        with self.thread_lock:
            sessions = list(self.worker_sessions.values())
            borrowed_workers_snapshot = dict(self.borrowed_workers)
            lent_workers_snapshot = dict(self.lent_workers)
            tasks_completed = self.tasks_completed_count
            tasks_failed = self.tasks_failed_count
            workers_failed = self.workers_failed_count
            neighbor_health_snapshot = dict(self.neighbor_health)

        total_registered = len(sessions)
        workers_running = sum(1 for s in sessions if s.get("state") == "busy")
        workers_idle = sum(1 for s in sessions if s.get("state") == "idle")
        workers_alive = sum(
            1
            for s in sessions
            if s.get("heartbeat_ok") and (now_mono - s.get("last_heartbeat", 0.0)) <= HEARTBEAT_TTL
        )
        workers_home = sum(1 for s in sessions if not s.get("origin_server_uuid"))
        workers_available_capacity = sum(
            1
            for s in sessions
            if s.get("state") == "idle" and not s.get("origin_server_uuid")
        )

        utilization = round((workers_running / max(1, total_registered)) * 100.0, 2) if total_registered else 0.0

        borrowed_workers_list = []
        for worker_id, info in lent_workers_snapshot.items():
            borrowed_workers_list.append(
                {
                    "direction": "out",
                    "peer_uuid": info.get("target_master_id", "unknown"),
                }
            )
        for worker_id, info in borrowed_workers_snapshot.items():
            borrowed_workers_list.append(
                {
                    "direction": "in",
                    "peer_uuid": info.get("original_master_id") or info.get("original_master_address", "unknown"),
                }
            )

        neighbor_entries = []
        for neighbor_id in self.neighbor_directory:
            health = neighbor_health_snapshot.get(neighbor_id, {})
            neighbor_entries.append(
                {
                    "server_uuid": neighbor_id,
                    "status": health.get("status", "unknown"),
                    "last_heartbeat": health.get("last_heartbeat"),
                }
            )

        oldest_task_age_s = 0
        tasks_pending = self.task_queue.qsize()
        if tasks_pending > 0:
            oldest_task_age_s = max(0, int(time.time() - self.start_time))

        return {
            "workers": {
                "total_registered": total_registered,
                "workers_utilization": utilization,
                "workers_alive": workers_alive,
                "workers_idle": workers_idle,
                "workers_borrowed": len(lent_workers_snapshot),
                "workers_received": len(borrowed_workers_snapshot),
                "workers_failed": workers_failed,
                "workers_home": workers_home,
                "workers_available_capacity": workers_available_capacity,
                "borrowed_workers": borrowed_workers_list,
            },
            "tasks": {
                "tasks_pending": tasks_pending,
                "tasks_running": workers_running,
                "tasks_completed": tasks_completed,
                "tasks_failed": tasks_failed,
                "oldest_task_age_s": oldest_task_age_s,
            },
            "config_thresholds": {
                "max_task": LOAD_CAPACITY,
                "warn_cpu_percent": WARN_CPU_PERCENT,
                "warn_memory_percent": WARN_MEMORY_PERCENT,
                "release_task": RELEASE_THRESHOLD,
            },
            "neighbors": neighbor_entries,
        }

    def build_supervisor_payload(self):
        return {
            "server_uuid": self.server_uuid,
            "hostname": self.server_uuid,
            "role": "master",
            "task": "performance_report",
            "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "message_id": str(uuid.uuid4()),
            "payload_version": "sprint4-monitor",
            "performance": {
                "system": self.collect_system_metrics(),
                "farm_state": self.collect_farm_state_metrics(),
            },
        }

    def send_payload_to_supervisor(self, payload):
        body = json.dumps(payload).encode("utf-8")

        sock = socket.create_connection((TCP_SOCKET_HOST, TCP_SOCKET_PORT), timeout=SOCKET_TIMEOUT)
        try:
            if TCP_SOCKET_TLS:
                if certifi:
                    context = ssl.create_default_context(cafile=certifi.where())
                else:
                    context = ssl.create_default_context()
                tls_sock = context.wrap_socket(sock, server_hostname=TCP_SOCKET_SNI)
                try:
                    tls_sock.sendall(body)
                finally:
                    tls_sock.close()
            else:
                sock.sendall(body)
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def supervisor_report_loop(self):
        while not self.stop_event.is_set():
            try:
                payload = self.build_supervisor_payload()
                self.send_payload_to_supervisor(payload)
            except Exception as exc:
                self.log(f"Falha ao enviar performance_report para Supervisor: {exc}")
            time.sleep(SUPERVISOR_INTERVAL_SECONDS)


def main():
    MasterServer().start()


if __name__ == "__main__":
    main()
