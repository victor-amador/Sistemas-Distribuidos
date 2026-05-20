import json
import os
import random
import socket
import time
import uuid

MASTER_HOST = os.getenv("MASTER_HOST", "localhost")
MASTER_PORT = int(os.getenv("MASTER_PORT", "5000"))
MASTER_UUID = os.getenv("MASTER_UUID", "Master A")
WORKER_UUID = os.getenv("WORKER_UUID", f"W-{uuid.uuid4().hex[:6].upper()}")
ORIGINAL_SERVER_UUID = os.getenv("ORIGINAL_SERVER_UUID", "")
WORKER_MODE = os.getenv("WORKER_MODE", "TASKS").upper()
SOCKET_TIMEOUT = float(os.getenv("SOCKET_TIMEOUT", "5"))
RECONNECT_DELAY = float(os.getenv("RECONNECT_DELAY", "2"))
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "10"))
PROCESSING_MIN = float(os.getenv("PROCESSING_MIN", "1"))
PROCESSING_MAX = float(os.getenv("PROCESSING_MAX", "3"))
FORCE_STATUS = os.getenv("FORCE_STATUS", "").upper()


def normalize_host(host):
    try:
        return socket.gethostbyname(host)
    except OSError:
        return host


def normalize_address(host, port):
    return f"{normalize_host(host)}:{port}"


def log(message):
    print(f"[WORKER {WORKER_UUID}] {message}")


def log_master_host_hint(host):
    if host in {"localhost", "127.0.0.1"}:
        log(
            "MASTER_HOST esta em localhost/127.0.0.1. Se o Master estiver em outro PC, configure o IP real da maquina do Master."
        )


def send_json(sock, payload):
    message = (json.dumps(payload) + "\n").encode("utf-8")
    total_sent = 0

    while total_sent < len(message):
        sent = sock.send(message[total_sent:])
        if sent == 0:
            raise ConnectionResetError("Socket fechado durante o envio.")
        total_sent += sent


def recv_json(sock, buffer):
    while "\n" not in buffer:
        chunk = sock.recv(1024)
        if not chunk:
            raise ConnectionResetError("Master encerrou a conexão.")
        buffer += chunk.decode("utf-8")

    raw_message, buffer = buffer.split("\n", 1)
    message = raw_message.strip()
    if not message:
        return recv_json(sock, buffer)
    return json.loads(message), buffer


def parse_address(address):
    host, port_text = address.rsplit(":", 1)
    return host, int(port_text)


def build_presentation_payload(state):
    payload = {
        "WORKER": "ALIVE",
        "WORKER_UUID": WORKER_UUID,
    }

    if state["borrowed"]:
        payload["SERVER_UUID"] = state["original_master_uuid"]
    elif ORIGINAL_SERVER_UUID and ORIGINAL_SERVER_UUID != state["current_master_uuid"]:
        payload["SERVER_UUID"] = ORIGINAL_SERVER_UUID

    return payload


def build_heartbeat_payload(state):
    return {
        "SERVER_UUID": state["current_master_uuid"],
        "TASK": "HEARTBEAT",
    }


def build_register_temporary_worker_payload(state):
    return {
        "type": "register_temporary_worker",
        "request_id": str(uuid.uuid4()),
        "payload": {
            "worker_id": WORKER_UUID,
            "original_master_address": state["original_master_address"],
        },
    }


def get_connected_master_ip(sock):
    return sock.getpeername()[0]


def process_query(task_payload):
    user_name = task_payload.get("USER", "desconhecido")
    processing_time = random.uniform(PROCESSING_MIN, PROCESSING_MAX)
    log(f"Processando tarefa QUERY para o usuário {user_name} por {processing_time:.2f}s")
    time.sleep(processing_time)

    if FORCE_STATUS in {"OK", "NOK"}:
        return FORCE_STATUS

    return random.choice(["OK", "NOK"])


def validate_task_payload(payload):
    task_name = str(payload.get("TASK", "")).upper()
    if not task_name:
        raise ValueError(f"Resposta sem campo TASK: {payload}")

    if task_name == "NO_TASK":
        return "NO_TASK"

    if task_name != "QUERY":
        raise ValueError(f"Tarefa inválida recebida do Master: {payload}")

    if "USER" not in payload:
        raise ValueError(f"Payload QUERY sem campo USER: {payload}")

    return "QUERY"


def validate_ack_payload(payload):
    if payload.get("STATUS") != "ACK":
        raise ValueError(f"ACK inválido recebido do Master: {payload}")
    if payload.get("WORKER_UUID") != WORKER_UUID:
        raise ValueError(f"ACK recebido para outro worker: {payload}")


def validate_heartbeat_payload(payload, expected_master_uuid):
    if payload.get("SERVER_UUID") != expected_master_uuid:
        raise ValueError(f"Resposta HEARTBEAT de servidor inesperado: {payload}")
    if str(payload.get("TASK", "")).upper() != "HEARTBEAT":
        raise ValueError(f"Resposta HEARTBEAT inválida: {payload}")
    if payload.get("RESPONSE") != "ALIVE":
        raise ValueError(f"Master não respondeu ALIVE ao HEARTBEAT: {payload}")


def handle_command_redirect(payload, state):
    command_payload = payload.get("payload", {})
    new_master_address = command_payload.get("new_master_address")
    if not new_master_address:
        raise ValueError(f"command_redirect sem new_master_address: {payload}")

    new_master_uuid = command_payload.get("new_master_uuid")
    new_host, new_port = parse_address(new_master_address)
    state["current_master_host"] = new_host
    state["current_master_port"] = new_port
    if new_master_uuid:
        state["current_master_uuid"] = new_master_uuid
    state["borrowed"] = True
    log(f"command_redirect recebido. Novo master: {new_master_address}")


def handle_command_release(payload, state):
    command_payload = payload.get("payload", {})
    original_master_address = command_payload.get("original_master_address")
    if not original_master_address:
        raise ValueError(f"command_release sem original_master_address: {payload}")

    original_master_uuid = command_payload.get("original_master_uuid")
    original_host, original_port = parse_address(original_master_address)
    state["current_master_host"] = original_host
    state["current_master_port"] = original_port
    state["current_master_uuid"] = original_master_uuid or state["original_master_uuid"]
    state["borrowed"] = False
    log(f"command_release recebido. Retornando para {original_master_address}")


def run_heartbeat_cycle(sock, buffer, state):
    while True:
        heartbeat_payload = build_heartbeat_payload(state)
        send_json(sock, heartbeat_payload)
        log(f"Heartbeat enviado: {heartbeat_payload}")

        response_payload, buffer = recv_json(sock, buffer)
        validate_heartbeat_payload(response_payload, state["current_master_uuid"])
        log(f"Heartbeat confirmado: {response_payload}")
        time.sleep(HEARTBEAT_INTERVAL)


def run_task_cycle(sock, buffer, state):
    temporary_registered = False

    while True:
        heartbeat_payload = build_heartbeat_payload(state)
        send_json(sock, heartbeat_payload)
        log(f"Heartbeat enviado antes da fila de tarefas: {heartbeat_payload}")

        heartbeat_response, buffer = recv_json(sock, buffer)
        validate_heartbeat_payload(heartbeat_response, state["current_master_uuid"])
        log(f"Heartbeat confirmado antes da fila de tarefas: {heartbeat_response}")

        if state["borrowed"] and not temporary_registered:
            register_payload = build_register_temporary_worker_payload(state)
            send_json(sock, register_payload)
            log(f"Registro temporário enviado: {register_payload}")
            temporary_registered = True

        presentation_payload = build_presentation_payload(state)
        send_json(sock, presentation_payload)
        log(f"Apresentação enviada: {presentation_payload}")

        response_payload, buffer = recv_json(sock, buffer)
        log(f"Resposta do Master: {response_payload}")

        if response_payload.get("type") == "command_redirect":
            handle_command_redirect(response_payload, state)
            return

        if response_payload.get("type") == "command_release":
            handle_command_release(response_payload, state)
            return

        task_kind = validate_task_payload(response_payload)
        if task_kind == "NO_TASK":
            log("Master informou que não há tarefa disponível.")
            time.sleep(RECONNECT_DELAY)
            continue

        status_payload = {
            "STATUS": process_query(response_payload),
            "TASK": "QUERY",
            "WORKER_UUID": WORKER_UUID,
        }
        send_json(sock, status_payload)
        log(f"Status enviado: {status_payload}")

        ack_payload, buffer = recv_json(sock, buffer)
        validate_ack_payload(ack_payload)
        log(f"ACK confirmado: {ack_payload}")
        time.sleep(RECONNECT_DELAY)


def initial_worker_state():
    normalized_master_host = normalize_host(MASTER_HOST)
    return {
        "current_master_host": MASTER_HOST,
        "current_master_port": MASTER_PORT,
        "current_master_uuid": MASTER_UUID,
        "original_master_host": MASTER_HOST,
        "original_master_port": MASTER_PORT,
        "original_master_uuid": MASTER_UUID,
        "original_master_address": normalize_address(normalized_master_host, MASTER_PORT),
        "borrowed": False,
    }


def run_worker():
    state = initial_worker_state()
    host_hint_logged = False

    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(SOCKET_TIMEOUT)
        buffer = ""

        try:
            if not host_hint_logged:
                log_master_host_hint(state["current_master_host"])
                host_hint_logged = True

            log(
                f"Conectando ao Master {state['current_master_uuid']} em {state['current_master_host']}:{state['current_master_port']}..."
            )
            sock.connect((state["current_master_host"], state["current_master_port"]))
            log(f"Conexão estabelecida com o IP do Master {get_connected_master_ip(sock)}.")
            host_hint_logged = False

            if WORKER_MODE == "HEARTBEAT":
                run_heartbeat_cycle(sock, buffer, state)
            elif WORKER_MODE == "TASKS":
                run_task_cycle(sock, buffer, state)
            else:
                raise ValueError(
                    f"WORKER_MODE inválido: {WORKER_MODE}. Use HEARTBEAT ou TASKS."
                )
        except (socket.timeout, TimeoutError):
            log("Timeout na comunicação com o Master. Tentando reconectar...")
            if state["borrowed"]:
                state["current_master_host"] = state["original_master_host"]
                state["current_master_port"] = state["original_master_port"]
                state["current_master_uuid"] = state["original_master_uuid"]
                state["borrowed"] = False
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError) as exc:
            log(f"Conexão perdida: {exc}")
            log_master_host_hint(state["current_master_host"])
            if state["borrowed"]:
                state["current_master_host"] = state["original_master_host"]
                state["current_master_port"] = state["original_master_port"]
                state["current_master_uuid"] = state["original_master_uuid"]
                state["borrowed"] = False
        except json.JSONDecodeError as exc:
            log(f"JSON inválido recebido do Master: {exc}")
        except Exception as exc:
            log(f"Erro inesperado: {exc}")
        finally:
            try:
                sock.close()
            except OSError:
                pass

        time.sleep(RECONNECT_DELAY)


def main():
    run_worker()


if __name__ == "__main__":
    main()
