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


def log(message):
    print(f"[WORKER {WORKER_UUID}] {message}")


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


def build_presentation_payload():
    payload = {
        "WORKER": "ALIVE",
        "WORKER_UUID": WORKER_UUID,
    }

    if ORIGINAL_SERVER_UUID and ORIGINAL_SERVER_UUID != MASTER_UUID:
        payload["SERVER_UUID"] = ORIGINAL_SERVER_UUID

    return payload


def build_heartbeat_payload():
    return {
        "SERVER_UUID": MASTER_UUID,
        "TASK": "HEARTBEAT",
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


def validate_heartbeat_payload(payload):
    if payload.get("SERVER_UUID") != MASTER_UUID:
        raise ValueError(f"Resposta HEARTBEAT de servidor inesperado: {payload}")
    if str(payload.get("TASK", "")).upper() != "HEARTBEAT":
        raise ValueError(f"Resposta HEARTBEAT inválida: {payload}")
    if payload.get("RESPONSE") != "ALIVE":
        raise ValueError(f"Master não respondeu ALIVE ao HEARTBEAT: {payload}")


def run_heartbeat_cycle(sock, buffer):
    while True:
        heartbeat_payload = build_heartbeat_payload()
        send_json(sock, heartbeat_payload)
        log(f"Heartbeat enviado: {heartbeat_payload}")

        response_payload, buffer = recv_json(sock, buffer)
        validate_heartbeat_payload(response_payload)
        log(f"Heartbeat confirmado: {response_payload}")
        time.sleep(HEARTBEAT_INTERVAL)


def run_task_cycle(sock, buffer):
    heartbeat_payload = build_heartbeat_payload()
    send_json(sock, heartbeat_payload)
    log(f"Heartbeat enviado antes da fila de tarefas: {heartbeat_payload}")

    heartbeat_response, buffer = recv_json(sock, buffer)
    validate_heartbeat_payload(heartbeat_response)
    log(f"Heartbeat confirmado antes da fila de tarefas: {heartbeat_response}")

    presentation_payload = build_presentation_payload()
    send_json(sock, presentation_payload)
    log(f"Apresentação enviada: {presentation_payload}")

    task_payload, buffer = recv_json(sock, buffer)
    task_kind = validate_task_payload(task_payload)
    log(f"Resposta do Master: {task_payload}")

    if task_kind == "NO_TASK":
        log("Master informou que não há tarefa disponível.")
        return

    status_payload = {
        "STATUS": process_query(task_payload),
        "TASK": "QUERY",
        "WORKER_UUID": WORKER_UUID,
    }
    send_json(sock, status_payload)
    log(f"Status enviado: {status_payload}")

    ack_payload, buffer = recv_json(sock, buffer)
    validate_ack_payload(ack_payload)
    log(f"ACK confirmado: {ack_payload}")


def run_worker():
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(SOCKET_TIMEOUT)
        buffer = ""

        try:
            log(f"Conectando ao Master {MASTER_UUID} em {MASTER_HOST}:{MASTER_PORT}...")
            sock.connect((MASTER_HOST, MASTER_PORT))
            log(f"Conexão estabelecida com o IP do Master {get_connected_master_ip(sock)}.")

            if WORKER_MODE == "HEARTBEAT":
                run_heartbeat_cycle(sock, buffer)
            elif WORKER_MODE == "TASKS":
                run_task_cycle(sock, buffer)
            else:
                raise ValueError(
                    f"WORKER_MODE inválido: {WORKER_MODE}. Use HEARTBEAT ou TASKS."
                )
        except (socket.timeout, TimeoutError):
            log("Timeout na comunicação com o Master. Tentando reconectar...")
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError) as exc:
            log(f"Conexão perdida: {exc}")
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
