PYTHON ?= /usr/bin/python3
PORT ?= 5090
TASK_USERS ?= Michel
HEARTBEAT_INTERVAL ?= 1
RECONNECT_DELAY ?= 10
FORCE_STATUS ?= OK

.PHONY: help check master worker-heartbeat worker-tasks worker2-tasks

help:
	@echo "Comandos disponiveis:"
	@echo "  make check             - valida a sintaxe dos arquivos Python"
	@echo "  make master            - inicia o Master"
	@echo "  make worker-heartbeat  - inicia o Worker da Sprint 1"
	@echo "  make worker-tasks      - inicia o Worker da Sprint 2"
	@echo "  make worker2-tasks     - inicia o segundo Worker da Sprint 2"
	@echo ""
	@echo "Variaveis opcionais:"
	@echo "  PORT=5090 TASK_USERS=Michel HEARTBEAT_INTERVAL=1 RECONNECT_DELAY=10 FORCE_STATUS=OK"

check:
	$(PYTHON) -m py_compile worker1.py worker2.py master.py

master:
	MASTER_PORT=$(PORT) TASK_USERS=$(TASK_USERS) $(PYTHON) master.py

worker-heartbeat:
	MASTER_PORT=$(PORT) WORKER_MODE=HEARTBEAT HEARTBEAT_INTERVAL=$(HEARTBEAT_INTERVAL) $(PYTHON) worker1.py

worker-tasks:
	MASTER_PORT=$(PORT) WORKER_MODE=TASKS FORCE_STATUS=$(FORCE_STATUS) RECONNECT_DELAY=$(RECONNECT_DELAY) $(PYTHON) worker1.py

worker2-tasks:
	MASTER_PORT=$(PORT) WORKER_MODE=TASKS FORCE_STATUS=$(FORCE_STATUS) RECONNECT_DELAY=$(RECONNECT_DELAY) $(PYTHON) worker2.py