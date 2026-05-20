PYTHON ?= /usr/bin/python3
PORT ?= 5090
TASK_USERS ?= Michel
HEARTBEAT_INTERVAL ?= 1
RECONNECT_DELAY ?= 10
FORCE_STATUS ?= OK
MSG ?= atualizacao

.PHONY: help check master master-sprint12 worker-heartbeat worker-sprint1 worker-tasks worker-sprint2 worker2-tasks master-a-sprint3 master-b-sprint3 worker-b-sprint3 save push

help:
	@echo "Comandos disponiveis:"
	@echo "  make check             - valida a sintaxe dos arquivos Python"
	@echo "  make master            - inicia o Master padrao das Sprints 1 e 2"
	@echo "  make master-sprint12   - alias para o Master das Sprints 1 e 2"
	@echo "  make worker-heartbeat  - inicia o Worker da Sprint 1"
	@echo "  make worker-sprint1    - alias para o Worker da Sprint 1"
	@echo "  make worker-tasks      - inicia o Worker da Sprint 2"
	@echo "  make worker-sprint2    - alias para o Worker da Sprint 2"
	@echo "  make worker2-tasks     - inicia o segundo Worker da Sprint 2"
	@echo "  make master-a-sprint3  - inicia o Master A saturado da Sprint 3"
	@echo "  make master-b-sprint3  - inicia o Master B vizinho da Sprint 3"
	@echo "  make worker-b-sprint3  - inicia o worker do Master B para emprestimo"
	@echo "  make save MSG=\"texto\" - faz add, commit e push"
	@echo "  make push              - envia os commits ja criados"
	@echo ""
	@echo "Variaveis opcionais:"
	@echo "  PORT=5090 TASK_USERS=Michel HEARTBEAT_INTERVAL=1 RECONNECT_DELAY=10 FORCE_STATUS=OK MSG=atualizacao"

check:
	$(PYTHON) -m py_compile worker1.py worker2.py master.py

master:
	MASTER_PORT=$(PORT) TASK_USERS=$(TASK_USERS) $(PYTHON) master.py

master-sprint12:
	MASTER_PORT=$(PORT) TASK_USERS=$(TASK_USERS) $(PYTHON) master.py

worker-heartbeat:
	MASTER_PORT=$(PORT) WORKER_MODE=HEARTBEAT HEARTBEAT_INTERVAL=$(HEARTBEAT_INTERVAL) $(PYTHON) worker1.py

worker-sprint1:
	MASTER_PORT=$(PORT) WORKER_MODE=HEARTBEAT HEARTBEAT_INTERVAL=$(HEARTBEAT_INTERVAL) $(PYTHON) worker1.py

worker-tasks:
	MASTER_PORT=$(PORT) WORKER_MODE=TASKS FORCE_STATUS=$(FORCE_STATUS) RECONNECT_DELAY=$(RECONNECT_DELAY) $(PYTHON) worker1.py

worker-sprint2:
	MASTER_PORT=$(PORT) WORKER_MODE=TASKS FORCE_STATUS=$(FORCE_STATUS) RECONNECT_DELAY=$(RECONNECT_DELAY) $(PYTHON) worker1.py

worker2-tasks:
	MASTER_PORT=$(PORT) WORKER_MODE=TASKS FORCE_STATUS=$(FORCE_STATUS) RECONNECT_DELAY=$(RECONNECT_DELAY) $(PYTHON) worker2.py

master-a-sprint3:
	MASTER_UUID=A MASTER_PORT=5101 TASK_USERS=Ana,Bia,Caio LOAD_CAPACITY=1 RELEASE_THRESHOLD=0 WORKER_LOAD_UNIT=1 NEIGHBOR_MASTERS='B@127.0.0.1:5102' $(PYTHON) master.py

master-b-sprint3:
	MASTER_UUID=B MASTER_PORT=5102 TASK_USERS= LOAD_CAPACITY=5 RELEASE_THRESHOLD=1 NEIGHBOR_MASTERS='A@127.0.0.1:5101' $(PYTHON) master.py

worker-b-sprint3:
	MASTER_HOST=127.0.0.1 MASTER_PORT=5102 MASTER_UUID=B WORKER_MODE=TASKS RECONNECT_DELAY=$(RECONNECT_DELAY) FORCE_STATUS=$(FORCE_STATUS) $(PYTHON) worker1.py

save:
	git add .
	@if git diff --cached --quiet; then \
		echo "Nenhuma alteracao para commit."; \
	else \
		git commit -m "$(MSG)"; \
		git push; \
	fi

push:
	git push