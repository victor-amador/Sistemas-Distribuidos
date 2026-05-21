ifeq ($(OS),Windows_NT)
PYTHON ?= py

define RUN_WITH_ENV
	cmd /C "$(foreach pair,$(1),set \"$(pair)\"&& )$(PYTHON) $(2)"
endef
else
PYTHON ?= /usr/bin/python3

define RUN_WITH_ENV
	$(foreach pair,$(1),$(pair) )$(PYTHON) $(2)
endef
endif

PORT ?= 5090
HOST ?= 127.0.0.1
TASK_USERS ?= Michel
HEARTBEAT_INTERVAL ?= 1
RECONNECT_DELAY ?= 10
FORCE_STATUS ?= OK
MODE ?= TASKS
MSG ?= atualizacao

.PHONY: help check master master-sprint12 worker1 worker1-heartbeat worker1-tasks worker2 worker-heartbeat worker-sprint1 worker-tasks worker-sprint2 worker2-tasks master-a-sprint3 master-b-sprint3 worker-b-sprint3 save push

help:
	@echo "Comandos disponiveis:"
	@echo "  make check             - valida a sintaxe dos arquivos Python"
	@echo "  make master            - inicia o Master padrao das Sprints 1 e 2"
	@echo "  make master-sprint12   - alias para o Master das Sprints 1 e 2"
	@echo "  make worker1           - inicia o worker1 com MODE=TASKS ou HEARTBEAT"
	@echo "  make worker1-heartbeat - inicia o worker1 em HEARTBEAT"
	@echo "  make worker1-tasks     - inicia o worker1 em TASKS"
	@echo "  make worker2           - inicia o worker2 em TASKS"
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
	@echo "  HOST=127.0.0.1 PORT=5090 MODE=TASKS TASK_USERS=Michel HEARTBEAT_INTERVAL=1 RECONNECT_DELAY=10 FORCE_STATUS=OK MSG=atualizacao"

check:
	$(PYTHON) -m py_compile worker1.py worker2.py master.py

master:
	$(call RUN_WITH_ENV,MASTER_PORT=$(PORT) TASK_USERS=$(TASK_USERS),master.py)

master-sprint12:
	$(call RUN_WITH_ENV,MASTER_PORT=$(PORT) TASK_USERS=$(TASK_USERS),master.py)

worker1:
	$(call RUN_WITH_ENV,MASTER_HOST=$(HOST) MASTER_PORT=$(PORT) WORKER_MODE=$(MODE) HEARTBEAT_INTERVAL=$(HEARTBEAT_INTERVAL) RECONNECT_DELAY=$(RECONNECT_DELAY) FORCE_STATUS=$(FORCE_STATUS),worker1.py)

worker1-heartbeat:
	$(call RUN_WITH_ENV,MASTER_HOST=$(HOST) MASTER_PORT=$(PORT) WORKER_MODE=HEARTBEAT HEARTBEAT_INTERVAL=$(HEARTBEAT_INTERVAL),worker1.py)

worker1-tasks:
	$(call RUN_WITH_ENV,MASTER_HOST=$(HOST) MASTER_PORT=$(PORT) WORKER_MODE=TASKS FORCE_STATUS=$(FORCE_STATUS) RECONNECT_DELAY=$(RECONNECT_DELAY),worker1.py)

worker2:
	$(call RUN_WITH_ENV,MASTER_HOST=$(HOST) MASTER_PORT=$(PORT) WORKER_MODE=TASKS FORCE_STATUS=$(FORCE_STATUS) RECONNECT_DELAY=$(RECONNECT_DELAY),worker2.py)

worker-heartbeat:
	$(call RUN_WITH_ENV,MASTER_HOST=$(HOST) MASTER_PORT=$(PORT) WORKER_MODE=HEARTBEAT HEARTBEAT_INTERVAL=$(HEARTBEAT_INTERVAL),worker1.py)

worker-sprint1:
	$(call RUN_WITH_ENV,MASTER_HOST=$(HOST) MASTER_PORT=$(PORT) WORKER_MODE=HEARTBEAT HEARTBEAT_INTERVAL=$(HEARTBEAT_INTERVAL),worker1.py)

worker-tasks:
	$(call RUN_WITH_ENV,MASTER_HOST=$(HOST) MASTER_PORT=$(PORT) WORKER_MODE=TASKS FORCE_STATUS=$(FORCE_STATUS) RECONNECT_DELAY=$(RECONNECT_DELAY),worker1.py)

worker-sprint2:
	$(call RUN_WITH_ENV,MASTER_HOST=$(HOST) MASTER_PORT=$(PORT) WORKER_MODE=TASKS FORCE_STATUS=$(FORCE_STATUS) RECONNECT_DELAY=$(RECONNECT_DELAY),worker1.py)

worker2-tasks:
	$(call RUN_WITH_ENV,MASTER_HOST=$(HOST) MASTER_PORT=$(PORT) WORKER_MODE=TASKS FORCE_STATUS=$(FORCE_STATUS) RECONNECT_DELAY=$(RECONNECT_DELAY),worker2.py)

master-a-sprint3:
	$(call RUN_WITH_ENV,MASTER_UUID=A MASTER_PORT=5101 TASK_USERS=Ana,Bia,Caio LOAD_CAPACITY=1 RELEASE_THRESHOLD=0 WORKER_LOAD_UNIT=1 NEIGHBOR_MASTERS=B@127.0.0.1:5102,master.py)

master-b-sprint3:
	$(call RUN_WITH_ENV,MASTER_UUID=B MASTER_PORT=5102 TASK_USERS= LOAD_CAPACITY=5 RELEASE_THRESHOLD=1 NEIGHBOR_MASTERS=A@127.0.0.1:5101,master.py)

worker-b-sprint3:
	$(call RUN_WITH_ENV,MASTER_HOST=127.0.0.1 MASTER_PORT=5102 MASTER_UUID=B WORKER_MODE=TASKS RECONNECT_DELAY=$(RECONNECT_DELAY) FORCE_STATUS=$(FORCE_STATUS),worker1.py)

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