# Runtime Alignment And Connectivity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Alinhar os pontos de entrada, os defaults de conexão e a documentação para que Master e Workers executem de forma previsível na mesma máquina e em máquinas diferentes.

**Architecture:** O plano concentra a execução em um contrato único de runtime: um conjunto mínimo de entrypoints explícitos, defaults coerentes entre Master e Worker, e documentação que reflita apenas arquivos realmente existentes. A implementação evita mudanças amplas no protocolo; primeiro estabiliza execução e observabilidade, depois cobre smoke tests de fluxo local e remoto.

**Tech Stack:** Python 3, sockets TCP, JSON newline-delimited, threads, scripts de inicialização para Windows, validação por py_compile e smoke tests manuais.

---

### Task 1: Auditar e fixar o contrato de execução

**Files:**
- Modify: `master.py`
- Modify: `worker1.py`
- Modify: `worker2.py`
- Test: runtime smoke via terminal

- [ ] **Step 1: Registrar o contrato-alvo de runtime antes de editar**

```text
Contrato desejado:
- Master local: host bind = 0.0.0.0, port default = 5090
- Worker local: target host default = 127.0.0.1, port default = 5090
- Worker remoto: host sempre informado explicitamente por argumento/variável
- Nenhum arquivo deve usar 8000 ou 10.62.217.203 como default hardcoded
```

- [ ] **Step 2: Criar uma verificação textual que deve falhar antes do ajuste completo**

Run: `grep -R "10.62.217.203\|8000\|5000" master.py worker1.py worker2.py`
Expected: retorna ocorrências mostrando defaults divergentes ou legado restante

- [ ] **Step 3: Ajustar os defaults mínimos nos arquivos centrais**

```python
# master.py
MASTER_HOST = os.getenv("MASTER_HOST", "0.0.0.0")
MASTER_PORT = int(os.getenv("MASTER_PORT", "5090"))

# worker1.py
MASTER_HOST = os.getenv("MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.getenv("MASTER_PORT", "5090"))
```

```python
# worker2.py
import os
os.environ.setdefault("WORKER_UUID", "W-SECONDARY")
os.environ.setdefault("ORIGINAL_SERVER_UUID", "")
from worker1 import main
```

- [ ] **Step 4: Rodar compilação estreita após o ajuste**

Run: `python3 -m py_compile master.py worker1.py worker2.py`
Expected: nenhum erro

- [ ] **Step 5: Commit**

```bash
git add master.py worker1.py worker2.py
git commit -m "fix: align default runtime configuration"
```

### Task 2: Consolidar entrypoints reais do projeto

**Files:**
- Create: `run_master.py`
- Create: `run_worker1.py`
- Create: `run_worker2.py`
- Modify: `Start-Master.cmd`
- Create: `Start-Worker1.cmd`
- Create: `Start-Worker2.cmd`
- Test: entrypoint smoke via terminal

- [ ] **Step 1: Escrever um smoke test manual que prove a necessidade dos entrypoints**

```text
Cenário mínimo:
1. Iniciar master com comando curto e sem depender de shell-specific env.
2. Iniciar worker com comando curto e sem editar o código.
3. Confirmar conexão, heartbeat e QUERY/ACK.
```

Run: `python3 master.py` e em outro terminal `python3 worker1.py`
Expected: pode funcionar localmente, mas ainda não oferece uma interface de execução segura e uniforme para Windows e para cenários remotos

- [ ] **Step 2: Criar o launcher Python do Master**

```python
# run_master.py
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--port", default="5090")
parser.add_argument("--task-users", default="Michel,Julia")
parser.add_argument("--master-uuid", default=None)
parser.add_argument("--load-capacity", default=None)
parser.add_argument("--release-threshold", default=None)
parser.add_argument("--worker-load-unit", default=None)
parser.add_argument("--neighbor-masters", default=None)
args = parser.parse_args()

os.environ["MASTER_PORT"] = str(args.port)
os.environ["TASK_USERS"] = args.task_users
if args.master_uuid:
    os.environ["MASTER_UUID"] = args.master_uuid
if args.load_capacity is not None:
    os.environ["LOAD_CAPACITY"] = str(args.load_capacity)
if args.release_threshold is not None:
    os.environ["RELEASE_THRESHOLD"] = str(args.release_threshold)
if args.worker_load_unit is not None:
    os.environ["WORKER_LOAD_UNIT"] = str(args.worker_load_unit)
if args.neighbor_masters is not None:
    os.environ["NEIGHBOR_MASTERS"] = args.neighbor_masters

from master import main
main()
```

- [ ] **Step 3: Criar o launcher Python do worker principal**

```python
# run_worker1.py
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--port", default="5090")
parser.add_argument("--mode", choices=["TASKS", "HEARTBEAT"], default="TASKS")
parser.add_argument("--heartbeat-interval", default="1")
parser.add_argument("--reconnect-delay", default="10")
parser.add_argument("--force-status", default="")
parser.add_argument("--master-uuid", default=None)
args = parser.parse_args()

os.environ["MASTER_HOST"] = args.host
os.environ["MASTER_PORT"] = str(args.port)
os.environ["WORKER_MODE"] = args.mode
os.environ["HEARTBEAT_INTERVAL"] = str(args.heartbeat_interval)
os.environ["RECONNECT_DELAY"] = str(args.reconnect_delay)
os.environ["FORCE_STATUS"] = args.force_status
if args.master_uuid:
    os.environ["MASTER_UUID"] = args.master_uuid

from worker1 import main
main()
```

- [ ] **Step 4: Criar o launcher Python do worker secundário**

```python
# run_worker2.py
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--port", default="5090")
parser.add_argument("--reconnect-delay", default="10")
parser.add_argument("--force-status", default="")
args = parser.parse_args()

os.environ["MASTER_HOST"] = args.host
os.environ["MASTER_PORT"] = str(args.port)
os.environ["WORKER_MODE"] = "TASKS"
os.environ["RECONNECT_DELAY"] = str(args.reconnect_delay)
os.environ["FORCE_STATUS"] = args.force_status

from worker2 import main
main()
```

- [ ] **Step 5: Padronizar os wrappers CMD para chamarem os launchers Python**

```bat
:: Start-Master.cmd
@echo off
setlocal
where py >nul 2>nul
if %errorlevel% neq 0 (
  echo Python launcher 'py' nao encontrado.
  exit /b 1
)
py "%~dp0run_master.py"
```

```bat
:: Start-Worker1.cmd
@echo off
setlocal
where py >nul 2>nul
if %errorlevel% neq 0 (
  echo Python launcher 'py' nao encontrado.
  exit /b 1
)
py "%~dp0run_worker1.py"
```

```bat
:: Start-Worker2.cmd
@echo off
setlocal
where py >nul 2>nul
if %errorlevel% neq 0 (
  echo Python launcher 'py' nao encontrado.
  exit /b 1
)
py "%~dp0run_worker2.py"
```

- [ ] **Step 6: Verificar compilação e presença dos entrypoints**

Run: `python3 -m py_compile master.py worker1.py worker2.py run_master.py run_worker1.py run_worker2.py`
Expected: nenhum erro

Run: `ls Start-Master.cmd Start-Worker1.cmd Start-Worker2.cmd run_master.py run_worker1.py run_worker2.py`
Expected: todos os arquivos listados

- [ ] **Step 7: Commit**

```bash
git add Start-Master.cmd Start-Worker1.cmd Start-Worker2.cmd run_master.py run_worker1.py run_worker2.py
git commit -m "feat: add stable runtime entrypoints"
```

### Task 3: Reescrever a documentação para refletir o estado real

**Files:**
- Modify: `README.md`
- Test: doc verification via grep and smoke commands

- [ ] **Step 1: Definir a estrutura final do README**

```text
Seções mínimas:
1. Visão geral
2. Arquivos reais do projeto
3. Execução local mínima
4. Execução em outro PC
5. Sprint 1
6. Sprint 2
7. Sprint 3
8. Problemas comuns
9. Publicação no GitHub
```

- [ ] **Step 2: Remover referências a arquivos inexistentes e comandos obsoletos**

```markdown
Remover do README atual:
- referências a Makefile
- referências a run_*.py se ainda não existirem no momento da edição
- referências a Start-Worker*.ps1 se esses arquivos não existirem
- defaults antigos que contradizem o código real
```

- [ ] **Step 3: Inserir comandos reais e consistentes com os arquivos do repositório**

```markdown
## Execucao principal

Terminal 1:
```bash
python3 run_master.py
```

Terminal 2:
```bash
python3 run_worker1.py
```

No Windows:
```cmd
py run_master.py
py run_worker1.py
```
```

- [ ] **Step 4: Documentar explicitamente os dois cenários de rede**

```markdown
- Mesmo PC: use o default 127.0.0.1
- Outro PC: passe o IP do Master explicitamente

Exemplo remoto:
```powershell
py run_worker1.py --host 192.168.15.50 --port 5090 --mode TASKS
```
```

- [ ] **Step 5: Validar README contra o diretório real**

Run: `grep -n "Makefile\|run_.*\.py\|Start-.*ps1" README.md`
Expected: nenhuma referência a arquivo ausente; apenas arquivos realmente presentes

- [ ] **Step 6: Commit**

```bash
git add README.md
git commit -m "docs: align runtime documentation with project files"
```

### Task 4: Criar smoke test manual reproduzível para execução local

**Files:**
- Modify: `README.md`
- Test: manual terminal smoke

- [ ] **Step 1: Escrever o cenário mínimo de teste local no README**

```markdown
### Smoke test local

Terminal 1:
```bash
python3 run_master.py
```

Terminal 2:
```bash
python3 run_worker1.py
```

Saída esperada:
- Master escutando em 0.0.0.0:5090
- Worker conectando em 127.0.0.1:5090
- Heartbeat confirmado
- QUERY enviada
- STATUS enviado
- ACK confirmado
```
```

- [ ] **Step 2: Executar o smoke test e capturar as linhas-chave**

Run: `python3 run_master.py`
Expected: `Servidor escutando em 0.0.0.0:5090`

Run: `python3 run_worker1.py`
Expected: `Conexão estabelecida`, `Heartbeat confirmado`, `ACK confirmado`

- [ ] **Step 3: Se o smoke falhar, voltar para a investigação antes de seguir**

```text
Não empilhar correções. Registrar:
- comando executado
- saída obtida
- ponto exato do fluxo onde falhou
- nova hipótese única
```

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "test: document local smoke test workflow"
```

### Task 5: Criar smoke test manual para execução em outro PC

**Files:**
- Modify: `README.md`
- Test: manual network smoke

- [ ] **Step 1: Escrever os pré-requisitos explícitos para teste remoto**

```markdown
### Smoke test em outro PC

Pre-requisitos:
- Master e Worker na mesma rede
- Firewall liberando a porta 5090
- IP real do Master conhecido
- Master iniciado antes do Worker
```
```

- [ ] **Step 2: Documentar o comando remoto exato**

```markdown
No PC do Master:
```bash
python3 run_master.py --port 5090
```

No outro PC:
```powershell
py run_worker1.py --host 192.168.15.50 --port 5090 --mode TASKS
```
```

- [ ] **Step 3: Documentar a matriz de falhas mais prováveis**

```markdown
- `Connection refused`: Master nao esta rodando ou a porta esta fechada
- timeout apos conectar: protocolo divergente entre Master e Worker
- host errado: Worker apontando para IP incorreto
- sem Python launcher: usar `python` em vez de `py`
```

- [ ] **Step 4: Validar o cenário remoto na mesma rede ou, se indisponível, registrar a limitação**

Run: `py run_worker1.py --host <IP_DO_MASTER> --port 5090 --mode TASKS`
Expected: conexão remota e heartbeat

Expected alternative if not testable now: nota explícita em README dizendo que o fluxo remoto depende de firewall e disponibilidade de rede local

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: add remote execution smoke test"
```

### Task 6: Revisão final de consistência

**Files:**
- Modify: `README.md`
- Modify: `master.py`
- Modify: `worker1.py`
- Modify: `worker2.py`
- Modify: `run_master.py`
- Modify: `run_worker1.py`
- Modify: `run_worker2.py`
- Modify: `Start-Master.cmd`
- Modify: `Start-Worker1.cmd`
- Modify: `Start-Worker2.cmd`
- Test: compile + smoke + grep consistency

- [ ] **Step 1: Rodar verificação de defaults conflitantes**

Run: `grep -R "10.62.217.203\|8000\|5000\|Makefile" README.md master.py worker1.py worker2.py run_master.py run_worker1.py run_worker2.py`
Expected: nenhuma ocorrência inválida de legado, exceto menções históricas removidas da documentação

- [ ] **Step 2: Rodar compilação final**

Run: `python3 -m py_compile master.py worker1.py worker2.py run_master.py run_worker1.py run_worker2.py`
Expected: nenhum erro

- [ ] **Step 3: Rodar smoke final de Sprint 2**

Run: `python3 run_master.py`
Expected: `Servidor escutando em 0.0.0.0:5090`

Run: `python3 run_worker1.py`
Expected: `Heartbeat confirmado`, `ACK confirmado`

Run: `python3 run_worker2.py`
Expected: segundo worker conecta sem quebrar o fluxo do primeiro

- [ ] **Step 4: Revisar o diff final antes de concluir**

Run: `git diff -- master.py worker1.py worker2.py run_master.py run_worker1.py run_worker2.py README.md Start-Master.cmd Start-Worker1.cmd Start-Worker2.cmd`
Expected: somente mudanças relacionadas a runtime, entrypoints e documentação

- [ ] **Step 5: Commit**

```bash
git add README.md master.py worker1.py worker2.py run_master.py run_worker1.py run_worker2.py Start-Master.cmd Start-Worker1.cmd Start-Worker2.cmd
git commit -m "chore: stabilize runtime and connectivity workflow"
```
