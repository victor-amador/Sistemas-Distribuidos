# Sistemas Distribuidos

Implementacao das sprints 1, 2 e 3 com Masters e Workers usando sockets TCP.

Status atual: as tres sprints estao operacionais no codigo atual e foram validadas localmente com compilacao e teste de fluxo. A unica ressalva pratica e que a interoperabilidade com a implementacao de outra equipe nao foi testada aqui; ela depende de a outra equipe seguir exatamente os mesmos payloads do enunciado.

## Arquivos

- `master.py`: servidor Master com heartbeat, fila de tarefas e negociacao Master-to-Master.
- `worker1.py`: worker principal para heartbeat, tarefas, redirecionamento e retorno.
- `worker2.py`: segundo worker usando a mesma implementacao do `worker1.py`.
- `run_master.py`, `run_worker1.py`, `run_worker2.py`: inicializadores em Python, ideais para qualquer maquina com Python instalado.
- `Start-Master.ps1`, `Start-Worker1.ps1`, `Start-Worker2.ps1`: atalhos para Windows PowerShell.
- `Start-Master.cmd`, `Start-Worker1.cmd`, `Start-Worker2.cmd`: atalhos para Windows CMD.

## Como executar

O projeto nao depende de `make`. O caminho principal agora e usar diretamente os arquivos `run_*.py`.

### Validar a sintaxe

macOS e Linux:

```bash
python3 -m py_compile worker1.py worker2.py master.py run_master.py run_worker1.py run_worker2.py
```

Windows:

```powershell
py -m py_compile worker1.py worker2.py master.py run_master.py run_worker1.py run_worker2.py
```

### Execucao principal

macOS e Linux, Terminal 1:

```bash
python3 run_master.py
```

macOS e Linux, Terminal 2:

```bash
python3 run_worker1.py
```

Windows PowerShell, Terminal 1:

```powershell
py run_master.py
```

Windows PowerShell, Terminal 2:

```powershell
py run_worker1.py
```

Windows CMD, Terminal 1:

```cmd
py run_master.py
```

Windows CMD, Terminal 2:

```cmd
py run_worker1.py
```

Se tudo roda na mesma maquina, nao passe IP. O default do worker ja e `127.0.0.1`.

Se aparecer `Connection refused`, isso normalmente significa apenas que o Master nao esta rodando naquela porta ou foi encerrado. Suba primeiro o Master e depois o Worker.

### Opcoes uteis

Worker em heartbeat:

```bash
python3 run_worker1.py --mode HEARTBEAT
```

```powershell
py run_worker1.py --mode HEARTBEAT
```

Outra porta:

```bash
python3 run_master.py --port 5100
python3 run_worker1.py --host 127.0.0.1 --port 5100 --mode TASKS
```

```powershell
py run_master.py --port 5100
py run_worker1.py --host 127.0.0.1 --port 5100 --mode TASKS
```

Worker em outro computador, conectado ao Master no MacBook:

```powershell
py run_worker1.py --host 192.168.15.50 --port 5090 --mode TASKS
```

Segundo worker:

```bash
python3 run_worker2.py
```

```powershell
py run_worker2.py
```

## Sprint 1

Objetivo validado:

- heartbeat continuo entre Worker e Master;
- resposta `ALIVE` do Master;
- timeout nos sockets para evitar bloqueio infinito.

Comandos:

```bash
python3 run_master.py
python3 run_worker1.py --mode HEARTBEAT
```

```powershell
py run_master.py
py run_worker1.py --mode HEARTBEAT
```

## Sprint 2

Objetivo validado:

- heartbeat e task queue no mesmo codigo;
- o Worker em modo de tarefa faz heartbeat antes de entrar no fluxo de tarefas;
- o Master responde com `QUERY` ou `NO_TASK`;
- o Worker envia `STATUS` e recebe `ACK`;
- Worker emprestado usa `SERVER_UUID` quando aplicavel.

Comandos:

```bash
python3 run_master.py
python3 run_worker1.py
python3 run_worker2.py
```

```powershell
py run_master.py
py run_worker1.py
py run_worker2.py
```

## Fluxo para apresentacao das Sprints 1 e 2

1. Mostrar o Master recebendo heartbeat e respondendo `ALIVE`.
2. Mostrar o worker de tarefas enviando heartbeat antes da fila.
3. Mostrar o recebimento de `QUERY`, envio de `STATUS` e recebimento de `ACK`.
4. Mostrar `NO_TASK` depois que a fila for consumida.

## Sprint 3

Tipos de mensagem novos:

- `request_help`
- `response_accepted`
- `response_rejected`
- `command_redirect`
- `register_temporary_worker`
- `command_release`
- `notify_worker_returned`

O Master continua atendendo heartbeat e task queue das sprints anteriores, mas agora tambem:

- detecta saturacao pela fila de tarefas;
- negocia workers com masters vizinhos por socket TCP;
- redireciona workers locais para outro Master;
- registra workers emprestados temporariamente;
- devolve o worker ao Master original quando a carga normaliza.

### Teste rapido da Sprint 3

Terminal 1:

```bash
python3 run_master.py --master-uuid B --port 5102 --task-users "" --load-capacity 5 --release-threshold 1 --neighbor-masters A@127.0.0.1:5101
```

Terminal 2:

```bash
python3 run_worker1.py --host 127.0.0.1 --port 5102 --mode TASKS --master-uuid B
```

Terminal 3:

```bash
python3 run_master.py --master-uuid A --port 5101 --task-users Ana,Bia,Caio --load-capacity 1 --release-threshold 0 --worker-load-unit 1 --neighbor-masters B@127.0.0.1:5102
```

No Windows, troque `python3` por `py`.

O comportamento esperado e:

1. O Master A envia `request_help` para o Master B.
2. O Master B responde `response_accepted`.
3. O worker do Master B recebe `command_redirect`.
4. O worker conecta no Master A e envia `register_temporary_worker`.
5. O worker emprestado executa as tarefas `QUERY` no Master A com `SERVER_UUID` do Master de origem.
6. Quando a carga cai, o Master A envia `command_release` e `notify_worker_returned`.
7. O worker volta ao Master B e retoma o fluxo normal.

## Conferencia final das Sprints 1, 2 e 3

Pontos conferidos no codigo atual:

1. `sendall` nao e usado; o envio e feito apenas com `send`.
2. Os sockets usados pelo Master e pelos Workers usam timeout.
3. O heartbeat continua sendo obrigatorio antes da fila de tarefas.
4. O Worker continua capturando o IP real do Master ao conectar.
5. A Sprint 3 foi integrada sem separar o projeto em outro codigo.
6. Tipos de mensagem desconhecidos com `type` sao logados e ignorados.
7. O fluxo completo de emprestimo e devolucao foi testado localmente.

## Publicar no GitHub

Depois de alterar o codigo, use:

```bash
git add .
git commit -m "describe a alteracao"
git push
```