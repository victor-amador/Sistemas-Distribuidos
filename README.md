# Sistemas Distribuidos

Implementacao das sprints 1, 2 e 3 com Masters e Workers usando sockets TCP.

Status atual: as tres sprints estao operacionais no codigo atual e foram validadas localmente com compilacao e teste de fluxo. A unica ressalva pratica e que a interoperabilidade com a implementacao de outra equipe nao foi testada aqui; ela depende de a outra equipe seguir exatamente os mesmos payloads do enunciado.

## Arquivos

- `master.py`: servidor Master com heartbeat, fila de tarefas e negociacao Master-to-Master.
- `worker1.py`: worker principal para heartbeat, tarefas, redirecionamento e retorno.
- `worker2.py`: segundo worker usando a mesma implementacao do `worker1.py`.
- `Makefile`: atalhos para executar, validar e publicar alteracoes.

## Como executar

### macOS e Linux

Validar a sintaxe:

```bash
make check
```

Forma mais simples para rodar no terminal:

Terminal 1:

```bash
make master
```

Terminal 2, com o worker principal:

```bash
make worker1
```

Se quiser escolher explicitamente o modo do worker1:

```bash
make worker1 MODE=HEARTBEAT
make worker1 MODE=TASKS
```

Atalhos diretos equivalentes:

```bash
make worker1-heartbeat
make worker1-tasks
make worker2
```

Subir o Master das Sprints 1 e 2:

```bash
make master
```

Subir o Worker da Sprint 1:

```bash
make worker-heartbeat
```

Subir o Worker da Sprint 2:

```bash
make worker-tasks
```

Subir um segundo Worker:

```bash
make worker2-tasks
```

Atalhos equivalentes por sprint:

```bash
make master-sprint12
make worker-sprint1
make worker-sprint2
```

Trocar a porta ou a tarefa:

```bash
make master PORT=5100 TASK_USERS=Joao
make worker1 PORT=5100 MODE=HEARTBEAT
make worker1 PORT=5100 MODE=TASKS
```

Se aparecer `Connection refused`, isso normalmente significa apenas que o Master nao esta rodando naquela porta ou foi encerrado. Nesse caso:

1. suba primeiro `make master`;
2. depois rode `make worker1` ou `make worker1 MODE=...`;
3. confirme que ambos usam a mesma `PORT`.

### Windows PowerShell

No Windows, o erro `make : O termo 'make' nao e reconhecido...` significa que o sistema nao tem `make` instalado. Mudar o IP nao resolve esse erro, porque o problema nao e de rede: e apenas a ausencia do comando `make`. Para evitar essa dependencia, use os scripts PowerShell do projeto.

Validar o Python:

```powershell
py --version
```

Terminal 1, subir o Master:

```powershell
.\Start-Master.ps1
```

Terminal 2, subir o worker principal:

```powershell
.\Start-Worker1.ps1
```

O default do worker ja aponta para o IP do seu MacBook: `192.168.15.50`.

Se quiser escolher o modo do worker principal:

```powershell
.\Start-Worker1.ps1 -Mode HEARTBEAT
.\Start-Worker1.ps1 -Mode TASKS
```

Se quiser subir o segundo worker:

```powershell
.\Start-Worker2.ps1
```

Se a porta for diferente:

```powershell
.\Start-Master.ps1 -Port 5100
.\Start-Worker1.ps1 -Host 192.168.15.50 -Port 5100 -Mode TASKS
```

Se o PowerShell bloquear a execucao de script, rode uma vez:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```

Se aparecer `Connection refused` no Windows, o motivo continua sendo o mesmo: o Master nao esta ativo naquela porta ou foi encerrado.

### Windows CMD

Se voce estiver usando o Prompt de Comando em vez do PowerShell, pode rodar diretamente:

```cmd
Start-Master.cmd
Start-Worker1.cmd
```

O default do worker ja aponta para o IP do seu MacBook: `192.168.15.50`.

Se quiser informar manualmente o IP e a porta do Master:

```cmd
Start-Worker1.cmd 192.168.15.50 5090 TASKS
Start-Worker2.cmd 192.168.15.50 5090
```

Se estiver tudo na mesma maquina, os defaults funcionam sem parametro.

## Sprint 1

Objetivo validado:

- heartbeat continuo entre Worker e Master;
- resposta `ALIVE` do Master;
- timeout nos sockets para evitar bloqueio infinito.

Comandos:

```bash
make master
make worker-heartbeat
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
make master
make worker-tasks
make worker2-tasks
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
make master-b-sprint3
```

Terminal 2:

```bash
make worker-b-sprint3
```

Terminal 3:

```bash
make master-a-sprint3
```

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

Depois de alterar o codigo, voce pode usar:

```bash
make save MSG="describe a alteracao"
```

Esse comando faz `git add .`, `git commit` e `git push` de uma vez.