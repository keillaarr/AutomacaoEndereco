# Automação de Carga de Endereços Oracle → PostgreSQL

Script desenvolvido para automatizar a extração de dados do banco **Oracle** e realizar a **carga (upsert)** no **PostgreSQL**, garantindo sincronização segura e eficiente das informações de endereço, telefone e e-mail das pessoas.

---

##  Funcionalidades

-  **Extração Oracle:** realiza consulta SQL complexa com `WITH` e `LEFT JOIN` para consolidar dados de endereço, telefone e e-mail.  
-  **Normalização de dados:** remove caracteres indesejados e trata campos nulos.  
-  **Criação automática da tabela:** verifica e cria a tabela de destino no PostgreSQL, se necessário.  
-  **Upsert inteligente:** insere ou atualiza registros em batches, comparando dados antes de atualizar.  
-  **Logging detalhado:** gera logs completos da execução em `carga_endereco.log`, com mensagens no console e no arquivo.  
-  **Tratamento de erros:** captura exceções e realiza `rollback` seguro em caso de falhas.  
-  **Fechamento seguro das conexões** (Oracle e PostgreSQL).

---

##  Estrutura

AutomacaoEndereco/
├── carga_endereco.py # Script principal
├── carga_endereco.log # Log de execução (gerado automaticamente)
└── README.md # Este arquivo



---

## Pré-requisitos

Antes de executar o script, instale as dependências:

```bash
pip install oracledb psycopg2-binary
```

## Como executar

No terminal, dentro da pasta do projeto, execute:
```bash
python carga_endereco.py
```
Durante a execução, o script:

Conecta ao Oracle e extrai os dados da query.

Conecta ao PostgreSQL e garante que a tabela public.endereco_teste exista.

Insere ou atualiza os registros de forma segura e otimizada.

Gera um log completo da operação (carga_endereco.log).


## Detalhes técnicos
Bancos de dados: Oracle Database e PostgreSQL

Bibliotecas principais:

oracledb → conexão e leitura dos dados Oracle

psycopg2-binary → conexão e escrita no PostgreSQL

logging → geração de logs com data/hora e status da execução

## Log de execução
Os logs são gravados tanto no console quanto no arquivo carga_endereco.log, permitindo fácil rastreabilidade.
Exemplo:
```bash
[2025-10-23 14:00:15] INFO: Iniciando carga ENDERECO
[2025-10-23 14:00:18] INFO: Conexão com Oracle estabelecida
[2025-10-23 14:00:22] INFO: Conexão com PostgreSQL estabelecida
[2025-10-23 14:00:30] INFO: Batch 12 processado com sucesso
[2025-10-23 14:00:32] INFO: Processo finalizado em 0:03:24

```

## Autora
Desenvolvido por Keilla Arruda

## Licença

Este projeto é de uso interno e educacional.
Sinta-se à vontade para adaptar e reutilizar a estrutura para outros processos de automação.

