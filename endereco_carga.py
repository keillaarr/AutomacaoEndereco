import os
import sys
import logging
from datetime import datetime

try:
    import oracledb
except ImportError:
    logging.error("Módulo 'oracledb' não encontrado. Instale com: pip install oracledb")
    sys.exit(1)

try:
    import psycopg2
except ImportError:
    logging.error("Módulo 'psycopg2-binary' não encontrado. Instale com: pip install psycopg2-binary")
    sys.exit(1)

# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("carga_endereco.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# =========================
# Credenciais dos bancos
# =========================
ORACLE_USER = "xxx"
ORACLE_PASSWORD = "xxx"
ORACLE_DSN = "xxx"

PG_DBNAME = "xxx"
PG_USER = "xxx"
PG_PASSWORD = "xxx"
PG_HOST = "xxx"
PG_PORT = 5432

# =========================
# Query Oracle
# =========================
QUERY_ORACLE = """
WITH telefones AS (
    SELECT
        t.PESSOA_ID,
        t.DDD,
        t.NUMERO,
        t.TIPOTELEFONE,
        ROW_NUMBER() OVER (PARTITION BY t.PESSOA_ID, t.TIPOTELEFONE ORDER BY t.ID) AS rn
    FROM TB1152_TELEFONE t
    WHERE UPPER(t.TIPOTELEFONE) IN ('CASA','CELULAR')
),
casa AS (
    SELECT
        PESSOA_ID,
        MAX(CASE WHEN rn = 1 THEN DDD END) AS DDDTEL1,
        MAX(CASE WHEN rn = 1 THEN NUMERO END) AS TELEFONE1,
        MAX(CASE WHEN rn = 2 THEN DDD END) AS DDDTEL2,
        MAX(CASE WHEN rn = 2 THEN NUMERO END) AS TELEFONE2
    FROM telefones
    WHERE TIPOTELEFONE = 'CASA'
    GROUP BY PESSOA_ID
),
celular AS (
    SELECT
        PESSOA_ID,
        MAX(DDD) AS DDDCEL,
        MAX(NUMERO) AS CELULAR
    FROM telefones
    WHERE TIPOTELEFONE = 'CELULAR'
    GROUP BY PESSOA_ID
),
emails AS (
    SELECT
        PESSOA_ID,
        LISTAGG(EMAIL, '; ') WITHIN GROUP (ORDER BY EMAIL) AS EMAILS
    FROM TB1120_EMAIL
    GROUP BY PESSOA_ID
)
SELECT
    p.IDPESSOA AS IDPESSOA,
    e.ATUALIZACAO AS DTATUALIZACAO,
    e.LOGRADOURO AS ENDERECORUA,
    e.NUMERO AS ENDERECONUMERO,
    e.COMPLEMENTO AS ENDERECOCOMPLEMENTO,
    e.BAIRRO AS ENDERECABAIRRO,
    e.CIDADE_ID AS CDCIDADE,
    e.CEP,
    c.DDDTEL1,
    c.TELEFONE1,
    c.DDDTEL2,
    c.TELEFONE2,
    cel.DDDCEL,
    cel.CELULAR,
    em.EMAILS
FROM TB1173_PESSOA p
LEFT JOIN TB1121_ENDERECO e
    ON RAWTOHEX(e.IDPESSOA) = RAWTOHEX(p.IDPESSOA)
LEFT JOIN casa c
    ON c.PESSOA_ID = p.ID
LEFT JOIN celular cel
    ON cel.PESSOA_ID = p.ID
LEFT JOIN emails em
    ON em.PESSOA_ID = p.ID
WHERE RAWTOHEX(p.IDPESSOA) NOT IN (
    SELECT RAWTOHEX(f.IDPESSOA) FROM TB1124_FALECIMENTO f
)


"""

# =========================
# Caracteres indesejados
# =========================
CARACTERES_INDESEJADOS = ['‰', '–', 'Š', 'ƒ', '‡', '”', '“', 'º', 'ª', '’']

def normalize_text(texto):
    if texto is None:
        return ''
    texto = str(texto).strip()
    for c in CARACTERES_INDESEJADOS:
        texto = texto.replace(c, '')
    return texto

# =========================
# Função para dividir em lotes
# =========================
def chunked_iterable(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

# =========================
# Garantir que a tabela exista
# =========================
def criar_tabela_se_nao_existir(pg_cursor, pg_conn):
    pg_cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.endereco_teste (
        idpessoa character varying NOT NULL PRIMARY KEY,
        dtatualizacao character varying(200),
        enderecorua character varying(500),
        endereconumero character varying(15),
        enderecocomplemento character varying(500),
        enderecobairro character varying(300),
        cdcidade text,
        cep character varying(8),
        dddtel1 character varying(4),
        telefone1 character varying(18),
        dddtel2 character varying(4),
        telefone2 character varying(18),
        dddcel character varying(4),
        celular character varying(18),
        email character varying(500),
        dtatualizacaoemail character varying(25)
    );
    """)
    pg_conn.commit()
    logging.info("Tabela public.endereco_teste verificada/criada com sucesso.")


# =========================
# Extração Oracle
# =========================
def extrair_dados_oracle(oracle_cursor):
    try:
        logging.info("Extraindo dados do Oracle...")
        oracle_cursor.execute(QUERY_ORACLE)
        dados = oracle_cursor.fetchall()
        logging.info(f"Foram encontrados {len(dados)} registros no Oracle.")
        return [tuple(normalize_text(col) for col in row) for row in dados]
    except Exception as e:
        logging.error(f"Erro ao extrair dados do Oracle: {e}")
        return []

# =========================
# Upsert PostgreSQL
# =========================
def upsert_postgres(pg_cursor, pg_conn, dados, batch_size=1000):
    if not dados:
        logging.warning("Nenhum dado para inserir/atualizar.")
        return

    pg_cursor.execute("SET search_path TO public;")

    for batch_num, batch in enumerate(chunked_iterable(dados, batch_size), start=1):
        for idx, d in enumerate(batch, start=1):
            try:
                # Mapear campos
                idpessoa = d[0]
                dtataulizacao = d[1] if d[1] else None
                enderecorua, endereconumero, enderecocomplemento = d[2], d[3], d[4]
                endercobairro, cdcidade, cep = d[5], d[6], d[7]
                dddtel1, telefone1, dddcel, celular, email = d[8], d[9], d[10], d[11], d[12]

                # Checar se existe
                pg_cursor.execute("SELECT * FROM endereco_teste WHERE idpessoa = %s", (idpessoa,))
                existente = pg_cursor.fetchone()

                if existente:
                    # Atualiza somente se houver diferença
                    if existente[1:] != (
                        dtataulizacao, enderecorua, endereconumero, enderecocomplemento,
                        endercobairro, cdcidade, cep, dddtel1, telefone1,
                        None, None, dddcel, celular, email, None
                    ):
                        pg_cursor.execute("""
                            UPDATE endereco_teste SET
                                dtataulizacao = %s,
                                enderecorua = %s,
                                endereconumero = %s,
                                enderecocomplemento = %s,
                                endercobairro = %s,
                                cdcidade = %s,
                                cep = %s,
                                dddtel1 = %s,
                                telefone1 = %s,
                                dddcel = %s,
                                celular = %s,
                                email = %s
                            WHERE idpessoa = %s
                        """, (
                            dtataulizacao, enderecorua, endereconumero, enderecocomplemento,
                            endercobairro, cdcidade, cep, dddtel1, telefone1,
                            dddcel, celular, email, idpessoa
                        ))
                else:
                    # Inserir
                    pg_cursor.execute("""
                        INSERT INTO endereco_teste (
                            idpessoa, dtataulizacao, enderecorua, endereconumero,
                            enderecocomplemento, endercobairro, cdcidade, cep,
                            dddtel1, telefone1, dddcel, celular, email
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        idpessoa, dtataulizacao, enderecorua, endereconumero,
                        enderecocomplemento, endercobairro, cdcidade, cep,
                        dddtel1, telefone1, dddcel, celular, email
                    ))

            except Exception as e:
                logging.error(f"Erro no registro {idx + (batch_num-1)*batch_size} (idpessoa={idpessoa}): {e}")
                pg_conn.rollback()
                continue

        pg_conn.commit()
        logging.info(f"Batch {batch_num} com {len(batch)} registros processado.")

# =========================
# Execução principal
# =========================
if __name__ == "__main__":
    inicio = datetime.now()
    logging.info("=== Iniciando carga ENDERECO ===")

    try:
        logging.info("Conectando ao Oracle...")
        oracle_conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=ORACLE_DSN
        )
        oracle_cursor = oracle_conn.cursor()
        logging.info("Conexão com Oracle estabelecida com sucesso!")
    except Exception as e:
        logging.error(f"Erro ao conectar Oracle: {e}")
        sys.exit(1)

    try:
        logging.info("Conectando ao PostgreSQL...")
        pg_conn = psycopg2.connect(
            dbname=PG_DBNAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        pg_cursor = pg_conn.cursor()
        logging.info("Conexão com PostgreSQL estabelecida com sucesso!")
    except Exception as e:
        logging.error(f"Erro ao conectar PostgreSQL: {e}")
        oracle_cursor.close()
        oracle_conn.close()
        sys.exit(1)

    try:
        criar_tabela_se_nao_existir(pg_cursor, pg_conn)
        dados = extrair_dados_oracle(oracle_cursor)
        upsert_postgres(pg_cursor, pg_conn, dados)
    finally:
        oracle_cursor.close()
        oracle_conn.close()
        pg_cursor.close()
        pg_conn.close()
        logging.info("Conexões encerradas com sucesso.")

    fim = datetime.now()
    logging.info(f"Processo finalizado em {fim - inicio}.")
