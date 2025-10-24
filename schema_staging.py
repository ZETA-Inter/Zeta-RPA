# Importações
import psycopg2
import os
from dotenv import load_dotenv

# -=-=-=-=-=-=-=-=-=-=-=-=-= FUNÇÕES -=-=-=-=-=-=-=-=-=-=-=-=-=
# Criação do schema staging caso não exista
def create_staging_schema(conn, cur):
    # Query para a criação do schema "staging"
    create_schema_query = "CREATE SCHEMA IF NOT EXISTS staging;"

    try:
        # Executando a query
        cur.execute(create_schema_query)
        # Confirma as alterações no banco de dados
        conn.commit()

        print("Schema 'staging' criado com sucesso.")

    except Exception as e:
        print(f"Erro ao criar o schema 'staging': {e}")
        conn.rollback()


# Como base na tabela infomation_schema pega todas as tabelas e suas respectivas colunas, tipo do dado e se é nulo ou não
def get_tables_columns(cur):
    # Pega todas as tabelas do banco
    cur.execute('''
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
    ''')
    tables = cur.fetchall()

    # cria um dicionário com a estrutura do banco
    structure = {}
    for table in tables:
        table_name = table[0]
        cur.execute(f'''
        SELECT column_name, data_type, is_nullable, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        ''')
        columns = cur.fetchall()

        # Armazena as colunas nesse dicionário
        structure[table_name] = columns
        
    return structure


# Cria as querys de CREATE TABLE e executa isso no banco do segundo ano, no schema staging
def create_tables(cur, conn, structure, schema="staging"):
    for table, columns in structure.items():
        column_line = []

        for column in columns:
            column_name = column[0] # Pegar o nome da coluna
            data_type = column[1].upper() # Pegar o tipo da coluna
            nullable = "NOT NULL" if column[2] == "NO" else "" # list comprehension para verificar se é nulo ou não
            character_maximum_length = column[3] # Pegar o tamanho máximo do caractere (caso seja varchar)

            if column_name.lower() == "id": # Verifica se o nome da coluna é "id"
                data_type = "SERIAL PRIMARY KEY"
            else: 
                if data_type.lower() == "character varying": # Verifica se o tipo é varchar e trasnforma em TEXT
                    data_type = f"VARCHAR ({character_maximum_length})" 
                
            column_line.append(f"{column_name} {data_type} {nullable}")

        # Junta as as linhas separando-as com vírgulas (,)
        column_definition = ", ".join(column_line)

        # Cria a query para criar as tabelas com as colunas
        create_query = f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({column_definition});"

        try:
            cur.execute(create_query)
            conn.commit()
            print(f"A tabela {table} foi criada com sucesso.")
        except Exception as e:
            print(f"Erro ao criar a tabela {table}: {e}. Tente novamente mais tarde.\n")

# Retorna as FKs do banco de origem (banco do primeiro ano)
def get_foreign_keys(cur, schema="public"):
    query_fk = """ 
    SELECT
        t.relname AS tabela_origem,
        a.attname AS coluna_origem,
        tr.relname AS tabela_referenciada,
        ar.attname AS coluna_referenciada,
        tc.conname AS nome_constraint
    FROM pg_constraint tc
    JOIN pg_class t ON t.oid = tc.conrelid
    JOIN pg_namespace tn ON tn.oid = t.relnamespace
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(tc.conkey)
    JOIN pg_class tr ON tr.oid = tc.confrelid
    JOIN pg_namespace trn ON trn.oid = tr.relnamespace
    JOIN pg_attribute ar ON ar.attrelid = tr.oid AND ar.attnum = ANY(tc.confkey)
    WHERE tc.contype = 'f'
      AND tn.nspname = %s
    ORDER BY t.relname, tc.conname;  
    """
     # Executa a query para pegar as FKs
    cur.execute(query_fk, (schema,))
    foreign_keys = cur.fetchall()
    return foreign_keys

# Cria as FKs no banco de destino (banco do segundo ano)
def create_foreign_keys(cur, conn, foreign_keys, schema="staging"):
    # Itera cada FK e cria a query de ALTER TABLE para adicionar a FK
    for fk in foreign_keys:
        tabela_origem = fk[0]
        coluna_origem = fk[1]
        tabela_referenciada = fk[2]
        coluna_referenciada = fk[3]
        nome_constraint = fk[4]

        alter_query = f"""
        ALTER TABLE {schema}.{tabela_origem}
        ADD CONSTRAINT {nome_constraint}
        FOREIGN KEY ({coluna_origem})
        REFERENCES {schema}.{tabela_referenciada} ({coluna_referenciada});
        """

        try:
            # Executa o ALTER TABLE para criar as FKs nas tabelas do segundo ano (no schema staging)
            cur.execute(alter_query)
            conn.commit()
            print(f"FK {nome_constraint} criada com sucesso.\n")
        except Exception as e:
            print(f"Erro ao criar FK {nome_constraint}: {e}\n")
            cur.connection.rollback()

# função de verificar se foi criado uma coluna
def sync_table_structure(cur_src, cur_dest, conn_dest, schema="staging"):
    query_tables = """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public';
    """
    cur_src.execute(query_tables)
    tables = [table[0] for table in cur_src.fetchall()]

    for table in tables:
        # Colunas do banco origem - primeiro ano
        cur_src.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s;
            """, ( table,)
        )
        src_rows = cur_src.fetchall()
        src_cols = {row[0]: row[1] for row in src_rows}
        # print(f"Colunas do primeiro: {src_cols}")

        # Colunas do banco destino - segundo ano (staging)
        cur_dest.execute(f"""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
            """, (schema, table)
        )
        dest_rows = cur_dest.fetchall()
        dest_cols = {row[0]: row[1] for row in dest_rows}
        # print(f"Colunas do segundo: {dest_cols}")

        # Normalização: trabalhar com nomes lower/strip para comparar
        src_cols_norm = {
            col_name.strip().lower(): (col_name, data_type, char_max)
            for col_name, data_type, char_max in src_rows
        }
        dest_cols_norm = {
            col_name.strip().lower(): (col_name, data_type, char_max)
            for col_name, data_type, char_max in dest_rows
        }

        # Descobre quais colunas existem no origem e não no destino (colunas que serão adicionadas)
        missing_norm = {k: v for k, v in src_cols_norm.items() if k not in dest_cols_norm}

        # Descobre quais colunas existem no destino e não no origem (colunas que serão removidas)
        removed_norm = {k: v for k, v in dest_cols_norm.items() if k not in src_cols_norm}

        # Adiciona as colunas que estão faltando no banco do segundo ano
        for norm_name, (orig_colname, data_type, char_max) in missing_norm.items():
            col_to_add = orig_colname
            data_type = f"VARCHAR ({char_max})" if data_type.lower() == "character varying" else data_type.lower()

            print(f"Adicionando coluna '{col_to_add}' ({data_type}) na tabela '{schema}.{table}'...")
            alter_query = f"ALTER TABLE {schema}.{table} ADD COLUMN {col_to_add} {data_type};"
            try:
                cur_dest.execute(alter_query)
                conn_dest.commit()
                print(f"Coluna '{col_to_add}' adicionada.")

            except Exception as e:
                conn_dest.rollback()
                print(f"Erro ao adicionar coluna {col_to_add} em {table}: {e}")


        for norm_name, (orig_dest_colname, data_type, char_max) in removed_norm.items():
            if norm_name == "id":
                continue  # Não remove a coluna 'id'

            col_to_drop = orig_dest_colname
            print(f"Removendo coluna '{col_to_drop}' da tabela '{schema}.{table}'...")
            query_drop = f"ALTER TABLE {schema}.{table} DROP COLUMN IF EXISTS {col_to_drop} CASCADE;"
            try:
                cur_dest.execute(query_drop)
                conn_dest.commit()
                print(f"Coluna '{col_to_drop}' removida com sucesso de '{schema}.{table}'.")
            except Exception as e:
                conn_dest.rollback()
                print(f"Erro ao remover coluna {col_to_drop} em {table}: {e}")


# Transfere os dados do banco do primeiro ano para o schema staging do segundo ano
def transfer_data(cur_src, cur_dest, conn_dest, schema="staging"):
    # Query que retorna todas as tabelas
    query_tables = """ 
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = %s;
    """
    # Exexucta a query no banco do primeiro ano
    cur_src.execute(query_tables, ('public',))
    tables_src = cur_src.fetchall()
    tables_src = [table[0] for table in tables_src]

    # Exexucta a query no banco do segundo ano
    cur_src.execute(query_tables, (schema,))
    tables_dest = cur_src.fetchall()
    tables_dest = [table[0] for table in tables_dest]

    for table_name in tables_src:
        # print(f"Transferindo dados da tabela '{table_name}'...")

        # Executa um SELECT em cada tabela do primeiro ano
        query_select = f"SELECT * FROM public.{table_name};"
        cur_src.execute(query_select)
        rows_src = cur_src.fetchall()

        # Executa um SELECT em cada tabela do primeiro ano
        query_select = f"SELECT * FROM {schema}.{table_name};"
        cur_dest.execute(query_select)
        rows_dest = cur_dest.fetchall()

        # Caso a tabela esteja vazia
        if not rows_src:
            print(f"Nenhum dado encontrado em '{table_name}'")
            continue

        # Caso estiver diferente as tabelas
        if len(rows_src) == len(rows_dest):
            print(f"Tabela '{table_name}' já está atualizada. Pulando...")
            continue
        else:
            print(f"Transferindo dados da tabela '{table_name}'...")

            # Prepara a query do INSERT com base nas colunas da tabela
            colnames = [desc[0] for desc in cur_src.description]
            columns_str = ", ".join(colnames)
            placeholders = ", ".join(["%s"] * len(colnames))

            insert_query = f"""
                INSERT INTO {schema}.{table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT (id) DO NOTHING;
            """

        try:
            # Executa os INSERTs nas tabelas do segundo ano
            cur_dest.executemany(insert_query, rows_src)

            # Commita as alterações realizadas
            conn_dest.commit()
            print(f"{len(rows_src)} registros inseridos em {schema}.{table_name}.")
        except Exception as e:
            # Caso ocorra algum erro, desfaz as alterações
            conn_dest.rollback()
            print(f"Erro ao inserir em {table_name}: {e}")

# Função principal
if __name__ == "__main__":
    # Carrega as envs
    load_dotenv()

    # Declarando as variáveis do banco do primeiro ano
    conn1 = psycopg2.connect(os.getenv("POSTGRES_URL_1"))
    cur1 = conn1.cursor()

    # Declarando as variáveis do banco do primeiro ano
    conn2 = psycopg2.connect(os.getenv("POSTGRES_URL_2"))
    cur2 = conn2.cursor()

    # Criando o schema de staging (caso ele não exista)
    print("Criando o schema 'staging':")
    create_staging_schema(conn2, cur2)

    # Montando a estrutura das tabelas
    structure = get_tables_columns(cur1)

    # Criando as tabelas no postgres
    print("\nCriando as tabelas no schema 'staging':")
    create_tables(cur2, conn2, structure, 'staging')

    # Criando as FKs no banco do segundo ano (não funciona essas funções)
    foreign_keys = get_foreign_keys(cur1, 'public')

    # print("\nCriando as Foreign Keys no schema 'staging':")
    create_foreign_keys(cur2, conn2, foreign_keys, 'staging')

    # Sincronizando a estrutura das tabelas (caso tenha adicionado alguma coluna nas tabelas)
    print("\nSincronizando a estrutura das tabelas no schema 'staging'")
    sync_table_structure(cur1, cur2, conn2, 'staging')

    # Transferindo os dados 
    print("\nTransferindo os dados para o schema 'staging':")
    transfer_data(cur1, cur2, conn2, 'staging')

    # Fecha as conexões
    cur1.close()
    conn1.close()
    cur2.close()
    conn2.close()