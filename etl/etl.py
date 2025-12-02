import os
import pandas as pd
from sqlalchemy import create_engine, text
import time
import numpy as np

print("--- Iniciando Script ETL (Modo Forçado) ---")

# --- 1. Conexão com o Banco ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("Erro: DATABASE_URL não definida.")
    exit()

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Conexão com PostgreSQL (Serviço 'db') estabelecida.")
except Exception as e:
    print(f"Erro fatal ao conectar ao banco: {e}")
    exit()

# --- 2. Criação dos Schemas e Tabelas OLAP (AGORA COM DROP) ---
def recriar_schema_olap():
    print("Recriando tabelas OLAP (DROP & CREATE)...")
    with engine.connect() as conn:
        with conn.begin(): # Transação atômica para recriar tudo
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS olap"))
            
            # --- 2.1. PRIMEIRO: Destruir tabelas antigas (na ordem correta por causa das FKs) ---
            print("Apagando tabelas antigas...")
            conn.execute(text("DROP TABLE IF EXISTS olap.fato_vendas_logistica CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS olap.fato_estoque_analitico CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS olap.dim_produto CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS olap.dim_fornecedor CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS olap.dim_transportadora CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS olap.dim_tempo CASCADE"))

            # --- 2.2. SEGUNDO: Criar tabelas novas (com as colunas corretas) ---
            print("Criando tabelas novas...")
            
            # Dimensões
            conn.execute(text("""
                CREATE TABLE olap.dim_produto (
                    sku_id VARCHAR PRIMARY KEY,
                    nome_produto VARCHAR,
                    categoria VARCHAR,
                    custo_fabricacao FLOAT,
                    preco_venda FLOAT
                );
            """))
            conn.execute(text("""
                CREATE TABLE olap.dim_fornecedor (
                    forn_id VARCHAR PRIMARY KEY,
                    nome_fornecedor VARCHAR,
                    localizacao VARCHAR
                );
            """))
            conn.execute(text("""
                CREATE TABLE olap.dim_transportadora (
                    transp_id VARCHAR PRIMARY KEY,
                    nome_transportadora VARCHAR,
                    modal VARCHAR
                );
            """))
            conn.execute(text("""
                CREATE TABLE olap.dim_tempo (
                    data_id DATE PRIMARY KEY,
                    ano INT,
                    mes INT,
                    dia INT
                );
            """))

            # Fatos
            conn.execute(text("""
                CREATE TABLE olap.fato_vendas_logistica (
                    id SERIAL PRIMARY KEY,
                    data_id DATE REFERENCES olap.dim_tempo(data_id),
                    sku_id VARCHAR REFERENCES olap.dim_produto(sku_id),
                    forn_id VARCHAR REFERENCES olap.dim_fornecedor(forn_id),
                    transp_id VARCHAR REFERENCES olap.dim_transportadora(transp_id),
                    receita_total FLOAT,
                    custo_total FLOAT,
                    margem_lucro FLOAT,
                    qtd_vendida INT,
                    custo_transporte FLOAT,
                    flag_entrega_prazo INT,
                    taxa_nao_conformidade FLOAT
                );
            """))
            conn.execute(text("""
                CREATE TABLE olap.fato_estoque_analitico (
                    id SERIAL PRIMARY KEY,
                    data_id DATE REFERENCES olap.dim_tempo(data_id),
                    sku_id VARCHAR REFERENCES olap.dim_produto(sku_id),
                    nivel_estoque INT,
                    giro_estoque_mensal FLOAT,
                    risco_ruptura FLOAT
                );
            """))
        print("Schema OLAP recriado com sucesso.")

recriar_schema_olap()

# --- 3. Extração (E) ---
print("Iniciando Extração (E)...")
arquivo_csv = '/app/data/supply_chain_data.csv'

try:
    df_kaggle = pd.read_csv(arquivo_csv)
    print(f"Arquivo {arquivo_csv} lido com sucesso. {len(df_kaggle)} linhas encontradas.")
except FileNotFoundError:
    print(f"ERRO FATAL: Arquivo {arquivo_csv} não encontrado.")
    exit()
except Exception as e:
    print(f"Erro ao ler o arquivo CSV: {e}")
    exit()

# --- 4. Transformação (T) ---
print("Iniciando Transformação (T)...")
df_transformado = df_kaggle.copy()

try:
    # 4.1. Simulação da 'data_pedido'
    dias_retroativos = pd.to_timedelta(np.arange(len(df_transformado)), unit='d')
    df_transformado['data_pedido'] = pd.to_datetime('today') - dias_retroativos
    df_transformado['data_id'] = df_transformado['data_pedido'].dt.date

    # 4.2. Simulação da 'flag_entrega_prazo'
    df_transformado['flag_entrega_prazo'] = np.random.choice([0, 1], size=len(df_transformado), p=[0.15, 0.85])

    # 4.3. IDs
    df_transformado['forn_id'] = 'FORN_' + df_transformado['Supplier name'].astype('category').cat.codes.astype(str)
    df_transformado['transp_id'] = 'CAR_' + df_transformado['Shipping carriers'].astype('category').cat.codes.astype(str)
    
    # 4.4. Padronização
    df_transformado['sku_id'] = df_transformado['SKU']
    df_transformado['nome_produto'] = df_transformado['SKU']
    df_transformado['categoria'] = df_transformado['Product type']
    df_transformado['nome_fornecedor'] = df_transformado['Supplier name']
    df_transformado['localizacao'] = df_transformado['Location']
    df_transformado['nome_transportadora'] = df_transformado['Shipping carriers']
    df_transformado['modal'] = df_transformado['Transportation modes']
    df_transformado['receita_total'] = df_transformado['Revenue generated']
    df_transformado['custo_total'] = df_transformado['Manufacturing costs']
    df_transformado['custo_transporte'] = df_transformado['Shipping costs']
    df_transformado['taxa_nao_conformidade'] = df_transformado['Defect rates']
    df_transformado['qtd_vendida'] = df_transformado['Number of products sold']
    df_transformado['preco_venda'] = df_transformado['Price']
    df_transformado['custo_fabricacao'] = df_transformado['Manufacturing costs']
    df_transformado['nivel_estoque'] = df_transformado['Stock levels']
    
    # Limpeza de chaves nulas
    print("Limpando dados...")
    df_transformado.dropna(subset=['sku_id', 'forn_id', 'transp_id', 'data_id'], how='any', inplace=True)

    # --- Dimensões ---
    df_dim_produto = df_transformado[['sku_id', 'nome_produto', 'categoria', 'custo_fabricacao', 'preco_venda']].drop_duplicates(subset=['sku_id'])
    df_dim_fornecedor = df_transformado[['forn_id', 'nome_fornecedor', 'localizacao']].drop_duplicates(subset=['forn_id'])
    df_dim_transportadora = df_transformado[['transp_id', 'nome_transportadora', 'modal']].drop_duplicates(subset=['transp_id'])
    
    df_dim_tempo = pd.DataFrame({'data_id': df_transformado['data_id'].unique()})
    df_dim_tempo = df_dim_tempo.drop_duplicates().dropna()
    df_dim_tempo['data_id'] = pd.to_datetime(df_dim_tempo['data_id'])
    df_dim_tempo['ano'] = df_dim_tempo['data_id'].dt.year
    df_dim_tempo['mes'] = df_dim_tempo['data_id'].dt.month
    df_dim_tempo['dia'] = df_dim_tempo['data_id'].dt.day

    # --- Fatos ---
    df_fato = df_transformado.copy()
    df_fato['margem_lucro'] = df_fato['receita_total'] - df_fato['custo_total']
    
    colunas_fato_vendas = [
        'data_id', 'sku_id', 'forn_id', 'transp_id', 'receita_total', 'custo_total', 
        'margem_lucro', 'qtd_vendida', 'custo_transporte', 'flag_entrega_prazo', 'taxa_nao_conformidade'
    ]
    df_fato_vendas_final = df_fato[colunas_fato_vendas]

    df_fato_estoque = df_transformado[['data_id', 'sku_id', 'nivel_estoque']].copy()
    df_fato_estoque['giro_estoque_mensal'] = np.random.uniform(0.5, 5.0, size=len(df_fato_estoque))
    df_fato_estoque['risco_ruptura'] = np.random.uniform(0.01, 0.9, size=len(df_fato_estoque))

    print("Transformação concluída.")

except Exception as e:
    print(f"Erro na Transformação (T): {e}")
    exit()


# --- 5. Carga (L) ---
print("Iniciando Carga (L)...")

def carregar_tabela(df, nome_tabela, schema='olap'):
    if df.empty: return
    try:
        with engine.connect() as conn:
            with conn.begin():
                # Não precisamos de TRUNCATE aqui porque acabamos de dar DROP/CREATE nas tabelas
                # Mas mantemos para garantir caso rode duas vezes seguidas sem recriar
                conn.execute(text(f"TRUNCATE TABLE {schema}.{nome_tabela} RESTART IDENTITY CASCADE"))
                df.to_sql(nome_tabela, conn, schema=schema, if_exists='append', index=False)
        print(f"Tabela {schema}.{nome_tabela} carregada com sucesso.")
    except Exception as e:
        print(f"ERRO ao carregar tabela {schema}.{nome_tabela}: {e}")

try:
    carregar_tabela(df_dim_tempo, 'dim_tempo')
    carregar_tabela(df_dim_produto, 'dim_produto')
    carregar_tabela(df_dim_fornecedor, 'dim_fornecedor')
    carregar_tabela(df_dim_transportadora, 'dim_transportadora')
    carregar_tabela(df_fato_vendas_final, 'fato_vendas_logistica')
    carregar_tabela(df_fato_estoque, 'fato_estoque_analitico')
    
    print("Carga OLAP concluída com sucesso.")

except Exception as e:
    print(f"Erro durante a Carga (L): {e}")