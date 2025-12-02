import os
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, DateTime, func, insert, update, select
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
# --- Configuração do Banco de Dados (SQLAlchemy) ---

# Pega a URL do banco do Docker Compose
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:admin@localhost:5432/sige_db")

engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Define as tabelas (Modelo Físico OLTP)
tbl_produto = Table(
    "produto", metadata,
    Column("sku_id", String, primary_key=True),
    Column("nome", String, nullable=False),
    Column("nivel_minimo", Integer, default=0),
    Column("nivel_maximo", Integer, default=1000),
    Column("custo_fabricacao", Float, default=0)
)

tbl_saldo_estoque = Table(
    "saldo_estoque", metadata,
    Column("sku_id", String, primary_key=True), # FK para produto
    Column("saldo_atual", Integer, default=0),
    Column("ultima_atualizacao", DateTime, default=func.now())
)

tbl_movimentacao_estoque = Table(
    "movimentacao_estoque", metadata,
    Column("mov_id", Integer, primary_key=True, autoincrement=True),
    Column("sku_id", String, nullable=False), # FK para produto
    Column("data_movimentacao", DateTime, default=func.now()),
    Column("tipo_movimentacao", String(1), nullable=False), # 'E' para Entrada, 'S' para Saída
    Column("quantidade", Integer, nullable=False)
)

# Cria as tabelas se não existirem
metadata.create_all(engine)

# Gerenciador de Sessão
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Modelos de Dados Pydantic (Validação) ---

class ProdutoCreate(BaseModel):
    sku_id: str
    nome: str
    nivel_minimo: int
    nivel_maximo: int
    custo_fabricacao: float

class MovimentacaoCreate(BaseModel):
    sku_id: str
    tipo_movimentacao: str # 'E' ou 'S'
    quantidade: int

# --- Inicialização da API ---
app = FastAPI(title="SIGE API", description="API para controle de estoque do SIGE")


# --- Endpoints da API (Casos de Uso) ---

@app.post("/produtos", status_code=status.HTTP_201_CREATED, summary="CU06: Cadastrar Item de Estoque")
def cadastrar_produto(produto: ProdutoCreate):
    """
    Cadastra um novo produto (SKU) e inicializa seu saldo em zero.
    """
    with get_db() as db:
        # 1. Validar Unicidade (Isso abre uma transação implícita)
        query_check = select(tbl_produto).where(tbl_produto.c.sku_id == produto.sku_id)
        if db.execute(query_check).first():
            raise HTTPException(status_code=400, detail="SKU já cadastrado.")

        try:
            # 2. Insere na tabela Produto
            stmt_prod = insert(tbl_produto).values(
                sku_id=produto.sku_id,
                nome=produto.nome,
                nivel_minimo=produto.nivel_minimo,
                nivel_maximo=produto.nivel_maximo,
                custo_fabricacao=produto.custo_fabricacao
            )
            db.execute(stmt_prod)

            # 3. Inicializa o saldo em zero
            stmt_saldo = insert(tbl_saldo_estoque).values(
                sku_id=produto.sku_id,
                saldo_atual=0,
                ultima_atualizacao=func.now()
            )
            db.execute(stmt_saldo)
            
            # 4. COMMIT EXPLÍCITO (Confirma a transação)
            db.commit()
            
            return {"message": "Produto cadastrado e saldo inicializado com sucesso.", "sku": produto.sku_id}
        
        except Exception as e:
            db.rollback() # Desfaz em caso de erro
            # Se for um erro HTTP já levantado, relança ele
            if isinstance(e, HTTPException):
                raise e
            raise HTTPException(status_code=500, detail=f"Erro no banco de dados: {e}")


@app.post("/movimentacoes", status_code=status.HTTP_201_CREATED, summary="CU07/CU08: Lançar Entrada/Saída")
def lancar_movimentacao(mov: MovimentacaoCreate):
    """
    Registra uma movimentação (Entrada ou Saída) e atualiza o saldo.
    """
    if mov.tipo_movimentacao not in ['E', 'S']:
        raise HTTPException(status_code=400, detail="Tipo de movimentação inválido. Use 'E' para Entrada ou 'S' para Saída.")

    with get_db() as db:
        try:
            # 1. Validar Existência e Travar Linha (SELECT FOR UPDATE)
            # Isso abre a transação implícita
            query_saldo = select(tbl_saldo_estoque).where(tbl_saldo_estoque.c.sku_id == mov.sku_id).with_for_update()
            saldo_atual_row = db.execute(query_saldo).first()
            
            if not saldo_atual_row:
                raise HTTPException(status_code=404, detail="SKU não encontrado no saldo.")
            
            saldo_atual = saldo_atual_row.saldo_atual
            novo_saldo = 0

            if mov.tipo_movimentacao == 'E':
                novo_saldo = saldo_atual + mov.quantidade
            
            elif mov.tipo_movimentacao == 'S':
                if mov.quantidade > saldo_atual:
                    raise HTTPException(status_code=400, detail=f"Estoque insuficiente. Saldo atual: {saldo_atual}")
                novo_saldo = saldo_atual - mov.quantidade

            # 2. Registra a movimentação
            stmt_mov = insert(tbl_movimentacao_estoque).values(
                sku_id=mov.sku_id,
                tipo_movimentacao=mov.tipo_movimentacao,
                quantidade=mov.quantidade
            )
            db.execute(stmt_mov)

            # 3. Atualizar Saldo
            stmt_saldo_update = update(tbl_saldo_estoque).where(
                tbl_saldo_estoque.c.sku_id == mov.sku_id
            ).values(
                saldo_atual=novo_saldo,
                ultima_atualizacao=func.now()
            )
            db.execute(stmt_saldo_update)

            # 4. COMMIT EXPLÍCITO
            db.commit()

            # 5. Lógica de Alerta (Pode ser feita após o commit, apenas leitura)
            alerta_minimo = False
            if mov.tipo_movimentacao == 'S':
                query_produto = select(tbl_produto).where(tbl_produto.c.sku_id == mov.sku_id)
                produto_row = db.execute(query_produto).first()
                if produto_row and novo_saldo < produto_row.nivel_minimo:
                    alerta_minimo = True

            return {
                "message": f"Movimentação '{mov.tipo_movimentacao}' registrada.",
                "sku": mov.sku_id,
                "novo_saldo": novo_saldo,
                "alerta_estoque_minimo": alerta_minimo
            }

        except Exception as e:
            db.rollback()
            if isinstance(e, HTTPException):
                raise e
            raise HTTPException(status_code=500, detail=f"Erro no banco de dados: {e}")


@app.get("/saldo/{sku_id}", summary="CU09: Consultar Saldo Atual")
def consultar_saldo(sku_id: str):
    """
    Consulta o saldo em tempo real de um SKU específico.
    """
    with get_db() as db:
        query = select(tbl_saldo_estoque).where(tbl_saldo_estoque.c.sku_id == sku_id)
        saldo_row = db.execute(query).first()
        
        if not saldo_row:
            raise HTTPException(status_code=404, detail="SKU não encontrado.")
        
        # --- CORREÇÃO AQUI ---
        # Converte a linha do banco (Row) para um dicionário Python (dict)
        # Isso evita o erro de serialização do FastAPI
        return dict(saldo_row._mapping)

@app.get("/produtos", summary="Listar todos os produtos e saldos")
def listar_produtos():
    """
    Retorna a lista completa de produtos cadastrados com seus saldos atuais.
    """
    with get_db() as db:
        try:
            # Faz um JOIN entre Produto e SaldoEstoque
            join_stmt = tbl_produto.join(tbl_saldo_estoque, tbl_produto.c.sku_id == tbl_saldo_estoque.c.sku_id)
            
            query = select(
                tbl_produto.c.sku_id,
                tbl_produto.c.nome,
                tbl_produto.c.custo_fabricacao,
                tbl_produto.c.nivel_minimo,
                tbl_saldo_estoque.c.saldo_atual,
                tbl_saldo_estoque.c.ultima_atualizacao
            ).select_from(join_stmt)

            result = db.execute(query).fetchall()
            
            # --- CORREÇÃO AQUI ---
            # Transformamos cada linha explicitamente usando _mapping
            lista_produtos = []
            for row in result:
                # Acessamos os dados via dicionário seguro (_mapping)
                dados = row._mapping
                lista_produtos.append({
                    "SKU": dados['sku_id'],
                    "Nome": dados['nome'],
                    "Saldo Atual": dados['saldo_atual'],
                    "Nível Mínimo": dados['nivel_minimo'],
                    "Custo (R$)": dados['custo_fabricacao'],
                    "Última Atualização": dados['ultima_atualizacao']
                })
            
            return lista_produtos
        
        except Exception as e:
            # Imprime o erro no log para facilitar o debug
            print(f"Erro ao listar produtos: {e}")
            raise HTTPException(status_code=500, detail=f"Erro interno ao listar produtos: {str(e)}")

# Função que roda o ETL
def job_etl():
    print("Cron Interno: Iniciando ETL...")
    subprocess.run(["python", "etl.py"])
    print("Cron Interno: ETL Finalizado.")

# Configura o agendador
scheduler = BackgroundScheduler()
# Define para rodar a cada 60 minutos (ou o tempo que quiser)
scheduler.add_job(job_etl, 'interval', minutes=60)
scheduler.start()

# ... resto do código da API (app = FastAPI...) ...