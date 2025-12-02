import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests # Para chamar a API FastAPI
import os
from sqlalchemy import create_engine, text

# --- Configuração ---

st.set_page_config(layout="wide", page_title="SIGE - Cadeia de Suprimentos")
st.title("Sistema de Informações Gerenciais (SIGE)")

# Pega as URLs dos serviços do Docker Compose (com fallback para localhost)
API_URL = os.getenv("API_URL", "http://localhost:8000")
DATABASE_URL_OLAP = os.getenv("DATABASE_URL", "postgresql://admin:admin@localhost:5432/sige_db")

# Conexão com o banco (para os dashboards)
# Usamos 'db' como host, que é o nome do serviço no Docker
try:
    if DATABASE_URL_OLAP is None:
        st.error("Variável de ambiente DATABASE_URL não definida! Verifique o docker-compose.yml")
        engine_olap = None
    else:
        # Esta linha agora usa a string de conexão correta vinda do Docker
        engine_olap = create_engine(DATABASE_URL_OLAP)

except Exception as e:
    st.error(f"Erro ao conectar ao banco OLAP: {e}")
    # Cria um engine nulo para evitar que o app quebre se o banco não estiver pronto
    engine_olap = None


# --- Funções de Chamada à API (Módulo Transacional) ---

def api_cadastrar_produto(sku, nome, min, max, custo):
    url = f"{API_URL}/produtos"
    data = {
        "sku_id": sku,
        "nome": nome,
        "nivel_minimo": min,
        "nivel_maximo": max,
        "custo_fabricacao": custo
    }
    try:
        response = requests.post(url, json=data)
        if response.status_code == 201:
            st.success(f"Produto {sku} cadastrado com sucesso!")
        else:
            st.error(f"Erro ao cadastrar: {response.json().get('detail')}")
    except requests.exceptions.ConnectionError:
        st.error(f"Erro de conexão: Não foi possível conectar à API em {API_URL}")

def api_lancar_movimentacao(sku, tipo, qtd):
    tipo_map = {"Entrada": "E", "Saída": "S"}
    url = f"{API_URL}/movimentacoes"
    data = {
        "sku_id": sku,
        "tipo_movimentacao": tipo_map[tipo],
        "quantidade": qtd
    }
    try:
        response = requests.post(url, json=data)
        if response.status_code == 201:
            st.success(f"Movimentação registrada! Novo Saldo: {response.json().get('novo_saldo')}")
            if response.json().get('alerta_estoque_minimo'):
                st.warning("ALERTA: O estoque deste item está abaixo do nível mínimo!")
        else:
            st.error(f"Erro na movimentação: {response.json().get('detail')}")
    except requests.exceptions.ConnectionError:
        st.error(f"Erro de conexão: Não foi possível conectar à API em {API_URL}")

def api_consultar_saldo(sku):
    if not sku:
        st.warning("Por favor, insira um SKU.")
        return
    url = f"{API_URL}/saldo/{sku}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            st.metric(label=f"Saldo do SKU: {data['sku_id']}", value=data['saldo_atual'])
            st.caption(f"Última atualização: {data['ultima_atualizacao']}")
        else:
            st.error(f"Erro ao consultar: {response.json().get('detail')}")
    except requests.exceptions.ConnectionError:
        st.error(f"Erro de conexão: Não foi possível conectar à API em {API_URL}")

def api_listar_todos_produtos():
    url = f"{API_URL}/produtos"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return pd.DataFrame(response.json())
        else:
            st.error("Erro ao buscar lista de produtos.")
            return pd.DataFrame()
    except requests.exceptions.ConnectionError:
        st.error(f"Erro de conexão com API em {API_URL}")
        return pd.DataFrame()

# --- Layout da Aplicação (Navegação) ---

st.sidebar.title("Navegação")

# 1. Define as opções disponíveis
OPCOES_MODULO = ["Dashboards (BI)", "Controle de Estoque (Operacional)"]

# 2. Verifica a URL para ver se já existe uma seleção salva
# Tenta pegar o parâmetro '?view=' da URL. Se não existir, assume 0 (Dashboards)
param_view = st.query_params.get("view") 
index_inicial = 1 if param_view == "estoque" else 0

# 3. Cria o rádio usando o índice recuperado da URL
modo = st.sidebar.radio(
    "Selecione o Módulo:", 
    OPCOES_MODULO, 
    index=index_inicial
)

# 4. Atualiza a URL imediatamente quando o usuário troca a opção
if modo == "Controle de Estoque (Operacional)":
    st.query_params["view"] = "estoque"
else:
    st.query_params["view"] = "bi"

if modo == "Controle de Estoque (Operacional)":
    st.header("Módulo de Controle de Estoque")
    st.markdown("Execute operações de gerenciamento de inventário em tempo real.")

    tab1, tab2, tab3 = st.tabs(["Consultar Saldo (CU09)", "Lançar Movimentação (CU07/CU08)", "Cadastrar Novo Produto (CU06)"])

    # --- CU09: Consultar Saldo ---
    # --- CU09: Consultar Saldo e Visualizar Catálogo ---
    with tab1:
        st.subheader("Catálogo de Produtos e Saldos")
        
        # 1. Carrega a tabela completa
        df_produtos = api_listar_todos_produtos()
        
        if not df_produtos.empty:
            # Mostra a tabela interativa
            st.dataframe(
                df_produtos, 
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Custo (R$)": st.column_config.NumberColumn(format="R$ %.2f"),
                    "Última Atualização": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm")
                }
            )
            
            st.divider() # Linha divisória
            
            # 2. Mantém a consulta específica (Filtro rápido)
            st.markdown("##### 🔍 Consulta Detalhada por SKU")
            col_search, col_btn = st.columns([3, 1])
            with col_search:
                # Opcional: Criar um selectbox com os SKUs existentes para facilitar
                lista_skus = df_produtos["SKU"].tolist()
                sku_selecionado = st.selectbox("Selecione ou digite um SKU:", options=[""] + lista_skus)
            
            with col_btn:
                st.write("") # Espaçamento
                st.write("") 
                btn_consultar = st.button("Consultar Detalhes")

            if btn_consultar and sku_selecionado:
                api_consultar_saldo(sku_selecionado)
        else:
            st.info("Nenhum produto cadastrado ainda.")

    # --- CU07/CU08: Lançar Movimentação ---
    with tab2:
        st.subheader("Lançar Entrada ou Saída")
        with st.form("mov_form"):
            mov_sku = st.text_input("SKU")
            mov_tipo = st.selectbox("Tipo de Movimentação", ["Entrada", "Saída"])
            mov_qtd = st.number_input("Quantidade", min_value=1, step=1)
            submitted_mov = st.form_submit_button("Registrar Movimentação")
            
            if submitted_mov:
                if not mov_sku or mov_qtd <= 0:
                    st.warning("Preencha todos os campos corretamente.")
                else:
                    api_lancar_movimentacao(mov_sku, mov_tipo, mov_qtd)

    # --- CU06: Cadastrar Produto ---
    with tab3:
        st.subheader("Cadastrar Novo Produto (SKU)")
        with st.form("prod_form"):
            prod_sku = st.text_input("Código SKU (ID Único)")
            prod_nome = st.text_input("Nome/Descrição do Produto")
            prod_custo = st.number_input("Custo de Fabricação (R$)", min_value=0.0, format="%.2f")
            prod_min = st.number_input("Nível Mínimo de Estoque", min_value=0, step=1)
            prod_max = st.number_input("Nível Máximo de Estoque", min_value=1, step=1)
            
            submitted_prod = st.form_submit_button("Salvar Novo Produto")
            
            if submitted_prod:
                if not prod_sku or not prod_nome:
                    st.warning("SKU e Nome são obrigatórios.")
                else:
                    api_cadastrar_produto(prod_sku, prod_nome, prod_min, prod_max, prod_custo)


elif modo == "Dashboards (BI)":
    st.header("Dashboards de Análise (OLAP)")
    st.markdown("Visão analítica com **Recomendações Inteligentes** para tomada de decisão.")

    if engine_olap is None:
        st.error("Conexão com o banco de dados OLAP não estabelecida. Verifique a variável DATABASE_URL.")
    else:
        try:
            # ---------------------------------------------------------
            # 1. Carregar Dados de Estoque (Fato Estoque)
            # ---------------------------------------------------------
            st.subheader("1. Indicadores de Estoque")
            with engine_olap.connect() as conn:
                query_estoque = text("""
                    SELECT f.*, p.nome_produto 
                    FROM olap.fato_estoque_analitico f
                    JOIN olap.dim_produto p ON f.sku_id = p.sku_id
                    LIMIT 200
                """)
                df_estoque = pd.read_sql(query_estoque, conn)
            
            if not df_estoque.empty:
                # --- LÓGICA DE DECISÃO (ESTOQUE) ---
                def definir_acao_estoque(row):
                    if row['risco_ruptura'] > 0.70:
                        return "🚨 AÇÃO CRÍTICA: Risco iminente de falta. Emitir pedido de compra urgente!"
                    elif row['risco_ruptura'] < 0.20:
                        return "✅ ESTÁVEL: Nível seguro. Nenhuma ação necessária."
                    else:
                        return "⚠️ ATENÇÃO: Monitorar consumo diário."

                df_estoque['Recomendacao'] = df_estoque.apply(definir_acao_estoque, axis=1)
                
                # KPIs
                col1, col2 = st.columns(2)
                col1.metric("SKUs em Risco (Média)", f"{df_estoque['risco_ruptura'].mean():.1%}")
                col2.metric("Giro Médio (Mensal)", f"{df_estoque['giro_estoque_mensal'].mean():.2f}x")
                
                # Gráfico
                fig_est = px.bar(
                    df_estoque.head(20), 
                    x='nome_produto', 
                    y='nivel_estoque', 
                    color='risco_ruptura', 
                    title="Nível de Estoque por Produto (Top 20)",
                    # AQUI ESTÁ O TRUQUE: Adicionamos a recomendação no hover
                    hover_data={'Recomendacao': True, 'nivel_estoque': True, 'risco_ruptura': ':.2%'}
                )
                # Formata o tooltip para quebrar linha se for muito longo
                fig_est.update_traces(hovertemplate="<b>Produto:</b> %{x}<br><b>Estoque:</b> %{y}<br><b>Risco:</b> %{marker.color:.1%}<br><br><b>💡 %{customdata[0]}</b>")
                st.plotly_chart(fig_est, width='stretch')
            else:
                st.warning("Não há dados na Fato Estoque (OLAP). Execute o ETL.")

            # ---------------------------------------------------------
            # 2. Carregar Dados de Vendas e Logística
            # ---------------------------------------------------------
            st.subheader("2. Indicadores de Vendas, Logística e Fornecedores")
            
            with engine_olap.connect() as conn:
                query_vendas = text("""
                    SELECT 
                        f.*,
                        p.nome_produto,
                        t.nome_transportadora,
                        forn.nome_fornecedor
                    FROM olap.fato_vendas_logistica f
                    LEFT JOIN olap.dim_produto p ON f.sku_id = p.sku_id
                    LEFT JOIN olap.dim_transportadora t ON f.transp_id = t.transp_id
                    LEFT JOIN olap.dim_fornecedor forn ON f.forn_id = forn.forn_id
                    LIMIT 500
                """)
                df_vendas = pd.read_sql(query_vendas, conn)

            if not df_vendas.empty:
                col_a, col_b = st.columns(2)
                
                # --- GRÁFICO 1: RECEITA (Top Produtos) ---
                with col_a:
                    df_receita = df_vendas.groupby('nome_produto')['receita_total'].sum().nlargest(10).reset_index()
                    # Lógica: O 1º lugar é o carro-chefe
                    top_produto = df_receita.iloc[0]['nome_produto']
                    df_receita['Analise'] = df_receita['nome_produto'].apply(
                        lambda x: "⭐ CARRO-CHEFE: Garantir disponibilidade total." if x == top_produto else "Produto de Alto Desempenho."
                    )

                    fig_receita = px.bar(
                        df_receita, 
                        x='nome_produto', 
                        y='receita_total', 
                        title="Top 10 Produtos (Receita)",
                        hover_data={'Analise': True}
                    )
                    fig_receita.update_traces(hovertemplate="<b>%{x}</b><br>Receita: R$ %{y:,.2f}<br><br><b>💡 %{customdata[0]}</b>")
                    st.plotly_chart(fig_receita, width='stretch')
                
                # --- GRÁFICO 2: TRANSPORTADORAS (Custo vs Pontualidade) ---
                with col_b:
                    df_transp = df_vendas.groupby('nome_transportadora').agg(
                        Custo_Medio=('custo_transporte', 'mean'), 
                        Pontualidade=('flag_entrega_prazo', 'mean')
                    ).reset_index()

                    # Lógica de Decisão Transportadora
                    def decisao_transporte(row):
                        if row['Pontualidade'] > 0.90 and row['Custo_Medio'] < df_transp['Custo_Medio'].mean():
                            return "🏆 MELHOR OPÇÃO: Alta eficiência e baixo custo. Aumentar volume."
                        elif row['Pontualidade'] < 0.70:
                            return "❌ PROBLEMA: Pontualidade crítica. Renegociar ou substituir."
                        elif row['Custo_Medio'] > df_transp['Custo_Medio'].quantile(0.75):
                            return "💲 CUSTO ALTO: Verificar se a rota justifica o preço."
                        else:
                            return "Manter monitoramento."

                    df_transp['Decisao'] = df_transp.apply(decisao_transporte, axis=1)

                    fig_transporte = px.scatter(
                        df_transp,
                        x='Custo_Medio', 
                        y='Pontualidade', 
                        color='nome_transportadora',
                        size='Custo_Medio',
                        title="Desempenho Transportadoras (Decisão)",
                        hover_data={'Decisao': True}
                    )
                    # Personalizando o tooltip
                    fig_transporte.update_traces(hovertemplate="<b>%{x}</b><br>Pontualidade: %{y:.1%}<br>Custo Médio: R$ %{x:.2f}<br><br><b>💡 %{customdata[0]}</b>")
                    st.plotly_chart(fig_transporte, width='stretch')

                # --- GRÁFICO 3: FORNECEDORES (O Exemplo que você pediu) ---
                st.subheader("3. Ranking de Fornecedores")
                
                df_forn = df_vendas.groupby('nome_fornecedor')['taxa_nao_conformidade'].mean().reset_index()
                df_forn = df_forn.sort_values('taxa_nao_conformidade', ascending=True) 

                # Cálculos para a lógica
                melhor_taxa = df_forn['taxa_nao_conformidade'].min()
                pior_taxa = df_forn['taxa_nao_conformidade'].max()

                # LÓGICA DE DECISÃO DINÂMICA
                def decisao_fornecedor(row):
                    if row['taxa_nao_conformidade'] == melhor_taxa:
                        return f"🏆 RECOMENDADO: Menor taxa de defeito ({row['taxa_nao_conformidade']:.2f}). Aumentar compras deste parceiro."
                    elif row['taxa_nao_conformidade'] == pior_taxa:
                        return f"⚠️ AÇÃO NECESSÁRIA: Pior taxa ({row['taxa_nao_conformidade']:.2f}). Cobrar plano de ação corretiva IMEDIATO."
                    elif row['taxa_nao_conformidade'] > 0.10: # Exemplo de corte de 10%
                        return "ALERTA: Taxa de defeito acima do aceitável. Monitorar lotes."
                    else:
                        return "Fornecedor dentro da média de mercado."

                df_forn['Acao_Sugerida'] = df_forn.apply(decisao_fornecedor, axis=1)
                
                fig_fornecedor = px.bar(
                    df_forn,
                    x='nome_fornecedor',
                    y='taxa_nao_conformidade',
                    color='taxa_nao_conformidade',
                    title="Qualidade de Fornecedores (Com Recomendações)",
                    color_continuous_scale='RdYlGn_r',
                    # Passamos a coluna nova para o gráfico
                    hover_data={'Acao_Sugerida': True, 'taxa_nao_conformidade': ':.4f'}
                )
                
                # Formatando o Tooltip para destacar a Ação
                fig_fornecedor.update_traces(
                    hovertemplate="<b>%{x}</b><br>" +
                                  "Taxa de Defeito: %{y:.4f}<br><br>" +
                                  "<b>💡 %{customdata[0]}</b>" # Mostra a coluna Acao_Sugerida
                )
                
                st.plotly_chart(fig_fornecedor, width='stretch')

            else:
                st.warning("Não há dados na Fato Vendas (OLAP). Execute o ETL.")

        except Exception as e:
            st.error(f"Erro ao consultar o banco OLAP: {e}")