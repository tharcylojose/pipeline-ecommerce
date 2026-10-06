"""
Dashboard interativo (Streamlit): mostra os pedidos brutos (vindos do
Kafka) e as métricas agregadas (calculadas pelo Airflow).

Para rodar:
    streamlit run dashboard.py
"""
import pandas as pd
import psycopg2
import streamlit as st

st.set_page_config(page_title="Pipeline de E-commerce", layout="wide")

st.title("📦 Pipeline de Pedidos — Kafka + Airflow")
st.caption("Pedidos brutos via Kafka, agregação via Airflow, tudo em PostgreSQL")


def get_connection():
    return psycopg2.connect(
        host="localhost",
        port="5433",
        dbname="pipeline_db",
        user="admin",
        password="admin123",
        client_encoding="UTF8",
    )


@st.cache_data(ttl=10)
def carregar_pedidos():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM pedidos_raw ORDER BY criado_em DESC", conn)
    conn.close()
    return df


@st.cache_data(ttl=10)
def carregar_analytics():
    conn = get_connection()
    df = pd.read_sql(
        "SELECT * FROM analytics_vendas_por_categoria ORDER BY calculado_em DESC",
        conn,
    )
    conn.close()
    return df


pedidos = carregar_pedidos()
analytics = carregar_analytics()

col1, col2, col3 = st.columns(3)
col1.metric("Total de pedidos recebidos", len(pedidos))
col2.metric("Valor total", f"R$ {pedidos['valor'].sum():,.2f}" if not pedidos.empty else "R$ 0,00")
col3.metric("Última agregação do Airflow", str(analytics["calculado_em"].max()) if not analytics.empty else "Ainda não rodou")

st.divider()

col_esq, col_dir = st.columns(2)

with col_esq:
    st.subheader("Pedidos por categoria (bruto, em tempo real)")
    if not pedidos.empty:
        st.bar_chart(pedidos["categoria"].value_counts())
    else:
        st.info("Nenhum pedido recebido ainda. Rode o producer.py e o consumer.py.")

with col_dir:
    st.subheader("Vendas por categoria (última agregação do Airflow)")
    if not analytics.empty:
        ultima_rodada = analytics[analytics["calculado_em"] == analytics["calculado_em"].max()]
        st.bar_chart(ultima_rodada.set_index("categoria")["valor_total"])
    else:
        st.info("O Airflow ainda não rodou nenhuma agregação. Espere o DAG disparar (a cada 5 min) ou dispare manualmente pela interface do Airflow.")

st.divider()

st.subheader("Últimos pedidos recebidos (bruto)")
st.dataframe(pedidos.head(20), use_container_width=True, hide_index=True)

if st.button("🔄 Atualizar dados"):
    st.cache_data.clear()
    st.rerun()
