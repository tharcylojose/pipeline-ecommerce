"""
DAG do Airflow: roda a cada 5 minutos, lê os pedidos brutos gravados
pelo consumer Kafka, calcula vendas totais por categoria, e grava o
resultado na tabela analytics_vendas_por_categoria.
"""
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_postgres_connection():
    return psycopg2.connect(
        host="postgres",  # nome do serviço no docker-compose (mesma rede)
        port="5432",       # porta interna do container, não a exposta (5433)
        dbname="pipeline_db",
        user="admin",
        password="admin123",
        client_encoding="UTF8",
    )


def agregar_vendas_por_categoria():
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT categoria, COUNT(*) AS total_pedidos, SUM(valor) AS valor_total
        FROM pedidos_raw
        GROUP BY categoria
        """
    )
    resultados = cur.fetchall()

    for categoria, total_pedidos, valor_total in resultados:
        cur.execute(
            """
            INSERT INTO analytics_vendas_por_categoria (categoria, total_pedidos, valor_total)
            VALUES (%s, %s, %s)
            """,
            (categoria, total_pedidos, valor_total),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Agregação concluída: {len(resultados)} categorias processadas.")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="agregacao_vendas",
    default_args=default_args,
    description="Agrega pedidos por categoria a partir do Kafka/Postgres",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "agregacao"],
) as dag:

    tarefa_agregar = PythonOperator(
        task_id="agregar_vendas_por_categoria",
        python_callable=agregar_vendas_por_categoria,
    )
