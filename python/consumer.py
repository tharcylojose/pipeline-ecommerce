"""
Consumer Kafka: lê os pedidos do tópico 'pedidos' e grava no PostgreSQL
(tabela pedidos_raw). Roda continuamente, ouvindo novas mensagens.
"""
import json

import psycopg2
from kafka import KafkaConsumer


def get_postgres_connection():
    return psycopg2.connect(
        host="localhost",
        port="5433",
        dbname="pipeline_db",
        user="admin",
        password="admin123",
        client_encoding="UTF8",
    )


def salvar_pedido(conn, pedido):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pedidos_raw (pedido_id, cliente, categoria, valor)
        VALUES (%s, %s, %s, %s)
        """,
        (pedido["pedido_id"], pedido["cliente"], pedido["categoria"], pedido["valor"]),
    )
    conn.commit()
    cur.close()


if __name__ == "__main__":
    consumer = KafkaConsumer(
        "pedidos",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="consumer-pedidos",
    )

    conn = get_postgres_connection()
    print("Ouvindo o tópico 'pedidos' (Ctrl+C pra parar)...")

    try:
        for mensagem in consumer:
            pedido = mensagem.value
            salvar_pedido(conn, pedido)
            print(f"Gravado no PostgreSQL: {pedido}")
    except KeyboardInterrupt:
        print("\nParado pelo usuário.")
    finally:
        conn.close()
