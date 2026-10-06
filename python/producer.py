"""
Producer Kafka: gera pedidos fake de e-commerce e envia pro tópico 'pedidos'.
Simula o tráfego que chegaria de um site de verdade.
"""
import json
import random
import time
import uuid

from faker import Faker
from kafka import KafkaProducer

fake = Faker("pt_BR")

CATEGORIAS = ["Eletronicos", "Roupas", "Livros", "Casa", "Esportes"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def gerar_pedido():
    return {
        "pedido_id": str(uuid.uuid4())[:8],
        "cliente": fake.name(),
        "categoria": random.choice(CATEGORIAS),
        "valor": round(random.uniform(20, 800), 2),
    }


if __name__ == "__main__":
    print("Enviando pedidos pro Kafka (Ctrl+C pra parar)...")
    try:
        while True:
            pedido = gerar_pedido()
            producer.send("pedidos", pedido)
            print(f"Enviado: {pedido}")
            time.sleep(2)  # um pedido novo a cada 2 segundos
    except KeyboardInterrupt:
        print("\nParado pelo usuário.")
    finally:
        producer.flush()
        producer.close()
