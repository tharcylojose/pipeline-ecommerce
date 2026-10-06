-- Tabela de pedidos brutos, alimentada pelo consumer Kafka
CREATE TABLE IF NOT EXISTS pedidos_raw (
    id SERIAL PRIMARY KEY,
    pedido_id VARCHAR(50) NOT NULL,
    cliente VARCHAR(150) NOT NULL,
    categoria VARCHAR(50) NOT NULL,
    valor NUMERIC(10, 2) NOT NULL,
    criado_em TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Tabela agregada, alimentada pelo DAG do Airflow
CREATE TABLE IF NOT EXISTS analytics_vendas_por_categoria (
    id SERIAL PRIMARY KEY,
    categoria VARCHAR(50) NOT NULL,
    total_pedidos INT NOT NULL,
    valor_total NUMERIC(12, 2) NOT NULL,
    calculado_em TIMESTAMP NOT NULL DEFAULT NOW()
);
