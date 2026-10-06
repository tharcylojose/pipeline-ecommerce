# Pipeline de Pedidos de E-commerce — Kafka + Airflow + PostgreSQL

Projeto de portfólio simulando um pipeline de dados de streaming:
pedidos chegam via Kafka, são gravados em tempo real no PostgreSQL, e
o Airflow orquestra uma agregação periódica em cima desses dados.

Roda 100% local via Docker — sem AWS, sem Astronomer, sem Confluent
Cloud,

## Arquitetura

```
producer.py (gera pedidos fake)
      ↓
   Kafka (tópico "pedidos")
      ↓
consumer.py (ouve o tópico, grava no Postgres)
      ↓
PostgreSQL (tabela pedidos_raw)
      ↓
Airflow (DAG agregacao_vendas, roda a cada 5 min)
      ↓
PostgreSQL (tabela analytics_vendas_por_categoria)
      ↓
dashboard.py (Streamlit, mostra tudo)
```

## Aviso sobre recursos

Kafka + Airflow juntos consomem bastante RAM (recomendado pelo menos
8GB livres no Docker Desktop). Se sua máquina for mais limitada, pode
demorar mais pra subir ou ficar lento.

## Como rodar

### 1. Subir a infraestrutura

```bash
docker compose up -d
```

Isso sobe Zookeeper, Kafka, PostgreSQL e Airflow. Pode demorar alguns
minutos na primeira vez (baixa várias imagens grandes).

Confira que tudo subiu:
```bash
docker ps
```

### 2. Acessar o Airflow

Abra http://localhost:8080 no navegador.
Login: `admin` / Senha: `admin123`

Você deve ver o DAG `agregacao_vendas` na lista. Ative ele (o
botãozinho ao lado do nome) para que rode automaticamente a cada 5
minutos.

### 3. Instalar as dependências Python

```bash
python -m venv venv
venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### 4. Rodar o producer (gera pedidos)

Em um terminal:
```bash
cd python
python producer.py
```

Deixe rodando — ele gera um pedido novo a cada 2 segundos.

### 5. Rodar o consumer (grava no banco)

Em **outro** terminal (com o venv ativado também):
```bash
cd python
python consumer.py
```

Deixe rodando também — ele grava cada pedido recebido no PostgreSQL.

### 6. Ver o dashboard

Em um **terceiro** terminal:
```bash
cd python
streamlit run dashboard.py
```

Isso abre o dashboard no navegador, mostrando os pedidos chegando em
tempo real e as agregações do Airflow.

## Estrutura de pastas

```
pipeline-ecommerce/
├── docker-compose.yml
├── requirements.txt
├── sql/
│   └── schema.sql
├── dags/
│   └── agregacao_dag.py      ← DAG do Airflow
└── python/
    ├── producer.py             ← gera pedidos, envia pro Kafka
    ├── consumer.py              ← lê do Kafka, grava no Postgres
    └── dashboard.py              ← visualização com Streamlit
```

## Parar tudo

```bash
docker compose stop
```

Pra apagar os dados e recomeçar do zero:
```bash
docker compose down -v
```
