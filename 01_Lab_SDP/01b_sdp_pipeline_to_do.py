# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 - SDP (Spark Declarative Pipelines)
# MAGIC ## Pipeline de Logistica - Versao TO-DO
# MAGIC
# MAGIC Neste lab voce vai construir um pipeline SDP (Spark Declarative Pipelines) completo para processar dados de logistica.
# MAGIC
# MAGIC **Importante:** Com SDP, cada tabela usa o fully qualified name (`catalog.schema.tabela`),
# MAGIC permitindo que um unico pipeline escreva em multiplos schemas de destino.
# MAGIC
# MAGIC **Camadas:**
# MAGIC - **Bronze**: Ingestao de dados brutos (Auto Loader + tabelas raw)
# MAGIC - **Silver**: Limpeza, validacao e enriquecimento
# MAGIC - **Gold**: Agregacoes para consumo analitico
# MAGIC
# MAGIC Complete os TO-DOs para finalizar o pipeline!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup e Imports

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType, ArrayType
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracao do Catalog

# COMMAND ----------

nome = spark.conf.get("pipeline.nome_participante")
catalog_name = f"workshop_logistica_{nome}"

# Paths dos volumes de streaming
path_pedidos_json = f"/Volumes/{catalog_name}/raw/pedidos_json"
path_status_json = f"/Volumes/{catalog_name}/raw/status_json"
path_nfs_json = f"/Volumes/{catalog_name}/raw/nfs_json"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # BRONZE Layer
# MAGIC Ingestao de dados brutos com Auto Loader e tabelas de referencia.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze - Pedidos (Auto Loader / Streaming)

# COMMAND ----------

# Schema dos itens de nota fiscal
item_schema = StructType([
    StructField("id_item", LongType()),
    StructField("descricao", StringType()),
    StructField("ncm", StringType()),
    StructField("quantidade", IntegerType()),
    StructField("unidade", StringType()),
    StructField("valor_unitario", DoubleType()),
    StructField("valor_total", DoubleType()),
    StructField("peso_kg", DoubleType()),
    StructField("dimensoes", StructType([
        StructField("comprimento_cm", IntegerType()),
        StructField("largura_cm", IntegerType()),
        StructField("altura_cm", IntegerType()),
    ])),
])

# Schema das notas fiscais
nf_schema = StructType([
    StructField("id_nf", LongType()),
    StructField("numero_nf", StringType()),
    StructField("id_pedido", LongType()),
    StructField("data_emissao", StringType()),
    StructField("valor_total", DoubleType()),
    StructField("chave_acesso", StringType()),
    StructField("itens", ArrayType(item_schema)),
])

# Schema completo do pedido
pedido_schema = StructType([
    StructField("id_pedido", LongType()),
    StructField("id_cliente", IntegerType()),
    StructField("data_pedido", StringType()),
    StructField("peso_total_kg", DoubleType()),
    StructField("volume_total_m3", DoubleType()),
    StructField("valor_mercadoria", DoubleType()),
    StructField("valor_frete", DoubleType()),
    StructField("tipo_frete", StringType()),
    StructField("prioridade", StringType()),
    StructField("cidade_origem", StringType()),
    StructField("uf_origem", StringType()),
    StructField("cidade_destino", StringType()),
    StructField("uf_destino", StringType()),
    StructField("notas_fiscais", ArrayType(nf_schema)),
])

@dlt.table(
    name=f"{catalog_name}.bronze.bronze_pedidos",
    comment="Pedidos ingeridos via Auto Loader a partir do volume de streaming",
    table_properties={"quality": "bronze"},
)
def bronze_pedidos():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{path_pedidos_json}/_schema")
        .option("multiLine", "true")
        .schema(pedido_schema)
        .load(path_pedidos_json)
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("data_ingestao", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze - Status Transporte (Auto Loader / Streaming)

# COMMAND ----------

status_schema = StructType([
    StructField("id_carga", LongType()),
    StructField("id_status", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("observacao", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
])

@dlt.table(
    name=f"{catalog_name}.bronze.bronze_status",
    comment="Status de transporte ingeridos via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_status():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{path_status_json}/_schema")
        .option("multiLine", "true")
        .schema(status_schema)
        .load(path_status_json)
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("data_ingestao", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze - Tabelas de Referencia (Materialized Views)

# COMMAND ----------

@dlt.table(
    name=f"{catalog_name}.bronze.bronze_clientes",
    comment="Tabela de clientes do schema raw",
    table_properties={"quality": "bronze"},
)
def bronze_clientes():
    return spark.read.table(f"{catalog_name}.raw.clientes")


@dlt.table(
    name=f"{catalog_name}.bronze.bronze_caminhoes",
    comment="Tabela de caminhoes do schema raw",
    table_properties={"quality": "bronze"},
)
def bronze_caminhoes():
    return spark.read.table(f"{catalog_name}.raw.caminhoes")


@dlt.table(
    name=f"{catalog_name}.bronze.bronze_motoristas",
    comment="Tabela de motoristas do schema raw",
    table_properties={"quality": "bronze"},
)
def bronze_motoristas():
    return spark.read.table(f"{catalog_name}.raw.motoristas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO-DO 1: Bronze - Movimento de Cargas

# COMMAND ----------

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 1: Criar materialized view bronze_movimento_cargas    ║
# ║  Dica: Use @dlt.table decorator com fully qualified name.    ║
# ║        Leia de spark.read.table(f"{catalog_name}.raw.        ║
# ║        movimento_cargas")                                     ║
# ║  Exemplo:                                                     ║
# ║    @dlt.table(                                                ║
# ║        name=f"{catalog_name}.bronze.bronze_movimento_cargas", ║
# ║        comment="...",                                         ║
# ║        table_properties={"quality": "bronze"})                ║
# ║    def bronze_movimento_cargas():                              ║
# ║        return spark.read.table(...)                            ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu codigo aqui ▼▼▼

pass

# ▲▲▲ Fim do TO-DO 1 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SILVER Layer
# MAGIC Limpeza, validacao com expectations e enriquecimento dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver - Pedidos Enriquecidos

# COMMAND ----------

@dlt.table(
    name=f"{catalog_name}.silver.silver_pedidos",
    comment="Pedidos enriquecidos com dados do cliente e validacoes de qualidade",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("id_pedido_valido", "id_pedido IS NOT NULL")
@dlt.expect("peso_positivo", "peso_total_kg > 0")
@dlt.expect_or_drop("valor_frete_valido", "valor_frete >= 0")
def silver_pedidos():
    pedidos = dlt.read_stream(f"{catalog_name}.bronze.bronze_pedidos")
    clientes = dlt.read(f"{catalog_name}.bronze.bronze_clientes")

    pedidos_enriquecidos = (
        pedidos
        .join(clientes, pedidos.id_cliente == clientes.id_cliente, "left")
        .select(
            pedidos.id_pedido,
            pedidos.id_cliente,
            clientes.razao_social,
            clientes.cnpj,
            clientes.uf.alias("uf_cliente"),
            clientes.cidade.alias("cidade_cliente"),
            pedidos.data_pedido,
            pedidos.peso_total_kg,
            pedidos.volume_total_m3,
            pedidos.valor_mercadoria,
            pedidos.valor_frete,
            pedidos.tipo_frete,
            pedidos.prioridade,
            pedidos.cidade_origem,
            pedidos.uf_origem,
            pedidos.cidade_destino,
            pedidos.uf_destino,
            pedidos.notas_fiscais,
            pedidos.arquivo_origem,
            pedidos.data_ingestao,
        )
    )

    # ╔══════════════════════════════════════════════════════════════╗
    # ║  TO-DO 2: Adicionar colunas de data (ano, mes, dia)         ║
    # ║  Dica: Use F.year(), F.month(), F.dayofmonth()              ║
    # ║  Exemplo:                                                    ║
    # ║    .withColumn("ano", F.year("data_pedido"))                 ║
    # ║    .withColumn("mes", F.month("data_pedido"))                ║
    # ║    .withColumn("dia", F.dayofmonth("data_pedido"))           ║
    # ╚══════════════════════════════════════════════════════════════╝
    # ▼▼▼ Seu codigo aqui ▼▼▼

    resultado = pedidos_enriquecidos  # Substitua esta linha adicionando as colunas

    # ▲▲▲ Fim do TO-DO 2 ▲▲▲

    return resultado

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver - Itens de Nota Fiscal (Explode)

# COMMAND ----------

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 3: Explodir os itens das notas fiscais                ║
# ║  Dica: Use F.explode() em duas etapas:                       ║
# ║    1) F.explode("notas_fiscais").alias("nf")                  ║
# ║    2) F.explode("nf.itens").alias("item")                     ║
# ║  Depois selecione os campos de pedido, nf e item.             ║
# ║  Exemplo:                                                     ║
# ║    pedidos.select("id_pedido", "id_cliente",                   ║
# ║        F.explode("notas_fiscais").alias("nf"))                 ║
# ║    .select("id_pedido", "id_cliente", "nf.id_nf",             ║
# ║        F.explode("nf.itens").alias("item"))                    ║
# ║    .select("id_pedido", "id_cliente", "id_nf",                ║
# ║        "item.descricao", "item.quantidade", ...)               ║
# ╚══════════════════════════════════════════════════════════════╝

@dlt.table(
    name=f"{catalog_name}.silver.silver_itens_nf",
    comment="Itens de nota fiscal explodidos a partir dos pedidos",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("quantidade_valida", "quantidade > 0")
def silver_itens_nf():
    pedidos = dlt.read_stream(f"{catalog_name}.bronze.bronze_pedidos")

    # ▼▼▼ Seu codigo aqui ▼▼▼

    return pedidos  # Substitua pelo codigo de explode

    # ▲▲▲ Fim do TO-DO 3 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver - Status de Transporte

# COMMAND ----------

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 4: Adicionar expectation para dropar registros         ║
# ║           onde id_carga IS NULL                                ║
# ║  Dica: Use @dlt.expect_or_drop("carga_valida",                ║
# ║        "id_carga IS NOT NULL") como decorator antes da funcao  ║
# ╚══════════════════════════════════════════════════════════════╝

@dlt.table(
    name=f"{catalog_name}.silver.silver_status_transporte",
    comment="Status de transporte enriquecido com descricao do status",
    table_properties={"quality": "silver"},
)
# ▼▼▼ Seu codigo aqui - adicione o decorator de expectation ▼▼▼

# ▲▲▲ Fim do TO-DO 4 ▲▲▲
def silver_status_transporte():
    status = dlt.read_stream(f"{catalog_name}.bronze.bronze_status")
    status_ref = spark.read.table(f"{catalog_name}.raw.status_transporte_ref")

    return (
        status
        .join(
            F.broadcast(status_ref),
            status.id_status == status_ref.id_status,
            "left",
        )
        .select(
            status.id_carga,
            status.id_status,
            status_ref.descricao.alias("descricao_status"),
            status_ref.ordem,
            F.to_timestamp(status.timestamp).alias("timestamp"),
            status.observacao,
            status.latitude,
            status.longitude,
            status.arquivo_origem,
            status.data_ingestao,
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # GOLD Layer
# MAGIC Agregacoes e metricas para consumo analitico.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold - Volume por Rota

# COMMAND ----------

@dlt.table(
    name=f"{catalog_name}.gold.gold_volume_por_rota",
    comment="Volume de pedidos agregado por rota (origem-destino)",
    table_properties={"quality": "gold"},
)
def gold_volume_por_rota():
    pedidos = dlt.read(f"{catalog_name}.silver.silver_pedidos")

    # ╔══════════════════════════════════════════════════════════════╗
    # ║  TO-DO 5: Criar a agregacao por rota                        ║
    # ║  Dica: Use .groupBy() com as colunas de origem e destino,   ║
    # ║        e .agg() com as funcoes de agregacao.                 ║
    # ║  Exemplo:                                                    ║
    # ║    pedidos.groupBy(                                          ║
    # ║        "cidade_origem", "uf_origem",                         ║
    # ║        "cidade_destino", "uf_destino"                        ║
    # ║    ).agg(                                                    ║
    # ║        F.count("id_pedido").alias("total_pedidos"),          ║
    # ║        F.sum("peso_total_kg").alias("peso_total"),           ║
    # ║        F.sum("valor_frete").alias("valor_frete_total"),      ║
    # ║        F.avg("valor_frete").alias("frete_medio"),            ║
    # ║    )                                                         ║
    # ╚══════════════════════════════════════════════════════════════╝
    # ▼▼▼ Seu codigo aqui ▼▼▼

    return pedidos  # Substitua pela agregacao

    # ▲▲▲ Fim do TO-DO 5 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold - Performance da Frota (Exemplo Completo)

# COMMAND ----------

@dlt.table(
    name=f"{catalog_name}.gold.gold_performance_frota",
    comment="Performance agregada da frota por tipo de caminhao",
    table_properties={"quality": "gold"},
)
def gold_performance_frota():
    cargas = dlt.read(f"{catalog_name}.bronze.bronze_movimento_cargas")
    caminhoes = dlt.read(f"{catalog_name}.bronze.bronze_caminhoes")

    return (
        cargas
        .join(caminhoes, cargas.id_caminhao == caminhoes.id_caminhao, "left")
        .groupBy(caminhoes.tipo)
        .agg(
            F.count("id_carga").alias("total_cargas"),
            F.avg("capacidade_toneladas").alias("capacidade_media"),
            F.sum("km_total").alias("km_total"),
            F.avg(
                F.col("peso_total_kg") / (F.col("capacidade_toneladas") * 1000) * 100
            ).alias("ocupacao_media_pct"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold - Status de Entregas

# COMMAND ----------

@dlt.table(
    name=f"{catalog_name}.gold.gold_status_entregas",
    comment="Resumo do status atual das entregas (ultimo status por carga)",
    table_properties={"quality": "gold"},
)
def gold_status_entregas():
    status = dlt.read(f"{catalog_name}.silver.silver_status_transporte")

    # ╔══════════════════════════════════════════════════════════════╗
    # ║  TO-DO 6: Criar agregacao do status mais recente por carga   ║
    # ║  Dica:                                                       ║
    # ║    1) Use Window para pegar o ultimo status de cada carga:   ║
    # ║       w = Window.partitionBy("id_carga")                     ║
    # ║           .orderBy(F.col("timestamp").desc())                ║
    # ║    2) Adicione row_number e filtre rn == 1                    ║
    # ║    3) Faca groupBy("descricao_status") e conte               ║
    # ║  Exemplo:                                                     ║
    # ║    w = Window.partitionBy("id_carga").orderBy(               ║
    # ║        F.col("timestamp").desc())                             ║
    # ║    ultimo_status = (status                                    ║
    # ║        .withColumn("rn", F.row_number().over(w))             ║
    # ║        .filter(F.col("rn") == 1))                            ║
    # ║    resultado = ultimo_status.groupBy("descricao_status")     ║
    # ║        .agg(F.count("id_carga").alias("total_cargas"),       ║
    # ║             F.avg("ordem").alias("ordem_media"))              ║
    # ╚══════════════════════════════════════════════════════════════╝
    # ▼▼▼ Seu codigo aqui ▼▼▼

    return status  # Substitua pela agregacao com window function

    # ▲▲▲ Fim do TO-DO 6 ▲▲▲
