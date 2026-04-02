# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 - SDP (Spark Declarative Pipelines)
# MAGIC ## Pipeline de Logistica - Versao COMPLETA (Gabarito)
# MAGIC
# MAGIC Esta e a versao de referencia com todos os TO-DOs implementados.
# MAGIC Use como gabarito para conferir suas respostas.
# MAGIC
# MAGIC **Importante:** Com SDP, cada tabela usa o fully qualified name (`catalog.schema.tabela`),
# MAGIC permitindo que um unico pipeline escreva em multiplos schemas de destino.
# MAGIC
# MAGIC **Camadas:**
# MAGIC - **Bronze**: Ingestao de dados brutos (Auto Loader + tabelas raw)
# MAGIC - **Silver**: Limpeza, validacao e enriquecimento
# MAGIC - **Gold**: Agregacoes para consumo analitico

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
# MAGIC ### Bronze - Movimento de Cargas (TO-DO 1 - RESOLVIDO)

# COMMAND ----------

@dlt.table(
    name=f"{catalog_name}.bronze.bronze_movimento_cargas",
    comment="Tabela de movimento de cargas do schema raw",
    table_properties={"quality": "bronze"},
)
def bronze_movimento_cargas():
    return spark.read.table(f"{catalog_name}.raw.movimento_cargas")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SILVER Layer
# MAGIC Limpeza, validacao com expectations e enriquecimento dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver - Pedidos Enriquecidos (TO-DO 2 - RESOLVIDO)

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

    return (
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
        # TO-DO 2 RESOLVIDO: Colunas de data extraidas do campo data_pedido
        .withColumn("ano", F.year("data_pedido"))
        .withColumn("mes", F.month("data_pedido"))
        .withColumn("dia", F.dayofmonth("data_pedido"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver - Itens de Nota Fiscal (TO-DO 3 - RESOLVIDO)

# COMMAND ----------

@dlt.table(
    name=f"{catalog_name}.silver.silver_itens_nf",
    comment="Itens de nota fiscal explodidos a partir dos pedidos",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("quantidade_valida", "quantidade > 0")
def silver_itens_nf():
    pedidos = dlt.read_stream(f"{catalog_name}.bronze.bronze_pedidos")

    # TO-DO 3 RESOLVIDO: Explode das notas fiscais e depois dos itens
    return (
        pedidos
        .select(
            "id_pedido",
            "id_cliente",
            "data_pedido",
            "cidade_origem",
            "uf_origem",
            "cidade_destino",
            "uf_destino",
            F.explode("notas_fiscais").alias("nf"),
        )
        .select(
            "id_pedido",
            "id_cliente",
            "data_pedido",
            "cidade_origem",
            "uf_origem",
            "cidade_destino",
            "uf_destino",
            F.col("nf.id_nf").alias("id_nf"),
            F.col("nf.numero_nf").alias("numero_nf"),
            F.col("nf.data_emissao").alias("data_emissao_nf"),
            F.col("nf.valor_total").alias("valor_total_nf"),
            F.explode("nf.itens").alias("item"),
        )
        .select(
            "id_pedido",
            "id_cliente",
            "data_pedido",
            "cidade_origem",
            "uf_origem",
            "cidade_destino",
            "uf_destino",
            "id_nf",
            "numero_nf",
            "data_emissao_nf",
            "valor_total_nf",
            F.col("item.id_item").alias("id_item"),
            F.col("item.descricao").alias("descricao"),
            F.col("item.ncm").alias("ncm"),
            F.col("item.quantidade").alias("quantidade"),
            F.col("item.unidade").alias("unidade"),
            F.col("item.valor_unitario").alias("valor_unitario"),
            F.col("item.valor_total").alias("valor_total_item"),
            F.col("item.peso_kg").alias("peso_kg"),
            F.col("item.dimensoes.comprimento_cm").alias("comprimento_cm"),
            F.col("item.dimensoes.largura_cm").alias("largura_cm"),
            F.col("item.dimensoes.altura_cm").alias("altura_cm"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver - Status de Transporte (TO-DO 4 - RESOLVIDO)

# COMMAND ----------

@dlt.table(
    name=f"{catalog_name}.silver.silver_status_transporte",
    comment="Status de transporte enriquecido com descricao do status",
    table_properties={"quality": "silver"},
)
# TO-DO 4 RESOLVIDO: Expectation para dropar registros sem id_carga
@dlt.expect_or_drop("carga_valida", "id_carga IS NOT NULL")
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
# MAGIC ### Gold - Volume por Rota (TO-DO 5 - RESOLVIDO)

# COMMAND ----------

@dlt.table(
    name=f"{catalog_name}.gold.gold_volume_por_rota",
    comment="Volume de pedidos agregado por rota (origem-destino)",
    table_properties={"quality": "gold"},
)
def gold_volume_por_rota():
    pedidos = dlt.read(f"{catalog_name}.silver.silver_pedidos")

    # TO-DO 5 RESOLVIDO: Agregacao por rota origem-destino
    return (
        pedidos
        .groupBy(
            "cidade_origem", "uf_origem",
            "cidade_destino", "uf_destino",
        )
        .agg(
            F.count("id_pedido").alias("total_pedidos"),
            F.sum("peso_total_kg").alias("peso_total"),
            F.sum("valor_frete").alias("valor_frete_total"),
            F.avg("valor_frete").alias("frete_medio"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold - Performance da Frota

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
# MAGIC ### Gold - Status de Entregas (TO-DO 6 - RESOLVIDO)

# COMMAND ----------

@dlt.table(
    name=f"{catalog_name}.gold.gold_status_entregas",
    comment="Resumo do status atual das entregas (ultimo status por carga)",
    table_properties={"quality": "gold"},
)
def gold_status_entregas():
    status = dlt.read(f"{catalog_name}.silver.silver_status_transporte")

    # TO-DO 6 RESOLVIDO: Window function para ultimo status + agregacao
    # Passo 1: Definir window para pegar o registro mais recente por carga
    w = Window.partitionBy("id_carga").orderBy(F.col("timestamp").desc())

    # Passo 2: Adicionar row_number e filtrar apenas o mais recente
    ultimo_status = (
        status
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Passo 3: Agregar por descricao de status
    return (
        ultimo_status
        .groupBy("descricao_status", "id_status", "ordem")
        .agg(
            F.count("id_carga").alias("total_cargas"),
            F.min("timestamp").alias("primeira_atualizacao"),
            F.max("timestamp").alias("ultima_atualizacao"),
        )
        .orderBy("ordem")
    )
