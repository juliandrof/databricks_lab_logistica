# Databricks notebook source

# MAGIC %md
# MAGIC # Lab 2d - Resumo da Execucao (COMPLETO)
# MAGIC
# MAGIC Neste notebook, geraremos um resumo completo de todas as tabelas
# MAGIC nos schemas bronze (raw), silver e gold, incluindo contagens e timestamps.
# MAGIC
# MAGIC **Tarefas:**
# MAGIC - TO-DO 6: Criar um DataFrame de resumo com nome da tabela, contagem e ultima atualizacao

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracao Inicial

# COMMAND ----------

dbutils.widgets.text("nome_participante", "", "Seu Nome (sem espaços ou acentos)")
nome = dbutils.widgets.get("nome_participante").strip().lower().replace(" ", "_")
assert nome != "", "⚠️ Por favor, preencha seu nome!"
catalog_name = f"workshop_logistica_{nome}"
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

import json
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas a Serem Inventariados

# COMMAND ----------

schemas = ["raw", "silver", "gold"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## TO-DO 6: Gerar Resumo de Todas as Tabelas
# MAGIC
# MAGIC Para cada schema listado acima:
# MAGIC 1. Liste todas as tabelas usando `SHOW TABLES IN {catalog_name}.{schema}`
# MAGIC 2. Para cada tabela, obtenha:
# MAGIC    - `table_name`: nome completo (schema.tabela)
# MAGIC    - `row_count`: contagem de registros via `SELECT COUNT(*)`
# MAGIC    - `last_updated`: timestamp da ultima modificacao via `DESCRIBE DETAIL`
# MAGIC 3. Armazene os resultados em uma lista de dicionarios chamada `resumo`

# COMMAND ----------

resumo = []

for schema in schemas:
    print(f"\n📂 Processando schema: {schema}")
    try:
        tabelas_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema}")
        tabelas = [row["tableName"] for row in tabelas_df.collect()]

        for tabela in tabelas:
            full_name = f"{schema}.{tabela}"
            try:
                # Contagem de registros
                contagem = spark.sql(
                    f"SELECT COUNT(*) AS total FROM {catalog_name}.{schema}.{tabela}"
                ).collect()[0]["total"]

                # Ultima atualizacao via DESCRIBE DETAIL
                detail = spark.sql(
                    f"DESCRIBE DETAIL {catalog_name}.{schema}.{tabela}"
                ).collect()[0]
                last_modified = str(detail["lastModified"]) if "lastModified" in detail.asDict() else "N/A"

                resumo.append({
                    "table_name": full_name,
                    "row_count": int(contagem),
                    "last_updated": last_modified,
                })
                print(f"  ✅ {full_name}: {contagem:,} registros")

            except Exception as e:
                resumo.append({
                    "table_name": full_name,
                    "row_count": -1,
                    "last_updated": f"ERRO: {str(e)[:100]}",
                })
                print(f"  ⚠️ {full_name}: Erro - {e}")

    except Exception as e:
        print(f"  ⚠️ Erro ao listar tabelas do schema {schema}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibir Resumo

# COMMAND ----------

if resumo:
    df_resumo = spark.createDataFrame(resumo)
    display(df_resumo.orderBy("table_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estatisticas Gerais

# COMMAND ----------

if resumo:
    total_tabelas = len(resumo)
    total_registros = sum(r["row_count"] for r in resumo if r["row_count"] > 0)
    print(f"📊 Total de tabelas: {total_tabelas}")
    print(f"📊 Total de registros: {total_registros:,}")
    print(f"📊 Timestamp do resumo: {datetime.now().isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado Final

# COMMAND ----------

if resumo:
    resumo_json = json.dumps({
        "timestamp": datetime.now().isoformat(),
        "catalog": catalog_name,
        "total_tabelas": len(resumo),
        "total_registros": sum(r["row_count"] for r in resumo if r["row_count"] > 0),
        "tabelas": resumo,
    }, default=str)
    print(f"✅ Resumo gerado com sucesso!")
    dbutils.notebook.exit(resumo_json)
else:
    dbutils.notebook.exit("FALHA: Nenhuma tabela encontrada nos schemas")
