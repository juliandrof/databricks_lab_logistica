# Databricks notebook source

# MAGIC %md
# MAGIC # Lab 2d - Resumo da Execucao (TO-DO)
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

# COMMAND ----------

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

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 6: Criar DataFrame de resumo com table_name,           ║
# ║  row_count e last_updated para todas as tabelas                ║
# ║  Dica: Use SHOW TABLES para listar, SELECT COUNT(*)            ║
# ║  para contagem, e DESCRIBE DETAIL para timestamps              ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu código aqui ▼▼▼

pass

# ▲▲▲ Fim do TO-DO 6 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibir Resumo

# COMMAND ----------

if resumo:
    df_resumo = spark.createDataFrame(resumo)
    display(df_resumo.orderBy("table_name"))
else:
    print("⚠️ Nenhum dado de resumo foi gerado. Complete o TO-DO 6!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estatisticas Gerais

# COMMAND ----------

if resumo:
    total_tabelas = len(resumo)
    total_registros = sum(r["row_count"] for r in resumo)
    print(f"📊 Total de tabelas: {total_tabelas}")
    print(f"📊 Total de registros: {total_registros:,}")
    print(f"📊 Timestamp do resumo: {datetime.now().isoformat()}")
else:
    print("⚠️ Complete o TO-DO 6 para ver as estatisticas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado Final

# COMMAND ----------

if resumo:
    resumo_json = json.dumps({
        "timestamp": datetime.now().isoformat(),
        "catalog": catalog_name,
        "total_tabelas": len(resumo),
        "total_registros": sum(r["row_count"] for r in resumo),
        "tabelas": resumo,
    }, default=str)
    print(f"✅ Resumo gerado com sucesso!")
    dbutils.notebook.exit(resumo_json)
else:
    dbutils.notebook.exit("FALHA: Resumo nao foi gerado. Complete o TO-DO 6!")
