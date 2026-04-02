# Databricks notebook source

# MAGIC %md
# MAGIC # Lab 2b - Trigger do Pipeline SDP (TO-DO)
# MAGIC
# MAGIC Neste notebook, dispararemos a execucao de um pipeline SDP (Spark Declarative Pipelines)
# MAGIC via REST API do Databricks e monitoraremos seu status ate a conclusao.
# MAGIC
# MAGIC **Tarefas:**
# MAGIC - TO-DO 3: Disparar o pipeline via REST API e monitorar a execucao

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

dbutils.widgets.text("pipeline_id", "", "Pipeline ID (opcional)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports e Configuracao da API

# COMMAND ----------

import requests
import time
import json

# Obter o token e a URL do workspace automaticamente
databricks_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

headers = {
    "Authorization": f"Bearer {databricks_token}",
    "Content-Type": "application/json",
}

print(f"Workspace URL: {databricks_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Localizar o Pipeline
# MAGIC
# MAGIC Se o `pipeline_id` nao foi informado via widget, buscaremos pelo nome do pipeline.

# COMMAND ----------

pipeline_id = dbutils.widgets.get("pipeline_id").strip()

if not pipeline_id:
    # Buscar pipeline pelo nome esperado
    pipeline_name = f"workshop_logistica_{nome}_pipeline"
    print(f"Buscando pipeline com nome: {pipeline_name}")

    response = requests.get(
        f"{databricks_url}/api/2.0/pipelines",
        headers=headers,
        params={"filter": f"name LIKE '{pipeline_name}'"},
    )
    response.raise_for_status()
    pipelines = response.json().get("statuses", [])

    if pipelines:
        pipeline_id = pipelines[0]["pipeline_id"]
        print(f"Pipeline encontrado: {pipeline_id}")
    else:
        dbutils.notebook.exit(f"FALHA: Pipeline '{pipeline_name}' nao encontrado")
else:
    print(f"Usando pipeline_id informado: {pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TO-DO 3: Disparar o Pipeline e Monitorar Execucao
# MAGIC
# MAGIC 1. Faca um POST para `/api/2.0/pipelines/{pipeline_id}/updates` para iniciar o pipeline
# MAGIC 2. Capture o `update_id` da resposta
# MAGIC 3. Em um loop, consulte o status do update ate que esteja completo
# MAGIC 4. Use GET `/api/2.0/pipelines/{pipeline_id}/updates/{update_id}` para verificar status
# MAGIC
# MAGIC Status possiveis: `QUEUED`, `CREATED`, `WAITING_FOR_RESOURCES`, `INITIALIZING`,
# MAGIC `SETTING_UP_TABLES`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELED`

# COMMAND ----------

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 3: Disparar o pipeline via REST API e monitorar        ║
# ║  a execucao ate conclusao                                      ║
# ║  Dica: Use requests.post para                                 ║
# ║  POST /api/2.0/pipelines/{pipeline_id}/updates                ║
# ║  e requests.get para monitorar o status                        ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu código aqui ▼▼▼

pass

# ▲▲▲ Fim do TO-DO 3 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado Final

# COMMAND ----------

# A variavel 'status_final' deve ser definida no TO-DO 3
try:
    if status_final == "COMPLETED":
        msg = f"SUCESSO: Pipeline {pipeline_id} executado com sucesso"
        print(f"✅ {msg}")
        dbutils.notebook.exit(msg)
    else:
        msg = f"FALHA: Pipeline {pipeline_id} terminou com status: {status_final}"
        print(f"❌ {msg}")
        dbutils.notebook.exit(msg)
except NameError:
    msg = "FALHA: Variavel 'status_final' nao definida. Complete o TO-DO 3!"
    print(f"⚠️ {msg}")
    dbutils.notebook.exit(msg)
