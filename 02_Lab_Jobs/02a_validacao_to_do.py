# Databricks notebook source

# MAGIC %md
# MAGIC # Lab 2a - Validacao dos Dados (TO-DO)
# MAGIC
# MAGIC Neste notebook, validaremos que todas as tabelas base existem e possuem dados suficientes
# MAGIC antes de prosseguir com o pipeline de transformacao.
# MAGIC
# MAGIC **Tarefas:**
# MAGIC - TO-DO 1: Validar existencia e contagem minima de registros das tabelas
# MAGIC - TO-DO 2: Validar integridade referencial entre motoristas e caminhoes

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

# MAGIC %md
# MAGIC ## Definicao das Tabelas e Contagens Minimas

# COMMAND ----------

# Tabelas que devem existir no schema "raw" e suas contagens minimas
tabelas_esperadas = {
    "clientes": 1000,
    "caminhoes": 1000,
    "motoristas": 1000,
    "pedidos": 10000,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## TO-DO 1: Validar Existencia e Contagem Minima de Registros
# MAGIC
# MAGIC Para cada tabela na lista `tabelas_esperadas`, verifique:
# MAGIC 1. Se a tabela existe no schema `raw`
# MAGIC 2. Se possui pelo menos a quantidade minima de registros esperada
# MAGIC
# MAGIC Armazene os resultados em uma lista de dicionarios com as chaves:
# MAGIC `tabela`, `contagem`, `minimo_esperado`, `status`

# COMMAND ----------

resultados_validacao = []

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 1: Validar existencia e contagem minima de registros   ║
# ║  Dica: Use spark.sql(f"SELECT COUNT(*) FROM                   ║
# ║        {catalog_name}.raw.{table}") para cada tabela           ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu código aqui ▼▼▼

pass

# ▲▲▲ Fim do TO-DO 1 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exibir Resultados da Validacao de Contagem

# COMMAND ----------

if resultados_validacao:
    df_resultados = spark.createDataFrame(resultados_validacao)
    display(df_resultados)
else:
    print("⚠️ Nenhum resultado de validacao foi gerado. Complete o TO-DO 1!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TO-DO 2: Validar Integridade Referencial
# MAGIC
# MAGIC Verifique se todos os valores de `id_caminhao` na tabela `motoristas`
# MAGIC existem na tabela `caminhoes`. Registros orfaos indicam problemas de integridade.

# COMMAND ----------

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 2: Validar integridade referencial entre motoristas     ║
# ║  e caminhoes                                                    ║
# ║  Dica: Use LEFT ANTI JOIN para encontrar registros orfaos      ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu código aqui ▼▼▼

pass

# ▲▲▲ Fim do TO-DO 2 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado Final

# COMMAND ----------

# Verificar se houve falhas
falhas = [r for r in resultados_validacao if r.get("status") == "FALHA"]

if falhas:
    msg = f"FALHA: {len(falhas)} validacao(oes) falharam: {[f['tabela'] for f in falhas]}"
    print(f"❌ {msg}")
    dbutils.notebook.exit(msg)
else:
    msg = "SUCESSO: Todas as validações passaram"
    print(f"✅ {msg}")
    dbutils.notebook.exit(msg)
