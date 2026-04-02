# Databricks notebook source

# MAGIC %md
# MAGIC # Lab 2c - Verificacao de Qualidade dos Dados (TO-DO)
# MAGIC
# MAGIC Neste notebook, executaremos verificacoes de qualidade nas camadas silver e gold
# MAGIC para garantir que o pipeline processou os dados corretamente.
# MAGIC
# MAGIC **Tarefas:**
# MAGIC - Check 1: Verificar ausencia de NULL em id_pedido (ja implementado)
# MAGIC - TO-DO 4: Verificar que valor_frete > 0 em silver_pedidos
# MAGIC - TO-DO 5: Verificar dados em gold_volume_por_rota

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
# MAGIC ## Lista de Resultados

# COMMAND ----------

resultados_qualidade = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1: Verificar ausencia de NULL em id_pedido (Exemplo)
# MAGIC
# MAGIC Este check ja esta implementado como referencia para os TO-DOs.

# COMMAND ----------

try:
    nulls_count = spark.sql(f"""
        SELECT COUNT(*) as total
        FROM {catalog_name}.silver.silver_pedidos
        WHERE id_pedido IS NULL
    """).collect()[0]["total"]

    status = "OK" if nulls_count == 0 else "FALHA"
    resultados_qualidade.append({
        "check": "NULL em id_pedido (silver_pedidos)",
        "resultado": nulls_count,
        "esperado": "0 registros NULL",
        "status": status,
    })
    print(f"{'✅' if status == 'OK' else '❌'} NULLs em id_pedido: {nulls_count}")
except Exception as e:
    resultados_qualidade.append({
        "check": "NULL em id_pedido (silver_pedidos)",
        "resultado": str(e),
        "esperado": "0 registros NULL",
        "status": "ERRO",
    })
    print(f"⚠️ Erro ao verificar NULLs em id_pedido: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TO-DO 4: Verificar que valor_frete > 0 em silver_pedidos
# MAGIC
# MAGIC Nenhum pedido deve ter valor de frete zero ou negativo.
# MAGIC Conte quantos registros possuem `valor_frete <= 0` e adicione o resultado
# MAGIC a lista `resultados_qualidade`.

# COMMAND ----------

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 4: Descomente o bloco abaixo para verificar que        ║
# ║           nenhum pedido tem valor_frete <= 0                    ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Descomente o bloco — conta registros com frete invalido ▼▼▼

# try:
#     frete_invalido = spark.sql(f"""
#         SELECT COUNT(*) AS total
#         FROM {catalog_name}.silver.silver_pedidos
#         WHERE valor_frete <= 0
#     """).collect()[0]["total"]
#     status = "OK" if frete_invalido == 0 else "FALHA"
#     resultados_qualidade.append({
#         "check": "valor_frete <= 0 (silver_pedidos)",
#         "resultado": int(frete_invalido),
#         "esperado": "0 registros com frete <= 0",
#         "status": status,
#     })
#     print(f"{'✅' if status == 'OK' else '❌'} Registros com valor_frete <= 0: {frete_invalido}")
# except Exception as e:
#     resultados_qualidade.append({
#         "check": "valor_frete <= 0 (silver_pedidos)",
#         "resultado": str(e),
#         "esperado": "0 registros com frete <= 0",
#         "status": "ERRO",
#     })
#     print(f"⚠️ Erro ao verificar valor_frete: {e}")

# ▲▲▲ Fim do TO-DO 4 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ## TO-DO 5: Verificar gold_volume_por_rota
# MAGIC
# MAGIC A tabela `gold.gold_volume_por_rota` deve:
# MAGIC 1. Possuir dados (pelo menos 1 registro)
# MAGIC 2. Nao ter valores negativos em `total_pedidos`
# MAGIC 3. Nao ter valores negativos em `peso_total`
# MAGIC
# MAGIC Adicione os resultados a lista `resultados_qualidade`.

# COMMAND ----------

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 5: Descomente o bloco abaixo para verificar que        ║
# ║           gold_volume_por_rota tem dados e sem valores negativos║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Descomente o bloco — 3 checks na tabela gold ▼▼▼

# # Check 5a: tabela tem dados
# try:
#     total_rotas = spark.sql(f"""
#         SELECT COUNT(*) AS total FROM {catalog_name}.gold.gold_volume_por_rota
#     """).collect()[0]["total"]
#     status = "OK" if total_rotas > 0 else "FALHA"
#     resultados_qualidade.append({
#         "check": "gold_volume_por_rota tem dados",
#         "resultado": int(total_rotas),
#         "esperado": "> 0 registros",
#         "status": status,
#     })
#     print(f"{'✅' if status == 'OK' else '❌'} Registros em gold_volume_por_rota: {total_rotas}")
# except Exception as e:
#     resultados_qualidade.append({
#         "check": "gold_volume_por_rota tem dados",
#         "resultado": str(e), "esperado": "> 0 registros", "status": "ERRO",
#     })
#     print(f"⚠️ Erro: {e}")
#
# # Check 5b: sem total_pedidos negativo
# try:
#     pedidos_negativos = spark.sql(f"""
#         SELECT COUNT(*) AS total FROM {catalog_name}.gold.gold_volume_por_rota
#         WHERE total_pedidos < 0
#     """).collect()[0]["total"]
#     status = "OK" if pedidos_negativos == 0 else "FALHA"
#     resultados_qualidade.append({
#         "check": "total_pedidos negativo (gold)",
#         "resultado": int(pedidos_negativos),
#         "esperado": "0 registros negativos",
#         "status": status,
#     })
#     print(f"{'✅' if status == 'OK' else '❌'} total_pedidos negativo: {pedidos_negativos}")
# except Exception as e:
#     resultados_qualidade.append({
#         "check": "total_pedidos negativo (gold)",
#         "resultado": str(e), "esperado": "0 negativos", "status": "ERRO",
#     })
#
# # Check 5c: sem peso_total negativo
# try:
#     peso_negativo = spark.sql(f"""
#         SELECT COUNT(*) AS total FROM {catalog_name}.gold.gold_volume_por_rota
#         WHERE peso_total < 0
#     """).collect()[0]["total"]
#     status = "OK" if peso_negativo == 0 else "FALHA"
#     resultados_qualidade.append({
#         "check": "peso_total negativo (gold)",
#         "resultado": int(peso_negativo),
#         "esperado": "0 registros negativos",
#         "status": status,
#     })
#     print(f"{'✅' if status == 'OK' else '❌'} peso_total negativo: {peso_negativo}")
# except Exception as e:
#     resultados_qualidade.append({
#         "check": "peso_total negativo (gold)",
#         "resultado": str(e), "esperado": "0 negativos", "status": "ERRO",
#     })

# ▲▲▲ Fim do TO-DO 5 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo dos Checks de Qualidade

# COMMAND ----------

if resultados_qualidade:
    df_qualidade = spark.createDataFrame(resultados_qualidade)
    display(df_qualidade)
else:
    print("⚠️ Apenas o Check 1 foi executado. Complete os TO-DOs 4 e 5!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado Final

# COMMAND ----------

falhas = [r for r in resultados_qualidade if r.get("status") in ("FALHA", "ERRO")]

if falhas:
    msg = f"FALHA: {len(falhas)} check(s) de qualidade falharam: {[f['check'] for f in falhas]}"
    print(f"❌ {msg}")
    dbutils.notebook.exit(msg)
else:
    msg = f"SUCESSO: Todos os {len(resultados_qualidade)} checks de qualidade passaram"
    print(f"✅ {msg}")
    dbutils.notebook.exit(msg)
