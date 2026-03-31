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
# ║  TO-DO 4: Verificar que valor_frete > 0 em silver_pedidos     ║
# ║  Dica: SELECT COUNT(*) FROM silver.silver_pedidos              ║
# ║        WHERE valor_frete <= 0                                  ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu código aqui ▼▼▼

pass

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
# ║  TO-DO 5: Verificar dados em gold_volume_por_rota              ║
# ║  Dica: Verifique contagem total > 0, e que nao ha              ║
# ║        total_pedidos ou peso_total negativos                   ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu código aqui ▼▼▼

pass

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
