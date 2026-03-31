# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Configuração do Ambiente - Workshop Logística
# MAGIC
# MAGIC Este notebook cria a estrutura de catálogo, schemas e volumes necessários para o workshop.
# MAGIC
# MAGIC **Cada participante deve preencher seu nome no widget abaixo** para criar um ambiente isolado.

# COMMAND ----------

dbutils.widgets.text("nome_participante", "", "Seu Nome (sem espaços ou acentos)")

# COMMAND ----------

nome = dbutils.widgets.get("nome_participante").strip().lower().replace(" ", "_")
assert nome != "", "⚠️ Por favor, preencha seu nome no widget acima! Use apenas letras minúsculas, sem espaços ou acentos."
assert nome.isidentifier(), f"⚠️ Nome inválido: '{nome}'. Use apenas letras, números e underline."

catalog_name = f"workshop_logistica_{nome}"
print(f"✅ Participante: {nome}")
print(f"📦 Catálogo: {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criação do Catálogo

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")
print(f"✅ Catálogo '{catalog_name}' criado com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criação dos Schemas

# COMMAND ----------

schemas = ["raw", "bronze", "silver", "gold", "ml"]
for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")
    print(f"  ✅ Schema '{schema}' criado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criação dos Volumes para Dados de Streaming

# COMMAND ----------

volumes = [
    ("raw", "pedidos_json"),
    ("raw", "nfs_json"),
    ("raw", "status_json"),
]
for schema, volume in volumes:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema}.{volume}")
    print(f"  ✅ Volume '{schema}.{volume}' criado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificação Final

# COMMAND ----------

print("=" * 60)
print(f"  🎉 Ambiente configurado com sucesso!")
print("=" * 60)
print(f"\n  📦 Catálogo:  {catalog_name}")
print(f"  📂 Schemas:   {', '.join(schemas)}")
print(f"  📁 Volumes:   {', '.join([f'{s}.{v}' for s, v in volumes])}")
print(f"\n  ✅ Próximo passo: Execute o notebook '01_dados_cadastrais'")
print("=" * 60)
