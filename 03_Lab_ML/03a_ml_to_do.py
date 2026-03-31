# Databricks notebook source

# MAGIC %md
# MAGIC # Lab 3: Machine Learning para Logistica
# MAGIC
# MAGIC Neste laboratorio, vamos aplicar tecnicas de Machine Learning a dados de logistica e transporte.
# MAGIC Trabalharemos com 3 casos de uso praticos:
# MAGIC
# MAGIC | # | Caso de Uso | Objetivo |
# MAGIC |---|------------|----------|
# MAGIC | 1 | **Predicao de Demanda** | Prever o volume semanal de pedidos por rota |
# MAGIC | 2 | **Otimizacao de Caminhoes Vazios** | Identificar caminhoes retornando vazios e combinar com pedidos pendentes |
# MAGIC | 3 | **Pedidos Surpresa** | Encontrar rapidamente o caminhao mais proximo para um pedido urgente |
# MAGIC
# MAGIC **Ferramentas:** PySpark, scikit-learn, MLflow, Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracao Inicial

# COMMAND ----------

# Configuracao do catalogo do participante
# Substitua pelo seu nome
nome_participante = "seu_nome"
catalog_name = f"workshop_logistica_{nome_participante}"

spark.sql(f"USE CATALOG {catalog_name}")
print(f"Usando catalogo: {catalog_name}")

# COMMAND ----------

# Imports necessarios
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Caso de Uso 1: Predicao de Demanda por Rota
# MAGIC
# MAGIC ## Contexto
# MAGIC Uma das maiores dificuldades na logistica e prever a demanda futura.
# MAGIC Se sabemos quantos pedidos teremos na proxima semana em cada rota,
# MAGIC podemos **alocar caminhoes antecipadamente**, reduzir custos e melhorar prazos.
# MAGIC
# MAGIC ## Abordagem
# MAGIC - Agrupar pedidos por **semana** e **rota** (cidade_origem -> cidade_destino)
# MAGIC - Criar features como: mes, dia da semana, peso medio, valor medio
# MAGIC - Usar **lag features** (volume da semana anterior) para capturar tendencias
# MAGIC - Treinar um modelo **RandomForestRegressor** com tracking via **MLflow**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Exploracao dos Dados de Pedidos

# COMMAND ----------

# Carregar dados de pedidos
df_pedidos = spark.table("silver.silver_pedidos")
display(df_pedidos.limit(10))

# COMMAND ----------

# Estatisticas basicas
print(f"Total de pedidos: {df_pedidos.count():,}")
print(f"Periodo: {df_pedidos.select(F.min('data_pedido'), F.max('data_pedido')).collect()[0]}")
print(f"Rotas unicas: {df_pedidos.select('cidade_origem', 'cidade_destino').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Feature Engineering
# MAGIC
# MAGIC Precisamos transformar os dados brutos de pedidos em features uteis para o modelo.
# MAGIC
# MAGIC **Features planejadas:**
# MAGIC - `semana`: numero da semana do ano
# MAGIC - `mes`: mes do pedido
# MAGIC - `dia_da_semana`: dia da semana (1=segunda, 7=domingo)
# MAGIC - `rota`: combinacao cidade_origem -> cidade_destino
# MAGIC - `peso_medio`: peso medio dos pedidos naquela semana/rota
# MAGIC - `valor_medio`: valor medio do frete naquela semana/rota
# MAGIC - `total_pedidos_semana_anterior`: volume da semana anterior (lag feature)

# COMMAND ----------

# Preparar coluna de semana e rota
df_base = df_pedidos.withColumn(
    "semana", F.weekofyear(F.col("data_pedido"))
).withColumn(
    "ano_semana", F.concat(F.col("ano"), F.lit("_W"), F.lpad(F.col("semana"), 2, "0"))
).withColumn(
    "rota", F.concat(F.col("cidade_origem"), F.lit(" -> "), F.col("cidade_destino"))
).withColumn(
    "dia_da_semana", F.dayofweek(F.col("data_pedido"))
)

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 1: Criar o DataFrame de features agregadas           ║
# ║  Dica: Agrupe por ano, semana, mes, rota usando groupBy()   ║
# ║  e calcule: count => total_pedidos, avg(peso_total_kg),     ║
# ║  avg(valor_frete), avg(dia_da_semana)                       ║
# ║  Depois use Window + F.lag() para criar                     ║
# ║  total_pedidos_semana_anterior                               ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu codigo aqui ▼▼▼

# Passo 1: Agregar por semana e rota
# df_features = df_base.groupBy( ... ).agg( ... )

# Passo 2: Criar lag feature com Window
# window_rota = Window.partitionBy("rota").orderBy("ano", "semana")
# df_features = df_features.withColumn(
#     "total_pedidos_semana_anterior",
#     F.lag("total_pedidos", 1).over(window_rota)
# )

pass

# ▲▲▲ Fim do TO-DO 1 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Treinamento do Modelo com MLflow
# MAGIC
# MAGIC Vamos usar o **MLflow** para rastrear nossos experimentos.
# MAGIC O MLflow registra automaticamente:
# MAGIC - Parametros do modelo (n_estimators, max_depth, etc.)
# MAGIC - Metricas (MAE, RMSE, R2)
# MAGIC - O modelo serializado (para deploy futuro)

# COMMAND ----------

# Converter para Pandas para usar sklearn
# (Assumindo que df_features foi criado no TO-DO 1)
# df_features_clean = df_features.filter(F.col("total_pedidos_semana_anterior").isNotNull())
# pdf = df_features_clean.toPandas()

# Definir features e target
feature_cols = ["semana", "mes", "dia_da_semana_medio", "peso_medio", "valor_medio", "total_pedidos_semana_anterior"]
target_col = "total_pedidos"

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 2: Treinar o modelo com MLflow tracking               ║
# ║  Dica: Use mlflow.sklearn.autolog() para log automatico.     ║
# ║  Crie um run com mlflow.start_run(run_name="demanda")        ║
# ║  Treine RandomForestRegressor(n_estimators=100, random_state=42) ║
# ║  Avalie com MAE, RMSE e R2                                   ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu codigo aqui ▼▼▼

# Passo 1: Separar treino e teste
# X = pdf[feature_cols]
# y = pdf[target_col]
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Passo 2: Configurar MLflow
# mlflow.sklearn.autolog()

# Passo 3: Treinar dentro de um run do MLflow
# with mlflow.start_run(run_name="demanda_logistica"):
#     model = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42)
#     model.fit(X_train, y_train)
#
#     y_pred = model.predict(X_test)
#     mae = mean_absolute_error(y_test, y_pred)
#     rmse = np.sqrt(mean_squared_error(y_test, y_pred))
#     r2 = r2_score(y_test, y_pred)
#
#     print(f"MAE: {mae:.2f}")
#     print(f"RMSE: {rmse:.2f}")
#     print(f"R2: {r2:.4f}")

pass

# ▲▲▲ Fim do TO-DO 2 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Caso de Uso 2: Otimizacao de Caminhoes Vazios
# MAGIC
# MAGIC ## Contexto
# MAGIC Um dos maiores despedicios na logistica e o **frete de retorno vazio**.
# MAGIC Caminhoes que acabaram de entregar frequentemente voltam sem carga,
# MAGIC gerando custo sem receita.
# MAGIC
# MAGIC ## Abordagem
# MAGIC 1. Identificar caminhoes que acabaram de completar uma entrega
# MAGIC 2. Obter sua localizacao atual (lat/long do ultimo status)
# MAGIC 3. Buscar pedidos pendentes proximos a essa localizacao
# MAGIC 4. Calcular distancia usando a **formula de Haversine**
# MAGIC 5. Recomendar o melhor match (caminhao x pedido)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Identificar Caminhoes com Entrega Concluida

# COMMAND ----------

# Caminhoes com ultima entrega realizada
df_status = spark.table("silver.silver_status_transporte")
df_cargas = spark.table("raw.movimento_cargas")
df_caminhoes = spark.table("raw.caminhoes")

# Encontrar o ultimo status de cada carga
window_status = Window.partitionBy("id_carga").orderBy(F.col("timestamp").desc())

df_ultimo_status = df_status.withColumn(
    "rn", F.row_number().over(window_status)
).filter(
    F.col("rn") == 1
).drop("rn")

# Filtrar apenas entregas realizadas
df_entregas_realizadas = df_ultimo_status.filter(
    F.col("descricao_status") == "Entrega Realizada"
)

print(f"Caminhoes que acabaram de entregar: {df_entregas_realizadas.count()}")
display(df_entregas_realizadas.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Obter Localizacao Atual dos Caminhoes

# COMMAND ----------

# Juntar com dados da carga para saber o caminhao e sua posicao
df_caminhoes_livres = df_entregas_realizadas.alias("s").join(
    df_cargas.alias("c"),
    F.col("s.id_carga") == F.col("c.id_carga"),
    "inner"
).select(
    F.col("c.id_caminhao"),
    F.col("c.placa"),
    F.col("s.latitude").alias("lat_caminhao"),
    F.col("s.longitude").alias("lon_caminhao"),
    F.col("c.cidade_destino").alias("cidade_atual"),
    F.col("c.uf_destino").alias("uf_atual")
).join(
    df_caminhoes.alias("cam"),
    F.col("c.id_caminhao") == F.col("cam.id_caminhao"),
    "inner"
).select(
    F.col("c.id_caminhao"),
    F.col("c.placa"),
    F.col("lat_caminhao"),
    F.col("lon_caminhao"),
    F.col("cidade_atual"),
    F.col("uf_atual"),
    F.col("cam.capacidade_toneladas"),
    F.col("cam.volume_m3").alias("capacidade_volume_m3")
)

display(df_caminhoes_livres.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Buscar Pedidos Pendentes

# COMMAND ----------

# Pedidos que ainda nao foram atribuidos a uma carga
# (simplificacao: pedidos recentes que podem precisar de transporte)
df_pedidos_pendentes = df_pedidos.select(
    "id_pedido",
    "cidade_origem",
    "uf_origem",
    "peso_total_kg",
    "volume_total_m3",
    "valor_frete",
    "prioridade"
).join(
    spark.table("raw.clientes").select(
        "cidade", "uf", "latitude", "longitude"
    ),
    (F.col("cidade_origem") == F.col("cidade")) & (F.col("uf_origem") == F.col("uf")),
    "left"
).select(
    "id_pedido",
    "cidade_origem",
    "uf_origem",
    "peso_total_kg",
    "volume_total_m3",
    "valor_frete",
    "prioridade",
    F.col("latitude").alias("lat_pedido"),
    F.col("longitude").alias("lon_pedido")
).filter(
    F.col("lat_pedido").isNotNull()
)

print(f"Pedidos pendentes com localizacao: {df_pedidos_pendentes.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Calcular Distancia com Haversine
# MAGIC
# MAGIC A **formula de Haversine** calcula a distancia entre dois pontos
# MAGIC na superficie da Terra, dados seus pares de latitude/longitude.
# MAGIC
# MAGIC $$d = 2r \cdot \arcsin\left(\sqrt{\sin^2\left(\frac{\phi_2 - \phi_1}{2}\right) + \cos(\phi_1)\cos(\phi_2)\sin^2\left(\frac{\lambda_2 - \lambda_1}{2}\right)}\right)$$
# MAGIC
# MAGIC Onde $r = 6371$ km (raio da Terra).

# COMMAND ----------

from math import radians, cos, sin, asin, sqrt

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 3: Implementar a UDF de distancia Haversine           ║
# ║  Dica: Use a formula acima. Recebe (lat1, lon1, lat2, lon2)  ║
# ║  Retorna a distancia em km. Registre como UDF do Spark       ║
# ║  com spark.udf.register("haversine", func, DoubleType())     ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu codigo aqui ▼▼▼

# def haversine(lat1, lon1, lat2, lon2):
#     """Calcula a distancia em km entre dois pontos usando Haversine."""
#     if any(v is None for v in [lat1, lon1, lat2, lon2]):
#         return None
#     # Converter para radianos
#     # Aplicar formula de Haversine
#     # Retornar distancia em km
#     ...

# Registrar como UDF
# haversine_udf = F.udf(haversine, DoubleType())
# spark.udf.register("haversine", haversine, DoubleType())

pass

# ▲▲▲ Fim do TO-DO 3 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Combinar Caminhoes com Pedidos

# COMMAND ----------

# Cross join entre caminhoes livres e pedidos pendentes
# (Em producao, filtrar por regiao para evitar cross join enorme)
# df_matches = df_caminhoes_livres.alias("cam").crossJoin(
#     df_pedidos_pendentes.alias("ped")
# ).withColumn(
#     "distancia_km",
#     haversine_udf(
#         F.col("lat_caminhao"), F.col("lon_caminhao"),
#         F.col("lat_pedido"), F.col("lon_pedido")
#     )
# ).filter(
#     # Filtrar apenas caminhoes com capacidade suficiente
#     (F.col("capacidade_toneladas") * 1000 >= F.col("peso_total_kg")) &
#     (F.col("capacidade_volume_m3") >= F.col("volume_total_m3"))
# ).filter(
#     # Limitar a caminhoes dentro de 200km
#     F.col("distancia_km") <= 200
# )

# Rankear por distancia (menor = melhor)
# window_pedido = Window.partitionBy("id_pedido").orderBy("distancia_km")
# df_recomendacoes = df_matches.withColumn(
#     "rank", F.row_number().over(window_pedido)
# ).filter(F.col("rank") <= 3)  # Top 3 caminhoes por pedido

# display(df_recomendacoes.limit(10))

print("Descomente o codigo acima apos completar o TO-DO 3")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Salvar Recomendacoes

# COMMAND ----------

# Criar schema ml se nao existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.ml")

# Salvar recomendacoes (descomente apos TO-DO 3)
# df_recomendacoes.select(
#     "id_pedido", "id_caminhao", "placa", "distancia_km",
#     "capacidade_toneladas", "peso_total_kg", "rank",
#     "cidade_atual", "cidade_origem"
# ).write.mode("overwrite").saveAsTable(f"{catalog_name}.ml.recomendacao_caminhoes_vazios")
# print("Recomendacoes salvas em ml.recomendacao_caminhoes_vazios")

print("Descomente apos completar TO-DO 3")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Caso de Uso 3: Pedidos Surpresa
# MAGIC
# MAGIC ## Contexto
# MAGIC Quando um pedido **urgente** chega de surpresa, precisamos encontrar
# MAGIC rapidamente o caminhao **mais proximo** que tenha **capacidade suficiente**
# MAGIC para transportar a carga.
# MAGIC
# MAGIC ## Abordagem
# MAGIC 1. Receber parametros do pedido surpresa (peso, volume, localizacao)
# MAGIC 2. Filtrar caminhoes com capacidade suficiente
# MAGIC 3. Filtrar motoristas ativos
# MAGIC 4. Obter ultima posicao de cada caminhao
# MAGIC 5. Calcular distancia e retornar os 5 mais proximos

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Simular um Pedido Surpresa

# COMMAND ----------

# Parametros do pedido surpresa
pedido_surpresa = {
    "peso_kg": 5000,       # 5 toneladas
    "volume_m3": 12,       # 12 metros cubicos
    "cidade": "Campinas",
    "uf": "SP",
    "latitude": -22.9099,
    "longitude": -47.0626
}

print("Pedido Surpresa recebido!")
for k, v in pedido_surpresa.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Encontrar os 5 Caminhoes Mais Proximos

# COMMAND ----------

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 4: Encontrar os 5 caminhoes mais proximos             ║
# ║  Dica:                                                        ║
# ║  1. Filtre raw.caminhoes por capacidade >= peso do pedido     ║
# ║  2. Join com raw.motoristas (status = 'Ativo')               ║
# ║  3. Join com ultimo status de cada carga (posicao atual)      ║
# ║  4. Calcule distancia com haversine_udf                       ║
# ║  5. Ordene por distancia e pegue LIMIT 5                      ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu codigo aqui ▼▼▼

# peso_pedido_ton = pedido_surpresa["peso_kg"] / 1000
# lat_pedido = pedido_surpresa["latitude"]
# lon_pedido = pedido_surpresa["longitude"]

# Passo 1: Caminhoes com capacidade suficiente
# df_cam_disponiveis = spark.table("raw.caminhoes").filter(
#     F.col("capacidade_toneladas") >= peso_pedido_ton
# )

# Passo 2: Motoristas ativos
# df_motoristas = spark.table("raw.motoristas").filter(F.col("status") == "Ativo")

# Passo 3: Join caminhoes + motoristas
# ...

# Passo 4: Ultima posicao (lat/long) de cada caminhao
# ...

# Passo 5: Calcular distancia e ordenar
# ...

pass

# ▲▲▲ Fim do TO-DO 4 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Registrar o Melhor Modelo no Unity Catalog
# MAGIC
# MAGIC O MLflow permite registrar modelos no **Unity Catalog** para
# MAGIC versionamento, governanca e deploy. Vamos registrar o modelo
# MAGIC de predicao de demanda treinado no Caso de Uso 1.

# COMMAND ----------

# ╔══════════════════════════════════════════════════════════════╗
# ║  TO-DO 5: Registrar o melhor modelo no Unity Catalog          ║
# ║  Dica:                                                        ║
# ║  1. Use mlflow.search_runs() para encontrar o melhor run      ║
# ║  2. Registre com mlflow.register_model(                       ║
# ║     f"runs:/{run_id}/model",                                  ║
# ║     f"{catalog_name}.ml.modelo_demanda"                       ║
# ║  )                                                            ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Seu codigo aqui ▼▼▼

# Passo 1: Buscar o melhor run (menor RMSE)
# mlflow.set_registry_uri("databricks-uc")
# experiment_name = "/Users/<seu_email>/demanda_logistica"
# runs = mlflow.search_runs(
#     experiment_names=[experiment_name],
#     order_by=["metrics.training_root_mean_squared_error ASC"],
#     max_results=1
# )
# best_run_id = runs.iloc[0]["run_id"]
# print(f"Melhor run: {best_run_id}")

# Passo 2: Registrar no Unity Catalog
# model_name = f"{catalog_name}.ml.modelo_demanda"
# mlflow.register_model(f"runs:/{best_run_id}/model", model_name)
# print(f"Modelo registrado como: {model_name}")

pass

# ▲▲▲ Fim do TO-DO 5 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo do Lab 3
# MAGIC
# MAGIC Neste laboratorio, voce praticou:
# MAGIC
# MAGIC | TO-DO | Conceito | Ferramentas |
# MAGIC |-------|---------|-------------|
# MAGIC | 1 | Feature Engineering para ML | PySpark groupBy, Window, lag |
# MAGIC | 2 | Treinamento com tracking | scikit-learn, MLflow |
# MAGIC | 3 | UDF de distancia geografica | Haversine, spark.udf.register |
# MAGIC | 4 | Busca espacial de recursos | Joins, filtros, ordenacao |
# MAGIC | 5 | Model Registry | MLflow + Unity Catalog |
# MAGIC
# MAGIC **Proximo passo:** Abra o notebook `03b_ml_completo` para ver as respostas!
