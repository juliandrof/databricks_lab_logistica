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
# MAGIC ## Instalacao de Dependencias

# COMMAND ----------

# MAGIC %pip install mlflow scikit-learn matplotlib typing_extensions --upgrade --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracao Inicial

# COMMAND ----------

dbutils.widgets.text("nome_participante", "", "Nome do Participante")

# COMMAND ----------

nome_participante = dbutils.widgets.get("nome_participante").strip().lower().replace(" ", "_")
assert nome_participante != "", "⚠️ Por favor, preencha seu nome no widget acima!"
catalog_name = f"workshop_logistica_{nome_participante}"

spark.sql(f"USE CATALOG {catalog_name}")
print(f"✅ Usando catálogo: {catalog_name}")

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
try:
    import matplotlib.pyplot as plt
except ImportError:
    plt = None
    print("⚠️ matplotlib nao disponivel — graficos serao ignorados")

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
# MAGIC ### 1.2 O que e Feature Engineering?
# MAGIC
# MAGIC **Feature Engineering** e o processo de transformar dados brutos em variaveis (features) que um modelo de Machine Learning consiga utilizar para fazer previsoes. E considerada uma das etapas mais importantes de um projeto de ML — frequentemente tem mais impacto na qualidade do modelo do que a escolha do algoritmo.
# MAGIC
# MAGIC **Por que e necessario?**
# MAGIC - Modelos de ML nao entendem dados brutos como textos, datas ou JSONs diretamente
# MAGIC - Precisamos criar **representacoes numericas** que capturem os padroes relevantes
# MAGIC - Boas features permitem que ate modelos simples tenham alta performance
# MAGIC
# MAGIC **Tipos comuns de features:**
# MAGIC | Tipo | Exemplo | Tecnica |
# MAGIC |------|---------|---------|
# MAGIC | **Temporal** | Semana, mes, dia da semana | Extrair de colunas de data |
# MAGIC | **Agregada** | Peso medio, valor medio por rota | groupBy + avg/sum/count |
# MAGIC | **Lag (historica)** | Volume da semana anterior | Window + lag() |
# MAGIC | **Categorica** | Rota (origem → destino) | Concatenacao de colunas |
# MAGIC | **Derivada** | Frete por kg | Divisao entre colunas |
# MAGIC
# MAGIC **No nosso caso:** vamos transformar pedidos brutos em features agregadas por semana e rota, com uma lag feature que captura a tendencia de demanda.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Criando as Features
# MAGIC
# MAGIC Agora vamos aplicar Feature Engineering nos dados de pedidos.
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
# ║  TO-DO 1: Descomente o bloco abaixo para criar as features  ║
# ║           agregadas por semana/rota com lag da semana anterior║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Descomente o bloco — agrega pedidos por semana/rota e cria lag feature ▼▼▼

# # Passo 1: Agregar por semana e rota
# df_features = df_base.groupBy(
#     "ano", "semana", "mes", "rota"
# ).agg(
#     F.count("id_pedido").alias("total_pedidos"),
#     F.avg("peso_total_kg").alias("peso_medio"),
#     F.avg("valor_frete").alias("valor_medio"),
#     F.avg("dia_da_semana").alias("dia_da_semana_medio"),
# )
#
# # Passo 2: Lag feature — volume da semana anterior por rota
# window_rota = Window.partitionBy("rota").orderBy("ano", "semana")
# df_features = df_features.withColumn(
#     "total_pedidos_semana_anterior",
#     F.lag("total_pedidos", 1).over(window_rota)
# )
#
# print(f"Features criadas: {df_features.count()} registros")
# display(df_features.limit(10))

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
# ║  TO-DO 2: Descomente o bloco abaixo para treinar o modelo   ║
# ║           RandomForest com tracking automatico via MLflow     ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Descomente o bloco — converte para Pandas, treina e avalia com MLflow ▼▼▼

# # Preparar dados (requer TO-DO 1 completo)
# df_features_clean = df_features.filter(F.col("total_pedidos_semana_anterior").isNotNull())
# pdf = df_features_clean.toPandas()
#
# # Passo 1: Separar treino e teste (80/20)
# X = pdf[feature_cols]
# y = pdf[target_col]
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
#
# # Passo 2: Habilitar autolog do MLflow para sklearn
# mlflow.sklearn.autolog()
#
# # Passo 3: Treinar dentro de um run do MLflow
# with mlflow.start_run(run_name="demanda_logistica") as run:
#     model = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)
#     model.fit(X_train, y_train)
#     y_pred = model.predict(X_test)
#     mae = mean_absolute_error(y_test, y_pred)
#     rmse = np.sqrt(mean_squared_error(y_test, y_pred))
#     r2 = r2_score(y_test, y_pred)
#     mlflow.log_metric("custom_mae", mae)
#     mlflow.log_metric("custom_rmse", rmse)
#     mlflow.log_metric("custom_r2", r2)
#     print(f"MAE:  {mae:.2f} pedidos")
#     print(f"RMSE: {rmse:.2f} pedidos")
#     print(f"R2:   {r2:.4f}")
#     print(f"\nRun ID: {run.info.run_id}")

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
# ║  TO-DO 3: Descomente o bloco abaixo para criar a UDF         ║
# ║           Haversine que calcula distancia em km entre          ║
# ║           dois pontos (lat/lon) na superficie da Terra         ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Descomente o bloco — funcao Haversine + registro como UDF do Spark ▼▼▼

# def haversine(lat1, lon1, lat2, lon2):
#     """Calcula a distancia em km entre dois pontos na Terra usando Haversine."""
#     if any(v is None for v in [lat1, lon1, lat2, lon2]):
#         return None
#     lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
#     dlat = lat2 - lat1
#     dlon = lon2 - lon1
#     a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
#     c = 2 * asin(sqrt(a))
#     r = 6371  # raio da Terra em km
#     return r * c
#
# # Registrar como UDF do Spark para usar em DataFrames e SQL
# haversine_udf = F.udf(haversine, DoubleType())
# spark.udf.register("haversine", haversine, DoubleType())
# print("UDF haversine registrada!")
# # Teste: SP -> RJ (~360km)
# print(f"Teste SP -> RJ: {haversine(-23.5505, -46.6333, -22.9068, -43.1729):.1f} km")

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
# ║  TO-DO 4: Descomente o bloco abaixo para encontrar os 5      ║
# ║           caminhoes mais proximos do pedido surpresa            ║
# ║  (requer TO-DO 3 — UDF haversine)                             ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Descomente o bloco — filtra caminhoes, join motoristas, calcula distancia ▼▼▼

# peso_pedido_ton = pedido_surpresa["peso_kg"] / 1000
# lat_pedido = pedido_surpresa["latitude"]
# lon_pedido = pedido_surpresa["longitude"]
#
# # Passo 1: Caminhoes com capacidade suficiente
# df_cam_disponiveis = spark.table("raw.caminhoes").filter(
#     F.col("capacidade_toneladas") >= peso_pedido_ton
# )
#
# # Passo 2: Motoristas ativos
# df_motoristas = spark.table("raw.motoristas").filter(F.col("status") == "Ativo")
#
# # Passo 3: Join caminhoes + motoristas
# df_cam_motorista = df_cam_disponiveis.alias("cam").join(
#     df_motoristas.alias("mot"),
#     F.col("cam.id_caminhao") == F.col("mot.id_caminhao"), "inner"
# ).select(
#     F.col("cam.id_caminhao"), F.col("cam.placa"),
#     F.col("cam.capacidade_toneladas"), F.col("cam.tipo"),
#     F.col("mot.nome").alias("motorista"), F.col("mot.celular"),
# )
#
# # Passo 4: Ultima posicao de cada caminhao
# df_posicao = df_status.alias("st").join(
#     df_cargas.alias("cg"), F.col("st.id_carga") == F.col("cg.id_carga"), "inner"
# ).select(F.col("cg.id_caminhao"), F.col("st.latitude"), F.col("st.longitude"), F.col("st.timestamp"))
# window_pos = Window.partitionBy("id_caminhao").orderBy(F.col("timestamp").desc())
# df_ultima_posicao = df_posicao.withColumn("rn", F.row_number().over(window_pos)).filter(F.col("rn") == 1).drop("rn")
#
# # Passo 5: Calcular distancia e retornar top 5
# df_resultado = df_cam_motorista.alias("cm").join(
#     df_ultima_posicao.alias("pos"), F.col("cm.id_caminhao") == F.col("pos.id_caminhao"), "inner"
# ).withColumn(
#     "distancia_km", haversine_udf(F.col("pos.latitude"), F.col("pos.longitude"), F.lit(lat_pedido), F.lit(lon_pedido))
# ).select(
#     "cm.id_caminhao", "cm.placa", "cm.tipo", "cm.capacidade_toneladas",
#     "cm.motorista", "cm.celular", "distancia_km",
# ).orderBy("distancia_km").limit(5)
#
# print("\nTop 5 caminhoes mais proximos:")
# display(df_resultado)

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
# ║  TO-DO 5: Descomente o bloco abaixo para registrar o melhor  ║
# ║           modelo no Unity Catalog (requer TO-DO 2 completo)   ║
# ╚══════════════════════════════════════════════════════════════╝
# ▼▼▼ Descomente o bloco — busca melhor run e registra modelo no UC ▼▼▼

# mlflow.set_registry_uri("databricks-uc")
#
# # Passo 1: Buscar o melhor run (menor RMSE)
# runs = mlflow.search_runs(
#     order_by=["metrics.training_root_mean_squared_error ASC"],
#     max_results=1,
# )
#
# if len(runs) > 0:
#     best_run_id = runs.iloc[0]["run_id"]
#     best_rmse = runs.iloc[0].get("metrics.training_root_mean_squared_error", "N/A")
#     print(f"Melhor run: {best_run_id} (RMSE: {best_rmse})")
#
#     # Passo 2: Registrar no Unity Catalog
#     model_name = f"{catalog_name}.ml.modelo_demanda"
#     result = mlflow.register_model(f"runs:/{best_run_id}/model", model_name)
#     print(f"Modelo registrado: {model_name} (versao {result.version})")
# else:
#     print("Nenhum run encontrado. Execute o TO-DO 2 primeiro.")

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
