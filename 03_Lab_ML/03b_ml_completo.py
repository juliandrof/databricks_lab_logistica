# Databricks notebook source

# MAGIC %md
# MAGIC # Lab 3: Machine Learning para Logistica (COMPLETO)
# MAGIC
# MAGIC Este notebook contem as respostas completas de todos os TO-DOs.
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
# MAGIC **Features criadas:**
# MAGIC - `semana`: numero da semana do ano
# MAGIC - `mes`: mes do pedido
# MAGIC - `dia_da_semana_medio`: dia da semana medio dos pedidos naquela semana/rota
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

# RESPOSTA TO-DO 1: Criar o DataFrame de features agregadas

# Passo 1: Agregar por semana e rota
# Agrupamos os pedidos por ano, semana, mes e rota, calculando metricas agregadas
df_features = df_base.groupBy(
    "ano", "semana", "mes", "rota"
).agg(
    F.count("id_pedido").alias("total_pedidos"),
    F.avg("peso_total_kg").alias("peso_medio"),
    F.avg("valor_frete").alias("valor_medio"),
    F.avg("dia_da_semana").alias("dia_da_semana_medio")
)

# Passo 2: Criar lag feature com Window
# A lag feature captura a tendencia: se a semana passada teve muitos pedidos,
# a proxima provavelmente tambem tera
window_rota = Window.partitionBy("rota").orderBy("ano", "semana")
df_features = df_features.withColumn(
    "total_pedidos_semana_anterior",
    F.lag("total_pedidos", 1).over(window_rota)
)

print(f"Features criadas: {df_features.count()} registros")
display(df_features.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Visualizacao das Features

# COMMAND ----------

# Visualizar distribuicao do volume de pedidos por semana
pdf_viz = df_features.groupBy("semana").agg(
    F.sum("total_pedidos").alias("total")
).orderBy("semana").toPandas()

fig, ax = plt.subplots(figsize=(14, 5))
ax.bar(pdf_viz["semana"], pdf_viz["total"], color="#FF3621", alpha=0.8)
ax.set_xlabel("Semana do Ano")
ax.set_ylabel("Total de Pedidos")
ax.set_title("Volume de Pedidos por Semana")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Treinamento do Modelo com MLflow
# MAGIC
# MAGIC Vamos usar o **MLflow** para rastrear nossos experimentos.
# MAGIC O MLflow registra automaticamente:
# MAGIC - Parametros do modelo (n_estimators, max_depth, etc.)
# MAGIC - Metricas (MAE, RMSE, R2)
# MAGIC - O modelo serializado (para deploy futuro)

# COMMAND ----------

# Converter para Pandas para usar sklearn
# Remover linhas sem lag feature (primeira semana de cada rota)
df_features_clean = df_features.filter(F.col("total_pedidos_semana_anterior").isNotNull())
pdf = df_features_clean.toPandas()

# Definir features e target
feature_cols = ["semana", "mes", "dia_da_semana_medio", "peso_medio", "valor_medio", "total_pedidos_semana_anterior"]
target_col = "total_pedidos"

print(f"Dataset para treino: {len(pdf)} registros")
print(f"Features: {feature_cols}")
print(f"Target: {target_col}")

# COMMAND ----------

# RESPOSTA TO-DO 2: Treinar o modelo com MLflow tracking

# Passo 1: Separar treino e teste (80/20)
X = pdf[feature_cols]
y = pdf[target_col]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(f"Treino: {len(X_train)} amostras")
print(f"Teste: {len(X_test)} amostras")

# Passo 2: Habilitar autolog do MLflow para sklearn
# Isso registra automaticamente parametros, metricas e artefatos
mlflow.sklearn.autolog()

# Passo 3: Treinar dentro de um run do MLflow
with mlflow.start_run(run_name="demanda_logistica") as run:
    # Criar e treinar o modelo Random Forest
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1  # Usar todos os cores disponiveis
    )
    model.fit(X_train, y_train)

    # Fazer predicoes no conjunto de teste
    y_pred = model.predict(X_test)

    # Calcular metricas
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    # Logar metricas customizadas (autolog ja loga as basicas)
    mlflow.log_metric("custom_mae", mae)
    mlflow.log_metric("custom_rmse", rmse)
    mlflow.log_metric("custom_r2", r2)

    print(f"MAE:  {mae:.2f} pedidos")
    print(f"RMSE: {rmse:.2f} pedidos")
    print(f"R2:   {r2:.4f}")
    print(f"\nRun ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Importancia das Features

# COMMAND ----------

# Visualizar importancia das features
importances = model.feature_importances_
feat_imp = pd.DataFrame({
    "feature": feature_cols,
    "importance": importances
}).sort_values("importance", ascending=True)

fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(feat_imp["feature"], feat_imp["importance"], color="#00A972", alpha=0.8)
ax.set_xlabel("Importancia")
ax.set_title("Importancia das Features - Random Forest")
ax.grid(axis="x", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Predicao vs Real

# COMMAND ----------

# Grafico de predicao vs valor real
fig, ax = plt.subplots(figsize=(8, 8))
ax.scatter(y_test, y_pred, alpha=0.5, color="#1B3139", s=20)
ax.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], "r--", lw=2, label="Perfeito")
ax.set_xlabel("Valor Real")
ax.set_ylabel("Valor Predito")
ax.set_title("Predicao vs Real - Demanda por Rota")
ax.legend()
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Caso de Uso 2: Otimizacao de Caminhoes Vazios
# MAGIC
# MAGIC ## Contexto
# MAGIC Um dos maiores desperdicios na logistica e o **frete de retorno vazio**.
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

# RESPOSTA TO-DO 3: Implementar a UDF de distancia Haversine

def haversine(lat1, lon1, lat2, lon2):
    """
    Calcula a distancia em km entre dois pontos na Terra
    usando a formula de Haversine.

    Parametros:
        lat1, lon1: latitude e longitude do ponto 1 (graus)
        lat2, lon2: latitude e longitude do ponto 2 (graus)

    Retorna:
        Distancia em quilometros
    """
    # Verificar valores nulos
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return None

    # Converter graus para radianos
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Diferencas
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Formula de Haversine
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))

    # Raio da Terra em km
    r = 6371

    return r * c

# Registrar como UDF do Spark para usar em DataFrames
haversine_udf = F.udf(haversine, DoubleType())

# Tambem registrar para uso em SQL
spark.udf.register("haversine", haversine, DoubleType())

print("UDF haversine registrada com sucesso!")

# Teste rapido: distancia entre Sao Paulo e Rio de Janeiro (~360km)
teste = haversine(-23.5505, -46.6333, -22.9068, -43.1729)
print(f"Teste SP -> RJ: {teste:.1f} km")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Combinar Caminhoes com Pedidos

# COMMAND ----------

# Cross join entre caminhoes livres e pedidos pendentes
# Em producao, filtrar por regiao para evitar cross join enorme
df_matches = df_caminhoes_livres.alias("cam").crossJoin(
    df_pedidos_pendentes.alias("ped")
).withColumn(
    "distancia_km",
    haversine_udf(
        F.col("lat_caminhao"), F.col("lon_caminhao"),
        F.col("lat_pedido"), F.col("lon_pedido")
    )
).filter(
    # Filtrar apenas caminhoes com capacidade suficiente
    (F.col("capacidade_toneladas") * 1000 >= F.col("peso_total_kg")) &
    (F.col("capacidade_volume_m3") >= F.col("volume_total_m3"))
).filter(
    # Limitar a caminhoes dentro de 200km
    F.col("distancia_km") <= 200
)

# Rankear por distancia (menor = melhor)
window_pedido = Window.partitionBy("id_pedido").orderBy("distancia_km")
df_recomendacoes = df_matches.withColumn(
    "rank", F.row_number().over(window_pedido)
).filter(F.col("rank") <= 3)  # Top 3 caminhoes por pedido

print(f"Total de recomendacoes: {df_recomendacoes.count()}")
display(df_recomendacoes.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Salvar Recomendacoes

# COMMAND ----------

# Criar schema ml se nao existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.ml")

# Salvar recomendacoes
df_recomendacoes.select(
    "id_pedido", "id_caminhao", "placa", "distancia_km",
    "capacidade_toneladas", "peso_total_kg", "rank",
    "cidade_atual", "cidade_origem"
).write.mode("overwrite").saveAsTable(f"{catalog_name}.ml.recomendacao_caminhoes_vazios")

print(f"Recomendacoes salvas em {catalog_name}.ml.recomendacao_caminhoes_vazios")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.7 Visualizacao das Recomendacoes

# COMMAND ----------

# Distribuicao de distancias das recomendacoes
pdf_dist = df_recomendacoes.select("distancia_km").toPandas()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Histograma de distancias
axes[0].hist(pdf_dist["distancia_km"], bins=30, color="#FF3621", alpha=0.7, edgecolor="white")
axes[0].set_xlabel("Distancia (km)")
axes[0].set_ylabel("Frequencia")
axes[0].set_title("Distribuicao de Distancias - Recomendacoes")
axes[0].grid(axis="y", alpha=0.3)

# Contagem por rank
pdf_rank = df_recomendacoes.groupBy("rank").count().orderBy("rank").toPandas()
axes[1].bar(pdf_rank["rank"].astype(str), pdf_rank["count"], color="#00A972", alpha=0.8)
axes[1].set_xlabel("Rank")
axes[1].set_ylabel("Quantidade")
axes[1].set_title("Recomendacoes por Rank")
axes[1].grid(axis="y", alpha=0.3)

plt.tight_layout()
plt.show()

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

# RESPOSTA TO-DO 4: Encontrar os 5 caminhoes mais proximos

peso_pedido_ton = pedido_surpresa["peso_kg"] / 1000
lat_pedido = pedido_surpresa["latitude"]
lon_pedido = pedido_surpresa["longitude"]

# Passo 1: Caminhoes com capacidade suficiente para o pedido
df_cam_disponiveis = spark.table("raw.caminhoes").filter(
    F.col("capacidade_toneladas") >= peso_pedido_ton
)
print(f"Caminhoes com capacidade >= {peso_pedido_ton}t: {df_cam_disponiveis.count()}")

# Passo 2: Motoristas ativos
df_motoristas = spark.table("raw.motoristas").filter(
    F.col("status") == "Ativo"
)
print(f"Motoristas ativos: {df_motoristas.count()}")

# Passo 3: Join caminhoes + motoristas
# Cada motorista esta associado a um caminhao via id_caminhao
df_cam_motorista = df_cam_disponiveis.alias("cam").join(
    df_motoristas.alias("mot"),
    F.col("cam.id_caminhao") == F.col("mot.id_caminhao"),
    "inner"
).select(
    F.col("cam.id_caminhao"),
    F.col("cam.placa"),
    F.col("cam.capacidade_toneladas"),
    F.col("cam.volume_m3").alias("capacidade_volume_m3"),
    F.col("cam.tipo"),
    F.col("mot.nome").alias("motorista"),
    F.col("mot.celular")
)

# Passo 4: Obter ultima posicao de cada caminhao
# Usar o ultimo registro de status de transporte vinculado a cargas do caminhao
df_posicao = df_status.alias("st").join(
    df_cargas.alias("cg"),
    F.col("st.id_carga") == F.col("cg.id_carga"),
    "inner"
).select(
    F.col("cg.id_caminhao"),
    F.col("st.latitude"),
    F.col("st.longitude"),
    F.col("st.timestamp")
)

# Pegar a posicao mais recente de cada caminhao
window_pos = Window.partitionBy("id_caminhao").orderBy(F.col("timestamp").desc())
df_ultima_posicao = df_posicao.withColumn(
    "rn", F.row_number().over(window_pos)
).filter(F.col("rn") == 1).drop("rn")

# Passo 5: Join com posicao, calcular distancia e ordenar
df_resultado = df_cam_motorista.alias("cm").join(
    df_ultima_posicao.alias("pos"),
    F.col("cm.id_caminhao") == F.col("pos.id_caminhao"),
    "inner"
).withColumn(
    "distancia_km",
    haversine_udf(
        F.col("pos.latitude"), F.col("pos.longitude"),
        F.lit(lat_pedido), F.lit(lon_pedido)
    )
).select(
    F.col("cm.id_caminhao"),
    F.col("cm.placa"),
    F.col("cm.tipo"),
    F.col("cm.capacidade_toneladas"),
    F.col("cm.capacidade_volume_m3"),
    F.col("cm.motorista"),
    F.col("cm.celular"),
    F.col("pos.latitude").alias("lat_caminhao"),
    F.col("pos.longitude").alias("lon_caminhao"),
    F.col("distancia_km")
).orderBy("distancia_km").limit(5)

print("\nTop 5 caminhoes mais proximos para o pedido surpresa:")
display(df_resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Visualizacao do Resultado

# COMMAND ----------

# Tabela formatada do resultado
pdf_resultado = df_resultado.toPandas()

fig, ax = plt.subplots(figsize=(10, 4))
ax.barh(
    pdf_resultado["placa"] + " (" + pdf_resultado["motorista"] + ")",
    pdf_resultado["distancia_km"],
    color=["#FF3621", "#FF6B4A", "#FF9A7B", "#FFC4B0", "#FFE0D5"],
    edgecolor="white"
)
ax.set_xlabel("Distancia (km)")
ax.set_title(f"Top 5 Caminhoes Mais Proximos - Pedido Surpresa em {pedido_surpresa['cidade']}/{pedido_surpresa['uf']}")
ax.grid(axis="x", alpha=0.3)
ax.invert_yaxis()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Registrar o Melhor Modelo no Unity Catalog
# MAGIC
# MAGIC O MLflow permite registrar modelos no **Unity Catalog** para
# MAGIC versionamento, governanca e deploy. Vamos registrar o modelo
# MAGIC de predicao de demanda treinado no Caso de Uso 1.

# COMMAND ----------

# RESPOSTA TO-DO 5: Registrar o melhor modelo no Unity Catalog

# Configurar o registry URI para Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Passo 1: Buscar o melhor run (menor RMSE)
# Procurar no experimento atual
runs = mlflow.search_runs(
    order_by=["metrics.training_root_mean_squared_error ASC"],
    max_results=1
)

if len(runs) > 0:
    best_run_id = runs.iloc[0]["run_id"]
    best_rmse = runs.iloc[0].get("metrics.training_root_mean_squared_error", "N/A")
    print(f"Melhor run encontrado:")
    print(f"  Run ID: {best_run_id}")
    print(f"  RMSE:   {best_rmse}")

    # Passo 2: Registrar o modelo no Unity Catalog
    model_name = f"{catalog_name}.ml.modelo_demanda"
    result = mlflow.register_model(
        f"runs:/{best_run_id}/model",
        model_name
    )
    print(f"\nModelo registrado com sucesso!")
    print(f"  Nome:   {model_name}")
    print(f"  Versao: {result.version}")
else:
    print("Nenhum run encontrado. Execute o treinamento (Caso de Uso 1) primeiro.")

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
# MAGIC ### Principais aprendizados:
# MAGIC - **Feature Engineering** e essencial: lag features capturam tendencias temporais
# MAGIC - **MLflow** facilita o tracking de experimentos e versionamento de modelos
# MAGIC - **UDFs** permitem estender o Spark com logica customizada (ex: Haversine)
# MAGIC - **Unity Catalog** oferece governanca centralizada para modelos de ML
# MAGIC - Combinar **dados espaciais** (lat/long) com regras de negocio resolve problemas reais de logistica
