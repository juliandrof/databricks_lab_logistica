# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4b: AI/BI Genie + Dashboard - VERSÃO COMPLETA (Gabarito)
# MAGIC
# MAGIC **Objetivo:** Criar views otimizadas para alimentar dashboards e o AI/BI Genie, transformando nossos dados logísticos em visualizações interativas.
# MAGIC
# MAGIC > ⚠️ **ATENÇÃO:** Este notebook contém todas as respostas dos TO-DOs. Use apenas como referência!
# MAGIC
# MAGIC ## O que vamos fazer neste lab:
# MAGIC 1. Criar **views no schema gold** otimizadas para dashboards (mapas, KPIs, gráficos)
# MAGIC 2. Adicionar **comentários** às tabelas para o Genie entender o contexto dos dados
# MAGIC 3. Configurar um **Genie Room** para perguntas em linguagem natural
# MAGIC 4. Montar um **AI/BI Dashboard** operacional completo
# MAGIC
# MAGIC ### Schemas disponíveis:
# MAGIC | Schema | Tabelas |
# MAGIC |--------|---------|
# MAGIC | `raw`  | clientes, caminhoes, motoristas, movimento_cargas |
# MAGIC | `silver` | silver_pedidos, silver_itens_nf, silver_status_transporte |
# MAGIC | `gold` | gold_volume_por_rota, gold_performance_frota, gold_status_entregas |
# MAGIC | `ml` | recomendacao_caminhoes_vazios |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

dbutils.widgets.text("nome_participante", "", "Nome do Participante")

# COMMAND ----------

nome_participante = dbutils.widgets.get("nome_participante").strip().lower().replace(" ", "_")
assert nome_participante != "", "⚠️ Por favor, preencha seu nome no widget acima!"
catalog = f"workshop_logistica_{nome_participante}"

spark.sql(f"USE CATALOG {catalog}")
print(f"✅ Usando catálogo: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Seção 1: Views para Dashboard (schema gold)
# MAGIC
# MAGIC Vamos criar views no schema `gold` otimizadas para visualização em dashboards.
# MAGIC Essas views serão a **camada de consumo** dos nossos dados logísticos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 View: `vw_mapa_entregas` - Visão Geográfica para MAPA
# MAGIC
# MAGIC Esta view é fundamental para a visualização em **mapa** no dashboard.
# MAGIC Precisamos das coordenadas (latitude/longitude) tanto da **origem** quanto do **destino** de cada pedido.
# MAGIC
# MAGIC **Colunas esperadas:**
# MAGIC - `id_pedido`, `cidade_origem`, `uf_origem`, `lat_origem`, `long_origem`
# MAGIC - `cidade_destino`, `uf_destino`, `lat_destino`, `long_destino`
# MAGIC - `peso_total_kg`, `valor_frete`, `tipo_frete`, `prioridade`, `status_atual`
# MAGIC
# MAGIC **Dica de joins:**
# MAGIC - `silver_pedidos` tem `id_cliente` (destino) e `cidade_origem`
# MAGIC - `raw.clientes` tem `latitude` e `longitude` por cidade
# MAGIC - `silver_status_transporte` tem o status mais recente de cada pedido

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TO-DO 1 COMPLETO: View geográfica para mapa de entregas
# MAGIC CREATE OR REPLACE VIEW gold.vw_mapa_entregas AS
# MAGIC WITH ultimo_status AS (
# MAGIC   SELECT
# MAGIC     id_pedido,
# MAGIC     descricao_status AS status_atual,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY id_pedido ORDER BY data_hora_status DESC) AS rn
# MAGIC   FROM silver.silver_status_transporte
# MAGIC ),
# MAGIC coords_origem AS (
# MAGIC   SELECT DISTINCT
# MAGIC     cidade,
# MAGIC     uf,
# MAGIC     latitude,
# MAGIC     longitude
# MAGIC   FROM raw.clientes
# MAGIC )
# MAGIC SELECT
# MAGIC   p.id_pedido,
# MAGIC   p.cidade_origem,
# MAGIC   p.uf_origem,
# MAGIC   o.latitude AS lat_origem,
# MAGIC   o.longitude AS long_origem,
# MAGIC   c.cidade AS cidade_destino,
# MAGIC   c.uf AS uf_destino,
# MAGIC   c.latitude AS lat_destino,
# MAGIC   c.longitude AS long_destino,
# MAGIC   p.peso_total_kg,
# MAGIC   p.valor_frete,
# MAGIC   p.tipo_frete,
# MAGIC   p.prioridade,
# MAGIC   COALESCE(s.status_atual, 'Sem Status') AS status_atual
# MAGIC FROM silver.silver_pedidos p
# MAGIC -- Join para coordenadas de DESTINO (via cliente)
# MAGIC LEFT JOIN raw.clientes c ON p.id_cliente = c.id_cliente
# MAGIC -- Join para coordenadas de ORIGEM (via cidade_origem)
# MAGIC LEFT JOIN coords_origem o ON p.cidade_origem = o.cidade AND p.uf_origem = o.uf
# MAGIC -- Join para status mais recente
# MAGIC LEFT JOIN ultimo_status s ON p.id_pedido = s.id_pedido AND s.rn = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 View: `vw_kpis_operacionais` - KPIs Operacionais
# MAGIC
# MAGIC View com os principais indicadores de performance da operação logística.
# MAGIC Esta view será usada nos **cards de KPI** do dashboard.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_kpis_operacionais AS
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT p.id_pedido) AS total_pedidos,
# MAGIC   COUNT(DISTINCT CASE WHEN s.descricao_status = 'Entregue' THEN p.id_pedido END) AS total_entregas_realizadas,
# MAGIC   COUNT(DISTINCT CASE WHEN s.descricao_status = 'Em Trânsito' THEN p.id_pedido END) AS total_em_transito,
# MAGIC   ROUND(
# MAGIC     COUNT(DISTINCT CASE WHEN s.descricao_status = 'Entregue' THEN p.id_pedido END) * 100.0
# MAGIC     / NULLIF(COUNT(DISTINCT p.id_pedido), 0), 1
# MAGIC   ) AS taxa_entrega_pct,
# MAGIC   ROUND(SUM(DISTINCT p.peso_total_kg), 2) AS peso_total_transportado,
# MAGIC   ROUND(SUM(DISTINCT p.valor_frete), 2) AS valor_frete_total,
# MAGIC   ROUND(AVG(DISTINCT p.valor_frete), 2) AS frete_medio,
# MAGIC   COUNT(DISTINCT pf.placa) AS total_caminhoes_ativos
# MAGIC FROM silver.silver_pedidos p
# MAGIC LEFT JOIN (
# MAGIC   SELECT id_pedido, descricao_status,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY id_pedido ORDER BY data_hora_status DESC) AS rn
# MAGIC   FROM silver.silver_status_transporte
# MAGIC ) s ON p.id_pedido = s.id_pedido AND s.rn = 1
# MAGIC LEFT JOIN gold.gold_performance_frota pf ON 1=1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 View: `vw_volume_diario` - Tendência de Volume Diário
# MAGIC
# MAGIC Esta view alimentará o **gráfico de linha** mostrando a evolução diária dos pedidos.
# MAGIC
# MAGIC **Colunas esperadas:**
# MAGIC - `data` - data do pedido (sem hora)
# MAGIC - `total_pedidos` - contagem de pedidos no dia
# MAGIC - `peso_total` - soma do peso no dia
# MAGIC - `valor_total` - soma do valor de frete no dia
# MAGIC - `pedidos_expressos` - quantidade de pedidos com prioridade 'Expressa'
# MAGIC - `pedidos_urgentes` - quantidade de pedidos com prioridade 'Urgente'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TO-DO 2 COMPLETO: View de volume diário
# MAGIC CREATE OR REPLACE VIEW gold.vw_volume_diario AS
# MAGIC SELECT
# MAGIC   CAST(data_pedido AS DATE) AS data,
# MAGIC   COUNT(*) AS total_pedidos,
# MAGIC   ROUND(SUM(peso_total_kg), 2) AS peso_total,
# MAGIC   ROUND(SUM(valor_frete), 2) AS valor_total,
# MAGIC   SUM(CASE WHEN prioridade = 'Expressa' THEN 1 ELSE 0 END) AS pedidos_expressos,
# MAGIC   SUM(CASE WHEN prioridade = 'Urgente' THEN 1 ELSE 0 END) AS pedidos_urgentes
# MAGIC FROM silver.silver_pedidos
# MAGIC GROUP BY CAST(data_pedido AS DATE)
# MAGIC ORDER BY data;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 View: `vw_performance_motoristas` - Performance dos Motoristas
# MAGIC
# MAGIC Top motoristas por entregas, km percorridos e avaliação média.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_performance_motoristas AS
# MAGIC SELECT
# MAGIC   m.nome AS nome_motorista,
# MAGIC   m.cnh,
# MAGIC   m.categoria_cnh,
# MAGIC   COUNT(DISTINCT mc.id_pedido) AS total_entregas,
# MAGIC   ROUND(SUM(mc.km_percorridos), 1) AS total_km,
# MAGIC   ROUND(AVG(mc.avaliacao_entrega), 2) AS avaliacao_media,
# MAGIC   ROUND(SUM(mc.valor_frete), 2) AS valor_frete_total,
# MAGIC   ROUND(AVG(mc.km_percorridos), 1) AS km_medio_por_entrega
# MAGIC FROM raw.motoristas m
# MAGIC INNER JOIN raw.movimento_cargas mc ON m.id_motorista = mc.id_motorista
# MAGIC GROUP BY m.nome, m.cnh, m.categoria_cnh
# MAGIC ORDER BY total_entregas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 View: `vw_rotas_mais_frequentes` - Top 20 Rotas
# MAGIC
# MAGIC As rotas com maior volume de pedidos e frete. Será usada em uma **tabela** no dashboard.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_rotas_mais_frequentes AS
# MAGIC SELECT
# MAGIC   rota,
# MAGIC   total_pedidos,
# MAGIC   peso_total_kg,
# MAGIC   valor_frete_total,
# MAGIC   frete_medio,
# MAGIC   ticket_medio_kg
# MAGIC FROM gold.gold_volume_por_rota
# MAGIC ORDER BY total_pedidos DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Seção 2: Comentários nas Tabelas para o Genie
# MAGIC
# MAGIC O **AI/BI Genie** usa os comentários (metadata) das tabelas e colunas para entender
# MAGIC o contexto dos dados e responder perguntas em linguagem natural com mais precisão.
# MAGIC
# MAGIC Quanto **mais descritivos** forem os comentários, **melhor** o Genie responde!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TO-DO 3 COMPLETO: Comentários nas views para o Genie
# MAGIC
# MAGIC -- ===== vw_mapa_entregas =====
# MAGIC COMMENT ON TABLE gold.vw_mapa_entregas IS 'Visão geográfica de entregas com coordenadas de origem e destino para visualização em mapa. Cada linha representa um pedido com sua localização de coleta (origem) e entrega (destino), incluindo latitude e longitude para plotagem em mapa interativo.';
# MAGIC
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.id_pedido IS 'Identificador único do pedido de transporte';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.cidade_origem IS 'Cidade onde a carga foi coletada (origem do transporte)';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.uf_origem IS 'Estado (UF) de origem da carga';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.lat_origem IS 'Latitude da cidade de origem do pedido, usar para plotar no mapa';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.long_origem IS 'Longitude da cidade de origem do pedido, usar para plotar no mapa';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.cidade_destino IS 'Cidade de destino da entrega (onde o cliente está)';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.uf_destino IS 'Estado (UF) de destino da entrega';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.lat_destino IS 'Latitude da cidade de destino, usar para plotar no mapa';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.long_destino IS 'Longitude da cidade de destino, usar para plotar no mapa';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.peso_total_kg IS 'Peso total da carga do pedido em quilogramas';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.valor_frete IS 'Valor do frete cobrado para este pedido em reais (BRL)';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.tipo_frete IS 'Tipo de frete: CIF (por conta do remetente) ou FOB (por conta do destinatário)';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.prioridade IS 'Prioridade do pedido: Normal, Urgente ou Expressa';
# MAGIC COMMENT ON COLUMN gold.vw_mapa_entregas.status_atual IS 'Status mais recente da entrega: Entregue, Em Trânsito, Pendente, Atrasado, Coletado';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== vw_kpis_operacionais =====
# MAGIC COMMENT ON TABLE gold.vw_kpis_operacionais IS 'KPIs operacionais consolidados da operação logística. Retorna uma única linha com os principais indicadores: total de pedidos, entregas realizadas, taxa de entrega, peso transportado, valor de frete e caminhões ativos. Ideal para cards de KPI em dashboards.';
# MAGIC
# MAGIC COMMENT ON COLUMN gold.vw_kpis_operacionais.total_pedidos IS 'Quantidade total de pedidos registrados no sistema';
# MAGIC COMMENT ON COLUMN gold.vw_kpis_operacionais.total_entregas_realizadas IS 'Quantidade de pedidos com status Entregue';
# MAGIC COMMENT ON COLUMN gold.vw_kpis_operacionais.total_em_transito IS 'Quantidade de pedidos atualmente em trânsito';
# MAGIC COMMENT ON COLUMN gold.vw_kpis_operacionais.taxa_entrega_pct IS 'Percentual de pedidos entregues sobre o total (0 a 100)';
# MAGIC COMMENT ON COLUMN gold.vw_kpis_operacionais.peso_total_transportado IS 'Soma do peso em kg de todos os pedidos';
# MAGIC COMMENT ON COLUMN gold.vw_kpis_operacionais.valor_frete_total IS 'Soma do valor de frete de todos os pedidos em reais (BRL)';
# MAGIC COMMENT ON COLUMN gold.vw_kpis_operacionais.frete_medio IS 'Valor médio de frete por pedido em reais (BRL)';
# MAGIC COMMENT ON COLUMN gold.vw_kpis_operacionais.total_caminhoes_ativos IS 'Quantidade de caminhões distintos com movimentações registradas';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== vw_volume_diario =====
# MAGIC COMMENT ON TABLE gold.vw_volume_diario IS 'Tendência de volume diário de pedidos logísticos. Cada linha representa um dia com a contagem de pedidos, peso total, valor de frete e breakdown por prioridade (expressa e urgente). Ideal para gráficos de linha mostrando evolução temporal.';
# MAGIC
# MAGIC COMMENT ON COLUMN gold.vw_volume_diario.data IS 'Data do pedido (sem horário), usar como eixo X em gráficos de tendência';
# MAGIC COMMENT ON COLUMN gold.vw_volume_diario.total_pedidos IS 'Quantidade de pedidos registrados neste dia';
# MAGIC COMMENT ON COLUMN gold.vw_volume_diario.peso_total IS 'Soma do peso em kg de todos os pedidos do dia';
# MAGIC COMMENT ON COLUMN gold.vw_volume_diario.valor_total IS 'Soma do valor de frete de todos os pedidos do dia em reais (BRL)';
# MAGIC COMMENT ON COLUMN gold.vw_volume_diario.pedidos_expressos IS 'Quantidade de pedidos com prioridade Expressa no dia';
# MAGIC COMMENT ON COLUMN gold.vw_volume_diario.pedidos_urgentes IS 'Quantidade de pedidos com prioridade Urgente no dia';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== vw_performance_motoristas =====
# MAGIC COMMENT ON TABLE gold.vw_performance_motoristas IS 'Performance individual dos motoristas da frota logística. Mostra total de entregas, quilômetros percorridos, avaliação média e frete gerado por motorista. Ordenado por número de entregas. Útil para ranking e análise de produtividade.';
# MAGIC
# MAGIC COMMENT ON COLUMN gold.vw_performance_motoristas.nome_motorista IS 'Nome completo do motorista';
# MAGIC COMMENT ON COLUMN gold.vw_performance_motoristas.total_entregas IS 'Quantidade total de entregas realizadas pelo motorista';
# MAGIC COMMENT ON COLUMN gold.vw_performance_motoristas.avaliacao_media IS 'Nota média de avaliação das entregas (escala de 1 a 5)';
# MAGIC COMMENT ON COLUMN gold.vw_performance_motoristas.total_km IS 'Total de quilômetros percorridos pelo motorista';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== vw_rotas_mais_frequentes =====
# MAGIC COMMENT ON TABLE gold.vw_rotas_mais_frequentes IS 'Top 20 rotas mais frequentes da operação logística, ordenadas por volume de pedidos. Mostra a rota (origem → destino), quantidade de pedidos, peso total e valores de frete. Útil para identificar corredores logísticos principais.';
# MAGIC
# MAGIC COMMENT ON COLUMN gold.vw_rotas_mais_frequentes.rota IS 'Rota no formato Cidade Origem → Cidade Destino';
# MAGIC COMMENT ON COLUMN gold.vw_rotas_mais_frequentes.total_pedidos IS 'Quantidade de pedidos nesta rota';
# MAGIC COMMENT ON COLUMN gold.vw_rotas_mais_frequentes.valor_frete_total IS 'Soma do valor de frete de todos os pedidos desta rota em reais (BRL)';
# MAGIC COMMENT ON COLUMN gold.vw_rotas_mais_frequentes.frete_medio IS 'Valor médio de frete por pedido nesta rota em reais (BRL)';

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Seção 3: Configurando o Genie Room
# MAGIC
# MAGIC O **AI/BI Genie** permite que usuários façam perguntas em **linguagem natural** sobre os dados.
# MAGIC Vamos configurar um Genie Room com nossas views logísticas.
# MAGIC
# MAGIC ## Passo a Passo para criar o Genie Room:
# MAGIC
# MAGIC ### Passo 1: Acessar o Genie
# MAGIC 1. No menu lateral do Databricks, clique em **AI/BI** > **Genie Rooms**
# MAGIC 2. Clique em **"+ New"** no canto superior direito
# MAGIC
# MAGIC ### Passo 2: Configurar o Room
# MAGIC 1. **Nome:** `Operações Logísticas`
# MAGIC 2. **Descrição:** `Room para análise da operação logística - pedidos, entregas, rotas e performance de frota`
# MAGIC 3. **SQL Warehouse:** Selecione o warehouse do workshop
# MAGIC
# MAGIC ### Passo 3: Adicionar Tabelas
# MAGIC Adicione as seguintes views do schema `gold`:
# MAGIC - `gold.vw_mapa_entregas`
# MAGIC - `gold.vw_kpis_operacionais`
# MAGIC - `gold.vw_volume_diario`
# MAGIC - `gold.vw_performance_motoristas`
# MAGIC - `gold.vw_rotas_mais_frequentes`
# MAGIC - `gold.gold_status_entregas`
# MAGIC
# MAGIC ### Passo 4: Adicionar Perguntas de Exemplo
# MAGIC Configure as seguintes perguntas como exemplos para orientar os usuários:
# MAGIC
# MAGIC | # | Pergunta |
# MAGIC |---|----------|
# MAGIC | 1 | Qual o volume de pedidos por estado? |
# MAGIC | 2 | Quais as 10 rotas com maior valor de frete? |
# MAGIC | 3 | Qual a taxa de entrega dos últimos 7 dias? |
# MAGIC | 4 | Quais motoristas têm a melhor avaliação? |
# MAGIC | 5 | Mostre no mapa onde estão as entregas pendentes |
# MAGIC | 6 | Qual o peso médio transportado por rota? |
# MAGIC | 7 | Quantos pedidos expressos temos em aberto? |
# MAGIC
# MAGIC ### Passo 5: Testar!
# MAGIC Após criar o room, teste com perguntas como:
# MAGIC - "Qual o frete médio por estado de destino?"
# MAGIC - "Quais são os 5 motoristas com mais entregas?"
# MAGIC - "Mostre a evolução diária de pedidos na última semana"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Seção 4: Criando o AI/BI Dashboard
# MAGIC
# MAGIC Agora vamos criar um **Dashboard Operacional** completo usando o AI/BI Dashboards.
# MAGIC
# MAGIC ## Visão Geral do Dashboard
# MAGIC
# MAGIC O dashboard terá os seguintes widgets organizados em uma página:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                  DASHBOARD OPERACIONAL - LOGÍSTICA                  │
# MAGIC ├──────────┬──────────┬──────────┬──────────┬─────────────────────────┤
# MAGIC │ Total    │ Entregas │ Taxa de  │ Frete    │ Caminhões               │
# MAGIC │ Pedidos  │ Realizad.│ Entrega  │ Médio    │ Ativos                  │
# MAGIC │  1.247   │   983    │  78,8%   │ R$1.450  │    42                   │
# MAGIC ├──────────┴──────────┴──────────┴──────────┴─────────────────────────┤
# MAGIC │                                                                      │
# MAGIC │  ┌─────────────────────────────┐  ┌──────────────────────────────┐  │
# MAGIC │  │   📍 MAPA DE ENTREGAS       │  │  📊 VOLUME POR UF           │  │
# MAGIC │  │                              │  │                              │  │
# MAGIC │  │   [Mapa do Brasil com        │  │  SP ████████████ 320        │  │
# MAGIC │  │    pontos de origem e         │  │  RJ ████████    210        │  │
# MAGIC │  │    destino coloridos          │  │  MG ██████      180        │  │
# MAGIC │  │    por status]                │  │  PR █████       150        │  │
# MAGIC │  │                              │  │  RS ████        120        │  │
# MAGIC │  └─────────────────────────────┘  └──────────────────────────────┘  │
# MAGIC │                                                                      │
# MAGIC │  ┌─────────────────────────────┐  ┌──────────────────────────────┐  │
# MAGIC │  │   📈 VOLUME DIÁRIO          │  │  🥧 STATUS ENTREGAS         │  │
# MAGIC │  │                              │  │                              │  │
# MAGIC │  │   ╱╲    ╱╲                  │  │     Entregue  62%           │  │
# MAGIC │  │  ╱  ╲╱╱  ╲    ╱╲          │  │     Em Trânsito 25%        │  │
# MAGIC │  │ ╱         ╲╱╱  ╲         │  │     Pendente  8%            │  │
# MAGIC │  │╱                  ╲       │  │     Atrasado  5%            │  │
# MAGIC │  └─────────────────────────────┘  └──────────────────────────────┘  │
# MAGIC │                                                                      │
# MAGIC │  ┌──────────────────────────────────────────────────────────────┐   │
# MAGIC │  │   📋 TOP ROTAS POR VOLUME                                    │   │
# MAGIC │  │                                                                │   │
# MAGIC │  │   Rota               │ Pedidos │ Peso(kg) │ Frete Total      │   │
# MAGIC │  │   SP → RJ            │   45    │  12.300  │  R$ 65.400       │   │
# MAGIC │  │   SP → MG            │   38    │  10.800  │  R$ 55.200       │   │
# MAGIC │  │   RJ → ES            │   32    │   8.500  │  R$ 42.100       │   │
# MAGIC │  └──────────────────────────────────────────────────────────────┘   │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Query para Status das Entregas (widget Pie Chart)
# MAGIC
# MAGIC Query que mostra a distribuição de entregas por status, usada no widget de **Pie Chart**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TO-DO 4 COMPLETO: Distribuição de entregas por status
# MAGIC SELECT
# MAGIC   descricao_status,
# MAGIC   COUNT(*) AS total
# MAGIC FROM gold.gold_status_entregas
# MAGIC GROUP BY descricao_status
# MAGIC ORDER BY total DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Passo a Passo para Criar o Dashboard
# MAGIC
# MAGIC #### Passo 1: Criar o Dashboard
# MAGIC 1. No menu lateral, clique em **AI/BI** > **Dashboards**
# MAGIC 2. Clique em **"+ Create Dashboard"**
# MAGIC 3. Nome: **"Dashboard Operacional - Logística"**
# MAGIC
# MAGIC #### Passo 2: Configurar o Canvas
# MAGIC - O dashboard abre no modo de edição (canvas)
# MAGIC - Na parte inferior, você verá a aba **"Data"** para adicionar datasets
# MAGIC
# MAGIC #### Passo 3: Adicionar Datasets
# MAGIC Para cada widget, precisamos de um dataset. Clique em **"+ Add dataset"** e adicione:
# MAGIC
# MAGIC | Dataset | Query SQL |
# MAGIC |---------|-----------|
# MAGIC | `ds_kpis` | `SELECT * FROM gold.vw_kpis_operacionais` |
# MAGIC | `ds_mapa` | `SELECT * FROM gold.vw_mapa_entregas` |
# MAGIC | `ds_volume_diario` | `SELECT * FROM gold.vw_volume_diario` |
# MAGIC | `ds_status` | `SELECT descricao_status, COUNT(*) as total FROM gold.gold_status_entregas GROUP BY descricao_status` |
# MAGIC | `ds_volume_uf` | `SELECT uf_destino, COUNT(*) as total_pedidos, SUM(peso_total_kg) as peso_total FROM gold.vw_mapa_entregas GROUP BY uf_destino ORDER BY total_pedidos DESC` |
# MAGIC | `ds_top_rotas` | `SELECT * FROM gold.vw_rotas_mais_frequentes` |
# MAGIC
# MAGIC #### Passo 4: Criar os Widgets
# MAGIC
# MAGIC **4.4.1 - KPI Cards (linha superior)**
# MAGIC
# MAGIC Para cada KPI, arraste um widget **"Counter"** para o canvas:
# MAGIC
# MAGIC | Card | Dataset | Campo | Formato |
# MAGIC |------|---------|-------|---------|
# MAGIC | Total Pedidos | `ds_kpis` | `total_pedidos` | Número inteiro |
# MAGIC | Entregas Realizadas | `ds_kpis` | `total_entregas_realizadas` | Número inteiro |
# MAGIC | Taxa de Entrega | `ds_kpis` | `taxa_entrega_pct` | Percentual (%) |
# MAGIC | Frete Médio | `ds_kpis` | `frete_medio` | Moeda (R$) |
# MAGIC | Caminhões Ativos | `ds_kpis` | `total_caminhoes_ativos` | Número inteiro |
# MAGIC
# MAGIC > **Dica:** Organize os 5 cards em uma linha horizontal no topo do dashboard.
# MAGIC > Use cores para destacar: verde para taxa de entrega, azul para totais.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **4.4.2 - Mapa de Entregas**
# MAGIC
# MAGIC 1. Arraste um widget **"Map"** para o canvas (área grande, lado esquerdo)
# MAGIC 2. Dataset: `ds_mapa`
# MAGIC 3. Configuração:
# MAGIC    - **Latitude:** `lat_destino`
# MAGIC    - **Longitude:** `long_destino`
# MAGIC    - **Color:** `status_atual` (para colorir por status)
# MAGIC    - **Tooltip:** `id_pedido`, `cidade_destino`, `uf_destino`, `status_atual`
# MAGIC 4. Paleta de cores sugerida:
# MAGIC    - Entregue = Verde
# MAGIC    - Em Trânsito = Azul
# MAGIC    - Pendente = Amarelo
# MAGIC    - Atrasado = Vermelho
# MAGIC
# MAGIC > **Dica:** O mapa mostrará pontos espalhados pelo Brasil, cada um representando
# MAGIC > o destino de um pedido, colorido conforme o status atual da entrega.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **4.4.3 - Volume por UF (Bar Chart)**
# MAGIC
# MAGIC 1. Arraste um widget **"Bar"** para o canvas (lado direito do mapa)
# MAGIC 2. Dataset: `ds_volume_uf`
# MAGIC 3. Configuração:
# MAGIC    - **X-axis:** `uf_destino`
# MAGIC    - **Y-axis:** `total_pedidos`
# MAGIC    - **Sort:** Descending by `total_pedidos`
# MAGIC    - **Orientation:** Horizontal (para facilitar leitura dos estados)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **4.4.4 - Volume Diário (Line Chart)**
# MAGIC
# MAGIC 1. Arraste um widget **"Line"** para o canvas (abaixo do mapa)
# MAGIC 2. Dataset: `ds_volume_diario`
# MAGIC 3. Configuração:
# MAGIC    - **X-axis:** `data`
# MAGIC    - **Y-axis:** `total_pedidos`
# MAGIC    - Adicione uma segunda série: `pedidos_expressos` (linha tracejada)
# MAGIC    - **Title:** "Evolução Diária de Pedidos"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **4.4.5 - Status das Entregas (Pie Chart)**
# MAGIC
# MAGIC 1. Arraste um widget **"Pie"** para o canvas (ao lado do volume diário)
# MAGIC 2. Dataset: `ds_status`
# MAGIC 3. Configuração:
# MAGIC    - **Label:** `descricao_status`
# MAGIC    - **Value:** `total`
# MAGIC    - **Show percentages:** Sim
# MAGIC    - **Title:** "Distribuição por Status"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **4.4.6 - Top Rotas (Table)**
# MAGIC
# MAGIC 1. Arraste um widget **"Table"** para o canvas (parte inferior, largura total)
# MAGIC 2. Dataset: `ds_top_rotas`
# MAGIC 3. Colunas visíveis:
# MAGIC    - `rota`, `total_pedidos`, `peso_total_kg`, `valor_frete_total`, `frete_medio`
# MAGIC 4. Formatação:
# MAGIC    - `valor_frete_total` e `frete_medio`: formato moeda (R$)
# MAGIC    - `peso_total_kg`: formato número com separador de milhar
# MAGIC
# MAGIC #### Passo 5: Publicar
# MAGIC 1. Clique em **"Publish"** no canto superior direito
# MAGIC 2. O dashboard estará disponível para todos os usuários com acesso ao workspace
# MAGIC 3. Opcionalmente, configure um **schedule** para atualização automática dos dados

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Validação
# MAGIC
# MAGIC Execute a célula abaixo para verificar se todas as views foram criadas corretamente.

# COMMAND ----------

# Validação das views criadas
views_esperadas = [
    "vw_mapa_entregas",
    "vw_kpis_operacionais",
    "vw_volume_diario",
    "vw_performance_motoristas",
    "vw_rotas_mais_frequentes"
]

print("=" * 60)
print("VALIDAÇÃO DAS VIEWS DO LAB 4")
print("=" * 60)

for view in views_esperadas:
    try:
        df = spark.sql(f"SELECT COUNT(*) as total FROM gold.{view}")
        total = df.collect()[0]["total"]
        print(f"✅ gold.{view}: {total} registros")
    except Exception as e:
        print(f"❌ gold.{view}: ERRO - {str(e)[:80]}")

print("=" * 60)

# COMMAND ----------

# Validação dos comentários
print("=" * 60)
print("VALIDAÇÃO DOS COMENTÁRIOS (GENIE)")
print("=" * 60)

for view in views_esperadas:
    try:
        result = spark.sql(f"DESCRIBE TABLE EXTENDED gold.{view}")
        comment_row = result.filter("col_name = 'Comment'").collect()
        if comment_row and comment_row[0]["data_type"]:
            print(f"✅ gold.{view}: comentário encontrado")
        else:
            print(f"⚠️ gold.{view}: sem comentário na tabela")
    except Exception as e:
        print(f"❌ gold.{view}: ERRO - {str(e)[:80]}")

print("=" * 60)
print("Todas as views e comentários estão configurados!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Parabéns! 🎉
# MAGIC
# MAGIC Você completou o **Lab 4 - AI/BI Genie + Dashboard**!
# MAGIC
# MAGIC ### Resumo do que foi feito:
# MAGIC - ✅ Views otimizadas para dashboards (com coordenadas para mapas!)
# MAGIC - ✅ Comentários nas tabelas para o Genie
# MAGIC - ✅ Genie Room configurado para perguntas em linguagem natural
# MAGIC - ✅ AI/BI Dashboard com KPIs, mapas, gráficos e tabelas
# MAGIC
# MAGIC ### Próximos passos:
# MAGIC - Explore o Genie Room fazendo perguntas sobre a operação
# MAGIC - Personalize o dashboard com filtros e cores
# MAGIC - Compartilhe o dashboard com a equipe
