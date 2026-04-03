<p align="center">
  <img src="images/header_workshop_logistica.png" alt="Workshop Hands-On Databricks - Logística" width="100%">
</p>

<h1 align="center">🚚 Workshop Hands-On Databricks — Logística</h1>

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks">
  <img src="https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark">
  <img src="https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake">
  <img src="https://img.shields.io/badge/MLflow-0194E2?style=for-the-badge&logo=mlflow&logoColor=white" alt="MLflow">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
</p>

<p align="center">
  Workshop prático de <strong>Engenharia de Dados, Machine Learning e Analytics</strong> aplicados ao setor de <strong>Logística e Transporte</strong>, utilizando a plataforma Databricks.
</p>

---

## 👥 Apresentadores

<table align="center">
  <tr>
    <td align="center" width="300">
      <img src="images/juliandro_circle.png" width="120" style="border-radius:50%"><br>
      <strong>Juliandro Figueiró</strong><br>
      <em>Sr. Solution Architect</em><br>
      <a href="https://www.linkedin.com/in/juliandro/">LinkedIn</a>
    </td>
    <td align="center" width="300">
      <img src="images/jean_circle.png" width="120" style="border-radius:50%"><br>
      <strong>Jean Ertzogue</strong><br>
      <em>Account Executive</em><br>
      <a href="https://www.linkedin.com/in/jeanertzogue/">LinkedIn</a>
    </td>
    <td align="center" width="300">
      <img src="images/marcio_circle.png" width="120" style="border-radius:50%"><br>
      <strong>Marcio Arbex</strong><br>
      <em>Field Engineering Director</em><br>
      <a href="https://www.linkedin.com/in/marcioarbex/">LinkedIn</a>
    </td>
  </tr>
</table>

---

## 📋 Agenda

| #  | Atividade | Duração | Descrição |
|----|-----------|---------|-----------|
| ⚙️ | **Setup Inicial** | 15 min | Configuração do catálogo e geração dos dados sintéticos |
| 1️⃣ | **Lab 1 — Spark Declarative Pipelines** | 40 min | Construção do pipeline medallion (Bronze → Silver → Gold) com streaming |
| ☕ | **Coffee Break** | 15 min | — |
| 2️⃣ | **Lab 2 — Lakeflow Jobs** | 25 min | Orquestração de tarefas com DAGs, monitoramento e alertas |
| 3️⃣ | **Lab 3 — Machine Learning** | 35 min | Predição de demanda, otimização de frota e pedidos surpresa |
| 4️⃣ | **Lab 4 — AI/BI Genie + Dashboard** | 30 min | Dashboard operacional com mapas e assistente inteligente Genie |
| 🎯 | **Encerramento** | 10 min | Resumo, próximos passos e Q&A |
| | **Total** | **~170 min** | |

---

## 🏗️ Arquitetura

<p align="center">
  <img src="images/arquitetura.png" alt="Arquitetura Medallion" width="90%">
</p>

O workshop segue a arquitetura **Medallion (Bronze → Silver → Gold)**, processando dados de operações logísticas desde a ingestão via streaming até a camada analítica:

- **Fontes**: JSONs de pedidos, notas fiscais e status de transporte (streaming) + cadastros estáticos
- **Bronze**: Dados brutos com metadados de ingestão
- **Silver**: Dados limpos, enriquecidos e normalizados
- **Gold**: Agregações e KPIs prontos para consumo

---

## 📊 Modelo de Dados

<p align="center">
  <img src="images/modelo_er.png" alt="Modelo Entidade-Relacionamento" width="90%">
</p>

### Tabelas Principais

| Tabela | Registros | Descrição |
|--------|-----------|-----------|
| `clientes` | 1.000 | Empresas do Sudeste com CNPJ, coordenadas geográficas |
| `caminhoes` | 1.000 | Frota com tipos VUC a Rodotrem, capacidades e dimensões |
| `motoristas` | 1.000 | Motoristas vinculados a caminhões, com CNH e avaliação |
| `pedidos` | 50.000 | Pedidos com 52 semanas de histórico e crescimento gradual |
| `notas_fiscais` | ~150.000 | NFs vinculadas a pedidos com chave de acesso |
| `itens_nf` | ~500.000 | Itens detalhados por NF (produtos, pesos, dimensões) |
| `movimento_cargas` | 5.000 | Cargas em transporte com rotas e pedidos (JSON) |
| `historico_status` | 10.000+ | Tracking de status com geolocalização |
| `status_transporte_ref` | 15 | Tabela de referência de status |
| `produtos_referencia` | 200 | Produtos transportados com NCM e categorias |

**Dados de Streaming (exportados pelo `01a_gerador_streaming.py`):**

> O gerador lê as tabelas `raw` (com integridade referencial) e exporta como JSONs nested nos volumes para o Auto Loader consumir. Depois inicia o loop contínuo gerando novos dados em tempo real.

---

## ✅ Pré-requisitos

### Workspace Databricks
- Workspace com **Unity Catalog** habilitado
- **Serverless** habilitado (recomendado para SDP)
- Acesso a **SQL Warehouse** (Serverless ou Pro)

### Permissões Necessárias

| Recurso | Permissão | Labs |
|---------|-----------|------|
| `CREATE CATALOG` | Metastore Admin ou permissão delegada | Setup |
| `CREATE SCHEMA` | Catalog Owner | Setup |
| `CREATE TABLE` | Schema Owner | Todos |
| `CREATE VOLUME` | Schema Owner | Setup |
| Cluster / Compute | `CAN ATTACH TO` | Todos |
| SQL Warehouse | `CAN USE` | Lab 4 |
| Spark Declarative Pipelines (SDP) | `CAN MANAGE` | Lab 1 |
| Workflows / Jobs | `CAN MANAGE` | Lab 2 |
| MLflow Experiments | `CAN MANAGE` | Lab 3 |
| AI/BI Dashboards | `CAN CREATE` | Lab 4 |
| AI/BI Genie | `CAN CREATE` | Lab 4 |

---

## 📁 Estrutura do Projeto

```
databricks_lab_logistica/
│
├── 00_Setup/
│   ├── 00_configuracao_catalogo.py    # Cria catálogo, schemas e volumes
│   └── 01_dados_cadastrais.py         # Gera todos os dados sintéticos
│
├── 01_Lab_SDP/
│   ├── 01a_gerador_streaming.py       # Gerador contínuo de JSONs (pedidos, NFs, status)
│   ├── 01b_sdp_pipeline_to_do.py      # Pipeline SDP com TO-DOs (exercício)
│   └── 01c_sdp_pipeline_completo.py   # Pipeline SDP completo (referência)
│
├── 02_Lab_Jobs/
│   ├── 02a_validacao_to_do.py         # Task 1: Validação de dados (código completo)
│   ├── 02b_trigger_pipeline_to_do.py  # Task 2: Trigger pipeline via API (código completo)
│   ├── 02c_qualidade_to_do.py         # Task 3: Quality checks (código completo)
│   └── 02d_resumo_to_do.py            # Task 4: Resumo da execução (código completo)
│
├── 03_Lab_ML/
│   ├── 03a_ml_to_do.py                # ML com TO-DOs (exercício)
│   └── 03b_ml_completo.py             # ML completo (referência)
│
├── 04_Lab_AIBI/
│   ├── 04a_genie_dashboard_to_do.py   # AI/BI com TO-DOs (exercício)
│   └── 04b_genie_dashboard_completo.py # AI/BI completo (referência)
│
├── images/                             # Diagramas e imagens
└── README.md                           # Este arquivo
```

---

## 🚀 Como Começar

### Passo 1: Importar o Repositório

1. No Databricks, vá em **Workspace** → **Repos**
2. Clique em **Add** → **Git folder**
3. Cole a URL abaixo e clique em **Create Repo**

```
https://github.com/juliandrof/databricks_lab_logistica.git
```

### Passo 2: Configurar o Catálogo

1. Abra `00_Setup/00_configuracao_catalogo.py`
2. Preencha seu nome no widget **`nome_participante`** (sem espaços ou acentos)
3. Execute todas as células

> ⚠️ **IMPORTANTE**: Use o mesmo nome em TODOS os notebooks! O catálogo será `workshop_logistica_{seu_nome}`.

### Passo 3: Gerar os Dados

1. Abra `00_Setup/01_dados_cadastrais.py`
2. Preencha o mesmo nome no widget
3. Execute todas as células (~2-3 minutos)
4. Verifique o resumo final com contagens de todas as tabelas

---

## 🔬 Labs — Detalhes

### Lab 1: Spark Declarative Pipelines (SDP) — 40 min

**Conceito**: Construir um pipeline de dados completo usando a abordagem declarativa do Databricks, com ingestão streaming via Auto Loader e transformações em camadas.

**O que você vai fazer:**
1. **Iniciar o gerador de streaming** (`01a_gerador_streaming.py`) — exporta os dados históricos de `raw` como JSONs nested e depois inicia geração contínua a cada 5 minutos
2. **Configurar o pipeline** via UI do Databricks (veja instruções abaixo)
3. **Completar os TO-DOs** (`01b_sdp_pipeline_to_do.py`) — 5 exercícios e executar o pipeline

| TO-DO | Descrição | O que fazer |
|-------|-----------|-------------|
| 1 | Ler tabela `movimento_cargas` do raw | Descomentar 1 linha de `return` |
| 2 | Extrair ano, mês e dia do pedido | Descomentar 3 linhas de `.withColumn()` |
| 3 | Adicionar regra de qualidade | Descomentar 1 decorator `@dlt.expect_or_drop(...)` |
| 4 | Completar agregação por rota | Descomentar 2 métricas no `.agg()` |
| 5 | Agregar status mais recente | Descomentar bloco de `groupBy` final |

> 💡 **Dica**: Em caso de dúvida, consulte o arquivo `01c_sdp_pipeline_completo.py` como referência.

**Para configurar o pipeline SDP:**
1. Vá em **Jobs & Pipelines** → **ETL pipeline**
2. **Pipeline name**: `pipeline_logistica_{seu_nome}`
3. **Add existing assets**: selecione o notebook `01_Lab_SDP/01b_sdp_pipeline_to_do.py`
4. **Target catalog**: `workshop_logistica_{seu_nome}`
5. **Target schema**: `default` (obrigatório na UI)
6. Em **Configuration**, adicione: Key: `pipeline.nome_participante` → Value: `{seu_nome}`
7. **Compute**: Serverless (recomendado) ou cluster existente
8. Clique em **Create** e depois **Start** para executar

---

### Lab 2: Lakeflow Jobs — 25 min

**Conceito**: Orquestrar múltiplas tarefas como um DAG (Directed Acyclic Graph), automatizar execuções com scheduling e monitorar resultados.

**O que você vai fazer:**

| Atividade | Descrição |
|-----------|-----------|
| 1 | **Ler os notebooks** — Abra `02a`, `02b`, `02c` e `02d` e leia célula a célula para entender o que cada um faz |
| 2 | **Criar o Job** via UI — Monte a orquestração das 4 tarefas como um DAG no Workflows |
| 3 | **Executar e analisar logs** — Execute o job, verifique os logs de cada tarefa |
| 4 | **Criar um novo job com task Pipeline** — Em vez de usar o notebook `02b` (API), use uma task do tipo **Pipeline** nativa do Databricks |

**O que cada notebook faz:**
- `02a_validacao` — Valida existência e contagem mínima das tabelas raw + integridade referencial motoristas→caminhões
- `02b_trigger_pipeline` — Dispara o pipeline SDP via REST API e monitora o status até conclusão
- `02c_qualidade` — Verifica qualidade dos dados na silver (frete > 0) e na gold (dados existem, sem negativos)
- `02d_resumo` — Gera inventário de todas as tabelas nos schemas raw, silver e gold com contagem e timestamps

**Para criar o Job (Atividade 2):**
1. Vá em **Workflows** → **Jobs** → **Create Job**
2. Nome: `job_logistica_{seu_nome}`
3. **Importante:** Em **Job parameters**, adicione o parâmetro: Key: `nome_participante` → Value: `{seu_nome}`
4. Adicione 4 tarefas (tipo **Notebook**) na seguinte ordem, cada uma apontando para o notebook correspondente:

```
Task 1: Validação (02a)  ──→  Task 2: Trigger Pipeline (02b)  ──→  Task 3: Qualidade (02c)  ──→  Task 4: Resumo (02d)
```

5. Execute e acompanhe os logs de cada tarefa

**Para criar o Job com task Pipeline (Atividade 4):**
1. Crie um **novo Job** (`job_logistica_{seu_nome}_v2`)
2. Em **Job parameters**, adicione: Key: `nome_participante` → Value: `{seu_nome}`
3. Monte o DAG com 4 tarefas:
   - Task 1: **Notebook** → `02a_validacao_to_do.py`
   - Task 2: **Pipeline** → selecione o pipeline `pipeline_logistica_{seu_nome}` (em vez do notebook 02b)
   - Task 3: **Notebook** → `02c_qualidade_to_do.py` (depende da Task 2)
   - Task 4: **Notebook** → `02d_resumo_to_do.py` (depende da Task 3)
4. Compare os logs e o comportamento com o Job anterior

---

### Lab 3: Machine Learning — 35 min

**Conceito**: Aplicar Machine Learning em três cenários reais de logística: predição de demanda, otimização de caminhões vazios e resposta a pedidos surpresa.

**O que você vai fazer:**
1. **Predição de Demanda** — Prever volume semanal de pedidos por rota
2. **Caminhões Vazios** — Identificar caminhões ociosos e match com pedidos próximos
3. **Pedidos Surpresa** — Encontrar o caminhão mais próximo com capacidade disponível

| TO-DO | Descrição | Use Case | Dificuldade |
|-------|-----------|----------|-------------|
| 1 | Criar features de demanda | Predição | ⭐⭐ |
| 2 | Treinar modelo com MLflow | Predição | ⭐⭐ |
| 3 | Implementar distância Haversine | Caminhões Vazios | ⭐⭐ |
| 4 | Encontrar caminhão para pedido surpresa | Pedido Surpresa | ⭐⭐⭐ |
| 5 | Registrar modelo no Unity Catalog | MLflow | ⭐ |

---

### Lab 4: AI/BI Genie + Dashboard — 30 min

**Conceito**: Criar um dashboard operacional interativo com mapas e utilizar o Genie para permitir que qualquer pessoa faça perguntas aos dados em linguagem natural.

**O que você vai fazer:**
1. **Criar views otimizadas** para dashboarding
2. **Configurar um Genie Room** com contexto das tabelas
3. **Criar um AI/BI Dashboard** com KPIs, gráficos e **mapa de entregas**
4. **Testar o Genie** com perguntas em português

| TO-DO | Descrição | Dificuldade |
|-------|-----------|-------------|
| 1 | Criar view geográfica para mapa de entregas | ⭐⭐ |
| 2 | Criar view de volume diário | ⭐ |
| 3 | Adicionar comentários para Genie | ⭐ |
| 4 | Query de status para KPI cards | ⭐ |

**Sugestão de layout do Dashboard:**

<p align="center">
  <img src="images/dashboard_layout.png" alt="Dashboard Operacional - Layout Sugerido" width="100%">
</p>

---

## ⚡ Dicas Importantes

> 🔄 **Consistência**: Use sempre o **mesmo nome** no widget `nome_participante` em todos os notebooks.

> 📖 **Referência**: Cada lab possui uma versão `_completo.py`. Use como referência quando travar em algum TO-DO.

> 🧹 **Limpeza**: Ao final do workshop, execute o comando abaixo para remover seu catálogo:
> ```sql
> DROP CATALOG workshop_logistica_{seu_nome} CASCADE;
> ```

> ⏱️ **Streaming**: O gerador de dados (`01a`) precisa estar rodando durante o Lab 1 para alimentar o pipeline com dados novos.

---

## 🔗 Referências

| Recurso | Link |
|---------|------|
| Spark Declarative Pipelines (SDP) | [Documentação](https://docs.databricks.com/en/dlt/index.html) |
| Auto Loader | [Documentação](https://docs.databricks.com/en/ingestion/cloud-files/index.html) |
| Lakeflow Jobs | [Documentação](https://docs.databricks.com/en/jobs/index.html) |
| MLflow | [Documentação](https://docs.databricks.com/en/mlflow/index.html) |
| Unity Catalog | [Documentação](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) |
| AI/BI Dashboards | [Documentação](https://docs.databricks.com/en/dashboards/index.html) |
| AI/BI Genie | [Documentação](https://docs.databricks.com/en/genie/index.html) |
| Data Quality Expectations | [Documentação](https://docs.databricks.com/en/dlt/expectations.html) |

---

<p align="center">
  <img src="images/databricks_logo.png" width="200">
  <br><br>
  <em>Workshop desenvolvido pela equipe Databricks Field Engineering — Brasil 🇧🇷</em>
</p>
