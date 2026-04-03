# Databricks notebook source
# MAGIC %md
# MAGIC # Gerador de Dados Streaming - Workshop Logistica
# MAGIC
# MAGIC Este notebook gera dados simulados para alimentar os volumes de streaming:
# MAGIC - **Pedidos JSON** -> `/Volumes/{catalog}/raw/pedidos_json/`
# MAGIC - **Status JSON** -> `/Volumes/{catalog}/raw/status_json/`
# MAGIC - **NFs JSON** -> `/Volumes/{catalog}/raw/nfs_json/`
# MAGIC
# MAGIC **Fase 1 (automatica):** Exporta os dados historicos de `raw` (gerados pelo `01_dados_cadastrais.py`) como JSONs nested nos volumes para o Auto Loader consumir.
# MAGIC
# MAGIC **Fase 2 (continua):** Gera novos dados a cada 5 minutos enquanto o notebook estiver rodando.
# MAGIC
# MAGIC Execute este notebook e deixe rodando em background enquanto trabalha no pipeline SDP.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracao

# COMMAND ----------

dbutils.widgets.text("nome_participante", "", "Nome do Participante")
dbutils.widgets.text("intervalo_segundos", "300", "Intervalo entre batches (s)")

# COMMAND ----------

nome = dbutils.widgets.get("nome_participante")
intervalo_segundos = int(dbutils.widgets.get("intervalo_segundos"))

catalog_name = f"workshop_logistica_{nome}"

print(f"Catalog: {catalog_name}")
print(f"Intervalo entre batches: {intervalo_segundos}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports e Constantes

# COMMAND ----------

import json
import random
import time
import os
from datetime import datetime, timedelta

# Paths dos volumes
PATH_PEDIDOS = f"/Volumes/{catalog_name}/raw/pedidos_json"
PATH_STATUS = f"/Volumes/{catalog_name}/raw/status_json"
PATH_NFS = f"/Volumes/{catalog_name}/raw/nfs_json"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dados de Referencia para Geracao

# COMMAND ----------

# Cidades e UFs para rotas logisticas
CIDADES_UF = [
    ("Sao Paulo", "SP"), ("Campinas", "SP"), ("Guarulhos", "SP"), ("Santos", "SP"),
    ("Rio de Janeiro", "RJ"), ("Niteroi", "RJ"), ("Duque de Caxias", "RJ"),
    ("Belo Horizonte", "MG"), ("Uberlandia", "MG"), ("Contagem", "MG"),
    ("Curitiba", "PR"), ("Londrina", "PR"), ("Maringa", "PR"),
    ("Porto Alegre", "RS"), ("Caxias do Sul", "RS"), ("Canoas", "RS"),
    ("Salvador", "BA"), ("Feira de Santana", "BA"),
    ("Recife", "PE"), ("Jaboatao dos Guararapes", "PE"),
    ("Fortaleza", "CE"), ("Manaus", "AM"), ("Goiania", "GO"),
    ("Brasilia", "DF"), ("Florianopolis", "SC"), ("Joinville", "SC"),
    ("Vitoria", "ES"), ("Belem", "PA"), ("Cuiaba", "MT"), ("Campo Grande", "MS"),
]

# Produtos logisticos realistas
PRODUTOS = [
    {"descricao": "Caixa de Pecas Automotivas", "ncm": "87089990", "categoria": "Automotivo", "peso_medio": 12.5, "valor_medio": 350.0},
    {"descricao": "Pallet de Alimentos Nao Pereciveis", "ncm": "19011090", "categoria": "Alimentos", "peso_medio": 45.0, "valor_medio": 1200.0},
    {"descricao": "Bobina de Aco", "ncm": "72101200", "categoria": "Siderurgia", "peso_medio": 500.0, "valor_medio": 8500.0},
    {"descricao": "Caixa de Eletronicos", "ncm": "85176299", "categoria": "Eletronicos", "peso_medio": 8.0, "valor_medio": 2500.0},
    {"descricao": "Pallet de Bebidas", "ncm": "22021000", "categoria": "Bebidas", "peso_medio": 60.0, "valor_medio": 800.0},
    {"descricao": "Saco de Cimento 50kg", "ncm": "25232900", "categoria": "Construcao", "peso_medio": 50.0, "valor_medio": 35.0},
    {"descricao": "Caixa de Medicamentos", "ncm": "30049099", "categoria": "Farmaceutico", "peso_medio": 5.0, "valor_medio": 4500.0},
    {"descricao": "Rolo de Tecido", "ncm": "52094200", "categoria": "Textil", "peso_medio": 25.0, "valor_medio": 600.0},
    {"descricao": "Caixa de Cosmeticos", "ncm": "33049990", "categoria": "Cosmeticos", "peso_medio": 6.0, "valor_medio": 1800.0},
    {"descricao": "Tambor de Quimicos", "ncm": "29051100", "categoria": "Quimico", "peso_medio": 200.0, "valor_medio": 3200.0},
    {"descricao": "Pallet de Papel A4", "ncm": "48025690", "categoria": "Papelaria", "peso_medio": 40.0, "valor_medio": 450.0},
    {"descricao": "Caixa de Ferramentas Industriais", "ncm": "82054000", "categoria": "Industrial", "peso_medio": 15.0, "valor_medio": 900.0},
    {"descricao": "Fardo de Algodao", "ncm": "52010010", "categoria": "Textil", "peso_medio": 220.0, "valor_medio": 2800.0},
    {"descricao": "Caixa de Calcados", "ncm": "64029990", "categoria": "Calcados", "peso_medio": 10.0, "valor_medio": 1500.0},
    {"descricao": "Engradado de Frutas", "ncm": "08051000", "categoria": "Hortifruti", "peso_medio": 20.0, "valor_medio": 300.0},
    {"descricao": "Pallet de Laticinios", "ncm": "04012190", "categoria": "Laticinios", "peso_medio": 35.0, "valor_medio": 950.0},
    {"descricao": "Container de Graos", "ncm": "10059010", "categoria": "Agro", "peso_medio": 1000.0, "valor_medio": 5500.0},
    {"descricao": "Caixa de Material Eletrico", "ncm": "85444900", "categoria": "Eletrico", "peso_medio": 18.0, "valor_medio": 750.0},
    {"descricao": "Pallet de Moveis Desmontados", "ncm": "94036000", "categoria": "Moveis", "peso_medio": 80.0, "valor_medio": 2200.0},
    {"descricao": "Caixa de Brinquedos", "ncm": "95030099", "categoria": "Brinquedos", "peso_medio": 7.0, "valor_medio": 650.0},
]

# Status de transporte — alinhados com a tabela status_transporte_ref do schema raw
STATUS_TRANSPORTE = [
    (1,  "Pedido Recebido"),
    (2,  "Aguardando Coleta"),
    (3,  "Coleta Realizada"),
    (4,  "Em Trânsito para CD"),
    (5,  "Recebido no CD"),
    (6,  "Em Separação"),
    (7,  "Carga Consolidada"),
    (8,  "Saiu para Entrega"),
    (9,  "Em Trânsito"),
    (10, "Parada para Descanso"),
    (11, "Tentativa de Entrega"),
    (12, "Entrega Realizada"),
    (13, "Devolvido ao Remetente"),
    (14, "Extraviado"),
    (15, "Avaria Registrada"),
]

# Pesos para cada status — "Entrega Realizada" (12) com peso alto para gerar dados suficientes para ML
STATUS_PESOS = [5, 5, 8, 8, 6, 5, 5, 10, 10, 3, 4, 20, 5, 2, 2]

# Observacoes por status
OBSERVACOES = {
    1:  ["Pedido recebido no sistema", "Novo pedido de transporte registrado"],
    2:  ["Coleta agendada para o proximo dia util", "Motorista designado para coleta"],
    3:  ["Coleta realizada com sucesso", "Mercadoria conferida e carregada"],
    4:  ["Veiculo em deslocamento para CD", "Em transito para centro de distribuicao"],
    5:  ["Carga recebida no CD", "Mercadoria em processo de triagem"],
    6:  ["Carga em separacao no CD", "Itens sendo separados para consolidacao"],
    7:  ["Carga consolidada", "Pronta para despacho"],
    8:  ["Veiculo saiu para entrega final", "Motorista em rota de entrega"],
    9:  ["Veiculo em transito na rodovia", "Passagem pelo pedagio registrada"],
    10: ["Parada obrigatoria para descanso", "Motorista em intervalo regulamentar"],
    11: ["Tentativa de entrega sem sucesso", "Destinatario ausente"],
    12: ["Entrega realizada com sucesso", "Recebedor assinou o comprovante", "Carga entregue no destino"],
    13: ["Devolvido ao remetente", "Recusa do recebedor", "Endereco nao localizado"],
    14: ["Carga extraviada", "Em processo de investigacao"],
    15: ["Avaria identificada na carga", "Danos registrados no sistema"],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funcoes de Geracao

# COMMAND ----------

# Contadores globais para IDs unicos
_pedido_counter = random.randint(100000, 200000)
_nf_counter = random.randint(500000, 600000)
_item_counter = random.randint(1000000, 2000000)

def gerar_item_nf():
    """Gera um item de nota fiscal."""
    global _item_counter
    _item_counter += 1
    produto = random.choice(PRODUTOS)
    quantidade = random.randint(1, 50)
    valor_unitario = round(produto["valor_medio"] * random.uniform(0.7, 1.3), 2)
    peso_kg = round(produto["peso_medio"] * quantidade * random.uniform(0.8, 1.2), 2)

    return {
        "id_item": _item_counter,
        "descricao": produto["descricao"],
        "ncm": produto["ncm"],
        "quantidade": quantidade,
        "unidade": "UN",
        "valor_unitario": valor_unitario,
        "valor_total": round(valor_unitario * quantidade, 2),
        "peso_kg": peso_kg,
        "dimensoes": {
            "comprimento_cm": random.randint(20, 120),
            "largura_cm": random.randint(20, 100),
            "altura_cm": random.randint(10, 80),
        },
    }


def gerar_nota_fiscal(id_pedido, data_base=None):
    """Gera uma nota fiscal com itens."""
    global _nf_counter
    _nf_counter += 1
    num_itens = random.randint(2, 5)
    itens = [gerar_item_nf() for _ in range(num_itens)]
    valor_total = round(sum(i["valor_total"] for i in itens), 2)
    ts = data_base if data_base else datetime.now()
    data_emissao = ts + timedelta(hours=random.randint(1, 48))

    return {
        "id_nf": _nf_counter,
        "numero_nf": str(random.randint(100000, 999999)),
        "id_pedido": id_pedido,
        "data_emissao": data_emissao.strftime("%Y-%m-%d %H:%M:%S"),
        "valor_total": valor_total,
        "chave_acesso": "".join([str(random.randint(0, 9)) for _ in range(44)]),
        "itens": itens,
    }


def gerar_pedidos_batch(num_pedidos, data_base=None):
    """Gera um batch de pedidos com notas fiscais embutidas.
    Se data_base for informada, usa essa data em vez de datetime.now().
    """
    global _pedido_counter
    pedidos = []
    nfs_standalone = []

    for _ in range(num_pedidos):
        _pedido_counter += 1
        # 70% das rotas concentradas nas 10 principais cidades (volume para ML)
        if random.random() < 0.70:
            origem = random.choice(CIDADES_UF[:10])
            destino = random.choice(CIDADES_UF[:10])
        else:
            origem = random.choice(CIDADES_UF)
            destino = random.choice(CIDADES_UF)
        while destino == origem:
            destino = random.choice(CIDADES_UF)

        num_nfs = random.randint(1, 3)
        notas = [gerar_nota_fiscal(_pedido_counter, data_base=data_base) for _ in range(num_nfs)]

        peso_total = round(sum(sum(i["peso_kg"] for i in nf["itens"]) for nf in notas), 2)
        volume_total = round(
            sum(
                sum(
                    (i["dimensoes"]["comprimento_cm"] * i["dimensoes"]["largura_cm"] * i["dimensoes"]["altura_cm"])
                    / 1_000_000
                    for i in nf["itens"]
                )
                for nf in notas
            ),
            4,
        )
        valor_mercadoria = round(sum(nf["valor_total"] for nf in notas), 2)
        tipo_frete = random.choice(["CIF", "FOB"])
        valor_frete = round(valor_mercadoria * random.uniform(0.03, 0.12), 2)

        if data_base:
            ts = data_base + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
        else:
            ts = datetime.now()

        pedido = {
            "id_pedido": _pedido_counter,
            "id_cliente": random.randint(1, 1000),
            "data_pedido": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "peso_total_kg": peso_total,
            "volume_total_m3": volume_total,
            "valor_mercadoria": valor_mercadoria,
            "valor_frete": valor_frete,
            "tipo_frete": tipo_frete,
            "prioridade": random.choice(["NORMAL", "EXPRESSA", "URGENTE"]),
            "cidade_origem": origem[0],
            "uf_origem": origem[1],
            "cidade_destino": destino[0],
            "uf_destino": destino[1],
            "notas_fiscais": notas,
        }
        pedidos.append(pedido)
        nfs_standalone.extend(notas)

    return pedidos, nfs_standalone


def gerar_status_batch(num_status, data_base=None):
    """Gera um batch de atualizacoes de status de transporte."""
    status_list = []
    for _ in range(num_status):
        status = random.choices(STATUS_TRANSPORTE, weights=STATUS_PESOS)[0]
        id_status = status[0]
        # Coordenadas aproximadas do Brasil
        lat = round(random.uniform(-33.0, 5.0), 6)
        lon = round(random.uniform(-73.0, -35.0), 6)
        ts = data_base + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59)) if data_base else datetime.now()

        status_list.append({
            "id_carga": random.randint(1, 5000),
            "id_status": id_status,
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "observacao": random.choice(OBSERVACOES[id_status]),
            "latitude": lat,
            "longitude": lon,
        })

    return status_list

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funcao de Escrita nos Volumes

# COMMAND ----------

def salvar_batch(pedidos, nfs, status_updates, batch_num):
    """Salva os dados gerados nos volumes como arquivos JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Salvar pedidos
    path_pedidos = f"{PATH_PEDIDOS}/pedidos_batch_{timestamp}.json"
    with open(path_pedidos, "w", encoding="utf-8") as f:
        json.dump(pedidos, f, ensure_ascii=False, indent=2)

    # Salvar NFs standalone
    path_nfs = f"{PATH_NFS}/nfs_batch_{timestamp}.json"
    with open(path_nfs, "w", encoding="utf-8") as f:
        json.dump(nfs, f, ensure_ascii=False, indent=2)

    # Salvar status
    path_status = f"{PATH_STATUS}/status_batch_{timestamp}.json"
    with open(path_status, "w", encoding="utf-8") as f:
        json.dump(status_updates, f, ensure_ascii=False, indent=2)

    print(f"  -> Pedidos: {path_pedidos} ({len(pedidos)} registros)")
    print(f"  -> NFs:     {path_nfs} ({len(nfs)} registros)")
    print(f"  -> Status:  {path_status} ({len(status_updates)} registros)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loop Principal de Geracao
# MAGIC
# MAGIC O loop roda continuamente gerando dados a cada intervalo configurado.
# MAGIC Para parar, cancele a execucao da celula.

# COMMAND ----------

print("=" * 70)
print(f"  GERADOR DE DADOS STREAMING - Workshop Logistica")
print(f"  Catalog: {catalog_name}")
print(f"  Intervalo: {intervalo_segundos}s")
print("=" * 70)
print()

# ── Carga historica: ler de raw e escrever como JSON nested nos volumes ──
# Os dados com integridade referencial foram gerados pelo 01_dados_cadastrais.py
# Aqui apenas transformamos em JSONs nested para o Auto Loader consumir
print("📦 Exportando dados historicos de raw para volumes JSON...")
batch_num = 0

# Ler pedidos, NFs e itens do schema raw
df_pedidos_raw = spark.table(f"{catalog_name}.raw.pedidos")
df_nfs_raw = spark.table(f"{catalog_name}.raw.notas_fiscais")
df_itens_raw = spark.table(f"{catalog_name}.raw.itens_nf")

total_pedidos = df_pedidos_raw.count()
print(f"  Pedidos em raw: {total_pedidos:,}")

# Converter para Pandas para montar JSONs nested
pdf_pedidos = df_pedidos_raw.toPandas()
pdf_nfs = df_nfs_raw.toPandas()
pdf_itens = df_itens_raw.toPandas()

# Indexar NFs e itens por FK para lookup rapido
nfs_por_pedido = pdf_nfs.groupby("id_pedido")
itens_por_nf = pdf_itens.groupby("id_nf")

# Gerar JSONs em batches de ~500 pedidos (1 arquivo por batch)
BATCH_SIZE = 500
pedidos_list = pdf_pedidos.to_dict("records")

for start in range(0, len(pedidos_list), BATCH_SIZE):
    batch_num += 1
    batch_pedidos = pedidos_list[start:start + BATCH_SIZE]
    pedidos_json = []
    nfs_json = []

    for ped in batch_pedidos:
        id_pedido = ped["id_pedido"]
        notas_nested = []

        if id_pedido in nfs_por_pedido.groups:
            for _, nf in nfs_por_pedido.get_group(id_pedido).iterrows():
                itens_nested = []
                if nf["id_nf"] in itens_por_nf.groups:
                    for _, item in itens_por_nf.get_group(nf["id_nf"]).iterrows():
                        itens_nested.append({
                            "id_item": int(item["id_item"]),
                            "descricao": str(item.get("descricao", "")),
                            "ncm": str(item.get("ncm", "")),
                            "quantidade": int(item.get("quantidade", 1)),
                            "unidade": str(item.get("unidade", "UN")),
                            "valor_unitario": float(item.get("valor_unitario", 0)),
                            "valor_total": float(item.get("valor_total", 0)),
                            "peso_kg": float(item.get("peso_kg", 0)),
                            "dimensoes": {
                                "comprimento_cm": int(item.get("comprimento_cm", 0)),
                                "largura_cm": int(item.get("largura_cm", 0)),
                                "altura_cm": int(item.get("altura_cm", 0)),
                            },
                        })
                nf_dict = {
                    "id_nf": int(nf["id_nf"]),
                    "numero_nf": str(nf.get("numero_nf", "")),
                    "id_pedido": int(id_pedido),
                    "data_emissao": str(nf.get("data_emissao", "")),
                    "valor_total": float(nf.get("valor_total", 0)),
                    "chave_acesso": str(nf.get("chave_acesso", "")),
                    "itens": itens_nested,
                }
                notas_nested.append(nf_dict)
                nfs_json.append(nf_dict)

        pedido_dict = {
            "id_pedido": int(ped["id_pedido"]),
            "id_cliente": int(ped["id_cliente"]),
            "data_pedido": str(ped["data_pedido"]),
            "peso_total_kg": float(ped.get("peso_total_kg", 0)),
            "volume_total_m3": float(ped.get("volume_total_m3", 0)),
            "valor_mercadoria": float(ped.get("valor_mercadoria", 0)),
            "valor_frete": float(ped.get("valor_frete", 0)),
            "tipo_frete": str(ped.get("tipo_frete", "")),
            "prioridade": str(ped.get("prioridade", "")),
            "cidade_origem": str(ped.get("cidade_origem", "")),
            "uf_origem": str(ped.get("uf_origem", "")),
            "cidade_destino": str(ped.get("cidade_destino", "")),
            "uf_destino": str(ped.get("uf_destino", "")),
            "notas_fiscais": notas_nested,
        }
        pedidos_json.append(pedido_dict)

    # Salvar pedidos e NFs como JSON
    timestamp = f"hist_{batch_num:04d}"
    path_p = f"{PATH_PEDIDOS}/pedidos_batch_{timestamp}.json"
    path_n = f"{PATH_NFS}/nfs_batch_{timestamp}.json"
    with open(path_p, "w", encoding="utf-8") as f:
        json.dump(pedidos_json, f, ensure_ascii=False, default=str)
    with open(path_n, "w", encoding="utf-8") as f:
        json.dump(nfs_json, f, ensure_ascii=False, default=str)

    if batch_num % 20 == 0 or start + BATCH_SIZE >= len(pedidos_list):
        print(f"  Batch {batch_num}: {start+1}-{min(start+BATCH_SIZE, len(pedidos_list))} de {len(pedidos_list)} pedidos")

# Exportar status (historico_status) como JSON
df_hist_status = spark.table(f"{catalog_name}.raw.historico_status")
pdf_status = df_hist_status.toPandas()
status_list = []
for _, row in pdf_status.iterrows():
    status_list.append({
        "id_carga": int(row["id_carga"]),
        "id_status": int(row["id_status"]),
        "timestamp": str(row["timestamp"]),
        "observacao": str(row.get("observacao", "")),
        "latitude": float(row.get("latitude", 0)),
        "longitude": float(row.get("longitude", 0)),
    })

# Salvar status em batches
for start in range(0, len(status_list), 1000):
    batch_num += 1
    batch_status = status_list[start:start + 1000]
    path_s = f"{PATH_STATUS}/status_batch_hist_{batch_num:04d}.json"
    with open(path_s, "w", encoding="utf-8") as f:
        json.dump(batch_status, f, ensure_ascii=False, default=str)

print(f"\n✅ Carga historica concluida!")
print(f"   📦 {total_pedidos:,} pedidos exportados de raw para volumes JSON")
print(f"   📁 {batch_num} arquivos JSON gerados")
print(f"   Execute o pipeline SDP para processar esses dados.\n")

# ── Loop continuo: gerar dados em tempo real ──
print("🔄 Iniciando geracao continua...\n")

while True:
    batch_num += 1
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    num_pedidos = random.randint(20, 50)
    num_status = random.randint(80, 120)

    print(f"[Batch {batch_num}] {agora} - Gerando dados...")
    print(f"  Pedidos: {num_pedidos} | Status: {num_status}")

    # Gerar dados
    pedidos, nfs_standalone = gerar_pedidos_batch(num_pedidos)
    status_updates = gerar_status_batch(num_status)

    # Salvar nos volumes
    salvar_batch(pedidos, nfs_standalone, status_updates, batch_num)

    print(f"[Batch {batch_num}] Concluido!\n")

    # Aguardar proximo ciclo
    print(f"Aguardando {intervalo_segundos}s para o proximo batch...")
    time.sleep(intervalo_segundos)
