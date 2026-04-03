# Databricks notebook source
# MAGIC %md
# MAGIC # Gerador de Dados Streaming - Workshop Logistica
# MAGIC
# MAGIC Este notebook gera dados simulados para alimentar os volumes de streaming:
# MAGIC - **Pedidos JSON** -> `/Volumes/{catalog}/raw/pedidos_json/`
# MAGIC - **Status JSON** -> `/Volumes/{catalog}/raw/status_json/`
# MAGIC - **NFs JSON** -> `/Volumes/{catalog}/raw/nfs_json/`
# MAGIC
# MAGIC **Fase 1 (automatica):** Gera carga historica de 1 ano (~200k pedidos, ~400k NFs, ~500k status) com crescimento semanal.
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

# Status de transporte (ordem logica)
STATUS_TRANSPORTE = [
    (1, "Carga Registrada"),
    (2, "Coleta Agendada"),
    (3, "Em Coleta"),
    (4, "Coletado"),
    (5, "No CD Origem"),
    (6, "Em Transito"),
    (7, "No CD Destino"),
    (8, "Saiu para Entrega"),
    (9, "Entregue"),
    (10, "Devolvido"),
]

# Observacoes por status
OBSERVACOES = {
    1: ["Carga registrada no sistema", "Novo pedido de transporte recebido", "Carga cadastrada aguardando coleta"],
    2: ["Coleta agendada para o proximo dia util", "Motorista designado para coleta", "Coleta confirmada pelo embarcador"],
    3: ["Veiculo em deslocamento para coleta", "Motorista a caminho do remetente", "Inicio do processo de coleta"],
    4: ["Carga coletada com sucesso", "Mercadoria conferida e carregada", "Coleta finalizada sem avarias"],
    5: ["Carga recebida no centro de distribuicao", "Mercadoria em processo de triagem", "Carga aguardando consolidacao"],
    6: ["Veiculo em transito na rodovia", "Passagem pelo pedágio registrada", "Transito normal sem intercorrencias"],
    7: ["Carga chegou ao CD destino", "Mercadoria em conferencia no destino", "Aguardando roteirizacao para entrega"],
    8: ["Veiculo saiu para entrega final", "Motorista em rota de entrega", "Ultima milha iniciada"],
    9: ["Entrega realizada com sucesso", "Recebedor assinou o comprovante", "Carga entregue no destino"],
    10: ["Destinatario ausente - devolvido", "Endereco nao localizado", "Recusa do recebedor"],
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


def gerar_nota_fiscal(id_pedido):
    """Gera uma nota fiscal com itens."""
    global _nf_counter
    _nf_counter += 1
    num_itens = random.randint(2, 5)
    itens = [gerar_item_nf() for _ in range(num_itens)]
    valor_total = round(sum(i["valor_total"] for i in itens), 2)

    return {
        "id_nf": _nf_counter,
        "numero_nf": str(random.randint(100000, 999999)),
        "id_pedido": id_pedido,
        "data_emissao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
        notas = [gerar_nota_fiscal(_pedido_counter) for _ in range(num_nfs)]

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


def gerar_status_batch(num_status):
    """Gera um batch de atualizacoes de status de transporte."""
    status_list = []
    for _ in range(num_status):
        status = random.choice(STATUS_TRANSPORTE)
        id_status = status[0]
        # Coordenadas aproximadas do Brasil
        lat = round(random.uniform(-33.0, 5.0), 6)
        lon = round(random.uniform(-73.0, -35.0), 6)

        status_list.append({
            "id_carga": random.randint(1, 5000),
            "id_status": id_status,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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

# ── Carga historica: gerar 1 ano de dados (~200k pedidos) ──
# Volume cresce gradualmente semana a semana para simular crescimento do negocio
# Cada semana gera 1 arquivo JSON com os pedidos daquela semana
TOTAL_SEMANAS = 52
print(f"📦 Gerando carga historica de {TOTAL_SEMANAS} semanas (~1 ano)...")
batch_num = 0
total_pedidos_historico = 0
total_nfs_historico = 0
total_status_historico = 0

for semana in range(TOTAL_SEMANAS, 0, -1):
    # Volume crescente: semana 52 (mais antiga) ~300 pedidos, semana 1 (mais recente) ~700
    base_pedidos = int(300 + (TOTAL_SEMANAS - semana) * 8)  # crescimento ~8 pedidos/semana
    # Variacao aleatoria de +/- 15% para parecer organico
    num_pedidos_semana = int(base_pedidos * random.uniform(0.85, 1.15))

    # Distribuir pedidos ao longo dos 7 dias da semana
    for dia in range(7):
        dias_atras = semana * 7 - dia
        if dias_atras <= 0:
            continue
        data_dia = datetime.now() - timedelta(days=dias_atras)

        # Mais pedidos em dias uteis (seg-sex) do que fim de semana
        if dia < 5:  # seg-sex
            pedidos_dia = int(num_pedidos_semana * random.uniform(0.16, 0.20))
        else:  # sab-dom
            pedidos_dia = int(num_pedidos_semana * random.uniform(0.04, 0.08))

        if pedidos_dia < 5:
            pedidos_dia = 5

        num_status_dia = int(pedidos_dia * random.uniform(2.0, 4.0))

        batch_num += 1
        pedidos, nfs_standalone = gerar_pedidos_batch(pedidos_dia, data_base=data_dia)
        status_updates = gerar_status_batch(num_status_dia)
        salvar_batch(pedidos, nfs_standalone, status_updates, batch_num)

        total_pedidos_historico += len(pedidos)
        total_nfs_historico += len(nfs_standalone)
        total_status_historico += len(status_updates)

    if semana % 10 == 0 or semana <= 3:
        print(f"  Semana -{semana}: ~{num_pedidos_semana} pedidos (base: {base_pedidos})")

print(f"\n✅ Carga historica concluida!")
print(f"   📦 {total_pedidos_historico:,} pedidos | {total_nfs_historico:,} NFs | {total_status_historico:,} status")
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
