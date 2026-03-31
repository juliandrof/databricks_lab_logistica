# Databricks notebook source
# MAGIC %md
# MAGIC # Geração de Dados Sintéticos - Workshop Logística
# MAGIC
# MAGIC Este notebook gera todos os dados cadastrais e transacionais sintéticos para o workshop de logística.
# MAGIC
# MAGIC **Tabelas geradas:**
# MAGIC | # | Tabela | Registros |
# MAGIC |---|--------|-----------|
# MAGIC | 1 | clientes | 1.000 |
# MAGIC | 2 | caminhoes | 1.000 |
# MAGIC | 3 | motoristas | 1.000 |
# MAGIC | 4 | status_transporte_ref | 15 |
# MAGIC | 5 | produtos_referencia | 200 |
# MAGIC | 6 | pedidos | 10.000 |
# MAGIC | 7 | notas_fiscais + itens_nf | ~60.000 + variável |
# MAGIC | 8 | movimento_cargas | 5.000 |
# MAGIC | 9 | historico_status | 10.000 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

dbutils.widgets.text("nome_participante", "", "Nome do Participante")

# COMMAND ----------

nome_participante = dbutils.widgets.get("nome_participante").strip().lower().replace(" ", "_")

if not nome_participante:
    raise ValueError("Por favor, informe o nome do participante no widget 'nome_participante'.")

catalog_name = f"workshop_logistica_{nome_participante}"
schema_name = "raw"

print(f"Catalog: {catalog_name}")
print(f"Schema: {catalog_name}.{schema_name}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Usando: {catalog_name}.{schema_name}")

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

random.seed(42)

# Data de referência
DATA_REFERENCIA = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dados Auxiliares Compartilhados

# COMMAND ----------

# ---------- Cidades do Sudeste com coordenadas reais ----------
CIDADES = [
    # SP
    ("São Paulo", "SP", -23.5505, -46.6333, "01000-000"),
    ("Campinas", "SP", -22.9099, -47.0626, "13000-000"),
    ("Santos", "SP", -23.9608, -46.3336, "11000-000"),
    ("Ribeirão Preto", "SP", -21.1704, -49.3732, "14000-000"),
    ("Sorocaba", "SP", -23.5015, -47.4526, "18000-000"),
    ("São José dos Campos", "SP", -23.1896, -45.8841, "12200-000"),
    ("Guarulhos", "SP", -23.4538, -46.5333, "07000-000"),
    ("Osasco", "SP", -23.5325, -46.7917, "06000-000"),
    ("Jundiaí", "SP", -23.1857, -46.8978, "13200-000"),
    ("Piracicaba", "SP", -22.7254, -47.6492, "13400-000"),
    ("Bauru", "SP", -22.3246, -49.0871, "17000-000"),
    ("Limeira", "SP", -22.5641, -47.4013, "13480-000"),
    ("Franca", "SP", -20.5390, -47.4008, "14400-000"),
    ("Taubaté", "SP", -23.0266, -45.5557, "12000-000"),
    ("São Carlos", "SP", -22.0087, -47.8909, "13560-000"),
    ("Araraquara", "SP", -21.7845, -48.1754, "14800-000"),
    ("Marília", "SP", -22.2139, -49.9461, "17500-000"),
    ("Presidente Prudente", "SP", -22.1207, -51.3882, "19000-000"),
    ("Mogi das Cruzes", "SP", -23.5229, -46.1855, "08700-000"),
    ("Sumaré", "SP", -22.8208, -47.2672, "13170-000"),
    # RJ
    ("Rio de Janeiro", "RJ", -22.9068, -43.1729, "20000-000"),
    ("Niterói", "RJ", -22.8833, -43.1036, "24000-000"),
    ("Petrópolis", "RJ", -22.5112, -43.1779, "25600-000"),
    ("Volta Redonda", "RJ", -22.5023, -44.1036, "27200-000"),
    ("Campos dos Goytacazes", "RJ", -21.7623, -41.3181, "28000-000"),
    ("Nova Iguaçu", "RJ", -22.7592, -43.4510, "26200-000"),
    ("Duque de Caxias", "RJ", -22.7856, -43.3117, "25000-000"),
    ("São Gonçalo", "RJ", -22.8269, -43.0634, "24400-000"),
    ("Macaé", "RJ", -22.3768, -41.7869, "27900-000"),
    ("Angra dos Reis", "RJ", -23.0067, -44.3181, "23900-000"),
    # MG
    ("Belo Horizonte", "MG", -19.9167, -43.9345, "30000-000"),
    ("Uberlândia", "MG", -18.9186, -48.2772, "38400-000"),
    ("Contagem", "MG", -19.9320, -44.0539, "32000-000"),
    ("Juiz de Fora", "MG", -21.7642, -43.3503, "36000-000"),
    ("Betim", "MG", -19.9678, -44.1983, "32600-000"),
    ("Montes Claros", "MG", -16.7350, -43.8615, "39400-000"),
    ("Uberaba", "MG", -19.7472, -47.9319, "38000-000"),
    ("Governador Valadares", "MG", -18.8510, -41.9494, "35000-000"),
    ("Ipatinga", "MG", -19.4684, -42.5367, "35160-000"),
    ("Poços de Caldas", "MG", -21.7872, -46.5614, "37700-000"),
    # ES
    ("Vitória", "ES", -20.3155, -40.3128, "29000-000"),
    ("Vila Velha", "ES", -20.3297, -40.2925, "29100-000"),
    ("Serra", "ES", -20.1209, -40.3075, "29160-000"),
    ("Cariacica", "ES", -20.2633, -40.4166, "29140-000"),
    ("Cachoeiro de Itapemirim", "ES", -20.8489, -41.1131, "29300-000"),
    ("Linhares", "ES", -19.3910, -40.0720, "29900-000"),
    ("Colatina", "ES", -19.5395, -40.6308, "29700-000"),
    ("Guarapari", "ES", -20.6743, -40.4998, "29200-000"),
]

# ---------- Nomes para razão social ----------
PREFIXOS_EMPRESA = [
    "Distribuidora", "Comercial", "Indústria", "Transportadora", "Logística",
    "Armazéns", "Comércio", "Atacado", "Varejo", "Empório", "Central",
    "Depósito", "Trading", "Importadora", "Exportadora", "Supermercados",
    "Materiais", "Ferragens", "Autopeças", "Farmacêutica", "Metalúrgica",
    "Siderúrgica", "Papel e Celulose", "Alimentos", "Bebidas", "Frigorífico",
    "Laticínios", "Têxtil", "Confecções", "Embalagens"
]

NOMES_EMPRESA = [
    "Norte Sul", "Santos", "Horizonte", "Planalto", "Serra Verde",
    "Atlântico", "Brasil Central", "Sudeste", "Três Irmãos", "São Jorge",
    "Minas Gerais", "Paulista", "Fluminense", "Capixaba", "Bandeirante",
    "Ipiranga", "Tiradentes", "Monte Azul", "Rio Claro", "Vale do Sol",
    "Costa Rica", "Bela Vista", "Santa Cruz", "São Marcos", "Estrela",
    "Águia", "Fenix", "Progresso", "União", "Vitória", "Aliança",
    "Pioneira", "Continental", "Nacional", "Imperial", "Real"
]

SUFIXOS_EMPRESA = [
    "Ltda", "SA", "ME", "EPP", "Eireli", "e Filhos Ltda",
    "e Cia Ltda", "do Brasil SA", "Comércio e Serviços Ltda",
    "Indústria e Comércio Ltda", "Distribuidora Ltda"
]

# ---------- Nomes brasileiros ----------
PRIMEIROS_NOMES = [
    "João", "José", "Carlos", "Paulo", "Pedro", "Lucas", "Marcos", "André",
    "Rafael", "Fernando", "Roberto", "Ricardo", "Antônio", "Luiz", "Eduardo",
    "Marcelo", "Bruno", "Daniel", "Gustavo", "Diego", "Thiago", "Felipe",
    "Leonardo", "Matheus", "Gabriel", "Henrique", "Vinícius", "Rodrigo",
    "Alexandre", "Sérgio", "Fábio", "Márcio", "Leandro", "Renato", "Cláudio",
    "Maria", "Ana", "Juliana", "Fernanda", "Patrícia", "Camila", "Luciana",
    "Adriana", "Aline", "Vanessa", "Tatiana", "Cláudia", "Simone", "Mônica",
    "Beatriz", "Larissa", "Gabriela", "Amanda", "Bruna", "Carolina"
]

SOBRENOMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves",
    "Pereira", "Lima", "Gomes", "Costa", "Ribeiro", "Martins", "Carvalho",
    "Almeida", "Lopes", "Soares", "Fernandes", "Vieira", "Barbosa", "Rocha",
    "Dias", "Nascimento", "Andrade", "Moreira", "Nunes", "Marques", "Machado",
    "Mendes", "Freitas", "Cardoso", "Ramos", "Gonçalves", "Santana", "Teixeira",
    "Araújo", "Pinto", "Correia", "Moura", "Azevedo", "Campos", "Monteiro",
    "Batista", "Rezende", "Cunha", "Duarte", "Castro", "Melo", "Reis", "Borges",
    "Fonseca", "Tavares", "Pires", "Coelho", "Medeiros"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tabela: clientes (1.000 registros)

# COMMAND ----------

def gerar_cnpj():
    """Gera CNPJ no formato XX.XXX.XXX/0001-XX"""
    p1 = random.randint(10, 99)
    p2 = random.randint(100, 999)
    p3 = random.randint(100, 999)
    p4 = random.randint(10, 99)
    return f"{p1:02d}.{p2:03d}.{p3:03d}/0001-{p4:02d}"

clientes_data = []
cnpjs_usados = set()

for i in range(1, 1001):
    cidade, uf, lat, lon, cep_base = random.choice(CIDADES)
    # Pequena variação nas coordenadas para simular endereços diferentes na mesma cidade
    lat_var = lat + random.uniform(-0.05, 0.05)
    lon_var = lon + random.uniform(-0.05, 0.05)

    # CNPJ único
    while True:
        cnpj = gerar_cnpj()
        if cnpj not in cnpjs_usados:
            cnpjs_usados.add(cnpj)
            break

    prefixo = random.choice(PREFIXOS_EMPRESA)
    nome = random.choice(NOMES_EMPRESA)
    sufixo = random.choice(SUFIXOS_EMPRESA)
    razao_social = f"{prefixo} {nome} {sufixo}"

    clientes_data.append((
        i, cnpj, razao_social, uf, cidade, cep_base,
        round(lat_var, 6), round(lon_var, 6)
    ))

schema_clientes = StructType([
    StructField("id_cliente", IntegerType(), False),
    StructField("cnpj", StringType(), False),
    StructField("razao_social", StringType(), False),
    StructField("uf", StringType(), False),
    StructField("cidade", StringType(), False),
    StructField("cep", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
])

df_clientes = spark.createDataFrame(clientes_data, schema=schema_clientes)
df_clientes.write.mode("overwrite").saveAsTable(f"{catalog_name}.raw.clientes")
print(f"Tabela clientes criada com {df_clientes.count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tabela: caminhoes (1.000 registros)

# COMMAND ----------

TIPOS_CAMINHAO = {
    "VUC":      {"capacidade": 3.5,  "comp": 6.3,  "larg": 2.2, "alt": 2.7},
    "Toco":     {"capacidade": 8.0,  "comp": 7.5,  "larg": 2.4, "alt": 2.8},
    "Truck":    {"capacidade": 14.0, "comp": 8.5,  "larg": 2.4, "alt": 2.8},
    "Carreta":  {"capacidade": 25.0, "comp": 15.0, "larg": 2.6, "alt": 2.8},
    "Bitrem":   {"capacidade": 37.0, "comp": 20.0, "larg": 2.6, "alt": 2.8},
    "Rodotrem": {"capacidade": 50.0, "comp": 25.0, "larg": 2.6, "alt": 2.8},
}

MARCAS_MODELOS = {
    "Volvo":         ["FH 540", "VM 270", "FMX 500"],
    "Scania":        ["R 450", "P 320", "S 500"],
    "Mercedes-Benz": ["Actros 2651", "Atego 2430", "Axor 2544"],
    "DAF":           ["XF 530", "CF 85", "LF 210"],
    "MAN":           ["TGX 29.480", "TGS 26.440", "TGL 12.190"],
    "Iveco":         ["S-Way 570", "Tector 240", "Hi-Way 600"],
}

LETRAS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
DIGITOS = "0123456789"

def gerar_placa():
    """Gera placa no formato ABC1D23"""
    return (
        random.choice(LETRAS) + random.choice(LETRAS) + random.choice(LETRAS) +
        random.choice(DIGITOS) + random.choice(LETRAS) +
        random.choice(DIGITOS) + random.choice(DIGITOS)
    )

caminhoes_data = []
placas_usadas = set()
tipos_list = list(TIPOS_CAMINHAO.keys())
marcas_list = list(MARCAS_MODELOS.keys())

for i in range(1, 1001):
    tipo = random.choice(tipos_list)
    specs = TIPOS_CAMINHAO[tipo]

    # Variação de ±10%
    var = lambda v: round(v * random.uniform(0.9, 1.1), 2)
    cap = var(specs["capacidade"])
    comp = var(specs["comp"])
    larg = var(specs["larg"])
    alt = var(specs["alt"])
    vol = round(comp * larg * alt, 2)

    marca = random.choice(marcas_list)
    modelo = random.choice(MARCAS_MODELOS[marca])
    ano = random.randint(2015, 2024)

    while True:
        placa = gerar_placa()
        if placa not in placas_usadas:
            placas_usadas.add(placa)
            break

    caminhoes_data.append((
        i, placa, tipo, cap, comp, larg, alt, vol, ano, marca, modelo
    ))

schema_caminhoes = StructType([
    StructField("id_caminhao", IntegerType(), False),
    StructField("placa", StringType(), False),
    StructField("tipo", StringType(), False),
    StructField("capacidade_toneladas", DoubleType(), False),
    StructField("comprimento_m", DoubleType(), False),
    StructField("largura_m", DoubleType(), False),
    StructField("altura_m", DoubleType(), False),
    StructField("volume_m3", DoubleType(), False),
    StructField("ano_fabricacao", IntegerType(), False),
    StructField("marca", StringType(), False),
    StructField("modelo", StringType(), False),
])

df_caminhoes = spark.createDataFrame(caminhoes_data, schema=schema_caminhoes)
df_caminhoes.write.mode("overwrite").saveAsTable(f"{catalog_name}.raw.caminhoes")
print(f"Tabela caminhoes criada com {df_caminhoes.count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tabela: motoristas (1.000 registros)

# COMMAND ----------

CNH_POR_TIPO = {
    "VUC": "C", "Toco": "C",
    "Truck": "D",
    "Carreta": "E", "Bitrem": "E", "Rodotrem": "E",
}

def gerar_cpf():
    """Gera CPF no formato XXX.XXX.XXX-XX"""
    p1 = random.randint(100, 999)
    p2 = random.randint(100, 999)
    p3 = random.randint(100, 999)
    p4 = random.randint(10, 99)
    return f"{p1}.{p2}.{p3}-{p4}"

def gerar_celular():
    """Gera celular no formato (XX)9XXXX-XXXX"""
    ddd = random.choice([11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 24, 27, 28, 31, 32, 33, 34, 35, 37, 38])
    p1 = random.randint(1000, 9999)
    p2 = random.randint(1000, 9999)
    return f"({ddd:02d})9{p1}-{p2}"

motoristas_data = []
cpfs_usados = set()

for i in range(1, 1001):
    cam = caminhoes_data[i - 1]  # 1:1 com caminhões
    tipo_caminhao = cam[2]
    cnh_cat = CNH_POR_TIPO[tipo_caminhao]

    while True:
        cpf = gerar_cpf()
        if cpf not in cpfs_usados:
            cpfs_usados.add(cpf)
            break

    nome = f"{random.choice(PRIMEIROS_NOMES)} {random.choice(SOBRENOMES)} {random.choice(SOBRENOMES)}"
    celular = gerar_celular()

    # CNH válida entre hoje e 5 anos no futuro, ou vencida até 6 meses atrás
    cnh_validade = DATA_REFERENCIA + timedelta(days=random.randint(-180, 1825))
    data_admissao = DATA_REFERENCIA - timedelta(days=random.randint(180, 3650))

    status = random.choices(["Ativo", "Férias", "Afastado"], weights=[85, 10, 5])[0]
    avaliacao = round(random.uniform(3.0, 5.0), 1)
    km_total = random.randint(10000, 800000)

    motoristas_data.append((
        i, cpf, nome, celular, cnh_cat,
        cnh_validade.strftime("%Y-%m-%d"),
        cam[0],  # id_caminhao
        data_admissao.strftime("%Y-%m-%d"),
        status, avaliacao, km_total
    ))

schema_motoristas = StructType([
    StructField("id_motorista", IntegerType(), False),
    StructField("cpf", StringType(), False),
    StructField("nome", StringType(), False),
    StructField("celular", StringType(), False),
    StructField("cnh_categoria", StringType(), False),
    StructField("cnh_validade", DateType(), False),
    StructField("id_caminhao", IntegerType(), False),
    StructField("data_admissao", DateType(), False),
    StructField("status", StringType(), False),
    StructField("avaliacao", DoubleType(), False),
    StructField("km_total_rodados", IntegerType(), False),
])

# Converter strings de data para date antes de criar DataFrame
from datetime import date as date_type

motoristas_data_typed = [
    (r[0], r[1], r[2], r[3], r[4],
     datetime.strptime(r[5], "%Y-%m-%d").date(),
     r[6],
     datetime.strptime(r[7], "%Y-%m-%d").date(),
     r[8], r[9], r[10])
    for r in motoristas_data
]

df_motoristas = spark.createDataFrame(motoristas_data_typed, schema=schema_motoristas)
df_motoristas.write.mode("overwrite").saveAsTable(f"{catalog_name}.raw.motoristas")
print(f"Tabela motoristas criada com {df_motoristas.count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tabela: status_transporte_ref (tabela de referência)

# COMMAND ----------

status_data = [
    (1,  "Pedido Recebido", 1),
    (2,  "Aguardando Coleta", 2),
    (3,  "Coleta Realizada", 3),
    (4,  "Em Trânsito para CD", 4),
    (5,  "Recebido no CD", 5),
    (6,  "Em Separação", 6),
    (7,  "Carga Consolidada", 7),
    (8,  "Saiu para Entrega", 8),
    (9,  "Em Trânsito", 9),
    (10, "Parada para Descanso", 10),
    (11, "Tentativa de Entrega", 11),
    (12, "Entrega Realizada", 12),
    (13, "Devolvido ao Remetente", 13),
    (14, "Extraviado", 14),
    (15, "Avaria Registrada", 15),
]

schema_status = StructType([
    StructField("id_status", IntegerType(), False),
    StructField("descricao", StringType(), False),
    StructField("ordem", IntegerType(), False),
])

df_status = spark.createDataFrame(status_data, schema=schema_status)
df_status.write.mode("overwrite").saveAsTable(f"{catalog_name}.raw.status_transporte_ref")
print(f"Tabela status_transporte_ref criada com {df_status.count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tabela: produtos_referencia (200 produtos)

# COMMAND ----------

PRODUTOS_POR_CATEGORIA = {
    "Alimentos e Bebidas": [
        ("Arroz Tipo 1 5kg", "1006.30.21", 5.0, 18.0),
        ("Feijão Carioca 1kg", "0713.33.19", 1.0, 8.0),
        ("Açúcar Cristal 5kg", "1701.99.00", 5.0, 15.0),
        ("Óleo de Soja 900ml", "1507.90.11", 0.9, 6.5),
        ("Café Torrado 500g", "0901.21.00", 0.5, 14.0),
        ("Leite UHT 1L", "0401.10.10", 1.05, 4.5),
        ("Biscoito Cream Cracker 400g", "1905.31.00", 0.4, 4.0),
        ("Macarrão Espaguete 500g", "1902.19.00", 0.5, 3.5),
        ("Farinha de Trigo 5kg", "1101.00.10", 5.0, 16.0),
        ("Cerveja Lata 350ml (cx 12)", "2203.00.00", 4.5, 28.0),
        ("Refrigerante 2L (cx 6)", "2202.10.00", 13.0, 24.0),
        ("Água Mineral 500ml (cx 24)", "2201.10.00", 12.5, 18.0),
        ("Suco Integral 1L", "2009.89.00", 1.1, 7.0),
        ("Molho de Tomate 340g", "2103.20.10", 0.4, 3.0),
        ("Azeite Extra Virgem 500ml", "1509.10.00", 0.5, 22.0),
        ("Sal Refinado 1kg", "2501.00.20", 1.0, 2.5),
        ("Vinagre 750ml", "2209.00.00", 0.8, 3.0),
        ("Leite Condensado 395g", "0402.99.00", 0.4, 5.5),
        ("Achocolatado em Pó 400g", "1806.90.00", 0.4, 6.0),
        ("Cereal Matinal 300g", "1904.10.00", 0.3, 8.0),
        ("Sardinha em Lata 125g (cx 24)", "1604.13.10", 3.5, 42.0),
        ("Margarina 500g", "1517.10.00", 0.5, 4.5),
    ],
    "Materiais de Construção": [
        ("Cimento CP-II 50kg", "2523.29.10", 50.0, 32.0),
        ("Areia Fina (saco 20kg)", "2505.10.00", 20.0, 12.0),
        ("Brita nº 1 (saco 20kg)", "2517.10.00", 20.0, 14.0),
        ("Tijolo Cerâmico (milheiro)", "6904.10.00", 2200.0, 850.0),
        ("Telha Cerâmica (cx 20)", "6905.10.00", 40.0, 120.0),
        ("Vergalhão CA-50 10mm (barra 12m)", "7214.20.00", 7.4, 35.0),
        ("Piso Cerâmico 60x60 (cx 2.5m²)", "6908.90.00", 25.0, 45.0),
        ("Argamassa Colante 20kg", "3214.90.00", 20.0, 18.0),
        ("Tinta Acrílica 18L", "3209.10.00", 25.0, 280.0),
        ("Tubo PVC 100mm 6m", "3917.23.00", 3.5, 28.0),
        ("Cal Hidratada 20kg", "2522.20.00", 20.0, 14.0),
        ("Fio Elétrico 2.5mm 100m", "8544.49.00", 2.5, 65.0),
        ("Chapa de Gesso 1.20x1.80m", "6809.11.00", 18.0, 32.0),
        ("Porta de Madeira 2.10x0.80m", "4418.20.00", 22.0, 180.0),
        ("Impermeabilizante 18L", "3214.10.00", 20.0, 195.0),
    ],
    "Eletroeletrônicos": [
        ("TV LED 55 polegadas", "8528.72.00", 15.0, 2800.0),
        ("Geladeira Frost Free 375L", "8418.21.00", 65.0, 2500.0),
        ("Máquina de Lavar 11kg", "8450.20.90", 55.0, 1800.0),
        ("Micro-ondas 30L", "8516.50.00", 14.0, 650.0),
        ("Notebook 15.6 polegadas", "8471.30.19", 2.5, 3200.0),
        ("Smartphone 128GB", "8517.13.00", 0.2, 1500.0),
        ("Ar-Condicionado Split 12000 BTUs", "8415.10.11", 35.0, 1900.0),
        ("Impressora Multifuncional", "8443.32.99", 8.0, 450.0),
        ("Monitor LED 24 polegadas", "8528.52.00", 4.5, 850.0),
        ("Ventilador de Coluna", "8414.51.10", 5.0, 180.0),
        ("Aspirador de Pó", "8508.11.00", 6.0, 350.0),
        ("Forno Elétrico 44L", "8516.60.00", 12.0, 420.0),
        ("Purificador de Água", "8421.21.00", 4.0, 380.0),
    ],
    "Automotivo": [
        ("Pneu 175/65 R14", "4011.10.00", 8.5, 280.0),
        ("Óleo Motor 5W30 (cx 24x1L)", "2710.19.91", 22.0, 360.0),
        ("Bateria Automotiva 60Ah", "8507.10.00", 15.0, 420.0),
        ("Pastilha de Freio Dianteira (cx 50)", "6813.81.00", 12.0, 750.0),
        ("Filtro de Ar (cx 20)", "8421.31.00", 4.0, 240.0),
        ("Lâmpada Automotiva H4 (cx 100)", "8539.21.00", 3.0, 350.0),
        ("Amortecedor Dianteiro (par)", "8708.80.00", 6.0, 320.0),
        ("Correia Dentada (cx 20)", "4010.31.00", 2.0, 280.0),
        ("Fluido de Freio DOT 4 500ml (cx 24)", "3819.00.00", 13.0, 192.0),
        ("Vela de Ignição (cx 100)", "8511.10.00", 3.0, 450.0),
    ],
    "Têxtil e Vestuário": [
        ("Caixa de Camisetas (50 un)", "6109.10.00", 8.0, 450.0),
        ("Fardo de Tecido Algodão (50m)", "5208.11.00", 15.0, 380.0),
        ("Caixa de Calçados (20 pares)", "6403.99.00", 12.0, 600.0),
        ("Fardo de Toalhas (100 un)", "6302.60.00", 25.0, 520.0),
        ("Caixa de Meias (200 pares)", "6115.95.00", 6.0, 280.0),
        ("Fardo de Jeans (30 un)", "6204.62.00", 18.0, 850.0),
        ("Caixa de Roupas Íntimas (100 un)", "6108.21.00", 4.0, 320.0),
        ("Fardo de Lençóis (50 un)", "6302.21.00", 20.0, 750.0),
        ("Caixa de Bonés (100 un)", "6505.00.22", 5.0, 250.0),
    ],
    "Químicos e Limpeza": [
        ("Detergente 500ml (cx 24)", "3402.20.00", 13.0, 48.0),
        ("Sabão em Pó 1kg (cx 20)", "3401.20.90", 21.0, 140.0),
        ("Desinfetante 2L (cx 6)", "3808.94.29", 13.0, 36.0),
        ("Água Sanitária 5L (cx 4)", "2828.90.11", 21.0, 28.0),
        ("Amaciante 2L (cx 6)", "3809.91.90", 13.0, 54.0),
        ("Limpador Multiuso 500ml (cx 24)", "3402.90.39", 13.0, 72.0),
        ("Esponja de Limpeza (cx 120)", "6805.30.00", 3.0, 84.0),
        ("Sabonete 90g (cx 72)", "3401.11.00", 7.0, 108.0),
        ("Papel Higiênico (fardo 64 rolos)", "4818.10.00", 8.0, 55.0),
        ("Papel Toalha (fardo 12 rolos)", "4818.20.00", 6.0, 42.0),
    ],
    "Farmacêutico": [
        ("Caixa de Medicamentos Diversos", "3004.90.99", 5.0, 450.0),
        ("Insumos Farmacêuticos (kit)", "2933.59.99", 8.0, 1200.0),
        ("Produtos de Higiene Pessoal (cx)", "3307.90.00", 6.0, 180.0),
        ("Vitaminas e Suplementos (cx 100)", "2106.90.30", 4.0, 320.0),
        ("Fraldas Descartáveis (fardo)", "9619.00.00", 10.0, 85.0),
        ("Álcool em Gel 500ml (cx 12)", "2207.20.19", 7.0, 72.0),
        ("Luvas Descartáveis (cx 2000)", "4015.19.00", 8.0, 120.0),
        ("Máscaras Descartáveis (cx 2000)", "6307.90.10", 4.0, 95.0),
        ("Termômetro Digital (cx 50)", "9025.19.90", 2.0, 350.0),
        ("Curativo Adesivo (cx 500)", "3005.10.00", 1.5, 85.0),
    ],
    "Agropecuário": [
        ("Ração Animal 25kg", "2309.90.90", 25.0, 55.0),
        ("Fertilizante NPK 50kg", "3105.20.00", 50.0, 120.0),
        ("Sementes de Soja (saco 40kg)", "1201.90.00", 40.0, 180.0),
        ("Defensivos Agrícolas (20L)", "3808.91.99", 22.0, 350.0),
        ("Ração para Aves 25kg", "2309.90.40", 25.0, 48.0),
        ("Sementes de Milho (saco 20kg)", "1005.90.10", 20.0, 280.0),
        ("Adubo Orgânico 25kg", "3101.00.00", 25.0, 35.0),
        ("Sal Mineral para Gado 25kg", "2501.00.90", 25.0, 42.0),
        ("Vermífugo Animal (cx 50)", "3004.90.29", 3.0, 280.0),
        ("Suplemento Vitamínico Animal 5kg", "2309.90.50", 5.0, 95.0),
    ],
    "Móveis": [
        ("Sofá 3 Lugares", "9401.61.00", 45.0, 1200.0),
        ("Mesa de Jantar 6 Lugares", "9403.60.00", 35.0, 950.0),
        ("Cadeira de Escritório", "9401.30.90", 12.0, 380.0),
        ("Estante 5 Prateleiras", "9403.60.00", 28.0, 420.0),
        ("Colchão Casal D33", "9404.21.00", 18.0, 850.0),
        ("Guarda-Roupa 6 Portas", "9403.50.00", 75.0, 1600.0),
        ("Cômoda 5 Gavetas", "9403.50.00", 30.0, 550.0),
        ("Rack para TV", "9403.60.00", 20.0, 320.0),
        ("Beliche", "9403.50.00", 40.0, 780.0),
        ("Mesa de Centro", "9403.60.00", 15.0, 280.0),
        ("Cama Box Solteiro", "9404.10.00", 22.0, 620.0),
        ("Armário de Cozinha", "9403.40.00", 25.0, 480.0),
    ],
}

# Gerar 200 produtos a partir do catálogo
produtos_data = []
id_prod = 1

# Primeiro, adicionar todos os produtos definidos
todos_produtos = []
for categoria, prods in PRODUTOS_POR_CATEGORIA.items():
    for desc, ncm, peso, valor in prods:
        todos_produtos.append((categoria, desc, ncm, peso, valor))

# Se temos menos de 200, vamos gerar variações
for cat, desc, ncm, peso, valor in todos_produtos:
    if id_prod > 200:
        break
    produtos_data.append((id_prod, desc, ncm, cat, round(peso * random.uniform(0.9, 1.1), 2), round(valor * random.uniform(0.85, 1.15), 2)))
    id_prod += 1

# Preencher até 200 com variações
while id_prod <= 200:
    cat, desc, ncm, peso, valor = random.choice(todos_produtos)
    variacao = random.choice(["Premium", "Econômico", "Industrial", "Especial", "Extra", "Super", "Mega", "Plus"])
    desc_var = f"{desc} - {variacao}"
    produtos_data.append((id_prod, desc_var, ncm, cat, round(peso * random.uniform(0.8, 1.2), 2), round(valor * random.uniform(0.7, 1.3), 2)))
    id_prod += 1

schema_produtos = StructType([
    StructField("id_produto", IntegerType(), False),
    StructField("descricao", StringType(), False),
    StructField("ncm", StringType(), False),
    StructField("categoria", StringType(), False),
    StructField("peso_medio_kg", DoubleType(), False),
    StructField("valor_medio", DoubleType(), False),
])

df_produtos = spark.createDataFrame(produtos_data, schema=schema_produtos)
df_produtos.write.mode("overwrite").saveAsTable(f"{catalog_name}.raw.produtos_referencia")
print(f"Tabela produtos_referencia criada com {df_produtos.count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tabela: pedidos (10.000 registros)

# COMMAND ----------

PRIORIDADES = ["Normal", "Expressa", "Urgente"]
PESOS_PRIORIDADE = [70, 20, 10]
TIPOS_FRETE = ["CIF", "FOB"]
BASE_RATES = {"Normal": 1.0, "Expressa": 1.5, "Urgente": 2.5}

pedidos_data = []

for i in range(1, 10001):
    id_cliente = random.randint(1, 1000)

    # Data do pedido nos últimos 90 dias
    dias_atras = random.randint(0, 90)
    data_pedido = DATA_REFERENCIA - timedelta(days=dias_atras, hours=random.randint(0, 23), minutes=random.randint(0, 59))

    peso_total = round(random.uniform(50, 5000), 2)
    volume_total = round(random.uniform(0.5, 50), 2)
    valor_mercadoria = round(random.uniform(500, 150000), 2)

    tipo_frete = random.choice(TIPOS_FRETE)
    prioridade = random.choices(PRIORIDADES, weights=PESOS_PRIORIDADE)[0]

    base_rate = BASE_RATES[prioridade]
    valor_frete = round(base_rate * (peso_total * 0.15 + volume_total * 50 + valor_mercadoria * 0.02), 2)

    cidade_origem_idx = random.randint(0, len(CIDADES) - 1)
    cidade_destino_idx = random.randint(0, len(CIDADES) - 1)
    while cidade_destino_idx == cidade_origem_idx:
        cidade_destino_idx = random.randint(0, len(CIDADES) - 1)

    c_orig = CIDADES[cidade_origem_idx]
    c_dest = CIDADES[cidade_destino_idx]

    pedidos_data.append((
        i, id_cliente, data_pedido,
        peso_total, volume_total, valor_mercadoria, valor_frete,
        tipo_frete, prioridade,
        c_orig[0], c_orig[1],  # cidade_origem, uf_origem
        c_dest[0], c_dest[1],  # cidade_destino, uf_destino
    ))

schema_pedidos = StructType([
    StructField("id_pedido", LongType(), False),
    StructField("id_cliente", IntegerType(), False),
    StructField("data_pedido", TimestampType(), False),
    StructField("peso_total_kg", DoubleType(), False),
    StructField("volume_total_m3", DoubleType(), False),
    StructField("valor_mercadoria", DoubleType(), False),
    StructField("valor_frete", DoubleType(), False),
    StructField("tipo_frete", StringType(), False),
    StructField("prioridade", StringType(), False),
    StructField("cidade_origem", StringType(), False),
    StructField("uf_origem", StringType(), False),
    StructField("cidade_destino", StringType(), False),
    StructField("uf_destino", StringType(), False),
])

df_pedidos = spark.createDataFrame(pedidos_data, schema=schema_pedidos)
df_pedidos.write.mode("overwrite").saveAsTable(f"{catalog_name}.raw.pedidos")
print(f"Tabela pedidos criada com {df_pedidos.count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tabelas: notas_fiscais e itens_nf

# COMMAND ----------

import json

notas_data = []
itens_data = []
id_nf_counter = 1
id_item_counter = 1

for pedido in pedidos_data:
    id_pedido = pedido[0]
    data_pedido = pedido[2]
    num_nfs = random.randint(1, 6)

    for _ in range(num_nfs):
        # Chave de acesso: 44 dígitos
        chave_acesso = ''.join([str(random.randint(0, 9)) for _ in range(44)])
        numero_nf = f"{random.randint(1, 999999):06d}"
        data_emissao = data_pedido + timedelta(hours=random.randint(1, 48))

        nf_valor_total = 0.0
        num_itens = random.randint(2, 8)

        itens_desta_nf = []

        for _ in range(num_itens):
            prod = random.choice(produtos_data)
            # prod: (id_produto, descricao, ncm, categoria, peso_medio_kg, valor_medio)
            quantidade = random.randint(1, 50)
            valor_unitario = round(prod[5] * random.uniform(0.8, 1.2), 2)
            valor_total_item = round(quantidade * valor_unitario, 2)
            peso_item = round(prod[4] * quantidade, 2)

            comprimento_cm = round(random.uniform(20, 120), 1)
            largura_cm = round(random.uniform(15, 80), 1)
            altura_cm = round(random.uniform(10, 60), 1)

            itens_desta_nf.append((
                id_item_counter, id_nf_counter, prod[0], prod[1], prod[2],
                quantidade, "UN", valor_unitario, valor_total_item,
                peso_item, comprimento_cm, largura_cm, altura_cm
            ))
            nf_valor_total += valor_total_item
            id_item_counter += 1

        notas_data.append((
            id_nf_counter, numero_nf, id_pedido, data_emissao,
            round(nf_valor_total, 2), chave_acesso
        ))
        itens_data.extend(itens_desta_nf)
        id_nf_counter += 1

print(f"Gerados {len(notas_data)} notas fiscais e {len(itens_data)} itens.")

# COMMAND ----------

schema_nf = StructType([
    StructField("id_nf", LongType(), False),
    StructField("numero_nf", StringType(), False),
    StructField("id_pedido", LongType(), False),
    StructField("data_emissao", TimestampType(), False),
    StructField("valor_total", DoubleType(), False),
    StructField("chave_acesso", StringType(), False),
])

# Criar em batches para eficiência
BATCH_SIZE = 20000

# Notas fiscais
for batch_start in range(0, len(notas_data), BATCH_SIZE):
    batch = notas_data[batch_start:batch_start + BATCH_SIZE]
    df_batch = spark.createDataFrame(batch, schema=schema_nf)
    mode = "overwrite" if batch_start == 0 else "append"
    df_batch.write.mode(mode).saveAsTable(f"{catalog_name}.raw.notas_fiscais")

print(f"Tabela notas_fiscais criada com {len(notas_data)} registros.")

# COMMAND ----------

schema_itens = StructType([
    StructField("id_item", LongType(), False),
    StructField("id_nf", LongType(), False),
    StructField("id_produto", IntegerType(), False),
    StructField("descricao", StringType(), False),
    StructField("ncm", StringType(), False),
    StructField("quantidade", IntegerType(), False),
    StructField("unidade", StringType(), False),
    StructField("valor_unitario", DoubleType(), False),
    StructField("valor_total", DoubleType(), False),
    StructField("peso_kg", DoubleType(), False),
    StructField("comprimento_cm", DoubleType(), False),
    StructField("largura_cm", DoubleType(), False),
    StructField("altura_cm", DoubleType(), False),
])

# Itens NF em batches
for batch_start in range(0, len(itens_data), BATCH_SIZE):
    batch = itens_data[batch_start:batch_start + BATCH_SIZE]
    df_batch = spark.createDataFrame(batch, schema=schema_itens)
    mode = "overwrite" if batch_start == 0 else "append"
    df_batch.write.mode(mode).saveAsTable(f"{catalog_name}.raw.itens_nf")

print(f"Tabela itens_nf criada com {len(itens_data)} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Tabela: movimento_cargas (5.000 registros)

# COMMAND ----------

STATUS_POSSIVEIS = [s[1] for s in status_data]

cargas_data = []
# Manter referência de pedidos disponíveis
pedidos_ids = list(range(1, 10001))
random.shuffle(pedidos_ids)
pedido_idx = 0

for i in range(1, 5001):
    cam = caminhoes_data[random.randint(0, 999)]
    id_caminhao = cam[0]
    placa = cam[1]
    tipo_frete = random.choice(TIPOS_FRETE)

    cidade_origem_idx = random.randint(0, len(CIDADES) - 1)
    cidade_destino_idx = random.randint(0, len(CIDADES) - 1)
    while cidade_destino_idx == cidade_origem_idx:
        cidade_destino_idx = random.randint(0, len(CIDADES) - 1)

    c_orig = CIDADES[cidade_origem_idx]
    c_dest = CIDADES[cidade_destino_idx]

    # Data de saída nos últimos 90 dias
    dias_atras = random.randint(0, 90)
    data_saida = DATA_REFERENCIA - timedelta(days=dias_atras, hours=random.randint(0, 23))
    data_prevista = data_saida + timedelta(hours=random.randint(6, 120))

    km_total = random.randint(50, 2500)
    peso_total = round(random.uniform(500, 40000), 2)
    volume_total = round(random.uniform(5, 80), 2)

    # Selecionar status atual
    status_atual = random.choices(
        STATUS_POSSIVEIS,
        weights=[5, 5, 5, 8, 5, 5, 8, 10, 15, 5, 5, 18, 2, 2, 2],
    )[0]

    # Pedidos associados (3-15 por carga)
    num_pedidos_carga = random.randint(3, 15)
    if pedido_idx + num_pedidos_carga > len(pedidos_ids):
        # Reciclar IDs se necessário
        pedido_idx = 0
        random.shuffle(pedidos_ids)
    pedidos_carga = pedidos_ids[pedido_idx:pedido_idx + num_pedidos_carga]
    pedido_idx += num_pedidos_carga
    pedidos_json = json.dumps(pedidos_carga)

    cargas_data.append((
        i, id_caminhao, placa, tipo_frete,
        c_orig[0], c_orig[1],
        c_dest[0], c_dest[1],
        data_saida, data_prevista,
        km_total, peso_total, volume_total,
        status_atual, pedidos_json
    ))

schema_cargas = StructType([
    StructField("id_carga", LongType(), False),
    StructField("id_caminhao", IntegerType(), False),
    StructField("placa", StringType(), False),
    StructField("tipo_frete", StringType(), False),
    StructField("cidade_origem", StringType(), False),
    StructField("uf_origem", StringType(), False),
    StructField("cidade_destino", StringType(), False),
    StructField("uf_destino", StringType(), False),
    StructField("data_saida", TimestampType(), False),
    StructField("data_prevista_chegada", TimestampType(), False),
    StructField("km_total", IntegerType(), False),
    StructField("peso_total_kg", DoubleType(), False),
    StructField("volume_total_m3", DoubleType(), False),
    StructField("status_atual", StringType(), False),
    StructField("pedidos_json", StringType(), False),
])

df_cargas = spark.createDataFrame(cargas_data, schema=schema_cargas)
df_cargas.write.mode("overwrite").saveAsTable(f"{catalog_name}.raw.movimento_cargas")
print(f"Tabela movimento_cargas criada com {df_cargas.count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Tabela: historico_status (10.000 registros)

# COMMAND ----------

OBSERVACOES = [
    "Operação normal",
    "Aguardando liberação",
    "Trânsito fluindo normalmente",
    "Parada programada para descanso",
    "Carga conferida e liberada",
    "Documentação verificada",
    "Veículo abastecido",
    "Troca de motorista realizada",
    "Destinatário ausente",
    "Endereço não localizado",
    "Entrega realizada com sucesso",
    "Carga avariada parcialmente",
    "Atraso devido a condições climáticas",
    "Fiscalização na rodovia",
    "Balança rodoviária - peso conferido",
    "Pedágio registrado",
    "Manutenção preventiva realizada",
    "Pneu trocado na estrada",
    "Carga redespachiada",
    "Coleta realizada no cliente",
]

historico_data = []
id_mov = 1
# Status IDs de 1 a 15
status_ids = list(range(1, 16))

# Distribuir históricos pelas cargas
cargas_para_historico = list(range(1, 5001))
random.shuffle(cargas_para_historico)

registros_restantes = 10000
carga_idx = 0

while registros_restantes > 0 and carga_idx < len(cargas_para_historico):
    id_carga = cargas_para_historico[carga_idx]
    carga = cargas_data[id_carga - 1]
    data_saida = carga[8]

    # Cada carga tem 1-5 registros de status
    num_status = min(random.randint(1, 5), registros_restantes)

    # Selecionar status em ordem lógica crescente
    max_status = random.randint(1, 12)  # até onde a carga progrediu
    status_selecionados = sorted(random.sample(range(1, max_status + 1), min(num_status, max_status)))

    for j, id_status in enumerate(status_selecionados):
        # Timestamp progressivo
        horas_offset = j * random.randint(2, 12)
        ts = data_saida + timedelta(hours=horas_offset, minutes=random.randint(0, 59))

        # Coordenadas interpoladas entre origem e destino
        c_orig = next((c for c in CIDADES if c[0] == carga[4]), CIDADES[0])
        c_dest = next((c for c in CIDADES if c[0] == carga[6]), CIDADES[1])
        frac = j / max(len(status_selecionados) - 1, 1)
        lat = round(c_orig[2] + frac * (c_dest[2] - c_orig[2]) + random.uniform(-0.1, 0.1), 6)
        lon = round(c_orig[3] + frac * (c_dest[3] - c_orig[3]) + random.uniform(-0.1, 0.1), 6)

        observacao = random.choice(OBSERVACOES)

        historico_data.append((
            id_mov, id_carga, id_status, ts, observacao, lat, lon
        ))
        id_mov += 1
        registros_restantes -= 1

        if registros_restantes <= 0:
            break

    carga_idx += 1

schema_historico = StructType([
    StructField("id_movimento", LongType(), False),
    StructField("id_carga", LongType(), False),
    StructField("id_status", IntegerType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("observacao", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
])

df_historico = spark.createDataFrame(historico_data, schema=schema_historico)
df_historico.write.mode("overwrite").saveAsTable(f"{catalog_name}.raw.historico_status")
print(f"Tabela historico_status criada com {df_historico.count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo Final

# COMMAND ----------

print("=" * 70)
print(f"  RESUMO - Dados gerados em: {catalog_name}.{schema_name}")
print("=" * 70)

tabelas = [
    "clientes",
    "caminhoes",
    "motoristas",
    "status_transporte_ref",
    "produtos_referencia",
    "pedidos",
    "notas_fiscais",
    "itens_nf",
    "movimento_cargas",
    "historico_status",
]

total_geral = 0
for tabela in tabelas:
    count = spark.table(f"{catalog_name}.raw.{tabela}").count()
    total_geral += count
    print(f"  {tabela:<30s} {count:>10,} registros")

print("-" * 70)
print(f"  {'TOTAL':<30s} {total_geral:>10,} registros")
print("=" * 70)
print("\nTodos os dados foram gerados com sucesso!")
