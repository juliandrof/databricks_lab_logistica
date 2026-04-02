#!/usr/bin/env python3
"""Generate ER diagram for Workshop Logística Databricks data model."""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

# Compact figure - let bbox_inches="tight" crop precisely
FIG_W, FIG_H = 48, 36
DPI = 200
TITLE = "Modelo de Dados — Workshop Logística"

FONT_LAYER = 36
FONT_TABLE = 26
FONT_COL = 20
FONT_TAG = 17

# Layer definitions
LAYERS = [
    ("RAW", "#EAF2FB", "#2979FF", [
        ("clientes", [
            ("id_cliente", "PK"), ("cnpj", ""), ("razao_social", ""),
            ("uf", ""), ("cidade", ""), ("cep", ""),
            ("latitude", ""), ("longitude", ""),
        ]),
        ("caminhoes", [
            ("id_caminhao", "PK"), ("placa", ""), ("tipo", ""),
            ("capacidade_ton", ""), ("volume_m3", ""),
            ("marca", ""), ("modelo", ""),
        ]),
        ("motoristas", [
            ("id_motorista", "PK"), ("cpf", ""), ("nome", ""),
            ("cnh_categoria", ""), ("id_caminhao", "FK"),
            ("status", ""), ("avaliacao", ""),
        ]),
        ("movimento_cargas", [
            ("id_carga", "PK"), ("id_caminhao", "FK"), ("placa", ""),
            ("tipo_frete", ""), ("cidade_origem", ""),
            ("cidade_destino", ""), ("pedidos_json", ""),
        ]),
        ("status_transporte_ref", [
            ("id_status", "PK"), ("descricao", ""), ("ordem", ""),
        ]),
    ]),
    ("BRONZE", "#FFF0E0", "#EF6C00", [
        ("bronze_pedidos", [
            ("id_pedido", "PK"), ("id_cliente", "FK"),
            ("data_pedido", ""), ("peso_total_kg", ""),
            ("valor_frete", ""), ("notas_fiscais[]", ""),
            ("arquivo_origem", ""), ("data_ingestao", ""),
        ]),
        ("bronze_status", [
            ("id_carga", "FK"), ("id_status", "FK"),
            ("timestamp", ""), ("observacao", ""),
            ("latitude", ""), ("longitude", ""),
            ("arquivo_origem", ""), ("data_ingestao", ""),
        ]),
        ("bronze_clientes", [
            ("id_cliente", "PK"), ("cnpj", ""), ("razao_social", ""),
            ("uf", ""), ("cidade", ""),
        ]),
        ("bronze_caminhoes", [
            ("id_caminhao", "PK"), ("placa", ""), ("tipo", ""),
            ("capacidade_ton", ""),
        ]),
        ("bronze_movimento_cargas", [
            ("id_carga", "PK"), ("id_caminhao", "FK"),
            ("cidade_origem", ""), ("cidade_destino", ""),
        ]),
    ]),
    ("SILVER", "#E8F5E9", "#388E3C", [
        ("silver_pedidos", [
            ("id_pedido", "PK"), ("id_cliente", "FK"),
            ("razao_social", ""), ("cnpj", ""),
            ("data_pedido", ""), ("ano", ""), ("mes", ""), ("dia", ""),
            ("peso_total_kg", ""), ("valor_frete", ""),
            ("cidade_origem", ""), ("cidade_destino", ""),
            ("prioridade", ""),
        ]),
        ("silver_itens_nf", [
            ("id_item", "PK"), ("id_pedido", "FK"), ("id_nf", "FK"),
            ("descricao", ""), ("ncm", ""),
            ("quantidade", ""), ("valor_unitario", ""),
            ("valor_total_item", ""), ("peso_kg", ""),
        ]),
        ("silver_status_transporte", [
            ("id_carga", "FK"), ("id_status", "FK"),
            ("descricao_status", ""), ("ordem", ""),
            ("timestamp", ""), ("observacao", ""),
            ("latitude", ""), ("longitude", ""),
        ]),
    ]),
    ("GOLD", "#FFFDE7", "#F9A825", [
        ("gold_volume_por_rota", [
            ("cidade_origem", ""), ("uf_origem", ""),
            ("cidade_destino", ""), ("uf_destino", ""),
            ("total_pedidos", ""), ("peso_total", ""),
            ("valor_frete_total", ""), ("frete_medio", ""),
        ]),
        ("gold_performance_frota", [
            ("tipo", ""), ("total_cargas", ""),
            ("capacidade_media", ""), ("km_total", ""),
            ("ocupacao_media_pct", ""),
        ]),
        ("gold_status_entregas", [
            ("descricao_status", ""), ("id_status", ""),
            ("ordem", ""), ("total_cargas", ""),
            ("primeira_atualizacao", ""), ("ultima_atualizacao", ""),
        ]),
    ]),
]

STACKED_DEFS = [
    ("AI/BI", "#E0F7FA", "#00838F", [
        ("Genie", [
            ("Linguagem natural", ""), ("Tabelas Gold", ""),
            ("Instruções customizadas", ""),
        ]),
        ("Dashboard", [
            ("Mapa de entregas", ""), ("Volume por rota", ""),
            ("KPIs operacionais", ""), ("Status entregas", ""),
        ]),
    ]),
    ("ML", "#F3E5F5", "#8E24AA", [
        ("ml_previsao_demanda", [
            ("rota", ""), ("semana", ""), ("total_pedidos", ""),
            ("peso_medio", ""), ("previsao", ""),
        ]),
        ("ml_otimizacao_frota", [
            ("id_caminhao", ""), ("latitude", ""), ("longitude", ""),
            ("distancia_km", ""), ("capacidade_livre", ""),
        ]),
    ]),
]

# Layout
TW = 6.0       # table width
RH = 0.65      # row height
HH = 0.9       # header height
TG = 0.5       # table gap
LPX = 0.7      # layer pad x
LPY = 0.8      # layer pad y
LHH = 1.3      # layer header height
LG = 1.2       # layer gap


def th(cols):
    return HH + len(cols) * RH + 0.3


def layout():
    result = {}
    x = 1.5

    # Main layers
    for lname, bg, border, tables in LAYERS:
        heights = [th(c) for _, c in tables]
        total = sum(heights) + TG * (len(tables) - 1)
        lw = TW + 2 * LPX
        lh = total + 2 * LPY + LHH
        ly = (FIG_H - lh) / 2 - 0.5

        layer = {"x": x, "y": ly, "w": lw, "h": lh,
                 "bg": bg, "border": border, "name": lname, "tables": []}

        ty = ly + lh - LHH - LPY
        tx = x + LPX
        for (tn, cols), h in zip(tables, heights):
            ty_top = ty - h
            layer["tables"].append({"name": tn, "cols": cols,
                                     "x": tx, "y": ty_top, "w": TW, "h": h})
            ty = ty_top - TG

        result[lname] = layer
        x += lw + LG

    # Stacked layers (AI/BI + ML)
    sx = x
    lw = TW + 2 * LPX
    infos = []
    for lname, bg, border, tables in STACKED_DEFS:
        heights = [th(c) for _, c in tables]
        total = sum(heights) + TG * (len(tables) - 1)
        lh = total + 2 * LPY + LHH
        infos.append((lname, bg, border, tables, heights, lh))

    total_sh = sum(i[5] for i in infos) + LG
    cy = (FIG_H - total_sh) / 2 - 0.5 + total_sh

    for lname, bg, border, tables, heights, lh in infos:
        ly = cy - lh
        layer = {"x": sx, "y": ly, "w": lw, "h": lh,
                 "bg": bg, "border": border, "name": lname, "tables": []}
        ty = ly + lh - LHH - LPY
        tx = sx + LPX
        for (tn, cols), h in zip(tables, heights):
            ty_top = ty - h
            layer["tables"].append({"name": tn, "cols": cols,
                                     "x": tx, "y": ty_top, "w": TW, "h": h})
            ty = ty_top - TG
        result[lname] = layer
        cy = ly - LG

    return result


def draw_table(ax, t, bc):
    x, y, w, h = t["x"], t["y"], t["w"], t["h"]
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.12",
                                 facecolor="white", edgecolor=bc, linewidth=3, zorder=3))
    ax.add_patch(FancyBboxPatch((x+0.06, y+h-HH-0.05), w-0.12, HH,
                                 boxstyle="round,pad=0.06", facecolor=bc,
                                 edgecolor="none", alpha=0.10, zorder=4))
    ax.text(x+w/2, y+h-HH/2-0.03, t["name"], ha="center", va="center",
            fontsize=FONT_TABLE, fontweight="bold", color="#000",
            fontfamily="monospace", zorder=5)
    sy = y + h - HH - 0.08
    ax.plot([x+0.2, x+w-0.2], [sy, sy], color="#BBB", lw=1.5, zorder=4)
    cy = sy - RH * 0.65
    for cn, tag in t["cols"]:
        if tag == "PK":
            ax.text(x+0.3, cy, "●", fontsize=FONT_TAG, color="#E65100",
                    fontweight="bold", ha="left", va="center", zorder=5)
            ax.text(x+0.8, cy, cn, fontsize=FONT_COL, color="#000",
                    fontweight="bold", fontfamily="monospace",
                    ha="left", va="center", zorder=5)
            ax.text(x+w-0.3, cy, "PK", fontsize=FONT_TAG, color="#E65100",
                    fontweight="bold", ha="right", va="center", zorder=5)
        elif tag == "FK":
            ax.text(x+0.8, cy, cn, fontsize=FONT_COL, color="#000",
                    fontfamily="monospace", fontstyle="italic",
                    ha="left", va="center", zorder=5)
            ax.text(x+w-0.3, cy, "FK", fontsize=FONT_TAG, color="#757575",
                    ha="right", va="center", zorder=5)
        else:
            ax.text(x+0.8, cy, cn, fontsize=FONT_COL, color="#000",
                    fontfamily="monospace", ha="left", va="center", zorder=5)
        cy -= RH


def draw_layer(ax, L):
    x, y, w, h = L["x"], L["y"], L["w"], L["h"]
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.2",
                                 facecolor=L["bg"], edgecolor=L["border"],
                                 linewidth=3.5, linestyle="--", alpha=0.7, zorder=1))
    ax.text(x+w/2, y+h-LHH/2+0.12, L["name"], ha="center", va="center",
            fontsize=FONT_LAYER, fontweight="bold", color=L["border"], zorder=5,
            bbox=dict(boxstyle="round,pad=0.4", facecolor="white",
                      edgecolor=L["border"], linewidth=3, alpha=0.95))


def main():
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H), dpi=DPI)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    ax.set_xlim(0, FIG_W)
    ax.set_ylim(0, FIG_H)
    ax.set_aspect("equal")
    ax.axis("off")

    ax.text(FIG_W/2, FIG_H - 1.5, TITLE, ha="center", va="center",
            fontsize=48, fontweight="bold", color="#000", zorder=10)

    L = layout()

    all_names = [n for n, *_ in LAYERS] + [n for n, *_ in STACKED_DEFS]
    for n in all_names:
        li = L[n]
        draw_layer(ax, li)
        for t in li["tables"]:
            draw_table(ax, t, li["border"])

    # Straight arrows between layers
    order = [n for n, *_ in LAYERS]
    for i in range(len(order)-1):
        s, d = L[order[i]], L[order[i+1]]
        ax.annotate("", xy=(d["x"]-0.08, d["y"]+d["h"]/2),
                    xytext=(s["x"]+s["w"]+0.08, s["y"]+s["h"]/2),
                    arrowprops=dict(arrowstyle="-|>", color="#333", lw=4,
                                    mutation_scale=40), zorder=2)

    # Gold → AI/BI and Gold → ML
    g = L["GOLD"]
    gx, gy = g["x"]+g["w"], g["y"]+g["h"]/2
    for dn in ["AI/BI", "ML"]:
        d = L[dn]
        ax.annotate("", xy=(d["x"]-0.08, d["y"]+d["h"]/2),
                    xytext=(gx+0.08, gy),
                    arrowprops=dict(arrowstyle="-|>", color="#333", lw=4,
                                    mutation_scale=40), zorder=2)

    import os
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "modelo_er.png")
    fig.savefig(out, dpi=DPI, bbox_inches="tight", facecolor="white", pad_inches=0.2)
    plt.close(fig)
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
