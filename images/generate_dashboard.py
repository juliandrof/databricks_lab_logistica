#!/usr/bin/env python3
"""Generate dashboard mockup for Workshop Logística."""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np
import os

FIG_W, FIG_H = 20, 14
DPI = 200

# Colors
BG = "#F5F6F8"
CARD_BG = "#FFFFFF"
CARD_BORDER = "#E0E0E0"
ACCENT = "#FF6F00"
BLUE = "#1565C0"
GREEN = "#388E3C"
PURPLE = "#7B1FA2"
RED = "#E53935"
GRAY = "#757575"
LIGHT_GRAY = "#BDBDBD"


def draw_card(ax, x, y, w, h, bg=CARD_BG, border=CARD_BORDER, radius=0.15):
    p = FancyBboxPatch((x, y), w, h, boxstyle=f"round,pad={radius}",
                        facecolor=bg, edgecolor=border, linewidth=1.5, zorder=2)
    ax.add_patch(p)


def draw_kpi(ax, x, y, w, h, label, value, color, delta=None):
    draw_card(ax, x, y, w, h)
    ax.text(x + w/2, y + h*0.7, value, ha="center", va="center",
            fontsize=28, fontweight="bold", color=color, zorder=5)
    ax.text(x + w/2, y + h*0.3, label, ha="center", va="center",
            fontsize=12, color=GRAY, zorder=5)
    if delta:
        ax.text(x + w - 0.2, y + h*0.7, delta, ha="right", va="center",
                fontsize=10, color=GREEN, fontweight="bold", zorder=5)


def draw_bar_chart(ax_sub, categories, values, colors, title):
    bars = ax_sub.barh(categories, values, color=colors, height=0.6, zorder=3)
    ax_sub.set_title(title, fontsize=13, fontweight="bold", color="#333", pad=10)
    ax_sub.set_xlim(0, max(values) * 1.15)
    for bar, v in zip(bars, values):
        ax_sub.text(bar.get_width() + max(values)*0.02, bar.get_y() + bar.get_height()/2,
                   f"{v:,.0f}", va="center", fontsize=10, color="#333")
    ax_sub.tick_params(axis='x', labelsize=9, colors=GRAY)
    ax_sub.tick_params(axis='y', labelsize=10, colors="#333")
    ax_sub.spines['top'].set_visible(False)
    ax_sub.spines['right'].set_visible(False)
    ax_sub.spines['left'].set_color(LIGHT_GRAY)
    ax_sub.spines['bottom'].set_color(LIGHT_GRAY)
    ax_sub.set_facecolor(CARD_BG)


def draw_line_chart(ax_sub, days, values, title):
    ax_sub.fill_between(days, values, alpha=0.15, color=BLUE)
    ax_sub.plot(days, values, color=BLUE, linewidth=2.5, zorder=3)
    ax_sub.set_title(title, fontsize=13, fontweight="bold", color="#333", pad=10)
    ax_sub.tick_params(axis='both', labelsize=9, colors=GRAY)
    ax_sub.spines['top'].set_visible(False)
    ax_sub.spines['right'].set_visible(False)
    ax_sub.spines['left'].set_color(LIGHT_GRAY)
    ax_sub.spines['bottom'].set_color(LIGHT_GRAY)
    ax_sub.set_facecolor(CARD_BG)


def draw_pie_chart(ax_sub, labels, sizes, colors, title):
    wedges, texts, autotexts = ax_sub.pie(sizes, labels=labels, colors=colors,
                                           autopct='%1.0f%%', startangle=90,
                                           textprops={'fontsize': 9})
    for t in autotexts:
        t.set_fontsize(9)
        t.set_fontweight('bold')
    ax_sub.set_title(title, fontsize=13, fontweight="bold", color="#333", pad=10)
    ax_sub.set_facecolor(CARD_BG)


def draw_map_placeholder(ax_sub):
    # Simplified Brazil map outline as scatter points for routes
    np.random.seed(42)
    # Origin cities (SE Brazil)
    ox = np.random.uniform(-49, -43, 30)
    oy = np.random.uniform(-25, -20, 30)
    # Destination cities
    dx = np.random.uniform(-51, -38, 30)
    dy = np.random.uniform(-28, -15, 30)

    ax_sub.set_facecolor("#E8F4FD")
    ax_sub.scatter(ox, oy, c=BLUE, s=40, zorder=4, alpha=0.7, label="Origem")
    ax_sub.scatter(dx, dy, c=RED, s=40, zorder=4, alpha=0.7, marker="^", label="Destino")

    for i in range(0, 30, 2):
        ax_sub.annotate("", xy=(dx[i], dy[i]), xytext=(ox[i], oy[i]),
                       arrowprops=dict(arrowstyle="-|>", color=ACCENT, lw=1, alpha=0.4),
                       zorder=3)

    ax_sub.set_title("Mapa de Entregas (Origem → Destino)", fontsize=13,
                     fontweight="bold", color="#333", pad=10)
    ax_sub.legend(fontsize=9, loc="lower left", framealpha=0.9)
    ax_sub.set_xlabel("Longitude", fontsize=9, color=GRAY)
    ax_sub.set_ylabel("Latitude", fontsize=9, color=GRAY)
    ax_sub.tick_params(axis='both', labelsize=8, colors=GRAY)
    ax_sub.spines['top'].set_visible(False)
    ax_sub.spines['right'].set_visible(False)
    ax_sub.spines['left'].set_color(LIGHT_GRAY)
    ax_sub.spines['bottom'].set_color(LIGHT_GRAY)


def main():
    fig = plt.figure(figsize=(FIG_W, FIG_H), dpi=DPI, facecolor=BG)

    # Title bar
    title_ax = fig.add_axes([0, 0.93, 1, 0.07], facecolor="#1B3A4B")
    title_ax.axis("off")
    title_ax.text(0.02, 0.5, "Logística — Painel Operacional",
                  ha="left", va="center", fontsize=22, fontweight="bold",
                  color="white", transform=title_ax.transAxes)
    title_ax.axhline(y=0, color=ACCENT, linewidth=4)

    # KPI cards row
    kpi_ax = fig.add_axes([0, 0.82, 1, 0.11], facecolor=BG)
    kpi_ax.set_xlim(0, 20)
    kpi_ax.set_ylim(0, 2)
    kpi_ax.axis("off")

    kpis = [
        ("Total Pedidos", "12.847", BLUE, "↑ 15%"),
        ("Taxa de Entrega", "87,3%", GREEN, "↑ 2,1%"),
        ("Frete Médio", "R$ 482", ACCENT, "↓ 3%"),
        ("Caminhões Ativos", "847", PURPLE, None),
    ]
    kw = 4.5
    kgap = 0.4
    kx = 0.5
    for label, value, color, delta in kpis:
        draw_kpi(kpi_ax, kx, 0.2, kw, 1.6, label, value, color, delta)
        kx += kw + kgap

    # Map (top center)
    map_ax = fig.add_axes([0.03, 0.42, 0.54, 0.38], facecolor=CARD_BG)
    draw_map_placeholder(map_ax)

    # Bar chart (top right) - Volume por UF
    bar_ax = fig.add_axes([0.61, 0.42, 0.36, 0.38], facecolor=CARD_BG)
    ufs = ["SP", "RJ", "MG", "PR", "RS", "BA", "SC"]
    volumes = [4250, 2180, 1650, 1320, 980, 750, 620]
    colors_bar = [BLUE, "#1E88E5", "#42A5F5", "#64B5F6", "#90CAF9", "#BBDEFB", "#E3F2FD"]
    draw_bar_chart(bar_ax, ufs, volumes, colors_bar, "Volume por UF (pedidos)")

    # Line chart (bottom left) - Volume Diário
    line_ax = fig.add_axes([0.03, 0.05, 0.44, 0.33], facecolor=CARD_BG)
    days = list(range(1, 31))
    np.random.seed(7)
    base = np.linspace(380, 450, 30)
    noise = np.random.normal(0, 25, 30)
    daily = base + noise
    draw_line_chart(line_ax, days, daily, "Volume Diário de Pedidos")
    line_ax.set_xlabel("Dia do Mês", fontsize=10, color=GRAY)

    # Pie chart (bottom right) - Status Entregas
    pie_ax = fig.add_axes([0.54, 0.05, 0.43, 0.33], facecolor=CARD_BG)
    status_labels = ["Entregue", "Em Trânsito", "No CD", "Devolvido"]
    status_sizes = [62, 22, 12, 4]
    status_colors = [GREEN, BLUE, ACCENT, RED]
    draw_pie_chart(pie_ax, status_labels, status_sizes, status_colors, "Status das Entregas")

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard_layout.png")
    fig.savefig(out, dpi=DPI, bbox_inches="tight", facecolor=BG, pad_inches=0.1)
    plt.close(fig)
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
