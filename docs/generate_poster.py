"""Generate a one-page feature overview poster for PlanetaryPy."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle
import matplotlib.patheffects as pe
from PIL import Image
import numpy as np

# --- Colors (Monokai-inspired) ---
BG = "#1a1a2e"
CARD_BG = "#22223a"
CARD_BORDER = "#333355"
WHITE = "#e0e0e0"
YELLOW = "#e6db74"
CYAN = "#66d9ef"
GREEN = "#a6e22e"
PINK = "#f92672"
ORANGE = "#fd971f"
GREY = "#888888"
DARK_GREY = "#555555"

ACCENT_COLORS = [YELLOW, CYAN, GREEN, PINK, ORANGE, CYAN, GREEN, PINK, ORANGE]

# --- Card data ---
cards = [
    {
        "title": "PDS Index Access",
        "stat": "90+",
        "stat_color": YELLOW,
        "desc": "Pre-configured indexes from NASA's\nPlanetary Data System.\nOne-line access as a Pandas DataFrame.",
        "code": 'pds.get_index("mro.ctx.edr")',
    },
    {
        "title": "PDS Catalog",
        "stat": "65 Missions",
        "stat_color": CYAN,
        "desc": "420 instruments, 2042 product types.\nBrowse missions, instruments,\nand fetch products by dotted key.",
        "code": 'Mission("mro").instruments',
    },
    {
        "title": "SPICE Kernels",
        "stat": "37 Missions",
        "stat_color": GREEN,
        "desc": "Archived SPICE kernels with\ndate-filtered subsets via NAIF.\nReady-to-load metakernels.",
        "code": 'ak.get_metakernel_and_files(\n  "mro", start="2024-01-01")',
    },
    {
        "title": "Auto-Updating Cache",
        "stat": "",
        "stat_color": ORANGE,
        "desc": "Indexes checked daily, refreshed\nwhen stale. Parquet format for\nfast reads and cloud-readiness.",
        "code": 'pds.get_index("mro.ctx.edr",\n  allow_refresh=True)',
    },
    {
        "title": "Dynamic URL Discovery",
        "stat": "",
        "stat_color": PINK,
        "desc": "No manual URL tracking needed.\nIndex URLs discovered automatically\nby scraping PDS archive pages.",
        "code": 'Index("mro.ctx.edr").url\n# auto-discovered',
    },
    {
        "title": "Fetch Any Product",
        "stat": "",
        "stat_color": CYAN,
        "desc": "Download any PDS product by ID.\nDuckDB-backed search across\nall missions instantly.",
        "code": 'fetch_product("mro.ctx.edr",\n  "P01_001234_1800_XI_00N042W")',
    },
    {
        "title": "Generic SPICE Kernels",
        "stat": "",
        "stat_color": GREEN,
        "desc": "Planetary constants and basic\nillumination calculations.\nOne-call download and loading.",
        "code": "download_generic_kernels()\nload_generic_kernels()",
    },
    {
        "title": "Zero Config",
        "stat": "",
        "stat_color": PINK,
        "desc": "Works out of the box.\nData in ~/planetarypy_data/\nOverride with env var.",
        "code": "config.storage_root\n# ~/planetarypy_data/",
    },
    {
        "title": "DataFrame-Native",
        "stat": "164K+ rows",
        "stat_color": ORANGE,
        "desc": "CTX EDR: 164,103 rows x 51 cols.\nAll indexes as Pandas DataFrames,\nready for analysis.",
        "code": 'df = pds.get_index("mro.ctx.edr")\ndf[cols].tail()',
    },
]


def draw_rounded_rect(ax, x, y, w, h, radius=0.015, fc=CARD_BG, ec=CARD_BORDER, lw=1):
    """Draw a rounded rectangle on the axes."""
    box = FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad={radius}",
        facecolor=fc, edgecolor=ec, linewidth=lw,
        transform=ax.transAxes, zorder=2,
    )
    ax.add_patch(box)
    return box


def main():
    fig, ax = plt.subplots(1, 1, figsize=(16, 12), dpi=150)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # --- Header ---
    ax.text(
        0.03, 0.965, "PlanetaryPy",
        fontsize=36, fontweight="bold", color=CYAN,
        va="top", ha="left", transform=ax.transAxes,
        fontfamily="sans-serif",
    )
    ax.text(
        0.03, 0.925, "Core Tools for Planetary Science Data",
        fontsize=14, color=GREY,
        va="top", ha="left", transform=ax.transAxes,
    )

    # Install badge
    badge_x, badge_y = 0.72, 0.945
    draw_rounded_rect(ax, badge_x - 0.005, badge_y - 0.015, 0.27, 0.04,
                      radius=0.008, fc="#1d2e1d", ec="#3a5a3a")
    ax.text(
        badge_x + 0.13, badge_y + 0.005, "pip install planetarypy",
        fontsize=13, color=GREEN, fontfamily="monospace",
        va="center", ha="center", transform=ax.transAxes, zorder=3,
    )

    # Header line
    ax.plot([0.03, 0.97], [0.905, 0.905], color=DARK_GREY, linewidth=0.5,
            transform=ax.transAxes, zorder=2)

    # --- Grid of cards ---
    cols, rows = 3, 3
    margin_x, margin_y = 0.03, 0.04
    gap_x, gap_y = 0.02, 0.02
    grid_top = 0.89
    grid_bottom = 0.07
    card_w = (1 - 2 * margin_x - (cols - 1) * gap_x) / cols
    card_h = (grid_top - grid_bottom - (rows - 1) * gap_y) / rows

    for i, card in enumerate(cards):
        col = i % cols
        row = i // cols
        cx = margin_x + col * (card_w + gap_x)
        cy = grid_top - (row + 1) * card_h - row * gap_y

        # Card background
        draw_rounded_rect(ax, cx, cy, card_w, card_h, radius=0.012)

        # Top accent line
        accent = ACCENT_COLORS[i]
        line_y = cy + card_h
        ax.plot([cx + 0.012, cx + card_w - 0.012], [line_y - 0.002, line_y - 0.002],
                color=accent, linewidth=3, transform=ax.transAxes, zorder=3,
                solid_capstyle="round")

        # Content positioning
        text_x = cx + 0.018
        top_y = cy + card_h - 0.025

        # Title
        ax.text(text_x, top_y, card["title"],
                fontsize=14, fontweight="bold", color=WHITE,
                va="top", ha="left", transform=ax.transAxes, zorder=3)

        # Stat (if present)
        content_y = top_y - 0.035
        if card["stat"]:
            ax.text(text_x, content_y, card["stat"],
                    fontsize=22, fontweight="bold", color=card["stat_color"],
                    va="top", ha="left", transform=ax.transAxes, zorder=3)
            content_y -= 0.045
        else:
            content_y -= 0.005

        # Description
        ax.text(text_x, content_y, card["desc"],
                fontsize=8.5, color=GREY, linespacing=1.4,
                va="top", ha="left", transform=ax.transAxes, zorder=3)

        # Code snippet box
        code_y = cy + 0.015
        code_h = 0.065
        code_w = card_w - 0.036
        draw_rounded_rect(ax, cx + 0.012, code_y, code_w, code_h,
                          radius=0.006, fc="#111122", ec="#2a2a44", lw=0.5)
        ax.text(cx + 0.022, code_y + code_h - 0.01, card["code"],
                fontsize=7.5, color=GREEN, fontfamily="monospace",
                va="top", ha="left", transform=ax.transAxes, zorder=3,
                linespacing=1.4)

    # --- Footer ---
    ax.plot([0.03, 0.97], [0.055, 0.055], color=DARK_GREY, linewidth=0.5,
            transform=ax.transAxes, zorder=2)
    footer = "BSD-3 License  |  github.com/planetarypy/planetarypy  |  planetarypy.org  |  Python >= 3.11"
    ax.text(0.5, 0.03, footer,
            fontsize=10, color=DARK_GREY,
            va="center", ha="center", transform=ax.transAxes)

    # --- Save ---
    out = "feature_poster.png"
    fig.savefig(out, dpi=150, bbox_inches="tight", pad_inches=0.1, facecolor=BG)
    plt.close()
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
