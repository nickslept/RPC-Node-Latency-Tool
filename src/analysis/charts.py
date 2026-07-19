from __future__ import annotations

import polars as pl
import seaborn as sns
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.legend import Legend

from .prep import DNR_LABEL

# Chart chrome
_SURFACE = "#fcfcfb"
_INK = "#0b0b0b"
_INK_SECONDARY = "#52514e"
_INK_MUTED = "#898781"
_GRID = "#e1e0d9"
_BASELINE = "#c3c2b7"

# each node provider is assigned a unique color in _PROVIDER_PALETTE
_PROVIDER_PALETTE = ["#2a78d6", "#1baf7a", "#eda100", "#008300", "#4a3aa7", "#e34948", "#e87ba4", "#eb6834"]

# Ordinal blue ramp for finishing places: place 1 (fastest) = darkest. Gray = did not report.
_PLACE_RAMP = ["#132A13", "#31572C", "#4F772D", "#90A955", "#C6CE72", "#A4A4A4"]
_DNR_COLOR = "#898781"

# Single hue for the per-provider percentile-band figures (the title carries the provider name).
_BAND_HUE = "#2a78d6"


def build_provider_color_map(ordered_providers: list[str]) -> dict[str, str]:
    """
    Takes in a list of ordered providers and assigns each provider to a fixed color.

    Returns a dictionary with the provider (e.g. alchemy) as the key, and a color from ``_PROVIDER_PALETTE`` as the value.
    """
    if len(ordered_providers) > len(_PROVIDER_PALETTE):
        raise ValueError(f"[ERROR] Palette supports at most {len(_PROVIDER_PALETTE)} providers, got {len(ordered_providers)}. Edit _PROVIDER_PALETTE in charts.py to add more custom colors.")
    return {provider: _PROVIDER_PALETTE[i] for i, provider in enumerate(ordered_providers)}


def _build_figure(figsize: tuple[float, float]) -> tuple[plt.Figure, plt.Axes]:
    """
    Creates a new figure and axes given figure dimensions. The following styling is applied:

    - ``_SURFACE`` color for the background
    - ``_BASELINE`` color for the x and y axis
    - ``_GRID`` horizontal gridline color. Gridlines appear BEHIND the data
    - ``_INK_MUTED`` tick line color
    - ``_INK_SECONDARY`` value label color
    
    Returns a ``(figure, axes)`` pair to plot data on. 
    """
    fig, ax = plt.subplots(figsize=figsize)
    fig.set_facecolor(_SURFACE)
    ax.set_facecolor(_SURFACE)
    ax.grid(True, axis="y", color=_GRID, linewidth=1)
    ax.set_axisbelow(True) # gridlines are drawn behind the data
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(_BASELINE)
    ax.tick_params(colors=_INK_MUTED, labelcolor=_INK_SECONDARY, labelsize=9)
    return fig, ax


def _restyle_legend(legend: Legend | None, title: str | None = None) -> None:
    """ 
    Restyles an existing legend. The following styling is applied:

    - The box/lines around a legend is removed
    - ``title`` is set to the legend title
    - ``_INK_SECONDARY`` is set to the legend title color, with a font size of 9
    - ``_INK_SECONDARY`` is set to the legend text/labels color, with a font size of 9

    Returns ``None`` and does nothing if ``Legend`` is ``None``
    """
    if legend is None:
        return
    legend.set_frame_on(False)
    legend.set_title(title)
    if legend.get_title() is not None:
        legend.get_title().set_color(_INK_SECONDARY)
        legend.get_title().set_fontsize(9)
    for text in legend.get_texts():
        text.set_color(_INK_SECONDARY)
        text.set_fontsize(9)


def _finish(fig: plt.Figure, ax: plt.Axes, *, title: str, xlabel: str, ylabel: str, out_path: str) -> None:
    ax.set_title(title, color=_INK, fontsize=13, pad=12, loc="left")
    ax.set_xlabel(xlabel, color=_INK_SECONDARY, fontsize=10)
    ax.set_ylabel(ylabel, color=_INK_SECONDARY, fontsize=10)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)


def save_latency_boxplot(long: pl.DataFrame, provider_colors: dict[str, str], out_path: str) -> None:
    """
    One picture for all providers: the distribution of each provider's latency behind the fastest
    node, as a box plot (fliers hidden so the boxes stay readable).
    """
    fig, ax = _build_figure((12, 7))
    order = list(provider_colors)
    sns.boxplot(
        data=long.to_pandas(),
        x="provider",
        y="offset_ms",
        order=order,
        hue="provider",
        hue_order=order,
        palette=provider_colors,
        legend=False,
        showfliers=False,
        width=0.5,
        linewidth=1.2,
        ax=ax,
    )
    _finish(
        fig,
        ax,
        title="Per-provider latency distribution",
        xlabel="",
        ylabel="Latency behind fastest node (ms)",
        out_path=out_path,
    )


def save_median_over_run(
    binned: pl.DataFrame, bin_seconds: int, provider_colors: dict[str, str], out_path: str
) -> None:
    """
    One picture for all providers: each provider's binned median latency behind the fastest node
    across the run.
    """
    fig, ax = _build_figure((12, 7))
    sns.lineplot(
        data=binned.to_pandas(),
        x="bin_start_min",
        y="median_ms",
        hue="provider",
        hue_order=list(provider_colors),
        palette=provider_colors,
        linewidth=2,
        ax=ax,
    )
    _restyle_legend(ax.get_legend())
    _finish(
        fig,
        ax,
        title=f"Median latency behind fastest node over the run ({bin_seconds}s bins)",
        xlabel="Time since run start (minutes)",
        ylabel="Median latency behind fastest node (ms)",
        out_path=out_path,
    )


def save_percentile_bands(node_band: pl.DataFrame, provider: str, bin_seconds: int, out_path: str) -> None:
    """
    One picture for ONE provider: binned p10-p90 and p25-p75 bands around the median latency
    across the run. ``node_band`` must already be filtered to that provider's rows.
    """
    fig, ax = _build_figure((11, 5.5))
    d = node_band.sort("bin_start_min")
    ax.fill_between(d["bin_start_min"], d["p10"], d["p90"], color=_BAND_HUE, alpha=0.15, linewidth=0, label="p10–p90")
    ax.fill_between(d["bin_start_min"], d["p25"], d["p75"], color=_BAND_HUE, alpha=0.32, linewidth=0, label="p25–p75")
    ax.plot(d["bin_start_min"], d["p50"], color=_BAND_HUE, linewidth=2, solid_capstyle="round", label="median")
    _restyle_legend(ax.legend(loc="upper right"))
    _finish(
        fig,
        ax,
        title=f"{provider}: latency percentiles over the run ({bin_seconds}s bins)",
        xlabel="Time since run start (minutes)",
        ylabel="Latency behind fastest node (ms)",
        out_path=out_path,
    )


def save_finishing_places(place_share: pl.DataFrame, provider_colors: dict[str, str], out_path: str) -> None:
    """
    One picture for all providers: a stacked bar per provider segmented by finishing place
    (share of ALL transactions; place 1 = fastest, gray = did not report).
    """
    labels = [col for col in place_share.columns if col != "provider"]
    colors = [
        _DNR_COLOR if label == DNR_LABEL else _PLACE_RAMP[min(int(label) - 1, len(_PLACE_RAMP) - 1)]
        for label in labels
    ]
    pdf = place_share.to_pandas().set_index("provider").reindex(list(provider_colors))[labels]

    fig, ax = _build_figure((10, 6))
    pdf.plot(kind="bar", stacked=True, color=colors, width=0.55, edgecolor=_SURFACE, linewidth=1, ax=ax)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.0%}"))
    ax.tick_params(axis="x", rotation=0)
    _restyle_legend(ax.legend(bbox_to_anchor=(1.02, 0.5), loc="center left"), title="Place")
    _finish(
        fig,
        ax,
        title="Finishing place per transaction, by provider",
        xlabel="",
        ylabel="Share of all transactions",
        out_path=out_path,
    )
