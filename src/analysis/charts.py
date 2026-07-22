from __future__ import annotations

import polars as pl
import seaborn as sns
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.legend import Legend

from .transform import DNR_LABEL

# --- COLORS ---

_BACKGROUND_HUE = "#ffffff"
_TEXT_PRIMARY_HUE = "#000000"
_TEXT_SECONDARY_HUE = "#52514e"
_TEXT_MUTED_HUE = "#898781"
_GRIDLINE_HUE = "#e1e0d9"
_AXIS_HUE = "#c3c2b7"

_PROVIDER_PALETTE = ["#2a78d6", "#1baf7a", "#eda100", "#008300", "#4a3aa7"]
_PLACEMENT_PALETTE = ["#132A13", "#31572C", "#4F772D", "#90A955", "#C6CE72", "#A4A4A4"]
_DNR_COLOR = "#898781"
_FAN_CHART_HUE = "#2a78d6"

# --- HELPERS ---

def build_provider_color_map(ordered_providers: list[str]) -> dict[str, str]:
    """
    Takes in a list of ordered providers and assigns each provider to a fixed color.

    Returns a dictionary with the provider (e.g. alchemy) as the key, and a color from ``_PROVIDER_PALETTE`` as the value.
    """
    if len(ordered_providers) > len(_PROVIDER_PALETTE):
        raise ValueError(f"Palette supports at most {len(_PROVIDER_PALETTE)} providers, got {len(ordered_providers)}. Edit _PROVIDER_PALETTE in charts.py to add more custom colors.")
    return {provider: _PROVIDER_PALETTE[i] for i, provider in enumerate(ordered_providers)}


def _build_figure(figsize: tuple[float, float]) -> tuple[plt.Figure, plt.Axes]:
    """
    Creates a new figure and axes given figure dimensions. The following styling is applied:

    - ``_BACKGROUND_HUE`` color for the background
    - ``_AXIS_HUE`` color for the x and y axis
    - ``_GRIDLINE_HUE`` horizontal gridline color. Gridlines appear BEHIND the data
    - ``_TEXT_MUTED_HUE`` tick line color
    - ``_TEXT_SECONDARY_HUE`` value label color
    
    Returns a ``(figure, axes)`` pair to plot data on. 
    """
    fig, ax = plt.subplots(figsize=figsize)
    fig.set_facecolor(_BACKGROUND_HUE)
    ax.set_facecolor(_BACKGROUND_HUE)
    ax.grid(True, axis="y", color=_GRIDLINE_HUE, linewidth=1)
    ax.set_axisbelow(True) # gridlines are drawn behind the data
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(_AXIS_HUE)
    ax.tick_params(colors=_TEXT_MUTED_HUE, labelcolor=_TEXT_SECONDARY_HUE, labelsize=9)
    return fig, ax


def _restyle_legend(legend: Legend | None, title: str | None = None) -> None:
    """ 
    Restyles an existing legend. The following styling is applied:

    - The box/lines around a legend is removed
    - ``title`` is set to the legend title
    - ``_TEXT_SECONDARY_HUE`` is set to the legend title color, with a font size of 9
    - ``_TEXT_SECONDARY_HUE`` is set to the legend text/labels color, with a font size of 9

    Returns ``None`` and does nothing if ``Legend`` is ``None``
    """
    if legend is None:
        return
    legend.set_frame_on(False)
    legend.set_title(title)
    if legend.get_title() is not None:
        legend.get_title().set_color(_TEXT_SECONDARY_HUE)
        legend.get_title().set_fontsize(12)
    for text in legend.get_texts():
        text.set_color(_TEXT_SECONDARY_HUE)
        text.set_fontsize(10)


def _label_and_save(fig: plt.Figure, ax: plt.Axes, *, title: str, xlabel: str, ylabel: str, out_path: str) -> None:
    """
    Labels a finished chart and saves it as an image. The following styling is applied:

    - ``title`` is set as the chart title, left-aligned, in ``_TEXT_PRIMARY_HUE`` color with a font size of 13
    - ``xlabel``/``ylabel`` are set as the axis labels in ``_TEXT_SECONDARY_HUE`` color with a font size of 10
    - Margins are auto-adjusted so nothing gets clipped

    Saves the figure to ``out_path`` at 150 DPI (keeping the ``_BACKGROUND_HUE`` background color), then
    closes it to free memory.
    """
    ax.set_title(title, color=_TEXT_PRIMARY_HUE, fontsize=13, pad=12, loc="left")
    ax.set_xlabel(xlabel, color=_TEXT_SECONDARY_HUE, fontsize=10)
    ax.set_ylabel(ylabel, color=_TEXT_SECONDARY_HUE, fontsize=10)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)

# --- CHART GEN & SAVING ---

def generate_and_save_delay_boxplot(
    long: pl.DataFrame,
    provider_colors: dict[str, str],
    out_path: str,
    *,
    title: str = "Distribution of delay behind fastest node, by provider",
) -> None:
    """
    Takes in a long formatted offset dataframe, a mapping of node provider to colors dict, and ``title`` which sets the chart title
    so the same plot can be reused across transaction scopes (e.g. all transactions vs. only the transactions where every node reported).
    Generates and saves a box plot of every provider's delay behind the fastest node.
    The plot is shaped by the following:

    - One box per provider, placed vertically in ``provider_colors`` order and filled with each provider's corresponding color
    - ``.boxplot()`` converts the x axis into categorical labels; the numeric tick values are dropped and replaced
      with the provider names, so no x axis label or legend is needed
    - The y axis plots the ``offset_ms`` column, so each box shows the distribution of that provider's delay in ms
    - Fliers (outlier dots beyond the whiskers) are hidden so the boxes remain readable

    Returns ``None``. Saves the finished chart to ``out_path``.
    """
    fig, ax = _build_figure((12, 7))
    order = list(provider_colors)
    sns.boxplot(
        data=long.to_pandas(),
        x="provider",
        y="offset_ms",
        order=order,
        hue="provider", # column name in long
        hue_order=order, # list of categorical variables (provider names)
        palette=provider_colors, # dict with provider keys & color values
        legend=False,
        showfliers=False,
        width=0.5,
        linewidth=1.25,
        ax=ax,
    )
    _label_and_save(
        fig,
        ax,
        title=title,
        xlabel="Node provider",
        ylabel="Delay behind fastest node (ms)",
        out_path=out_path,
    )


def generate_and_save_median_delay_lineplot_all_nodes(
    binned: pl.DataFrame, bin_seconds: int, provider_colors: dict[str, str], out_path: str
) -> None:
    """
    Takes in a time-binned dataframe, the bin size in seconds, and a mapping of node provider to colors dict.
    Generates and saves a line plot of every provider's median delay (in time bins) behind the fastest
    node across the run. The plot is shaped by the following:

    - One line per provider, colored by its corresponding color in ``provider_colors``
    - The x axis plots ``bin_start_min`` (each bin's start time in minutes since the run began)
    - The y axis plots the ``median_ms`` column, so each point is that provider's median delay in ms within the bin

    Returns ``None``. Saves the finished chart to ``out_path``.
    """
    fig, ax = _build_figure((12, 7))
    sns.lineplot(
        data=binned.to_pandas(),
        x="bin_start_min", #essentially like run start except based on bin time
        y="median_ms", #delay behind fastest node
        hue="provider",
        hue_order=list(provider_colors),
        palette=provider_colors,
        linewidth=2,
        ax=ax,
    )
    _restyle_legend(ax.get_legend())
    _label_and_save(
        fig,
        ax,
        title=f"Median delay behind the fastest node, by provider ({bin_seconds}s bins)",
        xlabel="Time since run start (min)",
        ylabel="Median delay behind fastest node (ms)",
        out_path=out_path,
    )


def generate_and_save_delay_fan_chart(binned_percentiles: pl.DataFrame, provider: str, bin_seconds: int, out_path: str) -> None:
    """
    Takes in a time-binned percentile dataframe for ONE provider, the provider name, and the bin size in seconds.
    Generates and saves a fan chart of that provider's delay behind the fastest node across
    the run: a median line surrounded by two shaded percentile ranges. ``binned_percentiles`` must already be filtered to that provider's rows. The plot is shaped by
    the following:

    - The x axis plots ``bin_start_min`` (each bin's start time in minutes since the run began)
    - The wider, lighter band fills ``p10``–``p90`` and the inner, darker band fills ``p25``–``p75``, so the
      shaded regions show how spread out the delay is within each bin
    - A solid line plots ``p50`` (the median delay) on top of the bands

    Returns ``None``. Saves the finished chart to ``out_path``.
    """
    fig, ax = _build_figure((12, 6))
    d = binned_percentiles.sort("bin_start_min")
    ax.fill_between(d["bin_start_min"], d["p10"], d["p90"], color=_FAN_CHART_HUE, alpha=0.16, linewidth=0, label="p10–p90")
    ax.fill_between(d["bin_start_min"], d["p25"], d["p75"], color=_FAN_CHART_HUE, alpha=0.32, linewidth=0, label="p25–p75")
    ax.plot(d["bin_start_min"], d["p50"], color=_FAN_CHART_HUE, linewidth=2, solid_capstyle="round", label="median")
    _restyle_legend(ax.legend())
    _label_and_save(
        fig,
        ax,
        title=f"{provider}'s delay behind the fastest node, median and spread ({bin_seconds}s bins)",
        xlabel="Time since run start (min)",
        ylabel="Delay behind fastest node (ms)",
        out_path=out_path,
    )


def generate_and_save_speed_ranking_stacked_bar_chart(
    place_share: pl.DataFrame,
    provider_colors: dict[str, str],
    out_path: str,
    *,
    title: str = "Reporting speed ranking across all transactions, by provider",
    ylabel: str = "Share of all transactions",
) -> None:
    """
    Takes in a place-share dataframe, a mapping of node provider to colors dict, and ``title`` & ``ylabel`` which set the scope-specific text 
    so the same plot can be reused across transaction scopes (e.g. all transactions vs. only the transactions where every node reported).

    Generates and saves a stacked bar chart showing the share of transactions in which a provider reported in each
    speed rank (rank 1 = fastest node to report a transaction). The plot is shaped by the following:

    - One stacked bar per provider, ordered along the x axis by ``provider_colors``
    - Each segment is colored by rank via ``_PLACEMENT_PALETTE`` (rank 1 = darkest). If ``place_share`` carries a
      ``DNR_LABEL`` column (transactions the provider never reported) that segment is filled with ``_DNR_COLOR``.
      ``place_share`` frames built with ``include_dnr=False`` have no such column and therefore no DNR segment.
    - The y axis plots the ``share`` values, so each bar sums to 1.0 (every transaction is either ranked or, when
      present, a DNR); tick labels are formatted as percentages

    Returns ``None``. Saves the finished chart to ``out_path``.
    """
    labels = [col for col in place_share.columns if col != "provider"]
    colors = [
        _DNR_COLOR if label == DNR_LABEL else _PLACEMENT_PALETTE[min(int(label) - 1, len(_PLACEMENT_PALETTE) - 1)]
        for label in labels
    ]
    pdf = place_share.to_pandas().set_index("provider").reindex(list(provider_colors))[labels]

    fig, ax = _build_figure((10, 6))
    pdf.plot(kind="bar", stacked=True, color=colors, width=0.5, ax=ax)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.0%}")) #sets y-axis tick labels as percents instead of decimals
    ax.tick_params(axis="x", rotation=0) # rotates names on x-axis to be readable
    _restyle_legend(ax.legend(bbox_to_anchor=(1, .5), loc="center left"), title="Reporting speed rank\n(1=first to report a tx)")
    _label_and_save(
        fig,
        ax,
        title=title,
        xlabel="Node provider",
        ylabel=ylabel,
        out_path=out_path,
    )