"""
Distribution model analytics for dist_model-style CSV files.

Setup:
  conda install pandas matplotlib numpy

Run:
  python dist_model_analytics.py --input dist_model.csv --output analytics_output

Outputs:
  - CSV summary tables
  - PNG charts
  - report.md with a compact narrative summary

Assumptions:
  - upstream_cc / downstream_cc are customer-count style impact fields.
  - upstream_node -> downstream_node represents directed network flow.
  - segment_id identifies a line/edge segment.
  - sus_device_id and mom_device_id represent protective/restoration device references when present.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


REQUIRED_COLUMNS = {
  "source_id",
  "circuit_id",
  "segment_id",
  "upstream_node",
  "downstream_node",
  "sus_device_id",
  "mom_device_id",
  "upstream_cc",
  "downstream_cc",
  "tree",
  "seq",
}


def safe_name(value: object, max_len: int = 60) -> str:
  if pd.isna(value):
    return "<missing>"
  text = str(value)
  return text if len(text) <= max_len else text[: max_len - 3] + "..."


def save_table(df: pd.DataFrame, output_dir: Path, name: str) -> Path:
  path = output_dir / f"{name}.csv"
  df.to_csv(path, index=True)
  return path


def save_chart(fig: plt.Figure, output_dir: Path, name: str) -> Path:
  path = output_dir / f"{name}.png"
  fig.tight_layout()
  fig.savefig(path, dpi=160, bbox_inches="tight")
  plt.close(fig)
  return path


def add_bar_labels(ax, values: Iterable[float]) -> None:
  for i, value in enumerate(values):
    if pd.isna(value):
      continue
    ax.text(i, value, f"{value:,.0f}", ha="center", va="bottom", fontsize=8)


def profile_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
  rows = len(df)
  profile = pd.DataFrame({
    "dtype": df.dtypes.astype(str),
    "non_null": df.notna().sum(),
    "nulls": df.isna().sum(),
    "null_pct": (df.isna().sum() / max(rows, 1) * 100).round(2),
    "unique": df.nunique(dropna=True),
  })

  numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
  numeric = df[numeric_cols].describe().T if numeric_cols else pd.DataFrame()

  categorical_cols = [c for c in df.columns if c not in numeric_cols]
  categorical = pd.DataFrame({
    "unique": df[categorical_cols].nunique(dropna=True),
    "top": [safe_name(df[c].mode(dropna=True).iloc[0]) if not df[c].mode(dropna=True).empty else None for c in categorical_cols],
    "top_count": [df[c].value_counts(dropna=True).iloc[0] if not df[c].value_counts(dropna=True).empty else 0 for c in categorical_cols],
  }) if categorical_cols else pd.DataFrame()

  return {
    "column_profile": profile,
    "numeric_summary": numeric,
    "categorical_summary": categorical,
  }


def add_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
  out = df.copy()
  out["circuit_id"] = out["circuit_id"].fillna("<source/root>")
  out["sus_device_id"] = out["sus_device_id"].fillna("<none>")
  out["mom_device_id"] = out["mom_device_id"].fillna("<none>")

  out["total_cc"] = out["upstream_cc"].fillna(0) + out["downstream_cc"].fillna(0)
  out["downstream_pct_of_total"] = np.where(
    out["total_cc"] > 0,
    out["downstream_cc"] / out["total_cc"],
    np.nan,
  )
  out["upstream_pct_of_total"] = np.where(
    out["total_cc"] > 0,
    out["upstream_cc"] / out["total_cc"],
    np.nan,
  )
  out["impact_ratio_down_to_up"] = np.where(
    out["upstream_cc"] > 0,
    out["downstream_cc"] / out["upstream_cc"],
    np.nan,
  )
  out["is_source_segment"] = out["upstream_cc"].fillna(0).eq(0)
  out["is_leaf_like"] = out["downstream_cc"].fillna(0).le(1)
  out["tree_depth_hint"] = out["tree"].astype(str).str.len()
  return out


def network_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
  upstream_edges = df.groupby("upstream_node").agg(
    outgoing_segments=("segment_id", "count"),
    total_downstream_cc=("downstream_cc", "sum"),
    max_downstream_cc=("downstream_cc", "max"),
  )

  downstream_edges = df.groupby("downstream_node").agg(
    incoming_segments=("segment_id", "count"),
    total_upstream_cc=("upstream_cc", "sum"),
  )

  node_degree = upstream_edges.join(downstream_edges, how="outer").fillna(0)
  node_degree["degree"] = node_degree["incoming_segments"] + node_degree["outgoing_segments"]
  node_degree["is_branch_point"] = node_degree["outgoing_segments"] > 1
  node_degree["is_terminal_node"] = node_degree["outgoing_segments"].eq(0)
  node_degree = node_degree.sort_values(["degree", "outgoing_segments"], ascending=False)

  top_segments = df[[
    "segment_id",
    "upstream_node",
    "downstream_node",
    "upstream_cc",
    "downstream_cc",
    "total_cc",
    "downstream_pct_of_total",
    "tree",
    "seq",
    "sus_device_id",
  ]].sort_values(["downstream_cc", "upstream_cc"], ascending=False)

  device_summary = df.groupby("sus_device_id", dropna=False).agg(
    segments=("segment_id", "count"),
    downstream_cc_total=("downstream_cc", "sum"),
    downstream_cc_max=("downstream_cc", "max"),
    upstream_cc_total=("upstream_cc", "sum"),
    first_tree=("tree", "min"),
  ).sort_values(["downstream_cc_total", "segments"], ascending=False)

  circuit_summary = df.groupby("circuit_id", dropna=False).agg(
    segments=("segment_id", "count"),
    source_segments=("is_source_segment", "sum"),
    leaf_like_segments=("is_leaf_like", "sum"),
    downstream_cc_total=("downstream_cc", "sum"),
    downstream_cc_max=("downstream_cc", "max"),
    unique_upstream_nodes=("upstream_node", "nunique"),
    unique_downstream_nodes=("downstream_node", "nunique"),
  ).sort_values("segments", ascending=False)

  tree_summary = df.groupby("tree").agg(
    segments=("segment_id", "count"),
    min_seq=("seq", "min"),
    max_seq=("seq", "max"),
    downstream_cc_total=("downstream_cc", "sum"),
    upstream_cc_total=("upstream_cc", "sum"),
  ).sort_index()

  return {
    "node_degree": node_degree,
    "top_segments_by_downstream_cc": top_segments,
    "device_summary": device_summary,
    "circuit_summary": circuit_summary,
    "tree_summary": tree_summary,
  }


def draw_charts(df: pd.DataFrame, tables: dict[str, pd.DataFrame], output_dir: Path) -> list[Path]:
  chart_paths: list[Path] = []

  # 1. Downstream customer count distribution
  fig, ax = plt.subplots(figsize=(8, 5))
  ax.hist(df["downstream_cc"].dropna(), bins=min(10, max(3, len(df) // 2)))
  ax.set_title("Distribution of Downstream Customer Counts")
  ax.set_xlabel("downstream_cc")
  ax.set_ylabel("segment count")
  chart_paths.append(save_chart(fig, output_dir, "chart_downstream_cc_histogram"))

  # 2. Top segments by downstream impact
  top = tables["top_segments_by_downstream_cc"].head(12).copy()
  top["label"] = top["segment_id"].map(lambda x: safe_name(x, 26))
  fig, ax = plt.subplots(figsize=(10, 5))
  ax.bar(range(len(top)), top["downstream_cc"])
  ax.set_title("Top Segments by Downstream Customer Count")
  ax.set_xlabel("segment")
  ax.set_ylabel("downstream_cc")
  ax.set_xticks(range(len(top)))
  ax.set_xticklabels(top["label"], rotation=45, ha="right")
  add_bar_labels(ax, top["downstream_cc"])
  chart_paths.append(save_chart(fig, output_dir, "chart_top_segments_downstream_cc"))

  # 3. Upstream vs downstream impact scatter
  fig, ax = plt.subplots(figsize=(7, 5))
  ax.scatter(df["upstream_cc"], df["downstream_cc"])
  ax.set_title("Upstream vs Downstream Customer Counts")
  ax.set_xlabel("upstream_cc")
  ax.set_ylabel("downstream_cc")
  for _, row in df.sort_values("downstream_cc", ascending=False).head(5).iterrows():
    ax.annotate(str(row["tree"]), (row["upstream_cc"], row["downstream_cc"]), fontsize=8)
  chart_paths.append(save_chart(fig, output_dir, "chart_upstream_vs_downstream_cc"))

  # 4. Protective device downstream exposure
  dev = tables["device_summary"].copy()
  dev = dev[dev.index.astype(str) != "<none>"].head(10)
  if not dev.empty:
    labels = [safe_name(x, 32) for x in dev.index]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(range(len(dev)), dev["downstream_cc_total"])
    ax.set_title("Downstream Exposure by SUS Device")
    ax.set_xlabel("sus_device_id")
    ax.set_ylabel("sum downstream_cc")
    ax.set_xticks(range(len(dev)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    add_bar_labels(ax, dev["downstream_cc_total"])
    chart_paths.append(save_chart(fig, output_dir, "chart_sus_device_downstream_exposure"))

  # 5. Tree/sequence map
  fig, ax = plt.subplots(figsize=(8, 5))
  sizes = 30 + df["downstream_cc"].fillna(0).clip(lower=0) * 1.2
  ax.scatter(df["seq"], df["tree"], s=sizes, alpha=0.65)
  ax.set_title("Tree / Sequence Map sized by downstream_cc")
  ax.set_xlabel("seq")
  ax.set_ylabel("tree")
  chart_paths.append(save_chart(fig, output_dir, "chart_tree_sequence_map"))

  # 6. Node degree / branch points
  nd = tables["node_degree"].head(12).copy()
  if not nd.empty:
    labels = [safe_name(x, 28) for x in nd.index]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(range(len(nd)), nd["outgoing_segments"])
    ax.set_title("Node Branching: Outgoing Segment Count")
    ax.set_xlabel("node")
    ax.set_ylabel("outgoing segments")
    ax.set_xticks(range(len(nd)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    add_bar_labels(ax, nd["outgoing_segments"])
    chart_paths.append(save_chart(fig, output_dir, "chart_node_branching"))

  return chart_paths


def draw_simple_network(df: pd.DataFrame, output_dir: Path) -> Path | None:
  """Small dependency-free network sketch using tree/seq hints."""
  if df.empty:
    return None

  nodes = sorted(set(df["upstream_node"].dropna()) | set(df["downstream_node"].dropna()))
  pos: dict[str, tuple[float, float]] = {}

  # Position upstream/downstream nodes using row order and tree depth hint.
  for i, row in df.sort_values(["tree", "seq"]).reset_index(drop=True).iterrows():
    depth = len(str(row["tree"]))
    up = row["upstream_node"]
    down = row["downstream_node"]
    pos.setdefault(up, (max(depth - 1, 0), -i))
    pos.setdefault(down, (depth, -i - 0.35))

  missing = [n for n in nodes if n not in pos]
  for j, n in enumerate(missing):
    pos[n] = (0, -len(pos) - j)

  fig, ax = plt.subplots(figsize=(12, 8))

  max_cc = max(float(df["downstream_cc"].max()), 1.0)
  for _, row in df.iterrows():
    up = row["upstream_node"]
    down = row["downstream_node"]
    x1, y1 = pos[up]
    x2, y2 = pos[down]
    width = 0.5 + 3.0 * (float(row["downstream_cc"] or 0) / max_cc)
    ax.plot([x1, x2], [y1, y2], linewidth=width, alpha=0.6)

  xs = [pos[n][0] for n in nodes]
  ys = [pos[n][1] for n in nodes]
  ax.scatter(xs, ys, s=45)

  # Label only the most structurally important nodes to avoid clutter.
  degree = pd.concat([
    df["upstream_node"].value_counts(),
    df["downstream_node"].value_counts(),
  ], axis=1).fillna(0).sum(axis=1).sort_values(ascending=False)
  label_nodes = set(degree.head(12).index)
  for n in nodes:
    if n in label_nodes:
      x, y = pos[n]
      ax.text(x + 0.05, y + 0.05, safe_name(n, 22), fontsize=8)

  ax.set_title("Approximate Network Sketch: edges weighted by downstream_cc")
  ax.set_xlabel("tree depth hint")
  ax.set_ylabel("relative row/order")
  ax.grid(True, alpha=0.25)
  return save_chart(fig, output_dir, "chart_approx_network_sketch")


def write_report(df: pd.DataFrame, tables: dict[str, pd.DataFrame], chart_paths: list[Path], table_paths: dict[str, Path], output_dir: Path) -> Path:
  total_segments = len(df)
  total_nodes = len(set(df["upstream_node"].dropna()) | set(df["downstream_node"].dropna()))
  source_segments = int(df["is_source_segment"].sum())
  leaf_like = int(df["is_leaf_like"].sum())
  max_downstream = df.sort_values("downstream_cc", ascending=False).iloc[0]

  branch_points = tables["node_degree"].query("is_branch_point == True") if "node_degree" in tables else pd.DataFrame()
  top_device = tables["device_summary"]
  top_device = top_device[top_device.index.astype(str) != "<none>"].head(1)

  lines = []
  lines.append("# Distribution Model Analytics Report")
  lines.append("")
  lines.append("## Executive Summary")
  lines.append(f"- Rows / segments analyzed: **{total_segments:,}**")
  lines.append(f"- Unique nodes observed: **{total_nodes:,}**")
  lines.append(f"- Source-like segments where upstream_cc = 0: **{source_segments:,}**")
  lines.append(f"- Leaf-like segments where downstream_cc <= 1: **{leaf_like:,}**")
  lines.append(f"- Highest downstream_cc segment: **{safe_name(max_downstream['segment_id'])}** with **{int(max_downstream['downstream_cc']):,}** downstream customers")
  if not top_device.empty:
    lines.append(f"- Highest summarized SUS device exposure: **{safe_name(top_device.index[0])}** with **{int(top_device.iloc[0]['downstream_cc_total']):,}** total downstream_cc across mapped segments")
  if not branch_points.empty:
    lines.append(f"- Branch points detected: **{len(branch_points):,}** nodes with more than one outgoing segment")

  lines.append("")
  lines.append("## Interesting Checks")
  lines.append("- `downstream_cc` can be read as a quick proxy for customer impact if a segment/device opens.")
  lines.append("- `upstream_cc + downstream_cc` is useful as a consistency/total-service-area check per segment.")
  lines.append("- High outgoing node degree identifies branch points where topology decisions may have large tracing consequences.")
  lines.append("- `tree` and `seq` appear useful as deterministic ordering hints for path/branch visualization.")

  lines.append("")
  lines.append("## Output Tables")
  for name, path in table_paths.items():
    lines.append(f"- `{path.name}`")

  lines.append("")
  lines.append("## Charts")
  for path in chart_paths:
    lines.append(f"- `{path.name}`")

  report_path = output_dir / "report.md"
  report_path.write_text("\n".join(lines), encoding="utf-8")
  return report_path


def analyze(input_path: Path, output_dir: Path) -> None:
  output_dir.mkdir(parents=True, exist_ok=True)
  df = pd.read_csv(input_path)

  missing = REQUIRED_COLUMNS - set(df.columns)
  if missing:
    print(f"Warning: missing expected columns: {sorted(missing)}")

  df = add_derived_fields(df)

  table_paths: dict[str, Path] = {}
  for name, table in profile_tables(df).items():
    table_paths[name] = save_table(table, output_dir, name)

  tables = network_tables(df)
  for name, table in tables.items():
    table_paths[name] = save_table(table, output_dir, name)

  derived_path = output_dir / "derived_rows.csv"
  df.to_csv(derived_path, index=False)
  table_paths["derived_rows"] = derived_path

  chart_paths = draw_charts(df, tables, output_dir)
  network_path = draw_simple_network(df, output_dir)
  if network_path:
    chart_paths.append(network_path)

  report_path = write_report(df, tables, chart_paths, table_paths, output_dir)

  print("Analytics complete.")
  print(f"Input: {input_path}")
  print(f"Output folder: {output_dir}")
  print(f"Report: {report_path}")
  print("\nTop 5 segments by downstream_cc:")
  print(tables["top_segments_by_downstream_cc"].head(5).to_string(index=False))


def RunAnalysis(model_path, out_dir) -> None:
  analyze(Path(model_path), Path(out_dir))
