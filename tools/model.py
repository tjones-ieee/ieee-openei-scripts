from __future__ import annotations

import os
from pathlib import Path
from dataclasses import dataclass, asdict
import pandas as pd
import geopandas as gpd

from tools.progress import print_progress


@dataclass
class ConnectivityModel:
  source_id: str
  circuit_id: str
  segment_id: str
  upstream_node: str
  downstream_node: str
  sus_device_id: str
  mom_device_id: str
  upstream_cc: int
  downstream_cc: int
  tree: str
  seq: int


@dataclass
class TracePrms:
  traversed_lines: list[str]
  line: dict
  node: str
  source_id: str
  circuit_id: str
  susaipid: str
  momaipid: str
  tree: str
  seq: int


class ConnectivityModelBuilder:
  tree_chars = "123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

  def __init__(self):
    self._SOURCES = {}
    self._CIRCUITS = {}
    self._LINES = {}
    self._LINES_BY_NODEA = {}
    self._LINES_BY_NODEB = {}
    self._DEVICES = {}
    self._DEVICES_BY_NODEA = {}
    self._DEVICES_BY_NODEB = {}
    self._TRANSFORMERS = {}
    self._TRANSFORMERS_BY_NODE = {}
    self._TOUCHED_LINES = set()
    self._MODEL: dict[str, ConnectivityModel] = {}

  def _get_tree(self, tree: str, idx: int):
    if idx >= len(self.tree_chars):
      super_idx = idx % len(self.tree_chars)
      return tree + self.tree_chars[super_idx] + self.tree_chars[super_idx]
    return tree + self.tree_chars[idx]

  def _df_to_dict(self, df: pd.DataFrame, key_col: str) -> dict:
    return {
      row[key_col]: row
      for row in df.to_dict(orient="records")
    }

  def _build_line_lookups(self, lines: dict):
    self._LINES_BY_NODEA = {}
    self._LINES_BY_NODEB = {}

    p = 0
    t = len(lines)
    print_progress(p, t)
    for code, item in lines.items():
      node_a = item.get("NodeA")
      node_b = item.get("NodeB")

      if pd.notna(node_a):
        node_a = str(node_a)
        self._LINES_BY_NODEA.setdefault(node_a, []).append(code)

      if pd.notna(node_b):
        node_b = str(node_b)
        self._LINES_BY_NODEB.setdefault(node_b, []).append(code)

      p += 1
      print_progress(p, t)
    print()

  def _build_device_lookups(self, devices: dict):
    self._DEVICES_BY_NODEA = {}
    self._DEVICES_BY_NODEB = {}

    p = 0
    t = len(devices)
    print_progress(p, t)
    for code, item in devices.items():
      node_a = item.get("NodeA")
      node_b = item.get("NodeB")

      if pd.notna(node_a):
        node_a = str(node_a)
        self._DEVICES_BY_NODEA.setdefault(node_a, []).append(code)

      if pd.notna(node_b):
        node_b = str(node_b)
        self._DEVICES_BY_NODEB.setdefault(node_b, []).append(code)

      p += 1
      print_progress(p, t)
    print()

  def _build_transformer_lookups(self, transformers: dict):
    self._TRANSFORMERS_BY_NODE = {}

    p = 0
    t = len(transformers)
    print_progress(p, t)
    for code, item in transformers.items():
      node_a = item.get("Node")

      if pd.notna(node_a):
        node_a = str(node_a)
        self._TRANSFORMERS_BY_NODE.setdefault(node_a, []).append(code)

      p += 1
      print_progress(p, t)
    print()

  def _prepare_data(self, dir: str):
    print("Loading sources, circuits, lines, devices, and transformers")
    self._SOURCES = self._df_to_dict(
      pd.read_csv(os.path.join(dir, "dist_sources.csv")),
      "Code"
    )

    self._CIRCUITS = self._df_to_dict(
      pd.read_csv(os.path.join(dir, "dist_circuits.csv")),
      "Code"
    )

    self._LINES = self._df_to_dict(
      pd.read_csv(os.path.join(dir, "dist_primary_lines.csv")),
      "Code"
    )

    self._DEVICES = self._df_to_dict(
      pd.read_csv(os.path.join(dir, "dist_devices.csv")),
      "Code"
    )

    self._TRANSFORMERS = self._df_to_dict(
      pd.read_csv(os.path.join(dir, "dist_transformers.csv")),
      "Transformer_Id"
    )

    print("Creating line lookups...")
    self._build_line_lookups(self._LINES)
    print("Creating device lookups...")
    self._build_device_lookups(self._DEVICES)
    print("Creating transformer lookups...")
    self._build_transformer_lookups(self._TRANSFORMERS)

  def _get_circuits(self, node: str):
    items = []
    for _, circuit in self._CIRCUITS.items():
      if circuit.get("NodeA") == node or circuit.get("NodeB") == node:
        items.append(circuit)
    return items

  def _get_lines(self, this_line_id: str, node: str):
    ids = list(self._LINES_BY_NODEA.get(node, []))
    ids.extend(self._LINES_BY_NODEB.get(node, []))

    items = []
    for id in ids:
      if id not in self._TOUCHED_LINES and id != this_line_id:
        item = self._LINES.get(id)
        if item is not None:
          items.append(item)
    return items

  def _get_devices(self, node: str):
    ids = list(self._DEVICES_BY_NODEA.get(node, []))
    ids.extend(self._DEVICES_BY_NODEB.get(node, []))

    items = []
    for id in ids:
      item = self._DEVICES.get(id)
      if item is not None:
        items.append(item)
    return items

  def _get_transformers(self, node: str):
    ids = list(self._TRANSFORMERS_BY_NODE.get(node, []))

    items = []
    for id in ids:
      item = self._TRANSFORMERS.get(id)
      if item is not None:
        items.append(item)
    return items

  def _update_customer_counts(self, traversed: list[str], cc: int):
    for line_code in traversed:
      m = self._MODEL.get(line_code)
      if m:
        m.downstream_cc += cc

  def _trace(self, prms: TracePrms):
    this_line_id = prms.line.get("Code")
    self._TOUCHED_LINES.add(this_line_id)

    traversed = prms.traversed_lines
    this_node = prms.node
    next_node = prms.line.get("NodeB") if prms.node == prms.line.get("NodeA") else prms.line.get("NodeA")
    if not next_node:
      return

    entry = ConnectivityModel(
      source_id=prms.source_id,
      circuit_id=prms.circuit_id,
      segment_id=this_line_id,
      upstream_node=this_node,
      downstream_node=next_node,
      sus_device_id=prms.susaipid,
      mom_device_id=prms.momaipid,
      upstream_cc=0,
      downstream_cc=0,
      tree=prms.tree,
      seq=prms.seq
    )
    self._MODEL[this_line_id] = entry

    devices = self._get_devices(next_node)
    for device in devices:
      if device.get("LineCode") == this_line_id and device.get("state", 1) != 1:
        return

    transformers = self._get_transformers(next_node)
    for transformer in transformers:
      self._update_customer_counts(traversed, transformer.get("Customer_Count", 0))

    sus_aip = prms.susaipid
    mom_aip = prms.momaipid
    for device in devices:
      if device.get("LineCode") == this_line_id and device.get("sus_aip", False) and device.get("type", "CB") != "CB":
        sus_aip = device.get("Code")
      if device.get("LineCode") == this_line_id and device.get("mom_aip", False) and device.get("type", "CB") != "CB":
        mom_aip = device.get("Code")

    candidates = self._get_lines(this_line_id, next_node)

    idx = 0
    for candidate in candidates:
      tree = prms.tree
      seq = prms.seq
      circuit_id = prms.circuit_id

      if len(candidates) == 1:
        seq += 1
      else:
        tree = self._get_tree(tree, idx)
        seq = 1

      circuits = self._get_circuits(next_node)
      for circuit in circuits:
        if circuit.get("NodeB") == candidate.get("NodeB"):
          circuit_id = circuit.get("Code", circuit_id)
          sus_aip = circuit_id
          mom_aip = circuit_id
          tree = "1"
          seq = 1
          break

      next_traversed = traversed + [candidate.get("Code")]

      next_prms = TracePrms(
        traversed_lines=next_traversed,
        line=candidate,
        node=next_node,
        source_id=prms.source_id,
        circuit_id=circuit_id,
        susaipid=sus_aip,
        momaipid=mom_aip,
        tree=tree,
        seq=seq
      )
      self._trace(next_prms)
      idx += 1

  def _populate_upstream_cc(self):
    max_cc_by_circuit: dict[str, int] = {}

    p = 0
    t = len(self._CIRCUITS) * len(self._MODEL) + len(self._MODEL)
    print_progress(p, t)

    for circuit_id in self._CIRCUITS.keys():
      max_cc = 0

      for m in self._MODEL.values():
        if m.circuit_id == circuit_id and m.downstream_cc > max_cc:
          max_cc = m.downstream_cc
        p += 1

      max_cc_by_circuit[circuit_id] = max_cc
      print_progress(p, t)

    for m in self._MODEL.values():
      max_cc = max_cc_by_circuit.get(m.circuit_id, 0)
      m.upstream_cc = max(0, max_cc - m.downstream_cc)
      p += 1
      print_progress(p, t)

    print()

  def build(self, network: str, dir: str):
    network_dir = Path(os.path.join(dir, network))
    geojson_dir = network_dir / "geojson"

    self._prepare_data(geojson_dir)

    print("Generating model...")
    t = len(self._LINES)
    for id, src in self._SOURCES.items():
      node = src.get("NodeA", "")
      lines = self._get_lines("", node)
      for line in lines:
        prms = TracePrms(
          traversed_lines=[line.get("Code")],
          line=line,
          node=node,
          source_id=id,
          circuit_id="",
          susaipid="",
          momaipid="",
          tree="1",
          seq=1
        )
        self._trace(prms)
        print_progress(len(self._MODEL), t)
    print()

    print("Calculating upstream customer counts...")
    self._populate_upstream_cc()

    print("Saving the model...")
    model_path = geojson_dir / "dist_model.csv"
    model = pd.DataFrame([asdict(x) for x in self._MODEL.values()])
    model.to_csv(model_path, index=False)

    print("Creating GeoJSON file to inspect the model...")
    lines_path = geojson_dir / "dist_primary_lines.geojson"
    lines = gpd.read_file(lines_path, engine="pyogrio")
    merged = lines.merge(
      model,
      how="left",
      left_on="Code",
      right_on="segment_id"
    )
    model_geo_path = geojson_dir / "dist_model_geo.geojson"
    merged.to_file(model_geo_path, driver="GeoJSON", engine="pyogrio")
    print(f"Modeling complete! {model_geo_path}")

