"""
Creates a network connectivity model for tracing and zones of protection.

While the OEDI data appears to be oriented from NodeA to NodeB, this model does not assume it.
"""
import os
from pathlib import Path
import pandas as pd
import geopandas as gpd
from dataclasses import dataclass, asdict

from tools.progress import print_progress

@dataclass
class ConnectivityModel:
  source_id:str
  circuit_id:str
  segment_id:str
  upstream_node:str
  downstream_node:str
  sus_device_id:str
  mom_device_id:str
  upstream_cc:int
  downstream_cc:int
  tree:str
  seq:int


_SOURCES = {}
_CIRCUITS = {}
_LINES = {}
_LINES_BY_NODEA = {}
_LINES_BY_NODEB = {}
_DEVICES = {}
_DEVICES_BY_NODEA = {}
_DEVICES_BY_NODEB = {}
_TRANSFORMERS = {}
_TRANSFORMERS_BY_NODE = {}

# prevent infinite loops
_TOUCHED_LINES = set()

_MODEL: dict[str, ConnectivityModel] = {}


tree_chars = "123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
def _get_tree(tree:str, idx:int):
  if idx >= len(tree_chars):
    super_idx = idx % len(tree_chars)
    return tree + tree_chars[super_idx] + tree_chars[super_idx]
  return tree + tree_chars[idx]

def _df_to_dict(df: pd.DataFrame, key_col: str) -> dict:
  return {
    row[key_col]: row
    for row in df.to_dict(orient="records")
  }

def _build_line_lookups(lines: dict):
  global _LINES_BY_NODEA
  global _LINES_BY_NODEB

  _LINES_BY_NODEA = {}
  _LINES_BY_NODEB = {}

  p = 0
  t = len(lines)
  print_progress(p,t)
  for code, item in lines.items():
    node_a = item.get("NodeA")
    node_b = item.get("NodeB")

    if pd.notna(node_a):
      node_a = str(node_a)
      _LINES_BY_NODEA.setdefault(node_a, []).append(code)

    if pd.notna(node_b):
      node_b = str(node_b)
      _LINES_BY_NODEB.setdefault(node_b, []).append(code)
    
    p += 1
    print_progress(p,t)
  print()
def _build_device_lookups(devices: dict) -> tuple[dict, dict]:
  global _DEVICES_BY_NODEA
  global _DEVICES_BY_NODEB

  _DEVICES_BY_NODEA = {}
  _DEVICES_BY_NODEB = {}

  p = 0
  t = len(devices)
  print_progress(p,t)
  for code, item in devices.items():
    node_a = item.get("NodeA")
    node_b = item.get("NodeB")

    if pd.notna(node_a):
      node_a = str(node_a)
      _DEVICES_BY_NODEA.setdefault(node_a, []).append(code)

    if pd.notna(node_b):
      node_b = str(node_b)
      _DEVICES_BY_NODEB.setdefault(node_b, []).append(code)
    
    p += 1
    print_progress(p,t)
  print()
def _build_transformer_lookups(transformers: dict) -> tuple[dict, dict]:
  global _TRANSFORMERS_BY_NODE

  _TRANSFORMERS_BY_NODE = {}
  p = 0
  t = len(transformers)
  print_progress(p,t)
  for code, item in transformers.items():
    node_a = item.get("Node")

    if pd.notna(node_a):
      node_a = str(node_a)
      _TRANSFORMERS_BY_NODE.setdefault(node_a, []).append(code)

    p += 1
    print_progress(p,t)
  print()

def _prepare_data(dir:str):
  global _SOURCES
  global _CIRCUITS
  global _LINES
  global _DEVICES
  global _TRANSFORMERS

  # load sources
  print("Loading sources, circuits, lines, devices, and transformers")
  _SOURCES = _df_to_dict(
    pd.read_csv(os.path.join(dir, "dist_sources.csv")),
    "Code"
  )

  _CIRCUITS = _df_to_dict(
    pd.read_csv(os.path.join(dir, "dist_circuits.csv")),
    "Code"
  )

  _LINES = _df_to_dict(
    pd.read_csv(os.path.join(dir, "dist_primary_lines.csv")),
    "Code"
  )

  _DEVICES = _df_to_dict(
    pd.read_csv(os.path.join(dir, "dist_devices.csv")),
    "Code"
  )

  _TRANSFORMERS = _df_to_dict(
    pd.read_csv(os.path.join(dir, "dist_transformers.csv")),
    "Transformer_Id"
  )

  # create lookups to quickly find lines and devices
  print("Creating line lookups...")
  _build_line_lookups(_LINES)
  print("Creating device lookups...")
  _build_device_lookups(_DEVICES)
  print("Creating transformer lookups...")
  _build_transformer_lookups(_TRANSFORMERS)


def _get_circuits(node:str):
  global _CIRCUITS

  items = []

  for _, circuit in _CIRCUITS.items():
    if circuit.get("NodeA") == node or circuit.get("NodeB") == node:
      items.append(circuit)

  return items
def _get_lines(this_line_id:str, node:str):
  global _LINES
  global _LINES_BY_NODEA
  global _LINES_BY_NODEB
  global _TOUCHED_LINES

  ids = list(_LINES_BY_NODEA.get(node, []))
  ids.extend(_LINES_BY_NODEB.get(node, []))

  items = []
  for id in ids:
    if id not in _TOUCHED_LINES and id != this_line_id:
      item = _LINES.get(id)
      if item is not None:
        items.append(item)
  return items

def _get_devices(node:str):
  global _DEVICES
  global _DEVICES_BY_NODEA
  global _DEVICES_BY_NODEB

  ids = list(_DEVICES_BY_NODEA.get(node, []))
  ids.extend(_DEVICES_BY_NODEB.get(node, []))

  items = []
  for id in ids:
    item = _DEVICES.get(id)
    if item is not None:
      items.append(item)
  return items

def _get_transformers(node:str):
  global _TRANSFORMERS
  global _TRANSFORMERS_BY_NODE
  
  ids = list(_TRANSFORMERS_BY_NODE.get(node, []))

  items = []
  for id in ids:
    item = _TRANSFORMERS.get(id)
    if item is not None:
      items.append(item)
  return items

@dataclass
class TracePrms:
  traversed_lines:list[str]
  line:dict
  node:str
  source_id:str
  circuit_id:str
  susaipid:str
  momaipid:str
  tree:str
  seq:int


def _update_customer_counts(traversed:list[str], cc:int):
  global _MODEL
  for line_code in traversed:
    m = _MODEL.get(line_code)
    if m:
      m.downstream_cc += cc


def _trace(prms:TracePrms):
  global _TOUCHED_LINES
  global _MODEL

  this_line_id = prms.line.get("Code")
  _TOUCHED_LINES.add(this_line_id)

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
  _MODEL[this_line_id] = entry
  
  # any devices on downstream node, check their state
  devices = _get_devices(next_node)
  for device in devices:
    if device.get("LineCode") == this_line_id and device.get("state", 1) != 1:
      return

  # any transformers on the line?
  # always assumed to be after a device on the node
  transformers = _get_transformers(next_node)
  for transformer in transformers:
    _update_customer_counts(traversed, transformer.get("Customer_Count", 0))
  
  # check for sus/mom aip (not circuit breakers)
  sus_aip = prms.susaipid
  mom_aip = prms.momaipid
  for device in devices:
    if device.get("LineCode") == this_line_id and device.get("sus_aip", False) and device.get("type", "CB") != "CB":
      sus_aip = device.get("Code")
    if device.get("LineCode") == this_line_id and device.get("mom_aip", False) and device.get("type", "CB") != "CB":
      mom_aip = device.get("Code")
  
  candidates = _get_lines(this_line_id, next_node)

  idx = 0
  for candidate in candidates:
    tree = prms.tree
    seq = prms.seq
    circuit_id = prms.circuit_id
    if len(candidates) == 1:
      seq += 1
    else:
      tree = _get_tree(tree, idx)
      seq = 1

    # have we reached a circuit?
    circuits = _get_circuits(next_node)
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
    _trace(next_prms)
  
    idx += 1

def create_model(network:str, dir:str):
  global _SOURCES
  global _LINES
  global _MODEL

  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"
  _prepare_data(geojson_dir)

  print("Generating model...")
  t = len(_LINES)
  for id, src in _SOURCES.items():
    node = src.get("NodeA", "")
    lines = _get_lines("", node)
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
      
      _trace(prms)
      print_progress(len(_MODEL), t)
  print()

  # calculate upstream customer counts...


  print("Saving the model...")
  model_path = geojson_dir / "dist_model.csv"
  model = pd.DataFrame([asdict(x) for x in _MODEL.values()])
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
  model_path = geojson_dir / "dist_model_geo.geojson"
  merged.to_file(model_path, driver="GeoJSON", engine="pyogrio")
  print(f"Modeling complete! {model_path}")

