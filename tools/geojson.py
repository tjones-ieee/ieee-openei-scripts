from collections import defaultdict
import os
from pathlib import Path
import pandas as pd
import geopandas as gpd
from uuid import uuid4
from dataclasses import asdict
from tools.customer_gen import create_customers
from tools.spatial import offset_point
from tools.progress import print_progress

_CRS_MAP = {
  "AUS": "EPSG:32614",
  "GSO": "EPSG:32617",
  "SFO": "EPSG:32610",
}

_IGNORE_NAME_PARTS = ["streetmap"]


def _get_merged_geojson(network:str, network_dir:str, file_name:str) -> gpd.GeoDataFrame | None:
  grouped: dict[str, list[Path]] = defaultdict(list)

  # group shapefiles by filename across all subfolders
  for folder_dir in network_dir.iterdir():
    if not folder_dir.is_dir() or folder_dir.name.lower() == "geojson":
      continue

    for shp_path in folder_dir.glob("*.shp"):
      shp_name = str(shp_path.name.lower())

      if any(part in shp_name for part in _IGNORE_NAME_PARTS):
        continue

      if not shp_name.startswith(file_name.lower()):
        continue

      grouped[shp_path.name].append(shp_path)

  # merge each shapefile group into one output geojson
  for shp_name, shp_paths in grouped.items():
    try:
      frames: list[gpd.GeoDataFrame] = []

      for shp_path in shp_paths:
        print(f"Loading: {shp_path}")

        gdf = gpd.read_file(shp_path, engine="pyogrio")
        gdf = gdf.set_crs(_CRS_MAP[network], allow_override=True)
        gdf = gdf.to_crs("EPSG:4326")

        # optional lineage fields
        gdf["network"] = network
        gdf["source_folder"] = shp_path.parent.name
        gdf["source_file"] = shp_path.name

        frames.append(gdf)

      if not frames:
        continue

      merged = gpd.GeoDataFrame(
        pd.concat(frames, ignore_index=True),
        crs="EPSG:4326",
      )

      return merged
    except Exception as ex:
      print(f"Failed: {shp_name} -> {ex}")

  return None


def merge_split_lines(network:str, dir:str):
  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"

  tran_path = geojson_dir / "tran_lines.geojson"
  primary_path = geojson_dir / "dist_primary_lines.geojson"
  secondary_path = geojson_dir / "dist_secondary_lines.geojson"

  if os.path.exists(tran_path) or os.path.exists(primary_path) or os.path.exists(secondary_path):
    print("One or more line files already exist. Delete existing line geojsons to run.")
    return

  gdf = _get_merged_geojson(network, network_dir, "Line_N")

  # create OH/UG flags
  equip = gdf["Equip"].fillna("").astype(str).str.upper()
  gdf["OH"] = equip.str.contains("_OH_", na=False)
  gdf["UG"] = equip.str.contains("_UG_", na=False)

  # split into transmission, primary distribution, and secondary service
  phasev = gdf["PhasesV"].fillna("").astype(str)
  tran_gdf = gdf[phasev.str.contains("_HV", case=False, na=False)].copy()
  primary_gdf = gdf[phasev.str.contains("_MV", case=False, na=False)].copy()
  secondary_gdf = gdf[phasev.str.contains("_LV", case=False, na=False)].copy()

  # create features...
  tran_gdf["network"] = network
  primary_gdf["network"] = network
  secondary_gdf["network"] = network

  if not tran_gdf.empty:
    tran_gdf.to_file(tran_path, driver="GeoJSON", engine="pyogrio")
    tran_gdf.drop(columns="geometry").to_csv(str(tran_path).replace(".geojson", ".csv"), index=False)
    print(f"Saved: {tran_path} ({len(tran_gdf)})")
  else:
    print("No transmission lines found.")

  if not primary_gdf.empty:
    primary_gdf.to_file(primary_path, driver="GeoJSON", engine="pyogrio")
    primary_gdf.drop(columns="geometry").to_csv(str(primary_path).replace(".geojson", ".csv"), index=False)
    print(f"Saved: {primary_path} ({len(primary_gdf)})")
  else:
    print("No primary lines found.")

  if not secondary_gdf.empty:
    secondary_gdf.to_file(secondary_path, driver="GeoJSON", engine="pyogrio")
    secondary_gdf.drop(columns="geometry").to_csv(str(secondary_path).replace(".geojson", ".csv"), index=False)
    print(f"Saved: {secondary_path} ({len(secondary_gdf)})")
  else:
    print("No secondary lines found.")

  return

def _extract_line_code(device_code: str) -> str | None:
  if not device_code:
    return None

  start = device_code.find('(')
  end = device_code.find(')', start + 1)

  if start == -1 or end == -1 or end <= start + 1:
    return None

  inner = device_code[start + 1:end].strip()
  if not inner:
    return None

  return f"L({inner})"
def merge_devices(network:str, dir:str):
  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"

  tran_path = geojson_dir / "tran_devices.geojson"
  dist_path = geojson_dir / "dist_devices.geojson"

  if os.path.exists(tran_path) or os.path.exists(dist_path):
    print("Devices already exist. Delete existing device geojson to run.")
    return

  gdf = _get_merged_geojson(network, network_dir, "SwitchingDevices_N")

  # print all distinct prefixes used for device typing
  # distinct_prefixes = sorted(
  #   gdf["Code"]
  #     .fillna("")
  #     .astype(str)
  #     .str.extract(r"^([^()]+)", expand=False)
  #     .dropna()
  #     .str.strip()
  #     .unique()
  # )
  # print(distinct_prefixes)
  # AUS = ['Breaker', 'ElbSwitch', 'Fuse', 'PadSwitch']
  # GSO = ['Breaker', 'DisSwitch', 'ElbSwitch', 'Fuse', 'GOAB_DisSwitch', 'PadSwitch']
  # SFO = ['Breaker', 'DisSwitch', 'ElbSwitch', 'Fuse', 'GOAB_DisSwitch', 'PadSwitch']

  # add features for type and sus/mom auto-isolation point flags
  gdf["type"] = (
    gdf["Code"]
      .fillna("")
      .astype(str)
      .str.extract(r"^([^()]+)", expand=False)
      .str.strip()
  )
  gdf["type"] = gdf["type"].replace({
    "Breaker": "CB",
    "Fuse": "FU",
    "ElbSwitch": "SW",
    "DisSwitch": "SW",
    "GOAB_DisSwitch": "SW",
    "PadSwitch": "SW",
  })
  gdf["state"] = (
    (gdf["type"] == "CB") |
    (
      ~gdf["Subest"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .eq("true")
    )
  ).astype(int)
  gdf["sus_aip"] = gdf["type"].isin(["CB", "FU"])
  gdf["mom_aip"] = gdf["type"].isin(["CB"])

  # append segment id's
  # the segment id is actually encoded within Code
  # line is "L(R:P3UDT12996-P3UDT14668)" and device is PadSwitch(R:P3UDT12996-P3UDT14668)P3U_174613
  gdf["LineCode"] = gdf["Code"].apply(_extract_line_code)

  # split into T&D
  nomv = gdf["NomV_kV"].fillna("").astype(str)
  tran_gdf = gdf[~nomv.str.contains("12.47", case=False, na=False)].copy()
  dist_gdf = gdf[nomv.str.contains("12.47", case=False, na=False)].copy()

  if not tran_gdf.empty:
    tran_gdf.to_file(tran_path, driver="GeoJSON", engine="pyogrio")
    tran_gdf.drop(columns="geometry").to_csv(str(tran_path).replace(".geojson", ".csv"), index=False)
    print(f"Saved: {tran_path} ({len(tran_gdf)})")
  else:
    print("No transmission devices found.")

  if not dist_gdf.empty:
    dist_gdf.to_file(dist_path, driver="GeoJSON", engine="pyogrio")
    dist_gdf.drop(columns="geometry").to_csv(str(dist_path).replace(".geojson", ".csv"), index=False)
    print(f"Saved: {dist_path} ({len(dist_gdf)})")
  else:
    print("No distribution devices found.")

def merge_nodes(network:str, dir:str):
  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"

  primary_path = geojson_dir / "dist_primary_nodes.geojson"
  secondary_path = geojson_dir / "dist_secondary_nodes.geojson"

  if os.path.exists(primary_path) or os.path.exists(secondary_path):
    print("One or more node files already exist. Delete existing node geojsons to run.")
    return

  gdf = _get_merged_geojson(network, network_dir, "DummyEquip")

  node = gdf["Node"].fillna("").astype(str).str.strip()

  primary_gdf = gdf[~node.str.endswith("LV", na=False)].copy()
  secondary_gdf = gdf[node.str.endswith("LV", na=False)].copy()

  if not primary_gdf.empty:
    primary_gdf.to_file(primary_path, driver="GeoJSON", engine="pyogrio")
    primary_gdf.drop(columns="geometry").to_csv(str(primary_path).replace(".geojson", ".csv"), index=False)
    print(f"Saved: {primary_path} ({len(primary_gdf)})")
  else:
    print("No primary nodes found.")

  if not secondary_gdf.empty:
    secondary_gdf.to_file(secondary_path, driver="GeoJSON", engine="pyogrio")
    secondary_gdf.drop(columns="geometry").to_csv(str(secondary_path).replace(".geojson", ".csv"), index=False)
    print(f"Saved: {secondary_path} ({len(secondary_gdf)})")
  else:
    print("No secondary nodes found.")

def merge_substations(network:str, dir:str):
  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"
  tran_path = geojson_dir / "tran_substations.geojson"
  dist_path = geojson_dir / "dist_substations.geojson"

  if os.path.exists(tran_path) or os.path.exists(dist_path):
    print("Substations already exist. Delete existing substation geojson to run.")
    return

  # transmission
  gdf = _get_merged_geojson(network, network_dir, "TransSubstation_N")
  gdf.to_file(tran_path, driver="GeoJSON", engine="pyogrio")
  gdf.drop(columns="geometry").to_csv(str(tran_path).replace(".geojson", ".csv"), index=False)
  print(f"Saved: {tran_path} ({len(gdf)})")
  
  # distribution
  gdf = _get_merged_geojson(network, network_dir, "HVMVSubstation_N")
  gdf.to_file(dist_path, driver="GeoJSON", engine="pyogrio")
  gdf.drop(columns="geometry").to_csv(str(dist_path).replace(".geojson", ".csv"), index=False)
  print(f"Saved: {dist_path} ({len(gdf)})")

def merge_transformers(network:str, dir:str):
  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"
  out_path = geojson_dir / "dist_transformers.geojson"
  customer_path = geojson_dir / "dist_customers.geojson"

  if os.path.exists(out_path):
    print("Transformers already exist. Delete existing transformer geojson to run.")
    return

  gdf = _get_merged_geojson(network, network_dir, "DistribTransf_N")

  # add unique identifier to each transformer
  print("Creating unique identifiers for each transformer.")
  gdf["Transformer_Id"] = [str(uuid4()) for _ in range(len(gdf))]


  # OEDI is modeled through transformers into the secondary network
  # all customers are in "NewConsumerGreenfield_N"
  # transformer customer counts cannot be directly determined without a connectivity trace
  # note that transformers have a primary node, and then the secondary node just as "LV" to the end of it
  # then the secondary network model works just like the primary model
  # However, for now, I will just create random customers based on the size of the transformer...
  rows: list[dict] = []
  print("Creating customers for each transformer.")
  total_xfmrs = len(gdf)
  processed = 0
  print_progress(processed, total_xfmrs)
  for _, transformer in gdf.iterrows():
    id = transformer["Transformer_Id"]
    kva = transformer["Size_kVA"]
    geom = transformer.geometry

    custs = create_customers(id, float(kva or 0.0))

    transformer["Customer_Count"] = len(custs)

    for cust in custs:
      row = asdict(cust)
      row["geometry"] = offset_point(geom, 0.00005) # so customers are around the transformer...
      rows.append(row)

    processed += 1
    print_progress(processed, total_xfmrs)
  print() # clears the carriage from print progress...

  # create/save the customer geodataframe
  customers = gpd.GeoDataFrame(rows, geometry="geometry", crs=gdf.crs)
  customers.to_file(customer_path, driver="GeoJSON", engine="pyogrio")
  customers.drop(columns="geometry").to_csv(str(customer_path).replace(".geojson", ".csv"), index=False)
  print(f"Saved: {customer_path} ({len(customers)})")

  # save the transformers
  gdf.to_file(out_path, driver="GeoJSON", engine="pyogrio")
  gdf.drop(columns="geometry").to_csv(str(out_path).replace(".geojson", ".csv"), index=False)
  print(f"Saved: {out_path} ({len(gdf)})")

def merge_customers(network:str, dir:str):
  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"
  out_path = geojson_dir / "customers_raw.geojson"

  if os.path.exists(out_path):
    print("Customers already exist. Delete existing customers geojson to run.")
    return

  gdf = _get_merged_geojson(network, network_dir, "NewConsumerGreenfield_N")

  # See merge_transformers()
  # need to decide how I want to handle customer counts...
  
  gdf.to_file(out_path, driver="GeoJSON", engine="pyogrio")
  gdf.drop(columns="geometry").to_csv(str(out_path).replace(".geojson", ".csv"), index=False)
  print(f"Saved: {out_path} ({len(gdf)})")

def create_sources(network:str, dir:str):
  """
  for "sources":
    within devices, where type = CB and subest = True and NomV_kV = 12.47
    have to create a fake line because NodeA for these devices does not exist in lines
    but it's easy because the device itself is a "line string"
      Code=source.Code
      NodeA=source.NodeA
      NodeB=source.NodeB
      NomV=12.47
      Len=0.007
      Equip=3P_OH_AL_ACSR_1033kcmil_Curlew_12.47_1
      R=0.00111
      X=0.00684
      C=0.24259
      R0=0.00283
      X0=0.0175
      C0=0.0175
      Imax=960
      Status=1
      Phases=ABC
      PhasesV=ABC_MV
      PhasesVI=3_MV_960
      LineCode=source.LineCode
  """
  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"

  devices_path = geojson_dir / "dist_devices.geojson"
  lines_path = geojson_dir / "dist_primary_lines.geojson"
  sources_path = geojson_dir / "dist_sources.geojson"

  if os.path.exists(sources_path):
    print("Sources already exist. Delete existing sources geojson to run.")
    return

  devices = gpd.read_file(devices_path, engine="pyogrio")
  lines = gpd.read_file(lines_path, engine="pyogrio")

  subest = (
    devices["Subest"]
      .fillna("")
      .astype(str)
      .str.strip()
      .str.lower()
  )
  nomv = (
    devices["NomV_kV"]
      .fillna("")
      .astype(str)
      .str.strip()
  )
  dtype = (
    devices["type"]
      .fillna("")
      .astype(str)
      .str.strip()
      .str.upper()
  )

  sources = devices[
    (dtype.eq("CB")) &
    (subest.isin(["true"])) &
    (nomv.eq("12.47"))
  ].copy()

  if sources.empty:
    print(f"No sources found for {network}")
    return

  # set the line code and create the "fake" line segments
  sources["LineCode"] = sources["Code"]
  source_lines = gpd.GeoDataFrame({
    "Code": sources["Code"],
    "NodeA": sources["NodeA"],
    "NodeB": sources["NodeB"],
    "NomV": 12.47,
    "Len": 0.007,
    "Equip": "3P_OH_AL_ACSR_1033kcmil_Curlew_12.47_1",
    "R": 0.00111,
    "X": 0.00684,
    "C": 0.24259,
    "R0": 0.00283,
    "X0": 0.0175,
    "C0": 0.0175,
    "Imax": 960,
    "Status": 1,
    "Phases": "ABC",
    "PhasesV": "ABC_MV",
    "PhasesVI": "3_MV_960",
    "Subest": True,
    "Feeder": True,
    "network": sources["network"],
    "source_folder": sources["source_folder"],
    "source_file": sources["source_file"],
    "OH": True,
    "UG": False,
    "geometry": sources.geometry,
  }, geometry="geometry", crs=devices.crs)

  merged = gpd.GeoDataFrame(
    pd.concat([lines, source_lines], ignore_index=True),
    geometry="geometry",
    crs=lines.crs,
  )

  # save updated lines
  merged.to_file(lines_path, driver="GeoJSON", engine="pyogrio")
  merged.drop(columns="geometry").to_csv(str(lines_path).replace(".geojson", ".csv"), index=False)
  print(f"Appended {len(source_lines)} source line(s) to {lines_path}")

  # save the sources
  sources.to_file(sources_path, driver="GeoJSON", engine="pyogrio")
  sources.drop(columns="geometry").to_csv(str(sources_path).replace(".geojson", ".csv"), index=False)
  print(f"Saved: {sources_path} ({len(sources)})")

def create_circuits(network:str, dir:str):
  """
  just a subset of devices, but all of the breakers
  """
  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"

  devices_path = geojson_dir / "dist_devices.geojson"
  circuits_path = geojson_dir / "dist_circuits.geojson"

  if os.path.exists(circuits_path):
    print("Circuits already exist. Delete existing circuits geojson to run.")
    return

  devices = gpd.read_file(devices_path, engine="pyogrio")

  subest = (
    devices["Subest"]
      .fillna("")
      .astype(str)
      .str.strip()
      .str.lower()
  )
  dtype = (
    devices["type"]
      .fillna("")
      .astype(str)
      .str.strip()
      .str.upper()
  )

  circuits = devices[
    (dtype.eq("CB")) &
    (~subest.isin(["true"]))
  ].copy()

  if circuits.empty:
    print(f"No circuits found for {network}")
    return

  # save the circuits
  circuits.to_file(circuits_path, driver="GeoJSON", engine="pyogrio")
  circuits.drop(columns="geometry").to_csv(str(circuits_path).replace(".geojson", ".csv"), index=False)
  print(f"Saved: {circuits_path} ({len(circuits)})")

