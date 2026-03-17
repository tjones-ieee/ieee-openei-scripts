from collections import defaultdict
import os
from pathlib import Path
import pandas as pd
import geopandas as gpd

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
  # ties_path = geojson_dir / "tie_lines.geojson"
  primary_path = geojson_dir / "primary_lines.geojson"
  secondary_path = geojson_dir / "secondary_lines.geojson"

  if os.path.exists(tran_path) or os.path.exists(primary_path) or os.path.exists(secondary_path):
    print("One or more line files already exist. Delete existing line geojsons to run.")
    return

  gdf = _get_merged_geojson(network, network_dir, "Line_N")

  phasev = gdf["PhasesV"].fillna("").astype(str)
  # subest = gdf["Subest"].fillna("True").astype(str)

  tran_gdf = gdf[phasev.str.contains("_HV", case=False, na=False)].copy()
  # ties_gdf = gdf[(phasev.str.contains("_MV", case=False, na=False)) & (subest.str.contains("True", case=False, na=False))].copy()
  # primary_gdf = gdf[(phasev.str.contains("_MV", case=False, na=False)) & (~subest.str.contains("True", case=False, na=False))].copy()
  primary_gdf = gdf[phasev.str.contains("_MV", case=False, na=False)].copy()
  secondary_gdf = gdf[phasev.str.contains("_LV", case=False, na=False)].copy()

  # create features...
  tran_gdf["network"] = network
  # ties_gdf["network"] = network
  primary_gdf["network"] = network
  secondary_gdf["network"] = network

  if not tran_gdf.empty:
    tran_gdf.to_file(tran_path, driver="GeoJSON", engine="pyogrio")
    tran_gdf.drop(columns="geometry").to_csv(str(tran_path).replace(".geojson", ".csv"), index=False)
    print(f"Saved: {tran_path} ({len(tran_gdf)})")
  else:
    print("No transmission lines found.")

  # if not ties_gdf.empty:
  #   ties_gdf.to_file(ties_path, driver="GeoJSON", engine="pyogrio")
  #   ties_gdf.drop(columns="geometry").to_csv(str(ties_path).replace(".geojson", ".csv"), index=False)
  #   print(f"Saved: {ties_path} ({len(ties_gdf)})")
  # else:
  #   print("No tie lines found.")

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

def merge_devices(network:str, dir:str):
  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"
  device_path = geojson_dir / "devices.geojson"

  if os.path.exists(device_path):
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
      gdf["Subest"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .eq("true")
    )
  ).astype(int)
  gdf["sus_aip"] = gdf["type"].isin(["CB", "FU"])
  gdf["mom_aip"] = gdf["type"].isin(["CB"])
  
  gdf.to_file(device_path, driver="GeoJSON", engine="pyogrio")
  gdf.drop(columns="geometry").to_csv(str(device_path).replace(".geojson", ".csv"), index=False)
  print(f"Saved: {device_path} ({len(gdf)})")

def merge_nodes(network:str, dir:str):
  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"

  primary_path = geojson_dir / "primary_nodes.geojson"
  secondary_path = geojson_dir / "secondary_nodes.geojson"

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
  out_path = geojson_dir / "transformers.geojson"

  if os.path.exists(out_path):
    print("Transformers already exist. Delete existing transformer geojson to run.")
    return

  gdf = _get_merged_geojson(network, network_dir, "DistribTransf_N")

  # OEDI is modeled through transformers into the secondary network
  # all customers are in "NewConsumerGreenfield_N"
  # transformer customer counts cannot be directly determined without a connectivity trace
  # note that transformers have a primary node, and then the secondary node just as "LV" to the end of it
  # then the secondary network model works just like the primary model
  
  gdf.to_file(out_path, driver="GeoJSON", engine="pyogrio")
  gdf.drop(columns="geometry").to_csv(str(out_path).replace(".geojson", ".csv"), index=False)
  print(f"Saved: {out_path} ({len(gdf)})")

def merge_customers(network:str, dir:str):
  network_dir = Path(os.path.join(dir, network))
  geojson_dir = network_dir / "geojson"
  out_path = geojson_dir / "customers.geojson"

  if os.path.exists(out_path):
    print("Customers already exist. Delete existing customers geojson to run.")
    return

  gdf = _get_merged_geojson(network, network_dir, "NewConsumerGreenfield_N")

  # See merge_transformers()
  # need to decide how I want to handle customer counts...
  
  gdf.to_file(out_path, driver="GeoJSON", engine="pyogrio")
  gdf.drop(columns="geometry").to_csv(str(out_path).replace(".geojson", ".csv"), index=False)
  print(f"Saved: {out_path} ({len(gdf)})")


