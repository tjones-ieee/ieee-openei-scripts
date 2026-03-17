import os
from pathlib import Path
import geopandas as gpd

_CRS_MAP = {
  "AUS": "EPSG:32614",
  "GSO": "EPSG:32617",
  "SFO": "EPSG:32610"
}

_IGNORE_NAME_PARTS = ["streetmap"]

def convert_to_geojson(network:str, dir:str):
  network_dir = Path(os.path.join(dir, network))
  outdir = Path(os.path.join(network_dir, "geojson"))
  os.mkdir(outdir)

  if not network_dir.exists():
    print(f"Skipping missing network dir: {network_dir}")
    return

  for folder_dir in network_dir.iterdir():
    if not folder_dir.is_dir():
      continue

    for shp_path in folder_dir.glob("*.shp"):
      geojson_path = Path(os.path.join(outdir,f"{folder_dir.stem}-{shp_path.stem}.geojson"))

      shp_name = shp_path.name.lower()
      if any(part.lower() in shp_name for part in _IGNORE_NAME_PARTS):
        print(f"Skipping ignored shapefile: {shp_path.name}")
        continue

      if geojson_path.exists():
        print(f"Skipping existing: {geojson_path}")
        continue

      try:
        print(f"Converting: {shp_path}")

        gdf = gpd.read_file(shp_path)

        # source CRS is missing from the shapefiles
        gdf = gdf.set_crs(_CRS_MAP[network], allow_override=True)

        # reproject to WGS84 for GeoJSON
        gdf = gdf.to_crs("EPSG:4326")

        gdf.to_file(geojson_path, driver="GeoJSON")
        print(f"Saved: {geojson_path}")

      except Exception as ex:
        print(f"Failed: {shp_path} -> {ex}")

