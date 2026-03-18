# MUST LOAD THE ENVIRONMENT VARIABLES IMMEDIATELY
from pathlib import Path
from dotenv import load_dotenv
ROOT = Path(__file__).resolve().parent
# load base defaults (not sensitive, committed)
load_dotenv(ROOT / ".env")
# dev overrides (ignored by git)
load_dotenv(ROOT / ".env.local", override=True)

import os, sys
from datetime import datetime
from dataclasses import dataclass, field, asdict

# settings and orchestration
from config import config
from tools.download import download
import tools.geojson as geo
from tools.model import create_model

def download_all():
  for network in ["AUS", "GSO", "SFO"]:
    prefix = f"SMART-DS/v1.0/GIS/{network}/"
    outdir = Path(os.path.join(config.OUTPUT_DIRECTORY, network))
    download(
      bucket="oedi-data-lake",
      prefix=prefix,
      outdir=outdir,
      region="us-west-2",
      # exts=[],
      # dry_run=True
    )

def convert_all():
  # for network in ["AUS", "GSO", "SFO"]:
  #   geo.merge_split_lines(network, dir=config.OUTPUT_DIRECTORY)
  #   geo.merge_devices(network, dir=config.OUTPUT_DIRECTORY)

  # must do nodes and lines first for devices
  # geo.merge_nodes("AUS", dir=config.OUTPUT_DIRECTORY)
  # geo.merge_split_lines("AUS", dir=config.OUTPUT_DIRECTORY)
  # geo.merge_devices("AUS", dir=config.OUTPUT_DIRECTORY)
  # geo.merge_substations("AUS", dir=config.OUTPUT_DIRECTORY)
  # geo.merge_transformers("AUS", dir=config.OUTPUT_DIRECTORY)
  # geo.merge_customers("AUS", dir=config.OUTPUT_DIRECTORY)
  # geo.create_sources("AUS", dir=config.OUTPUT_DIRECTORY) # requires lines and devices
  # geo.create_circuits("AUS", dir=config.OUTPUT_DIRECTORY) # requires devices

  create_model("AUS", dir=config.OUTPUT_DIRECTORY)

if __name__ == "__main__":
  # for best results, remove everything in config.OUTPUT_DIRECTORY first
  # once downloaded, you can comment download_all()
  # and then delete everything in /geojson thereafter

  # download_all() # download files
  convert_all() # convert to GeoJSON


# dumby equip === nodes
  # filter where not ending in LV
# lines always oriented NodeA -> NodeB
  # when subest == True -> state = 0
# devices always modeled as NodeA -> NodeB regardless of lat/lon
  # always align as device.NodeA = line.NodeA and device.NodeB = line.NodeB
  # device state derived from lines where subest == True
  # devices must be aggregated by type
    # phasing based on associated line
  # device point() derived from nodes node.Node = line.NodeA = device.NodeA


# ['Breaker', 'ElbSwitch', 'Fuse', 'PadSwitch']
# ['Breaker', 'DisSwitch', 'ElbSwitch', 'Fuse', 'GOAB_DisSwitch', 'PadSwitch']
# ['Breaker', 'DisSwitch', 'ElbSwitch', 'Fuse', 'GOAB_DisSwitch', 'PadSwitch']

# CB = Breaker
# SW = ElbSwitch, DisSwitch, GOAB_DisSwitch, PadSwitch
# FU = Fuse
# sus = CB, FU
# mom = CB



# for all other devices:
  # join devices to lines on NodeA and NodeB
  # line.Code is for segment id
