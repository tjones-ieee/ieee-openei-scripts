# MUST LOAD THE ENVIRONMENT VARIABLES IMMEDIATELY
from pathlib import Path
from dotenv import load_dotenv
ROOT = Path(__file__).resolve().parent
# load base defaults (not sensitive, committed)
load_dotenv(ROOT / ".env")
# dev overrides (ignored by git)
load_dotenv(ROOT / ".env.local", override=True)

import os, sys, argparse
from datetime import datetime
from dataclasses import dataclass, field, asdict

# settings and orchestration
from config import config
from tools.download import download

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


if __name__ == "__main__":
  # download files
  download_all()
  
