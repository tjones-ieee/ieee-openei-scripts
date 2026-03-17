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


if __name__ == "__main__":
  print("hello world")
  asdf = 1
