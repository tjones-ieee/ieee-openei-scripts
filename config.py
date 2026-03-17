import os
from pathlib import Path
from dataclasses import dataclass

@dataclass
class Config:
  VERSION: tuple = (1, 0, 0)

  OUTPUT_DIRECTORY: Path = None


config: Config = Config(
  OUTPUT_DIRECTORY = Path(os.getenv("OUTPUT_DIRECTORY")).resolve(),
)

# VERIFY THE CONFIG
assert config.OUTPUT_DIRECTORY.exists(), "Output directory does not exist"
assert config.OUTPUT_DIRECTORY.is_dir(), "Output path is not a directory"
assert os.access(config.OUTPUT_DIRECTORY, os.W_OK), "Output directory is not writable"
