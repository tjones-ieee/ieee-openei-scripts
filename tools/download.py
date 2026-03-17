from __future__ import annotations

from pathlib import Path
import boto3
from botocore import UNSIGNED
from botocore.config import Config


def _get_s3_client(region: str = "us-west-2"):
  return boto3.client(
    "s3",
    region_name=region,
    config=Config(signature_version=UNSIGNED),
  )


def _iter_keys(s3, bucket: str, prefix: str):
  paginator = s3.get_paginator("list_objects_v2")
  for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
      key = obj["Key"]
      if not key.endswith("/"):
        yield key


def _should_download(key: str, exts: list[str] | None) -> bool:
  if not exts:
    return True
  key_lower = key.lower()
  return any(key_lower.endswith(ext.lower()) for ext in exts)


def download(
  bucket: str,
  prefix: str,
  outdir: Path,
  region: str = "us-west-2",
  exts: list[str] | None = None,
  dry_run: bool = False,
):
  s3 = _get_s3_client(region=region)
  outdir.mkdir(parents=True, exist_ok=True)

  matched = 0
  downloaded = 0
  skipped = 0

  for key in _iter_keys(s3, bucket=bucket, prefix=prefix):
    if not _should_download(key, exts):
      continue

    matched += 1

    rel_path = key[len(prefix):] if key.startswith(prefix) else Path(key).name
    local_path = outdir / rel_path
    local_path.parent.mkdir(parents=True, exist_ok=True)

    if local_path.exists():
      print(f"Skipping existing: {local_path}")
      skipped += 1
      continue

    if dry_run:
      print(f"[DRY RUN] {key} -> {local_path}")
    else:
      print(f"Downloading: {key}")
      s3.download_file(bucket, key, str(local_path))
      downloaded += 1

  if dry_run:
    print(f"Done. {matched} file(s) matched.")
  else:
    print(f"Done. matched={matched}, downloaded={downloaded}, skipped={skipped}")
