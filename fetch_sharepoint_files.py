#!/usr/bin/env python3
"""
Stage 0 – Fetch Excel files from SharePoint into Inputs/.

Downloads .xlsx files from the COS SharePoint document library into the local
``Inputs/Deskcount/`` and ``Inputs/Occupancy/`` folders so the rest of the
pipeline can process them.

Standalone usage:
    python fetch_sharepoint_files.py [--library "Shared Documents"]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

from sharepoint_client import SharePointClient, SharePointConfig

# SharePoint folder paths that map to local Inputs/ subfolders
FOLDER_MAP = {
    "Deskcount": "General/Occupancy Data/Dashboard/Inputs/Deskcount",
    "Occupancy": "General/Occupancy Data/Dashboard/Inputs/Occupancy",
}


def log(msg: str) -> None:
    print(f"[fetch] {msg}")


def fetch_sharepoint_files(library_name: str | None = None) -> None:
    """Download .xlsx files from SharePoint into Inputs/."""
    kwargs = {}
    if library_name is not None:
        kwargs["library_name"] = library_name

    config = SharePointConfig.from_secrets(**kwargs)
    client = SharePointClient(config)

    total_downloaded = 0

    for local_folder, sp_folder in FOLDER_MAP.items():
        dest_dir = Path("Inputs") / local_folder
        dest_dir.mkdir(parents=True, exist_ok=True)

        log(f"Listing SharePoint: {sp_folder}")
        try:
            items = client.list_folder(sp_folder)
        except Exception as exc:
            log(f"  Warning: could not list '{sp_folder}': {exc}")
            items = []

        if not items:
            log(f"  No .xlsx files found in '{sp_folder}'")
            continue

        for item in items:
            dest_path = dest_dir / item["name"]

            # Skip if local file exists and matches remote size
            if dest_path.exists() and dest_path.stat().st_size == item["size"]:
                log(f"  Skip (unchanged): {item['name']}")
                continue

            log(f"  Downloading: {item['name']} ({item['size']:,} bytes)")
            client.download_file(item["id"], dest_path)
            total_downloaded += 1

    if total_downloaded == 0:
        # Check if we at least have existing files locally
        existing = list(Path("Inputs").glob("**/*.xlsx"))
        if not existing:
            raise RuntimeError(
                "No .xlsx files downloaded and none found locally in Inputs/. "
                "Check SharePoint folder paths and credentials."
            )
        log("No new files downloaded; using existing local files.")
    else:
        log(f"Downloaded {total_downloaded} file(s) from SharePoint.")


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def _parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Excel files from SharePoint")
    parser.add_argument(
        "--library",
        default=None,
        help='SharePoint document library name (default: "Documents")',
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args(sys.argv[1:])
    fetch_sharepoint_files(library_name=args.library)
