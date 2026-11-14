
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import shutil


def calculate_sha256(file_path: Path) -> str:
    """Calculate the SHA-256 checksum of a file."""
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def ingest_to_bronze(source_dir: Path, bronze_dir: Path) -> dict:
    """Copy CSV files into a new, timestamped Bronze ingestion."""
    source_dir = Path(source_dir)
    bronze_dir = Path(bronze_dir)

    if not source_dir.is_dir():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    files = sorted(source_dir.glob("*.csv"))

    if not files:
        raise ValueError(f"No CSV files found in: {source_dir}")

    ingestion_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    ingestion_dir = bronze_dir / ingestion_id
    ingestion_dir.mkdir(parents=True, exist_ok=False)

    manifest = {
        "ingestion_id": ingestion_id,
        "source": str(source_dir),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "file_count": len(files),
        "files": [],
    }

    try:
        for source_file in files:
            destination = ingestion_dir / source_file.name
            shutil.copy2(source_file, destination)

            source_hash = calculate_sha256(source_file)
            destination_hash = calculate_sha256(destination)

            if source_hash != destination_hash:
                raise ValueError(
                    f"Integrity check failed for {source_file.name}"
                )

            manifest["files"].append({
                "name": source_file.name,
                "size_bytes": destination.stat().st_size,
                "sha256": destination_hash,
            })

        manifest_path = ingestion_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=4, ensure_ascii=False),
            encoding="utf-8",
        )

    except Exception:
        shutil.rmtree(ingestion_dir)
        raise

    return manifest
    