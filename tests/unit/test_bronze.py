
import hashlib
import json

from src.ingestion.bronze import calculate_sha256, ingest_to_bronze


def test_calculate_sha256(tmp_path):
    file_path = tmp_path / "sample.txt"
    file_path.write_text("SmartBank Analytics", encoding="utf-8")

    expected = hashlib.sha256(
        b"SmartBank Analytics"
    ).hexdigest()

    assert calculate_sha256(file_path) == expected


def test_ingest_to_bronze(tmp_path):
    source = tmp_path / "raw"
    bronze = tmp_path / "bronze"
    source.mkdir()

    (source / "accounts.csv").write_text(
        "id,name\n1,Test",
        encoding="utf-8",
    )
    (source / "transactions.csv").write_text(
        "id,amount\n1,100",
        encoding="utf-8",
    )

    manifest = ingest_to_bronze(source, bronze)

    ingestion_dir = bronze / manifest["ingestion_id"]

    assert manifest["file_count"] == 2
    assert (ingestion_dir / "accounts.csv").exists()
    assert (ingestion_dir / "transactions.csv").exists()
    assert (ingestion_dir / "manifest.json").exists()

    saved_manifest = json.loads(
        (ingestion_dir / "manifest.json").read_text(encoding="utf-8")
    )
    assert saved_manifest["file_count"] == 2


def test_ingestion_preserves_history(tmp_path):
    source = tmp_path / "raw"
    bronze = tmp_path / "bronze"
    source.mkdir()

    (source / "accounts.csv").write_text(
        "id\n1",
        encoding="utf-8",
    )

    first = ingest_to_bronze(source, bronze)
    second = ingest_to_bronze(source, bronze)

    assert first["ingestion_id"] != second["ingestion_id"]
    assert (bronze / first["ingestion_id"]).exists()
    assert (bronze / second["ingestion_id"]).exists()



import pytest


def test_missing_source_directory(tmp_path):
    source = tmp_path / "missing"
    bronze = tmp_path / "bronze"

    with pytest.raises(FileNotFoundError):
        ingest_to_bronze(source, bronze)


def test_empty_source_directory(tmp_path):
    source = tmp_path / "raw"
    bronze = tmp_path / "bronze"
    source.mkdir()

    with pytest.raises(ValueError, match="No CSV files found"):
        ingest_to_bronze(source, bronze)


def test_failed_ingestion_removes_incomplete_version(tmp_path, monkeypatch):
    source = tmp_path / "raw"
    bronze = tmp_path / "bronze"
    source.mkdir()

    (source / "accounts.csv").write_text("id\n1", encoding="utf-8")

    def failing_copy(*args, **kwargs):
        raise OSError("Simulated copy failure")

    monkeypatch.setattr("src.ingestion.bronze.shutil.copy2", failing_copy)

    with pytest.raises(OSError, match="Simulated copy failure"):
        ingest_to_bronze(source, bronze)

    assert list(bronze.iterdir()) == []
