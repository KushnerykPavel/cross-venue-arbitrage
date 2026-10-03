import json
from datetime import date

import polars as pl

from experiments.analysis.latency_report import build_latency_report


CAPTURE_ID = "75a10192-28c0-4eab-9bb2-d09857055d9e"


def write_events(root, table, rows):
    path = root / "version=3" / "date=1970-01-01" / f"event_type={table}" / "venue=binance" / "market=BTC"
    path.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(path / "part.parquet")


def test_report_computes_nearest_quantiles_and_lists_outliers(tmp_path):
    dataset = tmp_path / "dataset"
    write_events(
        dataset,
        "order_book_events",
        {
            "capture_id": [CAPTURE_ID] * 4,
            "capture_sequence": [1, 2, 3, 4],
            "venue": ["binance"] * 4,
            "market_coin": ["BTC"] * 4,
            "local_receive_time": [10, 20, 30, 40],
            "processing_completion_time": [11, 22, 33, 54],
        },
    )
    write_events(
        dataset,
        "market_trades",
        {
            "capture_id": [CAPTURE_ID],
            "capture_sequence": [5],
            "venue": ["aster"],
            "market_coin": ["BTC"],
            "local_receive_time": [100],
            "processing_completion_time": [105],
        },
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "capture_id": CAPTURE_ID,
                "capture_status": "complete",
                "collector_label": "collector-eu-1",
                "git_commit": "abc123",
                "package_version": "0.1.0",
                "rust_target": "x86_64-unknown-linux-gnu",
                "dirty_build": False,
            }
        ),
        encoding="utf-8",
    )

    report = build_latency_report(dataset, manifest, date(2026, 10, 3), top_outliers=3)

    assert "| binance | 4 | 3 | 14 | 14 | 14 | 14 |" in report
    assert "| aster | 1 | 5 | 5 | 5 | 5 | 5 |" in report
    assert "| binance | BTC | 4 | 14 |" in report
    assert "| binance | BTC | 3 | 3 |" in report
    assert CAPTURE_ID in report
    assert "collector-eu-1" in report
    assert "abc123" in report
    assert "console presentation can delay the next receive" in report


def test_report_rejects_processing_before_receive(tmp_path):
    dataset = tmp_path / "dataset"
    write_events(
        dataset,
        "order_book_events",
        {
            "capture_id": [CAPTURE_ID],
            "capture_sequence": [1],
            "venue": ["binance"],
            "market_coin": ["BTC"],
            "local_receive_time": [20],
            "processing_completion_time": [10],
        },
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"capture_id": CAPTURE_ID}), encoding="utf-8")

    try:
        build_latency_report(dataset, manifest)
    except ValueError as error:
        assert "processing before receive" in str(error)
    else:
        raise AssertionError("expected malformed latency timestamps to be rejected")


def test_report_without_source_manifest_marks_collector_metadata_missing(tmp_path):
    dataset = tmp_path / "dataset"
    write_events(
        dataset,
        "order_book_events",
        {
            "capture_id": [CAPTURE_ID],
            "capture_sequence": [1],
            "venue": ["binance"],
            "market_coin": ["BTC"],
            "local_receive_time": [10],
            "processing_completion_time": [12],
        },
    )
    (dataset / "dataset-metadata.json").write_text(
        json.dumps({"source_capture_ids": [CAPTURE_ID], "source_capture_statuses": ["complete"]}),
        encoding="utf-8",
    )

    report = build_latency_report(dataset)

    assert CAPTURE_ID in report
    assert "source capture manifest is missing" in report
    assert "not available: the source capture manifest is not included" in report
