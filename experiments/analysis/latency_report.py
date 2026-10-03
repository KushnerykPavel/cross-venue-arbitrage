"""Build an offline receive-to-processing latency report from one Capture Run."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

PERCENTILES = (50, 95, 99, 99.9)
EVENT_TABLES = ("order_book_events", "market_trades")


def _event_files(dataset: Path) -> list[Path]:
    return sorted(
        path
        for table in EVENT_TABLES
        for path in dataset.glob(f"**/event_type={table}/**/*.parquet")
    )


def build_latency_report(
    dataset: str | Path,
    capture_manifest: str | Path | None = None,
    report_date: date | None = None,
    top_outliers: int = 10,
) -> str:
    """Return a Markdown report for event latencies in a single Capture Run.

    The source manifest supplies collector host/build metadata that the
    Parquet export does not retain. Without it, the report says those values
    are unavailable rather than substituting converter metadata.
    """
    dataset_path = Path(dataset)
    if top_outliers < 1:
        raise ValueError("top_outliers must be at least one")

    metadata_path = dataset_path / "dataset-metadata.json"
    dataset_metadata = (
        json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.is_file() else {}
    )
    if capture_manifest is not None:
        manifest = json.loads(Path(capture_manifest).read_text(encoding="utf-8"))
    else:
        if not dataset_metadata:
            raise ValueError("dataset-metadata.json or --capture-manifest is required")
        source_ids = dataset_metadata.get("source_capture_ids", [])
        if len(source_ids) != 1:
            raise ValueError("latency report requires an export of exactly one Capture Run")
        statuses = dataset_metadata.get("source_capture_statuses", [])
        manifest = {
            "capture_id": source_ids[0],
            "capture_status": statuses[0] if statuses else "unknown",
        }
    capture_id = str(manifest["capture_id"])
    files = _event_files(dataset_path)
    if not files:
        raise ValueError(f"no {', '.join(EVENT_TABLES)} parquet files found in {dataset_path}")

    observations = pl.concat(
        [
            pl.scan_parquet(str(path)).select(
                "capture_id",
                "capture_sequence",
                "venue",
                pl.col("market_coin").alias("market"),
                "local_receive_time",
                "processing_completion_time",
            )
            for path in files
        ],
        how="vertical",
    ).filter(pl.col("capture_id") == capture_id)
    observations = observations.with_columns(
        pl.col("local_receive_time").cast(pl.Int64),
        pl.col("processing_completion_time").cast(pl.Int64),
    ).with_columns(
        (pl.col("processing_completion_time") - pl.col("local_receive_time")).alias("latency_ns")
    )

    invalid_count = observations.filter(pl.col("latency_ns") < 0).select(pl.len()).collect().item()
    if invalid_count:
        raise ValueError(f"capture contains {invalid_count} events with processing before receive")

    stats_exprs = [pl.len().alias("event_count")]
    for percentile in PERCENTILES:
        stats_exprs.append(
            pl.col("latency_ns")
            .quantile(percentile / 100, interpolation="nearest")
            .cast(pl.Int64)
            .alias(f"p{percentile:g}")
        )
    stats_exprs.append(pl.col("latency_ns").max().alias("max"))
    summaries = (
        observations.group_by("venue")
        .agg(stats_exprs)
        .sort("venue")
        .collect()
    )
    if summaries.is_empty():
        raise ValueError(f"no latency-bearing events found for Capture Run {capture_id}")

    outliers = (
        observations.sort(
            ["latency_ns", "venue", "capture_sequence"],
            descending=[True, False, False],
        )
        .select("venue", "market", "capture_sequence", "latency_ns")
        .head(top_outliers)
        .collect()
    )

    date_text = (report_date or datetime.now(UTC).date()).isoformat()
    source_build = manifest.get("git_commit")
    build = (
        f"git `{source_build}`; package `{manifest.get('package_version', 'unknown')}`; "
        f"Rust target `{manifest.get('rust_target', 'unknown')}`; "
        f"dirty build: `{manifest.get('dirty_build', 'unknown')}`"
        if source_build
        else "not available: the source capture manifest is not included in this export"
    )
    host = manifest.get("collector_label") or "not available: source capture manifest is missing"
    lines = [
        f"# Receive-to-processing latency — {date_text}",
        "",
        f"- Capture Run: `{capture_id}` ({manifest.get('capture_status', 'unknown')})",
        f"- Host / collector label: `{host}`",
        f"- Collector build: {build}",
        f"- Parquet converter commit: `{dataset_metadata.get('converter_git_commit', 'not recorded')}`",
        f"- Source capture manifest: `{Path(capture_manifest) if capture_manifest else 'not included'}`",
        f"- Source dataset: `{dataset_path}`",
        "- Duration: `Processing Completion Time - Local Receive Time` (nanoseconds on the shared monotonic clock).",
        "- Percentiles use Polars nearest interpolation over captured event observations.",
        "",
        "## Per-venue summary",
        "",
        "| Venue | Events | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) | Max (ns) |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries.iter_rows(named=True):
        lines.append(
            "| {venue} | {event_count} | {p50} | {p95} | {p99} | {p99_9} | {maximum} |".format(
                venue=row["venue"],
                event_count=row["event_count"],
                p50=row["p50"],
                p95=row["p95"],
                p99=row["p99"],
                p99_9=row["p99.9"],
                maximum=row["max"],
            )
        )
    lines.extend(
        [
            "",
            f"## Largest {outliers.height} observations",
            "",
            "| Venue | Market | Capture Sequence | Latency (ns) |",
            "|---|---|---:|---:|",
        ]
    )
    for row in outliers.iter_rows(named=True):
        lines.append(
            f"| {row['venue']} | {row['market']} | {row['capture_sequence']} | {row['latency_ns']} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "This measures local receive-to-processing time at the collector. It is not exchange-to-process or network latency. As [ADR 0007](../../docs/adr/0007-shared-live-market-data-runtime.md) notes, console presentation can delay the next receive, so live console timing is not latency evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path, help="Parquet export directory for one Capture Run")
    parser.add_argument("--capture-manifest", type=Path, help="manifest.json for host and collector build metadata")
    parser.add_argument("--output", type=Path, help="report path (defaults under experiments/reports)")
    parser.add_argument("--top-outliers", type=int, default=10)
    args = parser.parse_args()

    report = build_latency_report(args.dataset, args.capture_manifest, top_outliers=args.top_outliers)
    report_date = datetime.now(UTC).date().isoformat()
    metadata_path = args.capture_manifest or args.dataset / "dataset-metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    capture_id = metadata.get("capture_id") or metadata.get("source_capture_ids", ["unknown"])[0]
    output = args.output or Path(__file__).parents[1] / "reports" / f"{report_date}-latency-{capture_id}.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report, encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
