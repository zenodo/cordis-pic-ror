"""Bake the small, diff-friendly subset of pipeline artifacts into ``output/``.

Produces the consumer-ready files. Human overrides from
``overrides/pic_ror_overrides.yaml`` are merged into the slim CSV here so
the consumer just loads one file.

Outputs:

- ``output/pic_mapping.csv`` — slim columns
  (``pic, ror_id, confidence, source``). Rows from the pipeline with
  ``confidence in {high, medium}`` plus rows from overrides
  (``confidence = manual``). PICs explicitly left unresolved by an
  override are dropped entirely.
- ``output/pic_mapping.jsonl`` — same set of PICs, one JSON object per
  line, with the pipeline's parsed ``evidence`` plus any override note.
  Useful for analysis and audit.
- ``output/summary.json`` — copied from the pipeline run.

Run after ``just pipeline`` (or ``just derive``) to refresh the tracked
artifacts. ``just export`` invokes this script.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from pathlib import Path

import yaml

from pipeline import default_export_dir, default_summary_path

# evidence_json fields are very large structured traces; default CSV field
# limit (~128 KB) is well below them.
csv.field_size_limit(sys.maxsize)

SLIM_COLUMNS = ("pic", "ror_id", "confidence", "source")
ACCEPTED_CONFIDENCE = frozenset({"high", "medium"})
MANUAL_CONFIDENCE = "manual"
MANUAL_SOURCE = "manual_override"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--exports",
        type=Path,
        default=default_export_dir(),
        help="Pipeline exports directory (default: tmp/org-pipeline/exports).",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=default_summary_path(),
        help="Pipeline summary JSON (default: tmp/org-pipeline/summary.json).",
    )
    parser.add_argument(
        "--overrides",
        type=Path,
        default=Path("overrides/pic_ror_overrides.yaml"),
        help="Human-curated overrides YAML (default: overrides/pic_ror_overrides.yaml).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("output"),
        help="Destination directory (default: output).",
    )
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    rich_csv = args.exports / "pic_ror_resolutions.csv"
    if not rich_csv.exists():
        parser.error(f"Missing {rich_csv}; run `just pipeline` first.")

    overrides = _load_overrides(args.overrides)
    rows = _merge(rich_csv, overrides)
    rows.sort(key=lambda r: r["pic"])

    _write_slim_csv(args.out / "pic_mapping.csv", rows)
    _write_jsonl(args.out / "pic_mapping.jsonl", rows)

    if args.summary.exists():
        shutil.copyfile(args.summary, args.out / "summary.json")
        print(f"Copied {args.summary} -> {args.out}/summary.json")
    else:
        print(f"WARN: {args.summary} not found; skipping summary copy.")


def _load_overrides(path: Path) -> dict[str, dict]:
    """Return ``{pic: {"ror": str_or_empty, "note": str}}``.

    A non-empty ``ror`` replaces the pipeline's pick for that PIC. An empty
    ``ror`` means the reviewer left the PIC unresolved (the pipeline's
    pick, if any, is dropped). ``note`` is captured for the JSONL trail.
    """
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or []
    if not isinstance(raw, list):
        raise SystemExit(
            f"{path} must be a YAML list, got {type(raw).__name__}"
        )
    out: dict[str, dict] = {}
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        pic = str(entry.get("pic") or "").strip()
        if not pic:
            continue
        out[pic] = {
            "ror": str(entry.get("ror") or "").strip(),
            "note": str(entry.get("note") or entry.get("reasoning") or "").strip(),
        }
    return out


def _merge(rich_csv: Path, overrides: dict[str, dict]) -> list[dict]:
    """Combine pipeline rows + overrides into a single list of output rows."""
    rows: list[dict] = []
    seen: set[str] = set()

    with rich_csv.open(newline="", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        missing = [c for c in SLIM_COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            raise SystemExit(
                f"{rich_csv} is missing expected columns: {missing}. "
                f"Got {reader.fieldnames!r}."
            )
        for row in reader:
            pic = row["pic"]
            override = overrides.get(pic)

            if override is not None:
                seen.add(pic)
                if not override["ror"]:
                    # Reviewer left this PIC unresolved; drop it.
                    continue
                rows.append(
                    {
                        "pic": pic,
                        "ror_id": override["ror"],
                        "confidence": MANUAL_CONFIDENCE,
                        "source": MANUAL_SOURCE,
                        "evidence": None,
                        "override_note": override["note"] or None,
                    }
                )
                continue

            if row["confidence"] not in ACCEPTED_CONFIDENCE:
                continue
            try:
                evidence = (
                    json.loads(row["evidence_json"])
                    if row.get("evidence_json")
                    else None
                )
            except (json.JSONDecodeError, ValueError):
                evidence = None
            rows.append(
                {
                    "pic": pic,
                    "ror_id": row["ror_id"],
                    "confidence": row["confidence"],
                    "source": row["source"],
                    "evidence": evidence,
                    "override_note": None,
                }
            )

    # Overrides for PICs the pipeline never produced a row for.
    for pic, override in overrides.items():
        if pic in seen or not override["ror"]:
            continue
        rows.append(
            {
                "pic": pic,
                "ror_id": override["ror"],
                "confidence": MANUAL_CONFIDENCE,
                "source": MANUAL_SOURCE,
                "evidence": None,
                "override_note": override["note"] or None,
            }
        )
    return rows


def _write_slim_csv(out_path: Path, rows: list[dict]) -> None:
    with out_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=SLIM_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row[k] for k in SLIM_COLUMNS})
    manual = sum(1 for r in rows if r["source"] == MANUAL_SOURCE)
    print(
        f"Wrote {len(rows)} rows to {out_path} "
        f"({len(rows) - manual} pipeline + {manual} manual)"
    )


def _write_jsonl(out_path: Path, rows: list[dict]) -> None:
    with out_path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, sort_keys=True) + "\n")
    print(f"Wrote {len(rows)} records to {out_path}")


if __name__ == "__main__":
    main()
