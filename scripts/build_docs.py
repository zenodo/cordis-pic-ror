"""Bake current pipeline state into ``docs/data.js`` for the docs site.

Reads the small, tracked artifacts under ``output/`` plus optional fresh
state under ``tmp/`` and ``overrides/ai_review/``, and writes a single
``docs/data.js`` that defines ``window.DATA``. The HTML pages under
``docs/`` are static; only ``data.js`` is regenerated.

Run with ``just docs`` after pipeline / review-queue updates.
"""

from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from pathlib import Path

import yaml

# evidence_json blobs in upstream CSVs blow past the default csv field
# limit; bump it once globally.
csv.field_size_limit(sys.maxsize)

REPO = Path(__file__).resolve().parent.parent
TOP_N_PREVIEW = 50
COVERAGE_TICKS = (10, 30, 100, 300, 1000, 3000, 10000)


def main() -> None:
    data = {
        "summary": _load_summary(),
        "resolutions": _load_resolutions(),
        "review_queue": _load_review_queue(),
        "ai_review": _load_ai_review(),
    }
    out_path = REPO / "docs" / "data.js"
    payload = json.dumps(data, indent=2, ensure_ascii=False)
    out_path.write_text(f"window.DATA = {payload};\n", encoding="utf-8")
    print(f"Wrote {out_path} ({out_path.stat().st_size // 1024} KB)")


def _load_summary() -> dict | None:
    p = REPO / "output" / "summary.json"
    if not p.exists():
        return None
    raw = json.loads(p.read_text(encoding="utf-8")) or {}
    return {
        "table_counts": raw.get("table_counts", {}),
        "resolution_confidence": raw.get("resolution_confidence", {}),
        "resolution_source": raw.get("resolution_source", {}),
        "community_label_quality": raw.get("community_label_quality", {}),
    }


def _load_resolutions() -> dict | None:
    p = REPO / "output" / "pic_mapping.csv"
    if not p.exists():
        return None
    by_conf: Counter[str] = Counter()
    by_source: Counter[str] = Counter()
    total = 0
    with p.open(encoding="utf-8") as fp:
        for row in csv.DictReader(fp):
            by_conf[row.get("confidence", "")] += 1
            by_source[row.get("source", "")] += 1
            total += 1
    return {
        "total": total,
        "by_confidence": dict(by_conf.most_common()),
        "by_source": [
            {"source": s, "count": n} for s, n in by_source.most_common(15)
        ],
    }


def _load_review_queue() -> dict | None:
    """Read the queue if present; produce a slim preview + cumulative coverage."""
    p = REPO / "tmp" / "org-pipeline" / "exports" / "review_queue.csv"
    if not p.exists():
        return None
    counts: list[int] = []
    preview: list[dict] = []
    total_appearances = 0
    with p.open(encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            try:
                count = int(row["count"])
            except (KeyError, ValueError):
                continue
            counts.append(count)
            total_appearances += count
            if len(preview) < TOP_N_PREVIEW:
                preview.append(
                    {
                        "pic": row.get("pic", ""),
                        "count": count,
                        "country": row.get("country", ""),
                        "short_name": row.get("short_name", ""),
                        "legal_name": row.get("legal_name", ""),
                        "pipeline_ror": row.get("pipeline_ror", ""),
                        "pipeline_confidence": row.get("pipeline_confidence", ""),
                    }
                )

    coverage_curve = []
    cum = 0
    for i, c in enumerate(counts, start=1):
        cum += c
        if i in COVERAGE_TICKS or i == len(counts):
            coverage_curve.append(
                {"top_n": i, "appearances": cum, "pct": (cum / total_appearances) if total_appearances else 0.0}
            )
    return {
        "total_pics": len(counts),
        "total_appearances": total_appearances,
        "top": preview,
        "coverage_curve": coverage_curve,
        # Compact cumulative array (sums every Nth row) for the JS chart
        "cumulative": _bucket_cumulative(counts, total_appearances),
    }


def _bucket_cumulative(counts: list[int], total: int) -> list[dict]:
    """Return ~100 (top_n, pct) samples for plotting."""
    if not counts:
        return []
    out = []
    n = len(counts)
    step = max(1, n // 200)
    cum = 0
    for i, c in enumerate(counts, start=1):
        cum += c
        if i == 1 or i == n or i % step == 0:
            out.append({"top_n": i, "pct": cum / total if total else 0.0})
    return out


def _load_ai_review() -> dict | None:
    p = REPO / "overrides" / "ai_review" / "top_3000.yaml"
    if not p.exists():
        return None
    entries = yaml.safe_load(p.read_text(encoding="utf-8")) or []
    accepted = [e for e in entries if (e.get("ror") or "").strip()]
    generalized = sum(
        1
        for e in accepted
        if "generalized to parent" in (e.get("reasoning") or "").lower()
    )

    # Coverage of accepted PICs against the review_queue if it's available.
    queue_freq: dict[str, int] = {}
    queue_path = REPO / "tmp" / "org-pipeline" / "exports" / "review_queue.csv"
    if queue_path.exists():
        with queue_path.open(encoding="utf-8") as fp:
            for row in csv.DictReader(fp):
                try:
                    queue_freq[row["pic"]] = int(row["count"])
                except (KeyError, ValueError):
                    continue
    accepted_pics = {str(e["pic"]) for e in accepted}
    covered = sum(queue_freq.get(p, 0) for p in accepted_pics)
    total_unresolved = sum(queue_freq.values()) if queue_freq else None

    return {
        "total": len(entries),
        "accepted": len(accepted),
        "unresolved": len(entries) - len(accepted),
        "generalized": generalized,
        "appearances_covered": covered if queue_freq else None,
        "total_unresolved_appearances": total_unresolved,
    }


if __name__ == "__main__":
    main()
