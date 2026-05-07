"""Frequency-ranked human review queue for unresolved CORDIS PICs.

Joins ``cordis_participants`` and (when present) ``openaire_orgs`` with
``pic_ror_resolutions`` and surfaces every PIC seen in CORDIS participants
that does *not* have a ``high`` or ``medium`` confidence resolution. For
each such PIC the queue records the total occurrence count across imported
frameworks, the per-origin breakdown (HE, H2020, FP7), the most common
``legal_name``, ``short_name``, ``country_code`` and ``website_url`` seen
in CORDIS participant rows, alternative names and external identifiers
(Wikidata, GRID, ISNI, Fundref) collected from any OpenAIRE org records
that link the PIC, and the pipeline's ``review``-confidence candidate
(``ror_id``, ``confidence``, ``source``) when one exists.

Rows are sorted by total count descending so a human reviewer can spend
their time on the most impactful entries first. Each row is one decision:

- a ``review``-confidence pipeline guess that looks right -> copy
  ``pipeline_ror`` into ``overrides/pic_ror_overrides.yaml`` (promotes it)
- a ``review``-confidence guess that looks wrong -> add an entry with an
  empty ``ror`` to ``overrides/`` (leaves it unresolved), or correct it
  manually
- no pipeline candidate -> look the institution up (e.g. ror.org search by
  ``legal_name`` + ``country_code``) and add the resolved ROR to ``overrides/``

Whichever frameworks are loaded into the DB are aggregated. To get the
full picture across HE, H2020, and FP7, re-run the pipeline with all
three archives before generating the queue.

Run:

    uv run python review_queue.py \\
        --db tmp/org-pipeline/organizations.sqlite \\
        --out tmp/org-pipeline/exports/review_queue.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

from pipeline import default_db_path, default_export_dir

ACCEPTED_CONFIDENCE = frozenset({"high", "medium"})

# OpenAIRE external-identifier columns surfaced for cross-checking against
# candidate ROR entries. Each is a JSON-encoded list of strings.
OPENAIRE_EXTERNAL_ID_COLUMNS = (
    ("wikidata", "wikidata_ids_json"),
    ("grid", "grid_ids_json"),
    ("isni", "isni_ids_json"),
    ("fundref", "fundref_ids_json"),
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", type=Path, default=default_db_path())
    parser.add_argument(
        "--out",
        type=Path,
        default=default_export_dir() / "review_queue.csv",
        help="Output CSV path (default: <export-dir>/review_queue.csv).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=30,
        help="How many top rows to print to stdout (default: 30).",
    )
    args = parser.parse_args()

    db_path = args.db
    if not db_path.exists():
        parser.error(f"DB not found: {db_path} (run the pipeline first).")

    rows = build_review_queue(db_path)
    write_review_queue(args.out, rows)
    print_summary(rows, args.top)


def build_review_queue(db_path: Path) -> list[dict]:
    """Return one row per unresolved PIC, sorted by total count desc."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        # PICs already auto-applied at consume time; exclude them.
        accepted = {
            row["pic"]
            for row in conn.execute(
                "SELECT pic FROM pic_ror_resolutions "
                "WHERE confidence IN ('high', 'medium')"
            )
        }
        # Pipeline's `review`-confidence candidates, indexed by PIC.
        review_by_pic = {
            row["pic"]: (row["ror_id"], row["confidence"], row["source"])
            for row in conn.execute(
                "SELECT pic, ror_id, confidence, source FROM pic_ror_resolutions "
                "WHERE confidence NOT IN ('high', 'medium')"
            )
        }
        # OpenAIRE side: alternative names and external identifiers indexed
        # by PIC, when the PIC appears in any openaire_orgs record's
        # pics_json. Used purely as additional signals for human review.
        openaire_by_pic = _load_openaire_signals(conn)
        # All participants whose PIC is not auto-resolved.
        participants = conn.execute(
            "SELECT pic, origin, legal_name, short_name, country_code, "
            "website_url FROM cordis_participants WHERE pic != '' "
            "ORDER BY pic"
        )

        counts: Counter[str] = Counter()
        origins_seen: set[str] = set()
        origin_counts: dict[str, Counter[str]] = defaultdict(Counter)
        names: dict[str, Counter[str]] = defaultdict(Counter)
        short_names: dict[str, Counter[str]] = defaultdict(Counter)
        countries: dict[str, Counter[str]] = defaultdict(Counter)
        websites: dict[str, Counter[str]] = defaultdict(Counter)

        for row in participants:
            pic = row["pic"]
            if pic in accepted:
                continue
            origin = row["origin"]
            origins_seen.add(origin)
            counts[pic] += 1
            origin_counts[pic][origin] += 1
            if row["legal_name"]:
                names[pic][row["legal_name"]] += 1
            if row["short_name"]:
                short_names[pic][row["short_name"]] += 1
            if row["country_code"]:
                countries[pic][row["country_code"]] += 1
            if row["website_url"]:
                websites[pic][row["website_url"]] += 1

    ordered_origins = sorted(origins_seen)
    rows: list[dict] = []
    for pic, count in counts.most_common():
        review = review_by_pic.get(pic, ("", "", ""))
        most_common = lambda c: c.most_common(1)[0][0] if c else ""
        oa = openaire_by_pic.get(pic, {})
        row = {
            "pic": pic,
            "count": count,
            **{
                f"count_{o.lower()}": origin_counts[pic].get(o, 0)
                for o in ordered_origins
            },
            "country": most_common(countries[pic]),
            "short_name": most_common(short_names[pic]),
            "legal_name": most_common(names[pic]),
            "website_url": most_common(websites[pic]),
            "openaire_alt_names": oa.get("alt_names", ""),
            "openaire_external_ids": oa.get("external_ids", ""),
            "pipeline_ror": review[0],
            "pipeline_confidence": review[1],
            "pipeline_source": review[2],
        }
        rows.append(row)
    return rows


def _load_openaire_signals(conn: sqlite3.Connection) -> dict[str, dict[str, str]]:
    """Return ``{pic: {alt_names, external_ids}}`` from openaire_orgs.

    Each PIC may appear in multiple OpenAIRE records (different OpenOrgs
    variants of the same legal entity). Alt names are deduped and joined
    with ``|``. External IDs are emitted as ``<scheme>:<value>`` tokens
    joined with ``|``, e.g. ``wikidata:Q123|grid:grid.4444.0|isni:0000…``.
    """
    if not _has_table(conn, "openaire_orgs"):
        return {}
    by_pic: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: {"alt_names": set(), "external_ids": set()}
    )
    select_cols = ", ".join(col for _, col in OPENAIRE_EXTERNAL_ID_COLUMNS)
    rows = conn.execute(
        f"""
        SELECT je.value AS pic, alt_names_json, {select_cols}
        FROM openaire_orgs, json_each(openaire_orgs.pics_json) je
        """
    )
    for row in rows:
        pic = row["pic"]
        bucket = by_pic[pic]
        for name in _safe_json_list(row["alt_names_json"]):
            if name:
                bucket["alt_names"].add(name)
        for scheme, col in OPENAIRE_EXTERNAL_ID_COLUMNS:
            for value in _safe_json_list(row[col]):
                if value:
                    bucket["external_ids"].add(f"{scheme}:{value}")
    # Compact to pipe-joined strings, capping alt_names at 5 to keep the
    # CSV manageable.
    out: dict[str, dict[str, str]] = {}
    for pic, bucket in by_pic.items():
        alt = sorted(bucket["alt_names"])
        if len(alt) > 5:
            alt = alt[:5]
        ext = sorted(bucket["external_ids"])
        out[pic] = {
            "alt_names": "|".join(alt),
            "external_ids": "|".join(ext),
        }
    return out


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
    )


def _safe_json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(decoded, list):
        return []
    return [str(item) for item in decoded if item is not None]


def write_review_queue(out_path: Path, rows: list[dict]) -> None:
    if not rows:
        print(f"No unresolved PICs; nothing to write to {out_path}.")
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {out_path}")


def print_summary(rows: list[dict], top: int) -> None:
    if not rows:
        return
    total = sum(r["count"] for r in rows)
    with_candidate = sum(1 for r in rows if r["pipeline_ror"])
    print(
        f"\n{len(rows)} unresolved PICs, {total} total appearances. "
        f"{with_candidate} have a `review`-confidence pipeline candidate; "
        f"{len(rows) - with_candidate} have no guess."
    )

    origin_keys = [k for k in rows[0] if k.startswith("count_")]
    print(f"\nTop {top} review queue entries:")
    header = (
        f"  {'PIC':<12} {'count':>5} "
        + " ".join(f"{k.replace('count_', '').upper():>5}" for k in origin_keys)
        + f"  {'cc':<3}  {'pipeline_ror':<12} {'src':<28}  {'short':<12}  legal_name"
    )
    print(header)
    for r in rows[:top]:
        per_origin = " ".join(f"{r[k]:>5}" for k in origin_keys)
        print(
            f"  {r['pic']:<12} {r['count']:>5} {per_origin}  "
            f"{r['country']:<3}  "
            f"{(r['pipeline_ror'] or '-'):<12} "
            f"{(r['pipeline_source'] or '(no candidate)')[:28]:<28}  "
            f"{r['short_name'][:12]:<12}  "
            f"{r['legal_name'][:55]}"
        )

    print("\nCumulative coverage if these are accepted into overrides:")
    for n in (10, 30, 100, 500, 1000):
        cum = sum(r["count"] for r in rows[:n])
        pct = cum / total * 100 if total else 0.0
        print(f"  top {n:4d}: {cum:>6} appearances ({pct:.1f}% of unresolved)")


if __name__ == "__main__":
    main()
