"""Report ROR coverage for EOR Repository community participants."""

from __future__ import annotations

import argparse
import json
import shlex
import sqlite3
import statistics
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline import default_db_path

COVERAGE_BINS = (
    ("0%", lambda value: value == 0),
    (">0-20%", lambda value: 0 < value <= 20),
    (">20-40%", lambda value: 20 < value <= 40),
    (">40-60%", lambda value: 40 < value <= 60),
    (">60-80%", lambda value: 60 < value <= 80),
    (">80-<100%", lambda value: 80 < value < 100),
    ("100%", lambda value: value == 100),
)
SIZE_BANDS = (
    ("1-5", lambda total: 1 <= total <= 5),
    ("6-10", lambda total: 6 <= total <= 10),
    ("11-20", lambda total: 11 <= total <= 20),
    ("21-40", lambda total: 21 <= total <= 40),
    ("41+", lambda total: total >= 41),
)


def build_coverage_report(
    *,
    db_path: str | Path | None = None,
    ror_zip: str | Path | None = None,
) -> dict[str, Any]:
    """Build the current ROR-coverage report from the pipeline database."""
    db = default_db_path(db_path)
    ror_path = None if ror_zip is None else Path(ror_zip).expanduser()
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        community = _community_coverage(conn)
        awards = _award_coverage(conn)
        report = {
            "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "db_path": str(db),
            "ror_zip_path": None if ror_path is None else str(ror_path),
            "communities_in_scope": len(community),
            "active_awards_in_scope": len(awards),
            "participant_list_availability": _participant_list_availability(community),
            "coverage_per_community": _coverage_summary(community.values()),
            "coverage_per_award": _award_summary(awards),
            "coverage_by_source_bucket": _coverage_by_source_bucket(community.values()),
            "coverage_by_project_size": _coverage_by_project_size(community.values()),
            "coordinator_coverage_cordis_only": _coordinator_coverage(conn, community),
        }
    if ror_path is not None:
        report["resolved_ror_types"] = _resolved_ror_types(
            db_path=db,
            ror_zip=ror_path,
        )
    return report


def render_coverage_markdown(report: dict[str, Any]) -> str:
    """Render a human-readable markdown summary from the JSON report."""
    availability = report["participant_list_availability"]
    community = report["coverage_per_community"]
    award = report["coverage_per_award"]
    coordinator = report["coordinator_coverage_cordis_only"]
    source_bucket = report["coverage_by_source_bucket"]
    size_band = report["coverage_by_project_size"]
    lines = [
        "# Organizations ROR Coverage",
        "",
        f"Generated from `{report['db_path']}` on {report['generated_at']}.",
        "",
        "Produced with:",
        "",
        "- `coverage.py`",
        "",
        "Rerun from the repo root:",
        "",
        "```bash",
        _rerun_command(report),
        "```",
        "",
        "This report answers a narrow question:",
        "if we only count participants that can be connected to ROR-backed linked",
        "organization entries, how much of the current EOR Repository community set can we",
        "cover?",
        "",
        "## Scope",
        "",
        (
            "- Award-bearing active EU communities in scope: "
            f"`{report['communities_in_scope']}`"
        ),
        f"- Active awards in scope: `{report['active_awards_in_scope']}`",
        (
            "- Communities with any participant list in the current pipeline: "
            f"`{availability['communities_with_participants']}` "
            f"(`{availability['pct_with_participants']}%`)"
        ),
        (
            "- Communities without any participant list in the current pipeline: "
            f"`{availability['communities_without_participants']}`"
        ),
        "",
        "Important caveat:",
        "the current pipeline DB was built with official HE CORDIS XML plus the",
        "community dump fallback. Official H2020 and FP7 CORDIS archives were not",
        "loaded for this report, so non-HE denominator completeness is still a bit",
        "conservative / uneven.",
        "",
        "## Key Numbers",
        "",
        (
            "- Median per community, `high+medium` ROR coverage: "
            f"`{community['median_high_medium_pct']}%`"
        ),
        (
            "- Median per community, `any` ROR coverage: "
            f"`{community['median_any_pct']}%`"
        ),
        (
            "- Median per award, `high+medium` ROR coverage: "
            f"`{award['median_high_medium_pct']}%`"
        ),
        (f"- Median per award, `any` ROR coverage: `{award['median_any_pct']}%`"),
        "",
        "## Coverage Bands Per Community",
        "",
        "| High+Medium band | Communities |",
        "| --- | ---: |",
    ]
    for label, count in community["distribution_high_medium_pct"].items():
        lines.append(f"| {label} | {count} |")
    lines.extend(
        [
            "",
            "| Any-confidence band | Communities |",
            "| --- | ---: |",
        ]
    )
    for label, count in community["distribution_any_pct"].items():
        lines.append(f"| {label} | {count} |")

    lines.extend(
        [
            "",
            "## Coverage By Project Size",
            "",
            "| Participant count | Communities | Median high+medium % | Median any % |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for label, values in size_band.items():
        lines.append(
            "| "
            f"{label} | {values['communities']} | "
            f"{_fmt(values['median_hm_pct'])} | {_fmt(values['median_any_pct'])} |"
        )

    lines.extend(
        [
            "",
            "## Coverage By Participant Source",
            "",
            "| Source bucket | Communities | With participants | Median participants | Median high+medium % | Median any % |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for label, values in source_bucket.items():
        lines.append(
            "| "
            f"{label} | {values['communities']} | {values['with_participants']} | "
            f"{_fmt(values['median_participants'])} | "
            f"{_fmt(values['median_hm_pct'])} | {_fmt(values['median_any_pct'])} |"
        )

    lines.extend(
        [
            "",
            "## Coordinator Coverage",
            "",
            "This is only measurable where official CORDIS participant roles are present.",
            "",
            (
                "- Communities with official CORDIS participant rows: "
                f"`{coordinator['communities_with_cordis_participants']}`"
            ),
            (
                "- Communities with a coordinator resolved at `high+medium`: "
                f"`{coordinator['communities_with_resolved_coordinator_hm']}` "
                f"(`{coordinator['pct_with_resolved_coordinator_hm']}%`)"
            ),
            (
                "- Communities with a coordinator resolved at `any`: "
                f"`{coordinator['communities_with_resolved_coordinator_any']}` "
                f"(`{coordinator['pct_with_resolved_coordinator_any']}%`)"
            ),
        ]
    )

    resolved_types = report.get("resolved_ror_types")
    if resolved_types is not None:
        lines.extend(
            [
                "",
                "## Resolved ROR Types",
                "",
                "These are only the participants that were successfully resolved to ROR.",
                "ROR type labels are coarse; e.g. labs often land under `facility` or",
                "`other` rather than a dedicated `lab` type.",
                "",
                "| ROR type | Unique resolved PICs | Participant occurrences |",
                "| --- | ---: | ---: |",
            ]
        )
        all_labels = {
            *resolved_types["unique_pic"].keys(),
            *resolved_types["occurrences"].keys(),
        }
        for label in sorted(all_labels):
            lines.append(
                "| "
                f"{label} | {resolved_types['unique_pic'].get(label, 0)} | "
                f"{resolved_types['occurrences'].get(label, 0)} |"
            )

    lines.extend(
        [
            "",
            "## Practical Reading",
            "",
            "- If you only auto-write linked ROR org entries, expect roughly half the",
            "  consortium to be filled in a strict automatic mode.",
            "- If you also surface `review` matches for acceptance, expect closer to",
            "  two-thirds.",
            "- The coordinator is often covered even when the full consortium is not.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_coverage_markdown(report: dict[str, Any], path: str | Path) -> Path:
    """Write the markdown coverage report to disk."""
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_coverage_markdown(report))
    return target


def write_coverage_json(report: dict[str, Any], path: str | Path) -> Path:
    """Write the JSON coverage report to disk."""
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return target


def coverage_main(argv: Sequence[str] | None = None) -> int:
    """CLI: build the ROR-coverage report from the pipeline database."""
    parser = argparse.ArgumentParser(
        prog="org-coverage",
        description="Compute participant->ROR coverage from the organizations DB.",
    )
    parser.add_argument("--db", type=Path, default=default_db_path())
    parser.add_argument("--ror-zip", type=Path, default=None)
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--markdown-out", type=Path, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    report = build_coverage_report(db_path=args.db, ror_zip=args.ror_zip)
    payload: dict[str, Any] = {"report": report}
    if args.json_out is not None:
        payload["json_out"] = str(write_coverage_json(report, args.json_out))
    if args.markdown_out is not None:
        payload["markdown_out"] = str(
            write_coverage_markdown(report, args.markdown_out)
        )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _community_coverage(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    active_awards = list(
        conn.execute(
            """
            SELECT slug, award_number
            FROM community_awards
            WHERE is_deleted = 0
            """
        )
    )
    award_numbers_by_slug: dict[str, set[str]] = defaultdict(set)
    for row in active_awards:
        award_numbers_by_slug[row["slug"]].add(row["award_number"])

    result: dict[str, dict[str, Any]] = {}
    for slug, awards in award_numbers_by_slug.items():
        placeholders = ",".join("?" for _ in awards)
        rows = list(
            conn.execute(
                f"""
                SELECT DISTINCT
                    pic,
                    ror_id,
                    resolution_confidence,
                    source
                FROM award_participants_best
                WHERE award_number IN ({placeholders})
                  AND pic != ''
                """,
                sorted(awards),
            )
        )
        participants = len({row["pic"] for row in rows})
        high = len(
            {
                row["pic"]
                for row in rows
                if row["resolution_confidence"] == "high" and row["ror_id"]
            }
        )
        high_medium = len(
            {
                row["pic"]
                for row in rows
                if row["resolution_confidence"] in ("high", "medium") and row["ror_id"]
            }
        )
        any_resolved = len({row["pic"] for row in rows if row["ror_id"]})
        sources = {row["source"] for row in rows}
        if not rows:
            source_bucket = "none"
        elif sources == {"cordis"}:
            source_bucket = "cordis"
        elif sources == {"community"}:
            source_bucket = "community"
        else:
            source_bucket = "mixed"
        result[slug] = {
            "award_count": len(awards),
            "participants": participants,
            "high": high,
            "high_medium": high_medium,
            "any": any_resolved,
            "source_bucket": source_bucket,
        }
    return result


def _award_coverage(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    award_numbers = [
        row["award_number"]
        for row in conn.execute(
            """
            SELECT DISTINCT award_number
            FROM community_awards
            WHERE is_deleted = 0
            ORDER BY award_number
            """
        )
    ]
    for award_number in award_numbers:
        rows = list(
            conn.execute(
                """
                SELECT DISTINCT pic, ror_id, resolution_confidence
                FROM award_participants_best
                WHERE award_number = ?
                  AND pic != ''
                """,
                (award_number,),
            )
        )
        participants = len({row["pic"] for row in rows})
        result.append(
            {
                "award_number": award_number,
                "participants": participants,
                "high_medium": len(
                    {
                        row["pic"]
                        for row in rows
                        if row["resolution_confidence"] in ("high", "medium")
                        and row["ror_id"]
                    }
                ),
                "any": len({row["pic"] for row in rows if row["ror_id"]}),
            }
        )
    return result


def _participant_list_availability(
    community: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    total = len(community)
    with_participants = sum(
        1 for values in community.values() if values["participants"]
    )
    without_participants = total - with_participants
    return {
        "communities_with_participants": with_participants,
        "communities_without_participants": without_participants,
        "pct_with_participants": _round_pct(with_participants, total),
    }


def _coverage_summary(community_rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in community_rows if row["participants"] > 0]
    high_values = [_pct(row["high"], row["participants"]) for row in rows]
    high_medium_values = [_pct(row["high_medium"], row["participants"]) for row in rows]
    any_values = [_pct(row["any"], row["participants"]) for row in rows]
    return {
        "median_high_pct": _round_value(_median(high_values)),
        "median_high_medium_pct": _round_value(_median(high_medium_values)),
        "median_any_pct": _round_value(_median(any_values)),
        "distribution_high_medium_pct": _bucket_counts(high_medium_values),
        "distribution_any_pct": _bucket_counts(any_values),
    }


def _award_summary(award_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in award_rows if row["participants"] > 0]
    return {
        "awards_with_participants": len(rows),
        "awards_without_participants": len(award_rows) - len(rows),
        "median_high_medium_pct": _round_value(
            _median([_pct(row["high_medium"], row["participants"]) for row in rows])
        ),
        "median_any_pct": _round_value(
            _median([_pct(row["any"], row["participants"]) for row in rows])
        ),
    }


def _coverage_by_source_bucket(
    community_rows: Iterable[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in community_rows:
        buckets[row["source_bucket"]].append(row)
    result: dict[str, dict[str, Any]] = {}
    for label in ("cordis", "community", "mixed", "none"):
        rows = buckets.get(label, [])
        with_participants = [row for row in rows if row["participants"] > 0]
        result[label] = {
            "communities": len(rows),
            "with_participants": len(with_participants),
            "median_participants": _round_value(
                _median([row["participants"] for row in with_participants])
            ),
            "median_hm_pct": _round_value(
                _median(
                    [
                        _pct(row["high_medium"], row["participants"])
                        for row in with_participants
                    ]
                )
            ),
            "median_any_pct": _round_value(
                _median(
                    [_pct(row["any"], row["participants"]) for row in with_participants]
                )
            ),
        }
    return result


def _coverage_by_project_size(
    community_rows: Iterable[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    rows = [row for row in community_rows if row["participants"] > 0]
    result: dict[str, dict[str, Any]] = {}
    for label, matcher in SIZE_BANDS:
        band = [row for row in rows if matcher(row["participants"])]
        result[label] = {
            "communities": len(band),
            "median_total_participants": _round_value(
                _median([row["participants"] for row in band])
            ),
            "median_resolved_count_any": _round_value(
                _median([row["any"] for row in band])
            ),
            "median_hm_pct": _round_value(
                _median([_pct(row["high_medium"], row["participants"]) for row in band])
            ),
            "median_any_pct": _round_value(
                _median([_pct(row["any"], row["participants"]) for row in band])
            ),
        }
    return result


def _coordinator_coverage(
    conn: sqlite3.Connection,
    community: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    cordis_slugs = [
        slug
        for slug, values in community.items()
        if values["source_bucket"] == "cordis"
    ]
    communities_with_any = 0
    communities_with_hm = 0
    communities_with_any_resolved = 0
    for slug in cordis_slugs:
        awards = [
            row["award_number"]
            for row in conn.execute(
                """
                SELECT award_number
                FROM community_awards
                WHERE is_deleted = 0 AND slug = ?
                """,
                (slug,),
            )
        ]
        placeholders = ",".join("?" for _ in awards)
        rows = list(
            conn.execute(
                f"""
                SELECT DISTINCT pic, ror_id, resolution_confidence
                FROM award_participants_best
                WHERE award_number IN ({placeholders})
                  AND source = 'cordis'
                  AND participant_type = 'coordinator'
                """,
                awards,
            )
        )
        if not rows:
            continue
        communities_with_any += 1
        if any(row["ror_id"] for row in rows):
            communities_with_any_resolved += 1
        if any(
            row["ror_id"] and row["resolution_confidence"] in ("high", "medium")
            for row in rows
        ):
            communities_with_hm += 1
    return {
        "communities_with_cordis_participants": len(cordis_slugs),
        "communities_with_any_coordinator_row": communities_with_any,
        "communities_with_resolved_coordinator_hm": communities_with_hm,
        "communities_with_resolved_coordinator_any": communities_with_any_resolved,
        "pct_with_resolved_coordinator_hm": _round_pct(
            communities_with_hm,
            communities_with_any,
        ),
        "pct_with_resolved_coordinator_any": _round_pct(
            communities_with_any_resolved,
            communities_with_any,
        ),
    }


def _resolved_ror_types(*, db_path: Path, ror_zip: Path) -> dict[str, dict[str, int]]:
    types_by_ror: dict[str, tuple[str, ...]] = {}
    with zipfile.ZipFile(ror_zip) as archive:
        name = next(item for item in archive.namelist() if item.endswith(".json"))
        with archive.open(name) as handle:
            for raw in json.load(handle):
                ror_id = raw["id"].rstrip("/").rsplit("/", 1)[-1]
                values = tuple(raw.get("types") or ["Unknown"])
                types_by_ror[ror_id] = values

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        occurrences = Counter()
        for row in conn.execute(
            """
            SELECT apb.ror_id
            FROM award_participants_best AS apb
            JOIN community_awards AS ca
              ON ca.award_number = apb.award_number
            WHERE ca.is_deleted = 0
              AND apb.ror_id != ''
            """
        ):
            occurrences[types_by_ror.get(row["ror_id"], ("Unknown",))[0]] += 1

        unique_pic = Counter()
        for row in conn.execute(
            """
            SELECT DISTINCT pic, ror_id
            FROM award_participants_best
            WHERE award_number IN (
                SELECT DISTINCT award_number
                FROM community_awards
                WHERE is_deleted = 0
            )
              AND ror_id != ''
            """
        ):
            unique_pic[types_by_ror.get(row["ror_id"], ("Unknown",))[0]] += 1

    return {
        "occurrences": dict(occurrences.most_common()),
        "unique_pic": dict(unique_pic.most_common()),
    }


def _pct(numerator: int, denominator: int) -> float:
    return 100.0 * numerator / denominator


def _round_pct(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return _round_value(_pct(numerator, denominator))


def _round_value(value: float | int | None) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    rounded = round(value, 1)
    return int(rounded) if rounded.is_integer() else rounded


def _median(values: Sequence[float | int]) -> float | int | None:
    if not values:
        return None
    return statistics.median(values)


def _bucket_counts(values: Sequence[float]) -> dict[str, int]:
    counts = Counter()
    for value in values:
        for label, matcher in COVERAGE_BINS:
            if matcher(value):
                counts[label] += 1
                break
    return {label: counts[label] for label, _ in COVERAGE_BINS}


def _fmt(value: float | int | None) -> str:
    return "-" if value is None else str(value)


def _rerun_command(report: dict[str, Any]) -> str:
    args = [
        "python coverage.py",
        f"--db {shlex.quote(report['db_path'])}",
    ]
    if report.get("ror_zip_path"):
        args.append(f"--ror-zip {shlex.quote(report['ror_zip_path'])}")
    args.append("--json-out coverage.json")
    args.append("--markdown-out docs/organizations-coverage.md")
    return " \\\n  ".join(args)


if __name__ == "__main__":
    raise SystemExit(coverage_main())
