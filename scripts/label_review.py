"""Review suspicious community organization labels in the EOR Repository dataset."""

from __future__ import annotations

import argparse
import csv
import json
import shlex
import sqlite3
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from pipeline import _json_list, default_db_path, normalize_name

DEFAULT_MARKDOWN_PATH = Path("docs/community-org-review.md")
DEFAULT_EXPORT_DIR = Path("tmp/org-pipeline/exports")


def build_label_review(
    *,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build a report of suspicious community organization labels."""
    db = default_db_path(db_path)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        suspicious_rows = _suspicious_rows(conn)
        likely_wrong_rows = _likely_wrong_rows(conn, suspicious_rows)
        likely_wrong_keys = {
            (row["slug"], row["org_order"]) for row in likely_wrong_rows
        }
        review_rows = [
            row
            for row in suspicious_rows
            if (row["slug"], row["org_order"]) not in likely_wrong_keys
        ]
        conflict_rows = _conflict_rows(conn)
    return {
        "db_path": str(db),
        "likely_wrong_rows": likely_wrong_rows,
        "review_rows": review_rows,
        "conflict_rows": conflict_rows,
        "summary": {
            "suspicious_rows": len(suspicious_rows),
            "suspicious_communities": len({row["slug"] for row in suspicious_rows}),
            "likely_wrong_rows": len(likely_wrong_rows),
            "likely_wrong_communities": len({row["slug"] for row in likely_wrong_rows}),
            "review_rows": len(review_rows),
            "review_communities": len({row["slug"] for row in review_rows}),
            "conflict_rows": len(conflict_rows),
            "conflict_communities": len({row["slug"] for row in conflict_rows}),
        },
    }


def render_label_review_markdown(report: dict[str, Any]) -> str:
    """Render the label-review report as markdown."""
    summary = report["summary"]
    lines = [
        "# Community Organization Label Review",
        "",
        f"Generated from `{report['db_path']}`.",
        "",
        "Produced with:",
        "",
        "- `label_review.py`",
        "",
        "Rerun from the repo root:",
        "",
        "```bash",
        _rerun_command(report),
        "```",
        "",
        "This report separates three things:",
        "",
        "- strong cases where a community org label seems linked to the wrong ROR",
        "- weaker label mismatches that still deserve review",
        "- participant-based community mappings that conflict with direct OpenAIRE PIC->ROR links",
        "",
        "## Summary",
        "",
        f"- Suspicious community org rows: `{summary['suspicious_rows']}`",
        f"- Communities with any suspicious row: `{summary['suspicious_communities']}`",
        f"- Likely wrong rows: `{summary['likely_wrong_rows']}`",
        f"- Communities with likely wrong rows: `{summary['likely_wrong_communities']}`",
        f"- Weaker review rows: `{summary['review_rows']}`",
        f"- Participant/OpenAIRE conflict rows: `{summary['conflict_rows']}`",
        "",
        "## Likely Wrong ROR Links",
        "",
        "| Community | Org # | Community label | Assigned ROR | Better exact ROR match |",
        "| --- | ---: | --- | --- | --- |",
    ]
    for row in report["likely_wrong_rows"]:
        assigned = f"{row['ror_id']} ({row['ror_display_name']})"
        lines.append(
            "| "
            f"{row['slug']} | {row['org_order']} | "
            f"{_cell(row['name'])} | "
            f"{_cell(assigned)} | "
            f"{_cell(row['other_rors'])} |"
        )

    lines.extend(
        [
            "",
            "## Weaker Label Mismatches",
            "",
            "These are rows where the community label does not match the assigned",
            "ROR aliases, but there was no stronger exact alternative ROR found by",
            "this report.",
            "",
            "| Community | Org # | Community label | Assigned ROR | Reason |",
            "| --- | ---: | --- | --- | --- |",
        ]
    )
    for row in report["review_rows"]:
        assigned = f"{row['ror_id']} ({row['ror_display_name']})"
        lines.append(
            "| "
            f"{row['slug']} | {row['org_order']} | {_cell(row['name'])} | "
            f"{_cell(assigned)} | "
            f"{row['reason']} |"
        )

    lines.extend(
        [
            "",
            "## Participant/OpenAIRE Conflicts",
            "",
            "These are community-derived participant mappings that point to a",
            "different ROR than the direct OpenAIRE PIC->ROR link.",
            "",
            "| Community | PIC | Participant name | Community-linked ROR | OpenAIRE direct ROR |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for row in report["conflict_rows"]:
        community_ror = f"{row['community_ror_id']} ({row['community_ror_name']})"
        openaire_ror = f"{row['openaire_ror_id']} ({row['openaire_ror_name']})"
        lines.append(
            "| "
            f"{row['slug']} | {row['pic']} | {_cell(row['participant_name'])} | "
            f"{_cell(community_ror)} | "
            f"{_cell(openaire_ror)} |"
        )
    return "\n".join(lines) + "\n"


def write_label_review_markdown(report: dict[str, Any], path: str | Path) -> Path:
    """Write the markdown review report to disk."""
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_label_review_markdown(report))
    return target


def write_label_review_csvs(
    report: dict[str, Any],
    out_dir: str | Path,
) -> list[Path]:
    """Write flat CSV exports for the review report."""
    target_dir = Path(out_dir).expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)
    outputs: list[Path] = []
    for name, rows in (
        ("likely-wrong-community-orgs.csv", report["likely_wrong_rows"]),
        ("review-community-orgs.csv", report["review_rows"]),
        ("community-org-conflicts.csv", report["conflict_rows"]),
    ):
        path = target_dir / name
        outputs.append(path)
        _write_csv(path, rows)
    return outputs


def label_review_main(argv: Sequence[str] | None = None) -> int:
    """CLI: build the suspicious community-org review report."""
    parser = argparse.ArgumentParser(
        description="Review suspicious community organization labels."
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Pipeline SQLite database path. Defaults to tmp/org-pipeline/organizations.sqlite.",
    )
    parser.add_argument(
        "--markdown-out",
        default=str(DEFAULT_MARKDOWN_PATH),
        help="Markdown report output path.",
    )
    parser.add_argument(
        "--csv-out-dir",
        default=str(DEFAULT_EXPORT_DIR),
        help="Directory for CSV exports.",
    )
    args = parser.parse_args(argv)

    report = build_label_review(db_path=args.db)
    markdown_path = write_label_review_markdown(report, args.markdown_out)
    csv_paths = write_label_review_csvs(report, args.csv_out_dir)
    print(
        json.dumps(
            {
                "db_path": report["db_path"],
                "markdown_out": str(markdown_path),
                "csv_out": [str(path) for path in csv_paths],
                "summary": report["summary"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _suspicious_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                slug,
                org_order,
                name,
                normalized_name,
                ror_id,
                ror_display_name,
                reason
            FROM suspicious_community_orgs
            ORDER BY slug, org_order
            """
        )
    ]


def _likely_wrong_rows(
    conn: sqlite3.Connection,
    suspicious_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    exact_name_matches = _ror_exact_name_matches(conn)
    likely_wrong: list[dict[str, Any]] = []
    for row in suspicious_rows:
        other_matches = [
            match
            for match in exact_name_matches.get(row["normalized_name"], [])
            if match["ror_id"] != row["ror_id"]
        ]
        assigned_display_normalized = normalize_name(row["ror_display_name"])
        if not other_matches or assigned_display_normalized == row["normalized_name"]:
            continue
        likely_wrong.append(
            {
                **row,
                "other_ror_count": len(other_matches),
                "other_rors": " | ".join(
                    f"{match['ror_id']} ({match['display_name']}, {match['country_code']})"
                    for match in other_matches
                ),
            }
        )
    return likely_wrong


def _conflict_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                o.slug,
                o.pic,
                o.participant_name,
                o.community_org_name,
                o.ror_id AS community_ror_id,
                community_ror.display_name AS community_ror_name,
                direct.ror_id AS openaire_ror_id,
                direct_ror.display_name AS openaire_ror_name
            FROM community_pic_ror_observations AS o
            JOIN community_pic_ror_matches AS m
              ON m.pic = o.pic AND m.ror_id = o.ror_id
            JOIN openaire_pic_ror_matches AS direct
              ON direct.pic = o.pic AND direct.strategy = 'direct_unique'
            LEFT JOIN ror_orgs AS community_ror
              ON community_ror.ror_id = o.ror_id
            LEFT JOIN ror_orgs AS direct_ror
              ON direct_ror.ror_id = direct.ror_id
            WHERE m.conflicts_with_openaire_direct = 1
            ORDER BY o.slug, o.pic
            """
        )
    ]


def _ror_exact_name_matches(
    conn: sqlite3.Connection,
) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT ror_id, display_name, country_code, normalized_names_json
        FROM ror_orgs
        """
    ):
        for normalized_name in _json_list(row["normalized_names_json"]):
            grouped[normalized_name].append(
                {
                    "country_code": row["country_code"],
                    "display_name": row["display_name"],
                    "ror_id": row["ror_id"],
                }
            )
    return grouped


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _rerun_command(report: dict[str, Any]) -> str:
    return " ".join(
        [
            "python",
            "label_review.py",
            "--db",
            shlex.quote(report["db_path"]),
            "--markdown-out",
            shlex.quote(str(DEFAULT_MARKDOWN_PATH)),
            "--csv-out-dir",
            shlex.quote(str(DEFAULT_EXPORT_DIR)),
        ]
    )


def _cell(value: str) -> str:
    return value.replace("|", "\\|")


if __name__ == "__main__":
    raise SystemExit(label_review_main())
