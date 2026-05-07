"""Build a rerunnable organizations pipeline from CORDIS, OpenAIRE, ROR, and
Zenodo community metadata.

This module keeps the pipeline logic in one place and exposes four small CLI
entry points:

- ``import_main`` loads raw datasets into SQLite.
- ``derive_main`` builds match/evidence tables from the raw imports.
- ``report_main`` prints and optionally writes a JSON summary.
- ``pipeline_main`` runs import, derive, export, and report in one command.

The pipeline is intentionally conservative. It keeps raw source tables,
candidate/evidence tables, and a separate final-resolution table so we do not
need to trust every noisy input equally.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import sqlite3
import tarfile
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from rapidfuzz import fuzz, process

DEFAULT_PIPELINE_DIR = Path("tmp/org-pipeline")
DEFAULT_DB_PATH = DEFAULT_PIPELINE_DIR / "organizations.sqlite"
DEFAULT_EXPORT_DIR = DEFAULT_PIPELINE_DIR / "exports"
DEFAULT_SUMMARY_PATH = DEFAULT_PIPELINE_DIR / "summary.json"

CORDIS_ORIGIN_PRIORITY = {"HE": 0, "H2020": 1, "FP7": 2}
CORPORATE_SUFFIXES = {
    "ab",
    "ag",
    "aps",
    "as",
    "bv",
    "co",
    "doo",
    "eirl",
    "ev",
    "geie",
    "gmbh",
    "inc",
    "ivzw",
    "kft",
    "kg",
    "limited",
    "llc",
    "ltd",
    "nv",
    "oy",
    "oyj",
    "plc",
    "pte",
    "sa",
    "sarl",
    "sas",
    "sci",
    "sl",
    "slu",
    "spa",
    "sprl",
    "srl",
    "sro",
    "vzw",
    "zoo",
}
GENERIC_SHORT_NAME_TOKENS = {
    "agency",
    "association",
    "centre",
    "center",
    "college",
    "foundation",
    "institute",
    "museum",
    "university",
}
OPENAIRE_EXACT_NAME_STRATEGIES = (
    "name_country_unique",
    "alt_name_country_unique",
    "short_name_country_unique",
)
PARTICIPANT_EXACT_NAME_STRATEGIES = ("participant_name_country_unique",)
TOKEN_SORT_STRATEGIES = (
    "token_sort_name_country_unique",
    "participant_token_sort_name_country_unique",
)
FUZZY_NAME_STRATEGIES = (
    "fuzzy_name_country_unique",
    "fuzzy_alias_country_unique",
    "participant_fuzzy_name_country_unique",
)
FUZZY_SCORE_THRESHOLD = 95.0
FUZZY_SCORE_MARGIN = 5.0
FUZZY_RESULT_LIMIT = 10
RERUNNABLE_EXPORTS = {
    "awards_best.csv": """
        SELECT
            award_number,
            source,
            source_origin,
            acronym,
            title,
            programme,
            award_url,
            project_website
        FROM awards_best
        ORDER BY award_number
    """,
    "award_participants_best.csv": """
        SELECT
            award_number,
            participant_order,
            source,
            source_origin,
            pic,
            participant_name,
            participant_type,
            country_code,
            website_url,
            ror_id,
            resolution_confidence,
            resolution_source
        FROM award_participants_best
        ORDER BY award_number, participant_order
    """,
    "pic_ror_resolutions.csv": """
        SELECT
            pic,
            ror_id,
            confidence,
            source,
            evidence_json
        FROM pic_ror_resolutions
        ORDER BY pic
    """,
    "community_pic_ror_matches.csv": """
        SELECT
            pic,
            ror_id,
            observation_count,
            slug_count,
            award_count,
            community_status,
            agrees_with_openaire_direct,
            conflicts_with_openaire_direct
        FROM community_pic_ror_matches
        ORDER BY pic, ror_id
    """,
    "community_pic_ror_observations.csv": """
        SELECT
            pic,
            ror_id,
            slug,
            award_number,
            participant_name,
            community_org_name,
            evidence_json
        FROM community_pic_ror_observations
        ORDER BY pic, ror_id, slug, award_number
    """,
    "openaire_pic_ror_matches.csv": """
        SELECT
            pic,
            ror_id,
            strategy,
            confidence,
            source_record_id,
            evidence_json
        FROM openaire_pic_ror_matches
        ORDER BY pic, strategy, ror_id, source_record_id
    """,
    "participant_pic_ror_matches.csv": """
        SELECT
            pic,
            ror_id,
            strategy,
            confidence,
            participant_source,
            source_origin,
            award_number,
            evidence_json
        FROM participant_pic_ror_matches
        ORDER BY pic, strategy, ror_id, participant_source, source_origin, award_number
    """,
    "suspicious_community_orgs.csv": """
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
    """,
}


@dataclass(slots=True)
class CordisArchive:
    """One official CORDIS archive plus the origin code it belongs to."""

    origin: str
    path: Path


@dataclass(slots=True)
class AliasMatch:
    """One exact alias-based candidate plus the alias values that supported it."""

    ror_ids: set[str]
    matched_names: tuple[str, ...]


@dataclass(slots=True)
class FuzzyMatch:
    """One fuzzy name candidate plus the values and scores behind it."""

    margin: float
    matched_names: tuple[str, ...]
    ratio: float
    ror_id: str
    runner_up_score: float
    score: float
    source_name: str
    source_normalized: str
    target_name: str
    target_normalized: str
    token_set_ratio: float
    token_sort_ratio: float


def default_db_path(path: str | Path | None = None) -> Path:
    """Return the SQLite target used by the standalone organizations pipeline."""
    if path is None:
        return DEFAULT_DB_PATH
    return Path(path).expanduser()


def default_export_dir(path: str | Path | None = None) -> Path:
    """Return the export directory used by the standalone organizations pipeline."""
    if path is None:
        return DEFAULT_EXPORT_DIR
    return Path(path).expanduser()


def default_summary_path(path: str | Path | None = None) -> Path:
    """Return the summary JSON path used by the standalone organizations pipeline."""
    if path is None:
        return DEFAULT_SUMMARY_PATH
    return Path(path).expanduser()


def init_org_db(path: str | Path | None = None) -> Path:
    """Create the SQLite schema when missing and return the resolved DB path."""
    db_path = default_db_path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
    return db_path


def import_sources(
    *,
    db_path: str | Path | None = None,
    cordis_archives: Sequence[CordisArchive] = (),
    community_dump: str | Path | None = None,
    openaire_tar: str | Path | None = None,
    ror_zip: str | Path | None = None,
    community_parent_slug: str = "eu",
) -> dict[str, Any]:
    """Load raw datasets into SQLite.

    Each import stage replaces the rows for its own raw tables so reruns stay
    deterministic.
    """

    db = init_org_db(db_path)
    stats: dict[str, Any] = {"db": str(db), "imports": {}}
    if cordis_archives:
        stats["imports"]["cordis"] = [
            import_cordis_archive(archive, db_path=db) for archive in cordis_archives
        ]
    if community_dump is not None:
        stats["imports"]["communities"] = import_community_dump(
            Path(community_dump),
            db_path=db,
            parent_slug=community_parent_slug,
        )
    if openaire_tar is not None:
        stats["imports"]["openaire"] = import_openaire_tar(
            Path(openaire_tar),
            db_path=db,
        )
    if ror_zip is not None:
        stats["imports"]["ror"] = import_ror_zip(Path(ror_zip), db_path=db)
    return stats


def derive_tables(*, db_path: str | Path | None = None) -> dict[str, Any]:
    """Rebuild all derived tables from the imported raw source tables."""
    db = default_db_path(db_path)
    with _connect(db) as conn:
        _clear_tables(
            conn,
            (
                "suspicious_community_orgs",
                "community_pic_ror_observations",
                "community_pic_ror_matches",
                "openaire_pic_ror_matches",
                "participant_pic_ror_matches",
                "pic_ror_resolutions",
                "awards_best",
                "award_participants_best",
            ),
        )
        suspicious_count = _build_suspicious_community_orgs(conn)
        openaire_count = _build_openaire_pic_ror_matches(conn)
        participant_count = _build_participant_pic_ror_matches(conn)
        community_count = _build_community_pic_ror_matches(conn)
        resolution_count = _build_pic_ror_resolutions(conn)
        awards_count, participants_count = _build_award_tables(conn)
        conn.commit()
    return {
        "db": str(db),
        "derived": {
            "suspicious_community_orgs": suspicious_count,
            "openaire_pic_ror_matches": openaire_count,
            "participant_pic_ror_matches": participant_count,
            "community_pic_ror_matches": community_count,
            "pic_ror_resolutions": resolution_count,
            "awards_best": awards_count,
            "award_participants_best": participants_count,
        },
    }


def export_tables(
    *,
    db_path: str | Path | None = None,
    export_dir: str | Path | None = None,
) -> list[Path]:
    """Write the most useful output tables to CSV for downstream inspection."""
    db = default_db_path(db_path)
    out_dir = default_export_dir(export_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    with _connect(db) as conn:
        for name, query in RERUNNABLE_EXPORTS.items():
            path = out_dir / name
            _export_query_to_csv(conn, query, path)
            written.append(path)
    return written


def build_summary(*, db_path: str | Path | None = None) -> dict[str, Any]:
    """Return a compact JSON-serializable summary of the current pipeline DB."""
    db = default_db_path(db_path)
    with _connect(db) as conn:
        table_counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "cordis_projects",
                "cordis_participants",
                "community_awards",
                "community_award_participants",
                "community_orgs",
                "openaire_orgs",
                "ror_orgs",
                "suspicious_community_orgs",
                "openaire_pic_ror_matches",
                "participant_pic_ror_matches",
                "community_pic_ror_observations",
                "community_pic_ror_matches",
                "pic_ror_resolutions",
                "awards_best",
                "award_participants_best",
            )
        }
        resolution_confidence = {
            row["confidence"]: row["count"]
            for row in conn.execute(
                """
                SELECT confidence, COUNT(*) AS count
                FROM pic_ror_resolutions
                GROUP BY confidence
                ORDER BY confidence
                """
            )
        }
        resolution_source = {
            row["source"]: row["count"]
            for row in conn.execute(
                """
                SELECT source, COUNT(*) AS count
                FROM pic_ror_resolutions
                GROUP BY source
                ORDER BY source
                """
            )
        }
        community_quality = {
            row["bucket"]: row["count"]
            for row in conn.execute(
                """
                SELECT bucket, COUNT(*) AS count
                FROM (
                    SELECT
                        CASE
                            WHEN ror_id = '' THEN 'no_ror'
                            WHEN EXISTS (
                                SELECT 1
                                FROM suspicious_community_orgs AS s
                                WHERE s.slug = community_orgs.slug
                                  AND s.org_order = community_orgs.org_order
                            ) THEN 'suspicious'
                            ELSE 'self_consistent'
                        END AS bucket
                    FROM community_orgs
                )
                GROUP BY bucket
                ORDER BY bucket
                """
            )
        }
    return {
        "db": str(db),
        "table_counts": table_counts,
        "resolution_confidence": resolution_confidence,
        "resolution_source": resolution_source,
        "community_label_quality": community_quality,
    }


def write_summary(
    summary: dict[str, Any],
    *,
    summary_path: str | Path | None = None,
) -> Path:
    """Persist the JSON summary to disk."""
    path = default_summary_path(summary_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return path


def run_pipeline(
    *,
    db_path: str | Path | None = None,
    export_dir: str | Path | None = None,
    summary_path: str | Path | None = None,
    cordis_archives: Sequence[CordisArchive] = (),
    community_dump: str | Path | None = None,
    openaire_tar: str | Path | None = None,
    ror_zip: str | Path | None = None,
    community_parent_slug: str = "eu",
) -> dict[str, Any]:
    """Run the full rerunnable pipeline end to end."""
    imported = import_sources(
        db_path=db_path,
        cordis_archives=cordis_archives,
        community_dump=community_dump,
        openaire_tar=openaire_tar,
        ror_zip=ror_zip,
        community_parent_slug=community_parent_slug,
    )
    derived = derive_tables(db_path=db_path)
    exports = export_tables(db_path=db_path, export_dir=export_dir)
    summary = build_summary(db_path=db_path)
    summary_file = write_summary(summary, summary_path=summary_path)
    return {
        "import": imported,
        "derive": derived,
        "exports": [str(path) for path in exports],
        "summary": summary,
        "summary_path": str(summary_file),
    }


def import_cordis_archive(
    archive: CordisArchive,
    *,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Import one official CORDIS project archive into the raw tables."""
    db = init_org_db(db_path)
    archive_path = archive.path.expanduser()
    seen = 0
    imported_projects = 0
    imported_participants = 0
    with zipfile.ZipFile(archive_path) as zipped, _connect(db) as conn:
        conn.execute("DELETE FROM cordis_projects WHERE origin = ?", (archive.origin,))
        conn.execute(
            "DELETE FROM cordis_participants WHERE origin = ?",
            (archive.origin,),
        )
        for name in sorted(zipped.namelist()):
            if not name.lower().endswith(".xml"):
                continue
            seen += 1
            try:
                root = ET.fromstring(zipped.read(name))
            except ET.ParseError:
                continue
            parsed = _parse_cordis_project(root, origin=archive.origin)
            if parsed is None:
                continue
            project, participants = parsed
            conn.execute(
                """
                INSERT INTO cordis_projects (
                    award_number,
                    origin,
                    acronym,
                    title,
                    programme,
                    project_url,
                    project_website
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project["award_number"],
                    project["origin"],
                    project["acronym"],
                    project["title"],
                    project["programme"],
                    project["project_url"],
                    project["project_website"],
                ),
            )
            for participant in participants:
                conn.execute(
                    """
                    INSERT INTO cordis_participants (
                        award_number,
                        origin,
                        participant_order,
                        participant_type,
                        pic,
                        legal_name,
                        normalized_legal_name,
                        short_name,
                        country_code,
                        website_url,
                        vat_number,
                        rcn
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        participant["award_number"],
                        participant["origin"],
                        participant["participant_order"],
                        participant["participant_type"],
                        participant["pic"],
                        participant["legal_name"],
                        participant["normalized_legal_name"],
                        participant["short_name"],
                        participant["country_code"],
                        participant["website_url"],
                        participant["vat_number"],
                        participant["rcn"],
                    ),
                )
            imported_projects += 1
            imported_participants += len(participants)
        conn.commit()
    return {
        "origin": archive.origin,
        "archive": str(archive_path),
        "seen_xml": seen,
        "projects": imported_projects,
        "participants": imported_participants,
    }


def import_community_dump(
    path: Path,
    *,
    db_path: str | Path | None = None,
    parent_slug: str = "eu",
) -> dict[str, Any]:
    """Import the OpenSearch JSONL community dump into raw tables."""
    db = init_org_db(db_path)
    records = 0
    awards = 0
    participants = 0
    orgs = 0
    with _connect(db) as conn:
        _clear_tables(
            conn,
            ("community_awards", "community_award_participants", "community_orgs"),
        )
        with path.expanduser().open() as handle:
            for line in handle:
                raw = json.loads(line)
                parent = raw.get("parent")
                if not (isinstance(parent, dict) and parent.get("slug") == parent_slug):
                    continue
                records += 1
                metadata = raw.get("metadata") or {}
                slug = raw.get("slug") or ""
                community_uuid = raw.get("id") or ""
                is_deleted = int(bool(raw.get("is_deleted")))
                for award_order, item in enumerate(
                    metadata.get("funding") or [], start=1
                ):
                    award = (item or {}).get("award") or {}
                    award_number = (award.get("number") or "").strip()
                    if not award_number:
                        continue
                    award_url = _award_url(award)
                    conn.execute(
                        """
                        INSERT INTO community_awards (
                            slug,
                            community_uuid,
                            is_deleted,
                            award_order,
                            award_number,
                            award_acronym,
                            award_title,
                            award_program,
                            award_url
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            slug,
                            community_uuid,
                            is_deleted,
                            award_order,
                            award_number,
                            award.get("acronym") or "",
                            _english_title(award.get("title")) or "",
                            award.get("program") or "",
                            award_url,
                        ),
                    )
                    awards += 1
                    for participant_order, participant in enumerate(
                        award.get("organizations") or [],
                        start=1,
                    ):
                        pic = (participant.get("id") or "").strip()
                        if participant.get("scheme") != "pic" or not pic:
                            continue
                        name = (participant.get("organization") or "").strip()
                        conn.execute(
                            """
                            INSERT INTO community_award_participants (
                                slug,
                                award_number,
                                participant_order,
                                pic,
                                participant_name,
                                normalized_name
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                slug,
                                award_number,
                                participant_order,
                                pic,
                                name,
                                normalize_name(name),
                            ),
                        )
                        participants += 1
                for org_order, org in enumerate(
                    metadata.get("organizations") or [], start=1
                ):
                    name = (org.get("name") or "").strip()
                    identifiers = org.get("identifiers") or []
                    conn.execute(
                        """
                        INSERT INTO community_orgs (
                            slug,
                            org_order,
                            name,
                            normalized_name,
                            provided_id,
                            provided_id_kind,
                            ror_id,
                            identifiers_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            slug,
                            org_order,
                            name,
                            normalize_name(name),
                            (org.get("id") or "").strip(),
                            _classify_provided_id(org.get("id") or ""),
                            _extract_community_ror_id(org),
                            json.dumps(identifiers, sort_keys=True),
                        ),
                    )
                    orgs += 1
        conn.commit()
    return {
        "path": str(path.expanduser()),
        "records": records,
        "awards": awards,
        "participants": participants,
        "organizations": orgs,
    }


def import_openaire_tar(
    path: Path,
    *,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Import the OpenAIRE organizations tarball into a raw SQLite table."""
    db = init_org_db(db_path)
    records = 0
    with _connect(db) as conn:
        _clear_tables(conn, ("openaire_orgs",))
        with tarfile.open(path.expanduser()) as archive:
            for member in archive:
                if not member.isfile() or not member.name.endswith(".json.gz"):
                    continue
                handle = archive.extractfile(member)
                if handle is None:
                    continue
                with gzip.GzipFile(fileobj=handle) as gzipped:
                    for line in gzipped:
                        raw = json.loads(line)
                        pids = raw.get("pids") or []
                        pictures = sorted(
                            {
                                item.get("value", "").strip()
                                for item in pids
                                if item.get("scheme") == "PIC" and item.get("value")
                            }
                        )
                        rors = sorted(
                            {
                                _short_ror_id(item.get("value"))
                                for item in pids
                                if item.get("scheme") == "ROR" and item.get("value")
                            }
                        )
                        grid_ids = sorted(
                            {
                                _normalize_identifier_value("grid", item.get("value"))
                                for item in pids
                                if item.get("scheme") == "GRID" and item.get("value")
                            }
                        )
                        isni_ids = sorted(
                            {
                                _normalize_identifier_value("isni", item.get("value"))
                                for item in pids
                                if item.get("scheme") == "ISNI" and item.get("value")
                            }
                        )
                        wikidata_ids = sorted(
                            {
                                _normalize_identifier_value(
                                    "wikidata",
                                    item.get("value"),
                                )
                                for item in pids
                                if item.get("scheme") == "Wikidata"
                                and item.get("value")
                            }
                        )
                        fundref_ids = sorted(
                            {
                                _normalize_identifier_value(
                                    "fundref",
                                    item.get("value"),
                                )
                                for item in pids
                                if item.get("scheme") == "FundRef" and item.get("value")
                            }
                        )
                        alt_names = sorted(
                            {
                                name.strip()
                                for name in raw.get("alternativeNames") or []
                                if name and name.strip()
                            }
                        )
                        website_url = (raw.get("websiteUrl") or "").strip()
                        conn.execute(
                            """
                            INSERT INTO openaire_orgs (
                                record_id,
                                legal_name,
                                normalized_name,
                                legal_short_name,
                                alt_names_json,
                                country_code,
                                website_url,
                                domains_json,
                                pics_json,
                                rors_json,
                                grid_ids_json,
                                isni_ids_json,
                                wikidata_ids_json,
                                fundref_ids_json
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                raw.get("id") or "",
                                raw.get("legalName") or "",
                                normalize_name(raw.get("legalName") or ""),
                                raw.get("legalShortName") or "",
                                json.dumps(alt_names, sort_keys=True),
                                (raw.get("country") or {}).get("code") or "",
                                website_url,
                                json.dumps(
                                    sorted(_domains_from_urls([website_url])),
                                    sort_keys=True,
                                ),
                                json.dumps(pictures, sort_keys=True),
                                json.dumps(rors, sort_keys=True),
                                json.dumps(grid_ids, sort_keys=True),
                                json.dumps(isni_ids, sort_keys=True),
                                json.dumps(wikidata_ids, sort_keys=True),
                                json.dumps(fundref_ids, sort_keys=True),
                            ),
                        )
                        records += 1
        conn.commit()
    return {"path": str(path.expanduser()), "records": records}


def import_ror_zip(
    path: Path,
    *,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Import the ROR JSON dump into SQLite."""
    db = init_org_db(db_path)
    records = 0
    with zipfile.ZipFile(path.expanduser()) as archive:
        json_name = next(
            name for name in archive.namelist() if name.lower().endswith(".json")
        )
        with archive.open(json_name) as handle:
            raw_records = json.load(handle)
    with _connect(db) as conn:
        _clear_tables(conn, ("ror_orgs",))
        for raw in raw_records:
            ror_id = _short_ror_id(raw.get("id"))
            if not ror_id:
                continue
            names = sorted(
                {
                    name.get("value", "").strip()
                    for name in raw.get("names") or []
                    if name.get("value")
                }
            )
            normalized_names = sorted(
                {normalize_name(value) for value in names if normalize_name(value)}
            )
            website_links = [
                link.get("value", "").strip()
                for link in raw.get("links") or []
                if link.get("type") == "website" and link.get("value")
            ]
            domains = sorted(
                _domains_from_urls(list(raw.get("domains") or []) + website_links)
            )
            conn.execute(
                """
                INSERT INTO ror_orgs (
                    ror_id,
                    display_name,
                    country_code,
                    names_json,
                    normalized_names_json,
                    domains_json,
                    website_links_json,
                    grid_ids_json,
                    isni_ids_json,
                    wikidata_ids_json,
                    fundref_ids_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ror_id,
                    _ror_display_name(raw),
                    _ror_country_code(raw),
                    json.dumps(names, sort_keys=True),
                    json.dumps(normalized_names, sort_keys=True),
                    json.dumps(domains, sort_keys=True),
                    json.dumps(sorted(website_links), sort_keys=True),
                    json.dumps(
                        sorted(_ror_external_ids(raw, "grid")),
                        sort_keys=True,
                    ),
                    json.dumps(
                        sorted(_ror_external_ids(raw, "isni")),
                        sort_keys=True,
                    ),
                    json.dumps(
                        sorted(_ror_external_ids(raw, "wikidata")),
                        sort_keys=True,
                    ),
                    json.dumps(
                        sorted(_ror_external_ids(raw, "fundref")),
                        sort_keys=True,
                    ),
                ),
            )
            records += 1
        conn.commit()
    return {"path": str(path.expanduser()), "records": records}


def import_main(argv: Sequence[str] | None = None) -> int:
    """CLI: import raw datasets into SQLite."""
    parser = argparse.ArgumentParser(
        prog="org-import",
        description="Import raw organization datasets into a rerunnable SQLite DB.",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument(
        "--cordis-archive",
        action="append",
        default=[],
        metavar="ORIGIN=PATH",
        help="Repeatable. Example: --cordis-archive HE=tmp/data/cordis-HORIZONprojects-xml.zip",
    )
    parser.add_argument("--community-dump", type=Path, default=None)
    parser.add_argument("--community-parent-slug", default="eu")
    parser.add_argument("--openaire-tar", type=Path, default=None)
    parser.add_argument("--ror-zip", type=Path, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = import_sources(
        db_path=args.db,
        cordis_archives=_parse_cordis_archives(args.cordis_archive),
        community_dump=args.community_dump,
        openaire_tar=args.openaire_tar,
        ror_zip=args.ror_zip,
        community_parent_slug=args.community_parent_slug,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def derive_main(argv: Sequence[str] | None = None) -> int:
    """CLI: rebuild derived tables and CSV exports."""
    parser = argparse.ArgumentParser(
        prog="org-derive",
        description="Rebuild organization match tables and export CSVs.",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--out", type=Path, default=DEFAULT_EXPORT_DIR)
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = derive_tables(db_path=args.db)
    exports = export_tables(db_path=args.db, export_dir=args.out)
    payload = {
        **result,
        "exports": [str(path) for path in exports],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def report_main(argv: Sequence[str] | None = None) -> int:
    """CLI: print and optionally write the pipeline summary."""
    parser = argparse.ArgumentParser(
        prog="org-report",
        description="Print a JSON summary of the current organizations DB.",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY_PATH)
    args = parser.parse_args(list(argv) if argv is not None else None)
    summary = build_summary(db_path=args.db)
    summary_path = write_summary(summary, summary_path=args.summary_json)
    payload = {"summary": summary, "summary_path": str(summary_path)}
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def pipeline_main(argv: Sequence[str] | None = None) -> int:
    """CLI: run the full pipeline end to end."""
    parser = argparse.ArgumentParser(
        prog="org-pipeline",
        description="Run the organizations pipeline from import through export.",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--out", type=Path, default=DEFAULT_EXPORT_DIR)
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY_PATH)
    parser.add_argument(
        "--cordis-archive",
        action="append",
        default=[],
        metavar="ORIGIN=PATH",
        help="Repeatable. Example: --cordis-archive HE=tmp/data/cordis-HORIZONprojects-xml.zip",
    )
    parser.add_argument("--community-dump", type=Path, default=None)
    parser.add_argument("--community-parent-slug", default="eu")
    parser.add_argument("--openaire-tar", type=Path, default=None)
    parser.add_argument("--ror-zip", type=Path, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = run_pipeline(
        db_path=args.db,
        export_dir=args.out,
        summary_path=args.summary_json,
        cordis_archives=_parse_cordis_archives(args.cordis_archive),
        community_dump=args.community_dump,
        openaire_tar=args.openaire_tar,
        ror_zip=args.ror_zip,
        community_parent_slug=args.community_parent_slug,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS cordis_projects (
            award_number TEXT NOT NULL,
            origin TEXT NOT NULL,
            acronym TEXT NOT NULL,
            title TEXT NOT NULL,
            programme TEXT NOT NULL,
            project_url TEXT NOT NULL,
            project_website TEXT NOT NULL,
            PRIMARY KEY (award_number, origin)
        );

        CREATE TABLE IF NOT EXISTS cordis_participants (
            award_number TEXT NOT NULL,
            origin TEXT NOT NULL,
            participant_order INTEGER NOT NULL,
            participant_type TEXT NOT NULL,
            pic TEXT NOT NULL,
            legal_name TEXT NOT NULL,
            normalized_legal_name TEXT NOT NULL,
            short_name TEXT NOT NULL,
            country_code TEXT NOT NULL,
            website_url TEXT NOT NULL,
            vat_number TEXT NOT NULL,
            rcn TEXT NOT NULL,
            PRIMARY KEY (award_number, origin, participant_order)
        );

        CREATE TABLE IF NOT EXISTS community_awards (
            slug TEXT NOT NULL,
            community_uuid TEXT NOT NULL,
            is_deleted INTEGER NOT NULL,
            award_order INTEGER NOT NULL,
            award_number TEXT NOT NULL,
            award_acronym TEXT NOT NULL,
            award_title TEXT NOT NULL,
            award_program TEXT NOT NULL,
            award_url TEXT NOT NULL,
            PRIMARY KEY (slug, award_order)
        );

        CREATE TABLE IF NOT EXISTS community_award_participants (
            slug TEXT NOT NULL,
            award_number TEXT NOT NULL,
            participant_order INTEGER NOT NULL,
            pic TEXT NOT NULL,
            participant_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            PRIMARY KEY (slug, award_number, participant_order)
        );

        CREATE TABLE IF NOT EXISTS community_orgs (
            slug TEXT NOT NULL,
            org_order INTEGER NOT NULL,
            name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            provided_id TEXT NOT NULL,
            provided_id_kind TEXT NOT NULL,
            ror_id TEXT NOT NULL,
            identifiers_json TEXT NOT NULL,
            PRIMARY KEY (slug, org_order)
        );

        CREATE TABLE IF NOT EXISTS openaire_orgs (
            record_id TEXT PRIMARY KEY,
            legal_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            legal_short_name TEXT NOT NULL,
            alt_names_json TEXT NOT NULL,
            country_code TEXT NOT NULL,
            website_url TEXT NOT NULL,
            domains_json TEXT NOT NULL,
            pics_json TEXT NOT NULL,
            rors_json TEXT NOT NULL,
            grid_ids_json TEXT NOT NULL,
            isni_ids_json TEXT NOT NULL,
            wikidata_ids_json TEXT NOT NULL,
            fundref_ids_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ror_orgs (
            ror_id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            country_code TEXT NOT NULL,
            names_json TEXT NOT NULL,
            normalized_names_json TEXT NOT NULL,
            domains_json TEXT NOT NULL,
            website_links_json TEXT NOT NULL,
            grid_ids_json TEXT NOT NULL,
            isni_ids_json TEXT NOT NULL,
            wikidata_ids_json TEXT NOT NULL,
            fundref_ids_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS suspicious_community_orgs (
            slug TEXT NOT NULL,
            org_order INTEGER NOT NULL,
            name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            ror_id TEXT NOT NULL,
            ror_display_name TEXT NOT NULL,
            reason TEXT NOT NULL,
            PRIMARY KEY (slug, org_order, reason)
        );

        CREATE TABLE IF NOT EXISTS openaire_pic_ror_matches (
            pic TEXT NOT NULL,
            ror_id TEXT NOT NULL,
            strategy TEXT NOT NULL,
            confidence TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY (pic, ror_id, strategy, source_record_id)
        );

        CREATE TABLE IF NOT EXISTS participant_pic_ror_matches (
            pic TEXT NOT NULL,
            ror_id TEXT NOT NULL,
            strategy TEXT NOT NULL,
            confidence TEXT NOT NULL,
            participant_source TEXT NOT NULL,
            source_origin TEXT NOT NULL,
            award_number TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY (
                pic,
                ror_id,
                strategy,
                participant_source,
                source_origin,
                award_number
            )
        );

        CREATE TABLE IF NOT EXISTS community_pic_ror_observations (
            pic TEXT NOT NULL,
            ror_id TEXT NOT NULL,
            slug TEXT NOT NULL,
            award_number TEXT NOT NULL,
            participant_name TEXT NOT NULL,
            community_org_name TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY (pic, ror_id, slug, award_number)
        );

        CREATE TABLE IF NOT EXISTS community_pic_ror_matches (
            pic TEXT NOT NULL,
            ror_id TEXT NOT NULL,
            observation_count INTEGER NOT NULL,
            slug_count INTEGER NOT NULL,
            award_count INTEGER NOT NULL,
            community_status TEXT NOT NULL,
            agrees_with_openaire_direct INTEGER NOT NULL,
            conflicts_with_openaire_direct INTEGER NOT NULL,
            PRIMARY KEY (pic, ror_id)
        );

        CREATE TABLE IF NOT EXISTS pic_ror_resolutions (
            pic TEXT PRIMARY KEY,
            ror_id TEXT NOT NULL,
            confidence TEXT NOT NULL,
            source TEXT NOT NULL,
            evidence_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS awards_best (
            award_number TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            source_origin TEXT NOT NULL,
            acronym TEXT NOT NULL,
            title TEXT NOT NULL,
            programme TEXT NOT NULL,
            award_url TEXT NOT NULL,
            project_website TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS award_participants_best (
            award_number TEXT NOT NULL,
            participant_order INTEGER NOT NULL,
            source TEXT NOT NULL,
            source_origin TEXT NOT NULL,
            pic TEXT NOT NULL,
            participant_name TEXT NOT NULL,
            participant_type TEXT NOT NULL,
            country_code TEXT NOT NULL,
            website_url TEXT NOT NULL,
            ror_id TEXT NOT NULL,
            resolution_confidence TEXT NOT NULL,
            resolution_source TEXT NOT NULL,
            PRIMARY KEY (award_number, source, participant_order)
        );

        CREATE INDEX IF NOT EXISTS cordis_participants_pic_idx
            ON cordis_participants (pic);
        CREATE INDEX IF NOT EXISTS community_award_participants_pic_idx
            ON community_award_participants (pic);
        CREATE INDEX IF NOT EXISTS community_awards_number_idx
            ON community_awards (award_number);
        CREATE INDEX IF NOT EXISTS community_orgs_ror_idx
            ON community_orgs (ror_id);
        CREATE INDEX IF NOT EXISTS openaire_pic_ror_matches_pic_idx
            ON openaire_pic_ror_matches (pic);
        CREATE INDEX IF NOT EXISTS community_pic_ror_matches_pic_idx
            ON community_pic_ror_matches (pic);
        """
    )
    conn.commit()


def _clear_tables(conn: sqlite3.Connection, tables: Iterable[str]) -> None:
    for table in tables:
        conn.execute(f"DELETE FROM {table}")


def _parse_cordis_project(
    root: ET.Element,
    *,
    origin: str,
) -> tuple[dict[str, str], list[dict[str, Any]]] | None:
    award_number = _xml_text(root, "./{*}id")
    title = _xml_text(root, "./{*}title")
    if not award_number or not title:
        return None
    project = {
        "award_number": award_number,
        "origin": origin,
        "acronym": _xml_text(root, "./{*}acronym"),
        "title": title,
        "programme": _cordis_programme(root),
        "project_url": f"https://cordis.europa.eu/project/id/{award_number}",
        "project_website": _cordis_project_website(root),
    }
    participants: list[dict[str, Any]] = []
    for participant_order, org in enumerate(
        root.findall(".//{*}organization"), start=1
    ):
        pic = _xml_text(org, "./{*}id")
        legal_name = _xml_text(org, "./{*}legalName")
        if not pic or not legal_name:
            continue
        participants.append(
            {
                "award_number": award_number,
                "origin": origin,
                "participant_order": participant_order,
                "participant_type": org.get("type", ""),
                "pic": pic,
                "legal_name": legal_name,
                "normalized_legal_name": normalize_name(legal_name),
                "short_name": _xml_text(org, "./{*}shortName"),
                "country_code": _xml_text(org, "./{*}address/{*}country"),
                "website_url": _xml_text(org, "./{*}address/{*}url"),
                "vat_number": _xml_text(org, "./{*}vatNumber"),
                "rcn": _xml_text(org, "./{*}rcn"),
            }
        )
    return project, participants


def _build_suspicious_community_orgs(conn: sqlite3.Connection) -> int:
    ror_names = _load_ror_names(conn)
    ror_display = _load_ror_display(conn)
    count = 0
    for row in conn.execute(
        """
        SELECT slug, org_order, name, normalized_name, ror_id
        FROM community_orgs
        WHERE ror_id != ''
        """
    ):
        aliases = ror_names.get(row["ror_id"])
        if aliases is None:
            conn.execute(
                """
                INSERT INTO suspicious_community_orgs (
                    slug,
                    org_order,
                    name,
                    normalized_name,
                    ror_id,
                    ror_display_name,
                    reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["slug"],
                    row["org_order"],
                    row["name"],
                    row["normalized_name"],
                    row["ror_id"],
                    "",
                    "missing-ror-record",
                ),
            )
            count += 1
            continue
        if row["normalized_name"] and row["normalized_name"] in aliases:
            continue
        conn.execute(
            """
            INSERT INTO suspicious_community_orgs (
                slug,
                org_order,
                name,
                normalized_name,
                ror_id,
                ror_display_name,
                reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["slug"],
                row["org_order"],
                row["name"],
                row["normalized_name"],
                row["ror_id"],
                ror_display.get(row["ror_id"], ""),
                "name-not-in-ror-aliases",
            ),
        )
        count += 1
    return count


def _build_openaire_pic_ror_matches(conn: sqlite3.Connection) -> int:
    ror_indexes = _load_ror_indexes(conn)
    fuzzy_index = _load_ror_fuzzy_index(conn)
    ror_context = _load_ror_context(conn)
    inserted = 0
    for row in conn.execute(
        """
        SELECT
            record_id,
            legal_name,
            normalized_name,
            legal_short_name,
            alt_names_json,
            country_code,
            website_url,
            domains_json,
            pics_json,
            rors_json,
            grid_ids_json,
            isni_ids_json,
            wikidata_ids_json,
            fundref_ids_json
        FROM openaire_orgs
        """
    ):
        pics = _json_list(row["pics_json"])
        if not pics:
            continue
        direct_rors = _json_list(row["rors_json"])
        strategies: dict[str, set[str]] = defaultdict(set)
        strategy_extra_by_ror: dict[tuple[str, str], dict[str, Any]] = {}
        if len(direct_rors) == 1:
            strategies["direct_unique"].add(direct_rors[0])
        elif len(direct_rors) > 1:
            strategies["direct_ambiguous"].update(direct_rors)
        if len(direct_rors) != 1:
            external = _unique_ror_candidates_from_external_ids(
                row,
                ror_indexes["external"],
            )
            if external:
                strategies["external_id_unique"].update(external)

            domain = _unique_ror_candidates_from_domains(
                _json_list(row["domains_json"]),
                ror_indexes["domain"],
            )
            if domain:
                strategies["domain_unique"].update(domain)

            exact_name = _unique_ror_candidates_from_name_country(
                row["normalized_name"],
                row["country_code"],
                ror_indexes["name_country"],
            )
            if exact_name:
                strategies["name_country_unique"].update(exact_name)

            short_name = _unique_ror_candidates_from_alias_names(
                [row["legal_short_name"]],
                row["country_code"],
                ror_indexes["name_country"],
            )
            if short_name.ror_ids:
                strategies["short_name_country_unique"].update(short_name.ror_ids)

            alt_names = _unique_ror_candidates_from_alias_names(
                _json_list(row["alt_names_json"]),
                row["country_code"],
                ror_indexes["name_country"],
            )
            if alt_names.ror_ids:
                strategies["alt_name_country_unique"].update(alt_names.ror_ids)

            token_sort = _unique_ror_candidates_from_token_sort_name_country(
                row["normalized_name"],
                row["country_code"],
                ror_indexes["token_sort_name_country"],
            )
            if token_sort and token_sort != exact_name:
                strategies["token_sort_name_country_unique"].update(token_sort)

            if not (
                exact_name or short_name.ror_ids or alt_names.ror_ids or token_sort
            ):
                fuzzy_name = _best_unique_fuzzy_match(
                    [row["legal_name"]],
                    row["country_code"],
                    fuzzy_index,
                )
                if fuzzy_name is not None:
                    strategies["fuzzy_name_country_unique"].add(fuzzy_name.ror_id)
                    strategy_extra_by_ror[
                        ("fuzzy_name_country_unique", fuzzy_name.ror_id)
                    ] = {
                        "fuzzy_match": _serialize_fuzzy_match(fuzzy_name),
                    }

                fuzzy_alias = _best_unique_fuzzy_match(
                    [row["legal_short_name"], *_json_list(row["alt_names_json"])],
                    row["country_code"],
                    fuzzy_index,
                )
                if fuzzy_alias is not None:
                    strategies["fuzzy_alias_country_unique"].add(fuzzy_alias.ror_id)
                    strategy_extra_by_ror[
                        ("fuzzy_alias_country_unique", fuzzy_alias.ror_id)
                    ] = {
                        "fuzzy_match": _serialize_fuzzy_match(fuzzy_alias),
                    }

        for strategy, ror_ids in strategies.items():
            if not ror_ids:
                continue
            confidence = {
                "direct_unique": "high",
                "external_id_unique": "medium",
                "domain_unique": "medium",
                "name_country_unique": "review",
                "alt_name_country_unique": "review",
                "short_name_country_unique": "review",
                "token_sort_name_country_unique": "review",
                "fuzzy_name_country_unique": "review",
                "fuzzy_alias_country_unique": "review",
                "direct_ambiguous": "review",
            }[strategy]
            for pic in pics:
                for ror_id in sorted(ror_ids):
                    extra = dict(strategy_extra_by_ror.get((strategy, ror_id), {}))
                    if strategy == "short_name_country_unique":
                        extra["matched_names"] = short_name.matched_names
                    elif strategy == "alt_name_country_unique":
                        extra["matched_names"] = alt_names.matched_names
                    elif strategy == "token_sort_name_country_unique":
                        extra["token_sort_name"] = token_sort_key(
                            row["normalized_name"]
                        )
                    evidence = _openaire_evidence(
                        row,
                        strategy,
                        ror_context,
                        ror_id,
                        **extra,
                    )
                    conn.execute(
                        """
                        INSERT INTO openaire_pic_ror_matches (
                            pic,
                            ror_id,
                            strategy,
                            confidence,
                            source_record_id,
                            evidence_json
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            pic,
                            ror_id,
                            strategy,
                            confidence,
                            row["record_id"],
                            json.dumps(evidence, sort_keys=True),
                        ),
                    )
                    inserted += 1
    return inserted


def _build_participant_pic_ror_matches(conn: sqlite3.Connection) -> int:
    ror_indexes = _load_ror_indexes(conn)
    fuzzy_index = _load_ror_fuzzy_index(conn)
    ror_context = _load_ror_context(conn)
    openaire_pics = {
        row["value"]
        for row in conn.execute(
            """
            SELECT value
            FROM openaire_orgs, json_each(openaire_orgs.pics_json)
            """
        )
    }
    inserted = 0
    for row in conn.execute(
        """
        SELECT
            award_number,
            origin,
            participant_order,
            participant_type,
            pic,
            legal_name,
            normalized_legal_name,
            short_name,
            country_code,
            website_url
        FROM cordis_participants
        WHERE pic != ''
          AND country_code != ''
          AND normalized_legal_name != ''
        """
    ):
        if row["pic"] in openaire_pics:
            continue
        exact_name = _unique_ror_candidates_from_name_country(
            row["normalized_legal_name"],
            row["country_code"],
            ror_indexes["name_country"],
        )
        if exact_name:
            inserted += _insert_participant_candidates(
                conn,
                pic=row["pic"],
                ror_ids=exact_name,
                strategy="participant_name_country_unique",
                confidence="review",
                participant_source="cordis",
                source_origin=row["origin"],
                award_number=row["award_number"],
                evidence_by_ror={
                    ror_id: _participant_evidence(
                        row,
                        "participant_name_country_unique",
                        ror_context,
                        ror_id,
                    )
                    for ror_id in exact_name
                },
            )

        token_sort = _unique_ror_candidates_from_token_sort_name_country(
            row["normalized_legal_name"],
            row["country_code"],
            ror_indexes["token_sort_name_country"],
        )
        if token_sort and token_sort != exact_name:
            inserted += _insert_participant_candidates(
                conn,
                pic=row["pic"],
                ror_ids=token_sort,
                strategy="participant_token_sort_name_country_unique",
                confidence="review",
                participant_source="cordis",
                source_origin=row["origin"],
                award_number=row["award_number"],
                evidence_by_ror={
                    ror_id: _participant_evidence(
                        row,
                        "participant_token_sort_name_country_unique",
                        ror_context,
                        ror_id,
                    )
                    for ror_id in token_sort
                },
            )

        if not exact_name and not token_sort:
            fuzzy_name = _best_unique_fuzzy_match(
                [row["legal_name"]],
                row["country_code"],
                fuzzy_index,
            )
            if fuzzy_name is not None:
                inserted += _insert_participant_candidates(
                    conn,
                    pic=row["pic"],
                    ror_ids={fuzzy_name.ror_id},
                    strategy="participant_fuzzy_name_country_unique",
                    confidence="review",
                    participant_source="cordis",
                    source_origin=row["origin"],
                    award_number=row["award_number"],
                    evidence_by_ror={
                        fuzzy_name.ror_id: _participant_evidence(
                            row,
                            "participant_fuzzy_name_country_unique",
                            ror_context,
                            fuzzy_name.ror_id,
                            fuzzy_match=fuzzy_name,
                        )
                    },
                )
    return inserted


def _build_community_pic_ror_matches(conn: sqlite3.Connection) -> int:
    ror_names = _load_ror_names(conn)
    ror_context = _load_ror_context(conn)
    suspicious = {
        (row["slug"], row["org_order"])
        for row in conn.execute("SELECT slug, org_order FROM suspicious_community_orgs")
    }
    by_slug: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    by_slug_name: dict[str, dict[str, str]] = defaultdict(dict)
    for row in conn.execute(
        """
        SELECT slug, org_order, name, normalized_name, ror_id
        FROM community_orgs
        WHERE ror_id != ''
        """
    ):
        if (row["slug"], row["org_order"]) in suspicious:
            continue
        if row["ror_id"] not in ror_names:
            continue
        aliases = set(ror_names[row["ror_id"]])
        if row["normalized_name"]:
            aliases.add(row["normalized_name"])
        for alias in aliases:
            by_slug[row["slug"]][alias].add(row["ror_id"])
        by_slug_name[row["slug"]].setdefault(row["ror_id"], row["name"])

    inserted = 0
    observations: dict[tuple[str, str], dict[str, Any]] = {}
    seen_observations: set[tuple[str, str, str, str]] = set()
    for row in conn.execute(
        """
        SELECT slug, award_number, pic, participant_name, normalized_name
        FROM community_award_participants
        WHERE pic != '' AND normalized_name != ''
        """
    ):
        alias_map = by_slug.get(row["slug"])
        if not alias_map:
            continue
        candidates = alias_map.get(row["normalized_name"], set())
        if len(candidates) != 1:
            continue
        ror_id = next(iter(candidates))
        observation_key = (row["pic"], ror_id, row["slug"], row["award_number"])
        if observation_key in seen_observations:
            continue
        seen_observations.add(observation_key)
        community_org_name = by_slug_name[row["slug"]][ror_id]
        conn.execute(
            """
            INSERT INTO community_pic_ror_observations (
                pic,
                ror_id,
                slug,
                award_number,
                participant_name,
                community_org_name,
                evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["pic"],
                ror_id,
                row["slug"],
                row["award_number"],
                row["participant_name"],
                community_org_name,
                json.dumps(
                    _community_observation_evidence(
                        row,
                        community_org_name,
                        ror_context,
                        ror_id,
                    ),
                    sort_keys=True,
                ),
            ),
        )
        key = (row["pic"], ror_id)
        bucket = observations.setdefault(
            key,
            {
                "observation_count": 0,
                "slugs": set(),
                "awards": set(),
            },
        )
        bucket["observation_count"] += 1
        bucket["slugs"].add(row["slug"])
        bucket["awards"].add(row["award_number"])
        inserted += 1

    direct_map = defaultdict(set)
    for row in conn.execute(
        """
        SELECT pic, ror_id
        FROM openaire_pic_ror_matches
        WHERE strategy = 'direct_unique'
        """
    ):
        direct_map[row["pic"]].add(row["ror_id"])

    by_pic = defaultdict(set)
    for pic, ror_id in observations:
        by_pic[pic].add(ror_id)
    aggregated = 0
    for (pic, ror_id), bucket in sorted(observations.items()):
        direct = direct_map.get(pic, set())
        conn.execute(
            """
            INSERT INTO community_pic_ror_matches (
                pic,
                ror_id,
                observation_count,
                slug_count,
                award_count,
                community_status,
                agrees_with_openaire_direct,
                conflicts_with_openaire_direct
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pic,
                ror_id,
                bucket["observation_count"],
                len(bucket["slugs"]),
                len(bucket["awards"]),
                "stable" if len(by_pic[pic]) == 1 else "ambiguous",
                int(bool(direct) and ror_id in direct),
                int(bool(direct) and ror_id not in direct),
            ),
        )
        aggregated += 1
    return aggregated


def _build_pic_ror_resolutions(conn: sqlite3.Connection) -> int:
    """Build a deliberately conservative final PIC->ROR table with decision trace."""

    openaire_by_pic = _load_openaire_candidates_by_pic(conn)
    participant_by_pic = _load_participant_candidates_by_pic(conn)
    community_by_pic = _load_community_candidates_by_pic(conn)
    ror_context = _load_ror_context(conn)

    resolved = 0
    all_pics = set(openaire_by_pic) | set(participant_by_pic) | set(community_by_pic)
    for pic in sorted(all_pics):
        openaire_rows = openaire_by_pic.get(pic, [])
        participant_rows = participant_by_pic.get(pic, [])
        community_rows = community_by_pic.get(pic, [])
        group_supports = _build_group_supports(
            openaire_rows=openaire_rows,
            participant_rows=participant_rows,
            community_rows=community_rows,
        )

        direct = _single_supported_ror(group_supports["direct_unique"])
        direct_ambiguous = set(group_supports["direct_ambiguous"])
        external = _single_supported_ror(group_supports["external"])
        domain = _single_supported_ror(group_supports["domain"])
        exact_name = _single_supported_ror(group_supports["exact_name"])
        fuzzy_name = _single_supported_ror(group_supports["fuzzy_name"])
        token_sort = _single_supported_ror(group_supports["token_sort"])
        stable_community = _stable_community_candidate(community_rows)
        community_ror = stable_community["ror_id"] if stable_community else ""

        chosen_ror = ""
        confidence = ""
        picked_score = _no_score(
            "The resolver only used exact rule outputs here. No fuzzy score "
            "was calculated."
        )
        source = ""
        reason = ""
        if direct:
            chosen_ror = direct
            confidence = "high"
            source = "openaire_direct"
            reason = "OpenAIRE carries exactly one direct PIC->ROR link."
        elif community_ror and community_ror in {
            external,
            domain,
            exact_name,
            token_sort,
        }:
            chosen_ror = community_ror
            confidence = "high"
            source = "community_supported"
            reason = "Stable community evidence agrees with another unique candidate."
        elif direct_ambiguous and external and external in direct_ambiguous:
            chosen_ror = external
            confidence = "medium"
            source = "openaire_ambiguous_external"
            reason = "Direct OpenAIRE ROR list is ambiguous, but external ids pick one candidate."
        elif direct_ambiguous and domain and domain in direct_ambiguous:
            chosen_ror = domain
            confidence = "medium"
            source = "openaire_ambiguous_domain"
            reason = "Direct OpenAIRE ROR list is ambiguous, but the website domain picks one candidate."
        elif direct_ambiguous and exact_name and exact_name in direct_ambiguous:
            chosen_ror = exact_name
            confidence = "medium"
            source = "openaire_ambiguous_exact_name"
            reason = "Direct OpenAIRE ROR list is ambiguous, but exact same-country name evidence picks one candidate."
        elif direct_ambiguous and token_sort and token_sort in direct_ambiguous:
            chosen_ror = token_sort
            confidence = "review"
            source = "openaire_ambiguous_token_sort"
            reason = "Direct OpenAIRE ROR list is ambiguous, and token-sort evidence picks one candidate."
        elif direct_ambiguous and fuzzy_name and fuzzy_name in direct_ambiguous:
            chosen_ror = fuzzy_name
            confidence = "review"
            source = "openaire_ambiguous_fuzzy_name"
            reason = (
                "Direct OpenAIRE ROR list is ambiguous, and close same-country "
                "name evidence picks one candidate."
            )
            picked_score = _candidate_score(
                openaire_rows + participant_rows,
                chosen_ror,
                FUZZY_NAME_STRATEGIES,
            )
        elif external and external == domain:
            chosen_ror = external
            confidence = "medium"
            source = "openaire_external_domain"
            reason = "External ids and website domain agree on one ROR."
        elif external and external == exact_name:
            chosen_ror = external
            confidence = "medium"
            source = "openaire_external_name"
            reason = (
                "External ids and exact same-country name evidence agree on one ROR."
            )
        elif domain and domain == exact_name:
            chosen_ror = domain
            confidence = "medium"
            source = "openaire_domain_name"
            reason = (
                "Website domain and exact same-country name evidence agree on one ROR."
            )
        elif domain and domain == fuzzy_name:
            chosen_ror = domain
            confidence = "medium"
            source = "openaire_domain_fuzzy_name"
            reason = (
                "Website domain and close same-country name evidence agree on one ROR."
            )
            picked_score = _candidate_score(
                openaire_rows + participant_rows,
                chosen_ror,
                FUZZY_NAME_STRATEGIES,
            )
        elif community_ror and stable_community["observation_count"] >= 2:
            chosen_ror = community_ror
            confidence = "medium"
            source = "community_repeat"
            reason = "The same community-derived mapping repeats across at least two observations."
        elif external:
            chosen_ror = external
            confidence = "review"
            source = "openaire_external"
            reason = "External identifiers map uniquely to one ROR."
        elif domain:
            chosen_ror = domain
            confidence = "review"
            source = "openaire_domain"
            reason = "Website domain maps uniquely to one ROR."
        elif exact_name:
            chosen_ror = exact_name
            confidence = "review"
            source = _review_exact_name_source(
                group_supports["exact_name"].get(exact_name, set())
            )
            reason = "Exact same-country name evidence points to one ROR."
        elif token_sort:
            chosen_ror = token_sort
            confidence = "review"
            source = _review_token_sort_source(
                group_supports["token_sort"].get(token_sort, set())
            )
            reason = "Token-sort exact name evidence points to one ROR."
        elif fuzzy_name:
            chosen_ror = fuzzy_name
            confidence = "review"
            source = _review_fuzzy_name_source(
                group_supports["fuzzy_name"].get(fuzzy_name, set())
            )
            reason = "Close same-country name evidence points to one ROR."
            picked_score = _candidate_score(
                openaire_rows + participant_rows,
                chosen_ror,
                FUZZY_NAME_STRATEGIES,
            )
        elif community_ror:
            chosen_ror = community_ror
            confidence = "review"
            source = "community_single"
            reason = "A single stable community-derived mapping exists."

        if not chosen_ror:
            continue
        conn.execute(
            """
            INSERT INTO pic_ror_resolutions (
                pic,
                ror_id,
                confidence,
                source,
                evidence_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                pic,
                chosen_ror,
                confidence,
                source,
                json.dumps(
                    {
                        "agreement_summary": _serialize_group_supports(
                            group_supports,
                            ror_context,
                        ),
                        "matches_checked": _trace_candidate_rows(
                            openaire_rows=openaire_rows,
                            participant_rows=participant_rows,
                            community_rows=community_rows,
                            ror_context=ror_context,
                        ),
                        "picked_match": {
                            "confidence": confidence,
                            "how": _resolution_source_label(source),
                            "how_code": source,
                            "reason": reason,
                            "score": picked_score,
                            "target": _target_ror_summary(ror_context, chosen_ror),
                        },
                    },
                    sort_keys=True,
                ),
            ),
        )
        resolved += 1
    return resolved


def _load_openaire_candidates_by_pic(
    conn: sqlite3.Connection,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT
            pic,
            ror_id,
            strategy,
            confidence,
            source_record_id,
            evidence_json
        FROM openaire_pic_ror_matches
        ORDER BY pic, strategy, ror_id, source_record_id
        """
    ):
        grouped[row["pic"]].append(
            {
                "confidence": row["confidence"],
                "evidence": json.loads(row["evidence_json"]),
                "ror_id": row["ror_id"],
                "source_record_id": row["source_record_id"],
                "strategy": row["strategy"],
            }
        )
    return grouped


def _load_participant_candidates_by_pic(
    conn: sqlite3.Connection,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT
            pic,
            ror_id,
            strategy,
            confidence,
            participant_source,
            source_origin,
            award_number,
            evidence_json
        FROM participant_pic_ror_matches
        ORDER BY pic, strategy, ror_id, participant_source, source_origin, award_number
        """
    ):
        grouped[row["pic"]].append(
            {
                "award_number": row["award_number"],
                "confidence": row["confidence"],
                "evidence": json.loads(row["evidence_json"]),
                "participant_source": row["participant_source"],
                "ror_id": row["ror_id"],
                "source_origin": row["source_origin"],
                "strategy": row["strategy"],
            }
        )
    return grouped


def _load_community_candidates_by_pic(
    conn: sqlite3.Connection,
) -> dict[str, list[dict[str, Any]]]:
    observations: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT
            pic,
            ror_id,
            slug,
            award_number,
            participant_name,
            community_org_name,
            evidence_json
        FROM community_pic_ror_observations
        ORDER BY pic, ror_id, slug, award_number
        """
    ):
        observations[(row["pic"], row["ror_id"])].append(
            {
                "award_number": row["award_number"],
                "community_org_name": row["community_org_name"],
                "evidence": json.loads(row["evidence_json"]),
                "participant_name": row["participant_name"],
                "slug": row["slug"],
            }
        )

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT
            pic,
            ror_id,
            observation_count,
            slug_count,
            award_count,
            community_status,
            agrees_with_openaire_direct,
            conflicts_with_openaire_direct
        FROM community_pic_ror_matches
        ORDER BY pic, ror_id
        """
    ):
        grouped[row["pic"]].append(
            {
                "agrees_with_openaire_direct": row["agrees_with_openaire_direct"],
                "award_count": row["award_count"],
                "community_status": row["community_status"],
                "conflicts_with_openaire_direct": row["conflicts_with_openaire_direct"],
                "observation_count": row["observation_count"],
                "observations": observations.get((row["pic"], row["ror_id"]), []),
                "ror_id": row["ror_id"],
                "slug_count": row["slug_count"],
            }
        )
    return grouped


def _build_group_supports(
    *,
    openaire_rows: Sequence[dict[str, Any]],
    participant_rows: Sequence[dict[str, Any]],
    community_rows: Sequence[dict[str, Any]],
) -> dict[str, dict[str, set[str]]]:
    groups = {
        "community": defaultdict(set),
        "direct_ambiguous": defaultdict(set),
        "direct_unique": defaultdict(set),
        "domain": defaultdict(set),
        "exact_name": defaultdict(set),
        "external": defaultdict(set),
        "fuzzy_name": defaultdict(set),
        "token_sort": defaultdict(set),
    }
    for row in openaire_rows:
        group = _candidate_group_for_strategy(row["strategy"])
        if group is None:
            continue
        groups[group][row["ror_id"]].add(f"openaire:{row['strategy']}")
    for row in participant_rows:
        group = _candidate_group_for_strategy(row["strategy"])
        if group is None:
            continue
        groups[group][row["ror_id"]].add(f"participant:{row['strategy']}")
    for row in community_rows:
        if row["community_status"] != "stable":
            continue
        groups["community"][row["ror_id"]].add("community:stable")
    return groups


def _candidate_group_for_strategy(strategy: str) -> str | None:
    if strategy == "direct_unique":
        return "direct_unique"
    if strategy == "direct_ambiguous":
        return "direct_ambiguous"
    if strategy == "external_id_unique":
        return "external"
    if strategy == "domain_unique":
        return "domain"
    if strategy in OPENAIRE_EXACT_NAME_STRATEGIES + PARTICIPANT_EXACT_NAME_STRATEGIES:
        return "exact_name"
    if strategy in FUZZY_NAME_STRATEGIES:
        return "fuzzy_name"
    if strategy in TOKEN_SORT_STRATEGIES:
        return "token_sort"
    return None


def _single_supported_ror(supports: dict[str, set[str]]) -> str:
    return next(iter(supports)) if len(supports) == 1 else ""


def _stable_community_candidate(
    community_rows: Sequence[dict[str, Any]],
) -> dict[str, Any] | None:
    stable_rows = [row for row in community_rows if row["community_status"] == "stable"]
    return stable_rows[0] if len(stable_rows) == 1 else None


def _serialize_group_supports(
    group_supports: dict[str, dict[str, set[str]]],
    ror_context: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for group, matches in group_supports.items():
        if not matches:
            continue
        serialized.append(
            {
                "matches": [
                    {
                        "backed_by": [
                            {
                                "code": supporter,
                                "label": _supporter_label(supporter),
                            }
                            for supporter in sorted(supporters)
                        ],
                        "target": _target_ror_summary(ror_context, ror_id),
                    }
                    for ror_id, supporters in sorted(matches.items())
                ],
                "signal": _support_group_label(group),
                "signal_code": group,
            }
        )
    return serialized


def _review_exact_name_source(supporters: set[str]) -> str:
    if supporters == {"openaire:name_country_unique"}:
        return "openaire_name_country"
    if supporters == {"openaire:alt_name_country_unique"}:
        return "openaire_alt_name_country"
    if supporters == {"openaire:short_name_country_unique"}:
        return "openaire_short_name_country"
    if supporters == {"participant:participant_name_country_unique"}:
        return "participant_name_country"
    return "exact_name"


def _review_token_sort_source(supporters: set[str]) -> str:
    if supporters == {"openaire:token_sort_name_country_unique"}:
        return "openaire_token_sort_name"
    if supporters == {"participant:participant_token_sort_name_country_unique"}:
        return "participant_token_sort_name"
    return "token_sort_name"


def _review_fuzzy_name_source(supporters: set[str]) -> str:
    if supporters == {"openaire:fuzzy_name_country_unique"}:
        return "openaire_fuzzy_name"
    if supporters == {"openaire:fuzzy_alias_country_unique"}:
        return "openaire_fuzzy_alias_country"
    if supporters == {"participant:participant_fuzzy_name_country_unique"}:
        return "participant_fuzzy_name"
    return "fuzzy_name"


def _candidate_score(
    rows: Sequence[dict[str, Any]],
    ror_id: str,
    strategies: Sequence[str],
) -> dict[str, Any]:
    for row in rows:
        if row["ror_id"] != ror_id or row["strategy"] not in strategies:
            continue
        return dict(row["evidence"]["comparison"]["score"])
    return _no_score()


def _build_award_tables(conn: sqlite3.Connection) -> tuple[int, int]:
    """Build best-available award/project tables for downstream backfill."""
    awards = _select_best_awards(conn)
    resolutions = {
        row["pic"]: row
        for row in conn.execute(
            """
            SELECT pic, ror_id, confidence, source
            FROM pic_ror_resolutions
            """
        )
    }
    participant_rows: list[tuple[Any, ...]] = []
    for award in awards:
        conn.execute(
            """
            INSERT INTO awards_best (
                award_number,
                source,
                source_origin,
                acronym,
                title,
                programme,
                award_url,
                project_website
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                award["award_number"],
                award["source"],
                award["source_origin"],
                award["acronym"],
                award["title"],
                award["programme"],
                award["award_url"],
                award["project_website"],
            ),
        )
        participant_rows.extend(
            _best_participants_for_award(
                conn,
                award_number=award["award_number"],
                source=award["source"],
                source_origin=award["source_origin"],
                resolutions=resolutions,
            )
        )
    for row in participant_rows:
        conn.execute(
            """
            INSERT INTO award_participants_best (
                award_number,
                participant_order,
                source,
                source_origin,
                pic,
                participant_name,
                participant_type,
                country_code,
                website_url,
                ror_id,
                resolution_confidence,
                resolution_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row,
        )
    return len(awards), len(participant_rows)


def _select_best_awards(conn: sqlite3.Connection) -> list[dict[str, str]]:
    cordis_rows = list(
        conn.execute(
            """
            SELECT
                award_number,
                origin,
                acronym,
                title,
                programme,
                project_url,
                project_website
            FROM cordis_projects
            ORDER BY award_number
            """
        )
    )
    best: dict[str, dict[str, str]] = {}
    for row in cordis_rows:
        current = best.get(row["award_number"])
        candidate = {
            "award_number": row["award_number"],
            "source": "cordis",
            "source_origin": row["origin"],
            "acronym": row["acronym"],
            "title": row["title"],
            "programme": row["programme"],
            "award_url": row["project_url"],
            "project_website": row["project_website"],
        }
        if current is None:
            best[row["award_number"]] = candidate
            continue
        current_priority = CORDIS_ORIGIN_PRIORITY.get(current["source_origin"], 999)
        candidate_priority = CORDIS_ORIGIN_PRIORITY.get(row["origin"], 999)
        if candidate_priority < current_priority:
            best[row["award_number"]] = candidate

    for row in conn.execute(
        """
        SELECT
            award_number,
            slug,
            award_acronym,
            award_title,
            award_program,
            award_url
        FROM community_awards
        ORDER BY award_number, slug
        """
    ):
        if row["award_number"] in best:
            continue
        best[row["award_number"]] = {
            "award_number": row["award_number"],
            "source": "community",
            "source_origin": row["slug"],
            "acronym": row["award_acronym"],
            "title": row["award_title"],
            "programme": row["award_program"],
            "award_url": row["award_url"],
            "project_website": "",
        }
    return [best[number] for number in sorted(best)]


def _best_participants_for_award(
    conn: sqlite3.Connection,
    *,
    award_number: str,
    source: str,
    source_origin: str,
    resolutions: dict[str, sqlite3.Row],
) -> list[tuple[Any, ...]]:
    if source == "cordis":
        rows = list(
            conn.execute(
                """
                SELECT
                    participant_order,
                    pic,
                    legal_name,
                    participant_type,
                    country_code,
                    website_url
                FROM cordis_participants
                WHERE award_number = ? AND origin = ?
                ORDER BY participant_order
                """,
                (award_number, source_origin),
            )
        )
        return [
            (
                award_number,
                row["participant_order"],
                source,
                source_origin,
                row["pic"],
                row["legal_name"],
                row["participant_type"],
                row["country_code"],
                row["website_url"],
                _resolution_value(resolutions, row["pic"], "ror_id"),
                _resolution_value(resolutions, row["pic"], "confidence"),
                _resolution_value(resolutions, row["pic"], "source"),
            )
            for row in rows
        ]

    rows = list(
        conn.execute(
            """
            SELECT
                pic,
                participant_name,
                MIN(participant_order) AS participant_order,
                COUNT(*) AS seen
            FROM community_award_participants
            WHERE award_number = ?
            GROUP BY pic, participant_name
            ORDER BY participant_order, pic, participant_name
            """,
            (award_number,),
        )
    )
    best_by_pic: dict[str, sqlite3.Row] = {}
    for row in rows:
        current = best_by_pic.get(row["pic"])
        if current is None or row["seen"] > current["seen"]:
            best_by_pic[row["pic"]] = row
    ordered = sorted(best_by_pic.values(), key=lambda item: item["participant_order"])
    result: list[tuple[Any, ...]] = []
    for participant_order, row in enumerate(ordered, start=1):
        result.append(
            (
                award_number,
                participant_order,
                source,
                source_origin,
                row["pic"],
                row["participant_name"],
                "",
                "",
                "",
                _resolution_value(resolutions, row["pic"], "ror_id"),
                _resolution_value(resolutions, row["pic"], "confidence"),
                _resolution_value(resolutions, row["pic"], "source"),
            )
        )
    return result


def _resolution_value(
    resolutions: dict[str, sqlite3.Row],
    pic: str,
    key: str,
) -> str:
    row = resolutions.get(pic)
    return str(row[key]) if row is not None else ""


def _load_ror_names(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        row["ror_id"]: set(_json_list(row["normalized_names_json"]))
        for row in conn.execute("SELECT ror_id, normalized_names_json FROM ror_orgs")
    }


def _load_ror_display(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        row["ror_id"]: row["display_name"]
        for row in conn.execute("SELECT ror_id, display_name FROM ror_orgs")
    }


def _load_ror_fuzzy_index(conn: sqlite3.Connection) -> dict[str, Any]:
    by_country: dict[str, dict[str, list[tuple[str, str]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    block_choices: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for row in conn.execute(
        """
        SELECT ror_id, country_code, names_json
        FROM ror_orgs
        WHERE country_code != ''
        """
    ):
        for raw_name in _json_list(row["names_json"]):
            normalized_name = normalize_name(raw_name)
            if not _meaningful_fuzzy_name(normalized_name):
                continue
            by_country[row["country_code"]][normalized_name].append(
                (row["ror_id"], raw_name)
            )
            for block_key in _fuzzy_block_keys(normalized_name):
                block_choices[row["country_code"]][block_key].add(normalized_name)
    return {
        "blocks": {
            country_code: {
                block_key: tuple(sorted(names)) for block_key, names in matches.items()
            }
            for country_code, matches in block_choices.items()
        },
        "targets": by_country,
    }


def _load_ror_context(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    return {
        row["ror_id"]: {
            "country_code": row["country_code"],
            "domains": _json_list(row["domains_json"]),
            "fundref_ids": _json_list(row["fundref_ids_json"]),
            "grid_ids": _json_list(row["grid_ids_json"]),
            "isni_ids": _json_list(row["isni_ids_json"]),
            "name": row["display_name"],
            "names": _json_list(row["names_json"]),
            "normalized_names": _json_list(row["normalized_names_json"]),
            "ror_id": row["ror_id"],
            "website_links": _json_list(row["website_links_json"]),
            "wikidata_ids": _json_list(row["wikidata_ids_json"]),
        }
        for row in conn.execute(
            """
            SELECT
                ror_id,
                display_name,
                country_code,
                names_json,
                normalized_names_json,
                domains_json,
                website_links_json,
                grid_ids_json,
                isni_ids_json,
                wikidata_ids_json,
                fundref_ids_json
            FROM ror_orgs
            """
        )
    }


def _load_ror_indexes(conn: sqlite3.Connection) -> dict[str, Any]:
    external: dict[str, dict[str, set[str]]] = {
        "grid": defaultdict(set),
        "isni": defaultdict(set),
        "wikidata": defaultdict(set),
        "fundref": defaultdict(set),
    }
    domain = defaultdict(set)
    name_country = defaultdict(set)
    token_sort_name_country = defaultdict(set)
    for row in conn.execute(
        """
        SELECT
            ror_id,
            country_code,
            normalized_names_json,
            domains_json,
            grid_ids_json,
            isni_ids_json,
            wikidata_ids_json,
            fundref_ids_json
        FROM ror_orgs
        """
    ):
        for normalized_name in _json_list(row["normalized_names_json"]):
            if normalized_name:
                name_country[(normalized_name, row["country_code"])].add(row["ror_id"])
                token_sort_name = token_sort_key(normalized_name)
                if token_sort_name:
                    token_sort_name_country[(token_sort_name, row["country_code"])].add(
                        row["ror_id"]
                    )
        for host in _json_list(row["domains_json"]):
            if host:
                domain[host].add(row["ror_id"])
        for scheme, column in (
            ("grid", "grid_ids_json"),
            ("isni", "isni_ids_json"),
            ("wikidata", "wikidata_ids_json"),
            ("fundref", "fundref_ids_json"),
        ):
            for value in _json_list(row[column]):
                external[scheme][value].add(row["ror_id"])
    return {
        "external": external,
        "domain": domain,
        "name_country": name_country,
        "token_sort_name_country": token_sort_name_country,
    }


def _unique_ror_candidates_from_external_ids(
    row: sqlite3.Row,
    external_index: dict[str, dict[str, set[str]]],
) -> set[str]:
    candidates: set[str] = set()
    for scheme, column in (
        ("grid", "grid_ids_json"),
        ("isni", "isni_ids_json"),
        ("wikidata", "wikidata_ids_json"),
        ("fundref", "fundref_ids_json"),
    ):
        for value in _json_list(row[column]):
            matches = external_index[scheme].get(value, set())
            if len(matches) == 1:
                candidates.update(matches)
    return candidates if len(candidates) == 1 else set()


def _unique_ror_candidates_from_domains(
    domains: Sequence[str],
    domain_index: dict[str, set[str]],
) -> set[str]:
    candidates: set[str] = set()
    for domain in domains:
        matches = domain_index.get(domain, set())
        if len(matches) == 1:
            candidates.update(matches)
    return candidates if len(candidates) == 1 else set()


def _unique_ror_candidates_from_name_country(
    normalized_name: str,
    country_code: str,
    name_country_index: dict[tuple[str, str], set[str]],
) -> set[str]:
    if not normalized_name or not country_code:
        return set()
    candidates = name_country_index.get((normalized_name, country_code), set())
    return candidates if len(candidates) == 1 else set()


def _unique_ror_candidates_from_alias_names(
    raw_names: Sequence[str],
    country_code: str,
    name_country_index: dict[tuple[str, str], set[str]],
) -> AliasMatch:
    if not country_code:
        return AliasMatch(ror_ids=set(), matched_names=())
    candidates: set[str] = set()
    matched_names: list[str] = []
    for raw_name in raw_names:
        normalized = normalize_name(raw_name)
        if not _meaningful_alias_name(normalized):
            continue
        matches = name_country_index.get((normalized, country_code), set())
        if len(matches) == 1:
            candidates.update(matches)
            matched_names.append(raw_name)
    if len(candidates) != 1:
        return AliasMatch(ror_ids=set(), matched_names=())
    return AliasMatch(
        ror_ids=candidates,
        matched_names=tuple(sorted(set(matched_names))),
    )


def _unique_ror_candidates_from_token_sort_name_country(
    normalized_name: str,
    country_code: str,
    token_sort_name_country_index: dict[tuple[str, str], set[str]],
) -> set[str]:
    token_sort_name = token_sort_key(normalized_name)
    if not token_sort_name or token_sort_name == normalized_name or not country_code:
        return set()
    candidates = token_sort_name_country_index.get(
        (token_sort_name, country_code),
        set(),
    )
    return candidates if len(candidates) == 1 else set()


def _best_unique_fuzzy_match(
    raw_names: Sequence[str],
    country_code: str,
    fuzzy_index: dict[str, Any],
) -> FuzzyMatch | None:
    if not country_code:
        return None

    best_by_ror: dict[str, FuzzyMatch] = {}
    matched_names_by_ror: dict[str, set[str]] = defaultdict(set)
    for raw_name in raw_names:
        normalized_name = normalize_name(raw_name)
        if not _meaningful_fuzzy_name(normalized_name):
            continue
        choices = _fuzzy_blocked_choices(normalized_name, country_code, fuzzy_index)
        if not choices:
            continue
        candidate_names = _fuzzy_candidate_names(normalized_name, choices)
        for target_normalized in candidate_names:
            ratio = round(fuzz.ratio(normalized_name, target_normalized), 1)
            token_sort_ratio = round(
                fuzz.token_sort_ratio(normalized_name, target_normalized),
                1,
            )
            token_set_ratio = round(
                fuzz.token_set_ratio(normalized_name, target_normalized),
                1,
            )
            score = max(ratio, token_sort_ratio, token_set_ratio)
            for ror_id, target_name in fuzzy_index["targets"][country_code][
                target_normalized
            ]:
                matched_names_by_ror[ror_id].add(raw_name)
                current = best_by_ror.get(ror_id)
                if current is not None and score <= current.score:
                    continue
                best_by_ror[ror_id] = FuzzyMatch(
                    margin=0.0,
                    matched_names=(),
                    ratio=ratio,
                    ror_id=ror_id,
                    runner_up_score=0.0,
                    score=score,
                    source_name=raw_name,
                    source_normalized=normalized_name,
                    target_name=target_name,
                    target_normalized=target_normalized,
                    token_set_ratio=token_set_ratio,
                    token_sort_ratio=token_sort_ratio,
                )

    if not best_by_ror:
        return None

    ordered = sorted(
        best_by_ror.values(),
        key=lambda item: (
            -item.score,
            -item.token_set_ratio,
            -item.token_sort_ratio,
            -item.ratio,
            item.ror_id,
        ),
    )
    best = ordered[0]
    runner_up_score = ordered[1].score if len(ordered) > 1 else 0.0
    margin = round(best.score - runner_up_score, 1)
    if best.score < FUZZY_SCORE_THRESHOLD:
        return None
    if len(ordered) > 1 and margin < FUZZY_SCORE_MARGIN:
        return None
    return FuzzyMatch(
        margin=margin,
        matched_names=tuple(sorted(matched_names_by_ror[best.ror_id])),
        ratio=best.ratio,
        ror_id=best.ror_id,
        runner_up_score=runner_up_score,
        score=best.score,
        source_name=best.source_name,
        source_normalized=best.source_normalized,
        target_name=best.target_name,
        target_normalized=best.target_normalized,
        token_set_ratio=best.token_set_ratio,
        token_sort_ratio=best.token_sort_ratio,
    )


def _fuzzy_candidate_names(
    normalized_name: str,
    choices: Sequence[str],
) -> set[str]:
    candidate_names: set[str] = set()
    for scorer in (fuzz.ratio, fuzz.token_sort_ratio, fuzz.token_set_ratio):
        for match_name, _, _ in process.extract(
            normalized_name,
            choices,
            scorer=scorer,
            limit=FUZZY_RESULT_LIMIT,
            score_cutoff=FUZZY_SCORE_THRESHOLD,
        ):
            candidate_names.add(str(match_name))
    return candidate_names


def _fuzzy_block_keys(normalized_name: str) -> set[str]:
    compact = normalized_name.replace(" ", "")
    tokens = normalized_name.split()
    keys = set()
    if compact:
        keys.add(f"compact:{compact[:6]}")
    if tokens:
        keys.add(f"first:{tokens[0][:4]}")
        keys.add(f"last:{tokens[-1][:4]}")
    return keys


def _fuzzy_blocked_choices(
    normalized_name: str,
    country_code: str,
    fuzzy_index: dict[str, Any],
) -> tuple[str, ...]:
    blocked = fuzzy_index["blocks"].get(country_code, {})
    choices: set[str] = set()
    for block_key in _fuzzy_block_keys(normalized_name):
        choices.update(blocked.get(block_key, ()))
    return tuple(sorted(choices))


def _target_ror_summary(
    ror_context: dict[str, dict[str, Any]],
    ror_id: str,
) -> dict[str, Any]:
    target = ror_context.get(ror_id, {"ror_id": ror_id, "name": "", "country_code": ""})
    return {
        "country_code": target.get("country_code", ""),
        "name": target.get("name", ""),
        "ror_id": ror_id,
    }


def _no_score(why: str | None = None) -> dict[str, Any]:
    return {
        "parts": [],
        "used": False,
        "value": None,
        "why": why or "This rule is exact, so no similarity score was calculated.",
    }


def _fuzzy_score(fuzzy_match: FuzzyMatch) -> dict[str, Any]:
    return {
        "margin": fuzzy_match.margin,
        "method": "max(ratio, token_sort_ratio, token_set_ratio)",
        "parts": [
            {"name": "ratio", "value": fuzzy_match.ratio},
            {
                "name": "token_sort_ratio",
                "value": fuzzy_match.token_sort_ratio,
            },
            {
                "name": "token_set_ratio",
                "value": fuzzy_match.token_set_ratio,
            },
        ],
        "runner_up": fuzzy_match.runner_up_score,
        "threshold": FUZZY_SCORE_THRESHOLD,
        "used": True,
        "value": fuzzy_match.score,
        "why": (
            "Accepted because the best same-country ROR cleared the fuzzy "
            "threshold and stayed ahead of the next-best ROR by the minimum "
            "margin."
        ),
    }


def _serialize_fuzzy_match(fuzzy_match: FuzzyMatch) -> dict[str, Any]:
    return {
        "margin": fuzzy_match.margin,
        "matched_names": list(fuzzy_match.matched_names),
        "ratio": fuzzy_match.ratio,
        "ror_id": fuzzy_match.ror_id,
        "runner_up_score": fuzzy_match.runner_up_score,
        "score": fuzzy_match.score,
        "source_name": fuzzy_match.source_name,
        "source_normalized": fuzzy_match.source_normalized,
        "target_name": fuzzy_match.target_name,
        "target_normalized": fuzzy_match.target_normalized,
        "token_set_ratio": fuzzy_match.token_set_ratio,
        "token_sort_ratio": fuzzy_match.token_sort_ratio,
    }


def _deserialize_fuzzy_match(data: dict[str, Any]) -> FuzzyMatch:
    return FuzzyMatch(
        margin=float(data["margin"]),
        matched_names=tuple(data.get("matched_names", [])),
        ratio=float(data["ratio"]),
        ror_id=str(data["ror_id"]),
        runner_up_score=float(data["runner_up_score"]),
        score=float(data["score"]),
        source_name=str(data["source_name"]),
        source_normalized=str(data["source_normalized"]),
        target_name=str(data["target_name"]),
        target_normalized=str(data["target_normalized"]),
        token_set_ratio=float(data["token_set_ratio"]),
        token_sort_ratio=float(data["token_sort_ratio"]),
    )


def _matching_ror_name(
    ror_context: dict[str, dict[str, Any]],
    ror_id: str,
    normalized_name: str,
) -> str:
    target = ror_context.get(ror_id, {})
    for name in target.get("names", []):
        if normalize_name(name) == normalized_name:
            return name
    return target.get("name", "")


def _name_check(
    *,
    field: str,
    source_value: str,
    target_value: str,
) -> dict[str, Any]:
    return {
        "field": field,
        "source_normalized": normalize_name(source_value),
        "source_value": source_value,
        "target_normalized": normalize_name(target_value),
        "target_value": target_value,
    }


def _country_check(source_country: str, target_country: str) -> dict[str, Any]:
    return {
        "field": "country",
        "source_value": source_country,
        "target_value": target_country,
    }


def _openaire_source(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "alternative_names": _json_list(row["alt_names_json"]),
        "country_code": row["country_code"],
        "legal_name": row["legal_name"],
        "legal_short_name": row["legal_short_name"],
        "record_id": row["record_id"],
        "website_url": row["website_url"],
    }


def _participant_source(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "award_number": row["award_number"],
        "country_code": row["country_code"],
        "legal_name": row["legal_name"],
        "origin": row["origin"],
        "participant_order": row["participant_order"],
        "participant_type": row["participant_type"],
        "short_name": row["short_name"],
        "website_url": row["website_url"],
    }


def _matched_external_id_checks(
    row: sqlite3.Row,
    ror_context: dict[str, dict[str, Any]],
    ror_id: str,
) -> list[dict[str, Any]]:
    target = ror_context.get(ror_id, {})
    checks: list[dict[str, Any]] = []
    for field, row_key, target_key in (
        ("GRID id", "grid_ids_json", "grid_ids"),
        ("ISNI id", "isni_ids_json", "isni_ids"),
        ("Wikidata id", "wikidata_ids_json", "wikidata_ids"),
        ("FundRef id", "fundref_ids_json", "fundref_ids"),
    ):
        target_values = set(target.get(target_key, []))
        for value in _json_list(row[row_key]):
            if value in target_values:
                checks.append(
                    {
                        "field": field,
                        "source_value": value,
                        "target_value": value,
                    }
                )
    return checks


def _trace_candidate_rows(
    *,
    openaire_rows: Sequence[dict[str, Any]],
    participant_rows: Sequence[dict[str, Any]],
    community_rows: Sequence[dict[str, Any]],
    ror_context: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    return {
        "community": [_community_trace_row(row, ror_context) for row in community_rows],
        "openaire": [
            {"confidence": row["confidence"], **row["evidence"]}
            for row in openaire_rows
        ],
        "participant": [
            {"confidence": row["confidence"], **row["evidence"]}
            for row in participant_rows
        ],
    }


def _community_trace_row(
    row: dict[str, Any],
    ror_context: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    openaire_direct_link = "none"
    if row["agrees_with_openaire_direct"]:
        openaire_direct_link = "agrees"
    elif row["conflicts_with_openaire_direct"]:
        openaire_direct_link = "conflicts"
    return {
        "from": "Community metadata",
        "observations": [item["evidence"] for item in row["observations"]],
        "openaire_direct_link": openaire_direct_link,
        "status": row["community_status"],
        "target": _target_ror_summary(ror_context, row["ror_id"]),
        "times_seen": row["observation_count"],
        "unique_awards": row["award_count"],
        "unique_communities": row["slug_count"],
    }


def _rule_label(strategy: str) -> str:
    return {
        "direct_unique": "direct ROR link in OpenAIRE",
        "direct_ambiguous": "multiple direct ROR links in OpenAIRE",
        "external_id_unique": "same external id",
        "domain_unique": "same website domain",
        "name_country_unique": "same name in the same country",
        "alt_name_country_unique": "same alternative name in the same country",
        "fuzzy_name_country_unique": "close name in the same country",
        "fuzzy_alias_country_unique": "close alternative name in the same country",
        "short_name_country_unique": "same short name in the same country",
        "token_sort_name_country_unique": "same name in the same country, ignoring word order",
        "participant_fuzzy_name_country_unique": "close participant name in the same country",
        "participant_name_country_unique": "same participant name in the same country",
        "participant_token_sort_name_country_unique": "same participant name in the same country, ignoring word order",
    }.get(strategy, strategy.replace("_", " "))


def _rule_explanation(strategy: str) -> str:
    return {
        "direct_unique": "OpenAIRE already links this PIC to exactly one ROR.",
        "direct_ambiguous": "OpenAIRE lists more than one ROR for this PIC.",
        "external_id_unique": "An external identifier points to exactly one ROR.",
        "domain_unique": "The website domain points to exactly one ROR.",
        "name_country_unique": "The organization name, after normalization, matches one ROR in the same country.",
        "alt_name_country_unique": "An alternative organization name matches one ROR in the same country.",
        "fuzzy_name_country_unique": "A close organization name match points to one ROR in the same country.",
        "fuzzy_alias_country_unique": "A close alternative organization name match points to one ROR in the same country.",
        "short_name_country_unique": "A short organization name matches one ROR in the same country.",
        "token_sort_name_country_unique": "The normalized name matches one ROR in the same country after ignoring word order.",
        "participant_fuzzy_name_country_unique": "A close participant legal name match points to one ROR in the same country.",
        "participant_name_country_unique": "The participant legal name matches one ROR in the same country.",
        "participant_token_sort_name_country_unique": "The participant legal name matches one ROR in the same country after ignoring word order.",
    }.get(strategy, strategy.replace("_", " "))


def _resolution_source_label(source: str) -> str:
    return {
        "openaire_direct": "direct ROR link from OpenAIRE",
        "community_supported": "community match confirmed by other evidence",
        "openaire_ambiguous_external": "OpenAIRE had several RORs, external id picked one",
        "openaire_ambiguous_domain": "OpenAIRE had several RORs, website domain picked one",
        "openaire_ambiguous_exact_name": "OpenAIRE had several RORs, exact name picked one",
        "openaire_ambiguous_fuzzy_name": "OpenAIRE had several RORs, close name picked one",
        "openaire_ambiguous_token_sort": "OpenAIRE had several RORs, word-order-insensitive name picked one",
        "openaire_external_domain": "external id and website domain agree",
        "openaire_external_name": "external id and exact name agree",
        "openaire_domain_fuzzy_name": "website domain and close name agree",
        "openaire_domain_name": "website domain and exact name agree",
        "community_repeat": "same community-based match seen more than once",
        "openaire_external": "external id only",
        "openaire_domain": "website domain only",
        "openaire_name_country": "exact OpenAIRE name + country",
        "openaire_alt_name_country": "exact OpenAIRE alternative name + country",
        "openaire_fuzzy_name": "close OpenAIRE name + country",
        "openaire_fuzzy_alias_country": "close OpenAIRE alternative name + country",
        "openaire_short_name_country": "exact OpenAIRE short name + country",
        "participant_fuzzy_name": "close participant name + country",
        "participant_name_country": "exact participant name + country",
        "openaire_token_sort_name": "same OpenAIRE name + country, ignoring word order",
        "participant_token_sort_name": "same participant name + country, ignoring word order",
        "exact_name": "exact name + country",
        "fuzzy_name": "close name + country",
        "token_sort_name": "same name + country, ignoring word order",
        "community_single": "single community-based match",
    }.get(source, source.replace("_", " "))


def _support_group_label(group: str) -> str:
    return {
        "community": "community label",
        "direct_ambiguous": "direct ROR link, but ambiguous",
        "direct_unique": "direct ROR link",
        "domain": "website domain",
        "exact_name": "same name + same country",
        "external": "external id",
        "fuzzy_name": "close name + same country",
        "token_sort": "same name + same country, ignoring word order",
    }.get(group, group.replace("_", " "))


def _supporter_label(value: str) -> str:
    source, _, strategy = value.partition(":")
    where = {
        "community": "community data",
        "openaire": "OpenAIRE",
        "participant": "project participant data",
    }.get(source, source)
    return f"{where}: {_rule_label(strategy)}"


def _openaire_evidence(
    row: sqlite3.Row,
    strategy: str,
    ror_context: dict[str, dict[str, Any]],
    ror_id: str,
    **extra: Any,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "comparison": {
            "checked": [],
            "kind": "",
            "score": _no_score(),
        },
        "from": "OpenAIRE",
        "rule": _rule_label(strategy),
        "rule_code": strategy,
        "source": _openaire_source(row),
        "target": _target_ror_summary(ror_context, ror_id),
        "why": _rule_explanation(strategy),
    }
    target = ror_context.get(ror_id, {})
    if strategy == "external_id_unique":
        evidence["comparison"] = {
            "checked": _matched_external_id_checks(row, ror_context, ror_id),
            "kind": "exact identifier match",
            "score": _no_score(),
        }
    elif strategy == "domain_unique":
        source_domains = _json_list(row["domains_json"])
        target_domains = target.get("domains", [])
        matched_domain = next(
            (domain for domain in source_domains if domain in set(target_domains)),
            "",
        )
        evidence["comparison"] = {
            "checked": [
                {
                    "field": "website domain",
                    "matched_value": matched_domain,
                    "source_values": source_domains,
                    "target_values": target_domains,
                }
            ],
            "kind": "exact domain match",
            "score": _no_score(),
        }
    elif strategy == "name_country_unique":
        target_name = _matching_ror_name(ror_context, ror_id, row["normalized_name"])
        evidence["comparison"] = {
            "checked": [
                _name_check(
                    field="name",
                    source_value=row["legal_name"],
                    target_value=target_name,
                ),
                _country_check(row["country_code"], target.get("country_code", "")),
            ],
            "kind": "exact name + country",
            "score": _no_score(),
        }
    elif strategy in {"alt_name_country_unique", "short_name_country_unique"}:
        matched_names = [name for name in extra.get("matched_names", ()) if name]
        source_name = matched_names[0] if matched_names else row["legal_name"]
        target_name = _matching_ror_name(
            ror_context,
            ror_id,
            normalize_name(source_name),
        )
        name_check = _name_check(
            field="name",
            source_value=source_name,
            target_value=target_name,
        )
        if matched_names:
            name_check["source_values_seen"] = matched_names
        evidence["comparison"] = {
            "checked": [
                name_check,
                _country_check(row["country_code"], target.get("country_code", "")),
            ],
            "kind": "exact alias + country",
            "score": _no_score(),
        }
    elif strategy in {"fuzzy_name_country_unique", "fuzzy_alias_country_unique"}:
        fuzzy_match = _deserialize_fuzzy_match(extra["fuzzy_match"])
        name_check = _name_check(
            field="name",
            source_value=fuzzy_match.source_name,
            target_value=fuzzy_match.target_name,
        )
        if fuzzy_match.matched_names:
            name_check["source_values_seen"] = list(fuzzy_match.matched_names)
        evidence["comparison"] = {
            "checked": [
                name_check,
                _country_check(row["country_code"], target.get("country_code", "")),
            ],
            "kind": "fuzzy name + country",
            "score": _fuzzy_score(fuzzy_match),
        }
    elif strategy == "token_sort_name_country_unique":
        target_name = _matching_ror_name(ror_context, ror_id, row["normalized_name"])
        evidence["comparison"] = {
            "checked": [
                {
                    **_name_check(
                        field="name",
                        source_value=row["legal_name"],
                        target_value=target_name,
                    ),
                    "source_words_sorted": token_sort_key(row["normalized_name"]),
                    "target_words_sorted": token_sort_key(normalize_name(target_name)),
                },
                _country_check(row["country_code"], target.get("country_code", "")),
            ],
            "kind": "same words + same country",
            "score": _no_score(),
        }
    elif strategy.startswith("direct"):
        direct_rors = _json_list(row["rors_json"])
        evidence["comparison"] = {
            "checked": [
                {
                    "field": "ROR id",
                    "matched_value": ror_id,
                    "source_values": direct_rors,
                    "target_value": ror_id,
                }
            ],
            "kind": "direct ROR id check",
            "score": _no_score(),
        }
    evidence.update(extra)
    evidence.pop("matched_names", None)
    evidence.pop("fuzzy_match", None)
    evidence.pop("token_sort_name", None)
    return evidence


def _participant_evidence(
    row: sqlite3.Row,
    strategy: str,
    ror_context: dict[str, dict[str, Any]],
    ror_id: str,
    fuzzy_match: FuzzyMatch | None = None,
) -> dict[str, Any]:
    target = ror_context.get(ror_id, {})
    target_name = _matching_ror_name(
        ror_context,
        ror_id,
        row["normalized_legal_name"],
    )
    name_check = _name_check(
        field="participant name",
        source_value=row["legal_name"],
        target_value=target_name,
    )
    comparison_kind = "exact name + country"
    if strategy == "participant_token_sort_name_country_unique":
        name_check["source_words_sorted"] = token_sort_key(row["normalized_legal_name"])
        name_check["target_words_sorted"] = token_sort_key(normalize_name(target_name))
        comparison_kind = "same words + same country"
    elif (
        strategy == "participant_fuzzy_name_country_unique" and fuzzy_match is not None
    ):
        name_check = _name_check(
            field="participant name",
            source_value=fuzzy_match.source_name,
            target_value=fuzzy_match.target_name,
        )
        if fuzzy_match.matched_names:
            name_check["source_values_seen"] = list(fuzzy_match.matched_names)
        comparison_kind = "fuzzy name + country"
    return {
        "comparison": {
            "checked": [
                name_check,
                _country_check(row["country_code"], target.get("country_code", "")),
            ],
            "kind": comparison_kind,
            "score": (
                _fuzzy_score(fuzzy_match)
                if strategy == "participant_fuzzy_name_country_unique"
                and fuzzy_match is not None
                else _no_score()
            ),
        },
        "from": "Project participant data",
        "rule": _rule_label(strategy),
        "rule_code": strategy,
        "source": _participant_source(row),
        "target": _target_ror_summary(ror_context, ror_id),
        "why": _rule_explanation(strategy),
    }


def _community_observation_evidence(
    row: sqlite3.Row,
    community_org_name: str,
    ror_context: dict[str, dict[str, Any]],
    ror_id: str,
) -> dict[str, Any]:
    target = ror_context.get(ror_id, {})
    target_name = _matching_ror_name(ror_context, ror_id, row["normalized_name"])
    return {
        "comparison": {
            "checked": [
                _name_check(
                    field="participant name",
                    source_value=row["participant_name"],
                    target_value=target_name,
                ),
                {
                    "field": "community organization label",
                    "source_normalized": normalize_name(community_org_name),
                    "source_value": community_org_name,
                    "target_normalized": normalize_name(target.get("name", "")),
                    "target_value": target.get("name", ""),
                },
            ],
            "kind": "exact name inside one community",
            "score": _no_score(),
        },
        "from": "Community metadata",
        "rule": "Same participant name inside this community",
        "rule_code": "community_exact_name",
        "source": {
            "award_number": row["award_number"],
            "community_org_name": community_org_name,
            "community_slug": row["slug"],
            "participant_name": row["participant_name"],
        },
        "target": _target_ror_summary(ror_context, ror_id),
        "why": (
            "Inside this community, the participant name matched a single "
            "ROR-backed organization label."
        ),
    }


def _insert_participant_candidates(
    conn: sqlite3.Connection,
    *,
    pic: str,
    ror_ids: set[str],
    strategy: str,
    confidence: str,
    participant_source: str,
    source_origin: str,
    award_number: str,
    evidence_by_ror: dict[str, dict[str, Any]],
) -> int:
    inserted = 0
    for ror_id in sorted(ror_ids):
        # OR IGNORE: a PIC that appears in multiple participant_order rows
        # for the same award (same entity, multiple roles) reaches this path
        # repeatedly with identical evidence. The primary key absorbs the
        # duplicate.
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO participant_pic_ror_matches (
                pic,
                ror_id,
                strategy,
                confidence,
                participant_source,
                source_origin,
                award_number,
                evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pic,
                ror_id,
                strategy,
                confidence,
                participant_source,
                source_origin,
                award_number,
                json.dumps(evidence_by_ror[ror_id], sort_keys=True),
            ),
        )
        inserted += cursor.rowcount
    return inserted


def _parse_cordis_archives(values: Sequence[str]) -> list[CordisArchive]:
    archives: list[CordisArchive] = []
    for value in values:
        if "=" not in value:
            raise ValueError(
                f"Expected ORIGIN=PATH for --cordis-archive, got {value!r} instead."
            )
        origin, path = value.split("=", 1)
        origin = origin.strip().upper()
        if origin not in CORDIS_ORIGIN_PRIORITY:
            raise ValueError(
                "Unsupported CORDIS origin for pipeline import: "
                f"{origin!r}. Expected HE, H2020, or FP7."
            )
        archives.append(CordisArchive(origin=origin, path=Path(path).expanduser()))
    return archives


def normalize_name(value: str) -> str:
    """Normalize organization names without dropping meaning-bearing tokens.

    The earlier exploratory notebooks showed that overly aggressive stop-word
    removal collapses distinct institutions such as different "University of
    León" records. This normalizer only strips punctuation, case, accents, and
    trailing company suffixes.
    """

    folded = (
        unicodedata.normalize("NFKD", value or "")
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
        .replace("&", " and ")
    )
    cleaned = re.sub(r"[^a-z0-9]+", " ", folded).strip()
    if not cleaned:
        return ""
    tokens = cleaned.split()
    while tokens and tokens[-1] in CORPORATE_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def token_sort_key(normalized_name: str) -> str:
    """Sort normalized name tokens for order-insensitive exact matching."""
    tokens = [token for token in (normalized_name or "").split() if token]
    return " ".join(sorted(tokens))


def _meaningful_alias_name(normalized_name: str) -> bool:
    if not normalized_name:
        return False
    if normalized_name in CORPORATE_SUFFIXES:
        return False
    if normalized_name in GENERIC_SHORT_NAME_TOKENS:
        return False
    tokens = normalized_name.split()
    if not tokens:
        return False
    return not (len(tokens) == 1 and len(tokens[0]) < 4)


def _meaningful_fuzzy_name(normalized_name: str) -> bool:
    if not _meaningful_alias_name(normalized_name):
        return False
    tokens = normalized_name.split()
    if len(tokens) >= 2:
        return True
    return len(normalized_name) >= 8


def normalize_domain(value: str) -> str:
    """Extract a stable host name from URLs or naked domains."""
    candidate = (value or "").strip()
    if not candidate:
        return ""
    parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
    host = parsed.netloc or parsed.path
    host = host.lower().split("@")[-1].split(":", 1)[0].strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def _domains_from_urls(values: Sequence[str]) -> set[str]:
    return {host for value in values if (host := normalize_domain(value))}


def _short_ror_id(value: str | None) -> str:
    if not value:
        return ""
    candidate = value.rstrip("/").rsplit("/", 1)[-1]
    return candidate.strip()


def _normalize_identifier_value(scheme: str, value: str | None) -> str:
    candidate = (value or "").strip()
    if not candidate:
        return ""
    lowered = candidate.lower()
    if scheme == "isni":
        return re.sub(r"\s+", "", lowered)
    return lowered


def _extract_community_ror_id(org: dict[str, Any]) -> str:
    top_level = (org.get("id") or "").strip()
    if _looks_like_ror_id(top_level):
        return top_level
    for item in org.get("identifiers") or []:
        if item.get("scheme") == "ror":
            candidate = (item.get("identifier") or "").strip()
            if candidate:
                return _short_ror_id(candidate)
    return ""


def _looks_like_ror_id(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-z]{9}", value or ""))


def _classify_provided_id(value: str) -> str:
    stripped = (value or "").strip()
    if not stripped:
        return "none"
    if stripped.startswith("edmo:"):
        return "edmo"
    if _looks_like_ror_id(stripped):
        return "ror"
    return "other"


def _xml_text(node: ET.Element | None, path: str) -> str:
    if node is None:
        return ""
    text = node.findtext(path, default="")
    return text.strip() if text else ""


def _cordis_programme(root: ET.Element) -> str:
    codes: list[tuple[str, bool]] = []
    for programme in root.findall(".//{*}programme"):
        if programme.get("type") != "relatedLegalBasis":
            continue
        code = _xml_text(programme, "./{*}code")
        if not code:
            continue
        unique = programme.get("uniqueProgrammePart", "").lower() == "true"
        codes.append((code, unique))
    if not codes:
        return ""
    unique_codes = [code for code, unique in codes if unique]
    if unique_codes:
        return min(unique_codes, key=len)
    return min((code for code, _ in codes), key=len)


def _cordis_project_website(root: ET.Element) -> str:
    for link in root.findall(".//{*}webLink"):
        if link.get("type") != "relatedWebsite":
            continue
        if link.get("represents") != "project":
            continue
        if website := _xml_text(link, "./{*}physUrl"):
            return website
    return ""


def _award_url(award: dict[str, Any]) -> str:
    for item in award.get("identifiers") or []:
        if item.get("scheme") == "url" and item.get("identifier"):
            return str(item["identifier"]).strip()
    return ""


def _english_title(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("en") or next(iter(value.values()), "")).strip()
    return str(value or "").strip()


def _ror_country_code(raw: dict[str, Any]) -> str:
    for location in raw.get("locations") or []:
        details = location.get("geonames_details") or {}
        if details.get("country_code"):
            return str(details["country_code"]).strip()
    return ""


def _ror_display_name(raw: dict[str, Any]) -> str:
    for item in raw.get("names") or []:
        types = item.get("types") or []
        if "ror_display" in types or "label" in types:
            return str(item.get("value") or "").strip()
    for item in raw.get("names") or []:
        if item.get("value"):
            return str(item["value"]).strip()
    return ""


def _ror_external_ids(raw: dict[str, Any], kind: str) -> set[str]:
    values: set[str] = set()
    for item in raw.get("external_ids") or []:
        if item.get("type") != kind:
            continue
        for field in ("all", "preferred"):
            current = item.get(field)
            if isinstance(current, list):
                for value in current:
                    normalized = _normalize_identifier_value(kind, value)
                    if normalized:
                        values.add(normalized)
            elif isinstance(current, str):
                normalized = _normalize_identifier_value(kind, current)
                if normalized:
                    values.add(normalized)
    return values


def _single_value(values: set[str]) -> str:
    return next(iter(values)) if len(values) == 1 else ""


def _json_list(raw: str) -> list[str]:
    return list(json.loads(raw))


def _export_query_to_csv(
    conn: sqlite3.Connection,
    query: str,
    path: Path,
) -> None:
    cursor = conn.execute(query)
    rows = cursor.fetchall()
    headers = [column[0] for column in cursor.description or []]
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row[column] for column in headers])


if __name__ == "__main__":
    raise SystemExit(pipeline_main())
