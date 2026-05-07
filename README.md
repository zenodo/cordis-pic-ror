# cordis-pic-ror

Resolve CORDIS participant **PIC** identifiers to **ROR** identifiers using
CORDIS project XML, OpenAIRE organization records, the ROR registry, and a
curated community-metadata layer.

The pipeline produces a small, conservative `pic_mapping.csv` (plus a
richer `pic_mapping.jsonl` audit trail). Hand-curated overrides from
`pic_ror_overrides.yaml` are merged into the published files at export time,
so consumers fetch a single CSV. The output is uploaded to a versioned
Zenodo record; downstream tools (e.g.
[`invenio-vocabularies`](https://github.com/inveniosoftware/invenio-vocabularies)'
CORDIS award importer) read it to dereference `organizations[].id`
against the `affiliations` vocabulary.

Documentation site: [zenodo.github.io/cordis-pic-ror](https://zenodo.github.io/cordis-pic-ror).
Interactive HTML overview of the pipeline, resolution rules, curation
workflow, and current numbers. Source lives under `docs/`.

## Layout

```
.
├── Justfile                    # task runner; see `just --list`
├── scripts/                    # CLI entrypoints (run via `just <recipe>`)
│   ├── pipeline.py             # full pipeline (import + derive + report)
│   ├── import.py               # raw-data ingest only
│   ├── derive.py               # rebuild derived match tables + CSV exports
│   ├── report.py               # write JSON summary
│   ├── coverage.py             # participant -> ROR coverage report
│   ├── label_review.py         # suspicious community-org label review
│   ├── review_queue.py         # frequency-ranked manual-review queue
│   └── export_outputs.py         # produce diff-friendly `output/` from `tmp/`
├── output/                     # consumer-ready outputs (committed CSV; JSONL gitignored)
│   ├── pic_mapping.csv         # slim mapping: pic, ror_id, confidence, source
│   ├── pic_mapping.jsonl       # rich audit trail (evidence + override notes)
│   └── summary.json            # pipeline run statistics
├── overrides/
│   ├── pic_ror_overrides.yaml  # hand-curated overrides, merged into pic_mapping.csv at export time
│   └── ai_review/              # AI-assisted review staging (see README inside)
│       └── top_3000.yaml
├── docs/                       # GitHub Pages source (HTML site + reference docs)
│   ├── index.html              # interactive pipeline overview (rebuild via `just docs`)
│   ├── curation.html           # manual / AI-assisted curation workflow
│   ├── data.html               # current numbers (live data baked in by `just docs`)
│   ├── style.css · script.js · data.js
│   ├── organizations-pipeline.md     # dense schema + rules reference
│   └── organizations-coverage.md
└── tmp/                        # gitignored: full SQLite + rich CSVs
```

## Quick start

```bash
just sync                       # install Python deps via uv
just pipeline                   # run the full import + derive + report pipeline
just export                       # refresh the slim, committed outputs in output/
```

`just --list` shows the full menu. The pipeline expects the source dumps in
`tmp/data/` by default; override with `DATA=/path/to/dumps just pipeline`.

## Manual curation

Once the pipeline finishes, curate any PICs the resolver couldn't auto-resolve:

```bash
just review-queue               # write tmp/.../review_queue.csv ranked by frequency
```

Open `tmp/org-pipeline/exports/review_queue.csv` in a spreadsheet. The
cumulative-coverage stats printed by the command tell you how much work is
worth it; walk the top of the list accordingly. For each row:

- `pipeline_ror` set and plausible: accept by adding `pic` + `ror` to
  [`overrides/pic_ror_overrides.yaml`](./overrides/pic_ror_overrides.yaml).
- `pipeline_ror` set but clearly wrong: correct it, or leave unresolved
  with empty `ror`.
- `pipeline_ror` empty: look the institution up (e.g. on
  [ror.org/search](https://ror.org/search)) using `legal_name`, `country`,
  `website_url`, and any OpenAIRE alternative names or external IDs
  surfaced in the row.

[`overrides/ai_review/top_3000.yaml`](./overrides/ai_review/top_3000.yaml)
already contains AI-generated decisions for the top 3000 unresolved PICs
as a starting point. See
[`overrides/ai_review/README.md`](./overrides/ai_review/README.md) for the
schema and triage workflow.

## Manual overrides

`overrides/pic_ror_overrides.yaml` is the one file consumers actually read.
It lets a human override the pipeline's guess or explicitly leave a PIC
unresolved, without rerunning the pipeline.

```yaml
- pic: "999996549"
  ror: "01ggx4157"
  name: "European Organization for Nuclear Research (CERN)"
  note: "Promoted from `review`; OpenAIRE record was missing the ROR pid."

- pic: "888777666"
  ror: ""                       # empty = leave PIC unresolved
  name: "Some defunct subsidiary"
  note: "Pipeline matched the parent group; we only want exact PIC matches."
```

`name` and `note` are editor affordances; they are not consumed by
downstream tooling. Optional `source: ai-assisted` and `reasoning` fields
(used by `ai_review/top_3000.yaml`) are also ignored at consume time but
preserve provenance.

## Outputs

The pipeline writes to two locations.

`output/` is consumer-ready, refreshed by `just export` after every pipeline
run. Contents:

- `pic_mapping.csv` (~500 KB, committed) — `pic, ror_id, confidence, source`,
  with overrides already merged in (rows from `pic_ror_overrides.yaml` are
  emitted with `confidence=manual`, `source=manual_override`).
- `pic_mapping.jsonl` (~33 MB, gitignored) — same set of PICs with the
  pipeline's parsed `evidence` and any override note. Audit trail.
- `summary.json` (~2 KB, committed) — pipeline run statistics.

`tmp/org-pipeline/` (gitignored, ~750 MB) holds the full SQLite database
and the rich exports, including `evidence_json` traces,
`awards_best.csv`, `award_participants_best.csv`,
`openaire_pic_ror_matches.csv`, `participant_pic_ror_matches.csv`,
`community_pic_ror_*.csv`, and `suspicious_community_orgs.csv`. The SQLite
is the authoritative artifact for analytical work; re-run `just pipeline`
to recreate it.

## Publishing to Zenodo

Upload `output/pic_mapping.csv` (and optionally `output/pic_mapping.jsonl`
for the audit trail) to the same Zenodo record, one new version per
publish.

## Why the pipeline is split

Raw imports are slow because they parse hundreds of MB of source data;
the derived stages run in seconds against the resulting SQLite. Splitting
them lets you iterate on match rules without redoing the imports.
`just import` replaces only the raw tables for the sources you pass in,
`just derive` always rebuilds every derived table from the current raw
DB, and you can reach for either independently when you're tuning code or
inspecting raw rows before trusting a resolution.

See [`docs/organizations-pipeline.md`](./docs/organizations-pipeline.md)
for a full walkthrough of the schema, derived tables, and resolution
rules, or the rendered [docs site](https://zenodo.github.io/cordis-pic-ror)
for an interactive overview.
