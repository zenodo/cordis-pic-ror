# cordis-pic-ror task runner. Run `just --list` for the full menu.

set dotenv-load := false

# Default: print the recipe list when no recipe is given.
default:
    @just --list

# ---------------------------------------------------------------------------
# Pipeline (raw imports + derived match tables + CSV exports)
# ---------------------------------------------------------------------------

# Where the heavy pipeline artifacts live (SQLite + rich CSVs).
# Override with `just pipeline DATA=/path/to/dumps` etc.
DATA := env_var_or_default("DATA", "tmp/data")
DB := env_var_or_default("DB", "tmp/org-pipeline/organizations.sqlite")

# Run the full pipeline (import + derive + report) against all three CORDIS
# frameworks plus the OpenAIRE / ROR / community dumps already in `tmp/data`.
# Adjust paths below if your dump set is different.
pipeline:
    uv run python scripts/pipeline.py \
        --db {{DB}} \
        --out tmp/org-pipeline/exports \
        --summary-json tmp/org-pipeline/summary.json \
        --cordis-archive HE={{DATA}}/cordis-HORIZONprojects-xml.zip \
        --cordis-archive H2020={{DATA}}/cordis-h2020projects-xml.zip \
        --cordis-archive FP7={{DATA}}/cordis-fp7projects-xml.zip \
        --community-dump {{DATA}}/zenodo-prod-communities.jsonl \
        --openaire-tar {{DATA}}/organization.tar \
        --ror-zip {{DATA}}/v2.4-2026-03-12-ror-data.zip

# Re-import raw datasets only (cheaper iteration than `just pipeline`).
import:
    uv run python scripts/import.py \
        --db {{DB}} \
        --cordis-archive HE={{DATA}}/cordis-HORIZONprojects-xml.zip \
        --cordis-archive H2020={{DATA}}/cordis-h2020projects-xml.zip \
        --cordis-archive FP7={{DATA}}/cordis-fp7projects-xml.zip \
        --community-dump {{DATA}}/zenodo-prod-communities.jsonl \
        --openaire-tar {{DATA}}/organization.tar \
        --ror-zip {{DATA}}/v2.4-2026-03-12-ror-data.zip

# Rebuild derived tables + CSV exports from the current SQLite (cheap).
derive:
    uv run python scripts/derive.py --db {{DB}} --out tmp/org-pipeline/exports

# Print + write the JSON summary of the current SQLite.
report:
    uv run python scripts/report.py --db {{DB}} --summary-json tmp/org-pipeline/summary.json

# ---------------------------------------------------------------------------
# Analysis & curation
# ---------------------------------------------------------------------------

# Coverage report against a ROR registry zip (writes to docs/ + tmp/).
coverage ROR_ZIP="tmp/data/v2.4-2026-03-12-ror-data.zip":
    uv run python scripts/coverage.py \
        --db {{DB}} \
        --ror-zip {{ROR_ZIP}} \
        --json-out tmp/org-pipeline/coverage.json \
        --markdown-out docs/organizations-coverage.md

# Suspicious community-org label review.
label-review:
    uv run python scripts/label_review.py --db {{DB}}

# Frequency-ranked manual-review queue for unresolved PICs.
review-queue:
    uv run python scripts/review_queue.py --db {{DB}} --out tmp/org-pipeline/exports/review_queue.csv

# ---------------------------------------------------------------------------
# Publishing artifacts
# ---------------------------------------------------------------------------

# Build the consumer-ready outputs in `output/` from a pipeline run.
# Merges `overrides/pic_ror_overrides.yaml` into the published CSV.
export:
    uv run python scripts/export_outputs.py

# Regenerate docs/data.js (current numbers used by the docs site).
docs:
    uv run python scripts/build_docs.py

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

# Drop derived/analytical state and start fresh.
clean:
    rm -rf tmp/org-pipeline

# Install Python deps via uv (idempotent).
sync:
    uv sync
