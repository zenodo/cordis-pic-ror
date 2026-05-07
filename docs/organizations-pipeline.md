# Organizations Pipeline

This pipeline turns the datasets we profiled for EOR Repository subcommunities into a
rerunnable SQLite database plus a small set of CSV exports.

The current first cut focuses on the sources that gave the strongest signal for
PIC, participant, and ROR work:

- official CORDIS project XML archives
- Zenodo community metadata
- the OpenAIRE organization tarball
- the ROR dump

It does **not** yet ingest the CORDIS organization named graph.
That file is a very large keyed JSON-LD blob, and our earlier profiling showed
that it adds much less value than the project XML for participant work.
If we want it later, it should be added as a separate stage.

## Diagram

The interactive pipeline diagram has moved to [`index.html`](./index.html)
(rendered at [zenodo.github.io/cordis-pic-ror](https://zenodo.github.io/cordis-pic-ror)).

## Files

- `import.py`
  Imports raw source data into SQLite.
- `derive.py`
  Rebuilds the derived match tables and writes CSV exports.
- `report.py`
  Prints and writes a JSON summary of the current database.
- `coverage.py`
  Computes participant->ROR coverage from the built database and writes JSON or
  Markdown output.
- `pipeline.py`
  Runs import, derive, export, and report in one command, and also holds the
  shared implementation for every stage.
- `organizations-coverage.md`
  Current checked-in coverage snapshot for the active EU community set.

## Quick Start

Run the full pipeline:

```bash
python pipeline.py \
  --db tmp/org-pipeline/organizations.sqlite \
  --out tmp/org-pipeline/exports \
  --summary-json tmp/org-pipeline/summary.json \
  --cordis-archive HE=tmp/data/cordis-HORIZONprojects-xml.zip \
  --community-dump zenodo-prod-communities.jsonl \
  --openaire-tar tmp/data/organization.tar \
  --ror-zip tmp/data/v2.4-2026-03-12-ror-data.zip
```

Add more official CORDIS archives by repeating `--cordis-archive`:

```bash
python pipeline.py \
  --cordis-archive HE=tmp/data/cordis-HORIZONprojects-xml.zip \
  --cordis-archive H2020=tmp/data/cordis-h2020projects-xml.zip \
  --cordis-archive FP7=tmp/data/cordis-fp7projects-xml.zip \
  ...
```

Run stages separately:

```bash
python import.py ...
python derive.py --db tmp/org-pipeline/organizations.sqlite --out tmp/org-pipeline/exports
python report.py --db tmp/org-pipeline/organizations.sqlite --summary-json tmp/org-pipeline/summary.json
python coverage.py \
  --db tmp/org-pipeline/organizations.sqlite \
  --ror-zip tmp/data/v2.4-2026-03-12-ror-data.zip \
  --json-out tmp/org-pipeline/coverage.json \
  --markdown-out organizations-coverage.md
```

## Why The Pipeline Is Split

The raw imports are expensive.
The derived stages are cheap.

That split matters because:

- you may want to rerun match rules after changing code
- you may want to inspect the raw source tables before trusting a resolution
- some sources update at different cadences

Import replaces only the raw tables for the sources you pass in.
Derive always rebuilds every derived table from the current raw database.

## Raw Tables

### `cordis_projects`

One row per official CORDIS project record and origin.

Key fields:

- `award_number`
- `origin`
- `acronym`
- `title`
- `programme`
- `project_url`
- `project_website`

### `cordis_participants`

Participant rows from official CORDIS project XML.
This is the authoritative `award -> participant PIC` source whenever we have the
official archive for that framework.

Key fields:

- `award_number`
- `origin`
- `participant_order`
- `participant_type`
- `pic`
- `legal_name`
- `short_name`
- `country_code`
- `website_url`
- `vat_number`
- `rcn`

### `community_awards`

Funding rows extracted from `zenodo-prod-communities.jsonl` for communities whose
parent slug is `eu` by default.

Key fields:

- `slug`
- `award_number`
- `award_acronym`
- `award_title`
- `award_program`
- `award_url`

### `community_award_participants`

Embedded participant PIC rows from the same dump.
This is the best available backfill source when official CORDIS participant data
is not present in the pipeline DB.

Key fields:

- `slug`
- `award_number`
- `participant_order`
- `pic`
- `participant_name`

### `community_orgs`

Curated `metadata.organizations` rows from the community dump.

Important detail:
The top-level `id` field is **not** always a ROR ID.
Some records use other schemes such as `edmo:*`.

Stored fields:

- `provided_id`
- `provided_id_kind`
- `ror_id`
- `identifiers_json`

`ror_id` is extracted only from:

- a top-level `id` that looks like a ROR
- an identifier entry with `scheme == "ror"`

### `openaire_orgs`

Raw OpenAIRE organization records.

Key fields:

- `record_id`
- `legal_name`
- `legal_short_name`
- `alt_names_json`
- `country_code`
- `website_url`
- `pics_json`
- `rors_json`
- `grid_ids_json`
- `isni_ids_json`
- `wikidata_ids_json`
- `fundref_ids_json`

### `ror_orgs`

Imported ROR registry rows.

Key fields:

- `ror_id`
- `display_name`
- `country_code`
- `names_json`
- `normalized_names_json`
- `domains_json`
- `website_links_json`
- external identifier JSON columns

## Derived Tables

### `suspicious_community_orgs`

Flags curated community org labels whose displayed name does not match the alias
set of the linked ROR row.

This catches cases like:

- correct institution name, wrong campus ROR
- correct institution family, wrong national body
- non-ROR ids incorrectly stored in the same shape as ROR-backed orgs

This table is intentionally strict.
If a community org points to a ROR but its normalized name is not present in that
ROR record’s alias set, we treat it as suspicious.

### `openaire_pic_ror_matches`

PIC->ROR candidates derived from OpenAIRE plus the imported ROR registry.

Strategies:

- `direct_unique`
  OpenAIRE record already contains exactly one ROR for the PIC.
- `direct_ambiguous`
  OpenAIRE record contains multiple RORs for the PIC.
- `external_id_unique`
  No direct ROR, but OpenAIRE external ids map uniquely to one ROR.
- `domain_unique`
  No direct ROR, but the OpenAIRE website domain maps uniquely to one ROR.
- `name_country_unique`
  No direct ROR, but `(normalized legal name, country)` maps uniquely to one ROR.
- `alt_name_country_unique`
  An OpenAIRE alternative name maps uniquely to one ROR in the same country.
- `fuzzy_name_country_unique`
  A close OpenAIRE legal name match maps uniquely to one ROR in the same
  country.
- `fuzzy_alias_country_unique`
  A close OpenAIRE alias match maps uniquely to one ROR in the same country.
- `short_name_country_unique`
  An OpenAIRE short name maps uniquely to one ROR in the same country.
- `token_sort_name_country_unique`
  A token-sorted legal name maps uniquely to one ROR in the same country.

Confidence levels:

- `high`: direct OpenAIRE PIC->ROR
- `medium`: external-id or domain-only unique
- `review`: exact-name, token-sort, or ambiguous-direct support that still needs
  conservative handling

### `participant_pic_ror_matches`

PIC->ROR candidates derived directly from participant rows in official CORDIS
project XML.

Current scope:

- official CORDIS participants only
- only for PICs that do not appear in OpenAIRE

Strategies:

- `participant_name_country_unique`
  The participant legal name maps uniquely to one ROR in the same country.
- `participant_fuzzy_name_country_unique`
  A close participant legal name match maps uniquely to one ROR in the same
  country.
- `participant_token_sort_name_country_unique`
  The token-sorted participant legal name maps uniquely to one ROR in the same
  country.

This table exists mainly for the no-OpenAIRE tail and for traceability.
It lets us keep participant-derived evidence explicit instead of folding it
directly into the final resolver.

Each row's `evidence_json` now uses plain labels and shows the actual
comparison:

- `from`
- `rule` / `rule_code`
- `source`
- `target`
- `comparison`

`comparison.checked` holds the values that were compared.
For name rules this includes both raw and normalized names.
For fuzzy rules, `comparison.score` also includes:

- scoring method
- overall score
- threshold
- runner-up score
- margin
- the individual sub-scores used to make the decision

### `community_pic_ror_observations`

Exact-name observations built from the community dump.

Rule:

1. keep only community org rows with a real ROR id
2. drop any row flagged in `suspicious_community_orgs`
3. build a local alias map for each community from the linked ROR aliases
4. match award participant names against that alias map
5. keep only exact matches that resolve to a single ROR inside that community

Each observation row stores the participant name, the existing community org
label, the target ROR, and the exact values that were checked.

### `community_pic_ror_matches`

Aggregated version of the observation table.

Useful fields:

- `observation_count`
- `slug_count`
- `award_count`
- `community_status`
- `agrees_with_openaire_direct`
- `conflicts_with_openaire_direct`

### `pic_ror_resolutions`

Conservative final PIC->ROR table.

Resolution rules, in order:

1. `openaire_direct`
   Use the unique direct OpenAIRE PIC->ROR link.
2. `community_supported`
   Use a stable community-derived match that agrees with another unique
   candidate.
3. `openaire_ambiguous_external`
   An ambiguous direct OpenAIRE ROR list is narrowed down by external ids.
4. `openaire_ambiguous_domain`
   An ambiguous direct OpenAIRE ROR list is narrowed down by website domain.
5. `openaire_ambiguous_exact_name`
   An ambiguous direct OpenAIRE ROR list is narrowed down by exact same-country
   name evidence.
6. `openaire_ambiguous_token_sort`
   An ambiguous direct OpenAIRE ROR list is narrowed down by token-sort name
   evidence.
7. `openaire_ambiguous_fuzzy_name`
   An ambiguous direct OpenAIRE ROR list is narrowed down by fuzzy same-country
   name evidence.
8. `openaire_external_domain`
   External-id and domain strategies agree on one ROR.
9. `openaire_external_name`
   External-id and name+country strategies agree on one ROR.
10. `openaire_domain_name`
   Domain and name+country strategies agree on one ROR.
11. `openaire_domain_fuzzy_name`
   Domain and fuzzy same-country name strategies agree on one ROR.
12. `community_repeat`
   Stable community-derived match repeated at least twice.
13. `openaire_external`
14. `openaire_domain`
15. `openaire_name_country` / `openaire_alt_name_country` /
    `openaire_short_name_country`
16. `participant_name_country`
17. `openaire_token_sort_name` / `participant_token_sort_name`
18. `openaire_fuzzy_name` / `openaire_fuzzy_alias_country` /
    `participant_fuzzy_name`
19. `community_single`

Confidence values:

- `high`
- `medium`
- `review`

This table is deliberately conservative.
It is designed to be inspectable and easy to tighten, not to maximize automatic
coverage at any cost.

Important detail:
`pic_ror_resolutions.evidence_json` now stores a structured decision trace:

- `picked_match`
  The final ROR, confidence, plain-language rule label, short reason, and an
  explicit `score` block.
- `matches_checked`
  The OpenAIRE, participant, and community matches the resolver looked at.
  Each match row includes `from`, `rule`, `source`, `target`, and
  `comparison`.
- `agreement_summary`
  A short summary of which signal groups agreed on which ROR.

Important detail:
most rules are still exact rules, so many rows still have
`comparison.score.used = false`.
Fuzzy rules now set `comparison.score.used = true` and write the score details
into the same block.

That means a final mapping can now be traced in both directions:

1. start from `pic_ror_resolutions`
2. inspect its `evidence_json`
3. jump to the relevant candidate tables for the full row-level evidence
4. inspect `community_pic_ror_observations` when the community layer was involved

### `awards_best`

Best available project metadata per award number.

Priority:

1. official CORDIS
2. community funding metadata fallback

Within CORDIS, origin priority is:

1. `HE`
2. `H2020`
3. `FP7`

### `award_participants_best`

Best available participant table for downstream use in subcommunity validation
and metadata backfill.

Priority:

1. official CORDIS participants
2. community embedded award participants when official data is absent

Each row is enriched with:

- `ror_id`
- `resolution_confidence`
- `resolution_source`

This is the main table to use when you want:

- “which org PICs belong to award X?”
- “which ROR-backed orgs can we prefill for award X?”
- “can any submitter affiliation be matched against project participants?”

## CSV Exports

`derive.py` writes these files by default:

- `awards_best.csv`
- `award_participants_best.csv`
- `pic_ror_resolutions.csv`
- `community_pic_ror_matches.csv`
- `community_pic_ror_observations.csv`
- `openaire_pic_ror_matches.csv`
- `participant_pic_ror_matches.csv`
- `suspicious_community_orgs.csv`

These are intended for inspection, review, and ad hoc SQL-free analysis.
The SQLite DB remains the authoritative output.

## Name Normalization

The normalizer is intentionally conservative.

It does:

- case folding
- accent folding
- punctuation cleanup
- trailing company-suffix stripping

It does **not** remove institution-defining words such as `university`,
`institute`, or `research`.

Reason:
an earlier, more aggressive normalizer collapsed distinct institutions and
created false matches.

## Known Limits

- The pipeline does not yet ingest the CORDIS organization named graph.
- Community metadata is a strong label source, but not perfect ground truth.
- `community_single` and some `review` OpenAIRE-derived resolutions still need
  manual inspection before being treated as production-safe.
- The final resolution table is conservative by design and will leave some PICs
  unresolved.

## Expected Next Uses

Short term:

- use `award_participants_best` to show participant PICs and candidate RORs when
  validating a subcommunity request
- compare owner affiliation text or verified email domains against participant orgs
- prefill community metadata with resolved organizations and keep unresolved ones
  as name-only rows

Next engineering step:

- wire `award_participants_best` and `pic_ror_resolutions` into the TUI model and
  validation flow
- optionally add the CORDIS org graph as a later enrichment stage
