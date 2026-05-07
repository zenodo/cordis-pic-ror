# AI-assisted manual review

`top_3000.yaml` holds AI-generated decisions for the top-3000 unresolved
PICs (sorted by total CORDIS occurrence). This file is a review staging
area, not the published overrides file. The human curator inspects it,
accepts or edits or rejects entries, and merges the kept decisions into
[`../pic_ror_overrides.yaml`](../pic_ror_overrides.yaml), which is what
gets uploaded to Zenodo alongside `pic_ror_resolutions.csv`.

## How it was produced

1. `just review-queue` ranked 65,674 unresolved PICs by total CORDIS
   occurrence across HE, H2020, and FP7.
2. Pass 1 (strict): 30 sub-agent batches of 100 PICs each scanned
   `legal_name` + `country` against ROR via web search. 1232 of 3000
   accepted, covering 24.2% of unresolved appearances.
3. Pass 2 (relaxed): the 1768 unresolved entries from pass 1 were
   re-scanned with richer signals (`website_url`, OpenAIRE `alt_names`,
   OpenAIRE external IDs covering wikidata/grid/isni/fundref) and a looser
   policy. When a subsidiary lacks a dedicated ROR but its parent group
   has one, the reviewer now accepts the parent. That added 410 accepts,
   of which 301 are explicitly generalized to a parent group.
4. Final merged: pass-1 accepts kept verbatim; pass-1 unresolved entries
   replaced by pass-2 results. 1642 of 2999 accepted, covering 30.3% of
   unresolved appearances and lifting the resolution rate on full
   participants by 12.4 percentage points.

## Entry schema

```yaml
- pic: "999997930"
  ror: "02feahw73"            # bare ROR id, or "" to leave unresolved
  name: "CNRS"                # editor display name; not consumed
  source: "ai-assisted"       # provenance; every entry in this file has it
  reasoning: "Pipeline matched via exact_name; 02feahw73 is the canonical
    CNRS ROR. Verified."
```

For parent-generalized entries the `reasoning` starts with
`"Generalized to parent: <name>."`. That string is the canonical marker;
grep it to filter accepts that traded subsidiary granularity for
coverage.

For unresolved entries the `reasoning` always states why (SME absent
from ROR, wrong-country subsidiary, ambiguous parents, and so on).

## Triage workflow

Suggested tactic for the human reviewer.

For high-frequency accepts (top of the file), scan the reasoning and
spot-check a handful by visiting `https://ror.org/<ror>`. Move kept ones
into `pic_ror_overrides.yaml`.

For generalized accepts, decide whether the parent-level granularity is
acceptable for your use case. If it is, keep them. If not, leave the PIC
unresolved in the overrides file with `ror: ""`.

For unresolved entries, the reasoning says why ROR lookup failed. There's
usually nothing to do, though you can try harder on a specific PIC if it
matters.

## Re-running

To regenerate from scratch:

1. Re-run the pipeline (`just pipeline`) to refresh
   `tmp/org-pipeline/organizations.sqlite` and the exports.
2. Re-run `just review-queue` for the new ranked CSV.
3. Replay the AI review by handing batches to a coding agent with the
   prompt template documented in the project history. Each batch is 100
   PICs; pass-1 uses the strict policy first, then pass-2 takes the
   relaxed policy over the strict-pass unresolved entries.

The aggregation script that produced this file lives in the commit
history; search for `top_3000_final.yaml`.
