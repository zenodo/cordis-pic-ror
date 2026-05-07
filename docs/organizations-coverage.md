# Organizations ROR Coverage

Generated from `tmp/org-pipeline/organizations.sqlite` on 2026-03-15T12:21:37+00:00.

Produced with:

- `coverage.py`

Rerun from the repo root:

```bash
python coverage.py \
  --db tmp/org-pipeline/organizations.sqlite \
  --ror-zip tmp/data/v2.4-2026-03-12-ror-data.zip \
  --json-out tmp/org-pipeline/coverage.json \
  --markdown-out organizations-coverage.md
```

This report answers a narrow question:
if we only count participants that can be connected to ROR-backed linked
organization entries, how much of the current EOR Repository community set can we
cover?

## Scope

- Award-bearing active EU communities in scope: `2636`
- Active awards in scope: `2687`
- Communities with any participant list in the current pipeline: `2603` (`98.7%`)
- Communities without any participant list in the current pipeline: `33`

Important caveat:
the current pipeline DB was built with official HE CORDIS XML plus the
community dump fallback. Official H2020 and FP7 CORDIS archives were not
loaded for this report, so non-HE denominator completeness is still a bit
conservative / uneven.

## Key Numbers

- Median per community, `high+medium` ROR coverage: `61.5%`
- Median per community, `any` ROR coverage: `75%`
- Median per award, `high+medium` ROR coverage: `61.5%`
- Median per award, `any` ROR coverage: `75%`

## Coverage Bands Per Community

| High+Medium band | Communities |
| --- | ---: |
| 0% | 27 |
| >0-20% | 44 |
| >20-40% | 415 |
| >40-60% | 784 |
| >60-80% | 716 |
| >80-<100% | 260 |
| 100% | 357 |

| Any-confidence band | Communities |
| --- | ---: |
| 0% | 8 |
| >0-20% | 8 |
| >20-40% | 180 |
| >40-60% | 565 |
| >60-80% | 774 |
| >80-<100% | 539 |
| 100% | 529 |

## Coverage By Project Size

| Participant count | Communities | Median high+medium % | Median any % |
| --- | ---: | ---: | ---: |
| 1-5 | 456 | 100 | 100 |
| 6-10 | 550 | 66.7 | 77.8 |
| 11-20 | 1061 | 56.2 | 66.7 |
| 21-40 | 457 | 53.8 | 64 |
| 41+ | 79 | 55.1 | 64.4 |

## Coverage By Participant Source

| Source bucket | Communities | With participants | Median participants | Median high+medium % | Median any % |
| --- | ---: | ---: | ---: | ---: | ---: |
| cordis | 1253 | 1253 | 13 | 60 | 72.7 |
| community | 1343 | 1343 | 13 | 62.5 | 75 |
| mixed | 7 | 7 | 27 | 74.5 | 85.1 |
| none | 33 | 0 | - | - | - |

## Coordinator Coverage

This is only measurable where official CORDIS participant roles are present.

- Communities with official CORDIS participant rows: `1253`
- Communities with a coordinator resolved at `high+medium`: `1001` (`79.9%`)
- Communities with a coordinator resolved at `any`: `1138` (`90.8%`)

## Resolved ROR Types

These are only the participants that were successfully resolved to ROR.
ROR type labels are coarse; e.g. labs often land under `facility` or
`other` rather than a dedicated `lab` type.

| ROR type | Unique resolved PICs | Participant occurrences |
| --- | ---: | ---: |
| archive | 50 | 125 |
| company | 1416 | 3708 |
| education | 1491 | 11721 |
| facility | 770 | 3924 |
| funder | 231 | 2840 |
| government | 429 | 1249 |
| healthcare | 191 | 436 |
| nonprofit | 512 | 1910 |
| other | 372 | 1215 |

## Practical Reading

- If you only auto-write linked ROR org entries, expect roughly half the
  consortium to be filled in a strict automatic mode.
- If you also surface `review` matches for acceptance, expect closer to
  two-thirds.
- The coordinator is often covered even when the full consortium is not.
