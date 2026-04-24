# concurrent_crawl.py refactor notes

## 2026-04-23

### Why refactor

- The old crawler only checked a tiny fixed list of paths.
- It treated `200 + content-type` as a hit, which produced obvious false positives.
- It wrote one `<domain>.txt` marker file and threw away the evidence needed for receipts, rankings, and comparisons.
- Resume behavior depended on newest file mtime instead of an explicit checkpoint.

### First implementation slice

- Replaced the single-path `CrawlResult` flow with a per-domain receipt.
- Added baseline probes for homepage, crawl basics, llms files, commerce/agent discovery, OpenAPI, and x402.
- Added a conditional `/products.json` probe when the homepage or UCP hints at a catalog surface.
- Switched from `HEAD`/content-type-only detection to bounded `GET` fetches with body inspection.
- Added schema-ish validators for:
  - homepage
  - robots.txt
  - sitemap.xml
  - llms.txt / llms-full.txt
  - `/.well-known/commerce`
  - `/.well-known/ucp`
  - agent JSON
  - agents JSON
  - OpenAPI
  - x402
  - products catalogs
- Added control-path fallback detection so generic `200 application/json` responses do not count as real positives.
- Added compact NDJSON receipt logging plus:
  - `checkpoint.json`
  - `positives/<domain>.json`
- Replaced the single append-only receipt log with rotated receipt shards under `results/receipts/`.
- Added shard-aware checkpoint state so resume continues from the active shard instead of assuming one giant file.
- Increased byte caps for `openapi.json` and `products.json` so large but real documents are less likely to be misclassified as invalid.
- Reworked checkpoint progress tracking so the next row index is tracked explicitly instead of depending on the last loop variable.

### Output model now

- `results/receipts/receipt-000001.ndjson` and later shards: compact receipt lines for all crawled domains
- `results/checkpoint.json`: explicit resume state
- `results/positives/*.json`: expanded JSON for interesting domains

### Still to do

- Split the crawl into explicit broad-pass and enrichment-pass modes instead of doing them in one function.
- Add domain-range sharding so multiple Mac Studio processes can work on different slices of `top-1m.csv`.
- Add richer action-surface detection from homepage links and selective deep probes beyond `/openapi.json` and `/products.json`.
- Add post-processing scripts to build:
  - compact Cloudflare receipt payloads
  - category leaderboards
  - compare pages
  - report-ready aggregates
