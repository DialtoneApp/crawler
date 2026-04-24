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
- Added `KeyboardInterrupt` handling so `Ctrl-C` writes a checkpoint immediately instead of only at the next periodic checkpoint boundary.
- Added login-wall detection so cross-host redirects into auth/SSO hosts are marked as `gated` instead of becoming positives.
- Tightened `llms`, `commerce`, and `x402` validation so HTML/script handoff pages do not count as valid machine-readable documents.
- Tightened UCP validation so placeholder/template payloads with values like `www.merchant.com` or `YOUR_FORTER_UCP_API_KEY` do not count as real merchant positives.
- Enriched valid catalog receipts with sample product details and valid UCP receipts with payment-handler details when available.
- Added follow-up enrichment for valid UCP roots by reading the versioned UCP document when it exposes richer payment-handler metadata.
- Added a lightweight public cart probe (`/cart.js`) for valid catalog sites so receipts can record cart currency and related basics.
- Increased byte caps for `openapi.json` and `products.json` so large but real documents are less likely to be misclassified as invalid.
- Reworked checkpoint progress tracking so the next row index is tracked explicitly instead of depending on the last loop variable.
- Added payment-surface enrichment so valid receipts can now capture provider hints and rails such as Shopify, Google Pay, x402, AsterPay, Nevermined, and crypto-oriented clues when they are actually present.
- Added stale-positive cleanup so recrawling a domain that no longer qualifies removes its old `results/positives/<domain>.json` file instead of leaving misleading residue behind.
- Improved catalog samples by deduplicating repeated products and attaching likely Shopify product URLs plus cart-derived currency when available.
- Reworked `/.well-known/commerce` parsing so API-first offers can count as real machine-buyable surfaces without a `products.json` catalog. The crawler now extracts priced sample offers, purchase-intent URLs, live machine-payment path statuses, and provider hints from the commerce document itself.
- Stopped treating generic OpenAPI `402 Payment Required` responses as `x402`. OpenAPI now records payment-challenge operations and `payment-signature` header support separately, so `x402` is only inferred from explicit x402-specific evidence.
- Extended commerce parsing to support alternate vendor schemas such as `offerings`, `priceCurrency`, `unit`, `checkout_url`, `paymentHandlers`, and `billing_provider`, so non-Shopify SaaS pricing manifests can still produce offer receipts.
- Tightened x402 validation so probe/example manifests that explicitly say they do not accept per-call x402 payments no longer count as live machine-payment surfaces.
- Added prelaunch/preorder detection from commerce status and billing-provider state, so priced offers that are documented but not yet live do not get upgraded to `machine_payable`.
- Added agent-discovery enrichment so valid `agent.json` documents can advertise follow-up public probes such as API OpenAPI URLs and public product endpoints. The crawler now uses those advertised URLs to enrich receipts for API-first commerce sites like x402 storefronts.
- Expanded product parsing beyond Shopify `products.json` to accept wrapped API payloads and record richer sample fields such as `sku`, `slug`, stock/preorder hints, and currency codes when publicly available.
- Raised byte caps for large `agent.json`, `agent-card.json`, `/.well-known/commerce`, and `x402` documents so modern discovery manifests like `emc2ai.io` no longer get dropped as truncated noise.
- Reworked HTTP outcome handling so validator-aware probes can inspect real `402 Payment Required` responses instead of collapsing them into generic `http_error`. This lets sites like `x402.quicknode.com` count as machine-payable from the actual payment challenge.
- Added action-surface extraction for API-first commerce:
  - OpenAPI payment-required operations now emit sample actions and runnable probe candidates.
  - x402 manifests now emit sample actions, priced action counts, payment networks/assets, and runnable probe candidates when the document contains enough detail.
  - agent docs now extract top-level `x402` endpoint maps and skill pricing into sample actions.
- Added a live `payment_probe` step that executes one discovered action per domain with a synthesized request body and records whether it returns:
  - a real `402` challenge
  - a successful unpaid response
  - input validation/auth boundaries
  - temporary service failures
- Folded those new signals into receipt classification and aggregates so positives now include:
  - `sample_actions`
  - `priced_action_count`
  - `payment_probe_*`
  - richer `x402_*` metadata
- Expanded `callable_surface` tagging to include agent cards, x402 manifests, and verified payment probes, since many API-first sites do not expose a separate retail catalog.
- Added payment-probe candidate ranking so the crawler prefers higher-intent actions like `send`, `buy`, `report`, or `search` over low-signal endpoints like `status` or message listing.
- Added templated payment-probe resolution for OpenAPI commerce sites. When a paid route uses a path parameter like `{slug}`, the crawler now tries to resolve a real public resource from a sibling collection endpoint before probing the paid URL, and it carries sample title/price details into the receipt when available.
- Added a Shopify UCP catalog fallback for stores that expose a valid `/.well-known/ucp` but block `products.json`. The crawler now extracts the store MCP endpoint from UCP, calls `search_catalog` with a valid Shopify agent profile, prefers a filters-only search first, and only falls back to a few neutral title/domain-derived query tokens when needed.
- Added off-origin x402 discovery so the crawler can follow `llms.txt` and `agent.json` hints to a remote `/.well-known/x402.json` manifest, validate it, and use its sample actions for the live payment probe. This fixes sites like `x402engine.app` that publish discovery on one host and execute paid actions on another.
- Raised x402 probe byte caps to `524 KB` so larger manifests do not get dropped as truncated noise before the crawler can extract priced actions and payment probes.

### Output model now

- `results/receipts/receipt-000001.ndjson` and later shards: compact receipt lines for all crawled domains
- `results/checkpoint.json`: explicit resume state
- `results/positives/*.json`: expanded JSON for interesting domains

### Checkpoint behavior

- Periodic checkpoints happen every `--checkpoint-every` completed domains.
- The default is still `100`.
- `Ctrl-C` now forces an immediate checkpoint with the latest completed row index, counts, and active shard state.

### Still to do

- Split the crawl into explicit broad-pass and enrichment-pass modes instead of doing them in one function.
- Add domain-range sharding so multiple Mac Studio processes can work on different slices of `top-1m.csv`.
- Add richer action-surface detection from homepage links and selective deep probes beyond `/openapi.json` and `/products.json`.
- Add post-processing scripts to build:
  - compact Cloudflare receipt payloads
  - category leaderboards
  - compare pages
  - report-ready aggregates
- Reduce noisy payment-host extraction from generic schema/docs URLs so hosts like `json-schema.org` do not leak into payment endpoint hints.
