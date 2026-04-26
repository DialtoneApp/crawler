# Crawler

Crawler for probing popular domains for machine-readable, callable, commerce, and payment surfaces.

The crawler reads a ranked domain CSV, writes compact receipt shards for every crawled domain, and writes expanded JSON/evidence only for domains with interesting signals.

## Requirements

- Python 3.10 or newer
- `curl` and `unzip` to fetch the Tranco input list

The crawler uses only the Python standard library.

## Download the Tranco top 1M

Download the latest standard Tranco list from the permanent URL documented at <https://tranco-list.eu/>:

```sh
curl -L -o top-1m.csv.zip https://tranco-list.eu/top-1m.csv.zip
unzip -p top-1m.csv.zip top-1m.csv > top-1m.csv
rm top-1m.csv.zip
```

The resulting `top-1m.csv` file is ignored by git.

## Run a crawl

Run a small smoke crawl first:

```sh
python3 concurrent_crawl.py --csv ./top-1m.csv --limit 100 --concurrency 8
```

Run the full crawl:

```sh
python3 concurrent_crawl.py --csv ./top-1m.csv --results-dir ./results --concurrency 24
```

By default, the crawler resumes from `results/checkpoint.json`. Use `--no-resume` to start reading the CSV from the beginning while appending new receipt rows.

Useful options:

```sh
python3 concurrent_crawl.py --help
```

## Outputs

- `results/receipts/receipt-*.ndjson`: one compact receipt per crawled domain
- `results/positives/*.json`: expanded receipts for domains with interesting signals
- `results/evidence/<domain>/`: selected raw evidence for interesting domains
- `results/checkpoint.json`: resume state

`results*/` directories are ignored by git.

## Export public artifacts

After a crawl, build the compact public export:

```sh
python3 export_public.py --results-dir ./results --output-dir ./results/exports/public --clean
```

## Build a targeted rerun slice

Create a smaller CSV from prior receipt shards:

```sh
python3 build_rerun_slice.py --results-dir ./results --csv ./top-1m.csv --output ./rerun.csv
python3 concurrent_crawl.py --csv ./rerun.csv --results-dir ./results-rerun
```

Generated `rerun*.csv` files are ignored by git.
