CREATE TABLE IF NOT EXISTS crawl_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_full_name TEXT,
    homepage_url TEXT NOT NULL,
    site_origin TEXT NOT NULL,
    user_agent TEXT NOT NULL,
    status TEXT NOT NULL,
    failure_reason TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    homepage_final_url TEXT,
    homepage_http_code INTEGER,
    homepage_response_bytes INTEGER NOT NULL DEFAULT 0,
    robots_txt_present INTEGER NOT NULL DEFAULT 0,
    llms_txt_present INTEGER NOT NULL DEFAULT 0,
    llm_txt_present INTEGER NOT NULL DEFAULT 0,
    agents_json_present INTEGER NOT NULL DEFAULT 0,
    pages_discovered INTEGER NOT NULL DEFAULT 0,
    pages_crawled INTEGER NOT NULL DEFAULT 0,
    pages_blocked INTEGER NOT NULL DEFAULT 0,
    html_pages INTEGER NOT NULL DEFAULT 0,
    pages_with_title INTEGER NOT NULL DEFAULT 0,
    pages_with_meta_description INTEGER NOT NULL DEFAULT 0,
    pages_with_canonical INTEGER NOT NULL DEFAULT 0,
    pages_with_json_ld INTEGER NOT NULL DEFAULT 0,
    pages_with_open_graph INTEGER NOT NULL DEFAULT 0,
    pages_with_twitter_card INTEGER NOT NULL DEFAULT 0,
    findings_count INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (repository_full_name) REFERENCES repositories(full_name) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_crawl_runs_homepage_url
ON crawl_runs(homepage_url);

CREATE INDEX IF NOT EXISTS idx_crawl_runs_repository
ON crawl_runs(repository_full_name);

CREATE INDEX IF NOT EXISTS idx_crawl_runs_started_at
ON crawl_runs(started_at);

CREATE TABLE IF NOT EXISTS crawl_assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_run_id INTEGER NOT NULL,
    asset_type TEXT NOT NULL,
    asset_url TEXT NOT NULL,
    http_code INTEGER,
    response_bytes INTEGER NOT NULL DEFAULT 0,
    content_type TEXT,
    is_present INTEGER NOT NULL DEFAULT 0,
    parsed_ok INTEGER NOT NULL DEFAULT 0,
    item_count INTEGER,
    fetched_at TEXT NOT NULL,
    UNIQUE (crawl_run_id, asset_type, asset_url),
    FOREIGN KEY (crawl_run_id) REFERENCES crawl_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_crawl_assets_run_type
ON crawl_assets(crawl_run_id, asset_type);

CREATE TABLE IF NOT EXISTS crawl_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_run_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    final_url TEXT,
    referrer_url TEXT,
    depth INTEGER NOT NULL,
    allowed_by_robots INTEGER NOT NULL DEFAULT 1,
    http_code INTEGER,
    response_bytes INTEGER NOT NULL DEFAULT 0,
    content_type TEXT,
    is_html INTEGER NOT NULL DEFAULT 0,
    title TEXT,
    meta_description_length INTEGER,
    canonical_url TEXT,
    meta_robots TEXT,
    x_robots_tag TEXT,
    h1_count INTEGER NOT NULL DEFAULT 0,
    word_count INTEGER NOT NULL DEFAULT 0,
    internal_link_count INTEGER NOT NULL DEFAULT 0,
    external_link_count INTEGER NOT NULL DEFAULT 0,
    has_json_ld INTEGER NOT NULL DEFAULT 0,
    has_open_graph INTEGER NOT NULL DEFAULT 0,
    has_twitter_card INTEGER NOT NULL DEFAULT 0,
    fetched_at TEXT NOT NULL,
    UNIQUE (crawl_run_id, url),
    FOREIGN KEY (crawl_run_id) REFERENCES crawl_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_crawl_pages_run_depth
ON crawl_pages(crawl_run_id, depth);

CREATE INDEX IF NOT EXISTS idx_crawl_pages_run_allowed
ON crawl_pages(crawl_run_id, allowed_by_robots);

CREATE TABLE IF NOT EXISTS crawl_findings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_run_id INTEGER NOT NULL,
    category TEXT NOT NULL,
    code TEXT NOT NULL,
    severity TEXT NOT NULL,
    page_url TEXT,
    metric_value INTEGER,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (crawl_run_id) REFERENCES crawl_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_crawl_findings_run
ON crawl_findings(crawl_run_id);

CREATE INDEX IF NOT EXISTS idx_crawl_findings_code
ON crawl_findings(code);
