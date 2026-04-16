CREATE TABLE IF NOT EXISTS crawls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    homepage_url TEXT NOT NULL,
    http_code INTEGER,
    response_bytes INTEGER NOT NULL,
    crawled_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_crawls_homepage_url
ON crawls(homepage_url);

CREATE INDEX IF NOT EXISTS idx_crawls_crawled_at
ON crawls(crawled_at);
