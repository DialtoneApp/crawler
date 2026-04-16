CREATE TABLE IF NOT EXISTS repositories (
    full_name TEXT PRIMARY KEY,
    owner_login TEXT NOT NULL,
    name TEXT NOT NULL,
    html_url TEXT NOT NULL,
    description TEXT,
    homepage_url TEXT,
    stars INTEGER NOT NULL,
    forks INTEGER NOT NULL,
    open_issues INTEGER NOT NULL,
    watchers INTEGER NOT NULL,
    language TEXT,
    default_branch TEXT,
    created_at TEXT,
    updated_at TEXT,
    pushed_at TEXT,
    archived INTEGER NOT NULL,
    is_fork INTEGER NOT NULL,
    search_rank INTEGER NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS repository_topics (
    full_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    PRIMARY KEY (full_name, topic),
    FOREIGN KEY (full_name) REFERENCES repositories(full_name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_repositories_search_rank
ON repositories(search_rank);

CREATE INDEX IF NOT EXISTS idx_repository_topics_topic
ON repository_topics(topic);
