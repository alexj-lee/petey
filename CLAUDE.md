# Paper Downloader — Project Guidelines

## Purpose
Download full-text articles from Elsevier ScienceDirect for LLM-agent knowledge graph construction. Running on a university server with institutional TDM API access.

## Rate Limiting & Backoff (CRITICAL)
- **Never** overwhelm Elsevier's servers. Be a polite API consumer.
- Base delay between requests: **1 second**
- Concurrent workers: **max 3**
- Exponential backoff on errors: `min(2^attempt * 1s + jitter, 120s)`
- On HTTP 429 (rate limit): respect `Retry-After` header; default wait 60s
- On HTTP 5xx: backoff + retry up to 5 times
- On HTTP 401/403: stop immediately, log, do not retry (key/auth issue)
- On HTTP 404: skip article, mark as unavailable, do not retry

## Storage Layout
```
/n1/paper/
  data/
    articles/          # Full-text XML files, named by PII
    metadata/          # Per-article JSON metadata
  db/
    progress.db        # SQLite tracking DB (search state + download state)
  logs/
    downloader.log
  CLAUDE.md
  .env                 # ELSEVIER_API_KEY (never commit)
  downloader.py        # Main download script
```

## API Notes
- Auth: `X-ELS-APIKey` header
- Full-text endpoint: `https://api.elsevier.com/content/article/doi/{doi}`
- Search endpoint: `https://api.elsevier.com/content/search/sciencedirect`
- Search paginates in chunks of 25; max offset 5999 per query (use date ranges to get beyond 6000)
- Prefer XML full-text (`Accept: application/xml`) for TDM use
- ScienceDirect search supports `ISSN()`, `PUBYEAR`, `DOCTYPE(ar)` filters

## Target Journals
Biomedical, materials science, and CS/AI journals on Elsevier/ScienceDirect. See `downloader.py` for the full list.

## Resumability
The SQLite DB tracks every article DOI/PII. Restart the script at any time — it skips already-downloaded or already-failed articles.
