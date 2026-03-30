#!/usr/bin/env python3
"""
Elsevier ScienceDirect bulk downloader for TDM / knowledge-graph construction.

Downloads full-text XML (structured text) + PDF (images/figures) for each article.
Journals are processed in prestige tiers, each with its own year window:
  Tier 1 (flagship)    — 2018–present
  Tier 2 (high-impact) — 2015–present
  Tier 3 (solid/broad) — 2010–present  (skip with --no-tier3)

Rate-limiting:
  - Global token bucket: 1 request every 2 s (30 req/min) across ALL workers
  - Max 3 concurrent workers (search + download combined)
  - Exponential backoff with jitter (cap 120 s); Retry-After honoured on 429

Usage:
    source .env && python3 downloader.py
    source .env && python3 downloader.py --no-tier3
    source .env && python3 downloader.py --search-only
    source .env && python3 downloader.py --download-only
    source .env && python3 downloader.py --stats
"""

import argparse
import asyncio
import json
import logging
import os
import random
import sqlite3
from datetime import datetime
from pathlib import Path

import aiohttp
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Paths & config
# ---------------------------------------------------------------------------

API_KEY  = os.environ.get("ELSEVIER_API_KEY", "")
BASE_URL = "https://api.elsevier.com"
BASE_DIR = Path(__file__).parent

ARTICLES_DIR = BASE_DIR / "data" / "articles"   # XML files
PDFS_DIR     = BASE_DIR / "data" / "pdfs"        # PDF files
DB_PATH      = BASE_DIR / "db" / "progress.db"
LOG_PATH     = BASE_DIR / "logs" / "downloader.log"

for d in (ARTICLES_DIR, PDFS_DIR, DB_PATH.parent, LOG_PATH.parent):
    d.mkdir(parents=True, exist_ok=True)

GLOBAL_REQ_INTERVAL = 0.6   # seconds between any two requests across all workers (~100 req/min)
MAX_WORKERS         = 3     # max concurrent in-flight requests
MAX_RETRIES         = 5
BACKOFF_BASE        = 2.0
BACKOFF_CAP         = 120.0
SEARCH_PAGE_SIZE    = 25    # Elsevier hard max

NOW_YEAR = datetime.utcnow().year

# ---------------------------------------------------------------------------
# Journal registry
#
# (tier, if_rank, exact_pub_name, srctitle_query_term, field)
#
# tier     — year window: 1→2018+, 2→2015+, 3→2010+
# if_rank  — download priority (lower = sooner); approximate Impact Factor band
#   1: IF ≥ 20    2: IF 10–20    3: IF 6–10    4: IF 3–6    5: IF < 3
#
# exact_pub_name is used for client-side post-filtering (srctitle is fuzzy).
# ---------------------------------------------------------------------------

JOURNALS = [
    # ══ Tier 1 ══ flagship journals, 2018–present ═══════════════════════════

    # ── Lancet family ────────────────────────────────────────────────────────
    (1, 1, "The Lancet",                              "The Lancet",                               "bio"),  # IF ~88
    (1, 1, "The Lancet Respiratory Medicine",         "The Lancet Respiratory Medicine",          "bio"),  # IF ~102
    (1, 1, "The Lancet Psychiatry",                   "The Lancet Psychiatry",                    "bio"),  # IF ~77
    (1, 1, "The Lancet Public Health",                "The Lancet Public Health",                 "bio"),  # IF ~72
    (1, 1, "The Lancet Infectious Diseases",          "The Lancet Infectious Diseases",           "bio"),  # IF ~71
    (1, 1, "The Lancet Diabetes & Endocrinology",     "The Lancet Diabetes and Endocrinology",    "bio"),  # IF ~44
    (1, 1, "The Lancet Oncology",                     "The Lancet Oncology",                      "bio"),  # IF ~40
    (1, 1, "The Lancet Neurology",                    "The Lancet Neurology",                     "bio"),  # IF ~30
    (1, 1, "The Lancet Gastroenterology & Hepatology","The Lancet Gastroenterology and Hepatology","bio"), # IF ~30
    (1, 1, "The Lancet Haematology",                  "The Lancet Haematology",                   "bio"),  # IF ~24
    (1, 1, "The Lancet Digital Health",               "The Lancet Digital Health",                "bio"),  # IF ~23
    (1, 3, "eClinicalMedicine",                       "eClinicalMedicine",                        "bio"),  # IF ~9

    # ── Cell Press flagship ──────────────────────────────────────────────────
    (1, 1, "Cell",                                    "Cell",                                     "bio"),  # IF ~67
    (1, 1, "Immunity",                                "Immunity",                                 "bio"),  # IF ~43
    (1, 1, "Cell Metabolism",                         "Cell Metabolism",                          "bio"),  # IF ~29
    (1, 1, "Cell Host & Microbe",                     "Cell Host and Microbe",                    "bio"),  # IF ~30
    (1, 1, "Cancer Cell",                             "Cancer Cell",                              "bio"),  # IF ~24
    (1, 2, "Molecular Cell",                          "Molecular Cell",                           "bio"),  # IF ~16
    (1, 2, "Cell Stem Cell",                          "Cell Stem Cell",                           "bio"),  # IF ~12
    (1, 2, "Cell Reports Medicine",                   "Cell Reports Medicine",                    "bio"),  # IF ~14
    (1, 2, "Cell Genomics",                           "Cell Genomics",                            "bio"),  # IF ~12
    (1, 2, "Molecular Therapy",                       "Molecular Therapy",                        "bio"),  # IF ~12
    (1, 3, "Neuron",                                  "Neuron",                                   "bio"),  # IF ~9
    (1, 3, "EBioMedicine",                            "EBioMedicine",                             "bio"),  # IF ~9
    (1, 3, "Developmental Cell",                      "Developmental Cell",                       "bio"),  # IF ~6
    (1, 4, "Cell Systems",                            "Cell Systems",                             "bio"),  # IF ~6

    # ── Tier 1 Materials ────────────────────────────────────────────────────
    (1, 1, "Joule",                                   "Joule",                                    "mat"),  # IF ~38
    (1, 1, "Progress in Materials Science",           "Progress in Materials Science",            "mat"),  # IF ~33
    (1, 1, "Materials Today",                         "Materials Today",                          "mat"),  # IF ~24
    (1, 2, "Nano Energy",                             "Nano Energy",                              "mat"),  # IF ~17
    (1, 2, "Carbon",                                  "Carbon",                                   "mat"),  # IF ~11
    (1, 3, "Acta Materialia",                         "Acta Materialia",                          "mat"),  # IF ~9

    # ── Tier 1 CS / AI ──────────────────────────────────────────────────────
    (1, 2, "Artificial Intelligence",                 "Artificial Intelligence",                  "cs"),   # IF ~14
    (1, 2, "Information Fusion",                      "Information Fusion",                       "cs"),   # IF ~18
    (1, 3, "Pattern Recognition",                     "Pattern Recognition",                      "cs"),   # IF ~8
    (1, 4, "Neural Networks",                         "Neural Networks",                          "cs"),   # IF ~6
    (1, 4, "Knowledge-Based Systems",                 "Knowledge-Based Systems",                  "cs"),   # IF ~7

    # ══ Tier 2 ══ high-impact, 2015–present ══════════════════════════════════

    # ── Tier 2 Biomedical ───────────────────────────────────────────────────
    (2, 2, "Biomaterials",                            "Biomaterials",                             "bio"),  # IF ~14
    (2, 2, "Biosensors and Bioelectronics",           "Biosensors and Bioelectronics",            "bio"),  # IF ~11
    (2, 2, "Journal of Controlled Release",           "Journal of Controlled Release",            "bio"),  # IF ~11
    (2, 2, "Redox Biology",                           "Redox Biology",                            "bio"),  # IF ~11
    (2, 3, "Cell Reports",                            "Cell Reports",                             "bio"),  # IF ~9
    (2, 3, "Acta Biomaterialia",                      "Acta Biomaterialia",                       "bio"),  # IF ~9
    (2, 3, "Cell Chemical Biology",                   "Cell Chemical Biology",                    "bio"),  # IF ~7
    (2, 4, "iScience",                                "iScience",                                 "bio"),  # IF ~4

    # ── Tier 2 Materials ────────────────────────────────────────────────────
    (2, 2, "Composites Part B: Engineering",          "Composites Part B",                        "mat"),  # IF ~13
    (2, 3, "Composites Science and Technology",       "Composites Science and Technology",        "mat"),  # IF ~9
    (2, 3, "Journal of Power Sources",                "Journal of Power Sources",                 "mat"),  # IF ~9
    (2, 3, "Corrosion Science",                       "Corrosion Science",                        "mat"),  # IF ~7
    (2, 4, "Scripta Materialia",                      "Scripta Materialia",                       "mat"),  # IF ~5
    (2, 4, "Journal of Alloys and Compounds",         "Journal of Alloys and Compounds",          "mat"),  # IF ~6
    (2, 4, "Applied Surface Science",                 "Applied Surface Science",                  "mat"),  # IF ~6
    (2, 4, "Surface and Coatings Technology",         "Surface and Coatings Technology",          "mat"),  # IF ~5

    # ── Tier 2 CS / AI ──────────────────────────────────────────────────────
    (2, 3, "Expert Systems with Applications",        "Expert Systems with Applications",         "cs"),   # IF ~8
    (2, 3, "Swarm and Evolutionary Computation",      "Swarm and Evolutionary Computation",       "cs"),   # IF ~8
    (2, 3, "Engineering Applications of Artificial Intelligence",
                                                      "Engineering Applications of Artificial Intelligence", "cs"),  # IF ~8
    (2, 3, "Information Processing and Management",   "Information Processing and Management",    "cs"),   # IF ~8
    (2, 3, "Applied Soft Computing",                  "Applied Soft Computing",                   "cs"),   # IF ~7
    (2, 3, "Future Generation Computer Systems",      "Future Generation Computer Systems",       "cs"),   # IF ~7
    (2, 4, "Information Sciences",                    "Information Sciences",                     "cs"),   # IF ~6
    (2, 4, "Neurocomputing",                          "Neurocomputing",                           "cs"),   # IF ~6
    (2, 4, "Pattern Recognition Letters",             "Pattern Recognition Letters",              "cs"),   # IF ~5

    # ══ Tier 3 ══ solid journals, 2010–present ════════════════════════════════

    # ── Tier 3 Biomedical ───────────────────────────────────────────────────
    (3, 3, "Pharmacological Research",                "Pharmacological Research",                 "bio"),  # IF ~7
    (3, 4, "Free Radical Biology and Medicine",       "Free Radical Biology and Medicine",        "bio"),  # IF ~6
    (3, 4, "Journal of Biomedical Informatics",       "Journal of Biomedical Informatics",        "bio"),  # IF ~4
    (3, 4, "Journal of Proteomics",                   "Journal of Proteomics",                    "bio"),  # IF ~4
    (3, 4, "Computers in Biology and Medicine",       "Computers in Biology and Medicine",        "bio"),  # IF ~4
    (3, 5, "Neuroscience",                            "Neuroscience",                             "bio"),  # IF ~3
    (3, 5, "Brain Research",                          "Brain Research",                           "bio"),  # IF ~3

    # ── Tier 3 Materials ────────────────────────────────────────────────────
    (3, 4, "Materials Science and Engineering: A",    "Materials Science and Engineering A",      "mat"),  # IF ~5
    (3, 4, "Ceramics International",                  "Ceramics International",                   "mat"),  # IF ~5
    (3, 5, "Journal of Nuclear Materials",            "Journal of Nuclear Materials",             "mat"),  # IF ~3
    (3, 5, "Journal of Magnetism and Magnetic Materials", "Journal of Magnetism and Magnetic Materials", "mat"),  # IF ~3
    (3, 5, "Materials Letters",                       "Materials Letters",                        "mat"),  # IF ~3
    (3, 5, "Thin Solid Films",                        "Thin Solid Films",                         "mat"),  # IF ~2

    # ── Tier 3 CS / AI ──────────────────────────────────────────────────────
    (3, 4, "Computer Vision and Image Understanding", "Computer Vision and Image Understanding",  "cs"),   # IF ~4
    (3, 4, "Journal of Parallel and Distributed Computing", "Journal of Parallel and Distributed Computing", "cs"),  # IF ~4
    (3, 4, "Computer Networks",                       "Computer Networks",                        "cs"),   # IF ~5
    (3, 5, "Data and Knowledge Engineering",          "Data and Knowledge Engineering",           "cs"),   # IF ~3
]

TIER_YEAR_START = {1: 2000, 2: 2015, 3: 2010}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_PATH)),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS search_progress (
            journal     TEXT    NOT NULL,
            year        INTEGER NOT NULL,
            next_start  INTEGER NOT NULL DEFAULT 0,
            total       INTEGER,
            done        INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (journal, year)
        );
        CREATE TABLE IF NOT EXISTS articles (
            doi             TEXT    PRIMARY KEY,
            journal         TEXT,
            title           TEXT,
            year            INTEGER,
            tier            INTEGER,
            if_rank         INTEGER,
            field           TEXT,
            status          TEXT    NOT NULL DEFAULT 'pending',
            -- pending | downloaded | unavailable | error
            pdf_downloaded  INTEGER NOT NULL DEFAULT 0,  -- 1 = PDF saved
            attempts        INTEGER NOT NULL DEFAULT 0,
            updated_at      TEXT
        );
    """)
    conn.commit()
    # Migrate older DBs that predate these columns (idempotent)
    for col, defn in [("if_rank", "INTEGER"), ("pdf_downloaded", "INTEGER NOT NULL DEFAULT 0")]:
        try:
            conn.execute(f"ALTER TABLE articles ADD COLUMN {col} {defn}")
            conn.commit()
        except sqlite3.OperationalError:
            pass
    return conn


def upsert_article(conn, doi, journal, title, year, tier, if_rank, field):
    conn.execute("""
        INSERT INTO articles (doi, journal, title, year, tier, if_rank, field, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(doi) DO NOTHING
    """, (doi, journal, title, year, tier, if_rank, field, datetime.utcnow().isoformat()))
    conn.commit()


def mark_article(conn, doi, status, pdf_downloaded=None):
    if pdf_downloaded is not None:
        conn.execute("""
            UPDATE articles SET status=?, pdf_downloaded=?, attempts=attempts+1, updated_at=?
            WHERE doi=?
        """, (status, int(pdf_downloaded), datetime.utcnow().isoformat(), doi))
    else:
        conn.execute("""
            UPDATE articles SET status=?, attempts=attempts+1, updated_at=?
            WHERE doi=?
        """, (status, datetime.utcnow().isoformat(), doi))
    conn.commit()


def pending_articles(conn):
    """Ordered by IF rank (highest IF first) then year (newest first)."""
    return conn.execute("""
        SELECT doi FROM articles
        WHERE status='pending' AND attempts < ?
        ORDER BY COALESCE(if_rank, 9) ASC, year DESC, ROWID ASC
    """, (MAX_RETRIES,)).fetchall()

# ---------------------------------------------------------------------------
# Global rate limiter — token bucket shared across all workers
# ---------------------------------------------------------------------------

class RateLimiter:
    def __init__(self, interval: float):
        self._interval = interval
        self._lock     = asyncio.Lock()
        self._next_ok  = 0.0

    async def acquire(self):
        async with self._lock:
            now  = asyncio.get_event_loop().time()
            wait = self._next_ok - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._next_ok = asyncio.get_event_loop().time() + self._interval

    def pause(self, seconds: float):
        """Called on 429: block all workers for `seconds`."""
        self._next_ok = asyncio.get_event_loop().time() + seconds


_rate_limiter: RateLimiter | None = None

# ---------------------------------------------------------------------------
# HTTP — global rate limit + exponential backoff
# ---------------------------------------------------------------------------

def _backoff(attempt: int) -> float:
    return min(BACKOFF_BASE ** attempt + random.uniform(0, 1), BACKOFF_CAP)


async def get_with_backoff(session: aiohttp.ClientSession, url: str, **kwargs):
    """Returns (http_status, bytes_or_None)."""
    for attempt in range(MAX_RETRIES):
        await _rate_limiter.acquire()
        try:
            async with session.get(url, **kwargs) as resp:
                if resp.status == 200:
                    return resp.status, await resp.read()
                if resp.status == 429:
                    wait = int(resp.headers.get("Retry-After", 60))
                    log.warning("429 — pausing all workers %ds (attempt %d)", wait, attempt + 1)
                    _rate_limiter.pause(wait)
                    await asyncio.sleep(wait)
                    continue
                if resp.status in (401, 403):
                    log.error("Auth error %d: %s", resp.status, url)
                    return resp.status, None
                if resp.status == 404:
                    return resp.status, None
                if resp.status >= 500:
                    delay = _backoff(attempt)
                    log.warning("%d server error — retry %d/%d in %.1fs", resp.status, attempt + 1, MAX_RETRIES, delay)
                    await asyncio.sleep(delay)
                    continue
                return resp.status, None
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            delay = _backoff(attempt)
            log.warning("Network error %s — retry %d/%d in %.1fs", exc, attempt + 1, MAX_RETRIES, delay)
            await asyncio.sleep(delay)
    log.error("Exhausted retries: %s", url)
    return -1, None

# ---------------------------------------------------------------------------
# Phase 1: Search / index
# ---------------------------------------------------------------------------

async def search_journal_year(session, conn, tier, if_rank, exact_name, search_term, field, year, lock):
    row = conn.execute(
        "SELECT next_start, done FROM search_progress WHERE journal=? AND year=?",
        (exact_name, year)
    ).fetchone()
    if row and row[1]:
        return
    start = row[0] if row else 0

    query = f"srctitle({search_term}) AND PUBYEAR IS {year}"
    total = None
    found = 0

    while True:
        url = (f"{BASE_URL}/content/search/sciencedirect"
               f"?query={query}&count={SEARCH_PAGE_SIZE}&start={start}"
               f"&field=doi,title,publicationName,coverDate")
        status, body = await get_with_backoff(
            session, url,
            headers={"X-ELS-APIKey": API_KEY, "Accept": "application/json"}
        )
        if status != 200 or not body:
            log.error("Search failed (%d) for %r year=%d", status, exact_name, year)
            break

        data    = json.loads(body)
        results = data.get("search-results", {})

        if total is None:
            total = int(results.get("opensearch:totalResults", 0))
            async with lock:
                conn.execute("""
                    INSERT INTO search_progress (journal, year, next_start, total)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(journal, year) DO UPDATE SET total=excluded.total
                """, (exact_name, year, start, total))
                conn.commit()

        entries = results.get("entry", [])
        if not entries:
            break

        matched = [
            e for e in entries
            if e.get("prism:publicationName", "").strip().lower() == exact_name.lower()
        ]
        async with lock:
            for e in matched:
                doi   = e.get("prism:doi", "").strip()
                title = e.get("dc:title", "").strip()
                cover = e.get("prism:coverDate", "")
                yr    = int(cover[:4]) if cover and len(cover) >= 4 else year
                if doi:
                    upsert_article(conn, doi, exact_name, title, yr, tier, if_rank, field)
                    found += 1

        start += len(entries)
        async with lock:
            conn.execute("UPDATE search_progress SET next_start=? WHERE journal=? AND year=?",
                         (start, exact_name, year))
            conn.commit()

        if start >= (total or 0) or start >= 6000:
            break

    async with lock:
        conn.execute("""
            INSERT INTO search_progress (journal, year, next_start, total, done)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(journal, year) DO UPDATE SET done=1, next_start=excluded.next_start
        """, (exact_name, year, start, total or 0))
        conn.commit()

    if found:
        log.info("T%d IF%d %-50s [%d]: %d articles", tier, if_rank, exact_name, year, found)


async def run_search_phase(conn, skip_tier3=False):
    log.info("=== PHASE 1: Indexing (skip_tier3=%s) ===", skip_tier3)
    lock      = asyncio.Lock()
    timeout   = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=MAX_WORKERS)

    # Build task list sorted strictly by (if_rank, tier, year desc) so we
    # index the highest-IF journals first and get them into the download queue
    # as early as possible. The rate limiter serialises requests anyway so
    # concurrency adds no throughput — strict ordering is free.
    tasks_meta = []
    for tier, if_rank, exact_name, search_term, field in JOURNALS:
        if skip_tier3 and tier == 3:
            continue
        yr_start = TIER_YEAR_START[tier]
        for yr in range(NOW_YEAR, yr_start - 1, -1):
            tasks_meta.append((if_rank, tier, yr, exact_name, search_term, field))

    tasks_meta.sort(key=lambda t: (t[0], t[1], -t[2]))  # if_rank ASC, tier ASC, year DESC

    n_journals = len({t[3] for t in tasks_meta})
    log.info("Index tasks: %d (across %d journals, ordered by IF rank)", len(tasks_meta), n_journals)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for i, (if_rank, tier, yr, name, term, field) in enumerate(
                tqdm(tasks_meta, desc="Indexing", unit="journal-year")):
            await search_journal_year(session, conn, tier, if_rank, name, term, field, yr, lock)

    n = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    log.info("Phase 1 done — %d articles indexed", n)

# ---------------------------------------------------------------------------
# Phase 2: Download XML + PDF
# ---------------------------------------------------------------------------

async def download_one(session, conn, doi, lock, pbar):
    url_base = f"{BASE_URL}/content/article/doi/{doi}"
    safe     = doi.replace("/", "_").replace(":", "_")
    xml_path = ARTICLES_DIR / f"{safe}.xml"
    pdf_path = PDFS_DIR     / f"{safe}.pdf"

    # ── XML ──────────────────────────────────────────────────────────────────
    xml_status, xml_body = await get_with_backoff(
        session, url_base, headers={"X-ELS-APIKey": API_KEY, "Accept": "application/xml"}
    )

    xml_ok = xml_status == 200 and xml_body and xml_body[:5] in (b"<?xml", b"<full")

    if not xml_ok:
        async with lock:
            if xml_status == 404:
                mark_article(conn, doi, "unavailable")
            elif xml_status in (401, 403):
                mark_article(conn, doi, "error")
            # else leave pending
            pbar.update(1)
        return

    xml_path.write_bytes(xml_body)

    # ── PDF ───────────────────────────────────────────────────────────────────
    pdf_ok = False
    pdf_status, pdf_body = await get_with_backoff(
        session, url_base, headers={"X-ELS-APIKey": API_KEY, "Accept": "application/pdf"}
    )
    if pdf_status == 200 and pdf_body and pdf_body[:5] == b"%PDF-":
        pdf_path.write_bytes(pdf_body)
        pdf_ok = True
    elif pdf_status not in (200, 404):
        log.warning("PDF fetch returned %d for %s", pdf_status, doi)

    async with lock:
        mark_article(conn, doi, "downloaded", pdf_downloaded=pdf_ok)
        pbar.update(1)


async def _drain_queue(session, conn, lock, sem, pbar, downloaded_set):
    """Pick up newly indexed pending articles and dispatch download tasks."""
    rows     = pending_articles(conn)
    new_dois = [r[0] for r in rows if r[0] not in downloaded_set]
    for doi in new_dois:
        downloaded_set.add(doi)

    if not new_dois:
        return

    pbar.total = (pbar.total or 0) + len(new_dois)
    pbar.refresh()

    tasks = []
    for doi in new_dois:
        async def bounded(d=doi):
            async with sem:
                await download_one(session, conn, d, lock, pbar)
        tasks.append(asyncio.create_task(bounded()))
    await asyncio.gather(*tasks)


async def run_download_phase(conn):
    """Standalone download (--download-only)."""
    log.info("=== PHASE 2: Downloading XML + PDF ===")
    rows    = pending_articles(conn)
    pending = [r[0] for r in rows]
    if not pending:
        log.info("Nothing to download.")
        return
    log.info("%d articles pending", len(pending))

    lock      = asyncio.Lock()
    timeout   = aiohttp.ClientTimeout(total=90)
    connector = aiohttp.TCPConnector(limit=MAX_WORKERS)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        sem = asyncio.Semaphore(MAX_WORKERS)
        with tqdm(total=len(pending), desc="Downloading", unit="art") as pbar:
            tasks = []
            for doi in pending:
                async def bounded(d=doi):
                    async with sem:
                        await download_one(session, conn, d, lock, pbar)
                tasks.append(asyncio.create_task(bounded()))
            await asyncio.gather(*tasks)

    _log_stats(conn)


async def run_pipeline(conn, skip_tier3=False):
    """Index and download concurrently — downloads start as soon as articles are indexed."""
    log.info("=== PIPELINE: Indexing + Downloading (XML + PDF) ===")
    search_done    = asyncio.Event()
    lock           = asyncio.Lock()
    dl_lock        = asyncio.Lock()
    downloaded_set: set[str] = set()

    timeout   = aiohttp.ClientTimeout(total=90)
    connector = aiohttp.TCPConnector(limit=MAX_WORKERS)

    async def consumer():
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            sem  = asyncio.Semaphore(MAX_WORKERS)
            pbar = tqdm(desc="Downloading", unit="art", dynamic_ncols=True)
            try:
                while True:
                    async with lock:
                        await _drain_queue(session, conn, dl_lock, sem, pbar, downloaded_set)
                    if search_done.is_set():
                        # One final drain after search completes
                        async with lock:
                            await _drain_queue(session, conn, dl_lock, sem, pbar, downloaded_set)
                        break
                    await asyncio.sleep(2)
            finally:
                pbar.close()

    search_task   = asyncio.create_task(run_search_phase(conn, skip_tier3=skip_tier3))
    consumer_task = asyncio.create_task(consumer())
    await search_task
    search_done.set()
    await consumer_task
    _log_stats(conn)

# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def _log_stats(conn):
    log.info("── Stats ──")
    for if_rank, field, status, n in conn.execute("""
        SELECT if_rank, field, status, COUNT(*) FROM articles
        GROUP BY if_rank, field, status ORDER BY if_rank, field, status
    """).fetchall():
        log.info("  IF%s  %-3s  %-12s  %7d", if_rank or "?", field or "?", status, n)
    dl  = conn.execute("SELECT COUNT(*) FROM articles WHERE status='downloaded'").fetchone()[0]
    pdf = conn.execute("SELECT COUNT(*) FROM articles WHERE pdf_downloaded=1").fetchone()[0]
    tot = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    xml_gb = sum(f.stat().st_size for f in ARTICLES_DIR.glob("*.xml")) / 1024**3
    pdf_gb = sum(f.stat().st_size for f in PDFS_DIR.glob("*.pdf"))     / 1024**3
    log.info("  Indexed: %d  XML: %d (%.1f GB)  PDF: %d (%.1f GB)",
             tot, dl, xml_gb, pdf, pdf_gb)


def print_stats(conn):
    print("\n=== Stats ===")
    print(f"  {'IFrank':6}  {'Field':5}  {'Status':12}  {'Count':>8}")
    print("  " + "-" * 38)
    for if_rank, field, status, n in conn.execute("""
        SELECT if_rank, field, status, COUNT(*) FROM articles
        GROUP BY if_rank, field, status ORDER BY if_rank, field, status
    """).fetchall():
        print(f"  IF{if_rank or '?':4}  {field or '?':5}  {status:12}  {n:>8,}")
    dl  = conn.execute("SELECT COUNT(*) FROM articles WHERE status='downloaded'").fetchone()[0]
    pdf = conn.execute("SELECT COUNT(*) FROM articles WHERE pdf_downloaded=1").fetchone()[0]
    tot = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    xml_gb = sum(f.stat().st_size for f in ARTICLES_DIR.glob("*.xml")) / 1024**3
    pdf_gb = sum(f.stat().st_size for f in PDFS_DIR.glob("*.pdf"))     / 1024**3
    print(f"\n  Indexed: {tot:,}  |  XML: {dl:,} ({xml_gb:.1f} GB)  |  PDF: {pdf:,} ({pdf_gb:.1f} GB)\n")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(search_only=False, download_only=False, stats_only=False, no_tier3=False):
    global _rate_limiter
    if not API_KEY:
        log.error("ELSEVIER_API_KEY not set — run: source .env")
        return

    _rate_limiter = RateLimiter(GLOBAL_REQ_INTERVAL)
    conn = init_db()
    try:
        if stats_only:
            print_stats(conn)
            return

        n_journals = len([j for j in JOURNALS if not (no_tier3 and j[0] == 3)])
        log.info("Journals: %d  |  Tiers: 1→%d+ 2→%d+ %s  |  Workers: %d",
                 n_journals,
                 TIER_YEAR_START[1], TIER_YEAR_START[2],
                 "3→%d+ (skipped)" % TIER_YEAR_START[3] if no_tier3 else "3→%d+" % TIER_YEAR_START[3],
                 MAX_WORKERS)

        if download_only:
            await run_download_phase(conn)
        elif search_only:
            await run_search_phase(conn, skip_tier3=no_tier3)
        else:
            await run_pipeline(conn, skip_tier3=no_tier3)
        print_stats(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--search-only",   action="store_true", help="Index only, no downloads")
    parser.add_argument("--download-only", action="store_true", help="Download already-indexed articles")
    parser.add_argument("--no-tier3",      action="store_true", help="Skip Tier 3 journals (lower-IF, 2010+)")
    parser.add_argument("--stats",         action="store_true", help="Print stats and exit")
    args = parser.parse_args()
    asyncio.run(main(
        search_only=args.search_only,
        download_only=args.download_only,
        stats_only=args.stats,
        no_tier3=args.no_tier3,
    ))
