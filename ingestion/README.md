# Proportion – News Ingestion Pipeline (MVP)

Proportion is a **reality-checked news intelligence system** designed to help teams understand when **media attention is misaligned with real-world impact**.

This repository documents the **news ingestion layer only** of the Proportion MVP.  
Other stages (deduplication, clustering, HIR calculation, and alerting) are intentionally developed in **separate branches/modules**.

---

## 🚀 What this pipeline does

At a high level, the ingestion pipeline:

1. Loads a predefined list of companies (entities)
2. Fetches recent news coverage from a licensed news API
3. Extracts full article text from publisher URLs
4. Deduplicates articles at ingestion time
5. Stores clean, structured documents in MongoDB
6. Logs request usage and ingestion outcomes

The pipeline is designed to be:
- **Idempotent** (safe to re-run)
- **Failure-tolerant** (one bad article or company does not crash the run)
- **Local-first** (no cloud dependency for MVP)

---

## 🧠 Design philosophy

### Why only news (for MVP)?
The MVP focuses on **news media** because it provides:
- High signal-to-noise ratio
- Credible, attributable sources
- Consistent structure for clustering

Social media and other signals are intentionally deferred to later stages.

---

### Why GNews API (instead of scraping)?
Scraping modern news websites is:
- Fragile
- Legally ambiguous
- Operationally expensive

This pipeline uses the **GNews API** for:
- Stable, licensed access
- Predictable limits
- Clean metadata

Scraping is avoided at the discovery stage.

---

### Why still extract article text ourselves?
While the API provides article snippets, they are often:
- Truncated
- Inconsistent
- Insufficient for NLP tasks

Therefore, this pipeline:
- Uses the API for **discovery**
- Extracts full text from the **publisher URL**
- Falls back gracefully if extraction fails

This separation significantly improves downstream clustering quality.

## 📂 Project structure
```bash
proportion/
├── config/
│ └── ticker.yaml # Entity configuration (single source of truth)
├── ingestion/
│ └── news_sources/
│ ├── ingestion_pipeline.py # Main entry point
│ ├── gnews_fetcher.py # News discovery via GNews API
│ ├── article_processor.py # Full-text extraction (with fallback)
│ ├── content_hash.py # Hash-based deduplication
│ ├── mongo_client.py # MongoDB connection helper
│ └── ticker_loader.py # Config loader + normalization
├── .gitignore
└── README.md
```

## 🛠️ How ingestion works (step by step)

1. **Entity loading**
   - Companies are defined in `config/ticker.yaml`
   - Config is normalized at load time

2. **News discovery**
   - One API request per company
   - Max ~10 articles per entity (MVP constraint)
   - Request usage is logged explicitly

3. **Article extraction**
   - Primary: `newspaper3k`
   - Fallback: `readability-lxml`
   - Articles below a minimum length are discarded

4. **Deduplication (ingestion-level)**
   - MongoDB unique index on URL
   - Content hashing using `xxhash`
   - Prevents re-ingestion across runs

5. **Persistence**
   - Clean documents stored in MongoDB
   - Flags added for downstream processing (clustering, embeddings)

---

## 🧪 What this pipeline does NOT do

This is intentional and by design for the `ingestion` branch.

- ❌ Story clustering  
- ❌ Semantic deduplication (cosine similarity)  
- ❌ HypetoImpact Ratio (HIR) calculation  
- ❌ Social or market data ingestion  
- ❌ Real-time streaming  

These capabilities are implemented in **subsequent pipeline stages**.

---

## 🔁 Idempotency & reliability

This pipeline is safe to re-run:

- Duplicate URLs are rejected at the database level
- Failed articles are skipped, not retried aggressively
- Company-level failures do not stop the run
- One run = one clean ingestion window

This makes it suitable for:
- Daily batch ingestion
- Cron-based scheduling
- Iterative development

---

## ⚙️ Setup & running locally

### 1. Prerequisites
- Python 3.9+
- MongoDB running locally
- GNews API key (free tier is sufficient for MVP)

### 2. Install dependencies
```bash
pip install -r requirements.txt
```
### 3. Environment variables
- Create a .env file in the project root:

``` bash
GNEWS_API_KEY=your_api_key_here
```

### 4. Run ingestion
```bash
python ingestion/news_sources/ingestion_pipeline.py
```
### 📈 Expected MVP scale
- ~8–10 companies

- ~80–100 articles per day

- One ingestion run per day

- Well within free API limits

- This volume is sufficient for downstream story clustering and signal validation.

### 🧭 Out of scope for this branch
The following are planned in later stages and live outside the ingestion branch:

- Multi-level deduplication

- Semantic (cosine similarity)

- Lexical overlap (optional)

- Story clustering

- Event-level grouping

- Narrative dominance metrics

- HypetoImpact Ratio (HIR)

- Coverage intensity vs real-world impact proxies

- Visualization & alerts

- Act / Monitor / Ignore signals

### 📌 Key takeaway
- This ingestion pipeline is deliberately boring, stable, and explainable.

- That is a feature — not a limitation.

- It provides a clean, trustworthy foundation for Proportion’s core insight:

```
When attention is high but impact is low — and when the opposite is true.
```