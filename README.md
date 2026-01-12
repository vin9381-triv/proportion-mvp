# Proportion  
**Reality-Checked News Intelligence**  
*(Working title – startup name TBD)*

---

## 🚀 Overview

**Proportion** is a news intelligence system designed to answer a practical but under-served question:

> **Is media attention aligned with real-world impact?**

The platform ingests news articles, groups them into event-level story clusters, analyzes sentiment and stance, and compares **coverage intensity** with **real-world impact signals** using a metric called **HypetoImpact Ratio (HIR)**.

The goal is to help teams identify:
- **Overhyped stories** (high attention, low impact)
- **Underreported issues** (low attention, high impact)

All signals are transparent and evidence-backed (sources, sentiment distribution, credibility weighting, impact proxies).

---

## 🧠 Problem Statement

Modern decision-makers face:
- information overload  
- noisy alerts  
- sentiment scores without context  

Existing tools show *what is trending*, but rarely answer:
- *Does this actually matter?*
- *Are we overreacting or missing something important?*

This misalignment leads to:
- wasted time
- poor decisions
- missed slow-burn risks

---

## 💡 Solution

Proportion provides a **decision-ready signal layer** on top of news data by:

1. Ingesting news articles from multiple sources  
2. Deduplicating and clustering them into real-world events  
3. Applying sentiment and stance analysis  
4. Weighting sources by credibility  
5. Comparing coverage vs impact using **HIR**  
6. Flagging stories as **Act / Monitor / Ignore**

---

## 🔍 What is HypetoImpact Ratio (HIR)?

**HIR** compares how much attention a story receives with how much measurable impact it has.

Conceptually:

HIR = Coverage Intensity / Real-World Impact

markdown
Copy code

- **HIR >> 1** → possible hype / overreaction  
- **HIR << 1** → possible underreporting / blind spot  

Impact proxies may include:
- search interest
- market movement
- verified real-world events
- other external signals

HIR is designed to be **explainable**, not a black box.

---

## 🧱 MVP Scope

### ✅ Included in MVP
- News ingestion (limited sources)
- Deduplication (hashing + similarity)
- Embedding-based story clustering
- Sentiment analysis
- Basic stance detection (planned)
- HIR computation
- Simple API
- Simple dashboard (Streamlit)
- Clear documentation

### ❌ Out of scope (for MVP)
- Paid data sources
- Large-scale streaming
- Complex frontend UI
- Heavy cloud infrastructure
- Perfect credibility modeling

---

## 🛠️ Tech Stack

**Language**
- Python

**Data & Storage**
- MongoDB (articles, clusters, scores)

**NLP / ML**
- sentence-transformers (embeddings)
- Hugging Face transformers (sentiment / stance)
- scikit-learn (clustering)

**Vector Similarity**
- FAISS (CPU)

**Backend**
- FastAPI

**Orchestration (lightweight)**
- Prefect (community edition)

**UI (MVP)**
- Streamlit

**Infrastructure**
- Linux (local-first)
- Conda
- Docker (selective use)

All tools are **free and open-source**.

---

## 📁 Project Structure
```
proportion/
├─ ingestion/ # News fetching & normalization
├─ pipeline/ # Embeddings, clustering, scoring
├─ nlp/ # Sentiment & stance analysis
├─ api/ # FastAPI backend
├─ dashboard/ # Streamlit UI
├─ notebooks/ # Experiments & prototyping
├─ data/ # Local datasets (gitignored)
├─ docker/ # Docker & compose files
├─ README.md
├─ requirements.txt
└─ .gitignore
```

---

## 🧭 Design Philosophy

- Local-first development  
- Free tools only  
- Simple > clever  
- Explainable > black box  
- MVP-focused  
- Interview-ready code quality  

This project is intentionally built to be:
- a startup prototype **and**
- a strong demonstration of applied ML/data engineering skills

---

## 🗺️ Roadmap (High-Level)

- [ ] Basic ingestion + deduplication
- [ ] Embedding-based clustering
- [ ] Sentiment analysis integration
- [ ] Initial HIR formulation
- [ ] Simple API endpoints
- [ ] Streamlit demo dashboard
- [ ] Stance detection (v2)
- [ ] Credibility scoring (v2)

---

## 📌 Status

**Early-stage / MVP in progress**

This repository is under active development.  
Structure and documentation are prioritized early to avoid tech debt.

---

## 🤝 Collaboration

Currently developed by a single founder.  
Open to collaboration, feedback, and design partners once the MVP stabilizes.

---

## ⚠️ Disclaimer

This project is a research-driven prototype.  
It does **not** make claims about truthfulness of news, nor does it provide financial, legal, or political advice.

---

## 📬 Contact

If you're interested in:
- collaborating
- giving feedback
- discussing the idea

Feel free to reach out via GitHub.

---