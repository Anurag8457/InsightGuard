# InsightGuard — findings write-up

Fill this in after running the pipeline and exploring the dashboard. The sections below are prompts, not a rigid format — delete what doesn't apply, add what does.

---

**Date:**  
**Dashboard file/link:**

---

## What we looked at

Brief description of the dataset and time range (e.g. "UCI Online Retail transactions Dec 2010 – Dec 2011, ~400k rows after validation").

Anomaly method: 7-day shifted rolling z-score per region, threshold |z| ≥ 3.  
Forecast method: Simple exponential smoothing, 7-day and 30-day horizons.

---

## Main findings

### 1. [Finding title]

What the numbers show and which dashboard view you pulled it from.

Why it matters for the business.

What you'd do about it.

---

### 2. [Finding title]

What the numbers show and which dashboard view you pulled it from.

Why it matters for the business.

What you'd do about it.

---

### 3. [Finding title — usually an anomaly or forecast observation]

What the detector flagged and whether it looks like a real signal or a data artefact.

What you'd do about it.

---

## Caveats

- ~25% of rows have no CustomerID (guest checkouts) — mapped to UNKNOWN dimension key, revenue is counted but customer-level analysis is incomplete.
- Product categories are keyword-based heuristics, not from a real taxonomy.
- Dataset is historical — no live feed, so forecasts are illustrative rather than operational.
- Sparse regions (e.g. RSA, Bahrain) have too few data points for reliable anomaly baselines.

---

## Bottom line

One paragraph. What's the single most important thing to act on, and what's the next analysis step if you had more time?
