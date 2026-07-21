# InsightGuard — Insights Memo

**Date:** `[YYYY-MM-DD]`  
**Author:** `[Name]`  
**Dashboard version:** `[Power BI/Tableau report link or filename]`

## Executive summary

`[In 2–3 sentences, summarize the business question, the strongest finding, and the recommended action.]`

## Scope and method

- **Dataset period:** `[Start date]` to `[End date]`
- **Rows/orders analyzed:** `[Count]`
- **Revenue definition:** Net positive sales after return handling.
- **Anomaly method:** Seven-day shifted rolling z-score by region for revenue and returns; threshold: `[3.0 or chosen value]`.
- **Forecast method:** Simple exponential smoothing with 7-day and 30-day horizons.

## Finding 1 — `[Short title: revenue, region, or trend]`

**Evidence:** `[Metric, date range, region/category, and dashboard view.]`

**Interpretation:** `[What happened and why it matters to the business.]`

**Recommended action:** `[Specific decision, follow-up analysis, or operational action.]`

## Finding 2 — `[Short title: returns, customers, or category]`

**Evidence:** `[Metric, comparison, and dashboard view.]`

**Interpretation:** `[What happened and why it matters to the business.]`

**Recommended action:** `[Specific decision, follow-up analysis, or operational action.]`

## Finding 3 — `[Short title: anomaly or forecast]`

**Evidence:** `[Region, anomaly date/severity or forecast horizon, and dashboard view.]`

**Interpretation:** `[What happened, whether it is signal or data quality, and why it matters.]`

**Recommended action:** `[Specific decision, alert threshold change, or investigation.]`

## Caveats

`[Note missing customer IDs, heuristic categories, historical-only data, sparse regions, or any other limitation that affects interpretation.]`

## Closing recommendation

`[One paragraph tying the three findings to a prioritized next step.]`

