# Rhino Dev Notes

## v0.2 Patch Work

### Issue: Crash Recovery — No Reaper for Stale Locked Jobs

**Problem:** If a worker crashes mid-batch, its jobs stay `locked` forever. No mechanism to reclaim them.

**Approach A: OR clause in claim CTE (REJECTED)**
- Added `OR (status = 'locked' AND locked_at <= NOW() - INTERVAL '30 seconds')` to the `claim_jobs` CTE
- **Result:** Massive throughput regression at every scale
  - 250k: 45,854 → 27,623 jobs/s
  - 500k: 41,885 → 18,741 jobs/s
  - 1M: 41,660 → crawling (~6,259 jobs/s)
- **Why it failed:** The OR prevents Postgres from using a single partial index efficiently. Forces bitmap OR or sequential scan on every claim cycle. The hot path gets punished for a condition that rarely triggers.
- **Verdict:** Dead. Do not use OR in the claim CTE.

**Approach B: Two separate queries (PENDING)**
- First query: claim pending jobs (hits `rhino_jobs_fetchable` index cleanly)
- Second query: claim stale locked jobs if batch isn't full (hits `rhino_jobs_reclaimable` index cleanly)
- Each query uses its own partial index optimally
- **Status:** Not yet implemented

**Approach C: Opportunistic reclaim on idle (FALLBACK)**
- Only reclaim when `claim_jobs` returns 0 (queue looks empty)
- Zero hot-path impact
- **Status:** Fallback if Approach B has issues

---

### Issue: UUID v4 B-tree Fragmentation at 5M+ Rows

**Problem:** v4 UUIDs cause random insert order → B-tree page splits → throughput cliff at scale. v0.1 baseline showed 14,963 jobs/s at 5M (vs 41,660 at 1M).

**Fix Attempt: UUID v7 via Postgres 17 upgrade**
- Upgraded docker-compose from `postgres:16` → `postgres:17`
- Changed column default from `gen_random_uuid()` → `uuidv7()` (built-in in PG17)
- **Result:** No improvement. 13,013 jobs/s at 5M (slightly worse than v4's 14,963)
- **Why it didn't help:** The fragmentation may not be the primary bottleneck at 5M, or the existing index/table wasn't rebuilt after the default change (old data still v4). Further investigation needed.
- **Verdict:** v7 is still the right default for new tables, but it alone doesn't fix the 5M cliff.

**Fix Attempt: PL/pgSQL uuidv7() function on PG16 (ABANDONED)**
- Tried creating a custom `uuidv7()` function in Postgres 16
- PL/pgSQL byte manipulation is error-prone — multiple syntax and type conversion failures
- **Verdict:** Not worth the complexity. PG17 has it built-in.

---

### Baseline Benchmarks (for comparison)

**v0.1 Baseline — PG16, no OR, UUID v4** (`output-20260420T141229Z.json`)
| Scenario | Throughput | Queue Wait p50 | Processing p50 |
|---|---|---|---|
| 250k | 45,854 jobs/s | 3,680ms | 180ms |
| 500k | 41,885 jobs/s | 7,815ms | 186ms |
| 1M | 41,660 jobs/s | 16,851ms | 209ms |
| 5M | 14,963 jobs/s | 153,899ms | 638ms |

**v7 + no OR — PG17** (`output-20260424T141455Z.json`)
| Scenario | Throughput | Queue Wait p50 | Processing p50 |
|---|---|---|---|
| 250k | 37,866 jobs/s | 3,841ms | 192ms |
| 500k | 39,461 jobs/s | 8,031ms | 203ms |
| 1M | 38,602 jobs/s | 17,908ms | 224ms |
| 5M | 13,013 jobs/s | 186,107ms | 698ms |

**OR clause — PG17** (`output-20260424T141127Z.json` — partial run)
| Scenario | Throughput | Queue Wait p50 | Processing p50 |
|---|---|---|---|
| 250k | 27,623 jobs/s | 6,135ms | 199ms |
| 500k | 18,741 jobs/s | 16,900ms | 221ms |
| 1M | ~6,259 jobs/s | 129,002ms | 285ms |
| 5M | did not finish | — | — |
