# SonicGen

**Audio Source Identification & Deduplication**

Give SonicGen an audio/video clip and it finds the _original_ source on YouTube, including the time offset. The goal: paste a video or upload audio → get the canonical, embedded YouTube original.

---

## Why this project matters

- **Real-world problem**: short‑form reposts explode across platforms. SonicGen links remixes/clips back to their source.
- **Full‑stack signal processing**: DSP (spectrogram/peaks → landmark hashes), scalable storage, and SQL matching.
- **Production thinking**: chunked inserts, batching, background workers, claim/status model, cloud storage, and rate limiting.
- **Results so far**: on internal tests, segment-based matching produced high-accuracy (>99% on a controlled set) even with partial fingerprints.

> If you only have 30 seconds:
>
> 1. `fingerprint_pipeline.py` runs the end-to-end pipeline.
> 2. `fingerprint_audio.py` creates deterministic hashes for robust matching.
> 3. `supabase_utils.py` handles DB ingest + SQL matching.
> 4. `youtube_api.py` ingests channel metadata via YouTube Data API.

---

## What SonicGen does

1. **Ingest** YouTube channel metadata (IDs, titles, durations).
2. **Download** audio (mp3) via `yt-dlp`, store it in Google Cloud Storage (GCS).
3. **Fingerprint** audio with a deterministic landmark-hash approach.
4. **Segment & sample** long fingerprints (coverage increases with length).
5. **Match** against an indexed database using a SQL function that groups hits by `(video_id, delta)` and returns ranked candidates.
6. **Decide** if it’s a match (thresholded by % of sampled hashes).
7. **Store or link**: either persist fingerprint rows (new original) or link to the original `video_id` for dedup.

**Ultimate UX** (in progress): paste a clip → SonicGen returns the original YouTube video embedded at the correct timestamp.

## Data model (simplified)

**videos**

- `id UUID`
- `youtube_id TEXT`
- `title TEXT`
- `duration INT`
- `original_video_id UUID`
- `match_status TEXT`

**fingerprints**

- `hash TEXT`
- `video_id UUID`
- `t_ref INT`

**fingerprint_hashes**

- `hash TEXT`
- `total_count BIGINT`
- `video_count BIGINT`

**Key SQL** (stored function)

- `find_fingerprint_candidates`
  - Filters stop-words by global frequency.
  - Joins occurrences → `(video_id, delta)` buckets; counts matches per bucket.
  - Returns top candidates for decision.

**Status life‑cycle** (for `videos`.`match_status`)

| State           | Meaning                                             |
| --------------- | --------------------------------------------------- |
| `NULL`          | Not claimed by any worker yet                       |
| `pending`       | Claimed/being processed                             |
| `fingerprinted` | Fingerprint stored; not matched                     |
| `matched`       | Linked to existing original via `original_video_id` |
| `too_short`     | Skipped (fingerprint too small for reliable match)  |
| `flag`          | Automatically flagged on processing error           |

---

## Matching logic (how a decision is made)

- Compute deterministic hashes from peaks and landmark pairs.
- **Segment sampling**: for long audio, take evenly spaced sections; `hashes_per_segment` and number of segments scale with length using anchor points.
- Flatten sampled segments into a single list so that both full and segmented fingerprints share the same structure for matching.
- Query `find_fingerprint_candidates` with the sampled hashes.
- Merge results with **Δ (delta) within ±1 frame** for the same `video_id`.
- Compute `match_percentage = matches / segment_length`. If ≥ **10%**, treat as a match.

> Rationale: the 10% threshold with length‑aware sampling balances false positives on short clips and recall on hour‑long content.

---

## Key files

- `backend/fingerprint_pipeline.py` — Orchestrates ingest → download → upload → fingerprint → match → update DB.
- `backend/fingerprint_audio.py` — DSP pipeline (STFT, peak picking, deterministic landmark hashing, segmentation).
- `backend/supabase_utils.py` — Postgres/Supabase access, chunked inserts, candidate search, status updates.
- `backend/download.py` — `yt-dlp` download + GCS upload helpers.
- `backend/youtube_api.py` — Channel/video metadata ingestion via YouTube Data API v3.

---

## Setup

### Requirements

- Python 3.11+
- Postgres (via Supabase) accessible from the worker
- Google Cloud Storage bucket for audio mp3s
- YouTube Data API key

## Running the pipeline

**Ingest channel metadata (optional prompt):**

```bash
python backend/fingerprint_pipeline.py
# Enter a handle like: @SomeChannel
# or press Enter to skip ingest and process what's already queued
```

What it does:

1. Ingests recent channel videos into `videos` (title, duration, youtube_id).
2. Selects unclaimed videos (`match_status IS NULL`) in keyset order.
3. Downloads audio → uploads mp3 to GCS.
4. Generates fingerprints; if too short → `too_short`.
5. Samples segments, finds candidates, applies threshold.
6. On match → `original_video_id` + `matched`. Else → inserts fingerprint rows and sets `fingerprinted`.

---

## Tunable Parameters

- **Segmentation anchors** (`segment_fingerprint`): controls coverage and per‑segment size vs input length.
- **Delta merge**: ±1 frame around top delta per `video_id`.
- **Threshold**: `match_percentage ≥ 0.10` (tune per use case).
- **Short clip policy**: fingerprints below ~1,000 hashes are treated as too short for segmentation; overall system thresholds are set higher (~10,000) before attempting a match.

---

## Roadmap

### 1. Scalable Parallel Ingestion

Deploy distributed worker instances on cloud VMs to accelerate fingerprint ingestion.
The current throughput (~1,000 rows/second via direct Postgres connection) will be scaled through parallelism, optimized batching, and asynchronous task coordination.

### 2. Dynamic Original-Detection System

Implement logic to automatically determine which video in a match pair is the true source.
If the “query” video is the original, the system will replace the existing database fingerprint with the newly generated one to preserve the highest-quality representation.

### 3. Interactive Frontend Interface

Develop a minimal web client that allows users to input a YouTube URL.
If the video matches an existing fingerprint, the system will display the original source as an embedded YouTube player with contextual information and interaction options.

### 4. Fingerprint Optimization & High-Performance Search

Experiment with parameter tuning to improve matching precision and recall.
Investigate integrating a **FAISS-based reverse index** on existing Supabase data to enable high-speed similarity search and large-scale audio deduplication.

---

## Stack

- Python, NumPy, SciPy, Librosa
- yt‑dlp, Google Cloud Storage, YouTube Data API v3
- Postgres/Supabase, psycopg

---
