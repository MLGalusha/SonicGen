"""Audio fingerprint generation tuned for long-form speech deduplication."""
from __future__ import annotations
from collections import deque
import hashlib
from typing import List, Sequence, Tuple
import math
import librosa
import numpy as np
from scipy.ndimage import maximum_filter


# Tunable parameters (override via env vars to experiment without code changes)
SAMPLE_RATE = 16000
HOP_LENGTH = 256                 # 62.5 fps
N_FFT = 2048

# Peaks
PEAK_NEIGHBORHOOD_FREQ = 12      # ~90–100 Hz span
PEAK_NEIGHBORHOOD_TIME = 7       # ~112 ms
PEAK_THRESHOLD_DB = -30

# Pairing
FAN_VALUE = 8
MAX_DELTA_FRAMES = 31            # ~0.5 s
DT_BUCKET_FRAMES = 2
MAX_HASHES_PER_SECOND = 40

def load_audio(path: str, sr: int = SAMPLE_RATE) -> Tuple[np.ndarray, int]:
    """Load audio file as mono and return samples + sample rate."""
    y, sr = librosa.load(path, sr=SAMPLE_RATE, mono=True, res_type='soxr_hq')
    return y, sr

def get_spectrogram(
    y: np.ndarray,
    sr: int,
    n_fft: int = N_FFT,
    hop_length: int = HOP_LENGTH,
    fmin: float = 100.0,
    fmax: float = 3000.0,
) -> np.ndarray:
    """Compute log-power spectrogram, optionally restricted to [fmin, fmax] Hz."""
    # Compute magnitude power spectrogram
    S = np.abs(librosa.stft(
        y, n_fft=n_fft, hop_length=hop_length,
        center=False, # no padding
        window='hann', # smooth fade-in/out
        )) ** 2

    # Convert power to decibels
    log_S = librosa.power_to_db(S, ref=1.0)

    # Get the actual frequency axis (in Hz)
    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)

    # Find frequency bin indices for the desired range
    freq_mask = (freqs >= fmin) & (freqs <= fmax)

    # Apply the mask to keep only rows (frequencies) within the range
    log_S_band = log_S[freq_mask, :]

    return log_S_band


def find_peaks(
    S: np.ndarray,
    neighborhood_size=(25, 25),
    threshold=-30.0,
    eps=1e-6
) -> np.ndarray:
    # Quantize to suppress floating-point jitter
    S_q = np.round(S, decimals=1)

    # Compute neighborhood with deterministic padding
    neighborhood = maximum_filter(S_q, size=neighborhood_size, mode='nearest')

    # Allow tiny tolerance for “equal to max”
    peaks = (S_q >= neighborhood - eps) & (S_q > threshold)

    # Return coordinates of detected peaks
    return np.argwhere(peaks)

def deterministic_rate_limit(
    candidates: list[tuple[int, int, int, int]],
    max_per_second: int,
    *,
    sample_rate: int = SAMPLE_RATE,
    hop_length: int = HOP_LENGTH,
) -> list[tuple[int, int, int, int]]:
    """
    Cap to at most `max_per_second` pairs in any rolling 1-second window (measured in frames).
    candidates must be sorted in a canonical order beforehand (see call site).
    Each candidate is (t1_frame, f1, f2, dt_frames).
    """
    if not max_per_second:
        return candidates

    W = sample_rate // hop_length        # frames in ~1 second (e.g., 16000/256=62)
    kept = []
    recent = deque()                     # stores t1 of kept items within the last 1s window

    for t1, f1, f2, dt in candidates:
        # evict items older than 1 second from the front
        while recent and (t1 - recent[0]) >= W:
            recent.popleft()

        if len(recent) < max_per_second:
            kept.append((t1, f1, f2, dt))
            recent.append(t1)

    return kept

def generate_hashes(
    peaks: Sequence[Sequence[int]],
    *,
    fan_value: int = FAN_VALUE,
    max_delta: int = MAX_DELTA_FRAMES,
    max_hashes_per_second: int = MAX_HASHES_PER_SECOND,
    dt_bucket = 2
) -> List[Tuple[str, int]]:
    """
    Deterministic landmark hashing with origin-invariant rate limiting.
    Returns: list of (hash:str, t_ref_frame:int)
    """
    if len(peaks) == 0:
        return []

    # Canonicalize peaks first: (time, then freq)
    peaks_sorted = sorted(((int(f), int(t)) for f, t in peaks), key=lambda p: (p[1], p[0]))

    # Build ALL candidate pairs (no second buckets)
    # Each candidate: (t1, f1, f2, dt)
    candidates: List[Tuple[int, int, int, int]] = []
    for i in range(len(peaks_sorted)):
        f1, t1 = peaks_sorted[i]

        # Pair with the first K targets that are within the time window
        taken = 0
        for k in range(i + 1, len(peaks_sorted)):
            f2, t2 = peaks_sorted[k]
            dt = t2 - t1
            if dt <= 0:
                continue
            if dt > max_delta:
                break  # peaks_sorted is time-sorted, so we can stop scanning

            # quantize dt in frames for stability
            if dt_bucket > 1:
                dt = (dt // dt_bucket) * dt_bucket

            candidates.append((t1, f1, f2, dt))
            taken += 1
            if taken >= fan_value:
                break  # cap fanout per anchor inside the window


    # Canonical global order BEFORE limiting
    # Pick any fixed, total ordering; this one is stable and simple.
    # Primary by t1 (time), then dt, then f1, f2.
    candidates.sort(key=lambda x: (x[0], x[3], x[1], x[2]))

    # 4) Origin-invariant rate limit: at most K per rolling ~1 second
    candidates = deterministic_rate_limit(
        candidates,
        max_hashes_per_second,
        sample_rate=SAMPLE_RATE,
        hop_length=HOP_LENGTH,
    )

    # Hash deterministically
    hashes: List[Tuple[str, int]] = []
    for t1, f1, f2, dt in candidates:
        hash_input = f"{f1}|{f2}|{dt}"
        h = hashlib.sha1(hash_input.encode("utf-8")).hexdigest()[:20]
        hashes.append((h, t1))

    # Final canonical sort for stable output comparisons
    hashes.sort(key=lambda x: (x[1], x[0]))  # (t_ref_frame, hash)

    # Remove duplicates while keeping order
    hashes = list(dict.fromkeys(hashes))
    return hashes


def segment_fingerprint(fingerprint):
    """
    Takes a fingerprint (list of (hash, frame) tuples), determines how many
    segments to create and how many hashes per segment based on fingerprint length,
    and returns both the parameters and the segmented fingerprint.

    Output:
        {
            "segments": [ ... ],
            "info": {
                "length": int,
                "segments": int,
                "hashes_per_segment": int,
                "coverage": float
            }
        }
    """
    L = len(fingerprint)

    # Handle very short fingerprints
    if L < 1000:
        return {
            "segments": fingerprint,
            "info": {"length": L, "segments": 1, "hashes_per_segment": L, "coverage": 1.0}
        }

    # Define scaling anchors (length, per_section, coverage, min_sections, max_sections)
    anchors = [
        (1000,   50, 0.25, 5, 25),
        (5000,   80, 0.20, 10, 30),
        (15000, 120, 0.15, 15, 40),
        (50000, 160, 0.075, 20, 50),
        (100000,200, 0.05, 50, 50),
    ]

    # Determine scale parameters from anchors
    for i in range(len(anchors) - 1):
        start, p_start, c_start, smin_start, smax_start = anchors[i]
        end, p_end, c_end, smin_end, smax_end = anchors[i + 1]
        if start <= L < end:
            t = (L - start) / (end - start)
            per_section = p_start + (t ** 0.5) * (p_end - p_start)
            coverage = c_start + t * (c_end - c_start)
            smin = smin_start + t * (smin_end - smin_start)
            smax = smax_start + t * (smax_end - smax_start)
            break
    else:
        per_section, coverage, smin, smax = 200, 0.05, 50, 50

    # Compute number of segments
    sections = (L * coverage) / per_section
    sections = max(smin, min(smax, math.ceil(sections)))

    # Adjust coverage after rounding
    actual_coverage = (sections * per_section) / L

    # Perform even-interval segmentation
    total = len(fingerprint)
    step = total // sections
    fingerprint_segments = []

    for i in range(int(sections)):
        start = int(i * step)
        end = start + int(round(per_section))
        if end > total:
            end = total
        segment = fingerprint[start:end]
        if len(segment) > 0:  # avoid empty tail
            fingerprint_segments.extend(segment)

    return {
        "segments": fingerprint_segments,
        "info": {
            "length": int(sections) * int(per_section),
            "segments": int(sections),
            "hashes_per_segment": int(per_section),
            "coverage": f"{int((round(actual_coverage, 2))*100)}%"
        }
    }


def fingerprint_audio(path: str) -> List[Tuple[str, int]]:
    """Full pipeline: load audio, compute spectrogram, extract hashes."""
    print("Creating fingerprint!\n")
    y, sr = load_audio(path)
    S = get_spectrogram(y, sr)
    peaks = find_peaks(S)
    hashes = generate_hashes(peaks)
    return hashes


if __name__ == "__main__":
    fingerprint = fingerprint_audio("./testing/data/test_audio/audio_file_1.mp3")
    # segments = segment_fingerprint(fingerprint)
    # for key, value in segments["info"].items():
    #     print(f"{key}: {value}")
    # for idx, pair_1 in enumerate(fingerprint):
    #     pair_2 = fingerprint[idx+1]
    #     dt = pair_2[1] - pair_1[1]
    #     print(pair_1[0], pair_2[0], dt, pair_1[1])
    print(len(fingerprint))
    for f in fingerprint:
        print(f)
"""
First thing first is we want to understand learn this full SQL
query and then decide what we want to keep in the query then lets
add it to our sql editor when reviewed and roughly understood.

Next we connect our supabase_utils functions to this new supabase
layout and use the functions properly and efficiently

Next start running the first 10 videos out of our pipeline lets
just go ahead and redownload the audio from youtube instead of
caring about pulling the id's from supabase...

Once that pipeline is ran we will go ahead and run our testing
data pipeline as well and get all of that datatesting ready again.

After this we compare and grab matching hashes from database.

Then we create an algorithm to score these hashes. The algorithm
will be a simple does exist in what uuid. We use that output and
the name of the testing files to have a broader score of how well
the fingerprinting and scoring algorithm does.

"""











"""
Okay so today the goal is to create a system to match
fingerprints temporarily outside of supabase

Next based on the output of the fingerprints look for more
systems you can make more deterministic

Next lets delete the fingerprints from our supabase database

Then lets combine the fingerprint_hash_counts with fingerprints
So now we have a unique has with list of ids:t_ref  I'm thinking
the id and t_ref have to stay together so they never get mixed
up accidentally and start matching to the wrong id. Then last
column is the count
TLDR: 3 columns -->
(UNIQUE HASH)
[LIST OF KEY(ID):VALUE(T_REF)]
(COUNT)

After doing both of those things and verifying that matching works
lets loop start pipeline to store these audio files

I question wether we go ahead and run the deduplication system
Lets actually not yet run the dedup yet lets just do what we did
and fingerprint 10 longest videos into supabase.

After that we can remake the tests and start testings the database.
"""
