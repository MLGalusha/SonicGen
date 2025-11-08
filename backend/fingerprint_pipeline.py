import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from youtube_api import youtube_ingest
from fingerprint_audio import fingerprint_audio, segment_fingerprint
from supabase_utils import (
    get_conn,
    ingest_video_fingerprints,
    next_videos_batch,
    find_fingerprint_candidates,
    mark_video_status
)
from download import (
    youtube_url,
    download_audio,
    init_bucket,
    upload_to_gcs,
)

# --- Config ---
DOWNLOAD_TMP_DIR = Path(os.getenv("DOWNLOAD_TMP_DIR", "data"))

# --- Helpers ---
def store_fingerprint(conn, video_uuid, fingerprint):
    """Store fingerprint in the database."""
    print(f"Storing Fingerprint\nID: {video_uuid}")
    return ingest_video_fingerprints(conn, video_uuid, fingerprint)

# --- Main Workflow ---
def process_videos(limit: int = 1):
    bucket = init_bucket()
    print(f"\n[Pipeline] Using bucket: {bucket.name}")

    cursor = None
    processing = True
    while processing:
        print(f"\n[Pipeline] Fetching batch (limit={limit}, cursor={cursor})")
        rows, cursor = next_videos_batch(limit=limit, cursor=cursor)
        if not rows:
            print("[Pipeline] No more videos returned from Supabase. Exiting.")
            break

        print("\nDownloading Audio")

        for row in rows:
            video_uuid = row["id"]
            youtube_video_id = row["youtube_id"]
            object_name = f"{youtube_video_id}.mp3"
            print(f"\n[Pipeline] Starting video {youtube_video_id} ({video_uuid})")
            mark_video_status(video_uuid, "pending")

            try:
                # --- Download & Upload ---
                url = youtube_url(youtube_video_id)
                audio_path = download_audio(url, youtube_video_id, DOWNLOAD_TMP_DIR)
                print("\nUploading to Cloud Bucket!")
                try:
                    upload_to_gcs(bucket, audio_path, object_name)

                    # --- Fingerprint Generation ---
                    fingerprint = fingerprint_audio(audio_path)
                    fingerprint_length = len(fingerprint)
                    print(f"[Pipeline] Fingerprint length: {fingerprint_length}")
                finally:
                    try: audio_path.unlink(missing_ok=True)
                    except Exception: pass

                # Create connection after making fingerprint to prevent timeout
                with get_conn() as conn:
                    # --- Skip short videos ---
                    if fingerprint_length < 10000:
                        print(f"Too short to store data\nID: {video_uuid}")
                        mark_video_status(video_uuid, "too_short", conn)
                        continue

                    # --- Segment & Match ---
                    seg = segment_fingerprint(fingerprint)
                    segments = seg["segments"]
                    segment_info = seg["info"]
                    print(f"[Pipeline] Segment info: {segment_info}")
                    matches = find_fingerprint_candidates(conn, segments)

                    if matches:
                        print(f"[Pipeline] {len(matches)} candidates returned from matcher.")
                        top_match = matches[0]
                        # Merge close deltas
                        for m in matches[1:]:
                            if abs(m["delta"] - top_match["delta"]) <= 1 and m["video_id"] == top_match["video_id"]:
                                top_match["matches"] += m["matches"]

                        match_percentage = top_match["matches"] / segment_info["length"]
                        print(
                            "[Pipeline] Top match video "
                            f"{top_match['video_id']} delta={top_match['delta']} "
                            f"matches={top_match['matches']} ratio={match_percentage:.2%}"
                        )

                        if match_percentage >= 0.1:
                            # Found a match
                            print(f"Not storing data\nID: {video_uuid}")
                            mark_video_status(video_uuid, "matched", conn, top_match["video_id"])
                            continue

                    # --- Store if no good match ---
                    store_fingerprint(conn, video_uuid, fingerprint)


            except KeyboardInterrupt as k:
                # Stop the pipeline entirely
                print(f"\n[INTERRUPT] Keyboard Interrupt {youtube_video_id}: {k}")
                try:
                    mark_video_status(video_uuid, None)

                except Exception as mark_err:
                    print(f"[CRITICAL] Failed to update video {video_uuid}: {mark_err}")
                processing = False

            except Exception as e:
                # Handle normal runtime failures for a single video
                print(f"\n[ERROR] Failed processing {youtube_video_id}: {e}")
                try:
                    mark_video_status(video_uuid, "flag")
                except Exception as mark_err:
                    print(f"[CRITICAL] Failed to flag video {video_uuid}: {mark_err}")
                continue   # <--- move on to next video


def _prompt_handle() -> Optional[str]:
    handle = input("Enter YouTube handle (e.g., @Channel) or press Enter to skip: ").strip()
    return handle or None


def _parse_date(label: str) -> Optional[datetime]:
    """Parse a user-provided date string into a timezone-aware datetime, or None if skipped."""
    value = input(label).strip()
    if not value:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(value, fmt)
            # make timezone-aware (UTC)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    print("Invalid date format; expected YYYY-MM-DD or MM/DD/YYYY. Skipping this filter.")
    return None


def ingest_channel(handle: str) -> None:
    """Ingest metadata for a given YouTube channel handle."""
    published_after = _parse_date("Enter published-after date (YYYY-MM-DD or MM/DD/YYYY) or press Enter: ")
    published_before = _parse_date("Enter published-before date (YYYY-MM-DD or MM/DD/YYYY) or press Enter: ")

    print(f"\nStarting metadata ingestion for {handle}...")
    youtube_ingest(handle, published_after, published_before)
    print("Metadata ingestion complete.\n")


def main() -> None:
    """Main orchestrator: ingest metadata, then download and deduplicate."""
    handle = _prompt_handle()
    if handle:
        ingest_channel(handle)
    else:
        print("No channel handle provided. Skipping metadata ingestion.")

    process_videos()

if __name__ == "__main__":
    main()