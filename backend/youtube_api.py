import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
from datetime import datetime, timezone
import isodate
from supabase import create_client


load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

API_KEY = os.getenv("GOOGLE_API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY)


# ────────────────────────────────────────────────
# Helper functions
# ────────────────────────────────────────────────

def get_channel_id_from_query(query):
    print(f"Searching for channel: {query}")
    res = youtube.search().list(
        part="snippet",
        q=query,
        type="channel",
        maxResults=1
    ).execute()
    return res["items"][0]["snippet"]["channelId"]

def get_channel_id_from_handle(handle: str):
    """Fetch the channel ID using a YouTube handle like '@PersonYoutube'"""
    handle = handle.lstrip("@")
    res = youtube.channels().list(
        part="id,snippet",
        forHandle=handle
    ).execute()
    items = res.get("items", [])
    return items[0]["id"] if items else None

def get_channel_info(channel_id):
    res = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    ).execute()
    item = res["items"][0]
    snippet = item["snippet"]
    return {
        "title": snippet["title"],
        "uploads_playlist": item["contentDetails"]["relatedPlaylists"]["uploads"]
    }

def get_video_metadata(video_ids):
    """Fetch metadata for up to 50 videos at once"""
    res = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=",".join(video_ids)
    ).execute()

    details = []
    for item in res["items"]:
        snippet = item.get("snippet", {})
        content = item.get("contentDetails", {})

        published_at = None
        if snippet.get("publishedAt"):
            published_at = datetime.fromisoformat(
                snippet["publishedAt"].replace("Z", "+00:00")
            )

        duration = None
        if content.get("duration"):
            try:
                duration = isodate.parse_duration(content["duration"])
            except Exception:
                pass

        details.append({
            "video_id": item["id"],
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "published_at": published_at.isoformat() if published_at else None,
            "duration": str(duration) if duration else None,
        })
    return details

def get_all_video_ids(playlist_id, max_results=50):
    video_ids = []
    next_page = None
    while True:
        res = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=max_results,
            pageToken=next_page
        ).execute()
        video_ids.extend([i["contentDetails"]["videoId"] for i in res["items"]])
        next_page = res.get("nextPageToken")
        if not next_page:
            break
    return video_ids

def upsert_videos_in_batches(videos, channel_id, batch_size=500):
    for i in range(0, len(videos), batch_size):
        batch = videos[i:i+batch_size]
        for v in batch:
            v["channel_id"] = channel_id
        try:
            supabase.table("videos").upsert(batch).execute()
            print(f"Upserted batch {i//batch_size + 1} ({len(batch)} videos)")
        except Exception as e:
            print(f"Error inserting batch {i//batch_size + 1}: {e}")

def filter_videos_by_date(videos, after_date=None, before_date=None):
    filtered = []
    for v in videos:
        published = v.get("published_at")
        if not published:
            continue
        published_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
        if after_date and published_dt < after_date:
            continue
        if before_date and published_dt > before_date:
            continue
        filtered.append(v)
    return filtered


# ────────────────────────────────────────────────
# Main workflow function
# ────────────────────────────────────────────────

def youtube_ingest(channel_handle, after_date=None, before_date=None):
    """Full workflow: fetch all videos from channel and upsert to Supabase"""
    channel_id = get_channel_id_from_handle(channel_handle)
    if not channel_id:
        print(f"Could not find channel for handle: {channel_handle}")
        return

    channel_info = get_channel_info(channel_id)
    playlist_id = channel_info["uploads_playlist"]
    video_ids = get_all_video_ids(playlist_id)
    print(f"Found {len(video_ids)} videos from {channel_info['title']}")

    # Insert channel record
    channel_row = {"id": channel_id, "title": channel_info["title"]}
    supabase.table("channels").upsert(channel_row).execute()
    print(f"Upserted channel: {channel_row['title']}")

    total_inserted = 0
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i+50]
        print(f"Fetching metadata for batch {i//50 + 1} ({len(batch_ids)} videos)")
        batch_metadata = get_video_metadata(batch_ids)
        filtered = filter_videos_by_date(batch_metadata, after_date, before_date)
        print(f"→ {len(filtered)} / {len(batch_metadata)} passed date filter")
        if filtered:
            upsert_videos_in_batches(filtered, channel_id)
            total_inserted += len(filtered)

    print(f"Inserted {total_inserted} videos into Supabase")


# ────────────────────────────────────────────────
# Entrypoint
# ────────────────────────────────────────────────

if __name__ == "__main__":
    # Only configure handle and date filters here
    channel_handle = "@RealCharlieKirk"
    after_date = None
    before_date = datetime(2025, 9, 11, tzinfo=timezone.utc)

    youtube_ingest(channel_handle, after_date, before_date)
