from __future__ import annotations
import os
from pathlib import Path
import yt_dlp
from dotenv import load_dotenv
from google.cloud import storage
from google.api_core.exceptions import NotFound


load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_AUDIO_BUCKET")
DOWNLOAD_TMP_DIR = Path(os.getenv("DOWNLOAD_TMP_DIR", "data"))
YOUTUBE_BASE_URL = "https://www.youtube.com/watch?v="
COOKIES_FILE = Path("./keys/cookies.txt")


def log(message: str) -> None:
    print(f"[Downloader] {message}")


class DownloadError(Exception):
    pass

def init_bucket() -> storage.bucket.Bucket:
    if not GCS_BUCKET_NAME:
        raise RuntimeError("Missing GCS_AUDIO_BUCKET env var for destination bucket")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    log(f"Initialized GCS bucket {GCS_BUCKET_NAME}")
    return bucket

def youtube_url(video_id: str) -> str:
    return f"{YOUTUBE_BASE_URL}{video_id}"

def download_audio(url: str, video_id: str, tmp_dir: Path) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    template = tmp_dir / f"{video_id}.%(ext)s"
    output_path = tmp_dir / f"{video_id}.mp3"

    if output_path.exists():
        output_path.unlink()

    log(f"Starting download for {video_id} -> {output_path}")

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": str(template),
        "noplaylist": True,
        "quiet": False,
        "cookiefile": str(COOKIES_FILE),
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ],
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.extract_info(url, download=True)
    except Exception as exc:  # noqa: BLE001
        raise DownloadError(f"Failed to download {url}: {exc}") from exc

    if not output_path.exists():
        raise DownloadError(f"yt-dlp reported success but {output_path} not found")

    size_mb = output_path.stat().st_size / (1024 * 1024)
    log(f"Download complete for {video_id}; size={size_mb:.2f} MB")

    return output_path


def upload_to_gcs(bucket: storage.bucket.Bucket, local_file: Path, destination_name: str) -> None:
    log(f"Uploading {local_file} to gs://{bucket.name}/{destination_name}")
    blob = bucket.blob(destination_name)
    blob.upload_from_filename(str(local_file), content_type="audio/mpeg")
    log(f"Upload complete for {destination_name}")


def download_from_gcs(bucket: storage.bucket.Bucket, local_file: Path, destination_name: str) -> Path | None:
    """
    Downloads a file from Google Cloud Storage to a local path.
    Returns the local file path if successful, or None if the blob doesn't exist.
    """
    blob = bucket.blob(destination_name)
    try:
        if not blob.exists():
            return None
        file_path = f"{local_file}/{destination_name}"
        print(f"Downloading gs://{bucket.name}/{destination_name} to {local_file}")
        blob.download_to_filename(file_path)
        print(f"Download complete for {destination_name}")
        return file_path
    except NotFound:
        return None


