import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
from typing import Optional, Iterable, Tuple, Mapping
from collections import Counter
import json
import os


# Load environment variables from the .env file so credentials like user, password, and host are available
load_dotenv()

def get_conn():
    """
    Establish a secure connection to the PostgreSQL database using credentials from environment variables.
    Returns an active psycopg connection object if successful, or raises a RuntimeError if the connection fails.
    """
    USER = os.getenv("PG_USER")
    PASSWORD = os.getenv("PG_PASSWORD")
    HOST = os.getenv("PG_HOST")
    PORT = os.getenv("PG_PORT")
    DBNAME = os.getenv("PG_DBNAME")

    try:
        connection = psycopg.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME,
            sslmode="require",  # enforce SSL for security when connecting to Supabase
            row_factory=dict_row,
        )
        print("[DB] Connection established.")
        return connection
    except Exception as e:
        raise RuntimeError(f"Database connection failed: {e}")

def aggregate_hash_counts(occurrences: Iterable[Tuple[str, int]]) -> Mapping[str, int]:
    """hash -> total_count for this video only."""
    c = Counter()
    for h, _ in occurrences:
        c[str(h)] += 1
    return c


def upsert_fingerprint_hashes(conn: psycopg.Connection, hash_counts: Mapping[str, int]) -> None:
    """
    Adds to total_count for all hashes and increments video_count by +1.
    If a hash is new, inserts it with total_count = count, video_count = 1.
    """
    if not hash_counts:
        return

    hashes = list(hash_counts.keys())
    totals = list(hash_counts.values())
    print(f"[DB] Upserting {len(hashes)} unique hashes into fingerprint_hashes.")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.fingerprint_hashes (hash, total_count, video_count)
            SELECT u.hash, u.c, 1
            FROM UNNEST(%s::text[], %s::bigint[]) AS u(hash, c)
            ON CONFLICT (hash)
            DO UPDATE
              SET total_count = fingerprint_hashes.total_count + EXCLUDED.total_count,
                  video_count = fingerprint_hashes.video_count + 1
            """,
            (hashes, totals),
        )


def insert_fingerprint(conn, video_id: str, occurrences, chunk_size: int = 5000):
    """
    Insert fingerprint occurrences for a given video in chunks to avoid statement timeouts.
    """

    # Normalize input data into a list of tuples (hash, video_id, t_ref)
    rows = [(str(item["hash"]), video_id, int(item["t_ref"]))
            if isinstance(item, dict)
            else (str(item[0]), video_id, int(item[1]))
            for item in occurrences]

    if not rows:
        return 0

    total_inserted = 0
    print(f"[DB] Inserting {len(rows)} fingerprint rows for video {video_id}.")
    with conn.cursor() as cur:
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            cur.executemany(
                "INSERT INTO fingerprints (hash, video_id, t_ref) VALUES (%s, %s, %s)",
                chunk
            )
            total_inserted += len(chunk)
            conn.commit()  # commit after each chunk so we don't lose progress if it fails
            print(f"[DB] Committed chunk {(i // chunk_size) + 1} ({len(chunk)} rows).")

        # Mark video as fingerprinted once all chunks are inserted
        cur.execute("UPDATE videos SET match_status = %s WHERE id = %s", ("fingerprinted", video_id))
        conn.commit()
        print(f"[DB] Video {video_id} marked as fingerprinted.")

    return total_inserted



def ingest_video_fingerprints(
    conn,
    video_id: str,
    occurrences: Iterable[Tuple[str, int]]
) -> int:
    """
    End-to-end ingest for one video:
      - upsert hash totals (+1 video_count where new to this video)
      - insert the occurrences
    """
    # Materialize occurrences once (we iterate twice)
    occ_list = [(str(h), int(t)) for h, t in occurrences]
    per_hash = aggregate_hash_counts(occ_list)
    print(f"[DB] Prepared {len(occ_list)} occurrences across {len(per_hash)} unique hashes for video {video_id}.")

    # One transaction for correctness + speed
    upsert_fingerprint_hashes(conn, per_hash)
    row_length = insert_fingerprint(conn, video_id, occ_list)
    return row_length


def find_fingerprint_candidates(conn, occurrences, *,
    ignore_fraction=0.01, min_matches=6,
    max_hits_per_hash=1000, limit_candidates=50):
    """
    Call the Postgres function find_fingerprint_candidates(...) directly to find matching videos
    given a clip's fingerprint occurrences.

    Converts the list of (hash, t_ref) pairs into JSON so it can be passed to the SQL function.

    Args:
        conn: Active psycopg connection
        occurrences: Iterable of dicts or tuples containing (hash, t_ref)
        ignore_fraction, min_matches, max_hits_per_hash, limit_candidates: Parameters controlling match logic

    Returns:
        List of candidate rows returned by the database function.
    """
    # Convert Python occurrences into JSON for Postgres' JSONB argument
    occ_payload = [
        {"hash": str(o["hash"]), "t_ref": int(o["t_ref"])}
        if isinstance(o, dict) else {"hash": str(o[0]), "t_ref": int(o[1])}
        for o in occurrences
    ]
    print(f"[DB] Sending {len(occ_payload)} occurrences to matcher.")
    occ_json = json.dumps(occ_payload)

    # SQL query calling the Postgres stored function directly
    query = """
        SELECT * FROM find_fingerprint_candidates(
            %(occurrences)s::jsonb,
            %(ignore_fraction)s,
            %(min_matches)s,
            %(max_hits_per_hash)s,
            %(limit_candidates)s
        );
    """

    # Execute the query and fetch all resulting candidate rows
    with conn.cursor() as cur:
        cur.execute(query, {
            "occurrences": occ_json,
            "ignore_fraction": ignore_fraction,
            "min_matches": min_matches,
            "max_hits_per_hash": max_hits_per_hash,
            "limit_candidates": limit_candidates
        })
        rows = cur.fetchall()
        print(f"[DB] Matcher returned {len(rows)} candidate rows.")
        return rows


def fetch_occurrences_by_video(conn, video_id, limit=10000):
    """
    Retrieve up to 'limit' fingerprint_occurrence rows associated with a given video_id.

    Args:
        conn: Active psycopg connection
        video_id: Video UUID or ID to query
        limit: Maximum number of rows to fetch

    Returns:
        List of (hash, video_id, t_ref) rows.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT hash, video_id, t_ref FROM fingerprints WHERE video_id = %s LIMIT %s",
            (video_id, limit)
        )
        return cur.fetchall()


def fetch_occurrences_for_hashes(conn, hashes, limit_per_hash=2000):
    """
    Fetch occurrences for a list of specific hash values.
    Useful for inspecting which videos a given set of hashes appears in.

    Args:
        conn: Active psycopg connection
        hashes: Sequence of hash strings
        limit_per_hash: Maximum number of rows to return per hash

    Returns:
        List of matching (hash, video_id, t_ref) rows.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT hash, video_id, t_ref FROM fingerprints "
            "WHERE hash = ANY(%s) LIMIT %s",
            (list(hashes), limit_per_hash)
        )
        return cur.fetchall()


def next_videos_batch(limit: int = 100, cursor: Optional[dict] = None):
    """
    Retrieve the next batch of videos pending processing using keyset pagination.
    Calls the stored function get_videos_pending_keyset(...) directly.

    Args:
        limit: Maximum number of videos to fetch in one batch
        cursor: Dictionary containing 'duration' and 'id' from the last fetched video
                (used as the keyset position for the next batch)

    Returns:
        Tuple of:
          - rows: List of videos returned from the query
          - next_cursor: Dictionary representing the last video's keyset (for the next call)
    """
    conn = get_conn()
    print(f"[DB] Requesting pending videos (limit={limit}, cursor={cursor}).")
    # Prepare parameters for the keyset pagination function
    params = {
        "p_limit": limit,
        "p_last_duration": None if not cursor else cursor["duration"],
        "p_last_id": None if not cursor else cursor["id"],
    }

    query = """
        SELECT * FROM get_videos_pending_keyset(
            %(p_limit)s,
            %(p_last_duration)s,
            %(p_last_id)s
        );
    """

    # Execute the stored function and collect its output rows
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    # Compute a new cursor from the final row in the batch for pagination continuity
    next_cursor = None
    if rows:
        last = rows[-1]
        next_cursor = {"duration": last["duration"], "id": last["id"]}

    conn.close()
    print(f"[DB] Retrieved {len(rows)} rows. Next cursor: {next_cursor}")
    return rows, next_cursor


def delete_video_fingerprints(conn, video_id: str) -> int:
    """
    Remove every fingerprint occurrence associated with `video_id`,
    and mark the video as not fingerprinted.

    Assumes your AFTER DELETE trigger on `fingerprint_hashes`:
      - decrements fingerprint_hashes.total_count per row
      - when the last (hash, video_id) row is removed
        decrements fingerprint_hashes.video_count
      - deletes fingerprint_hashes rows when either counter hits 0

    Returns:
        Number of rows deleted from fingerprints.
    """
    # Using `with conn:` makes the whole block a transaction (commit on success, rollback on error)
    with conn:
        with conn.cursor() as cur:
            # Delete all occurrences for this video (triggers handle counters/mappings/cleanup)
            cur.execute(
                "DELETE FROM public.fingerprints WHERE video_id = %s;",
                (video_id,)
            )
            deleted = cur.rowcount  # how many occurrence rows were removed
            print(f"[DB] Deleted {deleted} fingerprint rows for video {video_id}.")

            # Mark the video as not fingerprinted
            cur.execute(
                "UPDATE public.videos SET match_status = NULL WHERE id = %s;",
                (video_id,)
            )
            print(f"[DB] Cleared match_status for video {video_id}.")

    return deleted

def mark_video_status(video_uuid, status, conn=None, original_video_id=None):
    """Update video status and optionally set original_video_id."""
    created_conn = False
    if conn is None:
        conn = get_conn()
        created_conn = True

    with conn.cursor() as cur:
        if original_video_id:
            cur.execute(
                """
                UPDATE public.videos
                SET original_video_id = %s,
                    match_status = %s
                WHERE id = %s;
                """,
                (original_video_id, status, video_uuid),
            )
        else:
            cur.execute(
                """
                UPDATE public.videos
                SET match_status = %s
                WHERE id = %s;
                """,
                (status, video_uuid),
            )
    print(f"[DB] Video {video_uuid} status updated to '{status}' (original={original_video_id}).")
    # Commit and close connection if function created the connection
    if created_conn:
        conn.commit()
        conn.close()




if __name__ == "__main__":
    conn = get_conn()
    video_id = "575078a3-2c0e-44be-ae52-427e4ddd6727"
    deleted_rows = delete_video_fingerprints(conn, video_id)
    print(f"Total Rows Deleted: {deleted_rows}")
