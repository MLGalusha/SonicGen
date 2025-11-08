CREATE TABLE videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- internal ID
    video_id TEXT UNIQUE NOT NULL,                  -- YouTube video ID
    title TEXT,                                     -- video title
    description TEXT,                               -- video description
    published_at TIMESTAMPTZ,                       -- publishedAt from YouTube
    channel_id TEXT,                                -- YouTube channel ID
    channel_title TEXT,                             -- channel title
    duration INTERVAL,                              -- video duration
    view_count BIGINT,                              -- total views
    like_count BIGINT,                              -- total likes
    comment_count BIGINT,                           -- total comments
    created_at TIMESTAMPTZ DEFAULT now()            -- when we inserted
);


CREATE TABLE fingerprints (
    id BIGSERIAL PRIMARY KEY,
    video_id UUID REFERENCES videos(id) ON DELETE CASCADE,
    hash TEXT NOT NULL,
    t_ref INT NOT NULL
);

CREATE INDEX idx_fingerprints_hash ON fingerprints(hash);