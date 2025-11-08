-- Create channels table
CREATE TABLE channels (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Remove redundant channel_title from videos
ALTER TABLE videos DROP COLUMN channel_title;

-- Add foreign key to channels
ALTER TABLE videos
ADD CONSTRAINT fk_videos_channel FOREIGN KEY (channel_id)
REFERENCES channels(id) ON DELETE CASCADE;
