ALTER TABLE videos
ADD COLUMN IF NOT EXISTS fingerprinted BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_videos_fingerprinted ON videos(fingerprinted);
CREATE INDEX IF NOT EXISTS idx_fingerprints_video_id ON fingerprints(video_id);

UPDATE videos
SET fingerprinted = TRUE
WHERE id IN (SELECT DISTINCT video_id FROM fingerprints);

UPDATE videos
SET fingerprinted = TRUE
WHERE original_video_id IS NOT NULL;
