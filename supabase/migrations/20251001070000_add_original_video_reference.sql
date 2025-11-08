ALTER TABLE videos
ADD COLUMN original_video_id UUID REFERENCES videos(id) ON DELETE SET NULL;

CREATE INDEX idx_videos_original_video_id ON videos(original_video_id);
