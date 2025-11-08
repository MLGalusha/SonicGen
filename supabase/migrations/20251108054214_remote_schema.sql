drop extension if exists "pg_net";

revoke delete on table "public"."fingerprint_hash_counts" from "anon";

revoke insert on table "public"."fingerprint_hash_counts" from "anon";

revoke references on table "public"."fingerprint_hash_counts" from "anon";

revoke select on table "public"."fingerprint_hash_counts" from "anon";

revoke trigger on table "public"."fingerprint_hash_counts" from "anon";

revoke truncate on table "public"."fingerprint_hash_counts" from "anon";

revoke update on table "public"."fingerprint_hash_counts" from "anon";

revoke delete on table "public"."fingerprint_hash_counts" from "authenticated";

revoke insert on table "public"."fingerprint_hash_counts" from "authenticated";

revoke references on table "public"."fingerprint_hash_counts" from "authenticated";

revoke select on table "public"."fingerprint_hash_counts" from "authenticated";

revoke trigger on table "public"."fingerprint_hash_counts" from "authenticated";

revoke truncate on table "public"."fingerprint_hash_counts" from "authenticated";

revoke update on table "public"."fingerprint_hash_counts" from "authenticated";

revoke delete on table "public"."fingerprint_hash_counts" from "service_role";

revoke insert on table "public"."fingerprint_hash_counts" from "service_role";

revoke references on table "public"."fingerprint_hash_counts" from "service_role";

revoke select on table "public"."fingerprint_hash_counts" from "service_role";

revoke trigger on table "public"."fingerprint_hash_counts" from "service_role";

revoke truncate on table "public"."fingerprint_hash_counts" from "service_role";

revoke update on table "public"."fingerprint_hash_counts" from "service_role";

revoke delete on table "public"."noisy_hashes" from "anon";

revoke insert on table "public"."noisy_hashes" from "anon";

revoke references on table "public"."noisy_hashes" from "anon";

revoke select on table "public"."noisy_hashes" from "anon";

revoke trigger on table "public"."noisy_hashes" from "anon";

revoke truncate on table "public"."noisy_hashes" from "anon";

revoke update on table "public"."noisy_hashes" from "anon";

revoke delete on table "public"."noisy_hashes" from "authenticated";

revoke insert on table "public"."noisy_hashes" from "authenticated";

revoke references on table "public"."noisy_hashes" from "authenticated";

revoke select on table "public"."noisy_hashes" from "authenticated";

revoke trigger on table "public"."noisy_hashes" from "authenticated";

revoke truncate on table "public"."noisy_hashes" from "authenticated";

revoke update on table "public"."noisy_hashes" from "authenticated";

revoke delete on table "public"."noisy_hashes" from "service_role";

revoke insert on table "public"."noisy_hashes" from "service_role";

revoke references on table "public"."noisy_hashes" from "service_role";

revoke select on table "public"."noisy_hashes" from "service_role";

revoke trigger on table "public"."noisy_hashes" from "service_role";

revoke truncate on table "public"."noisy_hashes" from "service_role";

revoke update on table "public"."noisy_hashes" from "service_role";

alter table "public"."fingerprints" drop constraint "fingerprints_video_id_fkey";

alter table "public"."videos" drop constraint "videos_video_id_key";

drop function if exists "public"."adjust_hash_counts"(items jsonb);

drop function if exists "public"."refresh_noisy_hashes"(threshold integer);

alter table "public"."fingerprint_hash_counts" drop constraint "fingerprint_hash_counts_pkey";

alter table "public"."fingerprints" drop constraint "fingerprints_pkey";

alter table "public"."noisy_hashes" drop constraint "noisy_hashes_pkey";

drop index if exists "public"."fingerprint_hash_counts_pkey";

drop index if exists "public"."fingerprints_pkey";

drop index if exists "public"."idx_fingerprint_hash_counts_hash";

drop index if exists "public"."idx_fingerprints_hash";

drop index if exists "public"."idx_fingerprints_video_id";

drop index if exists "public"."idx_noisy_hashes_hash";

drop index if exists "public"."idx_videos_fingerprinted";

drop index if exists "public"."noisy_hashes_pkey";

drop index if exists "public"."videos_video_id_key";

drop table "public"."fingerprint_hash_counts";

drop table "public"."noisy_hashes";


  create table "public"."fingerprint_hashes" (
    "hash" text not null,
    "total_count" bigint not null default 0,
    "video_count" bigint not null default 0
      );


alter table "public"."fingerprints" drop column "id";

alter table "public"."fingerprints" alter column "video_id" set not null;

alter table "public"."videos" drop column "comment_count";

alter table "public"."videos" drop column "fingerprinted";

alter table "public"."videos" drop column "like_count";

alter table "public"."videos" drop column "video_id";

alter table "public"."videos" drop column "view_count";

alter table "public"."videos" add column "match_status" text;

alter table "public"."videos" add column "youtube_id" text not null;

drop sequence if exists "public"."fingerprints_id_seq";

CREATE UNIQUE INDEX fingerprint_hashes_pkey ON public.fingerprint_hashes USING btree (hash);

CREATE UNIQUE INDEX fingerprints_merged_pkey ON public.fingerprints USING btree (hash, video_id, t_ref);

CREATE INDEX fp_occ_hash_tref ON public.fingerprints USING btree (hash, t_ref);

CREATE INDEX idx_fingerprints_total_count_desc ON public.fingerprint_hashes USING btree (total_count DESC);

CREATE INDEX idx_fingerprints_video_count_desc ON public.fingerprint_hashes USING btree (video_count DESC);

CREATE UNIQUE INDEX videos_video_id_key ON public.videos USING btree (youtube_id);

alter table "public"."fingerprint_hashes" add constraint "fingerprint_hashes_pkey" PRIMARY KEY using index "fingerprint_hashes_pkey";

alter table "public"."fingerprints" add constraint "fingerprints_merged_pkey" PRIMARY KEY using index "fingerprints_merged_pkey";

alter table "public"."videos" add constraint "videos_match_status_check" CHECK ((match_status = ANY (ARRAY['pending'::text, 'matched'::text, 'fingerprinted'::text, 'too_short'::text, 'flag'::text]))) not valid;

alter table "public"."videos" validate constraint "videos_match_status_check";

alter table "public"."videos" add constraint "videos_video_id_key" UNIQUE using index "videos_video_id_key";

set check_function_bodies = off;

CREATE OR REPLACE FUNCTION public.find_fingerprint_candidates(occurrences jsonb, ignore_fraction double precision DEFAULT 0.01, min_matches integer DEFAULT 6, max_hits_per_hash integer DEFAULT 1000, limit_candidates integer DEFAULT 50)
 RETURNS TABLE(video_id uuid, delta integer, hashes text[], matches bigint)
 LANGUAGE plpgsql
AS $function$
BEGIN
  RETURN QUERY
  WITH input AS (
    SELECT (x->>'hash')::text AS hash,
           (x->>'t_ref')::int  AS t_ref
    FROM jsonb_array_elements(occurrences) AS x
  ),
  stop AS (
    SELECT hash FROM public.get_stopwords(ignore_fraction)
  ),
  q AS (
    SELECT i.hash, i.t_ref
    FROM input i
    LEFT JOIN stop s USING (hash)
    WHERE s.hash IS NULL
  ),
  j AS (
    SELECT f.video_id,
           (f.t_ref - q.t_ref) AS delta,
           q.hash,
           ROW_NUMBER() OVER (PARTITION BY q.hash ORDER BY f.video_id, f.t_ref) AS rn
    FROM q
    JOIN public.fingerprints f
      ON f.hash = q.hash
  )
  SELECT
    j.video_id,
    j.delta,
    ARRAY_AGG(DISTINCT j.hash) AS hashes,
    COUNT(*) AS matches
  FROM j
  WHERE j.rn <= max_hits_per_hash
  GROUP BY j.video_id, j.delta
  HAVING COUNT(*) >= min_matches
  ORDER BY matches DESC
  LIMIT limit_candidates;
END;
$function$
;

create type "public"."fingerprint_occurrence" as ("video_id" uuid, "t_ref" integer);

CREATE OR REPLACE FUNCTION public.fingerprint_top_limit(fraction numeric)
 RETURNS bigint
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
  est_count bigint;
  n_limit   bigint;
BEGIN
  SELECT reltuples::bigint INTO est_count
  FROM pg_class
  WHERE relname = 'fingerprint_hash_counts';

  n_limit := GREATEST(1, FLOOR(COALESCE(est_count,0) * fraction))::bigint;
  RETURN n_limit;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fp_occ_after_delete_stmt()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  -- deleted(hash, video_id, t_ref) is available as a transition table
  -- A) total_count: subtract number of deleted rows per hash
  WITH del_tot AS (
    SELECT hash, COUNT(*)::bigint AS cnt
    FROM deleted
    GROUP BY hash
  )
  UPDATE public.fingerprint_hashes f
  SET total_count = GREATEST(f.total_count - d.cnt, 0)
  FROM del_tot d
  WHERE f.hash = d.hash;

  -- B) pairs that appeared in this delete
  --    figure out which (hash, video_id) pairs are now GONE (no remaining occurrences)
  WITH pairs AS (
    SELECT DISTINCT hash, video_id
    FROM deleted
  ),
  remaining AS (
    SELECT o.hash, o.video_id
    FROM public.fingerprints o
    JOIN pairs p USING (hash, video_id)
    GROUP BY o.hash, o.video_id
  ),
  pairs_gone AS (
    SELECT p.hash, p.video_id
    FROM pairs p
    LEFT JOIN remaining r USING (hash, video_id)
    WHERE r.hash IS NULL
  )

  -- D) decrement video_count once per affected hash whose pair disappeared
  UPDATE public.fingerprint_hashes f
  SET video_count = GREATEST(f.video_count - 1, 0)
  FROM (SELECT DISTINCT hash FROM pairs_gone) g
  WHERE f.hash = g.hash;

  -- E) cleanup: drop summary rows that are now empty by either metric
  DELETE FROM public.fingerprint_hashes
  WHERE total_count = 0 OR video_count = 0;

  RETURN NULL; -- statement-level trigger
END
$function$
;

CREATE OR REPLACE FUNCTION public.get_stopwords(fraction double precision)
 RETURNS TABLE(hash text)
 LANGUAGE sql
AS $function$SELECT hash
FROM public.fingerprint_hashes
ORDER BY total_count DESC
LIMIT GREATEST(1, FLOOR(fraction * (SELECT COUNT(*) FROM public.fingerprint_hashes)));$function$
;

CREATE OR REPLACE FUNCTION public.get_videos_pending_keyset(p_limit integer, p_last_duration interval DEFAULT NULL::interval, p_last_id uuid DEFAULT NULL::uuid)
 RETURNS SETOF public.videos
 LANGUAGE sql
 STABLE
AS $function$SELECT v.*
FROM videos v
WHERE v.match_status IS NULL
  AND (
    p_last_duration IS NULL
    OR (v.duration, v.id) < (p_last_duration, v.id)
  )
ORDER BY v.duration DESC, v.id DESC
LIMIT p_limit;$function$
;

CREATE OR REPLACE FUNCTION public.set_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END
$function$
;

grant delete on table "public"."fingerprint_hashes" to "anon";

grant insert on table "public"."fingerprint_hashes" to "anon";

grant references on table "public"."fingerprint_hashes" to "anon";

grant select on table "public"."fingerprint_hashes" to "anon";

grant trigger on table "public"."fingerprint_hashes" to "anon";

grant truncate on table "public"."fingerprint_hashes" to "anon";

grant update on table "public"."fingerprint_hashes" to "anon";

grant delete on table "public"."fingerprint_hashes" to "authenticated";

grant insert on table "public"."fingerprint_hashes" to "authenticated";

grant references on table "public"."fingerprint_hashes" to "authenticated";

grant select on table "public"."fingerprint_hashes" to "authenticated";

grant trigger on table "public"."fingerprint_hashes" to "authenticated";

grant truncate on table "public"."fingerprint_hashes" to "authenticated";

grant update on table "public"."fingerprint_hashes" to "authenticated";

grant delete on table "public"."fingerprint_hashes" to "service_role";

grant insert on table "public"."fingerprint_hashes" to "service_role";

grant references on table "public"."fingerprint_hashes" to "service_role";

grant select on table "public"."fingerprint_hashes" to "service_role";

grant trigger on table "public"."fingerprint_hashes" to "service_role";

grant truncate on table "public"."fingerprint_hashes" to "service_role";

grant update on table "public"."fingerprint_hashes" to "service_role";

CREATE TRIGGER t_fp_occ_after_delete_stmt AFTER DELETE ON public.fingerprints REFERENCING OLD TABLE AS deleted FOR EACH STATEMENT EXECUTE FUNCTION public.fp_occ_after_delete_stmt();


