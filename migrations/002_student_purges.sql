-- Apply after 001_homework_files.sql and the backend student-deletion barrier migration.
-- These independent receipts deliberately have no FK to the student/submission/job.
CREATE TABLE IF NOT EXISTS homework_service_capabilities (
  service_name VARCHAR(64) PRIMARY KEY,
  protocol_version INT NOT NULL,
  heartbeat_at DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS homework_object_ownership (
  object_key VARCHAR(512) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
  student_id BIGINT NOT NULL,
  submission_id BIGINT UNSIGNED NULL,
  upload_job_id CHAR(36) NULL,
  kind VARCHAR(16) NOT NULL,
  writable_until DATETIME(6) NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY ix_hw_owned_student (student_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS homework_student_purges (
  job_id CHAR(36) PRIMARY KEY,
  student_id BIGINT NOT NULL UNIQUE,
  status VARCHAR(16) NOT NULL DEFAULT 'queued',
  total_units BIGINT UNSIGNED NOT NULL DEFAULT 0,
  completed_units BIGINT UNSIGNED NOT NULL DEFAULT 0,
  inventory_completed_at DATETIME(6) NULL,
  sql_unlinked_at DATETIME(6) NULL,
  wait_until DATETIME(6) NULL,
  lease_owner CHAR(36) NULL,
  lease_expires_at DATETIME(6) NULL,
  attempts INT UNSIGNED NOT NULL DEFAULT 0,
  available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  error_code VARCHAR(64) NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  completed_at DATETIME(6) NULL,
  KEY ix_hw_purge_claim (status,available_at,lease_expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS homework_student_purge_objects (
  job_id CHAR(36) NOT NULL,
  object_key VARCHAR(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  kind VARCHAR(16) NOT NULL,
  not_before DATETIME(6) NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'queued',
  attempts INT UNSIGNED NOT NULL DEFAULT 0,
  error_code VARCHAR(64) NULL,
  initial_deleted_at DATETIME(6) NULL,
  completed_at DATETIME(6) NULL,
  PRIMARY KEY (job_id,object_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS homework_s3_delete_receipts (
  queue_id BIGINT UNSIGNED NOT NULL,
  generation BIGINT UNSIGNED NOT NULL,
  object_key VARCHAR(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  completed_at DATETIME(6) NOT NULL,
  PRIMARY KEY(queue_id,generation)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP PROCEDURE IF EXISTS migrate_homework_purges;
DELIMITER //
CREATE PROCEDURE migrate_homework_purges()
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE()
      AND table_name='homework_file_jobs' AND column_name='processed_output_key') THEN
    ALTER TABLE homework_file_jobs ADD COLUMN processed_output_key VARCHAR(512) NULL;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE()
      AND table_name='homework_file_jobs' AND column_name='upload_expires_at') THEN
    ALTER TABLE homework_file_jobs ADD COLUMN upload_expires_at DATETIME(6) NULL;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE()
      AND table_name='homework_s3_delete_queue' AND column_name='lease_owner') THEN
    ALTER TABLE homework_s3_delete_queue
      ADD COLUMN lease_owner CHAR(36) NULL,
      ADD COLUMN lease_expires_at DATETIME(6) NULL,
      ADD COLUMN completed_at DATETIME(6) NULL;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE()
      AND table_name='homework_s3_delete_queue' AND column_name='generation') THEN
    ALTER TABLE homework_s3_delete_queue ADD COLUMN generation BIGINT UNSIGNED NOT NULL DEFAULT 1;
  END IF;
  ALTER TABLE homework_s3_delete_queue MODIFY COLUMN attempts INT UNSIGNED NOT NULL DEFAULT 0;
END//
DELIMITER ;
CALL migrate_homework_purges();
DROP PROCEDURE migrate_homework_purges;

-- Historical keys were not part of the metadata-only audit. Abort before
-- copying them into the ASCII registry, even with non-strict session SQL mode.
-- Additive tables/columns above may already exist when this assertion aborts.
-- Never merge, trim, transcode or repair content here; operator review required.
DROP PROCEDURE IF EXISTS migrate_homework_purges_preflight;
DELIMITER //
CREATE PROCEDURE migrate_homework_purges_preflight()
BEGIN
  IF EXISTS (SELECT 1 FROM (
    SELECT CAST(f.object_key AS BINARY) object_key,s.student_id,f.submission_id
      FROM homework_submission_files f LEFT JOIN homework_submissions s ON s.id=f.submission_id
    UNION ALL SELECT CAST(staging_key AS BINARY),student_id,submission_id
      FROM homework_file_jobs WHERE staging_key IS NOT NULL
    UNION ALL SELECT CAST(processed_output_key AS BINARY),student_id,submission_id
      FROM homework_file_jobs WHERE processed_output_key IS NOT NULL
    UNION ALL SELECT CAST(object_key AS BINARY),student_id,submission_id
      FROM homework_object_ownership
  ) historical_keys WHERE object_key IS NULL OR OCTET_LENGTH(object_key)=0
      OR OCTET_LENGTH(object_key)>512 OR OCTET_LENGTH(TRIM(object_key))=0
      OR OCTET_LENGTH(object_key)<>CHAR_LENGTH(CONVERT(object_key USING utf8mb4))) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='homework_historical_object_key_invalid';
  END IF;
  IF EXISTS (SELECT 1 FROM (
    SELECT CAST(f.object_key AS BINARY) object_key,s.student_id,f.submission_id
      FROM homework_submission_files f LEFT JOIN homework_submissions s ON s.id=f.submission_id
    UNION ALL SELECT CAST(staging_key AS BINARY),student_id,submission_id
      FROM homework_file_jobs WHERE staging_key IS NOT NULL
    UNION ALL SELECT CAST(processed_output_key AS BINARY),student_id,submission_id
      FROM homework_file_jobs WHERE processed_output_key IS NOT NULL
    UNION ALL SELECT CAST(object_key AS BINARY),student_id,submission_id
      FROM homework_object_ownership
  ) historical_keys WHERE student_id IS NULL OR student_id<1) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='homework_historical_object_owner_invalid';
  END IF;
  IF EXISTS (SELECT 1 FROM (
    SELECT CONVERT(object_key USING ascii) COLLATE ascii_bin registry_key
    FROM (
      SELECT CAST(f.object_key AS BINARY) object_key,s.student_id,f.submission_id
      FROM homework_submission_files f LEFT JOIN homework_submissions s ON s.id=f.submission_id
    UNION ALL SELECT CAST(staging_key AS BINARY),student_id,submission_id
      FROM homework_file_jobs WHERE staging_key IS NOT NULL
    UNION ALL SELECT CAST(processed_output_key AS BINARY),student_id,submission_id
      FROM homework_file_jobs WHERE processed_output_key IS NOT NULL
    UNION ALL SELECT CAST(object_key AS BINARY),student_id,submission_id
      FROM homework_object_ownership
    ) historical_keys GROUP BY registry_key
    HAVING COUNT(DISTINCT object_key)>1 OR COUNT(DISTINCT student_id)>1
      OR COUNT(DISTINCT submission_id)>1
  ) conflicts) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='homework_historical_object_key_collision';
  END IF;
END//
DELIMITER ;
CALL migrate_homework_purges_preflight();
DROP PROCEDURE migrate_homework_purges_preflight;

-- Capture every attributable historical key before new code can unlink it.
-- Unknown historical outbox-only objects have no safe inferred owner.
INSERT INTO homework_object_ownership(object_key,student_id,submission_id,kind)
SELECT f.object_key,s.student_id,s.id,'processed'
FROM homework_submission_files f JOIN homework_submissions s ON s.id=f.submission_id
LEFT JOIN homework_object_ownership o ON CAST(o.object_key AS BINARY)=CAST(f.object_key AS BINARY)
WHERE o.object_key IS NULL
AND NOT EXISTS(SELECT 1 FROM student_deletion_barriers b WHERE b.student_id=s.student_id);

INSERT INTO homework_object_ownership(object_key,student_id,submission_id,upload_job_id,kind,writable_until)
SELECT j.staging_key,j.student_id,j.submission_id,j.id,'staging',j.upload_expires_at
FROM homework_file_jobs j LEFT JOIN homework_object_ownership o ON CAST(o.object_key AS BINARY)=CAST(j.staging_key AS BINARY)
WHERE j.staging_key IS NOT NULL AND o.object_key IS NULL
AND NOT EXISTS(SELECT 1 FROM student_deletion_barriers b WHERE b.student_id=j.student_id);

INSERT INTO homework_object_ownership(object_key,student_id,submission_id,upload_job_id,kind)
SELECT j.processed_output_key,j.student_id,j.submission_id,j.id,'processed'
FROM homework_file_jobs j LEFT JOIN homework_object_ownership o ON CAST(o.object_key AS BINARY)=CAST(j.processed_output_key AS BINARY)
WHERE j.processed_output_key IS NOT NULL AND o.object_key IS NULL
AND NOT EXISTS(SELECT 1 FROM student_deletion_barriers b WHERE b.student_id=j.student_id);
