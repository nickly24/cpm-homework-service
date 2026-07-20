-- Repeatable migration for the standalone homework-file service.
-- It deliberately does not create or delete chat, realtime, notification or push tables.

DROP PROCEDURE IF EXISTS migrate_homework_file_preflight;
DELIMITER //
CREATE PROCEDURE migrate_homework_file_preflight()
BEGIN
  IF EXISTS (
    SELECT 1 FROM homework_sessions GROUP BY homework_id,student_id HAVING COUNT(*)>1
  ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT='duplicate homework_sessions; no data was merged';
  END IF;
  IF EXISTS (
    SELECT 1 FROM proctors WHERE group_id IS NOT NULL GROUP BY group_id HAVING COUNT(*)>1
  ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT='duplicate proctors.group_id; no data was merged';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE()
      AND table_name='homework_sessions' AND index_name='uq_homework_session_pair'
  ) THEN
    ALTER TABLE homework_sessions
      ADD UNIQUE KEY uq_homework_session_pair (homework_id,student_id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE()
      AND table_name='proctors' AND index_name='uq_proctor_group'
  ) THEN
    ALTER TABLE proctors ADD UNIQUE KEY uq_proctor_group (group_id);
  END IF;
END//
DELIMITER ;
CALL migrate_homework_file_preflight();
DROP PROCEDURE migrate_homework_file_preflight;

CREATE TABLE IF NOT EXISTS homework_submissions (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  homework_id INT NOT NULL,
  student_id INT NOT NULL,
  state VARCHAR(32) NOT NULL DEFAULT 'none',
  draft_file_id BIGINT UNSIGNED NULL,
  current_file_id BIGINT UNSIGNED NULL,
  reviewer_role VARCHAR(16) NULL,
  reviewer_id INT NULL,
  submitted_at_utc DATETIME(6) NULL,
  revision_count INT UNSIGNED NOT NULL DEFAULT 0,
  revision_comment VARCHAR(1000) NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_hw_submission_pair (homework_id,student_id),
  KEY ix_hw_submission_queue (state,submitted_at_utc),
  KEY ix_hw_submission_reviewer (reviewer_role,reviewer_id,state),
  KEY ix_hw_submission_student (student_id,updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP PROCEDURE IF EXISTS migrate_homework_revision_comment;
DELIMITER //
CREATE PROCEDURE migrate_homework_revision_comment()
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE()
      AND table_name='homework_submissions' AND column_name='revision_comment'
  ) THEN
    ALTER TABLE homework_submissions ADD COLUMN revision_comment VARCHAR(1000) NULL
      AFTER revision_count;
  END IF;
END//
DELIMITER ;
CALL migrate_homework_revision_comment();
DROP PROCEDURE migrate_homework_revision_comment;

CREATE TABLE IF NOT EXISTS homework_submission_files (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  submission_id BIGINT UNSIGNED NOT NULL,
  object_key VARCHAR(512) NOT NULL,
  status VARCHAR(24) NOT NULL,
  size_bytes BIGINT UNSIGNED NOT NULL,
  page_count SMALLINT UNSIGNED NOT NULL,
  sha256 CHAR(64) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_hw_file_key (object_key),
  KEY ix_hw_files_submission (submission_id,status),
  CONSTRAINT fk_hw_files_submission FOREIGN KEY (submission_id)
    REFERENCES homework_submissions(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS homework_file_jobs (
  id CHAR(36) PRIMARY KEY,
  client_upload_id CHAR(36) NOT NULL,
  submission_id BIGINT UNSIGNED NOT NULL,
  homework_id INT NOT NULL,
  student_id INT NOT NULL,
  kind VARCHAR(24) NOT NULL DEFAULT 'process_upload',
  status VARCHAR(24) NOT NULL DEFAULT 'uploading',
  stage VARCHAR(24) NOT NULL DEFAULT 'uploading',
  progress TINYINT UNSIGNED NOT NULL DEFAULT 0,
  staging_key VARCHAR(512) NULL,
  source_size_bytes BIGINT UNSIGNED NULL,
  result_file_id BIGINT UNSIGNED NULL,
  attempts TINYINT UNSIGNED NOT NULL DEFAULT 0,
  manual_attempts TINYINT UNSIGNED NOT NULL DEFAULT 0,
  available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  lease_owner VARCHAR(128) NULL,
  lease_expires_at DATETIME(6) NULL,
  heartbeat_at DATETIME(6) NULL,
  error_code VARCHAR(64) NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_hw_job_client (student_id,client_upload_id),
  KEY ix_hw_jobs_claim (status,available_at,lease_expires_at),
  KEY ix_hw_jobs_submission (submission_id,created_at),
  CONSTRAINT fk_hw_jobs_submission FOREIGN KEY (submission_id)
    REFERENCES homework_submissions(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS homework_s3_delete_queue (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  object_key VARCHAR(512) NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'queued',
  attempts TINYINT UNSIGNED NOT NULL DEFAULT 0,
  available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  error_code VARCHAR(64) NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_hw_s3_delete_key (object_key),
  KEY ix_hw_s3_delete_claim (status,available_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Diagnostic reports if the preflight stopped:
SELECT homework_id,student_id,COUNT(*) duplicate_count
FROM homework_sessions GROUP BY homework_id,student_id HAVING COUNT(*)>1;
SELECT group_id,COUNT(*) duplicate_count
FROM proctors WHERE group_id IS NOT NULL GROUP BY group_id HAVING COUNT(*)>1;

