# CPM Homework Service

Отдельный Flask-сервис для PDF домашних работ. Он использует общие JWT и MySQL с
основным CPM backend, но не содержит чатов, Socket.IO, push, VAPID, notifications
или MongoDB.

## Почему загрузка не перегружает сервис

Сервис выдаёт короткоживущий presigned POST. Браузер отправляет PDF напрямую в
Timeweb S3, поэтому байты файла не проходят через Flask/Gunicorn. После загрузки
браузер вызывает `complete`, а единственный глобальный worker последовательно
обрабатывает очередь. В каждом процессе Gunicorn запускается лёгкий runner, но
MySQL `GET_LOCK` допускает только одну тяжёлую PDF-операцию одновременно.

## Локальный запуск

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
# экспортировать значения .env удобным для вашей оболочки способом
.venv/bin/gunicorn main:app --bind 0.0.0.0:8001 -k gthread -w 2 --threads 8 -t 120
```

Перед первым запуском применить `migrations/001_homework_files.sql`. Миграция
повторяемая. При дублях `homework_sessions(homework_id,student_id)` или
`proctors(group_id)` она останавливается и ничего не объединяет автоматически.

## Direct upload

1. `POST /api/workspaces/{homework_id}/uploads` с JSON
   `{"client_upload_id":"UUID"}` и Bearer JWT.
2. Выполнить multipart POST непосредственно на `upload.url`, передав все
   `upload.fields` и PDF как поле `file`.
3. `POST /api/uploads/{job_id}/complete`.
4. Пока `GET /api/jobs/active` возвращает `polling_required=true`, повторять его
   не чаще одного раза в 10 секунд. Когда список пуст, polling полностью прекратить.
5. После `ready` открыть workspace и выполнить
   `POST /api/workspaces/{homework_id}/submit`.

S3 CORS настраивается один раз явным запуском
`python scripts/configure_s3_cors.py`. Приложение само настройки bucket не меняет.
Для `staging/` следует добавить lifecycle-удаление объектов старше двух суток.

## Основные API

- `GET /health`
- `GET /api/workspaces/{homework_id}` (`student_id` нужен staff)
- `POST /api/workspaces/{homework_id}/uploads`
- `POST /api/uploads/{job_id}/complete`
- `GET /api/jobs/active`, `GET /api/jobs/{job_id}`
- `POST /api/jobs/{job_id}/cancel`, `POST /api/jobs/{job_id}/retry`
- `POST /api/workspaces/{homework_id}/submit`
- `DELETE /api/workspaces/{homework_id}/draft` — убрать готовый черновик до отправки
- `GET /api/review-queue`
- `POST /api/submissions/{id}/{claim|takeover|release|request-revision|grade|edit-grade|resubmit}`
- `GET /api/submissions/{id}/file-url?draft=0&download=0`
- `GET /api/archive`
- `GET /api/monitoring`

JWT принимается только как `Authorization: Bearer ...`, подпись — только HS256.
Сервис использует тот же `JWT_SECRET_KEY`, что основной backend, и на каждом
запросе дополнительно проверяет аккаунт и актуальную группу в MySQL.

## Состояние работы и проверка

Загрузка и отправка — разные действия. Готовый PDF остаётся черновиком до `submit`.
В одной домашней работе может выполняться только одна загрузка/обработка; новый
upload, отправка и удаление черновика при активной задаче возвращают
`409 upload_in_progress`. Повтор `submit` уже отправленной работы возвращает её
состояние и исходное время отправки. `result` отсутствует — автоматический балл;
явные `null`, дробные числа, NaN, бесконечность и значения вне 0–100 отклоняются.

Workspace содержит `submission.current_file` и `submission.draft_file`
(`id`, `filename`, `page_count`, `size_bytes`, `created_at`), `reviewer.full_name`,
`suggested_score`, `active_job`, `limits` и `permissions.remove_draft`.
Имя PDF формируется из ученика, задания и даты: исходное локальное имя не хранится.
Черновик и сведения о его загрузке доступны только ученику.

`GET /api/review-queue?search=&state=&after=0&limit=50` ищет по ученику,
заданию и группе; state — `submitted`, `in_review`, `revision_requested` или
`all`. Ответ: `items`, `total`, `has_more`, `next_cursor`. Следующая страница
использует `after=next_cursor`; очередь упорядочена по ID по возрастанию.
Строки содержат метаданные PDF, `reviewer_name`, `suggested_score`.

`GET /api/archive` также доступен проктору для учеников его текущей группы.
Поддерживает те же `search`, `after`, `limit` и поля пагинации; ID идут по убыванию.
Сохраняются фильтры `student_id`, `homework_id`, `group_id`, `date_from`, `date_to`.
Строки содержат `result`, `deadline`, `date_pass` и метаданные PDF. `resubmit`
доступен только для оценённой работы: удаляет PDF и оценку, открывает чистую сдачу.

Основной API отклоняет ручное изменение/сброс файловой работы с
`409 use_file_review`; массовое занесение пропускает такие записи и возвращает
`skipped_files` и список `errors`. Ученик, уже отправивший PDF, не теряет право
на доработку из-за ручной отметки в старом журнале.

Незавершённые upload истекают через `UPLOAD_STALE_SECONDS` (по умолчанию
1800 секунд, не раньше двух сроков presign), освобождают работу и отправляют
staging в очередь удаления. Worker не может вернуть отправленную/оценённую работу
в черновик, а поздняя ошибка обработки не восстанавливает отменённую задачу.

## Проверка

```bash
PYTHONPATH=. python -m unittest discover -s tests -v
```

`scripts/load_smoke.py` создаёт 300 параллельных запросов инициализации upload,
не передавая PDF через сервис. Он требует `HOMEWORK_SERVICE_URL`, `HOMEWORK_ID` и
`STUDENT_JWT`. `scripts/s3_smoke.py` проверяет реальный bucket строго под
`_test/`: встроенный guard запрещает удаление любого обычного production key.

## Durable student deletion (protocol v1)

This is feature-gated by `STUDENT_DELETION_ENABLED=false` by default. Enable it
on **every backend and homework writer together** only after the shared backend
barrier migration, `002_student_purges.sql`, and the backend guard migration are
applied. Restart/drain old workers before enabling deletion. The homework
migration backfills ownership for attributable existing file/job keys and is
repeatable. `scripts/apply_migration.py apply --student-purges` applies 002 after
001; it does not apply backend migrations. Never use production configuration for
offline tests.

The primary backend owns `student_deletion_barriers(student_id,job_id,created_at)`
and `student_deletion_jobs`. It installs the barrier while holding MySQL
`GET_LOCK('cpm:student-write-fence',30)`, then inserts a request:

```sql
INSERT INTO homework_student_purges
(job_id,student_id,created_at,updated_at,available_at)
VALUES (?,?,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6),UTC_TIMESTAMP(6));
SELECT status,total_units,completed_units,wait_until,error_code
FROM homework_student_purges WHERE job_id=?;
UPDATE homework_student_purges
SET status='queued',available_at=UTC_TIMESTAMP(6),error_code=NULL
WHERE job_id=? AND status='failed';
```

Backend must require the `student-purge` row in
`homework_service_capabilities` to have `protocol_version=1` and
`heartbeat_at >= DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 60 SECOND)` before preview/start.
The worker publishes only while enabled, with the storage-policy gate below
explicitly verified, and after a successful read-only S3
`HeadBucket` probe; a failed probe invalidates readiness immediately. This checks
reachability and credentials, not delete permission for every possible key.
No new HTTP admin endpoint or shared admin secret is required.

The worker commits a durable `homework_student_purge_objects` exact-key manifest
before unlinking any files/jobs/submissions. Shared homework definitions and other
students' rows are untouched. It reports actual progress: one unit per unique
object plus one unit for committed SQL unlink. States are queued, running,
waiting, failed, completed; completion requires every receipt and SQL unlink.
The backend must wait for `completed` before deleting the account. Storage failure
stops in `failed` with manifest/progress intact; explicit retry reuses it. Expired
running leases recover after process death. There is no cross-store rollback.

All HTTP workflow transactions, background claims, and output
reservation/upload/finalization use the same reentrant global fence. PDF rendering
and source download run outside that fence; the target is rechecked afterward. Upload issuance and staff actions reject barriers;
student JWTs additionally require both live students/auth_users rows. PDF worker
output ownership is committed before S3 upload; the fence remains held through
upload and finalization. S3 output writes remain serialized with backend writes, so slow object uploads
can cause a retryable 30-second lock timeout.

Every issued POST persists its exact signed policy expiry, including reissues.
Staging is deleted initially and again after the last expiry plus
`PRESIGN_DRAIN_SECONDS` (60 seconds by default), before completion. Final delete acknowledgment is insufficient: an exact HEAD
must return 404/NoSuchKey; permission/network errors and a still-present object
keep the purge failed and retryable. Historical
staging without recorded expiry conservatively waits
`LEGACY_PRESIGN_MAX_SECONDS` (seven days by default) from barrier creation. Set
that bound only from verified previous deployment configuration. The additional flag `STUDENT_PURGE_STORAGE_POLICY_VERIFIED=false` is fail-closed
by default. Before setting it true, an operator must verify and record the provider
policy: bucket versioning/retention supports the agreed current-live-object scope,
and the S3/proxy maximum in-flight upload completion time fits the configured
drain margin. Both capability publication and every purge step require this flag.
Policy expiration alone does not revoke a transfer already accepted
by storage. If that cannot be guaranteed, do not claim strict erasure or enable
completion without storage-side write revocation. Backend and all upload issuers
must stay fenced during the waiting period.

Ownership and manifest/outbox receipt rows deliberately survive account deletion;
they contain numeric ownership IDs and opaque exact keys, not file contents.
Backend final checks must exclude these audit receipts, or require their completed
purge. Never delete them before the job is acknowledged complete. Retention policy
for these receipts is separate from content erasure. No broad prefix deletion is
used. Historical objects with no attributable key, noncurrent object versions,
backups, and third-party downloaded copies are outside this protocol's claim.

Read-only preview metadata can count submissions by student_id, files joined on
submission_id, and jobs by student_id. Unique object count must union keys from
homework_object_ownership, files joined to submissions, file_jobs.staging_key, and
file_jobs.processed_output_key. Ownership may include already-deleted keys; these
still require idempotent confirmation in the purge, so the manifest is authoritative.

Offline verification: `PYTHONPATH=. python -m unittest discover -s tests -v`.
`test_student_purges.py` uses an in-memory SQLite SQL adapter and fake S3 to verify
crash/retry/failure, late uploads, stale JWTs, two-student isolation, shared
homework, ownership-before-upload, and finalization barriers. It does not validate
MySQL DDL/trigger syntax, real advisory-lock contention, or provider behavior.
Real MySQL bootstrap DDL/repeatability checks are reported separately; client
protocol and concurrency checks remain distinct. Those integration checks must run on a separately authorized disposable MySQL/S3
fixture before release; no test here contacts production.

Turning off STUDENT_DELETION_ENABLED stops new purge work; it never bypasses
installed tombstones or enables old JWTs/presigns. Remove no barrier during a
partial failure. Outbox requeues use a new generation for cancelled/completed/failed
rows, preserve active running leases, and retain independent generation receipts.
