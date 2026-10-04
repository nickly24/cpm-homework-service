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

Удаление ученика использует уже существующую `homework_s3_delete_queue`.
Основной backend добавляет туда конкретные ключи в своей SQL-транзакции,
а этот worker удаляет объект и подтверждает отсутствие через HEAD перед
удалением строки очереди. Ошибка оставляет задание для повтора; активная ссылка
на файл сохраняет объект. Зависший `running` возвращается в обработку через
пять минут. Новые purge-таблицы, триггеры или миграция 002 не требуются.

Для изолированной проверки без настоящих настроек подключения:

```bash
python scripts/run_local_tests.py discover -s tests -v
```

```bash
PYTHONPATH=. python -m unittest discover -s tests -v
```

`scripts/load_smoke.py` создаёт 300 параллельных запросов инициализации upload,
не передавая PDF через сервис. Он требует `HOMEWORK_SERVICE_URL`, `HOMEWORK_ID` и
`STUDENT_JWT`. `scripts/s3_smoke.py` проверяет реальный bucket строго под
`_test/`: встроенный guard запрещает удаление любого обычного production key.
