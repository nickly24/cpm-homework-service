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
- `GET /api/review-queue`
- `POST /api/submissions/{id}/{claim|takeover|release|request-revision|grade|edit-grade|resubmit}`
- `GET /api/submissions/{id}/file-url?draft=0&download=0`
- `GET /api/archive`
- `GET /api/monitoring`

JWT принимается только как `Authorization: Bearer ...`, подпись — только HS256.
Сервис использует тот же `JWT_SECRET_KEY`, что основной backend, и на каждом
запросе дополнительно проверяет аккаунт и актуальную группу в MySQL.

## Проверка

```bash
PYTHONPATH=. python -m unittest discover -s tests -v
```

`scripts/load_smoke.py` создаёт 300 параллельных запросов инициализации upload,
не передавая PDF через сервис. Он требует `HOMEWORK_SERVICE_URL`, `HOMEWORK_ID` и
`STUDENT_JWT`. `scripts/s3_smoke.py` проверяет реальный bucket строго под
`_test/`: встроенный guard запрещает удаление любого обычного production key.
