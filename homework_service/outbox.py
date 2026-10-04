"""Exact-key deletion requests with replayable generations and retained receipts."""
from . import db


def enqueue_delete(cursor, key, delay_hours=0):
    if not key:
        return
    if not db.deletion_protection_active():
        cursor.execute("INSERT IGNORE INTO homework_s3_delete_queue (object_key,status,available_at) "
                       "VALUES (%s,'queued',DATE_ADD(UTC_TIMESTAMP(6),INTERVAL %s HOUR))", (key, int(delay_hours)))
        return
    # MySQL assignments intentionally test the old status until the final clause.
    # Running generations are immutable; an active worker cannot lose its lease.
    rearm = "status IN ('cancelled','completed','failed')"
    cursor.execute(
        'INSERT INTO homework_s3_delete_queue (object_key,status,available_at,created_at,generation) '
        "VALUES (%s,'queued',DATE_ADD(UTC_TIMESTAMP(6),INTERVAL %s HOUR),UTC_TIMESTAMP(6),1) "
        'ON DUPLICATE KEY UPDATE '
        f'generation=IF({rearm},generation+1,generation),'
        f'attempts=IF({rearm},0,attempts),'
        f'available_at=IF({rearm},VALUES(available_at),available_at),'
        f'error_code=IF({rearm},NULL,error_code),'
        f'completed_at=IF({rearm},NULL,completed_at),'
        f'lease_owner=IF({rearm},NULL,lease_owner),'
        f'lease_expires_at=IF({rearm},NULL,lease_expires_at),'
        f"status=IF({rearm},'queued',status)",
        (key, int(delay_hours)),
    )
