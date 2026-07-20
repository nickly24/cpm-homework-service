"""Audit or apply the homework-file migration.

The script never prints database credentials. For local maintenance it can reuse
the existing backend configuration without copying secrets into shell history.
"""
import argparse
import importlib.util
import sys
from pathlib import Path

import mysql.connector


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / 'migrations' / '001_homework_files.sql'


def load_config(main_backend):
    if main_backend:
        config_path = Path(main_backend).resolve() / 'cpm_back' / 'config.py'
        spec = importlib.util.spec_from_file_location('cpm_backend_config_only', config_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        config = module.config
        return {
            'host': config.MYSQL_HOST,
            'port': config.MYSQL_PORT,
            'user': config.MYSQL_USER,
            'password': config.MYSQL_PASSWORD,
            'database': config.MYSQL_DATABASE,
        }
    sys.path.insert(0, str(ROOT))
    from homework_service.config import Config
    return {
        'host': Config.MYSQL_HOST,
        'port': Config.MYSQL_PORT,
        'user': Config.MYSQL_USER,
        'password': Config.MYSQL_PASSWORD,
        'database': Config.MYSQL_DATABASE,
    }


def statements(sql):
    delimiter = ';'
    buffer = []
    for raw_line in sql.splitlines():
        line = raw_line.strip()
        if line.upper().startswith('DELIMITER '):
            delimiter = line.split(None, 1)[1]
            continue
        if not line or line.startswith('--'):
            continue
        buffer.append(raw_line)
        joined = '\n'.join(buffer).rstrip()
        if joined.endswith(delimiter):
            yield joined[:-len(delimiter)].strip()
            buffer = []
    if buffer:
        raise RuntimeError('unterminated_sql_statement')


def audit(cursor):
    cursor.execute(
        'SELECT COUNT(*) count FROM ('
        'SELECT 1 FROM homework_sessions GROUP BY homework_id,student_id HAVING COUNT(*)>1'
        ') duplicates'
    )
    session_duplicates = cursor.fetchone()['count']
    cursor.execute(
        'SELECT COUNT(*) count FROM ('
        'SELECT 1 FROM proctors WHERE group_id IS NOT NULL GROUP BY group_id HAVING COUNT(*)>1'
        ') duplicates'
    )
    proctor_duplicates = cursor.fetchone()['count']
    cursor.execute(
        "SELECT table_name AS found_table FROM information_schema.tables WHERE table_schema=DATABASE() "
        "AND table_name IN ('homework_submissions','homework_submission_files',"
        "'homework_file_jobs','homework_s3_delete_queue') ORDER BY table_name"
    )
    tables = [row['found_table'] for row in cursor.fetchall()]
    cursor.execute(
        "SELECT COUNT(*) count FROM information_schema.columns WHERE table_schema=DATABASE() "
        "AND table_name='homework_submissions' AND column_name='revision_comment'"
    )
    revision_comment = bool(cursor.fetchone()['count'])
    cursor.execute(
        "SELECT table_name AS indexed_table,index_name AS found_index FROM information_schema.statistics "
        "WHERE table_schema=DATABASE() AND ((table_name='homework_sessions' "
        "AND index_name='uq_homework_session_pair') OR (table_name='proctors' "
        "AND index_name='uq_proctor_group')) GROUP BY table_name,index_name ORDER BY table_name"
    )
    unique_indexes = [f"{row['indexed_table']}.{row['found_index']}" for row in cursor.fetchall()]
    return {
        'homework_session_duplicate_pairs': session_duplicates,
        'proctor_group_duplicate_pairs': proctor_duplicates,
        'existing_file_tables': tables,
        'revision_comment_exists': revision_comment,
        'required_unique_indexes': unique_indexes,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('check', 'apply'))
    parser.add_argument('--main-backend-config')
    args = parser.parse_args()
    connection = mysql.connector.connect(**load_config(args.main_backend_config), autocommit=False)
    try:
        cursor = connection.cursor(dictionary=True)
        before = audit(cursor)
        print('AUDIT', before)
        if before['homework_session_duplicate_pairs'] or before['proctor_group_duplicate_pairs']:
            raise SystemExit('Preflight failed: duplicate pairs found; database was not modified')
        if args.mode == 'check':
            connection.rollback()
            print('CHECK_OK')
            return
        cursor.close()
        cursor = connection.cursor()
        for statement in statements(MIGRATION.read_text()):
            cursor.execute(statement)
            if cursor.with_rows:
                cursor.fetchall()
            while cursor.nextset():
                if cursor.with_rows:
                    cursor.fetchall()
        connection.commit()
        cursor.close()
        cursor = connection.cursor(dictionary=True)
        after = audit(cursor)
        print('AFTER', after)
        print('MIGRATION_OK')
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == '__main__':
    main()
