"""Aggregate-only homework ownership checks; no data repair or destructive SQL.

The same SQL contract is deployed independently in backend and homework service.
A student scope includes incoming references and conflicts on that student's keys.
Files with no submission cannot be safely attributed, so they block every scope.
Independent object-ownership receipts may outlive their submission/upload job;
when those parents still exist, their ownership must agree.
"""

REQUIRED_COLUMNS = {
    'students': {'id'},
    'homework': {'id'},
    'homework_submissions': {'id', 'student_id', 'homework_id', 'current_file_id', 'draft_file_id'},
    'homework_submission_files': {'id', 'submission_id', 'object_key'},
    'homework_file_jobs': {'id', 'student_id', 'homework_id', 'submission_id', 'result_file_id',
                           'staging_key', 'processed_output_key'},
    'homework_object_ownership': {'object_key', 'student_id', 'submission_id', 'upload_job_id'},
    'homework_student_purges': {'job_id', 'student_id'},
    'homework_student_purge_objects': {'job_id', 'object_key'},
}

# UNION ALL preserves contradictions. A key belongs to the submission owner,
# never to the student inferred from a path or a shared homework definition.
# CAST AS BINARY gives opaque keys byte identity across legacy unicode_ci, new
# 0900_ai_ci and ascii_bin columns, without changing any stored collation.
KEY_OWNERS_SQL = (
    'SELECT CAST(f.object_key AS BINARY) object_key,s.student_id,f.submission_id FROM homework_submission_files f '
    'LEFT JOIN homework_submissions s ON s.id=f.submission_id '
    'UNION ALL SELECT CAST(staging_key AS BINARY),student_id,submission_id FROM homework_file_jobs WHERE staging_key IS NOT NULL '
    'UNION ALL SELECT CAST(processed_output_key AS BINARY),student_id,submission_id FROM homework_file_jobs WHERE processed_output_key IS NOT NULL '
    'UNION ALL SELECT CAST(object_key AS BINARY),student_id,submission_id FROM homework_object_ownership '
    'UNION ALL SELECT CAST(m.object_key AS BINARY),p.student_id,NULL FROM homework_student_purge_objects m '
    'LEFT JOIN homework_student_purges p ON CAST(p.job_id AS BINARY)=CAST(m.job_id AS BINARY)'
)


def _columns(cursor, columns):
    if columns is None:
        cursor.execute('SELECT TABLE_NAME,COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE()')
        columns = {}
        for row in cursor.fetchall():
            columns.setdefault(row['TABLE_NAME'], []).append(row)
    return {table: {col if isinstance(col, str) else col['COLUMN_NAME'] for col in cols}
            for table, cols in columns.items()}


def aggregate_file_consistency(cursor, student_id=None, columns=None):
    """Return only stable issue-code counts, or raise a sanitized schema error.

    This is read-only. Call under the shared writer fence for destructive flows;
    a preview is advisory and must be revalidated after acquiring that fence.
    Missing audited pointer columns are a schema error, never optional checks.
    """
    scope = 'global' if student_id is None else 'student'
    available = _columns(cursor, columns)
    missing = sum(len(required - available.get(table, set())) for table, required in REQUIRED_COLUMNS.items())
    if missing:
        raise FileConsistencyError('homework_file_schema_incomplete', {'missing_required_columns': missing}, scope)
    counts = {}

    def count(code, table_sql, invalid, owners=(), global_scope=False):
        condition = '(' + invalid + ')'
        params = ()
        if student_id is not None and not global_scope:
            condition += ' AND (' + ' OR '.join(owner + '=%s' for owner in owners) + ')'
            params = (student_id,) * len(owners)
        cursor.execute('SELECT COUNT(*) count FROM ' + table_sql + ' WHERE ' + condition, params)
        row = cursor.fetchone()
        if row is None or row.get('count') is None:
            raise FileConsistencyError('homework_file_check_incomplete', {}, scope)
        counts[code] = int(row['count'])

    count('orphan_file_submission',
          'homework_submission_files f LEFT JOIN homework_submissions s ON s.id=f.submission_id',
          's.id IS NULL', global_scope=True)
    count('orphan_purge_manifest',
          'homework_student_purge_objects m LEFT JOIN homework_student_purges p '
          'ON CAST(p.job_id AS BINARY)=CAST(m.job_id AS BINARY)',
          'p.job_id IS NULL', global_scope=True)
    count('empty_manifest_key',
          'homework_student_purge_objects m JOIN homework_student_purges p '
          'ON CAST(p.job_id AS BINARY)=CAST(m.job_id AS BINARY)',
          "m.object_key IS NULL OR TRIM(m.object_key)=''", ('p.student_id',))
    count('orphan_submission_student',
          'homework_submissions s LEFT JOIN students st ON st.id=s.student_id',
          'st.id IS NULL', ('s.student_id',))
    count('orphan_submission_homework',
          'homework_submissions s LEFT JOIN homework h ON h.id=s.homework_id',
          'h.id IS NULL', ('s.student_id',))
    for field in ('current_file_id', 'draft_file_id'):
        count('submission_' + field + '_mismatch',
              'homework_submissions s LEFT JOIN homework_submission_files f ON f.id=s.' + field +
              ' LEFT JOIN homework_submissions owner ON owner.id=f.submission_id',
              's.' + field + ' IS NOT NULL AND (f.id IS NULL OR f.submission_id<>s.id OR f.submission_id IS NULL)',
              ('s.student_id', 'owner.student_id'))
    count('job_submission_owner_mismatch',
          'homework_file_jobs j LEFT JOIN homework_submissions s ON s.id=j.submission_id',
          's.id IS NULL OR j.student_id IS NULL OR j.homework_id IS NULL '
          'OR j.student_id<>s.student_id OR j.homework_id<>s.homework_id',
          ('j.student_id', 's.student_id'))
    count('job_result_file_mismatch',
          'homework_file_jobs j LEFT JOIN homework_submission_files f ON f.id=j.result_file_id '
          'LEFT JOIN homework_submissions s ON s.id=j.submission_id '
          'LEFT JOIN homework_submissions owner ON owner.id=f.submission_id',
          'j.result_file_id IS NOT NULL AND (f.id IS NULL OR f.submission_id IS NULL '
          'OR f.submission_id<>j.submission_id)',
          ('j.student_id', 's.student_id', 'owner.student_id'))
    count('ownership_submission_mismatch',
          'homework_object_ownership o JOIN homework_submissions s ON s.id=o.submission_id',
          'o.student_id IS NULL OR o.student_id<>s.student_id', ('o.student_id', 's.student_id'))
    count('ownership_job_mismatch',
          'homework_object_ownership o JOIN homework_file_jobs j ON CAST(j.id AS BINARY)=CAST(o.upload_job_id AS BINARY) '
          'LEFT JOIN homework_submissions s ON s.id=j.submission_id',
          'o.student_id IS NULL OR o.student_id<>j.student_id '
          'OR (o.submission_id IS NOT NULL AND o.submission_id<>j.submission_id)',
          ('o.student_id', 'j.student_id', 's.student_id'))
    count('empty_file_key',
          'homework_submission_files f LEFT JOIN homework_submissions s ON s.id=f.submission_id',
          "f.object_key IS NULL OR TRIM(f.object_key)=''", ('s.student_id',))
    count('empty_ownership_key', 'homework_object_ownership o',
          "o.object_key IS NULL OR TRIM(o.object_key)=''", ('o.student_id',))
    count('empty_job_key', 'homework_file_jobs j LEFT JOIN homework_submissions s ON s.id=j.submission_id',
          "(j.staging_key IS NOT NULL AND TRIM(j.staging_key)='') OR "
          "(j.processed_output_key IS NOT NULL AND TRIM(j.processed_output_key)='')",
          ('j.student_id', 's.student_id'))

    # No exact key may identify two owners. NULL owners from orphan files are
    # covered above instead of disappearing through COUNT(DISTINCT ...).
    for code, owner in (('shared_object_ownership', 'student_id'),
                        ('shared_object_submission', 'submission_id')):
        having = 'COUNT(DISTINCT ' + owner + ')>1'
        params = ()
        if student_id is not None:
            having += ' AND SUM(CASE WHEN student_id=%s THEN 1 ELSE 0 END)>0'
            params = (student_id,)
        cursor.execute('SELECT COUNT(*) count FROM (SELECT object_key FROM (' + KEY_OWNERS_SQL +
                       ') key_owners WHERE object_key IS NOT NULL GROUP BY object_key HAVING ' + having + ') conflicts', params)
        row = cursor.fetchone()
        if row is None or row.get('count') is None:
            raise FileConsistencyError('homework_file_check_incomplete', {}, scope)
        counts[code] = int(row['count'])
    return {'scope': scope, 'counts': counts}


def check_file_consistency(cursor, student_id=None, columns=None):
    """Validate all ownership links; raise with aggregate counts before deletion."""
    result = aggregate_file_consistency(cursor, student_id, columns)
    if any(result['counts'].values()):
        raise FileConsistencyError('homework_file_ownership_inconsistent', result['counts'], result['scope'])
    return result


class FileConsistencyError(RuntimeError):
    def __init__(self, code, counts, scope):
        super().__init__(code)
        self.code, self.counts, self.scope = code, counts, scope
