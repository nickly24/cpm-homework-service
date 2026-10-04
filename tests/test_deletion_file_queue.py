"""The existing file queue acknowledges deletion only after HEAD confirms absence."""
from contextlib import contextmanager
import unittest
from unittest.mock import MagicMock, patch

from homework_service import jobs


class DeletionFileQueueTests(unittest.TestCase):
    def setUp(self):
        self.cursor=MagicMock()
        self.cursor.fetchone.return_value=None
        self.storage=MagicMock()
        self.app=MagicMock()
        self.app.extensions={'homework_storage':self.storage}
        self.row={'id':7,'object_key':'synthetic/one.pdf','attempts':0}
        @contextmanager
        def read():
            yield self.cursor
        @contextmanager
        def transaction():
            yield MagicMock(),self.cursor
        for item in [patch('homework_service.db.read_cursor',read),patch('homework_service.db.transaction',transaction)]:
            item.start();self.addCleanup(item.stop)

    def statements(self):
        return [call.args[0] for call in self.cursor.execute.call_args_list]

    def test_success_requires_delete_then_absence_before_acknowledgment(self):
        events=[]
        self.storage.delete.side_effect=lambda key:events.append('delete')
        self.storage.verify_absent.side_effect=lambda key:events.append('head404')
        self.cursor.execute.side_effect=lambda sql,*args:events.append('ack') if sql.startswith('DELETE') else None
        jobs._delete_object(self.app,self.row)
        self.assertEqual(events,['delete','head404','ack'])

    def test_head_failure_keeps_retry_record_instead_of_acknowledging(self):
        self.storage.verify_absent.side_effect=RuntimeError('synthetic forbidden')
        jobs._delete_object(self.app,self.row)
        self.assertFalse(any(sql.startswith('DELETE') for sql in self.statements()))
        updates=[call for call in self.cursor.execute.call_args_list if call.args[0].startswith('UPDATE')]
        self.assertEqual(updates[0].args[1][0],'retry')

    def test_key_referenced_by_live_file_is_never_deleted(self):
        self.cursor.fetchone.return_value={'exists':1}
        jobs._delete_object(self.app,self.row)
        self.storage.delete.assert_not_called()
        self.assertFalse(any(sql.startswith('DELETE') for sql in self.statements()))

    def test_terminal_failure_is_visible_in_existing_queue(self):
        self.storage.delete.side_effect=RuntimeError('synthetic unavailable')
        jobs._delete_object(self.app,dict(self.row,attempts=9))
        updates=[call for call in self.cursor.execute.call_args_list if call.args[0].startswith('UPDATE')]
        self.assertEqual(updates[0].args[1][0],'failed')
