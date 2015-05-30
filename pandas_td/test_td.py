from .td import DEFAULT_ENDPOINT
from .td import Connection
from .td import QueryEngine
from .td import ResultProxy
from .td import StreamingUploader
from .td import connect
from .td import read_td_query
from .td import read_td_table
from .td import to_td

import collections
import datetime
import gzip
import io
import os
import msgpack
import tdclient
import numpy as np
import pandas as pd

from unittest import TestCase
try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock
from nose.tools import ok_, eq_, raises

# mocks

class MockJob(object):
    def __init__(self, status='success'):
        self._status = status
        self.result = [{'c1': i, 'c2': '2001-01-01', 'time': i} for i in range(100)]
        self.result_bytes = self._pack_gz(self.result)
        self.job_id = 1
        self.result_size = len(self.result_bytes)
        self.result_schema = [['c1', 'int'], ['c2', 'string'], ['time', 'int']]
        self.debug = {
            'cmdout': 'output',
            'stderr': 'error',
        }

    def _pack_gz(self, result):
        packer = msgpack.Packer(autoreset=False)
        for row in result:
            packer.pack(row)
        buff = io.BytesIO()
        with gzip.GzipFile(fileobj=buff, mode='wb') as f:
            f.write(packer.bytes())
        return buff.getvalue()

    def wait(self):
        pass

    def status(self):
        return self._status

    def success(self):
        return self._status == 'success'

class MockRequest(object):
    def __init__(self, job):
        self.job = job
        self.headers = {
            'Content-length': str(self.job.result_size)
        }

    def iter_content(self, chunksize):
        yield self.job.result_bytes

    def close(self):
        pass

# test cases

class ConnectionConfigurationTestCase(TestCase):
    def setUp(self):
        self._environ = os.environ.copy()
        # clear environment variables
        if 'TD_API_KEY' in os.environ:
            del os.environ['TD_API_KEY']

    def tearDown(self):
        # restore environment variables
        os.environ.clear()
        os.environ.update(self._environ)

    @raises(KeyError)
    def test_error_without_parameters(self):
        Connection()

    def test_apikey(self):
        # parameter
        c1 = Connection(apikey='test-key')
        eq_(c1.apikey, 'test-key')
        # environment variable
        os.environ['TD_API_KEY'] = 'test-key'
        c2 = Connection(apikey='test-key')
        eq_(c2.apikey, 'test-key')

    def test_endpoint(self):
        os.environ['TD_API_KEY'] = 'test-key'
        # default
        c1 = Connection()
        eq_(c1.endpoint, DEFAULT_ENDPOINT)
        # parameter
        c2 = Connection(endpoint='http://api/')
        eq_(c2.endpoint, 'http://api/')
        # no trailing slash
        c3 = Connection(endpoint='http://api')
        eq_(c3.endpoint, 'http://api/')

class ConnectionTestCase(TestCase):
    def setUp(self):
        self.connection = Connection('test-key', 'test-endpoint')

    def test_databases(self):
        TestDatabase = collections.namedtuple('TestDatabase',
                                              ['name', 'count', 'permission', 'created_at', 'updated_at'])
        client = self.connection.client
        client.databases = MagicMock(return_value=[
            TestDatabase(
                name = 'test_db',
                count = 0,
                permission = 'administrator',
                created_at = datetime.datetime(2015, 1, 1, 0, 0, 0),
                updated_at = datetime.datetime(2015, 1, 1, 0, 0, 0),
            )
        ])
        d = self.connection.databases()
        eq_(len(d), 1)
        eq_(d.name[0], 'test_db')

    def test_tables(self):
        TestTable = collections.namedtuple('TestTable',
                                           ['name', 'count', 'estimated_storage_size', 'created_at', 'last_log_timestamp'])
        client = self.connection.client
        client.tables = MagicMock(return_value=[
            TestTable(
                name = 'test_tbl',
                count = 0,
                estimated_storage_size = 0,
                created_at = datetime.datetime(2015, 1, 1, 0, 0, 0),
                last_log_timestamp = datetime.datetime(2015, 1, 1, 0, 0, 0),
            )
        ])
        d = self.connection.tables('test_db')
        eq_(len(d), 1)
        eq_(d.name[0], 'test_tbl')

class QueryEngineTestCase(TestCase):
    def setUp(self):
        self.connection = Connection('test-key', 'test-endpoint')

    def test_execute_ok(self):
        # mock
        job = MockJob('success')
        self.connection.client.query = MagicMock(return_value=job)
        # test
        engine = QueryEngine(self.connection, 'test_db')
        r = engine.execute('select 1')
        self.connection.client.query.assert_called_with('test_db', 'select 1')
        ok_(isinstance(r, ResultProxy))
        eq_(r.engine, engine)
        eq_(r.job, job)

    @raises(RuntimeError)
    def test_execute_error(self):
        # mock
        job = MockJob('error')
        self.connection.client.query = MagicMock(return_value=job)
        # test
        engine = QueryEngine(self.connection, 'test_db')
        r = engine.execute('select 1')

class ResultProxyTestCase(TestCase):
    def setUp(self):
        self.connection = Connection('test-key', 'test-endpoint')
        self.engine = QueryEngine(self.connection, 'test_db')
        self.job = MockJob()
        self.result = ResultProxy(self.engine, self.job)
        self.engine._http_get = MagicMock(return_value=MockRequest(self.job))

    def test_ok(self):
        r = self.result
        # attributes
        eq_(r.status, self.job.status())
        eq_(r.size, self.job.result_size)
        eq_(r.description, self.job.result_schema)
        # result
        rows = list(r)
        eq_(len(rows), 100)
        eq_(rows[0], self.job.result[0])

    def test_to_dataframe(self):
        r = self.result
        d = r.to_dataframe()
        eq_(len(d), 100)
        eq_(list(d.columns), ['c1', 'c2', 'time'])
        eq_(list(d.c1), list(range(100)))

    def test_to_dataframe_index_col(self):
        r = self.result
        d = r.to_dataframe(index_col='c1')
        eq_(d.index.name, 'c1')
        eq_(list(d.index.values), list(range(100)))
        eq_(list(d.columns), ['c2', 'time'])

    def test_to_dataframe_parse_dates(self):
        r = self.result
        d = r.to_dataframe(parse_dates=['c2'])
        eq_(d.c2.dtype, np.dtype('datetime64[ns]'))

    def test_to_dataframe_time_series(self):
        r = self.result
        d = r.to_dataframe(index_col='time', parse_dates={'time': 's'})
        eq_(d.index.dtype, np.dtype('datetime64[ns]'))

class StreamingUploaderTestCase(TestCase):
    def setUp(self):
        self.uploader = StreamingUploader(None, 'test_db', 'test_tbl')

    def test_normalize_time_now(self):
        frame = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'])
        # time='now'
        f2 = self.uploader.normalize_dataframe(frame, 'now')
        eq_(list(f2.columns), ['x', 'y', 'time'])

    def test_normalize_time_column(self):
        frame = pd.DataFrame([[0, 'a', 1], [0, 'b', 2]], columns=['time', 'x', 'y'])
        # time='column'
        f1 = self.uploader.normalize_dataframe(frame, 'column')
        eq_(list(f1.columns), ['time', 'x', 'y'])

    def test_normalize_time_index(self):
        date_range = pd.date_range('2015-01-01', periods=2, freq='d')
        frame = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'], index=date_range)
        # time='index'
        f1 = self.uploader.normalize_dataframe(frame, 'index')
        eq_(list(f1.columns), ['x', 'y', 'time'])

    @raises(ValueError)
    def test_raise_invalid_time(self):
        frame = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'])
        self.uploader.normalize_dataframe(frame, 'invalid')

    def test_chunk_frame(self):
        frame = pd.DataFrame([[1], [2], [3], [4]])
        chunks = [chunk for chunk in self.uploader._chunk_frame(frame, 2)]
        eq_(len(chunks), 2)

    def test_pack(self):
        records = [{'x': 'a', 'y': 1}, {'x': 'b', 'y': 2}]
        data = self.uploader._pack(pd.DataFrame(records))
        for unpacked in msgpack.Unpacker(io.BytesIO(data), encoding='utf-8'):
            eq_(unpacked, records[0])
            records = records[1:]
        eq_(records, [])

    def test_gzip(self):
        data = self.uploader._gzip(b'abc')
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
            eq_(f.read(), b'abc')

class ReadTdTableQueryCase(TestCase):
    def setUp(self):
        job = MockJob()
        self.connection = connect('test-key', 'test-endpoint')
        self.connection.client.query = MagicMock(return_value=job)
        self.engine = self.connection.query_engine('test_db', type='presto')
        self.engine._http_get = MagicMock(return_value=MockRequest(job))

    def test_ok(self):
        read_td_query('select 1', self.engine)
        self.connection.client.query.assert_called_with('test_db', 'select 1', type='presto')

class ReadTdTableTestCase(TestCase):
    def setUp(self):
        job = MockJob()
        self.connection = connect('test-key', 'test-endpoint')
        self.connection.client.query = MagicMock(return_value=job)
        self.engine = self.connection.query_engine('test_db', type='presto')
        self.engine._http_get = MagicMock(return_value=MockRequest(job))

    def assert_query(self, query):
        self.connection.client.query.assert_called_with('test_db', "-- read_td_table('test_table')" + query, type='presto')

    @raises(ValueError)
    def test_invalid_sample_small(self):
        read_td_table('test_table', self.engine, sample=-1)

    @raises(ValueError)
    def test_invalid_sample_large(self):
        read_td_table('test_table', self.engine, sample=1.1)

    def test_default(self):
        read_td_table('test_table', self.engine)
        self.assert_query('''
SELECT *
FROM test_table
LIMIT 10000
''')

    def test_with_columns(self):
        read_td_table('test_table', self.engine, columns=['c1', 'c2'])
        self.assert_query('''
SELECT c1, c2
FROM test_table
LIMIT 10000
''')

    def test_with_sample(self):
        read_td_table('test_table', self.engine, sample=0.1)
        self.assert_query('''
SELECT *
FROM test_table
TABLESAMPLE BERNOULLI (10.0)
LIMIT 10000
''')

    def test_without_limit(self):
        read_td_table('test_table', self.engine, limit=None)
        self.assert_query('''
SELECT *
FROM test_table
''')

class ToTdTestCase(TestCase):
    def setUp(self):
        self.connection = connect('test-key', 'test-endpoint')
        self.frame = pd.DataFrame([[1,2],[3,4]], columns=['x', 'y'])

    @raises(ValueError)
    def test_invalid_table_name(self):
        to_td(self.frame, 'invalid', self.connection)

    @raises(ValueError)
    def test_invalid_if_exists(self):
        to_td(self.frame, 'test_db.test_table', self.connection, if_exists='invalid')

    @raises(RuntimeError)
    def test_table_already_exists(self):
        client = self.connection.client
        client.table = MagicMock()
        to_td(self.frame, 'test_db.test_table', self.connection)

    def test_ok(self):
        # mock
        client = self.connection.client
        client.table = MagicMock(side_effect=tdclient.api.NotFoundError('test_table'))
        client.create_log_table = MagicMock()
        client.import_data = MagicMock()
        # test
        to_td(self.frame, 'test_db.test_table', self.connection)
        client.table.assert_called_with('test_db', 'test_table')
        client.create_log_table.assert_called_with('test_db', 'test_table')

    def test_replace(self):
        # mock
        client = self.connection.client
        client.table = MagicMock(side_effect=tdclient.api.NotFoundError('test_table'))
        client.create_log_table = MagicMock()
        client.import_data = MagicMock()
        # first call
        to_td(self.frame, 'test_db.test_table', self.connection, if_exists='replace')
        client.create_log_table.assert_called_with('test_db', 'test_table')
        # mock
        client.table = MagicMock()
        client.delete_table = MagicMock()
        # second call
        to_td(self.frame, 'test_db.test_table', self.connection, if_exists='replace')
        client.delete_table.assert_called_with('test_db', 'test_table')
        client.create_log_table.assert_called_with('test_db', 'test_table')

    def test_append(self):
        # mock
        client = self.connection.client
        client.table = MagicMock(side_effect=tdclient.api.NotFoundError('test_table'))
        client.create_log_table = MagicMock()
        client.import_data = MagicMock()
        # first call
        to_td(self.frame, 'test_db.test_table', self.connection, if_exists='append')
        # second call
        client.table = MagicMock()
        to_td(self.frame, 'test_db.test_table', self.connection, if_exists='append')
        client.create_log_table.assert_called_once_with('test_db', 'test_table')
