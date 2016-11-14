from .td import Connection
from .td import QueryEngine
from .td import ResultProxy
from .td import StreamingUploader
from .td import _convert_time_column
from .td import _convert_index_column
from .td import _convert_date_format

from pandas_td import connect
from pandas_td import read_td
from pandas_td import read_td_query
from pandas_td import read_td_job
from pandas_td import read_td_table
from pandas_td import to_td

import collections
import datetime
import gzip
import io
import os
import time
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

class MockTable(object):
    def __init__(self):
        self.count = 0

class MockJob(object):
    def __init__(self, status='success'):
        self._status = status
        self.result = [{'c1': i, 'c2': '2001-01-01', 'time': i} for i in range(100)]
        self.result_bytes = self._pack_gz(self.result)
        self.job_id = 1
        self.type = 'presto'
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

    def url(self):
        return 'https://mock/jobs/1'

    def wait(self, **kwargs):
        pass

    def status(self):
        return self._status

    def finished(self):
        return True

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

    @raises(ValueError)
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
        eq_(c1.endpoint, 'https://api.treasuredata.com/')
        # parameter
        c2 = Connection(endpoint='http://api/')
        eq_(c2.endpoint, 'http://api/')
        # no trailing slash
        c3 = Connection(endpoint='http://api')
        eq_(c3.endpoint, 'http://api/')

class ConnectionTestCase(TestCase):
    def setUp(self):
        self.connection = Connection('test-key', 'test-endpoint')

    def test_empty_databases(self):
        client = self.connection.client
        client.databases = MagicMock(return_value=[])
        d = self.connection.databases()
        eq_(len(d), 0)

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

    def test_empty_tables(self):
        client = self.connection.client
        client.tables = MagicMock(return_value=[])
        d = self.connection.tables('test_db')
        eq_(len(d), 0)

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

    def test_pack_int_array(self):
        records = [{'time': 0, 'x': 0, 'y': 0}, {'time': 1, 'x': 1, 'y': 1}]
        data = self.uploader._pack(pd.DataFrame(records))
        for unpacked in msgpack.Unpacker(io.BytesIO(data), encoding='utf-8'):
            eq_(unpacked, records[0])
            records = records[1:]
        eq_(records, [])

    def test_drop_nan(self):
        records = [{'x': 'a', 'y': np.nan}, {'x': np.nan, 'y': 1.0}]
        data = self.uploader._pack(pd.DataFrame(records))
        unpacker = msgpack.Unpacker(io.BytesIO(data), encoding='utf-8')
        eq_(unpacker.unpack(), {'x': 'a'})
        eq_(unpacker.unpack(), {'y': 1.0})

    def test_gzip(self):
        data = self.uploader._gzip(b'abc')
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
            eq_(f.read(), b'abc')

class ReadTdQueryTestCase(TestCase):
    def setUp(self):
        job = MockJob()
        self.connection = connect('test-key', 'test-endpoint')
        self.connection.client.query = MagicMock(return_value=job)
        self.engine = self.connection.query_engine('test_db', type='presto')
        self.engine._http_get = MagicMock(return_value=MockRequest(job))

    def assert_query(self, query):
        self.connection.client.query.assert_called_with('test_db', '''-- read_td_query
-- set session distributed_join = 'false'
''' + query, type='presto')

    def test_ok(self):
        read_td_query('select 1', self.engine)
        self.assert_query('select 1')

class ReadTdJobTestCase(TestCase):
    def setUp(self):
        self.job = MockJob()
        self.connection = connect('test-key', 'test-endpoint')
        self.connection.client.job = MagicMock(return_value=self.job)
        self.engine = self.connection.query_engine('test_db', type='presto')
        self.engine._http_get = MagicMock(return_value=MockRequest(self.job))

    def test_ok(self):
        df = read_td_job(1, self.engine)
        eq_(len(df), len(self.job.result))

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
    def test_invalid_time_range(self):
        read_td_table('test_table', self.engine, time_range=(1.0, 2.0))

    def test_default(self):
        read_td_table('test_table', self.engine)
        self.assert_query('''
SELECT *
FROM test_table
LIMIT 10000
''')

    def test_time_range(self):
        time_range_tests = [
            [(None, None), "NULL", "NULL"],
            [(0, 1000000000), "'1970-01-01 00:00:00'", "'2001-09-09 01:46:40'"],
            [('2000-01-01', '2010-01-01'), "'2000-01-01 00:00:00'", "'2010-01-01 00:00:00'"],
            [(datetime.date(2000, 1, 1), datetime.datetime(2010, 1, 1, 0, 0, 0)),
             "'2000-01-01 00:00:00'", "'2010-01-01 00:00:00'"],
        ]
        for time_range, start, end in time_range_tests:
            read_td_table('test_table', self.engine, time_range=time_range)
        self.assert_query('''
SELECT *
FROM test_table
WHERE td_time_range(time, {0}, {1})
LIMIT 10000
'''.format(start, end))

    def test_with_columns(self):
        read_td_table('test_table', self.engine, columns=['c1', 'c2'])
        self.assert_query('''
SELECT c1, c2
FROM test_table
LIMIT 10000
''')

    def test_without_limit(self):
        read_td_table('test_table', self.engine, limit=None)
        self.assert_query('''
SELECT *
FROM test_table
''')

class ToTdTestCase(TestCase):
    def mock_client(self):
        mock_table = MockTable()
        client = MagicMock()
        client.table = MagicMock(side_effect=tdclient.api.NotFoundError('test_table'))
        client.delete_table = MagicMock()
        def create_log_table(database, table):
            client.table = MagicMock(return_value=mock_table)
        client.create_log_table = MagicMock(side_effect=create_log_table)
        def import_data(*args):
            # FIXME: This assumes importing 2 records at once
            mock_table.count += 2
            return 0.1
        client.import_data = MagicMock(side_effect=import_data)
        return client

    def setUp(self):
        self.connection = connect('test-key', 'test-endpoint')
        self.connection.client = self.mock_client()
        self.frame = pd.DataFrame([[1,2],[3,4]], columns=['x', 'y'])

    @raises(ValueError)
    def test_invalid_table_name(self):
        to_td(self.frame, 'invalid', self.connection)

    @raises(TypeError)
    def test_datetime_is_not_supported(self):
        client = self.connection.client
        # test
        frame = pd.DataFrame({'timestamp': [datetime.datetime(2000,1,1)]})
        to_td(frame, 'test_db.test_table', self.connection)

    # if_exists

    @raises(ValueError)
    def test_invalid_if_exists(self):
        to_td(self.frame, 'test_db.test_table', self.connection, if_exists='invalid')

    @raises(RuntimeError)
    def test_fail_if_exists(self):
        client = self.connection.client
        client.table = MagicMock()
        to_td(self.frame, 'test_db.test_table', self.connection)

    def test_ok_if_not_exists(self):
        client = self.connection.client
        to_td(self.frame, 'test_db.test_table', self.connection)
        client.table.assert_called_with('test_db', 'test_table')
        client.create_log_table.assert_called_with('test_db', 'test_table')

    def test_replace_if_exists(self):
        client = self.connection.client
        # first call
        to_td(self.frame, 'test_db.test_table', self.connection, if_exists='replace')
        client.create_log_table.assert_called_with('test_db', 'test_table')
        # second call
        to_td(self.frame, 'test_db.test_table', self.connection, if_exists='replace')
        client.delete_table.assert_called_with('test_db', 'test_table')
        client.create_log_table.assert_called_with('test_db', 'test_table')

    def test_append_if_exists(self):
        client = self.connection.client
        # first call
        to_td(self.frame, 'test_db.test_table', self.connection, if_exists='append')
        # second call
        to_td(self.frame, 'test_db.test_table', self.connection, if_exists='append')
        client.create_log_table.assert_called_once_with('test_db', 'test_table')

    # time_col

    @raises(ValueError)
    def test_error_time_col_and_time_index(self):
        _convert_time_column(self.frame, time_col='x', time_index=0)

    @raises(ValueError)
    def test_error_time_column_already_exists(self):
        f1 = pd.DataFrame([[0, 'a', 1], [0, 'b', 2]], columns=['time', 'x', 'y'])
        f2 = _convert_time_column(f1)

    def test_time_now(self):
        now = int(time.time())
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'])
        f2 = _convert_time_column(f1)
        eq_(list(f2.columns), ['x', 'y', 'time'])
        time_val = f2.ix[0, 'time']
        ok_((now - 1 < time_val) and (time_val < now + 1))

    def test_time_col_rename(self):
        f1 = pd.DataFrame([[978307200, 'a', 1], [978307200, 'b', 2]], columns=['unixtime', 'x', 'y'])
        f2 = _convert_time_column(f1, time_col='unixtime')
        eq_(list(f2.columns), ['time', 'x', 'y'])
        eq_(list(f2['time'].values), [978307200, 978307200])

    def test_time_col_by_unixtime(self):
        f1 = pd.DataFrame([[978307200, 'a', 1], [978307200, 'b', 2]], columns=['time', 'x', 'y'])
        f2 = _convert_time_column(f1, time_col='time')
        eq_(list(f2.columns), ['time', 'x', 'y'])
        eq_(list(f2['time'].values), [978307200, 978307200])

    def test_time_col_by_unixtime_substituted(self):
        # issue #3
        f1 = pd.DataFrame(index=range(2), columns=['time', 'x', 'y'])
        for i in range(len(f1)):
            f1['time'][i] = 978307200
            f1['x'][i] = 'a'
            f1['y'][i] = i
        f2 = _convert_time_column(f1, time_col='time')
        eq_(list(f2.columns), ['time', 'x', 'y'])
        eq_(list(f2['time'].values), [978307200, 978307200])

    def test_time_col_by_datetime(self):
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'])
        f1['time'] = pd.to_datetime('2001-01-01')
        f2 = _convert_time_column(f1, time_col='time')
        eq_(list(f2.columns), ['x', 'y', 'time'])
        eq_(list(f2['time'].values), [978307200, 978307200])

    def test_time_col_by_string(self):
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'])
        f1['time'] = '2001-01-01'
        f2 = _convert_time_column(f1, time_col='time')
        eq_(list(f2.columns), ['x', 'y', 'time'])
        eq_(list(f2['time'].values), [978307200, 978307200])

    # time_index

    @raises(TypeError)
    def test_invalid_arg_time_index(self):
        date_range = pd.date_range('2015-01-01', periods=2, freq='d')
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'], index=date_range)
        f2 = _convert_time_column(f1, time_index=True)

    @raises(IndexError)
    def test_invalid_level_time_index(self):
        date_range = pd.date_range('2015-01-01', periods=2, freq='d')
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'], index=date_range)
        f2 = _convert_time_column(f1, time_index=1)

    @raises(TypeError)
    def test_invalid_value_time_index(self):
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'])
        f2 = _convert_time_column(f1, time_index=0)

    def test_time_index(self):
        date_range = pd.date_range('2015-01-01', periods=2, freq='d')
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'], index=date_range)
        f2 = _convert_time_column(f1, time_index=0)
        eq_(list(f2.columns), ['x', 'y', 'time'])

    @raises(IndexError)
    def test_invalid_level_time_index_multi(self):
        date_range = pd.date_range('2015-01-01', periods=2, freq='d')
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'], index=[[0, 1], date_range])
        f2 = _convert_time_column(f1, time_index=2)

    @raises(TypeError)
    def test_invalid_value_time_index_multi(self):
        date_range = pd.date_range('2015-01-01', periods=2, freq='d')
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'], index=[[0, 1], date_range])
        f2 = _convert_time_column(f1, time_index=0)

    def test_time_index_multi(self):
        date_range = pd.date_range('2015-01-01', periods=2, freq='d')
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'], index=[[0, 1], date_range])
        f2 = _convert_time_column(f1, time_index=1)
        eq_(list(f2.columns), ['x', 'y', 'time'])

    # index / index_label

    @raises(TypeError)
    def test_invalid_index_type(self):
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'])
        f2 = _convert_index_column(f1, index=0)

    def test_no_index(self):
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'])
        f2 = _convert_index_column(f1, index=False)
        eq_(list(f2.columns), ['x', 'y'])

    def test_index(self):
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'])
        f2 = _convert_index_column(f1, index=True)
        eq_(list(f2.columns), ['x', 'y', 'index'])

    def test_index_name(self):
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'])
        f1.index.name = 'id'
        f2 = _convert_index_column(f1, index=True)
        eq_(list(f2.columns), ['x', 'y', 'id'])

    def test_index_label(self):
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'])
        f2 = _convert_index_column(f1, index=True, index_label='id')
        eq_(list(f2.columns), ['x', 'y', 'id'])

    def test_multi_index(self):
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'], index=[[0, 1], [0, 1]])
        f2 = _convert_index_column(f1, index=True)
        eq_(list(f2.columns), ['x', 'y', 'level_0', 'level_1'])

    def test_multi_index_name(self):
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'], index=[[0, 1], [0, 1]])
        f1.index.names = ['id1', 'id2']
        f2 = _convert_index_column(f1, index=True)
        eq_(list(f2.columns), ['x', 'y', 'id1', 'id2'])

    def test_multi_index_label(self):
        f1 = pd.DataFrame([['a', 1], ['b', 2]], columns=['x', 'y'], index=[[0, 1], [0, 1]])
        f2 = _convert_index_column(f1, index=True, index_label=['id1', 'id2'])
        eq_(list(f2.columns), ['x', 'y', 'id1', 'id2'])

    # date_format

    def test_date_format(self):
        ts = datetime.datetime(2000, 1, 2, 3, 4, 5)
        f1 = pd.DataFrame([['a', ts], ['b', ts]], columns=['x', 'y'])
        f2 = _convert_date_format(f1, date_format='%Y-%m-%d %T')
        eq_(f2['y'].tolist(), ['2000-01-02 03:04:05', '2000-01-02 03:04:05'])
