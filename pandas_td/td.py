import contextlib
import gzip
import io
import os
import time
import uuid
import zlib

import msgpack
import numpy as np
import pandas as pd
import requests
import tdclient

import logging
logger = logging.getLogger(__name__)

DEFAULT_ENDPOINT = 'https://api.treasuredata.com/'

class Connection(object):
    def __init__(self, apikey=None, endpoint=None, **kwargs):
        if apikey is None:
            apikey = os.environ['TD_API_KEY']
        if endpoint is None:
            endpoint = DEFAULT_ENDPOINT
        if not endpoint.endswith('/'):
            endpoint = endpoint + '/'
        self.apikey = apikey
        self.endpoint = endpoint
        self._kwargs = kwargs

    @property
    def client(self):
        if not hasattr(self, '_client'):
            self._client = tdclient.Client(self.apikey, self.endpoint, **self._kwargs)
        return self._client

    def databases(self):
        databases = self.client.databases()
        if databases:
            return pd.DataFrame(
                [[db.name, db.count, db.permission, db.created_at, db.updated_at] for db in databases],
                columns=['name', 'count', 'permission', 'created_at', 'updated_at'])
        else:
            return pd.DataFrame()

    def tables(self, database):
        tables = self.client.tables(database)
        if tables:
            return pd.DataFrame(
                [[t.name, t.count, t.estimated_storage_size, t.last_log_timestamp, t.created_at] for t in tables],
                columns=['name', 'count', 'estimated_storage_size', 'last_log_timestamp', 'created_at'])
        else:
            return pd.DataFrame()

    def query_engine(self, database, **kwargs):
        return QueryEngine(self, database, kwargs)

class QueryEngine(object):
    def __init__(self, connection, database, params):
        self.connection = connection
        self.database = database
        self._params = params

    def execute(self, query, **kwargs):
        # parameters
        params = self._params.copy()
        params.update(kwargs)

        # issue query
        job = self.connection.client.query(self.database, query, **params)
        job.wait()

        # status check
        if not job.success():
            stderr = job.debug['stderr']
            if stderr:
                logger.error(stderr)
            raise RuntimeError("job {0} {1}\n\nOutput:\n{2}".format(
                job.job_id,
                job.status(),
                job.debug['cmdout']))

        # result
        return ResultProxy(self, job)

class ResultProxy(object):
    def __init__(self, engine, job):
        self.engine = engine
        self.job = job
        self._iter = None

    @property
    def status(self):
        return self.job.status()

    @property
    def size(self):
        return self.job.result_size

    @property
    def description(self):
        return self.job.result_schema

    def iter_content(self, chunk_size):
        # start downloading
        headers = {
            'Authorization': 'TD1 {0}'.format(self.engine.connection.apikey),
            'Accept-Encoding': 'deflate, gzip',
        }
        r = requests.get('{endpoint}v3/job/result/{job_id}?format={format}'.format(
            endpoint = self.engine.connection.endpoint,
            job_id = self.job.job_id,
            format = 'msgpack.gz',
        ), headers=headers, stream=True)

        # content length
        maxval = None
        if 'Content-length' in r.headers:
            maxval = int(r.headers['Content-length'])

        # download
        with contextlib.closing(r) as r:
            d = zlib.decompressobj(16+zlib.MAX_WBITS)
            for chunk in r.iter_content(chunk_size):
                yield d.decompress(chunk)

    def read(self, size=16384):
        if self._iter is None:
            self._iter = self.iter_content(size)
        try:
            return next(self._iter)
        except StopIteration:
            return ''

    def __iter__(self):
        for record in msgpack.Unpacker(self, encoding='utf-8'):
            yield record

    def _parse_dates(self, frame, parse_dates):
        for name in parse_dates:
            if type(parse_dates) is list:
                frame[name] = pd.to_datetime(frame[name])
            else:
                if frame[name].dtype.kind == 'O':
                    frame[name] = pd.to_datetime(frame[name], format=parse_dates[name])
                else:
                    frame[name] = pd.to_datetime(frame[name], unit=parse_dates[name])
        return frame

    def _index_col(self, frame, index_col):
        frame.index = frame[index_col]
        frame.drop(index_col, axis=1, inplace=True)
        return frame

    def to_dataframe(self, index_col=None, parse_dates=None):
        columns = [c[0] for c in self.description]
        frame = pd.DataFrame(iter(self), columns=columns)
        if parse_dates is not None:
            frame = self._parse_dates(frame, parse_dates)
        if index_col is not None:
            frame = self._index_col(frame, index_col)
        return frame

class StreamingUploader(object):
    def __init__(self, client, database, table):
        self.client = client
        self.database = database
        self.table = table

    def current_time(self):
        return int(time.time())

    def normalize_dataframe(self, frame, time, index=False, index_label=False):
        frame = frame.copy()
        # time column
        if time != 'column' and 'time' in frame.columns:
            logger.warning('"time" column is overwritten.  Use time="column" to preserve "time" column')
        if time == 'column':
            if 'time' not in frame.columns:
                raise TypeError('"time" column is required when time="column"')
            if frame.time.dtype not in (np.dtype('int64'), np.dtype('datetime64[ns]')):
                raise TypeError('time type must be either int64 or datetime64')
            if frame.time.dtype == np.dtype('datetime64[ns]'):
                frame['time'] = frame.time.astype(np.int64, copy=True) // 10 ** 9
        elif time == 'now':
            frame['time'] = self.current_time()
        elif time == 'index':
            if frame.index.dtype != np.dtype('datetime64[ns]'):
                raise TypeError('index type must be datetime64[ns]')
            frame['time'] = frame.index.astype(np.int64) // 10 ** 9
            index = False
        else:
            raise ValueError('invalid value for time', time)
        # index column
        if index:
            if isinstance(frame.index, pd.MultiIndex):
                if index_label is None:
                    index_label = [v if v else "level_%d" % i for i, v in enumerate(frame.index.names)]
                for i, name in zip(frame.index.levels, index_label):
                    frame[name] = i.astype('object')
            else:
                if index_label is None:
                    index_label = frame.index.name if frame.index.name else 'index'
                frame[index_label] = frame.index.astype('object')
        return frame

    def _chunk_frame(self, frame, chunksize):
        for i in range((len(frame) - 1) // chunksize + 1):
            yield frame[i * chunksize : i * chunksize + chunksize]

    def _pack(self, chunk):
        packer = msgpack.Packer(autoreset=False)
        for _, row in chunk.iterrows():
            record = dict(row)
            packer.pack(record)
        return packer.bytes()

    def _gzip(self, data):
        buff = io.BytesIO()
        with gzip.GzipFile(fileobj=buff, mode='wb') as f:
            f.write(data)
        return buff.getvalue()

    def _upload(self, data):
        database = self.database
        table = self.table
        data_size = len(data)
        unique_id = uuid.uuid4()
        elapsed = self.client.import_data(database, table, 'msgpack.gz', data, data_size, unique_id)
        logger.debug('imported %d bytes in %.3f secs', data_size, elapsed)

    def upload_frame(self, frame, chunksize):
        for chunk in self._chunk_frame(frame, chunksize):
            self._upload(self._gzip(self._pack(chunk)))

# utility functions

def connect(apikey=None, endpoint=None, **kwargs):
    return Connection(apikey, endpoint, **kwargs)

def read_td_query(query, engine, index_col=None, params=None, parse_dates=None):
    '''Read Treasure Data query into a DataFrame.

    Returns a DataFrame corresponding to the result set of the query string.
    Optionally provide an index_col parameter to use one of the columns as
    the index, otherwise default integer index will be used.

    Parameters
    ----------
    query : string
        Query string to be executed.
    engine : QueryEngine
        A handler returned by Connection.query_engine.
    index_col : string, optional
        Column name to use as index for the returned DataFrame object.
    params : dict, optional
        Parameters to pass to execute method.
    parse_dates : list or dict
        - List of column names to parse as dates
        - Dict of {column_name: format string} where format string is strftime
          compatible in case of parsing string times or is one of (D, s, ns, ms, us)
          in case of parsing integer timestamps

    Returns
    -------
    DataFrame
    '''
    if params is None:
        params = {}
    r = engine.execute(query, **params)
    return r.to_dataframe(index_col=index_col, parse_dates=parse_dates)

# alias
read_td = read_td_query

def to_td(frame, name, con, if_exists='fail', time='now', index=True, index_label=None, chunksize=10000):
    '''Write a DataFrame to a Treasure Data table.

    This method converts the dataframe into a series of key-value pairs
    and send them using the Treasure Data streaming API. The data is divided
    into chunks of rows (default 10,000) and uploaded separately. If upload
    failed, the client retries the process for a certain amount of time
    (max_cumul_retry_delay; default 600 secs). This method may fail and
    raise an exception when retries did not success, in which case the data
    may be partially inserted. Use the bulk import utility if you cannot
    accept partial inserts.

    Parameters
    ----------
    frame : DataFrame
        DataFrame to be written.
    name : string
        Name of table to be written, in the form 'database.table'.
    con : Connection
        Connection to a Treasure Data account.
    if_exists: {'fail', 'replace', 'append'}, default 'fail'
        - fail: If table exists, do nothing.
        - replace: If table exists, drop it, recreate it, and insert data.
        - append: If table exists, insert data. Create if does not exist.
    time : {'now', 'column', 'index'}, default 'now'
        - now: Insert (or replace) a "time" column as the current time.
        - column: Use "time" column, must be an integer (unixtime) or datetime.
        - index: Convert DataFrame index into a "time" column, implies index=False.
    index : boolean, default True
        Write DataFrame index as a column.
    index_label : string or sequence, default None
        Column label for index column(s). If None is given (default) and index is True,
        then the index names are used. A sequence should be given if the DataFrame uses
        MultiIndex.
    chunksize : int, default 10,000
        Number of rows to be inserted in each chunk from the dataframe.
    '''
    database, table = name.split('.')

    # check existence
    if if_exists == 'fail':
        try:
            con.client.table(database, table)
        except tdclient.api.NotFoundError:
            con.client.create_log_table(database, table)
        else:
            raise RuntimeError('table "%s" already exists' % name)
    elif if_exists == 'replace':
        try:
            t = con.client.table(database, table)
        except tdclient.api.NotFoundError:
            pass
        else:
            t.delete()
        con.client.create_log_table(database, table)
    elif if_exists == 'append':
        try:
            con.client.table(database, table)
        except tdclient.api.NotFoundError:
            con.client.create_log_table(database, table)
    else:
        raise ValueError('invalid value for if_exists: %s' % if_exists)

    # upload
    uploader = StreamingUploader(con.client, database, table)
    frame = uploader.normalize_dataframe(frame, time, index, index_label)
    uploader.upload_frame(frame, chunksize)
