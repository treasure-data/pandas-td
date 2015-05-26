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
    def __init__(self, apikey=None, endpoint=None, database=None, type='presto'):
        if apikey is None:
            apikey = os.environ['TD_API_KEY']
        if endpoint is None:
            endpoint = DEFAULT_ENDPOINT
        self.apikey = apikey
        self.endpoint = endpoint
        self.client = tdclient.Client(apikey, endpoint)
        self.database = database
        self.type = type

    def databases(self):
        databases = self.client.databases()
        if databases:
            return pd.DataFrame(
                [[db.name, db.count, db.permission, db.created_at, db.updated_at] for db in databases],
                columns=['name', 'count', 'permission', 'created_at', 'updated_at'],
            )
        else:
            return pd.DataFrame()

    def tables(self, database=None):
        if database is None:
            database = self.database
        tables = self.client.tables(database)
        if tables:
            return pd.DataFrame(
                [[t.name, t.count, t.estimated_storage_size, t.last_log_timestamp, t.created_at] for t in tables],
                columns=['name', 'count', 'estimated_storage_size', 'last_log_timestamp', 'created_at'],
            )
        else:
            return pd.DataFrame()

    def query(self, query, **kwargs):
        # parameters
        if 'type' not in kwargs and self.type:
            kwargs['type'] = self.type

        # issue query
        job = self.client.query(self.database, query, **kwargs)

        # wait
        while not job.finished():
            time.sleep(1)
            job._update_status()

        # status check
        if not job.success():
            stderr = job._debug['stderr']
            if stderr:
                logger.error(stderr)
            raise RuntimeError("job {0} {1}\n\nOutput:\n{2}".format(
                job.job_id,
                job.status(),
                job._debug['cmdout']))

        # result as DataFrame
        return ResultProxy(self, job.job_id)

class ResultProxy(object):
    def __init__(self, connection, job_id):
        self.connection = connection
        self.job = connection.client.job(job_id)
        self._iter = None

    @property
    def status(self):
        return self.job.status()

    @property
    def size(self):
        return self.job._result_size

    @property
    def description(self):
        return self.job._hive_result_schema

    def iter_content(self, chunk_size):
        # start downloading
        headers = {
            'Authorization': 'TD1 {0}'.format(self.connection.apikey),
            'Accept-Encoding': 'deflate, gzip',
        }
        r = requests.get('{endpoint}v3/job/result/{job_id}?format={format}'.format(
            endpoint = self.connection.endpoint,
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

    def to_dataframe(self):
        columns = [c[0] for c in self.description]
        return pd.DataFrame(iter(self), columns=columns)

class StreamingUploader(object):
    def __init__(self, client, database, table):
        self.client = client
        self.database = database
        self.table = table

    def current_time(self):
        return int(time.time())

    def normalize_frame(self, frame, time='now'):
        if time != 'column' and 'time' in frame.columns:
            logger.warning('The values of "time" column are overwritten.  Use time="column" to preserve "time" column')
        if time == 'column':
            if 'time' not in frame.columns:
                raise TypeError('"time" column is required when time="column"')
            if frame.time.dtype not in (np.dtype('int64'), np.dtype('datetime64[ns]')):
                raise TypeError('time type must be either int64 or datetime64')
            if frame.time.dtype == np.dtype('datetime64[ns]'):
                frame = frame.copy()
                frame['time'] = frame.time.astype(np.int64, copy=True) // 10 ** 9
        elif time == 'now':
            frame = frame.copy()
            frame['time'] = self.current_time()
        elif time == 'index':
            if frame.index.dtype != np.dtype('datetime64[ns]'):
                raise TypeError('index type must be datetime64[ns]')
            frame = frame.copy()
            frame['time'] = frame.index.astype(np.int64) // 10 ** 9
        else:
            raise ValueError('invalid value for time', time)
        return frame

    def chunk_frame(self, frame):
        records = []
        for _, row in frame.iterrows():
            record = dict(row)
            records.append(record)
        yield records

    def pack_gz(self, records):
        buff = io.BytesIO()
        with gzip.GzipFile(fileobj=buff, mode='wb') as f:
            for record in records:
                f.write(msgpack.packb(record))
        return buff.getvalue()

    def upload(self, data):
        database = self.database
        table = self.table
        data_size = len(data)
        unique_id = uuid.uuid4()
        elapsed = self.client.import_data(database, table, 'msgpack.gz', data, data_size, unique_id)
        logger.debug('imported %d bytes in %.3f secs', data_size, elapsed)

# utility functions

def connect(apikey=None, endpoint=None, database=None, **kwargs):
    return Connection(apikey, endpoint, database, **kwargs)

def read_td(query, connection, **kwargs):
    return read_td_query(query, connection, **kwargs)

def read_td_query(query, connection, **kwargs):
    r = connection.query(query, **kwargs)
    return r.to_dataframe()

def to_td(frame, table, connection, time='now'):
    '''
    time = 'now' | 'column' | 'index'
    '''
    uploader = StreamingUploader(connection.client, connection.database, table)
    frame = uploader.normalize_frame(frame, time=time)
    for records in uploader.chunk_frame(frame):
        uploader.upload(uploader.pack_gz(records))
