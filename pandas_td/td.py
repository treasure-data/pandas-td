import gzip
import os
import StringIO
import time
import uuid
import msgpack
import numpy as np
import pandas as pd
import tdclient

import logging
logger = logging.getLogger(__name__)

DEFAULT_ENDPOINT = 'https://api.treasuredata.com/'

class Connection(object):
    def __init__(self, apikey=None, endpoint=None, database=None):
        if apikey is None:
            apikey = os.environ['TD_API_KEY']
        if endpoint is None:
            endpoint = DEFAULT_ENDPOINT
        self.client = tdclient.Client(apikey, endpoint)
        self.database = database

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

def connect(apikey=None, endpoint=None, database=None):
    return Connection(apikey, endpoint, database)

def read_td(query, connection):
    return read_td_query(query, connection)

def read_td_query(query, connection):
    job = connection.client.query(connection.database, query, type='presto')
    while not job.finished():
        time.sleep(2)
        job._update_status()
    return pd.DataFrame(job.result(), columns=[c[0] for c in job._hive_result_schema])

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
        for idx, row in frame.iterrows():
            record = dict(row)
            records.append(record)
        yield records

    def pack_gz(self, records):
        buff = StringIO.StringIO()
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

def to_td(frame, table, connection, time='now'):
    '''
    time = 'now' | 'column' | 'index'
    '''
    uploader = StreamingUploader(connection.client, connection.database, table)
    frame = uploader.normalize_frame(frame, time=time)
    for records in uploader.chunk_frame(frame):
        uploader.upload(uploader.pack_gz(records))
