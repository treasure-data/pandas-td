import contextlib
import datetime
import gzip
import io
import os
import re
import sys
import time
import uuid
import zlib

import msgpack
import numpy as np
import pandas as pd
import requests
import six
import tdclient

try:
    import IPython
    import IPython.display
except ImportError:
    IPython = None

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
            self._client = tdclient.Client(apikey=self.apikey, endpoint=self.endpoint, **self._kwargs)
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
    def __init__(self, connection, database, params=None, show_progress=5):
        self.connection = connection
        self.database = database
        self._params = {} if params is None else params
        self.show_progress = False
        if IPython and not sys.stdout.isatty():
            # Enable progress for IPython notebook
            self.show_progress = show_progress

    def _html_text(self, text):
        return '<div style="color: #888;"># {0}</div>'.format(text)

    def _html_presto_output(self, output):
        html = ''
        # started at
        for text in re.findall(r'started at.*', output):
            html += self._html_text(text)
        # warning
        html += '<pre style="color: #c44;">'
        for text in re.findall(r'\n\*\* .*', output):
            html += '{0}'.format(text)
        html += '</pre>\n'
        # progress
        progress = None
        for progress in re.findall(r'\n(\d{4}-\d{2}-\d{2}.*(?:\n .*)+)', output):
            pass
        if progress:
            html += '<pre>{0}</pre>'.format(progress)
        # finished at
        for rows, finished in re.findall(r'\n(\d+ rows.*)\n(finished at.*)', output):
            html += '{0}<br>'.format(rows)
            html += self._html_text(finished)
        return html

    def _display_progress(self, job, cursize=None):
        if not self.show_progress:
            return
        if isinstance(self.show_progress, six.integer_types) and hasattr(job, 'issued_at'):
            if datetime.datetime.utcnow() < job.issued_at + datetime.timedelta(seconds=self.show_progress):
                return
        # header
        html = '<div style="border-style: dashed; border-width: 1px;">\n'
        # issued at
        if hasattr(job, 'issued_at'):
            html += self._html_text('issued at {0}Z'.format(job.issued_at.isoformat()))
        html += 'URL: <a href="{0}" target="_blank">{0}</a><br>\n'.format(job.url)
        # query output
        if job.type == 'presto':
            if job.debug and job.debug['cmdout']:
                html += self._html_presto_output(job.debug['cmdout'])
        if job.result_size:
            html += "Result size: {:,} bytes<br>\n".format(job.result_size)
        if cursize:
            html += "Download: {:,} / ".format(cursize)
            html += "{:,} bytes".format(job.result_size)
            html += " (%.2f%%)<br>\n" % (cursize * 100.0 / job.result_size)
            if cursize >= job.result_size:
                now = datetime.datetime.utcnow().replace(microsecond=0)
                html += self._html_text('downloaded at {0}Z'.format(now.isoformat()))
        # footer
        html += '</div>\n'
        # display
        IPython.display.clear_output(True)
        IPython.display.display(IPython.display.HTML(html))

    def wait(self, job):
        try:
            while not job.finished():
                time.sleep(2)
                job.update()
                self._display_progress(job)
            job.update()
            self._display_progress(job)
        except KeyboardInterrupt:
            job.kill()
            raise

    def execute(self, query, **kwargs):
        # parameters
        params = self._params.copy()
        params.update(kwargs)

        # issue query
        issued_at = datetime.datetime.utcnow().replace(microsecond=0)
        job = self.connection.client.query(self.database, query, **params)
        job.issued_at = issued_at
        self.wait(job)

        # status check
        if not job.success():
            if job.debug and job.debug['stderr'] and job.debug['cmdout']:
                logger.error("%s\nOutput:\n%s", job.debug['stderr'], job.debug['cmdout'])
            raise RuntimeError("job {0} {1}".format(job.job_id, job.status()))

        # result
        return ResultProxy(self, job)

    def _http_get(self, uri, **kwargs):
        return requests.get(uri, **kwargs)

    def _start_download(self, job):
        # start downloading
        headers = {
            'Authorization': 'TD1 {0}'.format(self.connection.apikey),
            'Accept-Encoding': 'deflate, gzip',
        }
        r = self._http_get('{endpoint}v3/job/result/{job_id}?format={format}'.format(
            endpoint = self.connection.endpoint,
            job_id = job.job_id,
            format = 'msgpack.gz',
        ), headers=headers, stream=True)
        return r

    def iter_content(self, job, chunk_size):
        curval = 0
        d = zlib.decompressobj(16+zlib.MAX_WBITS)
        with contextlib.closing(self._start_download(job)) as r:
            for chunk in r.iter_content(chunk_size):
                curval += len(chunk)
                self._display_progress(job, curval)
                yield d.decompress(chunk)

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

    def read(self, size=16384):
        if self._iter is None:
            self._iter = self.engine.iter_content(self.job, size)
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

    def _chunk_frame(self, frame, chunksize):
        for i in range((len(frame) - 1) // chunksize + 1):
            yield frame[i * chunksize : i * chunksize + chunksize]

    def _pack(self, chunk):
        packer = msgpack.Packer(autoreset=False)
        for _, row in chunk.iterrows():
            row.dropna(inplace=True)
            packer.pack(dict(row))
        return packer.bytes()

    def _gzip(self, data):
        buff = io.BytesIO()
        with gzip.GzipFile(fileobj=buff, mode='wb') as f:
            f.write(data)
        return buff.getvalue()

    def _upload(self, data):
        data_size = len(data)
        unique_id = uuid.uuid4()
        elapsed = self.client.import_data(self.database, self.table, 'msgpack.gz', data, data_size, unique_id)
        logger.debug('imported %d bytes in %.3f secs', data_size, elapsed)

    def upload_frame(self, frame, chunksize):
        for chunk in self._chunk_frame(frame, chunksize):
            self._upload(self._gzip(self._pack(chunk)))

# public methods

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
        Handler returned by Connection.query_engine.
    index_col : string, optional
        Column name to use as index for the returned DataFrame object.
    params : dict, optional
        Parameters to pass to execute method.
    parse_dates : list or dict, optional
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

def read_td_table(table_name, engine, index_col=None, parse_dates=None, columns=None, time_range=None, sample=None, limit=10000):
    '''Read Treasure Data table into a DataFrame.

    The number of returned rows is limited by "limit" (default 10,000).
    Setting limit=None means all rows. Be careful when you set limit=None
    because your table might be very large and the result does not fit into memory.

    Parameters
    ----------
    table_name : string
        Name of Treasure Data table in database.
    engine : QueryEngine
        Handler returned by Connection.query_engine.
    index_col : string, optional
        Column name to use as index for the returned DataFrame object.
    parse_dates : list or dict, optional
        - List of column names to parse as dates
        - Dict of {column_name: format string} where format string is strftime
          compatible in case of parsing string times or is one of (D, s, ns, ms, us)
          in case of parsing integer timestamps
    columns : list, optional
        List of column names to select from table.
    time_range : tuple (start, end), optional
        Limit time range to select. "start" and "end" are one of None, integers,
        strings or datetime objects. "end" is exclusive, not included in the result.
    sample : double, optional
        Enable sampling data (Presto only). 1.0 means all data (100 percent).
        See TABLESAMPLE BERNOULLI at https://prestodb.io/docs/current/sql/select.html
    limit : int, default 10,000
        Maximum number of rows to select.

    Returns
    -------
    DataFrame
    '''
    query = """-- read_td_table('{table_name}')
SELECT {columns}
FROM {table_name}
""".format(columns = '*' if columns is None else ', '.join(columns),
           table_name = table_name)
    if sample is not None:
        if sample < 0 or sample > 1:
            raise ValueError('sample must be between 0.0 and 1.0')
        query += "TABLESAMPLE BERNOULLI ({0})\n".format(sample * 100)
    if time_range is not None:
        start, end = time_range
        query += "WHERE td_time_range(time, {0}, {1})\n".format(_convert_time(start), _convert_time(end))
    if limit is not None:
        query += "LIMIT {0}\n".format(limit)
    # execute
    r = engine.execute(query)
    return r.to_dataframe(index_col=index_col, parse_dates=parse_dates)

def _convert_time(time):
    if time is None:
        return "NULL"
    elif isinstance(time, six.integer_types):
        t = pd.to_datetime(time, unit='s')
    elif isinstance(time, six.string_types):
        t = pd.to_datetime(time)
    elif isinstance(time, (datetime.date, datetime.datetime)):
        t = pd.to_datetime(time)
    else:
        raise ValueError('invalid time value: {0}'.format(time))
    return "'{0}'".format(t.replace(microsecond=0))

# alias
read_td = read_td_query

def to_td(frame, name, con, if_exists='fail', time_col=None, time_index=None, index=True, index_label=None, chunksize=10000, date_format=None):
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
    time_col : string, optional
        Column name to use as "time" column for the table. Column type must be
        integer (unixtime), datetime, or string. If None is given (default),
        then the current time is used as time values.
    time_index : int, optional
        Level of index to use as "time" column for the table. Set 0 for a single index.
        This parameter implies index=False.
    index : boolean, default True
        Write DataFrame index as a column.
    index_label : string or sequence, default None
        Column label for index column(s). If None is given (default) and index is True,
        then the index names are used. A sequence should be given if the DataFrame uses
        MultiIndex.
    chunksize : int, default 10,000
        Number of rows to be inserted in each chunk from the dataframe.
    date_format : string, default None
        Format string for datetime objects
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
            con.client.table(database, table)
        except tdclient.api.NotFoundError:
            pass
        else:
            con.client.delete_table(database, table)
        con.client.create_log_table(database, table)
    elif if_exists == 'append':
        try:
            con.client.table(database, table)
        except tdclient.api.NotFoundError:
            con.client.create_log_table(database, table)
    else:
        raise ValueError('invalid value for if_exists: %s' % if_exists)

    # "time_index" implies "index=False"
    if time_index:
        index = None

    # convert
    frame = frame.copy()
    frame = _convert_time_column(frame, time_col, time_index)
    frame = _convert_index_column(frame, index, index_label)
    frame = _convert_date_format(frame, date_format)

    # upload
    uploader = StreamingUploader(con.client, database, table)
    uploader.upload_frame(frame, chunksize)

def _convert_time_column(frame, time_col=None, time_index=None):
    if time_col is not None and time_index is not None:
        raise ValueError('time_col and time_index cannot be used at the same time')
    if 'time' in frame.columns and time_col != 'time':
        raise ValueError('"time" column already exists')
    if time_col is not None:
        if time_col != 'time':
            frame.rename(columns={time_col: 'time'}, inplace=True)
        col = frame['time']
        if col.dtype != np.int64:
            if col.dtype != np.dtype('datetime64[ns]'):
                col = pd.to_datetime(col)
            frame['time'] = col.astype(np.int64, copy=True) // (10 ** 9)
    elif time_index is not None:
        if type(time_index) is bool or not isinstance(time_index, six.integer_types):
            raise TypeError('invalid type for time_index')
        if isinstance(frame.index, pd.MultiIndex):
            idx = frame.index.levels[time_index]
        else:
            if time_index == 0:
                idx = frame.index
            else:
                raise IndexError('list index out of range')
        if idx.dtype != np.dtype('datetime64[ns]'):
            raise TypeError('index type must be datetime64[ns]')
        frame['time'] = idx.astype(np.int64) // (10 ** 9)
    else:
        frame['time'] = int(time.time())
    return frame

def _convert_index_column(frame, index=None, index_label=None):
    if index is not None and not isinstance(index, bool):
        raise TypeError('index must be boolean')
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

def _convert_date_format(frame, date_format=None):
    if date_format is not None:
        def _convert(col):
            if col.dtype == np.dtype('datetime64[ns]'):
                return col.apply(lambda x: x.strftime(date_format))
            return col
        frame = frame.apply(_convert)
    return frame
