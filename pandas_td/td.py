import contextlib
import datetime
import gzip
import io
import re
import sys
import time
import uuid
import warnings
import zlib

import msgpack
import pandas as pd
import requests
import six
import tdclient
import tdclient.version

from pandas_td.version import __version__

try:
    # Python 3.x
    from urllib.parse import urlparse
    from urllib.parse import parse_qsl
except ImportError:
    # Python 2.x
    from urlparse import urlparse
    from urlparse import parse_qsl

try:
    import IPython
    import IPython.display
except ImportError:
    IPython = None

import logging
logger = logging.getLogger(__name__)

class Connection(object):
    def __init__(self, apikey=None, endpoint=None, **kwargs):
        if apikey is not None:
            kwargs['apikey'] = apikey
        if endpoint is not None:
            if not endpoint.endswith('/'):
                endpoint = endpoint + '/'
            kwargs['endpoint'] = endpoint
        if 'user_agent' not in kwargs:
            versions = [
                "pandas/{0}".format(pd.__version__),
                "tdclient/{0}".format(tdclient.version.__version__),
                "Python/{0}.{1}.{2}.{3}.{4}".format(*list(sys.version_info)),
            ]
            kwargs['user_agent'] = "pandas-td/{0} ({1})".format(__version__, ' '.join(versions))
        self.kwargs = kwargs
        self.client = self.get_client()

    def get_client(self):
        return tdclient.Client(**self.kwargs)

    @property
    def apikey(self):
        return self.client.api.apikey

    @property
    def endpoint(self):
        return self.client.api.endpoint

    def databases(self):
        warnings.warn("'databases' is deprecated. Use magic function instead.", DeprecationWarning)
        databases = self.client.databases()
        if databases:
            return pd.DataFrame(
                [[db.name, db.count, db.permission, db.created_at, db.updated_at] for db in databases],
                columns=['name', 'count', 'permission', 'created_at', 'updated_at'])
        else:
            return pd.DataFrame()

    def tables(self, database):
        warnings.warn("'tables' is deprecated. Use magic function instead.", DeprecationWarning)
        tables = self.client.tables(database)
        if tables:
            return pd.DataFrame(
                [[t.name, t.count, t.estimated_storage_size, t.last_log_timestamp, t.created_at] for t in tables],
                columns=['name', 'count', 'estimated_storage_size', 'last_log_timestamp', 'created_at'])
        else:
            return pd.DataFrame()

    def query_engine(self, database, **kwargs):
        warnings.warn("'query_engine' is deprecated. Use 'create_engine' instead.", DeprecationWarning)
        return QueryEngine(self, database, kwargs, header=True, show_progress=5)

class QueryEngine(object):
    def __init__(self, connection, database, params=None, header=False, show_progress=False, clear_progress=False):
        self.connection = connection
        self.database = database
        self._params = {} if params is None else params
        self._header = header
        if IPython and not sys.stdout.isatty():
            # Enable progress for IPython notebook
            self.show_progress = show_progress
            self.clear_progress = clear_progress
        else:
            self.show_progress = False
            self.clear_progress = False

    @property
    def type(self):
        return self._params.get('type')

    def create_header(self, name):
        # name
        if self._header is False:
            header = ''
        elif isinstance(self._header, six.string_types):
            header = "-- {0}\n".format(self._header)
        else:
            header = "-- {0}\n".format(name)
        return header

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
        for progress in re.findall(r'\n(\d{4}-\d{2}-\d{2}.*\n\d{8}.*(?:\n *\[\d+\].*)+)', output):
            pass
        if progress:
            html += '<code><small><small>{0}</small></small></code>'.format(progress)
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

    def wait_callback(self, job):
        job.update()
        self._display_progress(job)

    def get_result(self, job, wait=True):
        # wait for completion
        if wait:
            job.wait(wait_interval=2, wait_callback=self.wait_callback)

        # status check
        if not job.success():
            if job.debug and job.debug['stderr']:
                logger.error(job.debug['stderr'])
            raise RuntimeError("job {0} {1}".format(job.job_id, job.status()))

        # result
        return ResultProxy(self, job)

    def execute(self, query, **kwargs):
        # parameters
        params = self._params.copy()
        params.update(kwargs)

        # issue query
        issued_at = datetime.datetime.utcnow().replace(microsecond=0)
        job = self.connection.client.query(self.database, query, **params)
        job.issued_at = issued_at

        try:
            return self.get_result(job, wait=True)
        except KeyboardInterrupt:
            job.kill()
            raise

    def _http_get(self, uri, **kwargs):
        return requests.get(uri, **kwargs)

    def _start_download(self, job):
        # start downloading
        headers = {
            'Authorization': 'TD1 {0}'.format(self.connection.apikey),
            'Accept-Encoding': 'deflate, gzip',
            'User-Agent': "pandas-td/{0} ({1})".format(__version__, requests.utils.default_user_agent()),
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
        if self.clear_progress:
            IPython.display.clear_output()

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
        if not self.job.finished():
            self.job.wait()
        return self.job.result_size

    @property
    def description(self):
        if not self.job.finished():
            self.job.wait()
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

    def to_dataframe(self, index_col=None, parse_dates=None):
        columns = [c[0] for c in self.description]
        frame = pd.DataFrame(iter(self), columns=columns)
        if parse_dates is not None:
            frame = self._parse_dates(frame, parse_dates)
        if index_col is not None:
            frame.set_index(index_col, inplace=True)
        return frame

class StreamingUploader(object):
    def __init__(self, client, database, table, show_progress=False, clear_progress=False):
        self.client = client
        self.database = database
        self.table = table
        self.start_at = datetime.datetime.utcnow().replace(microsecond=0)
        self.messages = []
        self.uploading_at = None
        self.uploaded_at = None
        self.initial_count = None
        self.imported_count = None
        self.import_timeout = None
        self.imported_at = None
        self.frame_size = None
        if IPython and not sys.stdout.isatty():
            # Enable progress for IPython notebook
            self.show_progress = show_progress
            self.clear_progress = clear_progress
        else:
            self.show_progress = False
            self.clear_progress = False

    def _html_text(self, text):
        return '<div style="color: #888;"># {0}</div>'.format(text)

    def _display_progress(self, cursize=None):
        if not self.show_progress:
            return
        # header
        html = '<div style="border-style: dashed; border-width: 1px;">\n'
        # start
        if self.start_at:
            html += self._html_text('session started at {0}Z'.format(self.start_at.isoformat()))
            for msg in self.messages:
                html += msg + "<br>\n"
        # uploading
        if self.uploading_at:
            html += self._html_text('upload started at {0}Z'.format(self.uploading_at.isoformat()))
        if cursize is not None:
            html += "Upload: {:,} / ".format(cursize)
            html += "{:,} records".format(self.frame_size)
            html += " (%.2f%%)<br>\n" % (cursize * 100.0 / self.frame_size)
        if self.uploaded_at is not None:
            html += self._html_text('upload finished at {0}Z'.format(self.uploaded_at.isoformat()))
        if self.imported_count is not None:
            html += "waiting until the data become visible...<br>\n"
            html += "Imported: {:,} / ".format(self.imported_count)
            html += "{:,} records".format(self.frame_size)
            html += " (%.2f%%)<br>\n" % (self.imported_count * 100.0 / self.frame_size)
            if self.imported_count > self.frame_size:
                html += '<pre style="color: #c44;">'
                html += '* Imported more records than uploaded.  This usually means\n'
                html += '* other sessions imported some records into the same table.\n'
                html += '* In this case, to_td() cannot detect when your import finished.\n'
                html += '</pre>\n'
        if self.import_timeout is not None:
            STATUSPAGES = {
                'treasuredata.com': 'http://status.treasuredata.com/',
                'idcfcloud.com': 'http://ybi-status.idcfcloud.com/',
            }
            statuspage_url = None
            for domain, url in STATUSPAGES.items():
                if domain in self.client.api.endpoint:
                    statuspage_url = url
            html += '<pre style="color: #c44;">'
            html += '* Upload finished, but the data is not visible after waiting {0} seconds.\n'.format(self.import_timeout)
            html += '* This does not mean import failed, but it is taking longer than usual.\n'
            if statuspage_url:
                html += '* Check <a href="{0}" target="_blank">{0}</a> for import delays.\n'.format(statuspage_url)
            html += '</pre>\n'
        if self.imported_at is not None:
            html += self._html_text('import finished at {0}Z'.format(self.imported_at.isoformat()))
        # footer
        html += '</div>\n'
        # display
        IPython.display.clear_output(True)
        IPython.display.display(IPython.display.HTML(html))

    def message(self, msg):
        self.messages.append(msg)
        self._display_progress()

    def _chunk_frame(self, frame, chunksize):
        for i in range((len(frame) - 1) // chunksize + 1):
            yield frame[i * chunksize : i * chunksize + chunksize]

    def _pack(self, chunk):
        packer = msgpack.Packer(autoreset=False)
        for _, row in chunk.iterrows():
            # row.dtype can be non-object (such as numpy.int64 or numpy.float64)
            # when column types are homogeneous.  In this case, packer.pack raises
            # an exception because it doesn't know how to encode those data types.
            if row.dtype.name != 'object':
                row = row.astype('object')
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
        t = self.client.table(self.database, self.table)
        self.initial_count = t.count
        self.uploading_at = datetime.datetime.utcnow().replace(microsecond=0)
        self.frame_size = len(frame)
        self._display_progress(0)
        for i, chunk in enumerate(self._chunk_frame(frame, chunksize)):
            self._upload(self._gzip(self._pack(chunk)))
            self._display_progress(min(i * chunksize + chunksize, self.frame_size))
        self.uploaded_at = datetime.datetime.utcnow().replace(microsecond=0)
        self.imported_count = 0
        self._display_progress(self.frame_size)

    def wait_for_import(self, count, timeout=300):
        time_start = time.time()
        while time.time() < time_start + timeout:
            t = self.client.table(self.database, self.table)
            self.imported_count = t.count - self.initial_count
            if self.imported_count >= count:
                break
            self._display_progress(self.frame_size)
            time.sleep(2)
        # import finished
        self.imported_at = datetime.datetime.utcnow().replace(microsecond=0)
        # import timeout
        if self.imported_count < count:
            self.import_timeout = timeout
        self._display_progress(self.frame_size)
        # clear progress
        if self.clear_progress and self.imported_count == count:
            IPython.display.clear_output()

# public methods

def connect(apikey=None, endpoint=None, **kwargs):
    return Connection(apikey, endpoint, **kwargs)

def create_engine(url, con=None, header=True, show_progress=5.0, clear_progress=True):
    '''Create a handler for query engine based on a URL.

    The following environment variables are used for default connection:

      TD_API_KEY     API key
      TD_API_SERVER  API server (default: api.treasuredata.com)
      HTTP_PROXY     HTTP proxy (optional)

    Parameters
    ----------
    url : string
        Engine descriptor in the form "type://apikey@host/database?params..."
        Use shorthand notation "type:database?params..." for the default connection.
    con : Connection, optional
        Handler returned by connect. If not given, default connection is used.
    header : string or boolean, default True
        Prepend comment strings, in the form "-- comment", as a header of queries.
        Set False to disable header.
    show_progress : double or boolean, default 5.0
        Number of seconds to wait before printing progress.
        Set False to disable progress entirely.
    clear_progress : boolean, default True
        If True, clear progress when query completed.

    Returns
    -------
    QueryEngine
    '''
    url = urlparse(url)
    engine_type = url.scheme if url.scheme else 'presto'
    if con is None:
        if url.netloc:
            # create connection
            apikey, host = url.netloc.split('@')
            con = Connection(apikey=apikey, endpoint="https://{0}/".format(host))
        else:
            # default connection
            con = Connection()
    database = url.path[1:] if url.path.startswith('/') else url.path
    params = {
        'type': engine_type,
    }
    params.update(parse_qsl(url.query))
    return QueryEngine(con, database, params,
                       header=header,
                       show_progress=show_progress,
                       clear_progress=clear_progress)

def read_td_query(query, engine, index_col=None, parse_dates=None, distributed_join=False, params=None):
    '''Read Treasure Data query into a DataFrame.

    Returns a DataFrame corresponding to the result set of the query string.
    Optionally provide an index_col parameter to use one of the columns as
    the index, otherwise default integer index will be used.

    Parameters
    ----------
    query : string
        Query string to be executed.
    engine : QueryEngine
        Handler returned by create_engine.
    index_col : string, optional
        Column name to use as index for the returned DataFrame object.
    parse_dates : list or dict, optional
        - List of column names to parse as dates
        - Dict of {column_name: format string} where format string is strftime
          compatible in case of parsing string times or is one of (D, s, ns, ms, us)
          in case of parsing integer timestamps
    distributed_join : boolean, default False
        (Presto only) If True, distributed join is enabled. If False, broadcast join is used.
        See https://prestodb.io/docs/current/release/release-0.77.html
    params : dict, optional
        Parameters to pass to execute method.
        Available parameters:
        - result_url (str): result output URL
        - priority (int or str): priority (e.g. "NORMAL", "HIGH", etc.)
        - retry_limit (int): retry limit

    Returns
    -------
    DataFrame
    '''
    if params is None:
        params = {}
    # header
    header = engine.create_header("read_td_query")
    if engine.type == 'presto' and distributed_join is not None:
        header += "-- set session distributed_join = '{0}'\n".format('true' if distributed_join else 'false')
    # execute
    r = engine.execute(header + query, **params)
    return r.to_dataframe(index_col=index_col, parse_dates=parse_dates)

def read_td_job(job_id, engine, index_col=None, parse_dates=None):
    '''Read Treasure Data job result into a DataFrame.

    Returns a DataFrame corresponding to the result set of the job.
    This method waits for job completion if the specified job is still running.
    Optionally provide an index_col parameter to use one of the columns as
    the index, otherwise default integer index will be used.

    Parameters
    ----------
    job_id : integer
        Job ID.
    engine : QueryEngine
        Handler returned by create_engine.
    index_col : string, optional
        Column name to use as index for the returned DataFrame object.
    parse_dates : list or dict, optional
        - List of column names to parse as dates
        - Dict of {column_name: format string} where format string is strftime
          compatible in case of parsing string times or is one of (D, s, ns, ms, us)
          in case of parsing integer timestamps

    Returns
    -------
    DataFrame
    '''
    # get job
    job = engine.connection.client.job(job_id)
    # result
    r = engine.get_result(job, wait=True)
    return r.to_dataframe(index_col=index_col, parse_dates=parse_dates)

def read_td_table(table_name, engine, index_col=None, parse_dates=None, columns=None, time_range=None, limit=10000):
    '''Read Treasure Data table into a DataFrame.

    The number of returned rows is limited by "limit" (default 10,000).
    Setting limit=None means all rows. Be careful when you set limit=None
    because your table might be very large and the result does not fit into memory.

    Parameters
    ----------
    table_name : string
        Name of Treasure Data table in database.
    engine : QueryEngine
        Handler returned by create_engine.
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
    limit : int, default 10,000
        Maximum number of rows to select.

    Returns
    -------
    DataFrame
    '''
    # header
    query = engine.create_header("read_td_table('{0}')".format(table_name))
    # SELECT
    query += "SELECT {0}\n".format('*' if columns is None else ', '.join(columns))
    # FROM
    query += "FROM {0}\n".format(table_name)
    # WHERE
    if time_range is not None:
        start, end = time_range
        query += "WHERE td_time_range(time, {0}, {1})\n".format(_convert_time(start), _convert_time(end))
    # LIMIT
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
    uploader = StreamingUploader(con.client, database, table, show_progress=True, clear_progress=True)
    uploader.message('Streaming import into: {0}.{1}'.format(database, table))

    # check existence
    if if_exists == 'fail':
        try:
            con.client.table(database, table)
        except tdclient.api.NotFoundError:
            uploader.message('creating new table...')
            con.client.create_log_table(database, table)
        else:
            raise RuntimeError('table "%s" already exists' % name)
    elif if_exists == 'replace':
        try:
            con.client.table(database, table)
        except tdclient.api.NotFoundError:
            pass
        else:
            uploader.message('deleting old table...')
            con.client.delete_table(database, table)
        uploader.message('creating new table...')
        con.client.create_log_table(database, table)
    elif if_exists == 'append':
        try:
            con.client.table(database, table)
        except tdclient.api.NotFoundError:
            uploader.message('creating new table...')
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
    uploader.upload_frame(frame, chunksize)
    uploader.wait_for_import(len(frame))

def _convert_time_column(frame, time_col=None, time_index=None):
    if time_col is not None and time_index is not None:
        raise ValueError('time_col and time_index cannot be used at the same time')
    if 'time' in frame.columns and time_col != 'time':
        raise ValueError('"time" column already exists')
    if time_col is not None:
        # Use 'time_col' as time column
        if time_col != 'time':
            frame.rename(columns={time_col: 'time'}, inplace=True)
        col = frame['time']
        # convert python string to pandas datetime
        if col.dtype.name == 'object' and len(col) > 0 and isinstance(col[0], six.string_types):
            col = pd.to_datetime(col)
        # convert pandas datetime to unixtime
        if col.dtype.name == 'datetime64[ns]':
            frame['time'] = col.astype('int64') // (10 ** 9)
    elif time_index is not None:
        # Use 'time_index' as time column
        if type(time_index) is bool or not isinstance(time_index, six.integer_types):
            raise TypeError('invalid type for time_index')
        if isinstance(frame.index, pd.MultiIndex):
            idx = frame.index.levels[time_index]
        else:
            if time_index == 0:
                idx = frame.index
            else:
                raise IndexError('list index out of range')
        if idx.dtype.name != 'datetime64[ns]':
            raise TypeError('index type must be datetime64[ns]')
        # convert pandas datetime to unixtime
        frame['time'] = idx.astype('int64') // (10 ** 9)
    else:
        # Use current time as time column
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
            if col.dtype.name == 'datetime64[ns]':
                return col.apply(lambda x: x.strftime(date_format))
            return col
        frame = frame.apply(_convert)
    return frame
