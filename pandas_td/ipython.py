# ipython.py

import re
import sys
import pandas as pd
import pandas_td as td
import tdclient

MAGIC_CONTEXT_NAME = '_td_magic'

class MagicContext(object):
    def __init__(self):
        self.database = None

    def connect(self):
        return td.connect()

class MagicTable(object):
    def __init__(self, table):
        print("INFO: import {0}".format(table.name))
        self.table = table
        data = [c if len(c) == 3 else (c[0], c[1], '') for c in table.schema]
        self.columns = [c[2] if c[2] else c[0] for c in data]
        self.frame = pd.DataFrame(data, columns=['field', 'type', 'alias'])

    def __dir__(self):
        return self.columns

    def _repr_html_(self):
        return self.frame._repr_html_()

def get_td_magic_context():
    ipython = get_ipython()
    try:
        ctx = ipython.ev(MAGIC_CONTEXT_NAME)
    except NameError:
        ctx = MagicContext()
        ipython.push({MAGIC_CONTEXT_NAME: ctx})
    return ctx

def magic_databases(pattern):
    ctx = get_td_magic_context()
    con = ctx.connect()
    columns = ['name', 'count', 'permission', 'created_at', 'updated_at']
    values = [[getattr(db, c) for c in columns]
              for db in con.client.databases()
              if re.search(pattern, db.name)]
    return pd.DataFrame(values, columns=columns)

def magic_tables(pattern):
    ctx = get_td_magic_context()
    con = ctx.connect()
    columns = ['db_name', 'name', 'count', 'estimated_storage_size', 'last_log_timestamp', 'created_at']
    values = [[getattr(t, c) for c in columns]
              for db in con.client.databases()
              for t in con.client.tables(db.name)
              if re.search(pattern, t.identifier)]
    return pd.DataFrame(values, columns=columns)

def magic_jobs(pattern):
    ctx = get_td_magic_context()
    con = ctx.connect()
    columns = ['status', 'job_id', 'type', 'start_at', 'query']
    values = [[j.status(), j.job_id, j.type, j._start_at, j.query]
              for j in con.client.jobs()]
    return pd.DataFrame(values, columns=columns)

def magic_use(line):
    ctx = get_td_magic_context()
    con = ctx.connect()
    try:
        tables = con.client.tables(line)
    except tdclient.api.NotFoundError:
        sys.stderr.write("ERROR: Database '{0}' not found.".format(line))
        return
    # update context
    ctx.database = line
    # push table names
    get_ipython().push({t.name: MagicTable(t) for t in tables})

def magic_query(line, cell, engine_type):
    ctx = get_td_magic_context()
    con = ctx.connect()
    if line:
        database = line
    else:
        database = ctx.database
    engine = td.create_engine('{0}:{1}'.format(engine_type, database), con=con)
    return td.read_td_query(cell, engine)

def magic_query_plot(line, cell, engine_type):
    df = magic_query(line, cell, engine_type)
    # convert 'time' to time-series index
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df.set_index('time', inplace=True)
    return df.plot()

# extension

def load_ipython_extension(ipython):
    ipython.push('get_td_magic_context')

    # %td_databases [PATTERN]
    ipython.register_magic_function(magic_databases, magic_kind='line', magic_name='td_databases')

    # %td_tables [PATTERN]
    ipython.register_magic_function(magic_tables, magic_kind='line', magic_name='td_tables')

    # %td_jobs
    ipython.register_magic_function(magic_jobs, magic_kind='line', magic_name='td_jobs')

    # %td_use database
    ipython.register_magic_function(magic_use, magic_kind='line', magic_name='td_use')

    # %%td_hive, %%td_pig, %%td_presto
    # %%td_hive_plot, %%td_pig_plot, %%td_presto_plot
    for name in ['hive', 'pig', 'presto']:
        def query(line, cell):
            return magic_query(line, cell, name)
        def query_plot(line, cell):
            return magic_query_plot(line, cell, name)
        ipython.register_magic_function(query, magic_kind='cell', magic_name='td_' + name)
        ipython.register_magic_function(query_plot, magic_kind='cell', magic_name='td_' + name + '_plot')
