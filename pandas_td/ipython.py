# ipython.py

import argparse
import re
import sys
import numpy as np
import pandas as pd
import pandas_td as td
import tdclient
import IPython

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
        data = [c if len(c) == 3 else [c[0], c[1], ''] for c in table.schema]
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

class MagicQuery(object):
    def __init__(self, engine_type):
        self.type = engine_type
        self.parser = self._argument_parser()

    def _argument_parser(self):
        parser = argparse.ArgumentParser(
            prog = self.type,
            description = 'Cell magic to run a query.',
            add_help = False)
        parser.add_argument('database', nargs='?',
                            help='database name')
        parser.add_argument('--pivot', action='store_true',
                            help='run pivot_table against dimensions')
        parser.add_argument('--plot', action='store_true',
                            help='plot the query result')
        parser.add_argument('-n', '--dry-run', action='store_true',
                            help='output translated code without running query')
        parser.add_argument('-v', '--verbose', action='store_true',
                            help='verbose output')
        parser.add_argument('-o', '--out',
                            help='store the result to a variable')
        parser.add_argument('-q', '--quiet', action='store_true',
                            help='disable progress output')
        return parser

    def create_engine(self, ctx, args, code):
        name = '{0}:{1}'.format(self.type, args.database)
        if args.quiet:
            params = {'show_progress': False}
        elif args.verbose:
            params = {'show_progress': True, 'clear_progress': False}
        else:
            params = {}
        args = [repr(name)] + ['{0}={1}'.format(k, v) for k, v in params.items()]
        code.append("_e = td.create_engine({0})\n".format(', '.join(args)))
        return td.create_engine(name, con=ctx.connect(), **params)

    def pivot_table(self, d, ctx, args, code):
        index = d.columns[0]
        dimension = [c for c, t in zip(d.columns[1:], d.dtypes[1:]) if t == np.dtype('O')]
        measure = [c for c, t in zip(d.columns[1:], d.dtypes[1:]) if t != np.dtype('O')]
        if len(dimension) == 0:
            code.append("_d.set_index({0}, inplace=True)\n".format(repr(index)))
            d.set_index(index, inplace=True)
            return d
        if len(dimension) == 1:
            dimension = dimension[0]
        if len(measure) == 1:
            measure = measure[0]
        code.append("_d = _d.pivot_table({0}, {1}, {2})\n".format(repr(measure), repr(index), repr(dimension)))
        return d.pivot_table(measure, index, dimension)

    def __call__(self, line, cell):
        try:
            args = self.parser.parse_args(line.split())
        except SystemExit:
            return

        # implicit options
        if args.plot:
            args.pivot = True

        # context
        ctx = get_td_magic_context()
        if args.database is None:
            args.database = ctx.database

        code = []
        code.append("# translated code\n")

        # build query
        query = cell.format(**get_ipython().user_ns)
        code.append("_q = '''\n")
        code.append(query)
        code.append("\n'''\n")

        # create_engine
        engine = self.create_engine(ctx, args, code)

        # read_td_query
        code.append("_d = td.read_td_query(_q, _e)\n")
        if args.dry_run:
            html = '<pre style="background-color: #ffe;">' + ''.join(code) + '</pre>\n'
            IPython.display.display(IPython.display.HTML(html))
            return
        d = td.read_td_query(query, engine)

        # convert 'time' to datetime
        if 'time' in d.columns:
            if d['time'].dtype == np.dtype('O'):
                code.append("_d['time'] = pd.to_datetime(_d['time'])\n")
                d['time'] = pd.to_datetime(d['time'])
            else:
                code.append("_d['time'] = pd.to_datetime(_d['time'], unit='s')\n")
                d['time'] = pd.to_datetime(d['time'], unit='s')

        # pivot_table
        if args.pivot:
            d = self.pivot_table(d, ctx, args, code)
        elif 'time' in d.columns:
            code.append("_d.set_index('time', inplace=True)\n")
            d.set_index('time', inplace=True)

        # return value
        r = d
        if args.out:
            code.append("{0} = _d\n".format(args.out))
            get_ipython().push({args.out: d})
            r = None
        if args.plot:
            code.append("_d.plot()\n")
            r = d.plot()
        else:
            code.append("_d\n")

        # output
        if args.verbose:
            html = '<pre style="background-color: #ffe;">' + ''.join(code) + '</pre>\n'
            IPython.display.display(IPython.display.HTML(html))
        return r

# extension

def load_ipython_extension(ipython):
    ipython.push('get_td_magic_context')

    # line magic
    LINE_MAGICS = {
        'td_databases': magic_databases,
        'td_tables': magic_tables,
        'td_jobs': magic_jobs,
        'td_use': magic_use,
    }
    for name, func in LINE_MAGICS.items():
        ipython.register_magic_function(func, magic_kind='line', magic_name=name)

    # cell magic
    for name in ['hive', 'pig', 'presto']:
        ipython.register_magic_function(MagicQuery(name), magic_kind='cell', magic_name='td_' + name)
