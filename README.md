# Pandas-TD

[![Build Status](https://travis-ci.org/treasure-data/pandas-td.svg?branch=master)](https://travis-ci.org/treasure-data/pandas-td)
[![Code Health](https://landscape.io/github/treasure-data/pandas-td/master/landscape.svg?style=flat)](https://landscape.io/github/treasure-data/pandas-td/master)
[![Coverage Status](https://coveralls.io/repos/treasure-data/pandas-td/badge.svg?branch=master)](https://coveralls.io/r/treasure-data/pandas-td?branch=master)
[![PyPI version](https://badge.fury.io/py/pandas-td.svg)](http://badge.fury.io/py/pandas-td)

## Install

You can install the releases from [PyPI](https://pypi.python.org/).

```sh
$ pip install pandas-td
```

## Examples

```python
import os
from pandas_td import connect
from pandas_td import read_td
from pandas_td import to_td

# Initialize connection
td = connect(apikey=os.environ['TD_API_KEY'],
             endpoint='https://api.treasuredata.com/')
presto = td.query_engine(database='sample_datasets', type='presto')

# Read query result as DataFrame
df = read_td('select * from www_access', presto)

# Upload DataFrame to a table
to_td(df, 'my_db.test_table', td)
```

## License

Apache Software License, Version 2.0
