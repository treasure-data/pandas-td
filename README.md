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
import pandas_td

# Initialize connection
td = pandas_td.connect(apikey=os.environ['TD_API_KEY'],
                       endpoint='https://api.treasuredata.com',
                       database='sample_datasets',
                       type='presto')

# Read query result as DataFrame
df = pandas_td.read_td('select * from www_access', td)

# Upload DataFrame to a table
pandas_td.to_td(df, 'test_table', td)
```

## License

Apache Software License, Version 2.0
