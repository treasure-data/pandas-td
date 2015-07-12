# Version History

## 0.8.0 (2015-07-13)

* New function ``create_engine``.
* (Experimental) New module ``pandas_td.ipython`` for magic functions.
* ``Connection.databases`` and ``Connection.tables`` are deprecated.  Use magic functions.
* ``Connection.query_engine`` is deprecated.  Use ``create_engine`` instead.
* ``read_td_query`` sets ``distributed_join = 'false'`` by default for Presto.
