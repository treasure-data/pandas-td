# Version History

## 0.8.3 (TBD)

* Add 'clear_progress' parameter to create_engine.
* Magic query functions now substitute variables into query text.
* New option --dry-run for magic query functions.

## 0.8.2 (2015-07-20)

* Minor bug fixes.

## 0.8.1 (2015-07-18)

* Add options for ``%%td_hive``, ``%%td_pig``, and ``%%td_presto``.
* ``%%td_hive_plot``, ``%%td_pig_plot``, and ``%%td_presto_plot`` are deprecated.  Use ``--plot`` options instead.

## 0.8.0 (2015-07-13)

* New function ``create_engine``.
* (Experimental) New module ``pandas_td.ipython`` for magic functions.
* ``Connection.databases`` and ``Connection.tables`` are deprecated.  Use magic functions.
* ``Connection.query_engine`` is deprecated.  Use ``create_engine`` instead.
* ``read_td_query`` sets ``distributed_join = 'false'`` by default for Presto.
