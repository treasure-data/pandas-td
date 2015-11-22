==========
Job Queues
==========

.. warning::

    This feature is in early development stage and not ready for production.

"Job Queue" is a convenient tool to run queries in background.  You can keep working on your interactive session while running one or more long-running queries asynchronously.  You will get notified by HipChat or Slack as soon as your queries have completed (:doc:`notifications`).

Submitting Queries
==================

::

    import pandas_td as td

    # Create a queue with name (used for notifications)
    q1 = td.create_queue(name='q1')

    # Create a query engine as usual
    engine = td.create_engine('hive:sample_datasets')

    # Run query in the queue
    t1 = q1.submit_query('SELECT ... FROM www_access', engine)

    # Query result can be retrieved later as DataFrame
    df = t1.result()

Magic Functions
===============

::

    In [1]: %%td_hive sample_datasets -a q1
       ...: select count(1) cnt from www_access
       ...: 
    Queued as q1[0]

    In [2]: q1[0].result()
    Out[2]:
        cnt
    0  5000

A convenient way to retrieve the result is to use ``-o`` along with ``-a``::

    In [3]: %%td_hive sample_datasets -a q1 -o df1
       ...: select count(1) cnt from www_access
       ...: 
    Queued as q1[1]

    In [4]: df1  # the value will be stored when your job has finished
    Out[4]:
        cnt
    0  5000
