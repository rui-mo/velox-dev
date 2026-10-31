==================
Spark Query Runner
==================

Introduction
------------

The Spark Query Runner is a tool designed to facilitate the testing of Velox.
It helps ensure the correctness of Velox's computing against Spark and
provides a method for identifying potential issues in Velox's implementation.
Spark Query Runner is designed to run against Spark-4.3.0.

How It Works
------------

The Spark Query Runner operates by executing given SQL queries on Spark and
returning results as Velox data format, which allows the comparison of results
between Velox and Spark.

Since Spark 3.4, Spark Connect has introduced a decoupled client-server architecture
for Spark that allows remote connectivity to Spark clusters. From the client
perspective, Spark Connect mostly behaves as any other gRPC client, which is polyglot
and cross-platforms. During execution, the Spark Connect endpoint embedded on the
Spark Server receives and parses queries into Spark’s logical plan operators.
From there, the standard Spark execution process kicks in, ensuring that Spark
Connect leverages all of Spark’s optimizations and enhancements. Results are
streamed back to the client through gRPC as Arrow-encoded row batches.

In the Spark Query Runner, we use Spark Connect to submit queries to Spark and fetch
the results back to Velox. The steps for this process are as follows:

1. Provide the Spark SQL query to be executed. The query could be generated from Velox
   plan node or manually written.
2. Create a protobuf message `ExecutePlanRequest` from the SQL query. The protocols
   used by Spark Connect are defined in `Apache Spark <https://github.com/apache/spark/tree/v4.3.0/sql/connect/common/src/main/protobuf/spark/connect>`_.
3. Submit the message to SparkConnectService through gRPC API `ExecutePlan`.
4. Fetch Spark's results from execution response. Results are in Arrow IPC stream format,
   and can be read as Arrow RecordBatch by `arrow::ipc::RecordBatchReader`.
5. Convert Arrow RecordBatch as Velox vector for the comparison with Velox's results.

Usage
-----

To use the Spark Query Runner, you will need to deploy an executable Spark and start the
Spark Connect server with below command. Since Spark 4.0, the Spark Connect server is
bundled in the Spark distribution, so no extra jar is needed.

.. code-block::

    "$SPARK_HOME"/sbin/start-connect-server.sh

The Spark Query Runner sets the Spark SQL configs that match the configuration used by
the Velox fuzzers for its session, e.g. ANSI mode disabled and the session timezone set
to ``America/Los_Angeles``. The settings in
``scripts/ci/spark/conf/spark-defaults.conf.example`` apply the same configs to the
whole server. If Spark Connect server is started successfully, you can see log as
below. The server will be started at `localhost:15002`.

.. code-block::

    INFO SparkConnectServer: Spark Connect server started at: 0:0:0:0:0:0:0:0%0:15002

Another option is to use Spark Query Runner in the docker image `ghcr.io/facebookincubator/velox-dev:spark-server`
provided by Velox. It includes an executable Spark and the start script. You can download
the image and run below command to start Spark connect server in it.

.. code-block::

    bash /opt/start-spark.sh

You can then provide the Spark Query Runner with the SQL query and the data to run the
query on. The tool will execute the query on Spark and return results as Velox data format.

SQL Generation
--------------

The Spark Query Runner converts Velox plans that consist of Values, Project,
single-step Aggregation and Window nodes to Spark SQL. Expressions are translated
to the Spark SQL equivalents, e.g. ``add(a, b)`` becomes ``(a + b)``,
``like(s, p, e)`` becomes ``(s LIKE p ESCAPE e)`` and ``first_ignore_null(x)``
becomes ``first(x, true)``. Constants become literals of the same type, e.g.
``CAST(-5 AS INT)`` and ``timestamp_micros(1000L)``. Other functions are called
by their names. Plans, expressions, types and values that cannot be expressed in
Spark SQL with the same semantics, e.g. nondeterministic functions, partial
aggregations, sorted aggregations, RANGE frames with offsets, intervals day to
second, HUGEINT and timestamps with nanoseconds, are reported as not supported by
the reference DB. ``isConstantExprSupported`` returns false for such constants,
so the expression fuzzer generates them as columns.

The input data is written to Parquet files, which Spark reads as temporary views.
Spark cannot read struct fields with empty or case-insensitively duplicate names,
so the fields of such structs are named ``col1``, ``col2``, etc. in the files and
in the generated SQL. Year-month intervals are written as the number of months and
converted back by ``make_ym_interval``.

Error Handling
--------------

Spark errors are classified by their Java exception and SQLSTATE. Analysis errors
(``AnalysisException``, including legacy errors that have no SQLSTATE, and SQLSTATE
class 42), unsupported features (class 0A), resource limit errors (classes 53 and 54),
Spark internal errors (class XX) and fatal JVM errors, e.g. ``OutOfMemoryError``, mean
Spark cannot run the query, so the query is reported as not supported by the reference
DB and the results are not verified. Other errors, e.g. division by zero with ANSI mode
enabled, are reported as a reference query failure. A syntax error means the SQL
generated from the Velox plan is invalid and fails the test. A stopped SparkContext
fails the test as well because all following queries would fail. A query that runs
longer than the timeout, 5 minutes by default, is interrupted and reported as not
supported. Each failed query is logged with the ``Spark query`` prefix.

In local mode, a task that runs out of memory kills the whole Spark Connect server.
Running Spark in local-cluster mode, e.g. with ``spark.master local-cluster[1,4,3072]``
and ``spark.executor.memory 2g``, runs the tasks in a separate executor JVM, which is
relaunched after such a failure.

Spark is used as the reference DB in the aggregation fuzzer test and in the Spark
expression fuzzer test, in which the results from Velox and Spark are compared to check
for any differences. If the results match, it indicates that Velox is producing the
correct output. If the results differ, it suggests a potential issue in Velox that needs
to be investigated. The aggregation fuzzer test connects to the Spark Connect server at
``localhost:15002``. The Spark expression fuzzer test connects to the server specified by
``--spark_url``, e.g. ``--spark_url=localhost:15002``, and uses
``--spark_query_timeout_sec`` as the query timeout, 60 seconds by default. Functions that
have no equivalent in Spark SQL, e.g. nondeterministic functions, are not tested when
Spark is the reference DB. You can trigger the tests referring to :doc:`Fuzzer <fuzzer>`
and :doc:`Expression Fuzzer <expression-fuzzer>`.
