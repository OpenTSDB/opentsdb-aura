OpenTSDB Aura Time Series Store
===============================

Aura is an in-memory observability data store with the ability to flush "segments"
to other long-term storage systems.

Background
----------

Taking inspiration from Facebook's Gorilla/Behreingi and Pinterest's Yuvi 
in-memory stores, Aura compresses time series data following the Gorilla protocol
with the added capability to perform additional lossy compression.

The store is meant to hold the most recent time series data with a query engine
like OpenTSDB 3.0 routing queries to longer term storage for older data.

Configuration
-------------

Configuration is performed via a `ShardConfig` and OpenTSDB configuration class.
Future work will merge them.

Usage
-----

Currently, the repo contains the individual libraries for Aura components. We'll 
will tie in OpenTSDB 3.0's write path with Kafka and Pulsar streaming input as
well as the OpenTSDB query servlets.

To store data a `TimeSeriesStorageIf` instance must be instantiated with a
`ShardConfig`. Data can then be written via 
`TimeSeriesShardIF.addEvent(HashedLowLevelMetricData data)` where the data is
an implementation of the low-level interface.

Data can then be read out using the `AuraMetricsSourceFactory` and OpenTSDB query
layer.

Work is ongoing to integrate flushes and reads with an Aeropsike cluster for
long-term retention.

Contribute
----------

Please see the [Contributing](contributing.md) file for information on how to
get involved. We welcome issues, questions, and pull requests.

Maintainers
-----------

* Smruti Ranjan Sahoo
* Arun Gupta
* Ravi Kiran Chiruvolu
* Chaitanya GSK
* Chris Larsen

License
-------

This project is licensed under the terms of the Apache 2.0 open source license. 
Please refer to [LICENSE](LICENSE.md) for the full terms.