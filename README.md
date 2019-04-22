[![Go Report Card](https://goreportcard.com/badge/logrange/logrange)](https://goreportcard.com/report/logrange/logrange) [![Build Status](https://travis-ci.org/logrange/logrange.svg?branch=master)](https://travis-ci.org/logrange/logrange) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/logrange/logrange/blob/master/LICENSE)

# Logrange - streaming database 
Logrange is highly performant streaming database for aggregation streams of records like application logs, system metrics, audit logs etc. from thousands of sources. Logrange  provides an API to do operations from simple search to the data analysis and machine learning.

__The product__
* _Size tollerant_. Logrange is able to store as much data as written there. The only limitation is the storage space available for the database.
* _Higly performant_. Writing and reading millions of records per second (hundered megabytes or gigabytes of data).
* _Write optimized_. Logrange persists the raw data in realtime. All other data processing like indexing can be done later.
* _Low latency_. Data becomes available for read within milliseconds after it is written
* _Scalable_. Supporting tens of thousands different streams of records (terrabytes of the data)
* _Highly available_. In clustering solution, data could be replicated between logrange nodes. Logrange will support data and load distribution policies. 
* _Native for stream processing_. Merging, filtering and search using LQL (Logrange Query Language)
* _Open Source_. Logrange is 100% open source. It can be used for building trustworthy data storages.
* _Ready to use_. Basic installation includes pre-configured log processing tools: collector, forwarder, CLI tool and Logrange database service. 
* _Easy installation_ either in a containerized (k8s or docker) or a custom environment.

## Quick start
Logrange shipment includes 2 executables - `logrange` server and `lr` - the logrange client. In the quick start you can use precompiled binaries to try Logrange out. 

### Step 1. Install logrange server and run it.
The following 2 commands allow to download, install and run the logrange server:
```bash

$ curl -s http://get.logrange.io/install | bash -s logrange
$ logrange start --journals-dir /tmp
...
```
Normally, you have to see logrange server logs, cause the server attached to the console where the it runs. Don't stop it, just switch to another console.

### Step 2. Collecting logs and sending them to the server.
To collect logs, we need to download and run the `lr` client, which will collect logs from the machine where it runs (`/var/log` folder, by default). Having server runs, type in another console:
```bash

$ curl -s http://get.logrange.io/install | bash
$ lr collect
...
```
You have to see the collector logs, cause the collector is attached to the console where the it runs.

### Step 3. Connect to the server, using CLI tool.
You can connect to the logrange server and type commands in CLI tool:
```bash
$ lr shell
...
```

In the logrange shell, you can try `select` to retrieve collected data: 
```
> select limit 10
...
```

Or try `help` to find out what commands are available.

## Documentation 
[k8s Installation](https://github.com/logrange/k8s)

## Getting Help
- Found a bug or thinking about a new feature? [File an issue](https://github.com/logrange/logrange/issues/new)
- If you have any question or feedback regarding Logrange, send us e-mail to: mail@logrange.io

## Contact us
Whether you have problems with log aggregation, processing or analysis, or wanting to build a secure data aggregation solution.
 
Reach out to mail@logrange.io

## License
Apache License 2.0, see [LICENSE](LICENSE).

