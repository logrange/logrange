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
* _Easy installation_ either in a containerized or a custom environment.

## Get started
Logrange shipment includes 2 executables - `logrange` server and `lr` - the logrange client. To try Logrange out you need to have the server runs. You can try Logrange by one of the following way:
- [Run it from the binaries](#run-it-from-binaries)
- [Run it locally in Docker](#run-it-locally-in-docker)
- [Deploy it on Kubernetes](#deploy-it-on-kubernetes)
- [Build it from sources](#build-it-from-sources)

### Run it from the binaries
Install the __server__ by executing the command:
```bash
$ curl -s http://get.logrange.io/install | bash -s logrange
```
Install the __client__ by executing the command: 
```bash
$ curl -s http://get.logrange.io/install | bash 
```
by default the server tries to use `/opt/logrange` folder for data, in the test run you can use `/tmp` instead, so run the server:
```bash
$ logrange start --journals-dir /tmp
```
Now, when the server runs, run the client to collect logs (default folder is `/var/log`) in another console window:
```bash
$ lr collect
```
Logs from the `/var/log` will be scanned and sent to the server. 
Now run the CLI and try it out: 
```bash
$ lr shell
...
```
### Run it locally in Docker
<TBD>
### Deploy it on Kubernetes
To install logrange on Kubernetes just run the following command: 
```bash
$ curl -s http://get.logrange.io/k8s/install | bash
```
Logs will be collected and stored in the logrange server which runs in the k8s cluster. For more details are [here](https://github.com/logrange/k8s)

### Build it from sources
To build Logrange you need `go` [v1.11+](https://golang.org/dl/) installed locally.
```bash

$ go get github.com/logrange/logrange
$ cd $GOPATH/src/github.com/logrange/logrange # GOPATH is $HOME/go by default.

$ go build ./cmd/logrange
```

by default the server tries to use `/opt/logrange` folder for data, in the test run you can use `/tmp` instead, so run the server:
```bash
$ ./logrange start --journals-dir /tmp
```

To run collect for collecting log files from `/var/log` and sending the data to the server, use the following commands from `$GOPATH/src/github.com/logrange/logran` in :
```bash
$ go build ./cmd/lr
$ ./lr collect
...
```
To run the CLI tool type the command from `$GOPATH/src/github.com/logrange/logran`:
```bash
$ ./lr shell
...
```

## Contact us
Whether you have problems with log aggregation, processing or analysis, or wanting to build a secure data aggregation solution.
Reach out to mail@logrange.io
## License
Apache License 2.0, see [LICENSE](LICENSE).

