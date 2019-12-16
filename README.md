[![Go Report Card](https://goreportcard.com/badge/logrange/logrange)](https://goreportcard.com/report/logrange/logrange) [![Build Status](https://travis-ci.org/logrange/logrange.svg?branch=master)](https://travis-ci.org/logrange/logrange) [![GoDoc](https://godoc.org/github.com/logrange/logrange/api?status.png)](https://godoc.org/github.com/logrange/logrange/api)[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/logrange/logrange/blob/master/LICENSE)

# Logrange - streaming database 
[Logrange](https://www.logrange.io) is highly performant streaming database for aggregating streams of records like application logs, system metrics, audit logs etc. from thousands of sources. Logrange  provides an API to do operations from simple search to the data analysis and machine learning.

__The product__
* _Size tolerant_. Logrange is able to store as much data as written there. The only limitation is the storage space available for the database.
* _Highly performant_. Writing and reading millions of records per second (hundered megabytes or gigabytes of data).
* _Write optimized_. Logrange persists the raw data in realtime. All other data processing like indexing can be done later.
* _Low latency_. Data becomes available for read within milliseconds after it is written
* _Scalable_. Supporting tens of thousands different streams of records (terrabytes of the data)
* _Highly available_. In clustering solution, data could be replicated between logrange nodes. Logrange will support data and load distribution policies. 
* _Native for stream processing_. Merging, filtering and search using LQL (Logrange Query Language)
* _Open Source_. Logrange is 100% open source. It can be used for building trustworthy data storages.
* _Ready to use_. Basic installation includes pre-configured log processing tools: collector, forwarder, CLI tool and Logrange database service. 
* _Easy installation_ either in a containerized (k8s or docker) or a custom environment.

## Quick start
Logrange shipment includes 2 executables - `logrange` server and `lr` - the logrange client. In the quick start you can use precompiled binaries to try logrange out within 1 minute: 

### Step 1. Let's put everything into one dir
Make a directory and enter there:
```bash
mkdir lrquick
cd lrquick
```

### Step 2. Install logrange server and run it
```bash
curl -s http://get.logrange.io/install | bash -s logrange -d ./bin
./bin/logrange start --base-dir=./data --daemon
```
Normally, you have to see something like `Started. pid=12345`

### Step 3. Install logrange client and start collecting logs from the machine
```bash
curl -s http://get.logrange.io/install | bash -s lr -d ./bin
./bin/lr collect --storage-dir=./collector --daemon
```
The command above runs collector in background. It will send logs found in `/var/log` folder to the logrange server started in step 2.

### Step 4. Connect to the server, using CLI tool.
```bash
./bin/lr shell
...
```

In the logrange shell, you can try `select` to retrieve collected data: 
```
> select limit 10
```
Or try `help` command to find out what commands are available.

## Quick stop 
From the logrange folder (`lrquick`) type the following commands to stop collector and the logrange server:
```bash
./bin/lr stop-collect --storage-dir=./collector
./bin/logrange stop --base-dir=./data
```

Now, to clean up, just remove the `lrquick` folder:
```bash
cd ..
rm -rf ./lrquick/
```


## Documentation 
[k8s Installation](https://github.com/logrange/k8s)<br/>
[The product](https://logrange.io/docs)

## Getting Help
- Found a bug or thinking about a new feature? [File an issue](https://github.com/logrange/logrange/issues/new)
- If you have any question or feedback regarding Logrange, send us e-mail to: mail@logrange.io

## Contact us
Whether you have problems with log aggregating, processing or analysis, or wanting to build a secure data aggregating solution.
 
Reach out to mail@logrange.io

Join our [gitter chat](https://gitter.im/logrange/community)

## License
Apache License 2.0, see [LICENSE](LICENSE).

## Acknowledgments
* GoLand IDE by [JetBrains]( https://www.jetbrains.com/?from=logrange) is used for the code development

