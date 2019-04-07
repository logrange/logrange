[![Go Report Card](https://goreportcard.com/badge/logrange/logrange)](https://goreportcard.com/report/logrange/logrange) [![Build Status](https://travis-ci.org/logrange/logrange.svg?branch=master)](https://travis-ci.org/logrange/logrange) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/logrange/logrange/blob/master/LICENSE)

# Logrange - streaming database 
Logrange is highly performant streaming database, which allows aggregating, structuring and persisting streams of records like application logs, system metrics, audit logs etc. Logrange persists input streams on the disk and provides an API for accessing the stored data to build different analytic tools on top of it.

__Highlights__
* _Size tollerant_. Logrange performance doesn't depend on the database size either it is 100Kb or 100Tb of data
* _Higly performant_. Accepting millions of records or hundred megabytes per second
* _Low latency_. Data becomes available for read within milliseconds after it is written
* _Scalable_. Supporting tens of thousands different streams
* _Fast data processing_. No data indexing overhead, but effective streaming and batch data processing
* _Native for stream processing_. Merging, filtering and search using LQL (Logrange Query Language)
* _Ready to use_. Basic installation includes pre-configured log processing tools: collector, forwarder, CLI tool and Logrange database service. 
* _Easy installation_ for both containerized or a custom environments.
* _Save first for further processing_ concept. 

## Save first for further processing? What exactly does that mean?
Modern systems produce huge amount of information which can help to build statistics, analytics, to understand the system anomalies or even discovering security breches. Usually the information is written in form of system or application logs. 

The modern tools by log processing offers the concept where the data should be analyzed and filtered before it is saved. This concept is caused by huge amount of information, which can impact the log processing tools performance significantly. The problem with the concept is it allows to drop essential data due to weak analyzis or imperfection of the data filtering.

Logrange offers another concept. It is - _to save first and you can drop it later, if you don't need it_. Logrange is intended to be a tool, which allows to store whatever streaming data a system produces without worriying about the database size, or analyzing the data before saving it. 

## What does Logrange allow to do?
Logrange does the following things: 
* Collecting streams of records in different formats from multiple sources - files, databases, other systems. 
* Accessing to the aggregated data via API, which allows searching, merging, and filtering data from different streams of records.
* Retrieving and processing the stored information like analyzing the data or forwarding filtered or all aggregated data to 3rd party systems etc.

## What about other log aggregation solutions? How Logrange is different?
Logrange is intended for storing thousands of streams of records, like application logs, allowing millions writes per second with low latency. The written data becomes available within mileseconds after wrtiting it into database. The disk structures Logrange uses scale well, so its performance doesn't depend on how big the stored data is - either it is megabytes or terabytes of the data.

Logrange is focused on streams processing, but not on the data indexing. It is not indended for full text search, even though we do support features like `search` in Logrange as well. Logrange is optimized to work with streams of records and big arrays of the log data.

Moreover, Logrange allows to store not only application logs, but any streaming data, which could be collected from 3rd party system. This makes Logrange an integration tool of different types of streams collected from different sources and stored in one databas sutable for furhter processing.

The features like analytics, statistics and data learning could be easily built on top of Logrange database.

# Introduction
Logrange database can be run as stand-alone application or as a cluster (distributed system which consists of multiple instances). It provides an API which is used for writing by _Collectors_ - software clients which `writes` input streams of records into the Logrange database. Another type of clients are _Consumers_ that use Logrange API for retrieving data and sending it to another system for further processing or just show it to a user in interactive manner:

![Logrange Structure](https://raw.githubusercontent.com/logrange/logrange/master/doc/pics/Logrange%20Structure.png)

Logrange works following entities:
* _stream_ - a sequence of _records_. Stream can be imagined like an endless sequence of records. 
* _partition_ - is a slice of _stream_ of _records_ stored into Logrange database. So _partition_ can contain 0 or more records. Every _partition_ has an unique identifier which is just a combination of _tags_. Comparing to relational databases _partition_ has the same meaninig as a table of records.
* _tags_ - is a combination of key-value pairs, applied to a _partition_
* _record_ - is an atomic piece of information from a _stream_. Every _record_ contains 2+fields.
* _field_ - is a key-value pair, which is part of a record.

### Partitions and tags
In Logrange a persisted stream of records is called _partition_. Every _partition_ has an unique combination of _tags_. _tags_ are a comma separated key-value pairs written in the form like:
```
name=application1,ip="127.0.0.1"
```
To address a partition for __write__ operation an unique combintaion of _tags_ must be provided. For example, Collector must provide _tags_ combination which identifies the partition where records should be written. 

Tags are used for selecting partitions where records should be read. To select one or more partitions the condition of tags should be provided. For example:
* `name=application1,ip="127.0.0.1"` - select ALL partitions which tags contain both of the pairs `name=application1` and `ip="127.0.0.1"`
* `name=application1 OR ip="127.0.0.1"` - selects all partitions which tags contain either `name=application1` pair, or `ip="127.0.0.1"`pair, or both of them
* `name LIKE 'app*'` - selects all partitions which tags contain key-value pair with the key="name" and the value which starts from "app"
etc.

### Records and fields
A _stream_ consists of ordered _records_. Every record contains 2 mandatory fields and 0 or more optional, custom fields. The mandatory fields are:
* `ts` - the records timestamp. It is set by Collector and it can be 0
* `msg` - the record content. This is just a slice of bytes which can be treated as a text.
Optional fields are key-value pairs, which value can be addressed by the field name with `fields:` pfrefix. Fields can be combined to expressions. For example:
* `msg contains "abc"` - matches records, for which `msg` field contains text "abc"
* `msg contains "ERROR" AND fields:name = "app1"` - matches records, for which `msg` field contains text "ERROR" AND the field with the key="name" has value="app1"
etc.

## Main components
### Aggregator
### Clients
#### Log Collector
#### CLI tool
#### Log Forwarder
## Logrange Query Language (LQL)
# Available Configurations
# Roadmap

