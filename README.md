[![Go Report Card](https://goreportcard.com/badge/logrange/logrange)](https://goreportcard.com/report/logrange/logrange) [![Build Status](https://travis-ci.com/logrange/logrange.svg?branch=master)](https://travis-ci.com/logrange/logrange) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/logrange/logrange/blob/master/LICENSE)

# Logrange - distributed data aggregation system
Logrange is high performance distributed data aggregation system. It allows to persist massive data streams (literally the write speed could be hundreds of MBytes per second on a commodity machine). System could be scaled by traffic and the storage size, by adding new machines to the cluster. Logrange is tolerant to database size, what means that the data write, access and processing speed does not depend on the datastorage size. Logrange will be effective with managing giga-, tera- or even peta-bytes of data, what makes it be ideally used for:
* log aggregation and log-data management
* collecting data metrics and to build statistics over it
* storing and blazingly fast processing data streams
* running on premises, containerized, VM-based or as a cloud log-aggregation service

Logrange is simple, but it has wide configuration flexibility, what allows to run Logrange as standalone or a cluster of machines to be run on different platforms. It is one executable for target platform, which is in simplest configuration could be run on a laptop within seconds.

