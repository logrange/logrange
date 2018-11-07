# What is Logrange?

The idea behind Logrange is to have simple yet fast and robust way to store and retrieve log data both on-premise and in virtualized environments (VMs, containers, clouds). Besides common technical principles of distributed systems the main principles which we pursue in Logrange as a product is that it should be a human friendly system that is easy to start using and support and it should be reasonable/practical in what it does, i.e. every decision in what direction the product should go is made very carefully.

Logrange consists of the following core components:

- Aggregator: data storage which aggregates log data, coming from different sources (instances, VMs, containers);

- Collector: collects log data (from filesystem) on a given source (instance, VM, container) and sends it to Aggregator;

- Forwarder: extracts logs from Aggregator based on some criteria and sends them to a 3-rd party system.


```

+---------------+                                                   +----------------+
| VM            |   Athmosphera                          HTTP       | VM             |     Rsyslog
|-------------+ |                                                   | +------------+ |
| Collector 1 |--------> |                                 | <--------| Forwarder 1|-------> | -------> 3rd party system 1
+-------------+-+        |                                 |        | +------------+ |       |
                         |                                 |        | +------------+ |       |       
                         |                                 | <--------| Forwarder 2|-------> | -------> 3rd party system 2   
+---------------+        |                                 |        +-+------------+-+       |
| AWS instance  |        |        +---------------+        |                                 |
|-------------+ |        |        |               |        |                                 |
| Collector 2 |--------> |------> |   Aggregator  | <------|                                 |
+-------------+-+        |        |               |        |        +----------------+       |
       ...               |        +---------------+        |        | AWS instance   |       |
       ...               |                                 |        | +------------+ |       |
+---------------+        |                                 | <--------| Forwarder 3|-------> | -------> 3rd party system 3       
| k8s container |        |                                 |        | +------------+ |       |
|-------------+ |        |                                 |        | +------------+ |       |
| Collector N |--------> |                                 | <--------| Forwarder 4|-------> | -------> 3rd party system 4
+-------------+-+                                                   +-+------------+-+     

```