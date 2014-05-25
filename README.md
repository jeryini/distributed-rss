# Distributed RSS
Distributed system for reading RSS feeds.

## General Requirements
This solution requires the following systems:

* [MongoDB](http://www.mongodb.org/)
 The MongoDB database for storing feeds and entries. **Version 2.4.9**
* [Apache ActiveMQ](http://activemq.apache.org/)
 Message broker for distributing workload. **Version 5.9.1**

### Required libraries
Project uses Maven to define dependencies on third party libraries. Nonetheless here is the list of required libraries:

* [ROME](http://rometools.github.io/rome/)
Library for RSS/Atom in Java.
* [Mongo Java Driver](http://docs.mongodb.org/ecosystem/drivers/java/)
Java driver for MongoDB.
* [ActiveMQ](http://activemq.apache.org/)
Java driver for ActiveMQ message broker.
* [JUnit](http://junit.org/)
Library for Unit testing.
* [Joda-Time](http://www.joda.org/joda-time/)
Library for Java DateTime.
* [HttpComponents](http://hc.apache.org/httpcomponents-client-ga/)
Library for efficent HTTP support in Java.
* [Log4J](http://logging.apache.org/log4j/2.x/)
Logging framework.
* [Commons Codec](http://commons.apache.org/proper/commons-codec/)
Support for efficent encoders (e.g. SHA-1 digest util).
* [Commons CLI](http://commons.apache.org/proper/commons-cli/)
API for parsing command line options.

## General solution
The general solution consists of three JAR files:
* InsertResources: For inserting RSS feeds from CSV file into MongoDB.
* RSSDelegateWorker: For inserting jobs into message queue and checking for stalled jobs.
* RSSMainWorker: For running thread workers which fetch entries of feeds, fetch the web page and persist it to the MongoDB.

### Running
A quick tutorial for running the solution.

1. First run the InsertResources jar:
```java
java -jar InsertResources
```

The program accepts the following parameters:
```
usage: java -jar InsertResources
 -collName <arg>   the name of collection to use
 -dbName <arg>     the name of the database to use
 -filePath <arg>   the path of the file with RSS feeds
 -help             help for usage
 -host <arg>       database's host address
 -port <arg>       port on which the database is running
```


