# Kafka to RDBMs export demo

This project illustrates an approach for exporting Avro records from Kafka to a DB. 

The mechanism is agnostic of the actual Avro schema: the records are read as `GenericRecords` and the corresponding SQL `INSERT` statement is executed.

The target DB table is assumed to already exist. 

The implementation is based on [Akka streams](https://doc.akka.io/docs/akka/current/stream/index.html), [Alpakka](https://doc.akka.io/docs/alpakka/current/index.html) and [Slick](https://doc.akka.io/docs/alpakka/current/slick.html) (JDBC).

## How to run the demo:

In a `psql` shell, create the target destination database:

```
create database kafka_to_sql_demo;
create user kafkademo with password 'secret' ;
```

Create the destination table inside that database:

The columns of the DB must be made of: 

* all field of the kafka key, prefixed with `kafka_key`
* all fields of the value
* `kafka_offset` and `kafka_partition

  
```
create table historical_waiting_times (
    kafka_key_mainLocation text, 
    kafka_key_subLocation text, 
    kafka_key_startSegment bigint,   
    kafka_key_endSegment bigint,
    kafka_key_startTime bigint, 
    kafka_key_endTime bigint,
    kafka_key_deviceId bigint,
    kafka_key_laneType text,
    
    mainLocation text,
    subLocation text,
    startSegment bigint,        
    endSegment bigint,
    startTime bigint,
    endTime bigint,
    deviceId bigint,
    laneType text,
    paxEntry text,
    euProperty text,
    waitingTimeSeconds bigint,
       
    _kafka_offset bigint,
    _kafka_partition int
);
```

Execute the main function

## TODO

To be fixed:

* Due to the way I build SQL queries with Slick, I only support records with exactly 20 fields
* nested Avro schema are not yet supported
* only a subset of Avro primitive types are supported
