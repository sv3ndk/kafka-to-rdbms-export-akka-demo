# Kafka to RDBMs export demo

This project illustrates an approach for exporting Avro records from Kafka to a DB. 

The mechanism is agnostic of the actual Avro schema: the records are read as `GenericRecords` and the corresponding SQL `INSERT` statement is executed, mapping each avro field to a DB column. Nested Avro fields are simply prefixed with their parent name.

The target DB table is assumed to already exist. 

The implementation is based on [Akka streams](https://doc.akka.io/docs/akka/current/stream/index.html), [Alpakka](https://doc.akka.io/docs/alpakka/current/index.html), [Slick](https://doc.akka.io/docs/alpakka/current/slick.html) (JDBC) and [Avro4s](https://github.com/sksamuel/avro4s).

## How to run the demo:

### Create the source Kafka topic:
 
```
kafka-topics \
    --create \
    --zookeeper ${ZOOKEEPER_URL} \
    --replication-factor 3 \
    --partitions 2 \
    --topic demo-input
```


### Create the target DB setup

In a `psql` shell, create the target destination database:

```
create database kafka_to_sql_demo;
create user kafkademo with password 'secret' ;
```

Create the destination table inside that database:

The columns of the DB must be made of: 

* all field of the kafka key, prefixed with `kafka_key`
* all fields of the value
* `_kafka_offset` and `_kafka_offset`
* order of columns does not matter: inserts are done per key.

For example, give our [DataModel.scala](src/main/scala/com/svend/demo/DataModel.scala), we have in DB:


```

create table pizzas (

    -- metadata added to enable restartability
    _kafka_offset bigint,
    _kafka_partition int,

    -- kafka key fields are prefixed with kafka_key
    kafka_key_id int,

    -- fields of the value
    name text,
    vegetarian boolean,
    vegan boolean,
    calories int,

    -- nested fields are prefixed with the parent name
    chef_name text,
    chef_firstname text

);

```


### Launch the basic data-generator 

Update the `data-generator` [application.conf](src/main/resources/application.conf) as appropriate, then:

```
sbt "runMain com.svend.demo.DataGeneratorApp"
```

### Execute the main function

```
sbt "runMain com.svend.demo.IngestDemo"
```
## TODO

To be fixed:

* Due to the way I build SQL queries with Slick, I only support records with exactly known number of fields :(
* Not all Avro types are supported, though all primitives are there, + optional fields and nested records.
