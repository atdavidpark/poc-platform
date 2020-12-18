# Kafka Java Producer

Two projects

  * `avro-schemas` - holding the Avro schema and the generated Java classes. Can be used to register the schema against the schema registry using Maven.
  * `java-kakfa-producer` - the simulator, which reads the data from a CSV file and publishes the events into a Kafka topic, with the option to overwrite different settings relevant to throughput

## Create the necessary topic

We can create a topic using the `akhq` GUI or using the `kafka-topics` CLI shown here

```
docker exec -it kafka-1 kafka-topics --zookeeper zookeeper-1:2181 --create --topic controldata-v1 --partitions 16 --replication-factor 3

```

## Registering the Avro Schema

Change to the Java project

```
cd $DATAPLATFORM_HOME/../impl/java/avro-schemas
```

Run maven install plus the `schema-registry:register` to register the schemas listed in the `pom.xml` into the Confluent Schema Registry.

```
mvn clean install schema-registry:register
```

You can use the schema registry UI to cross-check the registration: <http://dataplatform:28102>.

## Prepare the data

The data is available on the `poc-datasource` machine in the `data` folder inside the `poc-platform` folder.

```
cd /home/ubuntu/poc-platform/data
```

We had to once remove the last 5 lines, which are garbage from the data generation (**Do not do this again!**).

```
for i in $(seq 1 5); do sed -i '$d' tng_small.csv; done;
for i in $(seq 1 5); do sed -i '$d' tng.csv; done;
```

Let's see the files

```
ubuntu@ip-172-26-4-40:~/poc-platform/impl/java/java-kafka-producer$ ls -lsah /home/ubuntu/poc-platform/data
total 6.3G
4.0K drwxrwxr-x 2 ubuntu ubuntu 4.0K Dec 18 09:49 .
4.0K drwxr-xr-x 8 ubuntu ubuntu 4.0K Dec 18 09:33 ..
5.5G -rw-rw-r-- 1 ubuntu ubuntu 5.5G Dec 18 09:49 tng.csv
 49M -rw-r--r-- 1 ubuntu ubuntu  49M Dec 18 09:45 tng_small.csv
```

Show number of lines 

```
ubuntu@ip-172-26-4-40:~/poc-platform/data$ wc -l tng_small.csv
703040 tng_small.csv
ubuntu@ip-172-26-4-40:~/poc-platform/data$ wc -l tng.csv
80216864 tng.csv
```

## Running Simulator with various settings


```
cd $DATAPLATFORM_HOME/../impl/java/java-kafka-producer
```

```
export INPUT_FILE=tng_small.csv
export INPUT_FILE=tng.csv
```

## Run it synchronously with acks=1 and LZ4 compression

```
mvn exec:java -Dexec.args="-f /home/ubuntu/poc-platform/data/$INPUT_FILE -k 1 -c lz4"
```

## Run it asynchronously with a large batch-size, acks=1 and LZ4 compression

```
mvn exec:java -Dexec.args="-f /home/ubuntu/poc-platform/data/$INPUT_FILE -a -k 1 -s 100000 -l 100 -c lz4"
```

## Run it asynchronously with a even larger batch-size, acks=1 and LZ4 compression

```
mvn exec:java -Dexec.args="-f /home/ubuntu/poc-platform/data/$INPUT_FILE -a -k 1 -s 200000 -l 100 -c lz4"
```

# Using ksqlDB to work with the data

```
docker exec -it ksqldb-cli ksql http://ksqldb-server-1:8088
```

```
CREATE STREAM IF NOT EXISTS controldata_s (
  id VARCHAR KEY
  , ARCHITIMETEXT VARCHAR
  , B1 VARCHAR
  , B2 VARCHAR
  , B3 VARCHAR
  , ELEM VARCHAR
  , INFO VARCHAR
  , RESOLUTION VARCHAR
  , VALUE DOUBLE
  , FLAG VARCHAR
 ) WITH (kafka_topic='controldata-v1',
        value_format='AVRO');
```

```
SELECT * FROM controldata_s EMIT CHANGES;
```

## Join

```
CREATE TABLE IF NOT EXISTS masterdata_t (id VARCHAR PRIMARY KEY,
   description VARCHAR)  
  WITH (kafka_topic='masterdata_v1', 
        value_format='JSON',
        partitions=16);
```

```
INSERT INTO masterdata_t VALUES ('N1111:K2222:K3333:Blement:Anfo', 'this is the metadata for N1111,K2222,K3333,Blement and Anfo');
```

```
INSERT INTO masterdata_t VALUES ('I1111:G2222:M3333:Blement:Bnfo', 'this is the metadata for I1111, G2222, M3333,Blement and Bnfo');
```


## Left Join Stream with Table

```
SELECT cd.id, cd.value, md.description 
FROM controldata_s 	cd
INNER JOIN masterdata_t 				md
ON cd.id  = md.id
EMIT CHANGES;
```


## Pull Query

```
SELECT * FROM controldata_s 
WHERE b1 = 'F1111' 
	AND b2 = 'A2222' 
	AND b3 = 'O3333' 
	AND elem = 'Alement'
	AND info = 'Anfo';
```

```
CREATE TABLE IF NOT EXISTS controldata_t
WITH (kafka_topic = 'controldata_t', value_format='AVRO')
AS
SELECT concat (b1,':',b2,':',b3,':',elem,':',info)          key
       , latest_by_offset(architimetext)                    architimetext
		, latest_by_offset(value)			                    value
		, latest_by_offset(flag)			                    flag
FROM controldata_s
GROUP BY concat (b1,':',b2,':',b3,':',elem,':',info)
EMIT CHANGES;
```

```
SELECT * 
FROM controldata_t 
WHERE KEY = 'N1111:A2222:A3333:Alement:Bnfo';
```




### Group By


```
CREATE TABLE controldata_by_1hour_tumbl_t AS
SELECT windowstart AS winstart
	, windowend 	AS winend
	, b1, b2, b3, elem, info
	, count(*) as nof
	, avg(value) 	AS avg_value
	, min(value)   min_value
	, max(value)   max_value 
FROM controldata_s 
WINDOW TUMBLING (SIZE 5 minutes)
GROUP BY b1, b2, b3, elem, info;
```
