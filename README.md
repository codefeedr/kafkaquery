# CodeFeedr KafkaQuery

CodeFeedr KafkaQuery allows to operate on JSON data in a Kafka instance using [Flink SQL](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html).

## Getting Started

Run ```sbt pack``` to create a package folder which includes program launch scripts in the following directory: ```kafkatime/target/pack/bin/kakfaquery```

Add CodeFeedr to you path:
```
export PATH=$PATH:<path to bin folder>

e.g. export PATH=$PATH:/Users/MyUsername/Documents/kafkaquery/kafkatime/target/pack/bin
```

Specify Kafka and Zookeeper addresses either in your environment variables or as [arguments](#address)  when executing the launch script.<br/>


| Property          | Default value  |Environment variable name (optional) |
|-------------------|----------------|-------------------------------------|
| Kafka Address     | localhost:9092 |KAFKA_ADDR                           |
| ZooKeeper Address | localhost:2181 |ZK_ADDR                              |

## Usage

Execute the codefeedr script which can be found here ```kafkaquery/kafkatime/target/pack/bin```

In the following examples Kafka and Zookeeper instances are running on the default addresses. <br>
To pause examples and mark & copy commands within them click on the examples to get redirected to the respective [Asciinema](https://asciinema.org/) page.


#### Example 1: Show all available topics & display a schema
[![asciinema page](docs/UsageExamples/showTopics.gif)](https://asciinema.org/a/360404)

#### Example 2: Display all data in the topic pypi_releases_min 
[![asciinema page](docs/UsageExamples/selectAll.gif)](https://asciinema.org/a/360660)

#### Example 3: Aggregate author names for every hour
[![asciinema page](docs/UsageExamples/aggregateAuthors.gif)](https://asciinema.org/a/360672)


## Commands

| Command                                                            | Description                                                                                                                                                                             | Example                                                                                                                                                                                          |
|--------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --help <br/> -h                                                    | Lists all available commands & options.                                                                                                                                                 | --help                                                                                                                                                                                           |
| --topics                                                           | Lists all available topics.                                                                                                                                                             | --topics                                                                                                                                                                                         |
| --topic <topic_name>                                               | Displays information about data format in the specified topic.                                                                                                                          | --topic "pypi_releases_min"                                                                                                                                                                      |
| --schema <topic_name>=<avro_Schema>                                | Updates (or adds if not present) the schema for the specified topic.                                                                                                                    | --schema "my_topic"="{\"type\":\"record\",\"name\":\"Person\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"city\",\"type\":\"string\"}]}" |
| --infer-schema <topic_name>                                        | Infers the Avro schema from the last record in the specified topic and registers it.                                                                                                    | --infer-schema "my_topic"                                                                                                                                                                        |
| --zookeeper <zookeeper_address> <a id="address"></a>               | Sets the ZooKeeper address to the specified one for this execution. The default address is taken from the environment variable ZK_ADDR or if not present "localhost:2181" will be used. | --zookeeper 192.168.1.10:4242                                                                                                                                                                    |
| --kafka <kafka_address>                                            | Sets the Kafka address to the specified one for this execution. The default address is taken from the environment variable KAFKA_ADDR or if not present "localhost:9092" will be used.  | --kafka 192.168.1.10:9161                                                                                                                                                                        |
| --query \<query> <br/> -q \<query>                                 | Executes query using  [Flink SQL](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html) and writes the result to the console.                                 | --query "SELECT title FROM pypi_releases_min"                                                                                                                                                    |
|                                                                    |                                                                                                                                                                                         |                                                                                                                                                                                                  |
| <b>Options for --query</b>                                         |                                                                                                                                                                                         |                                                                                                                                                                                                  |
| -q \<query> --port \<port> <br/> -q \<query> -p \<port>            | Executes query and writes result to local socket on the specified port.                                                                                                                 | -q "SELECT crate.id FROM crate_releases_min" -p 1234                                                                                                                                             |
| -q \<query> --kafka-topic \<name> <br/> -q \<query> -k \<name>     | Executes query and writes result to the specified Kafka topic.                                                                                                                          | -q "SELECT crate.id FROM crate_releases_min" -k "myTopic"                                                                                                                                        |
| -q \<query> --timeout \<duration> <br/> -q \<query> -t \<duration> | Executes query, terminates the program once there have been no new records for the specified duration (in seconds). TO BE DISCUSSED                                                     | -q "SELECT crate.id FROM crate_releases_min" -t 42                                                                                                                                               |
| -q \<query> --from-earliest                                        | Executes query and specify that the query results should be printed starting from the earliest retrievals. By default, the query output will be printed staring from earliest.          | -q "SELECT crate.id FROM crate_releases_min" --from-earliest                                                                                                                                     |
| -q \<query> --from-latest                                          | Executes query and specify that the query results should be printed starting from the latest retrievals.                                                                                | -q "SELECT crate.id FROM crate_releases_min" --from-latest                                                                                                                                       |

