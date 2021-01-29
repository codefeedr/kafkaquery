# CodeFeedr KafkaQuery

CodeFeedr KafkaQuery allows to process JSON data stored in Kafka with the help of [Flink SQL](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/queries.html#operations).

<!---
SOME NICE VIDEO WITH SOME SOMEWHAT SIMPLE QUERY: "SELECT name from person where age < 80"
-->

## Quick intro

KafkaQuery offers you to create a schema for JSON data in Kafka, perform queries and output the processed results to the terminal, a local socket or another Kafka topic.

Download the latest KafkaQuery release [here](https://github.com/codefeedr/kafkaquery/releases) and extract its content. 

Create a schema for your topic

    --infer-schema person

Check the schema of the topic

    --topic person

Query on the topic

    --query "SELECT * FROM person"
    
Apply a filter operation and view the results in the terminal

    --query "SELECT name FROM person WHERE age > 17"
    
or stream the processed data into a new topic

    --query "SELECT name FROM person WHERE age > 17" --output kafka:adults

These are some essential features but there are more functionalities. The following sections go over getting started, creating a schema, querying and User-defined functions.

### Getting started

The following example will go through setting up and using KafkaQuery. We will consume data from an example topic called `person` which contains messages of the following format  

```json
{	
	"name":"John Smith",
	"age":32,
	"height":"172cm"
}
```

Download the latest KafkaQuery release [here](https://github.com/codefeedr/kafkaquery/releases) and extract its content. 

<details> 
<summary>(Alternatively build KafkaQuery yourself)</summary>
<br>

Clone the project and open it as a sbt project. Run `sbt pack` to create a package folder containing program launch scripts in the following directory: ```target/pack/bin/kafkaquery/bin```


</details>

<details> 
<summary>Optional: Add CodeFeedr to you path</summary>


```
export PATH=$PATH:<path to bin folder>

e.g. export PATH=$PATH:/Users/MyUsername/Documents/kafkaquery/target/pack/bin
```

</details>

For any usage of KafkaQuery you need to execute the `codefeedr` script that can be found in the `bin` folder.

<details>
	<summary>Specify your ZooKeeper and Kafka addresses:</summary>

***

**By either**

<details>
	<summary>Setting environment variables for your ZooKeeper and Kafka addresses:</summary>

<br>

| Property          | Default value  |Environment variable name (optional) |
|-------------------|----------------|-------------------------------------|
| Kafka Address     | localhost:9092 |KAFKA_ADDR                           |
| ZooKeeper Address | localhost:2181 |ZK_ADDR                              |
<br>

</details>

**Or**

<details>
	<summary>Specifying your ZooKeeper and Kafka addresses for every execution:</summary>
<br>

Always append the following options to your command when running the program


`--zookeeper <address> --kafka <address>`

</details>

***

</details>


 <!--Maybe more details on how to execute the script? Maybe add a nice link on how to execute scripts-->

For information on how to use the commands check out the `help` command:

<details>
	<summary><code>./codefeedr --help</code></summary>

```
Codefeedr CLI 1.0.0
Usage: codefeedr [options]

  -q, --query <query>      Allows querying available data sources through Flink SQL. query - valid Flink SQL query. More information about Flink SQL can be found at: https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html.
  -p, --port <port>        Writes the output data of the given query to a socket which gets created with the specified port. Local connection with the host can be done by e.g. netcat.
  -k, --kafka_topic <kafka-topic>
                           Writes the output data of the given query to the specified Kafka topic. If the Kafka topic does not exist, it will be created.
  -t, --timeout <seconds>  Specifies a timeout in seconds. If no message is received for the duration of the timeout the program terminates.
  --from-earliest          Specifies that the data is consumed from the earliest offset.If no state is specified the query results will be printed from EARLIEST.
  --from-latest            Specifies that the data is consumed from the latest offset.
  --topic <topic_name>     Output the specified topic's schema which entails the field names and types.
  --topics                 List all topic names for which a schema is available.
  --schema:<topic_name>=<avro_Schema_file>
                           Updates the schema for the specified topic with the given Avro schema (as a file).
  --infer-schema <topic_name>
                           Infers and registers an Avro schema for the specified topic.
  --kafka <Kafka_address>  Sets the Kafka address for the execution.
  --zookeeper <ZK_address>
                           Sets the ZooKeeper address for the execution.
  --udf <function_file1,function_file2...>
                           Registers the specified User defined functions for usage in queries.
  -h, --help
```
</details>


### Your first query

To perform queries on a topic a corresponding schema is needed!

Infer the schema of the `person` topic (at least one message is necessary)

<details>
	<summary><code>./codefeedr --infer-schema person</code></summary>

`Successfully generated schema for topic person`
</details>

<!-- INFERENCE FAILS CASE EXPLANATION? Maybe make a collapse box with a small example on how when to decide for map-->


Examine the generated schema

<details>
	<summary><code>./codefeedr --topic person</code></summary>

```
{
  "type" : "record",
  "name" : "person",
  "namespace" : "infer",
  "fields" : [ {
    "name" : "name",
    "type" : "string"
  }, {
    "name" : "age",
    "type" : "long"
  }, {
    "name" : "height",
    "type" : "string"
  } ]
}
```
</details>

Perform your first query

<details>
	<summary><code>./codefeedr --query "SELECT name, age FROM person"</code></summary>

```
John Smith,32
Galen Evan,24
Rowen Alexa,16
Celine Lita,26
Paula Bess,15
Noble Leanna,52
Tami Bethany,39
Jessye Joby,41
Ike Marlowe,12
Emmeline Vale,23
```

</details>

Congratulations on your first output! The program is still running and trying to fetch data from the topic. Make sure to stop it (Ctrl+c).


Specify Kafka and Zookeeper addresses either in your environment variables or as [arguments](#address)  when executing the launch script.<br/>


| Property          | Default value  |Environment variable name (optional) |
|-------------------|----------------|-------------------------------------|
| Kafka Address     | localhost:9092 |KAFKA_ADDR                           |
| ZooKeeper Address | localhost:2181 |ZK_ADDR                              |

### Querying 

To pause examples and mark & copy commands within them click on the examples to get redirected to the respective [Asciinema](https://asciinema.org/) page.
All available KafkaQuery commands can be found [here](https://github.com/codefeedr/kafkaquery/wiki/Commands).


#### Example 1: Show all available topics & display a schema
[![asciinema page](docs/UsageExamples/showTopics.gif)](https://asciinema.org/a/360404)

#### Example 2: Display all data in the topic pypi_releases_min 
[![asciinema page](docs/UsageExamples/selectAll.gif)](https://asciinema.org/a/360660)

#### Example 3: Aggregate author names for every hour
[![asciinema page](docs/UsageExamples/aggregateAuthors.gif)](https://asciinema.org/a/360672)
