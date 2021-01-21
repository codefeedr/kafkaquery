# CodeFeedr KafkaQuery

CodeFeedr KafkaQuery allows to operate on JSON data in a Kafka instance using [Flink SQL](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html).

## Getting Started

Download the latest KafkaQuery release [here](https://github.com/codefeedr/kafkaquery/releases) and extract its content. 

<details> 
<summary>Alternatively build KafkaQuery yourself</summary>
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


Specify Kafka and Zookeeper addresses either in your environment variables or as [arguments](#address)  when executing the launch script.<br/>


| Property          | Default value  |Environment variable name (optional) |
|-------------------|----------------|-------------------------------------|
| Kafka Address     | localhost:9092 |KAFKA_ADDR                           |
| ZooKeeper Address | localhost:2181 |ZK_ADDR                              |

## Usage

For any usage of KafkaQuery the `codefeedr` script needs to be executed. It can be found in the `bin` folder.

In the following examples Kafka and Zookeeper instances are running on the default addresses. <br>
To pause examples and mark & copy commands within them click on the examples to get redirected to the respective [Asciinema](https://asciinema.org/) page.
All available KafkaQuery commands can be found [here](https://github.com/codefeedr/kafkaquery/wiki/Commands).


#### Example 1: Show all available topics & display a schema
[![asciinema page](docs/UsageExamples/showTopics.gif)](https://asciinema.org/a/360404)

#### Example 2: Display all data in the topic pypi_releases_min 
[![asciinema page](docs/UsageExamples/selectAll.gif)](https://asciinema.org/a/360660)

#### Example 3: Aggregate author names for every hour
[![asciinema page](docs/UsageExamples/aggregateAuthors.gif)](https://asciinema.org/a/360672)
