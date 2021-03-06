# KafkaQuery

KafkaQuery allows to process JSON data stored in Kafka with the help of [Flink SQL](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/queries.html#operations) in a streaming fashion.


[![asciinema page](docs/UsageExamples/Showcase.gif)](https://asciinema.org/a/388424)


## Quick intro

KafkaQuery offers you to create a schema for JSON data in Kafka, perform queries and output the processed results to the terminal, a local socket or another Kafka topic.

Download the latest KafkaQuery release [here](https://github.com/codefeedr/kafkaquery/releases) and extract its content. 

Create a schema for your topic

    --infer-schema person

Check the schema of the topic

    --schema person

Query on the topic

    --query "SELECT * FROM person"
    
Apply a filter operation and view the results in the terminal

    --query "SELECT name FROM person WHERE age > 17"
    
or stream the processed data into a new topic

    --query "SELECT name FROM person WHERE age > 17" --output kafka:adults

These are some essential features but there are more functionalities. The following sections go over getting started, creating a schema, querying and User-defined functions.


To pause and inspect any of the following examples follow their link to the respective [Asciinema](https://asciinema.org/) page.


## Getting started

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

For any usage of KafkaQuery you need to execute the `kafkaquery` script that can be found in the `bin` folder.

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

For information on the commands check out the `help` option or visit the [wiki](https://github.com/codefeedr/kafkaquery/wiki/Commands).



### Your first query

[![asciinema page](docs/UsageExamples/firstQuery.gif)](https://asciinema.org/a/zkjkCKmCpEFV3AjxksHZRGNFf)
(Click on the gif to access the recording and copy paste the commands)

## Creating a schema for your topic

An [Avro Schema](https://avro.apache.org/docs/current/spec.html) is needed for a topic to perform queries on it. KafkaQuery offers to either let your schema be generated or manually insert your own schema.

### Inferring a schema

KafkaQuery can generate a schema for your topic based on the latest message:
 
`--infer-schema <topic_name>`

If the program terminates with `Successfully generated schema for topic <topic_name>` you can start querying that topic.<br><br>



*While inferring, KafkaQuery might ask for your input to decide whether a construct should be considered a JSON object or map.

*Any numeric value will be mapped to type `long`

<details>
<summary>An example:</summary><br>


A schema for topic `color` is needed.

The latest message in the topic is: 
```
{
  "name":"Peter",
  "favoriteColors":{
                      "color1":"Red",
                      "color2":"Blue",
                      "color3":"Black"
                    }
}
```
Run the program with the following option:
<details>
<summary><code>--infer-schema color</code></summary>

```
Should this be a map (m) or an object (o)?
{
  "color1" : "Red",
  "color2" : "Blue",
  "color3" : "Black"
}
Please insert one of the following characters: m, o
```

`m`

`Successfully generated schema for topic color`

</details>

In this case, using a map (m) is appropriate.

Verify the schema with the following option:

<details><summary><code>--schema color</code></summary>

```
{
  "type" : "record",
  "name" : "color",
  "namespace" : "infer",
  "fields" : [ {
    "name" : "name",
    "type" : "string"
  }, {
    "name" : "favoriteColors",
    "type" : {
      "type" : "map",
      "values" : "string"
    }
  } ]
}
```

</details>

The topic `color` can now be queried on and has the fields `name` and `favoriteColors`.






</details>



### Manual schema insertion

Sometimes inferring a schema does not yield the expected result. 

For that case, it is possible to manually insert a schema: 

`--update-schema:<topic_name>=<avro_Schema_file>`

[Flink's data type mapping](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/formats/avro.html#data-type-mapping) is quite helpful when deciding for data types for your own schema.

<details><summary>An example:</summary><br>

Renaming a field of topic `person`.

<details><summary>Current schema</summary>

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

Create a file containing the updated schema:

<details><summary>schema.txt</summary>

```diff
{
  "type" : "record",
  "name" : "person",
  "namespace" : "infer",
  "fields" : [ {
+   "name" : "surname",
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

Run the program with the following option:

`--update-schema:person=path/to/schema.txt`

The schema for topic `person` is updated now.

</details>

## Querying 

To perform any query make use of the `--query <queryText>` command.

Details on how to specify the actual query text can be found [here (Flink SQL).](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/queries.html#operations)

#### For querying the following options are available:

***

`-t, --timeout <seconds>`  - Terminates the program once no new message has arrived for the specified duration.

##### Start strategy:

`--start earliest`  - messages are processed from the earliest available offset (default).

`--start latest`  - messages are processed from the latest available offset.
<br><br>

##### Output destinations:

`--output kafka:<topic_name>`  - Writes results to the specified Kafka topic.

`--output socket:<port>`  - Writes results to a local socket on the specified port.

*When not specified, results are printed to the terminal by default.
  
  ***


#### Example Usage: Count license occurrences hourly using [tumbling windows](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#tumbling-windows) with a timeout of 5s
[![asciinema page](docs/UsageExamples/UsageExample1.gif)](https://asciinema.org/a/R21SCIQS9MG79Nmfs3dUmlpw1)

 <!-- Add another example usage, outputting to kafka topic, then consuming again from that topic -->

## User defined functions

KafkaQuery allows making use of [Flink's User-defined Functions](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/functions/udfs.html#user-defined-functions), short UDFs. 

* Consider [Flink's type mapping for UDFs](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/formats/avro.html#data-type-mapping)
* As described in the [Flink documentation](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/functions/udfs.html#us) for function inputs and outputs make use of Java's wrapper types instead of primitives (e.g. Long instead of long)


#### Example usage of UDF's

Create your function according to [the documentation](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/functions/udfs.html#user-defined-functions):

`FeetToCm.java`

```java
import org.apache.flink.table.functions.ScalarFunction;

public class FeetToCm extends ScalarFunction {

    /**
     * Evaluates the argument height. Converts heights ending with 'ft' to centimeters.
     * @param height
     * @return height in centimeters
     */
    public String eval(String height) {
        if(height.endsWith("ft")) {
            return (Double.parseDouble(height.substring(0, height.length()-2)) * 30.48) + "cm";
        }
        return height;
    }
}
```

[![asciinema page](docs/UsageExamples/udf.gif)](https://asciinema.org/a/388476)
(Click on the gif to access the recording and copy paste the commands)


#### Usage of external libraries in UDFs

If a UDF makes use of external libraries make sure to add jars containing all necessary dependencies in the `udf_dependencies` folder.


## Authors

Developed by [Abele Mălan](https://github.com/AbeleMM), [Jakub Nguyen](https://github.com/jakub014), Daniel van den Akker, Christiaan Botha and Ayush Patandin.

