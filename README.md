# Kafka Streams

As a first step to start all applications, you need to start the Zookeeper and the Kafka.

### Step 1: Download the code

[Download](https://kafka.apache.org/downloads "Download") the 2.2.0 release and un-tar it.

### Step 2: Start the Zookeeper server

```shell
> bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

### Step 3: Start the Kafka server

```shell
> bin\windows\kafka-server-start.bat config\server.properties
```

## WordCount

Implementation of WordCount algorithm, which returns a list of all unique words from the input text and their number of occurrences.

### Step 1: Prepare the topics

Create the input topic named **word-count-input** and the output topic named **word-count-output**:
```shell
> bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-input
> bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-output
```
### Step 2: Start the WordCount application

Start the console producer:
```shell
> bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic word-count-input
```
Start the console consumer in a separate terminal:
```shell
> bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic word-count-output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```
Run the WordCount java application.

### Step 3: Process some data

Let's write a message with the console producer into the input topic **word-count-input** and check the output word count which will be written to the **word-count-output** topic and printed by the console consumer:
```shell
> bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic word-count-input
Hello world
This is Kafka Streams app
```

## Favourite Colour

This application counts and displays the colours and the number of people who consider a particular colour as their favorite. It is assumed that people can change their preferences.

### Step 1: Prepare the topics

Create the input topic named **fav-colour-input** and the output topic named **fav-colour-output**:
```shell
> bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic fav-colour-input
> bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic fav-colour-output
```
### Step 2: Start the FavouriteColour application

Start the console producer:
```shell
> bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic fav-colour-input
```
Start the console consumer in a separate terminal:
```shell
> bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic fav-colour-output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```
Run the FavouriteColour java application.

### Step 3: Process some data

As input, the application accepts `name, colour` pairs. Let's write a message with the console producer into the input topic **fav-colour-input** and check the output data which will be written to the **fav-colour-output** topic and printed by the console consumer:
```shell
> bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic fav-colour-input
ann, green
alex, blue
ann, blue
```
