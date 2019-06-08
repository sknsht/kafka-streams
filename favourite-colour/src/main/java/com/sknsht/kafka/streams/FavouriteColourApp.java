package com.sknsht.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColourApp {

    private static List<String> colours = Arrays.asList("green", "blue", "red");

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Disable the cache to demonstrate all the steps involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> input = builder.stream("fav-colour-input");

        // Input data: name, colour
        String delimiter = ",";
        KStream<String, String> usersAndColours = input
                .filter((key, value) -> value.contains(delimiter))
                .mapValues(value -> value.toLowerCase())
                .selectKey((key, value) -> value.split(delimiter)[0])
                .mapValues(value -> value.split(delimiter)[1].trim())
                .filter((user, colour) -> colours.contains(colour));

        usersAndColours.to("user-keys-and-colours", Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> usersAndColoursTable = builder.table("user-keys-and-colours");

        // Count the final occurrences of colours
        KTable<String, Long> favouriteColours = usersAndColoursTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        favouriteColours.toStream().to("fav-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // Shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}