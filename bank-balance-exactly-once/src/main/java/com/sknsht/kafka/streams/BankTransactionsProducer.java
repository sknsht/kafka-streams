package com.sknsht.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

public class BankTransactionsProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // Strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // Idempotent producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Ensure we don't push duplicates

        Producer<String, String> producer = new KafkaProducer<>(properties);

        List<String> users = Arrays.asList("john", "stephane", "alice");
        Stream.iterate(0, i -> i + 1).forEach(i -> {
            System.out.println("Producing batch: " + i);
            users.forEach(user -> producer.send(generateTransaction(user)));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                producer.close();
                return;
            }
        });
    }

    public static ProducerRecord<String, String> generateTransaction(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        int amount = ThreadLocalRandom.current().nextInt(-100, 100);
        String time = Instant.now().toString();

        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", time);

        return new ProducerRecord<>("bank-transactions", name, transaction.toString());
    }
}
