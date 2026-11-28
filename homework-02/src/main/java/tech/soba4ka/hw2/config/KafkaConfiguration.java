package tech.soba4ka.hw2.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KafkaConfiguration {
    public static KafkaProducer<String,String> producer(boolean roundRobin) {
        var p = new Properties();
        p.put("bootstrap.servers", System.getenv("BOOTSTRAP"));
        p.put("acks", "all");
        p.put("enable.idempotence", true);
        if (roundRobin) p.put("partitioner.class", RoundRobinPartitioner.class);
        return new KafkaProducer<>(p, new StringSerializer(), new StringSerializer());
    }

    public static Properties readerProperties() {
        var p = new Properties();
        p.put("bootstrap.servers", System.getenv("BOOTSTRAP"));
        p.put("enable.auto.commit", false);
        p.put("auto.offset.reset", "earliest");
        return p;
    }

    public static KafkaConsumer<String,String> reader(Properties properties) {
        return new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
    }
}
