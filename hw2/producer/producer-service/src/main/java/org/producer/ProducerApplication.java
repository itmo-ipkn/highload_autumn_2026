package org.producer;

import org.producer.properties.KafkaProperty;
import org.producer.properties.UserProperty;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({KafkaProperty.class, UserProperty.class})
public class ProducerApplication {
    static void main() {
        SpringApplication.run(ProducerApplication.class);
    }
}
