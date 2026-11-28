package org.highload;

import org.highload.propeties.KafkaProperty;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(KafkaProperty.class)
public class ConsumerApplication {
    static void main() {
        SpringApplication.run(ConsumerApplication.class);
    }
}
