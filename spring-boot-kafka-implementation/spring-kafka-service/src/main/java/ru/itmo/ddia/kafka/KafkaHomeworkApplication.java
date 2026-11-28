package ru.itmo.ddia.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaHomeworkApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaHomeworkApplication.class, args);
    }
}
