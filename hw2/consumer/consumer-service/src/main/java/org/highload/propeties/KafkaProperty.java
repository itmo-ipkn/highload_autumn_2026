package org.highload.propeties;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaProperty {

    @NotNull
    private Integer podId;

    @NotNull
    private Long processingDelayMs;

    @NotBlank
    private String topic;

}
