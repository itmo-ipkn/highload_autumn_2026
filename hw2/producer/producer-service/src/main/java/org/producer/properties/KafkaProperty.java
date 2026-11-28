package org.producer.properties;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Setter
@Getter
@Validated
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaProperty {
    @NotBlank
    private String topic;
    @NotNull
    @Positive
    private Integer rate;
    @NotBlank
    @Pattern(regexp = "user_id|none")
    private String keyMode;
}
