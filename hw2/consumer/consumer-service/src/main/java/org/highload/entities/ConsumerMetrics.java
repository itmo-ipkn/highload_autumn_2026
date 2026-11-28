package org.highload.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

@Entity
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "consumer_metrics")
public class ConsumerMetrics {
    @Id
    @Column(nullable = false)
    private Integer podId;

    @Column(nullable = false)
    private Long duplicateCount;
}
