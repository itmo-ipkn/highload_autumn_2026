package org.highload.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "events")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    @Id
    private UUID eventId;

    @Column(nullable = false)
    private Long userId;

    @Column(nullable = false)
    private Long seq;

    @Column(nullable = false)
    private Integer podId;

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false)
    private Integer partitionId;

    @Column(nullable = false)
    private Long offset;

    @Column(nullable = false)
    private Instant producedAt;

    @Column(nullable = false)
    private String type;
}
