package org.highload.repositories;

import org.highload.entities.ConsumerMetrics;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ConsumerMetricsRepository extends JpaRepository<ConsumerMetrics, Integer> {

    @Modifying
    @Query(value = """
            INSERT INTO consumer_metrics (
                pod_id,
                duplicate_count
            )
            VALUES (:podId, 1)
            ON CONFLICT (pod_id)
            DO UPDATE SET duplicate_count =
                consumer_metrics.duplicate_count + 1
            """, nativeQuery = true)
    int incrementDuplicates(@Param("podId") Integer podId);

}
