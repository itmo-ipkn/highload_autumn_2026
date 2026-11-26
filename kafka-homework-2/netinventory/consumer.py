def save_event(connection, message, event, code, consumer_id):
    user_id = event["user_id"]
    with connection.transaction():
        # Keep the recorded write order meaningful even when one user spans several partitions.
        connection.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (user_id,))
        inserted = connection.execute(
            """
            INSERT INTO processed_events (
                event_id, user_id, mode, seq, event_type, device_code, consumer_id, topic, partition, kafka_offset
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (
                event["event_id"], user_id, event["payload"]["key_mode"], event["seq"],
                event["type"], code, consumer_id, message.topic(), message.partition(), message.offset(),
            ),
        ).fetchone()
        if inserted is None:
            connection.execute(
                """
                INSERT INTO duplicate_deliveries (event_id, count) VALUES (%s, 1)
                ON CONFLICT (event_id) DO UPDATE SET count = duplicate_deliveries.count + 1
                """,
                (event["event_id"],),
            )
    return inserted is not None
