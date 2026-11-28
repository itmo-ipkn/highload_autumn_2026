import fs from "fs/promises";
import path from "path";

export interface DeliverableFile {
  path: string;
  title: string;
  category: "Infrastructure" | "Spring Boot Java" | "Verification & Report";
  language: string;
  description: string;
  content: string;
}

const FILE_MANIFEST: Omit<DeliverableFile, "content">[] = [
  {
    path: "docker-compose.yaml",
    title: "docker-compose.yaml",
    category: "Infrastructure",
    language: "yaml",
    description:
      "3-broker Kafka KRaft cluster (apache/kafka:3.8.0), kafka-init topic creator, PostgreSQL 16 sink, 3 Spring Boot Producer pods, and 3 Spring Boot Consumer pods.",
  },
  {
    path: "spring-kafka-service/Dockerfile",
    title: "Dockerfile (Spring Boot)",
    category: "Infrastructure",
    language: "dockerfile",
    description:
      "Multi-stage Maven + Eclipse Temurin Java 21 build with exec entrypoint for SIGTERM graceful buffer flushing.",
  },
  {
    path: "spring-kafka-service/pom.xml",
    title: "pom.xml",
    category: "Infrastructure",
    language: "xml",
    description:
      "Spring Boot 3.3.5 + Spring Kafka + Spring JDBC + PostgreSQL + Jackson JSR310 Maven configuration.",
  },
  {
    path: "spring-kafka-service/src/main/resources/application.yml",
    title: "application.yml",
    category: "Infrastructure",
    language: "yaml",
    description:
      "Configures acks=all, enable.idempotence=true, group-id=events-processor, enable-auto-commit=false, auto-offset-reset=earliest, and graceful shutdown.",
  },
  {
    path: "spring-kafka-service/src/main/resources/schema.sql",
    title: "schema.sql",
    category: "Infrastructure",
    language: "sql",
    description:
      "PostgreSQL sink schema with UNIQUE(event_id) for idempotent deduplication and dedup_stats counter.",
  },
  {
    path: "spring-kafka-service/src/main/java/ru/itmo/ddia/kafka/model/EventMessage.java",
    title: "EventMessage.java",
    category: "Spring Boot Java",
    language: "java",
    description:
      "Java 21 record defining event_id (UUID), user_id, type (page_view|add_to_cart|purchase), seq, produced_at, and payload (with ~2% poison pill flag).",
  },
  {
    path: "spring-kafka-service/src/main/java/ru/itmo/ddia/kafka/config/KafkaProducerConfig.java",
    title: "KafkaProducerConfig.java",
    category: "Spring Boot Java",
    language: "java",
    description:
      "Spring Kafka ProducerFactory with acks=all, enable.idempotence=true, retries=MAX_VALUE, and max.in.flight.requests.per.connection=5.",
  },
  {
    path: "spring-kafka-service/src/main/java/ru/itmo/ddia/kafka/producer/EventProducerService.java",
    title: "EventProducerService.java",
    category: "Spring Boot Java",
    language: "java",
    description:
      "Producer pod service: per-pod disjoint user_id ownership, strictly monotonic seq, KEY_MODE=user_id|none, CompletableFuture error logging, and @PreDestroy SIGTERM flush().",
  },
  {
    path: "spring-kafka-service/src/main/java/ru/itmo/ddia/kafka/config/KafkaConsumerConfig.java",
    title: "KafkaConsumerConfig.java",
    category: "Spring Boot Java",
    language: "java",
    description:
      "Manual offset commit (AckMode.MANUAL_IMMEDIATE), ConsumerAwareRebalanceListener committing processed offsets before partition revocation, and 3-attempt DefaultErrorHandler + DeadLetterPublishingRecoverer.",
  },
  {
    path: "spring-kafka-service/src/main/java/ru/itmo/ddia/kafka/consumer/EventConsumerService.java",
    title: "EventConsumerService.java",
    category: "Spring Boot Java",
    language: "java",
    description:
      "@KafkaListener for group events-processor: emulates PROCESSING_DELAY_MS, throws PoisonEventException on poison pills, persists idempotently to sink, and commits offset manually.",
  },
  {
    path: "spring-kafka-service/src/main/java/ru/itmo/ddia/kafka/sink/PostgresSinkRepository.java",
    title: "PostgresSinkRepository.java",
    category: "Spring Boot Java",
    language: "java",
    description:
      "Transactional JDBC repository performing INSERT INTO sink_events ... ON CONFLICT (event_id) DO NOTHING and incrementing dedup_stats on duplicate delivery.",
  },
  {
    path: "verify",
    title: "verify (CLI Script)",
    category: "Verification & Report",
    language: "bash",
    description:
      "Executable CLI verification script: prints GitHub login + timestamp on line 1, checks per-user seq completeness across sink + events.dlt, counts rejected duplicates, and counts out-of-order writes.",
  },
  {
    path: "README.md",
    title: "README.md (Отчет по экспериментам)",
    category: "Verification & Report",
    language: "markdown",
    description:
      "Complete Russian report with hypotheses, verify CLI outputs, and analysis for Experiments 1 (kill -9 consumer), 2 (stop 1 & 2 brokers), and 3 (KEY_MODE=none vs user_id).",
  },
];

export async function getAllDeliverableFiles(): Promise<DeliverableFile[]> {
  const root = process.cwd();
  const results: DeliverableFile[] = [];

  for (const item of FILE_MANIFEST) {
    try {
      const fullPath = path.join(/*turbopackIgnore: true*/ root, item.path);
      const content = await fs.readFile(fullPath, "utf-8");
      results.push({ ...item, content });
    } catch {
      results.push({
        ...item,
        content: `// File ${item.path} not found`,
      });
    }
  }

  return results;
}
