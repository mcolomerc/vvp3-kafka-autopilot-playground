---
name: producer-architecture
description: Kafka producer design — Spring Boot 3.2, Guava RateLimiter, PeakSimulator REST API
metadata:
  type: project
---

Module: kafka-producer (kafka-producer/pom.xml)
Main class: com.ververica.showcase.producer.ProducerApplication

Key design decisions:
- Plain kafka-clients (not spring-kafka) for fine-grained producer config control
- KafkaProducer built manually in KafkaProducerService constructor (not Spring-managed bean) so shutdown sequencing is explicit via @PreDestroy
- MessageCounters is a separate @Service static inner class to break circular dependency: PeakSimulator needs counters but must not depend on KafkaProducerService (which depends on ThroughputController which PeakSimulator also owns)
- Guava RateLimiter.setRate() is thread-safe — scheduler thread can update rate while producer thread calls acquire() concurrently
- PeakSimulator uses a single-threaded ScheduledExecutorService with 1-second ticks for linear ramp
- ApplicationRunner (not CommandLineRunner) used so produce loop starts after full context initialization — actuator /health is live before messages flow

Producer config:
- acks=1 (leader only — throughput demo, not financial data)
- linger.ms=5, batch.size=65536, compression=lz4 (throughput optimization)
- max.in.flight=5 (no per-key ordering needed)

REST API base path: /api
- GET  /api/status
- POST /api/config
- POST /api/peak/start  (409 if already active)
- POST /api/peak/stop

Peak lifecycle: ramp-up → sustain → auto ramp-down (stopTask scheduled after peakDurationSeconds)

K8s: k8s/producer/{configmap,deployment,service}.yaml
- imagePullPolicy: Never (local testing)
- readinessProbe + livenessProbe on /actuator/health:8080
