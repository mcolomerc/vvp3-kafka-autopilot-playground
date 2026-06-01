---
name: consumer-architecture
description: Kafka consumer design — plain Java 11 main class, structured logback logging, auto-commit sink
metadata:
  type: project
---

Module: kafka-consumer (kafka-consumer/pom.xml)
Main class: com.ververica.showcase.consumer.MetricsConsumer (set in maven-shade-plugin)

Key design decisions:
- No Spring — plain Java main class with JVM shutdown hook
- Shutdown via consumer.wakeup() → WakeupException caught → consumer.close() cleanly
- Auto-commit enabled (at-most-once): this is a logging-only sink; window loss is acceptable
- auto.offset.reset=earliest: replay all available windows on startup
- Running total counter (AtomicLong) + 60-second summary log for operator visibility
- @JsonIgnoreProperties(ignoreUnknown=true) on OrderMetrics for forward compatibility

Consumer config:
- group.id: metrics-logger (from env KAFKA_CONSUMER_GROUP)
- enable.auto.commit=true, auto.commit.interval.ms=5000
- max.poll.records=500
- poll timeout: Duration.ofMillis(1000)

Log format per record:
  [window={start}-{end}] category={} region={} orders={} revenue={:.2f} avg={:.2f} customers={}

Summary log every 60s:
  [SUMMARY] Total windows processed: {} | Last window: {}

Fat JAR built by maven-shade-plugin (not Spring Boot repackage)
Logback config: kafka-consumer/src/main/resources/logback.xml

K8s: k8s/consumer/{configmap,deployment}.yaml
- imagePullPolicy: Never
- No readiness probe (background consumer)
- terminationGracePeriodSeconds=15
