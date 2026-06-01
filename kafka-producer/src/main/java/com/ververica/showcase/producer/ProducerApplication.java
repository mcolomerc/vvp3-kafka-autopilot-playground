package com.ververica.showcase.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot entry point for the Orders Kafka Producer.
 *
 * On startup:
 *   1. Spring context initialises all beans (ThroughputController, PeakSimulator, etc.)
 *   2. Actuator endpoints become available at /actuator/health (used by K8s probes)
 *   3. REST API becomes available at /api/status, /api/config, /api/peak/start, /api/peak/stop
 *   4. KafkaProducerService.run() starts the produce loop via ApplicationRunner
 */
@SpringBootApplication
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }
}
