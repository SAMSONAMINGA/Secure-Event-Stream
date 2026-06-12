package com.example.transaction.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Dead Letter Topic (DLT) consumer.
 *
 * When the main consumer fails after all retries, Spring Kafka's
 * DeadLetterPublishingRecoverer (configured in KafkaConfig) routes
 * the raw message to a topic named:
 *
 *   <original-topic>.DLT
 *
 * This handler logs the failed event and exposes a hook for
 * alerting, manual replay, or storage in an incident table.
 *
 * Production note: in a real system, persist DLT records to a
 * `failed_events` table so operations teams can replay or audit them.
 */
@Component
@Slf4j
public class DeadLetterQueueHandler {

    @KafkaListener(
            topics = "${kafka.topic.transactions:transactions}.DLT",
            groupId = "${spring.kafka.consumer.group-id:transaction-processors}-dlt"
    )
    public void handleDeadLetter(
            String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = "kafka_dlt-exception-message", required = false) String exceptionMessage) {

        log.error(
                "DLT message received: topic={} partition={} offset={} cause='{}' payload={}",
                topic, partition, offset,
                exceptionMessage != null ? exceptionMessage : "unknown",
                sanitize(payload)
        );

        // TODO (production): persist to failed_events table, trigger alert, send to operations dashboard
    }

    /**
     * Truncate long payloads in log output to prevent log flooding
     * and avoid inadvertent PII exposure in log aggregation systems.
     */
    private String sanitize(String payload) {
        if (payload == null) return "<null>";
        return payload.length() > 500 ? payload.substring(0, 500) + "…[truncated]" : payload;
    }
}
