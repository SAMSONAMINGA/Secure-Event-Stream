package com.example.transaction.kafka;

import com.example.transaction.entity.Transaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes transaction events to Kafka.
 *
 * Resilience4j @Retry wraps the send call: up to 3 attempts with
 * exponential back-off are made before the event is considered undeliverable.
 * The fallback logs the failure so it can be investigated.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic.transactions:transactions}")
    private String transactionsTopic;

    /**
     * Sends a transaction event.  The message key is the accountId so that
     * all events for the same account land on the same partition (ordered delivery).
     *
     * Resilience4j retries this method up to 3 times on failure.
     * The retry instance is configured in application.yml under resilience4j.retry.
     */
    @Retry(name = "kafkaProducer", fallbackMethod = "producerFallback")
    public CompletableFuture<SendResult<String, String>> sendTransaction(Transaction transaction) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(TransactionEvent.from(transaction));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize transaction " + transaction.getId(), e);
        }

        log.info("Publishing transaction event: id={} accountId={} topic={}",
                transaction.getId(), transaction.getAccountId(), transactionsTopic);

        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(transactionsTopic, transaction.getAccountId(), payload);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Transaction event delivered: id={} partition={} offset={}",
                        transaction.getId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Delivery failed for transaction id={}: {}", transaction.getId(), ex.getMessage());
            }
        });

        return future;
    }

    /**
     * Fallback invoked after all retries are exhausted.
     * In production you would persist to an outbox or alert on-call.
     */
    public CompletableFuture<SendResult<String, String>> producerFallback(
            Transaction transaction, Throwable t) {
        log.error("All Kafka retries exhausted for transaction id={}: {}",
                transaction.getId(), t.getMessage());
        return CompletableFuture.failedFuture(t);
    }
}
