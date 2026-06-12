package com.example.transaction.kafka;

import com.example.transaction.entity.Transaction;
import com.example.transaction.entity.Transaction.TransactionStatus;
import com.example.transaction.fraud.FraudDetectionService;
import com.example.transaction.repository.TransactionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Consumes transaction events from Kafka.
 *
 * Acknowledgment mode is MANUAL_IMMEDIATE so the offset is only committed
 * after the event has been fully processed and persisted.  On failure the
 * message is NOT acknowledged, causing it to be retried or routed to the
 * Dead Letter Topic by the error handler defined in KafkaConfig.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionConsumer {

    private final TransactionRepository transactionRepository;
    private final FraudDetectionService fraudDetectionService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${kafka.topic.transactions:transactions}",
            groupId = "${spring.kafka.consumer.group-id:transaction-processors}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consume(
            String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("Received transaction event: topic={} partition={} offset={}", topic, partition, offset);

        try {
            TransactionEvent event = objectMapper.readValue(payload, TransactionEvent.class);
            processEvent(event);
            ack.acknowledge();
            log.info("Transaction event processed and acknowledged: id={}", event.getId());

        } catch (Exception e) {
            log.error("Failed to process transaction event at offset={}: {}", offset, e.getMessage(), e);
            // Do not acknowledge — Spring Kafka error handler sends to DLT
            throw new RuntimeException("Consumer processing failure", e);
        }
    }

    private void processEvent(TransactionEvent event) {
        Transaction transaction = transactionRepository.findById(event.getId())
                .orElseThrow(() -> new IllegalStateException(
                        "Transaction not found in DB for event id=" + event.getId()));

        if (transaction.getStatus() != TransactionStatus.PENDING
                && transaction.getStatus() != TransactionStatus.PROCESSING) {
            log.warn("Skipping already-terminal transaction: id={} status={}", event.getId(), transaction.getStatus());
            return;
        }

        transaction.setStatus(TransactionStatus.PROCESSING);
        transactionRepository.save(transaction);

        // Re-evaluate fraud rules during async processing
        FraudDetectionService.FraudCheckResult fraudResult = fraudDetectionService.evaluate(transaction);

        if (fraudResult.flagged()) {
            transaction.setStatus(TransactionStatus.FLAGGED);
            transaction.setFlaggedForReview(true);
            transaction.setFraudReason(fraudResult.reason());
            log.warn("Transaction FLAGGED during consumer processing: id={} reason={}",
                    event.getId(), fraudResult.reason());
        } else {
            transaction.setStatus(TransactionStatus.COMPLETED);
        }

        transactionRepository.save(transaction);
    }
}
