package com.example.transaction.kafka;

import com.example.transaction.entity.Transaction;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Kafka message payload — a safe, serializable snapshot.
 * Never embed the JPA entity directly in Kafka messages.
 */
@Data
@Builder
public class TransactionEvent {

    private UUID id;
    private String accountId;
    private BigDecimal amount;
    private String currency;
    private String type;
    private String status;
    private String merchantId;
    private String referenceId;
    private Instant createdAt;
    private boolean flaggedForReview;
    private String fraudReason;

    public static TransactionEvent from(Transaction t) {
        return TransactionEvent.builder()
                .id(t.getId())
                .accountId(t.getAccountId())
                .amount(t.getAmount())
                .currency(t.getCurrency())
                .type(t.getType().name())
                .status(t.getStatus().name())
                .merchantId(t.getMerchantId())
                .referenceId(t.getReferenceId())
                .createdAt(t.getCreatedAt())
                .flaggedForReview(t.isFlaggedForReview())
                .fraudReason(t.getFraudReason())
                .build();
    }
}
