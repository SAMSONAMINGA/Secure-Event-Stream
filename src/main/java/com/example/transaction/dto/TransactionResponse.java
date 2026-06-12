package com.example.transaction.dto;

import com.example.transaction.entity.Transaction;
import com.example.transaction.entity.Transaction.TransactionStatus;
import com.example.transaction.entity.Transaction.TransactionType;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Outbound DTO — never exposes internal entity fields directly.
 */
@Data
@Builder
public class TransactionResponse {

    private UUID id;
    private String accountId;
    private BigDecimal amount;
    private String currency;
    private TransactionType type;
    private TransactionStatus status;
    private String description;
    private String merchantId;
    private String referenceId;
    private boolean flaggedForReview;
    private String fraudReason;
    private Instant createdAt;
    private Instant updatedAt;

    public static TransactionResponse from(Transaction t) {
        return TransactionResponse.builder()
                .id(t.getId())
                .accountId(t.getAccountId())
                .amount(t.getAmount())
                .currency(t.getCurrency())
                .type(t.getType())
                .status(t.getStatus())
                .description(t.getDescription())
                .merchantId(t.getMerchantId())
                .referenceId(t.getReferenceId())
                .flaggedForReview(t.isFlaggedForReview())
                .fraudReason(t.getFraudReason())
                .createdAt(t.getCreatedAt())
                .updatedAt(t.getUpdatedAt())
                .build();
    }
}
