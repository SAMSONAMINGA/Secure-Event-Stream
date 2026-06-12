package com.example.transaction.dto;

import com.example.transaction.entity.Transaction.TransactionType;
import com.example.transaction.validator.ValidAmount;
import com.example.transaction.validator.ValidCurrency;
import jakarta.validation.constraints.*;
import lombok.Data;

import java.math.BigDecimal;

/**
 * Inbound DTO for creating a transaction.
 * All fields are validated before reaching the service layer.
 */
@Data
public class TransactionRequest {

    @NotBlank(message = "Account ID must not be blank")
    @Size(min = 3, max = 64, message = "Account ID must be between 3 and 64 characters")
    @Pattern(regexp = "^[A-Za-z0-9\\-_]+$", message = "Account ID must contain only alphanumeric characters, hyphens, or underscores")
    private String accountId;

    @NotNull(message = "Amount is required")
    @DecimalMin(value = "0.01", message = "Amount must be greater than 0")
    @DecimalMax(value = "10000000.00", message = "Amount exceeds maximum allowed value")
    @ValidAmount
    private BigDecimal amount;

    @NotBlank(message = "Currency is required")
    @ValidCurrency
    private String currency;

    @NotNull(message = "Transaction type is required")
    private TransactionType type;

    @Size(max = 500, message = "Description must not exceed 500 characters")
    private String description;

    @Size(max = 64, message = "Merchant ID must not exceed 64 characters")
    @Pattern(regexp = "^[A-Za-z0-9\\-_]*$", message = "Merchant ID must contain only alphanumeric characters, hyphens, or underscores")
    private String merchantId;

    @Size(max = 128, message = "Reference ID must not exceed 128 characters")
    private String referenceId;
}
