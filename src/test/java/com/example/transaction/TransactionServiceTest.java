package com.example.transaction;

import com.example.transaction.dto.TransactionRequest;
import com.example.transaction.entity.Transaction;
import com.example.transaction.entity.Transaction.TransactionStatus;
import com.example.transaction.entity.Transaction.TransactionType;
import com.example.transaction.exception.FraudDetectedException;
import com.example.transaction.fraud.FraudDetectionService;
import com.example.transaction.fraud.FraudDetectionService.FraudCheckResult;
import com.example.transaction.kafka.TransactionProducer;
import com.example.transaction.repository.TransactionRepository;
import com.example.transaction.service.TransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransactionServiceTest {

    @Mock
    private TransactionRepository transactionRepository;

    @Mock
    private FraudDetectionService fraudDetectionService;

    @Mock
    private TransactionProducer transactionProducer;

    @InjectMocks
    private TransactionService transactionService;

    private TransactionRequest validRequest;

    @BeforeEach
    void setUp() {
        validRequest = new TransactionRequest();
        validRequest.setAccountId("ACC-001");
        validRequest.setAmount(new BigDecimal("500.00"));
        validRequest.setCurrency("USD");
        validRequest.setType(TransactionType.DEBIT);
        validRequest.setDescription("Test payment");
    }

    @Test
    void create_shouldPersistAndPublish_whenClean() {
        Transaction saved = buildTransaction(UUID.randomUUID(), TransactionStatus.PENDING);
        when(fraudDetectionService.evaluate(any())).thenReturn(FraudCheckResult.clean());
        when(transactionRepository.save(any())).thenReturn(saved);
        when(transactionProducer.sendTransaction(any())).thenReturn(CompletableFuture.completedFuture(null));

        var response = transactionService.create(validRequest);

        assertThat(response).isNotNull();
        assertThat(response.getAccountId()).isEqualTo("ACC-001");
        verify(transactionRepository, times(1)).save(any());
        verify(transactionProducer, times(1)).sendTransaction(any());
    }

    @Test
    void create_shouldThrowFraudDetectedException_whenFlagged() {
        when(fraudDetectionService.evaluate(any()))
                .thenReturn(FraudCheckResult.flagged("Amount exceeds threshold"));

        assertThatThrownBy(() -> transactionService.create(validRequest))
                .isInstanceOf(FraudDetectedException.class)
                .hasMessageContaining("Amount exceeds threshold");

        verify(transactionRepository, never()).save(any());
        verify(transactionProducer, never()).sendTransaction(any());
    }

    @Test
    void findById_shouldReturnResponse_whenExists() {
        UUID id = UUID.randomUUID();
        when(transactionRepository.findById(id))
                .thenReturn(Optional.of(buildTransaction(id, TransactionStatus.COMPLETED)));

        var response = transactionService.findById(id);

        assertThat(response.getId()).isEqualTo(id);
        assertThat(response.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
    }

    @Test
    void findById_shouldThrow_whenNotFound() {
        UUID id = UUID.randomUUID();
        when(transactionRepository.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> transactionService.findById(id))
                .hasMessageContaining(id.toString());
    }

    private Transaction buildTransaction(UUID id, TransactionStatus status) {
        return Transaction.builder()
                .id(id)
                .accountId("ACC-001")
                .amount(new BigDecimal("500.00"))
                .currency("USD")
                .type(TransactionType.DEBIT)
                .status(status)
                .flaggedForReview(false)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .version(0L)
                .build();
    }
}
