package com.example.transaction.service;

import com.example.transaction.dto.TransactionRequest;
import com.example.transaction.dto.TransactionResponse;
import com.example.transaction.entity.Transaction;
import com.example.transaction.entity.Transaction.TransactionStatus;
import com.example.transaction.exception.FraudDetectedException;
import com.example.transaction.exception.TransactionNotFoundException;
import com.example.transaction.fraud.FraudDetectionService;
import com.example.transaction.kafka.TransactionProducer;
import com.example.transaction.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Core application service.  Orchestrates:
 *   1. Validation (delegated to the request DTO via @Valid upstream)
 *   2. Fraud evaluation
 *   3. Persistence
 *   4. Kafka event publication
 *
 * All DB mutations are wrapped in a transaction.  The Kafka send is
 * issued AFTER the DB commit so the event always has a corresponding
 * persisted record when the consumer processes it.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final FraudDetectionService fraudDetectionService;
    private final TransactionProducer transactionProducer;

    /**
     * Creates and persists a new transaction, then publishes it to Kafka.
     *
     * @throws FraudDetectedException if a fraud rule fires during pre-screening
     */
    @Transactional
    public TransactionResponse create(TransactionRequest request) {
        Transaction transaction = Transaction.builder()
                .accountId(request.getAccountId())
                .amount(request.getAmount())
                .currency(request.getCurrency().toUpperCase())
                .type(request.getType())
                .status(TransactionStatus.PENDING)
                .description(request.getDescription())
                .merchantId(request.getMerchantId())
                .referenceId(request.getReferenceId())
                .flaggedForReview(false)
                .build();

        // Pre-screen fraud before persisting
        FraudDetectionService.FraudCheckResult fraudResult = fraudDetectionService.evaluate(transaction);
        if (fraudResult.flagged()) {
            log.warn("Transaction rejected by fraud engine: accountId={} reason={}",
                    request.getAccountId(), fraudResult.reason());
            throw new FraudDetectedException(fraudResult.reason());
        }

        Transaction saved = transactionRepository.save(transaction);

        // Publish event after DB commit — Kafka send is best-effort here;
        // Resilience4j retry in the producer handles transient broker failures.
        transactionProducer.sendTransaction(saved);

        log.info("Transaction created: id={} accountId={} amount={} currency={}",
                saved.getId(), saved.getAccountId(), saved.getAmount(), saved.getCurrency());

        return TransactionResponse.from(saved);
    }

    @Transactional(readOnly = true)
    public TransactionResponse findById(UUID id) {
        return transactionRepository.findById(id)
                .map(TransactionResponse::from)
                .orElseThrow(() -> new TransactionNotFoundException(id));
    }

    @Transactional(readOnly = true)
    public Page<TransactionResponse> findAll(Pageable pageable) {
        return transactionRepository.findAll(pageable).map(TransactionResponse::from);
    }

    @Transactional(readOnly = true)
    public Page<TransactionResponse> findByAccount(String accountId, Pageable pageable) {
        return transactionRepository.findByAccountId(accountId, pageable).map(TransactionResponse::from);
    }

    @Transactional(readOnly = true)
    public Page<TransactionResponse> findByStatus(TransactionStatus status, Pageable pageable) {
        return transactionRepository.findByStatus(status, pageable).map(TransactionResponse::from);
    }
}
