package com.example.transaction.repository;

import com.example.transaction.entity.Transaction;
import com.example.transaction.entity.Transaction.TransactionStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Spring Data JPA repository.
 * All queries use prepared statements under the hood — SQL injection is not possible.
 */
@Repository
public interface TransactionRepository extends JpaRepository<Transaction, UUID> {

    Page<Transaction> findByAccountId(String accountId, Pageable pageable);

    Page<Transaction> findByStatus(TransactionStatus status, Pageable pageable);

    Page<Transaction> findByAccountIdAndStatus(String accountId, TransactionStatus status, Pageable pageable);

    List<Transaction> findByFlaggedForReviewTrue();

    @Query("SELECT t FROM Transaction t WHERE t.accountId = :accountId " +
           "AND t.createdAt >= :since AND t.amount >= :threshold")
    List<Transaction> findHighValueByAccountSince(
            @Param("accountId") String accountId,
            @Param("since") Instant since,
            @Param("threshold") BigDecimal threshold);

    @Query("SELECT COUNT(t) FROM Transaction t WHERE t.accountId = :accountId " +
           "AND t.createdAt >= :windowStart")
    long countTransactionsByAccountSince(
            @Param("accountId") String accountId,
            @Param("windowStart") Instant windowStart);

    @Query("SELECT COALESCE(SUM(t.amount), 0) FROM Transaction t WHERE t.accountId = :accountId " +
           "AND t.createdAt >= :windowStart AND t.status != 'FAILED'")
    BigDecimal sumAmountByAccountSince(
            @Param("accountId") String accountId,
            @Param("windowStart") Instant windowStart);
}
