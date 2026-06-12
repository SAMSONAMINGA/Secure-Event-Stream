package com.example.transaction.fraud;

import com.example.transaction.entity.Transaction;
import com.example.transaction.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stateless fraud-rule engine with O(1) average-case in-memory lookups.
 *
 * Rules applied (short-circuit on first hit):
 *   R1  Single transaction exceeds hard amount threshold
 *   R2  Account velocity: more than N transactions within a rolling window
 *   R3  Account daily volume: cumulative amount exceeds daily cap
 *   R4  Suspicious repeated amounts (exact same value, rapid-fire)
 *
 * ConcurrentHashMap is used for the in-memory "last-seen amount" cache so
 * reads and writes are O(1) with low lock contention under concurrent load.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {

    private final TransactionRepository transactionRepository;

    @Value("${fraud.amount-threshold:10000}")
    private BigDecimal amountThreshold;

    @Value("${fraud.velocity-window-minutes:10}")
    private int velocityWindowMinutes;

    @Value("${fraud.velocity-max-count:10}")
    private int velocityMaxCount;

    @Value("${fraud.daily-volume-cap:50000}")
    private BigDecimal dailyVolumeCap;

    /**
     * O(1) cache: accountId → last transaction amount.
     * Used to detect rapid repeat-amount patterns without a DB query.
     */
    private final ConcurrentHashMap<String, BigDecimal> lastAmountCache = new ConcurrentHashMap<>();

    public FraudCheckResult evaluate(Transaction transaction) {
        String accountId = transaction.getAccountId();
        BigDecimal amount = transaction.getAmount();

        // R1: Hard amount threshold
        if (amount.compareTo(amountThreshold) >= 0) {
            log.warn("Fraud R1 triggered: accountId={} amount={} threshold={}",
                    accountId, amount, amountThreshold);
            return FraudCheckResult.flagged("Amount exceeds single-transaction threshold of " + amountThreshold);
        }

        // R2: Velocity check (DB query, but bounded by index on accountId + createdAt)
        Instant velocityWindowStart = Instant.now().minus(velocityWindowMinutes, ChronoUnit.MINUTES);
        long recentCount = transactionRepository.countTransactionsByAccountSince(accountId, velocityWindowStart);
        if (recentCount >= velocityMaxCount) {
            log.warn("Fraud R2 triggered: accountId={} count={} window={}min",
                    accountId, recentCount, velocityWindowMinutes);
            return FraudCheckResult.flagged("Transaction velocity limit exceeded: " + recentCount
                    + " transactions in " + velocityWindowMinutes + " minutes");
        }

        // R3: Daily volume cap
        Instant dayStart = Instant.now().truncatedTo(ChronoUnit.DAYS);
        BigDecimal dailyTotal = transactionRepository.sumAmountByAccountSince(accountId, dayStart);
        if (dailyTotal.add(amount).compareTo(dailyVolumeCap) > 0) {
            log.warn("Fraud R3 triggered: accountId={} dailyTotal={} cap={}", accountId, dailyTotal, dailyVolumeCap);
            return FraudCheckResult.flagged("Daily volume cap exceeded: current daily total is " + dailyTotal);
        }

        // R4: Rapid repeat-amount detection — O(1) ConcurrentHashMap lookup
        BigDecimal lastAmount = lastAmountCache.get(accountId);
        if (lastAmount != null && lastAmount.compareTo(amount) == 0) {
            log.warn("Fraud R4 triggered: accountId={} repeated amount={}", accountId, amount);
            // Update cache before returning
            lastAmountCache.put(accountId, amount);
            return FraudCheckResult.flagged("Duplicate amount detected: same value submitted consecutively");
        }

        // Update the O(1) cache with the current amount
        lastAmountCache.put(accountId, amount);

        return FraudCheckResult.clean();
    }

    public record FraudCheckResult(boolean flagged, String reason) {
        static FraudCheckResult clean() {
            return new FraudCheckResult(false, null);
        }
        static FraudCheckResult flagged(String reason) {
            return new FraudCheckResult(true, reason);
        }
    }
}
