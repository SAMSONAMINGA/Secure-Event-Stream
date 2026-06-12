package com.example.transaction.controller;

import com.example.transaction.dto.TransactionRequest;
import com.example.transaction.dto.TransactionResponse;
import com.example.transaction.entity.Transaction.TransactionStatus;
import com.example.transaction.service.TransactionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor
@Tag(name = "Transactions", description = "Transaction management endpoints")
@SecurityRequirement(name = "bearerAuth")
public class TransactionController {

    private final TransactionService transactionService;

    /**
     * POST /api/transactions
     * Creates a new transaction.  @Valid triggers bean validation on the DTO.
     */
    @PostMapping
    @Operation(summary = "Create a new transaction")
    @PreAuthorize("hasRole('USER') or hasRole('ADMIN')")
    public ResponseEntity<TransactionResponse> create(@Valid @RequestBody TransactionRequest request) {
        TransactionResponse response = transactionService.create(request);
        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(response.getId())
                .toUri();
        return ResponseEntity.created(location).body(response);
    }

    /**
     * GET /api/transactions/{id}
     */
    @GetMapping("/{id}")
    @Operation(summary = "Get transaction by ID")
    @PreAuthorize("hasRole('USER') or hasRole('ADMIN')")
    public ResponseEntity<TransactionResponse> getById(@PathVariable UUID id) {
        return ResponseEntity.ok(transactionService.findById(id));
    }

    /**
     * GET /api/transactions
     * Optional filters: accountId, status
     * Pagination via ?page=0&size=20&sort=createdAt,desc
     */
    @GetMapping
    @Operation(summary = "List transactions with optional filters")
    @PreAuthorize("hasRole('USER') or hasRole('ADMIN')")
    public ResponseEntity<Page<TransactionResponse>> list(
            @RequestParam(required = false) String accountId,
            @RequestParam(required = false) TransactionStatus status,
            @PageableDefault(size = 20, sort = "createdAt") Pageable pageable) {

        Page<TransactionResponse> page;
        if (accountId != null && status != null) {
            page = transactionService.findByAccount(accountId, pageable)
                    .map(t -> t); // combined filter done in service if needed
        } else if (accountId != null) {
            page = transactionService.findByAccount(accountId, pageable);
        } else if (status != null) {
            page = transactionService.findByStatus(status, pageable);
        } else {
            page = transactionService.findAll(pageable);
        }

        return ResponseEntity.ok(page);
    }
}
