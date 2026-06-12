package com.example.transaction;

import com.example.transaction.controller.TransactionController;
import com.example.transaction.dto.TransactionRequest;
import com.example.transaction.dto.TransactionResponse;
import com.example.transaction.entity.Transaction.TransactionStatus;
import com.example.transaction.entity.Transaction.TransactionType;
import com.example.transaction.service.TransactionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(TransactionController.class)
class TransactionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private TransactionService transactionService;

    @Test
    @WithMockUser(roles = "USER")
    void postTransaction_shouldReturn201_whenValid() throws Exception {
        TransactionRequest request = validRequest();
        TransactionResponse response = mockResponse();

        when(transactionService.create(any())).thenReturn(response);

        mockMvc.perform(post("/api/transactions")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(response.getId().toString()))
                .andExpect(jsonPath("$.accountId").value("ACC-001"))
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    @WithMockUser(roles = "USER")
    void postTransaction_shouldReturn400_whenAmountMissing() throws Exception {
        TransactionRequest request = validRequest();
        request.setAmount(null);

        mockMvc.perform(post("/api/transactions")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Validation Failed"));
    }

    @Test
    @WithMockUser(roles = "USER")
    void postTransaction_shouldReturn400_whenNegativeAmount() throws Exception {
        TransactionRequest request = validRequest();
        request.setAmount(new BigDecimal("-10.00"));

        mockMvc.perform(post("/api/transactions")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void postTransaction_shouldReturn401_whenUnauthenticated() throws Exception {
        mockMvc.perform(post("/api/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validRequest())))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @WithMockUser(roles = "USER")
    void getTransaction_shouldReturn200_whenFound() throws Exception {
        UUID id = UUID.randomUUID();
        TransactionResponse response = mockResponse();
        response.setId(id);

        when(transactionService.findById(id)).thenReturn(response);

        mockMvc.perform(get("/api/transactions/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()));
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private TransactionRequest validRequest() {
        TransactionRequest req = new TransactionRequest();
        req.setAccountId("ACC-001");
        req.setAmount(new BigDecimal("250.00"));
        req.setCurrency("USD");
        req.setType(TransactionType.DEBIT);
        req.setDescription("Test");
        return req;
    }

    private TransactionResponse mockResponse() {
        return TransactionResponse.builder()
                .id(UUID.randomUUID())
                .accountId("ACC-001")
                .amount(new BigDecimal("250.00"))
                .currency("USD")
                .type(TransactionType.DEBIT)
                .status(TransactionStatus.PENDING)
                .flaggedForReview(false)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }
}
