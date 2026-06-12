package com.example.transaction.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
public class FraudDetectedException extends RuntimeException {
    public FraudDetectedException(String reason) {
        super("Transaction rejected by fraud engine: " + reason);
    }
}
