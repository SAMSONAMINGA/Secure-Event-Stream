package com.example.transaction.validator;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.math.BigDecimal;

/**
 * Custom validator: amount must be positive and have at most 4 decimal places.
 * Complements @DecimalMin/@DecimalMax at the field level.
 */
public class AmountValidator implements ConstraintValidator<ValidAmount, BigDecimal> {

    private static final int MAX_SCALE = 4;

    @Override
    public boolean isValid(BigDecimal value, ConstraintValidatorContext context) {
        if (value == null) {
            return true; // @NotNull handles null check
        }
        return value.compareTo(BigDecimal.ZERO) > 0
                && value.scale() <= MAX_SCALE;
    }
}
