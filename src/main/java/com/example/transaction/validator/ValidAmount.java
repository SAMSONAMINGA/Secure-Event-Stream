package com.example.transaction.validator;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.*;

@Documented
@Constraint(validatedBy = AmountValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidAmount {
    String message() default "Amount must have at most 4 decimal places and must not be negative";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}
