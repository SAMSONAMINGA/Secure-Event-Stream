package com.example.transaction.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI openAPI() {
        final String securitySchemeName = "bearerAuth";

        return new OpenAPI()
                .info(new Info()
                        .title("Transaction Processing API")
                        .description("""
                                Production-grade event-driven transaction processing system.
                                
                                ## Authentication
                                Call `POST /api/auth/login` to obtain a JWT, then click 
                                **Authorize** and enter `Bearer <token>`.
                                
                                ## Features
                                - Real-time Kafka event streaming
                                - Fraud detection with O(1) lookups
                                - Resilience4j retry with Dead Letter Queue
                                - RFC 7807 Problem Details error responses
                                """)
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("Platform Engineering")
                                .email("platform@example.com"))
                        .license(new License().name("MIT")))
                .addSecurityItem(new SecurityRequirement().addList(securitySchemeName))
                .components(new Components()
                        .addSecuritySchemes(securitySchemeName,
                                new SecurityScheme()
                                        .name(securitySchemeName)
                                        .type(SecurityScheme.Type.HTTP)
                                        .scheme("bearer")
                                        .bearerFormat("JWT")
                                        .description("Enter your JWT token. Obtain one from POST /api/auth/login")));
    }
}
