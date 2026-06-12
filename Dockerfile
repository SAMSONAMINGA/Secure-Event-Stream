# ─── Stage 1: Build ────────────────────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy pom first — leverages Docker layer cache so dependency downloads
# are skipped on code-only changes.
COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn package -DskipTests -q

# ─── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine AS runtime

# Non-root user for security — never run JVM services as root
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy only the fat jar from the build stage
COPY --from=builder /build/target/transaction-kafka-system-*.jar app.jar

# Ownership transfer before switching user
RUN chown appuser:appgroup app.jar

USER appuser

# Expose the application port
EXPOSE 8080

# JVM tuning for containers:
#   -XX:+UseContainerSupport           respect cgroup CPU/mem limits
#   -XX:MaxRAMPercentage=75.0          use 75 % of container RAM for heap
#   -XX:+UseG1GC                       balanced throughput + pause time
#   -Dspring.profiles.active           set via environment; default = prod
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:+UseG1GC -Djava.security.egd=file:/dev/./urandom"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
