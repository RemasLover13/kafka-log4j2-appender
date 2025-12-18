package com.remaslover;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Example application demonstrating the use of Kafka Log4j2 Appender.
 *
 * <p>This application generates various types of log messages that are sent to Kafka.
 * It simulates a real application with different log levels, structured logging,
 * and context data.</p>
 *
 * <p>To run:</p>
 * <pre>{@code
 * mvn exec:java -Dexec.mainClass="com.github.kafka.log4j2.example.ExampleApplication"
 * }</pre>
 *
 * <p>Or with custom Kafka settings:</p>
 * <pre>{@code
 * mvn exec:java -Dexec.mainClass="com.github.kafka.log4j2.example.ExampleApplication" \
 *   -Dlog4j2.kafka.bootstrap.servers=localhost:9092 \
 *   -Dlog4j2.kafka.topic=my-logs
 * }</pre>
 *
 * @since 1.0.0
 */
public class SampleApplication {

    private static final Logger LOGGER = LogManager.getLogger(SampleApplication.class);
    private static final Random RANDOM = new Random();
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(3);
    private static final AtomicLong REQUEST_COUNTER = new AtomicLong(0);

    private volatile boolean running = true;

    /**
     * Main entry point.
     *
     * @param args command line arguments (not used)
     */
    public static void main(String[] args) {
        SampleApplication app = new SampleApplication();
        app.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();
            LOGGER.info("Application shutdown complete");
        }));
    }

    /**
     * Starts the example application.
     * Schedules different types of log messages at various intervals.
     */
    public void start() {
        LOGGER.info(" Starting Kafka Log4j2 Example Application");
        LOGGER.info(" Kafka Bootstrap: {}", System.getProperty("log4j2.kafka.bootstrap.servers", "localhost:9092"));
        LOGGER.info(" Kafka Topic: {}", System.getProperty("log4j2.kafka.topic", "application-logs"));
        LOGGER.info(" Logs will be sent to Kafka. Check your Kafka consumer to see the logs.");

        SCHEDULER.scheduleAtFixedRate(this::logInfoMessages, 1, 2, TimeUnit.SECONDS);
        SCHEDULER.scheduleAtFixedRate(this::logWarningMessages, 3, 5, TimeUnit.SECONDS);
        SCHEDULER.scheduleAtFixedRate(this::logErrorMessages, 5, 10, TimeUnit.SECONDS);
        SCHEDULER.scheduleAtFixedRate(this::logWithContext, 2, 7, TimeUnit.SECONDS);
        SCHEDULER.scheduleAtFixedRate(this::logStructuredMessages, 4, 8, TimeUnit.SECONDS);
        SCHEDULER.scheduleAtFixedRate(this::simulateBusinessLogic, 1, 3, TimeUnit.SECONDS);

        LOGGER.info(" Application started successfully. Press Ctrl+C to stop.");
        LOGGER.info(" Generating logs every few seconds...");
    }

    /**
     * Stops the example application gracefully.
     */
    public void stop() {
        running = false;
        LOGGER.info(" Stopping application...");

        SCHEDULER.shutdown();
        try {
            if (!SCHEDULER.awaitTermination(5, TimeUnit.SECONDS)) {
                SCHEDULER.shutdownNow();
            }
        } catch (InterruptedException e) {
            SCHEDULER.shutdownNow();
            Thread.currentThread().interrupt();
        }

        LOGGER.info(" Application stopped");
    }

    /**
     * Logs informational messages simulating normal application operation.
     */
    private void logInfoMessages() {
        if (!running) return;

        String[] messages = {
                "Processing request #{} from user {}",
                "Database query executed successfully in {} ms",
                "Cache hit for key: {}",
                "User session created for user ID: {}",
                "API response sent in {} ms with status 200",
                "File uploaded successfully: {}. Size: {} bytes",
                "Email sent to: {}",
                "User {} logged in successfully",
                "Processing batch of {} records",
                "Connected to external service {}"
        };

        String message = messages[RANDOM.nextInt(messages.length)];
        long requestId = REQUEST_COUNTER.incrementAndGet();

        switch (countPlaceholders(message)) {
            case 0:
                LOGGER.info(message);
                break;
            case 1:
                LOGGER.info(message, requestId);
                break;
            case 2:
                LOGGER.info(message, requestId, "user" + RANDOM.nextInt(1000));
                break;
            case 3:
                LOGGER.info(message, "file" + RANDOM.nextInt(100) + ".txt", RANDOM.nextInt(10000));
                break;
        }
    }

    /**
     * Logs warning messages simulating potential issues.
     */
    private void logWarningMessages() {
        if (!running) return;

        String[] messages = {
                "High memory usage detected: {}% (threshold: 85%)",
                "Slow database query detected: {} ms (threshold: 1000ms)",
                "Cache miss rate is high: {}%",
                "Connection pool at {}% capacity",
                "Retrying failed request (attempt {})",
                "Rate limit approaching: {} requests/minute",
                "Deprecated API called: {}",
                "External service {} response time increased by {}%",
                "Disk space low: {} GB remaining",
                "Unusual pattern detected in user activity"
        };

        String message = messages[RANDOM.nextInt(messages.length)];

        switch (countPlaceholders(message)) {
            case 0:
                LOGGER.warn(message);
                break;
            case 1:
                LOGGER.warn(message, RANDOM.nextInt(30) + 70); // 70-99%
                break;
            case 2:
                LOGGER.warn(message, RANDOM.nextInt(1500) + 500, RANDOM.nextInt(3) + 1);
                break;
        }
    }

    /**
     * Logs error messages with exceptions simulating failures.
     */
    private void logErrorMessages() {
        if (!running) return;

        String[] messages = {
                "Failed to process request #{}",
                "Database connection lost to {}",
                "External service {} timeout after {} ms",
                "Invalid user input detected: {}",
                "Resource not found: {}",
                "Authentication failed for user {}",
                "Payment processing failed for transaction {}",
                "File upload failed: {}",
                "API rate limit exceeded for client {}",
                "System error in module {}"
        };

        String message = messages[RANDOM.nextInt(messages.length)];
        Exception exception = createRandomException();

        if (message.contains("{}")) {
            LOGGER.error(message, REQUEST_COUNTER.get(), exception);
        } else {
            LOGGER.error(message, exception);
        }
    }

    /**
     * Logs messages with ThreadContext (MDC) data simulating request context.
     */
    private void logWithContext() {
        if (!running) return;

        String[] userIds = {"user123", "user456", "user789", "user101", "user202", "admin001"};
        String[] sessionIds = {"sess-abc123", "sess-def456", "sess-ghi789", "sess-jkl012", "sess-mno345"};
        String[] requestIds = {"req-" + System.currentTimeMillis(),
                "req-" + (System.currentTimeMillis() + 1),
                "req-" + (System.currentTimeMillis() + 2)};
        String[] endpoints = {"/api/users", "/api/products", "/api/orders", "/api/payments", "/api/auth"};
        String[] methods = {"GET", "POST", "PUT", "DELETE", "PATCH"};

        ThreadContext.put("userId", userIds[RANDOM.nextInt(userIds.length)]);
        ThreadContext.put("sessionId", sessionIds[RANDOM.nextInt(sessionIds.length)]);
        ThreadContext.put("requestId", requestIds[RANDOM.nextInt(requestIds.length)]);
        ThreadContext.put("correlationId", "corr-" + RANDOM.nextInt(10000));
        ThreadContext.put("endpoint", endpoints[RANDOM.nextInt(endpoints.length)]);
        ThreadContext.put("method", methods[RANDOM.nextInt(methods.length)]);
        ThreadContext.put("clientIp", "192.168.1." + RANDOM.nextInt(255));
        ThreadContext.put("userAgent", RANDOM.nextBoolean() ? "Chrome" : "Firefox");

        try {
            LOGGER.info("Processing {} request to {}",
                    ThreadContext.get("method"),
                    ThreadContext.get("endpoint"));

            LOGGER.debug("Detailed debug information with full context");

            ThreadContext.push("transaction", "tx-" + RANDOM.nextInt(1000));
            ThreadContext.push("operation", "create");
            LOGGER.info("Transaction started");
            ThreadContext.pop();
            ThreadContext.pop();

            if (RANDOM.nextBoolean()) {
                ThreadContext.put("childOperation", "validation");
                LOGGER.debug("Validating request data");
                ThreadContext.remove("childOperation");
            }

        } finally {
            ThreadContext.clearAll();
        }
    }

    /**
     * Logs structured messages using MapMessage.
     */
    private void logStructuredMessages() {
        if (!running) return;

        org.apache.logging.log4j.message.MapMessage<?, ?> userActionMessage =
                new org.apache.logging.log4j.message.StringMapMessage()
                        .with("event", "user_action")
                        .with("action", RANDOM.nextBoolean() ? "login" : "logout")
                        .with("userId", "user" + RANDOM.nextInt(1000))
                        .with("timestamp", System.currentTimeMillis())
                        .with("success", RANDOM.nextBoolean())
                        .with("duration_ms", RANDOM.nextInt(500))
                        .with("source", RANDOM.nextBoolean() ? "web" : "mobile")
                        .with("ip", "192.168.1." + RANDOM.nextInt(255));

        LOGGER.info(userActionMessage);

        org.apache.logging.log4j.message.MapMessage<?, ?> apiCallMessage =
                new org.apache.logging.log4j.message.StringMapMessage()
                        .with("event", "api_call")
                        .with("endpoint", RANDOM.nextBoolean() ? "/api/users" : "/api/products")
                        .with("method", "GET")
                        .with("status_code", RANDOM.nextBoolean() ? 200 : 404)
                        .with("response_size", RANDOM.nextInt(5000))
                        .with("duration_ms", RANDOM.nextInt(300))
                        .with("client_id", "client-" + RANDOM.nextInt(100))
                        .with("cache_hit", RANDOM.nextBoolean());

        LOGGER.info(apiCallMessage);

        org.apache.logging.log4j.message.MapMessage<?, ?> metricMessage =
                new org.apache.logging.log4j.message.StringMapMessage()
                        .with("event", "business_metric")
                        .with("metric", "revenue")
                        .with("value", RANDOM.nextInt(10000))
                        .with("currency", "USD")
                        .with("period", "hourly")
                        .with("timestamp", System.currentTimeMillis());

        LOGGER.info(metricMessage);
    }

    /**
     * Simulates business logic with logging.
     */
    private void simulateBusinessLogic() {
        if (!running) return;

        long startTime = System.currentTimeMillis();
        String operation = getRandomOperation();

        LOGGER.debug("Starting business logic: {}", operation);

        try {
            Thread.sleep(RANDOM.nextInt(200));

            if (RANDOM.nextDouble() < 0.15) {
                throw new RuntimeException("Simulated business logic error in " + operation);
            }

            long duration = System.currentTimeMillis() - startTime;
            LOGGER.debug("Business logic {} completed successfully in {} ms", operation, duration);

            if (duration > 100) {
                LOGGER.warn("Slow operation detected: {} took {} ms", operation, duration);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Business logic interrupted: {}", operation);
        } catch (Exception e) {
            LOGGER.error("Business logic failed: {}", operation, e);
        }
    }

    /**
     * Creates a random exception for testing.
     *
     * @return a random exception instance
     */
    private Exception createRandomException() {
        Exception[] exceptions = {
                new RuntimeException("Unexpected runtime error"),
                new IllegalArgumentException("Invalid argument provided: " + RANDOM.nextInt(100)),
                new IllegalStateException("System in illegal state: code " + RANDOM.nextInt(10)),
                new NullPointerException("Unexpected null value at line " + RANDOM.nextInt(100)),
                new ArithmeticException("Division by zero"),
                new ArrayIndexOutOfBoundsException("Index " + RANDOM.nextInt(100) + " out of bounds"),
                new UnsupportedOperationException("Operation not supported in current context"),
                new SecurityException("Security violation detected")
        };

        Exception cause = RANDOM.nextBoolean() ?
                new RuntimeException("Root cause: internal error") : null;

        Exception exception = exceptions[RANDOM.nextInt(exceptions.length)];
        if (cause != null && RANDOM.nextBoolean()) {
            exception.initCause(cause);
        }

        return exception;
    }

    /**
     * Counts placeholder {} in a message string.
     *
     * @param message the message string
     * @return number of {} placeholders
     */
    private int countPlaceholders(String message) {
        int count = 0;
        int index = 0;
        while ((index = message.indexOf("{}", index)) != -1) {
            count++;
            index += 2;
        }
        return count;
    }

    /**
     * Gets a random operation name for simulation.
     *
     * @return operation name
     */
    private String getRandomOperation() {
        String[] operations = {
                "processPayment",
                "validateUser",
                "generateReport",
                "sendNotification",
                "updateDatabase",
                "callExternalAPI",
                "cacheOperation",
                "fileProcessing",
                "dataTransformation",
                "batchProcessing"
        };
        return operations[RANDOM.nextInt(operations.length)];
    }

    /**
     * Returns whether the application is running.
     *
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running;
    }
}
