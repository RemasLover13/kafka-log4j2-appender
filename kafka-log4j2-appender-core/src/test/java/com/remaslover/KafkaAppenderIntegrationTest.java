package com.remaslover;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link KafkaAppender} using Testcontainers.
 *
 * <p>Requires Docker to be running. These tests are skipped by default
 * and can be enabled with system property: -Dintegration.tests=true</p>
 */
@Testcontainers
class KafkaAppenderIntegrationTest {

    @Container
    private static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
                    .withStartupTimeout(Duration.ofSeconds(90));

    private LoggerContext context;
    private Logger logger;
    private Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        System.out.println("=== SETUP STARTED ===");
        System.out.println("Kafka bootstrap servers: " + KAFKA_CONTAINER.getBootstrapServers());

        testDirectKafkaConnection();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("test-topic-integration"));

        configureLog4j2();

        logger = LogManager.getLogger(KafkaAppenderIntegrationTest.class);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("=== SETUP COMPLETED ===");
    }

    @AfterEach
    void tearDown() {
        System.out.println("=== TEARDOWN STARTED ===");

        if (consumer != null) {
            consumer.close();
            System.out.println("Kafka consumer closed");
        }
        if (context != null) {
            context.close();
            System.out.println("LoggerContext closed");
        }

        System.out.println("=== TEARDOWN COMPLETED ===");
    }

    /**
     * Test direct Kafka connection to verify container is working
     */
    private void testDirectKafkaConnection() {
        System.out.println("Testing direct Kafka connection...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String testMessage = "Direct connection test " + System.currentTimeMillis();
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "test-topic-integration", "test-key", testMessage);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Failed to send direct test message: " + exception.getMessage());
                } else {
                    System.out.println("Direct test message sent successfully to topic: " + metadata.topic());
                }
            });

            producer.flush();
            System.out.println("Direct Kafka connection test passed");

            verifyDirectMessage(testMessage);

        } catch (Exception e) {
            System.err.println("Direct Kafka connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Verify direct message was received
     */
    private void verifyDirectMessage(String expectedMessage) {
        Properties verifyProps = new Properties();
        verifyProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        verifyProps.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-group-" + System.currentTimeMillis());
        verifyProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        verifyProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        verifyProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (Consumer<String, String> verifyConsumer = new KafkaConsumer<>(verifyProps)) {
            verifyConsumer.subscribe(Collections.singletonList("test-topic-integration"));

            ConsumerRecords<String, String> records = verifyConsumer.poll(Duration.ofSeconds(5));
            System.out.println("Direct verification found " + records.count() + " messages");

            boolean found = false;
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Direct message: " + record.value());
                if (record.value().contains(expectedMessage)) {
                    found = true;
                    break;
                }
            }

            if (found) {
                System.out.println("Direct message verification PASSED");
            } else {
                System.err.println("Direct message verification FAILED - message not found");
            }
        } catch (Exception e) {
            System.err.println("Direct message verification error: " + e.getMessage());
        }
    }

    /**
     * Programmatically configure Log4j2
     */
    private void configureLog4j2() {
        System.out.println("Configuring Log4j2 programmatically...");

        try {
            org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder<org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration> builder =
                    org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory.newConfigurationBuilder();

            builder.setStatusLevel(org.apache.logging.log4j.Level.WARN);
            builder.setConfigurationName("IntegrationTestConfig");

            org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder consoleAppender =
                    builder.newAppender("Console", "Console")
                            .addAttribute("target", "SYSTEM_OUT");
            consoleAppender.add(builder.newLayout("PatternLayout")
                    .addAttribute("pattern", "%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"));
            builder.add(consoleAppender);

            org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder kafkaAppender =
                    builder.newAppender("KafkaIntegration", "KafkaAppender")
                            .addAttribute("name", "KafkaIntegration")
                            .addAttribute("topic", "test-topic-integration")
                            .addAttribute("bootstrapServers", KAFKA_CONTAINER.getBootstrapServers())
                            .addAttribute("keySerializer", "org.apache.kafka.common.serialization.StringSerializer")
                            .addAttribute("valueSerializer", "org.apache.kafka.common.serialization.StringSerializer")
                            .addAttribute("ignoreExceptions", false);

            kafkaAppender.add(builder.newLayout("PatternLayout")
                    .addAttribute("pattern", "INTEGRATION: %d{yyyy-MM-dd HH:mm:ss} %-5level %logger{36} - %msg%n"));
            builder.add(kafkaAppender);

            builder.add(builder.newRootLogger(org.apache.logging.log4j.Level.INFO)
                    .add(builder.newAppenderRef("Console"))
                    .add(builder.newAppenderRef("KafkaIntegration")));

            org.apache.logging.log4j.core.config.Configuration config = builder.build();

            context = org.apache.logging.log4j.core.config.Configurator.initialize(config);

            System.out.println("Log4j2 configured successfully");

            if (context instanceof org.apache.logging.log4j.core.LoggerContext) {
                org.apache.logging.log4j.core.LoggerContext ctx = context;
                System.out.println("Appenders: " + ctx.getConfiguration().getAppenders().size());
                ctx.getConfiguration().getAppenders().forEach((name, appender) -> {
                    System.out.println("  - " + name + ": " + appender.getClass().getName());
                });
            }

        } catch (Exception e) {
            System.err.println("Failed to configure Log4j2: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Log4j2 configuration failed", e);
        }
    }

    /**
     * Tests that logs are successfully sent to Kafka.
     */
    @Test
    void testLogsAreSentToKafka() throws Exception {
        System.out.println("=== TEST: Logs are sent to Kafka ===");

        String testMessage = "Integration test message " + System.currentTimeMillis();
        System.out.println("Test message: " + testMessage);

        logger.info(testMessage);
        System.out.println("Message logged via Log4j2");

        Thread.sleep(5000);

        System.out.println("Polling Kafka for messages...");
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

        System.out.println("Received " + records.count() + " messages from Kafka");

        if (!records.isEmpty()) {
            System.out.println("Received messages:");
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("  - Topic: " + record.topic() +
                                   ", Partition: " + record.partition() +
                                   ", Offset: " + record.offset() +
                                   ", Value: " + record.value());
            }
        } else {
            System.out.println("No messages received from Kafka!");
            listKafkaTopics();
        }

        assertFalse(records.isEmpty(), "Should receive at least one message from Kafka");

        boolean found = false;
        for (var record : records) {
            if (record.value().contains(testMessage)) {
                found = true;
                System.out.println("Found test message in Kafka!");
                break;
            }
        }

        assertTrue(found, "Should find the test message in Kafka");
    }

    /**
     * List Kafka topics for debugging
     */
    private void listKafkaTopics() {
        try {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "list-topics-" + System.currentTimeMillis());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            try (Consumer<String, String> tempConsumer = new KafkaConsumer<>(props)) {
                var topics = tempConsumer.listTopics();
                System.out.println("Available topics:");
                topics.forEach((topic, partitions) -> {
                    System.out.println("  - " + topic + " (partitions: " + partitions.size() + ")");
                });
            }
        } catch (Exception e) {
            System.err.println("Failed to list topics: " + e.getMessage());
        }
    }

    /**
     * Tests multiple log messages.
     */
    @Test
    void testMultipleLogMessages() throws Exception {
        System.out.println("=== TEST: Multiple log messages ===");

        int messageCount = 3;

        for (int i = 0; i < messageCount; i++) {
            logger.info("Test message {}", i);
            Thread.sleep(100); // Small delay between messages
        }

        System.out.println("Sent " + messageCount + " messages");

        Thread.sleep(5000);

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

        System.out.println("Received " + records.count() + " messages from Kafka");

        if (!records.isEmpty()) {
            System.out.println("All received messages:");
            records.forEach(record ->
                    System.out.println("  - " + record.value()));
        }

        assertTrue(records.count() >= messageCount,
                String.format("Should receive at least %d messages, got %d", messageCount, records.count()));
    }

    /**
     * Tests error logs with exceptions.
     */
    @Test
    void testErrorLogsWithExceptions() throws Exception {
        System.out.println("=== TEST: Error logs with exceptions ===");

        String errorMessage = "Error test " + System.currentTimeMillis();
        System.out.println("Error message: " + errorMessage);

        try {
            throw new RuntimeException("Test exception for Kafka logging");
        } catch (RuntimeException e) {
            logger.error(errorMessage, e);
            System.out.println("Error logged with exception");
        }

        Thread.sleep(5000);

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

        System.out.println("Received " + records.count() + " messages from Kafka");

        boolean found = false;
        for (var record : records) {
            String value = record.value();
            System.out.println("Checking message: " + value.substring(0, Math.min(100, value.length())) + "...");

            if (value.contains(errorMessage) && value.contains("RuntimeException")) {
                found = true;
                System.out.println("Found error message with exception in Kafka!");
                break;
            }
        }

        assertTrue(found, "Should find error message with exception in Kafka");
    }

    /**
     * Simple debug test to verify basic functionality
     */
    @Test
    @Disabled("For debugging only")
    void debugTest() {
        System.out.println("=== DEBUG TEST ===");

        org.apache.logging.log4j.core.LoggerContext ctx =
                (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);

        System.out.println("Configuration name: " + ctx.getConfiguration().getName());
        System.out.println("Appenders:");
        ctx.getConfiguration().getAppenders().forEach((name, appender) -> {
            System.out.println("  " + name + ": " + appender.getClass().getName());
            if (appender instanceof KafkaAppender kafkaAppender) {
                System.out.println("    Topic: " + kafkaAppender.getTopic());
                System.out.println("    Bootstrap servers: " + kafkaAppender.getBootstrapServers());
                System.out.println("    Started: " + kafkaAppender.isStarted());
            }
        });

        logger.info("Debug test message");


        System.out.println("=== DEBUG TEST COMPLETE ===");
    }
}