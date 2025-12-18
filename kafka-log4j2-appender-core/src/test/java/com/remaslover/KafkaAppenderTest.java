package com.remaslover;


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaAppenderTest {
    private LoggerContext context;
    private Logger logger;

    @Mock
    private KafkaProducerManager producerManager;

    @BeforeEach
    void setUp() {
        ConfigurationBuilder<BuiltConfiguration> builder =
                ConfigurationBuilderFactory.newConfigurationBuilder();

        builder.setStatusLevel(Level.ERROR);
        builder.setConfigurationName("TestConfig");

        AppenderComponentBuilder appenderBuilder = builder.newAppender("KafkaTest", "KafkaAppender")
                .addAttribute("topic", "test-topic")
                .addAttribute("bootstrapServers", "localhost:9092")
                .addAttribute("keySerializer", "org.apache.kafka.common.serialization.StringSerializer")
                .addAttribute("valueSerializer", "org.apache.kafka.common.serialization.StringSerializer");

        appenderBuilder.add(builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%m%n"));

        builder.add(appenderBuilder);

        builder.add(builder.newRootLogger(Level.INFO)
                .add(builder.newAppenderRef("KafkaTest")));

        context = Configurator.initialize(builder.build());
        logger = context.getLogger(KafkaAppenderTest.class.getName());
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            Configurator.shutdown(context);
        }
    }

    /**
     * Tests that appender is created with correct configuration.
     */
    @Test
    void testAppenderCreation() {
        Configuration config = context.getConfiguration();
        Appender appender = config.getAppender("KafkaTest");

        assertNotNull(appender, "Appender should be created");
        assertInstanceOf(KafkaAppender.class, appender, "Appender should be KafkaAppender");

        KafkaAppender kafkaAppender = (KafkaAppender) appender;
        assertEquals("test-topic", kafkaAppender.getTopic());
        assertEquals("localhost:9092", kafkaAppender.getBootstrapServers());
        assertTrue(kafkaAppender.isStarted(), "Appender should be started");
    }

    /**
     * Tests that invalid configuration results in no appender creation.
     */
    @Test
    void testInvalidConfiguration() {
        ConfigurationBuilder<BuiltConfiguration> builder =
                ConfigurationBuilderFactory.newConfigurationBuilder();

        builder.setStatusLevel(Level.ERROR);

        AppenderComponentBuilder appenderBuilder = builder.newAppender("Invalid", "KafkaAppender")
                .addAttribute("bootstrapServers", "localhost:9092");

        appenderBuilder.add(builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%m%n"));

        builder.add(appenderBuilder);

        LoggerContext invalidContext = Configurator.initialize(builder.build());
        Configuration config = invalidContext.getConfiguration();
        Appender appender = config.getAppender("Invalid");

        assertNull(appender, "Appender should not be created with invalid configuration");

        Configurator.shutdown(invalidContext);
    }

    /**
     * Tests that logging events doesn't throw exceptions.
     */
    @Test
    void testLogEventAppending() {
        logger.info("Test log message");
        logger.error("Test error message", new RuntimeException("Test exception"));

        assertTrue(true, "Logging should complete without exceptions");
    }

    /**
     * Tests appender start and stop lifecycle.
     */
    @Test
    void testStartStopLifecycle() {
        Configuration config = context.getConfiguration();
        KafkaAppender appender = (KafkaAppender) config.getAppender("KafkaTest");

        assertTrue(appender.isStarted(), "Appender should be started after initialization");

        appender.stop();
        assertFalse(appender.isStarted(), "Appender should be stopped");

        appender.start();
        assertTrue(appender.isStarted(), "Appender should be restarted");
    }

    /**
     * Tests that getters return correct values.
     */
    @Test
    void testGetters() {
        Configuration config = context.getConfiguration();
        KafkaAppender appender = config.getAppender("KafkaTest");

        assertEquals("test-topic", appender.getTopic());
        assertEquals("localhost:9092", appender.getBootstrapServers());
        assertEquals("KafkaTest", appender.getName());
    }

    /**
     * Tests JSON layout configuration.
     */
    @Test
    void testJsonLayout() {
        ConfigurationBuilder<BuiltConfiguration> builder =
                ConfigurationBuilderFactory.newConfigurationBuilder();

        builder.setStatusLevel(Level.ERROR);
        builder.setConfigurationName("JsonTestConfig");

        AppenderComponentBuilder appenderBuilder = builder.newAppender("KafkaJson", "KafkaAppender")
                .addAttribute("topic", "json-topic")
                .addAttribute("bootstrapServers", "localhost:9092");

        appenderBuilder.add(builder.newLayout("JsonLayout")
                .addAttribute("includeTimestamp", true)
                .addAttribute("includeThreadInfo", true));

        builder.add(appenderBuilder);

        builder.add(builder.newRootLogger(Level.DEBUG)
                .add(builder.newAppenderRef("KafkaJson")));

        LoggerContext jsonContext = Configurator.initialize(builder.build());
        Logger jsonLogger = jsonContext.getLogger("JsonTestLogger");

        org.apache.logging.log4j.ThreadContext.put("userId", "test-user");
        jsonLogger.info("JSON formatted message");
        org.apache.logging.log4j.ThreadContext.clearMap();

        jsonLogger.error("Error with exception", new IllegalArgumentException("Test error"));

        Configurator.shutdown(jsonContext);
    }
}
