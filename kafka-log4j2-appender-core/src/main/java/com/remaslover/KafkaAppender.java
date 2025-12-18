package com.remaslover;


import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Custom Log4j2 Appender that sends log events to Apache Kafka.
 *
 * <p>Example configuration in log4j2.xml:</p>
 * <pre>{@code
 * <KafkaAppender name="KafkaAppender"
 *                topic="application-logs"
 *                bootstrapServers="localhost:9092"
 *                keySerializer="org.apache.kafka.common.serialization.StringSerializer"
 *                valueSerializer="org.apache.kafka.common.serialization.StringSerializer">
 *   <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n" />
 * </KafkaAppender>
 * }</pre>
 *
 * <p>Required attributes:</p>
 * <ul>
 *   <li>{@code name} - unique appender name</li>
 *   <li>{@code topic} - Kafka topic name</li>
 *   <li>{@code bootstrapServers} - comma-separated list of Kafka brokers</li>
 * </ul>
 *
 * <p>Optional attributes:</p>
 * <ul>
 *   <li>{@code keySerializer} - Kafka key serializer class (default: StringSerializer)</li>
 *   <li>{@code valueSerializer} - Kafka value serializer class (default: StringSerializer)</li>
 *   <li>{@code ignoreExceptions} - whether to ignore exceptions (default: true)</li>
 * </ul>
 *
 * @see AbstractAppender
 * @since 1.0.0
 */

@Plugin(name = "KafkaAppender",
        category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class KafkaAppender extends AbstractAppender {

    private final KafkaProducerManager producerManager;
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * Constructs a new KafkaAppender.
     *
     * @param name             the name of the appender
     * @param filter           the filter, may be null
     * @param layout           the layout, may be null
     * @param ignoreExceptions if true, exceptions will be logged but not re-thrown
     * @param properties       configuration properties
     * @param topic            Kafka topic to send logs to
     * @param bootstrapServers comma-separated list of Kafka bootstrap servers
     * @param keySerializer    fully qualified class name of key serializer
     * @param valueSerializer  fully qualified class name of value serializer
     */
    protected KafkaAppender(String name,
                            Filter filter,
                            Layout<? extends Serializable> layout,
                            boolean ignoreExceptions,
                            Property[] properties,
                            String topic,
                            String bootstrapServers,
                            String keySerializer,
                            String valueSerializer) {
        super(name, filter, layout, ignoreExceptions, properties);

        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(bootstrapServers, "bootstrapServers must not be null");
        Objects.requireNonNull(keySerializer, "keySerializer must not be null");
        Objects.requireNonNull(valueSerializer, "valueSerializer must not be null");

        this.producerManager = new KafkaProducerManager(
                topic,
                bootstrapServers,
                keySerializer,
                valueSerializer
        );
    }

    /**
     * Factory method for Log4j2 plugin system.
     *
     * @param name             the name of the appender (required)
     * @param topic            the Kafka topic (required)
     * @param bootstrapServers the Kafka bootstrap servers (required)
     * @param keySerializer    the key serializer class (optional, default: StringSerializer)
     * @param valueSerializer  the value serializer class (optional, default: StringSerializer)
     * @param ignoreExceptions whether to ignore exceptions (optional, default: true)
     * @param layout           the layout to use (optional)
     * @param filter           the filter to use (optional)
     * @param properties       additional properties (optional)
     * @return a new KafkaAppender instance, or null if configuration is invalid
     */
    @PluginFactory
    public static KafkaAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("topic") String topic,
            @PluginAttribute("bootstrapServers") String bootstrapServers,
            @PluginAttribute(value = "keySerializer",
                    defaultString = "org.apache.kafka.common.serialization.StringSerializer")
            String keySerializer,
            @PluginAttribute(value = "valueSerializer",
                    defaultString = "org.apache.kafka.common.serialization.StringSerializer")
            String valueSerializer,
            @PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) boolean ignoreExceptions,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") Filter filter,
            @PluginElement("Properties") Property[] properties) {

        if (name == null) {
            LOGGER.error("No name provided for KafkaAppender");
            return null;
        }

        if (topic == null) {
            LOGGER.error("No topic provided for KafkaAppender");
            return null;
        }

        if (bootstrapServers == null) {
            LOGGER.error("No bootstrapServers provided for KafkaAppender");
            return null;
        }

        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }

        return new KafkaAppender(
                name,
                filter,
                layout,
                ignoreExceptions,
                properties,
                topic,
                bootstrapServers,
                keySerializer,
                valueSerializer
        );
    }

    /**
     * Appends a log event to Kafka.
     *
     * @param event the log event to append
     */
    @Override
    public void append(LogEvent event) {
        if (!started.get()) {
            LOGGER.warn("KafkaAppender {} is not started, ignoring log event", getName());
            return;
        }

        try {
            String formattedMessage = getLayout().toSerializable(event).toString();
            producerManager.send(event, formattedMessage);
        } catch (Exception e) {
            if (!ignoreExceptions()) {
                throw new RuntimeException("Failed to send log event to Kafka", e);
            }
            LOGGER.error("Failed to send log event to Kafka", e);
        }
    }

    /**
     * Starts the appender and initializes the Kafka producer.
     */
    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            try {
                producerManager.start();
                super.start();
                LOGGER.info("KafkaAppender {} started successfully", getName());
            } catch (Exception e) {
                started.set(false);
                LOGGER.error("Failed to start KafkaAppender {}", getName(), e);
                throw new RuntimeException("Failed to start KafkaAppender", e);
            }
        }
    }

    /**
     * Stops the appender and shuts down the Kafka producer.
     */
    @Override
    public void stop() {
        if (started.compareAndSet(true, false)) {
            try {
                producerManager.stop();
                super.stop();
                LOGGER.info("KafkaAppender {} stopped successfully", getName());
            } catch (Exception e) {
                LOGGER.error("Failed to stop KafkaAppender {}", getName(), e);
            }
        }
    }

    /**
     * Returns whether the appender is started.
     *
     * @return true if started, false otherwise
     */
    @Override
    public boolean isStarted() {
        return started.get() && super.isStarted();
    }

    /**
     * Gets the Kafka topic used by this appender.
     *
     * @return the Kafka topic
     */
    public String getTopic() {
        return producerManager.getTopic();
    }

    /**
     * Gets the bootstrap servers used by this appender.
     *
     * @return the bootstrap servers
     */
    public String getBootstrapServers() {
        return producerManager.getBootstrapServers();
    }

    /**
     * Gets the Kafka producer manager (for testing purposes).
     *
     * @return the producer manager
     */
    KafkaProducerManager getProducerManager() {
        return producerManager;
    }
}
