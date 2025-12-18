package com.remaslover;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.config.Property;

import java.io.Serializable;

/**
 * Fluent builder for programmatic configuration of KafkaAppender.
 * Useful for Spring Boot auto-configuration or other programmatic setups.
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * KafkaAppender appender = new KafkaAppenderBuilder()
 *     .name("KafkaAppender")
 *     .topic("logs")
 *     .bootstrapServers("localhost:9092")
 *     .build();
 * }</pre>
 *
 * @see KafkaAppender
 * @since 1.0.0
 */
public class KafkaAppenderBuilder {

    private String name;
    private String topic;
    private String bootstrapServers;
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private boolean ignoreExceptions = true;
    private Layout<? extends Serializable> layout;
    private Filter filter;
    private Property[] properties;

    /**
     * Sets the appender name.
     *
     * @param name the appender name
     * @return this builder
     */
    public KafkaAppenderBuilder name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Sets the Kafka topic.
     *
     * @param topic the topic name
     * @return this builder
     */
    public KafkaAppenderBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * Sets the Kafka bootstrap servers.
     *
     * @param bootstrapServers comma-separated list of brokers
     * @return this builder
     */
    public KafkaAppenderBuilder bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    /**
     * Sets the key serializer class.
     *
     * @param keySerializer fully qualified class name
     * @return this builder
     */
    public KafkaAppenderBuilder keySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    /**
     * Sets the value serializer class.
     *
     * @param valueSerializer fully qualified class name
     * @return this builder
     */
    public KafkaAppenderBuilder valueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    /**
     * Sets whether to ignore exceptions.
     *
     * @param ignoreExceptions true to ignore exceptions
     * @return this builder
     */
    public KafkaAppenderBuilder ignoreExceptions(boolean ignoreExceptions) {
        this.ignoreExceptions = ignoreExceptions;
        return this;
    }

    /**
     * Sets the layout to use.
     *
     * @param layout the layout instance
     * @return this builder
     */
    public KafkaAppenderBuilder layout(Layout<? extends Serializable> layout) {
        this.layout = layout;
        return this;
    }

    /**
     * Sets the filter to use.
     *
     * @param filter the filter instance
     * @return this builder
     */
    public KafkaAppenderBuilder filter(Filter filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Sets additional properties.
     *
     * @param properties array of properties
     * @return this builder
     */
    public KafkaAppenderBuilder properties(Property[] properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Builds the KafkaAppender instance.
     *
     * @return configured KafkaAppender
     * @throws IllegalStateException if required parameters are missing
     */
    public KafkaAppender build() {
        if (name == null) {
            throw new IllegalStateException("Name is required");
        }
        if (topic == null) {
            throw new IllegalStateException("Topic is required");
        }
        if (bootstrapServers == null) {
            throw new IllegalStateException("Bootstrap servers are required");
        }

        return KafkaAppender.createAppender(
                name,
                topic,
                bootstrapServers,
                keySerializer,
                valueSerializer,
                ignoreExceptions,
                layout,
                filter,
                properties
        );
    }
}
