package com.remaslover;

import org.apache.kafka.clients.producer.*;
import org.apache.logging.log4j.core.LogEvent;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages Kafka producer lifecycle and message sending.
 * Thread-safe and handles producer reinitialization on errors.
 *
 * <p>This class is package-private and not meant to be used directly by users.</p>
 */
public class KafkaProducerManager implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
    private static final long INITIALIZATION_DELAY_MS = 1000;
    private static final long SHUTDOWN_TIMEOUT_MS = 30000;

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(
            "com.remaslover.log4j2.KafkaProducerManagerInternal"
    );

    private final String topic;
    private final String bootstrapServers;
    private final String keySerializer;
    private final String valueSerializer;

    private transient KafkaProducer<String, String> producer;
    private final transient Lock producerLock = new ReentrantLock();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final Map<String, Object> additionalProperties = new HashMap<>();

    /**
     * Constructs a new KafkaProducerManager with required parameters.
     *
     * @param topic            Kafka topic to send messages to
     * @param bootstrapServers comma-separated list of Kafka bootstrap servers
     * @param keySerializer    fully qualified class name of key serializer
     * @param valueSerializer  fully qualified class name of value serializer
     * @throws NullPointerException if any parameter is null
     */
    KafkaProducerManager(String topic,
                         String bootstrapServers,
                         String keySerializer,
                         String valueSerializer) {
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers must not be null");
        this.keySerializer = Objects.requireNonNull(keySerializer, "keySerializer must not be null");
        this.valueSerializer = Objects.requireNonNull(valueSerializer, "valueSerializer must not be null");
    }

    /**
     * Adds additional Kafka producer properties.
     * These properties will override default settings.
     *
     * @param key   property key (e.g., "acks", "retries")
     * @param value property value
     * @return this manager for method chaining
     */
    KafkaProducerManager addProperty(String key, Object value) {
        if (key != null && value != null) {
            additionalProperties.put(key, value);
        }
        return this;
    }

    /**
     * Starts the Kafka producer.
     * This method is idempotent.
     */
    void start() {
        if (running.compareAndSet(false, true)) {
            initializeProducer();
        }
    }

    /**
     * Initializes the Kafka producer.
     * This method is thread-safe and handles initialization errors.
     */
    private void initializeProducer() {
        producerLock.lock();
        try {
            if (producer == null) {
                Properties props = createProducerProperties();
                producer = new KafkaProducer<>(props);
                initialized.set(true);
                LOG.info("Kafka producer initialized for topic: {}", topic);
            }
        } catch (Exception e) {
            running.set(false);
            initialized.set(false);
            LOG.error("Failed to initialize Kafka producer for topic: {}", topic, e);
            throw new RuntimeException("Failed to initialize Kafka producer", e);
        } finally {
            producerLock.unlock();
        }
    }

    /**
     * Creates Kafka producer properties with sensible defaults.
     *
     * @return configured Properties object
     */
    private Properties createProducerProperties() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        props.putAll(additionalProperties);

        return props;
    }

    /**
     * Sends a log event to Kafka.
     *
     * @param event   the log event containing metadata
     * @param message the formatted message to send
     */
    void send(LogEvent event, String message) {
        if (!running.get() || !initialized.get()) {
            LOG.warn("Kafka producer is not initialized, dropping message");
            return;
        }

        producerLock.lock();
        try {
            if (producer != null) {
                String key = generateKey(event);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);

                Future<RecordMetadata> future = producer.send(record, new LoggingCallback(event, message));

                if (Boolean.parseBoolean(System.getProperty("kafka.log4j2.sync.send", "false"))) {
                    try {
                        future.get(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        LOG.warn("Failed to send log event to Kafka synchronously", e);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error sending log event to Kafka", e);
            handleProducerError(e);
        } finally {
            producerLock.unlock();
        }
    }

    /**
     * Generates a unique key for the Kafka message.
     * Uses thread name and timestamp for uniqueness.
     *
     * @param event the log event
     * @return generated key string
     */
    private String generateKey(LogEvent event) {
        return event.getThreadName() + "-" + event.getTimeMillis();
    }

    /**
     * Handles producer errors and attempts reinitialization.
     * This method ensures the producer is properly closed and recreated.
     *
     * @param e the exception that occurred
     */
    private void handleProducerError(Exception e) {
        producerLock.lock();
        try {
            if (producer != null) {
                try {
                    producer.close(Duration.ofMillis(SHUTDOWN_TIMEOUT_MS));
                } catch (Exception ex) {
                    LOG.warn("Error closing Kafka producer during error handling", ex);
                }
                producer = null;
                initialized.set(false);

                if (running.get()) {
                    LOG.info("Attempting to reinitialize Kafka producer after error");
                    try {
                        Thread.sleep(INITIALIZATION_DELAY_MS);
                        initializeProducer();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.warn("Reinitialization interrupted", ie);
                    } catch (Exception ex) {
                        LOG.error("Failed to reinitialize Kafka producer", ex);
                    }
                }
            }
        } finally {
            producerLock.unlock();
        }
    }

    /**
     * Stops the Kafka producer.
     * This method is idempotent and thread-safe.
     */
    void stop() {
        if (running.compareAndSet(true, false)) {
            producerLock.lock();
            try {
                if (producer != null) {
                    try {
                        producer.flush();
                        producer.close(Duration.ofMillis(SHUTDOWN_TIMEOUT_MS));
                        LOG.info("Kafka producer stopped for topic: {}", topic);
                    } catch (Exception e) {
                        LOG.warn("Error closing Kafka producer", e);
                    } finally {
                        producer = null;
                        initialized.set(false);
                    }
                }
            } finally {
                producerLock.unlock();
            }
        }
    }

    /**
     * Gets the Kafka topic.
     *
     * @return the topic name
     */
    String getTopic() {
        return topic;
    }

    /**
     * Gets the bootstrap servers.
     *
     * @return bootstrap servers string
     */
    String getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Gets whether the producer is running.
     *
     * @return true if running, false otherwise
     */
    boolean isRunning() {
        return running.get();
    }

    /**
     * Gets whether the producer is initialized.
     *
     * @return true if initialized, false otherwise
     */
    boolean isInitialized() {
        return initialized.get();
    }

    /**
     * Callback for handling Kafka send results.
     * Logs success or failure of message delivery.
     */
    private static class LoggingCallback implements Callback {
        private final LogEvent event;
        private final String message;

        LoggingCallback(LogEvent event, String message) {
            this.event = event;
            this.message = message;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                LOG.warn("Failed to send log event to Kafka [topic={}, partition={}, offset={}]",
                        metadata != null ? metadata.topic() : "unknown",
                        metadata != null ? metadata.partition() : -1,
                        metadata != null ? metadata.offset() : -1,
                        exception);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully sent log event to Kafka [topic={}, partition={}, offset={}]",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        }
    }
}
