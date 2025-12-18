package com.remaslover;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.impl.ThrowableProxy;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.message.Message;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * JSON Layout for Log4j2 that formats log events as JSON for Kafka.
 *
 * <p>Example configuration:</p>
 * <pre>{@code
 * <JsonLayout includeTimestamp="true"
 *            includeThreadInfo="true"
 *            includeStackTrace="true"
 *            includeContextData="true"
 *            timestampFormat="yyyy-MM-dd'T'HH:mm:ss.SSSZ" />
 * }</pre>
 *
 * <p>Generated JSON example:</p>
 * <pre>{@code
 * {
 *   "level": "ERROR",
 *   "logger": "com.example.MyClass",
 *   "message": "Something went wrong",
 *   "timestamp": "2024-01-01T12:00:00.000Z",
 *   "thread": "main",
 *   "threadId": 1,
 *   "threadPriority": 5,
 *   "throwable": {
 *     "message": "NullPointerException",
 *     "class": "java.lang.NullPointerException",
 *     "stackTrace": "at com.example..."
 *   },
 *   "context": {
 *     "userId": "123",
 *     "requestId": "req-456"
 *   }
 * }
 * }</pre>
 *
 * @see AbstractStringLayout
 * @since 1.0.0
 */
@Plugin(name = "JsonLayout",
        category = Node.CATEGORY,
        elementType = Layout.ELEMENT_TYPE,
        printObject = true)
public class JsonLayout extends AbstractStringLayout {

    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private final boolean includeTimestamp;
    private final boolean includeThreadInfo;
    private final boolean includeStackTrace;
    private final boolean includeContextData;
    private final String timestampFormat;

    /**
     * Constructs a new JsonLayout.
     *
     * @param includeTimestamp   whether to include timestamp
     * @param includeThreadInfo  whether to include thread information
     * @param includeStackTrace  whether to include stack traces
     * @param includeContextData whether to include context data (MDC)
     * @param timestampFormat    timestamp format pattern
     */
    protected JsonLayout(boolean includeTimestamp,
                         boolean includeThreadInfo,
                         boolean includeStackTrace,
                         boolean includeContextData,
                         String timestampFormat) {
        super(StandardCharsets.UTF_8);
        this.includeTimestamp = includeTimestamp;
        this.includeThreadInfo = includeThreadInfo;
        this.includeStackTrace = includeStackTrace;
        this.includeContextData = includeContextData;
        this.timestampFormat = timestampFormat;
    }

    /**
     * Creates and configures the ObjectMapper instance.
     *
     * @return configured ObjectMapper
     */
    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper;
    }

    /**
     * Factory method for Log4j2 plugin system.
     *
     * @param includeTimestamp   whether to include timestamp (default: true)
     * @param includeThreadInfo  whether to include thread information (default: true)
     * @param includeStackTrace  whether to include stack traces (default: true)
     * @param includeContextData whether to include context data (default: true)
     * @param timestampFormat    timestamp format pattern (default: ISO8601)
     * @return a new JsonLayout instance
     */
    @PluginFactory
    public static JsonLayout createLayout(
            @PluginAttribute(value = "includeTimestamp", defaultBoolean = true) boolean includeTimestamp,
            @PluginAttribute(value = "includeThreadInfo", defaultBoolean = true) boolean includeThreadInfo,
            @PluginAttribute(value = "includeStackTrace", defaultBoolean = true) boolean includeStackTrace,
            @PluginAttribute(value = "includeContextData", defaultBoolean = true) boolean includeContextData,
            @PluginAttribute(value = "timestampFormat",
                    defaultString = "yyyy-MM-dd'T'HH:mm:ss.SSSZ") String timestampFormat) {

        return new JsonLayout(
                includeTimestamp,
                includeThreadInfo,
                includeStackTrace,
                includeContextData,
                timestampFormat
        );
    }

    /**
     * Converts a LogEvent to a JSON string.
     *
     * @param event the log event to convert
     * @return JSON string representation
     */
    @Override
    public String toSerializable(LogEvent event) {
        if (event == null) {
            return "{\"error\":\"LogEvent is null\"}";
        }

        try {
            ObjectNode json = OBJECT_MAPPER.createObjectNode();

            if (event.getLevel() != null) {
                json.put("level", event.getLevel().name());
            } else {
                json.put("level", "UNKNOWN");
            }

            if (event.getLoggerName() != null) {
                json.put("logger", event.getLoggerName());
            } else {
                json.put("logger", "");
            }

            if (event.getMessage() != null) {
                Message message = event.getMessage();
                try {
                    json.put("message", message.getFormattedMessage());
                } catch (Exception e) {
                    json.put("message", "Error getting message: " + e.getMessage());
                }
            } else {
                json.put("message", "");
            }

            if (includeTimestamp) {
                addTimestamp(json, event);
            }

            if (includeThreadInfo) {
                addThreadInfo(json, event);
            }

            if (includeStackTrace) {
                addThrowableInfo(json, event);
            }

            if (includeContextData) {
                addContextData(json, event);
            }

            addStructuredMessage(json, event);

            return OBJECT_MAPPER.writeValueAsString(json);

        } catch (Exception e) {
            try {
                ObjectNode errorJson = OBJECT_MAPPER.createObjectNode();
                errorJson.put("error", "Failed to serialize log event");
                errorJson.put("errorMessage", e.getMessage());
                errorJson.put("timestamp", System.currentTimeMillis());
                return OBJECT_MAPPER.writeValueAsString(errorJson);
            } catch (Exception ex) {
                return "{\"error\":\"Double serialization error\"}";
            }
        }
    }

    /**
     * Adds timestamp to JSON object.
     *
     * @param json JSON object to add to
     * @param event log event containing timestamp
     */
    private void addTimestamp(ObjectNode json, LogEvent event) {
        try {
            if ("epoch".equals(timestampFormat)) {
                json.put("timestamp", event.getTimeMillis() / 1000.0);
            } else if ("epoch_millis".equals(timestampFormat)) {
                json.put("timestamp", event.getInstant().getEpochMillisecond());
            } else {
                try {
                    Instant instant = Instant.ofEpochMilli(event.getTimeMillis());
                    ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timestampFormat);
                    String formatted = formatter.format(zonedDateTime);
                    json.put("timestamp", formatted);
                } catch (IllegalArgumentException e) {
                    LOGGER.warn("Invalid timestamp format '{}', using default ISO format", timestampFormat, e);
                    json.put("timestamp",
                            DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(event.getTimeMillis())));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error formatting timestamp", e);
            json.put("timestamp", String.valueOf(event.getTimeMillis()));
        }
    }

    /**
     * Adds thread information to JSON object.
     *
     * @param json  JSON object to add to
     * @param event log event containing thread info
     */
    private void addThreadInfo(ObjectNode json, LogEvent event) {
        json.put("thread", event.getThreadName());
        json.put("threadId", event.getThreadId());
        json.put("threadPriority", event.getThreadPriority());
    }

    /**
     * Adds throwable information to JSON object.
     *
     * @param json  JSON object to add to
     * @param event log event containing throwable
     */
    private void addThrowableInfo(ObjectNode json, LogEvent event) {
        ThrowableProxy throwableProxy = event.getThrownProxy();
        if (throwableProxy != null) {
            ObjectNode throwableJson = OBJECT_MAPPER.createObjectNode();
            throwableJson.put("message", throwableProxy.getMessage());
            throwableJson.put("class", throwableProxy.getName());
            throwableJson.put("stackTrace", throwableProxy.getExtendedStackTraceAsString());

            ThrowableProxy cause = throwableProxy.getCauseProxy();
            if (cause != null) {
                ObjectNode causeJson = OBJECT_MAPPER.createObjectNode();
                causeJson.put("message", cause.getMessage());
                causeJson.put("class", cause.getName());
                throwableJson.set("cause", causeJson);
            }

            json.set("throwable", throwableJson);
        }
    }

    /**
     * Adds context data (MDC) to JSON object.
     *
     * @param json  JSON object to add to
     * @param event log event containing context data
     */
    private void addContextData(ObjectNode json, LogEvent event) {
        try {
            if (event.getContextData() != null) {
                ObjectNode contextNode = OBJECT_MAPPER.createObjectNode();

                event.getContextData().forEach((key, value) -> {
                    if (key != null && value != null) {
                        try {
                            contextNode.put(key, String.valueOf(value));
                        } catch (Exception e) {
                        }
                    }
                });

                if (contextNode.size() > 0) {
                    json.set("context", contextNode);
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to add context data: {}", e.getMessage());
        }
    }

    /**
     * Adds structured message data to JSON object.
     *
     * @param json  JSON object to add to
     * @param event log event containing structured message
     */
    private void addStructuredMessage(ObjectNode json, LogEvent event) {
        Message message = event.getMessage();
        if (message instanceof org.apache.logging.log4j.message.MapMessage) {
            try {
                org.apache.logging.log4j.message.MapMessage<?, ?> mapMessage =
                        (org.apache.logging.log4j.message.MapMessage<?, ?>) message;

                ObjectNode dataNode = OBJECT_MAPPER.createObjectNode();

                mapMessage.forEach((key, value) -> {
                    if (key != null && value != null) {
                        try {
                            // Пробуем разные типы данных
                            if (value instanceof String) {
                                dataNode.put(key, (String) value);
                            } else if (value instanceof Number) {
                                if (value instanceof Integer) {
                                    dataNode.put(key, (Integer) value);
                                } else if (value instanceof Long) {
                                    dataNode.put(key, (Long) value);
                                } else if (value instanceof Double) {
                                    dataNode.put(key, (Double) value);
                                } else if (value instanceof Float) {
                                    dataNode.put(key, (Float) value);
                                } else {
                                    dataNode.put(key, value.toString());
                                }
                            } else if (value instanceof Boolean) {
                                dataNode.put(key, (Boolean) value);
                            } else {
                                dataNode.put(key, value.toString());
                            }
                        } catch (Exception e) {
                            LOGGER.warn("Failed to serialize field {}: {}", key, e.getMessage());
                        }
                    }
                });

                if (dataNode.size() > 0) {
                    json.set("data", dataNode);
                }

            } catch (Exception e) {
                LOGGER.warn("Failed to process MapMessage: {}", e.getMessage());
            }
        }
    }
    /**
     * Converts a LogEvent to a byte array.
     *
     * @param event the log event to convert
     * @return UTF-8 byte array representation
     */
    @Override
    public byte[] toByteArray(LogEvent event) {
        return toSerializable(event).getBytes(getCharset());
    }

    /**
     * Gets the content type of this layout.
     *
     * @return "application/json"
     */
    @Override
    public String getContentType() {
        return "application/json";
    }

    /**
     * Gets whether timestamp is included.
     *
     * @return true if timestamp is included
     */
    public boolean isIncludeTimestamp() {
        return includeTimestamp;
    }

    /**
     * Gets whether thread info is included.
     *
     * @return true if thread info is included
     */
    public boolean isIncludeThreadInfo() {
        return includeThreadInfo;
    }

    /**
     * Gets whether stack trace is included.
     *
     * @return true if stack trace is included
     */
    public boolean isIncludeStackTrace() {
        return includeStackTrace;
    }

    /**
     * Gets whether context data is included.
     *
     * @return true if context data is included
     */
    public boolean isIncludeContextData() {
        return includeContextData;
    }

    /**
     * Gets the timestamp format.
     *
     * @return timestamp format string
     */
    public String getTimestampFormat() {
        return timestampFormat;
    }

    /**
     * Returns a string representation of this layout.
     *
     * @return string representation
     */
    @Override
    public String toString() {
        return "JsonLayout{" +
               "includeTimestamp=" + includeTimestamp +
               ", includeThreadInfo=" + includeThreadInfo +
               ", includeStackTrace=" + includeStackTrace +
               ", includeContextData=" + includeContextData +
               ", timestampFormat='" + timestampFormat + '\'' +
               '}';
    }

}
