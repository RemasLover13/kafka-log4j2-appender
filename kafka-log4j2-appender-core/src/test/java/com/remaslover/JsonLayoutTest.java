package com.remaslover;

import org.junit.jupiter.api.Test;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.jupiter.api.BeforeEach;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link JsonLayout}.
 *
 * <p>Tests JSON serialization of log events with various configurations.</p>
 */
class JsonLayoutTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    /**
     * Tests basic JSON serialization.
     */
    @Test
    void testBasicSerialization() throws Exception {
        JsonLayout layout = JsonLayout.createLayout(true, true, true, true, "epoch_millis");

        LogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Test")
                .setLevel(Level.INFO)
                .setMessage(new SimpleMessage("Test message"))
                .setTimeMillis(System.currentTimeMillis())
                .build();

        String json = layout.toSerializable(event);
        assertNotNull(json, "JSON should not be null");

        JsonNode node = objectMapper.readTree(json);
        assertEquals("INFO", node.get("level").asText());
        assertEquals("com.example.Test", node.get("logger").asText());
        assertEquals("Test message", node.get("message").asText());
        assertTrue(node.get("timestamp").isNumber());
    }

    /**
     * Tests JSON serialization with exception.
     */
    @Test
    void testSerializationWithException() throws Exception {
        JsonLayout layout = JsonLayout.createLayout(true, true, true, true, "epoch_millis");

        Exception exception = new RuntimeException("Test exception");
        LogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Test")
                .setLevel(Level.ERROR)
                .setMessage(new SimpleMessage("Error occurred"))
                .setThrown(exception)
                .setTimeMillis(System.currentTimeMillis())
                .build();

        String json = layout.toSerializable(event);
        JsonNode node = objectMapper.readTree(json);

        assertEquals("ERROR", node.get("level").asText());
        assertNotNull(node.get("throwable"), "Throwable should be included");
        assertEquals("Test exception", node.get("throwable").get("message").asText());
        assertEquals("java.lang.RuntimeException", node.get("throwable").get("class").asText());
    }

    /**
     * Tests JSON serialization without timestamp.
     */
    @Test
    void testSerializationWithoutTimestamp() throws Exception {
        JsonLayout layout = JsonLayout.createLayout(false, true, true, true, "epoch_millis");

        LogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Test")
                .setLevel(Level.INFO)
                .setMessage(new SimpleMessage("Test message"))
                .setTimeMillis(System.currentTimeMillis())
                .build();

        String json = layout.toSerializable(event);
        JsonNode node = objectMapper.readTree(json);

        assertNull(node.get("timestamp"), "Timestamp should not be included");
        assertEquals("INFO", node.get("level").asText());
    }

    /**
     * Tests JSON serialization without thread info.
     */
    @Test
    void testSerializationWithoutThreadInfo() throws Exception {
        JsonLayout layout = JsonLayout.createLayout(true, false, true, true, "epoch_millis");

        LogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Test")
                .setLoggerFqcn(getClass().getName())
                .setLevel(Level.INFO)
                .setMessage(new SimpleMessage("Test message"))
                .setThreadName("main")
                .setThreadId(1L)
                .setThreadPriority(5)
                .setTimeMillis(System.currentTimeMillis())
                .build();

        String json = layout.toSerializable(event);
        JsonNode node = objectMapper.readTree(json);

        assertNull(node.get("thread"), "Thread should not be included");
        assertNull(node.get("threadId"), "ThreadId should not be included");
        assertNull(node.get("threadPriority"), "ThreadPriority should not be included");
        assertEquals("INFO", node.get("level").asText());
    }

    /**
     * Tests JSON serialization with custom timestamp format.
     */
    @Test
    void testCustomTimestampFormat() throws Exception {
        JsonLayout layout = JsonLayout.createLayout(true, true, true, true, "yyyy-MM-dd");

        LogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Test")
                .setLevel(Level.INFO)
                .setMessage(new SimpleMessage("Test message"))
                .setTimeMillis(System.currentTimeMillis())
                .build();

        String json = layout.toSerializable(event);
        JsonNode node = objectMapper.readTree(json);

        assertTrue(node.get("timestamp").isTextual(), "Timestamp should be string");
        String timestamp = node.get("timestamp").asText();
        assertEquals(10, timestamp.length(), "Timestamp should be yyyy-MM-dd format");
    }

    /**
     * Tests JSON serialization with context data (MDC).
     */
    @Test
    void testSerializationWithMessageData() throws Exception {
        JsonLayout layout = JsonLayout.createLayout(true, true, true, true, "epoch_millis");

        LogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Test")
                .setLevel(Level.INFO)
                .setMessage(new SimpleMessage("Test message"))
                .setTimeMillis(System.currentTimeMillis())
                .build();


        String json = layout.toSerializable(event);
        JsonNode node = objectMapper.readTree(json);

        assertNotNull(node.get("message"), "Context should be included");
    }

    /**
     * Tests error handling during serialization.
     */
    @Test
    void testErrorHandling() {
        JsonLayout layout = JsonLayout.createLayout(true, true, true, true, "epoch_millis");

        String result = layout.toSerializable(null);
        assertNotNull(result, "Should return error JSON");
        assertTrue(result.contains("error"), "Should contain error message");
    }

    /**
     * Tests getters return correct values.
     */
    @Test
    void testGetters() {
        JsonLayout layout = JsonLayout.createLayout(
                true,
                false,
                true,
                false,
                "yyyy-MM-dd"
        );

        assertTrue(layout.isIncludeTimestamp());
        assertFalse(layout.isIncludeThreadInfo());
        assertTrue(layout.isIncludeStackTrace());
        assertFalse(layout.isIncludeContextData());
        assertEquals("yyyy-MM-dd", layout.getTimestampFormat());
    }

    /**
     * Tests toString method.
     */
    @Test
    void testToString() {
        JsonLayout layout = JsonLayout.createLayout(true, true, true, true, "epoch_millis");
        String str = layout.toString();

        assertTrue(str.contains("JsonLayout"), "Should contain class name");
        assertTrue(str.contains("includeTimestamp=true"), "Should contain field values");
    }

    /**
     * Tests content type.
     */
    @Test
    void testContentType() {
        JsonLayout layout = JsonLayout.createLayout(true, true, true, true, "epoch_millis");
        assertEquals("application/json", layout.getContentType());
    }
}
