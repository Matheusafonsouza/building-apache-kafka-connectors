package com.acme.kafka.connect.sample;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleSourceConnectorTest {

    private static final String SINGLE_TOPIC = "test";
    private static final String FIRST_PARAMETER = "kafka";
    private static final String SECOND_PARAMETER = "connect";
    private static final String MULTIPLE_TOPICS = "test1,test2";

    private SampleSourceConnector connector;
    private ConnectorContext ctx;
    private Map<String, String> sourceProperties;

    @BeforeEach
    public void setup() {
        connector = new SampleSourceConnector();
        ctx = mock(ConnectorContext.class);
        connector.initialize(ctx);

        sourceProperties = new HashMap<>();
        sourceProperties.put(SampleSourceConnector.TOPIC_CONFIG, SINGLE_TOPIC);
        sourceProperties.put(SampleSourceConnector.FIRST_PARAMETER, FIRST_PARAMETER);
        sourceProperties.put(SampleSourceConnector.SECOND_PARAMETER, SECOND_PARAMETER);
    }

    @Test
    public void testConnectorConfigValidation() {
        List<ConfigValue> configValues = connector.config().validate(sourceProperties);
        for (ConfigValue val : configValues) {
            assertEquals(0, val.errorMessages().size(), "Config property errors: " + val.errorMessages());
        }
    }

    @Test
    public void testSourceTasks() {
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertEquals(FIRST_PARAMETER,
                taskConfigs.get(0).get(SampleSourceConnector.FIRST_PARAMETER));
        assertEquals(SECOND_PARAMETER,
                taskConfigs.get(0).get(SampleSourceConnector.SECOND_PARAMETER));
        assertEquals(SINGLE_TOPIC,
                taskConfigs.get(0).get(SampleSourceConnector.TOPIC_CONFIG));

        // Should be able to return fewer than requested #
        taskConfigs = connector.taskConfigs(2);
        assertEquals(1, taskConfigs.size());
        assertEquals(FIRST_PARAMETER,
                taskConfigs.get(0).get(SampleSourceConnector.FIRST_PARAMETER));
        assertEquals(SECOND_PARAMETER,
                taskConfigs.get(0).get(SampleSourceConnector.SECOND_PARAMETER));
        assertEquals(SINGLE_TOPIC,
                taskConfigs.get(0).get(SampleSourceConnector.TOPIC_CONFIG));
    }

    @Test
    public void testSourceTasksStdin() {
        sourceProperties.remove(SampleSourceConnector.FIRST_PARAMETER);
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertNull(taskConfigs.get(0).get(SampleSourceConnector.FIRST_PARAMETER));
    }

    @Test
    public void testMultipleSourcesInvalid() {
        sourceProperties.put(SampleSourceConnector.TOPIC_CONFIG, MULTIPLE_TOPICS);
        assertThrows(ConfigException.class, () -> connector.start(sourceProperties));
    }

    @Test
    public void testTaskClass() {
        connector.start(sourceProperties);
        assertEquals(SampleSourceTask.class, connector.taskClass());
    }

    @Test
    public void testMissingTopic() {
        sourceProperties.remove(SampleSourceConnector.TOPIC_CONFIG);
        assertThrows(ConfigException.class, () -> connector.start(sourceProperties));
    }

    @Test
    public void testBlankTopic() {
        // Because of trimming this tests is same as testing for empty string.
        sourceProperties.put(SampleSourceConnector.TOPIC_CONFIG, "     ");
        assertThrows(ConfigException.class, () -> connector.start(sourceProperties));
    }

}