package com.acme.kafka.connect.sample;

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleSinkConnectorTest {

    private static final String MULTIPLE_TOPICS = "test1,test2";
    private static final String FIRST_PARAMETER = "kafka";
    private static final String SECOND_PARAMETER = "connect";

    private SampleSinkConnector connector;
    private ConnectorContext ctx;
    private Map<String, String> sinkProperties;

    @BeforeEach
    public void setup() {
        connector = new SampleSinkConnector();
        ctx = mock(ConnectorContext.class);
        connector.initialize(ctx);

        sinkProperties = new HashMap<>();
        sinkProperties.put(SinkConnector.TOPICS_CONFIG, MULTIPLE_TOPICS);
        sinkProperties.put(SampleSinkConnector.FIRST_PARAMETER, FIRST_PARAMETER);
        sinkProperties.put(SampleSinkConnector.SECOND_PARAMETER, SECOND_PARAMETER);
    }

    @Test
    public void testConnectorConfigValidation() {
        List<ConfigValue> configValues = connector.config().validate(sinkProperties);
        for (ConfigValue val : configValues) {
            assertEquals(0, val.errorMessages().size(), "Config property errors: " + val.errorMessages());
        }
    }

    @Test
    public void testSinkTasks() {
        connector.start(sinkProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertEquals(FIRST_PARAMETER, taskConfigs.get(0).get(SampleSinkConnector.FIRST_PARAMETER));
    }

    @Test
    public void testTaskClass() {
        connector.start(sinkProperties);
        assertEquals(SampleSinkTask.class, connector.taskClass());
    }
}