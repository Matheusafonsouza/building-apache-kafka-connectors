package com.acme.kafka.connect.sample;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SampleSourceTaskTest {

    private static final String TOPIC = "test";
    private static final String FIRST_PARAMETER = "kafka";
    private static final String SECOND_PARAMETER = "connect";

    private Map<String, String> config;
    private OffsetStorageReader offsetStorageReader;
    private SourceTaskContext context;
    private SampleSourceTask task;

    @BeforeEach
    public void setup() throws IOException {
        config = new HashMap<>();
        config.put(SampleSourceConnector.FIRST_PARAMETER, FIRST_PARAMETER);
        config.put(SampleSourceConnector.SECOND_PARAMETER, SECOND_PARAMETER);
        config.put(SampleSourceConnector.TOPIC_CONFIG, TOPIC);
        task = new SampleSourceTask();
        context = mock(SourceTaskContext.class);
        task.initialize(context);
    }

}