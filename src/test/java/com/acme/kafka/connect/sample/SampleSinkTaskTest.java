package com.acme.kafka.connect.sample;

import org.junit.jupiter.api.BeforeEach;

public class SampleSinkTaskTest {

    private SampleSinkTask task;

    @BeforeEach
    public void setup() {
        task = new SampleSinkTask();
    }

}