package com.acme.kafka.connect.sample;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class SampleSinkTask extends SinkTask {

    private String firstParameter = null;
    private String secondParameter = null;

    @Override
    public String version() {
        return new SampleSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        firstParameter = props.get(SampleSinkConnector.FIRST_PARAMETER);
        secondParameter = props.get(SampleSinkConnector.SECOND_PARAMETER);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        // implement what you want here
        // for (SinkRecord record : sinkRecords) {
        // }
    }

    @Override
    public void stop() {
        // Nothing to do since SampleSourceConnector has no background monitoring.
    }

}
