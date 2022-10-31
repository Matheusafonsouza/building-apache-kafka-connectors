package com.acme.kafka.connect.sample;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class SampleSourceTask extends SourceTask {

    private String topic = null;
    private String firstParameter = null;
    private String secondParameter = null;

    @Override
    public String version() {
        return new SampleSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(SampleSourceConnector.TOPIC_CONFIG);
        firstParameter = props.get(SampleSourceConnector.FIRST_PARAMETER);
        secondParameter = props.get(SampleSourceConnector.SECOND_PARAMETER);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }

    @Override
    public void stop() {
        // Nothing to do since SampleSourceConnector has no background monitoring.
    }

}
