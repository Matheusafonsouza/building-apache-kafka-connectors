package com.acme.kafka.connect.sample;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleSinkConnector extends SinkConnector {

    public static final String FIRST_PARAMETER = "first.required.param";
    public static final String SECOND_PARAMETER = "second.required.param";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(FIRST_PARAMETER, Type.STRING, null, Importance.HIGH, "First required parameter")
        .define(SECOND_PARAMETER, Type.STRING, null, Importance.HIGH, "Second required parameter");

    private String firstParameter;
    private String secondParameter;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        firstParameter = parsedConfig.getString(FIRST_PARAMETER);
        secondParameter = parsedConfig.getString(SECOND_PARAMETER);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SampleSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(FIRST_PARAMETER, firstParameter);
        config.put(SECOND_PARAMETER, secondParameter);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since SampleSinkConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
