package com.acme.kafka.connect.sample;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleSourceConnector extends SourceConnector {
    public static final String FIRST_PARAMETER = "first.required.param";
    public static final String SECOND_PARAMETER = "second.required.param";
    public static final String TOPIC_CONFIG = "topic";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(FIRST_PARAMETER, Type.STRING, null, Importance.HIGH, "First required parameter")
        .define(SECOND_PARAMETER, Type.STRING, null, Importance.HIGH, "Second required parameter")
        .define(TOPIC_CONFIG, Type.LIST, Importance.HIGH, "The topic to publish data to");

    private String firstParameter;
    private String secondParameter;
    private String topic;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        firstParameter = parsedConfig.getString(FIRST_PARAMETER);
        secondParameter = parsedConfig.getString(SECOND_PARAMETER);
        List<String> topics = parsedConfig.getList(TOPIC_CONFIG);
        if (topics.size() != 1) {
            throw new ConfigException("'topic' in SampleSourceConnector configuration requires definition of a single topic");
        }
        topic = topics.get(0);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SampleSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(FIRST_PARAMETER, firstParameter);
        config.put(SECOND_PARAMETER, secondParameter);
        config.put(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since SampleSourceConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
