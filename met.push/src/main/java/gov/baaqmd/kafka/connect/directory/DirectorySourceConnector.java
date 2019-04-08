package gov.baaqmd.kafka.connect.directory;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.*;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

import gov.baaqmd.kafka.connect.utils.StringUtils;

import java.util.*;


/**
 * DirectorySourceConnector implements the connector interface
 * to write on Kafka file system events (creations, modifications etc)
 *
 */
public class DirectorySourceConnector extends SourceConnector {
    public static final String PATH_CFG = "file.path";
    public static final String FILTER_CFG = "file.ext";
    public static final String TIMER_CFG = "timer.ms";
    public static final String SCHEMA_CFG = "schema.name";
    public static final String TOPIC_CFG = "topic";

    private String path_cfg;
    private String timer_cfg;
    private String schema_cfg;
    private String filter_cfg;
    private String topic_cfg;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }


    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
        schema_cfg = props.get(SCHEMA_CFG);
        if (schema_cfg == null || schema_cfg.isEmpty())
            throw new ConnectException("missing schema.name");

        topic_cfg = props.get(TOPIC_CFG);
        if (topic_cfg == null || topic_cfg.isEmpty())
            throw new ConnectException("missing topic_cfg");

        filter_cfg = props.get(FILTER_CFG);
        if (filter_cfg == null || filter_cfg.isEmpty())
            throw new ConnectException("missing filter_cfg");

        path_cfg = props.get(PATH_CFG);
        if (path_cfg == null || path_cfg.isEmpty())
            throw new ConnectException("missing path_cfg");

        timer_cfg = props.get(TIMER_CFG);
        if (timer_cfg == null || timer_cfg.isEmpty())
            timer_cfg = "1000";
    }


    /**
     * Returns the Task implementation for this Connector.
     *
     * @return tha Task implementation Class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return DirectorySourceTask.class;
    }


    /**
     * Returns a set of configurations for the Tasks based on the current configuration.
     * It always creates a single set of configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for the Task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        List<String> dirs = Arrays.asList(path_cfg.split(","));
        for (int i = 0; i < dirs.size(); i++) {
            Map<String, String> config = new HashMap<>();
            config.put(TIMER_CFG, timer_cfg);
            config.put(SCHEMA_CFG, schema_cfg);
            config.put(TOPIC_CFG, topic_cfg);
            config.put(FILTER_CFG, filter_cfg);
            config.put(PATH_CFG, dirs.get(i));
            configs.add(config);
        }
        return configs;
    }


    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(PATH_CFG, Type.STRING, Importance.HIGH, "Source directory.")
        .define(TOPIC_CFG, Type.STRING, Importance.HIGH, "The topic_cfg to publish data to");
 
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}