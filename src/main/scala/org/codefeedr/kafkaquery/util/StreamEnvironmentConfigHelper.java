package org.codefeedr.kafkaquery.util;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment;

public class StreamEnvironmentConfigHelper {

    public static Configuration getConfig(URLClassLoader functionClassloader) {
        Configuration effectiveConfiguration = new Configuration();

        effectiveConfiguration.set(PipelineOptions.OBJECT_REUSE, true);

        effectiveConfiguration.set(DeploymentOptions.TARGET, "local");
        effectiveConfiguration.set(DeploymentOptions.ATTACHED, true);
        ConfigUtils.encodeCollectionToConfig(
            effectiveConfiguration,
            PipelineOptions.CLASSPATHS,
            Arrays.asList(functionClassloader.getURLs().clone()),
            URL::toString);
        return effectiveConfiguration;
    }
}
