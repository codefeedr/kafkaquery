package org.kafkaquery.util

import org.apache.flink.configuration.{
  ConfigUtils,
  Configuration,
  DeploymentOptions,
  PipelineOptions
}

import java.net.{URL, URLClassLoader}

object StreamEnvConfigurator {

  def withClassLoader(functionClassLoader: URLClassLoader): Configuration = {
    val config = new Configuration()
    config.setBoolean(PipelineOptions.OBJECT_REUSE, true)

    config.set(DeploymentOptions.TARGET, "local")
    config.setBoolean(DeploymentOptions.ATTACHED, true)

    ConfigUtils.encodeArrayToConfig(
      config,
      PipelineOptions.CLASSPATHS,
      functionClassLoader.getURLs.clone(),
      (url: URL) => url.toString
    )

    config
  }

}
