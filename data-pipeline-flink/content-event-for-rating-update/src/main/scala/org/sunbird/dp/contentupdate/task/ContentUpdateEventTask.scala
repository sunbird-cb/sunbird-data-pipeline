package org.sunbird.dp.contentupdate.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.contentupdate.domain.Event
import org.sunbird.dp.contentupdate.function.ContentUpdateEventFunction
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil

import java.io.File

class ContentUpdateEventTask (config: ContentUpdateConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

    val source = kafkaConnector.kafkaEventSource[Event](config.inputTopic)

    val stream =
      env.addSource(source, config.ContentUpdateEventConsumer).uid(config.ContentUpdateEventConsumer).rebalance()
        .process(new ContentUpdateEventFunction(config)).setParallelism(config.contentUpdateParallelism)
        .name(config.contentUpdateFunction).uid(config.contentUpdateFunction)
    stream.getSideOutput(config.failedEvent).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaIssueTopic))
      .name(config.issueEventSink).uid(config.issueEventSink)
      .setParallelism(config.contentUpdateParallelism)
    env.setRuntimeMode(RuntimeExecutionMode.BATCH).execute(config.jobName)
  }
}

object ContentUpdateEventTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("content-update.conf").withFallback(ConfigFactory.systemEnvironment()))
    val contentConfig = new ContentUpdateConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(contentConfig)
    val task = new ContentUpdateEventTask(contentConfig, kafkaUtil)
    task.process()
  }
}

