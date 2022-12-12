package org.sunbird.dp.contentUpdate.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.contentUpdate.domain.Event
import org.sunbird.dp.contentUpdate.functions.ContentUpdaterEventFunction
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil

import java.io.File

class ContentUpdateEventStreamTask(config: ContentUpdateConfig, kafkaConnector: FlinkKafkaConnector) {

  def process():Unit={

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

    val source = kafkaConnector.kafkaEventSource[Event](config.kafkaInputTopic)

    val progressStream =
      env.addSource(source, config.contentUpdaterConsumer).uid(config.contentUpdaterConsumer)
        .setParallelism(config.kafkaConsumerParallelism).rebalance()
        .keyBy(new ContentUpdateGetKeySelector(config)).countWindow(config.thresholdBatchReadSize)
        .process(new ContentUpdaterEventFunction(config)).name(config.contentUpdaterFn).uid(config.contentUpdaterFn)
        .setParallelism(config.contentUpdaterParallelism)
    env.execute(config.jobName)
  }
}

object ContentUpdateEventStreamTask{

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("content-update-resource.conf").withFallback(ConfigFactory.systemEnvironment()))
    val  contentConfig= new ContentUpdateConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(contentConfig)
    val task = new ContentUpdateEventStreamTask(contentConfig, kafkaUtil)
    task.process()
  }
}

class ContentUpdateGetKeySelector(config: ContentUpdateConfig) extends KeySelector[Event, Int] {
  val shards = config.windowShards
  override def getKey(in: Event): Int = {
    if (Option(in.did()) == None) "".hashCode % shards else in.did().hashCode % shards
  }
}
