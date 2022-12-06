package org.sunbird.dp.rating.task

import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil
import org.sunbird.dp.rating.domain.Event
import org.sunbird.dp.rating.function.AssessmentProgressFunction

class AssessmentProgressTask(config:AssessmentProgressConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    val source = kafkaConnector.kafkaEventSource[Event](config.inputTopic)
    val stream =
      env.addSource(source, config.AssessmentConsumer).uid(config.AssessmentConsumer).rebalance()
        .process(new AssessmentProgressFunction(config)).setParallelism(config.assessmentParallelism)
        .name(config.assessmentFunction).uid(config.assessmentFunction)
    stream.getSideOutput(config.failedEvent).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaIssueTopic))
      .name(config.issueEventSink).uid(config.issueEventSink)
      .setParallelism(config.assessmentParallelism)
    env.execute(config.jobName)
  }
}

object AssessmentProgressTask {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("assessment-progress.conf").withFallback(ConfigFactory.systemEnvironment()))
    val assessmentConfig = new AssessmentProgressConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(assessmentConfig)
    val task = new AssessmentProgressTask(assessmentConfig, kafkaUtil)
    task.process()
  }
}

