package org.sunbird.assessment.submit.task

import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.assessment.submit.domain.Event
import org.sunbird.assessment.submit.functions.{AssessmentSubmitFunction, CompetencyUpdaterFunction}
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil

class AssessmentTask(config: AssessmentConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

    val competencySource = {
      kafkaConnector.kafkaEventSource[Event](config.assessmentInputTopic)
    }
    val passbookSource = {
      kafkaConnector.kafkaEventSource[Event](config.passbookInputTopic)
    }

    val competencyStream =
      env.addSource(competencySource, config.AssessmentConsumer).uid(config.AssessmentConsumer).rebalance()
        .process(new AssessmentSubmitFunction(config)).setParallelism(config.assessmentSubmitParallelism)
        .name(config.assessmentFunction).uid(config.assessmentFunction)
    competencyStream.getSideOutput(config.updateSuccessEventsOutputTag).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaSuccessTopic))
      .name(config.successIssueEventSink).uid(config.successIssueEventSink)
      .setParallelism(config.assessmentSubmitParallelism)
    competencyStream.getSideOutput(config.failedEvent).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaIssueTopic))
      .name(config.issueEventSink).uid(config.issueEventSink)
      .setParallelism(config.assessmentSubmitParallelism)

    val passbookStream =
      env.addSource(passbookSource, config.PassbookConsumer).uid(config.PassbookConsumer).rebalance()
        .process(new CompetencyUpdaterFunction(config)).setParallelism(config.passbookParallelism)
        .name(config.passbookFunction).uid(config.passbookFunction)
    passbookStream.getSideOutput(config.passbookFailedEvent).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaIssueTopic))
      .name(config.passbookIssueEventSink).uid(config.passbookIssueEventSink)
      .setParallelism(config.passbookParallelism)

    env.execute(config.jobName)
  }

}

object AssessmentTask {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("assessment-submit.conf").withFallback(ConfigFactory.systemEnvironment()))
    val assessmentConfig = new AssessmentConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(assessmentConfig)
    val task = new AssessmentTask(assessmentConfig, kafkaUtil)
    task.process()
  }
}
