package org.sunbird.dp.rating.function

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.dp.rating.domain.Event
import org.sunbird.dp.rating.task.AssessmentProgressConfig
import org.sunbird.dp.rating.util.{AssessmentUtils, RestApiUtil}

class AssessmentProgressFunction(config: AssessmentProgressConfig, @transient var cassandraUtil: CassandraUtil = null)(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[AssessmentProgressFunction])

  private var restApiUtil:RestApiUtil=_

  override def metricsList(): List[String] = {
    List()
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    restApiUtil=new RestApiUtil()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    val key=event.key
    val readAssessment=new ReadAssessmentFunction(config, event)
    val readQuestionList=new ReadQuestionListFunction(config,event)
    try{
      if(StringUtils.isNotBlank(key)){
        if(key.equalsIgnoreCase(config.READ_ASSESSMENT)){
         readAssessment.initiateReadAssessmentFunction()
        }else if(key.equalsIgnoreCase(config.READ_QUESTION_LIST)){
          readQuestionList.initiateReadQuestionListFunction()
        }
      }else{
        logger.info("Please Provide key value")
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        context.output(config.failedEvent, event)
        logger.info("Event throwing exception: ", ex.getMessage)
      }
    }
  }
}

