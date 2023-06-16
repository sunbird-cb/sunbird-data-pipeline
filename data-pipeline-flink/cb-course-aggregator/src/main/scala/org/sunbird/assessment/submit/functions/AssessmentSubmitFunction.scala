package org.sunbird.assessment.submit.functions

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory
import org.sunbird.assessment.submit.domain.Event
import org.sunbird.assessment.submit.task.AssessmentConfig
import org.sunbird.assessment.submit.util.RestApiUtil
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.CassandraUtil

import java.util
import java.util.Map
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.language.postfixOps

class AssessmentSubmitFunction(config: AssessmentConfig,
                               @transient var cassandraUtil: CassandraUtil = null
                              )(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[AssessmentSubmitFunction])
  private var restUtil: RestUtil = _

  override def metricsList() = List(config.updateCount, config.failedEventCount)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    restUtil = new RestUtil()
  }

  override def close(): Unit = {
    super.close()
  }

  /**
   * Method to write the assess event to cassandra table
   *
   * @param event   - Assess Batch Events
   * @param context - Process Context
   */
  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {
    try {
      logger.info("Entering to assessmentSubmit: "+event.toString)
      if (!event.primaryCategory.equalsIgnoreCase(config.practiceQuestionSet)) {
        var noOfAttempts: Int = 0
        noOfAttempts = getNoOfAttemptsMadeForThisAssessment(event)
        logger.info(s"noOfAttempts ${noOfAttempts}")
        logger.info("Saving to the Assessment Aggregator Table")
        saveAssessment(noOfAttempts, event, event.totalScore)
        metrics.incCounter(config.updateCount)
        context.output(config.updateSuccessEventsOutputTag, event)
      }
      if (event.primaryCategory.equalsIgnoreCase(config.competencyAssessment) && event.totalScore >= event.passPercentage) {
        getUserCompetencies(event)
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.info(s"Assessment Submit Failed with exception ${ex.getMessage}:")
        event.markFailed(ex.getMessage)
        context.output(config.failedEventsOutputTag, event)
        metrics.incCounter(config.failedEventCount)
    }
  }

  def getNoOfAttemptsMadeForThisAssessment(event: Event): Int = {
    if (!event.primaryCategory.equalsIgnoreCase(config.competencyAssessment) && !event.courseId.isEmpty && !event.batchId.isEmpty) {
      val query = QueryBuilder.select(config.userIdKey)
        .from(config.dbCoursesKeyspace, config.table).
        where(QueryBuilder.eq(config.batchIdKey, event.batchId))
        .and(QueryBuilder.eq(config.userIdKey, event.userId))
        .and(QueryBuilder.eq(config.contentIdKey, event.contentId))
        .and(QueryBuilder.eq(config.courseIdKey, event.courseId)).allowFiltering().toString
      val records = cassandraUtil.find(query)
      if (records.isEmpty) 1 else records.size() + 1
    }
    0
  }

  def getUserCompetencies(event: Event): Boolean = {
    if (!event.userId.isEmpty && !event.assessmentId.isEmpty) {
      val query = QueryBuilder.select(config.profileDetails)
        .from(config.dbSunbirdKeyspace, config.userTable)
        .where(QueryBuilder.eq(config.userId, event.userId)).allowFiltering().toString
      val record = cassandraUtil.findOne(query)
      val profileDetails = record.getString(config.profileDetails)
      logger.info(s"profileDetails${profileDetails} :")
      if (profileDetails.isEmpty || profileDetails != null) {
        val mapper = new ObjectMapper
        val profileDetailsMapper = mapper.readValue(profileDetails, classOf[Map[String, String]])
        logger.info(s"profileDetailsMapper: ${profileDetailsMapper} :")
        var newCompetencyMap: util.Map[String, String] = new util.HashMap()
        var competencies: util.List[Map[String, String]] = profileDetailsMapper.getOrDefault(config.competencies, null).asInstanceOf[util.List[Map[String, String]]]
        logger.info(s"competencies: ${competencies} :")
        newCompetencyMap.put(config.id, event.competency.getOrDefault(config.id, ""))
        newCompetencyMap.put(config.name, event.competency.getOrDefault(config.name, ""))
        newCompetencyMap.put(config.description, event.competency.getOrDefault(config.description, ""))
        newCompetencyMap.put(config.types, event.competency.getOrDefault(config.competencyType, ""))
        newCompetencyMap.put(config.source, event.competency.getOrDefault(config.source, ""))
        newCompetencyMap.put(config.competencySelfAttestedLevelValue, event.competency.getOrDefault(config.selectedLevelLevel, ""))
        newCompetencyMap.put(config.competencySelfAttestedLevel, event.competency.getOrDefault(config.selectedLevelId, ""))
        newCompetencyMap.put(config.competencySelfAttestedLevelName, event.competency.getOrDefault(config.selectedLevelName, ""))
        newCompetencyMap.put(config.competencySelfAttestedLevelDescription, event.competency.getOrDefault(config.selectedLevelDescription, ""))
        logger.info(s"New Competency Map ${newCompetencyMap} :")
        if (competencies == null) {
          competencies = new util.ArrayList[Map[String, String]]()
        }
        var count = 0
        if (event.competency != null) {
          for (competency <- competencies) {
            if (competency.get(config.name).equalsIgnoreCase(event.competency.get(config.name)) || competency.get(config.id).equalsIgnoreCase(event.competency.get(config.id))) {
              count = count + 1
            }
          }
          if (count == 0) {
            competencies.add(newCompetencyMap)
            logger.info(s"New Competency Map With Added Competency ${competencies} :")
            profileDetailsMapper.put("competencies", competencies.toString)
            logger.info(s"Newly Updated Profile Details ${profileDetailsMapper} :")
            updateProfileDetails(profileDetailsMapper, event.userId)
          }
        }
        return true
      }
      else
        return false
    }
    false
  }

  def updateProfileDetails(profileDetailsMapper: Map[String, String], userId: String): Unit = {
    try {
      val query = QueryBuilder.update(config.dbSunbirdKeyspace, config.userTable)
        .`with`(QueryBuilder.set(config.profileDetails, profileDetailsMapper))
        .where(QueryBuilder.eq(config.id, userId)).toString
      logger.info(s"Query: ${query}")
      cassandraUtil.upsert(query)
    } catch {
      case exception: Exception =>
        exception.printStackTrace()
        logger.info(s"Updating Profile Details Failed ${exception.getMessage} :")
    }
  }

  def saveAssessment(attemptId: Int, event: Event, totalScore: Double): Unit = {
    try {
      val query = QueryBuilder.insertInto(config.dbCoursesKeyspace, config.table)
        .value(config.courseIdKey, event.courseId).value(config.batchIdKey, event.batchId).value("user_id", event.userId)
        .value(config.contentIdKey, event.contentId).value(config.attemptIdKey, attemptId.toString)
        .value(config.updatedOnKey, new DateTime(DateTimeZone.UTC).getMillis).value(config.createdOnKey, new DateTime(DateTimeZone.UTC).getMillis)
        .value(config.lastAttemptedOnKey, new DateTime(DateTimeZone.UTC).getMillis).value(config.totalScoreKey, totalScore)
        .value(config.totalMaxScoreKey, 100.0).toString
      logger.info(s"Query: ${query}")
      cassandraUtil.upsert(query)
    } catch {
      case exception: Exception =>
        exception.printStackTrace()
        logger.info(s"Assessment Saving Failed with ${exception.getMessage} :")
    }
  }
}
