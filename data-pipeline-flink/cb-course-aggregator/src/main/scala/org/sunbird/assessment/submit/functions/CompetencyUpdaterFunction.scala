package org.sunbird.assessment.submit.functions


import com.google.gson.Gson
import org.apache.commons.collections.MapUtils
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
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
import scala.collection.convert.ImplicitConversions.`map AsJavaMap`

class CompetencyUpdaterFunction(config: AssessmentConfig,
                                @transient var cassandraUtil: CassandraUtil = null
                               )(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CompetencyUpdaterFunction])
  private var restApiUtil: RestApiUtil = _

  override def metricsList() = List(config.updateCount, config.failedEventCount)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    restApiUtil = new RestApiUtil()
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
    logger.info("Received Event in CompetencyUpdaterFunction. Event: " + event.toString)
    try {
      if (event.actor.get(config.id).equalsIgnoreCase(config.CERTIFICATE_GENERATOR)) {
        val userIds = event.edata.get(config.USER_IDS).asInstanceOf[util.List[String]]
        //TODO -- need to loop through the list of received userIds
        val userId = userIds.get(0)
        val courseId = event.edata.get(config.COURSE_ID)
        val courseCompetencies = enrichCompetencyV3(courseId).asInstanceOf[util.List[util.Map[String, Any]]]
        val profileDetails = getUserProfileDetails(userId).asInstanceOf[util.Map[String, Any]]

        if (profileDetails.containsKey(config.competencies)) {
          var userCompetencies: util.List[Map[String, Any]] = profileDetails.getOrDefault(config.competencies, null).asInstanceOf[util.List[Map[String, Any]]]
          val userCompetenciesMapJava = new java.util.HashMap[String, Object]()

          userCompetencies.forEach(content => {
            userCompetenciesMapJava.put(content.get(config.id).asInstanceOf[String], content)
          })

          val userCompetenciesMap: Map[String, Any] = userCompetenciesMapJava.asInstanceOf[util.Map[String, Any]]

          courseCompetencies.forEach(courseCompetency => {
            var competencyId: String = courseCompetency.get(config.id).asInstanceOf[String]
            var courseLevelValue: String = courseCompetency.get(config.selectedLevelLevel).asInstanceOf[String]
            var courseLevelName: String = courseCompetency.get(config.selectedLevelName).asInstanceOf[String]
            val competencyLevel = courseLevelValue.split(" ")(1)
            val competencyIntLevel = competencyLevel.toInt
            if (userCompetenciesMap.containsKey(competencyId)) {
              var userCompetency: Map[String, Any] = userCompetenciesMap.get(competencyId).asInstanceOf[Map[String, Any]]
              var userLevelValue: String = userCompetency.get(config.competencyCBPCompletionLevel).asInstanceOf[String]
              var userLevel: Int = 0
              if (null != userLevelValue) {
                var userLevel: Int = userLevelValue.toInt

                if (null != competencyIntLevel) {
                  if (competencyIntLevel > userLevel) {
                    userCompetency.put(config.competencyCBPCompletionLevel, competencyLevel)
                    userCompetency.put(config.competencyCBPCompletionLevelValue, courseLevelValue)
                    userCompetency.put(config.competencyCBPCompletionLevelName, courseLevelName)
                  }
                }
              } else {
                userCompetency.put(config.competencyCBPCompletionLevel, competencyLevel)
                userCompetency.put(config.competencyCBPCompletionLevelValue, courseLevelValue)
                userCompetency.put(config.competencyCBPCompletionLevelName, courseLevelName)
              }
            } else {
              userCompetencies.add(createCompetencyMap(courseCompetency))
            }
            profileDetails.put(config.competencies, userCompetencies)
          })
        } else {
          var userCompetenciesList: util.List[Map[String, Any]] = new util.ArrayList[Map[String, Any]]()
          courseCompetencies.forEach(courseCompetency => {
            userCompetenciesList.add(createCompetencyMap(courseCompetency))
          })
          profileDetails.put(config.competencies, userCompetenciesList)
        }
        updateProfileDetails(profileDetails, userId)
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.error(s"Passbook update Failed with exception ${ex.getMessage}:")
        event.markFailed(ex.getMessage)
        context.output(config.failedEventsOutputTag, event)
        metrics.incCounter(config.failedEventCount)
    }
  }

  def enrichCompetencyV3(courseId: Any): util.List[Any] = {
    try {
      var competencyV3 = new util.ArrayList[Any]()
      val filters = new util.HashMap[String, Any]()
      filters.put(config.IDENTIFIER, courseId)
      filters.put(config.STATUS, new util.ArrayList[String]() {
        add("Live")
      })
      filters.put(config.PRIMARY_CATEGORY, new util.ArrayList[String]() {
        add("Course")
      })
      val sortBy = new util.HashMap[String, Any]()
      sortBy.put(config.lastUpdatedOn, config.DESC)
      val request = new util.HashMap[String, Any]()
      request.put(config.FILTERS, filters)
      request.put(config.SORT_BY, sortBy)
      request.put(config.FIELDS, config.competencies_v3)
      val requestBody = new util.HashMap[String, Any]()
      requestBody.put(config.REQUEST, request)
      val url: String = config.KM_BASE_HOST + config.content_search
      val obj = restApiUtil.post(url, requestBody)
      val gson = new Gson()
      val response = gson.fromJson(obj, classOf[util.Map[String, Any]])
      if (MapUtils.isNotEmpty(response)) {
        val result = response.get(config.RESULT).asInstanceOf[util.Map[String, Any]]
        if (result.get(config.CONTENT) != null) {
          val contentList: util.List[util.Map[String, Any]] = result.get(config.CONTENT).asInstanceOf[util.List[util.Map[String, Any]]]
          competencyV3 = contentList.get(0).get(config.competencies_v3).asInstanceOf[util.ArrayList[Any]]
        }
      }
      competencyV3
    }
    catch {      case e: Exception => e.printStackTrace()
      logger.error(String.format("Failed during fetching mail %s", e.getMessage()))
      null
    }
  }

  def getUserProfileDetails(userId: String): util.Map[String, Any] = {
    val query = QueryBuilder.select(config.profileDetails)
      .from(config.dbSunbirdKeyspace, config.userTable)
      .where(QueryBuilder.eq(config.userId, userId)).allowFiltering().toString
    val record = cassandraUtil.findOne(query)
    val profileDetails = record.getString(config.profileDetails)
    if (profileDetails != null && !profileDetails.isEmpty) {
      val mapper = new ObjectMapper
      val profileDetailsMapper = mapper.readValue(profileDetails, classOf[Map[String, Any]])
      profileDetailsMapper
    } else {
      return null
    }
  }

  def updateProfileDetails(profileDetails: util.Map[String, Any], userId: String): Unit = {
    try {
      val mapper = new ObjectMapper
      val query = QueryBuilder.update(config.dbSunbirdKeyspace, config.userTable)
        .`with`(QueryBuilder.set(config.profileDetails, mapper.writeValueAsString(profileDetails)))
        .where(QueryBuilder.eq(config.id, userId))
      cassandraUtil.upsert(query.toString)
    } catch {
      case exception: Exception =>
        exception.printStackTrace()
        logger.error("Updating Profile Details Failed :" + exception.getMessage())
    }
  }

  def createCompetencyMap(competencyMap :Map[String, Any] ): Map[String, Any] = {
    var newCompetencyMap: Map[String, Any] = new util.HashMap()
    try {
      newCompetencyMap.put(config.id, competencyMap.get(config.id).asInstanceOf[String])
      newCompetencyMap.put(config.name, competencyMap.get(config.name).asInstanceOf[String])
      newCompetencyMap.put(config.description, competencyMap.get(config.description).asInstanceOf[String])
      newCompetencyMap.put(config.types, config.competency)
      newCompetencyMap.put(config.source, competencyMap.get(config.source).asInstanceOf[String])
      newCompetencyMap.put(config.competencyCBPCompletionLevel,  competencyMap.get(config.selectedLevelLevel).asInstanceOf[String].split(" ")(1))
      newCompetencyMap.put(config.competencyCBPCompletionLevelValue, competencyMap.get(config.selectedLevelLevel).asInstanceOf[String])
      newCompetencyMap.put(config.competencyCBPCompletionLevelName, competencyMap.get(config.selectedLevelName).asInstanceOf[String])
      newCompetencyMap.put(config.osid, java.util.UUID.randomUUID.toString)
    } catch {
      case exception: Exception =>
        exception.printStackTrace()
        logger.error("New Competency map creation failed :" + exception.getMessage())
    }
    newCompetencyMap
  }
}

