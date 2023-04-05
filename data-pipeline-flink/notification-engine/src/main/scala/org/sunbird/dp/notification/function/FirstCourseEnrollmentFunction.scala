package org.sunbird.dp.notification.function

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.gson.Gson
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.dp.notification.domain.Event
import org.sunbird.dp.notification.task.NotificationEngineConfig
import org.sunbird.dp.notification.util.{KafkaMessageGenerator, RestApiUtil, UserUtilityService}

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import java.util.concurrent.TimeUnit

class FirstCourseEnrollmentFunction(enrollmentConfig: NotificationEngineConfig)(implicit val mapTypeInfo: TypeInformation[Event]) {

  private[this] val logger = LoggerFactory.getLogger(classOf[FirstCourseEnrollmentFunction])

  private var restApiUtil: RestApiUtil = new RestApiUtil()
  private var cassandraUtil = new CassandraUtil(enrollmentConfig.dbHost, enrollmentConfig.dbPort)

  private val userUtil = new UserUtilityService(enrollmentConfig)

  private val kafkaMessageGenerator = new KafkaMessageGenerator(enrollmentConfig)

  def initiateFirstCourseEnrolmentNotification(data: util.HashMap[String, Any]): Unit = {
    logger.info("Entering to First Course Enrolment Notification")
    try {
      val startTime = System.currentTimeMillis()
      val userId = data.get("userId").toString
      val courseId = data.get("courseId").toString
      val query = QueryBuilder.select().countAll().from(enrollmentConfig.dbCoursesKeyspace, enrollmentConfig.courseTable)
        .where(QueryBuilder.eq("userid", userId)).toString
      val rowCount = cassandraUtil.findOne(query);
      val count = rowCount.get(0, classOf[Long])
      if (count == 1) {
        val userDetails = getUserDetails(userId)
        val courseName = getCourseNameById(courseId)
        if (MapUtils.isNotEmpty(userDetails) && StringUtils.isNotEmpty(courseName)) {
          configureParamAndSendMessageToKafka(userDetails, courseName)
        }
      }
      val endTime = System.currentTimeMillis()
      val elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)
      logger.info(s"Completed Operation in $elapsedSeconds seconds")
    }
    catch {
      case ex: Exception => {
        logger.error("Event throwing exception: "+ ex.getMessage)
      }
    }
  }

  def getUserDetails(userId: String): util.HashMap[String, Any] = {
    logger.info("Entering getUserDetails")
    var userMap = new util.HashMap[String, Any]()
    try {
      var response = new util.ArrayList[util.HashMap[String, Any]]()
      val query: BoolQueryBuilder = QueryBuilders.boolQuery()
      query.must(QueryBuilders.matchQuery(enrollmentConfig.STATUS, 1))
        .must(QueryBuilders.matchQuery(enrollmentConfig.IS_DELETED, false))
        .must(QueryBuilders.matchQuery(enrollmentConfig.userId, userId))
      val sourceBuilder = new SearchSourceBuilder().query(query)
      sourceBuilder.fetchSource(enrollmentConfig.fields.split(",", -1), null)
      response = userUtil.getUserRecordsFromES(enrollmentConfig.sb_es_user_profile_index, enrollmentConfig.es_profile_index_type, sourceBuilder)
      if (CollectionUtils.isNotEmpty(response)) {
        userMap = response.get(0)
      }
      userMap
    }
    catch {
      case e: Exception =>
        logger.error(String.format("Failed during fetching user details %s", e.getMessage()))
        null
    }
  }

  def getCourseNameById(courseId: String): String = {
    logger.info("Entering getCourseNameById")
    try {
      var courseName = ""
      val filters = new util.HashMap[String, Any]()
      filters.put(enrollmentConfig.IDENTIFIER, courseId)
      filters.put(enrollmentConfig.STATUS, new util.ArrayList[String]() {
        add("Live")
      })
      val request = new util.HashMap[String, Any]()
      request.put(enrollmentConfig.FILTERS, filters)
      request.put(enrollmentConfig.FIELDS, enrollmentConfig.CONTENT_SEARCH_FIELDS.split(",", -1))
      val requestBody = new util.HashMap[String, Any]()
      requestBody.put(enrollmentConfig.REQUEST, request)
      val url: String = enrollmentConfig.KM_BASE_HOST + enrollmentConfig.content_search
      val obj = restApiUtil.post(url, requestBody)
      val gson = new Gson()
      val response = gson.fromJson(obj, classOf[util.Map[String, Any]])
      if (MapUtils.isNotEmpty(response)) {
        val result = response.get(enrollmentConfig.RESULT).asInstanceOf[util.Map[String, Any]]
        if (result.get(enrollmentConfig.CONTENT) != null) {
          val contentList: util.List[util.Map[String, Any]] = result.get(enrollmentConfig.CONTENT).asInstanceOf[util.List[util.Map[String, Any]]]
          contentList.forEach(content => {
            courseName = content.get(enrollmentConfig.NAME).asInstanceOf[String]
          })
        }
      }
      courseName
    } catch {
      case e: Exception =>
        logger.error(String.format("Failed during fetching mail %s", e.getMessage()))
        null
    }
  }

  def configureParamAndSendMessageToKafka(userDetails: util.HashMap[String, Any], courseName: String): Unit = {
    logger.info("Entering configureParams")
    val param = new util.HashMap[String, Any]()
    param.put(enrollmentConfig.USER_KEYWORD + enrollmentConfig._FIRSTNAME, userDetails.get(enrollmentConfig.FIRST_NAME))
    param.put(enrollmentConfig.USER_KEYWORD + enrollmentConfig._LASTNAME, userDetails.get(enrollmentConfig.LAST_NAME))
    param.put(enrollmentConfig.COURSE_KEYWORD + enrollmentConfig._NAME, courseName)
    val now = Calendar.getInstance().getTime()
    val dateFormat=new SimpleDateFormat("dd-mm-yyyy")
    val formattedDate = dateFormat.format(now)
    param.put(enrollmentConfig.DATE, formattedDate)
    val emailList = new util.ArrayList[String]() {
      add(userDetails.get(enrollmentConfig.PRIMARY_EMAIL).toString)
    }
    kafkaMessageGenerator.initiateKafkaMessage(emailList, enrollmentConfig.FIRST_COURSE_EMAIL_TEMPLATE, param, enrollmentConfig.FIRST_COURSE_EMAIL_SUBJECT)
    logger.info("message send to kafka topic")
  }
}