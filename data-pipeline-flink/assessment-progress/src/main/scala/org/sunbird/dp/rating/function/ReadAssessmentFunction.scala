package org.sunbird.dp.rating.function

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.http.HttpStatus
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.dp.rating.domain.Event
import org.sunbird.dp.rating.task.AssessmentProgressConfig
import org.sunbird.dp.rating.util.{AssessmentUtils, RestApiUtil}
import org.sunbird.dp.rating.domain.Response
import org.sunbird.dp.rating.domain.SunbirdApiRespParam

import java.sql.Timestamp
import java.util
import java.util.stream.Collectors.toList
import java.util.{Calendar, Collections, Date, UUID}

class ReadAssessmentFunction (config:AssessmentProgressConfig,event: Event)(implicit val mapTypeInfo: TypeInformation[Event]) {
  private[this] val logger = LoggerFactory.getLogger(classOf[ReadAssessmentFunction])

  var cassandraUtil: CassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  private var assessmentUtils: AssessmentUtils = new AssessmentUtils(config)

  def initiateReadAssessmentFunction(): Unit = {
    var sunbirdApiRespParam = SunbirdApiRespParam()
    var response = Response(result = new util.HashMap[String, Any]())
    sunbirdApiRespParam.copy(status = config.SUCCESS)
    sunbirdApiRespParam.copy(resmsgid = UUID.randomUUID().toString)
    response.copy(params = sunbirdApiRespParam)
    response.copy(responseCode = HttpStatus.SC_OK)
    logger.info("event data " + event.toString)
    var errorMessage = new String()
    try {
      val userId = event.userId
      val token = event.token
      val assessmentIdentifier = event.assessmentIdentifier
      if (userId != null) {
        val assessmentAllDetail = new util.HashMap[String, Any]()
        errorMessage = assessmentUtils.fetchReadHierarchyDetails(assessmentAllDetail, token, assessmentIdentifier)
        logger.info("errorMessage " + errorMessage)
        if (errorMessage.isEmpty && !assessmentAllDetail.get(config.PRIMARY_CATEGORY).asInstanceOf[String].equalsIgnoreCase(config.PRACTICE_QUESTION_SET)) {
          logger.info("fetched assessment details for:" + assessmentIdentifier)
          val existingDataList = assessmentUtils.fetchUserAssessmentDataFromDB(userId, assessmentIdentifier)
          val assessmentStartTime = new Timestamp(new Date().getTime)
          logger.info("assessment start time " + assessmentStartTime)
          if (existingDataList.isEmpty) {
            logger.info("if existing data is empty")
            logger.info("Assessment read first time for the user")
            //response.result.put(config.QUESTION_SET,readAssessmentLevelData(assessmentAllDetail))
            response = Response(result = new util.HashMap[String, Any]() {
              put(config.QUESTION_SET, readAssessmentLevelData(assessmentAllDetail))
            })
            //TODO - Figure out how to set response of the object we want to pass
            val expectedDuration = assessmentAllDetail.get(config.EXPECTED_DURATION).asInstanceOf[Int]
            val isAssessmentUpdatedToDB: Boolean = addUserAssessmentDataToDB(userId, assessmentIdentifier, assessmentStartTime,
              calculateAssessmentSubmitTime(expectedDuration, assessmentStartTime), response.result.get(config.QUESTION_SET).asInstanceOf[util.HashMap[String, Any]], config.NOT_SUBMITTED)
            if (isAssessmentUpdatedToDB.equals(false)) {
              errorMessage = config.ASSESSMENT_DATA_START_TIME_NOT_UPDATED
            }
          }
          else {
            logger.info("Assessment read... user has details...")
            //TODO
            val existingAssessmentEndTime = existingDataList.get(0).get(config.END_TIME, classOf[Date])
            logger.info("existingAssessmentEndTime " + existingAssessmentEndTime)
            val time: Int = assessmentStartTime.compareTo(existingAssessmentEndTime)
            logger.info("time " + time)
            if (time < 0 && existingDataList.get(0).get(config.STATUS, classOf[String]).equalsIgnoreCase(config.NOT_SUBMITTED)) {
              logger.info("if block of time condition")
              val questionSetFromAssessmentString = existingDataList.get(0).get(config.ASSESSMENT_READ_RESPONSE, classOf[String])
              logger.info("questionSetFromAssessmentString " + questionSetFromAssessmentString)
              val questionSetFromAssessment: util.Map[String, Any] = new Gson().fromJson(questionSetFromAssessmentString,
                new TypeToken[util.Map[String, Any]]() {}.getType())
              logger.info("questionSetFromAssessment " + questionSetFromAssessment)
              response = Response(result = new util.HashMap[String, Any]() {
                put(config.QUESTION_SET, questionSetFromAssessment)
              })
              //response.result.put(config.QUESTION_SET,questionSetFromAssessment)
            } else {
              logger.info("Assessment read... adding user data to db...")
              //response.result.put(config.QUESTION_SET,readAssessmentLevelData(assessmentAllDetail))
              response = Response(result = new util.HashMap[String, Any]() {
                put(config.QUESTION_SET, readAssessmentLevelData(assessmentAllDetail))
              })
              logger.info("read assessment " + readAssessmentLevelData(assessmentAllDetail))
              val expectedDuration = assessmentAllDetail.get(config.EXPECTED_DURATION).asInstanceOf[Double]
              val isAssessmentUpdatedToDB: Boolean = addUserAssessmentDataToDB(userId, assessmentIdentifier, assessmentStartTime,
                calculateAssessmentSubmitTime(expectedDuration.toInt, assessmentStartTime),
                response.result.get(config.QUESTION_SET).asInstanceOf[util.HashMap[String, Any]],
                config.NOT_SUBMITTED)
              if (false.equals(isAssessmentUpdatedToDB)) {
                errorMessage = config.ASSESSMENT_DATA_START_TIME_NOT_UPDATED
              }
            }
          }
        } else if (errorMessage.isEmpty && assessmentAllDetail.get(config.PRIMARY_CATEGORY).asInstanceOf[String].equalsIgnoreCase(config.PRACTICE_QUESTION_SET)) {
          response = Response(result = new util.HashMap[String, Any]() {
            put(config.QUESTION_SET, readAssessmentLevelData(assessmentAllDetail))
          })
          //response.result.put(config.QUESTION_SET,readAssessmentLevelData(assessmentAllDetail))
        }
      } else {
        errorMessage = config.USER_ID_DOESNT_EXIST
      }
    }
    catch
    {
      case ex: Exception => {
        ex.printStackTrace()
        logger.info("Event throwing exception: ", ex.getMessage)
      }
    }
    if (StringUtils.isNotBlank(errorMessage)) {
      sunbirdApiRespParam.copy(status = config.FAILED)
      sunbirdApiRespParam.copy(errmsg = errorMessage)
      response.copy(params = sunbirdApiRespParam)
      response.copy(responseCode = HttpStatus.SC_INTERNAL_SERVER_ERROR)
    }
    logger.info("final response " + new Gson().toJson(response))
    return response
  }

  def readAssessmentLevelData(assessmentAllDetail: util.Map[String, Any]): util.Map[String, Any] = {
    val assessmentParams: Array[String] = config.ASSESSMENT_LEVEL_PARAMS.split(",", -1)
    logger.info("assessmentParams " + assessmentParams)
    val assessmentFilteredDetail = new util.HashMap[String, Any]()
    assessmentParams.foreach(params => {
      logger.info("params name " + params)
      if (assessmentAllDetail.containsKey(params)) {
        assessmentFilteredDetail.put(params, assessmentAllDetail.get(params))
      }
    })
    readSectionLevelParams(assessmentAllDetail, assessmentFilteredDetail)
    logger.info("readSectionLevelParams " + readSectionLevelParams(assessmentAllDetail, assessmentFilteredDetail))
    assessmentFilteredDetail
  }

  def readSectionLevelParams(assessmentAllDetail: util.Map[String, Any], assessmentFilteredDetail: util.HashMap[String, Any]): Unit = {
    val sectionResponse = new util.ArrayList[util.Map[String, Any]]()
    val sectionIdList = new util.ArrayList[String]()
    val sectionParamsFields = config.SELECTION_LEVEL_PARAMS.split(",", -1)
    logger.info("sectionParams " + sectionParamsFields.toString)
    val sections = assessmentAllDetail.get(config.CHILDREN).asInstanceOf[util.List[util.Map[String, Any]]]
    logger.info("sections " + sections)
    sections.forEach(sectionMap => {
      logger.info("sectionMap " + sectionMap.toString)
      sectionIdList.add(sectionMap.get(config.IDENTIFIER).asInstanceOf[String])
      logger.info("sectionIdList " + sectionIdList)
      val newSection = new util.HashMap[String, Any]()
      sectionParamsFields.foreach(paramsFields => {
        logger.info("section params name " + paramsFields)
        if (sectionMap.containsKey(paramsFields)) {
          newSection.put(paramsFields, sectionMap.get(paramsFields))
        }
      })
      logger.info("newSection " + newSection)
      val allQuestionIdList = new util.ArrayList[String]()
      val questions = sectionMap.get(config.CHILDREN).asInstanceOf[util.List[util.Map[String, Any]]]
      logger.info("Questions " + questions)
      questions.forEach(question => {
        allQuestionIdList.add(question.get(config.IDENTIFIER).asInstanceOf[String])
        logger.info("allQuestionIdList " + allQuestionIdList)
      })
      Collections.shuffle(allQuestionIdList)
      var childNodeList = new util.ArrayList[String]()
      if (sectionMap.get(config.MAX_QUESTIONS) != null) {
        val maxQuestions = sectionMap.get(config.MAX_QUESTIONS).asInstanceOf[Double]
        logger.info("maxQ " + maxQuestions)
        //TODO-need to check collect function is working fine or not if it is not working then make childnodelsit array list to list
        childNodeList = allQuestionIdList.stream.limit(maxQuestions.toLong).collect(toList[String]).asInstanceOf[util.ArrayList[String]]
        logger.info("child node list " + childNodeList)
      }
      newSection.put(config.CHILD_NODES, childNodeList)
      sectionResponse.add(newSection)
      logger.info("secotion rsponse " + sectionResponse)
    })
    assessmentFilteredDetail.put(config.CHILDREN, sectionResponse)
    assessmentFilteredDetail.put(config.CHILD_NODES, sectionIdList)
    logger.info("assessmentFilteredDetail end " + assessmentFilteredDetail)
  }

  def calculateAssessmentSubmitTime(expectedDuration: Int, assessmentStartTime: Date): Timestamp = {
    var assessmentDuration = config.user_assessment_submission_duration
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(new Timestamp(assessmentStartTime.getTime).getTime)
    if (config.user_assessment_submission_duration.isEmpty) {
      assessmentDuration = "120"
    }
    cal.add(Calendar.SECOND, expectedDuration + assessmentDuration.toInt)
    return new Timestamp(cal.getTime().getTime)
  }

  def addUserAssessmentDataToDB(userId: String, assessmentIdentifier: String, startTime: Timestamp, endTime: Timestamp, questionSet: util.HashMap[String, Any], status: String): Boolean = {
    val query = QueryBuilder.insertInto(config.dbKeyspace, config.assessmentTable).
      value(config.userId, userId).value(config.ASSESSMENT_ID_KEY, assessmentIdentifier).value(config.START_TIME, startTime).
      value(config.END_TIME, endTime).value(config.ASSESSMENT_READ_RESPONSE, new Gson().toJson(questionSet)).value(config.STATUS, status).toString
    val flag = cassandraUtil.upsert(query)
    flag
  }
}
