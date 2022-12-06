package org.sunbird.dp.rating.util

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.dp.rating.task.AssessmentProgressConfig

import java.util

class AssessmentUtils(config:AssessmentProgressConfig){

  private[this] val logger = LoggerFactory.getLogger(classOf[AssessmentUtils])

  private var restApiUtil:RestApiUtil=new RestApiUtil()
  private var cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)


  def fetchReadHierarchyDetails(assessmentAllDetail: util.HashMap[String, Any], token: String, assessmentIdentifier: String): String = {
    var readHierarchyApiResponse = getReadHierarchyApiResponse(assessmentIdentifier, token)
    logger.info("read hiarachy api response " + readHierarchyApiResponse)
    if (readHierarchyApiResponse.isEmpty || !config.OK.equalsIgnoreCase(readHierarchyApiResponse.get(config.RESPONSE_CODE).asInstanceOf[String])) {
      logger.info("read if block")
      return config.ASSESSMENT_HIERARCHY_READ_FAILED
    }
    val result = readHierarchyApiResponse.get(config.RESULT).asInstanceOf[util.Map[String, Any]]
    logger.info("result " + result)
    val questionSet = result.get(config.QUESTION_SET)
    logger.info("question Result " + questionSet)
    assessmentAllDetail.putAll(readHierarchyApiResponse.get(config.RESULT).asInstanceOf[util.Map[String, Any]].get(config.QUESTION_SET).asInstanceOf[util.Map[String, Any]])
    logger.info("fetchReadHierarchyDetails ---> assessmentAllDetails " + assessmentAllDetail)
    return StringUtils.EMPTY
  }

  def getReadHierarchyApiResponse(assessmentIdentifier: String, token: String): util.HashMap[String, Any] = {
    try {
      val sbUrl = new StringBuilder(config.ASSESSMENT_HOST)
      sbUrl.append(config.ASSESSMENT_HIERARCHY_RAED_PATH)
      logger.info("sbUrl " + sbUrl)
      val serviceURL = sbUrl.toString().replace(config.IDENTIFIER_REPLACER, assessmentIdentifier)
      logger.info("service url " + serviceURL)
      val headers = new util.HashMap[String, Any]()
      headers.put(config.X_AUTH_TOKEN, token)
      headers.put(config.AUTHORIZATION, config.SBAPI_KEY)
      val obj = fetchUsingHeaders(serviceURL, headers)
      logger.info("object from post method " + obj)
      val gson = new Gson()
      return gson.fromJson(obj, new TypeToken[util.HashMap[String, Any]]() {}.getType())
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("failed to read hierarchy:" + ex.getMessage)
      }
        new util.HashMap[String, Any]()
    }
  }

  def fetchUsingHeaders(uri: String, requestBody: util.HashMap[String, Any]): String = {
    var response = new String()
    try {
      response = restApiUtil.getRequest(uri, requestBody)
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info("Error received: " + e.getMessage())
        response = null
    }
    response
  }

  def fetchUserAssessmentDataFromDB(userId: String, assessmentIdentifier: String): util.List[Row] = {
    val query = QueryBuilder.select().all()
      .from(config.dbKeyspace, config.assessmentTable).
      where(QueryBuilder.eq(config.userId, userId))
      .and(QueryBuilder.eq(config.ASSESSMENT_ID_KEY, assessmentIdentifier))
      .allowFiltering().toString
    val existingDataList = cassandraUtil.find(query)
    logger.info("Existing Data List " + existingDataList)
    existingDataList
  }
}
