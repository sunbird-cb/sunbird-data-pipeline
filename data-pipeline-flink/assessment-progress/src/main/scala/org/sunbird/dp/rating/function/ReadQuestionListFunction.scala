package org.sunbird.dp.rating.function

import com.datastax.driver.core.Row
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.commons.collections
import org.apache.commons.collections.{CollectionUtils, MapUtils}
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

import java.util
import java.util.{Collections, UUID}

class ReadQuestionListFunction (config:AssessmentProgressConfig,event: Event)(implicit val mapTypeInfo: TypeInformation[Event]) {
  private[this] val logger = LoggerFactory.getLogger(classOf[ReadQuestionListFunction])

  private var restApiUtil: RestApiUtil = new RestApiUtil()
  var cassandraUtil: CassandraUtil = new CassandraUtil(config.dbHost,config.dbPort)
  private var assessmentUtils:AssessmentUtils=new AssessmentUtils(config)

  def initiateReadQuestionListFunction():Unit={
    var sunbirdApiRespParam = SunbirdApiRespParam()
    var response = Response(result = new util.HashMap[String, Any]())
    sunbirdApiRespParam.copy(status = config.SUCCESS)
    sunbirdApiRespParam.copy(resmsgid = UUID.randomUUID().toString)
    response.copy(params = sunbirdApiRespParam)
    response.copy(responseCode = HttpStatus.SC_OK)
    logger.info("event data " + event.toString)
    var errorMessage = new String()
    var primaryCategory=new String()
    var result=new util.HashMap[String,Any]()
    val authUserToken=event.token
    val requestBody=event.requestBody
    val userId=event.userId
    try {
      val identifierList=new util.ArrayList[String]()
      val questionList=new util.ArrayList[Any]()
      result= validateQuestionListAPI(userId,requestBody,authUserToken,identifierList)
      logger.info("result "+result.toString)
      errorMessage=result.get(config.ERROR_MESSAGE).asInstanceOf[String]
      if(errorMessage.isEmpty){
        if(result.containsKey(config.PRIMARY_CATEGORY) && result.get(config.PRIMARY_CATEGORY).asInstanceOf[String].equalsIgnoreCase(config.PRACTICE_QUESTION_SET)){
          primaryCategory=result.get(config.PRIMARY_CATEGORY).asInstanceOf[String]
        }
        errorMessage=fetchQuestionIdentifierValue(identifierList,questionList,primaryCategory)
        if(errorMessage.isEmpty && identifierList.size()==questionList.size()){
          response.result.put(config.QUESTIONS,questionList)
          logger.info("response obj "+response.result.toString)
        }
      }
    } catch {
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

  def validateQuestionListAPI(userId:String, requestBody: util.Map[String, Any], authUserToken: String, identifierList: util.ArrayList[String]): util.HashMap[String, Any] = {
    val result=new util.HashMap[String,Any]()
    if(StringUtils.isBlank(userId)){
      result.put(config.ERROR_MESSAGE,config.INVALID_USERID)
      return  result
    }
    if(StringUtils.isBlank(requestBody.get(config.ASSESSMENT_ID_KEY).asInstanceOf[String])){
      result.put(config.ERROR_MESSAGE,config.ASSESSMENT_ID_KEY_IS_NOT_PRESENT_IS_EMPTY)
      return result
    }
    identifierList.addAll(getQuestionIdList(requestBody))
    if(identifierList.isEmpty){
      result.put(config.ERROR_MESSAGE,config.IDENTIFIER_LIST_IS_EMPTY)
      return result
    }
    val assessmentDetail=new util.HashMap[String,Any]()
    assessmentUtils.fetchReadHierarchyDetails(assessmentDetail,authUserToken,requestBody.get(config.ASSESSMENT_ID_KEY).asInstanceOf[String])
    if(assessmentDetail.isEmpty){
      result.put(config.ERROR_MESSAGE,config.ASSESSMENT_HIERARCHY_READ_FAILED)
      return result
    }
    if(!assessmentDetail.get(config.PRIMARY_CATEGORY).asInstanceOf[String].equalsIgnoreCase(config.PRACTICE_QUESTION_SET)){
      val existingDataList:util.List[Row]=assessmentUtils.fetchUserAssessmentDataFromDB(userId,requestBody.get(config.ASSESSMENT_ID_KEY).asInstanceOf[String])
      var questionSetFromAssessmentString=new String()
      if(!existingDataList.isEmpty){
        questionSetFromAssessmentString=existingDataList.get(0).get(config.ASSESSMENT_READ_RESPONSE,classOf[String])
      }else{
        questionSetFromAssessmentString=""
      }
      if(!questionSetFromAssessmentString.isEmpty){
        var questionSetFromAssessment:util.HashMap[String,Any]=new Gson().fromJson(questionSetFromAssessmentString,new TypeToken[util.HashMap[String,Any]](){}.getType)
        val questionsFromAssessment=new util.ArrayList[String]()
        val sections=questionSetFromAssessment.get(config.CHILDREN).asInstanceOf[util.List[util.Map[String,Any]]]
        sections.forEach(section=>{
          questionsFromAssessment.addAll(section.get(config.CHILD_NODES).asInstanceOf[util.List[String]])
        })
        logger.info("questions from assessment "+questionsFromAssessment)
        logger.info("identifier List "+identifierList)
        // Out of the list of questions received in the payload, checking if the request
        // has only those ids which are a part of the user's latest assessment
        // Fetching all the remaining questions details from the Redis
        if(false.equals(validateQuestionListRequest(identifierList,questionsFromAssessment))){
          result.put(config.ERROR_MESSAGE,config.THE_QUESTIONS_IDS_PROVIDED_DONT_MATCH)
          return result
        }
      }else{
        result.put(config.ERROR_MESSAGE,config.ASSESSMENT_ID_INVALID_SESSION_EXPIRED)
        return result
      }
    }else{
      result.put(config.PRIMARY_CATEGORY,config.PRACTICE_QUESTION_SET)
    }
    result.put(config.ERROR_MESSAGE,"")
    result
  }

  def getQuestionIdList(questionListRequest: util.Map[String, Any]): util.List[String] = {
    try{
      if(questionListRequest.containsKey(config.REQUEST)){
        val request=questionListRequest.get(config.REQUEST).asInstanceOf[util.Map[String,Any]]
        if(!MapUtils.isEmpty(request) && request.containsKey(config.SEARCH)){
          val searchObj=request.get(config.SEARCH).asInstanceOf[util.Map[String,Any]]
          if(!MapUtils.isEmpty(searchObj) && searchObj.containsKey(config.IDENTIFIER) &&
            !CollectionUtils.isEmpty(searchObj.get(config.IDENTIFIER).asInstanceOf[util.List[String]])){
            return searchObj.get(config.IDENTIFIER).asInstanceOf[util.List[String]]
          }
        }
      }
    }catch {
      case e: Exception=>{
        logger.error("Failed to process the questionList request body. %s"+e.getMessage)
      }
    }
    return Collections.emptyList()
  }

  def validateQuestionListRequest(identifierList: util.ArrayList[String], questionsFromAssessment: util.ArrayList[String]): Boolean = {
    val questionSet=new util.HashSet[Any]()
    questionSet.add(questionsFromAssessment)
    logger.info("if "+questionSet.containsAll(identifierList))
    if(questionSet.containsAll(identifierList)){
      logger.info("if "+questionSet.containsAll(identifierList))
      true
    }else{
      false
    }
  }

  def fetchQuestionIdentifierValue(identifierList: util.ArrayList[String], questionList: util.ArrayList[Any], primaryCategory: String): String = {
    val newIdentifierList=new util.ArrayList[String]()
    newIdentifierList.addAll(identifierList)
    // Taking the list which was formed with the not found values in Redis, we are
    // making an internal POST call to Question List API to fetch the details
    if(!newIdentifierList.isEmpty){
      val questionMapList:util.List[util.Map[String,Any]]=readQuestionDetails(newIdentifierList)
      questionMapList.forEach(questionMapResponse=>{
        if(!questionMapResponse.isEmpty && config.OK.equalsIgnoreCase(questionMapResponse.get(config.RESPONSE_CODE).asInstanceOf[String])){
          val result:util.Map[String,Any]=questionMapResponse.get(config.RESULT).asInstanceOf[util.Map[String,Any]]
          val questionMap=result.get(config.QUESTIONS).asInstanceOf[util.List[util.Map[String,Any]]]
          questionMap.forEach(question=>{
            if(!question.isEmpty){
              questionList.add(filterQuestionMapDetail(question,primaryCategory))
            }else{
              logger.error(String.format("Failed to get Question Details for Id: %s",
                question.get(config.IDENTIFIER).toString()))
              return "Failed to get Question Details for Id: %s"
            }
          })
        }else{
          logger.error(String.format("Failed to get Question Details from the Question List API for the IDs: %s",
            newIdentifierList.toString()))
          return "Failed to get Question Details from the Question List API for the IDs"
        }
      })
    }
    return ""
  }

  def readQuestionDetails(identifiers: util.ArrayList[String]): util.List[util.Map[String, Any]] ={
    try {
      val sbUrl: StringBuilder = new StringBuilder(config.ASSESSMENT_HOST)
      sbUrl.append(config.QUESTION_PATH)
      val headers = new util.HashMap[String, Any]()
      headers.put(config.AUTHORIZATION, config.SBAPI_KEY)
      val requestBody = new util.HashMap[String, Any]()
      val requestData = new util.HashMap[String, Any]()
      val searchData = new util.HashMap[String, Any]()
      requestData.put(config.SEARCH, searchData)
      requestBody.put(config.REQUEST, requestData)
      val questionDataList = new util.ArrayList[util.Map[String, Any]]()
      val chunkSize = 15
      for (i <- 0 to identifiers.size() if i < chunkSize) {
        var identifierList = new util.ArrayList[String]()
        if ((i + chunkSize) >= identifiers.size()) {
          identifierList = identifiers.subList(i, identifiers.size()).asInstanceOf[util.ArrayList[String]]
        } else {
          identifierList = identifiers.subList(i, i + chunkSize).asInstanceOf[util.ArrayList[String]]
        }
        searchData.put(config.IDENTIFIER, identifierList)
        val data = restApiUtil.postRequest(sbUrl.toString(), headers, requestBody)
        val gson = new Gson()
        val gsonMap = gson.fromJson(data, classOf[util.Map[String, Any]])
        if (!MapUtils.isEmpty(gsonMap)) {
          questionDataList.add(gsonMap)
        }
      }
      return questionDataList
    }catch {
      case e:Exception=>{
        logger.error("Failed to process the readQuestionDetails. %s"+e.getMessage)
      }
    }
    return new util.ArrayList[util.Map[String, Any]]()
  }
  def filterQuestionMapDetail(questionMapResponse: util.Map[String, Any], primaryCategory: String): util.HashMap[String,Any] = {
    val questionParams=config.QUESTION_LEVEL_PARAMS.split(",",-1)
    val updatedQuestionMap=new util.HashMap[String,Any]()
    questionParams.foreach(questionParam=>{
      if(questionMapResponse.containsKey(questionParam)){
        updatedQuestionMap.put(questionParam,questionMapResponse.get(questionParam))
      }
    })
    if(questionMapResponse.containsKey(config.EDITOR_STATE) && primaryCategory.equalsIgnoreCase(config.PRACTICE_QUESTION_SET)){
      val editorState=questionMapResponse.get(config.EDITOR_STATE).asInstanceOf[util.Map[String,Any]]
      updatedQuestionMap.put(config.EDITOR_STATE,editorState)
    }
    if(questionMapResponse.containsKey(config.CHOICES)
                              && updatedQuestionMap.containsKey(config.PRIMARY_CATEGORY)
                                  && !updatedQuestionMap.get(config.PRIMARY_CATEGORY).asInstanceOf[String].equalsIgnoreCase(config.FTB_QUESTION)){
      val choicesObj=questionMapResponse.get(config.CHOICES).asInstanceOf[util.Map[String,Any]]
      val updatedChoicesMap=new util.HashMap[String,Any]()
      if(choicesObj.containsKey(config.OPTIONS)){
        val optionsMapList=choicesObj.get(config.OPTIONS).asInstanceOf[util.List[util.Map[String,Any]]]
        updatedChoicesMap.put(config.OPTIONS,optionsMapList)
      }
      updatedQuestionMap.put(config.CHOICES,updatedChoicesMap)
    }
    if(questionMapResponse.containsKey(config.RHS_CHOICES)
                                   && updatedQuestionMap.containsKey(config.PRIMARY_CATEGORY)
                                    && !updatedQuestionMap.get(config.PRIMARY_CATEGORY).asInstanceOf[String].equalsIgnoreCase(config.FTB_QUESTION)){
      val rhsChoicesObj=questionMapResponse.get(config.RHS_CHOICES).asInstanceOf[util.List[Any]]
      updatedQuestionMap.put(config.RHS_CHOICES,rhsChoicesObj)
    }
    updatedQuestionMap
  }
}
