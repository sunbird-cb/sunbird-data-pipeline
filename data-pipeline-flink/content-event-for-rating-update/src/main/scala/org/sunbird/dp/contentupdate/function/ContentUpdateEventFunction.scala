package org.sunbird.dp.contentupdate.function

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.gson.Gson
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdate.domain.Event
import org.sunbird.dp.contentupdate.task.ContentUpdateConfig
import org.sunbird.dp.contentupdate.util.RestApiUtil
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.CassandraUtil

import java.util
import java.util.stream.Collectors

class ContentUpdateEventFunction(config: ContentUpdateConfig, @transient var cassandraUtil: CassandraUtil = null)(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ContentUpdateEventFunction])

  private var restApiUtil:RestApiUtil=_


  override def metricsList(): List[String] = {
    List()
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    restApiUtil=new RestApiUtil()
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    try {
      val ratingQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.ratingsSummaryTable)
        .where(QueryBuilder.eq(config.activityType, event.activityType)).allowFiltering().toString
      val ratingSummaryRow=cassandraUtil.find(ratingQuery)
      logger.info("rating summary "+ratingSummaryRow)
      val totalCount=ratingSummaryRow.size()
      logger.info("totalCount "+totalCount)
      val limit_100_ids=new util.ArrayList[String]()
      var count: Int = 1
      val limit: Int = 100
      var offSet=0
      for(i<-offSet to limit if offSet<totalCount) {
        limit_100_ids.add(ratingSummaryRow.get(i).get(config.ACTIVITY_ID,classOf[String]))
        if (offSet == 0) {
          count = totalCount
        }
        offSet += 1
      }
      if(CollectionUtils.isNotEmpty(limit_100_ids)){
        val contentDetailsList=getContentDetails(limit_100_ids)
        logger.info("contentDetailsList "+contentDetailsList)
        if(CollectionUtils.isNotEmpty(contentDetailsList)){
          compareTheDetails(ratingSummaryRow,contentDetailsList,event.activityType)
        }
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        context.output(config.failedEvent, event)
        logger.error("Event throwing exception: ", ex.getMessage)
      }
    }
  }

  def getContentDetails(activityIds: util.ArrayList[String]): util.ArrayList[util.HashMap[String, Any]] = {
    val param = new util.HashMap[String, Any]()
    param.put(config.OFFSET, 0)
    param.put(config.LIMIT, 99)
    val filters = new util.HashMap[String, Any]()
    filters.put(config.STATUS, new util.ArrayList[String]() {
      add("LIVE")
    })
    filters.put(config.IDENTIFIER, activityIds)
    val sortBy = new util.HashMap[String, Any]()
    sortBy.put(config.LAST_UPDATE_ON, config.DESC)
    val fields = new util.ArrayList[String]()
    fields.add(config.VERSION_KEY)
    fields.add(config.averageRatingScore)
    fields.add(config.totalRatingsCount)
    param.put(config.FILTERS, filters)
    param.put(config.SORTBY, sortBy)
    param.put(config.FIELDS, fields)
    val request = new util.HashMap[String, Any]()
    request.put(config.REQUEST, param)
    logger.info("req Obj " + request)
    var response = new String()
    if (MapUtils.isNotEmpty(request)) {
      response = restApiUtil.postRequest(config.KM_BASE_HOST + config.CONTENT_SEARCH_ENDPOINT, request)
    }
    val gson = new Gson()
    val responseMap = gson.fromJson(response, classOf[util.HashMap[String, Any]])
    var versionKey = new String()
    var identifier = new String()
    val contentDetailsList = new util.ArrayList[util.HashMap[String, Any]]()
    if (MapUtils.isNotEmpty(responseMap)) {
      val result = responseMap.get(config.RESULT).asInstanceOf[util.Map[String, Any]]
      logger.info("request " + result)
      val content = result.get(config.CONTENT).asInstanceOf[util.List[util.Map[String, Any]]]
      logger.info("content " + content)
      if (CollectionUtils.isNotEmpty(content)) {
        content.forEach(map => {
          logger.info("map " + map)
          versionKey = map.get(config.VERSION_KEY).asInstanceOf[String]
          identifier = map.get(config.IDENTIFIER).toString
          logger.info("i "+identifier+" v "+versionKey)
          contentDetailsList.add(new util.HashMap[String, Any]() {
            put(config.IDENTIFIER,identifier)
            put(config.VERSION_KEY,versionKey)
            put(config.averageRatingScore, map.getOrDefault(config.averageRatingScore,0.0))
            put(config.totalRatingsCount, map.getOrDefault(config.totalRatingsCount,0.0))
          })
        })
      }
    }
    contentDetailsList
  }

  def compareTheDetails(ratingSummaryRow: util.List[Row], contentDetailsList: util.ArrayList[util.HashMap[String, Any]],activityType:String): Unit = {
    //TODO-Need to implement logic if version key is null inside ratings_summary table
  /*  for (i <- 0 to contentDetailsList.size() - 1) {
      logger.info("value i "+i)
      if (!ratingSummaryRow.get(i).get(config.LAST_UPDATED_VERSION_KEY, classOf[String]).equals(contentDetailsList.get(i).get(config.VERSION_KEY))) {
        logger.info(contentDetailsList.get(i).get(config.averageRatingScore)+" --- "+ratingSummaryRow.get(i).get(config.sum_of_total_ratings, classOf[Float]))
        logger.info(contentDetailsList.get(i).get(config.totalRatingsCount)+" --- "+ratingSummaryRow.get(i).get(config.total_number_of_ratings, classOf[Float]))
        if (!ratingSummaryRow.get(i).get(config.sum_of_total_ratings, classOf[Float]).equals(contentDetailsList.get(i).get(config.averageRatingScore))
          || !ratingSummaryRow.get(i).get(config.total_number_of_ratings, classOf[Float]).equals(contentDetailsList.get(i).get(config.totalRatingsCount))) {
          val courseId = ratingSummaryRow.get(i).get(config.ACTIVITY_ID, classOf[String])
          val averageRatingScore = ratingSummaryRow.get(i).get(config.sum_of_total_ratings, classOf[Float])
          val totalRatingsCount = ratingSummaryRow.get(i).get(config.total_number_of_ratings, classOf[Float])
          logger.info("rating " + averageRatingScore + " " + totalRatingsCount + " " + courseId)
          val versionKey = contentDetailsList.get(i).get(config.VERSION_KEY)
          logger.info("versionKey " + versionKey)
          val statusCode = updateToContentMetaData(config.CONTENT_BASE_HOST + config.CONTENT_UPDATE_ENDPOINT + courseId, versionKey, averageRatingScore, totalRatingsCount)
          if (statusCode.equals(200)) {
            updateVersionKeyToDB(courseId, versionKey, activityType)
          }
        }
      }
    }*/
    val nonmatch=ratingSummaryRow.stream().filter(dbRecords=>
           contentDetailsList.stream().noneMatch(contentMap=>
           dbRecords.get(config.LAST_UPDATED_VERSION_KEY,classOf[String]).equals(contentMap.get(config.VERSION_KEY)))).collect(Collectors.toList[Any])

    val ratingMatch=ratingSummaryRow.stream().filter(dbRecords=>contentDetailsList.stream().noneMatch(contentMap=>
      dbRecords.get(config.sum_of_total_ratings, classOf[Float]).equals(contentMap.get(config.averageRatingScore)) &&
        dbRecords.get(config.total_number_of_ratings, classOf[Float]).equals(contentMap.get(config.totalRatingsCount)))).collect(Collectors.toList[Any])

    logger.info("nonMatchValue "+nonmatch)
    logger.info("ratingMatch "+ratingMatch)

    ratingSummaryRow.forEach(row=>{
      contentDetailsList.forEach(contentMap=>{
        if(row.get(config.ACTIVITY_ID,classOf[String]).equals(contentMap.get(contentMap.get(config.IDENTIFIER)))){
          if (!row.get(config.LAST_UPDATED_VERSION_KEY, classOf[String]).equals(contentMap.get(config.VERSION_KEY))) {
            if (!row.get(config.sum_of_total_ratings, classOf[Float]).equals(contentMap.get(config.averageRatingScore)) &&
              !row.get(config.total_number_of_ratings, classOf[Float]).equals(contentMap.get(config.totalRatingsCount))) {
              val identifier = row.get(config.ACTIVITY_ID, classOf[String])
              val averageRatingScore = row.get(config.sum_of_total_ratings, classOf[Float])
              val totalRatingsCount = row.get(config.total_number_of_ratings, classOf[Float])
              logger.info("avg rating: " + averageRatingScore + ": total count: " + totalRatingsCount + ": id: " + identifier)
            }
          }
        }
      })
    })
  }

  def updateToContentMetaData(uri: String, versionKey: Any, averageRatingScore: Float, totalRatingsCount: Float): Int = {
    logger.info("Entering updateToContentMetaData")
    var responseCode=0
    try {
      val content = new util.HashMap[String, Any]()
      content.put(config.VERSION_KEY, versionKey)
      content.put(config.averageRatingScore, averageRatingScore.toString)
      content.put(config.totalRatingsCount, totalRatingsCount.toString)
      val request: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
      request.put(config.REQUEST, new java.util.HashMap[String, Any]() {
        {
          put(config.CONTENT, content)
        }
      })
      responseCode=restApiUtil.patchRequest(uri, request)
    } catch {
      case e: Exception => e.printStackTrace()
        logger.error(String.format("Failed during updating content meta data %s", e.getMessage()))
    }
    responseCode
  }

  def updateVersionKeyToDB(courseId: String, versionKey: Any, activityType: String): Unit = {
    try{
      val updateQuery = QueryBuilder.update(config.dbKeyspace, config.ratingsSummaryTable).`with`(QueryBuilder.set(config.LAST_UPDATED_VERSION_KEY, versionKey))
        .where(QueryBuilder.eq(config.ACTIVITY_ID, courseId)).and(QueryBuilder.eq(config.activityType, activityType)).toString
      logger.info("update query " + updateQuery)
      val response = cassandraUtil.upsert(updateQuery)
      if (response) {
        logger.info("version key updated successfully in DB")
      }
    }catch {
      case e: Exception => e.printStackTrace()
        logger.error(String.format("Failed during updating version key in DB %s", e.getMessage()))
    }
  }
}
