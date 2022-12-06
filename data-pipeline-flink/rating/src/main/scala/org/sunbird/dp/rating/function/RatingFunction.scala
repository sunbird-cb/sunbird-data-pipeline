package org.sunbird.dp.rating.function

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.gson.Gson
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.DataCache
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.dp.rating.domain.Event
import org.sunbird.dp.rating.task.RatingConfig
import org.sunbird.dp.rating.util.RestApiUtil

import java.util
import java.util.{HashMap, UUID}
import scala.collection.JavaConverters._
import scala.collection.mutable

class RatingFunction(config: RatingConfig, @transient var cassandraUtil: CassandraUtil = null)(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[RatingFunction])

  private var dataCache: DataCache = _
  private var restUtil: RestUtil = _
  private var restApiUtil:RestApiUtil=_

  override def metricsList(): List[String] = {
    List()
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    restApiUtil=new RestApiUtil()
  }

  case class RatingJson(objectType: String, var user_id: String, var date: String, var rating: Float, var review: String)


  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    var userStatus: Boolean = false
    try {
      val courseId = event.activityId
      val versionKey = getVersionKey(courseId)
      val query = QueryBuilder.select().column("userid").from(config.dbCoursesKeyspace, config.courseTable)
        .where(QueryBuilder.eq(config.userId, event.userId)).and(QueryBuilder.eq(config.courseId, event.activityId))
      val rows: java.util.List[Row] = cassandraUtil.find(query.toString);
      if (null != rows && !rows.isEmpty) {
        userStatus = true
        var delta = 0.0f
        val prevRatingValue = event.prevValues
        if(prevRatingValue!=null){
          delta = event.updatedValues.get("rating").asInstanceOf[Double].toFloat - event.prevValues.get("rating").asInstanceOf[Double].toFloat
        }else{
          delta = event.updatedValues.get("rating").asInstanceOf[Double].toFloat
        }
        val validReview = event.updatedValues.get("review").asInstanceOf[String]
        var tempRow: Row = null
        val ratingQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.ratingsSummaryTable)
          .where(QueryBuilder.eq(config.activityId, event.activityId))
          .and(QueryBuilder.eq(config.activityType, event.activityType)).toString
        val ratingRows: java.util.List[Row] = cassandraUtil.find(ratingQuery.toString);

        if (delta != 0.0f || (validReview.size > 100)) {
          var updatedRating: Float = 0.0f
          var updatedRatingValues: HashMap[Float, Float] = new HashMap[Float, Float]()
          var prevRating: Float = 0.0f
          var x = 0.0f
          var sumOfTotalRating: Float = 0.0f
          var totalNumberOfRatings: Float = 0.0f
          var summary: String = null
          if (null != ratingRows && !ratingRows.isEmpty) {
            tempRow = ratingRows.asScala.toList(0)
            if (delta != 0.0f) {
              if(prevRatingValue!=null) {
                prevRating = event.prevValues.get("rating").asInstanceOf[Double].toFloat
              }
              updatedRating = event.updatedValues.get("rating").asInstanceOf[Double].toFloat
              updatedRatingValues = update_ratings_count(tempRow, prevRating, updatedRating)
              sumOfTotalRating = tempRow.getFloat("sum_of_total_ratings") + delta
              totalNumberOfRatings = tempRow.getFloat("total_number_of_ratings")
              if(prevRatingValue==null){
                totalNumberOfRatings = totalNumberOfRatings + 1.0f
              }
              if(tempRow.getString("latest50reviews")!=null) {
                summary = tempRow.getString("latest50reviews")
              }
            }

            if (validReview.size > 100 && delta == 0.0f) {
              sumOfTotalRating = tempRow.getFloat("sum_of_total_ratings")
              totalNumberOfRatings = tempRow.getFloat("total_number_of_ratings")
              prevRating = event.prevValues.get("rating").asInstanceOf[Double].toFloat
              updatedRating = event.updatedValues.get("rating").asInstanceOf[Double].toFloat
              updatedRatingValues = update_ratings_count(tempRow, prevRating, updatedRating)
              if(tempRow.getString("latest50reviews")!=null) {
                summary = tempRow.getString("latest50reviews")
              }
            }
          }
          else {
            updatedRating = event.updatedValues.get("rating").asInstanceOf[Double].toFloat
            updatedRatingValues = update_ratings_count(tempRow, 0.0f, updatedRating)
            sumOfTotalRating =  event.updatedValues.get("rating").asInstanceOf[Double].toFloat
            totalNumberOfRatings = 1.0f
          }
          updateDB(event, updatedRatingValues, sumOfTotalRating,
            totalNumberOfRatings,
            summary)
          updateToContentMetaData(config.CONTENT_BASE_HOST + config.CONTENT_UPDATE_ENDPOINT + courseId, versionKey, sumOfTotalRating, totalNumberOfRatings)
        }
        if (validReview.size < 100 && delta == 0.0f) {
          tempRow = ratingRows.asScala.toList(0)
          val sumOfTotalRating = tempRow.getFloat("sum_of_total_ratings")
          val totalNumberOfRatings = tempRow.getFloat("total_number_of_ratings")
          val updatedRatingValues = update_ratings_count(tempRow, event.prevValues.get("rating").asInstanceOf[Double].toFloat, event.updatedValues.get("rating").asInstanceOf[Double].toFloat)
          var summary: String = null
          if(tempRow.getString("latest50reviews")!=null) {
            summary = tempRow.getString("latest50reviews")
          }
          updateDB(event, updatedRatingValues, sumOfTotalRating,
            totalNumberOfRatings,
            summary)
          updateToContentMetaData(config.CONTENT_BASE_HOST + config.CONTENT_UPDATE_ENDPOINT + courseId, versionKey, sumOfTotalRating, totalNumberOfRatings)
        }
        if (null != getRatingLookUp(event)) {
          deleteRatingLookup(event)
        }
        saveRatingLookup(event)
      } else {
        context.output(config.failedEvent, event)
      }
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
        context.output(config.failedEvent, event)
        logger.info("Event throwing exception: ", ex.getMessage)
      }
    }
  }

  def updateDB(event: Event, updatedRatingValues: HashMap[Float, Float],
               sumOfTotalRating: Float, totalRating: Float,
               summary: String): Unit = {
    val ratingDBResult = getRatingSummary(event)
    val validReview = event.updatedValues.get("review").asInstanceOf[String]

    var updatedReviews = ""
    if (null == ratingDBResult) {
      updatedReviews = update_Top50_Review_Summary(null, event)
      saveRatingSummary(event, updatedRatingValues, updatedReviews, sumOfTotalRating, totalRating)
    }
    else {
      updatedReviews = update_Top50_Review_Summary(summary, event)
      updateRatingSummary(event, updatedRatingValues, updatedReviews, sumOfTotalRating, totalRating)
    }
  }

  def update_ratings_count(tempRow: Row, prevRating: Float, updatedRating: Float): HashMap[Float, Float] = {
    var ratingMap: HashMap[Float, Float] = new HashMap()
    var x = 0;
    val defaultCount = 0.0f
    for (i <- 0 to 4) {
      ratingMap.put(i + 1.0f, 0.0f)
    }
    if (tempRow != null) {
      ratingMap.put(1.0f, (if (tempRow.getFloat("totalcount1stars")!=null)  tempRow.getFloat("totalcount1stars") else defaultCount))
      ratingMap.put(2.0f, (if (tempRow.getFloat("totalcount2stars")!=null)  tempRow.getFloat("totalcount2stars") else defaultCount))
      ratingMap.put(3.0f, (if (tempRow.getFloat("totalcount3stars")!=null)  tempRow.getFloat("totalcount3stars") else defaultCount))
      ratingMap.put(4.0f, (if (tempRow.getFloat("totalcount4stars")!=null)  tempRow.getFloat("totalcount4stars") else defaultCount))
      ratingMap.put(5.0f, (if (tempRow.getFloat("totalcount5stars")!=null)  tempRow.getFloat("totalcount5stars") else defaultCount))

    }
    val newRating = (updatedRating).floor
    val oldRating = (prevRating).floor
    if (ratingMap.containsKey(newRating) && newRating != oldRating) {
      ratingMap.put(newRating, ratingMap.get(newRating) + 1)
    }
    if (oldRating!=0.0f && ratingMap.get(oldRating) != 0.0f && newRating != oldRating) {
      if (ratingMap.containsKey(oldRating) && newRating != oldRating) {
        ratingMap.put(oldRating, ratingMap.get(oldRating) - 1)
      }
    }
    ratingMap

  }

  def update_Top50_Review_Summary(summary: String, event: Event): String = {
    var gson: Gson = new Gson()
    var ratingQueue = mutable.Queue[RatingJson]()
    val updatedReviewSize = event.updatedValues.get("review").asInstanceOf[String].size

    if (summary!=null) {
      var ratingJson: Array[RatingJson] = gson.fromJson(summary, classOf[Array[RatingJson]])
      ratingJson.foreach(
        row => {
          if(!row.user_id.asInstanceOf[String].equals(event.userId.asInstanceOf[String])){
            ratingQueue.enqueue(row)
          }
        });
      if (ratingQueue.size >= 50) {
        ratingQueue.dequeue()
      }
    }

    if (updatedReviewSize > 100) {
      ratingQueue.enqueue(RatingJson("review",
        event.userId.asInstanceOf[String],
        event.updatedValues.get("updatedOn").asInstanceOf[String],
        event.updatedValues.get("rating").asInstanceOf[Double].toFloat,
        event.updatedValues.get("review").asInstanceOf[String]))
      val finalResult = ratingQueue.toList
      gson.toJson(finalResult.toArray)
    } else {
      if(!ratingQueue.isEmpty){
        gson.toJson(ratingQueue.toList.toArray)
      }else {
        null
      }
    }
  }

  def getRatingSummary(event: Event): Row = {
    val query = QueryBuilder.select.all()
      .from(config.dbKeyspace, config.ratingsSummaryTable).
      where(QueryBuilder.eq("activityid", event.activityId))
      .and(QueryBuilder.eq("activitytype", event.activityType)).toString

    val row = cassandraUtil.findOne(query)
    logger.info("Successfully retrieved the rating for summary - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
    row
  }

  def getRatingLookUp(event: Event): Row = {
    var prevRating = 0.0f

    if(event.prevValues!=null) {
      prevRating = event.prevValues.get("rating").asInstanceOf[Double].toFloat
    }
    val query = QueryBuilder.select.all()
      .from(config.dbKeyspace, config.ratingsLookupTable).
      where(QueryBuilder.eq("activityid", event.activityId))
      .and(QueryBuilder.eq("activitytype", event.activityType))
      .and(QueryBuilder.eq("rating", prevRating)).toString

    val row = cassandraUtil.findOne(query)
    logger.info("Successfully retrieved the rating for summary - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "and updateValues" + event.updatedValues)
    row
  }

  def saveRatingSummary(event: Event, updatedRatingValues: HashMap[Float, Float],
                        summary: String, sumOfTotalRating: Float, totalRating: Float): Unit = {
    val query = QueryBuilder.insertInto(config.dbKeyspace, config.ratingsSummaryTable)
      .value("activityid", event.activityId)
      .value("activitytype", event.activityType)
      .value("latest50reviews", summary)
      .value("sum_of_total_ratings", sumOfTotalRating)
      .value("total_number_of_ratings", totalRating)
      .value("totalcount1stars", updatedRatingValues.get(1.0f))
      .value("totalcount2stars", updatedRatingValues.get(2.0f))
      .value("totalcount3stars", updatedRatingValues.get(3.0f))
      .value("totalcount4stars", updatedRatingValues.get(4.0f))
      .value("totalcount5stars", updatedRatingValues.get(5.0f)).toString

    cassandraUtil.upsert(query)
    logger.info("Successfully processed the rating event - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with "+ "and updateValues" + event.updatedValues)
  }

  def deleteRatingLookup(event: Event): Unit = {
    val updatingTime = event.prevValues.get("updatedOn").asInstanceOf[String]
    val timeBasedUuid = UUID.fromString(updatingTime)

    val query = QueryBuilder.delete()
      .from(config.dbKeyspace, config.ratingsLookupTable)
      .where(QueryBuilder.eq("activityid", event.activityId))
      .and(QueryBuilder.eq("activitytype", event.activityType))
      .and(QueryBuilder.eq("rating", event.prevValues.get("rating").asInstanceOf[Double].toFloat))
      .and(QueryBuilder.eq("updatedon", timeBasedUuid)).toString

    cassandraUtil.upsert(query)
    logger.info("Successfully saved the rating for lookup - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def saveRatingLookup(event: Event): Unit = {
    val updatingTime = event.updatedValues.get("updatedOn").asInstanceOf[String]
    val timeBasedUuid = UUID.fromString(updatingTime)
    val query = QueryBuilder.insertInto(config.dbKeyspace, config.ratingsLookupTable)
      .value("activityid", event.activityId)
      .value("activitytype", event.activityType)
      .value("rating", event.updatedValues.get("rating").asInstanceOf[Double].toFloat)
      .value("updatedon", timeBasedUuid)
      .value("review", event.updatedValues.get("review").toString)
      .value("userid", event.userId).toString

    cassandraUtil.upsert(query)
    logger.info("Successfully saved the rating for lookup - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def createRatingLookup(event: Event): Unit = {
    val updatingTime = event.updatedValues.get("updatedOn").asInstanceOf[String]
    val timeBasedUuid = UUID.fromString(updatingTime)
    val query = QueryBuilder.insertInto(config.dbKeyspace, config.ratingsLookupTable)
      .value("activityid", event.activityId)
      .value("activitytype", event.activityType)
      .value("rating", event.updatedValues.get("rating").asInstanceOf[Float])
      .value("updatedon", timeBasedUuid)
      .value("userid", event.userId).toString

    cassandraUtil.upsert(query)
    logger.info("Successfully saved the rating for lookup - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def updateRatingSummary(event: Event, updatedRatingValues: HashMap[Float, Float],
                          summary: String, sumOfTotalRating: Float, totalRating: Float): Unit = {
    val test = updatedRatingValues.get(1.0f)

    val updateQuery = QueryBuilder.update(config.dbKeyspace, config.ratingsSummaryTable)
      .`with`(QueryBuilder.set("latest50reviews", summary))
      .and(QueryBuilder.set("sum_of_total_ratings", sumOfTotalRating))
      .and(QueryBuilder.set("total_number_of_ratings", totalRating))
      .and(QueryBuilder.set("totalcount1stars", updatedRatingValues.get(1.0f)))
      .and(QueryBuilder.set("totalcount2stars", updatedRatingValues.get(2.0f)))
      .and(QueryBuilder.set("totalcount3stars", updatedRatingValues.get(3.0f)))
      .and(QueryBuilder.set("totalcount4stars", updatedRatingValues.get(4.0f)))
      .and(QueryBuilder.set("totalcount5stars", updatedRatingValues.get(5.0f)))
      .where(QueryBuilder.eq("activityid", event.activityId))
      .and(QueryBuilder.eq("activitytype", event.activityType))
    cassandraUtil.upsert(updateQuery.toString)
    logger.info("Successfully updated ratings in rating summary  - activity_id: "
      + event.activityId + " ,activity_type: " + event.activityType)
  }

  def getVersionKey(courseId: String) :String={
    var response=new String()
    val param=new util.HashMap[String,Any]()
    param.put(config.OFFSET,0)
    param.put(config.LIMIT,1)
    val filters=new util.HashMap[String,Any]()
    filters.put(config.STATUS,new util.ArrayList[String](){
      add("LIVE")
    })
    filters.put(config.IDENTIFIER,courseId)
    val sortBy=new util.HashMap[String,Any]()
    sortBy.put(config.LAST_UPDATE_ON,config.DESC)
    val fields=new util.ArrayList[String]()
    fields.add(config.VERSION_KEY)
    param.put(config.FILTERS,filters)
    param.put(config.SORTBY,sortBy)
    param.put(config.FIELDS,fields)
    val request=new util.HashMap[String,Any]()
    request.put(config.REQUEST,param)
    logger.info("req Obj "+request)
    if(MapUtils.isNotEmpty(request)) {
      response=restApiUtil.postRequest(config.KM_BASE_HOST+config.CONTENT_SEARCH_ENDPOINT,request)
    }
    var versionKey=new String()
    val gson = new Gson()
    val responseMap=gson.fromJson(response, classOf[util.HashMap[String,Any]])
    logger.info("ver "+responseMap)
    if(MapUtils.isNotEmpty(responseMap)){
      val result = responseMap.get(config.RESULT).asInstanceOf[util.Map[String, Any]]
      logger.info("request " + result)
      val content = result.get(config.CONTENT).asInstanceOf[util.List[util.Map[String, Any]]]
      logger.info("content " + content)
      if (CollectionUtils.isNotEmpty(content)) {
        content.forEach(map => {
          logger.info("map " + map)
          logger.info("key " + map.getOrDefault("versionKey", ""))
          versionKey = map.getOrDefault("versionKey", "").toString
        })
      }
    }
    logger.info("versoN "+versionKey)
    versionKey
  }
  def updateToContentMetaData(uri: String,versionKey:String,sumOfTotalRating: Float, totalNumberOfRatings: Float):Unit = {
    logger.info("Entering updateToContentMetaData")
    new Thread(() => {
      try {
        val content = new util.HashMap[String, Any]()
        content.put(config.VERSION_KEY,versionKey)
        content.put(config.AVERAGE_RATING, sumOfTotalRating)
        content.put(config.TOTAL_RATING_COUNT, totalNumberOfRatings)
        val request: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
        request.put(config.REQUEST, new java.util.HashMap[String, Any]() {
          {
            put(config.CONTENT, content)
          }
        })
        restApiUtil.patchRequest(uri, request)
      } catch {
        case e: Exception => e.printStackTrace()
          logger.info(String.format("Failed during sending mail %s", e.getMessage()))
      }
    }).start()
  }
}

