package org.sunbird.dp.rating.function

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.DataCache
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.{CassandraUtil, JSONUtil}
import org.sunbird.dp.rating.domain.Event
import org.sunbird.dp.rating.task.RatingConfig

import java.util.{HashMap, UUID}
import scala.collection.mutable

import scala.collection.JavaConverters._

class RatingFunction(config: RatingConfig, @transient var cassandraUtil: CassandraUtil = null)(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[RatingFunction])

  private var dataCache: DataCache = _
  private var restUtil: RestUtil = _

  override def metricsList(): List[String] = {
    List()
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  case class RatingJson(objectType: String, var user_id: String, var date: String, var rating: Float, var review: String)

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    var userStatus: Boolean = false
    try {

      val query = QueryBuilder.select().column("userid").from(config.dbCoursesKeyspace, config.courseTable)
        .where(QueryBuilder.eq(config.userId, event.userId)).and(QueryBuilder.eq(config.courseId, event.activityId))
      val rows: java.util.List[Row] = cassandraUtil.find(query.toString);
      if (null != rows && !rows.isEmpty) {
        userStatus = true
        var delta = 0.0f
      val prevRatingValue = event.prevValues.get("rating").asInstanceOf[Double].toFloat
        if(prevRatingValue!=null){
          delta = event.updatedValues.get("rating").asInstanceOf[Double].toFloat - event.prevValues.get("rating").asInstanceOf[Double].toFloat
        }else{
          delta = event.updatedValues.get("rating").asInstanceOf[Double].toFloat
        }
        val validReview = event.updatedValues.get("review").asInstanceOf[String]

        if (delta != 0.0f || (validReview.size > 100)) {
          var tempRow: Row = null

          val ratingQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.ratingsSummaryTable)
            .where(QueryBuilder.eq(config.activityId, event.activityId))
            .and(QueryBuilder.eq(config.activityType, event.activityType)).toString

          val ratingRows: java.util.List[Row] = cassandraUtil.find(ratingQuery.toString);
          var updatedRating: Float = 0.0f
          var updatedRatingValues: HashMap[Float, Float] = new HashMap[Float, Float]()
          var prevRating: Float = 0.0f
          var x = 0.0f
          var sumOfTotalRating: Float = 0.0f
          var totalNumberOfRatings: Float = 0.0f
          var summary: String = ""

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
               summary  = tempRow.getString("latest50reviews")
            }

            if (validReview.size > 100 && delta == 0) {
              sumOfTotalRating = tempRow.getFloat("sum_of_total_ratings")
              totalNumberOfRatings = tempRow.getFloat("total_number_of_ratings")
              summary  = tempRow.getString("latest50reviews")
            }
          }
          else {
            updatedRating = event.updatedValues.get("rating").asInstanceOf[Double].toFloat
            updatedRatingValues = update_ratings_count(tempRow, 0.0f, updatedRating)
            sumOfTotalRating = 0.0f + event.updatedValues.get("rating").asInstanceOf[Double].toFloat
            totalNumberOfRatings = 0.0f + 1
          }
          updateDB(event, updatedRatingValues, sumOfTotalRating,
            totalNumberOfRatings,
            summary)

        }else{
          if (null != getRatingLookUp(event)) {
            deleteRatingLookup(event)
          }
          saveRatingLookup(event)
        }
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
      if (validReview.size > 100) {
        updatedReviews = update_Top50_Review_Summary("", event)
        saveRatingSummary(event, updatedRatingValues, "", sumOfTotalRating, totalRating)
      }

    }
    else {
      if (validReview.size > 100) {
        updatedReviews = update_Top50_Review_Summary(summary, event)
        updateRatingSummary(event, updatedRatingValues, updatedReviews, sumOfTotalRating, totalRating)
      }
    }
    if (null != getRatingLookUp(event)) {
      deleteRatingLookup(event)
    }
    saveRatingLookup(event)
  }

  def update_ratings_count(tempRow: Row, prevRating: Float, updatedRating: Float): HashMap[Float, Float] = {
    var ratingMap: HashMap[Float, Float] = new HashMap()
    var x = 0;
    for (i <- 0 to 4) {
      ratingMap.put(i + 1, 0)
    }

    if (tempRow != null) {
      ratingMap.put(1, tempRow.getFloat("totalcount1stars"))
      ratingMap.put(2, tempRow.getFloat("totalcount2stars"))
      ratingMap.put(3, tempRow.getFloat("totalcount3stars"))
      ratingMap.put(4, tempRow.getFloat("totalcount4stars"))
      ratingMap.put(5, tempRow.getFloat("totalcount5stars"))
    }

    val newRating = (updatedRating).floor
    val oldRating = (prevRating).floor
    if (ratingMap.containsKey(newRating) && newRating != oldRating) {
      ratingMap.put(newRating, ratingMap.get(newRating) + 1)
    }
    if (prevRating != 0.0f) {
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
    if (updatedReviewSize > 100) {

      if (!summary.isEmpty) {
        var ratingJson: Array[RatingJson] = gson.fromJson(summary, classOf[Array[RatingJson]])
        ratingJson.foreach(
          row => {
            ratingQueue.enqueue(row)
          });
        if (ratingQueue.size >= 50) {
          ratingQueue.dequeue()
        }
      }
      ratingQueue.enqueue(RatingJson("review",
        event.userId.asInstanceOf[String],
        event.updatedValues.get("updatedOn").asInstanceOf[String],
        event.updatedValues.get("rating").asInstanceOf[Double].toFloat,
        event.updatedValues.get("review").asInstanceOf[String]))
      val finalResult = ratingQueue.toList
      gson.toJson(finalResult.toArray)
    } else {
      gson.toJson(summary)

    }
  }

  def getRatingSummary(event: Event): Row = {
    val query = QueryBuilder.select.all()
      .from(config.dbKeyspace, config.ratingsSummaryTable).
      where(QueryBuilder.eq("activityid", event.activityId))
      .and(QueryBuilder.eq("activitytype", event.activityType)).toString
    logger.info("completed query in getratingsummary")

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
      .value("totalcount1stars", updatedRatingValues.get(1))
      .value("totalcount2stars", updatedRatingValues.get(2))
      .value("totalcount3stars", updatedRatingValues.get(3))
      .value("totalcount4stars", updatedRatingValues.get(4))
      .value("totalcount5stars", updatedRatingValues.get(5)).toString


    cassandraUtil.upsert(query)
    logger.info("Successfully processed the rating event - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with "+ "and updateValues" + event.updatedValues)
  }

  def deleteRatingLookup(event: Event): Unit = {
    val updatingTime = event.prevValues.get("updatedOn").asInstanceOf[String]
    val timeBasedUuid = UUID.fromString(updatingTime)

    logger.info("inside beginning of deleteRatingLookup ")
    val query = QueryBuilder.delete(config.dbKeyspace, config.ratingsLookupTable)
      .from(config.dbKeyspace, config.ratingsSummaryTable)
      .where(QueryBuilder.eq("activityid", event.activityId))
      .and(QueryBuilder.eq("activitytype", event.activityType))
      .and(QueryBuilder.eq("rating", event.updatedValues.get("rating").asInstanceOf[Double].toFloat))
      .and(QueryBuilder.eq("updatedon", timeBasedUuid)).toString

    logger.info("inside beginning of deleteRatingLookup befor query exec : " + query)

    cassandraUtil.upsert(query)
    logger.info("Successfully saved the rating for lookup - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def saveRatingLookup(event: Event): Unit = {
    val updatingTime = event.updatedValues.get("updatedOn").asInstanceOf[String]
    val timeBasedUuid = UUID.fromString(updatingTime)
    logger.info("inside saveratinglookup")
    val query = QueryBuilder.insertInto(config.dbKeyspace, config.ratingsLookupTable)
      .value("activityid", event.activityId)
      .value("activitytype", event.activityType)
      .value("rating", event.updatedValues.get("rating").asInstanceOf[Double].toFloat)
      .value("updatedon", timeBasedUuid)
      .value("review", event.updatedValues.get("review").toString)
      .value("userid", event.userId).toString
    logger.info("saving in saveratinglookup" + query)

    cassandraUtil.upsert(query)
    logger.info("Successfully saved the rating for lookup - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def createRatingLookup(event: Event): Unit = {
    val updatingTime = event.prevValues.get("updatedOn").asInstanceOf[String]
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
    logger.info("strated in updateRatingSummary " + event.activityId + "" + event.activityType)
    logger.info("updated rating valus is :   " + updatedRatingValues)
    logger.info("updated rating valus is :   " + updatedRatingValues)
    val test = updatedRatingValues.get(1.0f)

    logger.info("updated rating valus is :   " + updatedRatingValues.get(1.0f) + "test" + test)

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
    logger.info("added query in updateRatingSummary  " + updateQuery.toString)

    cassandraUtil.upsert(updateQuery.toString)
    logger.info("Successfully updated ratings in rating summary  - activity_id: "
      + event.activityId + " ,activity_type: " + event.activityType)
  }
}

