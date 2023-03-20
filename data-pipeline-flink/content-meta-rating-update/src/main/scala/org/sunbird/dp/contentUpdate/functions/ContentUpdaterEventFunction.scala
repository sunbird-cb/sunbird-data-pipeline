package org.sunbird.dp.contentUpdate.functions

import com.google.gson.Gson
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentUpdate.domain.Event
import org.sunbird.dp.contentUpdate.task.ContentUpdateConfig
import org.sunbird.dp.contentUpdate.util.RestApiUtil
import org.sunbird.dp.core.job.{Metrics, WindowBaseProcessFunction}
import org.sunbird.dp.core.util.CassandraUtil

import java.{lang, util}

class ContentUpdaterEventFunction(config: ContentUpdateConfig, @transient var cassandraUtil: CassandraUtil = null)
                                 (implicit val eventTypeInfo: TypeInformation[Event])
  extends WindowBaseProcessFunction[Event, Event, Int](config) {
  override def metricsList(): List[String] = {
    List()
  }

  private[this] val logger = LoggerFactory.getLogger(classOf[ContentUpdaterEventFunction])
  private var restApiUtil: RestApiUtil = new RestApiUtil


  override def process(key: Int, context: ProcessWindowFunction[Event, Event, Int, GlobalWindow]#Context, elements: lang.Iterable[Event], metrics: Metrics): Unit = {
    try {
      logger.info("activate content update ")
      elements.forEach(l=>logger.info("l "+l))
      val ratingDetails=new util.HashMap[String,util.HashMap[String,Any]]()
      elements.forEach(element=>{
        if (ratingDetails.containsKey(element.identifier)) {
          if (element.totalRatingsCount.toFloat > ratingDetails.get(element.identifier).get(config.totalRatingsCount).asInstanceOf[Float]) {
            ratingDetails.put(element.identifier, new util.HashMap[String, Any]() {
              put(config.averageRatingScore, element.averageRatingScore.toFloat)
              put(config.totalRatingsCount, element.totalRatingsCount.toFloat)
            })
          }
        } else{
          ratingDetails.put(element.identifier, new util.HashMap[String, Any]() {
            put(config.averageRatingScore, element.averageRatingScore.toFloat)
            put(config.totalRatingsCount, element.totalRatingsCount.toFloat)
          })
        }
      })
      val identifiers=ratingDetails.keySet()
      val contentDetails=getContentDetailsMap(identifiers)
      filterDetails(ratingDetails,contentDetails)
    } catch {
      case exception: Exception => {
        logger.error("Failed to Process the message: " + exception.getMessage)
      }
    }
  }

  def getContentDetailsMap(identifiers: util.Set[String]): util.HashMap[String, util.HashMap[String, Any]] = {
    val param = new util.HashMap[String, Any]()
    param.put(config.OFFSET, 0)
    param.put(config.LIMIT, config.thresholdBatchReadSize)
    val filters = new util.HashMap[String, Any]()
    filters.put(config.STATUS, new util.ArrayList[String]() {
      add("LIVE")
    })
    filters.put(config.IDENTIFIER, identifiers)
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
    var response = new String()
    if (MapUtils.isNotEmpty(request)) {
      logger.info("Http Call started")
      response = restApiUtil.postRequest(config.KM_BASE_HOST + config.CONTENT_SEARCH_ENDPOINT, request)
    }
    val gson = new Gson()
    val responseMap = gson.fromJson(response, classOf[util.HashMap[String, Any]])
    var versionKey = new String()
    var identifier = new String()
    val contentDetailsMap = new util.HashMap[String, util.HashMap[String, Any]]()
    if (MapUtils.isNotEmpty(responseMap)) {
      val result = responseMap.get(config.RESULT).asInstanceOf[util.Map[String, Any]]
      val content = result.get(config.CONTENT).asInstanceOf[util.List[util.Map[String, Any]]]
      if (CollectionUtils.isNotEmpty(content)) {
        content.forEach(map => {
          versionKey = map.get(config.VERSION_KEY).asInstanceOf[String]
          identifier = map.get(config.IDENTIFIER).toString
          contentDetailsMap.put(identifier, new util.HashMap[String, Any]() {
            put(config.VERSION_KEY, versionKey)
            put(config.averageRatingScore, map.getOrDefault(config.averageRatingScore, 0.0))
            put(config.totalRatingsCount, map.getOrDefault(config.totalRatingsCount, 0.0))
          })
        })
      }
    }
    contentDetailsMap
  }

  def filterDetails(ratingDetails: util.HashMap[String, util.HashMap[String, Any]], contentDetails: util.HashMap[String, util.HashMap[String, Any]]) = {
    val ids=ratingDetails.keySet()
    ids.forEach(id=>{
        val averageRatingScore=ratingDetails.get(id).get(config.averageRatingScore)
        val totalRatingsCount=ratingDetails.get(id).get(config.totalRatingsCount)
        val versionKey=contentDetails.get(id).get(config.VERSION_KEY)
        val statusCode = updateToContentMetaData(config.CONTENT_BASE_HOST + config.CONTENT_UPDATE_ENDPOINT + id, versionKey, averageRatingScore.asInstanceOf[Float], totalRatingsCount.asInstanceOf[Float])
        if (statusCode.equals(200)) {
          logger.info("Rating Details Successfully updated in Content Meta Data")
        }
    })
  }

  def updateToContentMetaData(uri: String, versionKey: Any, averageRatingScore: Float, totalRatingsCount: Float): Int = {
    logger.info("Entering updateToContentMetaData")
    var responseCode = 0
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
      responseCode = restApiUtil.patchRequest(uri, request)
    } catch {
      case e: Exception => e.printStackTrace()
        logger.error(String.format("Failed during updating content meta data %s", e.getMessage()))
    }
    responseCode
  }
}
