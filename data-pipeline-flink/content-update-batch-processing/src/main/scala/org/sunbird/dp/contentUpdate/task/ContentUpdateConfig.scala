package org.sunbird.dp.contentUpdate.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.dp.core.job.BaseJobConfig

import java.util

class ContentUpdateConfig (override val config: Config) extends BaseJobConfig(config, "contentUpdateEvent") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val scalaMapTypeInfo: TypeInformation[Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val contentUpdaterParallelism: Int = config.getInt("task.content.update.parallelism")

  val contentUpdaterFn = "content-updater-fn"

  //url
  val CONTENT_BASE_HOST: String = config.getString("url.base_host")
  val CONTENT_UPDATE_ENDPOINT: String = config.getString("url.content_update")
  val KM_BASE_HOST: String = config.getString("url.km_base_host")
  val CONTENT_SEARCH_ENDPOINT: String = config.getString("url.content_search")

  val windowShards: Int = config.getInt("task.window.shards")
  val thresholdBatchReadSize: Int = config.getInt("threshold.batch.read.size")

  //consumer
  val contentUpdaterConsumer = "content-updater-consumer"

  //const
  val CONTENT = "content"
  val REQUEST = "request"
  val OFFSET = "offset"
  val LIMIT = "limit"
  val STATUS = "status"
  val IDENTIFIER = "identifier"
  val LAST_UPDATE_ON = "lastUpdatedOn"
  val DESC = "desc"
  val VERSION_KEY = "versionKey"
  val FILTERS = "filters"
  val SORTBY = "sort_By"
  val FIELDS = "fields"
  val RESULT = "result"
  val averageRatingScore = "averageRatingScore"
  val totalRatingsCount = "totalRatingsCount"
}
