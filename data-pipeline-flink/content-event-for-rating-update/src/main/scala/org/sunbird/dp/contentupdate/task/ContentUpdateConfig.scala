package org.sunbird.dp.contentupdate.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.contentupdate.domain.Event
import org.sunbird.dp.core.job.BaseJobConfig

class ContentUpdateConfig (override val config: Config) extends BaseJobConfig(config, "contentUpdateEvent") {
  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // content update specific
  val contentUpdateParallelism: Int = config.getInt("task.contentUpdate.parallelism")
  val kafkaIssueTopic: String = config.getString("kafka.output.topic")
  val issueEventSink = "contentUpdate-issue-event-sink"
  val failedEvent: OutputTag[Event] = OutputTag[Event]("failed-contentUpdate-events")

  //cassandra
  val ratingsSummaryTable: String = config.getString("ext-cassandra.ratings_summary_table")
  val dbKeyspace: String = config.getString("ext-cassandra.keyspace")
  val dbHost: String = config.getString("ext-cassandra.host")
  val dbPort: Int = config.getInt("ext-cassandra.port")

  //url
  val CONTENT_BASE_HOST: String = config.getString("url.base_host")
  val CONTENT_UPDATE_ENDPOINT: String = config.getString("url.content_update")
  val KM_BASE_HOST: String = config.getString("url.km_base_host")
  val CONTENT_SEARCH_ENDPOINT: String = config.getString("url.content_search")

  // constants
  val courseId = "courseid"
  val ACTIVITY_ID = "activityid"
  val activityType = "activitytype"
  val CONTENT = "content"
  val REQUEST = "request"
  val OFFSET = "offset"
  val LIMIT = "limit"
  val STATUS = "status"
  val IDENTIFIER = "identifier"
  val LAST_UPDATE_ON = "lastUpdatedOn"
  val DESC = "desc"
  val VERSION_KEY = "versionKey"
  val LAST_UPDATED_VERSION_KEY = "lastupdatedversionkey"
  val FILTERS = "filters"
  val SORTBY = "sort_By"
  val FIELDS = "fields"
  val RESULT = "result"
  val averageRatingScore="averageRatingScore"
  val sum_of_total_ratings="sum_of_total_ratings"
  val totalRatingsCount="totalRatingsCount"
  val total_number_of_ratings="total_number_of_ratings"

  // Consumers
  val ContentUpdateEventConsumer = "contentUpdate-consumer"

  // Functions
  val contentUpdateFunction = "ContentUpdateEventFunction"

}
