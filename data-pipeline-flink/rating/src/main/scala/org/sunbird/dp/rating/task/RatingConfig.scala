package org.sunbird.dp.rating.task
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.core.job.BaseJobConfig
import org.sunbird.dp.rating.domain.Event

class RatingConfig (override val config: Config) extends BaseJobConfig(config, "RatingJob") {
  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // rating specific
  val ratingParallelism: Int = config.getInt("task.rating.parallelism")
  val kafkaIssueTopic: String = config.getString("kafka.output.topic")
  val issueEventSink = "rating-issue-event-sink"
  val issueOutputTagName = "rating-issue-events"
  val failedEvent: OutputTag[Event] = OutputTag[Event]("failed-rating-events")

  //Cassandra
  val courseTable: String = config.getString("ext-cassandra.course_table")
  val ratingsTable: String = config.getString("ext-cassandra.ratings_table")
  val ratingsSummaryTable: String = config.getString("ext-cassandra.ratings_summary_table")
  val ratingsLookupTable: String = config.getString("ext-cassandra.ratings_lookup_table")
  val dbKeyspace: String = config.getString("ext-cassandra.keyspace")
  val dbHost: String = config.getString("ext-cassandra.host")
  val dbPort: Int = config.getInt("ext-cassandra.port")
  val dbCoursesKeyspace: String = config.getString("ext-cassandra.courses_keyspace")

  //url
  val CONTENT_BASE_HOST:String=config.getString("url.base_host")
  val CONTENT_UPDATE_ENDPOINT:String=config.getString("url.content_update")
  val KM_BASE_HOST:String=config.getString("url.km_base_host")
  val CONTENT_SEARCH_ENDPOINT:String=config.getString("url.content_search")

  // constants
  val courseId = "courseid"
  val userId = "userid"
  val activityId = "activityid"
  val activityType = "activitytype"
  val CONTENT="content"
  val REQUEST="request"
  val AVERAGE_RATING="me_averageRating"
  val TOTAL_RATING_COUNT="me_totalRatingsCount"
  val OFFSET="offset"
  val LIMIT="limit"
  val STATUS="status"
  val IDENTIFIER="identifier"
  val LAST_UPDATE_ON="lastUpdatedOn"
  val DESC="desc"
  val VERSION_KEY="versionKey"
  val FILTERS="filters"
  val SORTBY="sort_By"
  val FIELDS="fields"
  val RESULT="result"
  // Consumers
  val RatingConsumer = "rating-consumer"

  // Functions
  val ratingFunction = "RatingFunction"


}
