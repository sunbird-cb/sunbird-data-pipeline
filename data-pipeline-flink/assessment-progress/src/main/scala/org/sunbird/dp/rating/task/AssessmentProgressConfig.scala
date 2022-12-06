package org.sunbird.dp.rating.task
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.core.job.BaseJobConfig
import org.sunbird.dp.rating.domain.Event

class AssessmentProgressConfig (override val config: Config) extends BaseJobConfig(config, "assessmentProgress") {
  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // rating specific
  val assessmentParallelism: Int = config.getInt("task.assessment.parallelism")
  val kafkaIssueTopic: String = config.getString("kafka.output.topic")
  val issueEventSink = "assessment-issue-event-sink"
  val issueOutputTagName = "assessment-issue-events"
  val failedEvent: OutputTag[Event] = OutputTag[Event]("failed-assessment-events")

  //Cassandra
  val dbKeyspace: String = config.getString("ext-cassandra.keyspace")
  val dbHost: String = config.getString("ext-cassandra.host")
  val dbPort: Int = config.getInt("ext-cassandra.port")
  val assessmentTable:String=config.getString("ext-cassandra.TABLE_USER_ASSESSMENT_DATA")

  //url
  val ASSESSMENT_HOST:String=config.getString("url.assessment.host")
  val ASSESSMENT_HIERARCHY_RAED_PATH:String=config.getString("url.assessment.hierarchy.read.path")
  val QUESTION_PATH:String=config.getString("url.assessment.question.list.path")

  val ASSESSMENT_LEVEL_PARAMS:String=config.getString("fields.assessment.read.assessmentLevel.params")
  val SELECTION_LEVEL_PARAMS:String=config.getString("fields.assessment.read.sectionLevel.params")
  val QUESTION_LEVEL_PARAMS:String=config.getString("fields.assessment.read.questionLevel.params")

  //key
  val READ_ASSESSMENT: String = config.getString("key.read_assessment")
  val READ_QUESTION_LIST: String = config.getString("key.read_question_list")

  //duration
  val user_assessment_submission_duration:String=config.getString("duration.user.assessment.submission.duration")

  //key
  val SBAPI_KEY:String=config.getString("key.sb.api.key")
  // constants
  val courseId = "courseid"
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
  val IDENTIFIER_REPLACER = "{identifier}"
  val X_AUTH_TOKEN = "x-authenticated-user-token"
  val AUTHORIZATION = "authorization"
  val RESPONSE_CODE = "responseCode"
  val  OK = "OK"
  val ASSESSMENT_HIERARCHY_READ_FAILED = "Assessment hierarchy read failed, failed to process request"
  val QUESTION_SET = "questionSet"
  val QUESTIONS = "questions"
  val PRIMARY_CATEGORY = "primaryCategory"
  val PRACTICE_QUESTION_SET = "Practice Question Set"
  val ASSESSMENT_ID_KEY = "assessmentId"
  val userId = "userid"
  val CHILDREN = "children"
  val MAX_QUESTIONS = "maxQuestions"
  val CHILD_NODES = "childNodes"
  val EXPECTED_DURATION = "expectedDuration"
  val SUCCESS="success"
  val ERROR_MESSAGE = "errmsg"
  val INVALID_USERID="Invalid UserId"
  val ASSESSMENT_ID_KEY_IS_NOT_PRESENT_IS_EMPTY = "Assessment Id Key is not present/is empty"
  val SEARCH = "search"
  val IDENTIFIER_LIST_IS_EMPTY = "Identifier List is Empty"
  val THE_QUESTIONS_IDS_PROVIDED_DONT_MATCH = "The Questions Ids Provided don't match the active user assessment session"
  val ASSESSMENT_ID_INVALID_SESSION_EXPIRED = "Assessment Id Invalid/Session Expired/Redis Cache doesn't have this question list details"
  val EDITOR_STATE = "editorState"
  val CHOICES = "choices"
  val FTB_QUESTION = "FTB Question"
  val OPTIONS = "options"
  val RHS_CHOICES = "rhsChoices";
  val MTF_QUESTION = "MTF Question"
  // Consumers
  val AssessmentConsumer = "assessment-consumer"
  val NOT_SUBMITTED = "NOT SUBMITTED"
  val START_TIME = "starttime"
  val END_TIME = "endtime"
  val ASSESSMENT_READ_RESPONSE = "assessmentreadresponse"
  val ASSESSMENT_DATA_START_TIME_NOT_UPDATED = "Assessment Data & Start Time not updated in the DB! Please check!"
  val USER_ID_DOESNT_EXIST = "User Id doesn't exist! Please supply a valid user Id"
  val FAILED="failed"
  // Functions
  val assessmentFunction = "AssessmentFunction"





}
