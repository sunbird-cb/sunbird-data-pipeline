package org.sunbird.assessment.submit.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.assessment.submit.domain.Event
import org.sunbird.dp.core.job.BaseJobConfig

class AssessmentConfig (override val config: Config) extends BaseJobConfig(config, "assessmentFeatureJob"){
  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
  // Kafka Topics Configuration
  val assessmentInputTopic: String = config.getString("kafka.assessment.input.topic")
  val passbookInputTopic: String = config.getString("kafka.generate.certificate.topic")

  // assessment specific
  val assessmentSubmitParallelism: Int = config.getInt("task.assessmentSubmit.parallelism")
  val passbookParallelism: Int = config.getInt("task.passbook.parallelism")
  val kafkaIssueTopic: String = config.getString("kafka.output.topic")
  val kafkaSuccessTopic: String = config.getString("kafka.success.topic")

  val issueEventSink = "assessment-submit-issue-event-sink"
  val passbookIssueEventSink = "passbook-issue-event-sink"
  val successIssueEventSink = "success-assessment-issue-event-sink"
  //val issueOutputTagName = "assessment-submit-issue-events"
  val failedEvent: OutputTag[Event] = OutputTag[Event]("failed-assessment-submit-events")
  val passbookFailedEvent: OutputTag[Event] = OutputTag[Event]("failed-passbook-events")
  val failedEventsOutputTag: OutputTag[Event] = OutputTag[Event]("assess-submit-failed-events")
  val updateSuccessEventsOutputTag: OutputTag[Event] = OutputTag[Event]("update-success-event-count")
  val failedEventCount = "failed-event-count"
  val updateCount = "update-count"
  //Cassandra
  val table: String = config.getString("ext-cassandra.table")
  val userTable: String = config.getString("ext-cassandra.user_table")
  val dbHost: String = config.getString("ext-cassandra.host")
  val dbPort: Int = config.getInt("ext-cassandra.port")
  val dbCoursesKeyspace: String = config.getString("ext-cassandra.courses_keyspace")
  val dbSunbirdKeyspace: String = config.getString("ext-cassandra.keyspace")
  val userAssessmentDataTable: String = config.getString("ext-cassandra.user_assessment_data_table")

  // constants
  val courseId = "courseid"
  val userId = "userid"
  val profileDetails = "profiledetails"
  val name = "name"
  val id = "id"
  val description = "description"
  val types = "type"
  val source = "source"
  val competencies= "competencies"
  val competency= "COMPETENCY"
  val competencies_v3= "competencies_v3"
  val osid= "osid"
  val competencySelfAttestedLevelValue = "competencySelfAttestedLevelValue"
  val competencySelfAttestedLevel = "competencySelfAttestedLevel"
  val competencySelfAttestedLevelName = "competencySelfAttestedLevelName"
  val competencySelfAttestedLevelDescription = "competencySelfAttestedLevelDescription"
  val competencyCBPCompletionLevel="competencyCBPCompletionLevel"
  val competencyCBPCompletionLevelValue="competencyCBPCompletionLevelValue"
  val competencyCBPCompletionLevelName="competencyCBPCompletionLevelName"
  val assessmentId = "assessmentid"
  val competencyType = "competencyType"
  val selectedLevelLevel = "selectedLevelLevel"
  val selectedLevelId = "selectedLevelId"
  val selectedLevelName = "selectedLevelName"
  val selectedLevelDescription = "selectedLevelDescription"
  val practiceQuestionSet = "Practice Question Set"
  val competencyAssessment = "Competency Assessment"
  val courseAssessment = "Course Assessment"
  val userIdKey = "user_id"
  val courseIdKey = "course_id"
  val batchIdKey = "batch_id"
  val contentIdKey = "content_id"
  val attemptIdKey = "attempt_id"
  val updatedOnKey = "updated_on"
  val createdOnKey = "created_on"
  val lastAttemptedOnKey = "last_attempted_on"
  val totalScoreKey = "total_score"
  val totalMaxScoreKey = "total_max_score"
  val CERTIFICATE_GENERATOR="Certificate Generator"
  val RELATED="related"
  val USER_ID="userId"
  val COURSE_ID="courseId"
  val FILTERS = "filters"
  val FIELDS = "fields"
  val REQUEST = "request"
  val IDENTIFIER = "identifier"
  val STATUS = "status"
  val CONTENT = "content"
  val RESULT = "result"
  val PRIMARY_CATEGORY="primaryCategory"
  val lastUpdatedOn="lastUpdatedOn"
  val DESC="desc"
  val SORT_BY="sortBy"

  //url
  val KM_BASE_HOST: String = config.getString("url.km_base_host")
  val content_search: String = config.getString("url.content_search")


  // Consumers
  val AssessmentConsumer = "assessment-submit-consumer"
  val PassbookConsumer = "competency-passbook-consumer"

  // Functions
  val assessmentFunction = "AssessmentSubmitFunction"
  val passbookFunction = "PassbookFunction"
}
