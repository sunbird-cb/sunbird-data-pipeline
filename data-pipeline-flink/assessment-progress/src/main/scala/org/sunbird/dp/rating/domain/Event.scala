package org.sunbird.dp.rating.domain

import java.util
import org.sunbird.dp.core.domain.{Events, EventsPath}


class Event(eventMap: util.Map[String, Any]) extends Events(eventMap) {
  private val jobName = "assessmentProgress"

  def key:String={
    telemetry.read[String]("key").get
  }

  def userId: String = {
    telemetry.read[String]("userId").get
  }

  def assessmentIdentifier:String = {
    telemetry.read[String]("assessmentIdentifier").get
  }

  def token:String={
    telemetry.read[String]("x-authenticated-user-token").get
  }

  def requestBody:util.Map[String,Any]={
    telemetry.read[util.HashMap[String,Any]]("requestBody").get
  }

  def markFailed(errorMsg: String): Unit = {
    telemetry.addFieldIfAbsent(EventsPath.FLAGS_PATH, new util.HashMap[String, Boolean])
    telemetry.addFieldIfAbsent("metadata", new util.HashMap[String, AnyRef])
    telemetry.add("metadata.validation_error", errorMsg)
    telemetry.add("metadata.src", jobName)
  }

}
