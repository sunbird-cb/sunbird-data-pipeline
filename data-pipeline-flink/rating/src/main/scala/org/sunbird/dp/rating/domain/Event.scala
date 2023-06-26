package org.sunbird.dp.rating.domain

import java.util
import org.sunbird.dp.core.domain.{Events, EventsPath}


class Event(eventMap: util.Map[String, Any]) extends Events(eventMap) {
  private val jobName = "ratingFeature"

  def versionNumber: Int = {
    telemetry.read[Int]("version").get

  }

  def action: String = {
    telemetry.read[String]("action").get
  }

  def activityId: String = {
    telemetry.read[String]("activity_id").get
  }

  def activityType: String = {
    telemetry.read[String]("activity_Type").get
  }

  def userId: String = {
    telemetry.read[String]("user_id").get
  }

  def createdDate: String = {
    telemetry.read[String]("created_Date").get
  }

  def prevValues: util.Map[String, AnyRef] = {
    telemetry.read[util.Map[String, AnyRef]]("prevValues").getOrElse(null)
  }

  def updatedValues: util.Map[String, AnyRef] = {
    telemetry.read[util.Map[String, AnyRef]]("updatedValues").getOrElse(null)
  }

  def markFailed(errorMsg: String): Unit = {
    telemetry.addFieldIfAbsent(EventsPath.FLAGS_PATH, new util.HashMap[String, Boolean])
    telemetry.addFieldIfAbsent("metadata", new util.HashMap[String, AnyRef])
    telemetry.add("metadata.validation_error", errorMsg)
    telemetry.add("metadata.src", jobName)
  }

}
