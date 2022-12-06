package org.sunbird.dp.contentupdate.domain

import org.sunbird.dp.core.domain.{Events, EventsPath}

import java.util

class Event(eventMap: util.Map[String, Any]) extends Events(eventMap) {
  private val jobName = "contentUpdateEvent"

  def activityType: String = {
    telemetry.read[String]("activityType").get
  }

  def markFailed(errorMsg: String): Unit = {
    telemetry.addFieldIfAbsent(EventsPath.FLAGS_PATH, new util.HashMap[String, Boolean])
    telemetry.addFieldIfAbsent("metadata", new util.HashMap[String, AnyRef])
    telemetry.add("metadata.validation_error", errorMsg)
    telemetry.add("metadata.src", jobName)
  }
}
